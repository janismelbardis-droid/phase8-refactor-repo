from __future__ import annotations

import json
import os
import threading
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Sequence, Tuple

import pandas as pd

from app.data_binance import (
    _exact_1m_window_paths,
    _legacy_exact_1m_window_paths,
    _find_covering_1m_cache,
    _find_missing_1m_ranges,
    _local_1m_store_day_paths,
    _local_1m_store_dir,
    _load_local_1m_store_window,
    _parse_kline_cache_name,
    _read_cached_1m_frame,
    fetch_klines_1m_futures,
)
from app.indicators_streaming import (
    _find_covering_indicator_cache,
    _indicator_cache_paths,
    _local_indicator_store_day_paths,
    _local_indicator_store_dir,
    simulate_multitf_indicators,
)
from app.ema_settings import ema_settings_signature, normalize_ema_settings
from app.rsi_settings import normalize_rsi_settings, rsi_settings_signature
from app.strategy_requirements import (
    ALL_FAMILIES,
    DEFAULT_COMPUTE_FAMILIES,
    FIELD_TO_FAMILY,
    FAMILY_EMA,
    FAMILY_RSI,
    canonicalize_family_selection,
    normalize_required_fields,
    feature_signature,
    normalize_required_families,
)
from app.utils_time import required_warmup_minutes


REPO_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_TIMEFRAMES = ["1m", "5m", "15m", "30m", "1h"]
DEFAULT_REQ_PER_MIN = 20.0
MAX_LOG_LINES = 400

ProgressCallback = Callable[[str, Optional[float]], None]
LogCallback = Callable[[str], None]


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _null_progress(_: str, __: Optional[float] = None) -> None:
    return


def _null_log(_: str) -> None:
    return


def _stop_requested(stop_evt: Optional[threading.Event]) -> bool:
    return bool(stop_evt is not None and stop_evt.is_set())


def _resolve_cache_dir(repo_root: Path, preferred: str) -> Path:
    first = Path(preferred or "data_cache")
    if not first.is_absolute():
        first = (repo_root / first).resolve()
    if first.exists():
        return first
    fallback = Path.home() / "Downloads" / "data_cache"
    if fallback.exists():
        return fallback
    return first


def _parse_utc(value: Any) -> Optional[pd.Timestamp]:
    raw = str(value or "").strip()
    if not raw:
        return None
    ts = pd.to_datetime(raw, utc=True, errors="raise")
    if getattr(ts, "tzinfo", None) is None:
        ts = ts.tz_localize("UTC")
    else:
        ts = ts.tz_convert("UTC")
    return ts


def _require_window(start_raw: Any, end_raw: Any) -> Tuple[pd.Timestamp, pd.Timestamp]:
    start = _parse_utc(start_raw)
    end = _parse_utc(end_raw)
    if start is None or end is None:
        raise ValueError("Both start and end UTC are required for this action.")
    if end <= start:
        raise ValueError("End UTC must be after start UTC.")
    return start, end


def _last_closed_1m_candle_utc(now_utc: Optional[pd.Timestamp] = None) -> pd.Timestamp:
    current = pd.to_datetime(now_utc if now_utc is not None else pd.Timestamp.now(tz="UTC"), utc=True)
    return current.floor("min") - pd.Timedelta(minutes=1)


def _cap_candle_window_end(
    start_utc: pd.Timestamp,
    end_utc: pd.Timestamp,
    *,
    log: Optional[LogCallback] = None,
    now_utc: Optional[pd.Timestamp] = None,
) -> Tuple[pd.Timestamp, bool]:
    safe_end = _last_closed_1m_candle_utc(now_utc=now_utc)
    if end_utc <= safe_end:
        return end_utc, False
    if safe_end <= start_utc:
        raise ValueError(
            f"End UTC is ahead of the last closed 1m candle ({safe_end.isoformat()}). "
            "Choose an earlier end time."
        )
    if log is not None:
        log(
            f"Requested end {end_utc.isoformat()} is ahead of the last closed 1m candle. "
            f"Using {safe_end.isoformat()} instead."
        )
    return safe_end, True


def _iter_window_days(start_utc: pd.Timestamp, end_utc: pd.Timestamp) -> List[pd.Timestamp]:
    start_day = pd.to_datetime(start_utc, utc=True).floor("D")
    end_norm = pd.to_datetime(end_utc, utc=True)
    if end_norm <= start_day:
        return [start_day]
    last_day = (end_norm - pd.Timedelta(microseconds=1)).floor("D")
    days: List[pd.Timestamp] = []
    cur = start_day
    while cur <= last_day:
        days.append(cur)
        cur = cur + pd.Timedelta(days=1)
    return days


def _normalize_timeframes(raw: Any) -> List[str]:
    if isinstance(raw, str):
        items = [part.strip() for part in raw.split(",")]
    elif isinstance(raw, (list, tuple, set, frozenset)):
        items = [str(part).strip() for part in raw]
    else:
        items = []
    cleaned = {item for item in items if item}
    if not cleaned:
        cleaned = set(DEFAULT_TIMEFRAMES)
    ordered = [tf for tf in DEFAULT_TIMEFRAMES if tf in cleaned]
    extras = sorted(cleaned.difference(ordered))
    return ordered + extras


def _normalize_string_items(raw: Any) -> List[str]:
    if raw is None:
        return []
    if isinstance(raw, str):
        items = [part.strip() for part in raw.split(",")]
    elif isinstance(raw, (list, tuple, set, frozenset)):
        items = [str(part).strip() for part in raw]
    else:
        items = []
    return [item for item in items if item]


def _coerce_req_per_min(raw: Any) -> float:
    try:
        value = float(raw)
    except Exception:
        value = DEFAULT_REQ_PER_MIN
    return max(5.0, min(300.0, value))


def _coerce_indicator_families(raw: Any) -> List[str]:
    if raw is None:
        return sorted(DEFAULT_COMPUTE_FAMILIES)
    return canonicalize_family_selection(raw)


def _coerce_indicator_fields(raw: Any) -> Optional[List[str]]:
    if raw is None:
        return None
    cleaned: List[str] = []
    seen = set()
    for field_name in _normalize_string_items(raw):
        if field_name in seen:
            continue
        if field_name not in FIELD_TO_FAMILY and not field_name.startswith("market_"):
            raise ValueError(f"Unknown indicator field '{field_name}'.")
        seen.add(field_name)
        cleaned.append(field_name)
    return cleaned


def _coerce_required_fields_for_families(indicator_families: Sequence[str]) -> Optional[Sequence[str]]:
    families = canonicalize_family_selection(indicator_families)
    if not families or set(families) == set(ALL_FAMILIES):
        return None
    return sorted(normalize_required_fields(families, include_defaults=False))


def _resolve_indicator_selection(raw_fields: Any, raw_families: Any) -> Tuple[List[str], List[str], Optional[List[str]]]:
    indicator_fields = _coerce_indicator_fields(raw_fields)
    if indicator_fields is not None:
        if not indicator_fields:
            return [], [], []
        required_fields = sorted(normalize_required_fields(indicator_fields, include_defaults=False))
        indicator_families = sorted(normalize_required_families(indicator_fields, include_defaults=False))
        return indicator_fields, indicator_families, required_fields
    indicator_families = _coerce_indicator_families(raw_families)
    required_fields = _coerce_required_fields_for_families(indicator_families)
    return [], indicator_families, list(required_fields) if required_fields is not None else None


def _requested_ema_profile_signature(required_fields: Optional[Sequence[str]], ema_settings: Optional[Any], timeframes: Sequence[str]) -> Optional[List[List[Any]]]:
    req_fams = normalize_required_families(required_fields, include_defaults=True)
    if FAMILY_EMA not in req_fams:
        return None
    return [list(item) for item in ema_settings_signature(ema_settings, timeframes=timeframes)]


def _requested_rsi_profile_signature(required_fields: Optional[Sequence[str]], rsi_settings: Optional[Any], timeframes: Sequence[str]) -> Optional[List[List[Any]]]:
    req_fams = normalize_required_families(required_fields, include_defaults=True)
    if FAMILY_RSI not in req_fams:
        return None
    return [list(item) for item in rsi_settings_signature(rsi_settings, timeframes=timeframes)]


def _window_cache_paths(
    cache_dir: Path,
    symbol: str,
    start_utc: pd.Timestamp,
    end_utc: pd.Timestamp,
    price_source: str,
) -> Tuple[Path, Path]:
    pq, csv = _exact_1m_window_paths(str(cache_dir), symbol, start_utc, end_utc, price_source)
    return (Path(pq), Path(csv))


def _legacy_window_cache_paths(
    cache_dir: Path,
    symbol: str,
    start_utc: pd.Timestamp,
    end_utc: pd.Timestamp,
    price_source: str,
) -> Tuple[Path, Path]:
    pq, csv = _legacy_exact_1m_window_paths(str(cache_dir), symbol, start_utc, end_utc, price_source)
    return (Path(pq), Path(csv))


def _read_json(path: Path) -> Optional[Dict[str, Any]]:
    try:
        with path.open("r", encoding="utf-8") as fh:
            payload = json.load(fh)
        return payload if isinstance(payload, dict) else None
    except Exception:
        return None


def _maybe_len(df: Optional[pd.DataFrame]) -> int:
    if isinstance(df, pd.DataFrame):
        return int(len(df))
    return 0


def _ts_to_str(value: Any) -> Optional[str]:
    try:
        ts = pd.to_datetime(value, utc=True, errors="coerce")
    except Exception:
        return None
    if pd.isna(ts):
        return None
    return str(ts)


def _file_size_mb(path: Path) -> Optional[float]:
    try:
        return round(path.stat().st_size / (1024.0 * 1024.0), 3)
    except Exception:
        return None


def _tick_manager():
    from app import tick_manager as tm

    return tm


def _scan_existing_tick_days(cache_dir: Path, symbol: str) -> List[str]:
    folder = cache_dir / "aggtrades_futures_daily" / symbol.upper()
    if not folder.is_dir():
        return []
    days = set()
    for entry in folder.iterdir():
        day_s = entry.name[:10]
        if len(day_s) == 10 and day_s[4] == "-" and day_s[7] == "-":
            days.add(day_s)
    return sorted(days)


def _tick_category(status: str, integrity: str) -> str:
    st = str(status or "").upper().strip()
    badge = str(integrity or "").upper().strip()
    if st == "MISSING" or badge == "MISSING":
        return "missing"
    if "CORRUPT" in st or badge.startswith("BAD"):
        return "corrupt"
    if st == "FINAL" or badge == "OK FINAL":
        return "final"
    if st == "OPEN" or badge == "LIVE OPEN":
        return "open"
    return "repairable"


def _clamp_progress(value: Optional[float]) -> float:
    try:
        pct = float(value if value is not None else 0.0)
    except Exception:
        pct = 0.0
    return max(0.0, min(100.0, pct))


def _map_progress_stage(value: Optional[float], start: float, end: float) -> float:
    start_pct = _clamp_progress(start)
    end_pct = _clamp_progress(end)
    if end_pct <= start_pct:
        return start_pct
    raw_pct = _clamp_progress(value)
    return start_pct + ((raw_pct / 100.0) * (end_pct - start_pct))


def _action_can_cancel(action: str) -> bool:
    return str(action or "").strip().lower() in {
        "tick_verify_range",
        "tick_download_range",
        "tick_update_open",
        "tick_repair_range",
    }


@dataclass
class DownloaderJobRecord:
    job_id: str
    action: str
    state: str = "queued"
    message: str = "Queued"
    progress_pct: float = 0.0
    result: Optional[Dict[str, Any]] = None
    error: Optional[str] = None
    logs: List[str] = field(default_factory=list)
    can_cancel: bool = False
    stop_requested: bool = False
    created_at: str = field(default_factory=_utc_now_iso)
    updated_at: str = field(default_factory=_utc_now_iso)


class DownloaderRuntimeService:
    def __init__(self, repo_root: Path = REPO_ROOT) -> None:
        self.repo_root = Path(repo_root)

    def inspect_cache(self, params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        raw = params or {}
        symbol = str(raw.get("symbol") or "BTCUSDT").strip().upper() or "BTCUSDT"
        cache_dir = _resolve_cache_dir(self.repo_root, str(raw.get("cache_dir") or "data_cache"))
        cache_dir.mkdir(parents=True, exist_ok=True)
        price_source = str(raw.get("price_source") or "LAST").strip().upper() or "LAST"
        timeframes = _normalize_timeframes(raw.get("timeframes"))
        indicator_fields, indicator_families, required_fields = _resolve_indicator_selection(
            raw.get("indicator_fields"),
            raw.get("indicator_families"),
        )
        ema_settings = normalize_ema_settings(raw.get("ema_indicators", raw.get("ema_settings")))
        rsi_settings = normalize_rsi_settings(raw.get("rsi_indicators", raw.get("rsi_settings")))
        ema_profile_signature = _requested_ema_profile_signature(required_fields, ema_settings, timeframes)
        rsi_profile_signature = _requested_rsi_profile_signature(required_fields, rsi_settings, timeframes)

        start_utc = _parse_utc(raw.get("start"))
        end_utc = _parse_utc(raw.get("end"))
        if start_utc is not None and end_utc is not None and end_utc <= start_utc:
            raise ValueError("End UTC must be after start UTC.")

        window_days: List[pd.Timestamp] = []
        if start_utc is not None and end_utc is not None:
            window_days = _iter_window_days(start_utc, end_utc)

        return {
            "scope": {
                "symbol": symbol,
                "cache_dir": str(cache_dir),
                "start": str(start_utc) if start_utc is not None else None,
                "end": str(end_utc) if end_utc is not None else None,
                "price_source": price_source,
                "timeframes": timeframes,
                "indicator_fields": indicator_fields,
                "indicator_families": indicator_families,
                "ema_settings": ema_settings,
                "rsi_settings": rsi_settings,
                "ema_profile_signature": ema_profile_signature,
                "rsi_profile_signature": rsi_profile_signature,
                "indicator_feature_signature": feature_signature(required_fields, include_defaults=True),
            },
            "tick": self._inspect_tick_cache(cache_dir, symbol, window_days),
            "candles": self._inspect_candle_cache(cache_dir, symbol, start_utc, end_utc, price_source),
            "indicators": self._inspect_indicator_cache(
                cache_dir,
                symbol,
                start_utc,
                end_utc,
                timeframes=timeframes,
                price_source=price_source,
                selected_fields=indicator_fields,
                indicator_families=indicator_families,
                required_fields=required_fields,
                ema_settings=ema_settings,
                rsi_settings=rsi_settings,
            ),
        }

    def get_tick_report(self, params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        raw = params or {}
        symbol = str(raw.get("symbol") or "BTCUSDT").strip().upper() or "BTCUSDT"
        day_raw = str(raw.get("day") or "").strip()
        if not day_raw:
            raise ValueError("day is required.")
        day_utc = pd.Timestamp(day_raw, tz="UTC")
        cache_dir = _resolve_cache_dir(self.repo_root, str(raw.get("cache_dir") or "data_cache"))
        tm = _tick_manager()

        expected_path = tm.day_path(str(cache_dir), symbol, day_utc)
        meta_path = tm.meta_path(expected_path)
        variants = tm._list_day_file_variants(expected_path)
        existing_path, kind = tm._find_day_file_variants(expected_path)
        meta = tm._read_meta(meta_path) if os.path.isfile(meta_path) else None
        report = None
        if existing_path and os.path.isfile(existing_path):
            report = tm._verify_day_file(existing_path, symbol, day_utc, compute_hash=True, stop_evt=None, log_cb=None)
        report_text = tm._format_verification_report_text(
            symbol,
            day_raw,
            expected_path,
            existing_path,
            kind,
            meta,
            report,
            variants,
            tm._utc_now(),
        )
        return {
            "symbol": symbol,
            "day": day_raw,
            "cache_dir": str(cache_dir),
            "expected_path": expected_path,
            "existing_path": existing_path,
            "variant_kind": kind,
            "variants": [{"path": path, "kind": variant_kind} for path, variant_kind in variants],
            "meta": meta,
            "report": report,
            "report_text": report_text,
        }

    def run_action(
        self,
        action: str,
        params: Optional[Dict[str, Any]] = None,
        *,
        progress_cb: Optional[ProgressCallback] = None,
        log_cb: Optional[LogCallback] = None,
        stop_evt: Optional[threading.Event] = None,
    ) -> Dict[str, Any]:
        progress = progress_cb or _null_progress
        log = log_cb or _null_log
        action_key = str(action or "").strip().lower()

        if action_key == "tick_verify_range":
            return self._run_tick_verify(params, progress=progress, log=log, stop_evt=stop_evt)
        if action_key == "tick_download_range":
            return self._run_tick_download(params, progress=progress, log=log, stop_evt=stop_evt)
        if action_key == "tick_update_open":
            return self._run_tick_update_open(params, progress=progress, log=log, stop_evt=stop_evt)
        if action_key == "tick_repair_range":
            return self._run_tick_repair(params, progress=progress, log=log, stop_evt=stop_evt)
        if action_key == "candle_download_window":
            return self._run_candle_download(params, progress=progress, log=log)
        if action_key == "indicator_build_window":
            return self._run_indicator_build(params, progress=progress, log=log)
        raise ValueError(f"Unsupported downloader action '{action}'.")

    def _inspect_tick_cache(self, cache_dir: Path, symbol: str, window_days: Sequence[pd.Timestamp]) -> Dict[str, Any]:
        tm = _tick_manager()
        now_utc = tm._utc_now()
        if window_days:
            day_strings = [pd.to_datetime(day, utc=True).strftime("%Y-%m-%d") for day in window_days]
        else:
            day_strings = _scan_existing_tick_days(cache_dir, symbol)[-60:]

        rows: List[Dict[str, Any]] = []
        totals = {"days": 0, "final": 0, "open": 0, "repairable": 0, "corrupt": 0, "missing": 0}

        for day_s in day_strings:
            day_utc = pd.Timestamp(day_s, tz="UTC")
            expected_path = Path(tm.day_path(str(cache_dir), symbol, day_utc))
            meta_path = Path(tm.meta_path(str(expected_path)))
            meta = tm._read_meta(str(meta_path)) if meta_path.is_file() else None
            existing_path_raw, kind = tm._find_day_file_variants(str(expected_path))
            if existing_path_raw is None:
                status = "MISSING"
                integrity = "MISSING"
                existing_path = None
                variants = []
            else:
                existing_path = Path(existing_path_raw)
                variants = [{"path": path, "kind": variant_kind} for path, variant_kind in tm._list_day_file_variants(str(expected_path))]
                if kind == "TINY":
                    status = "TINY_FILE"
                elif kind == "TMP":
                    status = "TEMP_PRESENT"
                elif kind == "PART":
                    status = "PARTIAL_FILE"
                else:
                    status = tm._status_for_day(day_s, meta, now_utc) if meta is not None else str(kind or "UNKNOWN")
                integrity = tm._integrity_badge(day_s, meta, kind, now_utc)

            category = _tick_category(status, integrity)
            totals["days"] += 1
            totals[category] += 1
            rows.append(
                {
                    "day": day_s,
                    "status": status,
                    "integrity": integrity,
                    "category": category,
                    "rows": int(meta.get("rows")) if isinstance(meta, dict) and meta.get("rows") is not None else None,
                    "first_ts_utc": _ts_to_str(meta.get("first_ts_ms")) if isinstance(meta, dict) else None,
                    "last_ts_utc": _ts_to_str(meta.get("last_ts_ms")) if isinstance(meta, dict) else None,
                    "size_mb": _file_size_mb(existing_path) if isinstance(existing_path, Path) else None,
                    "path": str(existing_path) if isinstance(existing_path, Path) else str(expected_path),
                    "meta_path": str(meta_path),
                    "sha256_prefix": (str(meta.get("sha256") or "")[:12] or None) if isinstance(meta, dict) else None,
                    "has_meta": bool(meta),
                    "variant_kind": kind,
                    "variants": variants,
                }
            )

        return {
            "requested_days": day_strings,
            "totals": totals,
            "rows": rows,
            "note": "Select a UTC window to inspect specific day coverage." if not window_days else None,
        }

    def _inspect_candle_cache(
        self,
        cache_dir: Path,
        symbol: str,
        start_utc: Optional[pd.Timestamp],
        end_utc: Optional[pd.Timestamp],
        price_source: str,
    ) -> Dict[str, Any]:
        local_dir = Path(_local_1m_store_dir(str(cache_dir), symbol, price_source))
        base = {
            "price_source": price_source,
            "local_store_dir": str(local_dir),
            "exact_cache": None,
            "covering_cache": None,
            "local_days": [],
            "local_row_count": 0,
            "coverage_pct": None,
            "missing_ranges": [],
            "note": None,
        }
        if start_utc is None or end_utc is None:
            base["note"] = "Choose a UTC window to inspect exact coverage and missing candle segments."
            return base

        exact_parquet, exact_csv = _window_cache_paths(cache_dir, symbol, start_utc, end_utc, price_source)
        legacy_exact_parquet, legacy_exact_csv = _legacy_window_cache_paths(cache_dir, symbol, start_utc, end_utc, price_source)
        exact_path: Optional[Path]
        if exact_parquet.is_file():
            exact_path = exact_parquet
        elif exact_csv.is_file():
            exact_path = exact_csv
        elif legacy_exact_parquet.is_file():
            exact_path = legacy_exact_parquet
        elif legacy_exact_csv.is_file():
            exact_path = legacy_exact_csv
        else:
            exact_path = None
        exact_df = _read_cached_1m_frame(str(exact_path)) if exact_path is not None else None
        exact_meta = _parse_kline_cache_name(str(exact_path)) if exact_path is not None else None
        if exact_path is not None:
            base["exact_cache"] = {
                "exists": True,
                "path": str(exact_path),
                "rows": _maybe_len(exact_df),
                "format": exact_path.suffix.lstrip(".").lower(),
                "start_utc": _ts_to_str(exact_meta.get("start_utc")) if isinstance(exact_meta, dict) else None,
                "end_utc": _ts_to_str(exact_meta.get("end_utc")) if isinstance(exact_meta, dict) else None,
                "size_mb": _file_size_mb(exact_path),
            }
        else:
            base["exact_cache"] = {"exists": False, "path": str(exact_parquet)}

        covering = _find_covering_1m_cache(str(cache_dir), symbol, start_utc, end_utc, price_source)
        if covering is not None:
            cover_path, cover_start, cover_end = covering
            base["covering_cache"] = {
                "exists": True,
                "path": cover_path,
                "start_utc": str(pd.to_datetime(cover_start, utc=True)),
                "end_utc": str(pd.to_datetime(cover_end, utc=True)),
                "format": Path(cover_path).suffix.lstrip(".").lower(),
                "size_mb": _file_size_mb(Path(cover_path)),
            }
        else:
            base["covering_cache"] = {"exists": False, "path": None}

        local_df = _load_local_1m_store_window(str(cache_dir), symbol, start_utc, end_utc, price_source)
        local_rows = _maybe_len(local_df)
        base["local_row_count"] = local_rows
        try:
            expected_rows = len(pd.date_range(start_utc.floor("min"), end_utc.floor("min"), freq="1min", tz="UTC"))
        except Exception:
            expected_rows = local_rows
        if expected_rows > 0:
            base["coverage_pct"] = round((local_rows / float(max(1, expected_rows))) * 100.0, 1)

        missing_ranges = _find_missing_1m_ranges(start_utc, end_utc, local_df)
        base["missing_ranges"] = [
            {
                "start_utc": str(pd.to_datetime(seg_start, utc=True)),
                "end_utc": str(pd.to_datetime(seg_end, utc=True)),
                "minutes": int(max(1, ((pd.to_datetime(seg_end, utc=True) - pd.to_datetime(seg_start, utc=True)).total_seconds() // 60) + 1)),
            }
            for seg_start, seg_end in missing_ranges
        ]

        counts_by_day: Dict[str, int] = {}
        if isinstance(local_df, pd.DataFrame) and not local_df.empty and "open_time" in local_df.columns:
            try:
                day_keys = pd.to_datetime(local_df["open_time"], utc=True).dt.strftime("%Y-%m-%d")
                counts_by_day = day_keys.value_counts().to_dict()
            except Exception:
                counts_by_day = {}

        for day in _iter_window_days(start_utc, end_utc):
            day_s = pd.to_datetime(day, utc=True).strftime("%Y-%m-%d")
            parquet_path, csv_path = _local_1m_store_day_paths(str(cache_dir), symbol, price_source, day)
            day_path = Path(parquet_path) if os.path.isfile(parquet_path) else (Path(csv_path) if os.path.isfile(csv_path) else None)
            base["local_days"].append(
                {
                    "day": day_s,
                    "has_store": day_path is not None,
                    "path": str(day_path) if day_path is not None else parquet_path,
                    "rows": int(counts_by_day.get(day_s, 0)),
                    "size_mb": _file_size_mb(day_path) if day_path is not None else None,
                }
            )

        return base

    def _inspect_indicator_cache(
        self,
        cache_dir: Path,
        symbol: str,
        start_utc: Optional[pd.Timestamp],
        end_utc: Optional[pd.Timestamp],
        *,
        timeframes: Sequence[str],
        price_source: str,
        selected_fields: Sequence[str],
        indicator_families: Sequence[str],
        required_fields: Optional[Sequence[str]],
        ema_settings: Optional[Any],
        rsi_settings: Optional[Any],
    ) -> Dict[str, Any]:
        requested_ema_sig = _requested_ema_profile_signature(required_fields, ema_settings, timeframes)
        requested_rsi_sig = _requested_rsi_profile_signature(required_fields, rsi_settings, timeframes)
        local_dir = Path(
            _local_indicator_store_dir(
                str(cache_dir),
                symbol,
                timeframes=list(timeframes),
                price_source=price_source,
                macd_impl="TRADINGVIEW",
                adx_impl="TRADINGVIEW",
                required_fields=required_fields,
                ema_settings=ema_settings,
                rsi_settings=rsi_settings,
                market_state_thresholds=None,
            )
        )
        out = {
            "timeframes": list(timeframes),
            "selected_fields": list(selected_fields),
            "indicator_families": list(indicator_families),
            "required_families": sorted(normalize_required_families(required_fields, include_defaults=True)),
            "ema_profile_signature": requested_ema_sig,
            "rsi_profile_signature": requested_rsi_sig,
            "feature_signature": feature_signature(required_fields, include_defaults=True),
            "profile_hash": local_dir.name,
            "local_store_dir": str(local_dir),
            "exact_cache": None,
            "covering_cache": None,
            "local_days": [],
            "coverage_days": 0,
            "missing_days": 0,
            "note": None,
        }
        if not indicator_families and required_fields == []:
            out["note"] = "Select one or more indicator rule fields to inspect indicator coverage for this profile."
            return out
        if start_utc is None or end_utc is None:
            out["note"] = "Choose a UTC window to inspect indicator cache coverage for this profile."
            return out

        exact_pq, exact_meta = _indicator_cache_paths(
            str(cache_dir),
            symbol,
            start_utc,
            end_utc,
            list(timeframes),
            price_source,
            "TRADINGVIEW",
            "TRADINGVIEW",
            required_fields=required_fields,
            ema_settings=ema_settings,
            rsi_settings=rsi_settings,
        )
        exact_meta_payload = _read_json(Path(exact_meta))
        out["exact_cache"] = {
            "exists": os.path.isfile(exact_pq) and os.path.isfile(exact_meta),
            "path": exact_pq,
            "meta_path": exact_meta,
            "created_at_utc": exact_meta_payload.get("created_at_utc") if isinstance(exact_meta_payload, dict) else None,
            "required_families": exact_meta_payload.get("required_families") if isinstance(exact_meta_payload, dict) else None,
            "ema_profile_signature": exact_meta_payload.get("ema_profile_signature") if isinstance(exact_meta_payload, dict) else None,
            "rsi_profile_signature": exact_meta_payload.get("rsi_profile_signature") if isinstance(exact_meta_payload, dict) else None,
        }

        covering = _find_covering_indicator_cache(
            str(cache_dir),
            symbol,
            start_utc,
            end_utc,
            list(timeframes),
            price_source,
            "TRADINGVIEW",
            "TRADINGVIEW",
            required_fields=required_fields,
            ema_settings=ema_settings,
            rsi_settings=rsi_settings,
        )
        if covering is not None:
            cover_pq, cover_meta_path, cover_meta = covering
            out["covering_cache"] = {
                "exists": True,
                "path": cover_pq,
                "meta_path": cover_meta_path,
                "start_utc": cover_meta.get("start_utc"),
                "end_utc": cover_meta.get("end_utc"),
                "created_at_utc": cover_meta.get("created_at_utc"),
                "required_families": cover_meta.get("required_families"),
                "ema_profile_signature": cover_meta.get("ema_profile_signature"),
                "rsi_profile_signature": cover_meta.get("rsi_profile_signature"),
            }
        else:
            out["covering_cache"] = {"exists": False, "path": None, "meta_path": None}

        for day in _iter_window_days(start_utc, end_utc):
            day_s = pd.to_datetime(day, utc=True).strftime("%Y-%m-%d")
            pq_path, meta_path = _local_indicator_store_day_paths(
                str(cache_dir),
                symbol,
                day,
                timeframes=list(timeframes),
                price_source=price_source,
                macd_impl="TRADINGVIEW",
                adx_impl="TRADINGVIEW",
                required_fields=required_fields,
                ema_settings=ema_settings,
                rsi_settings=rsi_settings,
                market_state_thresholds=None,
            )
            meta = _read_json(Path(meta_path))
            has_store = os.path.isfile(pq_path) and os.path.isfile(meta_path)
            if has_store:
                out["coverage_days"] += 1
            else:
                out["missing_days"] += 1
            out["local_days"].append(
                {
                    "day": day_s,
                    "has_store": has_store,
                    "path": pq_path,
                    "meta_path": meta_path,
                    "created_at_utc": meta.get("created_at_utc") if isinstance(meta, dict) else None,
                    "ema_profile_signature": meta.get("ema_profile_signature") if isinstance(meta, dict) else None,
                    "rsi_profile_signature": meta.get("rsi_profile_signature") if isinstance(meta, dict) else None,
                }
            )
        return out

    def _normalized_scope(self, params: Optional[Dict[str, Any]]) -> Dict[str, Any]:
        raw = params or {}
        indicator_fields, indicator_families, required_fields = _resolve_indicator_selection(
            raw.get("indicator_fields"),
            raw.get("indicator_families"),
        )
        ema_settings = normalize_ema_settings(raw.get("ema_indicators", raw.get("ema_settings")))
        rsi_settings = normalize_rsi_settings(raw.get("rsi_indicators", raw.get("rsi_settings")))
        return {
            "symbol": str(raw.get("symbol") or "BTCUSDT").strip().upper() or "BTCUSDT",
            "cache_dir": str(_resolve_cache_dir(self.repo_root, str(raw.get("cache_dir") or "data_cache"))),
            "start": raw.get("start"),
            "end": raw.get("end"),
            "price_source": str(raw.get("price_source") or "LAST").strip().upper() or "LAST",
            "timeframes": _normalize_timeframes(raw.get("timeframes")),
            "indicator_fields": indicator_fields,
            "indicator_families": indicator_families,
            "required_fields": required_fields,
            "ema_settings": ema_settings,
            "rsi_settings": rsi_settings,
            "warmup_minutes": max(0, int(raw.get("warmup_minutes") or 0)),
            "req_per_min": _coerce_req_per_min(raw.get("req_per_min")),
            "include_no_meta": bool(raw.get("include_no_meta", False)),
            "prefer_tail_resume": bool(raw.get("prefer_tail_resume", True)),
        }

    def _summary_with_scope(self, scope: Dict[str, Any], *, force_start: Optional[pd.Timestamp] = None, force_end: Optional[pd.Timestamp] = None) -> Dict[str, Any]:
        inspect_params = dict(scope)
        if force_start is not None and force_end is not None:
            inspect_params["start"] = str(force_start)
            inspect_params["end"] = str(force_end)
        return self.inspect_cache(inspect_params)

    def _run_tick_verify(
        self,
        params: Optional[Dict[str, Any]],
        *,
        progress: ProgressCallback,
        log: LogCallback,
        stop_evt: Optional[threading.Event] = None,
    ) -> Dict[str, Any]:
        scope = self._normalized_scope(params)
        start_utc, end_utc = _require_window(scope["start"], scope["end"])
        tm = _tick_manager()
        cache_dir = Path(scope["cache_dir"])
        symbol = str(scope["symbol"])
        day_list = _iter_window_days(start_utc, end_utc)
        total = len(day_list)
        verified: List[Dict[str, Any]] = []
        progress("Preparing verification...", 0.0)

        for index, day_utc in enumerate(day_list, start=1):
            if _stop_requested(stop_evt):
                progress("Stop requested. Ending verification safely...", ((index - 1) / float(max(1, total))) * 100.0)
                break
            day_s = pd.to_datetime(day_utc, utc=True).strftime("%Y-%m-%d")
            expected_path = tm.day_path(str(cache_dir), symbol, day_utc)
            meta_path = tm.meta_path(expected_path)
            existing_path, kind = tm._find_day_file_variants(expected_path)
            if existing_path is None:
                log(f"{day_s}: missing file")
                verified.append({"day": day_s, "status": "MISSING", "rows": 0, "sha256_prefix": None})
                progress(f"Verified {index}/{total}: {day_s} missing", (index / float(max(1, total))) * 100.0)
                continue

            meta = None
            report = None
            if existing_path != expected_path:
                log(f"{day_s}: salvaging {os.path.basename(existing_path)} into FINAL path before verification.")
                meta = tm._salvage_variant_into_final(
                    existing_path,
                    expected_path,
                    meta_path,
                    symbol,
                    day_utc,
                    log_cb=log,
                    stop_evt=None,
                    compute_hash=True,
                )
                if meta is not None and os.path.isfile(expected_path):
                    report = tm._verify_day_file(expected_path, symbol, day_utc, compute_hash=True, stop_evt=stop_evt, log_cb=None)
                    existing_path = expected_path
                    kind = "FINAL"
            else:
                log(f"{day_s}: verifying {os.path.basename(existing_path)}")
                report = tm._verify_day_file(existing_path, symbol, day_utc, compute_hash=True, stop_evt=stop_evt, log_cb=log)
                verify_status = str(report.get("status") or "").upper().strip()
                if int(report.get("rows") or 0) > 0 and not verify_status.startswith("CORRUPT"):
                    meta = tm._meta_from_verification(symbol, day_utc, report)
                    tm._write_meta_atomic(meta_path, meta)
                else:
                    log(f"{day_s}: verification failed ({verify_status})")

            effective_meta = meta if isinstance(meta, dict) else (tm._read_meta(meta_path) if os.path.isfile(meta_path) else None)
            verified.append(
                {
                    "day": day_s,
                    "status": str(report.get("status") or effective_meta.get("status") or kind or "UNKNOWN") if isinstance(report, dict) else str((effective_meta or {}).get("status") or kind or "UNKNOWN"),
                    "rows": int((report or {}).get("rows") or (effective_meta or {}).get("rows") or 0),
                    "sha256_prefix": (str((report or {}).get("sha256") or (effective_meta or {}).get("sha256") or "")[:12] or None),
                }
            )
            progress(f"Verified {index}/{total}: {day_s}", (index / float(max(1, total))) * 100.0)
            if _stop_requested(stop_evt):
                break

        return {
            "action": "tick_verify_range",
            "symbol": symbol,
            "days": [item["day"] for item in verified],
            "verified": verified,
            "cancelled": _stop_requested(stop_evt),
            "summary": self._summary_with_scope(scope, force_start=start_utc, force_end=end_utc),
        }

    def _run_tick_download(
        self,
        params: Optional[Dict[str, Any]],
        *,
        progress: ProgressCallback,
        log: LogCallback,
        stop_evt: Optional[threading.Event] = None,
    ) -> Dict[str, Any]:
        scope = self._normalized_scope(params)
        start_utc, end_utc = _require_window(scope["start"], scope["end"])
        tm = _tick_manager()
        cache_dir = Path(scope["cache_dir"])
        symbol = str(scope["symbol"])
        rate_limiter = tm.RateLimiter(scope["req_per_min"])
        day_list = _iter_window_days(start_utc, end_utc)
        results: List[Dict[str, Any]] = []
        total = len(day_list)

        for done, day_utc in enumerate(day_list):
            if _stop_requested(stop_evt):
                progress("Stop requested. Ending download safely...", (done / float(max(1, total))) * 100.0)
                break
            day_s = pd.to_datetime(day_utc, utc=True).strftime("%Y-%m-%d")

            def _day_progress(frac: Optional[float], text: str = "") -> None:
                overall = ((done + max(0.0, min(1.0, float(frac or 0.0)))) / float(max(1, total))) * 100.0
                progress(text or f"Downloading {day_s}...", overall)

            log(f"{day_s}: downloading or refreshing verified daily aggTrades cache.")
            ok = tm._download_day_full(
                str(cache_dir),
                symbol,
                day_utc,
                log_cb=log,
                progress_cb=_day_progress,
                stop_evt=stop_evt,
                rate_limiter=rate_limiter,
            )
            results.append({"day": day_s, "success": bool(ok)})
            progress(f"Completed {done + 1}/{total}: {day_s}", ((done + 1) / float(max(1, total))) * 100.0)
            if _stop_requested(stop_evt):
                break
            tm._sleep_cancel(max(0.0, tm.DAY_PAUSE_S), stop_evt)

        return {
            "action": "tick_download_range",
            "symbol": symbol,
            "days": [item["day"] for item in results],
            "results": results,
            "cancelled": _stop_requested(stop_evt),
            "summary": self._summary_with_scope(scope, force_start=start_utc, force_end=end_utc),
        }

    def _run_tick_update_open(
        self,
        params: Optional[Dict[str, Any]],
        *,
        progress: ProgressCallback,
        log: LogCallback,
        stop_evt: Optional[threading.Event] = None,
    ) -> Dict[str, Any]:
        scope = self._normalized_scope(params)
        tm = _tick_manager()
        cache_dir = Path(scope["cache_dir"])
        symbol = str(scope["symbol"])
        day_raw = str((params or {}).get("day") or "").strip()
        day_utc = pd.Timestamp(day_raw, tz="UTC") if day_raw else tm._utc_now().floor("D")
        rate_limiter = tm.RateLimiter(scope["req_per_min"])

        def _day_progress(frac: Optional[float], text: str = "") -> None:
            if frac is None:
                progress(text or f"Updating OPEN day {day_utc.strftime('%Y-%m-%d')}...", None)
                return
            progress(text or f"Updating OPEN day {day_utc.strftime('%Y-%m-%d')}...", max(0.0, min(100.0, float(frac) * 100.0)))

        ok = tm._append_update_open_day(
            str(cache_dir),
            symbol,
            day_utc,
            log_cb=log,
            progress_cb=_day_progress,
            stop_evt=stop_evt,
            rate_limiter=rate_limiter,
        )
        day_start = pd.to_datetime(day_utc, utc=True)
        day_end = day_start + pd.Timedelta(days=1)
        return {
            "action": "tick_update_open",
            "symbol": symbol,
            "day": day_start.strftime("%Y-%m-%d"),
            "success": bool(ok),
            "cancelled": _stop_requested(stop_evt),
            "summary": self._summary_with_scope(scope, force_start=day_start, force_end=day_end),
        }

    def _run_tick_repair(
        self,
        params: Optional[Dict[str, Any]],
        *,
        progress: ProgressCallback,
        log: LogCallback,
        stop_evt: Optional[threading.Event] = None,
    ) -> Dict[str, Any]:
        scope = self._normalized_scope(params)
        tm = _tick_manager()
        cache_dir = Path(scope["cache_dir"])
        symbol = str(scope["symbol"])
        include_no_meta = bool(scope["include_no_meta"])
        prefer_tail_resume = bool(scope["prefer_tail_resume"])
        rate_limiter = tm.RateLimiter(scope["req_per_min"])
        start_utc = _parse_utc(scope["start"])
        end_utc = _parse_utc(scope["end"])
        if start_utc is not None and end_utc is not None:
            day_strings = [pd.to_datetime(day, utc=True).strftime("%Y-%m-%d") for day in _iter_window_days(start_utc, end_utc)]
        else:
            day_strings = _scan_existing_tick_days(cache_dir, symbol)

        today_s = tm._utc_now().strftime("%Y-%m-%d")
        eligible = [day_s for day_s in day_strings if day_s != today_s]
        repaired: List[Dict[str, Any]] = []
        total = len(eligible)
        if not eligible:
            progress("Nothing to repair. Historical incomplete days were not found in the selected scope.", 100.0)
            return {
                "action": "tick_repair_range",
                "symbol": symbol,
                "days": [],
                "results": [],
                "summary": self._summary_with_scope(scope),
            }

        now_utc = tm._utc_now()
        for done, day_s in enumerate(eligible):
            if _stop_requested(stop_evt):
                progress("Stop requested. Ending repair safely...", (done / float(max(1, total))) * 100.0)
                break
            day_utc = pd.Timestamp(day_s, tz="UTC")
            expected_path = tm.day_path(str(cache_dir), symbol, day_utc)
            meta_path = tm.meta_path(expected_path)
            meta = tm._read_meta(meta_path) if os.path.isfile(meta_path) else None
            existing_path, kind = tm._find_day_file_variants(expected_path)
            if existing_path is None:
                progress(f"Skipping {day_s}: no cache file exists.", ((done + 1) / float(max(1, total))) * 100.0)
                repaired.append({"day": day_s, "mode": "missing", "success": False})
                continue

            if existing_path != expected_path:
                log(f"{day_s}: salvaging {os.path.basename(existing_path)} before repair.")
                salvaged = tm._salvage_variant_into_final(
                    existing_path,
                    expected_path,
                    meta_path,
                    symbol,
                    day_utc,
                    log_cb=log,
                    stop_evt=stop_evt,
                    compute_hash=False,
                )
                if salvaged is not None:
                    meta = salvaged
                    existing_path = expected_path
                    kind = "FINAL"
                else:
                    log(f"{day_s}: salvage failed, proceeding with repair decision.")

            if existing_path == expected_path and (not meta or meta.get("last_id") is None) and include_no_meta:
                log(f"{day_s}: rebuilding missing meta from existing file before repair.")
                meta = tm._build_meta_from_existing_file(existing_path, meta_path, symbol, day_utc, log_cb=log, stop_evt=stop_evt)

            badge = tm._integrity_badge(day_s, meta, kind, now_utc)
            status = tm._status_for_day(day_s, meta, now_utc) if meta is not None and kind == "FINAL" else badge
            if status == "FINAL" or badge == "OK FINAL":
                progress(f"Skipping {day_s}: already FINAL.", ((done + 1) / float(max(1, total))) * 100.0)
                repaired.append({"day": day_s, "mode": "skip_final", "success": True})
                continue

            def _day_progress(frac: Optional[float], text: str = "") -> None:
                frac_f = max(0.0, min(1.0, float(frac or 0.0)))
                overall = ((done + frac_f) / float(max(1, total))) * 100.0
                progress(text or f"Repairing {day_s}...", overall)

            if prefer_tail_resume and tm._can_resume_tail_from_meta(meta):
                log(f"{day_s}: resumable tail gap detected, attempting safe tail resume.")
                ok = tm._resume_partial_tail_day(
                    str(cache_dir),
                    symbol,
                    day_utc,
                    log_cb=log,
                    progress_cb=_day_progress,
                    stop_evt=stop_evt,
                    rate_limiter=rate_limiter,
                )
                if not ok and not _stop_requested(stop_evt):
                    log(f"{day_s}: tail resume did not complete, falling back to full verified re-download.")
                    ok = tm._download_day_full(
                        str(cache_dir),
                        symbol,
                        day_utc,
                        log_cb=log,
                        progress_cb=_day_progress,
                        stop_evt=stop_evt,
                        rate_limiter=rate_limiter,
                    )
                repaired.append({"day": day_s, "mode": "resume_or_full", "success": bool(ok)})
            else:
                if prefer_tail_resume:
                    log(f"{day_s}: not resumable ({badge}), performing full verified re-download.")
                else:
                    log(f"{day_s}: prefer-tail-resume is OFF, performing full verified re-download.")
                ok = tm._download_day_full(
                    str(cache_dir),
                    symbol,
                    day_utc,
                    log_cb=log,
                    progress_cb=_day_progress,
                    stop_evt=stop_evt,
                    rate_limiter=rate_limiter,
                )
                repaired.append({"day": day_s, "mode": "full_redownload", "success": bool(ok)})

            progress(f"Completed repair {done + 1}/{total}: {day_s}", ((done + 1) / float(max(1, total))) * 100.0)
            if _stop_requested(stop_evt):
                break
            tm._sleep_cancel(max(0.0, tm.DAY_PAUSE_S), stop_evt)

        return {
            "action": "tick_repair_range",
            "symbol": symbol,
            "days": [item["day"] for item in repaired],
            "results": repaired,
            "cancelled": _stop_requested(stop_evt),
            "summary": self._summary_with_scope(scope, force_start=start_utc, force_end=end_utc) if start_utc is not None and end_utc is not None else self._summary_with_scope(scope),
        }

    def _run_candle_download(
        self,
        params: Optional[Dict[str, Any]],
        *,
        progress: ProgressCallback,
        log: LogCallback,
    ) -> Dict[str, Any]:
        scope = self._normalized_scope(params)
        start_utc, end_utc = _require_window(scope["start"], scope["end"])
        requested_end_utc = end_utc
        end_utc, window_end_clamped = _cap_candle_window_end(start_utc, end_utc, log=log)
        warmup_minutes = int(scope["warmup_minutes"])
        effective_start = start_utc - pd.Timedelta(minutes=warmup_minutes)
        cache_dir = Path(scope["cache_dir"])
        symbol = str(scope["symbol"])
        price_source = str(scope["price_source"])
        progress("Loading 1m candles from local store and Binance as needed...", 0.0)
        df = fetch_klines_1m_futures(
            symbol,
            effective_start,
            end_utc,
            str(cache_dir),
            progress_cb=progress,
            price_source=price_source,
        )
        if df.empty:
            raise ValueError("No candles were returned for the requested window.")
        log(
            f"Materialized {len(df):,} candle rows for {symbol} {price_source} "
            f"from {df['open_time'].min()} to {df['close_time'].max()}."
        )
        progress("Candle download complete.", 100.0)
        return {
            "action": "candle_download_window",
            "symbol": symbol,
            "price_source": price_source,
            "window_start": str(start_utc),
            "window_end": str(end_utc),
            "requested_window_end": str(requested_end_utc),
            "window_end_clamped": bool(window_end_clamped),
            "effective_start": str(effective_start),
            "warmup_minutes": warmup_minutes,
            "rows": int(len(df)),
            "summary": self._summary_with_scope(scope, force_start=start_utc, force_end=end_utc),
        }

    def _run_indicator_build(
        self,
        params: Optional[Dict[str, Any]],
        *,
        progress: ProgressCallback,
        log: LogCallback,
    ) -> Dict[str, Any]:
        scope = self._normalized_scope(params)
        start_utc, end_utc = _require_window(scope["start"], scope["end"])
        requested_end_utc = end_utc
        end_utc, window_end_clamped = _cap_candle_window_end(start_utc, end_utc, log=log)
        cache_dir = Path(scope["cache_dir"])
        symbol = str(scope["symbol"])
        price_source = str(scope["price_source"])
        timeframes = list(scope["timeframes"])
        indicator_fields = list(scope.get("indicator_fields") or [])
        indicator_families = list(scope["indicator_families"])
        required_fields = scope.get("required_fields")
        ema_settings = scope.get("ema_settings")
        rsi_settings = scope.get("rsi_settings")
        requested_warmup_minutes = int(scope["warmup_minutes"])
        warmup_minutes = max(
            requested_warmup_minutes,
            int(required_warmup_minutes(timeframes, required_fields=required_fields, ema_settings=ema_settings, rsi_settings=rsi_settings) or 0),
        )
        effective_start = start_utc - pd.Timedelta(minutes=warmup_minutes)
        if not indicator_families and required_fields == []:
            raise ValueError("Select at least one indicator rule field before building the indicator store.")

        progress("Preparing source candles for indicator library build...", 2.0)
        log(
            f"Loading {price_source} 1m candles for {symbol} "
            f"from {effective_start.isoformat()} to {end_utc.isoformat()}."
        )
        df = fetch_klines_1m_futures(
            symbol,
            effective_start,
            end_utc,
            str(cache_dir),
            progress_cb=lambda message, pct=None: progress(
                message,
                6.0 if pct is None else _map_progress_stage(pct, 6.0, 48.0),
            ),
            price_source=price_source,
        )
        if df.empty:
            raise ValueError("No candles were returned for the requested indicator window.")

        log(f"Source candle window ready with {len(df):,} 1m rows.")
        progress("Source candles ready. Calculating and storing indicator streams...", 52.0)
        if indicator_fields:
            selected_preview = ", ".join(indicator_fields[:10])
            if len(indicator_fields) > 10:
                selected_preview += f", +{len(indicator_fields) - 10} more"
            log(
                f"Building indicator fields {selected_preview} "
                f"(families {', '.join(indicator_families)}) for timeframes {', '.join(timeframes)}."
            )
        else:
            log(
                f"Building indicator families {', '.join(indicator_families)} "
                f"for timeframes {', '.join(timeframes)}."
            )

        def _indicator_progress(message: str, pct: Optional[float] = None) -> None:
            mapped = 55.0 if pct is None else _map_progress_stage(pct, 55.0, 92.0)
            progress(message, mapped)

        streams = simulate_multitf_indicators(
            df,
            timeframes,
            progress_cb=_indicator_progress,
            macd_impl="TRADINGVIEW",
            adx_impl="TRADINGVIEW",
            cache_dir=str(cache_dir),
            symbol=symbol,
            start_utc=effective_start,
            end_utc=end_utc,
            price_source=price_source,
            required_fields=required_fields,
            ema_settings=ema_settings,
            rsi_settings=rsi_settings,
        )
        stream_rows = {tf: int(len(frame)) for tf, frame in (streams or {}).items() if isinstance(frame, pd.DataFrame)}
        log("Indicator streams computed. Refreshing cache coverage summary so the panels reflect the new files.")
        progress("Refreshing indicator coverage summary...", 96.0)
        summary = self._summary_with_scope(scope, force_start=start_utc, force_end=end_utc)
        log(
            f"Indicator library ready for {symbol} using families {', '.join(indicator_families)} "
            f"and timeframes {', '.join(timeframes)}."
        )
        progress("Indicator library build complete.", 100.0)
        return {
            "action": "indicator_build_window",
            "symbol": symbol,
            "price_source": price_source,
            "window_start": str(start_utc),
            "window_end": str(end_utc),
            "requested_window_end": str(requested_end_utc),
            "window_end_clamped": bool(window_end_clamped),
            "effective_start": str(effective_start),
            "requested_warmup_minutes": requested_warmup_minutes,
            "warmup_minutes": warmup_minutes,
            "timeframes": timeframes,
            "indicator_fields": indicator_fields,
            "indicator_families": indicator_families,
            "ema_settings": ema_settings,
            "rsi_settings": rsi_settings,
            "ema_profile_signature": _requested_ema_profile_signature(required_fields, ema_settings, timeframes),
            "rsi_profile_signature": _requested_rsi_profile_signature(required_fields, rsi_settings, timeframes),
            "feature_signature": feature_signature(required_fields, include_defaults=True),
            "stream_rows": stream_rows,
            "summary": summary,
        }


class DownloaderJobStore:
    def __init__(self, runtime: DownloaderRuntimeService) -> None:
        self.runtime = runtime
        self._lock = threading.Lock()
        self._jobs: Dict[str, DownloaderJobRecord] = {}
        self._stop_events: Dict[str, threading.Event] = {}

    def create_job(self, action: str, params: Optional[Dict[str, Any]]) -> DownloaderJobRecord:
        action_name = str(action or "").strip()
        job = DownloaderJobRecord(
            job_id=str(uuid.uuid4()),
            action=action_name,
            can_cancel=_action_can_cancel(action_name),
        )
        stop_evt = threading.Event()
        with self._lock:
            self._jobs[job.job_id] = job
            self._stop_events[job.job_id] = stop_evt
        thread = threading.Thread(
            target=self._run_job,
            args=(job.job_id, job.action, dict(params or {}), stop_evt),
            name=f"ui-v2-data-job-{job.job_id[:8]}",
            daemon=True,
        )
        thread.start()
        return job

    def get(self, job_id: str) -> Optional[DownloaderJobRecord]:
        with self._lock:
            return self._jobs.get(job_id)

    def cancel_job(self, job_id: str) -> Optional[DownloaderJobRecord]:
        with self._lock:
            job = self._jobs.get(job_id)
            if job is None:
                return None
            if not job.can_cancel:
                raise ValueError(f"Downloader action '{job.action}' cannot be cancelled safely.")
            if job.state in {"completed", "failed", "cancelled"}:
                return job
            stop_evt = self._stop_events.get(job_id)
            if stop_evt is not None:
                stop_evt.set()
            job.stop_requested = True
            job.state = "cancelling"
            job.message = "Stop requested. Waiting for the downloader to exit safely..."
            job.updated_at = _utc_now_iso()
            return job

    def _append_log(self, job_id: str, message: str) -> None:
        line = str(message or "").strip()
        if not line:
            return
        with self._lock:
            job = self._jobs[job_id]
            job.logs.append(line)
            if len(job.logs) > MAX_LOG_LINES:
                job.logs = job.logs[-MAX_LOG_LINES:]
            job.updated_at = _utc_now_iso()

    def _update(
        self,
        job_id: str,
        *,
        state: Optional[str] = None,
        message: Optional[str] = None,
        progress_pct: Optional[float] = None,
        result: Optional[Dict[str, Any]] = None,
        error: Optional[str] = None,
    ) -> None:
        with self._lock:
            job = self._jobs[job_id]
            if state is not None:
                job.state = state
            if message is not None:
                job.message = message
            if progress_pct is not None:
                job.progress_pct = _clamp_progress(progress_pct)
            if result is not None:
                job.result = result
            if error is not None:
                job.error = error
            job.updated_at = _utc_now_iso()

    def _run_job(self, job_id: str, action: str, params: Dict[str, Any], stop_evt: threading.Event) -> None:
        def progress(message: str, pct: Optional[float] = None) -> None:
            self._update(
                job_id,
                state="cancelling" if _stop_requested(stop_evt) else "running",
                message=message,
                progress_pct=pct or 0.0,
            )

        def log(message: str) -> None:
            self._append_log(job_id, message)

        try:
            self._update(job_id, state="running", message=f"Starting {action}...", progress_pct=1.0)
            result = self.runtime.run_action(action, params, progress_cb=progress, log_cb=log, stop_evt=stop_evt)
            if _stop_requested(stop_evt):
                self._update(
                    job_id,
                    state="cancelled",
                    message="Downloader action stopped.",
                    result=result,
                )
            else:
                self._update(job_id, state="completed", message="Downloader action complete.", progress_pct=100.0, result=result)
        except Exception as exc:
            self._update(job_id, state="failed", message="Downloader action failed.", error=str(exc))
        finally:
            with self._lock:
                self._stop_events.pop(job_id, None)
