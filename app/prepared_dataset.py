from __future__ import annotations

"""Helpers for safely reusing prepared backtest datasets in memory and on disk."""

from dataclasses import dataclass, field
import json
import os
import hashlib
from typing import Any, Dict, Iterable, FrozenSet, Mapping, Optional, Sequence, Tuple

import pandas as pd

from .ema_settings import ema_settings_signature
from .rsi_settings import rsi_settings_signature
from .domain.market_state import coerce_market_state_thresholds, market_state_thresholds_dict
from .strategy_requirements import (
    FAMILY_EMA,
    FAMILY_MARKET_AUG,
    FAMILY_RSI,
    OHLCV_FIELDS,
    normalize_required_families,
    normalize_required_fields,
)

PREPARED_DATASET_STORE_VERSION = 2
PREPARED_DATASET_SUBDIR = "prepared_datasets"


@dataclass
class PreparedDataset:
    symbol: str
    start: pd.Timestamp
    end: pd.Timestamp
    timeframes: Tuple[str, ...]
    price_source: str
    macd_impl: str
    adx_impl: str
    required_families: FrozenSet[str]
    ema_profile_signature: Optional[Tuple[Tuple[str, int, int], ...]]
    rsi_profile_signature: Optional[Tuple[Tuple[str, int, int], ...]]
    market_threshold_signature: Optional[Tuple[Tuple[str, float | int], ...]]
    df_1m_full: pd.DataFrame
    streams_full: Dict[str, pd.DataFrame]
    created_at: pd.Timestamp = field(default_factory=lambda: pd.Timestamp.now(tz="UTC"))


def _ts_utc(value: Any) -> pd.Timestamp:
    ts = pd.to_datetime(value, utc=True, errors="coerce")
    if pd.isna(ts):
        raise ValueError(f"Invalid UTC timestamp: {value!r}")
    return ts


def market_threshold_signature(thresholds: Optional[Any]) -> Optional[Tuple[Tuple[str, float | int], ...]]:
    if thresholds is None:
        return None
    try:
        if isinstance(thresholds, Mapping):
            payload = market_state_thresholds_dict(coerce_market_state_thresholds(dict(thresholds)))
        else:
            payload = market_state_thresholds_dict(thresholds)
    except Exception:
        return None
    return tuple(sorted(payload.items(), key=lambda item: item[0]))


def _ema_signature_covers_request(
    entry_sig: Optional[Tuple[Tuple[str, int, int], ...]],
    req_sig: Optional[Tuple[Tuple[str, int, int], ...]],
) -> bool:
    if req_sig is None:
        return True
    if entry_sig is None:
        return False
    try:
        entry_map = {str(label): (int(fast), int(slow)) for label, fast, slow in entry_sig}
        req_map = {str(label): (int(fast), int(slow)) for label, fast, slow in req_sig}
    except Exception:
        return False
    for label, pair in req_map.items():
        if entry_map.get(label) != pair:
            return False
    return True


def _rsi_signature_covers_request(
    entry_sig: Optional[Tuple[Tuple[str, int, int], ...]],
    req_sig: Optional[Tuple[Tuple[str, int, int], ...]],
) -> bool:
    if req_sig is None:
        return True
    if entry_sig is None:
        return False
    try:
        entry_map = {str(label): (int(length), int(smoothing)) for label, length, smoothing in entry_sig}
        req_map = {str(label): (int(length), int(smoothing)) for label, length, smoothing in req_sig}
    except Exception:
        return False
    for label, pair in req_map.items():
        if entry_map.get(label) != pair:
            return False
    return True


def build_prepared_dataset(
    *,
    symbol: str,
    start: Any,
    end: Any,
    timeframes: Sequence[str],
    price_source: str,
    macd_impl: str,
    adx_impl: str,
    required_fields: Optional[Any],
    market_state_thresholds: Optional[Any] = None,
    df_1m_full: pd.DataFrame,
    streams_full: Dict[str, pd.DataFrame],
    ema_settings: Optional[Any] = None,
    rsi_settings: Optional[Any] = None,
) -> PreparedDataset:
    families = frozenset(normalize_required_families(required_fields, include_defaults=True))
    ema_sig = ema_settings_signature(ema_settings, timeframes=timeframes) if FAMILY_EMA in families else None
    rsi_sig = rsi_settings_signature(rsi_settings, timeframes=timeframes) if FAMILY_RSI in families else None
    th_sig = market_threshold_signature(market_state_thresholds) if FAMILY_MARKET_AUG in families else None
    return PreparedDataset(
        symbol=str(symbol or "").upper(),
        start=_ts_utc(start),
        end=_ts_utc(end),
        timeframes=tuple(str(tf).strip() for tf in (timeframes or []) if str(tf).strip()),
        price_source=str(price_source or "LAST").upper(),
        macd_impl=str(macd_impl or "TRADINGVIEW").upper(),
        adx_impl=str(adx_impl or "TRADINGVIEW").upper(),
        required_families=families,
        ema_profile_signature=ema_sig,
        rsi_profile_signature=rsi_sig,
        market_threshold_signature=th_sig,
        df_1m_full=df_1m_full,
        streams_full=dict(streams_full or {}),
    )


def prepared_dataset_identity_key(entry: PreparedDataset) -> Tuple[Any, ...]:
    return (
        entry.symbol,
        entry.start.value,
        entry.end.value,
        tuple(entry.timeframes),
        entry.price_source,
        entry.macd_impl,
        entry.adx_impl,
        tuple(sorted(entry.required_families)),
        entry.ema_profile_signature,
        entry.rsi_profile_signature,
        entry.market_threshold_signature,
    )


def slice_df_1m_window(df_1m: pd.DataFrame, start: Any, end: Any) -> pd.DataFrame:
    if not isinstance(df_1m, pd.DataFrame):
        return pd.DataFrame()
    if df_1m.empty:
        return df_1m.copy()
    start_ts = _ts_utc(start)
    end_ts = _ts_utc(end)
    try:
        if "open_time" in df_1m.columns:
            ot = pd.to_datetime(df_1m["open_time"], utc=True, errors="coerce")
            mask = (ot >= start_ts) & (ot <= end_ts)
            return df_1m.loc[mask].copy()
        if "close_time" in df_1m.columns:
            ct = pd.to_datetime(df_1m["close_time"], utc=True, errors="coerce")
            mask = (ct >= start_ts) & (ct <= end_ts)
            return df_1m.loc[mask].copy()
        idx = pd.to_datetime(df_1m.index, utc=True, errors="coerce")
        return df_1m.loc[(idx >= start_ts) & (idx <= end_ts)].copy()
    except Exception:
        return df_1m.copy()


def slice_streams_window(
    streams_full: Mapping[str, pd.DataFrame],
    start: Any,
    end: Any,
    *,
    timeframes: Optional[Sequence[str]] = None,
) -> Dict[str, pd.DataFrame]:
    out: Dict[str, pd.DataFrame] = {}
    start_ts = _ts_utc(start)
    end_ts = _ts_utc(end)
    want_raw = [str(tf).strip() for tf in (timeframes or []) if str(tf).strip()]
    if want_raw and "1m" not in want_raw and isinstance((streams_full or {}).get("1m"), pd.DataFrame):
        want_raw = ["1m", *want_raw]
    want = tuple(dict.fromkeys(want_raw))
    items = ((tf, streams_full.get(tf)) for tf in want) if want else (streams_full or {}).items()
    for tf, df in items:
        if not isinstance(df, pd.DataFrame):
            out[str(tf)] = pd.DataFrame()
            continue
        if df.empty:
            out[str(tf)] = df.copy()
            continue
        try:
            idx = pd.to_datetime(df.index, utc=True, errors="coerce")
            out[str(tf)] = df.loc[(idx >= start_ts) & (idx <= end_ts)].copy()
        except Exception:
            out[str(tf)] = df.copy()
    return out


def _request_signature(
    *,
    symbol: str,
    timeframes: Sequence[str],
    price_source: str,
    macd_impl: str,
    adx_impl: str,
    required_fields: Optional[Any],
    market_state_thresholds: Optional[Any] = None,
    ema_settings: Optional[Any] = None,
    rsi_settings: Optional[Any] = None,
) -> Tuple[
    str,
    Tuple[str, ...],
    str,
    str,
    str,
    FrozenSet[str],
    Optional[Tuple[Tuple[str, int, int], ...]],
    Optional[Tuple[Tuple[str, int, int], ...]],
    Optional[Tuple[Tuple[str, float | int], ...]],
]:
    req_symbol = str(symbol or "").upper()
    req_tfs = tuple(str(tf).strip() for tf in (timeframes or []) if str(tf).strip())
    req_price = str(price_source or "LAST").upper()
    req_macd = str(macd_impl or "TRADINGVIEW").upper()
    req_adx = str(adx_impl or "TRADINGVIEW").upper()
    req_fams = frozenset(normalize_required_families(required_fields, include_defaults=True))
    req_ema_sig = ema_settings_signature(ema_settings, timeframes=req_tfs) if FAMILY_EMA in req_fams else None
    req_rsi_sig = rsi_settings_signature(rsi_settings, timeframes=req_tfs) if FAMILY_RSI in req_fams else None
    req_threshold_sig = market_threshold_signature(market_state_thresholds) if FAMILY_MARKET_AUG in req_fams else None
    return (req_symbol, req_tfs, req_price, req_macd, req_adx, req_fams, req_ema_sig, req_rsi_sig, req_threshold_sig)


def _entry_covers_request(
    entry: PreparedDataset,
    *,
    symbol: str,
    start: Any,
    end: Any,
    timeframes: Sequence[str],
    price_source: str,
    macd_impl: str,
    adx_impl: str,
    required_fields: Optional[Any],
    market_state_thresholds: Optional[Any] = None,
    ema_settings: Optional[Any] = None,
    rsi_settings: Optional[Any] = None,
) -> bool:
    req_symbol, req_tfs, req_price, req_macd, req_adx, req_fams, req_ema_sig, req_rsi_sig, req_threshold_sig = _request_signature(
        symbol=symbol,
        timeframes=timeframes,
        price_source=price_source,
        macd_impl=macd_impl,
        adx_impl=adx_impl,
        required_fields=required_fields,
        ema_settings=ema_settings,
        rsi_settings=rsi_settings,
        market_state_thresholds=market_state_thresholds,
    )
    req_start = _ts_utc(start)
    req_end = _ts_utc(end)
    if entry.symbol != req_symbol:
        return False
    if entry.price_source != req_price or entry.macd_impl != req_macd or entry.adx_impl != req_adx:
        return False
    if entry.start > req_start or entry.end < req_end:
        return False
    if not set(req_tfs).issubset(set(entry.timeframes)):
        return False
    if not set(req_fams).issubset(set(entry.required_families)):
        return False
    if not _ema_signature_covers_request(entry.ema_profile_signature, req_ema_sig):
        return False
    if not _rsi_signature_covers_request(entry.rsi_profile_signature, req_rsi_sig):
        return False
    if req_threshold_sig is not None and entry.market_threshold_signature != req_threshold_sig:
        return False
    return True


def _normalized_required_field_names(required_fields: Optional[Any]) -> FrozenSet[str]:
    if required_fields is None:
        return frozenset()
    try:
        normalized = normalize_required_fields(required_fields, include_defaults=True)
        return frozenset(str(field).strip() for field in (normalized or []) if str(field).strip())
    except Exception:
        pass
    try:
        return frozenset(str(field).strip() for field in (required_fields or []) if str(field).strip())
    except Exception:
        return frozenset()


def _stream_field_is_materialized(df: pd.DataFrame, field: str) -> bool:
    if not isinstance(df, pd.DataFrame) or field not in df.columns:
        return False
    if field in OHLCV_FIELDS:
        try:
            return bool(pd.to_numeric(df[field], errors="coerce").notna().any())
        except Exception:
            try:
                return bool(df[field].notna().any())
            except Exception:
                return False
    return True


def _entry_has_required_fields(entry: PreparedDataset, required_fields: Optional[Any]) -> bool:
    req_fields = _normalized_required_field_names(required_fields)
    if not req_fields:
        return True
    streams = entry.streams_full if isinstance(entry.streams_full, Mapping) else {}
    if not streams:
        return False
    for field in req_fields:
        field_s = str(field).strip()
        if not field_s:
            continue
        if not any(_stream_field_is_materialized(df, field_s) for df in streams.values()):
            return False
    return True


def find_covering_prepared_dataset(
    entries: Iterable[PreparedDataset],
    *,
    symbol: str,
    start: Any,
    end: Any,
    timeframes: Sequence[str],
    price_source: str,
    macd_impl: str,
    adx_impl: str,
    required_fields: Optional[Any],
    market_state_thresholds: Optional[Any] = None,
    ema_settings: Optional[Any] = None,
    rsi_settings: Optional[Any] = None,
) -> Optional[PreparedDataset]:
    req_start = _ts_utc(start)
    req_end = _ts_utc(end)
    req_tfs = {str(tf).strip() for tf in (timeframes or []) if str(tf).strip()}
    req_fams = normalize_required_families(required_fields, include_defaults=True)

    best: Optional[PreparedDataset] = None
    best_score: Optional[Tuple[int, int, int]] = None
    for entry in list(entries or []):
        if not isinstance(entry, PreparedDataset):
            continue
        if not _entry_covers_request(
            entry,
            symbol=symbol,
            start=req_start,
            end=req_end,
            timeframes=timeframes,
            price_source=price_source,
            macd_impl=macd_impl,
            adx_impl=adx_impl,
            required_fields=required_fields,
            ema_settings=ema_settings,
            rsi_settings=rsi_settings,
            market_state_thresholds=market_state_thresholds,
        ):
            continue
        try:
            span_ns = int((entry.end.value - entry.start.value))
        except Exception:
            span_ns = 2**63 - 1
        score = (span_ns, len(entry.required_families), len(entry.timeframes))
        if best is None or best_score is None or score < best_score:
            best = entry
            best_score = score
    return best


def _ensure_dir(path: str) -> None:
    os.makedirs(path, exist_ok=True)


def _prepared_store_folder(cache_dir: str, symbol: str) -> str:
    folder = os.path.join(str(cache_dir or "data_cache"), PREPARED_DATASET_SUBDIR, str(symbol or "").upper())
    _ensure_dir(folder)
    return folder


def _prepared_store_base_name(entry: PreparedDataset) -> str:
    parts = [
        f"v={PREPARED_DATASET_STORE_VERSION}",
        f"sym={entry.symbol}",
        f"start={entry.start.strftime('%Y%m%d%H%M%S')}",
        f"end={entry.end.strftime('%Y%m%d%H%M%S')}",
        f"tfs={','.join(entry.timeframes)}",
        f"price={entry.price_source}",
        f"macd={entry.macd_impl}",
        f"adx={entry.adx_impl}",
        f"req={','.join(sorted(entry.required_families))}",
        f"ema={entry.ema_profile_signature}",
        f"rsi={entry.rsi_profile_signature}",
        f"th={entry.market_threshold_signature}",
    ]
    digest = hashlib.sha1("|".join(parts).encode("utf-8")).hexdigest()[:16]
    return f"prep_{digest}"


def _write_df_file(df: pd.DataFrame, path_no_ext: str, *, with_index: bool) -> str:
    if with_index:
        payload = df.copy()
        idx_name = "__prepared_index__"
        payload.insert(0, idx_name, pd.to_datetime(payload.index, utc=True, errors="coerce"))
        preferred = path_no_ext + ".parquet"
        try:
            payload.to_parquet(preferred, index=False)
            return os.path.basename(preferred)
        except Exception:
            fallback = path_no_ext + ".csv"
            payload.to_csv(fallback, index=False)
            return os.path.basename(fallback)
    preferred = path_no_ext + ".parquet"
    try:
        df.to_parquet(preferred, index=False)
        return os.path.basename(preferred)
    except Exception:
        fallback = path_no_ext + ".csv"
        df.to_csv(fallback, index=False)
        return os.path.basename(fallback)


def _read_df_file(path: str, *, with_index: bool) -> pd.DataFrame:
    if str(path).lower().endswith(".parquet"):
        df = pd.read_parquet(path)
    else:
        df = pd.read_csv(path)
    if with_index:
        idx_col = None
        for cand in ("__prepared_index__", "__index__", "index"):
            if cand in df.columns:
                idx_col = cand
                break
        if idx_col is not None:
            idx = pd.to_datetime(df[idx_col], utc=True, errors="coerce")
            df = df.drop(columns=[idx_col])
            df.index = idx
        else:
            try:
                df.index = pd.to_datetime(df.index, utc=True, errors="coerce")
            except Exception:
                pass
    else:
        for col in ("open_time", "close_time"):
            if col in df.columns:
                df[col] = pd.to_datetime(df[col], utc=True, errors="coerce")
    return df


def save_prepared_dataset_to_disk(cache_dir: str, entry: PreparedDataset) -> Optional[str]:
    if not isinstance(entry, PreparedDataset):
        return None
    if not isinstance(entry.df_1m_full, pd.DataFrame) or entry.df_1m_full.empty:
        return None
    if not isinstance(entry.streams_full, dict) or not entry.streams_full:
        return None
    folder = _prepared_store_folder(cache_dir, entry.symbol)
    base = _prepared_store_base_name(entry)
    meta_path = os.path.join(folder, f"{base}.meta.json")
    df_file = _write_df_file(entry.df_1m_full, os.path.join(folder, f"{base}.df1m"), with_index=False)
    stream_files: Dict[str, str] = {}
    for tf, df in (entry.streams_full or {}).items():
        if not isinstance(df, pd.DataFrame):
            continue
        stream_files[str(tf)] = _write_df_file(df, os.path.join(folder, f"{base}.stream.{tf}"), with_index=True)
    meta = {
        "store_version": PREPARED_DATASET_STORE_VERSION,
        "created_at_utc": _ts_utc(entry.created_at).isoformat(),
        "symbol": entry.symbol,
        "start_utc": entry.start.isoformat(),
        "end_utc": entry.end.isoformat(),
        "timeframes": list(entry.timeframes),
        "price_source": entry.price_source,
        "macd_impl": entry.macd_impl,
        "adx_impl": entry.adx_impl,
        "required_families": sorted(entry.required_families),
        "ema_profile_signature": [list(item) for item in entry.ema_profile_signature] if entry.ema_profile_signature is not None else None,
        "rsi_profile_signature": [list(item) for item in entry.rsi_profile_signature] if entry.rsi_profile_signature is not None else None,
        "market_threshold_signature": list(entry.market_threshold_signature) if entry.market_threshold_signature is not None else None,
        "df_1m_file": df_file,
        "stream_files": stream_files,
    }
    tmp_meta = meta_path + ".tmp"
    with open(tmp_meta, "w", encoding="utf-8") as f:
        json.dump(meta, f, ensure_ascii=False, indent=2)
    os.replace(tmp_meta, meta_path)
    return meta_path


def _load_prepared_dataset_meta(meta_path: str) -> Optional[Dict[str, Any]]:
    try:
        with open(meta_path, "r", encoding="utf-8") as f:
            meta = json.load(f)
    except Exception:
        return None
    try:
        if int(meta.get("store_version", -1)) != PREPARED_DATASET_STORE_VERSION:
            return None
    except Exception:
        return None
    return meta


def _meta_to_prepared_dataset(meta_path: str, meta: Dict[str, Any]) -> Optional[PreparedDataset]:
    folder = os.path.dirname(meta_path)
    df_file = meta.get("df_1m_file")
    stream_files = dict(meta.get("stream_files") or {})
    if not df_file or not stream_files:
        return None
    df_path = os.path.join(folder, str(df_file))
    if not os.path.isfile(df_path):
        return None
    try:
        df_1m = _read_df_file(df_path, with_index=False)
        streams: Dict[str, pd.DataFrame] = {}
        for tf, filename in stream_files.items():
            path = os.path.join(folder, str(filename))
            if not os.path.isfile(path):
                return None
            streams[str(tf)] = _read_df_file(path, with_index=True)
        ema_sig_raw = meta.get("ema_profile_signature")
        ema_sig = tuple(tuple(item) for item in ema_sig_raw) if ema_sig_raw is not None else None
        rsi_sig_raw = meta.get("rsi_profile_signature")
        rsi_sig = tuple(tuple(item) for item in rsi_sig_raw) if rsi_sig_raw is not None else None
        th_sig_raw = meta.get("market_threshold_signature")
        th_sig = tuple(tuple(item) for item in th_sig_raw) if th_sig_raw is not None else None
        return PreparedDataset(
            symbol=str(meta.get("symbol") or "").upper(),
            start=_ts_utc(meta.get("start_utc")),
            end=_ts_utc(meta.get("end_utc")),
            timeframes=tuple(str(tf).strip() for tf in (meta.get("timeframes") or []) if str(tf).strip()),
            price_source=str(meta.get("price_source") or "LAST").upper(),
            macd_impl=str(meta.get("macd_impl") or "TRADINGVIEW").upper(),
            adx_impl=str(meta.get("adx_impl") or "TRADINGVIEW").upper(),
            required_families=frozenset(str(x) for x in (meta.get("required_families") or [])),
            ema_profile_signature=ema_sig,
            rsi_profile_signature=rsi_sig,
            market_threshold_signature=th_sig,
            df_1m_full=df_1m,
            streams_full=streams,
            created_at=_ts_utc(meta.get("created_at_utc") or pd.Timestamp.now(tz="UTC")),
        )
    except Exception:
        return None


def find_covering_prepared_dataset_on_disk(
    cache_dir: str,
    *,
    symbol: str,
    start: Any,
    end: Any,
    timeframes: Sequence[str],
    price_source: str,
    macd_impl: str,
    adx_impl: str,
    required_fields: Optional[Any],
    market_state_thresholds: Optional[Any] = None,
    ema_settings: Optional[Any] = None,
    rsi_settings: Optional[Any] = None,
) -> Optional[PreparedDataset]:
    folder = os.path.join(str(cache_dir or "data_cache"), PREPARED_DATASET_SUBDIR, str(symbol or "").upper())
    if not os.path.isdir(folder):
        return None
    req_start = _ts_utc(start)
    req_end = _ts_utc(end)
    candidates: list[Tuple[Tuple[int, int, int], str, Dict[str, Any]]] = []
    for name in os.listdir(folder):
        if not name.endswith('.meta.json'):
            continue
        meta_path = os.path.join(folder, name)
        meta = _load_prepared_dataset_meta(meta_path)
        if meta is None:
            continue
        try:
            entry_stub = PreparedDataset(
                symbol=str(meta.get("symbol") or "").upper(),
                start=_ts_utc(meta.get("start_utc")),
                end=_ts_utc(meta.get("end_utc")),
                timeframes=tuple(str(tf).strip() for tf in (meta.get("timeframes") or []) if str(tf).strip()),
                price_source=str(meta.get("price_source") or "LAST").upper(),
                macd_impl=str(meta.get("macd_impl") or "TRADINGVIEW").upper(),
                adx_impl=str(meta.get("adx_impl") or "TRADINGVIEW").upper(),
                required_families=frozenset(str(x) for x in (meta.get("required_families") or [])),
                ema_profile_signature=(tuple(tuple(item) for item in meta.get("ema_profile_signature")) if meta.get("ema_profile_signature") is not None else None),
                rsi_profile_signature=(tuple(tuple(item) for item in meta.get("rsi_profile_signature")) if meta.get("rsi_profile_signature") is not None else None),
                market_threshold_signature=(tuple(tuple(item) for item in meta.get("market_threshold_signature")) if meta.get("market_threshold_signature") is not None else None),
                df_1m_full=pd.DataFrame(),
                streams_full={},
            )
        except Exception:
            continue
        if not _entry_covers_request(
            entry_stub,
            symbol=symbol,
            start=req_start,
            end=req_end,
            timeframes=timeframes,
            price_source=price_source,
            macd_impl=macd_impl,
            adx_impl=adx_impl,
            required_fields=required_fields,
            ema_settings=ema_settings,
            rsi_settings=rsi_settings,
            market_state_thresholds=market_state_thresholds,
        ):
            continue
        try:
            span_ns = int(entry_stub.end.value - entry_stub.start.value)
        except Exception:
            span_ns = 2**63 - 1
        score = (span_ns, len(entry_stub.required_families), len(entry_stub.timeframes))
        candidates.append((score, meta_path, meta))
    if not candidates:
        return None
    candidates.sort(key=lambda item: item[0])
    for _, meta_path, meta in candidates:
        entry = _meta_to_prepared_dataset(meta_path, meta)
        if entry is None:
            continue
        if not _entry_has_required_fields(entry, required_fields):
            continue
        return entry
    return None
