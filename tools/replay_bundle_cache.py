from __future__ import annotations

from dataclasses import asdict
import hashlib
import json
from pathlib import Path
import pickle
from typing import Any, Dict, Mapping, Optional

import pandas as pd

from app.backtest_models import BacktestConfig, BacktestResult
from app.research.replay import extract_backtest_replay_context
from app.rules import entry_filter_payload_to_dict
from app.strategy_requirements import StreamRequirementSpec


REPLAY_BUNDLE_STORE_VERSION = 1
REPLAY_BUNDLE_SUBDIR = "replay_bundles"


def _normalize_required_fields(raw: Any) -> list[str]:
    if isinstance(raw, StreamRequirementSpec):
        return sorted(str(field).strip() for field in raw.normalized_fields() if str(field).strip())
    if isinstance(raw, (set, frozenset, list, tuple)):
        return sorted(str(field).strip() for field in raw if str(field).strip())
    return []


def _normalize_cfg(cfg: BacktestConfig | Mapping[str, Any]) -> Dict[str, Any]:
    if isinstance(cfg, BacktestConfig):
        return asdict(cfg)
    if isinstance(cfg, Mapping):
        return dict(cfg)
    raise TypeError("cfg must be BacktestConfig or mapping")


def build_replay_bundle_spec(
    *,
    symbol: str,
    start: Any,
    end: Any,
    warmup_start: Any,
    timeframes: list[str],
    bt_mode: str,
    price_source: str,
    macd_impl: str,
    adx_impl: str,
    required_fields: Any,
    rules_model: Mapping[str, Any],
    tab_group_join_mode: Mapping[str, Any],
    group_rule_join_mode: Mapping[str, Any],
    entry_filters: Mapping[str, Any],
    base_cfg: BacktestConfig | Mapping[str, Any],
) -> Dict[str, Any]:
    return {
        "store_version": REPLAY_BUNDLE_STORE_VERSION,
        "symbol": str(symbol or "").upper(),
        "start": str(pd.to_datetime(start, utc=True)),
        "end": str(pd.to_datetime(end, utc=True)),
        "warmup_start": str(pd.to_datetime(warmup_start, utc=True)),
        "timeframes": [str(tf) for tf in list(timeframes or []) if str(tf or "").strip()],
        "bt_mode": str(bt_mode or ""),
        "price_source": str(price_source or "LAST").upper(),
        "macd_impl": str(macd_impl or "TRADINGVIEW").upper(),
        "adx_impl": str(adx_impl or "TRADINGVIEW").upper(),
        "required_fields": _normalize_required_fields(required_fields),
        "rules_model": dict(rules_model or {}),
        "tab_group_join_mode": dict(tab_group_join_mode or {}),
        "group_rule_join_mode": {str(key): list(value) for key, value in dict(group_rule_join_mode or {}).items()},
        "entry_filters": entry_filter_payload_to_dict(entry_filters),
        "base_cfg": _normalize_cfg(base_cfg),
    }


def replay_bundle_key(spec: Mapping[str, Any]) -> str:
    payload = json.dumps(dict(spec or {}), sort_keys=True, ensure_ascii=False, default=str).encode("utf-8")
    return hashlib.sha1(payload).hexdigest()


def replay_bundle_path(cache_dir: Path, *, symbol: str, spec: Mapping[str, Any]) -> Path:
    return (
        Path(cache_dir)
        / REPLAY_BUNDLE_SUBDIR
        / str(symbol or "").upper()
        / f"replay_{replay_bundle_key(spec)}.pkl"
    )


def _build_base_result_lite(result: Any) -> Optional[BacktestResult]:
    if not isinstance(result, BacktestResult):
        return None
    equity_curve = result.equity_curve.copy() if isinstance(result.equity_curve, pd.DataFrame) else pd.DataFrame()
    return BacktestResult(
        config=result.config,
        start=result.start,
        end=result.end,
        trades=list(result.trades or []),
        equity_curve=equity_curve,
        summary=dict(result.summary or {}),
        replay_context=None,
    )


def _build_event_activity_lite(payload: Any) -> Optional[Dict[str, Dict[str, Any]]]:
    if payload is None:
        return None
    if not isinstance(payload, Mapping):
        return None
    out: Dict[str, Dict[str, Any]] = {}
    for key, value in dict(payload).items():
        if not isinstance(value, Mapping):
            continue
        out[str(key)] = dict(value)
    return out


def _build_signal_rows_lite(rows: Any) -> Optional[list[Dict[str, Any]]]:
    if rows is None:
        return None
    if not isinstance(rows, (list, tuple)):
        return None
    out: list[Dict[str, Any]] = []
    for row in list(rows):
        if not isinstance(row, Mapping):
            continue
        out.append(dict(row))
    return out


def load_replay_bundle(
    cache_dir: Path,
    *,
    symbol: str,
    spec: Mapping[str, Any],
) -> Optional[Dict[str, Any]]:
    path = replay_bundle_path(Path(cache_dir), symbol=symbol, spec=spec)
    if not path.is_file():
        return None
    try:
        with path.open("rb") as fh:
            payload = pickle.load(fh)
    except Exception:
        return None
    if not isinstance(payload, dict):
        return None
    if int(payload.get("store_version", -1)) != REPLAY_BUNDLE_STORE_VERSION:
        return None
    if str(payload.get("spec_key") or "") != replay_bundle_key(spec):
        return None
    replay_context = payload.get("replay_context")
    if not isinstance(replay_context, Mapping):
        return None
    base_result_lite = payload.get("base_result_lite")
    if base_result_lite is not None and not isinstance(base_result_lite, BacktestResult):
        payload["base_result_lite"] = None
    event_activity = payload.get("event_activity")
    if event_activity is not None and not isinstance(event_activity, Mapping):
        payload["event_activity"] = None
    signal_rows = payload.get("signal_rows")
    if signal_rows is not None and not isinstance(signal_rows, list):
        payload["signal_rows"] = None
    return payload


def save_replay_bundle(
    cache_dir: Path,
    *,
    symbol: str,
    spec: Mapping[str, Any],
    result: Any,
    base_result_lite: Any = None,
    event_activity: Any = None,
    signal_rows: Any = None,
) -> Optional[Path]:
    replay_context = extract_backtest_replay_context(result)
    if not replay_context:
        return None
    path = replay_bundle_path(Path(cache_dir), symbol=symbol, spec=spec)
    path.parent.mkdir(parents=True, exist_ok=True)
    existing_payload: Dict[str, Any] = {}
    if path.is_file():
        try:
            with path.open("rb") as fh:
                loaded = pickle.load(fh)
            if isinstance(loaded, dict):
                existing_payload = loaded
        except Exception:
            existing_payload = {}
    base_result_payload = _build_base_result_lite(base_result_lite if base_result_lite is not None else result)
    if base_result_payload is None:
        existing_base_result = existing_payload.get("base_result_lite")
        if isinstance(existing_base_result, BacktestResult):
            base_result_payload = existing_base_result
    event_activity_payload = _build_event_activity_lite(event_activity)
    if event_activity_payload is None:
        existing_event_activity = existing_payload.get("event_activity")
        if isinstance(existing_event_activity, Mapping):
            event_activity_payload = dict(existing_event_activity)
    signal_rows_payload = _build_signal_rows_lite(signal_rows)
    if signal_rows_payload is None:
        existing_signal_rows = existing_payload.get("signal_rows")
        if isinstance(existing_signal_rows, list):
            signal_rows_payload = [dict(row) for row in list(existing_signal_rows) if isinstance(row, Mapping)]
    payload = {
        "store_version": REPLAY_BUNDLE_STORE_VERSION,
        "spec_key": replay_bundle_key(spec),
        "created_utc": str(pd.Timestamp.now(tz="UTC")),
        "spec": dict(spec or {}),
        "replay_context": replay_context,
        "base_result_lite": base_result_payload,
        "event_activity": event_activity_payload,
        "signal_rows": signal_rows_payload,
    }
    with path.open("wb") as fh:
        pickle.dump(payload, fh, protocol=pickle.HIGHEST_PROTOCOL)
    return path
