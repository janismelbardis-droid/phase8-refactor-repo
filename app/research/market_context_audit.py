from __future__ import annotations

"""Market-context replay helpers for candle-by-candle audit windows."""

from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence

import math

import pandas as pd
import numpy as np


PRICE_FIELDS: tuple[str, ...] = ("open", "high", "low", "close", "volume")

CONTEXT_FIELDS: tuple[str, ...] = (
    "market_state",
    "market_bias",
    "market_phase",
    "market_confidence",
    "market_trend_state",
    "market_structure_state",
    "market_phase_state",
    "market_breakout_signal",
    "market_volume_context",
    "market_pullback_state",
    "market_trend_age",
    "market_phase_age",
    "market_breakout_signal_age",
    "market_pullback_state_age",
    "market_protected_high",
    "market_protected_low",
    "market_support_zone_low",
    "market_support_zone_high",
    "market_resistance_zone_low",
    "market_resistance_zone_high",
    "market_structure_last_high",
    "market_structure_last_low",
    "market_structure_note",
    "market_structure_basis",
    "market_structure_context",
    "market_structure_pivot_bias",
    "market_structure_pivot_count",
    "market_structure_swing_hh",
    "market_structure_swing_hl",
    "market_structure_swing_lh",
    "market_structure_swing_ll",
    "market_market_type",
    "market_trend_regime",
    "market_boundary_high",
    "market_boundary_low",
    "market_context_note",
    "market_context_reasons",
    "market_pa_pattern",
    "market_breakout_note",
    "range_filter_state",
    "range_filter_phase",
    "range_filter_buy",
    "range_filter_sell",
    "frama_state",
    "frama_break_up",
    "frama_break_down",
    "frama_mid_cross",
    "vidya_state",
    "ms",
    "angle",
    "atr_angle",
)

TRANSITION_FIELDS: tuple[str, ...] = (
    "market_state",
    "market_trend_state",
    "market_structure_state",
    "market_phase_state",
    "market_breakout_signal",
    "market_volume_context",
    "market_pullback_state",
)

EVENT_FIELDS: tuple[str, ...] = ()


def _jsonable(value: Any) -> Any:
    try:
        if pd.isna(value):
            return None
    except Exception:
        pass
    if value is None or isinstance(value, (str, bool, int, np.bool_, np.integer)):
        return value
    if isinstance(value, (float, np.floating)):
        return float(value) if math.isfinite(value) else None
    try:
        ts = pd.to_datetime(value, utc=True, errors="raise")
        if pd.notna(ts):
            return str(ts)
    except Exception:
        pass
    try:
        scalar = value.item()
    except Exception:
        scalar = None
    if scalar is not None and scalar is not value:
        return _jsonable(scalar)
    try:
        num = float(value)
    except Exception:
        return str(value)
    return float(num) if math.isfinite(num) else None


def _boolish(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    text = str(value or "").strip().lower()
    if text in {"1", "true", "yes", "on"}:
        return True
    if text in {"0", "false", "no", "off", ""}:
        return False
    try:
        return bool(value)
    except Exception:
        return False


def _upper_text(value: Any) -> str:
    return str(value or "").strip().upper()


def _split_reasons(value: Any) -> List[str]:
    raw = str(value or "").strip()
    if not raw:
        return []
    return [part.strip() for part in raw.split("|") if part.strip()]


def _row_payload(frame: pd.DataFrame, index: int, timestamp: pd.Timestamp) -> Dict[str, Any]:
    row = frame.iloc[index]
    payload: Dict[str, Any] = {
        "bar_index": int(index),
        "timestamp_utc": str(timestamp),
    }
    for field in PRICE_FIELDS + CONTEXT_FIELDS:
        if field in frame.columns:
            payload[field] = _jsonable(row.get(field))
        else:
            payload[field] = None
    payload["market_context_reason_list"] = _split_reasons(payload.get("market_context_reasons"))
    return payload


def _transition_label(field: str, to_value: Any) -> str:
    text = str(to_value or "").replace("_", " ").strip().title()
    mapping = {
        "market_state": f"Market -> {text}",
        "market_trend_state": f"Trend -> {text}",
        "market_structure_state": f"Structure -> {text}",
        "market_phase_state": f"Phase -> {text}",
        "market_breakout_signal": f"Breakout -> {text}",
        "market_volume_context": f"Volume -> {text}",
        "range_filter_state": f"Range -> {text}",
        "frama_state": f"Frama -> {text}",
        "vidya_state": f"Vidya -> {text}",
    }
    return mapping.get(field, f"{field} -> {text}")


def _event_label(field: str) -> str:
    mapping = {
        "range_filter_buy": "Range Buy Trigger",
        "range_filter_sell": "Range Sell Trigger",
        "frama_break_up": "FRAMA Break Up",
        "frama_break_down": "FRAMA Break Down",
        "frama_mid_cross": "FRAMA Mid Cross",
    }
    return mapping.get(field, field.replace("_", " ").title())


def _latest_context_summary(frame: pd.DataFrame) -> Dict[str, Any]:
    if not isinstance(frame, pd.DataFrame) or frame.empty:
        return {
            "timestamp_utc": None,
            "market_trend_state": None,
            "market_structure_state": None,
            "market_phase_state": None,
            "market_breakout_signal": None,
            "market_volume_context": None,
            "market_context_note": None,
        }
    ts = pd.to_datetime(frame.index[-1], utc=True, errors="coerce")
    row = frame.iloc[-1]
    return {
        "timestamp_utc": str(ts) if pd.notna(ts) else None,
        "market_trend_state": _jsonable(row.get("market_trend_state")),
        "market_structure_state": _jsonable(row.get("market_structure_state")),
        "market_phase_state": _jsonable(row.get("market_phase_state")),
        "market_breakout_signal": _jsonable(row.get("market_breakout_signal")),
        "market_volume_context": _jsonable(row.get("market_volume_context")),
        "market_pullback_state": _jsonable(row.get("market_pullback_state")),
        "market_context_note": _jsonable(row.get("market_context_note")),
    }


def build_market_context_audit_window(
    frame: pd.DataFrame,
    *,
    timeframe: str,
    max_bars: int = 300,
    end_utc: Any = None,
) -> Dict[str, Any]:
    if not isinstance(frame, pd.DataFrame) or frame.empty:
        return {
            "timeframe": str(timeframe or ""),
            "requested_bars": int(max(1, int(max_bars or 1))),
            "bar_count": 0,
            "window_start_utc": None,
            "window_end_utc": None,
            "anchor_timestamp_utc": None,
            "rows": [],
            "markers": [],
            "latest_context": {},
        }

    idx = pd.to_datetime(frame.index, utc=True, errors="coerce")
    work = frame.copy()
    work.index = idx
    work = work[~work.index.isna()]
    if work.empty:
        return {
            "timeframe": str(timeframe or ""),
            "requested_bars": int(max(1, int(max_bars or 1))),
            "bar_count": 0,
            "window_start_utc": None,
            "window_end_utc": None,
            "anchor_timestamp_utc": None,
            "rows": [],
            "markers": [],
            "latest_context": {},
        }

    if end_utc not in (None, ""):
        cutoff = pd.to_datetime(end_utc, utc=True, errors="coerce")
        if pd.notna(cutoff):
            work = work.loc[work.index <= cutoff]
    if work.empty:
        return {
            "timeframe": str(timeframe or ""),
            "requested_bars": int(max(1, int(max_bars or 1))),
            "bar_count": 0,
            "window_start_utc": None,
            "window_end_utc": None,
            "anchor_timestamp_utc": None,
            "rows": [],
            "markers": [],
            "latest_context": {},
        }

    requested_bars = max(1, min(1200, int(max_bars or 300)))
    work = work.tail(requested_bars).copy()
    work = work.reset_index()
    first_col = str(work.columns[0])
    if first_col != "timestamp_utc":
        work = work.rename(columns={first_col: "timestamp_utc"})
    timestamps = pd.to_datetime(work["timestamp_utc"], utc=True, errors="coerce")
    base = work.drop(columns=["timestamp_utc"])

    rows: List[Dict[str, Any]] = []
    markers: List[Dict[str, Any]] = []
    prev_payload: Optional[Dict[str, Any]] = None
    for i, ts in enumerate(timestamps):
        row_payload = _row_payload(base, i, ts)
        rows.append(row_payload)

        if prev_payload is not None:
            for field in TRANSITION_FIELDS:
                cur_value = _upper_text(row_payload.get(field))
                prev_value = _upper_text(prev_payload.get(field))
                if not cur_value or cur_value == prev_value:
                    continue
                markers.append(
                    {
                        "bar_index": int(i),
                        "timestamp_utc": str(ts),
                        "kind": "state_change",
                        "field": field,
                        "from": prev_payload.get(field),
                        "to": row_payload.get(field),
                        "label": _transition_label(field, row_payload.get(field)),
                    }
                )

        for field in EVENT_FIELDS:
            if _boolish(row_payload.get(field)):
                markers.append(
                    {
                        "bar_index": int(i),
                        "timestamp_utc": str(ts),
                        "kind": "event",
                        "field": field,
                        "label": _event_label(field),
                    }
                )
        prev_payload = row_payload

    latest_context = _latest_context_summary(base.set_index(timestamps))
    return {
        "timeframe": str(timeframe or ""),
        "requested_bars": int(requested_bars),
        "bar_count": int(len(rows)),
        "window_start_utc": str(timestamps.iloc[0]) if len(timestamps) else None,
        "window_end_utc": str(timestamps.iloc[-1]) if len(timestamps) else None,
        "anchor_timestamp_utc": str(timestamps.iloc[-1]) if len(timestamps) else None,
        "rows": rows,
        "markers": markers,
        "latest_context": latest_context,
    }


def build_timeframe_context_summary(frames: Mapping[str, pd.DataFrame]) -> Dict[str, Dict[str, Any]]:
    out: Dict[str, Dict[str, Any]] = {}
    for tf, frame in dict(frames or {}).items():
        out[str(tf)] = _latest_context_summary(frame)
    return out


def required_market_audit_fields(extra_fields: Optional[Iterable[str]] = None) -> List[str]:
    fields = set(PRICE_FIELDS) | set(CONTEXT_FIELDS)
    if extra_fields is not None:
        fields.update(str(item).strip() for item in extra_fields if str(item).strip())
    return sorted(fields)
