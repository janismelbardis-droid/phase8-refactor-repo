from __future__ import annotations

"""Indicator math and streaming state simulation (multi-timeframe), plus disk cache."""

import os
import json
import gzip
import hashlib
import collections
import time
import uuid
from concurrent.futures import ProcessPoolExecutor, as_completed
from dataclasses import dataclass
from typing import Optional, Any, Dict, List, Tuple

import numpy as np
import pandas as pd
try:
    import talib  # type: ignore
except Exception:  # pragma: no cover
    talib = None

from .constants import *
from .utils_time import interval_to_ms, required_warmup_minutes
from .data_binance import _ensure_dir
from .execution_planner import (
    env_flag as execution_env_flag,
    env_int as execution_env_int,
    resolve_worker_count,
    split_batches,
)
from .strategy_requirements import (
    ADX_FIELDS,
    EMA_FIELDS,
    ATR_FIELDS,
    FAMILY_EMA,
    FAMILY_RSI,
    FRAMA_FIELDS,
    MACD_LINE_FIELDS,
    MACD_STATE_FIELDS,
    MARKET_AUG_FIELDS,
    OHLCV_FIELDS,
    RANGE_FILTER_FIELDS,
    RSI_FIELDS,
    STOCH_RSI_FIELDS,
    TAKER_BIAS_FIELDS,
    VIDYA_FIELDS,
    feature_signature,
    normalize_required_families,
    normalize_required_fields,
)
from .taker_bias import compute_taker_bias_payload
from .domain.market_state.thresholds import (
    MARKET_STATE_LABELS,
    MARKET_STATE_BIASES,
    MARKET_STATE_PHASES,
    VIDYA_ANGLE_STATES,
    VIDYA_ANGLE_ACCEL_STATES,
    MarketStateThresholds,
    DEFAULT_MARKET_STATE_THRESHOLDS,
    market_state_thresholds_dict,
    coerce_market_state_thresholds,
)
from .domain.market_state.pipeline import (
    finalize_market_state_payload,
    run_basic_market_state_pipeline,
    run_classic_market_state_pipeline,
    run_dynamic_market_state_pipeline,
)

INDICATOR_PARALLEL_MIN_ROWS_DEFAULT = 4096
INDICATOR_PARALLEL_MIN_DAYS_DEFAULT = 2
INDICATOR_PARALLEL_MAX_CHUNK_DAYS_DEFAULT = 7

MARKET_STATE_ROW_BASE_COLUMNS: Tuple[str, ...] = (
    "price",
    "open",
    "high",
    "low",
    "close",
    "volume",
    "open_time_ms",
    "close_time_ms",
    "tf",
    "timeframe",
    "symbol",
    "vidya_state",
    "frama_state",
    "range_filter_state",
    "range_filter_phase",
    "ms",
    "angle",
    "atr_angle",
    "macd",
    "signal",
    "atr",
    "vidya_delta_pct",
    "vidya_angle_deg",
    "vidya_angle_deg_norm",
)

def _ppo_flip_age(d_ppo: "np.ndarray") -> Optional[int]:
    """
    Candles since PPO dot last flipped color.
    Dot color rule: GREEN if d_ppo[i] > d_ppo[i-1] else RED.
    """
    if d_ppo is None or len(d_ppo) < 3:
        return None

    colors: List[Optional[str]] = [None] * len(d_ppo)
    for i in range(1, len(d_ppo)):
        a = d_ppo[i]
        b = d_ppo[i-1]
        if np.isnan(a) or np.isnan(b):
            colors[i] = None
        else:
            colors[i] = "GREEN" if a > b else "RED"

    cur = colors[-1]
    if cur is None:
        return None

    run_len = 0
    i = len(colors) - 1
    while i >= 1 and colors[i] == cur:
        run_len += 1
        i -= 1

    return max(0, run_len - 1)


def _market_state_row_input_columns(precomputed_columns: Dict[str, Any]) -> List[str]:
    ordered: List[str] = []
    seen = set()
    for name in MARKET_STATE_ROW_BASE_COLUMNS:
        key = str(name or "").strip()
        if key and key not in seen:
            seen.add(key)
            ordered.append(key)
    for name in dict(precomputed_columns or {}).keys():
        key = str(name or "").strip()
        if key and key not in seen:
            seen.add(key)
            ordered.append(key)
    return ordered




def _safe_float(x: Any) -> Optional[float]:
    try:
        if x is None:
            return None
        v = float(x)
        if np.isfinite(v):
            return v
    except Exception:
        pass
    return None


def _safe_float_or_nan(x: Any) -> float:
    value = _safe_float(x)
    return float(value) if value is not None else float(np.nan)


def _safe_bool(x: Any) -> bool:
    if isinstance(x, (bool, np.bool_)):
        return bool(x)
    s = str(x or "").strip().upper()
    return s in ("1", "TRUE", "YES", "Y", "ON")






def _attach_batched_columns(frame: "pd.DataFrame", columns: Dict[str, "pd.Series"]) -> "pd.DataFrame":
    """Attach many derived columns in one concat to avoid DataFrame fragmentation."""
    if frame is None or not columns:
        return frame

    existing = [name for name in columns.keys() if name in frame.columns]
    base = frame.drop(columns=existing, errors="ignore") if existing else frame
    additions = pd.DataFrame(columns, index=frame.index)
    return pd.concat([base, additions], axis=1, copy=False)

def _bool_i8_series(values: Any, index: "pd.Index") -> "pd.Series":
    ser = pd.Series(values, index=index)
    if ser.empty:
        return ser.astype(np.int8)
    arr = np.fromiter((1 if _safe_bool(v) else 0 for v in ser.to_numpy(dtype=object, copy=False)), dtype=np.int8, count=len(ser))
    return pd.Series(arr, index=index, dtype=np.int8)


def _ema_profile_signature_for_request(required_fields: Optional[Any], ema_settings: Optional[Any], timeframes: List[str]) -> Optional[List[List[Any]]]:
    req_fams = normalize_required_families(required_fields, include_defaults=True)
    if FAMILY_EMA not in req_fams:
        return None
    from .ema_settings import ema_settings_signature

    signature = ema_settings_signature(ema_settings, timeframes=timeframes)
    return [list(item) for item in signature] if signature else None


def _rsi_profile_signature_for_request(required_fields: Optional[Any], rsi_settings: Optional[Any], timeframes: List[str]) -> Optional[List[List[Any]]]:
    req_fams = normalize_required_families(required_fields, include_defaults=True)
    if FAMILY_RSI not in req_fams:
        return None
    from .rsi_settings import rsi_settings_signature

    signature = rsi_settings_signature(rsi_settings, timeframes=timeframes)
    return [list(item) for item in signature] if signature else None


def _augment_streams_with_ema_native(
    streams: Dict[str, "pd.DataFrame"],
    *,
    ema_settings: Optional[Any],
    required_fields: Optional[Any],
) -> Dict[str, "pd.DataFrame"]:
    req_fields = set(normalize_required_fields(required_fields, include_defaults=True))
    if not bool(req_fields & EMA_FIELDS):
        return dict(streams or {})

    from .ema_pair import build_ema_pair_frame
    from .ema_settings import resolve_ema_settings

    out: Dict[str, pd.DataFrame] = {}
    for tf, frame in dict(streams or {}).items():
        tf_key = str(tf)
        if not isinstance(frame, pd.DataFrame) or frame.empty:
            out[tf_key] = frame
            continue
        close_col = "close" if "close" in frame.columns else ("price" if "price" in frame.columns else None)
        if close_col is None:
            out[tf_key] = frame
            continue
        close_ser = pd.to_numeric(frame[close_col], errors="coerce").astype(float)
        pair = resolve_ema_settings(ema_settings, tf_key)
        next_frame = frame.copy()
        for key, series in build_ema_pair_frame(
            close_ser.to_numpy(dtype=float),
            frame.index,
            ema_1_length=int(pair["ema_1_length"]),
            ema_2_length=int(pair["ema_2_length"]),
        ).items():
            if key in req_fields:
                next_frame[key] = series
        out[tf_key] = next_frame
    return out



def _market_hierarchy_overlay(snap: Dict[str, Any], structure_flag: str = "MIXED", bias_hint: str = "NEUTRAL") -> Dict[str, Any]:
    width_tightness = float(_safe_float(snap.get("market_width_tightness")) or 0.0)
    width_expansion = float(_safe_float(snap.get("market_width_expansion")) or 0.0)
    containment_rate = float(_safe_float(snap.get("market_containment_rate")) or 0.0)
    rotationality = float(_safe_float(snap.get("market_rotationality")) or 0.0)
    compression_maturity = float(_safe_float(snap.get("market_compression_maturity")) or 0.0)
    range_contraction = float(_safe_float(snap.get("market_range_contraction")) or 0.0)
    swing_squeeze = float(_safe_float(snap.get("market_swing_squeeze")) or 0.0)
    bb_inside_kc = bool(_safe_bool(snap.get("market_bb_inside_kc")))
    bb_squeeze = float(_safe_float(snap.get("market_bb_squeeze_strength")) or 0.0)
    bb_expand = float(_safe_float(snap.get("market_bb_expand_strength")) or 0.0)
    env_width_atr = float(_safe_float(snap.get("market_envelope_width_atr")) or 0.0)
    bb_width_atr = float(_safe_float(snap.get("market_bb_width_atr")) or 0.0)
    atr_width_atr = float(_safe_float(snap.get("market_width_atr")) or 0.0)
    trend_score = float(_safe_float(snap.get("market_trend_score")) or 0.0)
    transition_score = float(_safe_float(snap.get("market_transition_score")) or 0.0)
    direction_evidence = float(_safe_float(snap.get("market_direction_evidence")) or 0.0)
    breakout_confirmation = float(_safe_float(snap.get("market_breakout_confirmation")) or 0.0)
    breakout_quality = float(_safe_float(snap.get("market_breakout_quality")) or 0.0)
    breakout_readiness = float(_safe_float(snap.get("market_breakout_readiness")) or 0.0)
    pullback_score = float(_safe_float(snap.get("market_pullback_score")) or 0.0)
    exhaustion_score = float(_safe_float(snap.get("market_exhaustion_score")) or 0.0)
    vwap_bias = str(snap.get("market_vwap_bias") or "NEUTRAL").upper().strip() or "NEUTRAL"
    long_drive = float(_safe_float(snap.get("market_breakout_drive_long")) or 0.0)
    short_drive = float(_safe_float(snap.get("market_breakout_drive_short")) or 0.0)
    single_spike_long = bool(_safe_bool(snap.get("market_breakout_single_spike_long")))
    single_spike_short = bool(_safe_bool(snap.get("market_breakout_single_spike_short")))
    sequence_long = bool(_safe_bool(snap.get("market_breakout_sequence_long")))
    sequence_short = bool(_safe_bool(snap.get("market_breakout_sequence_short")))

    true_compression = bool(
        bb_inside_kc
        and bb_squeeze >= 0.12
        and compression_maturity >= 60.0
        and containment_rate >= 0.58
        and rotationality >= 0.40
        and range_contraction >= 0.34
        and swing_squeeze >= 0.28
        and width_tightness >= 0.52
        and width_expansion <= 0.18
        and env_width_atr <= 3.20
        and bb_width_atr <= 2.40
        and breakout_confirmation < 0.58
    )
    volatile_rotation = bool(
        containment_rate >= 0.44
        and rotationality >= 0.42
        and (env_width_atr >= 4.00 or bb_width_atr >= 3.00 or width_expansion >= 0.22 or bb_expand >= 0.20)
        and not true_compression
    )
    high_vol = bool(
        not true_compression
        and (
            width_expansion >= 0.20
            or bb_expand >= 0.18
            or env_width_atr >= 3.20
            or atr_width_atr >= 2.80
        )
    )
    if true_compression:
        vol_regime = "COMPRESSION"
        vol_note = "squeeze / range width contracting / BB inside KC"
    elif volatile_rotation:
        vol_regime = "VOLATILE_ROTATION"
        vol_note = "wide two-sided rotation / large amplitude / not a true squeeze"
    elif high_vol:
        vol_regime = "HIGH_VOL"
        vol_note = "realized volatility is elevated / bars are too wide for compression"
    else:
        vol_regime = "NORMAL"
        vol_note = "normal volatility regime"

    structure_intact = str(structure_flag or "MIXED").upper().strip() == "INTACT"
    if vol_regime == "COMPRESSION":
        market_type = "COMPRESSION"
    elif volatile_rotation:
        market_type = "ROTATION"
    elif abs(direction_evidence) >= 2.5 and trend_score >= max(48.0, transition_score + 3.0) and structure_intact:
        market_type = "TREND"
    elif containment_rate >= 0.54 and abs(direction_evidence) < 2.5:
        market_type = "RANGE_BALANCE"
    else:
        market_type = "TRANSITION"

    if market_type == "TREND" and bias_hint == "LONG":
        trend_regime = "UPTREND"
    elif market_type == "TREND" and bias_hint == "SHORT":
        trend_regime = "DOWNTREND"
    elif market_type == "COMPRESSION":
        trend_regime = "FLAT_COMPRESSION"
    elif market_type == "ROTATION":
        trend_regime = "VOLATILE_BALANCE"
    elif market_type == "RANGE_BALANCE":
        trend_regime = "RANGE_BALANCE"
    else:
        trend_regime = "TRANSITION"

    if market_type == "COMPRESSION":
        trend_quality = "BUILDING"
    elif market_type == "ROTATION":
        trend_quality = "UNSTABLE"
    elif structure_intact and breakout_confirmation >= 0.62 and breakout_quality >= 0.52 and ((bias_hint == "LONG" and sequence_long) or (bias_hint == "SHORT" and sequence_short)):
        trend_quality = "CONFIRMED"
    elif structure_intact and exhaustion_score < 54.0 and pullback_score < 52.0:
        trend_quality = "HEALTHY"
    elif str(structure_flag or "MIXED").upper().strip() == "BROKEN":
        trend_quality = "BROKEN"
    elif pullback_score >= 48.0:
        trend_quality = "PULLBACK"
    elif exhaustion_score >= 54.0:
        trend_quality = "WEAKENING"
    else:
        trend_quality = "MIXED"

    breakout_style = "NONE"
    breakout_style_note = "no breakout sequence"
    if bias_hint == "LONG":
        if sequence_long and not single_spike_long:
            breakout_style = "SEQUENCE_LONG"
            breakout_style_note = "multi-candle upward drive is present"
        elif single_spike_long:
            breakout_style = "SPIKE_LONG"
            breakout_style_note = "single large bullish spike without enough follow-through candles"
    elif bias_hint == "SHORT":
        if sequence_short and not single_spike_short:
            breakout_style = "SEQUENCE_SHORT"
            breakout_style_note = "multi-candle downward drive is present"
        elif single_spike_short:
            breakout_style = "SPIKE_SHORT"
            breakout_style_note = "single large bearish spike without enough follow-through candles"

    header_bits = []
    if vol_regime == "COMPRESSION":
        header_bits.append("SQZ")
    elif vol_regime == "VOLATILE_ROTATION":
        header_bits.append("ROT")
    elif vol_regime == "HIGH_VOL":
        header_bits.append("HI-VOL")
    else:
        header_bits.append("NORM")
    if market_type == "TREND":
        header_bits.append("TREND")
    elif market_type == "RANGE_BALANCE":
        header_bits.append("RANGE")
    elif market_type == "ROTATION":
        header_bits.append("ROT")
    elif market_type == "COMPRESSION":
        header_bits.append("COIL")
    else:
        header_bits.append("TRANS")
    if trend_regime == "UPTREND":
        header_bits.append("UP")
    elif trend_regime == "DOWNTREND":
        header_bits.append("DOWN")
    elif trend_regime == "FLAT_COMPRESSION":
        header_bits.append("FLAT")

    action_state = "WATCH"
    if market_type == "TREND" and trend_quality in ("CONFIRMED", "HEALTHY") and ((bias_hint == "LONG" and vwap_bias == "LONG") or (bias_hint == "SHORT" and vwap_bias == "SHORT")):
        action_state = "TREND_OK"
    elif market_type == "COMPRESSION" and breakout_readiness >= 0.56:
        action_state = "COILING"
    elif market_type == "ROTATION":
        action_state = "CHOP"

    return {
        "market_volatility_regime": vol_regime,
        "market_volatility_note": vol_note,
        "market_market_type": market_type,
        "market_trend_regime": trend_regime,
        "market_trend_quality": trend_quality,
        "market_breakout_style": breakout_style,
        "market_breakout_style_note": breakout_style_note,
        "market_header_context": " | ".join(header_bits),
        "market_action_state": action_state,
        "market_true_compression": true_compression,
        "market_volatile_rotation": volatile_rotation,
        "market_breakout_sequence_long": sequence_long,
        "market_breakout_sequence_short": sequence_short,
        "market_breakout_single_spike_long": single_spike_long,
        "market_breakout_single_spike_short": single_spike_short,
        "market_breakout_drive_long": long_drive,
        "market_breakout_drive_short": short_drive,
    }
def _market_value_volatility_overlay(snap: Dict[str, Any], bias_hint: str = "NEUTRAL") -> Dict[str, Any]:
    bb_inside_kc = bool(_safe_bool(snap.get("market_bb_inside_kc")))
    bb_squeeze = float(_safe_float(snap.get("market_bb_squeeze_strength")) or 0.0)
    bb_expand = float(_safe_float(snap.get("market_bb_expand_strength")) or 0.0)
    bb_walk_long = bool(_safe_bool(snap.get("market_bb_band_walk_long")))
    bb_walk_short = bool(_safe_bool(snap.get("market_bb_band_walk_short")))
    bb_break_long = bool(_safe_bool(snap.get("market_bb_breakout_long")))
    bb_break_short = bool(_safe_bool(snap.get("market_bb_breakout_short")))
    vwap_bias = str(snap.get("market_vwap_bias") or "NEUTRAL").upper().strip() or "NEUTRAL"
    vwap_dist_atr = float(_safe_float(snap.get("market_vwap_distance_atr")) or 0.0)
    vwap_reclaim_long = bool(_safe_bool(snap.get("market_vwap_reclaim_long")))
    vwap_reclaim_short = bool(_safe_bool(snap.get("market_vwap_reclaim_short")))
    vwap_reject_long = bool(_safe_bool(snap.get("market_vwap_reject_long")))
    vwap_reject_short = bool(_safe_bool(snap.get("market_vwap_reject_short")))

    context = "NEUTRAL"
    note = "value/volatility context neutral"
    trend_adjust = 0.0
    compression_adjust = 0.0
    transition_adjust = 0.0
    breakout_bonus = 0.0

    if bb_inside_kc and bb_squeeze >= 0.10:
        context = "SQUEEZE"
        note = "Bollinger is inside Keltner; volatility is compressed"
        compression_adjust += float(min(6.0, 2.0 + 10.0 * bb_squeeze))
        transition_adjust -= 1.0
    elif bb_expand >= 0.22 and (bb_walk_long or bb_walk_short or bb_break_long or bb_break_short):
        context = "RELEASE"
        note = "Bollinger width is expanding out of the squeeze / release phase"
        breakout_bonus += float(min(0.12, 0.03 + 0.18 * bb_expand))
        transition_adjust += 1.5

    if bias_hint == "LONG":
        if vwap_bias == "LONG":
            trend_adjust += 2.5
            breakout_bonus += 0.03
            if vwap_reclaim_long:
                trend_adjust += 1.5
            if bb_walk_long:
                trend_adjust += 2.0
                breakout_bonus += 0.02
            if bb_break_long:
                trend_adjust += 1.5
        elif vwap_bias == "SHORT" or vwap_reject_long:
            trend_adjust -= 4.5
            transition_adjust += 4.0
            note = "price is below VWAP / rejecting VWAP against a long bias"
        if bb_walk_short or bb_break_short:
            trend_adjust -= 2.5
            transition_adjust += 2.5
    elif bias_hint == "SHORT":
        if vwap_bias == "SHORT":
            trend_adjust += 2.5
            breakout_bonus += 0.03
            if vwap_reclaim_short:
                trend_adjust += 1.5
            if bb_walk_short:
                trend_adjust += 2.0
                breakout_bonus += 0.02
            if bb_break_short:
                trend_adjust += 1.5
        elif vwap_bias == "LONG" or vwap_reject_short:
            trend_adjust -= 4.5
            transition_adjust += 4.0
            note = "price is above VWAP / rejecting VWAP against a short bias"
        if bb_walk_long or bb_break_long:
            trend_adjust -= 2.5
            transition_adjust += 2.5

    if abs(vwap_dist_atr) >= 1.15:
        transition_adjust += 0.5

    return {
        "market_value_context": str(context),
        "market_value_note": str(note),
        "market_value_trend_adjust": float(trend_adjust),
        "market_value_compression_adjust": float(compression_adjust),
        "market_value_transition_adjust": float(transition_adjust),
        "market_value_breakout_bonus": float(breakout_bonus),
    }


def _market_breakout_eligible_from_compression(snap: Dict[str, Any], prev_state: Optional[Dict[str, Any]] = None) -> bool:
    compression_maturity = float(_safe_float(snap.get("market_compression_maturity")) or 0.0)
    containment_rate = float(_safe_float(snap.get("market_containment_rate")) or 0.0)
    range_contraction = float(_safe_float(snap.get("market_range_contraction")) or 0.0)
    swing_squeeze = float(_safe_float(snap.get("market_swing_squeeze")) or 0.0)
    bb_inside_kc = bool(_safe_bool(snap.get("market_bb_inside_kc")))
    bb_squeeze = float(_safe_float(snap.get("market_bb_squeeze_strength")) or 0.0)

    if compression_maturity >= 60.0 and containment_rate >= 0.56 and (range_contraction >= 0.32 or swing_squeeze >= 0.32 or (bb_inside_kc and bb_squeeze >= 0.10)):
        return True

    if isinstance(prev_state, dict):
        prev_label = str(prev_state.get("market_state") or "").upper().strip()
        prev_breakout_state = str(prev_state.get("market_breakout_state") or "").upper().strip()
        prev_comp = float(_safe_float(prev_state.get("market_compression_maturity")) or 0.0)
        prev_bb_inside = bool(_safe_bool(prev_state.get("market_bb_inside_kc")))
        prev_bb_squeeze = float(_safe_float(prev_state.get("market_bb_squeeze_strength")) or 0.0)
        if prev_label == "COMPRESSION":
            return True
        if prev_breakout_state in ("BREAKOUT_READY", "BREAKOUT_WATCH_LONG", "BREAKOUT_WATCH_SHORT", "BREAKOUT_CONFIRM_LONG", "BREAKOUT_CONFIRM_SHORT"):
            return True
        if prev_comp >= 58.0 and (prev_bb_inside or prev_bb_squeeze >= 0.10):
            return True
    return False


def _color_sign(x: Any) -> int:
    s = str(x or "").upper().strip()
    if s == "GREEN":
        return 1
    if s == "RED":
        return -1
    return 0


def _state_sign(x: Any, pos: str = "BUY", neg: str = "SELL") -> int:
    s = str(x or "").upper().strip()
    if s == pos:
        return 1
    if s == neg:
        return -1
    return 0


def pretty_market_state_label(label: Any) -> str:
    s = str(label or "").strip()
    if not s:
        return "-"
    return s.replace("_", " ").title()


def _compute_market_state_snapshot_basic(snapshot: Optional[Dict[str, Any]], prev_state: Optional[Dict[str, Any]] = None, thresholds: MarketStateThresholds = DEFAULT_MARKET_STATE_THRESHOLDS) -> Dict[str, Any]:
    try:
        return run_basic_market_state_pipeline(
            snapshot,
            prev_state,
            thresholds,
            price_action_overlay_builder=lambda snap, prev, bias: _market_price_action_overlay(snap, prev_state=prev, bias_hint=bias),
            pretty_label_builder=pretty_market_state_label,
        )
    except Exception as exc:
        return build_market_state_failure_payload(snapshot, prev_state=prev_state, reason=str(exc), error_type=exc.__class__.__name__)


def _series_sign_from_state(ser: "pd.Series", pos: str = "BUY", neg: str = "SELL") -> "pd.Series":
    s = ser.astype(str).str.upper().str.strip()
    out = pd.Series(np.zeros(len(ser), dtype=float), index=ser.index)
    out[s == pos] = 1.0
    out[s == neg] = -1.0
    return out


def _series_sign_from_color(ser: "pd.Series") -> "pd.Series":
    s = ser.astype(str).str.upper().str.strip()
    out = pd.Series(np.zeros(len(ser), dtype=float), index=ser.index)
    out[s == "GREEN"] = 1.0
    out[s == "RED"] = -1.0
    return out


def _series_sign_from_angle_state(ser: "pd.Series") -> "pd.Series":
    s = ser.astype(str).str.upper().str.strip()
    out = pd.Series(np.zeros(len(ser), dtype=float), index=ser.index)
    out[s == "UP"] = 1.0
    out[s == "DOWN"] = -1.0
    return out


def _safe_series(df: "pd.DataFrame", key: str, fallback: Any = np.nan) -> "pd.Series":
    if isinstance(df, pd.DataFrame) and key in df.columns:
        ser = df.get(key)
        if isinstance(ser, pd.Series):
            try:
                return pd.to_numeric(ser, errors="coerce").astype(float)
            except Exception:
                return pd.Series(ser, index=df.index)
    if isinstance(fallback, pd.Series):
        return fallback.reindex(df.index)
    return pd.Series(fallback, index=df.index, dtype=float)


def _rolling_q(ser: "pd.Series", window: int, q: float, min_periods: int) -> "pd.Series":
    try:
        return ser.rolling(window, min_periods=min_periods).quantile(q)
    except Exception:
        return ser.rolling(window, min_periods=min_periods).median()


def _clip01_series(ser: "pd.Series") -> "pd.Series":
    return ser.clip(lower=0.0, upper=1.0)


def _ratio_to_window(value: "pd.Series", low: "pd.Series", high: "pd.Series") -> "pd.Series":
    span = (high - low).where((high - low).abs() > 1e-12)
    out = (value - low) / span
    return out.replace([np.inf, -np.inf], np.nan)




def _pa_state_safe_float(val: Any) -> Optional[float]:
    try:
        if val is None:
            return None
        fv = float(val)
        if not np.isfinite(fv):
            return None
        return float(fv)
    except Exception:
        return None


def _pa_restore_bar_history(prev_state: Optional[Dict[str, Any]], limit: int = 12) -> List[Dict[str, float]]:
    hist: List[Dict[str, float]] = []
    if not isinstance(prev_state, dict):
        return hist
    raw = prev_state.get('_market_pa_bar_history')
    if not isinstance(raw, list):
        return hist
    for item in raw[-max(4, int(limit)):]:
        if not isinstance(item, dict):
            continue
        seq = item.get('seq')
        try:
            seq_i = int(seq)
        except Exception:
            continue
        bar = {
            'seq': seq_i,
            'o': float(_pa_state_safe_float(item.get('o')) or 0.0),
            'h': float(_pa_state_safe_float(item.get('h')) or 0.0),
            'l': float(_pa_state_safe_float(item.get('l')) or 0.0),
            'c': float(_pa_state_safe_float(item.get('c')) or 0.0),
            'v': float(_pa_state_safe_float(item.get('v')) or 0.0),
            'body_frac': float(_pa_state_safe_float(item.get('body_frac')) or 0.0),
            'upper_wick_frac': float(_pa_state_safe_float(item.get('upper_wick_frac')) or 0.0),
            'lower_wick_frac': float(_pa_state_safe_float(item.get('lower_wick_frac')) or 0.0),
            'close_pos': float(_pa_state_safe_float(item.get('close_pos')) or 0.5),
            'range_atr': float(_pa_state_safe_float(item.get('range_atr')) or 0.0),
        }
        hist.append(bar)
    return hist[-max(4, int(limit)):]


def _pa_restore_pivots(prev_state: Optional[Dict[str, Any]], limit: int = 12) -> List[Dict[str, float]]:
    pivots: List[Dict[str, float]] = []
    if not isinstance(prev_state, dict):
        return pivots
    raw = prev_state.get('_market_pa_pivots')
    if not isinstance(raw, list):
        return pivots
    for item in raw[-max(4, int(limit)):]:
        if not isinstance(item, dict):
            continue
        kind = str(item.get('kind') or '').upper().strip()
        if kind not in ('H', 'L'):
            continue
        try:
            seq_i = int(item.get('seq'))
        except Exception:
            continue
        price = _pa_state_safe_float(item.get('price'))
        if price is None:
            continue
        pivots.append({'kind': kind, 'seq': seq_i, 'price': float(price)})
    return pivots[-max(4, int(limit)):]


def _pa_append_confirmed_pivot(pivots: List[Dict[str, float]], kind: str, price: float, seq: int, limit: int = 12) -> List[Dict[str, float]]:
    kind = str(kind or '').upper().strip()
    if kind not in ('H', 'L'):
        return pivots[-max(4, int(limit)):]
    clean = list(pivots or [])
    for item in clean:
        if int(item.get('seq', -1)) == int(seq) and str(item.get('kind') or '').upper().strip() == kind:
            return clean[-max(4, int(limit)):]
    if clean and str(clean[-1].get('kind') or '').upper().strip() == kind:
        last_price = _pa_state_safe_float(clean[-1].get('price'))
        if last_price is not None:
            replace = (kind == 'H' and float(price) >= float(last_price)) or (kind == 'L' and float(price) <= float(last_price))
            if replace:
                clean[-1] = {'kind': kind, 'seq': int(seq), 'price': float(price)}
            return clean[-max(4, int(limit)):]
    clean.append({'kind': kind, 'seq': int(seq), 'price': float(price)})
    return clean[-max(4, int(limit)):]


def _pa_update_confirmed_pivots(bar_history: List[Dict[str, float]], pivots: List[Dict[str, float]], tol: float, limit: int = 12) -> List[Dict[str, float]]:
    clean = list(pivots or [])
    if len(bar_history) < 5:
        return clean[-max(4, int(limit)):]
    window = list(bar_history[-5:])
    center = window[2]
    center_high = float(center.get('h', 0.0))
    center_low = float(center.get('l', 0.0))
    other_highs = [float(x.get('h', 0.0)) for i, x in enumerate(window) if i != 2]
    other_lows = [float(x.get('l', 0.0)) for i, x in enumerate(window) if i != 2]
    seq = int(center.get('seq', 0))
    pad = max(float(tol or 0.0) * 0.10, 1e-12)
    is_pivot_high = bool(other_highs and center_high >= max(other_highs) + pad)
    is_pivot_low = bool(other_lows and center_low <= min(other_lows) - pad)
    if is_pivot_high:
        clean = _pa_append_confirmed_pivot(clean, 'H', center_high, seq, limit=limit)
    if is_pivot_low:
        clean = _pa_append_confirmed_pivot(clean, 'L', center_low, seq, limit=limit)
    return clean[-max(4, int(limit)):]


def _pa_analyse_swing_structure(
    bar_history: List[Dict[str, float]],
    pivots: List[Dict[str, float]],
    close_price: Optional[float],
    high_price: Optional[float],
    low_price: Optional[float],
    tol: float,
    bias_hint: str = 'NEUTRAL',
) -> Dict[str, Any]:
    highs = [p for p in pivots if str(p.get('kind') or '').upper().strip() == 'H']
    lows = [p for p in pivots if str(p.get('kind') or '').upper().strip() == 'L']
    last_high = float(highs[-1]['price']) if highs else None
    prev_high = float(highs[-2]['price']) if len(highs) >= 2 else None
    last_low = float(lows[-1]['price']) if lows else None
    prev_low = float(lows[-2]['price']) if len(lows) >= 2 else None

    hh = bool(last_high is not None and prev_high is not None and float(last_high) > float(prev_high) + tol * 0.35)
    hl = bool(last_low is not None and prev_low is not None and float(last_low) > float(prev_low) + tol * 0.20)
    lh = bool(last_high is not None and prev_high is not None and float(last_high) < float(prev_high) - tol * 0.20)
    ll = bool(last_low is not None and prev_low is not None and float(last_low) < float(prev_low) - tol * 0.35)

    pivot_bias = 'LONG' if (hh and hl) else ('SHORT' if (lh and ll) else 'NEUTRAL')
    long_intact = bool(pivot_bias == 'LONG')
    short_intact = bool(pivot_bias == 'SHORT')
    long_broken = bool((bias_hint == 'LONG' or pivot_bias == 'LONG') and last_low is not None and ((close_price is not None and float(close_price) < float(last_low) - tol * 0.35) or (low_price is not None and float(low_price) < float(last_low) - tol * 0.55)))
    short_broken = bool((bias_hint == 'SHORT' or pivot_bias == 'SHORT') and last_high is not None and ((close_price is not None and float(close_price) > float(last_high) + tol * 0.35) or (high_price is not None and float(high_price) > float(last_high) + tol * 0.55)))

    recent = list(bar_history[-6:])
    progressive_higher_lows = False
    progressive_lower_highs = False
    if len(recent) >= 4:
        lows_recent = [float(x.get('l', 0.0)) for x in recent[-4:]]
        highs_recent = [float(x.get('h', 0.0)) for x in recent[-4:]]
        progressive_higher_lows = bool(all(lows_recent[i] >= lows_recent[i - 1] - tol * 0.12 for i in range(1, len(lows_recent))))
        progressive_lower_highs = bool(all(highs_recent[i] <= highs_recent[i - 1] + tol * 0.12 for i in range(1, len(highs_recent))))

    note = 'no confirmed swing sequence yet'
    context = 'NEUTRAL'
    if long_broken:
        context = 'LONG_BROKEN'
        note = 'close broke the last confirmed swing low'
    elif short_broken:
        context = 'SHORT_BROKEN'
        note = 'close broke the last confirmed swing high'
    elif long_intact:
        context = 'LONG_INTACT'
        note = 'confirmed swing HH/HL structure is intact'
    elif short_intact:
        context = 'SHORT_INTACT'
        note = 'confirmed swing LH/LL structure is intact'
    elif progressive_higher_lows and bias_hint == 'LONG' and last_low is not None:
        context = 'LONG_PROGRESSIVE'
        note = 'lows are stepping higher but swing confirmation is incomplete'
    elif progressive_lower_highs and bias_hint == 'SHORT' and last_high is not None:
        context = 'SHORT_PROGRESSIVE'
        note = 'highs are stepping lower but swing confirmation is incomplete'

    return {
        'available': bool(len(pivots) >= 2),
        'pivot_bias': pivot_bias,
        'context': context,
        'note': note,
        'swing_hh': bool(hh),
        'swing_hl': bool(hl),
        'swing_lh': bool(lh),
        'swing_ll': bool(ll),
        'long_intact': bool(long_intact),
        'short_intact': bool(short_intact),
        'long_broken': bool(long_broken),
        'short_broken': bool(short_broken),
        'last_high': (float(last_high) if last_high is not None else np.nan),
        'last_low': (float(last_low) if last_low is not None else np.nan),
        'prev_high': (float(prev_high) if prev_high is not None else np.nan),
        'prev_low': (float(prev_low) if prev_low is not None else np.nan),
        'pivot_count': int(len(pivots)),
        'progressive_higher_lows': bool(progressive_higher_lows),
        'progressive_lower_highs': bool(progressive_lower_highs),
    }


def _pa_multibar_sequence_context(bar_history: List[Dict[str, float]], atr: float, tol: float, bias_hint: str = 'NEUTRAL') -> Dict[str, Any]:
    recent = list(bar_history[-6:])
    out: Dict[str, Any] = {
        'context': 'NEUTRAL',
        'note': '',
        'long_exhaust': False,
        'short_exhaust': False,
        'long_continue': False,
        'short_continue': False,
    }
    if len(recent) < 4:
        return out

    closes = [float(x.get('c', 0.0)) for x in recent]
    highs = [float(x.get('h', 0.0)) for x in recent]
    lows = [float(x.get('l', 0.0)) for x in recent]
    bodies = [abs(float(x.get('c', 0.0)) - float(x.get('o', 0.0))) for x in recent]
    body_fracs = [float(x.get('body_frac', 0.0)) for x in recent]
    upper_fracs = [float(x.get('upper_wick_frac', 0.0)) for x in recent]
    lower_fracs = [float(x.get('lower_wick_frac', 0.0)) for x in recent]

    up_progress = float(closes[-1] - closes[-4])
    down_progress = float(closes[-4] - closes[-1])
    bar_pad = max(float(tol or 0.0) * 2.2, abs(float(atr or 0.0)) * 0.18, 1e-12)
    long_ladder = bool(sum(1 for i in range(1, len(recent)) if closes[i] >= closes[i - 1] - tol * 0.10) >= len(recent) - 2)
    short_ladder = bool(sum(1 for i in range(1, len(recent)) if closes[i] <= closes[i - 1] + tol * 0.10) >= len(recent) - 2)
    shrinking_last3 = bool(len(bodies) >= 3 and bodies[-3] > bodies[-2] > bodies[-1])
    rising_upper_pressure = bool(len(upper_fracs) >= 3 and upper_fracs[-1] >= upper_fracs[-2] >= upper_fracs[-3] and sum(1 for x in upper_fracs[-3:] if x >= 0.18) >= 2)
    rising_lower_pressure = bool(len(lower_fracs) >= 3 and lower_fracs[-1] >= lower_fracs[-2] >= lower_fracs[-3] and sum(1 for x in lower_fracs[-3:] if x >= 0.18) >= 2)
    body_softening = bool(len(body_fracs) >= 3 and body_fracs[-1] <= body_fracs[-2] <= body_fracs[-3])

    long_continue = bool(long_ladder and up_progress > bar_pad and min(lows[-3:]) >= min(lows[-4:-1]) - tol * 0.20 and sum(1 for x in recent[-3:] if float(x.get('c', 0.0)) >= float(x.get('o', 0.0))) >= 2)
    short_continue = bool(short_ladder and down_progress > bar_pad and max(highs[-3:]) <= max(highs[-4:-1]) + tol * 0.20 and sum(1 for x in recent[-3:] if float(x.get('c', 0.0)) <= float(x.get('o', 0.0))) >= 2)
    long_exhaust = bool(up_progress > tol * 0.60 and long_ladder and shrinking_last3 and body_softening and rising_upper_pressure and (closes[-1] - closes[-3]) <= bar_pad)
    short_exhaust = bool(down_progress > tol * 0.60 and short_ladder and shrinking_last3 and body_softening and rising_lower_pressure and (closes[-3] - closes[-1]) <= bar_pad)

    if long_exhaust:
        out['context'] = 'LONG_SEQUENCE_EXHAUST'
        out['note'] = 'multi-bar bull sequence is losing body size and adding upper-wick pressure'
    elif short_exhaust:
        out['context'] = 'SHORT_SEQUENCE_EXHAUST'
        out['note'] = 'multi-bar bear sequence is losing body size and adding lower-wick pressure'
    elif long_continue:
        out['context'] = 'LONG_SEQUENCE_CONTINUE'
        out['note'] = 'multi-bar candles are still building higher lows and close progress'
    elif short_continue:
        out['context'] = 'SHORT_SEQUENCE_CONTINUE'
        out['note'] = 'multi-bar candles are still building lower highs and downside close progress'

    out['long_exhaust'] = bool(long_exhaust)
    out['short_exhaust'] = bool(short_exhaust)
    out['long_continue'] = bool(long_continue)
    out['short_continue'] = bool(short_continue)
    return out

def _market_price_action_overlay(snap: Dict[str, Any], prev_state: Optional[Dict[str, Any]] = None, bias_hint: str = "NEUTRAL") -> Dict[str, Any]:
    eps = 1e-12

    def _safe_float(val: Any) -> Optional[float]:
        try:
            if val is None:
                return None
            fv = float(val)
            if not np.isfinite(fv):
                return None
            return float(fv)
        except Exception:
            return None

    def _prev_bar_value(field: str) -> Optional[float]:
        if not isinstance(prev_state, dict):
            return None
        same_bar = False
        try:
            last_close = _safe_float(prev_state.get("market_last_bar_close"))
            cur_close = _safe_float(snap.get("close"))
            if last_close is not None and cur_close is not None:
                same_bar = abs(float(last_close) - float(cur_close)) <= max(eps, abs(float(cur_close)) * 1e-12)
        except Exception:
            same_bar = False
        keys: List[str] = []
        if same_bar:
            keys.extend([f"market_prior_bar_{field}", f"market_prev_bar_{field}"])
        keys.extend([f"market_last_bar_{field}", f"market_prev_bar_{field}", f"market_prior_bar_{field}", field])
        for key in keys:
            val = _safe_float(prev_state.get(key))
            if val is not None:
                return float(val)
        return None

    bias_hint = str(bias_hint or "NEUTRAL").upper().strip() or "NEUTRAL"
    cur_price = _safe_float(snap.get("price"))
    c = _safe_float(snap.get("close"))
    if c is None:
        c = cur_price
    o = _safe_float(snap.get("open"))
    if o is None:
        o = c
    h = _safe_float(snap.get("high"))
    if h is None:
        vals = [x for x in (o, c, cur_price) if x is not None]
        h = max(vals) if vals else None
    l = _safe_float(snap.get("low"))
    if l is None:
        vals = [x for x in (o, c, cur_price) if x is not None]
        l = min(vals) if vals else None
    v = _safe_float(snap.get("volume"))
    atr = _safe_float(snap.get("atr"))
    if atr is None or abs(float(atr)) <= eps:
        atr = max(abs(float(c or 0.0)) * 0.001, 1.0) if c is not None else 1.0

    price_range = max(float(h or 0.0) - float(l or 0.0), eps) if h is not None and l is not None else max(abs(float(c or 0.0) - float(o or 0.0)), 1.0)
    body = abs(float(c or 0.0) - float(o or 0.0))
    body_frac = float(np.clip(body / max(price_range, eps), 0.0, 1.0))
    close_pos = float(np.clip(((float(c or 0.0) - float(l or 0.0)) / max(price_range, eps)) if l is not None else 0.5, 0.0, 1.0))
    upper_wick = max(0.0, float(h or 0.0) - max(float(o or 0.0), float(c or 0.0))) if h is not None else 0.0
    lower_wick = max(0.0, min(float(o or 0.0), float(c or 0.0)) - float(l or 0.0)) if l is not None else 0.0
    upper_wick_frac = float(np.clip(upper_wick / max(price_range, eps), 0.0, 1.0))
    lower_wick_frac = float(np.clip(lower_wick / max(price_range, eps), 0.0, 1.0))
    range_atr = float(max(price_range, eps) / max(abs(float(atr or 1.0)), eps))

    prev_seq = -1
    if isinstance(prev_state, dict):
        try:
            prev_seq = int(prev_state.get('_market_pa_seq', -1))
        except Exception:
            prev_seq = -1
    cur_seq = int(prev_seq + 1)
    bar_history = _pa_restore_bar_history(prev_state, limit=12)
    bar_history.append({
        'seq': int(cur_seq),
        'o': float(o or 0.0),
        'h': float(h or 0.0),
        'l': float(l or 0.0),
        'c': float(c or 0.0),
        'v': float(v or 0.0),
        'body_frac': float(body_frac),
        'upper_wick_frac': float(upper_wick_frac),
        'lower_wick_frac': float(lower_wick_frac),
        'close_pos': float(close_pos),
        'range_atr': float(range_atr),
    })
    bar_history = bar_history[-12:]
    pivots = _pa_restore_pivots(prev_state, limit=12)
    pivots = _pa_update_confirmed_pivots(bar_history, pivots, tol=max(eps, 0.05 * abs(float(atr or 1.0))), limit=12)

    prev_open = _prev_bar_value("open")
    prev_high = _prev_bar_value("high")
    prev_low = _prev_bar_value("low")
    prev_close = _prev_bar_value("close")
    prev_volume = _prev_bar_value("volume")
    prev2_open = _safe_float(prev_state.get("market_prior_bar_open")) if isinstance(prev_state, dict) else None
    prev2_high = _safe_float(prev_state.get("market_prior_bar_high")) if isinstance(prev_state, dict) else None
    prev2_low = _safe_float(prev_state.get("market_prior_bar_low")) if isinstance(prev_state, dict) else None
    prev2_close = _safe_float(prev_state.get("market_prior_bar_close")) if isinstance(prev_state, dict) else None
    prev2_volume = _safe_float(prev_state.get("market_last_bar_volume")) if isinstance(prev_state, dict) else None
    tol = max(eps, 0.05 * abs(float(atr or 1.0)))
    has_prev = prev_high is not None and prev_low is not None
    has_prev2 = prev2_high is not None and prev2_low is not None
    prev_body = abs(float(prev_close or 0.0) - float(prev_open or 0.0)) if prev_close is not None and prev_open is not None else body
    prev_range = max(float(prev_high or 0.0) - float(prev_low or 0.0), eps) if has_prev else max(prev_body, eps)
    prev_body_frac = float(np.clip(prev_body / max(prev_range, eps), 0.0, 1.0)) if has_prev else float(body_frac)
    prev_close_pos = float(np.clip(((float(prev_close or 0.0) - float(prev_low or 0.0)) / max(prev_range, eps)) if has_prev and prev_low is not None else 0.5, 0.0, 1.0))
    prev_upper_wick = max(0.0, float(prev_high or 0.0) - max(float(prev_open or 0.0), float(prev_close or 0.0))) if has_prev else 0.0
    prev_lower_wick = max(0.0, min(float(prev_open or 0.0), float(prev_close or 0.0)) - float(prev_low or 0.0)) if has_prev else 0.0
    prev_upper_wick_frac = float(np.clip(prev_upper_wick / max(prev_range, eps), 0.0, 1.0)) if has_prev else 0.0
    prev_lower_wick_frac = float(np.clip(prev_lower_wick / max(prev_range, eps), 0.0, 1.0)) if has_prev else 0.0

    higher_high = bool(has_prev and h is not None and float(h) > float(prev_high) + tol)
    higher_low = bool(has_prev and l is not None and float(l) > float(prev_low) + tol)
    lower_high = bool(has_prev and h is not None and float(h) < float(prev_high) - tol)
    lower_low = bool(has_prev and l is not None and float(l) < float(prev_low) - tol)
    inside_bar = bool(has_prev and h is not None and l is not None and float(h) <= float(prev_high) + tol and float(l) >= float(prev_low) - tol)
    outside_bar = bool(has_prev and h is not None and l is not None and float(h) >= float(prev_high) - tol and float(l) <= float(prev_low) + tol)
    swing_info = _pa_analyse_swing_structure(bar_history, pivots, c, h, l, tol, bias_hint=bias_hint)
    sequence_info = _pa_multibar_sequence_context(bar_history, float(atr or 1.0), tol, bias_hint=bias_hint)

    bullish_body = bool(c is not None and o is not None and float(c) > float(o))
    bearish_body = bool(c is not None and o is not None and float(c) < float(o))
    prev_bullish = bool(prev_close is not None and prev_open is not None and float(prev_close) > float(prev_open))
    prev_bearish = bool(prev_close is not None and prev_open is not None and float(prev_close) < float(prev_open))
    strong_bull_body = bool(bullish_body and body_frac >= 0.52 and close_pos >= 0.66)
    strong_bear_body = bool(bearish_body and body_frac >= 0.52 and close_pos <= 0.34)
    upper_rejection = bool(upper_wick_frac >= max(0.24, body_frac * 0.95) and close_pos <= 0.68)
    lower_rejection = bool(lower_wick_frac >= max(0.24, body_frac * 0.95) and close_pos >= 0.32)
    stall = bool(body_frac <= 0.26 and range_atr <= 1.15)

    breakout_confirmation = float(_safe_float(snap.get("market_breakout_confirmation")) or 0.0)
    volume_ratio = _safe_float(snap.get("market_volume_ratio"))
    if volume_ratio is None:
        if v is not None and prev_volume is not None and abs(float(prev_volume)) > eps:
            volume_ratio = float(v) / abs(float(prev_volume))
        else:
            volume_ratio = 1.0
    volume_support = float(_safe_float(snap.get("market_volume_support")) or 0.0)
    delta_agreement = float(_safe_float(snap.get("market_delta_agreement")) or 0.0)
    no_progress = float(_safe_float(snap.get("market_no_progress")) or 0.0)
    if no_progress <= 0.0:
        no_progress = float(np.clip(np.maximum(0.0, 1.0 - (body_frac / 0.40)) * (1.0 if float(volume_ratio or 1.0) >= 1.05 else 0.65), 0.0, 1.0))
    trend_decel = float(_safe_float(snap.get("market_trend_decel_strength")) or 0.0)
    weak_follow = bool((breakout_confirmation >= 0.48 and body_frac <= 0.30) or no_progress >= 0.62 or (float(volume_ratio or 1.0) >= 1.10 and body_frac <= 0.24))
    prev_volume_ratio = None
    if prev2_volume is not None and prev_volume is not None and abs(float(prev2_volume)) > eps:
        prev_volume_ratio = float(prev_volume) / abs(float(prev2_volume))

    bull_engulf = bool(
        prev_bearish and bullish_body and prev_open is not None and prev_close is not None
        and float(o or 0.0) <= float(prev_close) + tol
        and float(c or 0.0) >= float(prev_open) - tol
        and body >= max(prev_body * 0.85, tol * 1.25)
    )
    bear_engulf = bool(
        prev_bullish and bearish_body and prev_open is not None and prev_close is not None
        and float(o or 0.0) >= float(prev_close) - tol
        and float(c or 0.0) <= float(prev_open) + tol
        and body >= max(prev_body * 0.85, tol * 1.25)
    )
    outside_continue_long = bool(outside_bar and bullish_body and body_frac >= 0.45 and close_pos >= 0.62 and prev_high is not None and c is not None and float(c) >= float(prev_high) - tol * 0.10)
    outside_continue_short = bool(outside_bar and bearish_body and body_frac >= 0.45 and close_pos <= 0.38 and prev_low is not None and c is not None and float(c) <= float(prev_low) + tol * 0.10)
    efficient_drive_long = bool((strong_bull_body or bull_engulf or outside_continue_long) and float(volume_ratio or 1.0) >= 0.95 and range_atr >= 0.90 and not upper_rejection)
    efficient_drive_short = bool((strong_bear_body or bear_engulf or outside_continue_short) and float(volume_ratio or 1.0) >= 0.95 and range_atr >= 0.90 and not lower_rejection)

    prev_impulse_long = bool(has_prev and prev_bullish and prev_body_frac >= 0.52 and prev_close_pos >= 0.64 and (prev_range / max(abs(float(atr or 1.0)), eps)) >= 0.85)
    prev_impulse_short = bool(has_prev and prev_bearish and prev_body_frac >= 0.52 and prev_close_pos <= 0.36 and (prev_range / max(abs(float(atr or 1.0)), eps)) >= 0.85)
    sequence_long = bool(
        has_prev and has_prev2 and h is not None and l is not None and prev_high is not None and prev_low is not None
        and float(h) > float(prev_high) + tol
        and float(prev_high) > float(prev2_high) + tol
        and float(l) >= float(prev_low) - tol * 0.20
        and float(prev_low) > float(prev2_low) - tol * 0.20
        and c is not None and prev_close is not None and float(c) >= float(prev_close) - tol * 0.10
    )
    sequence_short = bool(
        has_prev and has_prev2 and h is not None and l is not None and prev_high is not None and prev_low is not None
        and float(l) < float(prev_low) - tol
        and float(prev_low) < float(prev2_low) - tol
        and float(h) <= float(prev_high) + tol * 0.20
        and float(prev_high) < float(prev2_high) + tol * 0.20
        and c is not None and prev_close is not None and float(c) <= float(prev_close) + tol * 0.10
    )
    followthrough_long_pass = bool(
        prev_impulse_long and c is not None and prev_close is not None and prev_low is not None
        and float(c) >= float(prev_close) + tol * 0.15
        and float(l or 0.0) >= float(prev_low) - tol * 0.35
        and (higher_high or close_pos >= 0.60 or breakout_confirmation >= 0.58)
        and not upper_rejection and not bearish_body
    )
    followthrough_short_pass = bool(
        prev_impulse_short and c is not None and prev_close is not None and prev_high is not None
        and float(c) <= float(prev_close) - tol * 0.15
        and float(h or 0.0) <= float(prev_high) + tol * 0.35
        and (lower_low or close_pos <= 0.40 or breakout_confirmation >= 0.58)
        and not lower_rejection and not bullish_body
    )
    stall_after_impulse_long = bool(prev_impulse_long and (inside_bar or stall or (body_frac <= 0.24 and no_progress >= 0.56)) and not followthrough_long_pass)
    stall_after_impulse_short = bool(prev_impulse_short and (inside_bar or stall or (body_frac <= 0.24 and no_progress >= 0.56)) and not followthrough_short_pass)
    followthrough_long_fail = bool(prev_impulse_long and not followthrough_long_pass and (upper_rejection or bearish_body or inside_bar or weak_follow or stall_after_impulse_long or (c is not None and prev_close is not None and float(c) <= float(prev_close) - tol * 0.15)))
    followthrough_short_fail = bool(prev_impulse_short and not followthrough_short_pass and (lower_rejection or bullish_body or inside_bar or weak_follow or stall_after_impulse_short or (c is not None and prev_close is not None and float(c) >= float(prev_close) + tol * 0.15)))

    prev_breakout_state = str(prev_state.get("market_breakout_state") or "NONE").upper().strip() if isinstance(prev_state, dict) else "NONE"
    prev_breakout_long = bool(prev_breakout_state in ("BREAKOUT_WATCH_LONG", "BREAKOUT_CONFIRM_LONG") or (isinstance(prev_state, dict) and str(prev_state.get("market_bias") or "").upper() == "LONG" and float(_safe_float(prev_state.get("market_breakout_confirmation")) or 0.0) >= 0.56))
    prev_breakout_short = bool(prev_breakout_state in ("BREAKOUT_WATCH_SHORT", "BREAKOUT_CONFIRM_SHORT") or (isinstance(prev_state, dict) and str(prev_state.get("market_bias") or "").upper() == "SHORT" and float(_safe_float(prev_state.get("market_breakout_confirmation")) or 0.0) >= 0.56))
    retest_band = max(tol * 2.5, abs(float(atr or 1.0)) * 0.22)

    retest_hold_long = bool(
        prev_high is not None and l is not None and c is not None
        and (prev_breakout_long or higher_high or (bias_hint == "LONG" and breakout_confirmation >= 0.54))
        and float(l) <= float(prev_high) + retest_band
        and float(l) >= float(prev_high) - retest_band
        and float(c) >= float(prev_high) - tol * 0.10
        and (bullish_body or close_pos >= 0.58 or lower_rejection)
    )
    retest_hold_short = bool(
        prev_low is not None and h is not None and c is not None
        and (prev_breakout_short or lower_low or (bias_hint == "SHORT" and breakout_confirmation >= 0.54))
        and float(h) >= float(prev_low) - retest_band
        and float(h) <= float(prev_low) + retest_band
        and float(c) <= float(prev_low) + tol * 0.10
        and (bearish_body or close_pos <= 0.42 or upper_rejection)
    )
    retest_fail_long = bool(
        prev_high is not None and l is not None and c is not None
        and (prev_breakout_long or higher_high or (bias_hint == "LONG" and breakout_confirmation >= 0.54))
        and float(l) <= float(prev_high) + retest_band
        and float(c) < float(prev_high) - tol * 0.30
        and (upper_rejection or close_pos <= 0.52 or bearish_body)
    )
    retest_fail_short = bool(
        prev_low is not None and h is not None and c is not None
        and (prev_breakout_short or lower_low or (bias_hint == "SHORT" and breakout_confirmation >= 0.54))
        and float(h) >= float(prev_low) - retest_band
        and float(c) > float(prev_low) + tol * 0.30
        and (lower_rejection or close_pos >= 0.48 or bullish_body)
    )

    effort_up_poor_result = bool(
        (float(volume_ratio or 1.0) >= 1.18 or volume_support >= 0.16)
        and (range_atr >= 1.10 or breakout_confirmation >= 0.56)
        and (body_frac <= 0.30 or close_pos <= 0.56 or upper_rejection or no_progress >= 0.60)
    )
    effort_down_poor_result = bool(
        (float(volume_ratio or 1.0) >= 1.18 or volume_support >= 0.16)
        and (range_atr >= 1.10 or breakout_confirmation >= 0.56)
        and (body_frac <= 0.30 or close_pos >= 0.44 or lower_rejection or no_progress >= 0.60)
    )
    climax_absorb_long = bool(
        float(volume_ratio or 1.0) >= 1.35 and range_atr >= 1.45 and breakout_confirmation >= 0.56
        and (upper_rejection or weak_follow or no_progress >= 0.55)
    )
    climax_absorb_short = bool(
        float(volume_ratio or 1.0) >= 1.35 and range_atr >= 1.45 and breakout_confirmation >= 0.56
        and (lower_rejection or weak_follow or no_progress >= 0.55)
    )

    acceptance_drive_long = bool(
        higher_high and (higher_low or (prev_low is not None and l is not None and float(l) >= float(prev_low) - tol * 0.10))
        and bullish_body and body_frac >= 0.46 and close_pos >= 0.68
        and float(volume_ratio or 1.0) >= 0.95 and not upper_rejection and no_progress <= 0.42
    )
    acceptance_drive_short = bool(
        lower_low and (lower_high or (prev_high is not None and h is not None and float(h) <= float(prev_high) + tol * 0.10))
        and bearish_body and body_frac >= 0.46 and close_pos <= 0.32
        and float(volume_ratio or 1.0) >= 0.95 and not lower_rejection and no_progress <= 0.42
    )
    sweep_fail_long = bool(
        higher_high and prev_high is not None and c is not None
        and float(c) < float(prev_high) - tol * 0.05
        and (upper_rejection or bearish_body or close_pos <= 0.50 or no_progress >= 0.52)
        and (float(volume_ratio or 1.0) >= 0.95 or volume_support >= 0.10)
    )
    sweep_fail_short = bool(
        lower_low and prev_low is not None and c is not None
        and float(c) > float(prev_low) + tol * 0.05
        and (lower_rejection or bullish_body or close_pos >= 0.50 or no_progress >= 0.52)
        and (float(volume_ratio or 1.0) >= 0.95 or volume_support >= 0.10)
    )
    reversal_risk_long = bool(
        prev_impulse_long and bearish_body and prev_close is not None and c is not None
        and float(c) <= float(prev_close) - tol * 0.28
        and (upper_rejection or close_pos <= 0.42 or body_frac >= 0.42)
        and float(volume_ratio or 1.0) >= 0.95
    )
    reversal_risk_short = bool(
        prev_impulse_short and bullish_body and prev_close is not None and c is not None
        and float(c) >= float(prev_close) + tol * 0.28
        and (lower_rejection or close_pos >= 0.58 or body_frac >= 0.42)
        and float(volume_ratio or 1.0) >= 0.95
    )

    constructive_pause_long = bool(
        prev_impulse_long and (inside_bar or stall or body_frac <= 0.28)
        and prev_low is not None and l is not None and float(l) >= float(prev_low) - tol * 0.18
        and prev_close is not None and c is not None and float(c) >= float(prev_close) - tol * 0.12
        and close_pos >= 0.46 and not upper_rejection and no_progress <= 0.60
        and float(volume_ratio or 1.0) >= 0.70
    )
    constructive_pause_short = bool(
        prev_impulse_short and (inside_bar or stall or body_frac <= 0.28)
        and prev_high is not None and h is not None and float(h) <= float(prev_high) + tol * 0.18
        and prev_close is not None and c is not None and float(c) <= float(prev_close) + tol * 0.12
        and close_pos <= 0.54 and not lower_rejection and no_progress <= 0.60
        and float(volume_ratio or 1.0) >= 0.70
    )
    rejection_cluster_long = bool(
        (bias_hint == "LONG" or prev_impulse_long or higher_high)
        and upper_rejection and prev_upper_wick_frac >= max(0.20, prev_body_frac * 0.55)
        and (weak_follow or no_progress >= 0.50 or (c is not None and prev_close is not None and float(c) <= float(prev_close) + tol * 0.05))
    )
    rejection_cluster_short = bool(
        (bias_hint == "SHORT" or prev_impulse_short or lower_low)
        and lower_rejection and prev_lower_wick_frac >= max(0.20, prev_body_frac * 0.55)
        and (weak_follow or no_progress >= 0.50 or (c is not None and prev_close is not None and float(c) >= float(prev_close) - tol * 0.05))
    )
    ladder_fail_long = bool(
        (higher_high or acceptance_drive_long or (bias_hint == "LONG" and breakout_confirmation >= 0.58))
        and prev_close is not None and c is not None and float(c) <= float(prev_close) + tol * 0.08
        and (upper_rejection or body_frac <= 0.34 or no_progress >= 0.54)
        and float(volume_ratio or 1.0) >= 0.90
    )
    ladder_fail_short = bool(
        (lower_low or acceptance_drive_short or (bias_hint == "SHORT" and breakout_confirmation >= 0.58))
        and prev_close is not None and c is not None and float(c) >= float(prev_close) - tol * 0.08
        and (lower_rejection or body_frac <= 0.34 or no_progress >= 0.54)
        and float(volume_ratio or 1.0) >= 0.90
    )

    active_retest_fail = retest_fail_long if bias_hint == "LONG" else (retest_fail_short if bias_hint == "SHORT" else (retest_fail_long or retest_fail_short))
    active_retest_hold = retest_hold_long if bias_hint == "LONG" else (retest_hold_short if bias_hint == "SHORT" else (retest_hold_long or retest_hold_short))

    pattern = "NEUTRAL"
    if sweep_fail_long:
        pattern = "LONG_SWEEP_FAIL"
    elif sweep_fail_short:
        pattern = "SHORT_SWEEP_FAIL"
    elif reversal_risk_long:
        pattern = "LONG_REVERSAL_RISK"
    elif reversal_risk_short:
        pattern = "SHORT_REVERSAL_RISK"
    elif acceptance_drive_long:
        pattern = "LONG_ACCEPTANCE"
    elif acceptance_drive_short:
        pattern = "SHORT_ACCEPTANCE"
    elif ladder_fail_long:
        pattern = "LONG_LADDER_FAIL"
    elif ladder_fail_short:
        pattern = "SHORT_LADDER_FAIL"
    elif rejection_cluster_long:
        pattern = "LONG_REJECTION_CLUSTER"
    elif rejection_cluster_short:
        pattern = "SHORT_REJECTION_CLUSTER"
    elif constructive_pause_long:
        pattern = "LONG_CONSTRUCTIVE_PAUSE"
    elif constructive_pause_short:
        pattern = "SHORT_CONSTRUCTIVE_PAUSE"
    elif active_retest_fail:
        pattern = "RETEST_FAIL"
    elif active_retest_hold:
        pattern = "RETEST_HOLD"
    elif bull_engulf:
        pattern = "BULL_ENGULF"
    elif bear_engulf:
        pattern = "BEAR_ENGULF"
    elif outside_continue_long:
        pattern = "BULL_OUTSIDE_CONTINUE"
    elif outside_continue_short:
        pattern = "BEAR_OUTSIDE_CONTINUE"
    elif climax_absorb_long or climax_absorb_short:
        pattern = "CLIMAX_ABSORPTION"
    elif effort_up_poor_result or effort_down_poor_result:
        pattern = "EFFORT_NO_RESULT"
    elif inside_bar:
        pattern = "INSIDE_BAR"
    elif outside_bar:
        pattern = "OUTSIDE_BAR"
    elif strong_bull_body:
        pattern = "STRONG_BULL_BODY"
    elif strong_bear_body:
        pattern = "STRONG_BEAR_BODY"
    elif bias_hint == "LONG" and upper_rejection:
        pattern = "UPPER_REJECTION"
    elif bias_hint == "SHORT" and lower_rejection:
        pattern = "LOWER_REJECTION"
    elif stall:
        pattern = "STALL"

    structure_flag = "MIXED"
    structure_pass = False
    structure_note = "no prior bar reference"
    structure_context = "NEUTRAL"
    structure_basis = "MICRO"
    if bool(swing_info.get('available')):
        structure_basis = "SWING"
        pivot_bias = str(swing_info.get('pivot_bias') or 'NEUTRAL').upper().strip()
        structure_context = str(swing_info.get('context') or 'NEUTRAL').upper().strip() or 'NEUTRAL'
        if bias_hint == "LONG":
            if bool(swing_info.get('long_broken')) or pivot_bias == "SHORT":
                structure_flag = "BROKEN"
                structure_note = str(swing_info.get('note') or "bull swing structure failed")
            elif bool(swing_info.get('long_intact')) or (pivot_bias == "LONG"):
                structure_flag = "INTACT"
                structure_pass = True
                structure_note = str(swing_info.get('note') or "confirmed swing HH/HL structure is intact")
            elif bool(swing_info.get('progressive_higher_lows')):
                structure_flag = "MIXED"
                structure_note = "higher lows are forming, but a full swing HH/HL confirmation is not complete"
            else:
                structure_flag = "MIXED"
                structure_note = "no confirmed long swing structure yet"
        elif bias_hint == "SHORT":
            if bool(swing_info.get('short_broken')) or pivot_bias == "LONG":
                structure_flag = "BROKEN"
                structure_note = str(swing_info.get('note') or "bear swing structure failed")
            elif bool(swing_info.get('short_intact')) or (pivot_bias == "SHORT"):
                structure_flag = "INTACT"
                structure_pass = True
                structure_note = str(swing_info.get('note') or "confirmed swing LH/LL structure is intact")
            elif bool(swing_info.get('progressive_lower_highs')):
                structure_flag = "MIXED"
                structure_note = "lower highs are forming, but a full swing LH/LL confirmation is not complete"
            else:
                structure_flag = "MIXED"
                structure_note = "no confirmed short swing structure yet"
        else:
            if bool(swing_info.get('long_broken')):
                structure_flag = "BROKEN"
                structure_note = "recent price broke below the last confirmed swing low"
            elif bool(swing_info.get('short_broken')):
                structure_flag = "BROKEN"
                structure_note = "recent price broke above the last confirmed swing high"
            elif pivot_bias == "LONG":
                structure_flag = "INTACT"
                structure_pass = True
                structure_note = str(swing_info.get('note') or "confirmed swing HH/HL structure is active")
            elif pivot_bias == "SHORT":
                structure_flag = "INTACT"
                structure_pass = True
                structure_note = str(swing_info.get('note') or "confirmed swing LH/LL structure is active")
            else:
                structure_flag = "MIXED"
                structure_note = str(swing_info.get('note') or "swing structure is not yet directional")
    elif has_prev:
        if higher_high and higher_low:
            structure_context = "LONG"
            structure_flag = "INTACT"
            structure_pass = True
            structure_note = "HH/HL sequence still holding"
        elif lower_high and lower_low:
            structure_context = "SHORT"
            structure_flag = "INTACT"
            structure_pass = True
            structure_note = "LH/LL sequence still holding"
        elif inside_bar:
            structure_flag = "MIXED"
            structure_note = "inside-bar pause; no fresh structure break"
        elif outside_bar:
            structure_flag = "MIXED"
            structure_note = "outside bar; structure is mixed"
        elif bias_hint == "LONG":
            if lower_low and (c is None or prev_low is None or float(c or 0.0) < float(prev_low) + tol * 0.25):
                structure_flag = "BROKEN"
                structure_note = "lower-low broke prior support"
            elif higher_low or higher_high or (prev_close is not None and c is not None and float(c) >= float(prev_close)):
                structure_flag = "INTACT"
                structure_pass = True
                structure_note = "bull structure still holding above prior low"
            else:
                structure_flag = "MIXED"
                structure_note = "bull structure not clean enough"
        elif bias_hint == "SHORT":
            if higher_high and (c is None or prev_high is None or float(c or 0.0) > float(prev_high) - tol * 0.25):
                structure_flag = "BROKEN"
                structure_note = "higher-high broke prior resistance"
            elif lower_high or lower_low or (prev_close is not None and c is not None and float(c) <= float(prev_close)):
                structure_flag = "INTACT"
                structure_pass = True
                structure_note = "bear structure still holding below prior high"
            else:
                structure_flag = "MIXED"
                structure_note = "bear structure not clean enough"
        else:
            structure_flag = "MIXED"
            structure_note = "no clean directional structure yet"

    phase2_context = "NEUTRAL"
    phase2_note = ""
    phase3_context = "NEUTRAL"
    phase3_note = ""
    phase4_context = "NEUTRAL"
    phase4_note = ""
    phase5_context = "NEUTRAL"
    phase5_note = ""
    phase6_context = "NEUTRAL"
    phase6_note = ""
    trap_flag = "NONE"
    trap_note = ""
    if sweep_fail_long:
        trap_flag = "LONG_TRAP"
        trap_note = "took prior high but closed back below it"
    elif sweep_fail_short:
        trap_flag = "SHORT_TRAP"
        trap_note = "took prior low but closed back above it"
    if bias_hint == "LONG":
        if retest_fail_long:
            phase2_context = "LONG_RETEST_FAIL"
            phase2_note = "breakout retest lost; watch failure below prior high"
        elif climax_absorb_long:
            phase2_context = "LONG_CLIMAX"
            phase2_note = "high effort up-bar is stalling; possible absorption/exhaustion"
        elif effort_up_poor_result:
            phase2_context = "LONG_EFFORT_NO_RESULT"
            phase2_note = "buyers pushed with effort but result is weak"
        elif retest_hold_long:
            phase2_context = "LONG_RETEST_HOLD"
            phase2_note = "breakout retest held; continuation structure improved"
        elif bull_engulf or outside_continue_long or efficient_drive_long:
            phase2_context = "LONG_CONTINUATION"
            phase2_note = "bull candle semantics support continuation"
    elif bias_hint == "SHORT":
        if retest_fail_short:
            phase2_context = "SHORT_RETEST_FAIL"
            phase2_note = "breakdown retest failed; watch reclaim above prior low"
        elif climax_absorb_short:
            phase2_context = "SHORT_CLIMAX"
            phase2_note = "high effort down-bar is stalling; possible absorption/exhaustion"
        elif effort_down_poor_result:
            phase2_context = "SHORT_EFFORT_NO_RESULT"
            phase2_note = "sellers pushed with effort but result is weak"
        elif retest_hold_short:
            phase2_context = "SHORT_RETEST_HOLD"
            phase2_note = "breakdown retest held; continuation structure improved"
        elif bear_engulf or outside_continue_short or efficient_drive_short:
            phase2_context = "SHORT_CONTINUATION"
            phase2_note = "bear candle semantics support continuation"
    else:
        if bull_engulf:
            phase2_context = "LONG_CONTINUATION"
            phase2_note = "bull engulfing impulse improved local continuation odds"
        elif bear_engulf:
            phase2_context = "SHORT_CONTINUATION"
            phase2_note = "bear engulfing impulse improved local continuation odds"
        elif climax_absorb_long or climax_absorb_short:
            phase2_context = "CLIMAX"
            phase2_note = "high effort but poor result; treat continuation carefully"

    if bias_hint == "LONG":
        if followthrough_long_fail:
            phase3_context = "LONG_FOLLOWTHROUGH_FAIL"
            phase3_note = "prior bull impulse did not follow through cleanly"
        elif stall_after_impulse_long:
            phase3_context = "LONG_STALL_AFTER_IMPULSE"
            phase3_note = "bull impulse stalled into a small-body pause"
        elif followthrough_long_pass:
            phase3_context = "LONG_FOLLOWTHROUGH_PASS"
            phase3_note = "bull impulse is still getting acceptable follow-through"
        elif sequence_long:
            phase3_context = "LONG_SEQUENCE"
            phase3_note = "multi-bar HH/HL sequence is still progressing"
    elif bias_hint == "SHORT":
        if followthrough_short_fail:
            phase3_context = "SHORT_FOLLOWTHROUGH_FAIL"
            phase3_note = "prior bear impulse did not follow through cleanly"
        elif stall_after_impulse_short:
            phase3_context = "SHORT_STALL_AFTER_IMPULSE"
            phase3_note = "bear impulse stalled into a small-body pause"
        elif followthrough_short_pass:
            phase3_context = "SHORT_FOLLOWTHROUGH_PASS"
            phase3_note = "bear impulse is still getting acceptable follow-through"
        elif sequence_short:
            phase3_context = "SHORT_SEQUENCE"
            phase3_note = "multi-bar LH/LL sequence is still progressing"
    else:
        if sequence_long:
            phase3_context = "LONG_SEQUENCE"
            phase3_note = "multi-bar HH/HL sequence is still progressing"
        elif sequence_short:
            phase3_context = "SHORT_SEQUENCE"
            phase3_note = "multi-bar LH/LL sequence is still progressing"
        elif followthrough_long_fail or followthrough_short_fail:
            phase3_context = "FOLLOWTHROUGH_FAIL"
            phase3_note = "recent impulse is not following through cleanly"

    if bias_hint == "LONG":
        if sweep_fail_long:
            phase4_context = "LONG_SWEEP_FAIL"
            phase4_note = "price took the prior high but failed to accept above it"
        elif reversal_risk_long:
            phase4_context = "LONG_REVERSAL_RISK"
            phase4_note = "a bearish reversal-style response appeared after the bull impulse"
        elif acceptance_drive_long:
            phase4_context = "LONG_ACCEPTANCE"
            phase4_note = "price is accepting above the prior high with a clean body and close"
    elif bias_hint == "SHORT":
        if sweep_fail_short:
            phase4_context = "SHORT_SWEEP_FAIL"
            phase4_note = "price took the prior low but failed to accept below it"
        elif reversal_risk_short:
            phase4_context = "SHORT_REVERSAL_RISK"
            phase4_note = "a bullish reversal-style response appeared after the bear impulse"
        elif acceptance_drive_short:
            phase4_context = "SHORT_ACCEPTANCE"
            phase4_note = "price is accepting below the prior low with a clean body and close"
    else:
        if sweep_fail_long:
            phase4_context = "LONG_SWEEP_FAIL"
            phase4_note = "price took the prior high but failed to accept above it"
        elif sweep_fail_short:
            phase4_context = "SHORT_SWEEP_FAIL"
            phase4_note = "price took the prior low but failed to accept below it"
        elif acceptance_drive_long:
            phase4_context = "LONG_ACCEPTANCE"
            phase4_note = "bullish acceptance is building above the prior high"
        elif acceptance_drive_short:
            phase4_context = "SHORT_ACCEPTANCE"
            phase4_note = "bearish acceptance is building below the prior low"

    if bias_hint == "LONG":
        if ladder_fail_long:
            phase5_context = "LONG_LADDER_FAIL"
            phase5_note = "price pushed higher but could not add clean close progress"
        elif rejection_cluster_long:
            phase5_context = "LONG_REJECTION_CLUSTER"
            phase5_note = "upper-wick rejection is clustering against the bull push"
        elif constructive_pause_long:
            phase5_context = "LONG_CONSTRUCTIVE_PAUSE"
            phase5_note = "post-impulse pause is holding constructively above prior support"
    elif bias_hint == "SHORT":
        if ladder_fail_short:
            phase5_context = "SHORT_LADDER_FAIL"
            phase5_note = "price pushed lower but could not add clean close progress"
        elif rejection_cluster_short:
            phase5_context = "SHORT_REJECTION_CLUSTER"
            phase5_note = "lower-wick rejection is clustering against the bear push"
        elif constructive_pause_short:
            phase5_context = "SHORT_CONSTRUCTIVE_PAUSE"
            phase5_note = "post-impulse pause is holding constructively below prior resistance"
    else:
        if ladder_fail_long:
            phase5_context = "LONG_LADDER_FAIL"
            phase5_note = "price pushed higher but could not add clean close progress"
        elif ladder_fail_short:
            phase5_context = "SHORT_LADDER_FAIL"
            phase5_note = "price pushed lower but could not add clean close progress"
        elif rejection_cluster_long:
            phase5_context = "LONG_REJECTION_CLUSTER"
            phase5_note = "upper-wick rejection is clustering against the bull push"
        elif rejection_cluster_short:
            phase5_context = "SHORT_REJECTION_CLUSTER"
            phase5_note = "lower-wick rejection is clustering against the bear push"
        elif constructive_pause_long:
            phase5_context = "LONG_CONSTRUCTIVE_PAUSE"
            phase5_note = "post-impulse pause is holding constructively above prior support"
        elif constructive_pause_short:
            phase5_context = "SHORT_CONSTRUCTIVE_PAUSE"
            phase5_note = "post-impulse pause is holding constructively below prior resistance"

    if bias_hint == "LONG":
        if bool(swing_info.get('long_broken')):
            phase6_context = "LONG_SWING_BREAK"
            phase6_note = str(swing_info.get('note') or 'price broke the last confirmed swing low')
        elif bool(sequence_info.get('long_exhaust')):
            phase6_context = "LONG_SEQUENCE_EXHAUST"
            phase6_note = str(sequence_info.get('note') or 'multi-bar bull sequence is tiring')
        elif bool(swing_info.get('long_intact')) or bool(sequence_info.get('long_continue')):
            phase6_context = "LONG_SWING_CONTINUE"
            phase6_note = str(sequence_info.get('note') or swing_info.get('note') or 'swing HH/HL structure still supports continuation')
    elif bias_hint == "SHORT":
        if bool(swing_info.get('short_broken')):
            phase6_context = "SHORT_SWING_BREAK"
            phase6_note = str(swing_info.get('note') or 'price broke the last confirmed swing high')
        elif bool(sequence_info.get('short_exhaust')):
            phase6_context = "SHORT_SEQUENCE_EXHAUST"
            phase6_note = str(sequence_info.get('note') or 'multi-bar bear sequence is tiring')
        elif bool(swing_info.get('short_intact')) or bool(sequence_info.get('short_continue')):
            phase6_context = "SHORT_SWING_CONTINUE"
            phase6_note = str(sequence_info.get('note') or swing_info.get('note') or 'swing LH/LL structure still supports continuation')
    else:
        if bool(swing_info.get('long_broken')):
            phase6_context = "LONG_SWING_BREAK"
            phase6_note = str(swing_info.get('note') or 'price broke the last confirmed swing low')
        elif bool(swing_info.get('short_broken')):
            phase6_context = "SHORT_SWING_BREAK"
            phase6_note = str(swing_info.get('note') or 'price broke the last confirmed swing high')
        elif bool(sequence_info.get('long_exhaust')):
            phase6_context = "LONG_SEQUENCE_EXHAUST"
            phase6_note = str(sequence_info.get('note') or 'multi-bar bull sequence is tiring')
        elif bool(sequence_info.get('short_exhaust')):
            phase6_context = "SHORT_SEQUENCE_EXHAUST"
            phase6_note = str(sequence_info.get('note') or 'multi-bar bear sequence is tiring')
        elif bool(swing_info.get('long_intact')) or bool(sequence_info.get('long_continue')):
            phase6_context = "LONG_SWING_CONTINUE"
            phase6_note = str(sequence_info.get('note') or swing_info.get('note') or 'swing HH/HL structure still supports continuation')
        elif bool(swing_info.get('short_intact')) or bool(sequence_info.get('short_continue')):
            phase6_context = "SHORT_SWING_CONTINUE"
            phase6_note = str(sequence_info.get('note') or swing_info.get('note') or 'swing LH/LL structure still supports continuation')

    pa_vol_flag = "MIXED"
    pa_vol_pass = False
    pa_vol_note = "no directional candle/volume edge yet"
    strict_flag = "MIXED"
    strict_pass = False
    strict_note = "no strict fuse decision yet"
    warning_note = ""
    trend_adjust = 0.0
    pullback_adjust = 0.0
    transition_adjust = 0.0
    exhaustion_adjust = 0.0
    quality_score = 0.0

    if bias_hint == "LONG":
        volume_confirm = bool(float(volume_ratio or 1.0) >= 0.95 or volume_support >= 0.08 or delta_agreement > 0.0)
        quality_checks = 0
        fail_reasons: List[str] = []
        support_reasons: List[str] = []
        if strong_bull_body or close_pos >= 0.58 or breakout_confirmation >= 0.58:
            quality_checks += 1
        else:
            fail_reasons.append("weak close")
        if not upper_rejection:
            quality_checks += 1
        else:
            fail_reasons.append("upper-wick rejection")
        if not weak_follow and not stall:
            quality_checks += 1
        else:
            fail_reasons.append("weak follow-through")
        if volume_confirm:
            quality_checks += 1
        else:
            fail_reasons.append("volume not confirming")
        if bull_engulf:
            quality_checks += 1
            support_reasons.append("bull engulfing")
        if outside_continue_long:
            quality_checks += 1
            support_reasons.append("outside continuation")
        if retest_hold_long:
            quality_checks += 1
            support_reasons.append("retest held")
        if efficient_drive_long:
            quality_checks += 1
            support_reasons.append("efficient drive")
        if bool(sequence_info.get('long_continue')) or bool(swing_info.get('long_intact')):
            quality_checks += 1
            support_reasons.append("swing structure")
        if retest_fail_long:
            quality_checks -= 2
            fail_reasons.append("retest failed")
        if effort_up_poor_result:
            quality_checks -= 1
            fail_reasons.append("effort no result")
        if climax_absorb_long:
            quality_checks -= 1
            fail_reasons.append("climax absorption")
        if bool(sequence_info.get('long_exhaust')):
            quality_checks -= 1
            fail_reasons.append("multi-bar exhaustion")
        if bool(swing_info.get('long_broken')):
            quality_checks -= 2
            fail_reasons.append("swing support broke")
        pa_vol_pass = bool(quality_checks >= 3 and not (upper_rejection and weak_follow) and not retest_fail_long and not climax_absorb_long)
        pa_vol_flag = "PASS" if pa_vol_pass else "NO_PASS"
        if pa_vol_pass:
            note_bits = support_reasons[:2] if support_reasons else ["bull body / close / volume passed"]
            pa_vol_note = " / ".join(note_bits)
        else:
            pa_vol_note = " / ".join(fail_reasons[:2]) if fail_reasons else "bull candle quality failed"
        if structure_flag == "INTACT":
            trend_adjust += 4.0
        elif structure_flag == "BROKEN":
            trend_adjust -= 8.0
            transition_adjust += 8.0
            exhaustion_adjust += 7.0
        if pa_vol_pass:
            trend_adjust += 5.0
            transition_adjust -= 1.5
        else:
            trend_adjust -= 5.0
            transition_adjust += 5.0
            exhaustion_adjust += 4.0
        if inside_bar and breakout_confirmation < 0.58:
            pullback_adjust += 3.0
            transition_adjust += 2.0
        if upper_rejection:
            exhaustion_adjust += 4.0
        if weak_follow or stall:
            transition_adjust += 4.0
            exhaustion_adjust += 2.0
        if bull_engulf or outside_continue_long or efficient_drive_long:
            trend_adjust += 2.5
            transition_adjust -= 1.0
        if acceptance_drive_long:
            trend_adjust += 3.0
            transition_adjust -= 1.5
        if retest_hold_long:
            trend_adjust += 3.0
            transition_adjust -= 1.5
            pullback_adjust += 1.0
        if sequence_long:
            trend_adjust += 2.0
            transition_adjust -= 1.0
        if followthrough_long_pass:
            trend_adjust += 2.5
            transition_adjust -= 1.0
        if stall_after_impulse_long:
            trend_adjust -= 1.5
            transition_adjust += 2.0
            exhaustion_adjust += 1.5
        if followthrough_long_fail:
            trend_adjust -= 3.5
            transition_adjust += 3.5
            exhaustion_adjust += 2.5
        if retest_fail_long:
            trend_adjust -= 4.5
            transition_adjust += 4.0
            exhaustion_adjust += 3.0
        if effort_up_poor_result:
            trend_adjust -= 3.0
            transition_adjust += 3.0
            exhaustion_adjust += 5.0
        if climax_absorb_long:
            trend_adjust -= 2.0
            transition_adjust += 2.5
            exhaustion_adjust += 4.5
        if sweep_fail_long:
            trend_adjust -= 4.5
            transition_adjust += 4.0
            exhaustion_adjust += 5.0
        if reversal_risk_long:
            trend_adjust -= 3.0
            transition_adjust += 3.5
            exhaustion_adjust += 3.5
        if constructive_pause_long:
            trend_adjust += 1.0
            pullback_adjust += 1.5
            transition_adjust -= 0.5
        if rejection_cluster_long:
            trend_adjust -= 2.5
            transition_adjust += 2.5
            exhaustion_adjust += 3.0
        if ladder_fail_long:
            trend_adjust -= 3.0
            transition_adjust += 3.0
            exhaustion_adjust += 3.0
        if phase6_context == "LONG_SWING_CONTINUE":
            trend_adjust += 4.0
            transition_adjust -= 2.0
        elif phase6_context == "LONG_SEQUENCE_EXHAUST":
            trend_adjust -= 4.0
            transition_adjust += 4.0
            exhaustion_adjust += 6.0
        elif phase6_context == "LONG_SWING_BREAK":
            trend_adjust -= 6.0
            transition_adjust += 6.0
            exhaustion_adjust += 6.0
        strict_checks = 0
        strict_support: List[str] = []
        strict_fail: List[str] = []
        if structure_flag == "INTACT":
            strict_checks += 1
            strict_support.append("structure intact")
        else:
            strict_fail.append("structure not intact")
        if pa_vol_pass:
            strict_checks += 1
            strict_support.append("pa-vol pass")
        else:
            strict_fail.append("pa-vol no pass")
        if sequence_long or retest_hold_long or bull_engulf or efficient_drive_long or acceptance_drive_long or constructive_pause_long or bool(sequence_info.get('long_continue')) or bool(swing_info.get('long_intact')):
            strict_checks += 1
            strict_support.append("continuation pattern")
        else:
            strict_fail.append("no clean continuation pattern")
        if followthrough_long_pass:
            strict_checks += 1
            strict_support.append("follow-through pass")
        elif followthrough_long_fail or stall_after_impulse_long:
            strict_fail.append("follow-through weak")
        else:
            strict_fail.append("follow-through not proven")
        if not (structure_flag == "BROKEN" or retest_fail_long or climax_absorb_long or effort_up_poor_result or followthrough_long_fail or sweep_fail_long or reversal_risk_long or ladder_fail_long or rejection_cluster_long or bool(sequence_info.get('long_exhaust')) or bool(swing_info.get('long_broken'))):
            strict_checks += 1
        else:
            strict_fail.append("failure / exhaustion warning")
        strict_pass = bool(strict_checks >= 4 and structure_flag == "INTACT" and pa_vol_pass and not followthrough_long_fail and not retest_fail_long and not sweep_fail_long and not reversal_risk_long and not ladder_fail_long and not rejection_cluster_long and not bool(sequence_info.get('long_exhaust')) and not bool(swing_info.get('long_broken')))
        strict_flag = "PASS" if strict_pass else "NO_PASS"
        strict_note = " / ".join((strict_support[:2] if strict_pass else strict_fail[:2])) if (strict_support or strict_fail) else ("strict bull fuse passed" if strict_pass else "strict bull fuse failed")
        warning_note = structure_note if structure_flag == "BROKEN" else (phase6_note if phase6_context in ("LONG_SWING_BREAK", "LONG_SEQUENCE_EXHAUST") else (phase4_note if phase4_context in ("LONG_SWEEP_FAIL", "LONG_REVERSAL_RISK") else (phase5_note if phase5_context in ("LONG_LADDER_FAIL", "LONG_REJECTION_CLUSTER") else (phase2_note if phase2_context in ("LONG_RETEST_FAIL", "LONG_EFFORT_NO_RESULT", "LONG_CLIMAX") else (phase3_note if phase3_context in ("LONG_FOLLOWTHROUGH_FAIL", "LONG_STALL_AFTER_IMPULSE") else (pa_vol_note if not pa_vol_pass else (strict_note if not strict_pass else "")))))))
        quality_score = float(np.clip(quality_checks / 6.0, -1.0, 1.0))
    elif bias_hint == "SHORT":
        volume_confirm = bool(float(volume_ratio or 1.0) >= 0.95 or volume_support >= 0.08 or delta_agreement > 0.0)
        quality_checks = 0
        fail_reasons: List[str] = []
        support_reasons: List[str] = []
        if strong_bear_body or close_pos <= 0.42 or breakout_confirmation >= 0.58:
            quality_checks += 1
        else:
            fail_reasons.append("weak close")
        if not lower_rejection:
            quality_checks += 1
        else:
            fail_reasons.append("lower-wick rejection")
        if not weak_follow and not stall:
            quality_checks += 1
        else:
            fail_reasons.append("weak follow-through")
        if volume_confirm:
            quality_checks += 1
        else:
            fail_reasons.append("volume not confirming")
        if bear_engulf:
            quality_checks += 1
            support_reasons.append("bear engulfing")
        if outside_continue_short:
            quality_checks += 1
            support_reasons.append("outside continuation")
        if retest_hold_short:
            quality_checks += 1
            support_reasons.append("retest held")
        if efficient_drive_short:
            quality_checks += 1
            support_reasons.append("efficient drive")
        if bool(sequence_info.get('short_continue')) or bool(swing_info.get('short_intact')):
            quality_checks += 1
            support_reasons.append("swing structure")
        if retest_fail_short:
            quality_checks -= 2
            fail_reasons.append("retest failed")
        if effort_down_poor_result:
            quality_checks -= 1
            fail_reasons.append("effort no result")
        if climax_absorb_short:
            quality_checks -= 1
            fail_reasons.append("climax absorption")
        if bool(sequence_info.get('short_exhaust')):
            quality_checks -= 1
            fail_reasons.append("multi-bar exhaustion")
        if bool(swing_info.get('short_broken')):
            quality_checks -= 2
            fail_reasons.append("swing resistance broke")
        pa_vol_pass = bool(quality_checks >= 3 and not (lower_rejection and weak_follow) and not retest_fail_short and not climax_absorb_short)
        pa_vol_flag = "PASS" if pa_vol_pass else "NO_PASS"
        if pa_vol_pass:
            note_bits = support_reasons[:2] if support_reasons else ["bear body / close / volume passed"]
            pa_vol_note = " / ".join(note_bits)
        else:
            pa_vol_note = " / ".join(fail_reasons[:2]) if fail_reasons else "bear candle quality failed"
        if structure_flag == "INTACT":
            trend_adjust += 4.0
        elif structure_flag == "BROKEN":
            trend_adjust -= 8.0
            transition_adjust += 8.0
            exhaustion_adjust += 7.0
        if pa_vol_pass:
            trend_adjust += 5.0
            transition_adjust -= 1.5
        else:
            trend_adjust -= 5.0
            transition_adjust += 5.0
            exhaustion_adjust += 4.0
        if inside_bar and breakout_confirmation < 0.58:
            pullback_adjust += 3.0
            transition_adjust += 2.0
        if lower_rejection:
            exhaustion_adjust += 4.0
        if weak_follow or stall:
            transition_adjust += 4.0
            exhaustion_adjust += 2.0
        if bear_engulf or outside_continue_short or efficient_drive_short:
            trend_adjust += 2.5
            transition_adjust -= 1.0
        if acceptance_drive_short:
            trend_adjust += 3.0
            transition_adjust -= 1.5
        if retest_hold_short:
            trend_adjust += 3.0
            transition_adjust -= 1.5
            pullback_adjust += 1.0
        if sequence_short:
            trend_adjust += 2.0
            transition_adjust -= 1.0
        if followthrough_short_pass:
            trend_adjust += 2.5
            transition_adjust -= 1.0
        if stall_after_impulse_short:
            trend_adjust -= 1.5
            transition_adjust += 2.0
            exhaustion_adjust += 1.5
        if followthrough_short_fail:
            trend_adjust -= 3.5
            transition_adjust += 3.5
            exhaustion_adjust += 2.5
        if retest_fail_short:
            trend_adjust -= 4.5
            transition_adjust += 4.0
            exhaustion_adjust += 3.0
        if effort_down_poor_result:
            trend_adjust -= 3.0
            transition_adjust += 3.0
            exhaustion_adjust += 5.0
        if climax_absorb_short:
            trend_adjust -= 2.0
            transition_adjust += 2.5
            exhaustion_adjust += 4.5
        if sweep_fail_short:
            trend_adjust -= 4.5
            transition_adjust += 4.0
            exhaustion_adjust += 5.0
        if reversal_risk_short:
            trend_adjust -= 3.0
            transition_adjust += 3.5
            exhaustion_adjust += 3.5
        if constructive_pause_short:
            trend_adjust += 1.0
            pullback_adjust += 1.5
            transition_adjust -= 0.5
        if rejection_cluster_short:
            trend_adjust -= 2.5
            transition_adjust += 2.5
            exhaustion_adjust += 3.0
        if ladder_fail_short:
            trend_adjust -= 3.0
            transition_adjust += 3.0
            exhaustion_adjust += 3.0
        if phase6_context == "SHORT_SWING_CONTINUE":
            trend_adjust += 4.0
            transition_adjust -= 2.0
        elif phase6_context == "SHORT_SEQUENCE_EXHAUST":
            trend_adjust -= 4.0
            transition_adjust += 4.0
            exhaustion_adjust += 6.0
        elif phase6_context == "SHORT_SWING_BREAK":
            trend_adjust -= 6.0
            transition_adjust += 6.0
            exhaustion_adjust += 6.0
        strict_checks = 0
        strict_support: List[str] = []
        strict_fail: List[str] = []
        if structure_flag == "INTACT":
            strict_checks += 1
            strict_support.append("structure intact")
        else:
            strict_fail.append("structure not intact")
        if pa_vol_pass:
            strict_checks += 1
            strict_support.append("pa-vol pass")
        else:
            strict_fail.append("pa-vol no pass")
        if sequence_short or retest_hold_short or bear_engulf or efficient_drive_short or acceptance_drive_short or constructive_pause_short or bool(sequence_info.get('short_continue')) or bool(swing_info.get('short_intact')):
            strict_checks += 1
            strict_support.append("continuation pattern")
        else:
            strict_fail.append("no clean continuation pattern")
        if followthrough_short_pass:
            strict_checks += 1
            strict_support.append("follow-through pass")
        elif followthrough_short_fail or stall_after_impulse_short:
            strict_fail.append("follow-through weak")
        else:
            strict_fail.append("follow-through not proven")
        if not (structure_flag == "BROKEN" or retest_fail_short or climax_absorb_short or effort_down_poor_result or followthrough_short_fail or sweep_fail_short or reversal_risk_short or ladder_fail_short or rejection_cluster_short or bool(sequence_info.get('short_exhaust')) or bool(swing_info.get('short_broken'))):
            strict_checks += 1
        else:
            strict_fail.append("failure / exhaustion warning")
        strict_pass = bool(strict_checks >= 4 and structure_flag == "INTACT" and pa_vol_pass and not followthrough_short_fail and not retest_fail_short and not sweep_fail_short and not reversal_risk_short and not ladder_fail_short and not rejection_cluster_short and not bool(sequence_info.get('short_exhaust')) and not bool(swing_info.get('short_broken')))
        strict_flag = "PASS" if strict_pass else "NO_PASS"
        strict_note = " / ".join((strict_support[:2] if strict_pass else strict_fail[:2])) if (strict_support or strict_fail) else ("strict bear fuse passed" if strict_pass else "strict bear fuse failed")
        warning_note = structure_note if structure_flag == "BROKEN" else (phase6_note if phase6_context in ("SHORT_SWING_BREAK", "SHORT_SEQUENCE_EXHAUST") else (phase4_note if phase4_context in ("SHORT_SWEEP_FAIL", "SHORT_REVERSAL_RISK") else (phase5_note if phase5_context in ("SHORT_LADDER_FAIL", "SHORT_REJECTION_CLUSTER") else (phase2_note if phase2_context in ("SHORT_RETEST_FAIL", "SHORT_EFFORT_NO_RESULT", "SHORT_CLIMAX") else (phase3_note if phase3_context in ("SHORT_FOLLOWTHROUGH_FAIL", "SHORT_STALL_AFTER_IMPULSE") else (pa_vol_note if not pa_vol_pass else (strict_note if not strict_pass else "")))))))
        quality_score = float(np.clip(quality_checks / 6.0, -1.0, 1.0))
    else:
        if inside_bar or stall:
            pa_vol_flag = "NO_PASS"
            pa_vol_note = "inside-bar / stall; no directional pass"
            warning_note = pa_vol_note
        elif strong_bull_body and float(volume_ratio or 1.0) >= 1.0:
            pa_vol_flag = "PASS"
            pa_vol_pass = True
            pa_vol_note = "bull impulse candle with volume support"
        elif strong_bear_body and float(volume_ratio or 1.0) >= 1.0:
            pa_vol_flag = "PASS"
            pa_vol_pass = True
            pa_vol_note = "bear impulse candle with volume support"
        else:
            pa_vol_flag = "MIXED"
            pa_vol_note = "no clear candle/volume pass yet"
        if bull_engulf or bear_engulf:
            pa_vol_flag = "PASS"
            pa_vol_pass = True
            pa_vol_note = "engulfing candle improved directional quality"
        if sequence_long or sequence_short:
            strict_flag = "PASS"
            strict_pass = True
            strict_note = "multi-bar structure sequence is clean"
        elif followthrough_long_fail or followthrough_short_fail or effort_up_poor_result or effort_down_poor_result or climax_absorb_long or climax_absorb_short or sweep_fail_long or sweep_fail_short or reversal_risk_long or reversal_risk_short or ladder_fail_long or ladder_fail_short or rejection_cluster_long or rejection_cluster_short or phase6_context in ("LONG_SEQUENCE_EXHAUST", "SHORT_SEQUENCE_EXHAUST", "LONG_SWING_BREAK", "SHORT_SWING_BREAK"):
            strict_flag = "NO_PASS"
            strict_pass = False
            strict_note = phase6_note or phase4_note or phase5_note or phase3_note or phase2_note or "recent impulse did not pass strict checks"
        else:
            strict_flag = "MIXED"
            strict_pass = False
            strict_note = "strict fuse is waiting for directional context"
        if inside_bar:
            transition_adjust += 2.0
        if outside_bar and body_frac >= 0.55:
            transition_adjust += 3.0
        if effort_up_poor_result or effort_down_poor_result or climax_absorb_long or climax_absorb_short or sweep_fail_long or sweep_fail_short or reversal_risk_long or reversal_risk_short or ladder_fail_long or ladder_fail_short or rejection_cluster_long or rejection_cluster_short or phase6_context in ("LONG_SEQUENCE_EXHAUST", "SHORT_SEQUENCE_EXHAUST", "LONG_SWING_BREAK", "SHORT_SWING_BREAK"):
            pa_vol_flag = "NO_PASS"
            pa_vol_pass = False
            warning_note = phase6_note or phase4_note or phase5_note or phase2_note or "high effort but poor result"
            strict_flag = "NO_PASS"
            strict_pass = False
            strict_note = phase6_note or phase4_note or phase5_note or phase2_note or "high effort but poor result"
        quality_score = 0.0

    return {
        "market_structure_flag": structure_flag,
        "market_structure_pass": bool(structure_pass),
        "market_structure_note": structure_note,
        "market_structure_context": structure_context,
        "market_structure_basis": structure_basis,
        "market_structure_pivot_bias": str(swing_info.get('pivot_bias') or 'NEUTRAL'),
        "market_structure_swing_hh": bool(swing_info.get('swing_hh')),
        "market_structure_swing_hl": bool(swing_info.get('swing_hl')),
        "market_structure_swing_lh": bool(swing_info.get('swing_lh')),
        "market_structure_swing_ll": bool(swing_info.get('swing_ll')),
        "market_structure_last_high": float(swing_info.get('last_high')) if _pa_state_safe_float(swing_info.get('last_high')) is not None else np.nan,
        "market_structure_last_low": float(swing_info.get('last_low')) if _pa_state_safe_float(swing_info.get('last_low')) is not None else np.nan,
        "market_structure_pivot_count": int(swing_info.get('pivot_count') or 0),
        "market_pa_vol_flag": pa_vol_flag,
        "market_pa_vol_pass": bool(pa_vol_pass),
        "market_pa_vol_note": pa_vol_note,
        "market_pa_pattern": pattern,
        "market_pa_body_frac": float(body_frac),
        "market_pa_close_frac": float(close_pos),
        "market_pa_upper_wick_frac": float(upper_wick_frac),
        "market_pa_lower_wick_frac": float(lower_wick_frac),
        "market_pa_range_atr": float(range_atr),
        "market_pa_volume_ratio": float(volume_ratio or 0.0),
        "market_pa_higher_high": bool(higher_high),
        "market_pa_higher_low": bool(higher_low),
        "market_pa_lower_high": bool(lower_high),
        "market_pa_lower_low": bool(lower_low),
        "market_pa_inside_bar": bool(inside_bar),
        "market_pa_outside_bar": bool(outside_bar),
        "market_pa_phase2_context": phase2_context,
        "market_pa_phase2_note": phase2_note,
        "market_pa_phase3_context": phase3_context,
        "market_pa_phase3_note": phase3_note,
        "market_pa_phase4_context": phase4_context,
        "market_pa_phase4_note": phase4_note,
        "market_pa_phase5_context": phase5_context,
        "market_pa_phase5_note": phase5_note,
        "market_pa_phase6_context": phase6_context,
        "market_pa_phase6_note": phase6_note,
        "market_pa_trap_flag": trap_flag,
        "market_pa_trap_note": trap_note,
        "market_pa_strict_flag": strict_flag,
        "market_pa_strict_pass": bool(strict_pass),
        "market_pa_strict_note": strict_note,
        "market_pa_quality_score": float(quality_score),
        "market_pa_engulf_long": bool(bull_engulf),
        "market_pa_engulf_short": bool(bear_engulf),
        "market_pa_retest_hold": bool(active_retest_hold),
        "market_pa_retest_fail": bool(active_retest_fail),
        "market_pa_sequence_long": bool(sequence_long),
        "market_pa_sequence_short": bool(sequence_short),
        "market_pa_followthrough_pass": bool(followthrough_long_pass or followthrough_short_pass),
        "market_pa_followthrough_fail": bool(followthrough_long_fail or followthrough_short_fail),
        "market_pa_stall_after_impulse": bool(stall_after_impulse_long or stall_after_impulse_short),
        "market_pa_effort_result": ("CLIMAX" if (climax_absorb_long or climax_absorb_short) else ("POOR" if (effort_up_poor_result or effort_down_poor_result) else ("EFFICIENT" if (efficient_drive_long or efficient_drive_short or acceptance_drive_long or acceptance_drive_short) else "NEUTRAL"))),
        "market_pa_outside_continue": bool(outside_continue_long or outside_continue_short),
        "market_pa_acceptance_drive": bool(acceptance_drive_long or acceptance_drive_short),
        "market_pa_sweep_fail": bool(sweep_fail_long or sweep_fail_short),
        "market_pa_reversal_risk": bool(reversal_risk_long or reversal_risk_short),
        "market_pa_constructive_pause": bool(constructive_pause_long or constructive_pause_short),
        "market_pa_rejection_cluster": bool(rejection_cluster_long or rejection_cluster_short),
        "market_pa_ladder_fail": bool(ladder_fail_long or ladder_fail_short),
        "market_pa_sequence_exhaust": bool(sequence_info.get('long_exhaust') or sequence_info.get('short_exhaust')),
        "market_pa_swing_continue": bool(sequence_info.get('long_continue') or sequence_info.get('short_continue')),
        "market_warning_note": warning_note,
        "market_pa_trend_adjust": float(trend_adjust),
        "market_pa_pullback_adjust": float(pullback_adjust),
        "market_pa_transition_adjust": float(transition_adjust),
        "market_pa_exhaustion_adjust": float(exhaustion_adjust),
        "market_no_progress": float(no_progress),
        "market_trend_decel_strength": float(trend_decel),
        "market_last_bar_open": (float(o) if o is not None else np.nan),
        "market_last_bar_high": (float(h) if h is not None else np.nan),
        "market_last_bar_low": (float(l) if l is not None else np.nan),
        "market_last_bar_close": (float(c) if c is not None else np.nan),
        "market_last_bar_volume": (float(v) if v is not None else np.nan),
        "market_prior_bar_open": (float(prev_open) if prev_open is not None else np.nan),
        "market_prior_bar_high": (float(prev_high) if prev_high is not None else np.nan),
        "market_prior_bar_low": (float(prev_low) if prev_low is not None else np.nan),
        "market_prior_bar_close": (float(prev_close) if prev_close is not None else np.nan),
        "_market_pa_seq": int(cur_seq),
        "_market_pa_bar_history": bar_history[-12:],
        "_market_pa_pivots": pivots[-12:],
    }

def _market_breakout_context(snap: Dict[str, Any], label: str, bias: str, phase: str, prev_state: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    compression_maturity = float(_safe_float(snap.get("market_compression_maturity")) or 0.0)
    containment_rate = float(_safe_float(snap.get("market_containment_rate")) or 0.0)
    range_contraction = float(_safe_float(snap.get("market_range_contraction")) or 0.0)
    swing_squeeze = float(_safe_float(snap.get("market_swing_squeeze")) or 0.0)
    breakout_quality = float(_safe_float(snap.get("market_breakout_quality")) or 0.0)
    breakout_fail = float(_safe_float(snap.get("market_breakout_fail_rate")) or 0.0)
    breakout_readiness = float(_safe_float(snap.get("market_breakout_readiness")) or 0.0)
    breakout_confirmation = float(_safe_float(snap.get("market_breakout_confirmation")) or 0.0)
    volume_support = float(_safe_float(snap.get("market_volume_support")) or 0.0)
    delta_strength = float(_safe_float(snap.get("market_delta_strength")) or 0.0)
    delta_agreement = float(_safe_float(snap.get("market_delta_agreement")) or 0.0)
    direction_evidence = float(_safe_float(snap.get("market_direction_evidence")) or 0.0)
    pressure_bias = str(snap.get("market_pressure_bias") or "NEUTRAL").upper().strip()
    bb_inside_kc = bool(_safe_bool(snap.get("market_bb_inside_kc")))
    bb_squeeze_strength = float(_safe_float(snap.get("market_bb_squeeze_strength")) or 0.0)
    bb_expand_strength = float(_safe_float(snap.get("market_bb_expand_strength")) or 0.0)
    bb_breakout_long = bool(_safe_bool(snap.get("market_bb_breakout_long")))
    bb_breakout_short = bool(_safe_bool(snap.get("market_bb_breakout_short")))
    bb_band_walk_long = bool(_safe_bool(snap.get("market_bb_band_walk_long")))
    bb_band_walk_short = bool(_safe_bool(snap.get("market_bb_band_walk_short")))
    vwap_bias = str(snap.get("market_vwap_bias") or "NEUTRAL").upper().strip() or "NEUTRAL"
    vwap_reclaim_long = bool(_safe_bool(snap.get("market_vwap_reclaim_long")))
    vwap_reclaim_short = bool(_safe_bool(snap.get("market_vwap_reclaim_short")))
    sequence_long = bool(_safe_bool(snap.get("market_breakout_sequence_long")))
    sequence_short = bool(_safe_bool(snap.get("market_breakout_sequence_short")))
    single_spike_long = bool(_safe_bool(snap.get("market_breakout_single_spike_long")))
    single_spike_short = bool(_safe_bool(snap.get("market_breakout_single_spike_short")))
    drive_long = float(_safe_float(snap.get("market_breakout_drive_long")) or 0.0)
    drive_short = float(_safe_float(snap.get("market_breakout_drive_short")) or 0.0)
    true_compression = bool(_safe_bool(snap.get("market_true_compression")))
    volatile_rotation = bool(_safe_bool(snap.get("market_volatile_rotation")))

    mature_compression = (
        compression_maturity >= 62.0
        and containment_rate >= 0.60
        and (range_contraction >= 0.36 or swing_squeeze >= 0.36 or (bb_inside_kc and bb_squeeze_strength >= 0.10))
    )
    compression_lineage = bool(true_compression) and _market_breakout_eligible_from_compression(snap, prev_state=prev_state)
    squeeze_ready = bool(bb_inside_kc and bb_squeeze_strength >= 0.10)
    productive_long = direction_evidence >= 2.0
    productive_short = direction_evidence <= -2.0
    value_ok_long = bool(vwap_bias == "LONG" or vwap_reclaim_long)
    value_ok_short = bool(vwap_bias == "SHORT" or vwap_reclaim_short)
    release_ok_long = bool(bb_expand_strength >= 0.20 or bb_breakout_long or bb_band_walk_long)
    release_ok_short = bool(bb_expand_strength >= 0.20 or bb_breakout_short or bb_band_walk_short)

    watch_long = (
        compression_lineage
        and not volatile_rotation
        and (mature_compression or squeeze_ready or label == "COMPRESSION")
        and pressure_bias == "LONG"
        and breakout_readiness >= 0.56
        and breakout_confirmation < 0.60
        and breakout_quality >= 0.38
        and volume_support >= 0.40
        and delta_strength >= 0.42
        and delta_agreement > 0.0
        and value_ok_long
    )
    watch_short = (
        compression_lineage
        and not volatile_rotation
        and (mature_compression or squeeze_ready or label == "COMPRESSION")
        and pressure_bias == "SHORT"
        and breakout_readiness >= 0.56
        and breakout_confirmation < 0.60
        and breakout_quality >= 0.38
        and volume_support >= 0.40
        and delta_strength >= 0.42
        and delta_agreement > 0.0
        and value_ok_short
    )

    confirm_long = (
        compression_lineage
        and not volatile_rotation
        and pressure_bias == "LONG"
        and breakout_confirmation >= 0.60
        and breakout_quality >= 0.50
        and volume_support >= 0.46
        and delta_strength >= 0.46
        and delta_agreement > 0.0
        and productive_long
        and value_ok_long
        and release_ok_long
        and sequence_long
        and not single_spike_long
        and drive_long >= 0.58
    )
    confirm_short = (
        compression_lineage
        and not volatile_rotation
        and pressure_bias == "SHORT"
        and breakout_confirmation >= 0.60
        and breakout_quality >= 0.50
        and volume_support >= 0.46
        and delta_strength >= 0.46
        and delta_agreement > 0.0
        and productive_short
        and value_ok_short
        and release_ok_short
        and sequence_short
        and not single_spike_short
        and drive_short >= 0.58
    )

    prev_breakout_state = "NONE"
    if isinstance(prev_state, dict):
        prev_breakout_state = str(prev_state.get("market_breakout_state") or "NONE").upper().strip() or "NONE"

    failed_breakout = (
        breakout_fail >= 0.58
        and breakout_confirmation < 0.46
        and prev_breakout_state in ("BREAKOUT_WATCH_LONG", "BREAKOUT_WATCH_SHORT", "BREAKOUT_CONFIRM_LONG", "BREAKOUT_CONFIRM_SHORT")
    )

    breakout_state = "NONE"
    breakout_bias = "NEUTRAL"
    breakout_score = 0
    breakout_note = "no active breakout warning"

    if confirm_long:
        breakout_state = "BREAKOUT_CONFIRM_LONG"
        breakout_bias = "LONG"
        breakout_score = int(np.clip(round(100.0 * (0.38 * breakout_confirmation + 0.20 * breakout_quality + 0.16 * volume_support + 0.10 * delta_strength + 0.08 * min(1.0, max(0.0, direction_evidence) / 4.0) + 0.08 * bb_expand_strength)), 0, 100))
        breakout_note = "price is releasing from compression with a multi-candle upward drive, BB/KC release, VWAP support, and long-side volume/delta agreement"
    elif confirm_short:
        breakout_state = "BREAKOUT_CONFIRM_SHORT"
        breakout_bias = "SHORT"
        breakout_score = int(np.clip(round(100.0 * (0.38 * breakout_confirmation + 0.20 * breakout_quality + 0.16 * volume_support + 0.10 * delta_strength + 0.08 * min(1.0, max(0.0, -direction_evidence) / 4.0) + 0.08 * bb_expand_strength)), 0, 100))
        breakout_note = "price is releasing from compression with a multi-candle downward drive, BB/KC release, VWAP support, and short-side volume/delta agreement"
    elif watch_long:
        breakout_state = "BREAKOUT_WATCH_LONG"
        breakout_bias = "LONG"
        breakout_score = int(np.clip(round(100.0 * (0.30 * breakout_readiness + 0.16 * volume_support + 0.16 * delta_strength + 0.14 * (compression_maturity / 100.0) + 0.12 * breakout_quality + 0.12 * max(bb_squeeze_strength, 0.0))), 0, 100))
        breakout_note = "mature compression / squeeze with long-side pressure; watch for a rapid multi-candle upward drive, then hold and acceptance above VWAP"
    elif watch_short:
        breakout_state = "BREAKOUT_WATCH_SHORT"
        breakout_bias = "SHORT"
        breakout_score = int(np.clip(round(100.0 * (0.30 * breakout_readiness + 0.16 * volume_support + 0.16 * delta_strength + 0.14 * (compression_maturity / 100.0) + 0.12 * breakout_quality + 0.12 * max(bb_squeeze_strength, 0.0))), 0, 100))
        breakout_note = "mature compression / squeeze with short-side pressure; watch for a rapid multi-candle downward drive, then hold and acceptance below VWAP"
    elif failed_breakout:
        prev_bias = "LONG" if "LONG" in prev_breakout_state else ("SHORT" if "SHORT" in prev_breakout_state else "NEUTRAL")
        breakout_state = "FAILED_BREAKOUT"
        breakout_bias = prev_bias
        breakout_score = int(np.clip(round(100.0 * breakout_fail), 0, 100))
        breakout_note = "recent breakout attempt failed and price fell back into the compressed range"
    elif compression_lineage and mature_compression and label == "COMPRESSION":
        breakout_state = "BREAKOUT_READY" if breakout_readiness >= 0.60 and volume_support >= 0.38 and (squeeze_ready or bb_squeeze_strength >= 0.08) else "NONE"
        breakout_bias = pressure_bias if pressure_bias in ("LONG", "SHORT") else "NEUTRAL"
        breakout_score = int(np.clip(round(100.0 * (0.48 * breakout_readiness + 0.18 * volume_support + 0.14 * delta_strength + 0.10 * (compression_maturity / 100.0) + 0.10 * max(bb_squeeze_strength, 0.0))), 0, 100)) if breakout_state != "NONE" else 0
        breakout_note = "compression is mature, BB/KC squeeze is active, and range-release conditions are improving" if breakout_state != "NONE" else breakout_note

    pretty_map = {
        "NONE": "-",
        "BREAKOUT_READY": "Breakout Ready",
        "BREAKOUT_WATCH_LONG": "Breakout Watch Long",
        "BREAKOUT_WATCH_SHORT": "Breakout Watch Short",
        "BREAKOUT_CONFIRM_LONG": "Breakout Confirm Long",
        "BREAKOUT_CONFIRM_SHORT": "Breakout Confirm Short",
        "FAILED_BREAKOUT": "Failed Breakout",
    }

    return {
        "market_breakout_style": ("SEQUENCE_LONG" if sequence_long and not single_spike_long else ("SEQUENCE_SHORT" if sequence_short and not single_spike_short else ("SPIKE_LONG" if single_spike_long else ("SPIKE_SHORT" if single_spike_short else "NONE")))),
        "market_breakout_state": breakout_state,
        "market_breakout_bias": breakout_bias,
        "market_breakout_pretty": pretty_map.get(breakout_state, pretty_market_state_label(breakout_state)),
        "market_breakout_score": int(np.clip(breakout_score, 0, 100)),
        "market_breakout_trigger_ready": bool(breakout_state in ("BREAKOUT_CONFIRM_LONG", "BREAKOUT_CONFIRM_SHORT")),
        "market_breakout_note": breakout_note,
    }


def _market_textures_and_summaries(label: str, snap: Dict[str, Any], bias: str) -> Tuple[str, str, str]:
    width_tight = float(_safe_float(snap.get("market_width_tightness")) or 0.0)
    slope_flat = float(_safe_float(snap.get("market_slope_flatness")) or 0.0)
    containment = float(_safe_float(snap.get("market_containment_rate")) or 0.0)
    breakout_quality = float(_safe_float(snap.get("market_breakout_quality")) or 0.0)
    breakout_readiness = float(_safe_float(snap.get("market_breakout_readiness")) or 0.0)
    breakout_confirmation = float(_safe_float(snap.get("market_breakout_confirmation")) or 0.0)
    compression_maturity = float(_safe_float(snap.get("market_compression_maturity")) or 0.0)
    range_contraction = float(_safe_float(snap.get("market_range_contraction")) or 0.0)
    swing_squeeze = float(_safe_float(snap.get("market_swing_squeeze")) or 0.0)
    delta_strength = float(_safe_float(snap.get("market_delta_strength")) or 0.0)
    delta_agreement = float(_safe_float(snap.get("market_delta_agreement")) or 0.0)
    trend_score = float(_safe_float(snap.get("market_trend_score")) or 0.0)
    pullback_score = float(_safe_float(snap.get("market_pullback_score")) or 0.0)
    exhaust_score = float(_safe_float(snap.get("market_exhaustion_score")) or 0.0)
    accel = float(_safe_float(snap.get("market_slope_accel_strength")) or 0.0)
    conflict = float(_safe_float(snap.get("market_conflict_score")) or 0.0)
    pressure_bias = str(snap.get("market_pressure_bias") or "NEUTRAL").upper().strip()

    texture = "BALANCED"
    summary_short = "market context forming"
    summary_long = "The market state engine is active but lacks a stronger descriptive profile yet."

    if label == "COMPRESSION":
        if pressure_bias in ("LONG", "SHORT") and compression_maturity >= 62.0 and breakout_readiness >= 0.58:
            texture = f"{pressure_bias}_PRESSURE_COMPRESSION"
            summary_short = f"mature squeeze / {pressure_bias.lower()} pressure building / breakout watch"
            summary_long = "The market is in a mature compression regime: highs and lows are squeezing, candles are staying contained, and directional pressure is leaning to one side. Treat this as compression first, with breakout watch conditions developing underneath."
        elif compression_maturity >= 68.0 and (range_contraction >= 0.42 or swing_squeeze >= 0.42):
            texture = "MATURE_COMPRESSION"
            summary_short = "squeezing highs-lows / contained candles / breakout watch neutral"
            summary_long = "This is mature compression: the local range is tightening across many bars, slope is flattening, and follow-through is poor. The market is coiling rather than transitioning, so the next edge comes from a real release, not from forcing directional trades inside the squeeze."
        else:
            texture = "BALANCED_COMPRESSION"
            summary_short = "tight width / contained candles / breakout quality poor"
            summary_long = "The structure is compressed: channel width is tight, candles are staying contained, and breakout follow-through is weak. This is a low-edge environment until expansion returns."
    elif label == "TRANSITION":
        if str(snap.get("market_volatility_regime") or "").upper().strip() == "VOLATILE_ROTATION":
            texture = "VOLATILE_ROTATION"
            summary_short = "wide rotation / high volatility / not a squeeze"
            summary_long = "This is a wide two-sided rotation, not real compression. Price is moving a lot inside a broad box, so breakout-style squeeze assumptions should be avoided until amplitude contracts materially."
        elif breakout_confirmation >= 0.58 and abs(float(_safe_float(snap.get("market_direction_evidence")) or 0.0)) >= 2.5:
            texture = "BREAKOUT_RELEASING"
            summary_short = "compression releasing / range breaking / confirmation building"
            summary_long = "The market is trying to release from compression. Price is beginning to escape the prior range and the release has enough price, volume, or delta support to justify a transition label, but it is still early enough that full expansion confirmation is pending."
        elif accel >= 0.60 and conflict >= 0.45:
            texture = "ROTATIONAL_TRANSITION"
            summary_short = "slope reorienting / width changing / direction mixed"
            summary_long = "Multiple inputs are re-organising at once. Slope and width are changing, but directional evidence is not yet settled enough to call a clean trend."
        elif accel >= 0.60 and abs(float(_safe_float(snap.get("market_direction_evidence")) or 0.0)) >= 2.5:
            texture = "EARLY_BREAK_ATTEMPT"
            summary_short = "bias emerging / acceleration rising / confirmation pending"
            summary_long = "Directional evidence is trying to build, but the structure still needs cleaner confirmation. Treat this as a potential release, not a confirmed trend."
        else:
            texture = "LATE_ROTATION"
            summary_short = "mixed structure / unstable bias / waiting cleaner alignment"
            summary_long = "The market is between regimes. Patience is preferable because the indicators are not yet agreeing on a stable directional or compression state."
    elif label.endswith("EXPANSION"):
        side = "bullish" if bias == "LONG" else "bearish"
        if trend_score >= 68.0 and breakout_quality >= 0.55 and exhaust_score < 45.0:
            texture = f"HEALTHY_{side.upper()}_TREND"
            summary_short = f"aligned slope / holding outside value / supportive {side} pressure"
            summary_long = f"Trend structure is healthy: directional alignment is good, breakout quality is holding, and participation is supportive for the active {side} move."
        elif exhaust_score >= 45.0:
            texture = f"AGING_{side.upper()}_TREND"
            summary_short = f"direction intact / impulse aging / monitor follow-through"
            summary_long = f"The market still points {side}, but the trend is maturing. Watch for weaker follow-through, more rejection, or a transition into pullback/exhaustion."
        else:
            texture = f"DRIFTING_{side.upper()}_TREND"
            summary_short = f"directional drift / moderate follow-through / needs fresh push"
            summary_long = f"Directional bias remains {side}, but the move is more drift than impulse right now. A fresh expansion pulse would improve continuation quality."
    elif label.endswith("PULLBACK"):
        side = "bullish" if bias == "LONG" else "bearish"
        texture = f"CONTROLLED_{side.upper()}_PULLBACK"
        summary_short = f"{side} bias intact / inside reset / wait re-drive"
        summary_long = f"The broader {side} bias is intact, but the current bar structure looks like a controlled pullback rather than fresh initiative. Better entries come from renewed confirmation."
    elif label.endswith("EXHAUSTION"):
        side = "bullish" if bias == "LONG" else "bearish"
        texture = f"LATE_{side.upper()}_EXHAUSTION"
        summary_short = f"trend stretched / slope decelerating / reversal risk higher"
        summary_long = f"The {side} trend is stretched and losing efficiency. Momentum is no longer cleanly translating into progress, so fresh chase entries should be treated cautiously."

    return texture, summary_short, summary_long


def compute_market_state_snapshot(snapshot: Optional[Dict[str, Any]], prev_state: Optional[Dict[str, Any]] = None, thresholds: MarketStateThresholds = DEFAULT_MARKET_STATE_THRESHOLDS) -> Dict[str, Any]:
    try:
        return run_classic_market_state_pipeline(
            snapshot,
            prev_state,
            thresholds,
            price_action_overlay_builder=lambda cur_snap, prev, bias: _market_price_action_overlay(cur_snap, prev_state=prev, bias_hint=bias),
            hierarchy_overlay_builder=lambda cur_snap, structure_flag, bias: _market_hierarchy_overlay(cur_snap, structure_flag=structure_flag, bias_hint=bias),
            value_overlay_builder=lambda cur_snap, bias: _market_value_volatility_overlay(cur_snap, bias_hint=bias),
            breakout_context_builder=lambda cur_snap, label, bias, phase, prev: _market_breakout_context(cur_snap, label, bias, phase, prev_state=prev),
            pretty_label_builder=pretty_market_state_label,
        )
    except Exception as exc:
        return build_market_state_failure_payload(snapshot, prev_state=prev_state, reason=str(exc), error_type=exc.__class__.__name__)

def _augment_stream_with_market_state(df: "pd.DataFrame", thresholds: MarketStateThresholds = DEFAULT_MARKET_STATE_THRESHOLDS) -> "pd.DataFrame":
    if df is None or len(df) == 0:
        return df

    out = df.copy()
    idx = out.index
    short_win = 24
    long_win = 72
    compression_win = 28
    pressure_win = 14
    breakout_watch_win = 10
    min_short = 6
    min_long = 12
    min_compression = 12
    eps = 1e-12

    close_ser = _safe_series(out, "close", fallback=_safe_series(out, "price"))
    close_float = close_ser.astype(float)
    open_ser = _safe_series(out, "open", fallback=close_ser)
    high_ser = _safe_series(out, "high", fallback=pd.concat([open_ser, close_ser], axis=1).max(axis=1))
    low_ser = _safe_series(out, "low", fallback=pd.concat([open_ser, close_ser], axis=1).min(axis=1))
    volume_ser = _safe_series(out, "volume", fallback=np.nan)
    open_float = open_ser.astype(float)
    high_float = high_ser.astype(float)
    low_float = low_ser.astype(float)
    volume_float = volume_ser.astype(float)

    line_ref = _safe_series(out, "vidya_line", fallback=_safe_series(out, "vidya_raw", fallback=close_ser))
    raw_ref = _safe_series(out, "vidya_raw", fallback=line_ref)
    active_ref = line_ref.where(line_ref.notna(), raw_ref)
    prev_ref = active_ref.shift(1)
    slope = active_ref - prev_ref
    slope_float = slope.astype(float)

    angle_raw = np.degrees(np.arctan(slope.to_numpy(dtype=float, copy=False)))
    angle_raw_ser = pd.Series(angle_raw, index=idx).astype(float)

    atr_series = _safe_series(out, "atr")
    true_range_proxy = (high_ser - low_ser).abs().replace(0.0, np.nan)
    atr_fallback = true_range_proxy.rolling(14, min_periods=1).median()
    atr_eff = atr_series.where(atr_series.abs() > eps, atr_fallback)
    atr_eff = atr_eff.where(atr_eff.abs() > eps, close_ser.abs() * 0.001)
    atr_eff = atr_eff.where(atr_eff.abs() > eps, 1.0)

    bb_mid = close_ser.rolling(20, min_periods=5).mean()
    bb_std = close_ser.rolling(20, min_periods=5).std(ddof=0).fillna(0.0)
    bb_upper = bb_mid + 2.0 * bb_std
    bb_lower = bb_mid - 2.0 * bb_std
    bb_width = (bb_upper - bb_lower).abs()
    kc_mid = close_ser.ewm(span=20, adjust=False, min_periods=5).mean()
    kc_mult = 1.5
    kc_upper = kc_mid + kc_mult * atr_eff
    kc_lower = kc_mid - kc_mult * atr_eff
    kc_width = (kc_upper - kc_lower).abs()
    bb_inside_kc = ((bb_upper <= kc_upper) & (bb_lower >= kc_lower) & bb_upper.notna() & bb_lower.notna() & kc_upper.notna() & kc_lower.notna())
    bb_squeeze_strength = _clip01_series(((kc_width - bb_width) / kc_width.where(kc_width.abs() > eps, np.nan)).replace([np.inf, -np.inf], np.nan).fillna(0.0))
    bb_width_ma = bb_width.rolling(10, min_periods=3).mean().where(lambda s: s.abs() > eps, np.nan)
    bb_expand_strength = _clip01_series(((bb_width - bb_width_ma) / bb_width_ma).replace([np.inf, -np.inf], np.nan).fillna(0.0))
    bb_breakout_long = ((close_ser > bb_upper) & bb_upper.notna()).fillna(False)
    bb_breakout_short = ((close_ser < bb_lower) & bb_lower.notna()).fillna(False)
    bb_band_walk_long = ((close_ser >= (bb_upper - 0.15 * bb_width.fillna(0.0))) & (close_ser > bb_mid)).fillna(False)
    bb_band_walk_short = ((close_ser <= (bb_lower + 0.15 * bb_width.fillna(0.0))) & (close_ser < bb_mid)).fillna(False)
    bb_width_atr = (bb_width / atr_eff.where(atr_eff.abs() > eps, np.nan)).replace([np.inf, -np.inf], np.nan).fillna(0.0)
    kc_width_atr = (kc_width / atr_eff.where(atr_eff.abs() > eps, np.nan)).replace([np.inf, -np.inf], np.nan).fillna(0.0)

    typical_price = ((high_ser + low_ser + close_ser) / 3.0).fillna(close_ser)
    vol_pos = volume_ser.fillna(0.0).clip(lower=0.0)
    if isinstance(out.index, pd.DatetimeIndex):
        try:
            session_keys = out.index.normalize()
        except Exception:
            session_keys = pd.Series(np.arange(len(out)), index=idx)
        cum_pv = (typical_price * vol_pos).groupby(session_keys).cumsum()
        cum_v = vol_pos.groupby(session_keys).cumsum()
    else:
        cum_pv = (typical_price * vol_pos).cumsum()
        cum_v = vol_pos.cumsum()
    vwap = (cum_pv / cum_v.where(cum_v.abs() > eps, np.nan)).fillna(close_ser.expanding(min_periods=1).mean())
    vwap_distance_atr = ((close_ser - vwap) / atr_eff).replace([np.inf, -np.inf], np.nan).fillna(0.0)
    vwap_bias = pd.Series(np.where(vwap_distance_atr >= 0.12, "LONG", np.where(vwap_distance_atr <= -0.12, "SHORT", "NEUTRAL")), index=idx)
    vwap_reclaim_long = ((close_ser > vwap) & (close_ser.shift(1) <= vwap.shift(1))).fillna(False)
    vwap_reclaim_short = ((close_ser < vwap) & (close_ser.shift(1) >= vwap.shift(1))).fillna(False)
    vwap_reject_long = ((high_ser >= vwap) & (close_ser < vwap) & (close_ser < open_ser)).fillna(False)
    vwap_reject_short = ((low_ser <= vwap) & (close_ser > vwap) & (close_ser > open_ser)).fillna(False)

    market_bb_middle = bb_mid.astype(float)
    market_bb_upper = bb_upper.astype(float)
    market_bb_lower = bb_lower.astype(float)
    market_bb_inside_kc = bb_inside_kc.astype(bool)
    market_bb_squeeze_strength = bb_squeeze_strength.astype(float)
    market_bb_expand_strength = bb_expand_strength.astype(float)
    market_bb_width_atr = bb_width_atr.astype(float)
    market_kc_width_atr = kc_width_atr.astype(float)
    market_bb_breakout_long = bb_breakout_long.astype(bool)
    market_bb_breakout_short = bb_breakout_short.astype(bool)
    market_bb_band_walk_long = bb_band_walk_long.astype(bool)
    market_bb_band_walk_short = bb_band_walk_short.astype(bool)
    market_kc_middle = kc_mid.astype(float)
    market_kc_upper = kc_upper.astype(float)
    market_kc_lower = kc_lower.astype(float)
    market_vwap = vwap.astype(float)
    market_vwap_distance_atr = vwap_distance_atr.astype(float)
    market_vwap_bias = vwap_bias.astype(object)
    market_vwap_reclaim_long = vwap_reclaim_long.astype(bool)
    market_vwap_reclaim_short = vwap_reclaim_short.astype(bool)
    market_vwap_reject_long = vwap_reject_long.astype(bool)
    market_vwap_reject_short = vwap_reject_short.astype(bool)

    angle_norm = np.degrees(np.arctan((slope / atr_eff).to_numpy(dtype=float, copy=False)))
    angle_norm_ser = pd.Series(angle_norm, index=idx).astype(float)

    angle_state = np.where(np.isnan(angle_norm), None, np.where(angle_norm > float(thresholds.vidya_angle_up_deg), "UP", np.where(angle_norm < float(thresholds.vidya_angle_down_deg), "DOWN", "FLAT")))

    angle_delta = pd.Series(angle_norm, index=idx).diff()
    angle_accel = np.where(angle_delta.isna(), None, np.where(angle_delta > 1.5, "ACCEL", np.where(angle_delta < -1.5, "DECEL", "STABLE")))

    frama_mid = _safe_series(out, "frama", fallback=np.nan)
    frama_upper = _safe_series(out, "frama_upper", fallback=np.nan)
    frama_lower = _safe_series(out, "frama_lower", fallback=np.nan)
    vidya_upper = _safe_series(out, "vidya_upper", fallback=np.nan)
    vidya_lower = _safe_series(out, "vidya_lower", fallback=np.nan)
    range_line = _safe_series(out, "range_filter_line", fallback=np.nan)
    range_upper = _safe_series(out, "range_filter_upper", fallback=np.nan)
    range_lower = _safe_series(out, "range_filter_lower", fallback=np.nan)

    if frama_mid.isna().all() and (frama_upper.notna().any() or frama_lower.notna().any()):
        frama_mid = pd.concat([frama_upper, frama_lower], axis=1).mean(axis=1)
    active_mid = frama_mid.where(frama_mid.notna(), active_ref.where(active_ref.notna(), range_line))
    active_upper = frama_upper.where(frama_upper.notna(), vidya_upper.where(vidya_upper.notna(), range_upper))
    active_lower = frama_lower.where(frama_lower.notna(), vidya_lower.where(vidya_lower.notna(), range_lower))

    frama_width = (frama_upper - frama_lower).abs()
    vidya_width = (vidya_upper - vidya_lower).abs()
    range_width = (range_upper - range_lower).abs()
    width_stack = pd.concat([frama_width, vidya_width, range_width], axis=1)
    active_width = width_stack.mean(axis=1, skipna=True)
    active_width = active_width.where(active_width.abs() > eps, atr_eff)
    width_atr = (active_width / atr_eff).replace([np.inf, -np.inf], np.nan)

    frama_slope_norm = ((frama_mid - frama_mid.shift(1)) / atr_eff).replace([np.inf, -np.inf], np.nan)
    vidya_slope_norm = (slope / atr_eff).replace([np.inf, -np.inf], np.nan)
    range_slope_norm = ((range_line - range_line.shift(1)) / atr_eff).replace([np.inf, -np.inf], np.nan)
    slope_stack = pd.concat([vidya_slope_norm, frama_slope_norm, range_slope_norm], axis=1)
    active_slope_norm = slope_stack.mean(axis=1, skipna=True)
    active_slope_norm = active_slope_norm.fillna(0.0)

    slope_abs = active_slope_norm.abs()
    slope_q25 = _rolling_q(slope_abs, long_win, 0.25, min_long)
    slope_q75 = _rolling_q(slope_abs, long_win, 0.75, min_long)
    slope_strength = _clip01_series(_ratio_to_window(slope_abs, slope_q25, slope_q75))
    slope_flatness = _clip01_series(1.0 - slope_strength)
    slope_delta_norm = active_slope_norm.diff().fillna(0.0)
    accel_scale = _rolling_q(slope_delta_norm.abs(), long_win, 0.75, min_long).where(lambda s: s.abs() > eps, 1.0)
    slope_accel_strength = _clip01_series((slope_delta_norm.abs() / accel_scale).replace([np.inf, -np.inf], np.nan).fillna(0.0))
    width_q20 = _rolling_q(width_atr, long_win, 0.20, min_long)
    width_q50 = _rolling_q(width_atr, long_win, 0.50, min_long)
    width_q80 = _rolling_q(width_atr, long_win, 0.80, min_long)
    width_rank = _ratio_to_window(width_atr, width_q20, width_q80).fillna(0.5)
    width_tightness = _clip01_series(1.0 - width_rank)
    width_expansion = _clip01_series(((width_atr - width_q50) / (width_q80 - width_q50).where((width_q80 - width_q50).abs() > eps, 1.0)).replace([np.inf, -np.inf], np.nan).fillna(0.0))
    width_change_strength = _clip01_series((width_atr.diff().abs() / width_atr.rolling(short_win, min_periods=min_short).median().where(lambda s: s.abs() > eps, 1.0)).replace([np.inf, -np.inf], np.nan).fillna(0.0))

    close_inside_frama = ((close_ser >= frama_lower) & (close_ser <= frama_upper) & frama_upper.notna() & frama_lower.notna()).astype(float)
    close_inside_vidya = ((close_ser >= vidya_lower) & (close_ser <= vidya_upper) & vidya_upper.notna() & vidya_lower.notna()).astype(float)
    close_inside_range = ((close_ser >= range_lower) & (close_ser <= range_upper) & range_upper.notna() & range_lower.notna()).astype(float)
    inside_stack = pd.concat([close_inside_frama.where(frama_upper.notna() & frama_lower.notna()), close_inside_vidya.where(vidya_upper.notna() & vidya_lower.notna()), close_inside_range.where(range_upper.notna() & range_lower.notna())], axis=1)
    inside_now = inside_stack.mean(axis=1, skipna=True).fillna(0.0)
    containment_rate = inside_now.rolling(short_win, min_periods=min_short).mean().fillna(inside_now)

    body = (close_ser - open_ser).abs()
    upper_wick = (high_ser - pd.concat([open_ser, close_ser], axis=1).max(axis=1)).clip(lower=0.0)
    lower_wick = (pd.concat([open_ser, close_ser], axis=1).min(axis=1) - low_ser).clip(lower=0.0)
    range_abs = (high_ser - low_ser).abs().replace(0.0, np.nan)

    prev_high = high_ser.shift(1)
    prev_low = low_ser.shift(1)
    overlap_amt = (pd.concat([high_ser, prev_high], axis=1).min(axis=1) - pd.concat([low_ser, prev_low], axis=1).max(axis=1)).clip(lower=0.0)
    overlap_den = pd.concat([range_abs, range_abs.shift(1), atr_eff], axis=1).max(axis=1).where(lambda s: s.abs() > eps, 1.0)
    overlap_ratio = (overlap_amt / overlap_den).replace([np.inf, -np.inf], np.nan).fillna(0.0)
    rotationality = overlap_ratio.rolling(short_win, min_periods=min_short).mean().fillna(overlap_ratio)

    outside_up = ((close_ser > active_upper) & active_upper.notna()).astype(float)
    outside_down = ((close_ser < active_lower) & active_lower.notna()).astype(float)
    outside_side = pd.Series(np.where(outside_up > 0.0, 1.0, np.where(outside_down > 0.0, -1.0, 0.0)), index=idx)
    breakout_hold = ((outside_side != 0.0) & (outside_side == outside_side.shift(1))).astype(float).rolling(short_win, min_periods=min_short).mean().fillna(0.0)
    breakout_fail = ((outside_side.shift(1) != 0.0) & (outside_side == 0.0)).astype(float).rolling(short_win, min_periods=min_short).mean().fillna(0.0)

    vid_sign = _series_sign_from_state(out.get("vidya_state", pd.Series(index=idx, dtype=object)))
    frama_sign = _series_sign_from_state(out.get("frama_state", pd.Series(index=idx, dtype=object)))
    range_sign = _series_sign_from_state(out.get("range_filter_state", pd.Series(index=idx, dtype=object)))
    macd_rel = pd.Series(np.where(_safe_series(out, "macd").fillna(0.0) > _safe_series(out, "signal").fillna(0.0), 1.0, np.where(_safe_series(out, "macd").fillna(0.0) < _safe_series(out, "signal").fillna(0.0), -1.0, 0.0)), index=idx)
    ms_sign = _series_sign_from_color(out.get("ms", pd.Series(index=idx, dtype=object)))
    angle_sign = _series_sign_from_angle_state(pd.Series(angle_state, index=idx, dtype=object))
    slope_sign = pd.Series(np.where(active_slope_norm > 0.0, 1.0, np.where(active_slope_norm < 0.0, -1.0, 0.0)), index=idx)

    direction_evidence = 3.0 * vid_sign + 2.0 * frama_sign + 2.0 * range_sign + 1.0 * macd_rel + 1.0 * ms_sign + 1.5 * angle_sign + 1.0 * slope_sign
    direction_abs_strength = _clip01_series((direction_evidence.abs() / 10.5).fillna(0.0))
    pos_votes = ((vid_sign > 0.0).astype(float) + (frama_sign > 0.0).astype(float) + (range_sign > 0.0).astype(float) + (macd_rel > 0.0).astype(float) + (ms_sign > 0.0).astype(float) + (angle_sign > 0.0).astype(float))
    neg_votes = ((vid_sign < 0.0).astype(float) + (frama_sign < 0.0).astype(float) + (range_sign < 0.0).astype(float) + (macd_rel < 0.0).astype(float) + (ms_sign < 0.0).astype(float) + (angle_sign < 0.0).astype(float))
    alignment_fraction = ((pd.concat([pos_votes, neg_votes], axis=1).max(axis=1)) / 6.0).fillna(0.0)
    conflict_score = _clip01_series((pd.concat([pos_votes, neg_votes], axis=1).min(axis=1) / 3.0).fillna(0.0))
    dominant_dir = pd.Series(np.where(direction_evidence >= 2.5, 1.0, np.where(direction_evidence <= -2.5, -1.0, 0.0)), index=idx)
    persistence = ((dominant_dir != 0.0) & (dominant_dir == dominant_dir.shift(1))).astype(float).rolling(short_win, min_periods=min_short).mean().fillna(0.0)

    vidya_delta_pct = _safe_series(out, "vidya_delta_pct").fillna(0.0)
    delta_abs = vidya_delta_pct.abs()
    delta_base = _rolling_q(delta_abs, long_win, 0.65, min_long).where(lambda s: s.abs() > eps, float(max(5.0, thresholds.vidya_delta_support_pct)))
    delta_strength = _clip01_series((delta_abs / delta_base).replace([np.inf, -np.inf], np.nan).fillna(0.0))
    delta_sign = pd.Series(np.where(vidya_delta_pct > float(thresholds.vidya_delta_weak_pct), 1.0, np.where(vidya_delta_pct < -float(thresholds.vidya_delta_weak_pct), -1.0, 0.0)), index=idx)
    delta_agreement = pd.Series(np.where((dominant_dir != 0.0) & (delta_sign == dominant_dir), 1.0, np.where((dominant_dir != 0.0) & (delta_sign != 0.0) & (delta_sign != dominant_dir), -1.0, 0.0)), index=idx)
    delta_accel = vidya_delta_pct.diff().fillna(0.0)
    delta_accel_strength = _clip01_series((delta_accel.abs() / (_rolling_q(delta_accel.abs(), long_win, 0.75, min_long).where(lambda s: s.abs() > eps, 1.0))).replace([np.inf, -np.inf], np.nan).fillna(0.0))
    delta_divergence_strength = _clip01_series(pd.Series(np.where((dominant_dir != 0.0) & (delta_agreement < 0.0), delta_strength, np.where((dominant_dir != 0.0) & (delta_sign == 0.0), 0.35, 0.0)), index=idx))

    vol_med = volume_ser.rolling(long_win, min_periods=min_short).median().where(lambda s: s.abs() > eps)
    volume_ratio = (volume_ser / vol_med).replace([np.inf, -np.inf], np.nan)
    volume_support = _clip01_series(((volume_ratio - 1.0) / 1.0).fillna(0.0))
    volume_dryness = _clip01_series(((1.0 - volume_ratio) / 0.7).fillna(0.0))

    mid_distance = (close_ser - active_mid).abs()
    extension_ratio = (mid_distance / pd.concat([active_width, atr_eff], axis=1).max(axis=1).where(lambda s: s.abs() > eps, 1.0)).replace([np.inf, -np.inf], np.nan).fillna(0.0)
    extension_strength = _clip01_series(((extension_ratio - 0.65) / 0.90).fillna(0.0))
    trend_decel_strength = _clip01_series(pd.Series(np.where((active_slope_norm * slope_delta_norm) < 0.0, np.minimum(1.0, slope_delta_norm.abs() / (slope_abs + 0.05)), 0.0), index=idx).fillna(0.0))
    opp_wick = pd.Series(np.where(dominant_dir > 0.0, upper_wick / atr_eff, np.where(dominant_dir < 0.0, lower_wick / atr_eff, 0.0)), index=idx).replace([np.inf, -np.inf], np.nan).fillna(0.0)
    wick_q50 = _rolling_q(opp_wick, long_win, 0.50, min_long)
    wick_q85 = _rolling_q(opp_wick, long_win, 0.85, min_long)
    opp_wick_strength = _clip01_series(_ratio_to_window(opp_wick, wick_q50, wick_q85).fillna(0.0))
    no_progress = _clip01_series(pd.Series(np.where(volume_support > 0.15, np.maximum(0.0, 1.0 - (body / atr_eff).fillna(0.0) / 0.40), 0.0), index=idx))

    counter_pressure = pd.Series(np.zeros(len(out), dtype=float), index=idx)
    bullish_counter = ((delta_sign < 0.0).astype(float) + (ms_sign < 0.0).astype(float) + (macd_rel < 0.0).astype(float) + (close_ser < active_mid).astype(float)) / 4.0
    bearish_counter = ((delta_sign > 0.0).astype(float) + (ms_sign > 0.0).astype(float) + (macd_rel > 0.0).astype(float) + (close_ser > active_mid).astype(float)) / 4.0
    counter_pressure[dominant_dir > 0.0] = bullish_counter[dominant_dir > 0.0]
    counter_pressure[dominant_dir < 0.0] = bearish_counter[dominant_dir < 0.0]

    envelope_high = high_ser.rolling(compression_win, min_periods=min_compression).max()
    envelope_low = low_ser.rolling(compression_win, min_periods=min_compression).min()
    envelope_width = (envelope_high - envelope_low).abs()
    envelope_width_atr = (envelope_width / atr_eff.where(atr_eff.abs() > eps, np.nan)).replace([np.inf, -np.inf], np.nan).fillna(0.0)
    envelope_prev = envelope_width.shift(pressure_win).where(lambda s: s.abs() > eps, envelope_width)
    envelope_scale = pd.concat([envelope_prev, envelope_width, atr_eff], axis=1).max(axis=1).where(lambda s: s.abs() > eps, 1.0)
    range_contraction = _clip01_series(((envelope_prev - envelope_width) / envelope_scale).replace([np.inf, -np.inf], np.nan).fillna(0.0))

    recent_high_box = high_ser.rolling(pressure_win, min_periods=max(6, pressure_win // 2)).max()
    recent_low_box = low_ser.rolling(pressure_win, min_periods=max(6, pressure_win // 2)).min()
    prior_high_box = recent_high_box.shift(pressure_win)
    prior_low_box = recent_low_box.shift(pressure_win)
    upper_squeeze = ((prior_high_box - recent_high_box) / envelope_scale).replace([np.inf, -np.inf], np.nan).fillna(0.0).clip(lower=0.0)
    lower_squeeze = ((recent_low_box - prior_low_box) / envelope_scale).replace([np.inf, -np.inf], np.nan).fillna(0.0).clip(lower=0.0)
    swing_squeeze = _clip01_series(((upper_squeeze + lower_squeeze) / 1.10).fillna(0.0))

    width_tightness_long = width_tightness.rolling(compression_win, min_periods=min_compression).mean().fillna(width_tightness)
    containment_long = containment_rate.rolling(compression_win, min_periods=min_compression).mean().fillna(containment_rate)
    rotationality_long = rotationality.rolling(compression_win, min_periods=min_compression).mean().fillna(rotationality)
    slope_flatness_long = slope_flatness.rolling(compression_win, min_periods=min_compression).mean().fillna(slope_flatness)
    breakout_fail_long = breakout_fail.rolling(pressure_win, min_periods=min_short).mean().fillna(breakout_fail)
    delta_strength_long = delta_strength.rolling(pressure_win, min_periods=min_short).mean().fillna(delta_strength)
    delta_signed_long = vidya_delta_pct.rolling(pressure_win, min_periods=min_short).mean().fillna(0.0)
    delta_directional_long = ((delta_agreement > 0.0).astype(float) * delta_strength).rolling(pressure_win, min_periods=min_short).mean().fillna(0.0)
    volume_support_long = volume_support.rolling(pressure_win, min_periods=min_short).mean().fillna(volume_support)
    volume_dryness_long = volume_dryness.rolling(pressure_win, min_periods=min_short).mean().fillna(volume_dryness)

    active_span = (active_upper - active_lower).abs().where(lambda s: s.abs() > eps, envelope_width).where(lambda s: s.abs() > eps, atr_eff)
    close_loc = ((close_ser - active_lower) / active_span).replace([np.inf, -np.inf], np.nan).clip(lower=0.0, upper=1.0).fillna(0.5)
    upper_pressure = ((close_loc >= 0.62) & (close_ser <= active_upper.fillna(np.inf))).astype(float).rolling(pressure_win, min_periods=min_short).mean().fillna(0.0)
    lower_pressure = ((close_loc <= 0.38) & (close_ser >= active_lower.fillna(-np.inf))).astype(float).rolling(pressure_win, min_periods=min_short).mean().fillna(0.0)
    pressure_long = _clip01_series(pd.Series(np.where(delta_signed_long > float(thresholds.vidya_delta_support_pct) * 0.55, upper_pressure, np.where(delta_signed_long < -float(thresholds.vidya_delta_support_pct) * 0.55, lower_pressure, 0.0)), index=idx) * (0.55 + 0.45 * delta_directional_long)).fillna(0.0)

    compression_maturity = (100.0 * (
        0.18 * width_tightness_long
        + 0.18 * containment_long
        + 0.14 * rotationality_long
        + 0.14 * slope_flatness_long
        + 0.16 * range_contraction
        + 0.14 * swing_squeeze
        + 0.06 * breakout_fail_long
    )).rolling(3, min_periods=1).mean().clip(lower=0.0, upper=100.0)

    compressed_high = envelope_high.shift(1)
    compressed_low = envelope_low.shift(1)
    range_break_side = pd.Series(np.where(close_ser > compressed_high, 1.0, np.where(close_ser < compressed_low, -1.0, 0.0)), index=idx)
    break_boundary = pd.Series(np.where(range_break_side > 0.0, compressed_high, np.where(range_break_side < 0.0, compressed_low, np.nan)), index=idx)
    range_break_dist = ((close_ser - break_boundary).abs() / atr_eff).replace([np.inf, -np.inf], np.nan).fillna(0.0)
    range_break_progress = _clip01_series(((range_break_dist - 0.10) / 0.90).fillna(0.0))
    range_break_hold = ((range_break_side != 0.0) & (range_break_side == range_break_side.shift(1))).astype(float).rolling(breakout_watch_win, min_periods=1).mean().fillna(0.0)
    delta_break_support = _clip01_series(pd.Series(np.where((range_break_side != 0.0) & (np.sign(delta_signed_long) == range_break_side), delta_strength_long, 0.0), index=idx).fillna(0.0))
    volume_break_support = _clip01_series(pd.Series(np.where(range_break_side != 0.0, volume_support_long, 0.0), index=idx).fillna(0.0))
    breakout_readiness = _clip01_series((0.46 * (compression_maturity / 100.0) + 0.24 * pressure_long + 0.18 * range_contraction + 0.12 * (1.0 - breakout_fail_long)).fillna(0.0))
    breakout_confirmation = _clip01_series((
        0.28 * (compression_maturity.shift(1).fillna(compression_maturity) / 100.0)
        + 0.22 * (range_break_side != 0.0).astype(float)
        + 0.18 * range_break_hold
        + 0.12 * width_expansion
        + 0.10 * delta_break_support
        + 0.10 * volume_break_support
    ).fillna(0.0))

    breakout_quality = _clip01_series((0.38 * breakout_hold + 0.17 * slope_strength + 0.10 * _clip01_series(pd.Series(np.where(delta_agreement > 0.0, delta_strength, 0.0), index=idx)) + 0.08 * volume_support + 0.27 * breakout_confirmation - 0.25 * breakout_fail).fillna(0.0))

    compression_base = (100.0 * (0.24 * width_tightness + 0.18 * slope_flatness + 0.16 * containment_rate + 0.12 * rotationality + 0.10 * breakout_fail + 0.08 * volume_dryness + 0.12 * (1.0 - persistence))).rolling(3, min_periods=1).mean().clip(lower=0.0, upper=100.0)
    compression_score = (100.0 * (0.58 * (compression_base / 100.0) + 0.30 * (compression_maturity / 100.0) + 0.12 * (1.0 - breakout_confirmation))).rolling(3, min_periods=1).mean().clip(lower=0.0, upper=100.0)
    trend_score = (100.0 * (0.23 * direction_abs_strength + 0.17 * slope_strength + 0.15 * persistence + 0.14 * breakout_quality + 0.09 * _clip01_series(pd.Series(np.where(delta_agreement > 0.0, delta_strength, 0.0), index=idx)) + 0.07 * volume_support + 0.07 * width_expansion + 0.08 * breakout_confirmation)).rolling(3, min_periods=1).mean().clip(lower=0.0, upper=100.0)
    transition_mixed = _clip01_series(1.0 - (2.0 * (containment_rate - 0.5).abs()).clip(upper=1.0))
    transition_base = (100.0 * (0.24 * slope_accel_strength + 0.18 * width_change_strength + 0.18 * conflict_score + 0.14 * transition_mixed + 0.08 * (1.0 - direction_abs_strength) + 0.08 * delta_accel_strength + 0.10 * (1.0 - breakout_quality))).rolling(3, min_periods=1).mean().clip(lower=0.0, upper=100.0)
    transition_decay = _clip01_series(((compression_maturity / 100.0) * (1.0 - breakout_confirmation) * (0.60 * containment_long + 0.40 * range_contraction)).fillna(0.0))
    transition_score = (transition_base * (1.0 - 0.60 * transition_decay) + 100.0 * 0.18 * breakout_confirmation).rolling(3, min_periods=1).mean().clip(lower=0.0, upper=100.0)
    exhaustion_score = (100.0 * (0.22 * extension_strength + 0.22 * trend_decel_strength + 0.17 * opp_wick_strength + 0.15 * breakout_fail + 0.14 * delta_divergence_strength + 0.10 * no_progress)).rolling(3, min_periods=1).mean().clip(lower=0.0, upper=100.0)
    pullback_score = (100.0 * (0.30 * direction_abs_strength + 0.24 * containment_rate + 0.20 * counter_pressure + 0.14 * slope_flatness + 0.12 * (1.0 - breakout_quality))).rolling(3, min_periods=1).mean().clip(lower=0.0, upper=100.0)

    pressure_bias = pd.Series(np.where((compression_maturity >= 58.0) & (pressure_long >= 0.42) & (delta_directional_long >= 0.42) & (delta_signed_long > float(thresholds.vidya_delta_support_pct) * 0.55), "LONG", np.where((compression_maturity >= 58.0) & (pressure_long >= 0.42) & (delta_directional_long >= 0.42) & (delta_signed_long < -float(thresholds.vidya_delta_support_pct) * 0.55), "SHORT", "NEUTRAL")), index=idx)

    bull_close = (close_ser > open_ser).astype(float)
    bear_close = (close_ser < open_ser).astype(float)
    higher_close = (close_ser > close_ser.shift(1)).astype(float)
    lower_close = (close_ser < close_ser.shift(1)).astype(float)
    higher_high = (high_ser > high_ser.shift(1)).astype(float)
    lower_low = (low_ser < low_ser.shift(1)).astype(float)
    body_atr = (body / atr_eff.where(atr_eff.abs() > eps, np.nan)).replace([np.inf, -np.inf], np.nan).fillna(0.0)
    pos_body_atr = pd.Series(np.where(close_ser >= open_ser, body_atr, 0.0), index=idx)
    neg_body_atr = pd.Series(np.where(close_ser <= open_ser, body_atr, 0.0), index=idx)
    bull_count3 = bull_close.rolling(3, min_periods=1).sum().fillna(0.0)
    bear_count3 = bear_close.rolling(3, min_periods=1).sum().fillna(0.0)
    higher_close3 = higher_close.rolling(3, min_periods=1).sum().fillna(0.0)
    lower_close3 = lower_close.rolling(3, min_periods=1).sum().fillna(0.0)
    higher_high3 = higher_high.rolling(3, min_periods=1).sum().fillna(0.0)
    lower_low3 = lower_low.rolling(3, min_periods=1).sum().fillna(0.0)
    mean_pos_body3 = pos_body_atr.rolling(3, min_periods=1).mean().fillna(0.0)
    mean_neg_body3 = neg_body_atr.rolling(3, min_periods=1).mean().fillna(0.0)
    max_body3 = body_atr.rolling(3, min_periods=1).max().fillna(0.0)
    max_body2_prev = body_atr.shift(1).rolling(2, min_periods=1).max().fillna(0.0)
    breakout_drive_long = _clip01_series((0.22 * (bull_count3 / 3.0) + 0.22 * (higher_close3 / 3.0) + 0.16 * (higher_high3 / 3.0) + 0.18 * np.clip(mean_pos_body3 / 0.70, 0.0, 1.0) + 0.10 * volume_support.rolling(3, min_periods=1).mean().fillna(volume_support) + 0.12 * np.clip((close_loc - 0.50) / 0.40, 0.0, 1.0)).fillna(0.0))
    breakout_drive_short = _clip01_series((0.22 * (bear_count3 / 3.0) + 0.22 * (lower_close3 / 3.0) + 0.16 * (lower_low3 / 3.0) + 0.18 * np.clip(mean_neg_body3 / 0.70, 0.0, 1.0) + 0.10 * volume_support.rolling(3, min_periods=1).mean().fillna(volume_support) + 0.12 * np.clip((0.50 - close_loc) / 0.40, 0.0, 1.0)).fillna(0.0))
    breakout_single_spike_long = ((body_atr >= 1.10) & (max_body2_prev <= 0.55) & (bull_count3 <= 2.0) & (higher_close3 <= 2.0)).fillna(False)
    breakout_single_spike_short = ((body_atr >= 1.10) & (max_body2_prev <= 0.55) & (bear_count3 <= 2.0) & (lower_close3 <= 2.0)).fillna(False)
    breakout_sequence_long = ((breakout_drive_long >= 0.58) & (bull_count3 >= 2.0) & (higher_close3 >= 2.0) & (higher_high3 >= 2.0) & (~breakout_single_spike_long)).fillna(False)
    breakout_sequence_short = ((breakout_drive_short >= 0.58) & (bear_count3 >= 2.0) & (lower_close3 >= 2.0) & (lower_low3 >= 2.0) & (~breakout_single_spike_short)).fillna(False)

    precomputed_columns = {
        "price": close_float,
        "close": close_float,
        "open": open_float,
        "high": high_float,
        "low": low_float,
        "volume": volume_float,
        "vidya_slope": slope_float,
        "vidya_angle_deg": angle_raw_ser,
        "market_bb_middle": market_bb_middle,
        "market_bb_upper": market_bb_upper,
        "market_bb_lower": market_bb_lower,
        "market_bb_inside_kc": market_bb_inside_kc,
        "market_bb_squeeze_strength": market_bb_squeeze_strength,
        "market_bb_expand_strength": market_bb_expand_strength,
        "market_bb_width_atr": market_bb_width_atr,
        "market_kc_width_atr": market_kc_width_atr,
        "market_bb_breakout_long": market_bb_breakout_long,
        "market_bb_breakout_short": market_bb_breakout_short,
        "market_bb_band_walk_long": market_bb_band_walk_long,
        "market_bb_band_walk_short": market_bb_band_walk_short,
        "market_kc_middle": market_kc_middle,
        "market_kc_upper": market_kc_upper,
        "market_kc_lower": market_kc_lower,
        "market_vwap": market_vwap,
        "market_vwap_distance_atr": market_vwap_distance_atr,
        "market_vwap_bias": market_vwap_bias,
        "market_vwap_reclaim_long": market_vwap_reclaim_long,
        "market_vwap_reclaim_short": market_vwap_reclaim_short,
        "market_vwap_reject_long": market_vwap_reject_long,
        "market_vwap_reject_short": market_vwap_reject_short,
        "vidya_angle_deg_norm": angle_norm_ser,
        "vidya_angle_state": pd.Series(angle_state, index=idx, dtype=object),
        "vidya_angle_accel": pd.Series(angle_accel, index=idx, dtype=object),
        "market_width_atr": width_atr.astype(float),
        "market_width_tightness": width_tightness.astype(float),
        "market_width_expansion": width_expansion.astype(float),
        "market_containment_rate": containment_rate.astype(float),
        "market_rotationality": rotationality.astype(float),
        "market_breakout_quality": breakout_quality.astype(float),
        "market_breakout_fail_rate": breakout_fail.astype(float),
        "market_breakout_readiness": breakout_readiness.astype(float),
        "market_breakout_confirmation": breakout_confirmation.astype(float),
        "market_range_contraction": range_contraction.astype(float),
        "market_swing_squeeze": swing_squeeze.astype(float),
        "market_envelope_width_atr": envelope_width_atr.astype(float),
        "market_breakout_drive_long": breakout_drive_long.astype(float),
        "market_breakout_drive_short": breakout_drive_short.astype(float),
        "market_breakout_single_spike_long": breakout_single_spike_long.astype(bool),
        "market_breakout_single_spike_short": breakout_single_spike_short.astype(bool),
        "market_breakout_sequence_long": breakout_sequence_long.astype(bool),
        "market_breakout_sequence_short": breakout_sequence_short.astype(bool),
        "market_compression_maturity": compression_maturity.astype(float),
        "market_slope_strength": slope_strength.astype(float),
        "market_slope_flatness": slope_flatness.astype(float),
        "market_slope_accel_strength": slope_accel_strength.astype(float),
        "market_direction_evidence": direction_evidence.astype(float),
        "market_alignment_fraction": alignment_fraction.astype(float),
        "market_conflict_score": conflict_score.astype(float),
        "market_delta_strength": delta_strength.astype(float),
        "market_delta_agreement": delta_agreement.astype(float),
        "market_volume_ratio": volume_ratio.astype(float),
        "market_volume_support": volume_support.astype(float),
        "market_no_progress": no_progress.astype(float),
        "market_trend_decel_strength": trend_decel_strength.astype(float),
        "market_extension_strength": extension_strength.astype(float),
        "market_pullback_score": pullback_score.astype(float),
        "market_compression_score": compression_score.astype(float),
        "market_transition_score": transition_score.astype(float),
        "market_trend_score": trend_score.astype(float),
        "market_exhaustion_score": exhaustion_score.astype(float),
        "market_pressure_bias": pressure_bias.astype(object),
    }
    out = _attach_batched_columns(out, precomputed_columns)
    market_input_columns = _market_state_row_input_columns(precomputed_columns)
    market_input_frame = out.reindex(columns=market_input_columns)

    labels = []
    biases = []
    phases = []
    confidences = []
    ages = []
    dir_scores = []
    energy_scores = []
    align_scores = []
    gap_norms = []
    textures = []
    summary_shorts = []
    summary_longs = []
    breakout_states = []
    breakout_biases = []
    breakout_pretties = []
    breakout_scores = []
    breakout_trigger_ready = []
    breakout_notes = []
    structure_flags = []
    structure_passes = []
    structure_notes = []
    structure_contexts = []
    structure_bases = []
    structure_pivot_biases = []
    structure_last_highs = []
    structure_last_lows = []
    structure_pivot_counts = []
    structure_swing_hhs = []
    structure_swing_hls = []
    structure_swing_lhs = []
    structure_swing_lls = []
    pa_vol_flags = []
    pa_vol_passes = []
    pa_vol_notes = []
    pa_patterns = []
    pa_body_fracs = []
    pa_close_fracs = []
    pa_upper_wick_fracs = []
    pa_lower_wick_fracs = []
    pa_range_atrs = []
    pa_volume_ratios = []
    pa_higher_highs = []
    pa_higher_lows = []
    pa_lower_highs = []
    pa_lower_lows = []
    pa_inside_bars = []
    pa_outside_bars = []
    pa_phase2_contexts = []
    pa_phase2_notes = []
    pa_phase3_contexts = []
    pa_phase3_notes = []
    pa_phase4_contexts = []
    pa_phase4_notes = []
    pa_phase5_contexts = []
    pa_phase5_notes = []
    pa_phase6_contexts = []
    pa_phase6_notes = []
    pa_trap_flags = []
    pa_trap_notes = []
    pa_strict_flags = []
    pa_strict_passes = []
    pa_strict_notes = []
    pa_quality_scores = []
    pa_engulf_longs = []
    pa_engulf_shorts = []
    pa_retest_holds = []
    pa_retest_fails = []
    pa_sequence_longs = []
    pa_sequence_shorts = []
    pa_followthrough_passes = []
    pa_followthrough_fails = []
    pa_stall_after_impulses = []
    pa_effort_results = []
    pa_outside_continues = []
    pa_acceptance_drives = []
    pa_sweep_fails = []
    pa_reversal_risks = []
    pa_constructive_pauses = []
    pa_rejection_clusters = []
    pa_ladder_fails = []
    pa_sequence_exhausts = []
    pa_swing_continues = []
    value_contexts = []
    value_notes = []
    bb_middles = []
    bb_uppers = []
    bb_lowers = []
    bb_inside_flags = []
    bb_squeeze_strengths = []
    bb_expand_strengths = []
    bb_walk_longs = []
    bb_walk_shorts = []
    bb_break_longs = []
    bb_break_shorts = []
    kc_middles = []
    kc_uppers = []
    kc_lowers = []
    vwap_vals = []
    vwap_dists = []
    vwap_biases = []
    vwap_reclaim_longs = []
    vwap_reclaim_shorts = []
    vwap_reject_longs = []
    vwap_reject_shorts = []
    warning_notes = []
    trend_states = []
    structure_states = []
    phase_states = []
    breakout_signals = []
    volume_contexts = []
    trend_ages = []
    phase_ages = []
    breakout_signal_ages = []
    protected_highs = []
    protected_lows = []
    context_notes = []
    context_reason_texts = []
    prev = None
    for values in market_input_frame.itertuples(index=False, name=None):
        state = compute_market_state_snapshot(dict(zip(market_input_columns, values)), prev, thresholds=thresholds)
        labels.append(state.get("market_state"))
        biases.append(state.get("market_bias"))
        phases.append(state.get("market_phase"))
        confidences.append(_safe_float_or_nan(state.get("market_confidence", np.nan)))
        ages.append(_safe_float_or_nan(state.get("market_state_age", np.nan)))
        dir_scores.append(_safe_float_or_nan(state.get("market_direction_score", np.nan)))
        energy_scores.append(_safe_float_or_nan(state.get("market_energy_score", np.nan)))
        align_scores.append(_safe_float_or_nan(state.get("market_alignment_bonus", np.nan)))
        gap_norms.append(_safe_float_or_nan(state.get("market_macd_gap_norm", np.nan)))
        textures.append(state.get("market_texture"))
        summary_shorts.append(state.get("market_summary_short"))
        summary_longs.append(state.get("market_summary_long"))
        breakout_states.append(state.get("market_breakout_state"))
        breakout_biases.append(state.get("market_breakout_bias"))
        breakout_pretties.append(state.get("market_breakout_pretty"))
        breakout_scores.append(_safe_float_or_nan(state.get("market_breakout_score", np.nan)))
        breakout_trigger_ready.append(bool(state.get("market_breakout_trigger_ready", False)))
        breakout_notes.append(state.get("market_breakout_note"))
        structure_flags.append(state.get("market_structure_flag"))
        structure_passes.append(bool(state.get("market_structure_pass", False)))
        structure_notes.append(state.get("market_structure_note"))
        structure_contexts.append(state.get("market_structure_context"))
        structure_bases.append(state.get("market_structure_basis"))
        structure_pivot_biases.append(state.get("market_structure_pivot_bias"))
        structure_last_highs.append(_safe_float_or_nan(state.get("market_structure_last_high", np.nan)))
        structure_last_lows.append(_safe_float_or_nan(state.get("market_structure_last_low", np.nan)))
        structure_pivot_counts.append(_safe_float_or_nan(state.get("market_structure_pivot_count", np.nan)))
        structure_swing_hhs.append(bool(state.get("market_structure_swing_hh", False)))
        structure_swing_hls.append(bool(state.get("market_structure_swing_hl", False)))
        structure_swing_lhs.append(bool(state.get("market_structure_swing_lh", False)))
        structure_swing_lls.append(bool(state.get("market_structure_swing_ll", False)))
        pa_vol_flags.append(state.get("market_pa_vol_flag"))
        pa_vol_passes.append(bool(state.get("market_pa_vol_pass", False)))
        pa_vol_notes.append(state.get("market_pa_vol_note"))
        pa_patterns.append(state.get("market_pa_pattern"))
        pa_body_fracs.append(_safe_float_or_nan(state.get("market_pa_body_frac", np.nan)))
        pa_close_fracs.append(_safe_float_or_nan(state.get("market_pa_close_frac", np.nan)))
        pa_upper_wick_fracs.append(_safe_float_or_nan(state.get("market_pa_upper_wick_frac", np.nan)))
        pa_lower_wick_fracs.append(_safe_float_or_nan(state.get("market_pa_lower_wick_frac", np.nan)))
        pa_range_atrs.append(_safe_float_or_nan(state.get("market_pa_range_atr", np.nan)))
        pa_volume_ratios.append(_safe_float_or_nan(state.get("market_pa_volume_ratio", np.nan)))
        pa_higher_highs.append(bool(state.get("market_pa_higher_high", False)))
        pa_higher_lows.append(bool(state.get("market_pa_higher_low", False)))
        pa_lower_highs.append(bool(state.get("market_pa_lower_high", False)))
        pa_lower_lows.append(bool(state.get("market_pa_lower_low", False)))
        pa_inside_bars.append(bool(state.get("market_pa_inside_bar", False)))
        pa_outside_bars.append(bool(state.get("market_pa_outside_bar", False)))
        pa_phase2_contexts.append(state.get("market_pa_phase2_context"))
        pa_phase2_notes.append(state.get("market_pa_phase2_note"))
        pa_phase3_contexts.append(state.get("market_pa_phase3_context"))
        pa_phase3_notes.append(state.get("market_pa_phase3_note"))
        pa_phase4_contexts.append(state.get("market_pa_phase4_context"))
        pa_phase4_notes.append(state.get("market_pa_phase4_note"))
        pa_phase5_contexts.append(state.get("market_pa_phase5_context"))
        pa_phase5_notes.append(state.get("market_pa_phase5_note"))
        pa_phase6_contexts.append(state.get("market_pa_phase6_context"))
        pa_phase6_notes.append(state.get("market_pa_phase6_note"))
        pa_trap_flags.append(state.get("market_pa_trap_flag"))
        pa_trap_notes.append(state.get("market_pa_trap_note"))
        pa_strict_flags.append(state.get("market_pa_strict_flag"))
        pa_strict_passes.append(bool(state.get("market_pa_strict_pass", False)))
        pa_strict_notes.append(state.get("market_pa_strict_note"))
        pa_quality_scores.append(_safe_float_or_nan(state.get("market_pa_quality_score", np.nan)))
        pa_engulf_longs.append(bool(state.get("market_pa_engulf_long", False)))
        pa_engulf_shorts.append(bool(state.get("market_pa_engulf_short", False)))
        pa_retest_holds.append(bool(state.get("market_pa_retest_hold", False)))
        pa_retest_fails.append(bool(state.get("market_pa_retest_fail", False)))
        pa_sequence_longs.append(bool(state.get("market_pa_sequence_long", False)))
        pa_sequence_shorts.append(bool(state.get("market_pa_sequence_short", False)))
        pa_followthrough_passes.append(bool(state.get("market_pa_followthrough_pass", False)))
        pa_followthrough_fails.append(bool(state.get("market_pa_followthrough_fail", False)))
        pa_stall_after_impulses.append(bool(state.get("market_pa_stall_after_impulse", False)))
        pa_effort_results.append(state.get("market_pa_effort_result"))
        pa_outside_continues.append(bool(state.get("market_pa_outside_continue", False)))
        pa_acceptance_drives.append(bool(state.get("market_pa_acceptance_drive", False)))
        pa_sweep_fails.append(bool(state.get("market_pa_sweep_fail", False)))
        pa_reversal_risks.append(bool(state.get("market_pa_reversal_risk", False)))
        pa_constructive_pauses.append(bool(state.get("market_pa_constructive_pause", False)))
        pa_rejection_clusters.append(bool(state.get("market_pa_rejection_cluster", False)))
        pa_ladder_fails.append(bool(state.get("market_pa_ladder_fail", False)))
        pa_sequence_exhausts.append(bool(state.get("market_pa_sequence_exhaust", False)))
        pa_swing_continues.append(bool(state.get("market_pa_swing_continue", False)))
        value_contexts.append(state.get("market_value_context"))
        value_notes.append(state.get("market_value_note"))
        bb_middles.append(_safe_float_or_nan(state.get("market_bb_middle", np.nan)))
        bb_uppers.append(_safe_float_or_nan(state.get("market_bb_upper", np.nan)))
        bb_lowers.append(_safe_float_or_nan(state.get("market_bb_lower", np.nan)))
        bb_inside_flags.append(bool(state.get("market_bb_inside_kc", False)))
        bb_squeeze_strengths.append(_safe_float_or_nan(state.get("market_bb_squeeze_strength", np.nan)))
        bb_expand_strengths.append(_safe_float_or_nan(state.get("market_bb_expand_strength", np.nan)))
        bb_walk_longs.append(bool(state.get("market_bb_band_walk_long", False)))
        bb_walk_shorts.append(bool(state.get("market_bb_band_walk_short", False)))
        bb_break_longs.append(bool(state.get("market_bb_breakout_long", False)))
        bb_break_shorts.append(bool(state.get("market_bb_breakout_short", False)))
        kc_middles.append(_safe_float_or_nan(state.get("market_kc_middle", np.nan)))
        kc_uppers.append(_safe_float_or_nan(state.get("market_kc_upper", np.nan)))
        kc_lowers.append(_safe_float_or_nan(state.get("market_kc_lower", np.nan)))
        vwap_vals.append(_safe_float_or_nan(state.get("market_vwap", np.nan)))
        vwap_dists.append(_safe_float_or_nan(state.get("market_vwap_distance_atr", np.nan)))
        vwap_biases.append(state.get("market_vwap_bias"))
        vwap_reclaim_longs.append(bool(state.get("market_vwap_reclaim_long", False)))
        vwap_reclaim_shorts.append(bool(state.get("market_vwap_reclaim_short", False)))
        vwap_reject_longs.append(bool(state.get("market_vwap_reject_long", False)))
        vwap_reject_shorts.append(bool(state.get("market_vwap_reject_short", False)))
        warning_notes.append(state.get("market_warning_note"))
        trend_states.append(state.get("market_trend_state"))
        structure_states.append(state.get("market_structure_state"))
        phase_states.append(state.get("market_phase_state"))
        breakout_signals.append(state.get("market_breakout_signal"))
        volume_contexts.append(state.get("market_volume_context"))
        trend_ages.append(_safe_float_or_nan(state.get("market_trend_age", np.nan)))
        phase_ages.append(_safe_float_or_nan(state.get("market_phase_age", np.nan)))
        breakout_signal_ages.append(_safe_float_or_nan(state.get("market_breakout_signal_age", np.nan)))
        protected_highs.append(_safe_float_or_nan(state.get("market_protected_high", np.nan)))
        protected_lows.append(_safe_float_or_nan(state.get("market_protected_low", np.nan)))
        context_notes.append(state.get("market_context_note"))
        context_reason_texts.append(state.get("market_context_reasons"))
        prev = state

    batched_columns = {
        "market_state": pd.Series(labels, index=idx, dtype=object),
        "market_bias": pd.Series(biases, index=idx, dtype=object),
        "market_phase": pd.Series(phases, index=idx, dtype=object),
        "market_confidence": pd.Series(confidences, index=idx, dtype=float),
        "market_state_age": pd.Series(ages, index=idx, dtype=float),
        "market_direction_score": pd.Series(dir_scores, index=idx, dtype=float),
        "market_energy_score": pd.Series(energy_scores, index=idx, dtype=float),
        "market_alignment_bonus": pd.Series(align_scores, index=idx, dtype=float),
        "market_macd_gap_norm": pd.Series(gap_norms, index=idx, dtype=float),
        "market_texture": pd.Series(textures, index=idx, dtype=object),
        "market_summary_short": pd.Series(summary_shorts, index=idx, dtype=object),
        "market_summary_long": pd.Series(summary_longs, index=idx, dtype=object),
        "market_breakout_state": pd.Series(breakout_states, index=idx, dtype=object),
        "market_breakout_bias": pd.Series(breakout_biases, index=idx, dtype=object),
        "market_breakout_pretty": pd.Series(breakout_pretties, index=idx, dtype=object),
        "market_breakout_score": pd.Series(breakout_scores, index=idx, dtype=float),
        "market_breakout_trigger_ready": pd.Series(breakout_trigger_ready, index=idx, dtype=bool),
        "market_breakout_note": pd.Series(breakout_notes, index=idx, dtype=object),
        "market_structure_flag": pd.Series(structure_flags, index=idx, dtype=object),
        "market_structure_pass": pd.Series(structure_passes, index=idx, dtype=bool),
        "market_structure_note": pd.Series(structure_notes, index=idx, dtype=object),
        "market_structure_context": pd.Series(structure_contexts, index=idx, dtype=object),
        "market_structure_basis": pd.Series(structure_bases, index=idx, dtype=object),
        "market_structure_pivot_bias": pd.Series(structure_pivot_biases, index=idx, dtype=object),
        "market_structure_last_high": pd.Series(structure_last_highs, index=idx, dtype=float),
        "market_structure_last_low": pd.Series(structure_last_lows, index=idx, dtype=float),
        "market_structure_pivot_count": pd.Series(structure_pivot_counts, index=idx, dtype=float),
        "market_structure_swing_hh": pd.Series(structure_swing_hhs, index=idx, dtype=bool),
        "market_structure_swing_hl": pd.Series(structure_swing_hls, index=idx, dtype=bool),
        "market_structure_swing_lh": pd.Series(structure_swing_lhs, index=idx, dtype=bool),
        "market_structure_swing_ll": pd.Series(structure_swing_lls, index=idx, dtype=bool),
        "market_pa_vol_flag": pd.Series(pa_vol_flags, index=idx, dtype=object),
        "market_pa_vol_pass": pd.Series(pa_vol_passes, index=idx, dtype=bool),
        "market_pa_vol_note": pd.Series(pa_vol_notes, index=idx, dtype=object),
        "market_pa_pattern": pd.Series(pa_patterns, index=idx, dtype=object),
        "market_pa_body_frac": pd.Series(pa_body_fracs, index=idx, dtype=float),
        "market_pa_close_frac": pd.Series(pa_close_fracs, index=idx, dtype=float),
        "market_pa_upper_wick_frac": pd.Series(pa_upper_wick_fracs, index=idx, dtype=float),
        "market_pa_lower_wick_frac": pd.Series(pa_lower_wick_fracs, index=idx, dtype=float),
        "market_pa_range_atr": pd.Series(pa_range_atrs, index=idx, dtype=float),
        "market_pa_volume_ratio": pd.Series(pa_volume_ratios, index=idx, dtype=float),
        "market_pa_higher_high": pd.Series(pa_higher_highs, index=idx, dtype=bool),
        "market_pa_higher_low": pd.Series(pa_higher_lows, index=idx, dtype=bool),
        "market_pa_lower_high": pd.Series(pa_lower_highs, index=idx, dtype=bool),
        "market_pa_lower_low": pd.Series(pa_lower_lows, index=idx, dtype=bool),
        "market_pa_inside_bar": pd.Series(pa_inside_bars, index=idx, dtype=bool),
        "market_pa_outside_bar": pd.Series(pa_outside_bars, index=idx, dtype=bool),
        "market_pa_phase2_context": pd.Series(pa_phase2_contexts, index=idx, dtype=object),
        "market_pa_phase2_note": pd.Series(pa_phase2_notes, index=idx, dtype=object),
        "market_pa_phase3_context": pd.Series(pa_phase3_contexts, index=idx, dtype=object),
        "market_pa_phase3_note": pd.Series(pa_phase3_notes, index=idx, dtype=object),
        "market_pa_phase4_context": pd.Series(pa_phase4_contexts, index=idx, dtype=object),
        "market_pa_phase4_note": pd.Series(pa_phase4_notes, index=idx, dtype=object),
        "market_pa_phase5_context": pd.Series(pa_phase5_contexts, index=idx, dtype=object),
        "market_pa_phase5_note": pd.Series(pa_phase5_notes, index=idx, dtype=object),
        "market_pa_phase6_context": pd.Series(pa_phase6_contexts, index=idx, dtype=object),
        "market_pa_phase6_note": pd.Series(pa_phase6_notes, index=idx, dtype=object),
        "market_pa_trap_flag": pd.Series(pa_trap_flags, index=idx, dtype=object),
        "market_pa_trap_note": pd.Series(pa_trap_notes, index=idx, dtype=object),
        "market_pa_strict_flag": pd.Series(pa_strict_flags, index=idx, dtype=object),
        "market_pa_strict_pass": pd.Series(pa_strict_passes, index=idx, dtype=bool),
        "market_pa_strict_note": pd.Series(pa_strict_notes, index=idx, dtype=object),
        "market_pa_quality_score": pd.Series(pa_quality_scores, index=idx, dtype=float),
        "market_pa_engulf_long": pd.Series(pa_engulf_longs, index=idx, dtype=bool),
        "market_pa_engulf_short": pd.Series(pa_engulf_shorts, index=idx, dtype=bool),
        "market_pa_retest_hold": pd.Series(pa_retest_holds, index=idx, dtype=bool),
        "market_pa_retest_fail": pd.Series(pa_retest_fails, index=idx, dtype=bool),
        "market_pa_sequence_long": pd.Series(pa_sequence_longs, index=idx, dtype=bool),
        "market_pa_sequence_short": pd.Series(pa_sequence_shorts, index=idx, dtype=bool),
        "market_pa_followthrough_pass": pd.Series(pa_followthrough_passes, index=idx, dtype=bool),
        "market_pa_followthrough_fail": pd.Series(pa_followthrough_fails, index=idx, dtype=bool),
        "market_pa_stall_after_impulse": pd.Series(pa_stall_after_impulses, index=idx, dtype=bool),
        "market_pa_effort_result": pd.Series(pa_effort_results, index=idx, dtype=object),
        "market_pa_outside_continue": pd.Series(pa_outside_continues, index=idx, dtype=bool),
        "market_pa_acceptance_drive": pd.Series(pa_acceptance_drives, index=idx, dtype=bool),
        "market_pa_sweep_fail": pd.Series(pa_sweep_fails, index=idx, dtype=bool),
        "market_pa_reversal_risk": pd.Series(pa_reversal_risks, index=idx, dtype=bool),
        "market_pa_constructive_pause": pd.Series(pa_constructive_pauses, index=idx, dtype=bool),
        "market_pa_rejection_cluster": pd.Series(pa_rejection_clusters, index=idx, dtype=bool),
        "market_pa_ladder_fail": pd.Series(pa_ladder_fails, index=idx, dtype=bool),
        "market_pa_sequence_exhaust": pd.Series(pa_sequence_exhausts, index=idx, dtype=bool),
        "market_pa_swing_continue": pd.Series(pa_swing_continues, index=idx, dtype=bool),
        "market_value_context": pd.Series(value_contexts, index=idx, dtype=object),
        "market_value_note": pd.Series(value_notes, index=idx, dtype=object),
        "market_bb_middle": pd.Series(bb_middles, index=idx, dtype=float),
        "market_bb_upper": pd.Series(bb_uppers, index=idx, dtype=float),
        "market_bb_lower": pd.Series(bb_lowers, index=idx, dtype=float),
        "market_bb_inside_kc": pd.Series(bb_inside_flags, index=idx, dtype=bool),
        "market_bb_squeeze_strength": pd.Series(bb_squeeze_strengths, index=idx, dtype=float),
        "market_bb_expand_strength": pd.Series(bb_expand_strengths, index=idx, dtype=float),
        "market_bb_band_walk_long": pd.Series(bb_walk_longs, index=idx, dtype=bool),
        "market_bb_band_walk_short": pd.Series(bb_walk_shorts, index=idx, dtype=bool),
        "market_bb_breakout_long": pd.Series(bb_break_longs, index=idx, dtype=bool),
        "market_bb_breakout_short": pd.Series(bb_break_shorts, index=idx, dtype=bool),
        "market_kc_middle": pd.Series(kc_middles, index=idx, dtype=float),
        "market_kc_upper": pd.Series(kc_uppers, index=idx, dtype=float),
        "market_kc_lower": pd.Series(kc_lowers, index=idx, dtype=float),
        "market_vwap": pd.Series(vwap_vals, index=idx, dtype=float),
        "market_vwap_distance_atr": pd.Series(vwap_dists, index=idx, dtype=float),
        "market_vwap_bias": pd.Series(vwap_biases, index=idx, dtype=object),
        "market_vwap_reclaim_long": pd.Series(vwap_reclaim_longs, index=idx, dtype=bool),
        "market_vwap_reclaim_short": pd.Series(vwap_reclaim_shorts, index=idx, dtype=bool),
        "market_vwap_reject_long": pd.Series(vwap_reject_longs, index=idx, dtype=bool),
        "market_vwap_reject_short": pd.Series(vwap_reject_shorts, index=idx, dtype=bool),
        "market_warning_note": pd.Series(warning_notes, index=idx, dtype=object),
        "market_trend_state": pd.Series(trend_states, index=idx, dtype=object),
        "market_structure_state": pd.Series(structure_states, index=idx, dtype=object),
        "market_phase_state": pd.Series(phase_states, index=idx, dtype=object),
        "market_breakout_signal": pd.Series(breakout_signals, index=idx, dtype=object),
        "market_volume_context": pd.Series(volume_contexts, index=idx, dtype=object),
        "market_trend_age": pd.Series(trend_ages, index=idx, dtype=float),
        "market_phase_age": pd.Series(phase_ages, index=idx, dtype=float),
        "market_breakout_signal_age": pd.Series(breakout_signal_ages, index=idx, dtype=float),
        "market_protected_high": pd.Series(protected_highs, index=idx, dtype=float),
        "market_protected_low": pd.Series(protected_lows, index=idx, dtype=float),
        "market_context_note": pd.Series(context_notes, index=idx, dtype=object),
        "market_context_reasons": pd.Series(context_reason_texts, index=idx, dtype=object),
    }
    return _attach_batched_columns(out, batched_columns)


def rebuild_market_state_columns(df: "pd.DataFrame", thresholds: Optional[MarketStateThresholds] = None) -> "pd.DataFrame":
    """Recompute derived VIDYA-angle and market-state columns on an existing stream dataframe."""
    return _augment_stream_with_market_state(df, thresholds=thresholds or DEFAULT_MARKET_STATE_THRESHOLDS)


def rebuild_market_state_streams(streams: Optional[Dict[str, "pd.DataFrame"]], thresholds: Optional[MarketStateThresholds] = None) -> Dict[str, "pd.DataFrame"]:
    out: Dict[str, pd.DataFrame] = {}
    th = thresholds or DEFAULT_MARKET_STATE_THRESHOLDS
    for tf, df in dict(streams or {}).items():
        try:
            out[str(tf)] = rebuild_market_state_columns(df, thresholds=th)
        except Exception:
            out[str(tf)] = df.copy() if isinstance(df, pd.DataFrame) else df
    return out


def summarize_market_state_streams(streams: Optional[Dict[str, "pd.DataFrame"]]) -> Dict[str, Dict[str, Any]]:
    summary: Dict[str, Dict[str, Any]] = {}
    for tf, df in dict(streams or {}).items():
        if not isinstance(df, pd.DataFrame) or df.empty:
            summary[str(tf)] = {"bars": 0, "states": {}, "latest": None, "latest_confidence": None}
            continue
        ser = df.get("market_state")
        if ser is None:
            summary[str(tf)] = {"bars": int(len(df)), "states": {}, "latest": None, "latest_confidence": None}
            continue
        counts = ser.dropna().astype(str).value_counts().to_dict()
        latest_state = None
        latest_conf = None
        latest_texture = None
        latest_summary = None
        try:
            latest_state = str(ser.iloc[-1]) if len(ser) else None
        except Exception:
            latest_state = None
        try:
            latest_conf = float(pd.to_numeric(df.get("market_confidence"), errors="coerce").iloc[-1])
        except Exception:
            latest_conf = None
        try:
            latest_texture = str(df.get("market_texture").iloc[-1]) if "market_texture" in df.columns else None
        except Exception:
            latest_texture = None
        try:
            latest_summary = str(df.get("market_summary_short").iloc[-1]) if "market_summary_short" in df.columns else None
        except Exception:
            latest_summary = None
        summary[str(tf)] = {
            "bars": int(len(df)),
            "states": {str(k): int(v) for k, v in counts.items()},
            "latest": latest_state,
            "latest_confidence": latest_conf,
            "latest_texture": latest_texture,
            "latest_summary": latest_summary,
        }
    return summary

def ema_tradingview(values: "np.ndarray", length: int) -> "np.ndarray":
    """
    TradingView/Pine ta.ema-style EMA with robust handling of leading NaNs.

    Behavior:
      - Output is NaN until we have `length` valid (non-NaN) samples starting from
        the first non-NaN value in the input series.
      - First EMA value is the SMA of the first `length` valid samples.
      - After seeding, EMA updates per bar. If an occasional NaN appears after
        seeding, we output NaN for that bar but keep the previous EMA value.
        (This matches practical Pine behavior for series that are NaN only in the
        lead-in region, e.g., MACD before slow EMA is available.)
    """
    v = np.asarray(values, dtype=float)
    out = np.full_like(v, np.nan, dtype=float)
    n = len(v)
    if length <= 0 or n == 0:
        return out

    # Find first index with a finite value
    first = None
    for i in range(n):
        x = v[i]
        if np.isfinite(x):
            first = i
            break
    if first is None:
        return out

    # Need `length` values from first valid onward
    if (n - first) < length:
        return out

    alpha = 2.0 / (length + 1.0)

    seed_slice = v[first:first + length]
    # If there are NaNs inside the seed slice (unlikely beyond lead-in), fail gracefully
    if not np.all(np.isfinite(seed_slice)):
        # fall back: find first window of `length` consecutive finite values
        seed_start = None
        run = 0
        for i in range(first, n):
            if np.isfinite(v[i]):
                run += 1
                if run >= length:
                    seed_start = i - length + 1
                    break
            else:
                run = 0
        if seed_start is None:
            return out
        first = seed_start
        seed_slice = v[first:first + length]
        if not np.all(np.isfinite(seed_slice)):
            return out

    seed = float(np.mean(seed_slice))
    seed_idx = first + length - 1
    out[seed_idx] = seed
    prev = seed

    for i in range(seed_idx + 1, n):
        x = float(v[i])
        if not np.isfinite(x):
            out[i] = np.nan
            continue
        prev = (alpha * x) + (1.0 - alpha) * prev
        out[i] = prev

    return out

def macd_tradingview(closes: "np.ndarray", fast: int = 12, slow: int = 26, signal: int = 9) -> Tuple["np.ndarray", "np.ndarray", "np.ndarray"]:
    c = np.asarray(closes, dtype=float)
    ema_fast = ema_tradingview(c, fast)
    ema_slow = ema_tradingview(c, slow)
    macd_line = ema_fast - ema_slow

    # Signal EMA on MACD (skip leading NaN region implicitly)
    sig_line = ema_tradingview(macd_line, signal)
    hist = macd_line - sig_line
    return macd_line, sig_line, hist

def macd_series(closes: "np.ndarray", impl: str) -> Tuple["np.ndarray", "np.ndarray", "np.ndarray"]:
    impl = (impl or "TRADINGVIEW").upper()
    if impl == "TALIB" and talib is not None:
        macd_line, sig_line, hist = talib.MACD(closes, MACD_FAST, MACD_SLOW, MACD_SIGNAL)
        return macd_line, sig_line, hist
    # default: TradingView-style
    return macd_tradingview(closes, MACD_FAST, MACD_SLOW, MACD_SIGNAL)


def _sma_strict(values: "np.ndarray", length: int) -> "np.ndarray":
    """Simple moving average that requires *length* finite samples per window.

    This is used as a lightweight fallback when TA-Lib isn't available.
    """
    v = np.asarray(values, dtype=float)
    out = np.full_like(v, np.nan, dtype=float)
    try:
        length = int(length)
    except Exception:
        return out
    if length <= 0:
        return out
    n = len(v)
    if n < length:
        return out

    finite = np.isfinite(v)
    v0 = np.where(finite, v, 0.0)

    csum = np.cumsum(v0, dtype=float)
    ccount = np.cumsum(finite.astype(np.int64), dtype=np.int64)

    # Window sums/counts for indices [length-1 .. n-1]
    sum_w = csum[length - 1:] - np.concatenate(([0.0], csum[:-length]))
    cnt_w = ccount[length - 1:] - np.concatenate(([0], ccount[:-length]))
    ok = (cnt_w == length)
    out[length - 1:] = np.where(ok, sum_w / float(length), np.nan)
    return out


def ppo_d_series(closes: "np.ndarray") -> "np.ndarray":
    """Return PPO *dot* series (dPPO) used by the UI and rule engine.

    Uses TA-Lib if present; otherwise falls back to TradingView-style EMA + strict SMA.
    """
    c = np.asarray(closes, dtype=float)
    if talib is not None:
        fast_ma = talib.EMA(c, PPO_FAST)
        slow_ma = talib.EMA(c, PPO_SLOW)
        with np.errstate(divide="ignore", invalid="ignore"):
            ppo_raw = (fast_ma - slow_ma) / slow_ma * 100.0
        return talib.SMA(ppo_raw, PPO_SMOOTH)

    # Fallback path (no TA-Lib)
    fast_ma = ema_tradingview(c, PPO_FAST)
    slow_ma = ema_tradingview(c, PPO_SLOW)
    with np.errstate(divide="ignore", invalid="ignore"):
        ppo_raw = (fast_ma - slow_ma) / slow_ma * 100.0
    return _sma_strict(ppo_raw, PPO_SMOOTH)


def _rsi_from_rma(avg_gain: float, avg_loss: float) -> float:
    try:
        gain = float(avg_gain)
        loss = float(avg_loss)
    except Exception:
        return float("nan")
    if not np.isfinite(gain) or not np.isfinite(loss):
        return float("nan")
    if loss <= 0.0:
        return 100.0
    if gain <= 0.0:
        return 0.0
    rs = gain / loss
    return 100.0 - (100.0 / (1.0 + rs))


def rsi_tradingview(closes: "np.ndarray", length: int = RSI_LENGTH) -> "np.ndarray":
    c = np.asarray(closes, dtype=float)
    out = np.full_like(c, np.nan, dtype=float)
    try:
        length_i = int(length)
    except Exception:
        return out
    if length_i <= 0 or len(c) <= 1:
        return out

    gains = _RMAState(length_i)
    losses = _RMAState(length_i)
    prev = float(c[0])
    if not np.isfinite(prev):
        return out

    for i in range(1, len(c)):
        cur = float(c[i])
        if not np.isfinite(cur):
            prev = cur
            continue
        delta = cur - prev
        prev = cur
        avg_gain = gains.update(max(delta, 0.0))
        avg_loss = losses.update(max(-delta, 0.0))
        rsi = _rsi_from_rma(avg_gain, avg_loss)
        if np.isfinite(rsi):
            out[i] = rsi
    return out


def rsi_series(
    closes: "np.ndarray",
    *,
    length: int = RSI_LENGTH,
    smoothing: int = RSI_SMOOTHING,
) -> Tuple["np.ndarray", "np.ndarray", "np.ndarray"]:
    c = np.asarray(closes, dtype=float)
    rsi = rsi_tradingview(c, length)
    smooth = _sma_strict(rsi, smoothing)
    state = np.full(len(c), np.int8(0), dtype=np.int8)
    for i in range(len(c)):
        rsi_v = float(rsi[i]) if i < len(rsi) else float("nan")
        smooth_v = float(smooth[i]) if i < len(smooth) else float("nan")
        if np.isfinite(rsi_v) and np.isfinite(smooth_v):
            state[i] = np.int8(1 if rsi_v > smooth_v else -1)
    return rsi, smooth, state


def _stoch_same_series(values: "np.ndarray", length: int) -> "np.ndarray":
    v = np.asarray(values, dtype=float)
    out = np.full_like(v, np.nan, dtype=float)
    try:
        length_i = int(length)
    except Exception:
        return out
    if length_i <= 0 or len(v) < length_i:
        return out

    for i in range(length_i - 1, len(v)):
        window = v[(i - length_i + 1):(i + 1)]
        if np.count_nonzero(np.isfinite(window)) != length_i:
            continue
        lo = float(np.min(window))
        hi = float(np.max(window))
        cur = float(v[i])
        if not np.isfinite(cur):
            continue
        denom = hi - lo
        out[i] = 0.0 if denom <= 0.0 else (100.0 * (cur - lo) / denom)
    return out


def stoch_rsi_series(
    closes: "np.ndarray",
    *,
    rsi_length: int = STOCH_RSI_RSI_LENGTH,
    stoch_length: int = STOCH_RSI_STOCH_LENGTH,
    smooth_k: int = STOCH_RSI_SMOOTH_K,
    smooth_d: int = STOCH_RSI_SMOOTH_D,
) -> Tuple["np.ndarray", "np.ndarray", "np.ndarray"]:
    c = np.asarray(closes, dtype=float)
    rsi = rsi_tradingview(c, rsi_length)
    raw = _stoch_same_series(rsi, stoch_length)
    k = _sma_strict(raw, smooth_k)
    d = _sma_strict(k, smooth_d)
    return rsi, k, d


def compute_live_stoch_rsi(
    full_closes: List[float],
    *,
    rsi_length: int = STOCH_RSI_RSI_LENGTH,
    stoch_length: int = STOCH_RSI_STOCH_LENGTH,
    smooth_k: int = STOCH_RSI_SMOOTH_K,
    smooth_d: int = STOCH_RSI_SMOOTH_D,
) -> Tuple[float, float, Optional[str]]:
    c = np.asarray(full_closes, dtype=float)
    _rsi, k, d = stoch_rsi_series(
        c,
        rsi_length=rsi_length,
        stoch_length=stoch_length,
        smooth_k=smooth_k,
        smooth_d=smooth_d,
    )
    if len(k) < 1 or len(d) < 1:
        raise ValueError("Not enough bars")
    curr_k = float(k[-1])
    curr_d = float(d[-1])
    if (not np.isfinite(curr_k)) or (not np.isfinite(curr_d)):
        raise ValueError("Stoch RSI not ready")
    kd_state = "GREEN" if curr_k > curr_d else "RED"
    return curr_k, curr_d, kd_state


def compute_live_rsi(
    full_closes: List[float],
    *,
    length: int = RSI_LENGTH,
    smoothing: int = RSI_SMOOTHING,
) -> Tuple[float, float, Optional[str]]:
    c = np.asarray(full_closes, dtype=float)
    rsi, smooth, _state = rsi_series(c, length=length, smoothing=smoothing)
    if len(rsi) < 1 or len(smooth) < 1:
        raise ValueError("Not enough bars")
    curr_rsi = float(rsi[-1])
    curr_smooth = float(smooth[-1])
    if (not np.isfinite(curr_rsi)) or (not np.isfinite(curr_smooth)):
        raise ValueError("RSI not ready")
    return curr_rsi, curr_smooth, ("GREEN" if curr_rsi > curr_smooth else "RED")

def compute_live_indicators(full_closes: List[float], macd_impl: str = "TRADINGVIEW") -> Tuple[float, float, str, str, Optional[str], str, Optional[int]]:
    """
    Returns:
      macd, signal,
      ms_color (GREEN if MACD>Signal else RED),
      macd_angle_color (GREEN if MACD_now > MACD_prev_bar else RED),
      sig_angle_color (GREEN if Signal_now > Signal_prev_bar else RED),
      ppo_color (GREEN if dPPO rising else RED),
      flip_age (candles since PPO dot flipped)

    Notes:
      - In TradingView mode, Signal becomes available only after enough MACD values exist
        (roughly MACD_SLOW + MACD_SIGNAL bars). Before that we raise, and caller should
        treat as "not ready" (show '-') rather than displaying NaN.
      - Signal angle is returned as None until we have a finite previous Signal bar.
    """
    np_closes = np.array(full_closes, dtype=float)

    macd_line, signal_line, _ = macd_series(np_closes, macd_impl)

    if len(macd_line) < 2 or len(signal_line) < 2:
        raise ValueError("Not enough bars")

    curr_macd = float(macd_line[-1])
    prev_macd = float(macd_line[-2])
    curr_sig = float(signal_line[-1])
    prev_sig = float(signal_line[-2])

    if (not np.isfinite(curr_macd)) or (not np.isfinite(prev_macd)) or (not np.isfinite(curr_sig)):
        raise ValueError("MACD/Signal not ready")

    ms_color = "GREEN" if curr_macd > curr_sig else "RED"
    macd_angle_color = "GREEN" if curr_macd > prev_macd else "RED"

    sig_angle_color: Optional[str] = None
    if np.isfinite(prev_sig):
        sig_angle_color = "GREEN" if curr_sig > prev_sig else "RED"

    d_ppo = ppo_d_series(np_closes)

    if len(d_ppo) < 2 or (not np.isfinite(float(d_ppo[-1]))) or (not np.isfinite(float(d_ppo[-2]))):
        raise ValueError("PPO not ready")

    ppo_color = "GREEN" if float(d_ppo[-1]) > float(d_ppo[-2]) else "RED"
    flip_age = _ppo_flip_age(d_ppo)

    return curr_macd, curr_sig, ms_color, macd_angle_color, sig_angle_color, ppo_color, flip_age

def slice_and_decimate(df: "pd.DataFrame", start: "pd.Timestamp", end: "pd.Timestamp", max_points: int = 8000) -> "pd.DataFrame":
    """Slice a time-indexed DataFrame to [start,end] and decimate to keep plots responsive.

    Tick backtests can produce very dense series (aggTrades). Plotting tens/hundreds of
    thousands of points in Tkinter will freeze the UI. This helper is used by zoom and
    trade-detail views to cap point counts.
    """
    if df is None or getattr(df, "empty", True):
        return df
    try:
        seg = df.loc[(df.index >= start) & (df.index <= end)]
    except Exception:
        seg = df
    if seg is None or seg.empty:
        return seg
    try:
        n = int(len(seg))
    except Exception:
        return seg
    if max_points and n > max_points:
        step = int(n // max_points) + 1
        try:
            seg = seg.iloc[::step]
        except Exception:
            pass
    return seg

@dataclass
class TFStorage:
    closes: List[float]

def _color_to_i8(c: Optional[str]) -> int:
    if c == "GREEN":
        return 1
    if c == "RED":
        return -1
    return 0

def _i8_to_color(v: Any) -> Optional[str]:
    try:
        iv = int(v)
    except Exception:
        return None
    if iv == 1:
        return "GREEN"
    if iv == -1:
        return "RED"
    return None

class _EMAState:
    """SMA-seeded recursive EMA state (matches common TA / TradingView style for finite input)."""
    __slots__ = ("length", "alpha", "_buf", "value", "seeded")

    def __init__(self, length: int):
        self.length = int(length)
        self.alpha = 2.0 / (self.length + 1.0) if self.length > 0 else 0.0
        self._buf: List[float] = []
        self.value: float = float("nan")
        self.seeded: bool = False

    def preview(self, x: float) -> float:
        if self.length <= 0:
            return float("nan")
        if not np.isfinite(x):
            return float("nan")
        if not self.seeded:
            n = len(self._buf)
            if n + 1 < self.length:
                return float("nan")
            if n + 1 == self.length:
                return float(np.mean(self._buf + [x]))
            # shouldn't happen
            return float("nan")
        return (self.alpha * x) + (1.0 - self.alpha) * float(self.value)

    def commit(self, x: float) -> float:
        if self.length <= 0:
            self.value = float("nan")
            self.seeded = False
            self._buf = []
            return float("nan")
        if not np.isfinite(x):
            # Keep previous EMA value; output NaN for this bar.
            return float("nan") if not self.seeded else float("nan")
        if not self.seeded:
            self._buf.append(float(x))
            if len(self._buf) < self.length:
                return float("nan")
            if len(self._buf) == self.length:
                self.value = float(np.mean(self._buf))
                self.seeded = True
                self._buf = []  # free memory
                return float(self.value)
        # seeded
        self.value = (self.alpha * float(x)) + (1.0 - self.alpha) * float(self.value)
        return float(self.value)

class _SMAState:
    """Rolling SMA state."""
    __slots__ = ("length", "_win", "_sum")

    def __init__(self, length: int):
        self.length = int(length)
        self._win: "collections.deque[float]" = collections.deque()
        self._sum: float = 0.0

    def preview(self, x: float) -> float:
        if self.length <= 0 or (not np.isfinite(x)):
            return float("nan")
        n = len(self._win)
        if n + 1 < self.length:
            return float("nan")
        if n + 1 == self.length:
            return (self._sum + float(x)) / float(self.length)
        # full window already
        oldest = float(self._win[0]) if n >= self.length else 0.0
        return (self._sum - oldest + float(x)) / float(self.length)

    def commit(self, x: float) -> float:
        if self.length <= 0 or (not np.isfinite(x)):
            return float("nan")
        if len(self._win) < self.length:
            self._win.append(float(x))
            self._sum += float(x)
            if len(self._win) < self.length:
                return float("nan")
            return self._sum / float(self.length)
        # roll
        old = float(self._win.popleft())
        self._sum -= old
        self._win.append(float(x))
        self._sum += float(x)
        return self._sum / float(self.length)

class _RMAState:
    """Wilder's RMA (TradingView's ta.rma) state with SMA seed."""
    __slots__ = ("length", "count", "_sum", "value")

    def __init__(self, length: int):
        self.length = int(length)
        self.count = 0
        self._sum = 0.0
        self.value = float("nan")

    def update(self, x: float) -> float:
        try:
            xf = float(x)
        except Exception:
            xf = float("nan")
        if not np.isfinite(xf):
            # propagate NaN without mutating (best-effort)
            return float("nan")

        self.count += 1
        if self.count < self.length:
            self._sum += xf
            return float("nan")
        if self.count == self.length:
            self._sum += xf
            self.value = float(self._sum / float(self.length))
            return float(self.value)

        # Wilder recursion: (prev*(len-1) + x) / len
        self.value = (float(self.value) * float(self.length - 1) + xf) / float(self.length)
        return float(self.value)

    def preview(self, x: float) -> float:
        """Preview next value without mutating state."""
        try:
            xf = float(x)
        except Exception:
            xf = float("nan")
        if not np.isfinite(xf):
            return float("nan")

        if self.count + 1 < self.length:
            return float("nan")
        if self.count + 1 == self.length:
            # would become seeded SMA
            return float((self._sum + xf) / float(self.length))
        # already seeded
        if not np.isfinite(float(self.value)):
            return float("nan")
        return (float(self.value) * float(self.length - 1) + xf) / float(self.length)


class _RsiState:
    __slots__ = (
        "length",
        "smoothing",
        "prev_close",
        "have_prev",
        "gain_rma",
        "loss_rma",
        "smooth_sma",
        "rsi",
        "smooth",
        "state",
    )

    def __init__(self, length: int = RSI_LENGTH, smoothing: int = RSI_SMOOTHING):
        self.length = max(1, int(length))
        self.smoothing = max(1, int(smoothing))
        self.prev_close = float("nan")
        self.have_prev = False
        self.gain_rma = _RMAState(self.length)
        self.loss_rma = _RMAState(self.length)
        self.smooth_sma = _SMAState(self.smoothing)
        self.rsi = float("nan")
        self.smooth = float("nan")
        self.state: Optional[str] = None

    def _state_from_values(self, rsi: float, smooth: float) -> Optional[str]:
        if not np.isfinite(rsi) or not np.isfinite(smooth):
            return None
        return "GREEN" if float(rsi) > float(smooth) else "RED"

    def preview(self, close: float) -> Tuple[float, float, Optional[str]]:
        try:
            c = float(close)
        except Exception:
            return float("nan"), float("nan"), None
        if not np.isfinite(c) or not self.have_prev:
            return float("nan"), float("nan"), None
        delta = c - float(self.prev_close)
        avg_gain = self.gain_rma.preview(max(delta, 0.0))
        avg_loss = self.loss_rma.preview(max(-delta, 0.0))
        rsi = _rsi_from_rma(avg_gain, avg_loss)
        if not np.isfinite(rsi):
            return float("nan"), float("nan"), None
        smooth = self.smooth_sma.preview(float(rsi))
        return float(rsi), float(smooth), self._state_from_values(float(rsi), float(smooth))

    def commit(self, close: float) -> Tuple[float, float, Optional[str]]:
        try:
            c = float(close)
        except Exception:
            return float("nan"), float("nan"), None
        if not np.isfinite(c):
            return float("nan"), float("nan"), None
        if not self.have_prev:
            self.prev_close = float(c)
            self.have_prev = True
            self.rsi = float("nan")
            self.smooth = float("nan")
            self.state = None
            return self.rsi, self.smooth, self.state

        delta = c - float(self.prev_close)
        self.prev_close = float(c)
        avg_gain = self.gain_rma.update(max(delta, 0.0))
        avg_loss = self.loss_rma.update(max(-delta, 0.0))
        self.rsi = _rsi_from_rma(avg_gain, avg_loss)
        if not np.isfinite(self.rsi):
            self.smooth = float("nan")
            self.state = None
            return self.rsi, self.smooth, self.state
        self.smooth = self.smooth_sma.commit(float(self.rsi))
        self.state = self._state_from_values(float(self.rsi), float(self.smooth))
        return self.rsi, self.smooth, self.state

    def snapshot(self) -> Tuple[float, float, Optional[str]]:
        return self.rsi, self.smooth, self.state


class _StochRsiState:
    __slots__ = (
        "rsi_length",
        "stoch_length",
        "smooth_k",
        "smooth_d",
        "prev_close",
        "have_prev",
        "gain_rma",
        "loss_rma",
        "rsi_win",
        "raw_win",
        "k_win",
        "k",
        "d",
        "kd",
    )

    def __init__(
        self,
        rsi_length: int = STOCH_RSI_RSI_LENGTH,
        stoch_length: int = STOCH_RSI_STOCH_LENGTH,
        smooth_k: int = STOCH_RSI_SMOOTH_K,
        smooth_d: int = STOCH_RSI_SMOOTH_D,
    ):
        self.rsi_length = max(1, int(rsi_length))
        self.stoch_length = max(1, int(stoch_length))
        self.smooth_k = max(1, int(smooth_k))
        self.smooth_d = max(1, int(smooth_d))
        self.prev_close = float("nan")
        self.have_prev = False
        self.gain_rma = _RMAState(self.rsi_length)
        self.loss_rma = _RMAState(self.rsi_length)
        self.rsi_win: "collections.deque[float]" = collections.deque(maxlen=self.stoch_length)
        self.raw_win: "collections.deque[float]" = collections.deque(maxlen=self.smooth_k)
        self.k_win: "collections.deque[float]" = collections.deque(maxlen=self.smooth_d)
        self.k = float("nan")
        self.d = float("nan")
        self.kd: Optional[str] = None

    def _stoch_from_values(self, values: List[float], current: float) -> float:
        if len(values) < self.stoch_length:
            return float("nan")
        arr = np.asarray(values[-self.stoch_length:], dtype=float)
        if np.count_nonzero(np.isfinite(arr)) != self.stoch_length:
            return float("nan")
        lo = float(np.min(arr))
        hi = float(np.max(arr))
        if not np.isfinite(current):
            return float("nan")
        denom = hi - lo
        return 0.0 if denom <= 0.0 else (100.0 * (float(current) - lo) / denom)

    def _state_from_kd(self, k: float, d: float) -> Optional[str]:
        if not np.isfinite(k) or not np.isfinite(d):
            return None
        return "GREEN" if float(k) > float(d) else "RED"

    def preview(self, close: float) -> Tuple[float, float, Optional[str]]:
        try:
            c = float(close)
        except Exception:
            return float("nan"), float("nan"), None
        if not np.isfinite(c) or not self.have_prev:
            return float("nan"), float("nan"), None

        delta = c - float(self.prev_close)
        avg_gain = self.gain_rma.preview(max(delta, 0.0))
        avg_loss = self.loss_rma.preview(max(-delta, 0.0))
        rsi = _rsi_from_rma(avg_gain, avg_loss)
        if not np.isfinite(rsi):
            return float("nan"), float("nan"), None

        rsi_vals = list(self.rsi_win)[-(self.stoch_length - 1):] if self.stoch_length > 1 else []
        rsi_vals.append(float(rsi))
        raw = self._stoch_from_values(rsi_vals, float(rsi))
        if not np.isfinite(raw):
            return float("nan"), float("nan"), None

        raw_vals = list(self.raw_win)[-(self.smooth_k - 1):] if self.smooth_k > 1 else []
        raw_vals.append(float(raw))
        if len(raw_vals) < self.smooth_k:
            return float("nan"), float("nan"), None
        k = float(np.mean(raw_vals))

        k_vals = list(self.k_win)[-(self.smooth_d - 1):] if self.smooth_d > 1 else []
        k_vals.append(float(k))
        if len(k_vals) < self.smooth_d:
            return k, float("nan"), None
        d = float(np.mean(k_vals))
        return k, d, self._state_from_kd(k, d)

    def commit(self, close: float) -> Tuple[float, float, Optional[str]]:
        try:
            c = float(close)
        except Exception:
            return float("nan"), float("nan"), None
        if not np.isfinite(c):
            return float("nan"), float("nan"), None
        if not self.have_prev:
            self.prev_close = float(c)
            self.have_prev = True
            self.k = float("nan")
            self.d = float("nan")
            self.kd = None
            return self.k, self.d, self.kd

        delta = c - float(self.prev_close)
        self.prev_close = float(c)
        avg_gain = self.gain_rma.update(max(delta, 0.0))
        avg_loss = self.loss_rma.update(max(-delta, 0.0))
        rsi = _rsi_from_rma(avg_gain, avg_loss)
        if not np.isfinite(rsi):
            self.k = float("nan")
            self.d = float("nan")
            self.kd = None
            return self.k, self.d, self.kd

        self.rsi_win.append(float(rsi))
        raw = self._stoch_from_values(list(self.rsi_win), float(rsi))
        if not np.isfinite(raw):
            self.k = float("nan")
            self.d = float("nan")
            self.kd = None
            return self.k, self.d, self.kd

        self.raw_win.append(float(raw))
        if len(self.raw_win) < self.smooth_k:
            self.k = float("nan")
            self.d = float("nan")
            self.kd = None
            return self.k, self.d, self.kd
        self.k = float(np.mean(self.raw_win))

        self.k_win.append(float(self.k))
        if len(self.k_win) < self.smooth_d:
            self.d = float("nan")
            self.kd = None
            return self.k, self.d, self.kd
        self.d = float(np.mean(self.k_win))
        self.kd = self._state_from_kd(self.k, self.d)
        return self.k, self.d, self.kd

    def snapshot(self) -> Tuple[float, float, Optional[str]]:
        return self.k, self.d, self.kd

class _WilderADXState:
    """TradingView/Wilder ADX(+DI/-DI) computed on CLOSED bars (ta.adx/ta.di+/- semantics)."""
    __slots__ = ("length", "prev_high", "prev_low", "prev_close", "rma_tr", "rma_pdm", "rma_mdm", "rma_adx",
                 "di_plus", "di_minus", "adx", "adx_angle")

    def __init__(self, length: int = ADX_LEN):
        self.length = int(length)
        self.prev_high = float("nan")
        self.prev_low = float("nan")
        self.prev_close = float("nan")
        self.rma_tr = _RMAState(self.length)
        self.rma_pdm = _RMAState(self.length)
        self.rma_mdm = _RMAState(self.length)
        self.rma_adx = _RMAState(self.length)
        self.di_plus = float("nan")
        self.di_minus = float("nan")
        self.adx = float("nan")
        self.adx_angle = None  # "GREEN" if ADX rising vs prev closed bar, else "RED" (or None if unknown)

    def update(self, high: float, low: float, close: float) -> Tuple[float, float, float]:
        """Commit a CLOSED bar (high/low/close). Returns (adx, di_plus, di_minus)."""
        try:
            h = float(high); l = float(low); c = float(close)
        except Exception:
            return float("nan"), float("nan"), float("nan")

        if not np.isfinite(h) or not np.isfinite(l) or not np.isfinite(c):
            return float("nan"), float("nan"), float("nan")

        # First bar: seed prevs, TR = high-low, DM=0
        if not np.isfinite(self.prev_close):
            tr = max(0.0, h - l)
            _ = self.rma_tr.update(tr)
            _ = self.rma_pdm.update(0.0)
            _ = self.rma_mdm.update(0.0)
            self.prev_high, self.prev_low, self.prev_close = h, l, c
            self.di_plus = float("nan")
            self.di_minus = float("nan")
            self.adx = float("nan")
            self.adx_angle = None
            return self.adx, self.di_plus, self.di_minus

        up_move = h - float(self.prev_high)
        down_move = float(self.prev_low) - l
        pdm = up_move if (up_move > down_move and up_move > 0.0) else 0.0
        mdm = down_move if (down_move > up_move and down_move > 0.0) else 0.0

        tr = max(
            h - l,
            abs(h - float(self.prev_close)),
            abs(l - float(self.prev_close)),
        )

        tr_rma = float(self.rma_tr.update(tr))
        pdm_rma = float(self.rma_pdm.update(pdm))
        mdm_rma = float(self.rma_mdm.update(mdm))

        di_p = float("nan")
        di_m = float("nan")
        if np.isfinite(tr_rma) and tr_rma != 0.0 and np.isfinite(pdm_rma) and np.isfinite(mdm_rma):
            di_p = 100.0 * pdm_rma / tr_rma
            di_m = 100.0 * mdm_rma / tr_rma

        self.di_plus = di_p
        self.di_minus = di_m

        dx = float("nan")
        if np.isfinite(di_p) and np.isfinite(di_m):
            denom = abs(di_p) + abs(di_m)
            if denom != 0.0:
                dx = 100.0 * abs(di_p - di_m) / denom

        adx_val = float(self.rma_adx.update(dx)) if np.isfinite(dx) else float("nan")
        # Angle / slope (GREEN if ADX rising vs previous closed bar, else RED)
        try:
            prev_adx = float(self.adx)
        except Exception:
            prev_adx = float("nan")
        if np.isfinite(adx_val) and np.isfinite(prev_adx):
            self.adx_angle = "GREEN" if float(adx_val) > float(prev_adx) else "RED"
        else:
            self.adx_angle = None
        self.adx = adx_val

        self.prev_high, self.prev_low, self.prev_close = h, l, c
        return self.adx, self.di_plus, self.di_minus


    def preview(self, high: float, low: float, close: float) -> Tuple[Optional[float], Optional[float], Optional[float], Optional[float], Optional[str]]:
        """Preview ADX/+DI/-DI using a FORMING bar (high/low/close) without mutating state.

        This matches TradingView's behavior when indicators are allowed to update intrabar.
        Angle rule mirrors the chart slope: GREEN if preview ADX > last CLOSED ADX, else RED.
        """
        try:
            h = float(high); l = float(low); c = float(close)
        except Exception:
            return None, None, None, None, None

        if not np.isfinite(h) or not np.isfinite(l) or not np.isfinite(c):
            return None, None, None, None, None

        # If we have no previous CLOSED bar, preview is not meaningful.
        if (not np.isfinite(self.prev_close)) or (not np.isfinite(self.prev_high)) or (not np.isfinite(self.prev_low)):
            return None, None, None, None, None

        up_move = h - float(self.prev_high)
        down_move = float(self.prev_low) - l
        pdm = up_move if (up_move > down_move and up_move > 0.0) else 0.0
        mdm = down_move if (down_move > up_move and down_move > 0.0) else 0.0

        tr = max(
            h - l,
            abs(h - float(self.prev_close)),
            abs(l - float(self.prev_close)),
        )

        tr_rma = float(self.rma_tr.preview(tr))
        pdm_rma = float(self.rma_pdm.preview(pdm))
        mdm_rma = float(self.rma_mdm.preview(mdm))

        di_p = float("nan")
        di_m = float("nan")
        if np.isfinite(tr_rma) and tr_rma != 0.0 and np.isfinite(pdm_rma) and np.isfinite(mdm_rma):
            di_p = 100.0 * pdm_rma / tr_rma
            di_m = 100.0 * mdm_rma / tr_rma

        dx = float("nan")
        if np.isfinite(di_p) and np.isfinite(di_m):
            denom = abs(di_p) + abs(di_m)
            if denom != 0.0:
                dx = 100.0 * abs(di_p - di_m) / denom

        adx_val = float(self.rma_adx.preview(dx)) if np.isfinite(dx) else float("nan")

        adx = float(adx_val) if np.isfinite(adx_val) else None
        dip = float(di_p) if np.isfinite(di_p) else None
        dim = float(di_m) if np.isfinite(di_m) else None
        dis = (float(dip - dim) if (dip is not None and dim is not None) else None)

        ang = None
        try:
            prev_adx = float(self.adx)
        except Exception:
            prev_adx = float("nan")
        if adx is not None and np.isfinite(prev_adx):
            ang = "GREEN" if float(adx) > float(prev_adx) else "RED"

        return adx, dip, dim, dis, ang

    def snapshot(self) -> Tuple[Optional[float], Optional[float], Optional[float], Optional[float], Optional[str]]:
        """Return (adx, di_plus, di_minus, di_spread, adx_angle) as optional values."""
        adx = float(self.adx) if np.isfinite(float(self.adx)) else None
        dip = float(self.di_plus) if np.isfinite(float(self.di_plus)) else None
        dim = float(self.di_minus) if np.isfinite(float(self.di_minus)) else None
        dis = (float(dip - dim) if (dip is not None and dim is not None) else None)
        ang = self.adx_angle if self.adx_angle in ("GREEN", "RED") else None
        return adx, dip, dim, dis, ang


class _PineWilderSumState:
    """Pine-script style Wilder smoothing in "sum" form.

    Matches the recurrence used in the provided Pine v4 script:
      sm := nz(sm[1]) - (nz(sm[1]) / len) + x

    Notes:
      - This state does NOT SMA-seed like TradingView's ta.rma.
      - It starts from 0.0 (nz for the first bar).
      - This can noticeably change early values when the dataset has limited warmup
        (e.g., higher timeframes or short history windows).
    """
    __slots__ = ("length", "value")

    def __init__(self, length: int):
        self.length = int(length)
        self.value = 0.0

    def update(self, x: float) -> float:
        try:
            xf = float(x)
        except Exception:
            xf = float("nan")
        if self.length <= 0 or (not np.isfinite(xf)):
            return float("nan")
        prev = float(self.value) if np.isfinite(float(self.value)) else 0.0
        self.value = prev - (prev / float(self.length)) + xf
        return float(self.value)

    def preview(self, x: float) -> float:
        try:
            xf = float(x)
        except Exception:
            xf = float("nan")
        if self.length <= 0 or (not np.isfinite(xf)):
            return float("nan")
        prev = float(self.value) if np.isfinite(float(self.value)) else 0.0
        return float(prev - (prev / float(self.length)) + xf)


class _PineADXState:
    """ADX(+DI/-DI) matching the provided Pine v4 script.

    Key differences vs TradingView/Wilder ADX:
      - TR/+DM/-DM are smoothed using the Pine script's recurrence (sum-form, no SMA seed).
      - ADX itself is computed as SMA(DX, len), not RMA(DX, len).
    """
    __slots__ = (
        "length",
        "prev_high",
        "prev_low",
        "prev_close",
        "sm_tr",
        "sm_pdm",
        "sm_mdm",
        "sma_adx",
        "di_plus",
        "di_minus",
        "adx",
        "adx_angle",
    )

    def __init__(self, length: int = ADX_LEN):
        self.length = int(length)
        self.prev_high = float("nan")
        self.prev_low = float("nan")
        self.prev_close = float("nan")
        self.sm_tr = _PineWilderSumState(self.length)
        self.sm_pdm = _PineWilderSumState(self.length)
        self.sm_mdm = _PineWilderSumState(self.length)
        self.sma_adx = _SMAState(self.length)
        self.di_plus = float("nan")
        self.di_minus = float("nan")
        self.adx = float("nan")
        self.adx_angle = None

    def update(self, high: float, low: float, close: float) -> Tuple[float, float, float]:
        """Commit a CLOSED bar (high/low/close). Returns (adx, di_plus, di_minus)."""
        try:
            h = float(high); l = float(low); c = float(close)
        except Exception:
            return float("nan"), float("nan"), float("nan")

        if not np.isfinite(h) or not np.isfinite(l) or not np.isfinite(c):
            return float("nan"), float("nan"), float("nan")

        # Pine script uses nz(prev) which becomes 0.0 when previous is na.
        ph = float(self.prev_high) if np.isfinite(float(self.prev_high)) else 0.0
        pl = float(self.prev_low) if np.isfinite(float(self.prev_low)) else 0.0
        pc = float(self.prev_close) if np.isfinite(float(self.prev_close)) else 0.0

        true_range = max(max(h - l, abs(h - pc)), abs(l - pc))
        dm_plus = (max(h - ph, 0.0) if ((h - ph) > (pl - l)) else 0.0)
        dm_minus = (max(pl - l, 0.0) if ((pl - l) > (h - ph)) else 0.0)

        sm_tr = float(self.sm_tr.update(true_range))
        sm_p = float(self.sm_pdm.update(dm_plus))
        sm_m = float(self.sm_mdm.update(dm_minus))

        di_p = float("nan")
        di_m = float("nan")
        if np.isfinite(sm_tr) and sm_tr != 0.0 and np.isfinite(sm_p) and np.isfinite(sm_m):
            di_p = 100.0 * sm_p / sm_tr
            di_m = 100.0 * sm_m / sm_tr

        self.di_plus = di_p
        self.di_minus = di_m

        dx = float("nan")
        if np.isfinite(di_p) and np.isfinite(di_m):
            denom = float(di_p) + float(di_m)
            if denom != 0.0:
                dx = 100.0 * abs(float(di_p) - float(di_m)) / denom

        adx_val = float(self.sma_adx.commit(dx)) if np.isfinite(dx) else float("nan")

        # Angle / slope on CLOSED bars: GREEN if ADX rising vs previous closed ADX
        try:
            prev_adx = float(self.adx)
        except Exception:
            prev_adx = float("nan")
        if np.isfinite(adx_val) and np.isfinite(prev_adx):
            self.adx_angle = "GREEN" if float(adx_val) > float(prev_adx) else "RED"
        else:
            self.adx_angle = None
        self.adx = adx_val

        self.prev_high, self.prev_low, self.prev_close = h, l, c
        return self.adx, self.di_plus, self.di_minus

    def preview(self, high: float, low: float, close: float) -> Tuple[Optional[float], Optional[float], Optional[float], Optional[float], Optional[str]]:
        """Preview ADX/+DI/-DI using a FORMING bar without mutating state."""
        try:
            h = float(high); l = float(low); c = float(close)
        except Exception:
            return None, None, None, None, None

        if not np.isfinite(h) or not np.isfinite(l) or not np.isfinite(c):
            return None, None, None, None, None

        ph = float(self.prev_high) if np.isfinite(float(self.prev_high)) else 0.0
        pl = float(self.prev_low) if np.isfinite(float(self.prev_low)) else 0.0
        pc = float(self.prev_close) if np.isfinite(float(self.prev_close)) else 0.0

        true_range = max(max(h - l, abs(h - pc)), abs(l - pc))
        dm_plus = (max(h - ph, 0.0) if ((h - ph) > (pl - l)) else 0.0)
        dm_minus = (max(pl - l, 0.0) if ((pl - l) > (h - ph)) else 0.0)

        sm_tr = float(self.sm_tr.preview(true_range))
        sm_p = float(self.sm_pdm.preview(dm_plus))
        sm_m = float(self.sm_mdm.preview(dm_minus))

        di_p = float("nan")
        di_m = float("nan")
        if np.isfinite(sm_tr) and sm_tr != 0.0 and np.isfinite(sm_p) and np.isfinite(sm_m):
            di_p = 100.0 * sm_p / sm_tr
            di_m = 100.0 * sm_m / sm_tr

        dx = float("nan")
        if np.isfinite(di_p) and np.isfinite(di_m):
            denom = float(di_p) + float(di_m)
            if denom != 0.0:
                dx = 100.0 * abs(float(di_p) - float(di_m)) / denom

        adx_val = float(self.sma_adx.preview(dx)) if np.isfinite(dx) else float("nan")

        adx = float(adx_val) if np.isfinite(adx_val) else None
        dip = float(di_p) if np.isfinite(di_p) else None
        dim = float(di_m) if np.isfinite(di_m) else None
        dis = (float(dip - dim) if (dip is not None and dim is not None) else None)

        ang = None
        try:
            prev_adx = float(self.adx)
        except Exception:
            prev_adx = float("nan")
        if adx is not None and np.isfinite(prev_adx):
            ang = "GREEN" if float(adx) > float(prev_adx) else "RED"

        return adx, dip, dim, dis, ang

    def snapshot(self) -> Tuple[Optional[float], Optional[float], Optional[float], Optional[float], Optional[str]]:
        """Return (adx, di_plus, di_minus, di_spread, adx_angle) as optional values."""
        adx = float(self.adx) if np.isfinite(float(self.adx)) else None
        dip = float(self.di_plus) if np.isfinite(float(self.di_plus)) else None
        dim = float(self.di_minus) if np.isfinite(float(self.di_minus)) else None
        dis = (float(dip - dim) if (dip is not None and dim is not None) else None)
        ang = self.adx_angle if self.adx_angle in ("GREEN", "RED") else None
        return adx, dip, dim, dis, ang


class _WilderATRState:
    """TradingView/Wilder ATR computed on CLOSED bars (ta.atr semantics).

    ATR = ta.rma(ta.tr, len)

    This state uses SMA seed for the first `len` samples, matching TradingView's ta.rma.
    """
    __slots__ = ("length", "prev_close", "rma_tr", "atr", "atr_angle")

    def __init__(self, length: int = ATR_LEN):
        self.length = int(length)
        self.prev_close = float("nan")
        self.rma_tr = _RMAState(self.length)
        self.atr = float("nan")
        self.atr_angle = None  # "GREEN" if ATR rising vs prev closed bar, else "RED" (or None if unknown)

    def update(self, high: float, low: float, close: float) -> float:
        """Commit a CLOSED bar (high/low/close). Returns atr."""
        try:
            h = float(high); l = float(low); c = float(close)
        except Exception:
            return float("nan")

        if not np.isfinite(h) or not np.isfinite(l) or not np.isfinite(c):
            return float("nan")

        # First bar: seed prev_close, TR = high-low
        if not np.isfinite(self.prev_close):
            tr = max(0.0, h - l)
            atr_val = float(self.rma_tr.update(tr))
            self.prev_close = c
            self.atr = atr_val
            self.atr_angle = None
            return float(self.atr)

        tr = max(
            h - l,
            abs(h - float(self.prev_close)),
            abs(l - float(self.prev_close)),
        )

        atr_val = float(self.rma_tr.update(tr))

        try:
            prev_atr = float(self.atr)
        except Exception:
            prev_atr = float("nan")

        if np.isfinite(atr_val) and np.isfinite(prev_atr):
            self.atr_angle = "GREEN" if float(atr_val) > float(prev_atr) else "RED"
        else:
            self.atr_angle = None

        self.atr = atr_val
        self.prev_close = c
        return float(self.atr)

    def preview(self, high: float, low: float, close: float) -> Tuple[Optional[float], Optional[str]]:
        """Preview ATR using a FORMING bar without mutating state."""
        try:
            h = float(high); l = float(low); c = float(close)
        except Exception:
            return None, None

        if not np.isfinite(h) or not np.isfinite(l) or not np.isfinite(c):
            return None, None

        if not np.isfinite(self.prev_close):
            return None, None

        tr = max(
            h - l,
            abs(h - float(self.prev_close)),
            abs(l - float(self.prev_close)),
        )

        atr_val = float(self.rma_tr.preview(tr))
        atr = float(atr_val) if np.isfinite(atr_val) else None

        ang = None
        try:
            prev_atr = float(self.atr)
        except Exception:
            prev_atr = float("nan")
        if atr is not None and np.isfinite(prev_atr):
            ang = "GREEN" if float(atr) > float(prev_atr) else "RED"

        return atr, ang

    def snapshot(self) -> Tuple[Optional[float], Optional[str]]:
        atr = float(self.atr) if np.isfinite(float(self.atr)) else None
        ang = self.atr_angle if self.atr_angle in ("GREEN", "RED") else None
        return atr, ang



def make_adx_state(adx_impl: str, length: int = ADX_LEN):
    """Factory for ADX state objects used by LIVE and history/backtest streams."""
    impl = str(adx_impl or "TRADINGVIEW").upper().strip()
    if impl == "PINE_SMA_DX":
        return _PineADXState(int(length))
    # default
    return _WilderADXState(int(length))


def make_atr_state(length: int = ATR_LEN):
    """Factory for ATR state objects used by LIVE and history/backtest streams."""
    return _WilderATRState(int(length))


class _FramaChannelState:
    """Incremental FRAMA Channel state matching the supplied TradingView script.

    The implementation follows the Pine logic exactly for the default script behavior:
      - source price = hl2
      - volatility = sma(high-low, 200)
      - fractal dimension from N1/N2/N3 windows
      - alpha clamp to [0.01, 1.0]
      - recursive filter uses previous *final* Filt value
      - final Filt = sma((bar_index < N + 1) ? price : raw_filt, 5)
      - state changes use bar-close semantics
    """

    __slots__ = (
        "length", "distance", "half", "bar_index",
        "highs", "lows", "ranges", "src5",
        "filt", "filt_upper", "filt_lower", "alpha",
        "state", "break_up", "break_down", "mid_cross",
        "up_count", "down_count",
        "prev_close", "prev_hlc3", "prev_filt", "prev_upper", "prev_lower",
    )

    def __init__(self, length: int = FRAMA_LEN, distance: float = FRAMA_BANDS_DISTANCE):
        try:
            length_i = int(length)
        except Exception:
            length_i = int(FRAMA_LEN)
        if length_i < 2:
            length_i = 2
        if length_i % 2 != 0:
            length_i += 1
        self.length = int(length_i)
        self.distance = float(distance)
        self.half = int(self.length // 2)
        self.bar_index = -1
        self.highs = collections.deque(maxlen=max(self.length, 8))
        self.lows = collections.deque(maxlen=max(self.length, 8))
        self.ranges = collections.deque(maxlen=200)
        self.src5 = collections.deque(maxlen=5)
        self.filt = float("nan")
        self.filt_upper = float("nan")
        self.filt_lower = float("nan")
        self.alpha = float("nan")
        self.state = None  # BUY / SELL / NEUTRAL / None
        self.break_up = False
        self.break_down = False
        self.mid_cross = False
        self.up_count = 0
        self.down_count = 0
        self.prev_close = float("nan")
        self.prev_hlc3 = float("nan")
        self.prev_filt = float("nan")
        self.prev_upper = float("nan")
        self.prev_lower = float("nan")

    @staticmethod
    def _cross(prev_a: float, prev_b: float, cur_a: float, cur_b: float) -> bool:
        if not (np.isfinite(prev_a) and np.isfinite(prev_b) and np.isfinite(cur_a) and np.isfinite(cur_b)):
            return False
        return ((cur_a > cur_b) and (prev_a <= prev_b)) or ((cur_a < cur_b) and (prev_a >= prev_b))

    @staticmethod
    def _crossover(prev_a: float, prev_b: float, cur_a: float, cur_b: float) -> bool:
        if not (np.isfinite(prev_a) and np.isfinite(prev_b) and np.isfinite(cur_a) and np.isfinite(cur_b)):
            return False
        return (prev_a <= prev_b) and (cur_a > cur_b)

    @staticmethod
    def _crossunder(prev_a: float, prev_b: float, cur_a: float, cur_b: float) -> bool:
        if not (np.isfinite(prev_a) and np.isfinite(prev_b) and np.isfinite(cur_a) and np.isfinite(cur_b)):
            return False
        return (prev_a >= prev_b) and (cur_a < cur_b)

    def _gate_directional_triggers(self, raw_break_up: bool, raw_break_down: bool) -> Tuple[bool, bool]:
        """Match the Pine label logic: only the first directional break after the opposite side labels."""
        trigger_up = False
        trigger_down = False
        if bool(raw_break_up):
            self.down_count = 0
            self.up_count = int(self.up_count) + 1
            trigger_up = self.up_count == 1
        if bool(raw_break_down):
            self.up_count = 0
            self.down_count = int(self.down_count) + 1
            trigger_down = self.down_count == 1
        return bool(trigger_up), bool(trigger_down)

    def update(self, high: float, low: float, close: float) -> Dict[str, Any]:
        try:
            h = float(high); l = float(low); c = float(close)
        except Exception:
            return self.snapshot()
        if not (np.isfinite(h) and np.isfinite(l) and np.isfinite(c)):
            return self.snapshot()

        self.bar_index += 1
        price = (h + l) / 2.0
        hlc3 = (h + l + c) / 3.0
        bar_range = max(0.0, h - l)

        self.highs.append(h)
        self.lows.append(l)
        self.ranges.append(bar_range)

        volatility = float("nan")
        if len(self.ranges) >= 200:
            volatility = float(sum(self.ranges) / 200.0)

        dimen = float("nan")
        if len(self.highs) >= self.length and len(self.lows) >= self.length:
            highs = list(self.highs)[-self.length:]
            lows = list(self.lows)[-self.length:]
            n3 = (max(highs) - min(lows)) / float(self.length)
            highs_rev = highs[::-1]
            lows_rev = lows[::-1]
            n1 = (max(highs_rev[:self.half]) - min(lows_rev[:self.half])) / float(self.half)
            n2 = (max(highs_rev[self.half:self.length]) - min(lows_rev[self.half:self.length])) / float(self.half)
            if n1 > 0.0 and n2 > 0.0 and n3 > 0.0:
                try:
                    dimen = (np.log(n1 + n2) - np.log(n3)) / np.log(2.0)
                except Exception:
                    dimen = float("nan")

        alpha = float("nan")
        if np.isfinite(dimen):
            try:
                alpha = float(np.exp(-4.6 * (float(dimen) - 1.0)))
            except Exception:
                alpha = float("nan")
        if np.isfinite(alpha):
            alpha = max(min(alpha, 1.0), 0.01)
        self.alpha = alpha

        prev_final = float(self.filt)
        raw_filt = price if not np.isfinite(prev_final) or (not np.isfinite(alpha)) else (alpha * price + (1.0 - alpha) * prev_final)

        src = price if self.bar_index < (self.length + 1) else raw_filt
        self.src5.append(src)
        filt = float("nan")
        if len(self.src5) >= 5:
            try:
                filt = float(sum(self.src5) / 5.0)
            except Exception:
                filt = float("nan")

        upper = float("nan")
        lower = float("nan")
        if np.isfinite(filt) and np.isfinite(volatility):
            upper = float(filt + volatility * self.distance)
            lower = float(filt - volatility * self.distance)

        mid_cross = self._cross(float(self.prev_close), float(self.prev_filt), c, filt)
        raw_break_up = self._crossover(float(self.prev_hlc3), float(self.prev_upper), hlc3, upper)
        raw_break_down = self._crossunder(float(self.prev_hlc3), float(self.prev_lower), hlc3, lower)

        state = self.state
        if mid_cross:
            state = "NEUTRAL"
        if raw_break_up:
            state = "BUY"
        if raw_break_down:
            state = "SELL"
        break_up, break_down = self._gate_directional_triggers(raw_break_up, raw_break_down)

        self.filt = filt
        self.filt_upper = upper
        self.filt_lower = lower
        self.mid_cross = bool(mid_cross)
        self.break_up = bool(break_up)
        self.break_down = bool(break_down)
        self.state = state
        self.prev_close = c
        self.prev_hlc3 = hlc3
        self.prev_filt = filt
        self.prev_upper = upper
        self.prev_lower = lower
        return self.snapshot()

    def snapshot(self) -> Dict[str, Any]:
        return {
            "frama": (float(self.filt) if np.isfinite(float(self.filt)) else None),
            "frama_upper": (float(self.filt_upper) if np.isfinite(float(self.filt_upper)) else None),
            "frama_lower": (float(self.filt_lower) if np.isfinite(float(self.filt_lower)) else None),
            "frama_alpha": (float(self.alpha) if np.isfinite(float(self.alpha)) else None),
            "frama_state": (self.state if self.state in ("BUY", "SELL", "NEUTRAL") else None),
            "frama_break_up": bool(self.break_up),
            "frama_break_down": bool(self.break_down),
            "frama_mid_cross": bool(self.mid_cross),
        }


def make_frama_state(length: int = FRAMA_LEN, distance: float = FRAMA_BANDS_DISTANCE):
    return _FramaChannelState(length=length, distance=distance)


def compute_frama_channel_series(high: np.ndarray, low: np.ndarray, close: np.ndarray, length: int = FRAMA_LEN, distance: float = FRAMA_BANDS_DISTANCE) -> Dict[str, np.ndarray]:
    """Compute FRAMA Channel series exactly from bar-close OHLC arrays.

    Returns arrays aligned one-for-one with the input bars. State is object dtype with
    values BUY / SELL / NEUTRAL / None. Event arrays are boolean pulses.
    """
    h = np.asarray(high, dtype=float)
    l = np.asarray(low, dtype=float)
    c = np.asarray(close, dtype=float)
    n = len(c)
    out = {
        "frama": np.full(n, np.nan, dtype=float),
        "frama_upper": np.full(n, np.nan, dtype=float),
        "frama_lower": np.full(n, np.nan, dtype=float),
        "frama_alpha": np.full(n, np.nan, dtype=float),
        "frama_state": np.empty(n, dtype=object),
        "frama_break_up": np.zeros(n, dtype=bool),
        "frama_break_down": np.zeros(n, dtype=bool),
        "frama_mid_cross": np.zeros(n, dtype=bool),
    }
    out["frama_state"][:] = None
    st = make_frama_state(length=length, distance=distance)
    for i in range(n):
        snap = st.update(h[i], l[i], c[i])
        if snap.get("frama") is not None:
            out["frama"][i] = float(snap["frama"])
        if snap.get("frama_upper") is not None:
            out["frama_upper"][i] = float(snap["frama_upper"])
        if snap.get("frama_lower") is not None:
            out["frama_lower"][i] = float(snap["frama_lower"])
        if snap.get("frama_alpha") is not None:
            out["frama_alpha"][i] = float(snap["frama_alpha"])
        out["frama_state"][i] = snap.get("frama_state")
        out["frama_break_up"][i] = bool(snap.get("frama_break_up"))
        out["frama_break_down"][i] = bool(snap.get("frama_break_down"))
        out["frama_mid_cross"][i] = bool(snap.get("frama_mid_cross"))
    return out



class _VolumaticVidyaState:
    """Incremental closed-bar state for BigBeluga's Volumatic VIDYA indicator.

    This class reproduces the core TradingView behavior that matters for rules:
      - VIDYA calculation uses the exact CMO-style momentum weighting from the Pine script
      - final VIDYA value is smoothed with SMA(15)
      - ATR uses TradingView/Wilder ta.atr(200)
      - trend flips only when source crosses the outer upper/lower bands
      - the displayed trend line (smoothed_value) is set to na on the trend-change bar
      - trend-volume counters preserve the Pine script's unusual ta.change(...) reset logic

    Phase 2 materializes the Pine script's liquidity-pivot lifecycle into machine-usable
    outputs while keeping the visible UI light. We do not draw TradingView objects here, but we
    do preserve their behavioral role: active low/high liquidity zones are created from confirmed
    pivots, remain active for at most 50 bars, and emit one-bar events when the current smoothed
    VIDYA line sweeps through them.
    """

    __slots__ = (
        'vidya_length', 'vidya_momentum', 'band_distance', 'pivot_left_bars', 'pivot_right_bars',
        'prev_source', 'prev_close', 'prev_upper', 'prev_lower', 'prev_smoothed_value',
        'atr_rma', 'moments', 'vidya_sma', 'vidya_raw',
        'is_trend_up', 'smoothed_value', 'vidya_value', 'upper_band', 'lower_band',
        'trend_cross_up', 'trend_cross_down', 'prev_trend_cross_up', 'prev_trend_cross_down',
        'up_trend_volume', 'down_trend_volume', 'delta_volume_pct',
        'bar_index', 'high_hist', 'low_hist', 'close_hist', 'volume_hist', 'smoothed_hist',
        'liquidity_lines_low', 'liquidity_lines_high', 'volume_value',
        'new_liq_low', 'new_liq_high', 'liq_low_taken', 'liq_high_taken',
        'active_liq_low_count', 'active_liq_high_count',
        'nearest_liq_low', 'nearest_liq_high', 'dist_to_liq_low_pct', 'dist_to_liq_high_pct',
        'last_liq_low_taken_price', 'last_liq_high_taken_price',
        'last_liq_low_taken_volume', 'last_liq_high_taken_volume',
        'vidya_slope', 'vidya_angle_deg', 'vidya_angle_deg_norm', 'vidya_angle_state', 'vidya_angle_accel',
        'prev_vidya_angle_deg_norm'
    )

    def __init__(
        self,
        vidya_length: int = VIDYA_LENGTH,
        vidya_momentum: int = VIDYA_MOMENTUM,
        band_distance: float = VIDYA_BAND_DISTANCE,
        pivot_left_bars: int = VIDYA_PIVOT_LEFT,
        pivot_right_bars: int = VIDYA_PIVOT_RIGHT,
    ):
        try:
            self.vidya_length = max(1, int(vidya_length))
        except Exception:
            self.vidya_length = int(VIDYA_LENGTH)
        try:
            self.vidya_momentum = max(1, int(vidya_momentum))
        except Exception:
            self.vidya_momentum = int(VIDYA_MOMENTUM)
        try:
            self.band_distance = float(band_distance)
        except Exception:
            self.band_distance = float(VIDYA_BAND_DISTANCE)
        self.pivot_left_bars = int(pivot_left_bars)
        self.pivot_right_bars = int(pivot_right_bars)

        self.prev_source = float('nan')
        self.prev_close = float('nan')
        self.prev_upper = float('nan')
        self.prev_lower = float('nan')
        self.prev_smoothed_value = float('nan')

        self.atr_rma = _RMAState(200)
        self.moments = collections.deque(maxlen=max(self.vidya_momentum, 4))
        self.vidya_sma = _SMAState(15)
        self.vidya_raw = 0.0  # Pine: var float vidya_value = 0.0

        self.is_trend_up = False
        self.smoothed_value = float('nan')
        self.vidya_value = float('nan')
        self.upper_band = float('nan')
        self.lower_band = float('nan')
        self.trend_cross_up = False
        self.trend_cross_down = False
        self.prev_trend_cross_up = False
        self.prev_trend_cross_down = False
        self.up_trend_volume = 0.0
        self.down_trend_volume = 0.0
        self.delta_volume_pct = 0.0

        self.bar_index = -1
        max_hist = max(32, int(self.pivot_left_bars + self.pivot_right_bars + 8))
        self.high_hist = collections.deque(maxlen=max_hist)
        self.low_hist = collections.deque(maxlen=max_hist)
        self.close_hist = collections.deque(maxlen=max_hist)
        self.volume_hist = collections.deque(maxlen=max_hist)
        self.smoothed_hist = collections.deque(maxlen=max_hist)
        self.liquidity_lines_low = []
        self.liquidity_lines_high = []
        self.volume_value = float('nan')

        self.new_liq_low = False
        self.new_liq_high = False
        self.liq_low_taken = False
        self.liq_high_taken = False
        self.active_liq_low_count = 0
        self.active_liq_high_count = 0
        self.nearest_liq_low = float('nan')
        self.nearest_liq_high = float('nan')
        self.dist_to_liq_low_pct = float('nan')
        self.dist_to_liq_high_pct = float('nan')
        self.last_liq_low_taken_price = float('nan')
        self.last_liq_high_taken_price = float('nan')
        self.last_liq_low_taken_volume = float('nan')
        self.last_liq_high_taken_volume = float('nan')
        self.vidya_slope = float('nan')
        self.vidya_angle_deg = float('nan')
        self.vidya_angle_deg_norm = float('nan')
        self.vidya_angle_state = None
        self.vidya_angle_accel = None
        self.prev_vidya_angle_deg_norm = float('nan')

    @staticmethod
    def _crossover(prev_a: float, prev_b: float, cur_a: float, cur_b: float) -> bool:
        if not (np.isfinite(prev_a) and np.isfinite(prev_b) and np.isfinite(cur_a) and np.isfinite(cur_b)):
            return False
        return (prev_a <= prev_b) and (cur_a > cur_b)

    @staticmethod
    def _crossunder(prev_a: float, prev_b: float, cur_a: float, cur_b: float) -> bool:
        if not (np.isfinite(prev_a) and np.isfinite(prev_b) and np.isfinite(cur_a) and np.isfinite(cur_b)):
            return False
        return (prev_a >= prev_b) and (cur_a < cur_b)

    @staticmethod
    def _pivot_high_ready(window: list[float], left: int, right: int) -> bool:
        try:
            if len(window) < int(left + right + 1):
                return False
            c = float(window[-right - 1])
            if not np.isfinite(c):
                return False
            left_vals = [float(x) for x in window[-right - 1 - left:-right - 1] if np.isfinite(float(x))]
            right_vals = [float(x) for x in window[-right:] if np.isfinite(float(x))] if right > 0 else []
            if len(left_vals) < left or len(right_vals) < right:
                return False
            return (c >= max(left_vals)) and (c > max(right_vals))
        except Exception:
            return False

    @staticmethod
    def _pivot_low_ready(window: list[float], left: int, right: int) -> bool:
        try:
            if len(window) < int(left + right + 1):
                return False
            c = float(window[-right - 1])
            if not np.isfinite(c):
                return False
            left_vals = [float(x) for x in window[-right - 1 - left:-right - 1] if np.isfinite(float(x))]
            right_vals = [float(x) for x in window[-right:] if np.isfinite(float(x))] if right > 0 else []
            if len(left_vals) < left or len(right_vals) < right:
                return False
            return (c <= min(left_vals)) and (c < min(right_vals))
        except Exception:
            return False

    @staticmethod
    def _dist_pct(price: float, level: float) -> float:
        try:
            p = float(price); lv = float(level)
            if np.isfinite(p) and np.isfinite(lv) and p != 0.0:
                return abs(lv - p) / abs(p) * 100.0
        except Exception:
            pass
        return float('nan')

    def _prune_expired_lines(self, lines: list[dict[str, float | int]]):
        pruned = []
        for line in list(lines):
            try:
                age = int(self.bar_index - int(line.get('x1', self.bar_index)))
            except Exception:
                age = 999999
            if age < 50:
                pruned.append(line)
        lines[:] = pruned

    def _take_crossed_lines(self, lines: list[dict[str, float | int]], price_level: float, prev_price_level: float, is_cross: bool):
        taken = []
        keep = []
        for line in list(lines):
            try:
                lvl = float(line.get('level'))
                x1 = int(line.get('x1', self.bar_index))
            except Exception:
                continue
            is_short_line = (self.bar_index - x1) < 50
            if not is_short_line:
                keep.append(line)
                continue
            if is_cross:
                price_cross = np.isfinite(price_level) and np.isfinite(prev_price_level) and (price_level < lvl) and (prev_price_level >= lvl)
            else:
                price_cross = np.isfinite(price_level) and np.isfinite(prev_price_level) and (price_level > lvl) and (prev_price_level <= lvl)
            if price_cross:
                taken.append(line)
            else:
                keep.append(line)
        lines[:] = keep
        return taken

    def _update_liquidity_metrics(self, close_price: float):
        self._prune_expired_lines(self.liquidity_lines_low)
        self._prune_expired_lines(self.liquidity_lines_high)
        self.active_liq_low_count = int(len(self.liquidity_lines_low))
        self.active_liq_high_count = int(len(self.liquidity_lines_high))

        low_levels = []
        high_levels = []
        for line in self.liquidity_lines_low:
            try:
                lv = float(line.get('level'))
                if np.isfinite(lv):
                    low_levels.append(lv)
            except Exception:
                pass
        for line in self.liquidity_lines_high:
            try:
                lv = float(line.get('level'))
                if np.isfinite(lv):
                    high_levels.append(lv)
            except Exception:
                pass

        cur = float(close_price) if np.isfinite(float(close_price)) else float('nan')
        self.nearest_liq_low = max(low_levels) if low_levels else float('nan')
        self.nearest_liq_high = min(high_levels) if high_levels else float('nan')
        self.dist_to_liq_low_pct = self._dist_pct(cur, self.nearest_liq_low)
        self.dist_to_liq_high_pct = self._dist_pct(cur, self.nearest_liq_high)

    def update(self, open_: float, high: float, low: float, close: float, volume: float) -> Dict[str, Any]:
        try:
            o = float(open_); h = float(high); l = float(low); c = float(close); v = float(volume)
        except Exception:
            return self.snapshot()
        if not (np.isfinite(o) and np.isfinite(h) and np.isfinite(l) and np.isfinite(c)):
            return self.snapshot()
        if not np.isfinite(v):
            v = 0.0

        self.bar_index += 1
        self.new_liq_low = False
        self.new_liq_high = False
        self.liq_low_taken = False
        self.liq_high_taken = False

        tr = float('nan')
        if np.isfinite(self.prev_close):
            tr = max(h - l, abs(h - float(self.prev_close)), abs(l - float(self.prev_close)))
        else:
            tr = max(0.0, h - l)
        atr_value = float(self.atr_rma.update(tr)) if np.isfinite(tr) else float('nan')

        source = float(c)
        if np.isfinite(self.prev_source):
            momentum = float(source - float(self.prev_source))
        else:
            momentum = 0.0
        self.moments.append(momentum)
        sum_pos_momentum = float(sum(m for m in self.moments if np.isfinite(m) and m >= 0.0))
        sum_neg_momentum = float(sum((-m) for m in self.moments if np.isfinite(m) and m < 0.0))
        denom = float(sum_pos_momentum + sum_neg_momentum)
        if denom > 0.0 and np.isfinite(denom):
            abs_cmo = abs(100.0 * (sum_pos_momentum - sum_neg_momentum) / denom)
        else:
            abs_cmo = 0.0

        alpha = 2.0 / float(self.vidya_length + 1)
        coeff = float(alpha * abs_cmo / 100.0)
        coeff = max(0.0, min(1.0, coeff)) if np.isfinite(coeff) else 0.0

        prev_raw = float(self.vidya_raw) if np.isfinite(float(self.vidya_raw)) else 0.0
        raw_vidya = float(coeff * source + (1.0 - coeff) * prev_raw)
        self.vidya_raw = raw_vidya
        vidya_value = float(self.vidya_sma.commit(raw_vidya))

        upper_band = float('nan')
        lower_band = float('nan')
        if np.isfinite(vidya_value) and np.isfinite(atr_value):
            upper_band = float(vidya_value + atr_value * self.band_distance)
            lower_band = float(vidya_value - atr_value * self.band_distance)

        prev_trend = bool(self.is_trend_up)
        if self._crossover(float(self.prev_source), float(self.prev_upper), source, upper_band):
            self.is_trend_up = True
        if self._crossunder(float(self.prev_source), float(self.prev_lower), source, lower_band):
            self.is_trend_up = False

        smoothed_value = float(lower_band) if bool(self.is_trend_up) else float(upper_band)
        trend_cross_up = (not prev_trend) and bool(self.is_trend_up)
        trend_cross_down = (not bool(self.is_trend_up)) and prev_trend
        if bool(self.is_trend_up) != prev_trend:
            smoothed_value = float('nan')

        change_up = bool(trend_cross_up) != bool(self.prev_trend_cross_up)
        change_down = bool(trend_cross_down) != bool(self.prev_trend_cross_down)
        if change_up or change_down:
            self.up_trend_volume = 0.0
            self.down_trend_volume = 0.0
        else:
            if c > o:
                self.up_trend_volume = float(self.up_trend_volume + v)
            if c < o:
                self.down_trend_volume = float(self.down_trend_volume + v)

        avg_volume_delta = float(self.up_trend_volume + self.down_trend_volume) / 2.0
        if avg_volume_delta > 0.0 and np.isfinite(avg_volume_delta):
            delta_pct = float((float(self.up_trend_volume) - float(self.down_trend_volume)) / avg_volume_delta * 100.0)
        else:
            delta_pct = 0.0

        self.high_hist.append(h)
        self.low_hist.append(l)
        self.close_hist.append(c)
        self.volume_hist.append(v)
        self.smoothed_hist.append(smoothed_value)

        window_len = int(self.pivot_left_bars + self.pivot_right_bars + 1)
        pivot_high = self._pivot_high_ready(list(self.high_hist), self.pivot_left_bars, self.pivot_right_bars)
        pivot_low = self._pivot_low_ready(list(self.close_hist), self.pivot_left_bars, self.pivot_right_bars)

        sum_len = int(self.pivot_right_bars + self.pivot_left_bars)
        if len(self.volume_hist) >= sum_len and sum_len > 0:
            try:
                recent = list(self.volume_hist)[-sum_len:]
                self.volume_value = float(sum(float(x) for x in recent) / float(sum_len))
            except Exception:
                pass

        prev_smoothed = float(self.prev_smoothed_value) if np.isfinite(float(self.prev_smoothed_value)) else float('nan')
        if pivot_low and len(self.low_hist) >= window_len and np.isfinite(smoothed_value):
            pivot_low_price = float(list(self.low_hist)[-self.pivot_right_bars - 1])
            if np.isfinite(pivot_low_price) and pivot_low_price > float(smoothed_value):
                line = {
                    'x1': int(self.bar_index - self.pivot_right_bars),
                    'x2': int(self.bar_index - self.pivot_right_bars + 5),
                    'level': float(pivot_low_price),
                    'volume': float(self.volume_value) if np.isfinite(float(self.volume_value)) else float('nan'),
                }
                self.liquidity_lines_low.append(line)
                self.new_liq_low = True

        if pivot_high and len(self.high_hist) >= window_len and np.isfinite(smoothed_value):
            pivot_high_price = float(list(self.high_hist)[-self.pivot_right_bars - 1])
            if np.isfinite(pivot_high_price) and pivot_high_price < float(smoothed_value):
                line = {
                    'x1': int(self.bar_index - self.pivot_right_bars),
                    'x2': int(self.bar_index - self.pivot_right_bars + 5),
                    'level': float(pivot_high_price),
                    'volume': (-float(self.volume_value) if np.isfinite(float(self.volume_value)) else float('nan')),
                }
                self.liquidity_lines_high.append(line)
                self.new_liq_high = True

        taken_high = self._take_crossed_lines(self.liquidity_lines_high, smoothed_value, prev_smoothed, True)
        taken_low = self._take_crossed_lines(self.liquidity_lines_low, smoothed_value, prev_smoothed, False)
        if taken_high:
            self.liq_high_taken = True
            try:
                last_high = taken_high[-1]
                self.last_liq_high_taken_price = float(last_high.get('level'))
                self.last_liq_high_taken_volume = float(self.volume_value) if np.isfinite(float(self.volume_value)) else float(last_high.get('volume'))
            except Exception:
                pass
        if taken_low:
            self.liq_low_taken = True
            try:
                last_low = taken_low[-1]
                self.last_liq_low_taken_price = float(last_low.get('level'))
                self.last_liq_low_taken_volume = float(self.volume_value) if np.isfinite(float(self.volume_value)) else float(last_low.get('volume'))
            except Exception:
                pass

        self.vidya_value = vidya_value
        self.upper_band = upper_band
        self.lower_band = lower_band
        self.smoothed_value = smoothed_value
        self.trend_cross_up = bool(trend_cross_up)
        self.trend_cross_down = bool(trend_cross_down)
        self.prev_trend_cross_up = bool(trend_cross_up)
        self.prev_trend_cross_down = bool(trend_cross_down)
        self.delta_volume_pct = delta_pct

        active_line = float(smoothed_value) if np.isfinite(float(smoothed_value)) else float('nan')
        prev_line = float(prev_smoothed) if np.isfinite(float(prev_smoothed)) else float('nan')
        slope = float('nan')
        angle_deg = float('nan')
        angle_deg_norm = float('nan')
        angle_state = None
        angle_accel = None
        if np.isfinite(active_line) and np.isfinite(prev_line):
            slope = float(active_line - prev_line)
            try:
                angle_deg = float(np.degrees(np.arctan(float(slope))))
            except Exception:
                angle_deg = float('nan')
            if np.isfinite(float(atr_value)) and abs(float(atr_value)) > 1e-12:
                try:
                    angle_deg_norm = float(np.degrees(np.arctan(float(slope) / float(atr_value))))
                except Exception:
                    angle_deg_norm = float('nan')
            if np.isfinite(float(angle_deg_norm)):
                if float(angle_deg_norm) > 2.0:
                    angle_state = 'UP'
                elif float(angle_deg_norm) < -2.0:
                    angle_state = 'DOWN'
                else:
                    angle_state = 'FLAT'
                prev_angle = float(self.prev_vidya_angle_deg_norm) if np.isfinite(float(self.prev_vidya_angle_deg_norm)) else float('nan')
                if np.isfinite(prev_angle):
                    delta_ang = float(angle_deg_norm - prev_angle)
                    if delta_ang > 1.5:
                        angle_accel = 'ACCEL'
                    elif delta_ang < -1.5:
                        angle_accel = 'DECEL'
                    else:
                        angle_accel = 'STABLE'
        self.vidya_slope = slope
        self.vidya_angle_deg = angle_deg
        self.vidya_angle_deg_norm = angle_deg_norm
        self.vidya_angle_state = angle_state
        self.vidya_angle_accel = angle_accel
        self.prev_vidya_angle_deg_norm = angle_deg_norm

        self.prev_source = source
        self.prev_close = c
        self.prev_upper = upper_band
        self.prev_lower = lower_band
        self.prev_smoothed_value = smoothed_value
        self._update_liquidity_metrics(c)
        return self.snapshot()

    def snapshot(self) -> Dict[str, Any]:
        state = None
        if np.isfinite(float(self.upper_band)) and np.isfinite(float(self.lower_band)):
            state = 'BUY' if bool(self.is_trend_up) else 'SELL'
        return {
            'vidya_line': (float(self.smoothed_value) if np.isfinite(float(self.smoothed_value)) else None),
            'vidya_raw': (float(self.vidya_value) if np.isfinite(float(self.vidya_value)) else None),
            'vidya_upper': (float(self.upper_band) if np.isfinite(float(self.upper_band)) else None),
            'vidya_lower': (float(self.lower_band) if np.isfinite(float(self.lower_band)) else None),
            'vidya_state': state,
            'vidya_trend_up': bool(self.trend_cross_up),
            'vidya_trend_down': bool(self.trend_cross_down),
            'vidya_up_volume': float(self.up_trend_volume),
            'vidya_down_volume': float(self.down_trend_volume),
            'vidya_delta_pct': float(self.delta_volume_pct),
            'vidya_slope': (float(self.vidya_slope) if np.isfinite(float(self.vidya_slope)) else None),
            'vidya_angle_deg': (float(self.vidya_angle_deg) if np.isfinite(float(self.vidya_angle_deg)) else None),
            'vidya_angle_deg_norm': (float(self.vidya_angle_deg_norm) if np.isfinite(float(self.vidya_angle_deg_norm)) else None),
            'vidya_angle_state': (self.vidya_angle_state if self.vidya_angle_state in VIDYA_ANGLE_STATES else None),
            'vidya_angle_accel': (self.vidya_angle_accel if self.vidya_angle_accel in VIDYA_ANGLE_ACCEL_STATES else None),
            'vidya_active_liq_low_count': int(self.active_liq_low_count),
            'vidya_active_liq_high_count': int(self.active_liq_high_count),
            'vidya_nearest_liq_low': (float(self.nearest_liq_low) if np.isfinite(float(self.nearest_liq_low)) else None),
            'vidya_nearest_liq_high': (float(self.nearest_liq_high) if np.isfinite(float(self.nearest_liq_high)) else None),
            'vidya_dist_to_liq_low_pct': (float(self.dist_to_liq_low_pct) if np.isfinite(float(self.dist_to_liq_low_pct)) else None),
            'vidya_dist_to_liq_high_pct': (float(self.dist_to_liq_high_pct) if np.isfinite(float(self.dist_to_liq_high_pct)) else None),
            'vidya_new_liq_low': bool(self.new_liq_low),
            'vidya_new_liq_high': bool(self.new_liq_high),
            'vidya_liq_low_taken': bool(self.liq_low_taken),
            'vidya_liq_high_taken': bool(self.liq_high_taken),
            'vidya_last_liq_low_taken_price': (float(self.last_liq_low_taken_price) if np.isfinite(float(self.last_liq_low_taken_price)) else None),
            'vidya_last_liq_high_taken_price': (float(self.last_liq_high_taken_price) if np.isfinite(float(self.last_liq_high_taken_price)) else None),
            'vidya_last_liq_low_taken_volume': (float(self.last_liq_low_taken_volume) if np.isfinite(float(self.last_liq_low_taken_volume)) else None),
            'vidya_last_liq_high_taken_volume': (float(self.last_liq_high_taken_volume) if np.isfinite(float(self.last_liq_high_taken_volume)) else None),
        }


def make_vidya_state(
    vidya_length: int = VIDYA_LENGTH,
    vidya_momentum: int = VIDYA_MOMENTUM,
    band_distance: float = VIDYA_BAND_DISTANCE,
    pivot_left_bars: int = VIDYA_PIVOT_LEFT,
    pivot_right_bars: int = VIDYA_PIVOT_RIGHT,
):
    return _VolumaticVidyaState(
        vidya_length=vidya_length,
        vidya_momentum=vidya_momentum,
        band_distance=band_distance,
        pivot_left_bars=pivot_left_bars,
        pivot_right_bars=pivot_right_bars,
    )


def compute_vidya_series(
    open_: np.ndarray,
    high: np.ndarray,
    low: np.ndarray,
    close: np.ndarray,
    volume: np.ndarray,
    vidya_length: int = VIDYA_LENGTH,
    vidya_momentum: int = VIDYA_MOMENTUM,
    band_distance: float = VIDYA_BAND_DISTANCE,
    pivot_left_bars: int = VIDYA_PIVOT_LEFT,
    pivot_right_bars: int = VIDYA_PIVOT_RIGHT,
) -> Dict[str, np.ndarray]:
    """Compute the core Volumatic VIDYA outputs exactly on closed-bar OHLCV arrays."""
    o = np.asarray(open_, dtype=float)
    h = np.asarray(high, dtype=float)
    l = np.asarray(low, dtype=float)
    c = np.asarray(close, dtype=float)
    v = np.asarray(volume, dtype=float)
    n = len(c)
    out = {
        'vidya_line': np.full(n, np.nan, dtype=float),
        'vidya_raw': np.full(n, np.nan, dtype=float),
        'vidya_upper': np.full(n, np.nan, dtype=float),
        'vidya_lower': np.full(n, np.nan, dtype=float),
        'vidya_state': np.empty(n, dtype=object),
        'vidya_trend_up': np.zeros(n, dtype=bool),
        'vidya_trend_down': np.zeros(n, dtype=bool),
        'vidya_up_volume': np.full(n, np.nan, dtype=float),
        'vidya_down_volume': np.full(n, np.nan, dtype=float),
        'vidya_delta_pct': np.full(n, np.nan, dtype=float),
        'vidya_active_liq_low_count': np.full(n, np.nan, dtype=float),
        'vidya_active_liq_high_count': np.full(n, np.nan, dtype=float),
        'vidya_nearest_liq_low': np.full(n, np.nan, dtype=float),
        'vidya_nearest_liq_high': np.full(n, np.nan, dtype=float),
        'vidya_dist_to_liq_low_pct': np.full(n, np.nan, dtype=float),
        'vidya_dist_to_liq_high_pct': np.full(n, np.nan, dtype=float),
        'vidya_new_liq_low': np.zeros(n, dtype=bool),
        'vidya_new_liq_high': np.zeros(n, dtype=bool),
        'vidya_liq_low_taken': np.zeros(n, dtype=bool),
        'vidya_liq_high_taken': np.zeros(n, dtype=bool),
    }
    out['vidya_state'][:] = None
    st = make_vidya_state(vidya_length=vidya_length, vidya_momentum=vidya_momentum, band_distance=band_distance, pivot_left_bars=pivot_left_bars, pivot_right_bars=pivot_right_bars)
    for i in range(n):
        snap = st.update(o[i], h[i], l[i], c[i], v[i])
        if snap.get('vidya_line') is not None:
            out['vidya_line'][i] = float(snap['vidya_line'])
        if snap.get('vidya_raw') is not None:
            out['vidya_raw'][i] = float(snap['vidya_raw'])
        if snap.get('vidya_upper') is not None:
            out['vidya_upper'][i] = float(snap['vidya_upper'])
        if snap.get('vidya_lower') is not None:
            out['vidya_lower'][i] = float(snap['vidya_lower'])
        out['vidya_state'][i] = snap.get('vidya_state')
        out['vidya_trend_up'][i] = bool(snap.get('vidya_trend_up'))
        out['vidya_trend_down'][i] = bool(snap.get('vidya_trend_down'))
        out['vidya_up_volume'][i] = float(snap.get('vidya_up_volume', 0.0))
        out['vidya_down_volume'][i] = float(snap.get('vidya_down_volume', 0.0))
        out['vidya_delta_pct'][i] = float(snap.get('vidya_delta_pct', 0.0))
        out['vidya_active_liq_low_count'][i] = float(snap.get('vidya_active_liq_low_count', 0.0))
        out['vidya_active_liq_high_count'][i] = float(snap.get('vidya_active_liq_high_count', 0.0))
        if snap.get('vidya_nearest_liq_low') is not None:
            out['vidya_nearest_liq_low'][i] = float(snap.get('vidya_nearest_liq_low'))
        if snap.get('vidya_nearest_liq_high') is not None:
            out['vidya_nearest_liq_high'][i] = float(snap.get('vidya_nearest_liq_high'))
        if snap.get('vidya_dist_to_liq_low_pct') is not None:
            out['vidya_dist_to_liq_low_pct'][i] = float(snap.get('vidya_dist_to_liq_low_pct'))
        if snap.get('vidya_dist_to_liq_high_pct') is not None:
            out['vidya_dist_to_liq_high_pct'][i] = float(snap.get('vidya_dist_to_liq_high_pct'))
        out['vidya_new_liq_low'][i] = bool(snap.get('vidya_new_liq_low', False))
        out['vidya_new_liq_high'][i] = bool(snap.get('vidya_new_liq_high', False))
        out['vidya_liq_low_taken'][i] = bool(snap.get('vidya_liq_low_taken', False))
        out['vidya_liq_high_taken'][i] = bool(snap.get('vidya_liq_high_taken', False))
    return out


class _RangeFilterState:
    """Closed-bar TradingView-exact state for the Range Filter Buy and Sell script."""

    __slots__ = (
        'per', 'mult', 'wper', 'avrng_ema', 'smooth_ema', 'prev_src', 'prev_filt',
        'prev_state_long', 'prev_state_short', 'cond_ini', 'upward', 'downward',
        'filt', 'smrng', 'hband', 'lband', 'phase', 'long_cond', 'short_cond', 'buy_event', 'sell_event'
    )

    def __init__(self, per: int = RANGE_FILTER_PER, mult: float = RANGE_FILTER_MULT):
        try:
            self.per = max(1, int(per))
        except Exception:
            self.per = int(RANGE_FILTER_PER)
        try:
            self.mult = max(0.1, float(mult))
        except Exception:
            self.mult = float(RANGE_FILTER_MULT)
        self.wper = max(1, int(self.per * 2 - 1))
        self.avrng_ema = _EMAState(self.per)
        self.smooth_ema = _EMAState(self.wper)
        self.prev_src = float('nan')
        self.prev_filt = float('nan')
        self.prev_state_long = False
        self.prev_state_short = False
        self.cond_ini = 0
        self.upward = 0.0
        self.downward = 0.0
        self.filt = float('nan')
        self.smrng = float('nan')
        self.hband = float('nan')
        self.lband = float('nan')
        self.phase = 'NEUTRAL'
        self.long_cond = False
        self.short_cond = False
        self.buy_event = False
        self.sell_event = False

    def update(self, source: float) -> Dict[str, Any]:
        try:
            x = float(source)
        except Exception:
            x = float('nan')

        abs_delta = float('nan')
        if np.isfinite(x) and np.isfinite(self.prev_src):
            abs_delta = abs(float(x) - float(self.prev_src))
        avrng = self.avrng_ema.commit(abs_delta)
        smooth_base = self.smooth_ema.commit(avrng)
        smrng = float(smooth_base * self.mult) if np.isfinite(float(smooth_base)) else float('nan')

        prev_filt_nz = float(self.prev_filt) if np.isfinite(float(self.prev_filt)) else 0.0
        if np.isfinite(x) and np.isfinite(smrng):
            if x > prev_filt_nz:
                filt = prev_filt_nz if (x - smrng) < prev_filt_nz else float(x - smrng)
            else:
                filt = prev_filt_nz if (x + smrng) > prev_filt_nz else float(x + smrng)
        else:
            filt = float('nan')

        prev_filt = float(self.prev_filt)
        upward_prev = float(self.upward) if np.isfinite(float(self.upward)) else 0.0
        downward_prev = float(self.downward) if np.isfinite(float(self.downward)) else 0.0
        if np.isfinite(filt) and np.isfinite(prev_filt):
            if filt > prev_filt:
                upward = upward_prev + 1.0
            elif filt < prev_filt:
                upward = 0.0
            else:
                upward = upward_prev
            if filt < prev_filt:
                downward = downward_prev + 1.0
            elif filt > prev_filt:
                downward = 0.0
            else:
                downward = downward_prev
        else:
            upward = upward_prev
            downward = downward_prev

        prev_src = float(self.prev_src)
        buy_strong = bool(
            np.isfinite(x) and np.isfinite(filt) and (x > filt) and (upward > 0.0) and np.isfinite(prev_src) and (x > prev_src)
        )
        buy_weak = bool(
            np.isfinite(x) and np.isfinite(filt) and (x > filt) and (upward > 0.0) and np.isfinite(prev_src) and (x < prev_src)
        )
        sell_strong = bool(
            np.isfinite(x) and np.isfinite(filt) and (x < filt) and (downward > 0.0) and np.isfinite(prev_src) and (x < prev_src)
        )
        sell_weak = bool(
            np.isfinite(x) and np.isfinite(filt) and (x < filt) and (downward > 0.0) and np.isfinite(prev_src) and (x > prev_src)
        )
        long_cond = bool(buy_strong or buy_weak)
        short_cond = bool(sell_strong or sell_weak)
        phase = 'BUY_STRONG' if buy_strong else ('BUY_WEAK' if buy_weak else ('SELL_STRONG' if sell_strong else ('SELL_WEAK' if sell_weak else 'NEUTRAL')))

        prev_cond_ini = int(self.cond_ini)
        cond_ini = 1 if long_cond else (-1 if short_cond else prev_cond_ini)
        buy_event = bool(long_cond and prev_cond_ini == -1)
        sell_event = bool(short_cond and prev_cond_ini == 1)

        hband = float(filt + smrng) if np.isfinite(filt) and np.isfinite(smrng) else float('nan')
        lband = float(filt - smrng) if np.isfinite(filt) and np.isfinite(smrng) else float('nan')

        self.prev_src = x
        self.prev_filt = filt
        self.cond_ini = int(cond_ini)
        self.upward = float(upward)
        self.downward = float(downward)
        self.filt = filt
        self.smrng = smrng
        self.hband = hband
        self.lband = lband
        self.phase = str(phase)
        self.long_cond = bool(long_cond)
        self.short_cond = bool(short_cond)
        self.buy_event = bool(buy_event)
        self.sell_event = bool(sell_event)
        return self.snapshot()

    def snapshot(self) -> Dict[str, Any]:
        state = 'BUY' if self.long_cond else ('SELL' if self.short_cond else 'NEUTRAL')
        return {
            'range_filter_line': (float(self.filt) if np.isfinite(float(self.filt)) else None),
            'range_filter_upper': (float(self.hband) if np.isfinite(float(self.hband)) else None),
            'range_filter_lower': (float(self.lband) if np.isfinite(float(self.lband)) else None),
            'range_filter_smooth_range': (float(self.smrng) if np.isfinite(float(self.smrng)) else None),
            'range_filter_state': state,
            'range_filter_phase': (str(self.phase) if str(self.phase) in ('BUY_STRONG', 'BUY_WEAK', 'SELL_STRONG', 'SELL_WEAK', 'NEUTRAL') else 'NEUTRAL'),
            'range_filter_buy': bool(self.buy_event),
            'range_filter_sell': bool(self.sell_event),
            'range_filter_long_cond': bool(self.long_cond),
            'range_filter_short_cond': bool(self.short_cond),
            'range_filter_cond_ini': int(self.cond_ini),
            'range_filter_upward_count': float(self.upward),
            'range_filter_downward_count': float(self.downward),
        }


def make_range_filter_state(per: int = RANGE_FILTER_PER, mult: float = RANGE_FILTER_MULT):
    return _RangeFilterState(per=per, mult=mult)


def compute_range_filter_series(close: np.ndarray, per: int = RANGE_FILTER_PER, mult: float = RANGE_FILTER_MULT) -> Dict[str, np.ndarray]:
    c = np.asarray(close, dtype=float)
    n = len(c)
    out = {
        'range_filter_line': np.full(n, np.nan, dtype=float),
        'range_filter_upper': np.full(n, np.nan, dtype=float),
        'range_filter_lower': np.full(n, np.nan, dtype=float),
        'range_filter_smooth_range': np.full(n, np.nan, dtype=float),
        'range_filter_state': np.empty(n, dtype=object),
        'range_filter_phase': np.empty(n, dtype=object),
        'range_filter_buy': np.zeros(n, dtype=bool),
        'range_filter_sell': np.zeros(n, dtype=bool),
        'range_filter_long_cond': np.zeros(n, dtype=bool),
        'range_filter_short_cond': np.zeros(n, dtype=bool),
        'range_filter_cond_ini': np.full(n, np.nan, dtype=float),
        'range_filter_upward_count': np.full(n, np.nan, dtype=float),
        'range_filter_downward_count': np.full(n, np.nan, dtype=float),
    }
    out['range_filter_state'][:] = None
    out['range_filter_phase'][:] = None
    st = make_range_filter_state(per=per, mult=mult)
    for i in range(n):
        snap = st.update(c[i])
        if snap.get('range_filter_line') is not None:
            out['range_filter_line'][i] = float(snap['range_filter_line'])
        if snap.get('range_filter_upper') is not None:
            out['range_filter_upper'][i] = float(snap['range_filter_upper'])
        if snap.get('range_filter_lower') is not None:
            out['range_filter_lower'][i] = float(snap['range_filter_lower'])
        if snap.get('range_filter_smooth_range') is not None:
            out['range_filter_smooth_range'][i] = float(snap['range_filter_smooth_range'])
        out['range_filter_state'][i] = snap.get('range_filter_state')
        out['range_filter_phase'][i] = snap.get('range_filter_phase')
        out['range_filter_buy'][i] = bool(snap.get('range_filter_buy', False))
        out['range_filter_sell'][i] = bool(snap.get('range_filter_sell', False))
        out['range_filter_long_cond'][i] = bool(snap.get('range_filter_long_cond', False))
        out['range_filter_short_cond'][i] = bool(snap.get('range_filter_short_cond', False))
        out['range_filter_cond_ini'][i] = float(snap.get('range_filter_cond_ini', np.nan))
        out['range_filter_upward_count'][i] = float(snap.get('range_filter_upward_count', 0.0))
        out['range_filter_downward_count'][i] = float(snap.get('range_filter_downward_count', 0.0))
    return out

class _FlipAgeState:
    """Maintain PPO flip-age (candles since dot flipped) incrementally."""
    __slots__ = ("bars", "last_dppo", "last_color", "run_len")

    def __init__(self):
        self.bars: int = 0  # number of dPPO bars processed (including NaNs)
        self.last_dppo: float = float("nan")  # last finite dPPO
        self.last_color: Optional[str] = None  # last computed dot color (GREEN/RED) on committed bars
        self.run_len: int = 0  # consecutive bars with last_color (committed)

    def commit(self, dppo: float):
        self.bars += 1
        if not np.isfinite(dppo):
            return
        if np.isfinite(self.last_dppo):
            c = "GREEN" if float(dppo) > float(self.last_dppo) else "RED"
            if self.last_color is None:
                self.last_color = c
                self.run_len = 1
            else:
                if c == self.last_color:
                    self.run_len += 1
                else:
                    self.last_color = c
                    self.run_len = 1
        self.last_dppo = float(dppo)

    def preview(self, dppo_preview: float) -> Optional[int]:
        # Mirror _ppo_flip_age behavior: require at least 3 bars and a valid current color.
        if (self.bars + 1) < 3:
            return None
        if self.last_color is None:
            return None
        if (not np.isfinite(self.last_dppo)) or (not np.isfinite(dppo_preview)):
            return None
        c = "GREEN" if float(dppo_preview) > float(self.last_dppo) else "RED"
        run = (self.run_len + 1) if c == self.last_color else 1
        return max(0, int(run - 1))


class _CachedSnapshotProxy:
    """Memoize snapshot() until the underlying state mutates."""

    __slots__ = ("_inner", "_snapshot_valid", "_snapshot_value")

    def __init__(self, inner: Any):
        self._inner = inner
        self._snapshot_valid = False
        self._snapshot_value: Any = None

    def _invalidate(self) -> None:
        self._snapshot_valid = False
        self._snapshot_value = None

    def snapshot(self) -> Any:
        if not self._snapshot_valid:
            self._snapshot_value = self._inner.snapshot()
            self._snapshot_valid = True
        return self._snapshot_value

    def update(self, *args, **kwargs) -> Any:
        result = self._inner.update(*args, **kwargs)
        self._invalidate()
        return result

    def commit(self, *args, **kwargs) -> Any:
        result = self._inner.commit(*args, **kwargs)
        self._invalidate()
        return result

    def __getattr__(self, name: str) -> Any:
        return getattr(self._inner, name)


class _TFIndicatorState:
    """Incremental multi-indicator state per timeframe."""
    __slots__ = (
        "ema_fast", "ema_slow", "ema_sig", "last_macd", "last_sig",
        "ppo_fast", "ppo_slow", "ppo_sma", "last_dppo",
        "flip",
    )

    def __init__(self):
        self.ema_fast = _EMAState(MACD_FAST)
        self.ema_slow = _EMAState(MACD_SLOW)
        self.ema_sig = _EMAState(MACD_SIGNAL)  # on MACD series (finite only)
        self.last_macd: float = float("nan")
        self.last_sig: float = float("nan")

        self.ppo_fast = _EMAState(PPO_FAST)
        self.ppo_slow = _EMAState(PPO_SLOW)
        self.ppo_sma = _SMAState(PPO_SMOOTH)  # SMA on PPO raw
        self.last_dppo: float = float("nan")  # last committed dPPO value (finite)
        self.flip = _FlipAgeState()

    def preview(self, price: float) -> Tuple[float, float, Optional[str], Optional[str], Optional[str], Optional[str], Optional[int]]:
        # MACD
        ef = self.ema_fast.preview(price)
        es = self.ema_slow.preview(price)
        if np.isfinite(ef) and np.isfinite(es):
            macd = float(ef - es)
        else:
            macd = float("nan")

        if np.isfinite(macd):
            sig = float(self.ema_sig.preview(macd))
        else:
            sig = float("nan")

        ms = None
        if np.isfinite(macd) and np.isfinite(sig):
            ms = "GREEN" if macd > sig else "RED"

        angle = None
        if np.isfinite(macd) and np.isfinite(self.last_macd):
            angle = "GREEN" if macd > float(self.last_macd) else "RED"

        sig_angle = None
        if np.isfinite(sig) and np.isfinite(self.last_sig):
            sig_angle = "GREEN" if sig > float(self.last_sig) else "RED"

        # PPO + flip age
        pf = self.ppo_fast.preview(price)
        ps = self.ppo_slow.preview(price)
        dppo = float("nan")
        if np.isfinite(pf) and np.isfinite(ps) and ps != 0.0:
            ppo_raw = (float(pf) - float(ps)) / float(ps) * 100.0
            dppo = float(self.ppo_sma.preview(ppo_raw))

        ppo = None
        flip_age = None
        if np.isfinite(dppo) and np.isfinite(self.last_dppo):
            ppo = "GREEN" if dppo > float(self.last_dppo) else "RED"
            flip_age = self.flip.preview(dppo)

        return macd, sig, ms, angle, sig_angle, ppo, flip_age

    def commit(self, price: float):
        # Commit MACD side
        ef = self.ema_fast.commit(price)
        es = self.ema_slow.commit(price)
        macd = float("nan")
        if np.isfinite(ef) and np.isfinite(es):
            macd = float(ef - es)
        if np.isfinite(macd):
            sig_c = self.ema_sig.commit(macd)
            self.last_macd = macd
            if np.isfinite(sig_c):
                self.last_sig = float(sig_c)

        # Commit PPO side
        pf = self.ppo_fast.commit(price)
        ps = self.ppo_slow.commit(price)
        dppo = float("nan")
        if np.isfinite(pf) and np.isfinite(ps) and ps != 0.0:
            ppo_raw = (float(pf) - float(ps)) / float(ps) * 100.0
            dppo = float(self.ppo_sma.commit(ppo_raw))
        if np.isfinite(dppo):
            # Update last_dppo and flip-age state.
            if np.isfinite(self.last_dppo):
                # flip.commit expects the current dppo; it updates run lengths internally.
                self.flip.commit(dppo)
            else:
                # still count bars in flip-state
                self.flip.commit(dppo)
            self.last_dppo = dppo
        else:
            # still advance flip-state bar count (to mirror array-length based behavior)
            self.flip.commit(float("nan"))

def simulate_multitf_indicators_fast(
    df_1m: "pd.DataFrame",
    timeframes: List[str],
    progress_cb=None,
    macd_impl: str = "TRADINGVIEW",
    adx_impl: str = "TRADINGVIEW",
    *,
    required_fields: Optional[Any] = None,
    ema_settings: Optional[Any] = None,
    rsi_settings: Optional[Any] = None,
    market_state_thresholds: MarketStateThresholds = DEFAULT_MARKET_STATE_THRESHOLDS,
) -> Dict[str, "pd.DataFrame"]:
    _ = (macd_impl or "TRADINGVIEW").upper()
    _ = (adx_impl or "TRADINGVIEW").upper()

    req_fields = set(normalize_required_fields(required_fields, include_defaults=True))
    need_macd_preview = bool(req_fields & (MACD_LINE_FIELDS | MACD_STATE_FIELDS))
    need_adx = bool(req_fields & ADX_FIELDS)
    need_atr = bool(req_fields & ATR_FIELDS)
    need_ema = bool(req_fields & EMA_FIELDS)
    need_rsi = bool(req_fields & RSI_FIELDS)
    need_stoch_rsi = bool(req_fields & STOCH_RSI_FIELDS)
    need_taker_bias = bool(req_fields & TAKER_BIAS_FIELDS)
    need_frama = bool(req_fields & FRAMA_FIELDS)
    need_vidya = bool(req_fields & VIDYA_FIELDS)
    need_range_filter = bool(req_fields & RANGE_FILTER_FIELDS)
    need_market_aug = bool(req_fields & MARKET_AUG_FIELDS)

    tfs_in = [str(tf).strip() for tf in (timeframes or []) if str(tf).strip()]
    seen = set()
    timeframes = []
    for tf in tfs_in:
        if tf not in seen:
            seen.add(tf)
            timeframes.append(tf)
    if "1m" not in seen:
        timeframes = ["1m"] + timeframes

    n = len(df_1m)
    if n == 0:
        return {tf: pd.DataFrame() for tf in timeframes}

    try:
        close_times = pd.to_datetime(df_1m["close_time"], utc=True)
    except Exception:
        close_times = pd.to_datetime(df_1m["close_time"])
        try:
            close_times = close_times.dt.tz_localize("UTC")
        except Exception:
            pass

    try:
        open_times = pd.to_datetime(df_1m["open_time"], utc=True)
    except Exception:
        open_times = pd.to_datetime(df_1m["open_time"])
        try:
            open_times = open_times.dt.tz_localize("UTC")
        except Exception:
            pass

    def _numeric_array(column: str, *, fallback: Any = 0.0, fill_value: float = 0.0) -> np.ndarray:
        raw = df_1m[column] if column in df_1m.columns else fallback
        if not isinstance(raw, pd.Series):
            raw = pd.Series(np.full(n, fill_value, dtype=float), index=df_1m.index)
        return pd.to_numeric(raw, errors="coerce").fillna(fill_value).astype(float).to_numpy()

    opens = pd.to_numeric(df_1m.get("open", df_1m["close"]), errors="coerce").astype(float).to_numpy()
    prices = pd.to_numeric(df_1m["close"], errors="coerce").astype(float).to_numpy()
    highs = pd.to_numeric(df_1m.get("high", df_1m["close"]), errors="coerce").astype(float).to_numpy()
    lows = pd.to_numeric(df_1m.get("low", df_1m["close"]), errors="coerce").astype(float).to_numpy()
    volumes = _numeric_array("volume", fill_value=0.0)
    trade_counts = _numeric_array("trade_count", fill_value=0.0)
    taker_buy_volumes = _numeric_array("taker_buy_volume", fill_value=0.0)

    try:
        open_ns = open_times.astype("int64").to_numpy(dtype=np.int64, copy=False)
        open_ms = (open_ns // 1_000_000).astype(np.int64, copy=False)
    except Exception:
        open_ms = np.array([int(pd.Timestamp(t).value // 1_000_000) for t in open_times], dtype=np.int64)

    tf_ms: Dict[str, int] = {}
    for tf in timeframes:
        try:
            tf_ms[tf] = interval_to_ms(tf)
        except Exception:
            tf_ms[tf] = 60_000

    st: Dict[str, _TFIndicatorState] = {tf: _TFIndicatorState() for tf in timeframes} if need_macd_preview else {}
    adx_state: Dict[str, Any] = {tf: _CachedSnapshotProxy(make_adx_state(adx_impl, ADX_LEN)) for tf in timeframes} if need_adx else {}
    atr_state: Dict[str, Any] = {tf: _CachedSnapshotProxy(make_atr_state(ATR_LEN)) for tf in timeframes} if need_atr else {}
    raw_rsi_settings = rsi_settings if rsi_settings is not None else ema_settings
    if need_rsi:
        from .rsi_settings import resolve_rsi_settings
        rsi_state: Dict[str, Any] = {
            tf: _RsiState(
                length=int(resolve_rsi_settings(raw_rsi_settings, tf)["length"]),
                smoothing=int(resolve_rsi_settings(raw_rsi_settings, tf)["smoothing"]),
            )
            for tf in timeframes
        }
    else:
        rsi_state = {}
    stoch_rsi_state: Dict[str, Any] = {tf: _StochRsiState() for tf in timeframes} if need_stoch_rsi else {}
    frama_state: Dict[str, Any] = {tf: _CachedSnapshotProxy(make_frama_state(FRAMA_LEN, FRAMA_BANDS_DISTANCE)) for tf in timeframes} if need_frama else {}
    vidya_state: Dict[str, Any] = {
        tf: _CachedSnapshotProxy(make_vidya_state(VIDYA_LENGTH, VIDYA_MOMENTUM, VIDYA_BAND_DISTANCE, VIDYA_PIVOT_LEFT, VIDYA_PIVOT_RIGHT))
        for tf in timeframes
    } if need_vidya else {}
    range_filter_state: Dict[str, Any] = {
        tf: _CachedSnapshotProxy(make_range_filter_state(RANGE_FILTER_PER, RANGE_FILTER_MULT)) for tf in timeframes
    } if need_range_filter else {}

    agg_op: Dict[str, float] = {tf: float("nan") for tf in timeframes}
    agg_hi: Dict[str, float] = {tf: float("nan") for tf in timeframes}
    agg_lo: Dict[str, float] = {tf: float("nan") for tf in timeframes}
    agg_cl: Dict[str, float] = {tf: float("nan") for tf in timeframes}
    agg_vol: Dict[str, float] = {tf: 0.0 for tf in timeframes}
    agg_trade_count: Dict[str, float] = {tf: 0.0 for tf in timeframes}
    agg_taker_buy_volume: Dict[str, float] = {tf: 0.0 for tf in timeframes}
    need_reset: Dict[str, bool] = {tf: True for tf in timeframes}

    def _float_array() -> np.ndarray:
        return np.full(n, np.nan, dtype=float)

    def _i8_array() -> np.ndarray:
        return np.zeros(n, dtype=np.int8)

    def _bool_array() -> np.ndarray:
        return np.zeros(n, dtype=bool)

    def _obj_array() -> np.ndarray:
        arr = np.empty(n, dtype=object)
        arr[:] = None
        return arr

    out: Dict[str, Dict[str, Any]] = {}
    float_keys = (
        "open", "high", "low", "close", "volume", "macd", "signal", "adx", "atr", "di_plus", "di_minus", "di_spread",
        "rsi", "rsi_smooth",
        "stoch_rsi_k", "stoch_rsi_d",
        "frama", "frama_upper", "frama_lower", "frama_alpha",
        "vidya_line", "vidya_raw", "vidya_upper", "vidya_lower", "vidya_up_volume", "vidya_down_volume", "vidya_delta_pct",
        "vidya_active_liq_low_count", "vidya_active_liq_high_count", "vidya_nearest_liq_low", "vidya_nearest_liq_high",
        "vidya_dist_to_liq_low_pct", "vidya_dist_to_liq_high_pct",
        "range_filter_line", "range_filter_upper", "range_filter_lower", "range_filter_smooth_range",
        "range_filter_cond_ini", "range_filter_upward_count", "range_filter_downward_count",
        "taker_buy_volume", "taker_sell_volume", "taker_delta_volume", "taker_delta_pct", "taker_buy_share_pct", "taker_trade_count",
    )
    i8_keys = ("ms", "angle", "sig_angle", "ppo", "adx_angle", "atr_angle", "rsi_state", "stoch_rsi_kd")
    bool_keys = (
        "frama_break_up", "frama_break_down", "frama_mid_cross",
        "vidya_trend_up", "vidya_trend_down", "vidya_new_liq_low", "vidya_new_liq_high", "vidya_liq_low_taken", "vidya_liq_high_taken",
        "range_filter_buy", "range_filter_sell", "range_filter_long_cond", "range_filter_short_cond",
    )
    obj_keys = ("frama_state", "vidya_state", "range_filter_state", "range_filter_phase", "taker_bias_state")
    for tf in timeframes:
        tf_out: Dict[str, Any] = {}
        if "price" in req_fields:
            tf_out["price"] = prices.copy()
        for key in float_keys:
            if key in req_fields:
                tf_out[key] = _float_array()
        for key in i8_keys:
            if key in req_fields:
                tf_out[f"{key}_i"] = _i8_array()
        if "flip_age" in req_fields:
            tf_out["flip_age_i"] = np.full(n, -1, dtype=np.int32)
        for key in bool_keys:
            if key in req_fields:
                tf_out[key] = _bool_array()
        for key in obj_keys:
            if key in req_fields:
                tf_out[key] = _obj_array()
        out[tf] = tf_out

    last_report = -1
    for i in range(n):
        price = float(prices[i])
        minute_end_ms = int(open_ms[i] + 60_000)

        try:
            op1 = float(opens[i])
        except Exception:
            op1 = float("nan")
        try:
            hi1 = float(highs[i])
        except Exception:
            hi1 = float("nan")
        try:
            lo1 = float(lows[i])
        except Exception:
            lo1 = float("nan")
        try:
            vol1 = float(volumes[i])
        except Exception:
            vol1 = 0.0
        try:
            trades1 = float(trade_counts[i])
        except Exception:
            trades1 = 0.0
        try:
            taker_buy1 = float(taker_buy_volumes[i])
        except Exception:
            taker_buy1 = 0.0
        if not np.isfinite(op1):
            op1 = price
        if not np.isfinite(hi1):
            hi1 = price
        if not np.isfinite(lo1):
            lo1 = price
        if not np.isfinite(vol1):
            vol1 = 0.0
        if not np.isfinite(trades1):
            trades1 = 0.0
        if not np.isfinite(taker_buy1):
            taker_buy1 = 0.0

        for tf in timeframes:
            if need_reset.get(tf, True) or (not np.isfinite(float(agg_hi.get(tf, float("nan"))))):
                agg_op[tf] = op1
                agg_hi[tf] = hi1
                agg_lo[tf] = lo1
                agg_cl[tf] = price
                agg_vol[tf] = vol1
                agg_trade_count[tf] = trades1
                agg_taker_buy_volume[tf] = taker_buy1
                need_reset[tf] = False
            else:
                try:
                    agg_hi[tf] = max(float(agg_hi[tf]), hi1)
                    agg_lo[tf] = min(float(agg_lo[tf]), lo1)
                except Exception:
                    agg_hi[tf] = hi1
                    agg_lo[tf] = lo1
                agg_cl[tf] = price
                try:
                    agg_vol[tf] = float(agg_vol.get(tf, 0.0)) + float(vol1)
                except Exception:
                    agg_vol[tf] = float(vol1)
                try:
                    agg_trade_count[tf] = float(agg_trade_count.get(tf, 0.0)) + float(trades1)
                except Exception:
                    agg_trade_count[tf] = float(trades1)
                try:
                    agg_taker_buy_volume[tf] = float(agg_taker_buy_volume.get(tf, 0.0)) + float(taker_buy1)
                except Exception:
                    agg_taker_buy_volume[tf] = float(taker_buy1)

            tf_out = out[tf]
            if "open" in tf_out:
                tf_out["open"][i] = float(agg_op.get(tf, op1))
            if "high" in tf_out:
                tf_out["high"][i] = float(agg_hi.get(tf, hi1))
            if "low" in tf_out:
                tf_out["low"][i] = float(agg_lo.get(tf, lo1))
            if "close" in tf_out:
                tf_out["close"][i] = float(agg_cl.get(tf, price))
            if "volume" in tf_out:
                tf_out["volume"][i] = float(agg_vol.get(tf, vol1))
            if need_taker_bias:
                bias_payload = compute_taker_bias_payload(
                    agg_vol.get(tf, vol1),
                    agg_taker_buy_volume.get(tf, taker_buy1),
                    agg_trade_count.get(tf, trades1),
                )
                for key, value in bias_payload.items():
                    if key not in tf_out:
                        continue
                    if key == "taker_bias_state":
                        tf_out[key][i] = value
                    elif value is not None:
                        tf_out[key][i] = float(value)

            if need_macd_preview:
                macd, sig, ms, ang, sig_ang, ppo, fa = st[tf].preview(price)
                if np.isfinite(macd) and "macd" in tf_out:
                    tf_out["macd"][i] = macd
                if np.isfinite(sig) and "signal" in tf_out:
                    tf_out["signal"][i] = sig
                if np.isfinite(sig) and "ms_i" in tf_out:
                    tf_out["ms_i"][i] = _color_to_i8(ms)
                if np.isfinite(sig) and "angle_i" in tf_out:
                    tf_out["angle_i"][i] = _color_to_i8(ang)
                if np.isfinite(sig) and "sig_angle_i" in tf_out:
                    tf_out["sig_angle_i"][i] = _color_to_i8(sig_ang)
                if ppo is not None and "ppo_i" in tf_out:
                    tf_out["ppo_i"][i] = _color_to_i8(ppo)
                if fa is not None and "flip_age_i" in tf_out:
                    tf_out["flip_age_i"][i] = int(fa)

            if need_rsi:
                rsi_v, rsi_smooth_v, rsi_state_v = rsi_state[tf].preview(float(agg_cl.get(tf, price)))
                if np.isfinite(rsi_v) and "rsi" in tf_out:
                    tf_out["rsi"][i] = float(rsi_v)
                if np.isfinite(rsi_smooth_v) and "rsi_smooth" in tf_out:
                    tf_out["rsi_smooth"][i] = float(rsi_smooth_v)
                if rsi_state_v is not None and "rsi_state_i" in tf_out:
                    tf_out["rsi_state_i"][i] = _color_to_i8(rsi_state_v)

            if need_stoch_rsi:
                s_k, s_d, s_kd = stoch_rsi_state[tf].preview(float(agg_cl.get(tf, price)))
                if np.isfinite(s_k) and "stoch_rsi_k" in tf_out:
                    tf_out["stoch_rsi_k"][i] = float(s_k)
                if np.isfinite(s_d) and "stoch_rsi_d" in tf_out:
                    tf_out["stoch_rsi_d"][i] = float(s_d)
                if s_kd is not None and "stoch_rsi_kd_i" in tf_out:
                    tf_out["stoch_rsi_kd_i"][i] = _color_to_i8(s_kd)

            if need_adx:
                a_adx, a_dp, a_dm, a_ds, a_ang = adx_state[tf].snapshot()
                if a_adx is not None and "adx" in tf_out:
                    tf_out["adx"][i] = float(a_adx)
                if a_dp is not None and "di_plus" in tf_out:
                    tf_out["di_plus"][i] = float(a_dp)
                if a_dm is not None and "di_minus" in tf_out:
                    tf_out["di_minus"][i] = float(a_dm)
                if a_ds is not None and "di_spread" in tf_out:
                    tf_out["di_spread"][i] = float(a_ds)
                if a_ang is not None and "adx_angle_i" in tf_out:
                    tf_out["adx_angle_i"][i] = _color_to_i8(a_ang)

            if need_atr:
                a_atr, a_atr_ang = atr_state[tf].snapshot()
                if a_atr is not None and "atr" in tf_out:
                    tf_out["atr"][i] = float(a_atr)
                if a_atr_ang is not None and "atr_angle_i" in tf_out:
                    tf_out["atr_angle_i"][i] = _color_to_i8(a_atr_ang)

            if need_frama:
                f_snap = frama_state[tf].snapshot()
                for key in ("frama", "frama_upper", "frama_lower", "frama_alpha"):
                    if f_snap.get(key) is not None and key in tf_out:
                        tf_out[key][i] = float(f_snap[key])
                if "frama_state" in tf_out:
                    tf_out["frama_state"][i] = f_snap.get("frama_state")
                for key in ("frama_break_up", "frama_break_down", "frama_mid_cross"):
                    if key in tf_out:
                        tf_out[key][i] = bool(f_snap.get(key))

            if need_vidya:
                v_snap = vidya_state[tf].snapshot()
                for key in ("vidya_line", "vidya_raw", "vidya_upper", "vidya_lower", "vidya_nearest_liq_low", "vidya_nearest_liq_high", "vidya_dist_to_liq_low_pct", "vidya_dist_to_liq_high_pct"):
                    if v_snap.get(key) is not None and key in tf_out:
                        tf_out[key][i] = float(v_snap[key])
                for key in ("vidya_up_volume", "vidya_down_volume", "vidya_delta_pct", "vidya_active_liq_low_count", "vidya_active_liq_high_count"):
                    if key in tf_out:
                        tf_out[key][i] = float(v_snap.get(key, 0.0))
                if "vidya_state" in tf_out:
                    tf_out["vidya_state"][i] = v_snap.get("vidya_state")
                for key in ("vidya_trend_up", "vidya_trend_down", "vidya_new_liq_low", "vidya_new_liq_high", "vidya_liq_low_taken", "vidya_liq_high_taken"):
                    if key in tf_out:
                        tf_out[key][i] = bool(v_snap.get(key, False))

            if need_range_filter:
                rf_snap = range_filter_state[tf].snapshot()
                for key in ("range_filter_line", "range_filter_upper", "range_filter_lower", "range_filter_smooth_range"):
                    if rf_snap.get(key) is not None and key in tf_out:
                        tf_out[key][i] = float(rf_snap[key])
                if "range_filter_state" in tf_out:
                    tf_out["range_filter_state"][i] = rf_snap.get("range_filter_state")
                if "range_filter_phase" in tf_out:
                    tf_out["range_filter_phase"][i] = rf_snap.get("range_filter_phase")
                for key in ("range_filter_buy", "range_filter_sell", "range_filter_long_cond", "range_filter_short_cond"):
                    if key in tf_out:
                        tf_out[key][i] = bool(rf_snap.get(key, False))
                if "range_filter_cond_ini" in tf_out:
                    tf_out["range_filter_cond_ini"][i] = float(rf_snap.get("range_filter_cond_ini", np.nan))
                if "range_filter_upward_count" in tf_out:
                    tf_out["range_filter_upward_count"][i] = float(rf_snap.get("range_filter_upward_count", 0.0))
                if "range_filter_downward_count" in tf_out:
                    tf_out["range_filter_downward_count"][i] = float(rf_snap.get("range_filter_downward_count", 0.0))

        for tf in timeframes:
            do_commit = tf == "1m"
            if not do_commit:
                try:
                    do_commit = (minute_end_ms % int(tf_ms[tf])) == 0
                except Exception:
                    do_commit = False
            if not do_commit:
                continue

            if need_macd_preview:
                st[tf].commit(price)

            try:
                o = float(agg_op.get(tf, price))
                h = float(agg_hi.get(tf, price))
                l = float(agg_lo.get(tf, price))
                c = float(agg_cl.get(tf, price))
                v = float(agg_vol.get(tf, 0.0))
                tc = float(agg_trade_count.get(tf, 0.0))
                tbv = float(agg_taker_buy_volume.get(tf, 0.0))
            except Exception:
                o = h = l = c = price
                v = 0.0
                tc = 0.0
                tbv = 0.0

            if need_adx:
                adx_state[tf].update(h, l, c)
            if need_atr:
                atr_state[tf].update(h, l, c)
            if need_rsi:
                rsi_state[tf].commit(c)
            if need_stoch_rsi:
                stoch_rsi_state[tf].commit(c)
            if need_frama:
                frama_state[tf].update(h, l, c)
            if need_vidya:
                vidya_state[tf].update(o, h, l, c, v)
            if need_range_filter:
                range_filter_state[tf].update(c)

            tf_out = out[tf]
            if need_adx:
                a_adx, a_dp, a_dm, a_ds, a_ang = adx_state[tf].snapshot()
                if a_adx is not None and "adx" in tf_out:
                    tf_out["adx"][i] = float(a_adx)
                if a_dp is not None and "di_plus" in tf_out:
                    tf_out["di_plus"][i] = float(a_dp)
                if a_dm is not None and "di_minus" in tf_out:
                    tf_out["di_minus"][i] = float(a_dm)
                if a_ds is not None and "di_spread" in tf_out:
                    tf_out["di_spread"][i] = float(a_ds)
                if a_ang is not None and "adx_angle_i" in tf_out:
                    tf_out["adx_angle_i"][i] = _color_to_i8(a_ang)
            if need_atr:
                a_atr, a_atr_ang = atr_state[tf].snapshot()
                if a_atr is not None and "atr" in tf_out:
                    tf_out["atr"][i] = float(a_atr)
                if a_atr_ang is not None and "atr_angle_i" in tf_out:
                    tf_out["atr_angle_i"][i] = _color_to_i8(a_atr_ang)
            if need_rsi:
                rsi_v, rsi_smooth_v, rsi_state_v = rsi_state[tf].snapshot()
                if np.isfinite(rsi_v) and "rsi" in tf_out:
                    tf_out["rsi"][i] = float(rsi_v)
                if np.isfinite(rsi_smooth_v) and "rsi_smooth" in tf_out:
                    tf_out["rsi_smooth"][i] = float(rsi_smooth_v)
                if rsi_state_v is not None and "rsi_state_i" in tf_out:
                    tf_out["rsi_state_i"][i] = _color_to_i8(rsi_state_v)
            if need_stoch_rsi:
                s_k, s_d, s_kd = stoch_rsi_state[tf].snapshot()
                if np.isfinite(s_k) and "stoch_rsi_k" in tf_out:
                    tf_out["stoch_rsi_k"][i] = float(s_k)
                if np.isfinite(s_d) and "stoch_rsi_d" in tf_out:
                    tf_out["stoch_rsi_d"][i] = float(s_d)
                if s_kd is not None and "stoch_rsi_kd_i" in tf_out:
                    tf_out["stoch_rsi_kd_i"][i] = _color_to_i8(s_kd)
            if need_frama:
                f_snap = frama_state[tf].snapshot()
                for key in ("frama", "frama_upper", "frama_lower", "frama_alpha"):
                    if f_snap.get(key) is not None and key in tf_out:
                        tf_out[key][i] = float(f_snap[key])
                if "frama_state" in tf_out:
                    tf_out["frama_state"][i] = f_snap.get("frama_state")
                for key in ("frama_break_up", "frama_break_down", "frama_mid_cross"):
                    if key in tf_out:
                        tf_out[key][i] = bool(f_snap.get(key))
            if need_vidya:
                v_snap = vidya_state[tf].snapshot()
                for key in ("vidya_line", "vidya_raw", "vidya_upper", "vidya_lower", "vidya_nearest_liq_low", "vidya_nearest_liq_high", "vidya_dist_to_liq_low_pct", "vidya_dist_to_liq_high_pct"):
                    if v_snap.get(key) is not None and key in tf_out:
                        tf_out[key][i] = float(v_snap[key])
                for key in ("vidya_up_volume", "vidya_down_volume", "vidya_delta_pct", "vidya_active_liq_low_count", "vidya_active_liq_high_count"):
                    if key in tf_out:
                        tf_out[key][i] = float(v_snap.get(key, 0.0))
                if "vidya_state" in tf_out:
                    tf_out["vidya_state"][i] = v_snap.get("vidya_state")
                for key in ("vidya_trend_up", "vidya_trend_down", "vidya_new_liq_low", "vidya_new_liq_high", "vidya_liq_low_taken", "vidya_liq_high_taken"):
                    if key in tf_out:
                        tf_out[key][i] = bool(v_snap.get(key, False))
            if need_range_filter:
                rf_snap = range_filter_state[tf].snapshot()
                for key in ("range_filter_line", "range_filter_upper", "range_filter_lower", "range_filter_smooth_range"):
                    if rf_snap.get(key) is not None and key in tf_out:
                        tf_out[key][i] = float(rf_snap[key])
                if "range_filter_state" in tf_out:
                    tf_out["range_filter_state"][i] = rf_snap.get("range_filter_state")
                if "range_filter_phase" in tf_out:
                    tf_out["range_filter_phase"][i] = rf_snap.get("range_filter_phase")
                for key in ("range_filter_buy", "range_filter_sell", "range_filter_long_cond", "range_filter_short_cond"):
                    if key in tf_out:
                        tf_out[key][i] = bool(rf_snap.get(key, False))
                if "range_filter_cond_ini" in tf_out:
                    tf_out["range_filter_cond_ini"][i] = float(rf_snap.get("range_filter_cond_ini", np.nan))
                if "range_filter_upward_count" in tf_out:
                    tf_out["range_filter_upward_count"][i] = float(rf_snap.get("range_filter_upward_count", 0.0))
                if "range_filter_downward_count" in tf_out:
                    tf_out["range_filter_downward_count"][i] = float(rf_snap.get("range_filter_downward_count", 0.0))
            if need_taker_bias:
                bias_payload = compute_taker_bias_payload(v, tbv, tc)
                for key, value in bias_payload.items():
                    if key not in tf_out:
                        continue
                    if key == "taker_bias_state":
                        tf_out[key][i] = value
                    elif value is not None:
                        tf_out[key][i] = float(value)
            need_reset[tf] = True

        if progress_cb:
            pct = int((i / max(1, n - 1)) * 100)
            if pct != last_report and pct % 2 == 0:
                progress_cb(f"Calculating indicators (fast)... {pct}%", pct)
                last_report = pct

    idx = pd.DatetimeIndex(close_times)
    streams: Dict[str, "pd.DataFrame"] = {}
    base_keys = (
        "price", "open", "high", "low", "close", "volume", "macd", "signal", "adx", "atr", "di_plus", "di_minus", "di_spread",
        "rsi", "rsi_smooth", "rsi_state",
        "stoch_rsi_k", "stoch_rsi_d",
        "frama", "frama_upper", "frama_lower", "frama_alpha", "frama_state", "frama_break_up", "frama_break_down", "frama_mid_cross",
        "vidya_line", "vidya_raw", "vidya_upper", "vidya_lower", "vidya_state", "vidya_trend_up", "vidya_trend_down", "vidya_up_volume",
        "vidya_down_volume", "vidya_delta_pct", "vidya_active_liq_low_count", "vidya_active_liq_high_count", "vidya_nearest_liq_low",
        "vidya_nearest_liq_high", "vidya_dist_to_liq_low_pct", "vidya_dist_to_liq_high_pct", "vidya_new_liq_low", "vidya_new_liq_high",
        "vidya_liq_low_taken", "vidya_liq_high_taken", "range_filter_line", "range_filter_upper", "range_filter_lower",
        "range_filter_smooth_range", "range_filter_state", "range_filter_phase", "range_filter_buy", "range_filter_sell",
        "range_filter_long_cond", "range_filter_short_cond", "range_filter_cond_ini", "range_filter_upward_count", "range_filter_downward_count",
        "taker_buy_volume", "taker_sell_volume", "taker_delta_volume", "taker_delta_pct", "taker_buy_share_pct", "taker_trade_count", "taker_bias_state",
    )
    for tf in timeframes:
        tf_out = out[tf]
        columns: Dict[str, Any] = {}
        for key in base_keys:
            if key in tf_out:
                columns[key] = tf_out[key]
        if "ms_i" in tf_out:
            columns["ms"] = np.where(tf_out["ms_i"] == 1, "GREEN", np.where(tf_out["ms_i"] == -1, "RED", None))
        if "angle_i" in tf_out:
            columns["angle"] = np.where(tf_out["angle_i"] == 1, "GREEN", np.where(tf_out["angle_i"] == -1, "RED", None))
        if "sig_angle_i" in tf_out:
            columns["sig_angle"] = np.where(tf_out["sig_angle_i"] == 1, "GREEN", np.where(tf_out["sig_angle_i"] == -1, "RED", None))
        if "ppo_i" in tf_out:
            columns["ppo"] = np.where(tf_out["ppo_i"] == 1, "GREEN", np.where(tf_out["ppo_i"] == -1, "RED", None))
        if "flip_age_i" in tf_out:
            columns["flip_age"] = np.where(tf_out["flip_age_i"] >= 0, tf_out["flip_age_i"].astype(float), np.nan)
        if "adx_angle_i" in tf_out:
            columns["adx_angle"] = np.where(tf_out["adx_angle_i"] == 1, "GREEN", np.where(tf_out["adx_angle_i"] == -1, "RED", None))
        if "atr_angle_i" in tf_out:
            columns["atr_angle"] = np.where(tf_out["atr_angle_i"] == 1, "GREEN", np.where(tf_out["atr_angle_i"] == -1, "RED", None))
        if "rsi_state_i" in tf_out:
            columns["rsi_state"] = np.where(tf_out["rsi_state_i"] == 1, "GREEN", np.where(tf_out["rsi_state_i"] == -1, "RED", None))
        if "stoch_rsi_kd_i" in tf_out:
            columns["stoch_rsi_kd"] = np.where(tf_out["stoch_rsi_kd_i"] == 1, "GREEN", np.where(tf_out["stoch_rsi_kd_i"] == -1, "RED", None))
        df = pd.DataFrame(columns, index=idx)
        streams[tf] = _augment_stream_with_market_state(df, thresholds=market_state_thresholds) if need_market_aug else df

    if need_ema:
        streams = _augment_streams_with_ema_native(
            streams,
            ema_settings=ema_settings,
            required_fields=req_fields,
        )
    return streams

def _indicator_cache_paths(
    cache_dir: str,
    symbol: str,
    start_utc: "pd.Timestamp",
    end_utc: "pd.Timestamp",
    timeframes: List[str],
    price_source: str,
    macd_impl: str,
    adx_impl: str,
    required_fields: Optional[Any] = None,
    ema_settings: Optional[Any] = None,
    rsi_settings: Optional[Any] = None,
) -> Tuple[str, str]:
    """Return (parquet_path, meta_json_path)."""
    _ensure_dir(cache_dir)
    folder = os.path.join(cache_dir, INDICATOR_CACHE_SUBDIR, symbol.upper())
    _ensure_dir(folder)

    tfs_norm = ",".join([tf.strip() for tf in (timeframes or []) if tf.strip()])
    feat_sig = feature_signature(required_fields, include_defaults=True)
    ema_sig = _ema_profile_signature_for_request(required_fields, ema_settings, timeframes)
    rsi_sig = _rsi_profile_signature_for_request(required_fields, rsi_settings if rsi_settings is not None else ema_settings, timeframes)
    key = "|".join(
        [
            f"v={INDICATOR_CACHE_VERSION}",
            f"sym={symbol.upper()}",
            f"start={pd.to_datetime(start_utc, utc=True).strftime('%Y%m%d%H%M%S')}",
            f"end={pd.to_datetime(end_utc, utc=True).strftime('%Y%m%d%H%M%S')}",
            f"tfs={tfs_norm}",
            f"price={str(price_source or 'LAST').upper()}",
            f"macd_impl={str(macd_impl or 'TRADINGVIEW').upper()}",
            f"adx_impl={str(adx_impl or 'TRADINGVIEW').upper()}",
            f"MACD={MACD_FAST},{MACD_SLOW},{MACD_SIGNAL}",
            f"PPO={PPO_FAST},{PPO_SLOW},{PPO_SMOOTH}",
            f"ADX={ADX_LEN}",
            f"ATR={ATR_LEN}",
            f"VIDYA={VIDYA_LENGTH},{VIDYA_MOMENTUM},{VIDYA_BAND_DISTANCE}",
            f"RANGEFILTER={RANGE_FILTER_PER},{RANGE_FILTER_MULT}",
            f"STOCHRSI={STOCH_RSI_RSI_LENGTH},{STOCH_RSI_STOCH_LENGTH},{STOCH_RSI_SMOOTH_K},{STOCH_RSI_SMOOTH_D}",
            f"FEATURES={feat_sig}",
            f"EMA={json.dumps(ema_sig, ensure_ascii=False, separators=(',', ':')) if ema_sig is not None else '-'}",
            f"RSI={json.dumps(rsi_sig, ensure_ascii=False, separators=(',', ':')) if rsi_sig is not None else '-'}",
        ]
    )
    h = hashlib.sha1(key.encode("utf-8")).hexdigest()[:16]
    base = f"ind_{h}"
    pq = os.path.join(folder, f"{base}.parquet")
    meta = os.path.join(folder, f"{base}.meta.json")
    return pq, meta


def _indicator_cache_matches(
    meta: Dict[str, Any],
    *,
    timeframes: List[str],
    price_source: str,
    macd_impl: str,
    adx_impl: str,
    required_fields: Optional[Any] = None,
    ema_settings: Optional[Any] = None,
    rsi_settings: Optional[Any] = None,
) -> bool:
    try:
        if int(meta.get("report_version", -1)) != INDICATOR_CACHE_VERSION:
            return False
    except Exception:
        return False
    meta_tfs = [str(tf).strip() for tf in list(meta.get("timeframes") or []) if str(tf).strip()]
    req_tfs = [str(tf).strip() for tf in list(timeframes or []) if str(tf).strip()]
    if meta_tfs != req_tfs:
        return False
    if str(meta.get("price_source") or "LAST").upper() != str(price_source or "LAST").upper():
        return False
    if str(meta.get("macd_impl") or "TRADINGVIEW").upper() != str(macd_impl or "TRADINGVIEW").upper():
        return False
    if str(meta.get("adx_impl") or "TRADINGVIEW").upper() != str(adx_impl or "TRADINGVIEW").upper():
        return False
    req_fams = normalize_required_families(required_fields, include_defaults=True)
    meta_fams = normalize_required_families(
        meta.get("required_families") or list(normalize_required_families(None, include_defaults=True)),
        include_defaults=True,
    )
    if not req_fams.issubset(meta_fams):
        return False
    req_ema_sig = _ema_profile_signature_for_request(required_fields, ema_settings, timeframes)
    if req_ema_sig is not None:
        meta_ema_sig = meta.get("ema_profile_signature")
        if meta_ema_sig is None:
            return False
        try:
            meta_ema_norm = [list(item) for item in list(meta_ema_sig or [])]
        except Exception:
            return False
        if meta_ema_norm != req_ema_sig:
            return False
    req_rsi_sig = _rsi_profile_signature_for_request(required_fields, rsi_settings if rsi_settings is not None else ema_settings, timeframes)
    if req_rsi_sig is not None:
        meta_rsi_sig = meta.get("rsi_profile_signature")
        if meta_rsi_sig is None:
            return False
        try:
            meta_rsi_norm = [list(item) for item in list(meta_rsi_sig or [])]
        except Exception:
            return False
        if meta_rsi_norm != req_rsi_sig:
            return False
    return True


def _find_covering_indicator_cache(
    cache_dir: str,
    symbol: str,
    start_utc: "pd.Timestamp",
    end_utc: "pd.Timestamp",
    timeframes: List[str],
    price_source: str,
    macd_impl: str,
    adx_impl: str,
    required_fields: Optional[Any] = None,
    ema_settings: Optional[Any] = None,
    rsi_settings: Optional[Any] = None,
) -> Optional[Tuple[str, str, Dict[str, Any]]]:
    folder = os.path.join(cache_dir, INDICATOR_CACHE_SUBDIR, symbol.upper())
    if not os.path.isdir(folder):
        return None
    best: Optional[Tuple[str, str, Dict[str, Any]]] = None
    best_span: Optional[int] = None
    for name in os.listdir(folder):
        if not name.endswith(".meta.json"):
            continue
        meta_path = os.path.join(folder, name)
        try:
            with open(meta_path, "r", encoding="utf-8") as f:
                meta = json.load(f)
        except Exception:
            continue
        if not _indicator_cache_matches(
            meta,
            timeframes=timeframes,
            price_source=price_source,
            macd_impl=macd_impl,
            adx_impl=adx_impl,
            required_fields=required_fields,
            ema_settings=ema_settings,
            rsi_settings=rsi_settings,
        ):
            continue
        try:
            meta_start = pd.to_datetime(meta.get("start_utc"), utc=True)
            meta_end = pd.to_datetime(meta.get("end_utc"), utc=True)
        except Exception:
            continue
        if meta_start > start_utc or meta_end < end_utc:
            continue
        pq_path = meta_path[: -len(".meta.json")] + ".parquet"
        if not os.path.isfile(pq_path):
            continue
        span = int((meta_end.value - meta_start.value) // 1_000_000)
        if best is None or best_span is None or span < best_span:
            best = (pq_path, meta_path, meta)
            best_span = span
    return best


def _slice_stream_cache_window(
    streams: Dict[str, "pd.DataFrame"],
    start_utc: "pd.Timestamp",
    end_utc: "pd.Timestamp",
) -> Dict[str, "pd.DataFrame"]:
    out: Dict[str, pd.DataFrame] = {}
    start_utc = pd.to_datetime(start_utc, utc=True)
    end_utc = pd.to_datetime(end_utc, utc=True)
    for tf, df in (streams or {}).items():
        if not isinstance(df, pd.DataFrame) or df.empty:
            out[tf] = df.copy() if isinstance(df, pd.DataFrame) else pd.DataFrame()
            continue
        try:
            sliced = df.loc[(df.index >= start_utc) & (df.index <= end_utc)].copy()
        except Exception:
            sliced = df.copy()
        out[tf] = sliced
    return out


_INDICATOR_CACHE_VALIDATION_FIELDS = frozenset(
    {"flip_age"}
    | set(OHLCV_FIELDS)
    | set(ADX_FIELDS)
    | set(EMA_FIELDS)
    | set(ATR_FIELDS)
    | set(FRAMA_FIELDS)
    | set(MACD_LINE_FIELDS)
    | set(MACD_STATE_FIELDS)
    | set(MARKET_AUG_FIELDS)
    | set(RANGE_FILTER_FIELDS)
    | set(RSI_FIELDS)
    | set(STOCH_RSI_FIELDS)
    | set(TAKER_BIAS_FIELDS)
    | set(VIDYA_FIELDS)
)


def _indicator_streams_have_required_fields(
    streams: Optional[Dict[str, "pd.DataFrame"]],
    required_fields: Optional[Any],
) -> bool:
    req_fields = set(normalize_required_fields(required_fields, include_defaults=True))
    req_fields &= set(_INDICATOR_CACHE_VALIDATION_FIELDS)
    if not req_fields:
        return True
    if not isinstance(streams, dict) or not streams:
        return False
    for field in req_fields:
        found = False
        field_s = str(field).strip()
        for df in streams.values():
            if not isinstance(df, pd.DataFrame):
                continue
            if field_s not in df.columns:
                continue
            if field_s in OHLCV_FIELDS:
                try:
                    if not pd.to_numeric(df[field_s], errors="coerce").notna().any():
                        continue
                except Exception:
                    try:
                        if not df[field_s].notna().any():
                            continue
                    except Exception:
                        continue
            found = True
            break
        if not found:
            return False
    return True


LOCAL_INDICATOR_STORE_VERSION = 1
LOCAL_INDICATOR_STORE_SUBDIR = "indicator_store"


def _market_threshold_store_signature(market_state_thresholds: Optional[Any]) -> Optional[Tuple[Tuple[str, Any], ...]]:
    if market_state_thresholds is None:
        return None
    try:
        payload = market_state_thresholds_dict(coerce_market_state_thresholds(market_state_thresholds))
        return tuple(sorted(payload.items(), key=lambda item: item[0]))
    except Exception:
        return None


def _local_indicator_store_profile(
    *,
    timeframes: List[str],
    price_source: str,
    macd_impl: str,
    adx_impl: str,
    required_fields: Optional[Any],
    ema_settings: Optional[Any] = None,
    rsi_settings: Optional[Any] = None,
    market_state_thresholds: Optional[Any] = None,
) -> Dict[str, Any]:
    req_fams = sorted(normalize_required_families(required_fields, include_defaults=True))
    ema_sig = _ema_profile_signature_for_request(required_fields, ema_settings, timeframes)
    rsi_sig = _rsi_profile_signature_for_request(required_fields, rsi_settings if rsi_settings is not None else ema_settings, timeframes)
    return {
        "report_version": INDICATOR_CACHE_VERSION,
        "local_store_version": LOCAL_INDICATOR_STORE_VERSION,
        "timeframes": [str(tf).strip() for tf in (timeframes or []) if str(tf).strip()],
        "price_source": str(price_source or "LAST").upper(),
        "macd_impl": str(macd_impl or "TRADINGVIEW").upper(),
        "adx_impl": str(adx_impl or "TRADINGVIEW").upper(),
        "required_families": req_fams,
        "ema_profile_signature": ema_sig,
        "rsi_profile_signature": rsi_sig,
        "market_threshold_signature": _market_threshold_store_signature(market_state_thresholds),
        "feature_signature": feature_signature(required_fields, include_defaults=True),
    }


def _local_indicator_store_profile_hash(
    *,
    timeframes: List[str],
    price_source: str,
    macd_impl: str,
    adx_impl: str,
    required_fields: Optional[Any],
    ema_settings: Optional[Any] = None,
    rsi_settings: Optional[Any] = None,
    market_state_thresholds: Optional[Any] = None,
) -> str:
    payload = _local_indicator_store_profile(
        timeframes=timeframes,
        price_source=price_source,
        macd_impl=macd_impl,
        adx_impl=adx_impl,
        required_fields=required_fields,
        ema_settings=ema_settings,
        rsi_settings=rsi_settings,
        market_state_thresholds=market_state_thresholds,
    )
    raw = json.dumps(payload, sort_keys=True, ensure_ascii=False, default=str)
    return hashlib.sha1(raw.encode("utf-8")).hexdigest()[:16]


def _local_indicator_store_dir(
    cache_dir: str,
    symbol: str,
    *,
    timeframes: List[str],
    price_source: str,
    macd_impl: str,
    adx_impl: str,
    required_fields: Optional[Any],
    ema_settings: Optional[Any] = None,
    rsi_settings: Optional[Any] = None,
    market_state_thresholds: Optional[Any] = None,
) -> str:
    folder = os.path.join(
        str(cache_dir or "data_cache"),
        LOCAL_INDICATOR_STORE_SUBDIR,
        str(symbol or "").upper(),
        _local_indicator_store_profile_hash(
            timeframes=timeframes,
            price_source=price_source,
            macd_impl=macd_impl,
            adx_impl=adx_impl,
            required_fields=required_fields,
            ema_settings=ema_settings,
            rsi_settings=rsi_settings,
            market_state_thresholds=market_state_thresholds,
        ),
    )
    _ensure_dir(folder)
    return folder


def _local_indicator_store_day_paths(
    cache_dir: str,
    symbol: str,
    day_utc: "pd.Timestamp",
    *,
    timeframes: List[str],
    price_source: str,
    macd_impl: str,
    adx_impl: str,
    required_fields: Optional[Any],
    ema_settings: Optional[Any] = None,
    rsi_settings: Optional[Any] = None,
    market_state_thresholds: Optional[Any] = None,
) -> Tuple[str, str]:
    folder = _local_indicator_store_dir(
        cache_dir,
        symbol,
        timeframes=timeframes,
        price_source=price_source,
        macd_impl=macd_impl,
        adx_impl=adx_impl,
        required_fields=required_fields,
        ema_settings=ema_settings,
        rsi_settings=rsi_settings,
        market_state_thresholds=market_state_thresholds,
    )
    day_key = pd.to_datetime(day_utc, utc=True).strftime("%Y%m%d")
    base = f"{str(symbol or '').upper()}_{day_key}"
    return (os.path.join(folder, base + '.parquet'), os.path.join(folder, base + '.meta.json'))


def _iter_utc_days(start_utc: "pd.Timestamp", end_utc: "pd.Timestamp") -> List["pd.Timestamp"]:
    cur = pd.to_datetime(start_utc, utc=True).normalize()
    stop = pd.to_datetime(end_utc, utc=True).normalize()
    days: List[pd.Timestamp] = []
    while cur <= stop:
        days.append(cur)
        cur = cur + pd.Timedelta(days=1)
    return days


def _merge_stream_sets(parts: List[Dict[str, "pd.DataFrame"]], timeframes: List[str]) -> Dict[str, "pd.DataFrame"]:
    out: Dict[str, pd.DataFrame] = {}
    for tf in [str(tf).strip() for tf in (timeframes or []) if str(tf).strip()]:
        frames: List[pd.DataFrame] = []
        for part in parts:
            df = (part or {}).get(tf)
            if isinstance(df, pd.DataFrame) and not df.empty:
                frames.append(df)
        if not frames:
            out[tf] = pd.DataFrame()
            continue
        merged = pd.concat(frames, axis=0)
        try:
            merged.index = pd.to_datetime(merged.index, utc=True, errors="coerce")
            merged = merged[~merged.index.isna()]
        except Exception:
            pass
        merged = merged[~merged.index.duplicated(keep="last")].sort_index()
        out[tf] = merged
    return out


def _load_local_indicator_store_window(
    cache_dir: str,
    symbol: str,
    start_utc: "pd.Timestamp",
    end_utc: "pd.Timestamp",
    *,
    timeframes: List[str],
    price_source: str,
    macd_impl: str,
    adx_impl: str,
    required_fields: Optional[Any],
    ema_settings: Optional[Any] = None,
    rsi_settings: Optional[Any] = None,
    market_state_thresholds: Optional[Any] = None,
) -> Optional[Dict[str, "pd.DataFrame"]]:
    pieces: List[Dict[str, pd.DataFrame]] = []
    days = _iter_utc_days(start_utc, end_utc)
    if not days:
        return None
    for day in days:
        pq_path, meta_path = _local_indicator_store_day_paths(
            cache_dir,
            symbol,
            day,
            timeframes=timeframes,
            price_source=price_source,
            macd_impl=macd_impl,
            adx_impl=adx_impl,
            required_fields=required_fields,
            ema_settings=ema_settings,
            rsi_settings=rsi_settings,
            market_state_thresholds=market_state_thresholds,
        )
        if (not os.path.isfile(pq_path)) or (not os.path.isfile(meta_path)):
            return None
        loaded = _load_indicator_cache(pq_path, meta_path)
        if loaded is None:
            return None
        pieces.append(loaded)
    merged = _merge_stream_sets(pieces, timeframes)
    sliced = _slice_stream_cache_window(merged, start_utc, end_utc)
    if not _indicator_streams_have_required_fields(sliced, required_fields):
        return None
    return sliced


def _save_local_indicator_store(
    cache_dir: str,
    symbol: str,
    streams: Dict[str, "pd.DataFrame"],
    *,
    timeframes: List[str],
    price_source: str,
    macd_impl: str,
    adx_impl: str,
    required_fields: Optional[Any],
    ema_settings: Optional[Any] = None,
    rsi_settings: Optional[Any] = None,
    market_state_thresholds: Optional[Any] = None,
) -> None:
    profile = _local_indicator_store_profile(
        timeframes=timeframes,
        price_source=price_source,
        macd_impl=macd_impl,
        adx_impl=adx_impl,
        required_fields=required_fields,
        ema_settings=ema_settings,
        rsi_settings=rsi_settings,
        market_state_thresholds=market_state_thresholds,
    )
    merged = _merge_stream_sets([streams], timeframes)
    all_index: List[pd.Timestamp] = []
    for tf in profile["timeframes"]:
        df = merged.get(tf)
        if isinstance(df, pd.DataFrame) and not df.empty:
            try:
                all_index.extend(pd.to_datetime(df.index, utc=True, errors="coerce").dropna().tolist())
            except Exception:
                pass
    if not all_index:
        return
    start_utc = min(all_index)
    end_utc = max(all_index)
    for day in _iter_utc_days(start_utc, end_utc):
        day_start = pd.to_datetime(day, utc=True)
        day_end = day_start + pd.Timedelta(days=1) - pd.Timedelta(microseconds=1)
        day_streams = _slice_stream_cache_window(merged, day_start, day_end)
        if not any(isinstance(df, pd.DataFrame) and not df.empty for df in day_streams.values()):
            continue
        pq_path, meta_path = _local_indicator_store_day_paths(
            cache_dir,
            symbol,
            day_start,
            timeframes=timeframes,
            price_source=price_source,
            macd_impl=macd_impl,
            adx_impl=adx_impl,
            required_fields=required_fields,
            ema_settings=ema_settings,
            rsi_settings=rsi_settings,
            market_state_thresholds=market_state_thresholds,
        )
        from .rsi_settings import resolve_rsi_settings
        rsi_default = resolve_rsi_settings(rsi_settings if rsi_settings is not None else ema_settings)
        meta = dict(profile)
        meta.update({
            "created_at_utc": pd.Timestamp.now(tz="UTC").isoformat(),
            "symbol": str(symbol or "").upper(),
            "start_utc": day_start.isoformat(),
            "end_utc": day_end.isoformat(),
            "macd_params": {"fast": MACD_FAST, "slow": MACD_SLOW, "signal": MACD_SIGNAL},
            "ppo_params": {"fast": PPO_FAST, "slow": PPO_SLOW, "smooth": PPO_SMOOTH},
            "adx_params": {"length": ADX_LEN},
            "rsi_params": {"length": int(rsi_default["length"]), "smoothing": int(rsi_default["smoothing"])},
            "vidya_params": {"length": VIDYA_LENGTH, "momentum": VIDYA_MOMENTUM, "band_distance": VIDYA_BAND_DISTANCE},
            "stoch_rsi_params": {"rsi_length": STOCH_RSI_RSI_LENGTH, "stoch_length": STOCH_RSI_STOCH_LENGTH, "smooth_k": STOCH_RSI_SMOOTH_K, "smooth_d": STOCH_RSI_SMOOTH_D},
        })
        _save_indicator_cache(pq_path, meta_path, meta, day_streams)


def _env_flag(name: str) -> bool:
    return execution_env_flag(name)


def _indicator_parallel_min_rows() -> int:
    return execution_env_int(
        ("BT_INDICATOR_PARALLEL_MIN_ROWS", "INDICATOR_PARALLEL_MIN_ROWS"),
        default=INDICATOR_PARALLEL_MIN_ROWS_DEFAULT,
        minimum=0,
    )


def _indicator_parallel_min_days() -> int:
    return execution_env_int(
        ("BT_INDICATOR_PARALLEL_MIN_DAYS", "INDICATOR_PARALLEL_MIN_DAYS"),
        default=INDICATOR_PARALLEL_MIN_DAYS_DEFAULT,
        minimum=1,
    )


def _indicator_parallel_worker_count(task_count: int) -> int:
    return resolve_worker_count(
        task_count,
        env_names=("BT_INDICATOR_MAX_WORKERS", "INDICATOR_MAX_WORKERS"),
        reserve_cpu=1,
    )


def _indicator_parallel_max_chunk_days() -> int:
    return execution_env_int(
        ("BT_INDICATOR_MAX_CHUNK_DAYS", "INDICATOR_MAX_CHUNK_DAYS"),
        default=INDICATOR_PARALLEL_MAX_CHUNK_DAYS_DEFAULT,
        minimum=1,
    )


def _slice_1m_source_window(
    df_1m: "pd.DataFrame",
    start_utc: "pd.Timestamp",
    end_utc: "pd.Timestamp",
) -> "pd.DataFrame":
    if not isinstance(df_1m, pd.DataFrame) or df_1m.empty:
        return df_1m.iloc[0:0].copy() if isinstance(df_1m, pd.DataFrame) else pd.DataFrame()
    start_utc = pd.to_datetime(start_utc, utc=True)
    end_utc = pd.to_datetime(end_utc, utc=True)
    ts_col = "close_time" if "close_time" in df_1m.columns else ("open_time" if "open_time" in df_1m.columns else None)
    if ts_col is None:
        return df_1m.copy()
    try:
        stamps = pd.to_datetime(df_1m[ts_col], utc=True, errors="coerce")
        mask = (stamps >= start_utc) & (stamps <= end_utc)
        return df_1m.loc[mask].copy()
    except Exception:
        return df_1m.copy()


def _build_local_indicator_day_meta(
    *,
    cache_dir: str,
    symbol: str,
    day_utc: "pd.Timestamp",
    target_start_utc: "pd.Timestamp",
    target_end_utc: "pd.Timestamp",
    source_start_utc: "pd.Timestamp",
    source_end_utc: "pd.Timestamp",
    timeframes: List[str],
    price_source: str,
    macd_impl: str,
    adx_impl: str,
    required_fields: Optional[Any],
    ema_settings: Optional[Any] = None,
    rsi_settings: Optional[Any] = None,
    market_state_thresholds: Optional[Any] = None,
    build_mode: str = "sequential_day_store",
    timing_sec: Optional[float] = None,
    source_rows: Optional[int] = None,
    stream_rows: Optional[Dict[str, int]] = None,
) -> Dict[str, Any]:
    profile = _local_indicator_store_profile(
        timeframes=timeframes,
        price_source=price_source,
        macd_impl=macd_impl,
        adx_impl=adx_impl,
        required_fields=required_fields,
        ema_settings=ema_settings,
        rsi_settings=rsi_settings,
        market_state_thresholds=market_state_thresholds,
    )
    from .rsi_settings import resolve_rsi_settings
    rsi_default = resolve_rsi_settings(rsi_settings if rsi_settings is not None else ema_settings)
    meta = dict(profile)
    meta.update({
        "created_at_utc": pd.Timestamp.now(tz="UTC").isoformat(),
        "symbol": str(symbol or "").upper(),
        "start_utc": pd.to_datetime(target_start_utc, utc=True).isoformat(),
        "end_utc": pd.to_datetime(target_end_utc, utc=True).isoformat(),
        "source_start_utc": pd.to_datetime(source_start_utc, utc=True).isoformat(),
        "source_end_utc": pd.to_datetime(source_end_utc, utc=True).isoformat(),
        "day_utc": pd.to_datetime(day_utc, utc=True).normalize().isoformat(),
        "build_mode": str(build_mode or "sequential_day_store"),
        "macd_params": {"fast": MACD_FAST, "slow": MACD_SLOW, "signal": MACD_SIGNAL},
        "ppo_params": {"fast": PPO_FAST, "slow": PPO_SLOW, "smooth": PPO_SMOOTH},
        "adx_params": {"length": ADX_LEN},
        "rsi_params": {"length": int(rsi_default["length"]), "smoothing": int(rsi_default["smoothing"])},
        "vidya_params": {"length": VIDYA_LENGTH, "momentum": VIDYA_MOMENTUM, "band_distance": VIDYA_BAND_DISTANCE},
        "stoch_rsi_params": {"rsi_length": STOCH_RSI_RSI_LENGTH, "stoch_length": STOCH_RSI_STOCH_LENGTH, "smooth_k": STOCH_RSI_SMOOTH_K, "smooth_d": STOCH_RSI_SMOOTH_D},
    })
    if timing_sec is not None:
        meta["timing_sec"] = float(timing_sec)
    if source_rows is not None:
        meta["source_rows"] = int(source_rows)
    if stream_rows is not None:
        meta["stream_rows"] = {str(k): int(v) for k, v in dict(stream_rows).items()}
    return meta


def _indicator_store_chunk_build_task(task: Dict[str, Any]) -> Dict[str, Any]:
    started = time.perf_counter()
    df_1m = task["df_1m"]
    source_start_utc = pd.to_datetime(task["source_start_utc"], utc=True)
    source_end_utc = pd.to_datetime(task["source_end_utc"], utc=True)
    streams = simulate_multitf_indicators_fast(
        df_1m,
        task["timeframes"],
        progress_cb=None,
        macd_impl=task["macd_impl"],
        adx_impl=task["adx_impl"],
        required_fields=task["required_fields"],
        ema_settings=task.get("ema_settings"),
        rsi_settings=task.get("rsi_settings"),
        market_state_thresholds=task.get("market_state_thresholds", DEFAULT_MARKET_STATE_THRESHOLDS),
    )
    timing_sec = float(time.perf_counter() - started)
    day_reports: List[Dict[str, Any]] = []
    day_targets = list(task.get("day_targets") or [])
    for day_target in day_targets:
        day_utc = pd.to_datetime(day_target["day_utc"], utc=True)
        target_start_utc = pd.to_datetime(day_target["target_start_utc"], utc=True)
        target_end_utc = pd.to_datetime(day_target["target_end_utc"], utc=True)
        sliced = _slice_stream_cache_window(streams, target_start_utc, target_end_utc)
        stream_rows = {
            str(tf): int(len(frame))
            for tf, frame in dict(sliced or {}).items()
            if isinstance(frame, pd.DataFrame)
        }
        pq_path, meta_path = _local_indicator_store_day_paths(
            task["cache_dir"],
            task["symbol"],
            day_utc,
            timeframes=task["timeframes"],
            price_source=task["price_source"],
            macd_impl=task["macd_impl"],
            adx_impl=task["adx_impl"],
            required_fields=task["required_fields"],
            ema_settings=task.get("ema_settings"),
            rsi_settings=task.get("rsi_settings"),
            market_state_thresholds=task.get("market_state_thresholds"),
        )
        meta = _build_local_indicator_day_meta(
            cache_dir=task["cache_dir"],
            symbol=task["symbol"],
            day_utc=day_utc,
            target_start_utc=target_start_utc,
            target_end_utc=target_end_utc,
            source_start_utc=source_start_utc,
            source_end_utc=source_end_utc,
            timeframes=task["timeframes"],
            price_source=task["price_source"],
            macd_impl=task["macd_impl"],
            adx_impl=task["adx_impl"],
            required_fields=task["required_fields"],
            ema_settings=task.get("ema_settings"),
            rsi_settings=task.get("rsi_settings"),
            market_state_thresholds=task.get("market_state_thresholds"),
            build_mode=task.get("build_mode", "sequential_day_store"),
            timing_sec=timing_sec,
            source_rows=int(len(df_1m)),
            stream_rows=stream_rows,
        )
        _save_indicator_cache(pq_path, meta_path, meta, sliced)
        day_reports.append({
            "day": day_utc.strftime("%Y-%m-%d"),
            "source_rows": int(len(df_1m)),
            "stream_rows": stream_rows,
            "timing_sec": timing_sec,
            "mode": str(task.get("build_mode", "sequential_day_store")),
        })
    day_reports.sort(key=lambda item: str(item.get("day") or ""))
    return {
        "days": [item.get("day") for item in day_reports],
        "day_count": len(day_reports),
        "source_rows": int(len(df_1m)),
        "timing_sec": timing_sec,
        "mode": str(task.get("build_mode", "sequential_day_store")),
        "day_reports": day_reports,
    }


def _materialize_local_indicator_store_days(
    df_1m: "pd.DataFrame",
    *,
    cache_dir: str,
    symbol: str,
    start_utc: "pd.Timestamp",
    end_utc: "pd.Timestamp",
    timeframes: List[str],
    price_source: str,
    macd_impl: str,
    adx_impl: str,
    required_fields: Optional[Any],
    ema_settings: Optional[Any] = None,
    rsi_settings: Optional[Any] = None,
    market_state_thresholds: Optional[Any] = None,
    progress_cb=None,
) -> Dict[str, Any]:
    started = time.perf_counter()
    start_utc = pd.to_datetime(start_utc, utc=True)
    end_utc = pd.to_datetime(end_utc, utc=True)
    days = _iter_utc_days(start_utc, end_utc)
    if not days:
        return {"status": "skipped", "build_mode": "none", "missing_days": 0, "workers": 0, "timing_sec": 0.0}

    missing_days: List[pd.Timestamp] = []
    for day in days:
        pq_path, meta_path = _local_indicator_store_day_paths(
            cache_dir,
            symbol,
            day,
            timeframes=timeframes,
            price_source=price_source,
            macd_impl=macd_impl,
            adx_impl=adx_impl,
            required_fields=required_fields,
            ema_settings=ema_settings,
            rsi_settings=rsi_settings,
            market_state_thresholds=market_state_thresholds,
        )
        loaded = None if (not os.path.isfile(pq_path)) or (not os.path.isfile(meta_path)) else _load_indicator_cache(pq_path, meta_path)
        if loaded is None or not _indicator_streams_have_required_fields(loaded, required_fields):
            missing_days.append(day)

    if not missing_days:
        return {
            "status": "ready",
            "build_mode": "existing_local_store",
            "missing_days": 0,
            "workers": 0,
            "timing_sec": float(time.perf_counter() - started),
            "day_reports": [],
        }

    total_rows = int(len(df_1m)) if isinstance(df_1m, pd.DataFrame) else 0
    available_days = max(0, len(days) - len(missing_days))
    if available_days <= 0 and len(days) < _indicator_parallel_min_days() and total_rows < _indicator_parallel_min_rows():
        return {
            "status": "skipped",
            "reason": "small_window",
            "build_mode": "none",
            "missing_days": len(missing_days),
            "workers": 0,
            "timing_sec": float(time.perf_counter() - started),
            "day_reports": [],
        }

    warmup_minutes = int(required_warmup_minutes(timeframes, required_fields, ema_settings=ema_settings, rsi_settings=rsi_settings))
    warmup_delta = pd.Timedelta(minutes=max(0, warmup_minutes))
    build_mode = "sequential_day_store"
    workers = 1
    can_parallel = not _env_flag("BT_INDICATOR_DISABLE_PARALLEL")
    if can_parallel and len(missing_days) >= _indicator_parallel_min_days() and total_rows >= _indicator_parallel_min_rows():
        workers = _indicator_parallel_worker_count(len(missing_days))
        if workers > 1:
            build_mode = "parallel_day_store"

    use_chunk_planner = bool(available_days <= 0 and len(missing_days) >= max(4, workers * 2))
    chunk_days = 1
    if use_chunk_planner:
        chunk_days = max(
            2,
            min(
                _indicator_parallel_max_chunk_days(),
                max(2, (len(missing_days) + max(1, workers) - 1) // max(1, workers)),
            ),
        )
        build_mode = ("parallel_chunk_store" if workers > 1 else "sequential_chunk_store")

    tasks: List[Dict[str, Any]] = []
    task_days: List[List[pd.Timestamp]] = split_batches(missing_days, batch_size=chunk_days)

    for day_group in task_days:
        normalized_days = [pd.to_datetime(day, utc=True).normalize() for day in day_group]
        first_day = normalized_days[0]
        last_day = normalized_days[-1]
        last_day_end = last_day + pd.Timedelta(days=1) - pd.Timedelta(microseconds=1)
        source_start = max(start_utc, first_day - warmup_delta)
        source_end = min(end_utc, last_day_end)
        source_df = _slice_1m_source_window(df_1m, source_start, source_end)
        if source_df.empty:
            return {
                "status": "skipped",
                "reason": f"missing_source_for_{first_day.strftime('%Y-%m-%d')}",
                "build_mode": "none",
                "missing_days": len(missing_days),
                "workers": 0,
                "timing_sec": float(time.perf_counter() - started),
                "day_reports": [],
            }
        day_targets = []
        for day_start in normalized_days:
            day_end = day_start + pd.Timedelta(days=1) - pd.Timedelta(microseconds=1)
            target_start = max(start_utc, day_start)
            target_end = min(end_utc, day_end)
            day_targets.append({
                "day_utc": day_start,
                "target_start_utc": target_start,
                "target_end_utc": target_end,
            })
        tasks.append({
            "df_1m": source_df,
            "cache_dir": cache_dir,
            "symbol": symbol,
            "source_start_utc": source_start,
            "source_end_utc": source_end,
            "day_targets": day_targets,
            "timeframes": list(timeframes or []),
            "price_source": price_source,
            "macd_impl": macd_impl,
            "adx_impl": adx_impl,
            "required_fields": required_fields,
            "ema_settings": ema_settings,
            "rsi_settings": rsi_settings,
            "market_state_thresholds": market_state_thresholds,
            "build_mode": build_mode,
        })

    if build_mode.startswith("parallel_"):
        workers = _indicator_parallel_worker_count(len(tasks))
        if workers <= 1:
            build_mode = build_mode.replace("parallel_", "sequential_", 1)
            for task in tasks:
                task["build_mode"] = build_mode

    if progress_cb:
        try:
            progress_cb(
                f"Materializing local indicator store for {len(missing_days)} day(s) across {len(tasks)} task(s) via {build_mode.replace('_', ' ')}.",
                25,
            )
        except Exception:
            pass

    day_reports: List[Dict[str, Any]] = []
    task_reports: List[Dict[str, Any]] = []
    if build_mode.startswith("parallel_"):
        with ProcessPoolExecutor(max_workers=workers) as executor:
            futures = [executor.submit(_indicator_store_chunk_build_task, task) for task in tasks]
            for idx, future in enumerate(as_completed(futures), start=1):
                report = future.result()
                task_reports.append(report)
                day_reports.extend(list(report.get("day_reports") or []))
                if progress_cb:
                    try:
                        pct = min(90, 25 + int((idx / max(1, len(tasks))) * 55))
                        report_days = ", ".join(list(report.get("days") or [])[:3])
                        progress_cb(
                            f"Built local indicator task {idx}/{len(tasks)} ({report.get('day_count', 0)} day(s): {report_days}).",
                            pct,
                        )
                    except Exception:
                        pass
    else:
        for idx, task in enumerate(tasks, start=1):
            report = _indicator_store_chunk_build_task(task)
            task_reports.append(report)
            day_reports.extend(list(report.get("day_reports") or []))
            if progress_cb:
                try:
                    pct = min(90, 25 + int((idx / max(1, len(tasks))) * 55))
                    report_days = ", ".join(list(report.get("days") or [])[:3])
                    progress_cb(
                        f"Built local indicator task {idx}/{len(tasks)} ({report.get('day_count', 0)} day(s): {report_days}).",
                        pct,
                    )
                except Exception:
                    pass

    day_reports.sort(key=lambda item: str(item.get("day") or ""))
    task_reports.sort(key=lambda item: str((list(item.get("days") or [""]) or [""])[0]))
    return {
        "status": "built",
        "build_mode": build_mode,
        "missing_days": len(missing_days),
        "task_count": len(tasks),
        "chunk_days": chunk_days,
        "workers": workers if build_mode.startswith("parallel_") else 1,
        "warmup_minutes": warmup_minutes,
        "timing_sec": float(time.perf_counter() - started),
        "day_reports": day_reports,
        "task_reports": task_reports,
    }

def _load_indicator_cache(pq_path: str, meta_path: str) -> Optional[Dict[str, "pd.DataFrame"]]:
    if (not os.path.isfile(pq_path)) or (not os.path.isfile(meta_path)):
        return None
    try:
        with open(meta_path, "r", encoding="utf-8") as f:
            meta = json.load(f)
        if int(meta.get("report_version", -1)) != INDICATOR_CACHE_VERSION:
            return None
    except Exception:
        return None

    try:
        wide = pd.read_parquet(pq_path)
    except Exception:
        try:
            wide = pd.read_pickle(pq_path)
        except Exception:
            return None

    try:
        idx = pd.to_datetime(wide.index, utc=True)
    except Exception:
        idx = wide.index
    wide.index = idx

    streams: Dict[str, pd.DataFrame] = {}
    try:
        tfs = list(meta.get("timeframes") or [])
    except Exception:
        tfs = []
    for tf in tfs:
        prefix = f"{tf}__"
        cols = [c for c in wide.columns if str(c).startswith(prefix)]
        if not cols:
            continue
        sub = wide[cols].copy()
        sub.columns = [str(c)[len(prefix):] for c in cols]

        # Decode to expected stream columns
        try:
            columns: Dict[str, Any] = {}
            columns["price"] = pd.to_numeric(sub.get("price"), errors="coerce").astype(float)
            columns["open"] = pd.to_numeric(sub.get("open"), errors="coerce").astype(float) if "open" in sub else np.nan
            columns["high"] = pd.to_numeric(sub.get("high"), errors="coerce").astype(float) if "high" in sub else np.nan
            columns["low"] = pd.to_numeric(sub.get("low"), errors="coerce").astype(float) if "low" in sub else np.nan
            columns["close"] = pd.to_numeric(sub.get("close"), errors="coerce").astype(float) if "close" in sub else columns["price"]
            columns["volume"] = pd.to_numeric(sub.get("volume"), errors="coerce").astype(float) if "volume" in sub else np.nan
            columns["macd"] = pd.to_numeric(sub.get("macd"), errors="coerce").astype(float)
            columns["signal"] = pd.to_numeric(sub.get("signal"), errors="coerce").astype(float)
            columns["rsi"] = pd.to_numeric(sub.get("rsi"), errors="coerce").astype(float) if "rsi" in sub else np.nan
            columns["rsi_smooth"] = pd.to_numeric(sub.get("rsi_smooth"), errors="coerce").astype(float) if "rsi_smooth" in sub else np.nan
            columns["stoch_rsi_k"] = pd.to_numeric(sub.get("stoch_rsi_k"), errors="coerce").astype(float) if "stoch_rsi_k" in sub else np.nan
            columns["stoch_rsi_d"] = pd.to_numeric(sub.get("stoch_rsi_d"), errors="coerce").astype(float) if "stoch_rsi_d" in sub else np.nan
            columns["ema_1"] = pd.to_numeric(sub.get("ema_1"), errors="coerce").astype(float) if "ema_1" in sub else np.nan
            columns["ema_2"] = pd.to_numeric(sub.get("ema_2"), errors="coerce").astype(float) if "ema_2" in sub else np.nan
            columns["ema_1_slope"] = pd.to_numeric(sub.get("ema_1_slope"), errors="coerce").astype(float) if "ema_1_slope" in sub else np.nan
            columns["ema_2_slope"] = pd.to_numeric(sub.get("ema_2_slope"), errors="coerce").astype(float) if "ema_2_slope" in sub else np.nan
            columns["ema_spread"] = pd.to_numeric(sub.get("ema_spread"), errors="coerce").astype(float) if "ema_spread" in sub else np.nan
            columns["ema_spread_pct"] = pd.to_numeric(sub.get("ema_spread_pct"), errors="coerce").astype(float) if "ema_spread_pct" in sub else np.nan
            columns["ema_spread_delta"] = pd.to_numeric(sub.get("ema_spread_delta"), errors="coerce").astype(float) if "ema_spread_delta" in sub else np.nan
            columns["ema_spread_pct_delta"] = pd.to_numeric(sub.get("ema_spread_pct_delta"), errors="coerce").astype(float) if "ema_spread_pct_delta" in sub else np.nan
            columns["ema_stack"] = sub.get("ema_stack") if "ema_stack" in sub else None
            columns["ema_1_angle_state"] = sub.get("ema_1_angle_state") if "ema_1_angle_state" in sub else None
            columns["ema_2_angle_state"] = sub.get("ema_2_angle_state") if "ema_2_angle_state" in sub else None
            columns["ema_spread_trend"] = sub.get("ema_spread_trend") if "ema_spread_trend" in sub else None
            columns["ema_stack_pressure"] = sub.get("ema_stack_pressure") if "ema_stack_pressure" in sub else None
            if "ema_cross_up_i" in sub:
                columns["ema_cross_up"] = pd.to_numeric(sub.get("ema_cross_up_i"), errors="coerce").fillna(0).astype(int).astype(bool)
            else:
                columns["ema_cross_up"] = False
            if "ema_cross_down_i" in sub:
                columns["ema_cross_down"] = pd.to_numeric(sub.get("ema_cross_down_i"), errors="coerce").fillna(0).astype(int).astype(bool)
            else:
                columns["ema_cross_down"] = False
            columns["ms"] = sub.get("ms_i").map(_i8_to_color) if "ms_i" in sub else None
            columns["angle"] = sub.get("angle_i").map(_i8_to_color) if "angle_i" in sub else None
            columns["sig_angle"] = sub.get("sig_angle_i").map(_i8_to_color) if "sig_angle_i" in sub else None
            columns["ppo"] = sub.get("ppo_i").map(_i8_to_color) if "ppo_i" in sub else None
            if "flip_age_i" in sub:
                fa = pd.to_numeric(sub.get("flip_age_i"), errors="coerce").fillna(-1).astype(int)
                columns["flip_age"] = np.where(fa >= 0, fa.astype(float), np.nan)
            else:
                columns["flip_age"] = np.nan
            columns["adx"] = pd.to_numeric(sub.get("adx"), errors="coerce").astype(float) if "adx" in sub else np.nan
            columns["atr"] = pd.to_numeric(sub.get("atr"), errors="coerce").astype(float) if "atr" in sub else np.nan
            columns["di_plus"] = pd.to_numeric(sub.get("di_plus"), errors="coerce").astype(float) if "di_plus" in sub else np.nan
            columns["di_minus"] = pd.to_numeric(sub.get("di_minus"), errors="coerce").astype(float) if "di_minus" in sub else np.nan
            columns["di_spread"] = pd.to_numeric(sub.get("di_spread"), errors="coerce").astype(float) if "di_spread" in sub else np.nan
            columns["adx_angle"] = sub.get("adx_angle_i").map(_i8_to_color) if "adx_angle_i" in sub else None
            columns["atr_angle"] = sub.get("atr_angle_i").map(_i8_to_color) if "atr_angle_i" in sub else None
            columns["rsi_state"] = sub.get("rsi_state_i").map(_i8_to_color) if "rsi_state_i" in sub else None
            columns["stoch_rsi_kd"] = sub.get("stoch_rsi_kd_i").map(_i8_to_color) if "stoch_rsi_kd_i" in sub else None
            columns["frama_state"] = sub.get("frama_state") if "frama_state" in sub else None
            if "frama_break_up_i" in sub:
                columns["frama_break_up"] = pd.to_numeric(sub.get("frama_break_up_i"), errors="coerce").fillna(0).astype(int).astype(bool)
            else:
                columns["frama_break_up"] = False
            if "frama_break_down_i" in sub:
                columns["frama_break_down"] = pd.to_numeric(sub.get("frama_break_down_i"), errors="coerce").fillna(0).astype(int).astype(bool)
            else:
                columns["frama_break_down"] = False
            if "frama_mid_cross_i" in sub:
                columns["frama_mid_cross"] = pd.to_numeric(sub.get("frama_mid_cross_i"), errors="coerce").fillna(0).astype(int).astype(bool)
            else:
                columns["frama_mid_cross"] = False
            columns["vidya_line"] = pd.to_numeric(sub.get("vidya_line"), errors="coerce").astype(float) if "vidya_line" in sub else np.nan
            columns["vidya_raw"] = pd.to_numeric(sub.get("vidya_raw"), errors="coerce").astype(float) if "vidya_raw" in sub else np.nan
            columns["vidya_upper"] = pd.to_numeric(sub.get("vidya_upper"), errors="coerce").astype(float) if "vidya_upper" in sub else np.nan
            columns["vidya_lower"] = pd.to_numeric(sub.get("vidya_lower"), errors="coerce").astype(float) if "vidya_lower" in sub else np.nan
            columns["vidya_state"] = sub.get("vidya_state") if "vidya_state" in sub else None
            if "vidya_trend_up_i" in sub:
                columns["vidya_trend_up"] = pd.to_numeric(sub.get("vidya_trend_up_i"), errors="coerce").fillna(0).astype(int).astype(bool)
            else:
                columns["vidya_trend_up"] = False
            if "vidya_trend_down_i" in sub:
                columns["vidya_trend_down"] = pd.to_numeric(sub.get("vidya_trend_down_i"), errors="coerce").fillna(0).astype(int).astype(bool)
            else:
                columns["vidya_trend_down"] = False
            columns["vidya_up_volume"] = pd.to_numeric(sub.get("vidya_up_volume"), errors="coerce").astype(float) if "vidya_up_volume" in sub else np.nan
            columns["vidya_down_volume"] = pd.to_numeric(sub.get("vidya_down_volume"), errors="coerce").astype(float) if "vidya_down_volume" in sub else np.nan
            columns["vidya_delta_pct"] = pd.to_numeric(sub.get("vidya_delta_pct"), errors="coerce").astype(float) if "vidya_delta_pct" in sub else np.nan
            columns["vidya_active_liq_low_count"] = pd.to_numeric(sub.get("vidya_active_liq_low_count"), errors="coerce").astype(float) if "vidya_active_liq_low_count" in sub else np.nan
            columns["vidya_active_liq_high_count"] = pd.to_numeric(sub.get("vidya_active_liq_high_count"), errors="coerce").astype(float) if "vidya_active_liq_high_count" in sub else np.nan
            columns["vidya_nearest_liq_low"] = pd.to_numeric(sub.get("vidya_nearest_liq_low"), errors="coerce").astype(float) if "vidya_nearest_liq_low" in sub else np.nan
            columns["vidya_nearest_liq_high"] = pd.to_numeric(sub.get("vidya_nearest_liq_high"), errors="coerce").astype(float) if "vidya_nearest_liq_high" in sub else np.nan
            columns["vidya_dist_to_liq_low_pct"] = pd.to_numeric(sub.get("vidya_dist_to_liq_low_pct"), errors="coerce").astype(float) if "vidya_dist_to_liq_low_pct" in sub else np.nan
            columns["vidya_dist_to_liq_high_pct"] = pd.to_numeric(sub.get("vidya_dist_to_liq_high_pct"), errors="coerce").astype(float) if "vidya_dist_to_liq_high_pct" in sub else np.nan
            if "vidya_new_liq_low_i" in sub:
                columns["vidya_new_liq_low"] = pd.to_numeric(sub.get("vidya_new_liq_low_i"), errors="coerce").fillna(0).astype(int).astype(bool)
            else:
                columns["vidya_new_liq_low"] = False
            if "vidya_new_liq_high_i" in sub:
                columns["vidya_new_liq_high"] = pd.to_numeric(sub.get("vidya_new_liq_high_i"), errors="coerce").fillna(0).astype(int).astype(bool)
            else:
                columns["vidya_new_liq_high"] = False
            if "vidya_liq_low_taken_i" in sub:
                columns["vidya_liq_low_taken"] = pd.to_numeric(sub.get("vidya_liq_low_taken_i"), errors="coerce").fillna(0).astype(int).astype(bool)
            else:
                columns["vidya_liq_low_taken"] = False
            if "vidya_liq_high_taken_i" in sub:
                columns["vidya_liq_high_taken"] = pd.to_numeric(sub.get("vidya_liq_high_taken_i"), errors="coerce").fillna(0).astype(int).astype(bool)
            else:
                columns["vidya_liq_high_taken"] = False
            columns["vidya_slope"] = pd.to_numeric(sub.get("vidya_slope"), errors="coerce").astype(float) if "vidya_slope" in sub else np.nan
            columns["vidya_angle_deg"] = pd.to_numeric(sub.get("vidya_angle_deg"), errors="coerce").astype(float) if "vidya_angle_deg" in sub else np.nan
            columns["vidya_angle_deg_norm"] = pd.to_numeric(sub.get("vidya_angle_deg_norm"), errors="coerce").astype(float) if "vidya_angle_deg_norm" in sub else np.nan
            columns["vidya_angle_state"] = sub.get("vidya_angle_state") if "vidya_angle_state" in sub else None
            columns["vidya_angle_accel"] = sub.get("vidya_angle_accel") if "vidya_angle_accel" in sub else None
            columns["market_state"] = sub.get("market_state") if "market_state" in sub else None
            columns["market_bias"] = sub.get("market_bias") if "market_bias" in sub else None
            columns["market_phase"] = sub.get("market_phase") if "market_phase" in sub else None
            columns["market_confidence"] = pd.to_numeric(sub.get("market_confidence"), errors="coerce").astype(float) if "market_confidence" in sub else np.nan
            columns["market_state_age"] = pd.to_numeric(sub.get("market_state_age"), errors="coerce").astype(float) if "market_state_age" in sub else np.nan
            columns["market_direction_score"] = pd.to_numeric(sub.get("market_direction_score"), errors="coerce").astype(float) if "market_direction_score" in sub else np.nan
            columns["market_energy_score"] = pd.to_numeric(sub.get("market_energy_score"), errors="coerce").astype(float) if "market_energy_score" in sub else np.nan
            columns["market_alignment_bonus"] = pd.to_numeric(sub.get("market_alignment_bonus"), errors="coerce").astype(float) if "market_alignment_bonus" in sub else np.nan
            columns["market_macd_gap_norm"] = pd.to_numeric(sub.get("market_macd_gap_norm"), errors="coerce").astype(float) if "market_macd_gap_norm" in sub else np.nan
            columns["market_structure_flag"] = sub.get("market_structure_flag") if "market_structure_flag" in sub else None
            if "market_structure_pass_i" in sub:
                columns["market_structure_pass"] = pd.to_numeric(sub.get("market_structure_pass_i"), errors="coerce").fillna(0).astype(int).astype(bool)
            else:
                columns["market_structure_pass"] = False
            columns["market_structure_note"] = sub.get("market_structure_note") if "market_structure_note" in sub else None
            columns["market_structure_context"] = sub.get("market_structure_context") if "market_structure_context" in sub else None
            columns["market_structure_basis"] = sub.get("market_structure_basis") if "market_structure_basis" in sub else None
            columns["market_structure_pivot_bias"] = sub.get("market_structure_pivot_bias") if "market_structure_pivot_bias" in sub else None
            columns["market_structure_last_high"] = pd.to_numeric(sub.get("market_structure_last_high"), errors="coerce").astype(float) if "market_structure_last_high" in sub else np.nan
            columns["market_structure_last_low"] = pd.to_numeric(sub.get("market_structure_last_low"), errors="coerce").astype(float) if "market_structure_last_low" in sub else np.nan
            columns["market_structure_pivot_count"] = pd.to_numeric(sub.get("market_structure_pivot_count"), errors="coerce").astype(float) if "market_structure_pivot_count" in sub else np.nan
            if "market_structure_swing_hh_i" in sub:
                columns["market_structure_swing_hh"] = pd.to_numeric(sub.get("market_structure_swing_hh_i"), errors="coerce").fillna(0).astype(int).astype(bool)
            else:
                columns["market_structure_swing_hh"] = False
            if "market_structure_swing_hl_i" in sub:
                columns["market_structure_swing_hl"] = pd.to_numeric(sub.get("market_structure_swing_hl_i"), errors="coerce").fillna(0).astype(int).astype(bool)
            else:
                columns["market_structure_swing_hl"] = False
            if "market_structure_swing_lh_i" in sub:
                columns["market_structure_swing_lh"] = pd.to_numeric(sub.get("market_structure_swing_lh_i"), errors="coerce").fillna(0).astype(int).astype(bool)
            else:
                columns["market_structure_swing_lh"] = False
            if "market_structure_swing_ll_i" in sub:
                columns["market_structure_swing_ll"] = pd.to_numeric(sub.get("market_structure_swing_ll_i"), errors="coerce").fillna(0).astype(int).astype(bool)
            else:
                columns["market_structure_swing_ll"] = False
            columns["market_pa_vol_flag"] = sub.get("market_pa_vol_flag") if "market_pa_vol_flag" in sub else None
            if "market_pa_vol_pass_i" in sub:
                columns["market_pa_vol_pass"] = pd.to_numeric(sub.get("market_pa_vol_pass_i"), errors="coerce").fillna(0).astype(int).astype(bool)
            else:
                columns["market_pa_vol_pass"] = False
            columns["market_pa_vol_note"] = sub.get("market_pa_vol_note") if "market_pa_vol_note" in sub else None
            columns["market_pa_pattern"] = sub.get("market_pa_pattern") if "market_pa_pattern" in sub else None
            columns["market_pa_body_frac"] = pd.to_numeric(sub.get("market_pa_body_frac"), errors="coerce").astype(float) if "market_pa_body_frac" in sub else np.nan
            columns["market_pa_close_frac"] = pd.to_numeric(sub.get("market_pa_close_frac"), errors="coerce").astype(float) if "market_pa_close_frac" in sub else np.nan
            columns["market_pa_upper_wick_frac"] = pd.to_numeric(sub.get("market_pa_upper_wick_frac"), errors="coerce").astype(float) if "market_pa_upper_wick_frac" in sub else np.nan
            columns["market_pa_lower_wick_frac"] = pd.to_numeric(sub.get("market_pa_lower_wick_frac"), errors="coerce").astype(float) if "market_pa_lower_wick_frac" in sub else np.nan
            columns["market_pa_range_atr"] = pd.to_numeric(sub.get("market_pa_range_atr"), errors="coerce").astype(float) if "market_pa_range_atr" in sub else np.nan
            columns["market_pa_volume_ratio"] = pd.to_numeric(sub.get("market_pa_volume_ratio"), errors="coerce").astype(float) if "market_pa_volume_ratio" in sub else np.nan
            if "market_pa_higher_high_i" in sub:
                columns["market_pa_higher_high"] = pd.to_numeric(sub.get("market_pa_higher_high_i"), errors="coerce").fillna(0).astype(int).astype(bool)
            else:
                columns["market_pa_higher_high"] = False
            if "market_pa_higher_low_i" in sub:
                columns["market_pa_higher_low"] = pd.to_numeric(sub.get("market_pa_higher_low_i"), errors="coerce").fillna(0).astype(int).astype(bool)
            else:
                columns["market_pa_higher_low"] = False
            if "market_pa_lower_high_i" in sub:
                columns["market_pa_lower_high"] = pd.to_numeric(sub.get("market_pa_lower_high_i"), errors="coerce").fillna(0).astype(int).astype(bool)
            else:
                columns["market_pa_lower_high"] = False
            if "market_pa_lower_low_i" in sub:
                columns["market_pa_lower_low"] = pd.to_numeric(sub.get("market_pa_lower_low_i"), errors="coerce").fillna(0).astype(int).astype(bool)
            else:
                columns["market_pa_lower_low"] = False
            if "market_pa_inside_bar_i" in sub:
                columns["market_pa_inside_bar"] = pd.to_numeric(sub.get("market_pa_inside_bar_i"), errors="coerce").fillna(0).astype(int).astype(bool)
            else:
                columns["market_pa_inside_bar"] = False
            if "market_pa_outside_bar_i" in sub:
                columns["market_pa_outside_bar"] = pd.to_numeric(sub.get("market_pa_outside_bar_i"), errors="coerce").fillna(0).astype(int).astype(bool)
            else:
                columns["market_pa_outside_bar"] = False
            columns["market_pa_phase2_context"] = sub.get("market_pa_phase2_context") if "market_pa_phase2_context" in sub else None
            columns["market_pa_phase2_note"] = sub.get("market_pa_phase2_note") if "market_pa_phase2_note" in sub else None
            columns["market_pa_phase3_context"] = sub.get("market_pa_phase3_context") if "market_pa_phase3_context" in sub else None
            columns["market_pa_phase3_note"] = sub.get("market_pa_phase3_note") if "market_pa_phase3_note" in sub else None
            columns["market_pa_phase4_context"] = sub.get("market_pa_phase4_context") if "market_pa_phase4_context" in sub else None
            columns["market_pa_phase4_note"] = sub.get("market_pa_phase4_note") if "market_pa_phase4_note" in sub else None
            columns["market_pa_phase5_context"] = sub.get("market_pa_phase5_context") if "market_pa_phase5_context" in sub else None
            columns["market_pa_phase5_note"] = sub.get("market_pa_phase5_note") if "market_pa_phase5_note" in sub else None
            columns["market_pa_phase6_context"] = sub.get("market_pa_phase6_context") if "market_pa_phase6_context" in sub else None
            columns["market_pa_phase6_note"] = sub.get("market_pa_phase6_note") if "market_pa_phase6_note" in sub else None
            columns["market_pa_trap_flag"] = sub.get("market_pa_trap_flag") if "market_pa_trap_flag" in sub else None
            columns["market_pa_trap_note"] = sub.get("market_pa_trap_note") if "market_pa_trap_note" in sub else None
            columns["market_pa_strict_flag"] = sub.get("market_pa_strict_flag") if "market_pa_strict_flag" in sub else None
            if "market_pa_strict_pass_i" in sub:
                columns["market_pa_strict_pass"] = pd.to_numeric(sub.get("market_pa_strict_pass_i"), errors="coerce").fillna(0).astype(int).astype(bool)
            else:
                columns["market_pa_strict_pass"] = False
            columns["market_pa_strict_note"] = sub.get("market_pa_strict_note") if "market_pa_strict_note" in sub else None
            columns["market_pa_quality_score"] = pd.to_numeric(sub.get("market_pa_quality_score"), errors="coerce").astype(float) if "market_pa_quality_score" in sub else np.nan
            if "market_pa_engulf_long_i" in sub:
                columns["market_pa_engulf_long"] = pd.to_numeric(sub.get("market_pa_engulf_long_i"), errors="coerce").fillna(0).astype(int).astype(bool)
            else:
                columns["market_pa_engulf_long"] = False
            if "market_pa_engulf_short_i" in sub:
                columns["market_pa_engulf_short"] = pd.to_numeric(sub.get("market_pa_engulf_short_i"), errors="coerce").fillna(0).astype(int).astype(bool)
            else:
                columns["market_pa_engulf_short"] = False
            if "market_pa_retest_hold_i" in sub:
                columns["market_pa_retest_hold"] = pd.to_numeric(sub.get("market_pa_retest_hold_i"), errors="coerce").fillna(0).astype(int).astype(bool)
            else:
                columns["market_pa_retest_hold"] = False
            if "market_pa_retest_fail_i" in sub:
                columns["market_pa_retest_fail"] = pd.to_numeric(sub.get("market_pa_retest_fail_i"), errors="coerce").fillna(0).astype(int).astype(bool)
            else:
                columns["market_pa_retest_fail"] = False
            if "market_pa_sequence_long_i" in sub:
                columns["market_pa_sequence_long"] = pd.to_numeric(sub.get("market_pa_sequence_long_i"), errors="coerce").fillna(0).astype(int).astype(bool)
            else:
                columns["market_pa_sequence_long"] = False
            if "market_pa_sequence_short_i" in sub:
                columns["market_pa_sequence_short"] = pd.to_numeric(sub.get("market_pa_sequence_short_i"), errors="coerce").fillna(0).astype(int).astype(bool)
            else:
                columns["market_pa_sequence_short"] = False
            if "market_pa_followthrough_pass_i" in sub:
                columns["market_pa_followthrough_pass"] = pd.to_numeric(sub.get("market_pa_followthrough_pass_i"), errors="coerce").fillna(0).astype(int).astype(bool)
            else:
                columns["market_pa_followthrough_pass"] = False
            if "market_pa_followthrough_fail_i" in sub:
                columns["market_pa_followthrough_fail"] = pd.to_numeric(sub.get("market_pa_followthrough_fail_i"), errors="coerce").fillna(0).astype(int).astype(bool)
            else:
                columns["market_pa_followthrough_fail"] = False
            if "market_pa_stall_after_impulse_i" in sub:
                columns["market_pa_stall_after_impulse"] = pd.to_numeric(sub.get("market_pa_stall_after_impulse_i"), errors="coerce").fillna(0).astype(int).astype(bool)
            else:
                columns["market_pa_stall_after_impulse"] = False
            columns["market_pa_effort_result"] = sub.get("market_pa_effort_result") if "market_pa_effort_result" in sub else None
            if "market_pa_outside_continue_i" in sub:
                columns["market_pa_outside_continue"] = pd.to_numeric(sub.get("market_pa_outside_continue_i"), errors="coerce").fillna(0).astype(int).astype(bool)
            else:
                columns["market_pa_outside_continue"] = False
            if "market_pa_acceptance_drive_i" in sub:
                columns["market_pa_acceptance_drive"] = pd.to_numeric(sub.get("market_pa_acceptance_drive_i"), errors="coerce").fillna(0).astype(int).astype(bool)
            else:
                columns["market_pa_acceptance_drive"] = False
            if "market_pa_sweep_fail_i" in sub:
                columns["market_pa_sweep_fail"] = pd.to_numeric(sub.get("market_pa_sweep_fail_i"), errors="coerce").fillna(0).astype(int).astype(bool)
            else:
                columns["market_pa_sweep_fail"] = False
            if "market_pa_reversal_risk_i" in sub:
                columns["market_pa_reversal_risk"] = pd.to_numeric(sub.get("market_pa_reversal_risk_i"), errors="coerce").fillna(0).astype(int).astype(bool)
            else:
                columns["market_pa_reversal_risk"] = False
            if "market_pa_constructive_pause_i" in sub:
                columns["market_pa_constructive_pause"] = pd.to_numeric(sub.get("market_pa_constructive_pause_i"), errors="coerce").fillna(0).astype(int).astype(bool)
            else:
                columns["market_pa_constructive_pause"] = False
            if "market_pa_rejection_cluster_i" in sub:
                columns["market_pa_rejection_cluster"] = pd.to_numeric(sub.get("market_pa_rejection_cluster_i"), errors="coerce").fillna(0).astype(int).astype(bool)
            else:
                columns["market_pa_rejection_cluster"] = False
            if "market_pa_ladder_fail_i" in sub:
                columns["market_pa_ladder_fail"] = pd.to_numeric(sub.get("market_pa_ladder_fail_i"), errors="coerce").fillna(0).astype(int).astype(bool)
            else:
                columns["market_pa_ladder_fail"] = False
            if "market_pa_sequence_exhaust_i" in sub:
                columns["market_pa_sequence_exhaust"] = pd.to_numeric(sub.get("market_pa_sequence_exhaust_i"), errors="coerce").fillna(0).astype(int).astype(bool)
            else:
                columns["market_pa_sequence_exhaust"] = False
            if "market_pa_swing_continue_i" in sub:
                columns["market_pa_swing_continue"] = pd.to_numeric(sub.get("market_pa_swing_continue_i"), errors="coerce").fillna(0).astype(int).astype(bool)
            else:
                columns["market_pa_swing_continue"] = False
            columns["market_warning_note"] = sub.get("market_warning_note") if "market_warning_note" in sub else None
            columns["market_trend_state"] = sub.get("market_trend_state") if "market_trend_state" in sub else None
            columns["market_structure_state"] = sub.get("market_structure_state") if "market_structure_state" in sub else None
            columns["market_phase_state"] = sub.get("market_phase_state") if "market_phase_state" in sub else None
            columns["market_breakout_signal"] = sub.get("market_breakout_signal") if "market_breakout_signal" in sub else None
            columns["market_volume_context"] = sub.get("market_volume_context") if "market_volume_context" in sub else None
            columns["market_trend_age"] = pd.to_numeric(sub.get("market_trend_age"), errors="coerce").astype(float) if "market_trend_age" in sub else np.nan
            columns["market_phase_age"] = pd.to_numeric(sub.get("market_phase_age"), errors="coerce").astype(float) if "market_phase_age" in sub else np.nan
            columns["market_breakout_signal_age"] = pd.to_numeric(sub.get("market_breakout_signal_age"), errors="coerce").astype(float) if "market_breakout_signal_age" in sub else np.nan
            columns["market_protected_high"] = pd.to_numeric(sub.get("market_protected_high"), errors="coerce").astype(float) if "market_protected_high" in sub else np.nan
            columns["market_protected_low"] = pd.to_numeric(sub.get("market_protected_low"), errors="coerce").astype(float) if "market_protected_low" in sub else np.nan
            columns["market_context_note"] = sub.get("market_context_note") if "market_context_note" in sub else None
            columns["market_context_reasons"] = sub.get("market_context_reasons") if "market_context_reasons" in sub else None
            columns["taker_buy_volume"] = pd.to_numeric(sub.get("taker_buy_volume"), errors="coerce").astype(float) if "taker_buy_volume" in sub else np.nan
            columns["taker_sell_volume"] = pd.to_numeric(sub.get("taker_sell_volume"), errors="coerce").astype(float) if "taker_sell_volume" in sub else np.nan
            columns["taker_delta_volume"] = pd.to_numeric(sub.get("taker_delta_volume"), errors="coerce").astype(float) if "taker_delta_volume" in sub else np.nan
            columns["taker_delta_pct"] = pd.to_numeric(sub.get("taker_delta_pct"), errors="coerce").astype(float) if "taker_delta_pct" in sub else np.nan
            columns["taker_buy_share_pct"] = pd.to_numeric(sub.get("taker_buy_share_pct"), errors="coerce").astype(float) if "taker_buy_share_pct" in sub else np.nan
            columns["taker_trade_count"] = pd.to_numeric(sub.get("taker_trade_count"), errors="coerce").astype(float) if "taker_trade_count" in sub else np.nan
            columns["taker_bias_state"] = sub.get("taker_bias_state") if "taker_bias_state" in sub else None
            columns["range_filter_line"] = pd.to_numeric(sub.get("range_filter_line"), errors="coerce").astype(float) if "range_filter_line" in sub else np.nan
            columns["range_filter_upper"] = pd.to_numeric(sub.get("range_filter_upper"), errors="coerce").astype(float) if "range_filter_upper" in sub else np.nan
            columns["range_filter_lower"] = pd.to_numeric(sub.get("range_filter_lower"), errors="coerce").astype(float) if "range_filter_lower" in sub else np.nan
            columns["range_filter_smooth_range"] = pd.to_numeric(sub.get("range_filter_smooth_range"), errors="coerce").astype(float) if "range_filter_smooth_range" in sub else np.nan
            columns["range_filter_state"] = sub.get("range_filter_state") if "range_filter_state" in sub else None
            columns["range_filter_phase"] = sub.get("range_filter_phase") if "range_filter_phase" in sub else None
            if "range_filter_buy_i" in sub:
                columns["range_filter_buy"] = pd.to_numeric(sub.get("range_filter_buy_i"), errors="coerce").fillna(0).astype(int).astype(bool)
            else:
                columns["range_filter_buy"] = False
            if "range_filter_sell_i" in sub:
                columns["range_filter_sell"] = pd.to_numeric(sub.get("range_filter_sell_i"), errors="coerce").fillna(0).astype(int).astype(bool)
            else:
                columns["range_filter_sell"] = False
            if "range_filter_long_cond_i" in sub:
                columns["range_filter_long_cond"] = pd.to_numeric(sub.get("range_filter_long_cond_i"), errors="coerce").fillna(0).astype(int).astype(bool)
            else:
                columns["range_filter_long_cond"] = False
            if "range_filter_short_cond_i" in sub:
                columns["range_filter_short_cond"] = pd.to_numeric(sub.get("range_filter_short_cond_i"), errors="coerce").fillna(0).astype(int).astype(bool)
            else:
                columns["range_filter_short_cond"] = False
            columns["range_filter_cond_ini"] = pd.to_numeric(sub.get("range_filter_cond_ini"), errors="coerce").astype(float) if "range_filter_cond_ini" in sub else np.nan
            columns["range_filter_upward_count"] = pd.to_numeric(sub.get("range_filter_upward_count"), errors="coerce").astype(float) if "range_filter_upward_count" in sub else np.nan
            columns["range_filter_downward_count"] = pd.to_numeric(sub.get("range_filter_downward_count"), errors="coerce").astype(float) if "range_filter_downward_count" in sub else np.nan
            df = pd.DataFrame(columns, index=wide.index)
            streams[str(tf)] = df
        except Exception:
            continue

    if not streams:
        return None
    return streams

def _indicator_cache_temp_path(target_path: str) -> str:
    return f"{target_path}.{os.getpid()}.{uuid.uuid4().hex}.tmp"


def _replace_indicator_cache_file(src_path: str, dst_path: str, *, attempts: int = 8, base_sleep_sec: float = 0.03) -> None:
    last_exc: Optional[BaseException] = None
    for attempt in range(max(1, int(attempts))):
        try:
            os.replace(src_path, dst_path)
            return
        except FileNotFoundError:
            raise
        except PermissionError as exc:
            last_exc = exc
        except OSError as exc:
            winerror = getattr(exc, "winerror", None)
            if winerror not in (5, 32):
                raise
            last_exc = exc
        if not os.path.isfile(src_path):
            raise FileNotFoundError(src_path)
        time.sleep(float(base_sleep_sec) * float(attempt + 1))
    if last_exc is not None:
        raise last_exc


def _save_indicator_cache(pq_path: str, meta_path: str, meta: Dict[str, Any], streams: Dict[str, "pd.DataFrame"]):
    # Build a wide frame using only the columns that were actually materialized.
    wide = None
    color_keys = ("ms", "angle", "sig_angle", "ppo", "adx_angle", "atr_angle", "rsi_state", "stoch_rsi_kd")
    numeric_keys = (
        "price", "macd", "signal", "rsi", "rsi_smooth", "stoch_rsi_k", "stoch_rsi_d", "ema_1", "ema_2", "ema_1_slope", "ema_2_slope", "ema_spread", "ema_spread_pct",
        "ema_spread_delta", "ema_spread_pct_delta",
        "adx", "atr", "di_plus", "di_minus", "di_spread",
        "frama", "frama_upper", "frama_lower", "frama_alpha",
        "vidya_line", "vidya_raw", "vidya_upper", "vidya_lower", "vidya_up_volume", "vidya_down_volume",
        "vidya_delta_pct", "vidya_active_liq_low_count", "vidya_active_liq_high_count", "vidya_nearest_liq_low",
        "vidya_nearest_liq_high", "vidya_dist_to_liq_low_pct", "vidya_dist_to_liq_high_pct", "vidya_slope",
        "vidya_angle_deg", "vidya_angle_deg_norm", "market_confidence", "market_state_age", "market_direction_score",
        "market_energy_score", "market_alignment_bonus", "market_macd_gap_norm", "market_structure_last_high",
        "market_structure_last_low", "market_structure_pivot_count", "market_trend_age", "market_phase_age",
        "market_breakout_signal_age", "market_protected_high", "market_protected_low", "market_pa_body_frac", "market_pa_close_frac",
        "market_pa_upper_wick_frac", "market_pa_lower_wick_frac", "market_pa_range_atr", "market_pa_volume_ratio",
        "market_pa_quality_score", "range_filter_line", "range_filter_upper", "range_filter_lower",
        "range_filter_smooth_range", "range_filter_cond_ini", "range_filter_upward_count", "range_filter_downward_count",
        "taker_buy_volume", "taker_sell_volume", "taker_delta_volume", "taker_delta_pct", "taker_buy_share_pct", "taker_trade_count",
    )
    object_keys = (
        "ema_stack", "ema_1_angle_state", "ema_2_angle_state", "ema_spread_trend", "ema_stack_pressure", "frama_state", "vidya_state", "vidya_angle_state",
        "vidya_angle_accel", "market_state", "market_bias",
        "market_phase", "market_structure_flag", "market_structure_note", "market_structure_context",
        "market_structure_basis", "market_structure_pivot_bias", "market_pa_vol_flag", "market_pa_vol_note",
        "market_pa_pattern", "market_pa_phase2_context", "market_pa_phase2_note", "market_pa_phase3_context",
        "market_pa_phase3_note", "market_pa_phase4_context", "market_pa_phase4_note", "market_pa_phase5_context",
        "market_pa_phase5_note", "market_pa_phase6_context", "market_pa_phase6_note", "market_pa_trap_flag",
        "market_pa_trap_note", "market_pa_strict_flag", "market_pa_strict_note", "market_pa_effort_result",
        "market_warning_note", "market_trend_state", "market_structure_state", "market_phase_state",
        "market_breakout_signal", "market_volume_context", "market_context_note", "market_context_reasons",
        "range_filter_state", "range_filter_phase", "taker_bias_state",
    )
    bool_keys = (
        "ema_cross_up", "ema_cross_down", "frama_break_up", "frama_break_down", "frama_mid_cross",
        "vidya_trend_up", "vidya_trend_down",
        "vidya_new_liq_low", "vidya_new_liq_high", "vidya_liq_low_taken", "vidya_liq_high_taken",
        "market_structure_pass", "market_structure_swing_hh", "market_structure_swing_hl", "market_structure_swing_lh",
        "market_structure_swing_ll", "market_pa_vol_pass", "market_pa_higher_high", "market_pa_higher_low",
        "market_pa_lower_high", "market_pa_lower_low", "market_pa_inside_bar", "market_pa_outside_bar",
        "market_pa_strict_pass", "market_pa_engulf_long", "market_pa_engulf_short", "market_pa_retest_hold",
        "market_pa_retest_fail", "market_pa_sequence_long", "market_pa_sequence_short", "market_pa_followthrough_pass",
        "market_pa_followthrough_fail", "market_pa_stall_after_impulse", "market_pa_outside_continue",
        "market_pa_acceptance_drive", "market_pa_sweep_fail", "market_pa_reversal_risk",
        "market_pa_constructive_pause", "market_pa_rejection_cluster", "market_pa_ladder_fail",
        "market_pa_sequence_exhaust", "market_pa_swing_continue", "range_filter_buy", "range_filter_sell",
        "range_filter_long_cond", "range_filter_short_cond",
    )

    for tf, df in streams.items():
        if df is None or df.empty:
            continue
        packed_columns: Dict[str, Any] = {}
        for key in numeric_keys:
            if key in df:
                packed_columns[f"{tf}__{key}"] = pd.to_numeric(df.get(key), errors="coerce").astype(float)
        for key in color_keys:
            if key in df:
                packed_columns[f"{tf}__{key}_i"] = df.get(key).map(_color_to_i8).astype(np.int8)
        if "flip_age" in df:
            fa_i = pd.to_numeric(df.get("flip_age"), errors="coerce").fillna(-1).astype(int)
            packed_columns[f"{tf}__flip_age_i"] = fa_i.astype(np.int32)
        for key in object_keys:
            if key in df:
                packed_columns[f"{tf}__{key}"] = df.get(key)
        for key in bool_keys:
            if key in df:
                packed_columns[f"{tf}__{key}_i"] = _bool_i8_series(df.get(key), df.index)
        base = pd.DataFrame(packed_columns, index=df.index)
        wide = base if wide is None else wide.join(base, how="outer")
    if wide is None:
        return

    tmp_pq = _indicator_cache_temp_path(pq_path)
    tmp_meta = _indicator_cache_temp_path(meta_path)
    try:
        try:
            wide.to_parquet(tmp_pq, index=True)
        except Exception:
            wide.to_pickle(tmp_pq)
        with open(tmp_meta, "w", encoding="utf-8") as f:
            json.dump(meta, f, indent=2, ensure_ascii=False)
        _replace_indicator_cache_file(tmp_pq, pq_path)
        _replace_indicator_cache_file(tmp_meta, meta_path)
    finally:
        try:
            if os.path.isfile(tmp_pq):
                os.remove(tmp_pq)
        except Exception:
            pass
        try:
            if os.path.isfile(tmp_meta):
                os.remove(tmp_meta)
        except Exception:
            pass

def simulate_multitf_indicators(
    df_1m: "pd.DataFrame",
    timeframes: List[str],
    progress_cb=None,
    macd_impl: str = "TRADINGVIEW",
    adx_impl: str = "TRADINGVIEW",
    *,
    cache_dir: Optional[str] = None,
    symbol: Optional[str] = None,
    start_utc: Optional["pd.Timestamp"] = None,
    end_utc: Optional["pd.Timestamp"] = None,
    price_source: str = "LAST",
    use_cache: bool = True,
    required_fields: Optional[Any] = None,
    ema_settings: Optional[Any] = None,
    rsi_settings: Optional[Any] = None,
    market_state_thresholds: MarketStateThresholds = DEFAULT_MARKET_STATE_THRESHOLDS,
    save_exact_cache: bool = False,
) -> Dict[str, "pd.DataFrame"]:
    """
    Public API used by the backtest worker.
    - Uses fast simulation.
    - Persists canonical day-store data when a cache directory is configured.
    - Exact-window indicator cache files are opt-in because they duplicate the day-store.
    """
    timings: Dict[str, float] = {}
    build_mode = "monolithic_fast"
    build_stats: Dict[str, Any] = {}
    # Determine cache window defaults from df_1m if not provided.
    if start_utc is None:
        try:
            start_utc = pd.to_datetime(df_1m["open_time"].min(), utc=True)
        except Exception:
            start_utc = pd.Timestamp.now(tz="UTC")
    if end_utc is None:
        try:
            end_utc = pd.to_datetime(df_1m["close_time"].max(), utc=True)
        except Exception:
            end_utc = pd.Timestamp.now(tz="UTC")
    start_utc = pd.to_datetime(start_utc, utc=True)
    end_utc = pd.to_datetime(end_utc, utc=True)
    from .rsi_settings import resolve_rsi_settings
    resolved_rsi = resolve_rsi_settings(rsi_settings if rsi_settings is not None else ema_settings)

    if cache_dir and use_cache and symbol:
        lookup_started = time.perf_counter()
        pq, meta_path = _indicator_cache_paths(
            cache_dir,
            symbol,
            start_utc,
            end_utc,
            timeframes,
            price_source,
            macd_impl,
            adx_impl,
            required_fields=required_fields,
            ema_settings=ema_settings,
            rsi_settings=rsi_settings,
        )
        cached = _load_indicator_cache(pq, meta_path)
        timings["exact_cache_lookup_sec"] = float(time.perf_counter() - lookup_started)
        if cached is not None and _indicator_streams_have_required_fields(cached, required_fields):
            if progress_cb:
                try:
                    progress_cb(f"Loaded indicator cache ({os.path.basename(pq)}).", 100)
                except Exception:
                    pass
            return cached
        covering_started = time.perf_counter()
        covering = _find_covering_indicator_cache(
            cache_dir,
            symbol,
            start_utc,
            end_utc,
            timeframes,
            price_source,
            macd_impl,
            adx_impl,
            required_fields=required_fields,
            ema_settings=ema_settings,
            rsi_settings=rsi_settings,
        )
        timings["covering_cache_lookup_sec"] = float(time.perf_counter() - covering_started)
        if covering is not None:
            cover_pq, cover_meta_path, _cover_meta = covering
            cover_streams = _load_indicator_cache(cover_pq, cover_meta_path)
            if cover_streams is not None and _indicator_streams_have_required_fields(cover_streams, required_fields):
                sliced_streams = _slice_stream_cache_window(cover_streams, start_utc, end_utc)
                if progress_cb:
                    try:
                        progress_cb(f"Loaded covering indicator cache ({os.path.basename(cover_pq)}).", 100)
                    except Exception:
                        pass
                if save_exact_cache:
                    try:
                        meta = {
                            "report_version": INDICATOR_CACHE_VERSION,
                            "created_at_utc": pd.Timestamp.now(tz="UTC").isoformat(),
                            "symbol": symbol.upper(),
                            "start_utc": pd.to_datetime(start_utc, utc=True).isoformat(),
                            "end_utc": pd.to_datetime(end_utc, utc=True).isoformat(),
                            "timeframes": list(timeframes or []),
                            "price_source": str(price_source or "LAST").upper(),
                            "macd_impl": str(macd_impl or "TRADINGVIEW").upper(),
                            "adx_impl": str(adx_impl or "TRADINGVIEW").upper(),
                            "macd_params": {"fast": MACD_FAST, "slow": MACD_SLOW, "signal": MACD_SIGNAL},
                            "ppo_params": {"fast": PPO_FAST, "slow": PPO_SLOW, "smooth": PPO_SMOOTH},
                            "adx_params": {"length": ADX_LEN},
                            "rsi_params": {"length": int(resolved_rsi["length"]), "smoothing": int(resolved_rsi["smoothing"])},
                            "vidya_params": {"length": VIDYA_LENGTH, "momentum": VIDYA_MOMENTUM, "band_distance": VIDYA_BAND_DISTANCE},
                            "stoch_rsi_params": {"rsi_length": STOCH_RSI_RSI_LENGTH, "stoch_length": STOCH_RSI_STOCH_LENGTH, "smooth_k": STOCH_RSI_SMOOTH_K, "smooth_d": STOCH_RSI_SMOOTH_D},
                            "required_families": sorted(normalize_required_families(required_fields, include_defaults=True)),
                            "ema_profile_signature": _ema_profile_signature_for_request(required_fields, ema_settings, timeframes),
                            "rsi_profile_signature": _rsi_profile_signature_for_request(required_fields, rsi_settings if rsi_settings is not None else ema_settings, timeframes),
                            "build_mode": "covering_cache_slice",
                            "build_stats": {"covering_cache_file": os.path.basename(cover_pq)},
                            "timings": dict(timings),
                        }
                        _save_indicator_cache(pq, meta_path, meta, sliced_streams)
                    except Exception:
                        pass
                return sliced_streams
        local_started = time.perf_counter()
        local_store = _load_local_indicator_store_window(
            cache_dir,
            symbol,
            start_utc,
            end_utc,
            timeframes=timeframes,
            price_source=price_source,
            macd_impl=macd_impl,
            adx_impl=adx_impl,
            required_fields=required_fields,
            ema_settings=ema_settings,
            rsi_settings=rsi_settings,
            market_state_thresholds=market_state_thresholds,
        )
        timings["local_store_lookup_sec"] = float(time.perf_counter() - local_started)
        if local_store is not None:
            if progress_cb:
                try:
                    progress_cb("Loaded local indicator store.", 100)
                except Exception:
                    pass
            if save_exact_cache:
                try:
                    meta = {
                        "report_version": INDICATOR_CACHE_VERSION,
                        "created_at_utc": pd.Timestamp.now(tz="UTC").isoformat(),
                        "symbol": symbol.upper(),
                        "start_utc": pd.to_datetime(start_utc, utc=True).isoformat(),
                        "end_utc": pd.to_datetime(end_utc, utc=True).isoformat(),
                        "timeframes": list(timeframes or []),
                        "price_source": str(price_source or "LAST").upper(),
                        "macd_impl": str(macd_impl or "TRADINGVIEW").upper(),
                        "adx_impl": str(adx_impl or "TRADINGVIEW").upper(),
                        "macd_params": {"fast": MACD_FAST, "slow": MACD_SLOW, "signal": MACD_SIGNAL},
                        "ppo_params": {"fast": PPO_FAST, "slow": PPO_SLOW, "smooth": PPO_SMOOTH},
                        "adx_params": {"length": ADX_LEN},
                        "rsi_params": {"length": int(resolved_rsi["length"]), "smoothing": int(resolved_rsi["smoothing"])},
                        "vidya_params": {"length": VIDYA_LENGTH, "momentum": VIDYA_MOMENTUM, "band_distance": VIDYA_BAND_DISTANCE},
                        "stoch_rsi_params": {"rsi_length": STOCH_RSI_RSI_LENGTH, "stoch_length": STOCH_RSI_STOCH_LENGTH, "smooth_k": STOCH_RSI_SMOOTH_K, "smooth_d": STOCH_RSI_SMOOTH_D},
                        "required_families": sorted(normalize_required_families(required_fields, include_defaults=True)),
                        "ema_profile_signature": _ema_profile_signature_for_request(required_fields, ema_settings, timeframes),
                        "rsi_profile_signature": _rsi_profile_signature_for_request(required_fields, rsi_settings if rsi_settings is not None else ema_settings, timeframes),
                        "build_mode": "existing_local_store",
                        "build_stats": {"source_rows": int(len(df_1m)) if isinstance(df_1m, pd.DataFrame) else 0},
                        "timings": dict(timings),
                    }
                    _save_indicator_cache(pq, meta_path, meta, local_store)
                except Exception:
                    pass
            return local_store

        materialize_started = time.perf_counter()
        materialized = _materialize_local_indicator_store_days(
            df_1m,
            cache_dir=cache_dir,
            symbol=symbol,
            start_utc=start_utc,
            end_utc=end_utc,
            timeframes=timeframes,
            price_source=price_source,
            macd_impl=macd_impl,
            adx_impl=adx_impl,
            required_fields=required_fields,
            ema_settings=ema_settings,
            rsi_settings=rsi_settings,
            market_state_thresholds=market_state_thresholds,
            progress_cb=progress_cb,
        )
        timings["local_store_materialize_sec"] = float(time.perf_counter() - materialize_started)
        if materialized.get("status") in {"built", "ready"}:
            reload_started = time.perf_counter()
            local_store = _load_local_indicator_store_window(
                cache_dir,
                symbol,
                start_utc,
                end_utc,
                timeframes=timeframes,
                price_source=price_source,
                macd_impl=macd_impl,
                adx_impl=adx_impl,
                required_fields=required_fields,
                ema_settings=ema_settings,
                rsi_settings=rsi_settings,
                market_state_thresholds=market_state_thresholds,
            )
            timings["local_store_reload_sec"] = float(time.perf_counter() - reload_started)
            if local_store is not None:
                build_mode = str(materialized.get("build_mode") or "existing_local_store")
                build_stats = dict(materialized)
                if save_exact_cache:
                    try:
                        meta = {
                            "report_version": INDICATOR_CACHE_VERSION,
                            "created_at_utc": pd.Timestamp.now(tz="UTC").isoformat(),
                            "symbol": symbol.upper(),
                            "start_utc": start_utc.isoformat(),
                            "end_utc": end_utc.isoformat(),
                            "timeframes": list(timeframes or []),
                            "price_source": str(price_source or "LAST").upper(),
                            "macd_impl": str(macd_impl or "TRADINGVIEW").upper(),
                            "adx_impl": str(adx_impl or "TRADINGVIEW").upper(),
                            "macd_params": {"fast": MACD_FAST, "slow": MACD_SLOW, "signal": MACD_SIGNAL},
                            "ppo_params": {"fast": PPO_FAST, "slow": PPO_SLOW, "smooth": PPO_SMOOTH},
                            "adx_params": {"length": ADX_LEN},
                            "rsi_params": {"length": int(resolved_rsi["length"]), "smoothing": int(resolved_rsi["smoothing"])},
                            "vidya_params": {"length": VIDYA_LENGTH, "momentum": VIDYA_MOMENTUM, "band_distance": VIDYA_BAND_DISTANCE},
                            "stoch_rsi_params": {"rsi_length": STOCH_RSI_RSI_LENGTH, "stoch_length": STOCH_RSI_STOCH_LENGTH, "smooth_k": STOCH_RSI_SMOOTH_K, "smooth_d": STOCH_RSI_SMOOTH_D},
                            "required_families": sorted(normalize_required_families(required_fields, include_defaults=True)),
                            "ema_profile_signature": _ema_profile_signature_for_request(required_fields, ema_settings, timeframes),
                            "rsi_profile_signature": _rsi_profile_signature_for_request(required_fields, rsi_settings if rsi_settings is not None else ema_settings, timeframes),
                            "build_mode": build_mode,
                            "build_stats": build_stats,
                            "timings": dict(timings),
                        }
                        _save_indicator_cache(pq, meta_path, meta, local_store)
                    except Exception:
                        pass
                return local_store

    simulate_started = time.perf_counter()
    streams = simulate_multitf_indicators_fast(
        df_1m,
        timeframes,
        progress_cb=progress_cb,
        macd_impl=macd_impl,
        adx_impl=adx_impl,
        required_fields=required_fields,
        ema_settings=ema_settings,
        rsi_settings=rsi_settings,
        market_state_thresholds=market_state_thresholds,
    )
    timings["simulate_core_sec"] = float(time.perf_counter() - simulate_started)

    if cache_dir and use_cache and symbol:
        try:
            pq, meta_path = _indicator_cache_paths(
                cache_dir,
                symbol,
                start_utc,
                end_utc,
                timeframes,
                price_source,
                macd_impl,
                adx_impl,
                required_fields=required_fields,
                ema_settings=ema_settings,
                rsi_settings=rsi_settings,
            )
            if save_exact_cache:
                meta = {
                    "report_version": INDICATOR_CACHE_VERSION,
                    "created_at_utc": pd.Timestamp.now(tz="UTC").isoformat(),
                    "symbol": symbol.upper(),
                    "start_utc": pd.to_datetime(start_utc, utc=True).isoformat(),
                    "end_utc": pd.to_datetime(end_utc, utc=True).isoformat(),
                    "timeframes": list(timeframes or []),
                    "price_source": str(price_source or "LAST").upper(),
                    "macd_impl": str(macd_impl or "TRADINGVIEW").upper(),
                    "adx_impl": str(adx_impl or "TRADINGVIEW").upper(),
                    "macd_params": {"fast": MACD_FAST, "slow": MACD_SLOW, "signal": MACD_SIGNAL},
                    "ppo_params": {"fast": PPO_FAST, "slow": PPO_SLOW, "smooth": PPO_SMOOTH},
                    "adx_params": {"length": ADX_LEN},
                    "rsi_params": {"length": int(resolved_rsi["length"]), "smoothing": int(resolved_rsi["smoothing"])},
                    "vidya_params": {"length": VIDYA_LENGTH, "momentum": VIDYA_MOMENTUM, "band_distance": VIDYA_BAND_DISTANCE},
                    "stoch_rsi_params": {"rsi_length": STOCH_RSI_RSI_LENGTH, "stoch_length": STOCH_RSI_STOCH_LENGTH, "smooth_k": STOCH_RSI_SMOOTH_K, "smooth_d": STOCH_RSI_SMOOTH_D},
                    "required_families": sorted(normalize_required_families(required_fields, include_defaults=True)),
                    "ema_profile_signature": _ema_profile_signature_for_request(required_fields, ema_settings, timeframes),
                    "rsi_profile_signature": _rsi_profile_signature_for_request(required_fields, rsi_settings if rsi_settings is not None else ema_settings, timeframes),
                    "build_mode": build_mode,
                    "build_stats": {
                        **dict(build_stats),
                        "source_rows": int(len(df_1m)) if isinstance(df_1m, pd.DataFrame) else 0,
                    },
                    "timings": dict(timings),
                }
                exact_save_started = time.perf_counter()
                _save_indicator_cache(pq, meta_path, meta, streams)
                timings["exact_cache_save_sec"] = float(time.perf_counter() - exact_save_started)
            local_save_started = time.perf_counter()
            _save_local_indicator_store(
                cache_dir,
                symbol,
                streams,
                timeframes=timeframes,
                price_source=price_source,
                macd_impl=macd_impl,
                adx_impl=adx_impl,
                required_fields=required_fields,
                ema_settings=ema_settings,
                rsi_settings=rsi_settings,
                market_state_thresholds=market_state_thresholds,
            )
            timings["local_store_save_sec"] = float(time.perf_counter() - local_save_started)
            if progress_cb:
                try:
                    if save_exact_cache:
                        progress_cb(f"Saved indicator cache ({os.path.basename(pq)}).", 100)
                    else:
                        progress_cb("Saved local indicator day-store.", 100)
                except Exception:
                    pass
        except Exception:
            # never fail the backtest due to caching
            pass

    return streams
