from __future__ import annotations

from typing import Any, Dict, Optional, Sequence

import numpy as np
import pandas as pd

from .indicators_streaming import ema_tradingview


EMA_PAIR_DEFAULT_PAYLOAD: Dict[str, Any] = {
    "ema_1": None,
    "ema_2": None,
    "ema_stack": "NEUTRAL",
    "ema_cross_up": None,
    "ema_cross_down": None,
    "ema_1_slope": None,
    "ema_2_slope": None,
    "ema_1_angle_state": None,
    "ema_2_angle_state": None,
    "ema_spread": None,
    "ema_spread_pct": None,
    "ema_spread_delta": None,
    "ema_spread_pct_delta": None,
    "ema_spread_trend": None,
    "ema_stack_pressure": None,
}


def _angle_state_array(slopes: np.ndarray) -> np.ndarray:
    states = np.empty(slopes.size, dtype=object)
    states[:] = None
    valid = np.isfinite(slopes)
    states[valid & (slopes > 0.0)] = "UP"
    states[valid & (slopes < 0.0)] = "DOWN"
    states[valid & (slopes == 0.0)] = "FLAT"
    return states


def _finite_or_none(value: Any) -> Optional[float]:
    try:
        num = float(value)
    except Exception:
        return None
    return num if np.isfinite(num) else None


def _spread_trend_array(spreads: np.ndarray) -> np.ndarray:
    states = np.empty(spreads.size, dtype=object)
    states[:] = None
    abs_spread = np.abs(spreads)
    prev_abs_spread = np.concatenate(([np.nan], abs_spread[:-1]))
    valid = np.isfinite(abs_spread) & np.isfinite(prev_abs_spread)
    delta = abs_spread - prev_abs_spread
    flat = valid & np.isclose(delta, 0.0, rtol=1e-9, atol=1e-12)
    states[valid & (delta > 0.0) & ~flat] = "EXPANDING"
    states[valid & (delta < 0.0) & ~flat] = "CONTRACTING"
    states[flat] = "FLAT"
    return states


def _stack_pressure_array(stacks: np.ndarray, spread_trend: np.ndarray) -> np.ndarray:
    pressure = np.empty(stacks.size, dtype=object)
    pressure[:] = "NEUTRAL"
    bullish = stacks == "BUY"
    bearish = stacks == "SELL"
    pressure[bullish & (spread_trend == "EXPANDING")] = "BULL_ACCELERATING"
    pressure[bullish & (spread_trend == "CONTRACTING")] = "BULL_DECELERATING"
    pressure[bearish & (spread_trend == "EXPANDING")] = "BEAR_ACCELERATING"
    pressure[bearish & (spread_trend == "CONTRACTING")] = "BEAR_DECELERATING"
    return pressure


def compute_ema_pair_arrays(
    closes: Sequence[float],
    ema_1_length: int,
    ema_2_length: int,
) -> Dict[str, np.ndarray]:
    try:
        raw_closes = [] if closes is None else list(closes)
        series = np.asarray(raw_closes, dtype=float)
    except Exception:
        return {}
    if series.size == 0:
        return {}

    try:
        fast_length = max(1, int(ema_1_length or 9))
    except Exception:
        fast_length = 9
    try:
        slow_length = max(1, int(ema_2_length or 21))
    except Exception:
        slow_length = 21

    try:
        ema_1 = np.asarray(ema_tradingview(series, fast_length), dtype=float)
        ema_2 = np.asarray(ema_tradingview(series, slow_length), dtype=float)
    except Exception:
        return {}
    if ema_1.size == 0 or ema_2.size == 0 or ema_1.size != series.size or ema_2.size != series.size:
        return {}

    prev_1 = np.concatenate(([np.nan], ema_1[:-1]))
    prev_2 = np.concatenate(([np.nan], ema_2[:-1]))

    valid_pair = np.isfinite(ema_1) & np.isfinite(ema_2)
    valid_prev_pair = np.isfinite(prev_1) & np.isfinite(prev_2)
    valid_cross = valid_pair & valid_prev_pair

    ema_1_slope = ema_1 - prev_1
    ema_2_slope = ema_2 - prev_2
    ema_1_slope[~(np.isfinite(ema_1) & np.isfinite(prev_1))] = np.nan
    ema_2_slope[~(np.isfinite(ema_2) & np.isfinite(prev_2))] = np.nan

    ema_stack = np.full(series.size, "NEUTRAL", dtype=object)
    ema_stack[valid_pair & (ema_1 > ema_2)] = "BUY"
    ema_stack[valid_pair & (ema_1 < ema_2)] = "SELL"

    ema_cross_up = np.zeros(series.size, dtype=bool)
    ema_cross_down = np.zeros(series.size, dtype=bool)
    ema_cross_up[valid_cross] = (ema_1[valid_cross] > ema_2[valid_cross]) & (prev_1[valid_cross] <= prev_2[valid_cross])
    ema_cross_down[valid_cross] = (ema_1[valid_cross] < ema_2[valid_cross]) & (prev_1[valid_cross] >= prev_2[valid_cross])

    ema_spread_signed = ema_1 - ema_2
    ema_spread_signed[~valid_pair] = np.nan
    ema_spread = np.abs(ema_spread_signed)
    ema_spread[~valid_pair] = np.nan
    ema_spread_pct = np.full(series.size, np.nan, dtype=float)
    valid_pct = valid_pair & (np.abs(ema_2) != 0.0)
    ema_spread_pct[valid_pct] = (ema_spread[valid_pct] / np.abs(ema_2[valid_pct])) * 100.0
    prev_spread = np.concatenate(([np.nan], ema_spread[:-1]))
    prev_spread_pct = np.concatenate(([np.nan], ema_spread_pct[:-1]))
    ema_spread_delta = ema_spread - prev_spread
    ema_spread_pct_delta = ema_spread_pct - prev_spread_pct
    ema_spread_delta[~(np.isfinite(ema_spread) & np.isfinite(prev_spread))] = np.nan
    ema_spread_pct_delta[~(np.isfinite(ema_spread_pct) & np.isfinite(prev_spread_pct))] = np.nan
    ema_spread_trend = _spread_trend_array(ema_spread)
    ema_stack_pressure = _stack_pressure_array(ema_stack, ema_spread_trend)

    return {
        "ema_1": ema_1,
        "ema_2": ema_2,
        "ema_stack": ema_stack,
        "ema_cross_up": ema_cross_up,
        "ema_cross_down": ema_cross_down,
        "ema_1_slope": ema_1_slope,
        "ema_2_slope": ema_2_slope,
        "ema_1_angle_state": _angle_state_array(ema_1_slope),
        "ema_2_angle_state": _angle_state_array(ema_2_slope),
        "ema_spread": ema_spread,
        "ema_spread_pct": ema_spread_pct,
        "ema_spread_delta": ema_spread_delta,
        "ema_spread_pct_delta": ema_spread_pct_delta,
        "ema_spread_trend": ema_spread_trend,
        "ema_stack_pressure": ema_stack_pressure,
    }


def build_ema_pair_payload(
    closes: Sequence[float],
    ema_1_length: int,
    ema_2_length: int,
) -> Dict[str, Any]:
    arrays = compute_ema_pair_arrays(closes, ema_1_length=ema_1_length, ema_2_length=ema_2_length)
    if not arrays:
        return dict(EMA_PAIR_DEFAULT_PAYLOAD)

    return {
        "ema_1": _finite_or_none(arrays["ema_1"][-1]),
        "ema_2": _finite_or_none(arrays["ema_2"][-1]),
        "ema_stack": str(arrays["ema_stack"][-1]) if arrays["ema_stack"].size else "NEUTRAL",
        "ema_cross_up": bool(arrays["ema_cross_up"][-1]) if arrays["ema_cross_up"].size else None,
        "ema_cross_down": bool(arrays["ema_cross_down"][-1]) if arrays["ema_cross_down"].size else None,
        "ema_1_slope": _finite_or_none(arrays["ema_1_slope"][-1]),
        "ema_2_slope": _finite_or_none(arrays["ema_2_slope"][-1]),
        "ema_1_angle_state": arrays["ema_1_angle_state"][-1] if arrays["ema_1_angle_state"].size else None,
        "ema_2_angle_state": arrays["ema_2_angle_state"][-1] if arrays["ema_2_angle_state"].size else None,
        "ema_spread": _finite_or_none(arrays["ema_spread"][-1]),
        "ema_spread_pct": _finite_or_none(arrays["ema_spread_pct"][-1]),
        "ema_spread_delta": _finite_or_none(arrays["ema_spread_delta"][-1]),
        "ema_spread_pct_delta": _finite_or_none(arrays["ema_spread_pct_delta"][-1]),
        "ema_spread_trend": arrays["ema_spread_trend"][-1] if arrays["ema_spread_trend"].size else None,
        "ema_stack_pressure": arrays["ema_stack_pressure"][-1] if arrays["ema_stack_pressure"].size else None,
    }


def build_ema_pair_frame(
    closes: Sequence[float],
    index: Any,
    ema_1_length: int,
    ema_2_length: int,
) -> Dict[str, pd.Series]:
    arrays = compute_ema_pair_arrays(closes, ema_1_length=ema_1_length, ema_2_length=ema_2_length)
    if not arrays:
        return {}
    frame_index = pd.Index(index)
    return {
        "ema_1": pd.Series(arrays["ema_1"], index=frame_index, dtype=float),
        "ema_2": pd.Series(arrays["ema_2"], index=frame_index, dtype=float),
        "ema_stack": pd.Series(arrays["ema_stack"], index=frame_index, dtype=object),
        "ema_cross_up": pd.Series(arrays["ema_cross_up"], index=frame_index, dtype=bool),
        "ema_cross_down": pd.Series(arrays["ema_cross_down"], index=frame_index, dtype=bool),
        "ema_1_slope": pd.Series(arrays["ema_1_slope"], index=frame_index, dtype=float),
        "ema_2_slope": pd.Series(arrays["ema_2_slope"], index=frame_index, dtype=float),
        "ema_1_angle_state": pd.Series(arrays["ema_1_angle_state"], index=frame_index, dtype=object),
        "ema_2_angle_state": pd.Series(arrays["ema_2_angle_state"], index=frame_index, dtype=object),
        "ema_spread": pd.Series(arrays["ema_spread"], index=frame_index, dtype=float),
        "ema_spread_pct": pd.Series(arrays["ema_spread_pct"], index=frame_index, dtype=float),
        "ema_spread_delta": pd.Series(arrays["ema_spread_delta"], index=frame_index, dtype=float),
        "ema_spread_pct_delta": pd.Series(arrays["ema_spread_pct_delta"], index=frame_index, dtype=float),
        "ema_spread_trend": pd.Series(arrays["ema_spread_trend"], index=frame_index, dtype=object),
        "ema_stack_pressure": pd.Series(arrays["ema_stack_pressure"], index=frame_index, dtype=object),
    }
