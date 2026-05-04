from __future__ import annotations

"""Market-state thresholds and label constants extracted from the indicator monolith."""

from dataclasses import dataclass
from typing import Any, Dict, Optional

import numpy as np

MARKET_STATE_LABELS = (
    "BULL_EXPANSION",
    "BULL_PULLBACK",
    "BULL_EXHAUSTION",
    "BEAR_EXPANSION",
    "BEAR_PULLBACK",
    "BEAR_EXHAUSTION",
    "TRANSITION",
    "COMPRESSION",
)

MARKET_STATE_BIASES = ("LONG", "SHORT", "NEUTRAL")
MARKET_STATE_PHASES = ("EXPANSION", "PULLBACK", "EXHAUSTION", "TRANSITION", "COMPRESSION")
VIDYA_ANGLE_STATES = ("UP", "DOWN", "FLAT")
VIDYA_ANGLE_ACCEL_STATES = ("ACCEL", "DECEL", "STABLE")


@dataclass(frozen=True)
class MarketStateThresholds:
    vidya_angle_up_deg: float = 2.0
    vidya_angle_down_deg: float = -2.0
    vidya_angle_strong_deg: float = 8.0
    vidya_delta_support_pct: float = 15.0
    vidya_delta_weak_pct: float = 5.0
    direction_long_min: int = 4
    direction_short_max: int = -4
    compression_direction_abs_max: int = 2
    compression_macd_gap_norm_max: float = 0.35
    confidence_base: int = 18
    expansion_confirmation_min: float = 0.56
    expansion_pullback_floor: float = 40.0
    expansion_exhaustion_cap: float = 48.0
    expansion_max_weakness_score: float = 1.25


DEFAULT_MARKET_STATE_THRESHOLDS = MarketStateThresholds()


def market_state_thresholds_dict(thresholds: Optional[MarketStateThresholds] = None) -> Dict[str, float]:
    th = thresholds or DEFAULT_MARKET_STATE_THRESHOLDS
    return {
        "vidya_angle_up_deg": float(th.vidya_angle_up_deg),
        "vidya_angle_down_deg": float(th.vidya_angle_down_deg),
        "vidya_angle_strong_deg": float(th.vidya_angle_strong_deg),
        "vidya_delta_support_pct": float(th.vidya_delta_support_pct),
        "vidya_delta_weak_pct": float(th.vidya_delta_weak_pct),
        "direction_long_min": int(th.direction_long_min),
        "direction_short_max": int(th.direction_short_max),
        "compression_direction_abs_max": int(th.compression_direction_abs_max),
        "compression_macd_gap_norm_max": float(th.compression_macd_gap_norm_max),
        "confidence_base": int(th.confidence_base),
        "expansion_confirmation_min": float(th.expansion_confirmation_min),
        "expansion_pullback_floor": float(th.expansion_pullback_floor),
        "expansion_exhaustion_cap": float(th.expansion_exhaustion_cap),
        "expansion_max_weakness_score": float(th.expansion_max_weakness_score),
    }


def coerce_market_state_thresholds(values: Optional[Dict[str, Any]] = None) -> MarketStateThresholds:
    base = market_state_thresholds_dict()
    src = dict(values or {})

    def _get_num(key: str, cast=float):
        raw = src.get(key, base[key])
        try:
            val = cast(raw)
            if np.isfinite(float(val)):
                return val
        except Exception:
            pass
        return cast(base[key])

    out = {
        "vidya_angle_up_deg": _get_num("vidya_angle_up_deg", float),
        "vidya_angle_down_deg": _get_num("vidya_angle_down_deg", float),
        "vidya_angle_strong_deg": _get_num("vidya_angle_strong_deg", float),
        "vidya_delta_support_pct": _get_num("vidya_delta_support_pct", float),
        "vidya_delta_weak_pct": _get_num("vidya_delta_weak_pct", float),
        "direction_long_min": _get_num("direction_long_min", int),
        "direction_short_max": _get_num("direction_short_max", int),
        "compression_direction_abs_max": _get_num("compression_direction_abs_max", int),
        "compression_macd_gap_norm_max": _get_num("compression_macd_gap_norm_max", float),
        "confidence_base": _get_num("confidence_base", int),
        "expansion_confirmation_min": _get_num("expansion_confirmation_min", float),
        "expansion_pullback_floor": _get_num("expansion_pullback_floor", float),
        "expansion_exhaustion_cap": _get_num("expansion_exhaustion_cap", float),
        "expansion_max_weakness_score": _get_num("expansion_max_weakness_score", float),
    }

    out["vidya_angle_strong_deg"] = max(abs(float(out["vidya_angle_up_deg"])), abs(float(out["vidya_angle_down_deg"])), float(out["vidya_angle_strong_deg"]))
    out["vidya_delta_support_pct"] = max(0.0, float(out["vidya_delta_support_pct"]))
    out["vidya_delta_weak_pct"] = max(0.0, min(float(out["vidya_delta_support_pct"]), float(out["vidya_delta_weak_pct"])))
    out["compression_direction_abs_max"] = max(0, int(out["compression_direction_abs_max"]))
    out["confidence_base"] = max(0, int(out["confidence_base"]))
    out["expansion_confirmation_min"] = float(np.clip(float(out["expansion_confirmation_min"]), 0.40, 0.90))
    out["expansion_pullback_floor"] = float(np.clip(float(out["expansion_pullback_floor"]), 20.0, 80.0))
    out["expansion_exhaustion_cap"] = float(np.clip(float(out["expansion_exhaustion_cap"]), 20.0, 80.0))
    out["expansion_max_weakness_score"] = float(np.clip(float(out["expansion_max_weakness_score"]), 0.0, 4.0))
    return MarketStateThresholds(**out)
