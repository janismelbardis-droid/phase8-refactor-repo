from __future__ import annotations

import json
import sys
from pathlib import Path
from typing import Any, Dict

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


def load_json(name: str) -> Dict[str, Any]:
    return json.loads((ROOT / "tests" / "golden_samples" / name).read_text(encoding="utf-8"))


def bull_dynamic_sample() -> Dict[str, Any]:
    return {
        "open": 100.0,
        "high": 101.0,
        "low": 99.5,
        "close": 100.8,
        "price": 100.8,
        "volume": 12.0,
        "vidya_state": "BUY",
        "frama_state": "BUY",
        "range_filter_state": "BUY",
        "range_filter_phase": "EXPANSION",
        "ms": "GREEN",
        "angle": "GREEN",
        "atr_angle": "GREEN",
        "macd": 1.2,
        "signal": 0.9,
        "atr": 0.8,
        "vidya_delta_pct": 16.0,
        "vidya_angle_deg": 10.0,
        "market_compression_score": 20.0,
        "market_transition_score": 18.0,
        "market_trend_score": 62.0,
        "market_pullback_score": 21.0,
        "market_exhaustion_score": 14.0,
        "market_breakout_quality": 0.71,
        "market_breakout_readiness": 0.64,
        "market_breakout_confirmation": 0.68,
        "market_compression_maturity": 25.0,
        "market_containment_rate": 0.22,
        "market_slope_flatness": 0.11,
        "market_range_contraction": 0.15,
        "market_swing_squeeze": 0.14,
        "market_delta_strength": 0.82,
        "market_direction_evidence": 3.8,
        "market_conflict_score": 0.12,
        "market_alignment_fraction": 0.88,
        "market_pressure_bias": "LONG",
    }


def compression_basic_sample() -> Dict[str, Any]:
    return {
        "open": 100.0,
        "high": 100.4,
        "low": 99.6,
        "close": 100.0,
        "price": 100.0,
        "volume": 10.0,
        "vidya_state": "NEUTRAL",
        "frama_state": "NEUTRAL",
        "range_filter_state": "NEUTRAL",
        "range_filter_phase": "NEUTRAL",
        "ms": "NEUTRAL",
        "angle": "FLAT",
        "atr_angle": "RED",
        "macd": 0.02,
        "signal": 0.01,
        "atr": 0.8,
        "vidya_delta_pct": 2.0,
        "vidya_angle_deg": 0.5,
    }


def bear_dynamic_sample() -> Dict[str, Any]:
    return {
        "open": 100.0,
        "high": 100.2,
        "low": 98.8,
        "close": 99.0,
        "price": 99.0,
        "volume": 14.0,
        "vidya_state": "SELL",
        "frama_state": "SELL",
        "range_filter_state": "SELL",
        "range_filter_phase": "EXPANSION",
        "ms": "RED",
        "angle": "RED",
        "atr_angle": "RED",
        "macd": -1.0,
        "signal": -0.7,
        "atr": 0.9,
        "vidya_delta_pct": -18.0,
        "vidya_angle_deg": -9.0,
        "market_compression_score": 18.0,
        "market_transition_score": 22.0,
        "market_trend_score": 61.0,
        "market_pullback_score": 19.0,
        "market_exhaustion_score": 16.0,
        "market_breakout_quality": 0.69,
        "market_breakout_readiness": 0.66,
        "market_breakout_confirmation": 0.67,
        "market_compression_maturity": 20.0,
        "market_containment_rate": 0.20,
        "market_slope_flatness": 0.14,
        "market_range_contraction": 0.19,
        "market_swing_squeeze": 0.13,
        "market_delta_strength": 0.80,
        "market_direction_evidence": -3.9,
        "market_conflict_score": 0.11,
        "market_alignment_fraction": 0.86,
        "market_pressure_bias": "SHORT",
    }


def orderflow_sample() -> Dict[str, Any]:
    return {
        "of_dom_source": "LOCAL_L2",
        "of_last_trade_ts_ms": 1000,
        "of_dom_depth_age_ms": 120,
        "of_dom_snapshot_age_ms": 180,
        "of_dom_resync_required": False,
        "of_dom_local_book_synced": True,
        "of_dom_partial_connected": True,
        "of_dom_sync_state": "SYNCED",
    }
