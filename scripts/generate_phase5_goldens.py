from __future__ import annotations

import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from app.indicators_streaming import compute_market_state_snapshot
from app.research.replay import extract_state_signature, replay_market_state_sequence
from app.services.live.health_service import build_orderflow_quality_overlay
from tests.common import bear_dynamic_sample, bull_dynamic_sample, orderflow_sample

rows = [
    bull_dynamic_sample(),
    {
        **bull_dynamic_sample(),
        "open": 100.8,
        "high": 101.4,
        "low": 100.4,
        "close": 101.2,
        "price": 101.2,
        "volume": 13.0,
        "macd": 1.25,
        "signal": 0.95,
        "atr": 0.82,
        "vidya_delta_pct": 14.0,
        "vidya_angle_deg": 9.0,
        "market_compression_score": 24.0,
        "market_transition_score": 26.0,
        "market_trend_score": 57.0,
        "market_pullback_score": 29.0,
        "market_exhaustion_score": 16.0,
        "market_breakout_quality": 0.62,
        "market_breakout_readiness": 0.58,
        "market_breakout_confirmation": 0.59,
        "market_compression_maturity": 32.0,
        "market_containment_rate": 0.28,
        "market_slope_flatness": 0.18,
        "market_range_contraction": 0.21,
        "market_swing_squeeze": 0.20,
        "market_delta_strength": 0.74,
        "market_direction_evidence": 3.1,
        "market_conflict_score": 0.22,
        "market_alignment_fraction": 0.79,
        "market_pressure_bias": "LONG",
    },
    {
        **bear_dynamic_sample(),
        "open": 101.2,
        "high": 101.3,
        "low": 99.4,
        "close": 99.8,
        "price": 99.8,
        "volume": 16.0,
        "market_trend_score": 63.0,
        "market_breakout_quality": 0.70,
        "market_breakout_readiness": 0.65,
        "market_breakout_confirmation": 0.68,
        "market_alignment_fraction": 0.87,
    },
]
replayed = replay_market_state_sequence(rows, compute_fn=compute_market_state_snapshot)
market_payload = {
    "rows": rows,
    "expected": [
        {"name": name, "signature": extract_state_signature(state)}
        for name, state in zip(["bull_expansion", "bull_followup", "bear_expansion"], replayed)
    ],
}
(ROOT / "tests" / "golden_samples" / "market_state_replay.json").write_text(json.dumps(market_payload, indent=2), encoding="utf-8")

params = {
    "now_ms": 1300,
    "mode": "AUTO",
    "dom_good_ms": 250,
    "dom_degraded_ms": 1000,
    "tape_good_ms": 250,
    "tape_degraded_ms": 1000,
    "snapshot_stale_ms": 2000,
    "tape_enabled": True,
}
orderflow = build_orderflow_quality_overlay(orderflow_sample(), **params)
orderflow_payload = {
    "params": params,
    "signature": {
        key: orderflow.get(key)
        for key in [
            "of_data_quality",
            "of_data_confidence",
            "of_watch_allowed",
            "of_confirmation_allowed",
            "of_quality_note",
            "orderflow_status",
        ]
    },
}
(ROOT / "tests" / "golden_samples" / "orderflow_overlay.json").write_text(json.dumps(orderflow_payload, indent=2), encoding="utf-8")
print("Phase 5 goldens generated")
