from __future__ import annotations

import py_compile
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
TARGETS = [
    ROOT / "run_app.py",
    ROOT / "app" / "indicators_streaming.py",
    ROOT / "app" / "live_engine.py",
    ROOT / "app" / "services" / "live" / "health_service.py",
    ROOT / "app" / "domain" / "market_state" / "__init__.py",
    ROOT / "app" / "domain" / "market_state" / "schema.py",
    ROOT / "app" / "domain" / "market_state" / "features.py",
    ROOT / "app" / "domain" / "market_state" / "classification.py",
    ROOT / "app" / "domain" / "market_state" / "pipeline.py",
]

for path in TARGETS:
    py_compile.compile(str(path), doraise=True)
    print(f"OK  {path.relative_to(ROOT)}")

from app.domain.market_state import build_market_state_inputs, decision_from_state_payload
from app.indicators_streaming import compute_market_state_snapshot
from app.services.live.health_service import build_orderflow_quality_overlay

sample = {
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
    "ms": "BULLISH",
    "angle": "UP",
    "atr_angle": "UP",
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

inputs = build_market_state_inputs(sample)
assert inputs.indicators.vidya_state == "BUY"
assert inputs.features.trend_score == 62.0

state = compute_market_state_snapshot(sample)
decision = decision_from_state_payload(state)
assert isinstance(state, dict)
assert decision.label in {"BULL_EXPANSION", "BULL_PULLBACK", "BULL_EXHAUSTION", "TRANSITION", "COMPRESSION", "BEAR_EXPANSION", "BEAR_PULLBACK", "BEAR_EXHAUSTION"}
assert "market_summary_short" in state

of = build_orderflow_quality_overlay(
    {
        "of_dom_source": "LOCAL_L2",
        "of_last_trade_ts_ms": 1000,
        "of_dom_depth_age_ms": 120,
        "of_dom_snapshot_age_ms": 200,
        "of_dom_resync_required": False,
        "of_dom_local_book_synced": True,
        "of_dom_partial_connected": True,
        "of_dom_sync_state": "SYNCED",
    },
    now_ms=1300,
    mode="AUTO",
    dom_good_ms=250,
    dom_degraded_ms=1000,
    tape_good_ms=250,
    tape_degraded_ms=1000,
    snapshot_stale_ms=2000,
    tape_enabled=True,
)
assert of["of_data_quality"] in {"GOOD", "DEGRADED", "STALE", "NO_DATA", "INVALID", "DISABLED"}
print("Phase 2 runtime checks passed")
