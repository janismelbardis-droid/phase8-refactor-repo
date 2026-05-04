from __future__ import annotations

import py_compile
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

TARGETS = [
    ROOT / "app" / "runtime_safety.py",
    ROOT / "app" / "domain" / "market_state" / "schema.py",
    ROOT / "app" / "domain" / "market_state" / "pipeline.py",
    ROOT / "app" / "services" / "live" / "health_service.py",
    ROOT / "app" / "indicators_streaming.py",
]

for path in TARGETS:
    py_compile.compile(str(path), doraise=True)
    print(f"OK  {path.relative_to(ROOT)}")

from app.domain.market_state.pipeline import run_basic_market_state_pipeline, run_dynamic_market_state_pipeline
from app.domain.market_state.thresholds import DEFAULT_MARKET_STATE_THRESHOLDS
from app.services.live.health_service import build_orderflow_quality_overlay

sample = {
    "open": 100.0,
    "high": 101.0,
    "low": 99.0,
    "close": 100.5,
    "price": 100.5,
    "volume": 20.0,
    "vidya_state": "BUY",
    "frama_state": "BUY",
    "range_filter_state": "BUY",
    "range_filter_phase": "EXPANSION",
    "ms": "GREEN",
    "angle": "GREEN",
    "atr_angle": "GREEN",
    "macd": 1.1,
    "signal": 0.8,
    "atr": 0.7,
    "vidya_delta_pct": 12.0,
    "vidya_angle_deg": 8.0,
    "market_compression_score": 21.0,
    "market_transition_score": 18.0,
    "market_trend_score": 59.0,
    "market_pullback_score": 16.0,
    "market_exhaustion_score": 12.0,
    "market_breakout_quality": 0.66,
    "market_breakout_readiness": 0.61,
    "market_breakout_confirmation": 0.63,
    "market_direction_evidence": 3.4,
    "market_alignment_fraction": 0.83,
    "market_pressure_bias": "LONG",
}

basic = run_basic_market_state_pipeline(
    sample,
    None,
    DEFAULT_MARKET_STATE_THRESHOLDS,
    price_action_overlay_builder=lambda *_args, **_kwargs: (_ for _ in ()).throw(RuntimeError("pa overlay fail")),
    pretty_label_builder=lambda label: label.title(),
)
assert basic["market_state_status"] in {"partial", "failed"}
assert "price_action_overlay" in basic.get("market_state_status_reason", "")


dynamic = run_dynamic_market_state_pipeline(
    sample,
    None,
    DEFAULT_MARKET_STATE_THRESHOLDS,
    price_action_overlay_builder=lambda snap, prev, bias: {},
    hierarchy_overlay_builder=lambda *_args, **_kwargs: (_ for _ in ()).throw(ValueError("hierarchy fail")),
    value_overlay_builder=lambda *_args, **_kwargs: {},
    breakout_context_builder=lambda *_args, **_kwargs: {},
    base_summary_builder=lambda *_args, **_kwargs: ("TEXTURE", "short", "long"),
    pretty_label_builder=lambda label: (_ for _ in ()).throw(RuntimeError("pretty fail")),
)
assert dynamic["market_state_status"] in {"partial", "failed"}
assert dynamic["market_state_degraded_features"]

orderflow = build_orderflow_quality_overlay(
    {"of_dom_source": "LOCAL_L2", "of_last_trade_ts_ms": "bad-ts"},
    now_ms=1000,
    mode="AUTO",
    dom_good_ms=100,
    dom_degraded_ms=500,
    tape_good_ms=100,
    tape_degraded_ms=500,
    snapshot_stale_ms=500,
    tape_enabled=True,
)
assert "orderflow_status" in orderflow
print("Phase 4 runtime checks passed")
