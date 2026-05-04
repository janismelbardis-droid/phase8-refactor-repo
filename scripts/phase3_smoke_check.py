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
    ROOT / "app" / "domain" / "market_state" / "__init__.py",
    ROOT / "app" / "domain" / "market_state" / "scoring.py",
    ROOT / "app" / "domain" / "market_state" / "classification.py",
    ROOT / "app" / "domain" / "market_state" / "summaries.py",
    ROOT / "app" / "domain" / "market_state" / "breakout.py",
    ROOT / "app" / "domain" / "market_state" / "pipeline.py",
]

for path in TARGETS:
    py_compile.compile(str(path), doraise=True)
    print(f"OK  {path.relative_to(ROOT)}")

from app.domain.market_state import ensure_market_state_inputs
from app.domain.market_state.pipeline import run_basic_market_state_pipeline, run_dynamic_market_state_pipeline
from app.indicators_streaming import (
    _market_breakout_context,
    _market_hierarchy_overlay,
    _market_price_action_overlay,
    _market_textures_and_summaries,
    _market_value_volatility_overlay,
    compute_market_state_snapshot,
    pretty_market_state_label,
)

basic_sample = {
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
}

basic_state = run_basic_market_state_pipeline(
    basic_sample,
    None,
    ensure_market_state_inputs(basic_sample).raw.get("thresholds") or __import__("app.domain.market_state.thresholds", fromlist=["DEFAULT_MARKET_STATE_THRESHOLDS"]).DEFAULT_MARKET_STATE_THRESHOLDS,
    price_action_overlay_builder=lambda snap, prev, bias: _market_price_action_overlay(snap, prev_state=prev, bias_hint=bias),
    pretty_label_builder=pretty_market_state_label,
)
assert isinstance(basic_state, dict)
assert basic_state["market_state"] in {"BULL_EXPANSION", "BULL_PULLBACK", "BULL_EXHAUSTION", "BEAR_EXPANSION", "BEAR_PULLBACK", "BEAR_EXHAUSTION", "TRANSITION", "COMPRESSION"}


dynamic_sample = {
    **basic_sample,
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

dynamic_state = run_dynamic_market_state_pipeline(
    dynamic_sample,
    None,
    __import__("app.domain.market_state.thresholds", fromlist=["DEFAULT_MARKET_STATE_THRESHOLDS"]).DEFAULT_MARKET_STATE_THRESHOLDS,
    price_action_overlay_builder=lambda snap, prev, bias: _market_price_action_overlay(snap, prev_state=prev, bias_hint=bias),
    hierarchy_overlay_builder=lambda snap, structure_flag, bias: _market_hierarchy_overlay(snap, structure_flag=structure_flag, bias_hint=bias),
    value_overlay_builder=lambda snap, bias: _market_value_volatility_overlay(snap, bias_hint=bias),
    breakout_context_builder=lambda snap, label, bias, phase, prev: _market_breakout_context(snap, label, bias, phase, prev_state=prev),
    base_summary_builder=_market_textures_and_summaries,
    pretty_label_builder=pretty_market_state_label,
)
assert isinstance(dynamic_state, dict)
assert dynamic_state["market_texture"]
assert "market_summary_short" in dynamic_state

public_state = compute_market_state_snapshot(dynamic_sample)
assert public_state["market_state"] == dynamic_state["market_state"]
assert public_state["market_texture"] == dynamic_state["market_texture"]
print("Phase 3 runtime checks passed")
