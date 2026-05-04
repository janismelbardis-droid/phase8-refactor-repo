from __future__ import annotations

from app.domain.market_state.classic_context import build_classic_market_context, map_classic_context_to_legacy
from app.indicators_streaming import compute_market_state_snapshot
from tests.common import bull_dynamic_sample, compression_basic_sample


def test_classic_context_detects_uptrend_expansion_and_legacy_bull_expansion() -> None:
    snap = {
        "open": 100.0,
        "high": 101.6,
        "low": 99.9,
        "close": 101.4,
        "price": 101.4,
        "market_structure_flag": "INTACT",
        "market_structure_context": "LONG",
        "market_structure_pivot_bias": "LONG",
        "market_structure_swing_hh": True,
        "market_structure_swing_hl": True,
        "market_structure_note": "higher highs and higher lows remain intact",
        "market_trend_regime": "UPTREND",
        "market_trend_quality": "CONFIRMED",
        "market_market_type": "TREND",
        "market_pa_vol_pass": True,
        "market_pa_volume_ratio": 1.24,
        "market_pa_followthrough_pass": True,
        "market_pa_outside_continue": True,
        "market_pa_phase2_context": "LONG_CONTINUATION",
        "market_breakout_state": "BREAKOUT_CONFIRM_LONG",
        "market_breakout_bias": "LONG",
        "market_breakout_note": "bull breakout is holding above the base",
        "vidya_state": "BUY",
        "frama_state": "BUY",
        "range_filter_state": "BUY",
        "ms": "GREEN",
        "angle": "GREEN",
        "atr_angle": "GREEN",
        "macd": 1.2,
        "signal": 0.9,
    }

    classic = build_classic_market_context(snap)
    legacy = map_classic_context_to_legacy(snap, classic)

    assert classic["market_trend_state"] == "UPTREND"
    assert classic["market_structure_state"] == "HH_HL"
    assert classic["market_phase_state"] == "EXPANSION"
    assert classic["market_pullback_state"] in {"NONE", "RESOLVED"}
    assert classic["market_breakout_signal"] == "CONFIRM_LONG"
    assert classic["market_volume_context"] == "SUPPORTIVE"
    assert legacy["market_state"] == "BULL_EXPANSION"
    assert legacy["market_bias"] == "LONG"
    assert legacy["market_phase"] == "EXPANSION"


def test_classic_context_detects_exhaustion_without_forcing_full_reversal() -> None:
    snap = {
        "open": 101.4,
        "high": 101.9,
        "low": 100.8,
        "close": 101.0,
        "price": 101.0,
        "market_structure_flag": "INTACT",
        "market_structure_context": "LONG",
        "market_structure_pivot_bias": "LONG",
        "market_structure_swing_hh": True,
        "market_structure_swing_hl": True,
        "market_structure_note": "uptrend still formally intact",
        "market_trend_regime": "UPTREND",
        "market_trend_quality": "WEAKENING",
        "market_market_type": "TREND",
        "market_pa_volume_ratio": 1.52,
        "market_pa_rejection_cluster": True,
        "market_pa_ladder_fail": True,
        "market_pa_sequence_exhaust": True,
        "market_pa_followthrough_fail": True,
        "market_pa_phase6_context": "LONG_SEQUENCE_EXHAUST",
        "market_breakout_note": "new highs are not holding cleanly",
        "vidya_state": "BUY",
        "frama_state": "BUY",
        "range_filter_state": "BUY",
        "ms": "GREEN",
        "angle": "GREEN",
        "atr_angle": "RED",
        "macd": 0.8,
        "signal": 0.7,
    }

    classic = build_classic_market_context(snap)
    legacy = map_classic_context_to_legacy(snap, classic)

    assert classic["market_trend_state"] == "UPTREND"
    assert classic["market_phase_state"] == "EXHAUSTION"
    assert classic["market_pullback_state"] in {"NONE", "END_CANDIDATE", "ACTIVE"}
    assert classic["market_volume_context"] == "CLIMACTIC"
    assert legacy["market_state"] == "BULL_EXHAUSTION"


def test_classic_context_detects_flat_balance_and_legacy_compression() -> None:
    snap = {
        "open": 100.0,
        "high": 100.3,
        "low": 99.7,
        "close": 100.0,
        "price": 100.0,
        "market_structure_flag": "MIXED",
        "market_structure_context": "NEUTRAL",
        "market_trend_regime": "FLAT_COMPRESSION",
        "market_market_type": "COMPRESSION",
        "market_pa_volume_ratio": 0.62,
        "market_breakout_state": "BREAKOUT_READY",
        "market_breakout_bias": "LONG",
        "vidya_state": "NEUTRAL",
        "frama_state": "NEUTRAL",
        "range_filter_state": "NEUTRAL",
        "ms": "NEUTRAL",
        "angle": "FLAT",
        "atr_angle": "RED",
        "macd": 0.01,
        "signal": 0.01,
    }

    classic = build_classic_market_context(snap)
    legacy = map_classic_context_to_legacy(snap, classic)

    assert classic["market_trend_state"] == "FLAT"
    assert classic["market_phase_state"] == "BALANCE"
    assert classic["market_pullback_state"] == "NONE"
    assert classic["market_breakout_signal"] == "WATCH_LONG"
    assert classic["market_volume_context"] == "DRY"
    assert legacy["market_state"] == "COMPRESSION"
    assert legacy["market_phase"] == "COMPRESSION"


def test_classic_context_range_balance_overrides_local_hh_hl_trend() -> None:
    snap = {
        "open": 100.1,
        "high": 100.5,
        "low": 99.9,
        "close": 100.2,
        "price": 100.2,
        "market_structure_flag": "INTACT",
        "market_structure_context": "LONG",
        "market_structure_pivot_bias": "LONG",
        "market_structure_swing_hh": True,
        "market_structure_swing_hl": True,
        "market_structure_note": "local higher highs exist inside the active base",
        "market_structure_last_high": 100.5,
        "market_structure_last_low": 99.9,
        "market_trend_regime": "UPTREND",
        "market_market_type": "RANGE_BALANCE",
        "market_breakout_state": "NONE",
        "market_pa_volume_ratio": 0.96,
        "vidya_state": "BUY",
        "frama_state": "BUY",
        "range_filter_state": "NEUTRAL",
        "ms": "NEUTRAL",
        "angle": "FLAT",
        "atr_angle": "RED",
        "macd": 0.05,
        "signal": 0.04,
    }

    classic = build_classic_market_context(snap)
    legacy = map_classic_context_to_legacy(snap, classic)

    assert classic["market_structure_state"] == "HH_HL"
    assert classic["market_trend_state"] == "FLAT"
    assert classic["market_phase_state"] == "BALANCE"
    assert classic["market_pullback_state"] == "NONE"
    assert classic["market_breakout_signal"] == "WATCH_LONG"
    assert legacy["market_state"] == "COMPRESSION"
    assert classic["market_support_zone_low"] < classic["market_support_zone_high"]
    assert classic["market_resistance_zone_low"] < classic["market_resistance_zone_high"]


def test_classic_context_detects_pullback_resolution_inside_uptrend() -> None:
    snap = {
        "open": 100.8,
        "high": 101.9,
        "low": 100.4,
        "close": 101.7,
        "price": 101.7,
        "atr": 1.1,
        "market_structure_flag": "INTACT",
        "market_structure_context": "LONG",
        "market_structure_pivot_bias": "LONG",
        "market_structure_swing_hh": True,
        "market_structure_swing_hl": True,
        "market_structure_note": "uptrend structure remains intact at the defended higher low",
        "market_market_type": "TREND",
        "market_pa_retest_hold": True,
        "market_pa_followthrough_pass": True,
        "market_pa_acceptance_drive": True,
        "market_breakout_state": "BREAKOUT_CONFIRM_LONG",
        "market_breakout_bias": "LONG",
        "market_pa_volume_ratio": 1.12,
        "market_structure_last_high": 101.9,
        "market_structure_last_low": 100.3,
    }

    classic = build_classic_market_context(snap)

    assert classic["market_trend_state"] == "UPTREND"
    assert classic["market_pullback_state"] == "RESOLVED"
    assert classic["market_phase_state"] == "EXPANSION"


def test_compute_market_state_snapshot_exposes_classic_fields_and_keeps_legacy_contract() -> None:
    bull_state = compute_market_state_snapshot(bull_dynamic_sample())
    compression_state = compute_market_state_snapshot(compression_basic_sample(), bull_state)

    assert bull_state["market_trend_state"] in {"UPTREND", "FLAT"}
    assert bull_state["market_phase_state"] in {"EXPANSION", "PULLBACK", "EXHAUSTION", "BALANCE"}
    assert bull_state["market_state"] in {"BULL_EXPANSION", "BULL_PULLBACK", "BULL_EXHAUSTION", "TRANSITION", "COMPRESSION"}
    assert "market_context_reasons" in bull_state

    assert compression_state["market_trend_age"] >= 1
    assert compression_state["market_phase_age"] >= 1
    assert compression_state["market_breakout_signal_age"] >= 1
    assert compression_state["market_state"] in {"COMPRESSION", "TRANSITION", "BULL_PULLBACK", "BEAR_PULLBACK"}
