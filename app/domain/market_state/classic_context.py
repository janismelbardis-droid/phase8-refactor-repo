from __future__ import annotations

"""Classic TA oriented market-context classification.

This module intentionally keeps the user-facing regime logic grounded in
auditable concepts such as structure, phase, breakout behavior, and volume.
Legacy market-state labels are derived from this classic context elsewhere.
"""

from typing import Any, Dict, List, Mapping, Optional, Sequence

import numpy as np

from .classification import compute_state_age


def _text(value: Any, default: str = "") -> str:
    try:
        out = str(value if value is not None else default)
    except Exception:
        return default
    return out.strip()


def _upper(value: Any, default: str = "") -> str:
    out = _text(value, default).upper().strip()
    return out or default


def _float(value: Any, default: Optional[float] = None) -> Optional[float]:
    try:
        if value is None:
            return default
        out = float(value)
        if np.isfinite(out):
            return float(out)
    except Exception:
        pass
    return default


def _bool(value: Any) -> bool:
    if isinstance(value, (bool, np.bool_)):
        return bool(value)
    text = _upper(value, "")
    if text in {"1", "TRUE", "YES", "Y", "ON"}:
        return True
    if text in {"0", "FALSE", "NO", "N", "OFF", ""}:
        return False
    try:
        return bool(value)
    except Exception:
        return False


def _clamp_int(value: float, lo: int = 0, hi: int = 100) -> int:
    return int(max(lo, min(hi, round(float(value)))))


def _join_reasons(parts: Sequence[str]) -> str:
    clean = [str(part or "").strip() for part in parts if str(part or "").strip()]
    return " | ".join(clean[:4])


def _range_like_context(snapshot: Mapping[str, Any]) -> tuple[bool, str]:
    market_type = _upper(snapshot.get("market_market_type"), "")
    trend_regime = _upper(snapshot.get("market_trend_regime"), "")
    breakout_state = _upper(snapshot.get("market_breakout_state"), "NONE")
    phase4 = _upper(snapshot.get("market_pa_phase4_context"), "NEUTRAL")

    breakout_confirmed = breakout_state in {"BREAKOUT_CONFIRM_LONG", "BREAKOUT_CONFIRM_SHORT"} or phase4 in {
        "LONG_ACCEPTANCE",
        "SHORT_ACCEPTANCE",
    }
    if breakout_confirmed:
        return (False, "")

    if market_type in {"COMPRESSION", "RANGE_BALANCE", "ROTATION"}:
        return (True, f"{market_type.lower().replace('_', ' ')} context overrides local directional swings")
    if trend_regime in {"FLAT_COMPRESSION", "VOLATILE_BALANCE", "RANGE_BALANCE"}:
        return (True, f"{trend_regime.lower().replace('_', ' ')} regime keeps the market flat")
    return (False, "")


def _price_action_side(snapshot: Mapping[str, Any], structure_state: str, trend_state: str = "FLAT") -> str:
    if trend_state == "UPTREND" or structure_state == "HH_HL":
        return "LONG"
    if trend_state == "DOWNTREND" or structure_state == "LH_LL":
        return "SHORT"

    structure_context = _upper(snapshot.get("market_structure_context"), "NEUTRAL")
    pivot_bias = _upper(snapshot.get("market_structure_pivot_bias"), "NEUTRAL")
    market_type = _upper(snapshot.get("market_market_type"), "")
    phase2 = _upper(snapshot.get("market_pa_phase2_context"), "NEUTRAL")

    if market_type == "TREND":
        if structure_context == "LONG" and pivot_bias != "SHORT":
            return "LONG"
        if structure_context == "SHORT" and pivot_bias != "LONG":
            return "SHORT"
    if phase2 in {"LONG_CONTINUATION", "LONG_RETEST_HOLD"} and pivot_bias != "SHORT":
        return "LONG"
    if phase2 in {"SHORT_CONTINUATION", "SHORT_RETEST_HOLD"} and pivot_bias != "LONG":
        return "SHORT"
    if structure_context == pivot_bias and structure_context in {"LONG", "SHORT"}:
        return structure_context
    return "NEUTRAL"


def _counter_candle(snapshot: Mapping[str, Any], trend_state: str) -> bool:
    close_val = _float(snapshot.get("close"), _float(snapshot.get("price"), 0.0)) or 0.0
    open_val = _float(snapshot.get("open"), close_val) or close_val
    if trend_state == "UPTREND":
        return close_val < open_val
    if trend_state == "DOWNTREND":
        return close_val > open_val
    return False


def _zone_padding(snapshot: Mapping[str, Any]) -> float:
    atr = abs(_float(snapshot.get("atr"), 0.0) or 0.0)
    high_val = _float(snapshot.get("high"))
    low_val = _float(snapshot.get("low"))
    boundary_high = _float(snapshot.get("market_boundary_high"))
    boundary_low = _float(snapshot.get("market_boundary_low"))

    bar_range = abs((high_val or 0.0) - (low_val or 0.0)) if high_val is not None and low_val is not None else 0.0
    boundary_span = abs(boundary_high - boundary_low) if boundary_high is not None and boundary_low is not None else 0.0
    pad = max(atr * 0.22, bar_range * 0.35, boundary_span * 0.12, 1e-6)
    cap = max(atr * 0.75, bar_range * 0.9, boundary_span * 0.35, pad)
    return float(min(pad, cap))


def _zone_bounds(center: Optional[float], snapshot: Mapping[str, Any]) -> tuple[float, float]:
    if center is None or not np.isfinite(center):
        return (np.nan, np.nan)
    pad = _zone_padding(snapshot)
    return (float(center - pad), float(center + pad))


def _has_meaningful_structure_metadata(snapshot: Mapping[str, Any]) -> bool:
    structure_flag = _upper(snapshot.get("market_structure_flag"), "MIXED")
    structure_context = _upper(snapshot.get("market_structure_context"), "NEUTRAL")
    pivot_bias = _upper(snapshot.get("market_structure_pivot_bias"), "NEUTRAL")
    market_type = _upper(snapshot.get("market_market_type"), "")
    trend_regime = _upper(snapshot.get("market_trend_regime"), "")
    last_high = _float(snapshot.get("market_structure_last_high"))
    last_low = _float(snapshot.get("market_structure_last_low"))
    pivot_count = int(_float(snapshot.get("market_structure_pivot_count"), 0.0) or 0.0)
    has_swings = any(
        _bool(snapshot.get(key))
        for key in (
            "market_structure_swing_hh",
            "market_structure_swing_hl",
            "market_structure_swing_lh",
            "market_structure_swing_ll",
        )
    )

    if has_swings or pivot_count > 0:
        return True
    if last_high is not None or last_low is not None:
        return True
    if structure_flag not in {"", "MIXED"}:
        return True
    if structure_context in {"LONG", "SHORT"} or pivot_bias in {"LONG", "SHORT"}:
        return True
    if market_type in {"TREND", "COMPRESSION", "RANGE_BALANCE", "ROTATION"}:
        return True
    if trend_regime in {"UPTREND", "DOWNTREND", "FLAT_COMPRESSION", "VOLATILE_BALANCE", "RANGE_BALANCE"}:
        return True
    return False


def infer_bias_hint(snapshot: Mapping[str, Any]) -> str:
    score = 0
    for key, pos, neg in (
        ("vidya_state", "BUY", "SELL"),
        ("frama_state", "BUY", "SELL"),
        ("range_filter_state", "BUY", "SELL"),
        ("ms", "GREEN", "RED"),
        ("angle", "GREEN", "RED"),
        ("atr_angle", "GREEN", "RED"),
    ):
        value = _upper(snapshot.get(key), "")
        if value == pos:
            score += 1
        elif value == neg:
            score -= 1
    if score >= 2:
        return "LONG"
    if score <= -2:
        return "SHORT"
    return "NEUTRAL"


def _prev_named_age(prev_state: Optional[Mapping[str, Any]], field: str, value: str) -> int:
    prev_payload = dict(prev_state or {})
    prev_value = _text(prev_payload.get(field), "")
    prev_age = 0
    try:
        prev_age = int(prev_payload.get(f"{field}_age") or 0)
    except Exception:
        prev_age = 0
    return (prev_age + 1) if prev_value == value else 1


def derive_structure_state(snapshot: Mapping[str, Any], prev_state: Optional[Mapping[str, Any]] = None) -> tuple[str, str]:
    structure_flag = _upper(snapshot.get("market_structure_flag"), "MIXED")
    structure_context = _upper(snapshot.get("market_structure_context"), "NEUTRAL")
    pivot_bias = _upper(snapshot.get("market_structure_pivot_bias"), "NEUTRAL")
    hh = _bool(snapshot.get("market_structure_swing_hh"))
    hl = _bool(snapshot.get("market_structure_swing_hl"))
    lh = _bool(snapshot.get("market_structure_swing_lh"))
    ll = _bool(snapshot.get("market_structure_swing_ll"))
    note = _text(snapshot.get("market_structure_note"), "structure unresolved")

    prev_trend = _upper((prev_state or {}).get("market_trend_state"), "")
    prev_bias = _upper((prev_state or {}).get("market_bias"), "NEUTRAL")
    if structure_flag == "BROKEN":
        if prev_trend == "UPTREND" or prev_bias == "LONG" or structure_context == "LONG":
            return ("BROKEN_UPTREND", note or "prior uptrend structure broke")
        if prev_trend == "DOWNTREND" or prev_bias == "SHORT" or structure_context == "SHORT":
            return ("BROKEN_DOWNTREND", note or "prior downtrend structure broke")
        return ("MIXED", note or "structure broke without a confirmed prior trend")

    if hh and hl:
        return ("HH_HL", note or "higher highs and higher lows are intact")
    if lh and ll:
        return ("LH_LL", note or "lower highs and lower lows are intact")
    if structure_flag == "INTACT":
        if pivot_bias == "LONG" or structure_context == "LONG":
            return ("HH_HL", note or "long structure is intact")
        if pivot_bias == "SHORT" or structure_context == "SHORT":
            return ("LH_LL", note or "short structure is intact")
    return ("MIXED", note or "structure is mixed")


def derive_trend_state(snapshot: Mapping[str, Any], structure_state: str) -> tuple[str, str]:
    range_like, range_reason = _range_like_context(snapshot)
    if range_like:
        return ("FLAT", range_reason)
    if structure_state == "HH_HL":
        return ("UPTREND", "confirmed HH/HL structure")
    if structure_state == "LH_LL":
        return ("DOWNTREND", "confirmed LH/LL structure")
    if structure_state in {"BROKEN_UPTREND", "BROKEN_DOWNTREND"}:
        return ("FLAT", "previous directional structure broke")

    structure_context = _upper(snapshot.get("market_structure_context"), "NEUTRAL")
    pivot_bias = _upper(snapshot.get("market_structure_pivot_bias"), "NEUTRAL")
    market_type = _upper(snapshot.get("market_market_type"), "")
    if market_type == "TREND":
        if structure_context == "LONG" and pivot_bias != "SHORT":
            return ("UPTREND", "price-action structure is still leaning long")
        if structure_context == "SHORT" and pivot_bias != "LONG":
            return ("DOWNTREND", "price-action structure is still leaning short")

    has_structure_metadata = _has_meaningful_structure_metadata(snapshot)
    if not has_structure_metadata:
        direction_evidence = _float(snapshot.get("market_direction_evidence"), 0.0) or 0.0
        trend_score = _float(snapshot.get("market_trend_score"), 0.0) or 0.0
        compression_score = _float(snapshot.get("market_compression_score"), 0.0) or 0.0
        transition_score = _float(snapshot.get("market_transition_score"), 0.0) or 0.0
        pressure_bias = _upper(snapshot.get("market_pressure_bias"), "NEUTRAL")
        phase2 = _upper(snapshot.get("market_pa_phase2_context"), "NEUTRAL")
        phase4 = _upper(snapshot.get("market_pa_phase4_context"), "NEUTRAL")
        if (
            phase4 == "LONG_ACCEPTANCE"
            or (phase2 in {"LONG_CONTINUATION", "LONG_RETEST_HOLD"} and pressure_bias != "SHORT")
            or direction_evidence >= 2.5
            or (pressure_bias == "LONG" and trend_score >= max(40.0, compression_score + 8.0, transition_score + 6.0))
        ):
            return ("UPTREND", "directional evidence still points higher")
        if (
            phase4 == "SHORT_ACCEPTANCE"
            or (phase2 in {"SHORT_CONTINUATION", "SHORT_RETEST_HOLD"} and pressure_bias != "LONG")
            or direction_evidence <= -2.5
            or (pressure_bias == "SHORT" and trend_score >= max(40.0, compression_score + 8.0, transition_score + 6.0))
        ):
            return ("DOWNTREND", "directional evidence still points lower")
    return ("FLAT", "no clean directional trend")


def derive_volume_context(snapshot: Mapping[str, Any]) -> tuple[str, str]:
    volume_ratio = _float(snapshot.get("market_pa_volume_ratio"), 1.0) or 1.0
    pa_vol_pass = _bool(snapshot.get("market_pa_vol_pass"))
    rejection_cluster = _bool(snapshot.get("market_pa_rejection_cluster"))
    ladder_fail = _bool(snapshot.get("market_pa_ladder_fail"))
    reversal_risk = _bool(snapshot.get("market_pa_reversal_risk"))
    sequence_exhaust = _bool(snapshot.get("market_pa_sequence_exhaust"))
    sweep_fail = _bool(snapshot.get("market_pa_sweep_fail"))
    effort_result = _upper(snapshot.get("market_pa_effort_result"), "NEUTRAL")

    if volume_ratio <= 0.72:
        return ("DRY", "volume is dry versus recent bars")
    if volume_ratio >= 1.35 and (
        rejection_cluster
        or ladder_fail
        or reversal_risk
        or sequence_exhaust
        or sweep_fail
        or effort_result in {"LONG_POOR", "SHORT_POOR", "POOR", "WEAK"}
    ):
        return ("CLIMACTIC", "high volume is meeting stall or rejection behavior")
    if pa_vol_pass or volume_ratio >= 1.02:
        return ("SUPPORTIVE", "volume supports the current move")
    return ("WEAK", "move is not getting enough volume support")


def derive_breakout_signal(
    snapshot: Mapping[str, Any],
    prev_state: Optional[Mapping[str, Any]],
    trend_state: str,
    structure_state: str,
    volume_context: str,
) -> tuple[str, str]:
    legacy_state = _upper(snapshot.get("market_breakout_state"), "NONE")
    legacy_bias = _upper(snapshot.get("market_breakout_bias"), "NEUTRAL")
    acceptance = _bool(snapshot.get("market_pa_acceptance_drive"))
    retest_hold = _bool(snapshot.get("market_pa_retest_hold"))
    retest_fail = _bool(snapshot.get("market_pa_retest_fail"))
    sweep_fail = _bool(snapshot.get("market_pa_sweep_fail"))
    phase2 = _upper(snapshot.get("market_pa_phase2_context"), "NEUTRAL")
    phase4 = _upper(snapshot.get("market_pa_phase4_context"), "NEUTRAL")
    market_type = _upper(snapshot.get("market_market_type"), "")
    ready_bias = legacy_bias if legacy_bias in {"LONG", "SHORT"} else _price_action_side(snapshot, structure_state, trend_state)
    base_active = market_type in {"COMPRESSION", "RANGE_BALANCE", "ROTATION"} or trend_state == "FLAT"

    if legacy_state == "BREAKOUT_CONFIRM_LONG" or phase4 == "LONG_ACCEPTANCE" or (acceptance and ready_bias == "LONG" and volume_context == "SUPPORTIVE"):
        return ("CONFIRM_LONG", _text(snapshot.get("market_breakout_note"), "bull breakout is confirming"))
    if legacy_state == "BREAKOUT_CONFIRM_SHORT" or phase4 == "SHORT_ACCEPTANCE" or (acceptance and ready_bias == "SHORT" and volume_context == "SUPPORTIVE"):
        return ("CONFIRM_SHORT", _text(snapshot.get("market_breakout_note"), "bear breakout is confirming"))
    if legacy_state == "FAILED_BREAKOUT":
        if ready_bias == "LONG":
            return ("FAIL_LONG", _text(snapshot.get("market_breakout_note"), "bull breakout failed back into base"))
        if ready_bias == "SHORT":
            return ("FAIL_SHORT", _text(snapshot.get("market_breakout_note"), "bear breakout failed back into base"))
    if sweep_fail or retest_fail or phase2 in {"LONG_RETEST_FAIL", "SHORT_RETEST_FAIL"}:
        if trend_state == "UPTREND" or ready_bias == "LONG" or phase2 == "LONG_RETEST_FAIL":
            return ("FAIL_LONG", _text(snapshot.get("market_breakout_note"), "bull breakout failed to hold"))
        if trend_state == "DOWNTREND" or ready_bias == "SHORT" or phase2 == "SHORT_RETEST_FAIL":
            return ("FAIL_SHORT", _text(snapshot.get("market_breakout_note"), "bear breakout failed to hold"))
    if legacy_state in {"BREAKOUT_WATCH_LONG", "BREAKOUT_READY"} and ready_bias == "LONG":
        return ("WATCH_LONG", _text(snapshot.get("market_breakout_note"), "watching for bullish range release"))
    if legacy_state in {"BREAKOUT_WATCH_SHORT", "BREAKOUT_READY"} and ready_bias == "SHORT":
        return ("WATCH_SHORT", _text(snapshot.get("market_breakout_note"), "watching for bearish range release"))
    if base_active and volume_context != "DRY":
        if ready_bias == "LONG" or phase2 in {"LONG_CONTINUATION", "LONG_RETEST_HOLD"} or (structure_state == "HH_HL" and retest_hold):
            return ("WATCH_LONG", "structure is leaning long against a live base")
        if ready_bias == "SHORT" or phase2 in {"SHORT_CONTINUATION", "SHORT_RETEST_HOLD"} or (structure_state == "LH_LL" and retest_hold):
            return ("WATCH_SHORT", "structure is leaning short against a live base")
    return ("NONE", _text(snapshot.get("market_breakout_note"), "no active breakout context"))


def derive_pullback_state(
    snapshot: Mapping[str, Any],
    trend_state: str,
    structure_state: str,
    breakout_signal: str,
    volume_context: str,
) -> tuple[str, str]:
    if trend_state == "FLAT":
        return ("NONE", "no directional trend means there is no active pullback lifecycle")
    if not _has_meaningful_structure_metadata(snapshot):
        if breakout_signal in {"CONFIRM_LONG", "CONFIRM_SHORT"}:
            return ("RESOLVED", "directional continuation is active but no deeper swing pullback structure was required")
        return ("NONE", "pullback lifecycle needs confirmed swing / defense structure")

    phase2 = _upper(snapshot.get("market_pa_phase2_context"), "NEUTRAL")
    phase4 = _upper(snapshot.get("market_pa_phase4_context"), "NEUTRAL")
    phase5 = _upper(snapshot.get("market_pa_phase5_context"), "NEUTRAL")
    pattern = _upper(snapshot.get("market_pa_pattern"), "NEUTRAL")
    constructive_pause = _bool(snapshot.get("market_pa_constructive_pause"))
    retest_hold = _bool(snapshot.get("market_pa_retest_hold"))
    retest_fail = _bool(snapshot.get("market_pa_retest_fail"))
    followthrough_pass = _bool(snapshot.get("market_pa_followthrough_pass"))
    followthrough_fail = _bool(snapshot.get("market_pa_followthrough_fail"))
    acceptance = _bool(snapshot.get("market_pa_acceptance_drive"))
    sweep_fail = _bool(snapshot.get("market_pa_sweep_fail"))
    rejection_cluster = _bool(snapshot.get("market_pa_rejection_cluster"))
    ladder_fail = _bool(snapshot.get("market_pa_ladder_fail"))
    sequence_exhaust = _bool(snapshot.get("market_pa_sequence_exhaust"))
    if structure_state in {"BROKEN_UPTREND", "BROKEN_DOWNTREND"} or retest_fail or sweep_fail:
        return ("FAILED", "pullback failed because the defended trend level did not hold")

    resolved_long = breakout_signal == "CONFIRM_LONG" or phase4 == "LONG_ACCEPTANCE" or (acceptance and followthrough_pass and trend_state == "UPTREND")
    resolved_short = breakout_signal == "CONFIRM_SHORT" or phase4 == "SHORT_ACCEPTANCE" or (acceptance and followthrough_pass and trend_state == "DOWNTREND")
    if resolved_long or resolved_short:
        return ("RESOLVED", "pullback ended and the trend reclaimed control")

    candidate_long = trend_state == "UPTREND" and (retest_hold or constructive_pause or phase2 == "LONG_RETEST_HOLD" or phase5 == "LONG_CONSTRUCTIVE_PAUSE")
    candidate_short = trend_state == "DOWNTREND" and (retest_hold or constructive_pause or phase2 == "SHORT_RETEST_HOLD" or phase5 == "SHORT_CONSTRUCTIVE_PAUSE")
    if candidate_long or candidate_short:
        if rejection_cluster or ladder_fail or sequence_exhaust or volume_context in {"WEAK", "CLIMACTIC"} or followthrough_fail:
            return ("END_CANDIDATE", "pullback is pressing into the defense zone and may be close to resolution")
        return ("ACTIVE", "pullback is active but the defended trend level still holds")

    if constructive_pause or pattern == "PULLBACK" or phase2 in {"LONG_RETEST_HOLD", "SHORT_RETEST_HOLD"}:
        return ("ACTIVE", "counter-trend move is active without a full structure break")
    return ("NONE", "trend is not currently in a pullback cycle")


def derive_phase_state(
    snapshot: Mapping[str, Any],
    trend_state: str,
    structure_state: str,
    breakout_signal: str,
    volume_context: str,
    pullback_state: str,
) -> tuple[str, str]:
    if trend_state == "FLAT":
        return ("BALANCE", "range / balance context is active")

    market_quality = _upper(snapshot.get("market_trend_quality"), "")
    phase2 = _upper(snapshot.get("market_pa_phase2_context"), "NEUTRAL")
    phase3 = _upper(snapshot.get("market_pa_phase3_context"), "NEUTRAL")
    phase5 = _upper(snapshot.get("market_pa_phase5_context"), "NEUTRAL")
    phase6 = _upper(snapshot.get("market_pa_phase6_context"), "NEUTRAL")
    constructive_pause = _bool(snapshot.get("market_pa_constructive_pause"))
    sequence_exhaust = _bool(snapshot.get("market_pa_sequence_exhaust"))
    rejection_cluster = _bool(snapshot.get("market_pa_rejection_cluster"))
    ladder_fail = _bool(snapshot.get("market_pa_ladder_fail"))
    reversal_risk = _bool(snapshot.get("market_pa_reversal_risk"))
    retest_fail = _bool(snapshot.get("market_pa_retest_fail"))
    retest_hold = _bool(snapshot.get("market_pa_retest_hold"))
    followthrough_pass = _bool(snapshot.get("market_pa_followthrough_pass"))
    followthrough_fail = _bool(snapshot.get("market_pa_followthrough_fail"))
    sweep_fail = _bool(snapshot.get("market_pa_sweep_fail"))
    outside_continue = _bool(snapshot.get("market_pa_outside_continue"))
    counter_candle = _counter_candle(snapshot, trend_state)

    exhaustion_flags = 0
    if volume_context == "CLIMACTIC":
        exhaustion_flags += 1
    if volume_context == "WEAK":
        exhaustion_flags += 1
    if rejection_cluster or ladder_fail or reversal_risk or sweep_fail or retest_fail:
        exhaustion_flags += 1
    if sequence_exhaust or phase6 in {"LONG_SEQUENCE_EXHAUST", "SHORT_SEQUENCE_EXHAUST", "LONG_SWING_BREAK", "SHORT_SWING_BREAK"}:
        exhaustion_flags += 1
    if followthrough_fail or phase3 in {"LONG_FOLLOWTHROUGH_FAIL", "SHORT_FOLLOWTHROUGH_FAIL"}:
        exhaustion_flags += 1
    if market_quality == "WEAKENING":
        exhaustion_flags += 1

    expansion_flags = 0
    if breakout_signal in {"CONFIRM_LONG", "CONFIRM_SHORT"}:
        expansion_flags += 2
    if followthrough_pass or outside_continue:
        expansion_flags += 1
    if volume_context == "SUPPORTIVE":
        expansion_flags += 1
    if phase2 in {"LONG_CONTINUATION", "SHORT_CONTINUATION", "LONG_RETEST_HOLD", "SHORT_RETEST_HOLD"}:
        expansion_flags += 1
    if market_quality in {"CONFIRMED", "HEALTHY"}:
        expansion_flags += 1

    if structure_state in {"BROKEN_UPTREND", "BROKEN_DOWNTREND"}:
        return ("BALANCE", "trend structure already broke")
    if pullback_state in {"ACTIVE", "END_CANDIDATE"}:
        return ("PULLBACK", "counter-trend pause is developing inside the active trend defense zone")
    if pullback_state == "FAILED":
        return ("BALANCE", "pullback failed and the prior trend defense level broke")
    if exhaustion_flags >= 2 and expansion_flags <= 2:
        return ("EXHAUSTION", "trend is still present but follow-through is deteriorating")
    if pullback_state == "RESOLVED" or expansion_flags >= 2 or retest_hold:
        return ("EXPANSION", "price is extending with structure and follow-through")
    return ("PULLBACK", "trend is intact but currently not in clean expansion")


def build_classic_market_context(snapshot: Mapping[str, Any], prev_state: Optional[Mapping[str, Any]] = None) -> Dict[str, Any]:
    structure_state, structure_reason = derive_structure_state(snapshot, prev_state=prev_state)
    trend_state, trend_reason = derive_trend_state(snapshot, structure_state)
    volume_context, volume_reason = derive_volume_context(snapshot)
    breakout_signal, breakout_reason = derive_breakout_signal(snapshot, prev_state, trend_state, structure_state, volume_context)
    pullback_state, pullback_reason = derive_pullback_state(snapshot, trend_state, structure_state, breakout_signal, volume_context)
    phase_state, phase_reason = derive_phase_state(snapshot, trend_state, structure_state, breakout_signal, volume_context, pullback_state)

    trend_age = _prev_named_age(prev_state, "market_trend_state", trend_state)
    phase_age = _prev_named_age(prev_state, "market_phase_state", phase_state)
    breakout_age = _prev_named_age(prev_state, "market_breakout_signal", breakout_signal)
    pullback_age = _prev_named_age(prev_state, "market_pullback_state", pullback_state)

    protected_high = _float(snapshot.get("market_structure_last_high"), _float(snapshot.get("market_boundary_high"), np.nan))
    protected_low = _float(snapshot.get("market_structure_last_low"), _float(snapshot.get("market_boundary_low"), np.nan))
    support_zone_low, support_zone_high = _zone_bounds(protected_low, snapshot)
    resistance_zone_low, resistance_zone_high = _zone_bounds(protected_high, snapshot)

    reasons = [trend_reason, structure_reason, phase_reason]
    if pullback_state not in {"NONE", ""}:
        reasons.append(pullback_reason)
    if breakout_signal != "NONE":
        reasons.append(breakout_reason)
    reasons.append(volume_reason)
    context_reasons = _join_reasons(reasons)

    return {
        "market_trend_state": trend_state,
        "market_structure_state": structure_state,
        "market_phase_state": phase_state,
        "market_breakout_signal": breakout_signal,
        "market_volume_context": volume_context,
        "market_pullback_state": pullback_state,
        "market_trend_age": int(max(1, trend_age)),
        "market_phase_age": int(max(1, phase_age)),
        "market_breakout_signal_age": int(max(1, breakout_age)),
        "market_pullback_state_age": int(max(1, pullback_age)),
        "market_protected_high": protected_high if protected_high is not None else np.nan,
        "market_protected_low": protected_low if protected_low is not None else np.nan,
        "market_support_zone_low": support_zone_low,
        "market_support_zone_high": support_zone_high,
        "market_resistance_zone_low": resistance_zone_low,
        "market_resistance_zone_high": resistance_zone_high,
        "market_context_note": phase_reason,
        "market_context_reasons": context_reasons,
    }


def map_classic_context_to_legacy(snapshot: Mapping[str, Any], classic: Mapping[str, Any], prev_state: Optional[Mapping[str, Any]] = None) -> Dict[str, Any]:
    trend_state = _upper(classic.get("market_trend_state"), "FLAT")
    structure_state = _upper(classic.get("market_structure_state"), "MIXED")
    phase_state = _upper(classic.get("market_phase_state"), "BALANCE")
    breakout_signal = _upper(classic.get("market_breakout_signal"), "NONE")
    volume_context = _upper(classic.get("market_volume_context"), "WEAK")

    bias = "LONG" if trend_state == "UPTREND" else ("SHORT" if trend_state == "DOWNTREND" else "NEUTRAL")
    if trend_state == "UPTREND":
        label = {
            "EXPANSION": "BULL_EXPANSION",
            "PULLBACK": "BULL_PULLBACK",
            "EXHAUSTION": "BULL_EXHAUSTION",
        }.get(phase_state, "BULL_PULLBACK")
        phase = phase_state if phase_state in {"EXPANSION", "PULLBACK", "EXHAUSTION"} else "PULLBACK"
    elif trend_state == "DOWNTREND":
        label = {
            "EXPANSION": "BEAR_EXPANSION",
            "PULLBACK": "BEAR_PULLBACK",
            "EXHAUSTION": "BEAR_EXHAUSTION",
        }.get(phase_state, "BEAR_PULLBACK")
        phase = phase_state if phase_state in {"EXPANSION", "PULLBACK", "EXHAUSTION"} else "PULLBACK"
    else:
        base_like = (
            breakout_signal in {"WATCH_LONG", "WATCH_SHORT"}
            or _upper(snapshot.get("market_market_type"), "") in {"COMPRESSION", "RANGE_BALANCE", "ROTATION"}
            or volume_context == "DRY"
            or _upper(snapshot.get("range_filter_state"), "NEUTRAL") == "NEUTRAL"
            or _upper(snapshot.get("angle"), "FLAT") == "FLAT"
            or _upper(snapshot.get("atr_angle"), "RED") == "RED"
        )
        label = "COMPRESSION" if base_like else "TRANSITION"
        phase = "COMPRESSION" if base_like else "TRANSITION"

    confidence = 40.0
    if trend_state != "FLAT":
        confidence += 8.0
    if structure_state in {"HH_HL", "LH_LL"}:
        confidence += 16.0
    elif structure_state in {"BROKEN_UPTREND", "BROKEN_DOWNTREND"}:
        confidence -= 12.0
    if phase_state == "EXPANSION":
        confidence += 14.0
    elif phase_state == "PULLBACK":
        confidence += 4.0
    elif phase_state == "EXHAUSTION":
        confidence -= 4.0
    else:
        confidence -= 2.0
    if breakout_signal in {"CONFIRM_LONG", "CONFIRM_SHORT"}:
        confidence += 8.0
    elif breakout_signal in {"FAIL_LONG", "FAIL_SHORT"}:
        confidence -= 8.0
    if volume_context == "SUPPORTIVE":
        confidence += 8.0
    elif volume_context == "WEAK":
        confidence -= 6.0
    elif volume_context == "DRY":
        confidence -= 10.0
    elif volume_context == "CLIMACTIC":
        confidence -= 4.0

    indicators_long = sum(
        1
        for key, good in (
            ("vidya_state", "BUY"),
            ("frama_state", "BUY"),
            ("range_filter_state", "BUY"),
            ("ms", "GREEN"),
        )
        if _upper(snapshot.get(key), "") == good
    )
    indicators_short = sum(
        1
        for key, good in (
            ("vidya_state", "SELL"),
            ("frama_state", "SELL"),
            ("range_filter_state", "SELL"),
            ("ms", "RED"),
        )
        if _upper(snapshot.get(key), "") == good
    )
    direction_score = indicators_long - indicators_short
    if trend_state == "UPTREND":
        direction_score = max(direction_score, 3)
    elif trend_state == "DOWNTREND":
        direction_score = min(direction_score, -3)
    else:
        direction_score = 0

    energy_score = 0
    if phase_state == "EXPANSION":
        energy_score += 2
    elif phase_state == "EXHAUSTION":
        energy_score -= 1
    if volume_context == "SUPPORTIVE":
        energy_score += 1
    elif volume_context in {"WEAK", "DRY"}:
        energy_score -= 1

    alignment_bonus = 0
    if bias == "LONG":
        alignment_bonus = indicators_long
    elif bias == "SHORT":
        alignment_bonus = indicators_short

    macd = _float(snapshot.get("macd"), 0.0) or 0.0
    signal = _float(snapshot.get("signal"), 0.0) or 0.0
    denom = abs(signal) if abs(signal) > 1e-9 else max(abs(macd), 1.0)
    macd_gap_norm = float((macd - signal) / denom) if denom else 0.0

    pretty = {
        "BULL_EXPANSION": "Bull Expansion",
        "BULL_PULLBACK": "Bull Pullback",
        "BULL_EXHAUSTION": "Bull Exhaustion",
        "BEAR_EXPANSION": "Bear Expansion",
        "BEAR_PULLBACK": "Bear Pullback",
        "BEAR_EXHAUSTION": "Bear Exhaustion",
        "COMPRESSION": "Compression",
        "TRANSITION": "Transition",
    }.get(label, label.replace("_", " ").title())

    texture = f"CLASSIC_{trend_state}_{phase_state}"
    breakout_txt = breakout_signal.replace("_", " ").lower() if breakout_signal != "NONE" else "no breakout"
    summary_short = f"{trend_state.replace('_', ' ').title()} / {phase_state.title()} / {volume_context.title()}"
    if breakout_signal != "NONE":
        summary_short = f"{summary_short} / {breakout_txt}"
    summary_long = _join_reasons(
        [
            _text(classic.get("market_context_note")),
            _text(classic.get("market_context_reasons")),
            _text(snapshot.get("market_breakout_note")),
        ]
    ) or "classic market context is active"

    state_age = compute_state_age(label, prev_state)
    return {
        "market_state": label,
        "market_bias": bias,
        "market_phase": phase,
        "market_confidence": _clamp_int(confidence, 5, 100),
        "market_state_age": int(max(1, state_age)),
        "market_state_pretty": pretty,
        "market_direction_score": int(direction_score),
        "market_energy_score": int(energy_score),
        "market_alignment_bonus": int(alignment_bonus),
        "market_macd_gap_norm": float(macd_gap_norm) if np.isfinite(macd_gap_norm) else np.nan,
        "market_texture": texture,
        "market_summary_short": summary_short,
        "market_summary_long": summary_long,
        "market_pressure_bias": bias,
    }
