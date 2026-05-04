from __future__ import annotations

"""Typed pipeline adapters and staged market-state orchestration."""

from typing import Any, Callable, Dict, Mapping, Optional

import numpy as np

from .breakout import apply_breakout_value_bonus
from .classification import (
    classify_basic_market_state,
    classify_dynamic_market_state,
    compute_basic_alignment_bonus,
    compute_basic_confidence,
    compute_dynamic_confidence,
    compute_state_age,
    normalize_market_bias,
    normalize_market_label,
    normalize_market_phase,
)
from .classic_context import build_classic_market_context, infer_bias_hint, map_classic_context_to_legacy
from .features import build_market_state_inputs
from .schema import BreakoutAssessment, MarketStateDecision, MarketStateInputs, coerce_mapping
from .scoring import compute_basic_market_scores, compute_dynamic_market_scores
from .summaries import build_basic_market_summaries, build_dynamic_market_summaries
from ...runtime_safety import ExecutionReport, annotate_payload, safe_execute


def ensure_market_state_inputs(snapshot: Any, prev_state: Any = None) -> MarketStateInputs:
    return build_market_state_inputs(snapshot, prev_state=prev_state)


def decision_from_state_payload(payload: Any) -> MarketStateDecision:
    raw = coerce_mapping(payload)
    decision = MarketStateDecision.from_mapping(raw)
    decision = MarketStateDecision(
        label=normalize_market_label(decision.label),
        bias=normalize_market_bias(decision.bias),
        phase=normalize_market_phase(decision.phase),
        confidence=max(0, min(100, int(decision.confidence))),
        age=max(1, int(decision.age)),
        pretty=decision.pretty,
        direction_score=int(decision.direction_score),
        energy_score=int(decision.energy_score),
        alignment_bonus=int(decision.alignment_bonus),
        macd_gap_norm=decision.macd_gap_norm,
        texture=decision.texture,
        summary_short=decision.summary_short,
        summary_long=decision.summary_long,
        pressure_bias=normalize_market_bias(decision.pressure_bias),
        breakout=BreakoutAssessment.from_mapping(raw),
        status=decision.status,
        payload=raw,
    )
    return decision


def finalize_market_state_payload(payload: Any) -> Dict[str, Any]:
    return decision_from_state_payload(payload).as_dict()


def build_market_state_failure_payload(snapshot: Any, prev_state: Any = None, *, reason: str = "", error_type: str = "") -> Dict[str, Any]:
    typed_inputs = ensure_market_state_inputs(snapshot, prev_state=prev_state)
    previous = typed_inputs.previous_state
    age = compute_state_age('TRANSITION', previous)
    state = {
        'market_state': 'TRANSITION',
        'market_bias': 'NEUTRAL',
        'market_phase': 'TRANSITION',
        'market_confidence': 0,
        'market_state_age': int(max(1, age)),
        'market_state_pretty': 'Transition',
        'market_direction_score': 0,
        'market_energy_score': 0,
        'market_alignment_bonus': 0,
        'market_macd_gap_norm': np.nan,
        'market_texture': 'DEGRADED_RUNTIME',
        'market_summary_short': 'market-state runtime degraded / fallback active',
        'market_summary_long': 'The market-state pipeline hit a runtime issue and returned a safe fallback transition state.',
        'market_breakout_state': 'NONE',
        'market_breakout_bias': 'NEUTRAL',
        'market_breakout_pretty': '-',
        'market_breakout_score': 0,
        'market_breakout_trigger_ready': False,
        'market_breakout_note': 'fallback active',
        'market_state_status': 'failed',
        'market_state_status_reason': str(reason or 'market-state fallback active'),
        'market_state_error_type': str(error_type or ''),
        'market_state_degraded_feature': 'pipeline',
        'market_state_error_count': 1,
        'market_state_degraded_features': ['pipeline'],
    }
    return finalize_market_state_payload(state)


def _safe_dict_call(func: Callable[[], Dict[str, Any]], *, degraded_feature: str, module: str, function: str) -> tuple[Dict[str, Any], ExecutionReport]:
    return safe_execute(func, default={}, module=module, function=function, degraded_feature=degraded_feature)


def run_basic_market_state_pipeline(
    snapshot: Any,
    prev_state: Any,
    thresholds: Any,
    price_action_overlay_builder: Callable[[Dict[str, Any], Any, str], Dict[str, Any]],
    pretty_label_builder: Callable[[str], str],
) -> Dict[str, Any]:
    try:
        typed_inputs = ensure_market_state_inputs(snapshot, prev_state=prev_state)
        snap = dict(typed_inputs.raw)
        prev_state = typed_inputs.previous_state
        indicators = typed_inputs.indicators

        scores = compute_basic_market_scores(snap, indicators, thresholds)
        pre_classification = classify_basic_market_state(scores, indicators, thresholds)
        pa_overlay, pa_report = _safe_dict_call(
            lambda: price_action_overlay_builder(snap, prev_state, pre_classification.bias),
            degraded_feature='price_action_overlay',
            module='market_state',
            function='run_basic_market_state_pipeline',
        )
        classification = classify_basic_market_state(scores, indicators, thresholds, pa_overlay=pa_overlay)
        alignment_bonus = compute_basic_alignment_bonus(indicators, classification.bias, scores.vidya_angle_state)
        confidence = compute_basic_confidence(classification.label, scores, thresholds, pa_overlay=pa_overlay, alignment_bonus=alignment_bonus)
        age = compute_state_age(classification.label, prev_state)
        summaries, summary_report = safe_execute(
            lambda: build_basic_market_summaries(classification.label, pa_overlay),
            default=(classification.label, 'runtime degraded / summary fallback', 'Summary builder degraded; using fallback text.'),
            module='market_state',
            function='run_basic_market_state_pipeline',
            degraded_feature='summary_builder',
        )
        texture, summary_short, summary_long = summaries

        pretty_label, label_report = safe_execute(
            lambda: pretty_label_builder(classification.label),
            default=classification.label.replace('_', ' ').title(),
            module='market_state',
            function='run_basic_market_state_pipeline',
            degraded_feature='pretty_label',
        )

        state = {
            'market_state': classification.label,
            'market_bias': classification.bias,
            'market_phase': classification.phase,
            'market_confidence': confidence,
            'market_state_age': int(max(1, age)),
            'market_state_pretty': pretty_label,
            'market_direction_score': int(scores.direction_score),
            'market_energy_score': int(scores.energy_score),
            'market_alignment_bonus': int(alignment_bonus),
            'market_macd_gap_norm': (float(scores.macd_gap_norm) if scores.macd_gap_norm is not None and np.isfinite(float(scores.macd_gap_norm)) else np.nan),
            'market_texture': texture,
            'market_summary_short': summary_short,
            'market_summary_long': summary_long,
            'market_breakout_state': 'NONE',
            'market_breakout_bias': 'NEUTRAL',
            'market_breakout_pretty': '-',
            'market_breakout_score': 0,
            'market_breakout_trigger_ready': False,
            'market_breakout_note': 'no active breakout warning',
        }
        state.update(pa_overlay)
        annotate_payload(state, 'market_state', pa_report, summary_report, label_report)
        return finalize_market_state_payload(state)
    except Exception as exc:
        return build_market_state_failure_payload(snapshot, prev_state=prev_state, reason=str(exc), error_type=exc.__class__.__name__)


def _coerce_bool(value: Any) -> bool:
    if isinstance(value, (bool, np.bool_)):
        return bool(value)
    text = str(value or '').strip().upper()
    return text in {'1', 'TRUE', 'YES', 'Y', 'ON'}


def _coerce_float(value: Any, default: float = 0.0) -> float:
    try:
        out = float(value)
        if np.isfinite(out):
            return float(out)
    except Exception:
        pass
    return float(default)


def run_dynamic_market_state_pipeline(
    snapshot: Any,
    prev_state: Any,
    thresholds: Any,
    price_action_overlay_builder: Callable[[Dict[str, Any], Any, str], Dict[str, Any]],
    hierarchy_overlay_builder: Callable[[Dict[str, Any], str, str], Dict[str, Any]],
    value_overlay_builder: Callable[[Dict[str, Any], str], Dict[str, Any]],
    breakout_context_builder: Callable[[Dict[str, Any], str, str, str, Any], Dict[str, Any]],
    base_summary_builder: Callable[[str, Mapping[str, Any], str], Any],
    pretty_label_builder: Callable[[str], str],
) -> Dict[str, Any]:
    try:
        typed_inputs = ensure_market_state_inputs(snapshot, prev_state=prev_state)
        snap = dict(typed_inputs.raw)
        prev_state = typed_inputs.previous_state
        features = typed_inputs.features

        pre_bias = 'LONG' if features.direction_evidence >= 2.5 else ('SHORT' if features.direction_evidence <= -2.5 else 'NEUTRAL')
        pa_overlay, pa_report = _safe_dict_call(
            lambda: price_action_overlay_builder(snap, prev_state, pre_bias),
            degraded_feature='price_action_overlay',
            module='market_state',
            function='run_dynamic_market_state_pipeline',
        )
        hierarchy_overlay, hierarchy_report = _safe_dict_call(
            lambda: hierarchy_overlay_builder(snap, str(pa_overlay.get('market_structure_flag') or 'MIXED'), pre_bias),
            degraded_feature='hierarchy_overlay',
            module='market_state',
            function='run_dynamic_market_state_pipeline',
        )
        snap.update(hierarchy_overlay)
        value_overlay, value_report = _safe_dict_call(
            lambda: value_overlay_builder(snap, pre_bias),
            degraded_feature='value_overlay',
            module='market_state',
            function='run_dynamic_market_state_pipeline',
        )
        scores = compute_dynamic_market_scores(snap, features, thresholds, pa_overlay, hierarchy_overlay, value_overlay)
        classification = classify_dynamic_market_state(scores, thresholds, hierarchy_overlay)
        confidence = compute_dynamic_confidence(classification.label, scores, thresholds, pa_overlay=pa_overlay)
        age = compute_state_age(classification.label, prev_state)
        breakout_ctx, breakout_report = _safe_dict_call(
            lambda: breakout_context_builder(snap, classification.label, classification.bias, classification.phase, prev_state),
            degraded_feature='breakout_context',
            module='market_state',
            function='run_dynamic_market_state_pipeline',
        )
        breakout_ctx = apply_breakout_value_bonus(breakout_ctx, value_overlay)
        summaries, summary_report = safe_execute(
            lambda: build_dynamic_market_summaries(
                classification.label,
                snap,
                classification.bias,
                pa_overlay,
                value_overlay,
                breakout_ctx,
                base_summary_builder,
            ),
            default=(classification.label, 'runtime degraded / summary fallback', 'Summary builder degraded; using fallback text.'),
            module='market_state',
            function='run_dynamic_market_state_pipeline',
            degraded_feature='summary_builder',
        )
        texture, summary_short, summary_long = summaries
        pretty_label, label_report = safe_execute(
            lambda: pretty_label_builder(classification.label),
            default=classification.label.replace('_', ' ').title(),
            module='market_state',
            function='run_dynamic_market_state_pipeline',
            degraded_feature='pretty_label',
        )

        state = {
            'market_state': classification.label,
            'market_bias': classification.bias,
            'market_phase': classification.phase,
            'market_confidence': confidence,
            'market_state_age': int(max(1, age)),
            'market_state_pretty': pretty_label,
            'market_direction_score': int(scores.direction_score),
            'market_energy_score': int(scores.energy_score),
            'market_alignment_bonus': int(scores.alignment_bonus),
            'market_macd_gap_norm': (float(scores.macd_gap_norm) if scores.macd_gap_norm is not None and np.isfinite(float(scores.macd_gap_norm)) else np.nan),
            'market_texture': texture,
            'market_summary_short': summary_short,
            'market_summary_long': summary_long,
            'market_pressure_bias': scores.pressure_bias if scores.pressure_bias in ('LONG', 'SHORT', 'NEUTRAL') else 'NEUTRAL',
            'market_breakout_state': breakout_ctx.get('market_breakout_state'),
            'market_breakout_bias': breakout_ctx.get('market_breakout_bias'),
            'market_breakout_pretty': breakout_ctx.get('market_breakout_pretty'),
            'market_breakout_score': breakout_ctx.get('market_breakout_score'),
            'market_breakout_trigger_ready': breakout_ctx.get('market_breakout_trigger_ready'),
            'market_breakout_note': breakout_ctx.get('market_breakout_note'),
        }
        state.update(pa_overlay)
        state.update(hierarchy_overlay)
        state.update(value_overlay)
        state['market_breakout_confirmation'] = scores.breakout_confirmation
        state['market_bb_middle'] = _coerce_float(snap.get('market_bb_middle'), np.nan)
        state['market_bb_upper'] = _coerce_float(snap.get('market_bb_upper'), np.nan)
        state['market_bb_lower'] = _coerce_float(snap.get('market_bb_lower'), np.nan)
        state['market_bb_inside_kc'] = bool(_coerce_bool(snap.get('market_bb_inside_kc')))
        state['market_bb_squeeze_strength'] = _coerce_float(snap.get('market_bb_squeeze_strength'), 0.0)
        state['market_bb_expand_strength'] = _coerce_float(snap.get('market_bb_expand_strength'), 0.0)
        state['market_bb_band_walk_long'] = bool(_coerce_bool(snap.get('market_bb_band_walk_long')))
        state['market_bb_band_walk_short'] = bool(_coerce_bool(snap.get('market_bb_band_walk_short')))
        state['market_bb_breakout_long'] = bool(_coerce_bool(snap.get('market_bb_breakout_long')))
        state['market_bb_breakout_short'] = bool(_coerce_bool(snap.get('market_bb_breakout_short')))
        state['market_kc_middle'] = _coerce_float(snap.get('market_kc_middle'), np.nan)
        state['market_kc_upper'] = _coerce_float(snap.get('market_kc_upper'), np.nan)
        state['market_kc_lower'] = _coerce_float(snap.get('market_kc_lower'), np.nan)
        state['market_vwap'] = _coerce_float(snap.get('market_vwap'), np.nan)
        state['market_vwap_distance_atr'] = _coerce_float(snap.get('market_vwap_distance_atr'), 0.0)
        state['market_vwap_bias'] = str(snap.get('market_vwap_bias') or 'NEUTRAL')
        state['market_vwap_reclaim_long'] = bool(_coerce_bool(snap.get('market_vwap_reclaim_long')))
        state['market_vwap_reclaim_short'] = bool(_coerce_bool(snap.get('market_vwap_reclaim_short')))
        state['market_vwap_reject_long'] = bool(_coerce_bool(snap.get('market_vwap_reject_long')))
        state['market_vwap_reject_short'] = bool(_coerce_bool(snap.get('market_vwap_reject_short')))
        state['market_bb_width_atr'] = _coerce_float(snap.get('market_bb_width_atr'), 0.0)
        state['market_kc_width_atr'] = _coerce_float(snap.get('market_kc_width_atr'), 0.0)
        state['market_envelope_width_atr'] = _coerce_float(snap.get('market_envelope_width_atr'), 0.0)
        state['market_breakout_drive_long'] = _coerce_float(snap.get('market_breakout_drive_long'), 0.0)
        state['market_breakout_drive_short'] = _coerce_float(snap.get('market_breakout_drive_short'), 0.0)
        state['market_breakout_sequence_long'] = bool(_coerce_bool(snap.get('market_breakout_sequence_long')))
        state['market_breakout_sequence_short'] = bool(_coerce_bool(snap.get('market_breakout_sequence_short')))
        state['market_breakout_single_spike_long'] = bool(_coerce_bool(snap.get('market_breakout_single_spike_long')))
        state['market_breakout_single_spike_short'] = bool(_coerce_bool(snap.get('market_breakout_single_spike_short')))
        annotate_payload(state, 'market_state', pa_report, hierarchy_report, value_report, breakout_report, summary_report, label_report)
        return finalize_market_state_payload(state)
    except Exception as exc:
        return build_market_state_failure_payload(snapshot, prev_state=prev_state, reason=str(exc), error_type=exc.__class__.__name__)


def run_classic_market_state_pipeline(
    snapshot: Any,
    prev_state: Any,
    thresholds: Any,
    price_action_overlay_builder: Callable[[Dict[str, Any], Any, str], Dict[str, Any]],
    hierarchy_overlay_builder: Callable[[Dict[str, Any], str, str], Dict[str, Any]],
    value_overlay_builder: Callable[[Dict[str, Any], str], Dict[str, Any]],
    breakout_context_builder: Callable[[Dict[str, Any], str, str, str, Any], Dict[str, Any]],
    pretty_label_builder: Callable[[str], str],
) -> Dict[str, Any]:
    try:
        snap = coerce_mapping(snapshot)
        prev_state = coerce_mapping(prev_state)

        initial_bias_hint = infer_bias_hint(snap)
        pa_overlay, pa_report = _safe_dict_call(
            lambda: price_action_overlay_builder(snap, prev_state, initial_bias_hint),
            degraded_feature='price_action_overlay',
            module='market_state',
            function='run_classic_market_state_pipeline',
        )
        snap.update(pa_overlay)

        structure_flag = str(pa_overlay.get('market_structure_flag') or 'MIXED')
        hierarchy_overlay, hierarchy_report = _safe_dict_call(
            lambda: hierarchy_overlay_builder(snap, structure_flag, initial_bias_hint),
            degraded_feature='hierarchy_overlay',
            module='market_state',
            function='run_classic_market_state_pipeline',
        )
        snap.update(hierarchy_overlay)

        value_overlay, value_report = _safe_dict_call(
            lambda: value_overlay_builder(snap, initial_bias_hint),
            degraded_feature='value_overlay',
            module='market_state',
            function='run_classic_market_state_pipeline',
        )
        snap.update(value_overlay)

        classic = build_classic_market_context(snap, prev_state=prev_state)
        legacy_seed = map_classic_context_to_legacy(snap, classic, prev_state=prev_state)
        snap.update(classic)
        snap.update(legacy_seed)

        breakout_ctx, breakout_report = _safe_dict_call(
            lambda: breakout_context_builder(
                snap,
                str(legacy_seed.get('market_state') or 'TRANSITION'),
                str(legacy_seed.get('market_bias') or 'NEUTRAL'),
                str(legacy_seed.get('market_phase') or 'TRANSITION'),
                prev_state,
            ),
            degraded_feature='breakout_context',
            module='market_state',
            function='run_classic_market_state_pipeline',
        )
        breakout_ctx = apply_breakout_value_bonus(breakout_ctx, value_overlay)
        snap.update(breakout_ctx)

        # Run one more pass so the classic breakout signal can react to the now-populated
        # breakout context fields without abandoning the classic classification contract.
        classic = build_classic_market_context(snap, prev_state=prev_state)
        state = map_classic_context_to_legacy(snap, classic, prev_state=prev_state)
        state.update(classic)
        state.update(pa_overlay)
        state.update(hierarchy_overlay)
        state.update(value_overlay)
        state.update(breakout_ctx)

        pretty_label, label_report = safe_execute(
            lambda: pretty_label_builder(state.get('market_state')),
            default=str(state.get('market_state') or 'Transition').replace('_', ' ').title(),
            module='market_state',
            function='run_classic_market_state_pipeline',
            degraded_feature='pretty_label',
        )
        state['market_state_pretty'] = pretty_label
        annotate_payload(state, 'market_state', pa_report, hierarchy_report, value_report, breakout_report, label_report)
        return finalize_market_state_payload(state)
    except Exception as exc:
        return build_market_state_failure_payload(snapshot, prev_state=prev_state, reason=str(exc), error_type=exc.__class__.__name__)
