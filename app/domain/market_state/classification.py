from __future__ import annotations

"""Classification helpers for staged market-state decisions."""

from dataclasses import dataclass
from typing import Any, Mapping, Optional

import numpy as np

from .scoring import BasicMarketScores, DynamicMarketScores
from .thresholds import MARKET_STATE_BIASES, MARKET_STATE_LABELS, MARKET_STATE_PHASES, MarketStateThresholds


def normalize_market_label(value: Any, default: str = 'TRANSITION') -> str:
    text = str(value or '').upper().strip()
    return text if text in MARKET_STATE_LABELS else default


def normalize_market_bias(value: Any, default: str = 'NEUTRAL') -> str:
    text = str(value or '').upper().strip()
    return text if text in MARKET_STATE_BIASES else default


def normalize_market_phase(value: Any, default: str = 'TRANSITION') -> str:
    text = str(value or '').upper().strip()
    return text if text in MARKET_STATE_PHASES else default


@dataclass(frozen=True)
class BasicClassificationResult:
    label: str
    bias: str
    phase: str
    pullback_flags: int
    exhaustion_flags: int


@dataclass(frozen=True)
class DynamicClassificationResult:
    label: str
    bias: str
    phase: str


def classify_basic_market_state(
    scores: BasicMarketScores,
    indicators: Any,
    thresholds: MarketStateThresholds,
    pa_overlay: Optional[Mapping[str, Any]] = None,
) -> BasicClassificationResult:
    bias = 'NEUTRAL'
    label = 'TRANSITION'
    phase = 'TRANSITION'
    pullback_flags = 0
    exhaustion_flags = 0
    range_state = indicators.range_filter_state
    range_phase = indicators.range_filter_phase
    ms = indicators.ms
    macd_angle = indicators.angle
    vidya_state = indicators.vidya_state
    vidya_delta_pct = indicators.vidya_delta_pct if indicators.vidya_delta_pct is not None else 0.0

    if scores.direction_score >= int(thresholds.direction_long_min):
        bias = 'LONG'
        if ms == 'RED':
            pullback_flags += 1
        if macd_angle == 'RED':
            pullback_flags += 1
        if range_phase in ('BUY_WEAK', 'NEUTRAL'):
            pullback_flags += 1
        if scores.vidya_angle_state == 'FLAT':
            pullback_flags += 1
        if vidya_delta_pct < 10.0:
            pullback_flags += 1

        if scores.vidya_angle_state == 'DOWN':
            exhaustion_flags += 2
        elif scores.vidya_angle_state == 'FLAT':
            exhaustion_flags += 1
        if macd_angle == 'RED':
            exhaustion_flags += 1
        if ms == 'RED':
            exhaustion_flags += 1
        if vidya_delta_pct < -10.0:
            exhaustion_flags += 1
        if range_state == 'NEUTRAL':
            exhaustion_flags += 1

        if exhaustion_flags >= 3 and scores.energy_score <= 0:
            label = 'BULL_EXHAUSTION'
            phase = 'EXHAUSTION'
        elif pullback_flags >= 2 and vidya_state == 'BUY' and range_state != 'SELL':
            label = 'BULL_PULLBACK'
            phase = 'PULLBACK'
        else:
            label = 'BULL_EXPANSION'
            phase = 'EXPANSION'

    elif scores.direction_score <= int(thresholds.direction_short_max):
        bias = 'SHORT'
        if ms == 'GREEN':
            pullback_flags += 1
        if macd_angle == 'GREEN':
            pullback_flags += 1
        if range_phase in ('SELL_WEAK', 'NEUTRAL'):
            pullback_flags += 1
        if scores.vidya_angle_state == 'FLAT':
            pullback_flags += 1
        if vidya_delta_pct > -10.0:
            pullback_flags += 1

        if scores.vidya_angle_state == 'UP':
            exhaustion_flags += 2
        elif scores.vidya_angle_state == 'FLAT':
            exhaustion_flags += 1
        if macd_angle == 'GREEN':
            exhaustion_flags += 1
        if ms == 'GREEN':
            exhaustion_flags += 1
        if vidya_delta_pct > 10.0:
            exhaustion_flags += 1
        if range_state == 'NEUTRAL':
            exhaustion_flags += 1

        if exhaustion_flags >= 3 and scores.energy_score <= 0:
            label = 'BEAR_EXHAUSTION'
            phase = 'EXHAUSTION'
        elif pullback_flags >= 2 and vidya_state == 'SELL' and range_state != 'BUY':
            label = 'BEAR_PULLBACK'
            phase = 'PULLBACK'
        else:
            label = 'BEAR_EXPANSION'
            phase = 'EXPANSION'

    else:
        flat_enough = scores.vidya_angle_state == 'FLAT' and abs(float(vidya_delta_pct)) < float(thresholds.vidya_delta_support_pct)
        low_gap = scores.macd_gap_norm is None or float(scores.macd_gap_norm) < float(thresholds.compression_macd_gap_norm_max)
        if abs(scores.direction_score) <= int(thresholds.compression_direction_abs_max) and flat_enough and low_gap and indicators.atr_angle == 'RED':
            label = 'COMPRESSION'
            phase = 'COMPRESSION'
        else:
            label = 'TRANSITION'
            phase = 'TRANSITION'

    overlay = dict(pa_overlay or {})
    structure_flag = str(overlay.get('market_structure_flag') or '')
    vol_flag = str(overlay.get('market_pa_vol_flag') or '')
    if bias == 'LONG':
        if structure_flag == 'BROKEN':
            exhaustion_flags += 2
            pullback_flags += 1
        elif bool(overlay.get('market_pa_vol_pass')):
            pullback_flags = max(0, pullback_flags - 1)
        elif vol_flag == 'NO_PASS':
            exhaustion_flags += 1
        if exhaustion_flags >= 3 and scores.energy_score <= 0:
            label = 'BULL_EXHAUSTION'
            phase = 'EXHAUSTION'
        elif pullback_flags >= 2 and vidya_state == 'BUY' and range_state != 'SELL':
            label = 'BULL_PULLBACK'
            phase = 'PULLBACK'
        else:
            label = 'BULL_EXPANSION'
            phase = 'EXPANSION'
    elif bias == 'SHORT':
        if structure_flag == 'BROKEN':
            exhaustion_flags += 2
            pullback_flags += 1
        elif bool(overlay.get('market_pa_vol_pass')):
            pullback_flags = max(0, pullback_flags - 1)
        elif vol_flag == 'NO_PASS':
            exhaustion_flags += 1
        if exhaustion_flags >= 3 and scores.energy_score <= 0:
            label = 'BEAR_EXHAUSTION'
            phase = 'EXHAUSTION'
        elif pullback_flags >= 2 and vidya_state == 'SELL' and range_state != 'BUY':
            label = 'BEAR_PULLBACK'
            phase = 'PULLBACK'
        else:
            label = 'BEAR_EXPANSION'
            phase = 'EXPANSION'

    return BasicClassificationResult(label=label, bias=bias, phase=phase, pullback_flags=pullback_flags, exhaustion_flags=exhaustion_flags)


def classify_dynamic_market_state(
    scores: DynamicMarketScores,
    thresholds: MarketStateThresholds,
    hierarchy_overlay: Mapping[str, Any],
) -> DynamicClassificationResult:
    bias = 'LONG' if scores.direction_evidence >= 2.5 else ('SHORT' if scores.direction_evidence <= -2.5 else 'NEUTRAL')
    if bias == 'NEUTRAL' and scores.pressure_bias in ('LONG', 'SHORT') and scores.compression_score >= 55.0:
        bias = 'NEUTRAL'

    label = 'TRANSITION'
    phase = 'TRANSITION'
    if scores.quiet_compression or scores.pressure_compression or scores.comp_dominant:
        label = 'COMPRESSION'
        phase = 'COMPRESSION'
        bias = 'NEUTRAL'
        if not bool(hierarchy_overlay.get('market_true_compression')):
            label = 'TRANSITION'
            phase = 'TRANSITION'
    elif bias == 'LONG':
        if scores.exhaust_dominant:
            label = 'BULL_EXHAUSTION'
            phase = 'EXHAUSTION'
        elif scores.long_expansion_blocked and scores.pullback_score >= max(float(thresholds.expansion_pullback_floor), scores.transition_effective - 2.0):
            label = 'BULL_PULLBACK'
            phase = 'PULLBACK'
        elif scores.long_expansion_blocked and scores.transition_effective >= max(42.0, scores.pullback_score + 1.0):
            label = 'TRANSITION'
            phase = 'TRANSITION'
        elif scores.pullback_dominant and not scores.trend_dominant:
            label = 'BULL_PULLBACK'
            phase = 'PULLBACK'
        elif scores.trend_dominant and scores.breakout_confirmation >= float(thresholds.expansion_confirmation_min) and not scores.long_expansion_blocked:
            label = 'BULL_EXPANSION'
            phase = 'EXPANSION'
        elif scores.trend_dominant and scores.pullback_score >= float(thresholds.expansion_pullback_floor):
            label = 'BULL_PULLBACK'
            phase = 'PULLBACK'
        else:
            label = 'TRANSITION'
            phase = 'TRANSITION'
    elif bias == 'SHORT':
        if scores.exhaust_dominant:
            label = 'BEAR_EXHAUSTION'
            phase = 'EXHAUSTION'
        elif scores.short_expansion_blocked and scores.pullback_score >= max(float(thresholds.expansion_pullback_floor), scores.transition_effective - 2.0):
            label = 'BEAR_PULLBACK'
            phase = 'PULLBACK'
        elif scores.short_expansion_blocked and scores.transition_effective >= max(42.0, scores.pullback_score + 1.0):
            label = 'TRANSITION'
            phase = 'TRANSITION'
        elif scores.pullback_dominant and not scores.trend_dominant:
            label = 'BEAR_PULLBACK'
            phase = 'PULLBACK'
        elif scores.trend_dominant and scores.breakout_confirmation >= float(thresholds.expansion_confirmation_min) and not scores.short_expansion_blocked:
            label = 'BEAR_EXPANSION'
            phase = 'EXPANSION'
        elif scores.trend_dominant and scores.pullback_score >= float(thresholds.expansion_pullback_floor):
            label = 'BEAR_PULLBACK'
            phase = 'PULLBACK'
        else:
            label = 'TRANSITION'
            phase = 'TRANSITION'
    else:
        label = 'COMPRESSION' if scores.structural_compression else 'TRANSITION'
        phase = 'COMPRESSION' if scores.structural_compression else 'TRANSITION'

    if label == 'COMPRESSION' and not bool(hierarchy_overlay.get('market_true_compression')):
        label = 'TRANSITION'
        phase = 'TRANSITION'

    return DynamicClassificationResult(label=label, bias=bias, phase=phase)


def compute_state_age(label: str, prev_state: Optional[Any]) -> int:
    prev_label = None
    prev_age = 0
    if isinstance(prev_state, dict):
        prev_label = str(prev_state.get('market_state') or prev_state.get('label') or '').strip() or None
        try:
            prev_age = int(prev_state.get('market_state_age') or prev_state.get('age') or 0)
        except Exception:
            prev_age = 0
    elif prev_state is not None:
        prev_label = str(prev_state).strip() or None
    return (prev_age + 1) if prev_label == label else 1


def compute_basic_alignment_bonus(indicators: Any, bias: str, vidya_angle_state: str) -> int:
    bonus = 0
    if bias == 'LONG':
        bonus += 1 if indicators.vidya_state == 'BUY' else 0
        bonus += 1 if indicators.frama_state == 'BUY' else 0
        bonus += 1 if indicators.range_filter_state == 'BUY' else 0
        bonus += 1 if vidya_angle_state == 'UP' else 0
    elif bias == 'SHORT':
        bonus += 1 if indicators.vidya_state == 'SELL' else 0
        bonus += 1 if indicators.frama_state == 'SELL' else 0
        bonus += 1 if indicators.range_filter_state == 'SELL' else 0
        bonus += 1 if vidya_angle_state == 'DOWN' else 0
    return int(bonus)


def compute_basic_confidence(label: str, scores: BasicMarketScores, thresholds: MarketStateThresholds, pa_overlay: Optional[Mapping[str, Any]] = None, alignment_bonus: int = 0) -> int:
    confidence = int(thresholds.confidence_base) + int(abs(scores.direction_score) * 7) + int(max(0, scores.energy_score) * 6) + int(alignment_bonus * 5)
    if label in ('TRANSITION', 'COMPRESSION'):
        confidence = 15 + int(max(0, 3 - abs(scores.direction_score)) * 6) + int(max(0, -scores.energy_score) * 4)
    overlay = dict(pa_overlay or {})
    if str(overlay.get('market_structure_flag') or '').upper().strip() == 'BROKEN':
        confidence -= 18
    if str(overlay.get('market_pa_vol_flag') or '').upper().strip() == 'NO_PASS':
        confidence -= 10
    if str(overlay.get('market_pa_strict_flag') or '').upper().strip() == 'NO_PASS':
        confidence -= 12
    if str(overlay.get('market_pa_phase6_context') or '').upper().strip() in ('LONG_SEQUENCE_EXHAUST', 'SHORT_SEQUENCE_EXHAUST', 'LONG_SWING_BREAK', 'SHORT_SWING_BREAK'):
        confidence -= 6
    return int(max(0, min(100, confidence)))


def compute_dynamic_confidence(label: str, scores: DynamicMarketScores, thresholds: MarketStateThresholds, pa_overlay: Optional[Mapping[str, Any]] = None) -> int:
    ordered = sorted((scores.effective_compression, scores.transition_effective, scores.trend_score, scores.pullback_score, scores.exhaustion_score), reverse=True)
    top_score = float(ordered[0] if ordered else 0.0)
    second_score = float(ordered[1] if len(ordered) > 1 else 0.0)
    confidence = int(np.clip(float(thresholds.confidence_base) + top_score * 0.58 + max(0.0, top_score - second_score) * 0.45 + scores.alignment_fraction * 12.0, 5.0, 100.0))
    if label in ('COMPRESSION', 'TRANSITION'):
        confidence = int(np.clip(20.0 + top_score * 0.50 + max(0.0, top_score - second_score) * 0.35, 5.0, 100.0))
    overlay = dict(pa_overlay or {})
    if str(overlay.get('market_structure_flag') or '').upper().strip() == 'BROKEN':
        confidence -= 18
    if str(overlay.get('market_pa_vol_flag') or '').upper().strip() == 'NO_PASS':
        confidence -= 10
    if str(overlay.get('market_pa_strict_flag') or '').upper().strip() == 'NO_PASS':
        confidence -= 12
    if str(overlay.get('market_pa_phase6_context') or '').upper().strip() in ('LONG_SEQUENCE_EXHAUST', 'SHORT_SEQUENCE_EXHAUST', 'LONG_SWING_BREAK', 'SHORT_SWING_BREAK'):
        confidence -= 6
    return int(np.clip(confidence, 5, 100))
