from __future__ import annotations

"""Score-building helpers for the staged market-state pipeline."""

from dataclasses import dataclass
from typing import Any, Mapping, Optional

import numpy as np

from .schema import IndicatorSnapshot, MarketFeatures
from .thresholds import MarketStateThresholds


def _safe_float(value: Any) -> Optional[float]:
    try:
        if value is None:
            return None
        out = float(value)
        if np.isfinite(out):
            return float(out)
    except Exception:
        pass
    return None


def _safe_bool(value: Any) -> bool:
    if isinstance(value, (bool, np.bool_)):
        return bool(value)
    text = str(value or '').strip().upper()
    return text in {'1', 'TRUE', 'YES', 'Y', 'ON'}


def _state_sign(value: Any) -> int:
    text = str(value or '').upper().strip()
    if text in {'BUY', 'LONG', 'UP'}:
        return 1
    if text in {'SELL', 'SHORT', 'DOWN'}:
        return -1
    return 0


def _color_sign(value: Any) -> int:
    text = str(value or '').upper().strip()
    if text == 'GREEN':
        return 1
    if text == 'RED':
        return -1
    return 0


@dataclass(frozen=True)
class BasicMarketScores:
    vidya_angle_state: str
    direction_score: int
    energy_score: int
    macd_gap_norm: Optional[float]


@dataclass(frozen=True)
class DynamicMarketScores:
    compression_score: float
    transition_score: float
    trend_score: float
    pullback_score: float
    exhaustion_score: float
    breakout_quality: float
    breakout_readiness: float
    breakout_confirmation: float
    compression_maturity: float
    containment_rate: float
    slope_flatness: float
    range_contraction: float
    swing_squeeze: float
    delta_strength: float
    direction_evidence: float
    conflict_score: float
    alignment_fraction: float
    pressure_bias: str
    effective_compression: float
    transition_penalty: float
    transition_effective: float
    structural_compression: bool
    quiet_compression: bool
    pressure_compression: bool
    comp_dominant: bool
    trend_dominant: bool
    pullback_dominant: bool
    exhaust_dominant: bool
    long_weakness_score: float
    short_weakness_score: float
    long_expansion_blocked: bool
    short_expansion_blocked: bool
    direction_score: int
    energy_score: int
    alignment_bonus: int
    macd_gap_norm: Optional[float]


def infer_vidya_angle_state(snapshot: Mapping[str, Any], indicators: IndicatorSnapshot, thresholds: MarketStateThresholds) -> str:
    vidya_angle_norm = indicators.vidya_angle_deg_norm
    if vidya_angle_norm is None:
        vidya_angle_norm = indicators.vidya_angle_deg
    state = str(snapshot.get('vidya_angle_state') or '').upper().strip()
    if state in {'UP', 'DOWN', 'FLAT'}:
        return state
    if vidya_angle_norm is None:
        return 'FLAT'
    if vidya_angle_norm > float(thresholds.vidya_angle_up_deg):
        return 'UP'
    if vidya_angle_norm < float(thresholds.vidya_angle_down_deg):
        return 'DOWN'
    return 'FLAT'


def compute_basic_market_scores(snapshot: Mapping[str, Any], indicators: IndicatorSnapshot, thresholds: MarketStateThresholds) -> BasicMarketScores:
    vidya_delta_pct = indicators.vidya_delta_pct if indicators.vidya_delta_pct is not None else 0.0
    vidya_angle_norm = indicators.vidya_angle_deg_norm
    if vidya_angle_norm is None:
        vidya_angle_norm = indicators.vidya_angle_deg
    vidya_angle_state = infer_vidya_angle_state(snapshot, indicators, thresholds)

    direction_score = 0
    direction_score += 3 * _state_sign(indicators.vidya_state)
    direction_score += 2 * _state_sign(indicators.frama_state)
    direction_score += 2 * _state_sign(indicators.range_filter_state)
    if indicators.macd is not None and indicators.signal is not None:
        direction_score += 1 if indicators.macd > indicators.signal else -1 if indicators.macd < indicators.signal else 0
    direction_score += _color_sign(indicators.ms)
    direction_score += 2 if vidya_angle_state == 'UP' else -2 if vidya_angle_state == 'DOWN' else 0

    energy_score = 0
    energy_score += _color_sign(indicators.atr_angle)
    energy_score += _color_sign(indicators.angle)
    if vidya_delta_pct >= float(thresholds.vidya_delta_support_pct):
        energy_score += 2
    elif vidya_delta_pct <= -float(thresholds.vidya_delta_support_pct):
        energy_score -= 2
    elif abs(float(vidya_delta_pct)) < float(thresholds.vidya_delta_weak_pct):
        energy_score -= 1

    if vidya_angle_norm is not None:
        if abs(float(vidya_angle_norm)) >= float(thresholds.vidya_angle_strong_deg):
            energy_score += 1
        elif abs(float(vidya_angle_norm)) <= abs(float(thresholds.vidya_angle_up_deg)):
            energy_score -= 1

    macd_gap_norm = None
    if indicators.macd is not None and indicators.signal is not None:
        gap = abs(float(indicators.macd) - float(indicators.signal))
        if indicators.atr is not None and abs(float(indicators.atr)) > 1e-12:
            macd_gap_norm = gap / abs(float(indicators.atr))
        else:
            macd_gap_norm = gap

    return BasicMarketScores(
        vidya_angle_state=vidya_angle_state,
        direction_score=int(direction_score),
        energy_score=int(energy_score),
        macd_gap_norm=macd_gap_norm,
    )


def compute_dynamic_market_scores(
    snapshot: Mapping[str, Any],
    features: MarketFeatures,
    thresholds: MarketStateThresholds,
    pa_overlay: Mapping[str, Any],
    hierarchy_overlay: Mapping[str, Any],
    value_overlay: Mapping[str, Any],
) -> DynamicMarketScores:
    compression_score = float(features.compression_score)
    transition_score = float(features.transition_score)
    trend_score = max(0.0, float(features.trend_score) + float(pa_overlay.get('market_pa_trend_adjust') or 0.0) + float(value_overlay.get('market_value_trend_adjust') or 0.0))
    pullback_score = max(0.0, float(features.pullback_score) + float(pa_overlay.get('market_pa_pullback_adjust') or 0.0))
    exhaustion_score = max(0.0, float(features.exhaustion_score) + float(pa_overlay.get('market_pa_exhaustion_adjust') or 0.0))
    breakout_quality = float(features.breakout_quality)
    breakout_readiness = float(features.breakout_readiness)
    breakout_confirmation = float(features.breakout_confirmation)
    compression_maturity = float(features.compression_maturity)
    containment_rate = float(features.containment_rate)
    slope_flatness = float(features.slope_flatness)
    range_contraction = float(features.range_contraction)
    swing_squeeze = float(features.swing_squeeze)
    delta_strength = float(features.delta_strength)
    direction_evidence = float(features.direction_evidence)
    conflict_score = float(features.conflict_score)
    alignment_fraction = float(features.alignment_fraction)
    pressure_bias = str(features.pressure_bias or 'NEUTRAL').upper().strip() or 'NEUTRAL'

    effective_compression = max(compression_score, compression_maturity) + float(value_overlay.get('market_value_compression_adjust') or 0.0)
    transition_penalty = 0.0
    if compression_maturity >= 55.0 and breakout_confirmation < 0.56:
        transition_penalty = (
            (0.18 * compression_maturity)
            + (18.0 * containment_rate)
            + (14.0 * range_contraction)
            + (12.0 * swing_squeeze)
            + (8.0 * max(0.0, slope_flatness - 0.50))
        )
    transition_effective = max(
        0.0,
        transition_score - transition_penalty + breakout_confirmation * 22.0 + float(pa_overlay.get('market_pa_transition_adjust') or 0.0) + float(value_overlay.get('market_value_transition_adjust') or 0.0),
    )

    structural_compression = bool(
        effective_compression >= 56.0
        and compression_maturity >= 58.0
        and containment_rate >= 0.58
        and (range_contraction >= 0.34 or swing_squeeze >= 0.34)
        and breakout_confirmation < 0.58
        and bool(hierarchy_overlay.get('market_true_compression'))
    )
    quiet_compression = bool(structural_compression and delta_strength < 0.62 and breakout_quality < 0.52)
    pressure_compression = bool(structural_compression and pressure_bias in ('LONG', 'SHORT') and breakout_readiness >= 0.56)

    comp_dominant = bool(hierarchy_overlay.get('market_true_compression')) and effective_compression >= max(50.0, trend_score + 4.0, transition_effective + 1.5)
    trend_dominant = trend_score >= max(48.0, effective_compression + 4.0, transition_effective + 2.0)
    pullback_dominant = pullback_score >= max(46.0, transition_effective + 2.0)
    exhaust_dominant = exhaustion_score >= max(56.0, trend_score * 0.82)

    pa_vol_flag = str(pa_overlay.get('market_pa_vol_flag') or '').upper().strip()
    strict_flag = str(pa_overlay.get('market_pa_strict_flag') or '').upper().strip()
    phase3_context = str(pa_overlay.get('market_pa_phase3_context') or '').upper().strip()
    phase4_context = str(pa_overlay.get('market_pa_phase4_context') or '').upper().strip()
    phase5_context = str(pa_overlay.get('market_pa_phase5_context') or '').upper().strip()
    phase6_context = str(pa_overlay.get('market_pa_phase6_context') or '').upper().strip()

    long_weakness_score = 0.0
    short_weakness_score = 0.0
    if pa_vol_flag == 'NO_PASS':
        long_weakness_score += 0.75
        short_weakness_score += 0.75
    if strict_flag == 'NO_PASS':
        long_weakness_score += 0.75
        short_weakness_score += 0.75
    if phase3_context in ('LONG_FOLLOWTHROUGH_FAIL', 'LONG_STALL_AFTER_IMPULSE'):
        long_weakness_score += 1.0
    if phase3_context in ('SHORT_FOLLOWTHROUGH_FAIL', 'SHORT_STALL_AFTER_IMPULSE'):
        short_weakness_score += 1.0
    if phase4_context in ('LONG_SWEEP_FAIL', 'LONG_REVERSAL_RISK'):
        long_weakness_score += 1.0
    if phase4_context in ('SHORT_SWEEP_FAIL', 'SHORT_REVERSAL_RISK'):
        short_weakness_score += 1.0
    if phase5_context in ('LONG_LADDER_FAIL', 'LONG_REJECTION_CLUSTER'):
        long_weakness_score += 0.75
    if phase5_context in ('SHORT_LADDER_FAIL', 'SHORT_REJECTION_CLUSTER'):
        short_weakness_score += 0.75
    if phase6_context in ('LONG_SEQUENCE_EXHAUST', 'LONG_SWING_BREAK'):
        long_weakness_score += 1.25
    if phase6_context in ('SHORT_SEQUENCE_EXHAUST', 'SHORT_SWING_BREAK'):
        short_weakness_score += 1.25

    long_breakout_sequence = bool(_safe_bool(snapshot.get('market_breakout_sequence_long')))
    short_breakout_sequence = bool(_safe_bool(snapshot.get('market_breakout_sequence_short')))
    long_single_spike = bool(_safe_bool(snapshot.get('market_breakout_single_spike_long')))
    short_single_spike = bool(_safe_bool(snapshot.get('market_breakout_single_spike_short')))
    long_expansion_blocked = bool(
        long_weakness_score > float(thresholds.expansion_max_weakness_score)
        or exhaustion_score >= float(thresholds.expansion_exhaustion_cap)
        or (breakout_confirmation >= max(float(thresholds.expansion_confirmation_min), 0.60) and long_single_spike and not long_breakout_sequence)
    )
    short_expansion_blocked = bool(
        short_weakness_score > float(thresholds.expansion_max_weakness_score)
        or exhaustion_score >= float(thresholds.expansion_exhaustion_cap)
        or (breakout_confirmation >= max(float(thresholds.expansion_confirmation_min), 0.60) and short_single_spike and not short_breakout_sequence)
    )

    macd_gap_norm = _safe_float(snapshot.get('market_macd_gap_norm'))
    energy_score = int(np.clip(round((trend_score - effective_compression * 0.60 + breakout_quality * 28.0 + breakout_confirmation * 16.0 - conflict_score * 10.0) / 10.0), -9, 9))
    alignment_bonus = int(np.clip(round(alignment_fraction * 4.0), 0, 4))
    direction_score = int(np.clip(round(direction_evidence * 2.0), -12, 12))

    return DynamicMarketScores(
        compression_score=compression_score,
        transition_score=transition_score,
        trend_score=trend_score,
        pullback_score=pullback_score,
        exhaustion_score=exhaustion_score,
        breakout_quality=breakout_quality,
        breakout_readiness=breakout_readiness,
        breakout_confirmation=breakout_confirmation,
        compression_maturity=compression_maturity,
        containment_rate=containment_rate,
        slope_flatness=slope_flatness,
        range_contraction=range_contraction,
        swing_squeeze=swing_squeeze,
        delta_strength=delta_strength,
        direction_evidence=direction_evidence,
        conflict_score=conflict_score,
        alignment_fraction=alignment_fraction,
        pressure_bias=pressure_bias,
        effective_compression=effective_compression,
        transition_penalty=transition_penalty,
        transition_effective=transition_effective,
        structural_compression=structural_compression,
        quiet_compression=quiet_compression,
        pressure_compression=pressure_compression,
        comp_dominant=bool(comp_dominant),
        trend_dominant=bool(trend_dominant),
        pullback_dominant=bool(pullback_dominant),
        exhaust_dominant=bool(exhaust_dominant),
        long_weakness_score=long_weakness_score,
        short_weakness_score=short_weakness_score,
        long_expansion_blocked=long_expansion_blocked,
        short_expansion_blocked=short_expansion_blocked,
        direction_score=direction_score,
        energy_score=energy_score,
        alignment_bonus=alignment_bonus,
        macd_gap_norm=macd_gap_norm,
    )
