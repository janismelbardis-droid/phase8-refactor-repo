from __future__ import annotations

"""Inspector/detail text helpers extracted from the Tk UI shell.

These helpers keep rendering behavior intact while shrinking the amount of
presentation code that lives directly on ``VisualApp``.
"""

import time
from typing import Any, Dict, List

import numpy as np

from ..indicators_streaming import pretty_market_state_label, market_state_thresholds_dict
from .presenters import market_state_playbook, market_state_thesis


def popup_yes_no(value: Any) -> str:
    try:
        return "YES" if bool(value) else "NO"
    except Exception:
        return "NO"


def detail_section(title: str, lines: List[str]) -> str:
    out = [title]
    out.extend([f"  {line}" for line in lines if isinstance(line, str) and line.strip()])
    return "\n".join(out)


def derive_classic_ta_snapshot(latest: Dict[str, Any]) -> Dict[str, str]:
        label = str(latest.get("market_state") or "").upper().strip()
        bias = str(latest.get("market_bias") or "NEUTRAL").upper().strip() or "NEUTRAL"
        phase = str(latest.get("market_phase") or "TRANSITION").upper().strip() or "TRANSITION"
        structure_flag = str(latest.get("market_structure_flag") or "MIXED").upper().strip() or "MIXED"
        pa_flag = str(latest.get("market_pa_vol_flag") or "MIXED").upper().strip() or "MIXED"
        strict_flag = str(latest.get("market_pa_strict_flag") or "MIXED").upper().strip() or "MIXED"
        of_risk = str(latest.get("market_of_risk_flag") or "-").upper().strip() or "-"
        breakout_state = str(latest.get("market_breakout_state") or "NONE").upper().strip() or "NONE"
        breakout_bias = str(latest.get("market_breakout_bias") or "NEUTRAL").upper().strip() or "NEUTRAL"
        breakout_style = str(latest.get("market_breakout_style") or "NONE").upper().strip() or "NONE"
        phase3 = str(latest.get("market_pa_phase3_context") or "NEUTRAL").upper().strip() or "NEUTRAL"
        phase4 = str(latest.get("market_pa_phase4_context") or "NEUTRAL").upper().strip() or "NEUTRAL"
        phase5 = str(latest.get("market_pa_phase5_context") or "NEUTRAL").upper().strip() or "NEUTRAL"
        phase6 = str(latest.get("market_pa_phase6_context") or "NEUTRAL").upper().strip() or "NEUTRAL"
        vwap_bias = str(latest.get("market_vwap_bias") or "NEUTRAL").upper().strip() or "NEUTRAL"
        vol_regime = str(latest.get("market_volatility_regime") or "NORMAL").upper().strip() or "NORMAL"
        market_type_raw = str(latest.get("market_market_type") or "TRANSITION").upper().strip() or "TRANSITION"
        trend_regime_raw = str(latest.get("market_trend_regime") or "TRANSITION").upper().strip() or "TRANSITION"
        trend_quality_raw = str(latest.get("market_trend_quality") or "MIXED").upper().strip() or "MIXED"

        market_type_map = {
            "COMPRESSION": "Compression / coil",
            "TREND": "Trend",
            "RANGE_BALANCE": "Range / balance",
            "ROTATION": "Volatile rotation",
            "TRANSITION": "Transition / mixed",
        }
        trend_regime_map = {
            "UPTREND": "Uptrend",
            "DOWNTREND": "Downtrend",
            "FLAT_COMPRESSION": "Flat / compression",
            "VOLATILE_BALANCE": "Volatile balance",
            "RANGE_BALANCE": "Range / balance",
            "TRANSITION": "Transition / no clean trend",
        }
        trend_quality_map = {
            "CONFIRMED": "Confirmed",
            "HEALTHY": "Healthy",
            "BUILDING": "Building pressure",
            "PULLBACK": "Pullback inside trend",
            "WEAKENING": "Weakening",
            "BROKEN": "Broken / damaged",
            "UNSTABLE": "Unstable",
            "MIXED": "Developing / mixed",
        }
        volatility_map = {
            "COMPRESSION": "Compression",
            "NORMAL": "Normal",
            "HIGH_VOL": "High volatility",
            "VOLATILE_ROTATION": "Volatile rotation",
        }

        market_type = market_type_map.get(market_type_raw, market_type_raw.title())
        trend_regime = trend_regime_map.get(trend_regime_raw, trend_regime_raw.title())
        trend_quality = trend_quality_map.get(trend_quality_raw, trend_quality_raw.title())
        volatility_regime = volatility_map.get(vol_regime, vol_regime.title())

        if structure_flag == "INTACT":
            if bool(latest.get("market_structure_swing_hh")) and bool(latest.get("market_structure_swing_hl")):
                swing_map = "HH + HL intact"
            elif bool(latest.get("market_structure_swing_lh")) and bool(latest.get("market_structure_swing_ll")):
                swing_map = "LH + LL intact"
            else:
                swing_map = "pivot structure intact"
        elif structure_flag == "BROKEN":
            swing_map = "pivot structure broken"
        else:
            swing_map = "mixed / overlapping"

        if phase6 in ("LONG_SWING_BREAK", "SHORT_SWING_BREAK"):
            local_sequence = "broken"
        elif phase6 in ("LONG_SEQUENCE_EXHAUST", "SHORT_SEQUENCE_EXHAUST"):
            local_sequence = "exhausting"
        elif phase3 in ("LONG_SEQUENCE", "SHORT_SEQUENCE", "LONG_FOLLOWTHROUGH_PASS", "SHORT_FOLLOWTHROUGH_PASS"):
            local_sequence = "confirming"
        elif phase3 in ("LONG_FOLLOWTHROUGH_FAIL", "SHORT_FOLLOWTHROUGH_FAIL", "LONG_STALL_AFTER_IMPULSE", "SHORT_STALL_AFTER_IMPULSE"):
            local_sequence = "damaged"
        else:
            local_sequence = "mixed"

        if bool(latest.get("market_pa_retest_hold")):
            sr_context = "Retest holding"
        elif bool(latest.get("market_pa_retest_fail")):
            sr_context = "Retest failing"
        elif bool(latest.get("market_boundary_outside_above")):
            sr_context = "Accepted above recent resistance"
        elif bool(latest.get("market_boundary_outside_below")):
            sr_context = "Accepted below recent support"
        elif bool(latest.get("market_boundary_near_upper")):
            sr_context = "Near local resistance / range high"
        elif bool(latest.get("market_boundary_near_lower")):
            sr_context = "Near local support / range low"
        elif vwap_bias == "LONG":
            sr_context = "Holding above value / support zone"
        elif vwap_bias == "SHORT":
            sr_context = "Holding below value / resistance zone"
        else:
            sr_context = "No strong level read"

        if breakout_state.startswith("BREAKOUT_CONFIRM"):
            pattern_context = f"Compression breakout {breakout_bias.title()}"
        elif breakout_style in ("SEQUENCE_LONG", "SEQUENCE_SHORT"):
            pattern_context = "Multi-candle breakout drive"
        elif breakout_style in ("SPIKE_LONG", "SPIKE_SHORT"):
            pattern_context = "Single-candle spike / not ideal breakout proof"
        elif phase == "PULLBACK":
            pattern_context = "Trend pullback"
        elif phase == "EXPANSION":
            pattern_context = "Trend continuation"
        elif market_type_raw == "COMPRESSION":
            pattern_context = "Tight squeeze / stored energy"
        elif market_type_raw == "ROTATION":
            pattern_context = "Wide rotational range"
        elif phase4 in ("LONG_SWEEP_FAIL", "SHORT_SWEEP_FAIL"):
            pattern_context = "Sweep failure / trap risk"
        elif phase5 in ("LONG_CONSTRUCTIVE_PAUSE", "SHORT_CONSTRUCTIVE_PAUSE"):
            pattern_context = "Constructive pause"
        elif phase5 in ("LONG_REJECTION_CLUSTER", "SHORT_REJECTION_CLUSTER"):
            pattern_context = "Repeated rejection cluster"
        else:
            pattern_context = "No clean classic pattern"

        if breakout_state.startswith("BREAKOUT_CONFIRM") and strict_flag == "PASS" and of_risk == "PASS":
            actionability = "Actionable breakout"
        elif strict_flag == "PASS" and pa_flag == "PASS" and structure_flag == "INTACT" and of_risk == "PASS" and market_type_raw == "TREND":
            actionability = "Trend setup quality good"
        elif market_type_raw == "ROTATION":
            actionability = "Caution: volatile rotation"
        elif of_risk == "BLOCK":
            actionability = "Blocked by live order flow"
        elif strict_flag == "NO_PASS":
            actionability = "Wait: hard setup fuse failed"
        elif pa_flag == "NO_PASS":
            actionability = "Wait: candle / volume quality poor"
        else:
            actionability = "Watch / no clean execution edge"

        return {
            "market_type": market_type,
            "trend_regime": trend_regime,
            "trend_quality": trend_quality,
            "volatility_regime": volatility_regime,
            "swing_map": swing_map,
            "local_sequence": local_sequence,
            "support_resistance": sr_context,
            "pattern_context": pattern_context,
            "actionability": actionability,
        }

def build_market_state_detail_text(app: Any, tf: str) -> str:
        latest = (app.latest.get(tf, {}) if hasattr(app, "latest") else {}) or {}

        def _fmt_num(v: Any, fmt: str = ".3f", suffix: str = "") -> str:
            try:
                fv = float(v)
                if np.isfinite(fv):
                    return f"{format(fv, fmt)}{suffix}"
            except Exception:
                pass
            return "-"

        label = latest.get("market_state")
        pretty = pretty_market_state_label(label) if label else "-"
        bias = latest.get("market_bias") or "-"
        phase = latest.get("market_phase") or "-"
        classic = derive_classic_ta_snapshot(latest)
        macd = latest.get("macd")
        signal = latest.get("signal")
        if macd is None or signal is None:
            macd_rel = "-"
        else:
            try:
                m = float(macd)
                s = float(signal)
                if np.isfinite(m) and np.isfinite(s):
                    if m > s:
                        macd_rel = f"MACD > Signal ({m:.6f} > {s:.6f})"
                    elif m < s:
                        macd_rel = f"MACD < Signal ({m:.6f} < {s:.6f})"
                    else:
                        macd_rel = f"MACD = Signal ({m:.6f})"
                else:
                    macd_rel = "-"
            except Exception:
                macd_rel = "-"

        th_map = market_state_thresholds_dict(app._market_state_thresholds_obj())
        breakout_last = (getattr(app, '_breakout_alert_last_by_tf', {}) or {}).get(tf, {}) or {}

        sections = []
        sections.append(detail_section("=== MARKET SUMMARY ===", [
            f"Timeframe: {tf}",
            f"State: {pretty}   Bias: {bias}   Phase: {phase}",
            f"Confidence: {_fmt_num(latest.get('market_confidence'), '.0f', '%')}   State Age: {_fmt_num(latest.get('market_state_age'), '.0f', ' bars')}",
            f"Volatility Regime: {classic['volatility_regime']}",
            f"Trend Regime: {classic['trend_regime']}   Trend Quality: {classic['trend_quality']}",
            f"Market Type: {classic['market_type']}   Actionability: {classic['actionability']}",
            f"Texture: {latest.get('market_texture') or '-'}",
            f"Summary: {latest.get('market_summary_short') or '-'}",
        ]))

        sections.append(detail_section("=== CLASSIC TA EVALUATION ===", [
            f"Volatility: {classic['volatility_regime']}",
            f"Trend: {classic['trend_regime']}   Quality: {classic['trend_quality']}",
            f"Swing Map: {classic['swing_map']}",
            f"Local Sequence: {classic['local_sequence']}   Pattern Context: {classic['pattern_context']}",
            f"Support / Resistance Context: {classic['support_resistance']}",
            f"Value Context: {latest.get('market_value_context') or '-'}   VWAP Bias: {latest.get('market_vwap_bias') or '-'}",
            "Reading note: Swing Structure is the broader pivot HH/HL or LH/LL map. Local Sequence is the immediate candle / multi-bar behavior and can break earlier.",
        ]))

        sections.append(detail_section("=== SWING STRUCTURE ===", [
            f"Swing Structure: {latest.get('market_structure_flag') or '-'}   Pass: {popup_yes_no(latest.get('market_structure_pass'))}",
            f"Swing Note: {latest.get('market_structure_note') or '-'}",
            f"Basis: {latest.get('market_structure_basis') or '-'}   Context: {latest.get('market_structure_context') or '-'}   Pivot Bias: {latest.get('market_structure_pivot_bias') or '-'}",
            f"HH / HL / LH / LL: {bool(latest.get('market_structure_swing_hh'))} / {bool(latest.get('market_structure_swing_hl'))} / {bool(latest.get('market_structure_swing_lh'))} / {bool(latest.get('market_structure_swing_ll'))}",
            f"Last Swing High / Low: {_fmt_num(latest.get('market_structure_last_high'), '.2f')} / {_fmt_num(latest.get('market_structure_last_low'), '.2f')}   Pivot Count: {_fmt_num(latest.get('market_structure_pivot_count'), '.0f')}",
            f"Boundary High / Low: {_fmt_num(latest.get('market_boundary_high'), '.2f')} / {_fmt_num(latest.get('market_boundary_low'), '.2f')}   Width ATR: {_fmt_num(latest.get('market_boundary_width_atr'), '.2f')}",
            f"Near Upper / Lower: {bool(latest.get('market_boundary_near_upper'))} / {bool(latest.get('market_boundary_near_lower'))}   Outside Above / Below: {bool(latest.get('market_boundary_outside_above'))} / {bool(latest.get('market_boundary_outside_below'))}",
            f"Boundary Note: {latest.get('of_boundary_note') or latest.get('market_boundary_note') or '-'}",
        ]))

        sections.append(detail_section("=== MICRO PRICE ACTION / VOLUME ===", [
            f"Micro PA (PA-Vol): {latest.get('market_pa_vol_flag') or '-'}   Pass: {popup_yes_no(latest.get('market_pa_vol_pass'))}   Pattern: {latest.get('market_pa_pattern') or '-'}",
            f"Setup Fuse (Strict): {latest.get('market_pa_strict_flag') or '-'}   Pass: {popup_yes_no(latest.get('market_pa_strict_pass'))}",
            f"Micro Note: {latest.get('market_pa_vol_note') or '-'}",
            f"Setup Note: {latest.get('market_pa_strict_note') or '-'}",
            f"Body / ClosePos / UpperWick / LowerWick: {_fmt_num(latest.get('market_pa_body_frac'), '.2f')} / {_fmt_num(latest.get('market_pa_close_frac'), '.2f')} / {_fmt_num(latest.get('market_pa_upper_wick_frac'), '.2f')} / {_fmt_num(latest.get('market_pa_lower_wick_frac'), '.2f')}",
            f"Range ATR / Vol Ratio / Quality: {_fmt_num(latest.get('market_pa_range_atr'), '.2f')} / {_fmt_num(latest.get('market_pa_volume_ratio'), '.2f')} / {_fmt_num(latest.get('market_pa_quality_score'), '.2f')}",
            f"Local HH / HL / LH / LL: {bool(latest.get('market_pa_higher_high'))} / {bool(latest.get('market_pa_higher_low'))} / {bool(latest.get('market_pa_lower_high'))} / {bool(latest.get('market_pa_lower_low'))}",
            f"Inside / Outside Bar: {bool(latest.get('market_pa_inside_bar'))} / {bool(latest.get('market_pa_outside_bar'))}",
            f"Engulf L/S: {bool(latest.get('market_pa_engulf_long'))} / {bool(latest.get('market_pa_engulf_short'))}   Retest Hold / Fail: {bool(latest.get('market_pa_retest_hold'))} / {bool(latest.get('market_pa_retest_fail'))}",
            f"Sequence L/S: {bool(latest.get('market_pa_sequence_long'))} / {bool(latest.get('market_pa_sequence_short'))}   Follow Pass / Fail: {bool(latest.get('market_pa_followthrough_pass'))} / {bool(latest.get('market_pa_followthrough_fail'))}",
            f"Stall After Impulse: {bool(latest.get('market_pa_stall_after_impulse'))}   Effort Result: {latest.get('market_pa_effort_result') or '-'}",
            f"Acceptance / SweepFail / ReversalRisk: {bool(latest.get('market_pa_acceptance_drive'))} / {bool(latest.get('market_pa_sweep_fail'))} / {bool(latest.get('market_pa_reversal_risk'))}",
            f"ConstructivePause / RejectionCluster / LadderFail: {bool(latest.get('market_pa_constructive_pause'))} / {bool(latest.get('market_pa_rejection_cluster'))} / {bool(latest.get('market_pa_ladder_fail'))}",
            f"SwingContinue / SeqExhaust: {bool(latest.get('market_pa_swing_continue'))} / {bool(latest.get('market_pa_sequence_exhaust'))}",
            f"Phase2 / Phase3 / Phase4 / Phase5 / Phase6: {latest.get('market_pa_phase2_context') or '-'} / {latest.get('market_pa_phase3_context') or '-'} / {latest.get('market_pa_phase4_context') or '-'} / {latest.get('market_pa_phase5_context') or '-'} / {latest.get('market_pa_phase6_context') or '-'}",
            f"Phase Notes: {latest.get('market_pa_phase2_note') or '-'} | {latest.get('market_pa_phase3_note') or '-'} | {latest.get('market_pa_phase4_note') or '-'} | {latest.get('market_pa_phase5_note') or '-'} | {latest.get('market_pa_phase6_note') or '-'}",
            f"Trap Flag: {latest.get('market_pa_trap_flag') or '-'}   Trap Note: {latest.get('market_pa_trap_note') or '-'}",
            f"Warning: {latest.get('market_warning_note') or '-'}",
        ]))

        sections.append(detail_section("=== BREAKOUT / TRIGGER STATUS ===", [
            f"Breakout State: {latest.get('market_breakout_pretty') or latest.get('market_breakout_state') or '-'}   Bias: {latest.get('market_breakout_bias') or '-'}   Score: {_fmt_num(latest.get('market_breakout_score'), '.0f', '%')}",
            f"Breakout Note: {latest.get('market_breakout_note') or '-'}",
            f"Breakout Style: {latest.get('market_breakout_style') or '-'}   Drive L/S: {_fmt_num(latest.get('market_breakout_drive_long'), '.2f')} / {_fmt_num(latest.get('market_breakout_drive_short'), '.2f')}",
            f"Sequence L/S: {bool(latest.get('market_breakout_sequence_long'))} / {bool(latest.get('market_breakout_sequence_short'))}   Spike L/S: {bool(latest.get('market_breakout_single_spike_long'))} / {bool(latest.get('market_breakout_single_spike_short'))}",
            f"Trigger Raw / Live: {popup_yes_no(latest.get('market_breakout_trigger_ready_raw'))} / {popup_yes_no(latest.get('market_breakout_trigger_ready'))}",
            f"Boundary: {latest.get('of_boundary_state_pretty') or latest.get('of_boundary_state') or '-'}   Touches Upper / Lower: {_fmt_num(latest.get('market_boundary_touch_count_upper'), '.0f')} / {_fmt_num(latest.get('market_boundary_touch_count_lower'), '.0f')}",
            f"Outside Hold L/S: {_fmt_num(latest.get('of_boundary_outside_hold_long'), '.0f')} / {_fmt_num(latest.get('of_boundary_outside_hold_short'), '.0f')}   Watch / Confirm Locks: {popup_yes_no(latest.get('of_watch_lock_active'))} / {popup_yes_no(latest.get('of_confirmation_lock_active'))}",
            f"Last Breakout Alert: {breakout_last.get('event_time', '-')}   Dir: {breakout_last.get('direction', '-')}   Price: {breakout_last.get('price', '-')}",
        ]))

        sections.append(detail_section("=== ORDER FLOW / EXECUTION ===", [
            f"Order Flow: {latest.get('of_state_pretty') or latest.get('of_state') or '-'}   Bias: {latest.get('of_bias') or '-'}   Score: {_fmt_num(latest.get('of_filter_score'), '.0f', '%')}",
            f"Gate: {latest.get('market_breakout_filter_gate') or '-'}   Pass: {popup_yes_no(latest.get('market_breakout_filter_pass') or latest.get('of_filter_pass'))}",
            f"OF Integration: {latest.get('of_integration_mode') or '-'}   Effect: {latest.get('of_integration_effect') or '-'}   Blocked: {popup_yes_no(latest.get('of_integration_blocked'))}",
            f"Order Flow Note: {latest.get('market_breakout_filter_note') or latest.get('of_filter_note') or latest.get('of_note') or '-'}",
            f"OF Risk: {latest.get('market_of_risk_flag') or '-'}   Pass: {popup_yes_no(latest.get('market_of_risk_pass'))}   Score: {_fmt_num(latest.get('market_of_risk_score'), '.0f', '%')}",
            f"OF Risk Note: {latest.get('market_of_risk_note') or '-'}",
            f"Intrabar Hostile L/S: {_fmt_num(latest.get('of_intrabar_hostile_long_hits'), '.0f')} / {_fmt_num(latest.get('of_intrabar_hostile_short_hits'), '.0f')}   Absorb L/S: {_fmt_num(latest.get('of_intrabar_absorption_long_hits'), '.0f')} / {_fmt_num(latest.get('of_intrabar_absorption_short_hits'), '.0f')}",
            f"Intrabar Flips / Blocks: {_fmt_num(latest.get('of_intrabar_flip_count'), '.0f')} / {_fmt_num(latest.get('of_intrabar_filter_block_hits'), '.0f')}",
            f"Integration Note: {latest.get('of_integration_note') or '-'}",
        ]))

        sections.append(detail_section("=== VALUE / VOLATILITY CONTEXT ===", [
            f"Value Context: {latest.get('market_value_context') or '-'}   Note: {latest.get('market_value_note') or '-'}",
            f"VWAP: {_fmt_num(latest.get('market_vwap'), '.2f')}   Dist ATR: {_fmt_num(latest.get('market_vwap_distance_atr'), '.2f')}   Bias: {latest.get('market_vwap_bias') or '-'}",
            f"VWAP Reclaim L/S: {bool(latest.get('market_vwap_reclaim_long'))} / {bool(latest.get('market_vwap_reclaim_short'))}   Reject L/S: {bool(latest.get('market_vwap_reject_long'))} / {bool(latest.get('market_vwap_reject_short'))}",
            f"BB Mid / Upper / Lower: {_fmt_num(latest.get('market_bb_middle'), '.2f')} / {_fmt_num(latest.get('market_bb_upper'), '.2f')} / {_fmt_num(latest.get('market_bb_lower'), '.2f')}",
            f"BB Inside KC: {bool(latest.get('market_bb_inside_kc'))}   Squeeze / Expand: {_fmt_num(latest.get('market_bb_squeeze_strength'), '.2f')} / {_fmt_num(latest.get('market_bb_expand_strength'), '.2f')}",
            f"BB / KC / Envelope Width ATR: {_fmt_num(latest.get('market_bb_width_atr'), '.2f')} / {_fmt_num(latest.get('market_kc_width_atr'), '.2f')} / {_fmt_num(latest.get('market_envelope_width_atr'), '.2f')}",
            f"Band Walk L/S: {bool(latest.get('market_bb_band_walk_long'))} / {bool(latest.get('market_bb_band_walk_short'))}",
            f"Containment: {_fmt_num(latest.get('market_containment_rate'), '.2f')}   Width ATR: {_fmt_num(latest.get('market_width_atr'), '.2f')}",
        ]))

        sections.append(detail_section("=== RAW SCORES / THRESHOLDS ===", [
            f"Direction Score: {_fmt_num(latest.get('market_direction_score'), '.0f')}   Energy Score: {_fmt_num(latest.get('market_energy_score'), '.0f')}   Alignment Bonus: {_fmt_num(latest.get('market_alignment_bonus'), '.0f')}",
            f"Compression / Transition / Trend: {_fmt_num(latest.get('market_compression_score'), '.0f')} / {_fmt_num(latest.get('market_transition_score'), '.0f')} / {_fmt_num(latest.get('market_trend_score'), '.0f')}",
            f"Pullback / Exhaustion: {_fmt_num(latest.get('market_pullback_score'), '.0f')} / {_fmt_num(latest.get('market_exhaustion_score'), '.0f')}",
            f"BreakoutQ: {_fmt_num(latest.get('market_breakout_quality'), '.2f')}   Breakout Confirmation: {_fmt_num(latest.get('market_breakout_confirmation'), '.2f')}   Pressure: {latest.get('market_pressure_bias') or '-'}",
            f"MACD Gap Norm: {_fmt_num(latest.get('market_macd_gap_norm'), '.3f')}",
            f"Thresholds -> AngleUp {th_map.get('vidya_angle_up_deg')}° | AngleDown {th_map.get('vidya_angle_down_deg')}° | Strong {th_map.get('vidya_angle_strong_deg')}°",
            f"Thresholds -> ΔVol support {th_map.get('vidya_delta_support_pct')}% | weak {th_map.get('vidya_delta_weak_pct')}%",
            f"Thresholds -> Dir LONG ≥ {th_map.get('direction_long_min')} | SHORT ≤ {th_map.get('direction_short_max')} | Compression |dir| ≤ {th_map.get('compression_direction_abs_max')}",
            f"Thresholds -> Compression MACD gap ≤ {th_map.get('compression_macd_gap_norm_max')} | Confidence base {th_map.get('confidence_base')}",
        ]))

        sections.append(detail_section("=== CONTRIBUTOR DETAILS ===", [
            str(latest.get('market_summary_long') or market_state_thesis(label)),
            market_state_playbook(label),
            f"VIDYA State: {latest.get('vidya_state') or '-'}   Slope: {_fmt_num(latest.get('vidya_slope'))}   Angle: {_fmt_num(latest.get('vidya_angle_deg_norm'), '.2f', '° norm')}",
            f"VIDYA Angle State / Accel: {latest.get('vidya_angle_state') or '-'} / {latest.get('vidya_angle_accel') or '-'}   ΔVol: {_fmt_num(latest.get('vidya_delta_pct'), '.2f', '%')}",
            f"FRAMA State: {latest.get('frama_state') or '-'}   Range State / Phase: {latest.get('range_filter_state') or '-'} / {latest.get('range_filter_phase') or '-'}",
            f"MACD Relation: {macd_rel}",
            f"MACD Momentum State / Angle: {latest.get('ms') or '-'} / {latest.get('angle') or '-'}",
            f"ATR / ATR Angle: {_fmt_num(latest.get('atr'))} / {latest.get('atr_angle') or '-'}",
        ]))

        sections.append(detail_section("=== QUICK RULE IDEAS ===", [
            f"- {tf} market_state = {label or '-'}",
            f"- {tf} market_bias = {bias if bias in ('LONG', 'SHORT', 'NEUTRAL') else '-'}",
            f"- {tf} market_phase = {phase if phase in ('EXPANSION', 'PULLBACK', 'EXHAUSTION', 'TRANSITION', 'COMPRESSION') else '-'}",
            f"- {tf} market_confidence >= {_fmt_num(latest.get('market_confidence'), '.0f')}",
        ]))

        return "\n\n".join(sections)

def build_orderflow_detail_text(app: Any, tf: str) -> str:
        latest = (app.latest.get(tf, {}) if hasattr(app, "latest") else {}) or {}

        def _fmt_num(v: Any, fmt: str = ".3f", suffix: str = "") -> str:
            try:
                fv = float(v)
                if np.isfinite(fv):
                    return f"{format(fv, fmt)}{suffix}"
            except Exception:
                pass
            return "-"

        dom_connected = bool(latest.get('of_dom_connected'))
        lines = [
            f"Timeframe: {tf}",
            f"Order Flow State: {latest.get('of_state_pretty') or latest.get('of_state') or '-'}",
            f"Bias: {latest.get('of_bias') or '-'}   Filter Pass: {'YES' if bool(latest.get('market_breakout_filter_pass') or latest.get('of_filter_pass')) else 'NO'}",
            f"Mode: {latest.get('of_feature_mode') or '-'}   DOM Source: {latest.get('of_dom_source') or '-'}",
            f"Integration: {latest.get('of_integration_mode') or '-'}   Effect: {latest.get('of_integration_effect') or '-'}   Blocked: {'YES' if bool(latest.get('of_integration_blocked')) else 'NO'}",
            f"Trigger Raw / Live: {'YES' if bool(latest.get('market_breakout_trigger_ready_raw')) else 'NO'} / {'YES' if bool(latest.get('market_breakout_trigger_ready')) else 'NO'}",
            f"OF Risk: {latest.get('market_of_risk_flag') or '-'}   Note: {latest.get('market_of_risk_note') or '-'}",
            f"Intrabar Hostile L/S: {_fmt_num(latest.get('of_intrabar_hostile_long_hits'), '.0f')} / {_fmt_num(latest.get('of_intrabar_hostile_short_hits'), '.0f')}   Flips: {_fmt_num(latest.get('of_intrabar_flip_count'), '.0f')}",
            f"Integration Note: {latest.get('of_integration_note') or '-'}",
            f"Data Quality: {latest.get('of_data_quality') or '-'}   Confidence: {_fmt_num(latest.get('of_data_confidence'), '.0f', '%')}",
            f"Event Count: {_fmt_num(latest.get('of_event_count'), '.0f')}   Last Event: {latest.get('of_last_event_type') or '-'}",
            f"Last Event Summary: {latest.get('of_last_event_summary') or '-'}",
            f"Watch Allowed / Confirm Allowed: {bool(latest.get('of_watch_allowed'))} / {bool(latest.get('of_confirmation_allowed'))}",
            f"Quality Note: {latest.get('of_quality_note') or '-'}",
            f"Window Short / Long: {_fmt_num(latest.get('of_window_short_sec'), '.0f', 's')} / {_fmt_num(latest.get('of_window_long_sec'), '.0f', 's')}",
            f"Tape Delta: {_fmt_num(latest.get('of_delta_pct'), '.2f', '%')}   Progress: {_fmt_num(latest.get('of_progress_bps'), '.2f', ' bps')}",
            f"Tape Age: {_fmt_num(latest.get('of_tape_age_ms'), '.0f', ' ms')}   Tape Quality: {latest.get('of_tape_quality') or '-'}",
            f"Volume Burst: {_fmt_num(latest.get('of_volume_burst'), '.2f')}   Volume Ratio: {_fmt_num(latest.get('of_volume_burst_ratio'), '.2f')}   Trade Ratio: {_fmt_num(latest.get('of_trade_burst_ratio'), '.2f')}",
            f"Trades/sec: {_fmt_num(latest.get('of_trade_rate'), '.2f')}   Count Short / Long: {_fmt_num(latest.get('of_trade_count_short'), '.0f')} / {_fmt_num(latest.get('of_trade_count_long'), '.0f')}",
            f"Buy Qty Short / Sell Qty Short: {_fmt_num(latest.get('of_buy_qty_short'), '.4f')} / {_fmt_num(latest.get('of_sell_qty_short'), '.4f')}",
            f"Tape Watch Long / Short: {bool(latest.get('of_tape_watch_long', latest.get('of_breakout_watch_long')))} / {bool(latest.get('of_tape_watch_short', latest.get('of_breakout_watch_short')))}",
            f"Tape Confirm Long / Short: {bool(latest.get('of_tape_confirm_long', latest.get('of_breakout_confirm_long')))} / {bool(latest.get('of_tape_confirm_short', latest.get('of_breakout_confirm_short')))}",
            f"Absorption Long / Short: {bool(latest.get('of_absorption_long'))} / {bool(latest.get('of_absorption_short'))}",
            f"OF Score: {_fmt_num(latest.get('of_filter_score'), '.0f', '%')}   Tape Score: {_fmt_num(latest.get('of_tape_filter_score'), '.0f', '%')}   DOM Score: {_fmt_num(latest.get('of_dom_filter_score'), '.0f', '%')}",
            f"Gate: {latest.get('market_breakout_filter_gate') or '-'}",
            f"Gate Note: {latest.get('market_breakout_filter_note') or latest.get('of_filter_note') or latest.get('of_note') or '-'}",
            f"Boundary State: {latest.get('of_boundary_state_pretty') or latest.get('of_boundary_state') or '-'}   Bias: {latest.get('of_boundary_bias') or '-'}",
            f"Boundary Valid / Source: {bool(latest.get('market_boundary_valid', latest.get('of_boundary_valid')))} / {latest.get('market_boundary_source') or '-'}",
            f"Boundary High / Low: {_fmt_num(latest.get('market_boundary_high'), '.4f')} / {_fmt_num(latest.get('market_boundary_low'), '.4f')}   Width ATR: {_fmt_num(latest.get('market_boundary_width_atr'), '.2f')}",
            f"Dist Upper / Lower ATR: {_fmt_num(latest.get('market_boundary_dist_to_upper_atr'), '.2f')} / {_fmt_num(latest.get('market_boundary_dist_to_lower_atr'), '.2f')}",
            f"Touches Upper / Lower: {_fmt_num(latest.get('market_boundary_touch_count_upper'), '.0f')} / {_fmt_num(latest.get('market_boundary_touch_count_lower'), '.0f')}   Threshold: {_fmt_num(latest.get('of_boundary_touch_threshold'), '.0f')}",
            f"Near Upper / Lower: {bool(latest.get('market_boundary_near_upper'))} / {bool(latest.get('market_boundary_near_lower'))}   Outside Above / Below: {bool(latest.get('market_boundary_outside_above'))} / {bool(latest.get('market_boundary_outside_below'))}",
            f"Boundary Watch L/S: {bool(latest.get('of_boundary_watch_long'))} / {bool(latest.get('of_boundary_watch_short'))}   Confirm L/S: {bool(latest.get('of_boundary_confirm_long'))} / {bool(latest.get('of_boundary_confirm_short'))}",
            f"Boundary Hold L/S: {_fmt_num(latest.get('of_boundary_outside_hold_long'), '.0f')} / {_fmt_num(latest.get('of_boundary_outside_hold_short'), '.0f')}   Failed L/S: {bool(latest.get('of_boundary_failed_long'))} / {bool(latest.get('of_boundary_failed_short'))}",
            f"Boundary Note: {latest.get('of_boundary_note') or latest.get('market_boundary_note') or '-'}",
            f"Watch Lock / Confirm Lock: {bool(latest.get('of_watch_lock_active'))} / {bool(latest.get('of_confirmation_lock_active'))}",
            f"Watch Lock Reason: {latest.get('of_watch_lock_reason') or '-'}",
            f"Confirm Lock Reason: {latest.get('of_confirmation_lock_reason') or '-'}",
            "",
            f"DOM Connected: {'YES' if dom_connected else 'NO'}   Sync: {latest.get('of_dom_sync_state') or '-'}   Resync Needed: {'YES' if bool(latest.get('of_dom_resync_required')) else 'NO'}",
            f"DOM Quality: {latest.get('of_dom_quality') or '-'}   DOM Confidence: {_fmt_num(latest.get('of_dom_confidence'), '.0f', '%')}",
            f"DOM State: {latest.get('of_dom_state_pretty') or latest.get('of_dom_state') or '-'}   Bias: {latest.get('of_dom_bias') or '-'}",
            f"DOM Pressure: {_fmt_num(latest.get('of_dom_pressure_pct'), '.2f', '%')}   Accel: {_fmt_num(latest.get('of_dom_pressure_accel'), '.2f', ' pt')}   Spread: {_fmt_num(latest.get('of_dom_spread_bps'), '.2f', ' bps')}",
            f"DOM Snapshot ID / Last u / Last pu: {_fmt_num(latest.get('of_dom_local_snapshot_id'), '.0f')} / {_fmt_num(latest.get('of_dom_local_last_event_u'), '.0f')} / {_fmt_num(latest.get('of_dom_local_last_event_pu'), '.0f')}",
            f"DOM Buffered / Applied Events: {_fmt_num(latest.get('of_dom_local_buffered_events'), '.0f')} / {_fmt_num(latest.get('of_dom_local_updates_applied'), '.0f')}",
            f"DOM Book Levels Bid / Ask: {_fmt_num(latest.get('of_dom_book_levels_bid'), '.0f')} / {_fmt_num(latest.get('of_dom_book_levels_ask'), '.0f')}",
            f"DOM Partial Fallback Enabled / Active: {bool(latest.get('of_dom_partial_fallback_enabled'))} / {bool(latest.get('of_dom_partial_fallback_active'))}",
            f"DOM Partial Connected / Updates: {bool(latest.get('of_dom_partial_connected'))} / {_fmt_num(latest.get('of_dom_partial_updates_total'), '.0f')}",
            f"DOM Depth Age / Snapshot Age: {_fmt_num(latest.get('of_dom_depth_age_ms'), '.0f', ' ms')} / {_fmt_num(latest.get('of_dom_snapshot_age_ms'), '.0f', ' ms')}",
            f"DOM Top5 / Top10 Imbalance: {_fmt_num(latest.get('of_dom_top5_imbalance_pct'), '.2f', '%')} / {_fmt_num(latest.get('of_dom_top10_imbalance_pct'), '.2f', '%')}",
            f"DOM Bid/Ask Stack Ratio: {_fmt_num(latest.get('of_dom_bid_stack_ratio'), '.2f')} / {_fmt_num(latest.get('of_dom_ask_stack_ratio'), '.2f')}",
            f"DOM Bid/Ask Pull Ratio: {_fmt_num(latest.get('of_dom_bid_pull_ratio'), '.2f')} / {_fmt_num(latest.get('of_dom_ask_pull_ratio'), '.2f')}",
            f"DOM Watch Long / Short: {bool(latest.get('of_dom_breakout_watch_long'))} / {bool(latest.get('of_dom_breakout_watch_short'))}",
            f"DOM Confirm Long / Short: {bool(latest.get('of_dom_breakout_confirm_long'))} / {bool(latest.get('of_dom_breakout_confirm_short'))}",
            f"DOM Updates Short / Long: {_fmt_num(latest.get('of_dom_updates_short'), '.0f')} / {_fmt_num(latest.get('of_dom_updates_long'), '.0f')}",
            f"Best Bid / Ask / Mid: {_fmt_num(latest.get('of_dom_best_bid'), '.4f')} / {_fmt_num(latest.get('of_dom_best_ask'), '.4f')} / {_fmt_num(latest.get('of_dom_mid'), '.4f')}",
            f"DOM Note: {latest.get('of_dom_note') or 'DOM not connected yet'}",
            "",
            "Interpretation:",
            str(latest.get('of_note') or 'Tape/order-flow microstructure is not available yet.'),
        ]
        return "\n".join(lines)

def build_orderflow_trail_text(app: Any, tf: str) -> str:
        tf = str(tf)
        latest = (app.latest.get(tf, {}) if hasattr(app, "latest") else {}) or {}
        engine = getattr(app, "live_engine", None)
        rows: List[Dict[str, Any]] = []
        try:
            if engine is not None and hasattr(engine, "get_orderflow_event_history"):
                rows = list(engine.get_orderflow_event_history(tf, limit=80) or [])
        except Exception:
            rows = []

        lines = [
            f"Timeframe: {tf}",
            f"Current OF State: {latest.get('of_state_pretty') or latest.get('of_state') or '-'}",
            f"Current Boundary: {latest.get('of_boundary_state_pretty') or latest.get('of_boundary_state') or '-'}",
            f"Current Gate: {latest.get('market_breakout_filter_gate') or '-'}   Pass: {'YES' if bool(latest.get('market_breakout_filter_pass')) else 'NO'}",
            f"Current Note: {latest.get('market_breakout_filter_note') or latest.get('of_filter_note') or latest.get('of_note') or '-'}",
            "",
            f"Trail Size: {len(rows)}",
            "Recent Event Trail (newest first):",
        ]
        if not rows:
            lines.append("- no replay trail has been recorded yet")
            return "\n".join(lines)

        for evt in reversed(rows):
            try:
                ts_ms = int(evt.get('ts_ms') or 0)
                ts_txt = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(ts_ms / 1000.0)) if ts_ms > 0 else '-'
            except Exception:
                ts_txt = '-'
            seq = evt.get('seq') or '-'
            event_type = evt.get('event_type') or '-'
            summary = evt.get('summary') or '-'
            seen_count = int(evt.get('seen_count') or 1)
            delta_txt = '-'
            prog_txt = '-'
            dom_txt = '-'
            try:
                delta_txt = f"{float(evt.get('delta_pct') or 0.0):.2f}%"
            except Exception:
                pass
            try:
                prog_txt = f"{float(evt.get('progress_bps') or 0.0):.2f}bps"
            except Exception:
                pass
            try:
                dom_txt = f"{float(evt.get('dom_pressure_pct') or 0.0):.2f}%"
            except Exception:
                pass
            lines.append(f"[{seq}] {ts_txt} | {event_type} | seen {seen_count}x")
            lines.append(f"  {summary}")
            lines.append(f"  Δ {delta_txt}   Progress {prog_txt}   DOM {dom_txt}")
            lines.append(f"  Gate {evt.get('filter_gate') or '-'}   Pass {bool(evt.get('filter_pass'))}   Quality {evt.get('of_data_quality') or '-'}")
            reason = str(evt.get('reason') or evt.get('of_filter_note') or evt.get('of_boundary_note') or '').strip()
            if reason:
                lines.append(f"  Why: {reason}")
            lines.append("")
        return "\n".join(lines)
