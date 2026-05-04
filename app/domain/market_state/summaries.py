from __future__ import annotations

"""Summary-building helpers for the staged market-state pipeline."""

from typing import Any, Callable, Mapping, Tuple


def build_basic_market_summaries(label: str, pa_overlay: Mapping[str, Any]) -> Tuple[str, str, str]:
    default_summary = {
        'BULL_EXPANSION': 'aligned slope / supportive delta / continuation structure',
        'BULL_PULLBACK': 'bullish bias intact / controlled reset / wait re-drive',
        'BULL_EXHAUSTION': 'bullish stretch fading / momentum cooling / risk of stall',
        'BEAR_EXPANSION': 'aligned slope / supportive delta / downside continuation',
        'BEAR_PULLBACK': 'bearish bias intact / controlled reset / wait re-drive',
        'BEAR_EXHAUSTION': 'bearish stretch fading / momentum cooling / risk of stall',
        'TRANSITION': 'mixed direction / structure reorganising / confirmation pending',
        'COMPRESSION': 'tight range / flat pressure / breakout quality poor',
    }
    summary_short = default_summary.get(label, 'market state active')
    summary_long = default_summary.get(label, 'market state active')
    if str(pa_overlay.get('market_structure_flag') or '') == 'BROKEN':
        summary_short = f'{summary_short} / structure broken'
        summary_long = f'{summary_long} Structure broke versus the prior bar.'
    elif str(pa_overlay.get('market_pa_vol_flag') or '') == 'NO_PASS':
        summary_short = f'{summary_short} / candle-volume no-pass'
        summary_long = f'{summary_long} Candle/volume hard-check did not pass.'
    texture = 'BASIC_DIRECTIONAL' if label.startswith(('BULL_', 'BEAR_')) else ('BASIC_COMPRESSION' if label == 'COMPRESSION' else 'BASIC_TRANSITION')
    return texture, summary_short, summary_long


def append_note(summary_long: str, note: str) -> str:
    note = str(note or '').strip()
    if not note:
        return summary_long
    if note.lower() in str(summary_long).lower():
        return summary_long
    return f"{summary_long} {note[:1].upper() + note[1:]}."


def append_short_once(summary_short: str, token: str) -> str:
    token = str(token or '').strip()
    if not token:
        return summary_short
    if token.lower() in str(summary_short).lower():
        return summary_short
    return f'{summary_short} / {token}'


def build_dynamic_market_summaries(
    label: str,
    snapshot: Mapping[str, Any],
    bias: str,
    pa_overlay: Mapping[str, Any],
    value_overlay: Mapping[str, Any],
    breakout_ctx: Mapping[str, Any],
    base_summary_builder: Callable[[str, Mapping[str, Any], str], Tuple[str, str, str]],
) -> Tuple[str, str, str]:
    texture, summary_short, summary_long = base_summary_builder(label, snapshot, bias)
    breakout_pretty = str(breakout_ctx.get('market_breakout_pretty') or '-')
    breakout_note = str(breakout_ctx.get('market_breakout_note') or '').strip()
    breakout_state = str(breakout_ctx.get('market_breakout_state') or 'NONE').upper().strip() or 'NONE'
    if breakout_state != 'NONE':
        if summary_short:
            summary_short = f'{summary_short} / {breakout_pretty.lower()}'
        summary_long = append_note(summary_long, breakout_note)

    phase2_context = str(pa_overlay.get('market_pa_phase2_context') or 'NEUTRAL').upper().strip() or 'NEUTRAL'
    phase2_note = str(pa_overlay.get('market_pa_phase2_note') or '').strip()
    if phase2_context in ('LONG_RETEST_HOLD', 'SHORT_RETEST_HOLD'):
        summary_short = append_short_once(summary_short, 'retest holding')
        summary_long = append_note(summary_long, phase2_note)
    elif phase2_context in ('LONG_RETEST_FAIL', 'SHORT_RETEST_FAIL', 'LONG_EFFORT_NO_RESULT', 'SHORT_EFFORT_NO_RESULT', 'LONG_CLIMAX', 'SHORT_CLIMAX', 'CLIMAX'):
        token = 'retest fail' if 'RETEST_FAIL' in phase2_context else 'effort-result warning'
        if 'effort-result warning' not in summary_short.lower() and 'retest fail' not in summary_short.lower():
            summary_short = f'{summary_short} / {token}'
        summary_long = append_note(summary_long, phase2_note)
    elif phase2_context in ('LONG_CONTINUATION', 'SHORT_CONTINUATION'):
        summary_long = append_note(summary_long, phase2_note)

    phase3_context = str(pa_overlay.get('market_pa_phase3_context') or 'NEUTRAL').upper().strip() or 'NEUTRAL'
    phase3_note = str(pa_overlay.get('market_pa_phase3_note') or '').strip()
    if phase3_context in ('LONG_SEQUENCE', 'SHORT_SEQUENCE', 'LONG_FOLLOWTHROUGH_PASS', 'SHORT_FOLLOWTHROUGH_PASS'):
        if 'sequence' not in summary_short.lower() and 'follow-through' not in summary_short.lower():
            summary_short = f'{summary_short} / micro-structure confirming'
        summary_long = append_note(summary_long, phase3_note)
    elif phase3_context in ('LONG_FOLLOWTHROUGH_FAIL', 'SHORT_FOLLOWTHROUGH_FAIL', 'LONG_STALL_AFTER_IMPULSE', 'SHORT_STALL_AFTER_IMPULSE', 'FOLLOWTHROUGH_FAIL'):
        summary_short = append_short_once(summary_short, 'follow-through weak')
        summary_long = append_note(summary_long, phase3_note)

    phase4_context = str(pa_overlay.get('market_pa_phase4_context') or 'NEUTRAL').upper().strip() or 'NEUTRAL'
    phase4_note = str(pa_overlay.get('market_pa_phase4_note') or '').strip()
    if phase4_context in ('LONG_ACCEPTANCE', 'SHORT_ACCEPTANCE'):
        summary_short = append_short_once(summary_short, 'acceptance')
        summary_long = append_note(summary_long, phase4_note)
    elif phase4_context in ('LONG_SWEEP_FAIL', 'SHORT_SWEEP_FAIL', 'LONG_REVERSAL_RISK', 'SHORT_REVERSAL_RISK'):
        summary_short = append_short_once(summary_short, 'trap-reversal risk')
        summary_long = append_note(summary_long, phase4_note)

    phase5_context = str(pa_overlay.get('market_pa_phase5_context') or 'NEUTRAL').upper().strip() or 'NEUTRAL'
    phase5_note = str(pa_overlay.get('market_pa_phase5_note') or '').strip()
    if phase5_context in ('LONG_CONSTRUCTIVE_PAUSE', 'SHORT_CONSTRUCTIVE_PAUSE'):
        summary_short = append_short_once(summary_short, 'constructive pause')
        summary_long = append_note(summary_long, phase5_note)
    elif phase5_context in ('LONG_REJECTION_CLUSTER', 'SHORT_REJECTION_CLUSTER', 'LONG_LADDER_FAIL', 'SHORT_LADDER_FAIL'):
        summary_short = append_short_once(summary_short, 'micro warning')
        summary_long = append_note(summary_long, phase5_note)

    phase6_context = str(pa_overlay.get('market_pa_phase6_context') or 'NEUTRAL').upper().strip() or 'NEUTRAL'
    phase6_note = str(pa_overlay.get('market_pa_phase6_note') or '').strip()
    if phase6_context in ('LONG_SWING_CONTINUE', 'SHORT_SWING_CONTINUE'):
        summary_short = append_short_once(summary_short, 'swing structure')
        summary_long = append_note(summary_long, phase6_note)
    elif phase6_context in ('LONG_SEQUENCE_EXHAUST', 'SHORT_SEQUENCE_EXHAUST', 'LONG_SWING_BREAK', 'SHORT_SWING_BREAK'):
        summary_short = append_short_once(summary_short, 'swing warning')
        summary_long = append_note(summary_long, phase6_note)

    if str(pa_overlay.get('market_structure_flag') or '') == 'BROKEN':
        summary_short = append_short_once(summary_short, 'structure broken')
        summary_long = append_note(summary_long, str(pa_overlay.get('market_structure_note') or '').strip())
    elif str(pa_overlay.get('market_pa_vol_flag') or '') == 'NO_PASS':
        summary_short = append_short_once(summary_short, 'candle-volume no-pass')
        summary_long = append_note(summary_long, str(pa_overlay.get('market_pa_vol_note') or '').strip())

    if str(pa_overlay.get('market_pa_strict_flag') or '') == 'NO_PASS':
        summary_short = append_short_once(summary_short, 'strict fuse no-pass')
        summary_long = append_note(summary_long, str(pa_overlay.get('market_pa_strict_note') or '').strip())
    elif str(pa_overlay.get('market_pa_strict_flag') or '') == 'PASS':
        summary_long = append_note(summary_long, str(pa_overlay.get('market_pa_strict_note') or '').strip())

    value_note = str(value_overlay.get('market_value_note') or '').strip()
    if value_note:
        summary_long = append_note(summary_long, value_note)

    return texture, summary_short, summary_long
