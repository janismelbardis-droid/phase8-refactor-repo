from __future__ import annotations

"""Human-readable preset explainer / storyboard generator.

This module turns the raw preset JSON into a compact narrative that explains
what the strategy is trying to do, what kind of market shape it assumes, and
what a typical trade should look like.
"""

from typing import Any, Dict, List, Optional, Tuple

import numpy as np
import pandas as pd

from .indicators_streaming import coerce_market_state_thresholds, market_state_thresholds_dict


RULE_TAB_ORDER = ("Long Entry", "Long Exit", "Short Entry", "Short Exit")

_FIELD_LABELS = {
    "ms": "MACD/signal color",
    "angle": "MACD angle",
    "sig_angle": "Signal angle",
    "ppo": "PPO dot",
    "flip_age": "PPO flip age",
    "macd": "MACD",
    "signal": "Signal",
    "ms_cross": "MACD × Signal",
    "macd0_cross": "MACD × 0",
    "adx": "ADX",
    "atr": "ATR",
    "di_plus": "+DI",
    "di_minus": "-DI",
    "di_spread": "DI spread",
    "adx_angle": "ADX angle",
    "atr_angle": "ATR angle",
    "di_cross": "DI cross",
    "frama_state": "FRAMA state",
    "frama_break_up": "FRAMA breakout up",
    "frama_break_down": "FRAMA breakout down",
    "frama_mid_cross": "FRAMA mid cross",
    "vidya_state": "VIDYA state",
    "vidya_line": "VIDYA line",
    "vidya_raw": "VIDYA raw",
    "vidya_upper": "VIDYA upper band",
    "vidya_lower": "VIDYA lower band",
    "vidya_delta_pct": "VIDYA delta volume %",
    "vidya_slope": "VIDYA slope",
    "vidya_angle_deg": "VIDYA angle",
    "vidya_angle_deg_norm": "VIDYA angle (norm)",
    "vidya_angle_state": "VIDYA angle state",
    "vidya_angle_accel": "VIDYA angle acceleration",
    "vidya_trend_up": "VIDYA trend up",
    "vidya_trend_down": "VIDYA trend down",
    "range_filter_state": "Range Filter state",
    "range_filter_phase": "Range Filter phase",
    "range_filter_line": "Range Filter line",
    "range_filter_upper": "Range Filter upper band",
    "range_filter_lower": "Range Filter lower band",
    "range_filter_buy": "Range Filter buy event",
    "range_filter_sell": "Range Filter sell event",
    "market_state": "Market state",
    "market_bias": "Market bias",
    "market_phase": "Market phase",
    "market_confidence": "Market confidence",
    "market_state_age": "Market state age",
}

_STATE_PRETTY = {
    "BULL_EXPANSION": "Bull Expansion",
    "BULL_PULLBACK": "Bull Pullback",
    "BULL_EXHAUSTION": "Bull Exhaustion",
    "BEAR_EXPANSION": "Bear Expansion",
    "BEAR_PULLBACK": "Bear Pullback",
    "BEAR_EXHAUSTION": "Bear Exhaustion",
    "TRANSITION": "Transition",
    "COMPRESSION": "Compression",
    "LONG": "Long",
    "SHORT": "Short",
    "NEUTRAL": "Neutral",
    "EXPANSION": "Expansion",
    "PULLBACK": "Pullback",
    "EXHAUSTION": "Exhaustion",
    "ACCEL": "Accelerating",
    "DECEL": "Decelerating",
    "STABLE": "Stable",
    "UP": "Up",
    "DOWN": "Down",
    "FLAT": "Flat",
    "BUY": "Bullish",
    "SELL": "Bearish",
    "GREEN": "Green",
    "RED": "Red",
}




_TF_ORDER = {
    "1m": 1,
    "3m": 3,
    "5m": 5,
    "15m": 15,
    "30m": 30,
    "1h": 60,
    "2h": 120,
    "4h": 240,
    "1d": 1440,
}



STATE_DIAG_LABELS = [
    "BULL_EXPANSION",
    "BULL_PULLBACK",
    "BULL_EXHAUSTION",
    "BEAR_EXPANSION",
    "BEAR_PULLBACK",
    "BEAR_EXHAUSTION",
    "TRANSITION",
    "COMPRESSION",
    "UNKNOWN",
]

def _trade_attr(tr: Any, name: str, default: Any = None) -> Any:
    try:
        return getattr(tr, name)
    except Exception:
        return default


def _fmt_num(v: Any, digits: int = 2, suffix: str = "") -> str:
    try:
        f = float(v)
        if abs(f) >= 100:
            return f"{f:.0f}{suffix}"
        return f"{f:.{digits}f}{suffix}"
    except Exception:
        return "—"


def _fmt_ts(v: Any) -> str:
    if v is None:
        return "—"
    try:
        s = str(v)
        return s.replace("T", " ").replace("+00:00", " UTC")
    except Exception:
        return str(v)


def _ordered_tfs(snapshots: Dict[str, Any]) -> List[str]:
    keys = [str(k) for k in (snapshots or {}).keys()]
    return sorted(keys, key=lambda k: (_TF_ORDER.get(k, 10_000), k))


def _snapshot_state_line(tf: str, snap: Dict[str, Any]) -> str:
    state = _pretty_value((snap or {}).get("market_state"))
    bias = _pretty_value((snap or {}).get("market_bias"))
    phase = _pretty_value((snap or {}).get("market_phase"))
    conf = (snap or {}).get("market_confidence")
    age = (snap or {}).get("market_state_age")
    vidya_angle = (snap or {}).get("vidya_angle_deg_norm")
    vidya_delta = (snap or {}).get("vidya_delta_pct")
    return (
        f"{tf}: {state} | {bias} | {phase} | conf {_fmt_num(conf, 0, '%')} | "
        f"age {_fmt_num(age, 0)} | angle {_fmt_num(vidya_angle, 2, '°')} | Δvol {_fmt_num(vidya_delta, 1, '%')}"
    )


def _trace_hint(trace: Any) -> str:
    txt = str(trace or "").strip()
    if not txt:
        return "—"
    lines = [ln.strip() for ln in txt.splitlines() if str(ln).strip()]
    trigger = next((ln for ln in lines if "[TRIGGER]" in ln), None)
    if trigger:
        return trigger
    if len(lines) >= 2:
        return lines[1]
    return lines[0]


def _score_trade_for_archetype(tr: Any, archetype: str) -> float:
    archetype_up = str(archetype or "").upper()
    entry_snapshot = _trade_attr(tr, "entry_snapshot", {}) or {}
    states = [str((entry_snapshot.get(tf) or {}).get("market_state") or "").upper() for tf in entry_snapshot]
    phases = [str((entry_snapshot.get(tf) or {}).get("market_phase") or "").upper() for tf in entry_snapshot]
    age_vals = []
    for tf in entry_snapshot:
        try:
            age_vals.append(float((entry_snapshot.get(tf) or {}).get("market_state_age")))
        except Exception:
            pass
    pnl = 0.0
    try:
        pnl = float(_trade_attr(tr, "net_pnl", 0.0) or 0.0)
    except Exception:
        pnl = 0.0
    score = 0.0
    if "PULLBACK" in archetype_up:
        score += 6.0 if any("PULLBACK" in x for x in states + phases) else 0.0
        score += 1.5 if pnl > 0 else 0.0
    elif "BREAKOUT" in archetype_up:
        score += 6.0 if any("EXPANSION" in x for x in states + phases) else 0.0
        score += 1.0 if pnl > 0 else 0.0
    elif "REGIME SHIFT" in archetype_up:
        trace = str(_trade_attr(tr, "entry_rule_trace", "") or "").upper()
        score += 4.0 if ("TURNED" in trace or "FLIPPED" in trace) else 0.0
        score += 2.0 if any(a <= 2 for a in age_vals) else 0.0
        score += 1.0 if pnl > 0 else 0.0
    else:
        score += 2.0 if pnl > 0 else 0.0
        score += 1.0 if entry_snapshot else 0.0
    score += min(2.0, abs(pnl) / 50.0)
    return float(score)


def _pick_trade_examples(bt_result: Any, archetype: str, limit: int = 3) -> List[Any]:
    trades = list(getattr(bt_result, "trades", []) or [])
    if not trades:
        return []
    ranked = sorted(
        trades,
        key=lambda tr: (_score_trade_for_archetype(tr, archetype), float(_trade_attr(tr, "net_pnl", 0.0) or 0.0), abs(float(_trade_attr(tr, "return_pct", 0.0) or 0.0))),
        reverse=True,
    )
    chosen: List[Any] = []
    seen: set = set()

    def _append_first(pred):
        for tr in ranked:
            tid = int(_trade_attr(tr, "trade_id", -1) or -1)
            if tid in seen:
                continue
            if pred(tr):
                chosen.append(tr)
                seen.add(tid)
                return True
        return False

    _append_first(lambda tr: float(_trade_attr(tr, "net_pnl", 0.0) or 0.0) > 0)
    _append_first(lambda tr: float(_trade_attr(tr, "net_pnl", 0.0) or 0.0) <= 0)
    for tr in ranked:
        if len(chosen) >= max(1, int(limit)):
            break
        tid = int(_trade_attr(tr, "trade_id", -1) or -1)
        if tid in seen:
            continue
        chosen.append(tr)
        seen.add(tid)
    return chosen[: max(1, int(limit))]


def _trade_example_lines(tr: Any) -> List[str]:
    side = str(_trade_attr(tr, "side", "") or "-")
    tid = _trade_attr(tr, "trade_id", "-")
    pnl = _trade_attr(tr, "net_pnl", None)
    ret = _trade_attr(tr, "return_pct", None)
    dur = _trade_attr(tr, "duration_min", None)
    lines = [
        f"Trade #{tid} | {side} | net {_fmt_num(pnl, 2)} | return {_fmt_num(ret, 2, '%')} | duration {_fmt_num(dur, 0, ' min')}",
        f"Entry: {_fmt_ts(_trade_attr(tr, 'entry_fill_time', None) or _trade_attr(tr, 'entry_time', None))} | {_fmt_num(_trade_attr(tr, 'entry_price', None), 4)} | reason: {str(_trade_attr(tr, 'entry_reason', '') or '—')}",
        f"Exit : {_fmt_ts(_trade_attr(tr, 'exit_fill_time', None) or _trade_attr(tr, 'exit_time', None))} | {_fmt_num(_trade_attr(tr, 'exit_price', None), 4)} | reason: {str(_trade_attr(tr, 'exit_reason', '') or '—')}",
    ]
    entry_snapshot = _trade_attr(tr, "entry_snapshot", {}) or {}
    if entry_snapshot:
        lines.append("Entry state by timeframe:")
        for tf in _ordered_tfs(entry_snapshot):
            snap = entry_snapshot.get(tf) or {}
            if not isinstance(snap, dict):
                continue
            lines.append(f"  - {_snapshot_state_line(tf, snap)}")
    hint = _trace_hint(_trade_attr(tr, "entry_rule_trace", ""))
    if hint != "—":
        lines.append(f"Trace hint: {hint}")
    return lines


def _append_real_examples(lines: List[str], bt_result: Any, archetype: str) -> None:
    trades = list(getattr(bt_result, "trades", []) or [])
    lines.append("Real Backtest Examples")
    lines.append("----------------------")
    if not trades:
        lines.append("No current backtest trades loaded.")
        lines.append("")
        return
    lines.append(f"Using the currently loaded backtest: {len(trades)} trade(s) available.")
    for idx, tr in enumerate(_pick_trade_examples(bt_result, archetype, limit=3), start=1):
        lines.append("")
        lines.append(f"Example {idx}")
        lines.append(f"~~~~~~~~~")
        lines.extend(_trade_example_lines(tr))
    lines.append("")

def _pretty_value(v: Any) -> str:
    if v is None:
        return "—"
    if isinstance(v, float):
        if abs(v) >= 100:
            return f"{v:.0f}"
        if abs(v) >= 10:
            return f"{v:.1f}"
        return f"{v:.2f}"
    s = str(v)
    return _STATE_PRETTY.get(s, _STATE_PRETTY.get(s.upper(), s.replace("_", " ").title()))


def _field_label(field: str) -> str:
    return _FIELD_LABELS.get(field, field.replace("_", " ").title())


def _rule_phrase(rule: Dict[str, Any]) -> str:
    tf = str(rule.get("timeframe") or "?")
    mode = str(rule.get("mode") or "state").lower()
    field = str(rule.get("field") or "")
    op = str(rule.get("op") or "=")
    value = rule.get("value")
    lbl = _field_label(field)
    prefix = "Trigger: " if bool(rule.get("is_trigger", False)) and mode == "event" else ""

    if mode == "event":
        if op == "TURNED":
            txt = f"{tf} {lbl} turned {_pretty_value(value)}"
        elif op == "FLIPPED":
            txt = f"{tf} {lbl} flipped {_pretty_value(value)}"
        elif op == "CROSS":
            txt = f"{tf} {lbl} crossed {_pretty_value(value)}"
        else:
            txt = f"{tf} {lbl} {op} {_pretty_value(value)}"
    else:
        op_txt = {"=": "is", "!=": "is not", ">=": "≥", "<=": "≤", ">": ">", "<": "<"}.get(op, op)
        txt = f"{tf} {lbl} {op_txt} {_pretty_value(value)}"

    bars_mode = str(rule.get("bars_ago_mode") or "OFF").upper().strip()
    try:
        bars_n = int(rule.get("bars_ago_n") or 0)
    except Exception:
        bars_n = 0
    if bars_mode == "EXACT":
        txt += f" [exactly {bars_n} bar{'s' if bars_n != 1 else ''} ago]"
    elif bars_mode == "WITHIN":
        txt += f" [within {bars_n} bar{'s' if bars_n != 1 else ''}]"
    if bool(rule.get("require_still_valid", False)) and bars_mode != "OFF":
        txt += " and still valid now"
    return prefix + txt


def _join_phrases(parts: List[str], join_mode: str) -> str:
    parts = [p for p in parts if p]
    if not parts:
        return "—"
    if len(parts) == 1:
        return parts[0]
    connector = f" {join_mode.upper()} "
    return connector.join(parts)


def _tab_rules(preset: Dict[str, Any], tab: str) -> List[Dict[str, Any]]:
    tabs = preset.get("tabs", {}) if isinstance(preset.get("tabs", {}), dict) else {}
    spec = tabs.get(tab, {}) if isinstance(tabs.get(tab, {}), dict) else {}
    groups = spec.get("groups", []) if isinstance(spec.get("groups", []), list) else []
    out: List[Dict[str, Any]] = []
    for g in groups:
        if isinstance(g, dict):
            for r in (g.get("rules", []) if isinstance(g.get("rules", []), list) else []):
                if isinstance(r, dict):
                    out.append(r)
    return out


def infer_preset_archetype(preset: Dict[str, Any]) -> Tuple[str, str, str]:
    long_rules = _tab_rules(preset, "Long Entry")
    short_rules = _tab_rules(preset, "Short Entry")
    all_rules = long_rules + short_rules
    fields = {str(r.get("field") or "") for r in all_rules}
    values = {str(r.get("value") or "") for r in all_rules}
    ops = {str(r.get("op") or "") for r in all_rules}

    has_pullback = any("PULLBACK" in str(v).upper() for v in values) or "market_phase" in fields
    has_expansion = any("EXPANSION" in str(v).upper() for v in values)
    has_turn = any(str(r.get("op") or "") in ("TURNED", "FLIPPED") for r in all_rules)
    has_cross = any(str(r.get("op") or "") == "CROSS" for r in all_rules)
    uses_market_state = "market_state" in fields or "market_phase" in fields or "market_bias" in fields
    uses_vidya = any(f.startswith("vidya") for f in fields)
    uses_frama = any(f.startswith("frama") for f in fields)
    uses_range = any(f.startswith("range_filter") for f in fields)

    if has_pullback and uses_vidya and has_cross:
        return (
            "Pullback Continuation",
            "Wait for structure to stay intact during a pullback, then enter when momentum re-confirms.",
            "This preset is built to avoid fake first impulses by buying or selling the first controlled retrace after alignment.",
        )
    if has_expansion and has_cross:
        return (
            "Breakout Continuation",
            "Enter when trend filters and momentum agree during active expansion.",
            "This preset is built to catch strong runners that may not offer a comfortable retrace.",
        )
    if has_turn and uses_vidya and (uses_frama or uses_range):
        return (
            "Regime Shift Confirmation",
            "Treat a fresh trend turn as a setup start, then require extra confirmation before acting.",
            "This preset is built around early regime change, with extra conditions intended to filter false flips.",
        )
    if uses_market_state:
        return (
            "State-Gated Strategy",
            "Only trade when the higher-level market state permits the setup.",
            "This preset relies on the market-state engine to simplify multi-indicator context into a tradable regime.",
        )
    if uses_vidya or uses_frama or uses_range:
        return (
            "Trend-Filtered Momentum",
            "Use trend structure filters first, then let momentum decide timing.",
            "This preset aims to keep direction and timing separate rather than using one raw signal for everything.",
        )
    return (
        "Custom Composite Logic",
        "A mixed preset combining multiple state and event filters.",
        "This preset is custom enough that the safest reading is via its concrete entry and exit conditions below.",
    )


def _visual_block(archetype: str) -> Tuple[str, List[str], List[str]]:
    a = archetype.upper()
    if "PULLBACK" in a:
        price = "Price : ▁▂▅▇▆▅▆█"
        model = [
            "1) Regime or trend alignment appears.",
            "2) Price pulls back while structure stays intact.",
            "3) Momentum stabilizes and turns back in the trend direction.",
            "4) Entry triggers on re-expansion.",
            "5) Best case: continuation follows from a better price.",
        ]
        fail = [
            "Failure path: initial turn appears, but pullback becomes too deep.",
            "Momentum never re-confirms, or trend filters flip back.",
            "Setup should cancel before entry rather than forcing a trade.",
        ]
        return price, model, fail
    if "BREAKOUT" in a:
        price = "Price : ▁▂▄▆▇▇█▉"
        model = [
            "1) Trend filters align quickly.",
            "2) Momentum expands immediately.",
            "3) Entry triggers without waiting for a full pullback.",
            "4) Goal is to capture runaway moves that rarely retrace cleanly.",
        ]
        fail = [
            "Failure path: breakout stalls quickly.",
            "The move loses participation and turns into a failed expansion.",
        ]
        return price, model, fail
    if "REGIME SHIFT" in a:
        price = "Price : ▂▁▂▄▅▄▅▇"
        model = [
            "1) A fresh directional turn appears.",
            "2) The setup is armed, not traded immediately.",
            "3) Confirmation filters decide whether the new regime is real.",
            "4) Entry comes only after the market proves the turn can survive.",
        ]
        fail = [
            "Failure path: fast flip back after the first turn.",
            "The filter layer should stop the strategy from chasing the false turn.",
        ]
        return price, model, fail
    price = "Price : ▁▃▄▅▄▃▅▆"
    model = [
        "1) Context filter defines where the strategy is allowed to act.",
        "2) A trigger or confirmation then decides exact timing.",
    ]
    fail = [
        "Failure path: filters disagree or momentum never confirms.",
    ]
    return price, model, fail



def _state_key(v: Any) -> str:
    s = str(v or '').strip().upper()
    return s if s in STATE_DIAG_LABELS else 'UNKNOWN'


def _phase_key(v: Any) -> str:
    s = str(v or '').strip().upper()
    return s if s in ('EXPANSION', 'PULLBACK', 'EXHAUSTION', 'TRANSITION', 'COMPRESSION') else 'UNKNOWN'


def _bucket_name(net_pnl: Any) -> str:
    try:
        x = float(net_pnl or 0.0)
    except Exception:
        return 'FLAT'
    if x > 1e-12:
        return 'WIN'
    if x < -1e-12:
        return 'LOSS'
    return 'FLAT'


def _gather_trade_timeframes(bt_result: Any) -> List[str]:
    out = set()
    for tr in list(getattr(bt_result, 'trades', []) or []):
        for attr in ('entry_snapshot', 'exit_snapshot'):
            snap_map = _trade_attr(tr, attr, {}) or {}
            if isinstance(snap_map, dict):
                out.update(str(k) for k in snap_map.keys())
    return sorted(out, key=lambda k: (_TF_ORDER.get(k, 10_000), k))


def _trade_snapshot(tr: Any, attr: str, tf: str) -> Dict[str, Any]:
    mp = _trade_attr(tr, attr, {}) or {}
    if not isinstance(mp, dict):
        return {}
    snap = mp.get(tf) or {}
    return snap if isinstance(snap, dict) else {}


def _fmt_pct_from_fraction(v: Any, digits: int = 1) -> str:
    try:
        f = float(v)
        return f"{(100.0 * f):.{digits}f}%"
    except Exception:
        return '—'


def _state_diag_table(rows: List[Dict[str, Any]], *, title: str) -> List[str]:
    lines: List[str] = [title, '-' * len(title)]
    if not rows:
        lines.append('No rows.')
        lines.append('')
        return lines
    name_w = max(12, max(len(str(r.get('name') or '')) for r in rows))
    hdr = (
        f"{'State'.ljust(name_w)}  {'Trades':>6}  {'Win%':>6}  {'AvgRet':>8}  "
        f"{'AvgPnL':>10}  {'TotPnL':>10}  {'AvgDur':>8}  {'Conf':>6}  {'Age':>6}"
    )
    lines.append(hdr)
    lines.append('-' * len(hdr))
    for r in rows:
        lines.append(
            f"{str(r.get('name') or '').ljust(name_w)}  "
            f"{int(r.get('trades') or 0):>6d}  "
            f"{float(r.get('win_rate_pct') or 0.0):>5.1f}%  "
            f"{float(r.get('avg_return_pct') or 0.0):>7.2f}%  "
            f"{float(r.get('avg_pnl') or 0.0):>10.2f}  "
            f"{float(r.get('total_pnl') or 0.0):>10.2f}  "
            f"{float(r.get('avg_duration_min') or 0.0):>7.0f}m  "
            f"{float(r.get('avg_confidence') or 0.0):>5.0f}%  "
            f"{float(r.get('avg_age') or 0.0):>5.1f}"
        )
    lines.append('')
    return lines


def _confusion_lines(matrix: Dict[str, Dict[str, int]], *, title: str) -> List[str]:
    lines: List[str] = [title, '-' * len(title)]
    if not matrix:
        lines.append('No transitions.')
        lines.append('')
        return lines
    row_keys = [k for k in STATE_DIAG_LABELS if k in matrix]
    col_keys = [k for k in STATE_DIAG_LABELS if any((matrix.get(r, {}) or {}).get(k, 0) for r in matrix)]
    if not row_keys:
        row_keys = sorted(matrix.keys())
    if not col_keys:
        col_keys = sorted({ck for rv in matrix.values() for ck in rv})
    cell_w = max(5, max(len(_pretty_value(k)) for k in col_keys + row_keys))
    row_w = max(12, max(len(_pretty_value(k)) for k in row_keys))
    header = 'From -> To'.ljust(row_w) + '  ' + '  '.join(_pretty_value(k).rjust(cell_w) for k in col_keys)
    lines.append(header)
    lines.append('-' * len(header))
    for rk in row_keys:
        row = [str((matrix.get(rk, {}) or {}).get(ck, 0)).rjust(cell_w) for ck in col_keys]
        lines.append(_pretty_value(rk).ljust(row_w) + '  ' + '  '.join(row))
    lines.append('')
    return lines


def _phase_rows(trades: List[Any], tf: str) -> List[Dict[str, Any]]:
    buckets: Dict[str, Dict[str, Any]] = {}
    for tr in trades:
        snap = _trade_snapshot(tr, 'entry_snapshot', tf)
        name = _phase_key(snap.get('market_phase'))
        rec = buckets.setdefault(name, {'name': _pretty_value(name), 'trades': 0, 'wins': 0, 'total_return_pct': 0.0, 'total_pnl': 0.0, 'total_duration_min': 0.0})
        rec['trades'] += 1
        pnl = float(_trade_attr(tr, 'net_pnl', 0.0) or 0.0)
        ret = float(_trade_attr(tr, 'return_pct', 0.0) or 0.0)
        dur = float(_trade_attr(tr, 'duration_min', 0.0) or 0.0)
        rec['wins'] += 1 if pnl > 0 else 0
        rec['total_return_pct'] += ret
        rec['total_pnl'] += pnl
        rec['total_duration_min'] += dur
    rows: List[Dict[str, Any]] = []
    for key, rec in buckets.items():
        n = max(1, int(rec['trades']))
        rows.append({
            'name': rec['name'],
            'trades': rec['trades'],
            'win_rate_pct': 100.0 * rec['wins'] / n,
            'avg_return_pct': rec['total_return_pct'] / n,
            'avg_pnl': rec['total_pnl'] / n,
            'total_pnl': rec['total_pnl'],
            'avg_duration_min': rec['total_duration_min'] / n,
            'avg_confidence': 0.0,
            'avg_age': 0.0,
        })
    rows.sort(key=lambda r: (float(r['avg_return_pct']), float(r['total_pnl']), int(r['trades'])), reverse=True)
    return rows


def summarize_market_state_outcomes(bt_result: Any) -> Dict[str, Any]:
    trades = list(getattr(bt_result, 'trades', []) or [])
    summary: Dict[str, Any] = {'trade_count': len(trades), 'timeframes': {}}
    for tf in _gather_trade_timeframes(bt_result):
        buckets: Dict[str, Dict[str, Any]] = {}
        phase_buckets: Dict[str, Dict[str, Any]] = {}
        bucket_outcomes: Dict[str, Dict[str, int]] = {}
        transitions: Dict[str, Dict[str, int]] = {}
        for tr in trades:
            es = _trade_snapshot(tr, 'entry_snapshot', tf)
            xs = _trade_snapshot(tr, 'exit_snapshot', tf)
            entry_state = _state_key(es.get('market_state'))
            exit_state = _state_key(xs.get('market_state'))
            outcome = _bucket_name(_trade_attr(tr, 'net_pnl', 0.0))
            pnl = float(_trade_attr(tr, 'net_pnl', 0.0) or 0.0)
            ret = float(_trade_attr(tr, 'return_pct', 0.0) or 0.0)
            dur = float(_trade_attr(tr, 'duration_min', 0.0) or 0.0)
            conf = float(es.get('market_confidence') or 0.0) if es else 0.0
            age = float(es.get('market_state_age') or 0.0) if es else 0.0
            rec = buckets.setdefault(entry_state, {'name': _pretty_value(entry_state), 'trades': 0, 'wins': 0, 'total_return_pct': 0.0, 'total_pnl': 0.0, 'total_duration_min': 0.0, 'total_confidence': 0.0, 'total_age': 0.0})
            rec['trades'] += 1
            rec['wins'] += 1 if pnl > 0 else 0
            rec['total_return_pct'] += ret
            rec['total_pnl'] += pnl
            rec['total_duration_min'] += dur
            rec['total_confidence'] += conf
            rec['total_age'] += age
            phase = _phase_key(es.get('market_phase'))
            prec = phase_buckets.setdefault(phase, {'name': _pretty_value(phase), 'trades': 0, 'wins': 0, 'total_return_pct': 0.0, 'total_pnl': 0.0, 'total_duration_min': 0.0})
            prec['trades'] += 1
            prec['wins'] += 1 if pnl > 0 else 0
            prec['total_return_pct'] += ret
            prec['total_pnl'] += pnl
            prec['total_duration_min'] += dur
            bucket_outcomes.setdefault(entry_state, {'WIN': 0, 'LOSS': 0, 'FLAT': 0})[outcome] += 1
            transitions.setdefault(entry_state, {})[exit_state] = transitions.setdefault(entry_state, {}).get(exit_state, 0) + 1
        rows: List[Dict[str, Any]] = []
        for key, rec in buckets.items():
            n = max(1, int(rec['trades']))
            rows.append({
                'name': rec['name'],
                'state_key': key,
                'trades': rec['trades'],
                'win_rate_pct': 100.0 * rec['wins'] / n,
                'avg_return_pct': rec['total_return_pct'] / n,
                'avg_pnl': rec['total_pnl'] / n,
                'total_pnl': rec['total_pnl'],
                'avg_duration_min': rec['total_duration_min'] / n,
                'avg_confidence': rec['total_confidence'] / n,
                'avg_age': rec['total_age'] / n,
                'outcomes': bucket_outcomes.get(key, {}),
            })
        rows.sort(key=lambda r: (float(r['avg_return_pct']), float(r['total_pnl']), int(r['trades'])), reverse=True)
        phase_rows: List[Dict[str, Any]] = []
        for key, rec in phase_buckets.items():
            n = max(1, int(rec['trades']))
            phase_rows.append({
                'name': rec['name'],
                'phase_key': key,
                'trades': rec['trades'],
                'win_rate_pct': 100.0 * rec['wins'] / n,
                'avg_return_pct': rec['total_return_pct'] / n,
                'avg_pnl': rec['total_pnl'] / n,
                'total_pnl': rec['total_pnl'],
                'avg_duration_min': rec['total_duration_min'] / n,
                'avg_confidence': 0.0,
                'avg_age': 0.0,
            })
        phase_rows.sort(key=lambda r: (float(r['avg_return_pct']), float(r['total_pnl']), int(r['trades'])), reverse=True)
        summary['timeframes'][tf] = {
            'rows': rows,
            'phase_rows': phase_rows,
            'transitions': transitions,
        }
    return summary




def _avg_metric(rows: List[Dict[str, Any]], key: str, *, names: Optional[Tuple[str, ...]] = None) -> Tuple[float, int]:
    total = 0.0
    count = 0
    for row in rows or []:
        if names is not None and str(row.get("state_key") or row.get("phase_key") or "").upper().strip() not in names:
            continue
        n = int(row.get("trades") or 0)
        if n <= 0:
            continue
        total += float(row.get(key) or 0.0) * n
        count += n
    return (total / count if count else 0.0), count


def suggest_market_state_thresholds(bt_result: Any, current_thresholds: Optional[Dict[str, Any]] = None, preset: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    th_obj = coerce_market_state_thresholds(current_thresholds)
    current = market_state_thresholds_dict(th_obj)
    suggested = dict(current)
    preset = preset if isinstance(preset, dict) else {}

    trades = list(getattr(bt_result, 'trades', []) or []) if bt_result is not None else []
    overall_avg_return = 0.0
    overall_win_rate = 0.0
    if trades:
        overall_avg_return = sum(float(_trade_attr(tr, 'return_pct', 0.0) or 0.0) for tr in trades) / max(1, len(trades))
        overall_win_rate = 100.0 * sum(1 for tr in trades if float(_trade_attr(tr, 'net_pnl', 0.0) or 0.0) > 0.0) / max(1, len(trades))

    reasons: List[str] = []
    warnings: List[str] = []
    rule_hints: List[str] = []

    if len(trades) < 10:
        warnings.append('Fewer than 10 trades are loaded, so any threshold advice would be noisy. Run a larger backtest before applying automatic tuning.')
        return {
            'current': current,
            'suggested': suggested,
            'changes': [],
            'reasons': reasons,
            'warnings': warnings,
            'rule_hints': rule_hints,
            'overall_avg_return': overall_avg_return,
            'overall_win_rate': overall_win_rate,
        }

    summary = summarize_market_state_outcomes(bt_result)
    tf_summaries = list((summary.get('timeframes') or {}).items())
    if not tf_summaries:
        warnings.append('No market-state snapshots were found on the current trades. Run the backtest again with market-state snapshots attached.')
        return {
            'current': current,
            'suggested': suggested,
            'changes': [],
            'reasons': reasons,
            'warnings': warnings,
            'rule_hints': rule_hints,
            'overall_avg_return': overall_avg_return,
            'overall_win_rate': overall_win_rate,
        }

    pullback_advantage = 0
    expansion_advantage = 0
    noisy_penalty = 0
    exhaustion_penalty = 0
    strong_confidence_gap = False

    for tf, tf_sum in tf_summaries:
        rows = list(tf_sum.get('rows', []) or [])
        if not rows:
            continue

        noisy_ret, noisy_count = _avg_metric(rows, 'avg_return_pct', names=('TRANSITION', 'COMPRESSION'))
        trend_ret, trend_count = _avg_metric(rows, 'avg_return_pct', names=('BULL_EXPANSION', 'BULL_PULLBACK', 'BEAR_EXPANSION', 'BEAR_PULLBACK'))
        if noisy_count >= 6 and noisy_ret < min(overall_avg_return - 0.10, trend_ret - 0.10):
            noisy_penalty += 1
            rule_hints.append(f'{tf}: Transition / Compression entries underperform directional states. Consider explicit no-trade filters for those states.')

        pullback_ret, pullback_count = _avg_metric(rows, 'avg_return_pct', names=('BULL_PULLBACK', 'BEAR_PULLBACK'))
        expansion_ret, expansion_count = _avg_metric(rows, 'avg_return_pct', names=('BULL_EXPANSION', 'BEAR_EXPANSION'))
        if pullback_count >= 6 and pullback_ret > expansion_ret + 0.08:
            pullback_advantage += 1
        elif expansion_count >= 6 and expansion_ret > pullback_ret + 0.08:
            expansion_advantage += 1

        exhaustion_ret, exhaustion_count = _avg_metric(rows, 'avg_return_pct', names=('BULL_EXHAUSTION', 'BEAR_EXHAUSTION'))
        if exhaustion_count >= 4 and exhaustion_ret < overall_avg_return - 0.08:
            exhaustion_penalty += 1
            rule_hints.append(f'{tf}: Exhaustion states are weak in this backtest. Consider blocking entries there or demanding stronger re-confirmation before entry.')

        loss_rows = [r for r in rows if float(r.get('avg_return_pct') or 0.0) < 0.0 and int(r.get('trades') or 0) >= 3]
        win_rows = [r for r in rows if float(r.get('avg_return_pct') or 0.0) > 0.0 and int(r.get('trades') or 0) >= 3]
        if loss_rows and win_rows:
            avg_loss_conf = sum(float(r.get('avg_confidence') or 0.0) for r in loss_rows) / len(loss_rows)
            avg_win_conf = sum(float(r.get('avg_confidence') or 0.0) for r in win_rows) / len(win_rows)
            if avg_win_conf >= avg_loss_conf + 8.0:
                strong_confidence_gap = True

    if noisy_penalty >= 1:
        suggested['direction_long_min'] = min(8, int(suggested['direction_long_min']) + 1)
        suggested['direction_short_max'] = max(-8, int(suggested['direction_short_max']) - 1)
        suggested['compression_direction_abs_max'] = min(4, int(suggested['compression_direction_abs_max']) + 1)
        suggested['compression_macd_gap_norm_max'] = round(min(1.5, float(suggested['compression_macd_gap_norm_max']) + 0.05), 3)
        reasons.append('Transition / Compression trades are dragging results. Tightening directional thresholds and broadening the compression bucket should push more ambiguous bars into no-trade states.')

    if pullback_advantage > expansion_advantage:
        suggested['vidya_angle_strong_deg'] = round(min(20.0, float(suggested['vidya_angle_strong_deg']) + 1.0), 3)
        suggested['vidya_delta_support_pct'] = round(min(40.0, float(suggested['vidya_delta_support_pct']) + 2.0), 3)
        reasons.append('Pullback states outperform expansion states. Requiring stronger slope / delta-volume support should reduce premature expansion labels and keep more setups in pullback-ready classification.')
    elif expansion_advantage > pullback_advantage:
        base_floor = max(abs(float(suggested['vidya_angle_up_deg'])), abs(float(suggested['vidya_angle_down_deg'])))
        suggested['vidya_angle_strong_deg'] = round(max(base_floor, float(suggested['vidya_angle_strong_deg']) - 1.0), 3)
        suggested['vidya_delta_support_pct'] = round(max(0.0, float(suggested['vidya_delta_support_pct']) - 2.0), 3)
        reasons.append('Expansion states outperform pullbacks. Relaxing the strong-angle / delta-volume thresholds should let genuine breakouts keep their expansion label longer.')

    if exhaustion_penalty >= 1:
        suggested['vidya_angle_up_deg'] = round(min(10.0, float(suggested['vidya_angle_up_deg']) + 0.5), 3)
        suggested['vidya_angle_down_deg'] = round(max(-10.0, float(suggested['vidya_angle_down_deg']) - 0.5), 3)
        suggested['vidya_delta_weak_pct'] = round(min(float(suggested['vidya_delta_support_pct']), float(suggested['vidya_delta_weak_pct']) + 1.0), 3)
        reasons.append('Exhaustion states are still leaking into entries. Making flat / weak VIDYA conditions easier to detect should mark late-stage trend fatigue earlier.')

    if strong_confidence_gap:
        reasons.append('Winning states carry meaningfully higher confidence than losing states. Confidence already has signal value here, so keep it visible in rules and diagnostics.')

    suggested = market_state_thresholds_dict(coerce_market_state_thresholds(suggested))
    changes: List[Dict[str, Any]] = []
    for key, old_val in current.items():
        new_val = suggested.get(key)
        if float(new_val) != float(old_val):
            changes.append({'key': key, 'old': old_val, 'new': new_val})

    if not changes and not reasons:
        warnings.append('The current backtest did not produce a strong enough statistical pattern for automatic threshold changes. That is often a good sign: use the diagnostics tables and keep manual control.')

    return {
        'current': current,
        'suggested': suggested,
        'changes': changes,
        'reasons': reasons,
        'warnings': warnings,
        'rule_hints': list(dict.fromkeys(rule_hints)),
        'overall_avg_return': overall_avg_return,
        'overall_win_rate': overall_win_rate,
    }


def build_guided_threshold_tuning_report(bt_result: Any, current_thresholds: Optional[Dict[str, Any]] = None, preset_name: str = 'Current Preset', preset: Optional[Dict[str, Any]] = None) -> Tuple[str, Dict[str, Any]]:
    preset = preset if isinstance(preset, dict) else {}
    meta = preset.get('meta', {}) if isinstance(preset.get('meta', {}), dict) else {}
    note = str(meta.get('strategy_note') or '').strip()
    advice = suggest_market_state_thresholds(bt_result, current_thresholds=current_thresholds, preset=preset)
    lines: List[str] = []
    lines.append(f'Guided Threshold Tuning Assistant — {preset_name}')
    lines.append('=' * max(42, len(lines[0])))
    lines.append('')
    if note:
        lines.append(f'Preset note: {note}')
        lines.append('')
    trades = list(getattr(bt_result, 'trades', []) or []) if bt_result is not None else []
    lines.append('Context')
    lines.append('-------')
    lines.append(f'Trades loaded: {len(trades)}')
    lines.append(f'Overall average return: {float(advice.get("overall_avg_return") or 0.0):.2f}%')
    lines.append(f'Overall win rate: {float(advice.get("overall_win_rate") or 0.0):.1f}%')
    lines.append('This assistant does not change indicator formulas. It only suggests classifier-threshold adjustments and rule-layer follow-ups based on the currently loaded backtest.')
    lines.append('')

    lines.append('Suggested Threshold Profile')
    lines.append('---------------------------')
    changes = list(advice.get('changes', []) or [])
    if changes:
        for ch in changes:
            lines.append(f"{ch['key']}: {ch['old']}  ->  {ch['new']}")
    else:
        lines.append('No automatic threshold changes suggested from this sample.')
    lines.append('')

    reasons = list(advice.get('reasons', []) or [])
    if reasons:
        lines.append('Why these changes')
        lines.append('-----------------')
        for idx, reason in enumerate(reasons, start=1):
            lines.append(f'{idx}. {reason}')
        lines.append('')

    rule_hints = list(advice.get('rule_hints', []) or [])
    if rule_hints:
        lines.append('Rule-layer hints')
        lines.append('----------------')
        for hint in rule_hints:
            lines.append(f'- {hint}')
        lines.append('')

    warnings = list(advice.get('warnings', []) or [])
    if warnings:
        lines.append('Warnings')
        lines.append('--------')
        for warning in warnings:
            lines.append(f'- {warning}')
        lines.append('')

    lines.append('How to use this')
    lines.append('---------------')
    lines.append('1. Read the suggested profile and compare it against the State Outcomes report.')
    lines.append('2. Apply the suggested profile to the current session only; then inspect the Market State Board and the backtest examples again.')
    lines.append('3. If the new labels look sensible, save those values into your threshold presets or tune them manually in the Threshold Lab.')
    lines.append('4. Promote consistently good states into explicit entry filters, and demote weak states into no-trade filters.')
    lines.append('')

    return '\n'.join(lines).rstrip() + '\n', advice


def build_market_state_outcome_report(bt_result: Any, preset_name: str = 'Current Preset', preset: Optional[Dict[str, Any]] = None) -> str:
    preset = preset if isinstance(preset, dict) else {}
    meta = preset.get('meta', {}) if isinstance(preset.get('meta', {}), dict) else {}
    note = str(meta.get('strategy_note') or '').strip()
    lines: List[str] = []
    lines.append(f'Market State Outcome Diagnostics — {preset_name}')
    lines.append('=' * max(40, len(lines[0])))
    lines.append('')
    if note:
        lines.append(f'Preset note: {note}')
        lines.append('')
    trades = list(getattr(bt_result, 'trades', []) or []) if bt_result is not None else []
    if not trades:
        lines.append('No backtest result is currently loaded.')
        lines.append('Run a backtest first, then open this report to see which market states helped or hurt the active preset.')
        return '\n'.join(lines).rstrip() + '\n'

    total_pnl = sum(float(_trade_attr(tr, 'net_pnl', 0.0) or 0.0) for tr in trades)
    avg_ret = sum(float(_trade_attr(tr, 'return_pct', 0.0) or 0.0) for tr in trades) / max(1, len(trades))
    win_count = sum(1 for tr in trades if float(_trade_attr(tr, 'net_pnl', 0.0) or 0.0) > 0)
    lines.append('Overall')
    lines.append('-------')
    lines.append(f'Trades: {len(trades)}')
    lines.append(f'Win rate: {100.0 * win_count / max(1, len(trades)):.1f}%')
    lines.append(f'Average return: {avg_ret:.2f}%')
    lines.append(f'Total net PnL: {total_pnl:.2f}')
    lines.append('Interpretation: use the state tables below to see which entry environments deserve more trust and which ones are mostly noise for this preset.')
    lines.append('')

    summary = summarize_market_state_outcomes(bt_result)
    tfs = _gather_trade_timeframes(bt_result)
    if not tfs:
        lines.append('No market-state snapshots were attached to the current trades.')
        return '\n'.join(lines).rstrip() + '\n'

    for tf in tfs:
        tf_sum = (summary.get('timeframes') or {}).get(tf, {})
        rows = list(tf_sum.get('rows', []) or [])
        phase_rows = list(tf_sum.get('phase_rows', []) or [])
        transitions = dict(tf_sum.get('transitions', {}) or {})
        lines.append(f'{tf} Entry-State Diagnostics')
        lines.append('-' * len(lines[-1]))
        if not rows:
            lines.append('No rows.')
            lines.append('')
            continue
        best = max(rows, key=lambda r: (float(r.get('avg_return_pct') or 0.0), float(r.get('total_pnl') or 0.0)))
        worst = min(rows, key=lambda r: (float(r.get('avg_return_pct') or 0.0), float(r.get('total_pnl') or 0.0)))
        lines.append(f'Best observed entry state: {best.get("name")} | avg ret {float(best.get("avg_return_pct") or 0.0):.2f}% | trades {int(best.get("trades") or 0)}')
        lines.append(f'Worst observed entry state: {worst.get("name")} | avg ret {float(worst.get("avg_return_pct") or 0.0):.2f}% | trades {int(worst.get("trades") or 0)}')
        lines.append('')
        lines.extend(_state_diag_table(rows, title=f'{tf} states ranked by outcome'))
        if phase_rows:
            lines.extend(_state_diag_table(phase_rows, title=f'{tf} phases ranked by outcome'))
        lines.extend(_confusion_lines(transitions, title=f'{tf} entry-state → exit-state transitions'))
        lines.append('Outcome buckets by entry state')
        lines.append('------------------------------')
        for r in rows:
            oc = dict(r.get('outcomes', {}) or {})
            lines.append(
                f"{r.get('name')}: WIN {int(oc.get('WIN', 0))} | LOSS {int(oc.get('LOSS', 0))} | FLAT {int(oc.get('FLAT', 0))}"
            )
        lines.append('')
        lines.append('How to use this')
        lines.append('---------------')
        lines.append(f'If {tf} shows one or two entry states with clearly stronger win rate / return, promote those states into gating rules for this preset.')
        lines.append(f'If {tf} shows weak results in Transition / Compression or Exhaustion, use those as explicit no-trade filters.')
        lines.append('')

    return '\n'.join(lines).rstrip() + '\n'


def _state_direction_sign(state: Any) -> int:
    key = str(state or '').upper().strip()
    if key.startswith('BULL_'):
        return 1
    if key.startswith('BEAR_'):
        return -1
    return 0


def summarize_market_state_stream_audit(streams: Optional[Dict[str, Any]], horizons: Tuple[int, ...] = (1, 3, 5)) -> Dict[str, Any]:
    out: Dict[str, Any] = {'timeframes': {}, 'horizons': tuple(int(h) for h in horizons if int(h) > 0)}
    max_h = max(out['horizons']) if out['horizons'] else 1
    for tf, df in dict(streams or {}).items():
        tf_key = str(tf)
        if not isinstance(df, pd.DataFrame) or df.empty or 'market_state' not in df.columns:
            out['timeframes'][tf_key] = {'bars': 0, 'rows': [], 'weaknesses': [], 'breakouts': {}, 'flag_rows': []}
            continue
        close = pd.to_numeric(df.get('close'), errors='coerce')
        state_ser = df.get('market_state').astype(str)
        pa_flag_ser = df.get('market_pa_vol_flag') if 'market_pa_vol_flag' in df.columns else pd.Series(index=df.index, dtype=object)
        strict_flag_ser = df.get('market_pa_strict_flag') if 'market_pa_strict_flag' in df.columns else pd.Series(index=df.index, dtype=object)
        structure_ser = df.get('market_structure_flag') if 'market_structure_flag' in df.columns else pd.Series(index=df.index, dtype=object)
        breakout_ser = df.get('market_breakout_state') if 'market_breakout_state' in df.columns else pd.Series(index=df.index, dtype=object)
        conf_ser = pd.to_numeric(df.get('market_confidence'), errors='coerce') if 'market_confidence' in df.columns else pd.Series(index=df.index, dtype=float)

        state_stats: Dict[str, Dict[str, Any]] = {}
        flag_stats: Dict[str, Dict[str, Any]] = {}
        sticky_long = sticky_short = 0
        sticky_long_total = sticky_short_total = 0

        for i in range(0, max(0, len(df) - max_h)):
            c0 = float(close.iloc[i]) if np.isfinite(close.iloc[i]) else np.nan
            if not np.isfinite(c0) or abs(c0) < 1e-12:
                continue
            state = str(state_ser.iloc[i] or '').upper().strip()
            if not state or state == 'NAN':
                continue
            sign = _state_direction_sign(state)
            bucket = state_stats.setdefault(state, {'samples': 0, 'confidence_total': 0.0, 'signed': {h: [] for h in out['horizons']}})
            bucket['samples'] += 1
            try:
                bucket['confidence_total'] += float(conf_ser.iloc[i]) if np.isfinite(conf_ser.iloc[i]) else 0.0
            except Exception:
                pass
            for h in out['horizons']:
                c1 = float(close.iloc[i + h]) if np.isfinite(close.iloc[i + h]) else np.nan
                if not np.isfinite(c1):
                    continue
                ret = ((c1 - c0) / c0) * 100.0
                signed_ret = ret * float(sign) if sign != 0 else ret
                bucket['signed'][h].append(float(signed_ret))

            pa_flag = str(pa_flag_ser.iloc[i] or '').upper().strip()
            strict_flag = str(strict_flag_ser.iloc[i] or '').upper().strip()
            structure_flag = str(structure_ser.iloc[i] or '').upper().strip()
            flag_key = f'{structure_flag or "-"} / {pa_flag or "-"} / {strict_flag or "-"}'
            fbucket = flag_stats.setdefault(flag_key, {'samples': 0, 'signed3': []})
            fbucket['samples'] += 1
            if 3 in out['horizons'] and len(df) > i + 3 and np.isfinite(close.iloc[i + 3]):
                ret3 = (((float(close.iloc[i + 3]) - c0) / c0) * 100.0) * (float(sign) if sign != 0 else 1.0)
                fbucket['signed3'].append(float(ret3))

            if state == 'BULL_EXPANSION' and sign == 1:
                sticky_long_total += 1
                if len(df) > i + 2 and np.isfinite(close.iloc[i + 1]) and np.isfinite(close.iloc[i + 2]):
                    if float(close.iloc[i + 1]) < c0 and float(close.iloc[i + 2]) <= float(close.iloc[i + 1]):
                        sticky_long += 1
            elif state == 'BEAR_EXPANSION' and sign == -1:
                sticky_short_total += 1
                if len(df) > i + 2 and np.isfinite(close.iloc[i + 1]) and np.isfinite(close.iloc[i + 2]):
                    if float(close.iloc[i + 1]) > c0 and float(close.iloc[i + 2]) >= float(close.iloc[i + 1]):
                        sticky_short += 1

        rows: List[Dict[str, Any]] = []
        for state, rec in state_stats.items():
            samples = int(rec['samples'])
            avg_conf = (float(rec['confidence_total']) / samples) if samples else 0.0
            row = {'state_key': state, 'name': _pretty_value(state), 'samples': samples, 'avg_confidence': avg_conf}
            weakness_hits = 0
            for h in out['horizons']:
                vals = list(rec['signed'].get(h) or [])
                avg = float(np.mean(vals)) if vals else 0.0
                win_rate = 100.0 * sum(1 for v in vals if v > 0.0) / max(1, len(vals)) if vals else 0.0
                row[f'avg_signed_{h}'] = avg
                row[f'win_rate_{h}'] = win_rate
                if 'EXPANSION' in state and h in (1, 3) and avg <= 0.0:
                    weakness_hits += 1
            row['weakness_hits'] = weakness_hits
            rows.append(row)
        rows.sort(key=lambda r: (float(r.get('avg_signed_3') or 0.0), float(r.get('avg_signed_1') or 0.0), int(r.get('samples') or 0)), reverse=True)

        flag_rows: List[Dict[str, Any]] = []
        for key, rec in flag_stats.items():
            vals = list(rec.get('signed3') or [])
            avg = float(np.mean(vals)) if vals else 0.0
            flag_rows.append({'flags': key, 'samples': int(rec.get('samples') or 0), 'avg_signed_3': avg})
        flag_rows.sort(key=lambda r: (float(r.get('avg_signed_3') or 0.0), int(r.get('samples') or 0)), reverse=True)

        breakout_stats: Dict[str, Dict[str, Any]] = {}
        for i in range(0, max(0, len(df) - max_h)):
            state = str(breakout_ser.iloc[i] or '').upper().strip()
            if state not in ('BREAKOUT_CONFIRM_LONG', 'BREAKOUT_CONFIRM_SHORT', 'BREAKOUT_WATCH_LONG', 'BREAKOUT_WATCH_SHORT'):
                continue
            c0 = float(close.iloc[i]) if np.isfinite(close.iloc[i]) else np.nan
            if not np.isfinite(c0) or abs(c0) < 1e-12:
                continue
            sign = 1.0 if state.endswith('LONG') else -1.0
            rec = breakout_stats.setdefault(state, {'samples': 0, 'signed3': [], 'signed5': []})
            rec['samples'] += 1
            for h in (3, 5):
                if len(df) > i + h and np.isfinite(close.iloc[i + h]):
                    ret = (((float(close.iloc[i + h]) - c0) / c0) * 100.0) * sign
                    rec[f'signed{h}'].append(float(ret))
        breakouts = {}
        for key, rec in breakout_stats.items():
            breakouts[key] = {
                'samples': int(rec['samples']),
                'avg_signed_3': float(np.mean(rec['signed3'])) if rec['signed3'] else 0.0,
                'avg_signed_5': float(np.mean(rec['signed5'])) if rec['signed5'] else 0.0,
            }

        weaknesses: List[str] = []
        if sticky_long_total >= 6 and (sticky_long / max(1, sticky_long_total)) >= 0.25:
            weaknesses.append(f'Bull Expansion is sticky on {tf_key}: {sticky_long}/{sticky_long_total} expansion bars immediately rolled over for 2 bars. Tighten expansion or demote faster to Pullback/Transition.')
        if sticky_short_total >= 6 and (sticky_short / max(1, sticky_short_total)) >= 0.25:
            weaknesses.append(f'Bear Expansion is sticky on {tf_key}: {sticky_short}/{sticky_short_total} expansion bars immediately bounced for 2 bars. Tighten expansion or demote faster to Pullback/Transition.')
        for row in rows:
            if 'EXPANSION' in str(row.get('state_key')) and int(row.get('samples') or 0) >= 6 and float(row.get('avg_signed_1') or 0.0) <= 0.0:
                weaknesses.append(f"{tf_key}: {row.get('name')} has non-positive 1-bar follow-through ({float(row.get('avg_signed_1') or 0.0):.2f}%).")
            if 'PULLBACK' in str(row.get('state_key')) and int(row.get('samples') or 0) >= 6 and float(row.get('avg_signed_3') or 0.0) > 0.0:
                weaknesses.append(f"{tf_key}: {row.get('name')} is acting constructively over 3 bars ({float(row.get('avg_signed_3') or 0.0):.2f}%), so it may deserve more trust than Expansion in this sample.")

        out['timeframes'][tf_key] = {
            'bars': int(len(df)),
            'rows': rows,
            'weaknesses': weaknesses,
            'breakouts': breakouts,
            'flag_rows': flag_rows,
        }
    return out


def build_market_state_stream_audit_report(streams: Optional[Dict[str, Any]], source_name: str = 'Loaded Dataset', preset_name: str = 'Current Preset') -> str:
    lines: List[str] = []
    title = f'Market State Empty Backtest / Weakness Audit — {preset_name}'
    lines.append(title)
    lines.append('=' * max(44, len(title)))
    lines.append('')
    if not isinstance(streams, dict) or not streams:
        lines.append('No history/backtest streams are currently loaded.')
        lines.append('Load history or run a backtest first, then open this audit to score the rectangles bar-by-bar against future candles.')
        return '\n'.join(lines).rstrip() + '\n'
    audit = summarize_market_state_stream_audit(streams)
    horizons = list(audit.get('horizons') or [1, 3, 5])
    lines.append('What this does')
    lines.append('--------------')
    lines.append(f'- Source: {source_name}')
    lines.append('- This is an empty backtest for the Market State rectangles: each bar is scored against the next 1 / 3 / 5 bars without requiring trade rules.')
    lines.append('- Positive signed return means the rectangle direction matched the next move. Negative signed return means it fought the next move.')
    lines.append('')
    for tf, tf_sum in (audit.get('timeframes') or {}).items():
        rows = list(tf_sum.get('rows') or [])
        lines.append(f'{tf} Rectangle Effectiveness')
        lines.append('-' * len(lines[-1]))
        lines.append(f"Bars audited: {int(tf_sum.get('bars') or 0)}")
        if not rows:
            lines.append('No rows.')
            lines.append('')
            continue
        header = 'State                        Samples   Conf   ' + '   '.join([f'{h}b avg / win' for h in horizons])
        lines.append(header)
        lines.append('-' * len(header))
        for row in rows[:8]:
            parts = []
            for h in horizons:
                parts.append(f"{float(row.get(f'avg_signed_{h}') or 0.0):>5.2f}% / {float(row.get(f'win_rate_{h}') or 0.0):>4.0f}%")
            lines.append(f"{str(row.get('name') or '-')[:26]:26}  {int(row.get('samples') or 0):>6}  {float(row.get('avg_confidence') or 0.0):>5.1f}   " + '   '.join(parts))
        lines.append('')
        flag_rows = list(tf_sum.get('flag_rows') or [])
        if flag_rows:
            lines.append('Hard-flag combinations (3-bar signed return)')
            lines.append('--------------------------------------------')
            for row in flag_rows[:6]:
                lines.append(f"{str(row.get('flags') or '-')[:42]:42}  samples {int(row.get('samples') or 0):>4}  avg3 {float(row.get('avg_signed_3') or 0.0):>5.2f}%")
            lines.append('')
        breakouts = dict(tf_sum.get('breakouts') or {})
        if breakouts:
            lines.append('Breakout follow-through audit')
            lines.append('-----------------------------')
            for key, rec in sorted(breakouts.items()):
                lines.append(f"{_pretty_value(key)}: samples {int(rec.get('samples') or 0)} | 3-bar {float(rec.get('avg_signed_3') or 0.0):.2f}% | 5-bar {float(rec.get('avg_signed_5') or 0.0):.2f}%")
            lines.append('')
        weaknesses = list(tf_sum.get('weaknesses') or [])
        lines.append('Observed weaknesses / loopholes')
        lines.append('-------------------------------')
        if weaknesses:
            for w in weaknesses[:8]:
                lines.append(f'- {w}')
        else:
            lines.append('- No dominant weakness heuristic fired on this sample.')
        lines.append('')
    lines.append('How to use this')
    lines.append('---------------')
    lines.append('1. If Expansion has poor 1-bar / 3-bar signed return, tighten the expansion thresholds in Threshold Lab.')
    lines.append('2. If Pullback has better forward signed return than Expansion, trust Pullback more on that timeframe.')
    lines.append('3. Use the hard-flag table to verify whether Structure / PA-Vol / Strict are helping or just adding noise.')
    lines.append('4. Treat this as a diagnostics engine first. Use it to guide manual threshold tuning before changing any formulas.')
    return '\n'.join(lines).rstrip() + '\n'


def build_preset_report(preset: Dict[str, Any], preset_name: str = "Current Preset", bt_result: Optional[Any] = None) -> str:
    preset = preset if isinstance(preset, dict) else {}
    settings = preset.get("settings", {}) if isinstance(preset.get("settings", {}), dict) else {}
    meta = preset.get("meta", {}) if isinstance(preset.get("meta", {}), dict) else {}
    note = str(meta.get("strategy_note") or "").strip()
    archetype, one_liner, thesis = infer_preset_archetype(preset)
    visual, ideal_path, fail_path = _visual_block(archetype)

    lines: List[str] = []
    lines.append(f"Preset Visualization Report — {preset_name}")
    lines.append("=" * max(34, len(lines[0])))
    lines.append("")
    lines.append(f"Archetype: {archetype}")
    lines.append(f"Intent: {one_liner}")
    lines.append(f"Interpretation: {thesis}")
    if note:
        lines.append(f"Your note: {note}")
    lines.append("")
    symbol = settings.get("symbol")
    bt_mode = settings.get("bt_mode")
    enabled_tfs = [tf for tf, enabled in (settings.get("timeframes", {}) if isinstance(settings.get("timeframes", {}), dict) else {}).items() if enabled]
    if symbol or bt_mode or enabled_tfs:
        lines.append("Context")
        lines.append("-------")
        if symbol:
            lines.append(f"Symbol: {symbol}")
        if bt_mode:
            lines.append(f"Backtest mode: {bt_mode}")
        if enabled_tfs:
            lines.append(f"Enabled timeframes: {', '.join(enabled_tfs)}")
        lines.append("")

    lines.append("Visual Shape")
    lines.append("------------")
    lines.append(visual)
    lines.append("")

    lines.append("Ideal Storyboard")
    lines.append("---------------")
    for step in ideal_path:
        lines.append(step)
    lines.append("")

    lines.append("Failure Storyboard")
    lines.append("------------------")
    for step in fail_path:
        lines.append(step)
    lines.append("")

    tabs = preset.get("tabs", {}) if isinstance(preset.get("tabs", {}), dict) else {}
    for tab in RULE_TAB_ORDER:
        spec = tabs.get(tab, {}) if isinstance(tabs.get(tab, {}), dict) else {}
        groups = spec.get("groups", []) if isinstance(spec.get("groups", []), list) else []
        groups_join = str(spec.get("groups_join") or "AND").upper().strip()
        lines.append(tab)
        lines.append("-" * len(tab))
        if not groups:
            lines.append("No rules.")
            lines.append("")
            continue
        for gi, group in enumerate(groups, start=1):
            if not isinstance(group, dict):
                continue
            rules = [r for r in (group.get("rules", []) if isinstance(group.get("rules", []), list) else []) if isinstance(r, dict)]
            join_mode = str(group.get("rules_join") or "AND").upper().strip()
            if not rules:
                lines.append(f"Group {gi}: empty")
                continue
            lines.append(f"Group {gi} ({join_mode}):")
            if join_mode == "SEQUENCE":
                lines.append("  Ordered progression: each rule must complete in listed order across time.")
            for r in rules:
                lines.append(f"  • {_rule_phrase(r)}")
        lines.append(f"Groups combine with: {groups_join}")
        lines.append("")

    entry_filters = preset.get("entry_filters", {}) if isinstance(preset.get("entry_filters", {}), dict) else {}
    if entry_filters:
        lines.append("Entry Filters")
        lines.append("-------------")
        for side, cfg in entry_filters.items():
            if isinstance(cfg, dict):
                mode = str(cfg.get("mode") or "OFF")
                enabled = bool(cfg.get("enabled", False))
                if enabled and mode != "OFF":
                    parts = [mode.replace("_", " ").title()]
                    if "expansion_atr_mult" in cfg:
                        parts.append(f"expansion ≥ {float(cfg.get('expansion_atr_mult') or 0):.2f} ATR")
                    if "confirm_bars" in cfg:
                        parts.append(f"wait {int(cfg.get('confirm_bars') or 0)} bar(s)")
                    lines.append(f"{side}: {'; '.join(parts)}")
                else:
                    lines.append(f"{side}: Off")
        lines.append("")

    if bt_result is not None:
        _append_real_examples(lines, bt_result, archetype)

    lines.append("What this preset is probably trying to catch")
    lines.append("------------------------------------------")
    if archetype == "Pullback Continuation":
        lines.append("A fresh directional regime that breathes back before continuation.")
        lines.append("The preset likely values cleaner entries over catching the very first impulse bar.")
    elif archetype == "Breakout Continuation":
        lines.append("A strong move that expands immediately and may not give a comfortable retest.")
        lines.append("The preset likely prioritizes participation over perfect entry price.")
    elif archetype == "Regime Shift Confirmation":
        lines.append("A new turn that still needs proof before it is trusted.")
        lines.append("The preset likely exists to filter fake flips and delayed momentum failures.")
    else:
        lines.append("A custom multi-filter setup whose edge depends on context being aligned at the same time.")
    lines.append("")

    return "\n".join(lines).rstrip() + "\n"
