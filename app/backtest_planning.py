from __future__ import annotations

"""Backtest planning and compiled-signal helpers.

These helpers were extracted from :mod:`app.backtest` in the Phase 3
structural split so the core engine can stay focused on execution while the
public import surface remains unchanged.
"""

from typing import Any, Dict, List, Optional, Tuple

import numpy as np
import pandas as pd

from .rules import Rule

__all__ = [
    "_max_bars_ago_needed",
    "_barsago_profile_for_tab",
    "_tab_uses_sequence",
    "_planned_eval_tabs",
    "_rule_uses_barsago",
    "compiled_rule_support",
    "_compiled_text_array",
    "_compiled_numeric_array",
    "_compiled_prev_array",
    "_compiled_cmp_numeric",
    "_compile_rule_signal_array",
    "_compile_group_signal_array",
    "_compile_tab_signal_array",
    "_compile_tab_group_signal_arrays",
    "_compile_tab_signal_arrays",
    "_compiled_tabs_cover_plan",
]

def _max_bars_ago_needed(rules_model: Dict[str, List[List["Rule"]]]) -> int:
    """Return the maximum bars_ago_n used by any rule with bars_ago_mode != OFF."""
    m = 0
    try:
        for groups in (rules_model or {}).values():
            for g in (groups or []):
                for r in (g or []):
                    try:
                        mode = str(getattr(r, "bars_ago_mode", "OFF") or "OFF").upper().strip()
                        if mode not in ("", "OFF"):
                            m = max(m, int(getattr(r, "bars_ago_n", 0) or 0))
                        compare_to = getattr(r, "compare_to", None)
                        if isinstance(compare_to, dict):
                            m = max(m, int(compare_to.get("bars_ago", compare_to.get("bars_ago_n", 0)) or 0))
                            text = str(compare_to.get("field") or compare_to.get("name") or "").strip()
                            if text.endswith("]") and "[" in text:
                                m = max(m, int(text.rpartition("[")[2][:-1] or 0))
                        elif compare_to is not None:
                            text = str(compare_to or "").strip()
                            if text.endswith("]") and "[" in text:
                                m = max(m, int(text.rpartition("[")[2][:-1] or 0))
                    except Exception:
                        continue
    except Exception:
        pass
    return int(m)


def _barsago_profile_for_tab(rules_model: Dict[str, List[List["Rule"]]], tab_name: str) -> Dict[str, Any]:
    """Collect Bars-Ago settings used by rules in a given tab.

    Returns a JSON-serializable dict like:
      {
        "tab": "Long Entry",
        "exact": [0, 2],
        "within": [3],
        "items": [
          {"label": "...", "timeframe": "15m", "field": "ms_cross", "mode": "EXACT", "n": 2, "is_trigger": False},
          ...
        ]
      }

    This is used for Trade Inspector visualization (vertical lines / windows).
    """
    exact: List[int] = []
    within: List[int] = []
    items: List[Dict[str, Any]] = []
    try:
        groups = (rules_model or {}).get(tab_name, []) or []
        for g in groups:
            for r in (g or []):
                try:
                    m = str(getattr(r, "bars_ago_mode", "OFF") or "OFF").upper().strip()
                    if m in ("", "OFF"):
                        continue
                    n = int(getattr(r, "bars_ago_n", 0) or 0)
                    rec = {
                        "label": (r.label() if hasattr(r, "label") else f"{getattr(r,'timeframe','')} {getattr(r,'field','')} {getattr(r,'op','')} {getattr(r,'value','')}"),
                        "timeframe": str(getattr(r, "timeframe", "") or ""),
                        "field": str(getattr(r, "field", "") or ""),
                        "mode": m,
                        "n": n,
                        "is_trigger": bool(getattr(r, "is_trigger", False)),
                    }
                    items.append(rec)
                    if m in ("EXACT", "=="):
                        exact.append(n)
                    elif m in ("WITHIN", "<=", "LE"):
                        within.append(n)
                except Exception:
                    continue
    except Exception:
        pass
    # Unique + sorted
    try:
        exact = sorted({int(x) for x in exact if int(x) >= 0})
    except Exception:
        exact = []
    try:
        within = sorted({int(x) for x in within if int(x) >= 0})
    except Exception:
        within = []
    return {"tab": str(tab_name), "exact": exact, "within": within, "items": items}


def _tab_uses_sequence(group_rule_join_mode: Optional[Dict[str, List[str]]], tab_name: str) -> bool:
    try:
        modes = (group_rule_join_mode or {}).get(str(tab_name), []) or []
        return any(str(mode or "").upper().strip() == "SEQUENCE" for mode in modes)
    except Exception:
        return False


def _planned_eval_tabs(*,
                       active_tabs: Dict[str, bool],
                       sequence_tabs: Optional[Dict[str, bool]] = None,
                       pos_side: Optional[str] = None,
                       has_scheduled_order: bool = False,
                       allow_reverse: bool = False,
                       default_eval_impl: bool = True) -> Dict[str, bool]:
    """Return the tabs that need evaluation on the current bar.

    Safety rules:
    - If the evaluator was monkeypatched/replaced, preserve legacy behavior and evaluate every active tab.
    - Tabs using SEQUENCE groups keep evaluating even when their signals are not immediately actionable,
      because the rule context carries forward sequence progress across bars.
    - Otherwise, only evaluate tabs that can affect state on this bar.
    """
    plan: Dict[str, bool] = {str(tab): bool(enabled) for tab, enabled in (active_tabs or {}).items()}
    if not default_eval_impl:
        return plan

    seq_map = {str(tab): bool(val) for tab, val in (sequence_tabs or {}).items()}
    side = str(pos_side or "").upper().strip()
    actionable_tabs: set[str] = set()
    if not bool(has_scheduled_order):
        if side == "LONG":
            actionable_tabs.add("Long Exit")
            if bool(allow_reverse):
                actionable_tabs.add("Short Entry")
        elif side == "SHORT":
            actionable_tabs.add("Short Exit")
            if bool(allow_reverse):
                actionable_tabs.add("Long Entry")
        else:
            actionable_tabs.update({"Long Entry", "Short Entry"})

    out: Dict[str, bool] = {}
    for tab, enabled in plan.items():
        out[tab] = bool(enabled) and (tab in actionable_tabs or bool(seq_map.get(tab, False)))
    return out


_COMPILED_STATE_TEXT_FIELDS = {
    "ms", "angle", "sig_angle", "ppo", "adx_angle", "atr_angle", "rsi_state", "stoch_rsi_kd",
    "ema_stack", "ema_1_angle_state", "ema_2_angle_state", "ema_spread_trend", "ema_stack_pressure",
    "frama_state", "vidya_state", "range_filter_state", "range_filter_phase",
    "vidya_angle_state", "vidya_angle_accel", "market_state", "market_bias", "market_phase",
    "taker_bias_state",
}

_COMPILED_STATE_NUMERIC_FIELDS = {
    "price", "open", "high", "low", "close", "volume",
    "ema_1", "ema_2", "ema_1_slope", "ema_2_slope", "ema_spread", "ema_spread_pct",
    "ema_spread_delta", "ema_spread_pct_delta",
    "flip_age", "macd", "signal", "rsi", "rsi_smooth", "stoch_rsi_k", "stoch_rsi_d",
    "adx", "atr", "di_plus", "di_minus", "di_spread",
    "vidya_line", "vidya_raw", "vidya_upper", "vidya_lower", "vidya_up_volume", "vidya_down_volume",
    "vidya_delta_pct", "vidya_slope", "vidya_angle_deg", "vidya_angle_deg_norm",
    "vidya_active_liq_low_count", "vidya_active_liq_high_count", "vidya_nearest_liq_low", "vidya_nearest_liq_high",
    "vidya_dist_to_liq_low_pct", "vidya_dist_to_liq_high_pct",
    "range_filter_line", "range_filter_upper", "range_filter_lower", "range_filter_smooth_range",
    "range_filter_upward_count", "range_filter_downward_count",
    "taker_buy_volume", "taker_sell_volume", "taker_delta_volume", "taker_delta_pct",
    "taker_buy_share_pct", "taker_trade_count",
    "market_confidence", "market_state_age", "market_direction_score", "market_energy_score",
    "market_alignment_bonus", "market_macd_gap_norm",
}

_COMPILED_EVENT_EDGE_FIELDS = {
    "ema_cross_up", "ema_cross_down",
    "frama_break_up", "frama_break_down", "frama_mid_cross",
    "vidya_trend_up", "vidya_trend_down",
    "vidya_new_liq_low", "vidya_new_liq_high",
    "vidya_liq_low_taken", "vidya_liq_high_taken",
}


def _rule_uses_barsago(rule: Any) -> bool:
    try:
        mode = str(getattr(rule, "bars_ago_mode", "OFF") or "OFF").upper().strip()
    except Exception:
        mode = "OFF"
    return mode not in ("", "OFF")


def _rule_barsago(rule: Any) -> Tuple[str, int]:
    try:
        mode = str(getattr(rule, "bars_ago_mode", "OFF") or "OFF").upper().strip()
    except Exception:
        mode = "OFF"
    if mode in ("", "OFF"):
        return "OFF", 0
    try:
        n = max(0, int(getattr(rule, "bars_ago_n", 0) or 0))
    except Exception:
        n = 0
    return mode, int(n)


def _rule_uses_field_compare(rule: Any) -> bool:
    try:
        target = getattr(rule, "compare_to", None)
    except Exception:
        target = None
    return target not in (None, "", {})


def _parse_compare_to_target(target: Any) -> Optional[Tuple[str, int]]:
    if target in (None, "", {}):
        return None
    bars_ago = 0
    if isinstance(target, dict):
        field = str(target.get("field") or target.get("name") or "").strip()
        try:
            bars_ago = int(target.get("bars_ago", target.get("bars_ago_n", 0)) or 0)
        except Exception:
            bars_ago = 0
    else:
        field = str(target or "").strip()
    if field.endswith("]") and "[" in field:
        base, _, suffix = field.rpartition("[")
        field = base.strip()
        try:
            bars_ago = int(str(suffix[:-1]).strip() or 0)
        except Exception:
            bars_ago = 0
    if not field or bars_ago < 0:
        return None
    return field, int(bars_ago)


def compiled_rule_support(rule: Any) -> Tuple[bool, str]:
    try:
        mode = str(getattr(rule, "mode", "") or "").strip().lower()
        field = str(getattr(rule, "field", "") or "").strip()
        op = str(getattr(rule, "op", "") or "").strip().upper()
        compare_target = _parse_compare_to_target(getattr(rule, "compare_to", None))
        bars_mode, _bars_n = _rule_barsago(rule)

        if mode == "state":
            if bars_mode not in ("OFF", "EXACT", "=="):
                return False, "bars_ago"
            if compare_target is not None:
                compare_field, _bars_ago = compare_target
                if field in _COMPILED_STATE_NUMERIC_FIELDS and compare_field in _COMPILED_STATE_NUMERIC_FIELDS:
                    return True, "compiled"
                return False, f"unsupported-field-compare:{field or 'unknown'}:{compare_field or 'unknown'}"
            if field in _COMPILED_STATE_TEXT_FIELDS or field in _COMPILED_STATE_NUMERIC_FIELDS:
                return True, "compiled"
            return False, f"unsupported-state-field:{field or 'unknown'}"

        if mode != "event":
            return False, f"unsupported-mode:{mode or 'unknown'}"
        if bars_mode != "OFF":
            return False, "bars_ago"

        if field in ("ms", "angle", "sig_angle", "ppo", "adx_angle", "atr_angle", "rsi_state", "stoch_rsi_kd"):
            if op in {"FLIPPED", "TURNED", "CROSS"}:
                return True, "compiled"
            return False, f"unsupported-event-op:{op or 'unknown'}"

        if field in _COMPILED_EVENT_EDGE_FIELDS:
            return True, "compiled"

        if field in ("range_filter_buy", "range_filter_sell"):
            if op in {"", "EVENT"}:
                return True, "compiled"
            return False, f"unsupported-range-event-op:{op}"

        if field in ("ms_cross", "macd0_cross", "di_cross"):
            if op == "CROSS":
                return True, "compiled"
            return False, f"unsupported-cross-op:{op or 'unknown'}"

        return False, f"unsupported-event-field:{field or 'unknown'}"
    except Exception as exc:
        return False, f"compile-check-error:{exc}"


def _compiled_text_array(dft: "pd.DataFrame", field: str) -> Tuple[np.ndarray, np.ndarray]:
    if field not in dft.columns:
        raise KeyError(field)
    s = dft[field]
    mask = (~pd.isna(s)).to_numpy(dtype=bool, copy=False)
    vals = s.astype(object).to_numpy(copy=False)
    out = np.empty(len(vals), dtype=object)
    for i, v in enumerate(vals):
        out[i] = "" if pd.isna(v) else str(v).upper()
    return out, mask


def _compiled_numeric_array(dft: "pd.DataFrame", field: str) -> np.ndarray:
    if field not in dft.columns:
        raise KeyError(field)
    return pd.to_numeric(dft[field], errors="coerce").to_numpy(dtype=float, copy=False)


def _compiled_prev_array(arr: np.ndarray, prev_idx: np.ndarray, default_value: Any) -> np.ndarray:
    n = len(prev_idx)
    if getattr(arr, 'dtype', None) == object:
        out = np.empty(n, dtype=object)
        out[:] = default_value
    else:
        out = np.full(n, default_value, dtype=arr.dtype)
    valid = prev_idx >= 0
    if np.any(valid):
        out[valid] = arr[prev_idx[valid]]
    return out


def _compiled_cmp_numeric(op: str, arr: np.ndarray, value: float) -> np.ndarray:
    valid = np.isfinite(arr)
    op_n = str(op or "==").strip()
    if op_n in ("=", "=="):
        return valid & (arr == float(value))
    if op_n == "!=":
        return valid & (arr != float(value))
    if op_n == ">":
        return valid & (arr > float(value))
    if op_n == ">=":
        return valid & (arr >= float(value))
    if op_n == "<":
        return valid & (arr < float(value))
    if op_n == "<=":
        return valid & (arr <= float(value))
    return np.zeros(len(arr), dtype=bool)


def _compiled_cmp_numeric_array(op: str, lhs: np.ndarray, rhs: np.ndarray) -> np.ndarray:
    valid = np.isfinite(lhs) & np.isfinite(rhs)
    op_n = str(op or "==").strip()
    if op_n in ("=", "=="):
        return valid & (lhs == rhs)
    if op_n == "!=":
        return valid & (lhs != rhs)
    if op_n == ">":
        return valid & (lhs > rhs)
    if op_n == ">=":
        return valid & (lhs >= rhs)
    if op_n == "<":
        return valid & (lhs < rhs)
    if op_n == "<=":
        return valid & (lhs <= rhs)
    return np.zeros(len(lhs), dtype=bool)


def _compiled_shift_by_source_bars(arr: np.ndarray, prev_idx: np.ndarray, bars_ago: int, default_value: Any) -> np.ndarray:
    try:
        bars = max(0, int(bars_ago or 0))
    except Exception:
        bars = 0
    if bars <= 0:
        return arr
    n = len(prev_idx)
    if getattr(arr, "dtype", None) == object:
        out = np.empty(n, dtype=object)
        out[:] = default_value
    else:
        out = np.full(n, default_value, dtype=arr.dtype)
    idx = np.arange(n, dtype=np.int64)
    valid = np.ones(n, dtype=bool)
    for _ in range(bars):
        next_idx = np.full(n, -1, dtype=np.int64)
        ok = valid & (idx >= 0) & (idx < len(prev_idx))
        if np.any(ok):
            next_idx[ok] = prev_idx[idx[ok]]
        idx = next_idx
        valid = idx >= 0
        if not np.any(valid):
            return out
    out[valid] = arr[idx[valid]]
    return out


def _compile_rule_signal_array(rule: Any, subs: Dict[str, "pd.DataFrame"], prev_source_row_by_tf: Dict[str, Optional[np.ndarray]], base_len: int) -> Optional[np.ndarray]:
    try:
        tf = str(getattr(rule, "timeframe", "") or "").strip()
        dft = subs.get(tf)
        prev_idx = prev_source_row_by_tf.get(tf)
        if dft is None or dft.empty or prev_idx is None:
            return None
        if len(dft) != int(base_len) or len(prev_idx) != int(base_len):
            return None
        mode = str(getattr(rule, "mode", "") or "").strip().lower()
        field = str(getattr(rule, "field", "") or "").strip()
        op = str(getattr(rule, "op", "") or "").strip()
        value = getattr(rule, "value", None)
        compare_target = _parse_compare_to_target(getattr(rule, "compare_to", None))
        bars_mode, bars_n = _rule_barsago(rule)

        if mode == "state":
            if bars_mode not in ("OFF", "EXACT", "=="):
                return None
            lhs_bars_ago = int(bars_n if bars_mode in ("EXACT", "==") else 0)
            if compare_target is not None:
                compare_field, compare_bars_ago = compare_target
                if field not in _COMPILED_STATE_NUMERIC_FIELDS or compare_field not in _COMPILED_STATE_NUMERIC_FIELDS:
                    return None
                lhs_raw = _compiled_numeric_array(dft, field)
                lhs = _compiled_shift_by_source_bars(lhs_raw, prev_idx, lhs_bars_ago, np.nan)
                rhs_raw = _compiled_numeric_array(dft, compare_field)
                rhs = _compiled_shift_by_source_bars(rhs_raw, prev_idx, compare_bars_ago, np.nan)
                return _compiled_cmp_numeric_array(op, lhs, rhs)
            if field in _COMPILED_STATE_TEXT_FIELDS:
                cur, valid = _compiled_text_array(dft, field)
                cur = _compiled_shift_by_source_bars(cur, prev_idx, lhs_bars_ago, "")
                valid = _compiled_shift_by_source_bars(valid, prev_idx, lhs_bars_ago, False)
                want = str(value).upper()
                if op in ("", "=", "=="):
                    return valid & (cur == want)
                if op == "!=":
                    return valid & (cur != want)
                return valid & (cur == want)
            if field in _COMPILED_STATE_NUMERIC_FIELDS:
                arr = _compiled_shift_by_source_bars(_compiled_numeric_array(dft, field), prev_idx, lhs_bars_ago, np.nan)
                return _compiled_cmp_numeric(op, arr, float(value))
            return None

        if mode != "event":
            return None
        if bars_mode != "OFF":
            return None

        if field in ("ms", "angle", "sig_angle", "ppo", "adx_angle", "atr_angle", "rsi_state", "stoch_rsi_kd"):
            cur, valid_cur = _compiled_text_array(dft, field)
            prev = _compiled_prev_array(cur, prev_idx, "")
            valid_prev = prev_idx >= 0
            op_u = op.upper()
            if op_u == "FLIPPED":
                return valid_cur & valid_prev & np.isin(prev, ["GREEN", "RED"]) & np.isin(cur, ["GREEN", "RED"]) & (prev != cur)
            if op_u in ("TURNED", "CROSS"):
                want = str(value).upper()
                if want not in ("GREEN", "RED"):
                    return None
                return valid_cur & valid_prev & (prev != want) & (cur == want)
            return None

        if field in _COMPILED_EVENT_EDGE_FIELDS:
            if field not in dft.columns:
                return None
            cur = dft[field].fillna(False).astype(bool).to_numpy(dtype=bool, copy=False)
            prev = _compiled_prev_array(cur, prev_idx, False)
            valid_prev = prev_idx >= 0
            return valid_prev & cur & (~prev)

        if field in ("range_filter_buy", "range_filter_sell"):
            if field not in dft.columns:
                return None
            cur = dft[field].fillna(False).astype(bool).to_numpy(dtype=bool, copy=False)
            prev = _compiled_prev_array(cur, prev_idx, False)
            valid_prev = prev_idx >= 0
            return valid_prev & cur & (~prev)

        if field == "ms_cross":
            macd = _compiled_numeric_array(dft, "macd")
            signal = _compiled_numeric_array(dft, "signal")
            prev_macd = _compiled_prev_array(macd, prev_idx, np.nan)
            prev_signal = _compiled_prev_array(signal, prev_idx, np.nan)
            valid = np.isfinite(macd) & np.isfinite(signal) & (prev_idx >= 0) & np.isfinite(prev_macd) & np.isfinite(prev_signal)
            v = str(value).upper()
            if op.upper() != "CROSS":
                return None
            if v in ("UP", "GREEN", "BULL", "BULLISH"):
                return valid & (prev_macd <= prev_signal) & (macd > signal)
            if v in ("DOWN", "RED", "BEAR", "BEARISH"):
                return valid & (prev_macd >= prev_signal) & (macd < signal)
            return None

        if field == "macd0_cross":
            macd = _compiled_numeric_array(dft, "macd")
            prev_macd = _compiled_prev_array(macd, prev_idx, np.nan)
            valid = np.isfinite(macd) & (prev_idx >= 0) & np.isfinite(prev_macd)
            if op.upper() != "CROSS":
                return None
            v = str(value).upper()
            if v == "UP":
                return valid & (prev_macd <= 0.0) & (macd > 0.0)
            if v == "DOWN":
                return valid & (prev_macd >= 0.0) & (macd < 0.0)
            return None

        if field == "di_cross":
            di_plus = _compiled_numeric_array(dft, "di_plus")
            di_minus = _compiled_numeric_array(dft, "di_minus")
            prev_plus = _compiled_prev_array(di_plus, prev_idx, np.nan)
            prev_minus = _compiled_prev_array(di_minus, prev_idx, np.nan)
            valid = np.isfinite(di_plus) & np.isfinite(di_minus) & (prev_idx >= 0) & np.isfinite(prev_plus) & np.isfinite(prev_minus)
            if op.upper() != "CROSS":
                return None
            v = str(value).upper()
            if v == "UP":
                return valid & (prev_plus <= prev_minus) & (di_plus > di_minus)
            if v == "DOWN":
                return valid & (prev_plus >= prev_minus) & (di_plus < di_minus)
            return None

        return None
    except Exception:
        return None


def _compile_group_signal_array(rules: List[Any], join_mode: str, subs: Dict[str, "pd.DataFrame"], prev_source_row_by_tf: Dict[str, Optional[np.ndarray]], base_len: int) -> Optional[np.ndarray]:
    jm = str(join_mode or "OR").upper().strip()
    if jm == "SEQUENCE":
        return None
    if not rules:
        return np.zeros(int(base_len), dtype=bool)
    compiled_rules: List[np.ndarray] = []
    for rule in rules:
        arr = _compile_rule_signal_array(rule, subs, prev_source_row_by_tf, base_len)
        if arr is None:
            return None
        compiled_rules.append(arr)
    triggers = [arr for arr, rule in zip(compiled_rules, rules) if bool(getattr(rule, "is_trigger", False)) and str(getattr(rule, "mode", "")).strip().lower() == "event"]
    filters = [arr for arr, rule in zip(compiled_rules, rules) if not (bool(getattr(rule, "is_trigger", False)) and str(getattr(rule, "mode", "")).strip().lower() == "event")]

    def _combine(arrays: List[np.ndarray], mode_name: str) -> np.ndarray:
        if not arrays:
            return np.ones(int(base_len), dtype=bool)
        if str(mode_name or "OR").upper().strip() == "AND":
            out = arrays[0].copy()
            for arr in arrays[1:]:
                out &= arr
            return out
        out = arrays[0].copy()
        for arr in arrays[1:]:
            out |= arr
        return out

    if not triggers:
        return _combine(compiled_rules, jm)
    trig_ok = _combine(triggers, "OR")
    filt_ok = _combine(filters, jm)
    return trig_ok & filt_ok


def _compile_tab_signal_array(tab_name: str, rules_model: Dict[str, List[List[Any]]], tab_group_join_mode: Dict[str, str], group_rule_join_mode: Dict[str, List[str]], subs: Dict[str, "pd.DataFrame"], prev_source_row_by_tf: Dict[str, Optional[np.ndarray]], base_len: int) -> Optional[np.ndarray]:
    groups = rules_model.get(tab_name, [])
    if not groups:
        return np.zeros(int(base_len), dtype=bool)
    group_modes = group_rule_join_mode.get(tab_name, [])
    compiled_groups: List[np.ndarray] = []
    for gi, group in enumerate(groups):
        jm = group_modes[gi] if gi < len(group_modes) else "OR"
        arr = _compile_group_signal_array(group, jm, subs, prev_source_row_by_tf, base_len)
        if arr is None:
            return None
        compiled_groups.append(arr)
    join_groups = str(tab_group_join_mode.get(tab_name, "AND") or "AND").upper().strip()
    out = compiled_groups[0].copy()
    if join_groups == "AND":
        for arr in compiled_groups[1:]:
            out &= arr
    else:
        for arr in compiled_groups[1:]:
            out |= arr
    return out


def _compile_tab_signal_arrays(rules_model: Dict[str, List[List[Any]]], tab_group_join_mode: Dict[str, str], group_rule_join_mode: Dict[str, List[str]], subs: Dict[str, "pd.DataFrame"], prev_source_row_by_tf: Dict[str, Optional[np.ndarray]], base_len: int) -> Dict[str, np.ndarray]:
    out: Dict[str, np.ndarray] = {}
    for tab_name in ("Long Entry", "Long Exit", "Short Entry", "Short Exit"):
        arr = _compile_tab_signal_array(tab_name, rules_model, tab_group_join_mode, group_rule_join_mode, subs, prev_source_row_by_tf, base_len)
        if arr is not None:
            out[tab_name] = arr
    return out


def _compile_tab_group_signal_arrays(
    rules_model: Dict[str, List[List[Any]]],
    group_rule_join_mode: Dict[str, List[str]],
    subs: Dict[str, "pd.DataFrame"],
    prev_source_row_by_tf: Dict[str, Optional[np.ndarray]],
    base_len: int,
) -> Dict[str, List[np.ndarray]]:
    out: Dict[str, List[np.ndarray]] = {}
    for tab_name in ("Long Entry", "Long Exit", "Short Entry", "Short Exit"):
        groups = rules_model.get(tab_name, [])
        if not groups:
            out[tab_name] = []
            continue
        group_modes = group_rule_join_mode.get(tab_name, [])
        compiled_groups: List[np.ndarray] = []
        failed = False
        for gi, group in enumerate(groups):
            jm = group_modes[gi] if gi < len(group_modes) else "OR"
            arr = _compile_group_signal_array(group, jm, subs, prev_source_row_by_tf, base_len)
            if arr is None:
                failed = True
                break
            compiled_groups.append(arr)
        if not failed:
            out[tab_name] = compiled_groups
    return out


def _compiled_tabs_cover_plan(eval_tabs: Dict[str, bool], compiled_tabs: Dict[str, np.ndarray]) -> bool:
    for tab_name, enabled in (eval_tabs or {}).items():
        if bool(enabled) and tab_name not in compiled_tabs:
            return False
    return True
