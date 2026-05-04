from __future__ import annotations

from typing import Any, Dict, List, Optional, Tuple

import numpy as np
import pandas as pd

from .backtest_models import BacktestConfig, _ResolvedLevelExit, _TradeLifecycle
from .backtest_planning import _barsago_profile_for_tab
from .rules import Rule, RuleEvalContext, eval_tab_generic_trace, format_tab_trace


def _safe_float(v: Any) -> Optional[float]:
    try:
        f = float(v)
        return f if np.isfinite(f) else None
    except Exception:
        return None


def _make_trade_lifecycle(engine_type: str, fill_source: str) -> _TradeLifecycle:
    return _TradeLifecycle(
        state="FLAT",
        engine_type=str(engine_type or ""),
        fill_source=str(fill_source or ""),
    )


def _open_trade_lifecycle(life: _TradeLifecycle,
                          side: str,
                          signal_time: Any,
                          order_created_time: Any,
                          entry_fill_time: Any,
                          entry_fill_price: float,
                          qty: float,
                          entry_reason: str,
                          entry_snapshot: Optional[Dict[str, Dict[str, Any]]] = None,
                          entry_barsago: Optional[Dict[str, Any]] = None,
                          entry_rule_trace: str = "") -> _TradeLifecycle:
    life.state = "OPEN"
    life.side = str(side).upper()
    life.signal_time = pd.to_datetime(signal_time, utc=True) if signal_time is not None else None
    life.order_created_time = pd.to_datetime(order_created_time, utc=True) if order_created_time is not None else None
    life.entry_fill_time = pd.to_datetime(entry_fill_time, utc=True) if entry_fill_time is not None else None
    life.exit_signal_time = None
    life.exit_order_created_time = None
    life.exit_fill_time = None
    life.entry_fill_price = float(entry_fill_price)
    life.exit_fill_price = None
    life.qty = float(qty)
    life.entry_reason = str(entry_reason)
    life.exit_reason = ""
    life.entry_rule_trace = str(entry_rule_trace or "")
    life.exit_rule_trace = ""
    life.ambiguous_exit = False
    life.entry_snapshot = entry_snapshot if isinstance(entry_snapshot, dict) else {}
    life.exit_snapshot = {}
    life.entry_barsago = entry_barsago if isinstance(entry_barsago, dict) else {}
    life.exit_barsago = {}
    return life


def _close_trade_lifecycle(life: Optional[_TradeLifecycle],
                           exit_fill_time: Any,
                           exit_fill_price: float,
                           exit_reason: str,
                           exit_snapshot: Optional[Dict[str, Dict[str, Any]]] = None,
                           exit_barsago: Optional[Dict[str, Any]] = None,
                           ambiguous_exit: bool = False,
                           exit_rule_trace: str = "",
                           exit_signal_time: Any = None,
                           exit_order_created_time: Any = None) -> Dict[str, Any]:
    if life is None:
        return {
            "signal_time": None,
            "order_created_time": None,
            "entry_fill_time": None,
            "exit_signal_time": pd.to_datetime(exit_signal_time, utc=True) if exit_signal_time is not None else None,
            "exit_order_created_time": pd.to_datetime(exit_order_created_time, utc=True) if exit_order_created_time is not None else None,
            "exit_fill_time": pd.to_datetime(exit_fill_time, utc=True) if exit_fill_time is not None else None,
            "engine_type": "",
            "fill_source": "",
            "ambiguous_exit": bool(ambiguous_exit),
            "entry_rule_trace": "",
            "exit_rule_trace": str(exit_rule_trace or ""),
        }

    life.state = "CLOSED"
    life.exit_signal_time = pd.to_datetime(exit_signal_time, utc=True) if exit_signal_time is not None else None
    life.exit_order_created_time = pd.to_datetime(exit_order_created_time, utc=True) if exit_order_created_time is not None else None
    life.exit_fill_time = pd.to_datetime(exit_fill_time, utc=True) if exit_fill_time is not None else None
    life.exit_fill_price = float(exit_fill_price)
    life.exit_reason = str(exit_reason)
    life.exit_rule_trace = str(exit_rule_trace or "")
    life.exit_snapshot = exit_snapshot if isinstance(exit_snapshot, dict) else {}
    life.exit_barsago = exit_barsago if isinstance(exit_barsago, dict) else {}
    life.ambiguous_exit = bool(ambiguous_exit)
    return {
        "signal_time": life.signal_time,
        "order_created_time": life.order_created_time,
        "entry_fill_time": life.entry_fill_time,
        "exit_signal_time": life.exit_signal_time,
        "exit_order_created_time": life.exit_order_created_time,
        "exit_fill_time": life.exit_fill_time,
        "engine_type": str(life.engine_type or ""),
        "fill_source": str(life.fill_source or ""),
        "ambiguous_exit": bool(life.ambiguous_exit),
        "entry_rule_trace": str(life.entry_rule_trace or ""),
        "exit_rule_trace": str(life.exit_rule_trace or ""),
    }


def _open_position_from_flat(side: str,
                             t: pd.Timestamp,
                             price: float,
                             cur_by_tf: Dict[str, Dict[str, Any]],
                             rules_model: Dict[str, List[List[Rule]]],
                             cfg: BacktestConfig,
                             reason: str,
                             signal_time: Optional[Any] = None,
                             order_created_time: Optional[Any] = None,
                             engine_type: str = "OHLCV",
                             fill_source: str = "BAR",
                             entry_rule_trace: str = "") -> Tuple[Dict[str, Any], float]:
    side = str(side).upper()
    action = "BUY" if side == "LONG" else "SELL"
    px = _apply_slippage(float(price), action, cfg.slippage_bps)
    notional = float(cfg.order_notional_usdt)
    qty = notional / px
    fee_entry = cfg.fee_rate * (qty * px)
    entry_barsago = _barsago_profile_for_tab(rules_model, "Long Entry" if side == "LONG" else "Short Entry")
    stop_loss_price, take_profit_price = _compute_stop_take_prices(px, side, cfg, cur_by_tf)
    break_even_price, break_even_activation_price = _compute_break_even_prices(px, side, cfg)
    trailing_stop_pct = _trailing_stop_pct(cfg)
    trailing_stop_price, trailing_stop_anchor_price = _compute_initial_trailing_stop(px, side, cfg)
    life = _make_trade_lifecycle(engine_type=engine_type, fill_source=fill_source)
    life = _open_trade_lifecycle(
        life,
        side=side,
        signal_time=(signal_time if signal_time is not None else t),
        order_created_time=(order_created_time if order_created_time is not None else (signal_time if signal_time is not None else t)),
        entry_fill_time=t,
        entry_fill_price=px,
        qty=qty,
        entry_reason=reason,
        entry_snapshot=cur_by_tf,
        entry_barsago=entry_barsago,
        entry_rule_trace=entry_rule_trace,
    )
    pos = {
        "side": side,
        "entry_time": t,
        "entry_price": px,
        "qty": qty,
        "fee_entry": fee_entry,
        "entry_snapshot": cur_by_tf,
        "mfe": 0.0,
        "mae": 0.0,
        "entry_barsago": entry_barsago,
        "entry_reason": reason,
        "entry_rule_trace": str(entry_rule_trace or ""),
        "stop_loss_price": stop_loss_price,
        "take_profit_price": take_profit_price,
        "break_even_price": break_even_price,
        "break_even_activation_price": break_even_activation_price,
        "break_even_armed": False,
        "break_even_armed_row_index": None,
        "trailing_stop_pct": trailing_stop_pct,
        "trailing_stop_price": trailing_stop_price,
        "trailing_stop_anchor_price": trailing_stop_anchor_price,
        "progress_high_price": float(px),
        "progress_low_price": float(px),
        "failed_pullback_abort_evaluated": False,
        "failed_pullback_abort_eval_row_index": None,
        "failed_pullback_abort_eval_mfe_pct": None,
        "failed_pullback_abort_eval_close_return_pct": None,
        "lifecycle": life,
        "entry_row_index": None,
    }
    return pos, fee_entry



def _apply_slippage(price: float, action: str, slippage_bps: float) -> float:
    """
    action:
      "BUY" increases price, "SELL" decreases price (worse fill).
    """
    if slippage_bps <= 0:
        return price
    s = slippage_bps / 10_000.0
    if action == "BUY":
        return price * (1.0 + s)
    if action == "SELL":
        return price * (1.0 - s)
    return price



def _normalize_exit_mode(mode: Any, *, is_take_profit: bool = False) -> str:
    m = str(mode or "").upper().strip().replace("-", "_").replace(" ", "_")
    aliases = {
        "": "OFF",
        "NONE": "OFF",
        "OFF": "OFF",
        "DISABLED": "OFF",
        "PERCENT": "PERCENT",
        "PCT": "PERCENT",
        "%": "PERCENT",
        "ABS": "ABSOLUTE",
        "ABSOLUTE": "ABSOLUTE",
        "PRICE": "ABSOLUTE",
        "POINTS": "ABSOLUTE",
        "ATR": "ATR",
        "FIELD": "FIELD",
        "INDICATOR": "FIELD",
        "SNAPSHOT_FIELD": "FIELD",
        "LEVEL": "FIELD",
        "R": "R_MULTIPLE",
        "R_MULTIPLE": "R_MULTIPLE",
        "RISK_REWARD": "R_MULTIPLE",
        "RR": "R_MULTIPLE",
    }
    norm = aliases.get(m, m)
    if norm == "R_MULTIPLE" and not is_take_profit:
        return "OFF"
    if norm not in {"OFF", "PERCENT", "ABSOLUTE", "ATR", "FIELD", "R_MULTIPLE"}:
        return "OFF"
    return norm



def _float_cfg(cfg: BacktestConfig, name: str, default: float = 0.0) -> float:
    try:
        return float(getattr(cfg, name, default) or default)
    except Exception:
        return float(default)



def _str_cfg(cfg: BacktestConfig, name: str, default: str = "") -> str:
    try:
        return str(getattr(cfg, name, default) or default)
    except Exception:
        return str(default)



def _snapshot_numeric_value(entry_snapshot: Optional[Dict[str, Dict[str, Any]]], timeframe: str, field: str) -> Optional[float]:
    if not isinstance(entry_snapshot, dict):
        return None
    tf = str(timeframe or "").strip()
    fld = str(field or "").strip()
    if not fld:
        return None

    candidate_tfs: List[str] = []
    if tf:
        candidate_tfs.append(tf)
    if "1m" not in candidate_tfs:
        candidate_tfs.append("1m")
    for k in entry_snapshot.keys():
        if k not in candidate_tfs:
            candidate_tfs.append(k)

    for tf_key in candidate_tfs:
        snap = entry_snapshot.get(tf_key)
        if not isinstance(snap, dict):
            continue
        val = _safe_float(snap.get(fld))
        if val is not None:
            return float(val)
    return None



def _field_level_with_offset(base_level: float, side: str, is_stop: bool, offset_pct: float) -> float:
    side = str(side).upper()
    out = float(base_level)
    frac = max(0.0, float(offset_pct)) / 100.0
    if frac <= 0.0:
        return out
    if is_stop:
        if side == "LONG":
            return out * (1.0 - frac)
        return out * (1.0 + frac)
    if side == "LONG":
        return out * (1.0 + frac)
    return out * (1.0 - frac)



def _compute_stop_take_prices(entry_fill_price: float,
                              side: str,
                              cfg: BacktestConfig,
                              entry_snapshot: Optional[Dict[str, Dict[str, Any]]] = None) -> Tuple[Optional[float], Optional[float]]:
    side = str(side).upper()
    entry_fill_price = float(entry_fill_price)

    stop_mode = _normalize_exit_mode(_str_cfg(cfg, "stop_loss_mode", "PERCENT"), is_take_profit=False)
    take_mode = _normalize_exit_mode(_str_cfg(cfg, "take_profit_mode", "PERCENT"), is_take_profit=True)

    stop_value = _float_cfg(cfg, "stop_loss_value", _float_cfg(cfg, "stop_loss_pct", 0.0))
    take_value = _float_cfg(cfg, "take_profit_value", _float_cfg(cfg, "take_profit_pct", 0.0))

    stop_tf = _str_cfg(cfg, "stop_loss_timeframe", "1m")
    take_tf = _str_cfg(cfg, "take_profit_timeframe", "1m")
    stop_field = _str_cfg(cfg, "stop_loss_field", "")
    take_field = _str_cfg(cfg, "take_profit_field", "")
    stop_offset_pct = _float_cfg(cfg, "stop_loss_offset_pct", 0.0)
    take_offset_pct = _float_cfg(cfg, "take_profit_offset_pct", 0.0)

    # Backward compatibility: legacy reports/configs that only have *_pct fields.
    if stop_mode == "PERCENT" and stop_value <= 0.0:
        stop_value = _float_cfg(cfg, "stop_loss_pct", 0.0)
    if take_mode == "PERCENT" and take_value <= 0.0:
        take_value = _float_cfg(cfg, "take_profit_pct", 0.0)

    stop_price: Optional[float] = None
    take_profit_price: Optional[float] = None

    if stop_mode == "PERCENT" and stop_value > 0.0:
        frac = stop_value / 100.0
        stop_price = entry_fill_price * (1.0 - frac) if side == "LONG" else entry_fill_price * (1.0 + frac)
    elif stop_mode == "ABSOLUTE" and stop_value > 0.0:
        stop_price = entry_fill_price - stop_value if side == "LONG" else entry_fill_price + stop_value
    elif stop_mode == "ATR" and stop_value > 0.0:
        atr_val = _snapshot_numeric_value(entry_snapshot, stop_tf, "atr")
        if atr_val is not None and atr_val > 0.0:
            stop_price = entry_fill_price - (stop_value * atr_val) if side == "LONG" else entry_fill_price + (stop_value * atr_val)
    elif stop_mode == "FIELD":
        level = _snapshot_numeric_value(entry_snapshot, stop_tf, stop_field)
        if level is not None:
            level = _field_level_with_offset(level, side, True, stop_offset_pct)
            if side == "LONG" and level < entry_fill_price:
                stop_price = level
            elif side == "SHORT" and level > entry_fill_price:
                stop_price = level

    if take_mode == "PERCENT" and take_value > 0.0:
        frac = take_value / 100.0
        take_profit_price = entry_fill_price * (1.0 + frac) if side == "LONG" else entry_fill_price * (1.0 - frac)
    elif take_mode == "ABSOLUTE" and take_value > 0.0:
        take_profit_price = entry_fill_price + take_value if side == "LONG" else entry_fill_price - take_value
    elif take_mode == "ATR" and take_value > 0.0:
        atr_val = _snapshot_numeric_value(entry_snapshot, take_tf, "atr")
        if atr_val is not None and atr_val > 0.0:
            take_profit_price = entry_fill_price + (take_value * atr_val) if side == "LONG" else entry_fill_price - (take_value * atr_val)
    elif take_mode == "FIELD":
        level = _snapshot_numeric_value(entry_snapshot, take_tf, take_field)
        if level is not None:
            level = _field_level_with_offset(level, side, False, take_offset_pct)
            if side == "LONG" and level > entry_fill_price:
                take_profit_price = level
            elif side == "SHORT" and level < entry_fill_price:
                take_profit_price = level
    elif take_mode == "R_MULTIPLE" and take_value > 0.0 and stop_price is not None and np.isfinite(float(stop_price)):
        risk_dist = abs(entry_fill_price - float(stop_price))
        if risk_dist > 0.0:
            take_profit_price = entry_fill_price + (take_value * risk_dist) if side == "LONG" else entry_fill_price - (take_value * risk_dist)

    if stop_price is not None:
        try:
            stop_price = float(stop_price)
            if not np.isfinite(stop_price):
                stop_price = None
        except Exception:
            stop_price = None
    if take_profit_price is not None:
        try:
            take_profit_price = float(take_profit_price)
            if not np.isfinite(take_profit_price):
                take_profit_price = None
        except Exception:
            take_profit_price = None

    return stop_price, take_profit_price


def _break_even_after_mfe_pct(cfg: BacktestConfig) -> float:
    return max(0.0, _float_cfg(cfg, "break_even_after_mfe_pct", 0.0))


def _compute_break_even_prices(entry_fill_price: float,
                               side: str,
                               cfg: BacktestConfig) -> Tuple[Optional[float], Optional[float]]:
    pct = _break_even_after_mfe_pct(cfg)
    if pct <= 0.0:
        return None, None
    entry_fill_price = float(entry_fill_price)
    frac = pct / 100.0
    side = str(side).upper()
    activation_price = entry_fill_price * (1.0 + frac) if side == "LONG" else entry_fill_price * (1.0 - frac)
    break_even_price = float(entry_fill_price)
    if not np.isfinite(activation_price) or not np.isfinite(break_even_price):
        return None, None
    return break_even_price, activation_price


def _break_even_enabled(cfg: BacktestConfig) -> bool:
    return _break_even_after_mfe_pct(cfg) > 0.0


def _trailing_stop_pct(cfg: BacktestConfig) -> float:
    return max(0.0, _float_cfg(cfg, "trailing_stop_pct", 0.0))


def _compute_initial_trailing_stop(entry_fill_price: float,
                                   side: str,
                                   cfg: BacktestConfig) -> Tuple[Optional[float], Optional[float]]:
    pct = _trailing_stop_pct(cfg)
    if pct <= 0.0:
        return None, None
    entry_fill_price = float(entry_fill_price)
    frac = pct / 100.0
    side = str(side).upper()
    anchor_price = float(entry_fill_price)
    trailing_stop_price = anchor_price * (1.0 - frac) if side == "LONG" else anchor_price * (1.0 + frac)
    if not np.isfinite(anchor_price) or not np.isfinite(trailing_stop_price):
        return None, None
    return trailing_stop_price, anchor_price


def _trailing_stop_enabled(cfg: BacktestConfig) -> bool:
    return _trailing_stop_pct(cfg) > 0.0


def _failed_pullback_abort_minutes(cfg: BacktestConfig) -> int:
    return max(0, int(round(_float_cfg(cfg, "failed_pullback_abort_minutes", 0.0))))


def _failed_pullback_abort_min_mfe_pct(cfg: BacktestConfig) -> float:
    return max(0.0, _float_cfg(cfg, "failed_pullback_abort_min_mfe_pct", 0.0))


def _failed_pullback_abort_max_close_return_pct(cfg: BacktestConfig) -> float:
    return _float_cfg(cfg, "failed_pullback_abort_max_close_return_pct", 0.0)


def _failed_pullback_abort_enabled(cfg: BacktestConfig) -> bool:
    return _failed_pullback_abort_minutes(cfg) > 0 and _failed_pullback_abort_min_mfe_pct(cfg) > 0.0


def _break_even_active_for_bar(break_even_armed_row_index: Optional[int], current_row_index: int) -> bool:
    if break_even_armed_row_index is None:
        return False
    try:
        return int(current_row_index) > int(break_even_armed_row_index)
    except Exception:
        return False


def _update_bar_break_even_state(position: Dict[str, Any],
                                 row_index: int,
                                 bar_high: float,
                                 bar_low: float) -> bool:
    if not isinstance(position, dict):
        return False
    if position.get("break_even_armed_row_index") is not None:
        return False

    activation_price = position.get("break_even_activation_price")
    if activation_price is None:
        return False

    try:
        activation = float(activation_price)
        high = float(bar_high)
        low = float(bar_low)
    except Exception:
        return False
    if not np.isfinite(activation) or not np.isfinite(high) or not np.isfinite(low):
        return False

    side = str(position.get("side") or "").upper()
    crossed = high >= activation if side == "LONG" else low <= activation
    if not crossed:
        return False

    position["break_even_armed"] = True
    position["break_even_armed_row_index"] = int(row_index)
    return True


def _update_tick_break_even_state(side: str,
                                  trade_price: float,
                                  break_even_activation_price: Optional[float],
                                  break_even_armed: bool) -> bool:
    if bool(break_even_armed):
        return True
    if break_even_activation_price is None:
        return False

    try:
        trade_price = float(trade_price)
        activation = float(break_even_activation_price)
    except Exception:
        return False
    if not np.isfinite(trade_price) or not np.isfinite(activation):
        return False

    side = str(side).upper()
    if side == "LONG":
        return trade_price >= activation
    return trade_price <= activation


def _update_bar_trailing_stop_state(position: Dict[str, Any],
                                    row_index: int,
                                    bar_high: float,
                                    bar_low: float) -> bool:
    if not isinstance(position, dict):
        return False

    pct = max(0.0, _safe_float(position.get("trailing_stop_pct")) or 0.0)
    if pct <= 0.0:
        return False

    try:
        high = float(bar_high)
        low = float(bar_low)
    except Exception:
        return False
    if not np.isfinite(high) or not np.isfinite(low):
        return False

    side = str(position.get("side") or "").upper()
    anchor = _safe_float(position.get("trailing_stop_anchor_price"))
    current_stop = _safe_float(position.get("trailing_stop_price"))
    frac = pct / 100.0

    if side == "LONG":
        next_anchor = max(float(anchor) if anchor is not None else high, high)
        next_stop = next_anchor * (1.0 - frac)
        improved = current_stop is None or next_stop > float(current_stop)
    else:
        next_anchor = min(float(anchor) if anchor is not None else low, low)
        next_stop = next_anchor * (1.0 + frac)
        improved = current_stop is None or next_stop < float(current_stop)

    if not np.isfinite(next_anchor) or not np.isfinite(next_stop) or not improved:
        return False

    position["trailing_stop_anchor_price"] = float(next_anchor)
    position["trailing_stop_price"] = float(next_stop)
    position["trailing_stop_updated_row_index"] = int(row_index)
    return True


def _update_bar_progress_state(position: Dict[str, Any],
                               bar_high: float,
                               bar_low: float) -> bool:
    if not isinstance(position, dict):
        return False
    try:
        high = float(bar_high)
        low = float(bar_low)
    except Exception:
        return False
    if not np.isfinite(high) or not np.isfinite(low):
        return False

    changed = False
    current_high = _safe_float(position.get("progress_high_price"))
    current_low = _safe_float(position.get("progress_low_price"))
    if current_high is None or high > current_high:
        position["progress_high_price"] = float(high)
        changed = True
    if current_low is None or low < current_low:
        position["progress_low_price"] = float(low)
        changed = True
    return changed


def _evaluate_bar_failed_pullback_abort(position: Dict[str, Any],
                                        row_index: int,
                                        bar_close: float,
                                        cfg: BacktestConfig) -> Optional[Dict[str, float | str | int]]:
    if not isinstance(position, dict) or not _failed_pullback_abort_enabled(cfg):
        return None
    if bool(position.get("failed_pullback_abort_evaluated")):
        return None

    entry_row_index = position.get("entry_row_index")
    if entry_row_index is None:
        return None

    target_bars = _failed_pullback_abort_minutes(cfg)
    try:
        bars_open = int(row_index) - int(entry_row_index) + 1
        close_price = float(bar_close)
        entry_price = float(position.get("entry_price") or 0.0)
    except Exception:
        return None
    if bars_open < target_bars or not np.isfinite(close_price) or entry_price <= 0.0:
        return None

    side = str(position.get("side") or "").upper()
    progress_high = _safe_float(position.get("progress_high_price"))
    progress_low = _safe_float(position.get("progress_low_price"))
    if progress_high is None:
        progress_high = entry_price
    if progress_low is None:
        progress_low = entry_price

    if side == "SHORT":
        mfe_pct = ((entry_price - float(progress_low)) / entry_price) * 100.0
        close_return_pct = ((entry_price - close_price) / entry_price) * 100.0
    else:
        mfe_pct = ((float(progress_high) - entry_price) / entry_price) * 100.0
        close_return_pct = ((close_price - entry_price) / entry_price) * 100.0

    position["failed_pullback_abort_evaluated"] = True
    position["failed_pullback_abort_eval_row_index"] = int(row_index)
    position["failed_pullback_abort_eval_mfe_pct"] = float(mfe_pct)
    position["failed_pullback_abort_eval_close_return_pct"] = float(close_return_pct)

    if mfe_pct >= _failed_pullback_abort_min_mfe_pct(cfg):
        return None
    if close_return_pct > _failed_pullback_abort_max_close_return_pct(cfg):
        return None

    return {
        "reason": f"Failed Pullback Abort ({target_bars}m)",
        "bars_open": int(bars_open),
        "mfe_pct": float(mfe_pct),
        "close_return_pct": float(close_return_pct),
    }


def _update_tick_trailing_stop_state(side: str,
                                     trade_price: float,
                                     trailing_stop_pct: float,
                                     trailing_stop_anchor_price: Optional[float],
                                     trailing_stop_price: Optional[float]) -> Tuple[Optional[float], Optional[float]]:
    pct = max(0.0, float(trailing_stop_pct or 0.0))
    if pct <= 0.0:
        return trailing_stop_anchor_price, trailing_stop_price

    try:
        trade_price = float(trade_price)
    except Exception:
        return trailing_stop_anchor_price, trailing_stop_price
    if not np.isfinite(trade_price):
        return trailing_stop_anchor_price, trailing_stop_price

    anchor = _safe_float(trailing_stop_anchor_price)
    if anchor is None:
        anchor = float(trade_price)
    stop = _safe_float(trailing_stop_price)
    frac = pct / 100.0
    side = str(side).upper()

    if side == "LONG":
        if trade_price > anchor:
            anchor = float(trade_price)
        next_stop = anchor * (1.0 - frac)
        if stop is None or next_stop > stop:
            stop = float(next_stop)
    else:
        if trade_price < anchor:
            anchor = float(trade_price)
        next_stop = anchor * (1.0 + frac)
        if stop is None or next_stop < stop:
            stop = float(next_stop)

    return (float(anchor) if anchor is not None and np.isfinite(anchor) else None,
            float(stop) if stop is not None and np.isfinite(stop) else None)


def _intrabar_exit_enabled(cfg: BacktestConfig) -> bool:
    stop_mode = _normalize_exit_mode(_str_cfg(cfg, "stop_loss_mode", "PERCENT"), is_take_profit=False)
    take_mode = _normalize_exit_mode(_str_cfg(cfg, "take_profit_mode", "PERCENT"), is_take_profit=True)
    stop_value = _float_cfg(cfg, "stop_loss_value", _float_cfg(cfg, "stop_loss_pct", 0.0))
    take_value = _float_cfg(cfg, "take_profit_value", _float_cfg(cfg, "take_profit_pct", 0.0))
    stop_field = _str_cfg(cfg, "stop_loss_field", "")
    take_field = _str_cfg(cfg, "take_profit_field", "")

    stop_enabled = (
        (stop_mode in {"PERCENT", "ABSOLUTE", "ATR"} and stop_value > 0.0)
        or (stop_mode == "FIELD" and bool(stop_field.strip()))
    )
    take_enabled = (
        (take_mode in {"PERCENT", "ABSOLUTE", "ATR", "R_MULTIPLE"} and take_value > 0.0)
        or (take_mode == "FIELD" and bool(take_field.strip()))
    )
    return bool(stop_enabled or take_enabled or _break_even_enabled(cfg) or _trailing_stop_enabled(cfg))



def _resolve_ohlcv_open_gap_exit(side: str,
                                 bar_open: float,
                                 stop_price: Optional[float],
                                 take_profit_price: Optional[float],
                                 trailing_stop_price: Optional[float] = None,
                                 break_even_price: Optional[float] = None,
                                 break_even_active: bool = False) -> Optional[_ResolvedLevelExit]:
    side = str(side).upper()
    try:
        bar_open = float(bar_open)
    except Exception:
        return None
    if not np.isfinite(bar_open):
        return None

    if side == "LONG":
        if stop_price is not None and np.isfinite(float(stop_price)) and bar_open <= float(stop_price):
            return _ResolvedLevelExit(reason="Stop Loss", fill_price=float(bar_open), at_open=True, ambiguous=False)
        if trailing_stop_price is not None and np.isfinite(float(trailing_stop_price)) and bar_open <= float(trailing_stop_price):
            return _ResolvedLevelExit(reason="Trailing Stop", fill_price=float(bar_open), at_open=True, ambiguous=False)
        if bool(break_even_active) and break_even_price is not None and np.isfinite(float(break_even_price)) and bar_open <= float(break_even_price):
            return _ResolvedLevelExit(reason="Break Even", fill_price=float(bar_open), at_open=True, ambiguous=False)
        if take_profit_price is not None and np.isfinite(float(take_profit_price)) and bar_open >= float(take_profit_price):
            return _ResolvedLevelExit(reason="Take Profit", fill_price=float(bar_open), at_open=True, ambiguous=False)
    else:
        if stop_price is not None and np.isfinite(float(stop_price)) and bar_open >= float(stop_price):
            return _ResolvedLevelExit(reason="Stop Loss", fill_price=float(bar_open), at_open=True, ambiguous=False)
        if trailing_stop_price is not None and np.isfinite(float(trailing_stop_price)) and bar_open >= float(trailing_stop_price):
            return _ResolvedLevelExit(reason="Trailing Stop", fill_price=float(bar_open), at_open=True, ambiguous=False)
        if bool(break_even_active) and break_even_price is not None and np.isfinite(float(break_even_price)) and bar_open >= float(break_even_price):
            return _ResolvedLevelExit(reason="Break Even", fill_price=float(bar_open), at_open=True, ambiguous=False)
        if take_profit_price is not None and np.isfinite(float(take_profit_price)) and bar_open <= float(take_profit_price):
            return _ResolvedLevelExit(reason="Take Profit", fill_price=float(bar_open), at_open=True, ambiguous=False)
    return None



def _resolve_ohlcv_intrabar_range_exit(side: str,
                                       bar_high: float,
                                       bar_low: float,
                                       stop_price: Optional[float],
                                       take_profit_price: Optional[float],
                                       trailing_stop_price: Optional[float] = None,
                                       break_even_price: Optional[float] = None,
                                       break_even_active: bool = False) -> Optional[_ResolvedLevelExit]:
    side = str(side).upper()
    try:
        bar_high = float(bar_high)
        bar_low = float(bar_low)
    except Exception:
        return None
    if not np.isfinite(bar_high) or not np.isfinite(bar_low):
        return None

    if side == "LONG":
        hit_stop = bool(stop_price is not None and np.isfinite(float(stop_price)) and bar_low <= float(stop_price))
        hit_trailing = bool(trailing_stop_price is not None and np.isfinite(float(trailing_stop_price)) and bar_low <= float(trailing_stop_price))
        hit_be = bool(bool(break_even_active) and break_even_price is not None and np.isfinite(float(break_even_price)) and bar_low <= float(break_even_price))
        hit_tp = bool(take_profit_price is not None and np.isfinite(float(take_profit_price)) and bar_high >= float(take_profit_price))
        if hit_stop and (hit_trailing or hit_be or hit_tp):
            return _ResolvedLevelExit(reason="Stop Loss", fill_price=float(stop_price), at_open=False, ambiguous=True)
        if hit_stop:
            return _ResolvedLevelExit(reason="Stop Loss", fill_price=float(stop_price), at_open=False, ambiguous=False)
        if hit_trailing and (hit_be or hit_tp):
            return _ResolvedLevelExit(reason="Trailing Stop", fill_price=float(trailing_stop_price), at_open=False, ambiguous=True)
        if hit_trailing:
            return _ResolvedLevelExit(reason="Trailing Stop", fill_price=float(trailing_stop_price), at_open=False, ambiguous=False)
        if hit_be and hit_tp:
            return _ResolvedLevelExit(reason="Break Even", fill_price=float(break_even_price), at_open=False, ambiguous=True)
        if hit_be:
            return _ResolvedLevelExit(reason="Break Even", fill_price=float(break_even_price), at_open=False, ambiguous=False)
        if hit_tp:
            return _ResolvedLevelExit(reason="Take Profit", fill_price=float(take_profit_price), at_open=False, ambiguous=False)
    else:
        hit_stop = bool(stop_price is not None and np.isfinite(float(stop_price)) and bar_high >= float(stop_price))
        hit_trailing = bool(trailing_stop_price is not None and np.isfinite(float(trailing_stop_price)) and bar_high >= float(trailing_stop_price))
        hit_be = bool(bool(break_even_active) and break_even_price is not None and np.isfinite(float(break_even_price)) and bar_high >= float(break_even_price))
        hit_tp = bool(take_profit_price is not None and np.isfinite(float(take_profit_price)) and bar_low <= float(take_profit_price))
        if hit_stop and (hit_trailing or hit_be or hit_tp):
            return _ResolvedLevelExit(reason="Stop Loss", fill_price=float(stop_price), at_open=False, ambiguous=True)
        if hit_stop:
            return _ResolvedLevelExit(reason="Stop Loss", fill_price=float(stop_price), at_open=False, ambiguous=False)
        if hit_trailing and (hit_be or hit_tp):
            return _ResolvedLevelExit(reason="Trailing Stop", fill_price=float(trailing_stop_price), at_open=False, ambiguous=True)
        if hit_trailing:
            return _ResolvedLevelExit(reason="Trailing Stop", fill_price=float(trailing_stop_price), at_open=False, ambiguous=False)
        if hit_be and hit_tp:
            return _ResolvedLevelExit(reason="Break Even", fill_price=float(break_even_price), at_open=False, ambiguous=True)
        if hit_be:
            return _ResolvedLevelExit(reason="Break Even", fill_price=float(break_even_price), at_open=False, ambiguous=False)
        if hit_tp:
            return _ResolvedLevelExit(reason="Take Profit", fill_price=float(take_profit_price), at_open=False, ambiguous=False)
    return None



def _resolve_tick_level_exit(side: str,
                             trade_price: float,
                             stop_price: Optional[float],
                             take_profit_price: Optional[float],
                             trailing_stop_price: Optional[float] = None,
                             break_even_price: Optional[float] = None,
                             break_even_active: bool = False) -> Optional[str]:
    side = str(side).upper()
    try:
        trade_price = float(trade_price)
    except Exception:
        return None
    if not np.isfinite(trade_price):
        return None

    if side == "LONG":
        if stop_price is not None and np.isfinite(float(stop_price)) and trade_price <= float(stop_price):
            return "Stop Loss"
        if trailing_stop_price is not None and np.isfinite(float(trailing_stop_price)) and trade_price <= float(trailing_stop_price):
            return "Trailing Stop"
        if bool(break_even_active) and break_even_price is not None and np.isfinite(float(break_even_price)) and trade_price <= float(break_even_price):
            return "Break Even"
        if take_profit_price is not None and np.isfinite(float(take_profit_price)) and trade_price >= float(take_profit_price):
            return "Take Profit"
    else:
        if stop_price is not None and np.isfinite(float(stop_price)) and trade_price >= float(stop_price):
            return "Stop Loss"
        if trailing_stop_price is not None and np.isfinite(float(trailing_stop_price)) and trade_price >= float(trailing_stop_price):
            return "Trailing Stop"
        if bool(break_even_active) and break_even_price is not None and np.isfinite(float(break_even_price)) and trade_price >= float(break_even_price):
            return "Break Even"
        if take_profit_price is not None and np.isfinite(float(take_profit_price)) and trade_price <= float(take_profit_price):
            return "Take Profit"
    return None



def _build_tab_rule_trace(tab_name: str,
                          rules_model: Dict[str, List[List[Rule]]],
                          tab_group_join_mode: Dict[str, str],
                          group_rule_join_mode: Dict[str, List[str]],
                          cur_by_tf: Dict[str, Dict[str, Any]],
                          prev_by_tf: Dict[str, Dict[str, Any]],
                          ctx: Optional[RuleEvalContext] = None) -> str:
    try:
        # Trace generation must be side-effect free. Some EVENT / SEQUENCE paths can
        # mutate RuleEvalContext (for example via ctx.note_event(...)) during trace
        # re-evaluation, which can pollute future bars-ago / sequence state and make
        # backtests appear nondeterministic. Build traces against an isolated copy.
        trace_ctx = ctx
        if ctx is not None:
            trace_ctx = ctx.clone_for_trace() if hasattr(ctx, "clone_for_trace") else ctx
        return format_tab_trace(
            eval_tab_generic_trace(
                tab_name,
                rules_model,
                tab_group_join_mode,
                group_rule_join_mode,
                cur_by_tf,
                prev_by_tf,
                ctx=trace_ctx,
            )
        )
    except Exception as e:
        return f"Tab: {tab_name} | trace_error={e}"



def _build_non_rule_exit_trace(reason: str) -> str:
    return "\n".join([
        f"Exit cause: {reason}",
        "Type: non-rule exit",
        "This close was produced by execution-side exit handling (for example stop loss / trailing stop / take profit / forced close), not by the Long Exit / Short Exit rule tabs.",
    ])
