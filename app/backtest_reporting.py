from __future__ import annotations

from typing import Any, Dict, List, Mapping, Optional

import copy
import json

import numpy as np
import pandas as pd

from .backtest_models import Trade


def _make_trade_df(trades: List[Trade]) -> pd.DataFrame:
    rows = []
    for t in trades:
        signal_time = pd.to_datetime(getattr(t, "signal_time", None), utc=True) if getattr(t, "signal_time", None) is not None else pd.NaT
        order_created_time = pd.to_datetime(getattr(t, "order_created_time", None), utc=True) if getattr(t, "order_created_time", None) is not None else pd.NaT
        entry_fill_time = pd.to_datetime(getattr(t, "entry_fill_time", None), utc=True) if getattr(t, "entry_fill_time", None) is not None else pd.NaT
        exit_fill_time = pd.to_datetime(getattr(t, "exit_fill_time", None), utc=True) if getattr(t, "exit_fill_time", None) is not None else pd.NaT

        signal_to_entry_sec = np.nan
        entry_to_exit_sec = np.nan
        order_to_entry_sec = np.nan
        try:
            if pd.notna(signal_time) and pd.notna(entry_fill_time):
                signal_to_entry_sec = float((entry_fill_time - signal_time).total_seconds())
        except Exception:
            signal_to_entry_sec = np.nan
        try:
            if pd.notna(order_created_time) and pd.notna(entry_fill_time):
                order_to_entry_sec = float((entry_fill_time - order_created_time).total_seconds())
        except Exception:
            order_to_entry_sec = np.nan
        try:
            if pd.notna(entry_fill_time) and pd.notna(exit_fill_time):
                entry_to_exit_sec = float((exit_fill_time - entry_fill_time).total_seconds())
        except Exception:
            entry_to_exit_sec = np.nan

        rows.append({
            "trade_id": t.trade_id,
            "side": t.side,
            "entry_time": t.entry_time,
            "entry_price": t.entry_price,
            "exit_time": t.exit_time,
            "exit_price": t.exit_price,
            "qty": t.qty,
            "entry_notional": t.entry_notional,
            "exit_notional": t.exit_notional,
            "gross_pnl": t.gross_pnl,
            "fees": t.fees,
            "fee_entry": float(getattr(t, "fee_entry", 0.0) or 0.0),
            "fee_exit": float(getattr(t, "fee_exit", 0.0) or 0.0),
            "net_pnl": t.net_pnl,
            "return_pct": t.return_pct,
            "duration_min": t.duration_min,
            "mfe": t.mfe,
            "mae": t.mae,
            "mfe_pct": t.mfe_pct,
            "mae_pct": t.mae_pct,
            "entry_reason": t.entry_reason,
            "exit_reason": t.exit_reason,
            "entry_rule_trace": str(getattr(t, "entry_rule_trace", "") or ""),
            "exit_rule_trace": str(getattr(t, "exit_rule_trace", "") or ""),
            "signal_time": signal_time,
            "order_created_time": order_created_time,
            "entry_fill_time": entry_fill_time,
            "exit_fill_time": exit_fill_time,
            "signal_to_entry_sec": signal_to_entry_sec,
            "order_to_entry_sec": order_to_entry_sec,
            "entry_to_exit_sec": entry_to_exit_sec,
            "engine_type": str(getattr(t, "engine_type", "") or ""),
            "fill_source": str(getattr(t, "fill_source", "") or ""),
            "ambiguous_exit": bool(getattr(t, "ambiguous_exit", False)),
            "stop_loss_price": float(getattr(t, "stop_loss_price", 0.0)) if getattr(t, "stop_loss_price", None) is not None else np.nan,
            "take_profit_price": float(getattr(t, "take_profit_price", 0.0)) if getattr(t, "take_profit_price", None) is not None else np.nan,
            "entry_snapshot": json.dumps(t.entry_snapshot, default=str),
            "exit_snapshot": json.dumps(t.exit_snapshot, default=str),
        })
    df = pd.DataFrame(rows)
    return df



def _apply_trade_reporting_metrics(summary: Dict[str, Any], trades: List[Trade]) -> Dict[str, Any]:
    out = dict(summary or {})
    engine_counts: Dict[str, int] = {}
    exit_reason_counts: Dict[str, int] = {}
    fill_source_counts: Dict[str, int] = {}
    ambiguous_exit_count = 0
    stop_loss_count = 0
    take_profit_count = 0
    break_even_count = 0
    trailing_stop_count = 0
    signal_to_entry_vals: List[float] = []
    order_to_entry_vals: List[float] = []
    entry_to_exit_vals: List[float] = []

    for t in trades or []:
        try:
            eng = str(getattr(t, "engine_type", "") or "")
        except Exception:
            eng = ""
        if eng:
            engine_counts[eng] = int(engine_counts.get(eng, 0)) + 1

        try:
            fill_src = str(getattr(t, "fill_source", "") or "")
        except Exception:
            fill_src = ""
        if fill_src:
            fill_source_counts[fill_src] = int(fill_source_counts.get(fill_src, 0)) + 1

        try:
            reason = str(getattr(t, "exit_reason", "") or "")
        except Exception:
            reason = ""
        if reason:
            exit_reason_counts[reason] = int(exit_reason_counts.get(reason, 0)) + 1
            if reason == "Stop Loss":
                stop_loss_count += 1
            elif reason == "Trailing Stop":
                trailing_stop_count += 1
            elif reason == "Take Profit":
                take_profit_count += 1
            elif reason == "Break Even":
                break_even_count += 1

        try:
            if bool(getattr(t, "ambiguous_exit", False)):
                ambiguous_exit_count += 1
        except Exception:
            pass

        try:
            st = pd.to_datetime(getattr(t, "signal_time", None), utc=True) if getattr(t, "signal_time", None) is not None else pd.NaT
            ef = pd.to_datetime(getattr(t, "entry_fill_time", None), utc=True) if getattr(t, "entry_fill_time", None) is not None else pd.NaT
            if pd.notna(st) and pd.notna(ef):
                signal_to_entry_vals.append(float((ef - st).total_seconds()))
        except Exception:
            pass
        try:
            oc = pd.to_datetime(getattr(t, "order_created_time", None), utc=True) if getattr(t, "order_created_time", None) is not None else pd.NaT
            ef = pd.to_datetime(getattr(t, "entry_fill_time", None), utc=True) if getattr(t, "entry_fill_time", None) is not None else pd.NaT
            if pd.notna(oc) and pd.notna(ef):
                order_to_entry_vals.append(float((ef - oc).total_seconds()))
        except Exception:
            pass
        try:
            ef = pd.to_datetime(getattr(t, "entry_fill_time", None), utc=True) if getattr(t, "entry_fill_time", None) is not None else pd.NaT
            xf = pd.to_datetime(getattr(t, "exit_fill_time", None), utc=True) if getattr(t, "exit_fill_time", None) is not None else pd.NaT
            if pd.notna(ef) and pd.notna(xf):
                entry_to_exit_vals.append(float((xf - ef).total_seconds()))
        except Exception:
            pass

    out["engine_type_counts"] = engine_counts
    out["fill_source_counts"] = fill_source_counts
    out["exit_reason_counts"] = exit_reason_counts
    out["ambiguous_exit_count"] = int(ambiguous_exit_count)
    out["stop_loss_count"] = int(stop_loss_count)
    out["take_profit_count"] = int(take_profit_count)
    out["break_even_count"] = int(break_even_count)
    out["trailing_stop_count"] = int(trailing_stop_count)
    out["avg_signal_to_entry_sec"] = float(sum(signal_to_entry_vals) / len(signal_to_entry_vals)) if signal_to_entry_vals else 0.0
    out["avg_order_to_entry_sec"] = float(sum(order_to_entry_vals) / len(order_to_entry_vals)) if order_to_entry_vals else 0.0
    out["avg_entry_to_exit_sec"] = float(sum(entry_to_exit_vals) / len(entry_to_exit_vals)) if entry_to_exit_vals else 0.0
    return out



def _normalize_backtest_replay_context(
    replay_context: Mapping[str, Any],
    *,
    preserve_identity: bool = False,
) -> Dict[str, Any]:
    if not isinstance(replay_context, Mapping):
        raise TypeError("replay_context must be a mapping")

    streams_full = replay_context.get("streams_full")
    if not isinstance(streams_full, dict) or "1m" not in streams_full:
        raise ValueError("replay_context must include a non-empty streams_full mapping with '1m'.")

    start = pd.to_datetime(replay_context.get("start"), utc=True, errors="coerce")
    end = pd.to_datetime(replay_context.get("end"), utc=True, errors="coerce")
    if pd.isna(start) or pd.isna(end):
        raise ValueError("replay_context must include valid UTC-normalizable start/end timestamps.")

    normalized = {
        "symbol": str(replay_context.get("symbol") or ""),
        "streams_full": streams_full,
        "start": pd.Timestamp(start),
        "end": pd.Timestamp(end),
        "rules_model": copy.deepcopy(dict(replay_context.get("rules_model") or {})),
        "tab_group_join_mode": copy.deepcopy(dict(replay_context.get("tab_group_join_mode") or {})),
        "group_rule_join_mode": copy.deepcopy(dict(replay_context.get("group_rule_join_mode") or {})),
        "df_1m_full": replay_context.get("df_1m_full"),
        "entry_filters": copy.deepcopy(replay_context.get("entry_filters")),
    }
    for key in (
        "__compiled_tab_signals_cache",
        "__compiled_group_signals_cache",
        "__compiled_cache_range_start",
        "__compiled_cache_range_end",
        "__compiled_cache_len_1m",
        "__eval_snapshot_pair_cache",
        "__eval_snapshot_pair_cache_signature",
    ):
        if key in replay_context:
            normalized[key] = replay_context.get(key)
    if preserve_identity and isinstance(replay_context, dict):
        replay_context.update(normalized)
        return replay_context
    return normalized
 
def build_backtest_replay_context(symbol: str,
                                  streams_full: Dict[str, pd.DataFrame],
                                  start: pd.Timestamp,
                                  end: pd.Timestamp,
                                  rules_model: Dict[str, Any],
                                  tab_group_join_mode: Dict[str, str],
                                  group_rule_join_mode: Dict[str, List[str]],
                                  df_1m_full: Optional[pd.DataFrame] = None,
                                  entry_filters: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    return _normalize_backtest_replay_context({
        "symbol": str(symbol or ""),
        "streams_full": streams_full,
        "start": pd.to_datetime(start, utc=True, errors="coerce"),
        "end": pd.to_datetime(end, utc=True, errors="coerce"),
        "rules_model": rules_model,
        "tab_group_join_mode": tab_group_join_mode,
        "group_rule_join_mode": group_rule_join_mode,
        "df_1m_full": df_1m_full,
        "entry_filters": entry_filters,
    })
