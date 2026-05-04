from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Any, Dict, List, Mapping, Optional, Sequence
import time

import numpy as np
import pandas as pd

from .backtest_execution import (
    _apply_slippage,
    _break_even_active_for_bar,
    _compute_break_even_prices,
    _compute_initial_trailing_stop,
    _compute_stop_take_prices,
    _resolve_ohlcv_intrabar_range_exit,
    _resolve_ohlcv_open_gap_exit,
    _trailing_stop_pct,
    _update_tick_trailing_stop_state,
)
from .backtest_frames import _prev_source_row_index_for_df, make_streams_htf_closed_only
from .backtest_models import BacktestConfig, BacktestResult, Trade
from .backtest_planning import _compile_tab_signal_arrays
from .backtest_reporting import _apply_trade_reporting_metrics
from .strategy_compiler import FAST_BAR_ENGINE, compile_strategy_plan
from .utils_time import interval_to_ms


@dataclass
class CompiledBarPlan:
    symbol: str
    start: pd.Timestamp
    end: pd.Timestamp
    prepared_streams: Dict[str, pd.DataFrame]
    df_1m: pd.DataFrame
    idx: pd.DatetimeIndex
    compiled_tabs: Dict[str, np.ndarray]
    decision_mask: np.ndarray
    open_arr: np.ndarray
    high_arr: np.ndarray
    low_arr: np.ndarray
    close_arr: np.ndarray
    strategy_plan_engine: str
    strategy_plan_notes: List[str]
    strategy_plan_blockers: List[str]
    timing_prepare_streams_sec: float
    timing_compile_signals_sec: float


@dataclass
class CompiledReplayPlan:
    symbol: str
    start: pd.Timestamp
    end: pd.Timestamp
    idx: pd.DatetimeIndex
    compiled_tabs: Dict[str, np.ndarray]
    decision_mask: np.ndarray
    open_arr: np.ndarray
    high_arr: np.ndarray
    low_arr: np.ndarray
    close_arr: np.ndarray
    strategy_plan_engine: str
    strategy_plan_notes: List[str]
    strategy_plan_blockers: List[str]
    timing_prepare_streams_sec: float
    timing_compile_signals_sec: float


@dataclass
class CompiledReplayPriceExit:
    row_index: Optional[int]
    reason: str = ""
    fill_price_raw: float = 0.0
    ambiguous: bool = False
    at_open: bool = False


def _timestamp_from_ns(ns: int) -> pd.Timestamp:
    return pd.Timestamp(int(ns), tz="UTC")


def _decision_mask_for_step_tf(idx: pd.DatetimeIndex, step_tf: str) -> np.ndarray:
    try:
        tf = str(step_tf or "1m").strip().lower()
        step_ms = int(interval_to_ms(tf))
    except Exception:
        tf = "1m"
        step_ms = 60_000
    if tf == "1m" or step_ms <= 60_000:
        return np.ones(len(idx), dtype=bool)
    try:
        close_ms = (idx.astype("int64").to_numpy(dtype=np.int64, copy=False) // 1_000_000).astype(np.int64, copy=False)
    except Exception:
        close_ms = np.array([int(pd.Timestamp(ts).value // 1_000_000) for ts in idx], dtype=np.int64)
    end_ms = close_ms + 1
    return (end_ms % step_ms) == 0


def _numeric_series(df: pd.DataFrame, column: str, fallback: Optional[np.ndarray] = None) -> np.ndarray:
    if column in df.columns:
        return pd.to_numeric(df[column], errors="coerce").to_numpy(dtype=float, copy=False)
    if fallback is not None:
        return fallback
    return np.full(len(df), np.nan, dtype=float)


def _fill_missing_numeric(base: np.ndarray, fallback: np.ndarray) -> np.ndarray:
    if base.shape != fallback.shape:
        return base
    try:
        return np.where(np.isfinite(base), base, fallback)
    except Exception:
        return base


def _aligned_price_frame(df_1m_full: Optional[pd.DataFrame], idx: pd.DatetimeIndex) -> Optional[pd.DataFrame]:
    if df_1m_full is None or df_1m_full.empty:
        return None

    frame = df_1m_full.copy()
    if "close_time" in frame.columns:
        source_idx = pd.to_datetime(frame["close_time"], utc=True, errors="coerce")
    elif "open_time" in frame.columns:
        source_idx = pd.to_datetime(frame["open_time"], utc=True, errors="coerce")
    else:
        source_idx = pd.to_datetime(frame.index, utc=True, errors="coerce")

    source_idx = pd.DatetimeIndex(source_idx)
    valid_mask = ~source_idx.isna()
    if not bool(valid_mask.any()):
        return None

    frame = frame.loc[valid_mask].copy()
    frame.index = source_idx[valid_mask]
    frame = frame[~frame.index.duplicated(keep="last")]
    if not frame.index.equals(idx):
        frame = frame.reindex(idx)
    return frame


def _equity_mark(balance: float, side: int, qty: float, entry_price: float, mark_price: float) -> float:
    if side > 0:
        return float(balance) + (float(mark_price) - float(entry_price)) * float(qty)
    if side < 0:
        return float(balance) + (float(entry_price) - float(mark_price)) * float(qty)
    return float(balance)


def _make_summary(
    *,
    cfg: BacktestConfig,
    start: pd.Timestamp,
    end: pd.Timestamp,
    trades: List[Trade],
    ending_balance: float,
    max_drawdown_pct: float,
    plan_engine: str,
) -> Dict[str, Any]:
    initial_balance = float(cfg.initial_balance)
    gross_profit = sum(float(t.net_pnl) for t in trades if float(t.net_pnl) > 0.0)
    gross_loss = -sum(float(t.net_pnl) for t in trades if float(t.net_pnl) < 0.0)
    wins = sum(1 for t in trades if float(t.net_pnl) > 0.0)
    losses = sum(1 for t in trades if float(t.net_pnl) < 0.0)
    num_trades = len(trades)
    profit_factor = (gross_profit / gross_loss) if gross_loss > 0.0 else (float("inf") if gross_profit > 0.0 else 0.0)
    win_rate_pct = (wins / num_trades * 100.0) if num_trades > 0 else 0.0
    avg_trade_return_pct = (sum(float(t.return_pct) for t in trades) / num_trades) if num_trades > 0 else 0.0
    summary = {
        "engine": plan_engine,
        "start": str(pd.to_datetime(start, utc=True)),
        "end": str(pd.to_datetime(end, utc=True)),
        "initial_balance": float(initial_balance),
        "ending_balance": float(ending_balance),
        "net_profit": float(ending_balance - initial_balance),
        "return_pct": float(((ending_balance / initial_balance) - 1.0) * 100.0) if initial_balance > 0 else 0.0,
        "num_trades": int(num_trades),
        "wins": int(wins),
        "losses": int(losses),
        "win_rate_pct": float(win_rate_pct),
        "profit_factor": float(profit_factor) if np.isfinite(profit_factor) else float("inf"),
        "avg_trade_return_pct": float(avg_trade_return_pct),
        "max_drawdown_pct": float(max_drawdown_pct),
    }
    return _apply_trade_reporting_metrics(summary, trades)


def build_compiled_bar_plan(
    *,
    symbol: str,
    streams_full: Mapping[str, pd.DataFrame],
    df_1m_full: Optional[pd.DataFrame] = None,
    start: Any,
    end: Any,
    rules_model: Mapping[str, Sequence[Sequence[Any]]],
    tab_group_join_mode: Mapping[str, str],
    group_rule_join_mode: Mapping[str, Sequence[str]],
    cfg: BacktestConfig,
    entry_filters: Optional[Mapping[str, Any]] = None,
    streams_already_htf_closed_only: bool = False,
) -> CompiledBarPlan:
    strategy_plan = compile_strategy_plan(
        rules_model,
        tab_group_join_mode=tab_group_join_mode,
        group_rule_join_mode=group_rule_join_mode,
        entry_filters=entry_filters,
        backtest_cfg=cfg,
    )
    if not strategy_plan.supports_fast_bar:
        raise ValueError(f"Strategy requires fallback engine: {', '.join(strategy_plan.blockers)}")

    start_ts = pd.to_datetime(start, utc=True)
    end_ts = pd.to_datetime(end, utc=True)
    if pd.isna(start_ts) or pd.isna(end_ts) or end_ts <= start_ts:
        raise ValueError("Invalid backtest window.")

    t0 = time.perf_counter()
    prepared_streams = dict(streams_full or {}) if bool(streams_already_htf_closed_only) else make_streams_htf_closed_only(dict(streams_full or {}))
    timing_prepare_streams_sec = time.perf_counter() - t0

    df1 = prepared_streams.get("1m")
    if df1 is None or df1.empty:
        raise ValueError("Compiled bar engine requires a non-empty 1m stream.")

    idx = pd.DatetimeIndex(pd.to_datetime(df1.index, utc=True, errors="coerce"))
    if idx.empty:
        raise ValueError("1m stream index is empty.")

    t0 = time.perf_counter()
    prev_source_row_by_tf = {
        tf: _prev_source_row_index_for_df(dft, tf)
        for tf, dft in prepared_streams.items()
        if isinstance(dft, pd.DataFrame) and not dft.empty
    }
    compiled_tabs = _compile_tab_signal_arrays(
        dict(rules_model or {}),
        dict(tab_group_join_mode or {}),
        {k: list(v) for k, v in dict(group_rule_join_mode or {}).items()},
        prepared_streams,
        prev_source_row_by_tf,
        len(df1),
    )
    timing_compile_signals_sec = time.perf_counter() - t0

    for tab_name in strategy_plan.active_tabs:
        if tab_name not in compiled_tabs:
            raise ValueError(f"Compiled signal generation failed for tab '{tab_name}'.")

    decision_mask = _decision_mask_for_step_tf(idx, cfg.step_timeframe)
    price_frame = _aligned_price_frame(df_1m_full, idx)
    candle_frame = price_frame if price_frame is not None else df1
    price_arr = _numeric_series(df1, "price", fallback=np.full(len(df1), np.nan, dtype=float))
    open_arr = _numeric_series(candle_frame, "open", fallback=np.full(len(df1), np.nan, dtype=float))
    open_arr = _fill_missing_numeric(open_arr, price_arr)
    high_arr = _numeric_series(candle_frame, "high", fallback=open_arr.copy())
    high_arr = _fill_missing_numeric(high_arr, open_arr)
    low_arr = _numeric_series(candle_frame, "low", fallback=open_arr.copy())
    low_arr = _fill_missing_numeric(low_arr, open_arr)
    close_arr = _numeric_series(candle_frame, "close", fallback=price_arr.copy())
    close_arr = _fill_missing_numeric(close_arr, open_arr)

    return CompiledBarPlan(
        symbol=str(symbol or ""),
        start=start_ts,
        end=end_ts,
        prepared_streams=prepared_streams,
        df_1m=df1,
        idx=idx,
        compiled_tabs=compiled_tabs,
        decision_mask=decision_mask,
        open_arr=open_arr,
        high_arr=high_arr,
        low_arr=low_arr,
        close_arr=close_arr,
        strategy_plan_engine=str(strategy_plan.engine),
        strategy_plan_notes=list(strategy_plan.notes),
        strategy_plan_blockers=list(strategy_plan.blockers),
        timing_prepare_streams_sec=float(timing_prepare_streams_sec),
        timing_compile_signals_sec=float(timing_compile_signals_sec),
    )


def _compiled_replay_context(
    *,
    plan: CompiledBarPlan,
    rules_model: Mapping[str, Sequence[Sequence[Any]]],
    tab_group_join_mode: Mapping[str, str],
    group_rule_join_mode: Mapping[str, Sequence[str]],
    entry_filters: Optional[Mapping[str, Any]],
) -> Dict[str, Any]:
    return {
        "symbol": str(plan.symbol or ""),
        "start": pd.to_datetime(plan.start, utc=True),
        "end": pd.to_datetime(plan.end, utc=True),
        "rules_model": dict(rules_model or {}),
        "tab_group_join_mode": dict(tab_group_join_mode or {}),
        "group_rule_join_mode": {k: list(v) for k, v in dict(group_rule_join_mode or {}).items()},
        "entry_filters": dict(entry_filters or {}),
        "__replay_engine": FAST_BAR_ENGINE,
        "__compiled_bar_plan": plan,
    }


def _compiled_replay_plan(plan: CompiledBarPlan | CompiledReplayPlan) -> CompiledReplayPlan:
    if isinstance(plan, CompiledReplayPlan):
        return plan
    if not isinstance(plan, CompiledBarPlan):
        raise TypeError("Expected CompiledBarPlan or CompiledReplayPlan.")
    return CompiledReplayPlan(
        symbol=str(plan.symbol or ""),
        start=pd.to_datetime(plan.start, utc=True),
        end=pd.to_datetime(plan.end, utc=True),
        idx=pd.DatetimeIndex(plan.idx),
        compiled_tabs={str(key): value for key, value in dict(plan.compiled_tabs or {}).items()},
        decision_mask=plan.decision_mask,
        open_arr=plan.open_arr,
        high_arr=plan.high_arr,
        low_arr=plan.low_arr,
        close_arr=plan.close_arr,
        strategy_plan_engine=str(plan.strategy_plan_engine or ""),
        strategy_plan_notes=list(plan.strategy_plan_notes or []),
        strategy_plan_blockers=list(plan.strategy_plan_blockers or []),
        timing_prepare_streams_sec=float(plan.timing_prepare_streams_sec),
        timing_compile_signals_sec=float(plan.timing_compile_signals_sec),
    )


def _compiled_replay_effective_capture_trade_details(
    cfg: BacktestConfig,
    capture_trade_details: Optional[bool],
) -> bool:
    if capture_trade_details is None:
        return bool(getattr(cfg, "capture_trade_details", True))
    return bool(capture_trade_details)


def _compiled_replay_event_driven_supported(
    cfg: BacktestConfig,
    *,
    capture_trade_details: Optional[bool],
) -> bool:
    if _compiled_replay_effective_capture_trade_details(cfg, capture_trade_details):
        return False
    stop_mode = str(getattr(cfg, "stop_loss_mode", "OFF") or "OFF").upper()
    take_mode = str(getattr(cfg, "take_profit_mode", "OFF") or "OFF").upper()
    return stop_mode in {"OFF", "PERCENT", "ABSOLUTE"} and take_mode in {"OFF", "PERCENT", "ABSOLUTE"}


def _compiled_trade_extrema(
    *,
    entry_fill_price_raw: float,
    entry_row_index: Optional[int],
    end_row_inclusive: Optional[int],
    high_arr: np.ndarray,
    low_arr: np.ndarray,
) -> tuple[float, float]:
    base_price = float(entry_fill_price_raw)
    if entry_row_index is None or end_row_inclusive is None:
        return (base_price, base_price)
    start_row = int(entry_row_index)
    end_row = int(end_row_inclusive)
    if end_row < start_row:
        return (base_price, base_price)
    high_slice = high_arr[start_row : end_row + 1]
    low_slice = low_arr[start_row : end_row + 1]
    if high_slice.size == 0 or low_slice.size == 0:
        return (base_price, base_price)
    try:
        trade_high = max(base_price, float(np.nanmax(high_slice)))
    except Exception:
        trade_high = base_price
    try:
        trade_low = min(base_price, float(np.nanmin(low_slice)))
    except Exception:
        trade_low = base_price
    return (float(trade_high), float(trade_low))


def _append_compiled_equity_segment(
    *,
    start_row: int,
    end_row: int,
    final_row: int,
    pos_side: int,
    balance: float,
    qty: float,
    entry_price: float,
    close_arr: np.ndarray,
    idx_ns: np.ndarray,
    sample_stride: int,
    peak_equity: float,
    max_drawdown_pct: float,
    equity_time_ns: List[int],
    equity_vals: List[float],
) -> tuple[float, float]:
    if end_row < start_row:
        return (float(peak_equity), float(max_drawdown_pct))

    row_ix = np.arange(int(start_row), int(end_row) + 1, dtype=np.int64)
    if pos_side == 0:
        equity_now = np.full(row_ix.size, float(balance), dtype=float)
    elif pos_side > 0:
        equity_now = float(balance) + (close_arr[start_row : end_row + 1] - float(entry_price)) * float(qty)
    else:
        equity_now = float(balance) + (float(entry_price) - close_arr[start_row : end_row + 1]) * float(qty)

    if equity_now.size == 0:
        return (float(peak_equity), float(max_drawdown_pct))

    segment_peaks = np.maximum(float(peak_equity), np.maximum.accumulate(equity_now))
    if bool(segment_peaks.size):
        peak_equity = max(float(peak_equity), float(segment_peaks[-1]))
        if np.any(segment_peaks > 0.0):
            drawdowns = np.where(segment_peaks > 0.0, ((segment_peaks - equity_now) / segment_peaks) * 100.0, 0.0)
            if bool(drawdowns.size):
                max_drawdown_pct = max(float(max_drawdown_pct), float(np.max(drawdowns)))

    sample_mask = ((row_ix % int(sample_stride)) == 0) | (row_ix == int(final_row))
    if bool(sample_mask.any()):
        sampled_rows = row_ix[sample_mask]
        equity_time_ns.extend([int(v) for v in idx_ns[sampled_rows].tolist()])
        equity_vals.extend([float(v) for v in equity_now[sample_mask].tolist()])

    return (float(peak_equity), float(max_drawdown_pct))


def _find_first_compiled_price_exit(
    *,
    side: str,
    entry_row_index: int,
    end_exclusive: int,
    entry_price: float,
    open_arr: np.ndarray,
    high_arr: np.ndarray,
    low_arr: np.ndarray,
    stop_price: Optional[float],
    take_profit_price: Optional[float],
    break_even_price: Optional[float],
    break_even_activation_price: Optional[float],
    trailing_stop_pct: float,
) -> Optional[CompiledReplayPriceExit]:
    has_levels = any(
        [
            stop_price is not None,
            take_profit_price is not None,
            break_even_price is not None and break_even_activation_price is not None,
            float(trailing_stop_pct or 0.0) > 0.0,
        ]
    )
    if not has_levels or int(end_exclusive) <= int(entry_row_index):
        return None

    bars = np.arange(int(entry_row_index), int(end_exclusive), dtype=np.int64)
    if bars.size == 0:
        return None

    open_seg = open_arr[bars]
    high_seg = high_arr[bars]
    low_seg = low_arr[bars]
    side_text = str(side or "").upper()

    trailing_prev: Optional[np.ndarray] = None
    if float(trailing_stop_pct or 0.0) > 0.0:
        frac = float(trailing_stop_pct) / 100.0
        prev_anchor = np.empty(bars.size, dtype=float)
        prev_anchor[0] = float(entry_price)
        if bars.size > 1:
            if side_text == "LONG":
                prev_anchor[1:] = np.maximum(float(entry_price), np.maximum.accumulate(high_seg[:-1]))
            else:
                prev_anchor[1:] = np.minimum(float(entry_price), np.minimum.accumulate(low_seg[:-1]))
        trailing_prev = prev_anchor * (1.0 - frac) if side_text == "LONG" else prev_anchor * (1.0 + frac)

    break_even_prev: Optional[np.ndarray] = None
    if break_even_price is not None and break_even_activation_price is not None:
        break_even_prev = np.zeros(bars.size, dtype=bool)
        if bars.size > 1:
            if side_text == "LONG":
                break_even_prev[1:] = np.maximum.accumulate(high_seg[:-1]) >= float(break_even_activation_price)
            else:
                break_even_prev[1:] = np.minimum.accumulate(low_seg[:-1]) <= float(break_even_activation_price)

    gap_exists = np.zeros(bars.size, dtype=bool)
    range_exists = np.zeros(bars.size, dtype=bool)

    if side_text == "LONG":
        if stop_price is not None:
            stop_hit_gap = open_seg <= float(stop_price)
            stop_hit_range = low_seg <= float(stop_price)
            gap_exists |= stop_hit_gap
            range_exists |= stop_hit_range
        if trailing_prev is not None:
            trail_hit_gap = open_seg <= trailing_prev
            trail_hit_range = low_seg <= trailing_prev
            gap_exists |= trail_hit_gap
            range_exists |= trail_hit_range
        if break_even_prev is not None and break_even_price is not None:
            be_hit_gap = break_even_prev & (open_seg <= float(break_even_price))
            be_hit_range = break_even_prev & (low_seg <= float(break_even_price))
            gap_exists |= be_hit_gap
            range_exists |= be_hit_range
        if take_profit_price is not None:
            tp_hit_gap = open_seg >= float(take_profit_price)
            tp_hit_range = high_seg >= float(take_profit_price)
            gap_exists |= tp_hit_gap
            range_exists |= tp_hit_range
    else:
        if stop_price is not None:
            stop_hit_gap = open_seg >= float(stop_price)
            stop_hit_range = high_seg >= float(stop_price)
            gap_exists |= stop_hit_gap
            range_exists |= stop_hit_range
        if trailing_prev is not None:
            trail_hit_gap = open_seg >= trailing_prev
            trail_hit_range = high_seg >= trailing_prev
            gap_exists |= trail_hit_gap
            range_exists |= trail_hit_range
        if break_even_prev is not None and break_even_price is not None:
            be_hit_gap = break_even_prev & (open_seg >= float(break_even_price))
            be_hit_range = break_even_prev & (high_seg >= float(break_even_price))
            gap_exists |= be_hit_gap
            range_exists |= be_hit_range
        if take_profit_price is not None:
            tp_hit_gap = open_seg <= float(take_profit_price)
            tp_hit_range = low_seg <= float(take_profit_price)
            gap_exists |= tp_hit_gap
            range_exists |= tp_hit_range

    if bars.size > 0:
        gap_exists[0] = False

    exit_positions = np.flatnonzero(gap_exists | range_exists)
    if exit_positions.size == 0:
        return None

    first_pos = int(exit_positions[0])
    row_index = int(bars[first_pos])
    trailing_price = None if trailing_prev is None else float(trailing_prev[first_pos])
    break_even_active = False if break_even_prev is None else bool(break_even_prev[first_pos])

    if bool(gap_exists[first_pos]):
        resolved = _resolve_ohlcv_open_gap_exit(
            side_text,
            float(open_seg[first_pos]),
            stop_price,
            take_profit_price,
            trailing_price,
            break_even_price,
            break_even_active,
        )
    else:
        resolved = _resolve_ohlcv_intrabar_range_exit(
            side_text,
            float(high_seg[first_pos]),
            float(low_seg[first_pos]),
            stop_price,
            take_profit_price,
            trailing_price,
            break_even_price,
            break_even_active,
        )
    if resolved is None:
        return None
    return CompiledReplayPriceExit(
        row_index=row_index,
        reason=str(resolved.reason),
        fill_price_raw=float(resolved.fill_price),
        ambiguous=bool(resolved.ambiguous),
        at_open=bool(resolved.at_open),
    )


def run_backtest_compiled_replay_event_driven(
    plan: CompiledBarPlan | CompiledReplayPlan,
    *,
    cfg: BacktestConfig,
    rules_model: Mapping[str, Sequence[Sequence[Any]]],
    tab_group_join_mode: Mapping[str, str],
    group_rule_join_mode: Mapping[str, Sequence[str]],
    entry_filters: Optional[Mapping[str, Any]] = None,
    replay_context: Optional[Mapping[str, Any]] = None,
) -> BacktestResult:
    t_start_total = time.perf_counter()
    replay_plan = _compiled_replay_plan(plan)
    idx = replay_plan.idx
    idx_ns = idx.asi8
    row_count = len(idx)
    start_i = max(1, int(idx.searchsorted(replay_plan.start, side="left")))
    end_i = min(row_count - 1, int(idx.searchsorted(replay_plan.end, side="right") - 1))
    if end_i < start_i:
        raise ValueError("No candles in requested compiled backtest window.")

    open_arr = replay_plan.open_arr
    high_arr = replay_plan.high_arr
    low_arr = replay_plan.low_arr
    close_arr = replay_plan.close_arr
    decision_mask = replay_plan.decision_mask
    compiled_tabs = replay_plan.compiled_tabs
    empty_signal = np.zeros(row_count, dtype=bool)
    long_entry_arr = compiled_tabs.get("Long Entry", empty_signal)
    long_exit_arr = compiled_tabs.get("Long Exit", empty_signal)
    short_entry_arr = compiled_tabs.get("Short Entry", empty_signal)
    short_exit_arr = compiled_tabs.get("Short Exit", empty_signal)

    allow_reverse = bool(cfg.allow_reverse)
    fee_rate = float(cfg.fee_rate)
    order_notional = float(cfg.order_notional_usdt)
    slippage_bps = float(cfg.slippage_bps)
    sample_stride = max(1, int(getattr(cfg, "equity_curve_stride", 1) or 1))

    stop_mode = str(getattr(cfg, "stop_loss_mode", "OFF") or "OFF").upper()
    take_mode = str(getattr(cfg, "take_profit_mode", "OFF") or "OFF").upper()
    stop_value = float(getattr(cfg, "stop_loss_value", getattr(cfg, "stop_loss_pct", 0.0)) or 0.0)
    take_value = float(getattr(cfg, "take_profit_value", getattr(cfg, "take_profit_pct", 0.0)) or 0.0)
    stop_pct_frac = (stop_value / 100.0) if stop_mode == "PERCENT" and stop_value > 0.0 else 0.0
    take_pct_frac = (take_value / 100.0) if take_mode == "PERCENT" and take_value > 0.0 else 0.0
    stop_abs = stop_value if stop_mode == "ABSOLUTE" and stop_value > 0.0 else 0.0
    take_abs = take_value if take_mode == "ABSOLUTE" and take_value > 0.0 else 0.0
    stop_enabled = (stop_mode == "PERCENT" and stop_pct_frac > 0.0) or (stop_mode == "ABSOLUTE" and stop_abs > 0.0)
    take_enabled = (take_mode == "PERCENT" and take_pct_frac > 0.0) or (take_mode == "ABSOLUTE" and take_abs > 0.0)

    decision_eval_mask = np.zeros(row_count, dtype=bool)
    decision_eval_mask[start_i : end_i + 1] = decision_mask[start_i : end_i + 1]
    can_schedule_mask = np.zeros(row_count, dtype=bool)
    can_schedule_mask[start_i:end_i] = True

    flat_open_long_mask = decision_eval_mask & can_schedule_mask & long_entry_arr & ~short_entry_arr
    flat_open_short_mask = decision_eval_mask & can_schedule_mask & short_entry_arr & ~long_entry_arr
    flat_open_indices = np.flatnonzero(flat_open_long_mask | flat_open_short_mask)
    flat_open_sides = np.where(flat_open_long_mask[flat_open_indices], 1, -1)

    long_reverse_mask = decision_eval_mask & can_schedule_mask & short_entry_arr if allow_reverse else np.zeros(row_count, dtype=bool)
    long_close_mask = decision_eval_mask & can_schedule_mask & long_exit_arr & ~long_reverse_mask
    short_reverse_mask = decision_eval_mask & can_schedule_mask & long_entry_arr if allow_reverse else np.zeros(row_count, dtype=bool)
    short_close_mask = decision_eval_mask & can_schedule_mask & short_exit_arr & ~short_reverse_mask

    long_action_indices = np.flatnonzero(long_reverse_mask | long_close_mask)
    long_action_codes = np.where(long_reverse_mask[long_action_indices], 2, 1)
    short_action_indices = np.flatnonzero(short_reverse_mask | short_close_mask)
    short_action_codes = np.where(short_reverse_mask[short_action_indices], -2, -1)

    balance = float(cfg.initial_balance)
    trade_id = 0
    peak_equity = float(cfg.initial_balance)
    max_drawdown_pct = 0.0
    equity_time_ns: List[int] = []
    equity_vals: List[float] = []
    trades: List[Trade] = []

    pos_side = 0
    qty = 0.0
    entry_price = 0.0
    entry_fill_price_raw = 0.0
    entry_time_ns: Optional[int] = None
    entry_row_index: Optional[int] = None
    entry_reason = ""
    fee_entry = 0.0
    stop_price: Optional[float] = None
    take_profit_price: Optional[float] = None
    break_even_price: Optional[float] = None
    break_even_activation_price: Optional[float] = None
    trailing_stop_pct = _trailing_stop_pct(cfg)

    def _bar_open_price(row_index: int) -> float:
        raw_open = float(open_arr[row_index]) if np.isfinite(open_arr[row_index]) else float(close_arr[row_index])
        return float(raw_open)

    def _next_flat_open_signal(current_row: int) -> tuple[Optional[int], int]:
        pos = int(np.searchsorted(flat_open_indices, int(current_row), side="left"))
        if pos >= int(flat_open_indices.size):
            return (None, 0)
        return (int(flat_open_indices[pos]), int(flat_open_sides[pos]))

    def _next_position_action(current_row: int, current_side: int) -> tuple[Optional[int], int]:
        action_indices = long_action_indices if current_side > 0 else short_action_indices
        action_codes = long_action_codes if current_side > 0 else short_action_codes
        pos = int(np.searchsorted(action_indices, int(current_row), side="left"))
        if pos >= int(action_indices.size):
            return (None, 0)
        return (int(action_indices[pos]), int(action_codes[pos]))

    def _open_position(side_text: str, row_index: int, reason: str) -> None:
        nonlocal balance, pos_side, qty, entry_price, entry_fill_price_raw, entry_time_ns, entry_row_index, entry_reason
        nonlocal fee_entry, stop_price, take_profit_price, break_even_price, break_even_activation_price

        entry_fill_price_raw = _bar_open_price(row_index)
        action = "BUY" if str(side_text).upper() == "LONG" else "SELL"
        fill_price = _apply_slippage(float(entry_fill_price_raw), action, slippage_bps)
        qty = order_notional / fill_price
        fee_entry = fee_rate * (qty * fill_price)
        balance -= fee_entry
        pos_side = 1 if str(side_text).upper() == "LONG" else -1
        entry_price = float(fill_price)
        entry_time_ns = int(idx_ns[row_index])
        entry_row_index = int(row_index)
        entry_reason = str(reason)

        if pos_side > 0:
            stop_price = (fill_price * (1.0 - stop_pct_frac)) if stop_mode == "PERCENT" and stop_enabled else ((fill_price - stop_abs) if stop_mode == "ABSOLUTE" and stop_enabled else None)
            take_profit_price = (fill_price * (1.0 + take_pct_frac)) if take_mode == "PERCENT" and take_enabled else ((fill_price + take_abs) if take_mode == "ABSOLUTE" and take_enabled else None)
        else:
            stop_price = (fill_price * (1.0 + stop_pct_frac)) if stop_mode == "PERCENT" and stop_enabled else ((fill_price + stop_abs) if stop_mode == "ABSOLUTE" and stop_enabled else None)
            take_profit_price = (fill_price * (1.0 - take_pct_frac)) if take_mode == "PERCENT" and take_enabled else ((fill_price - take_abs) if take_mode == "ABSOLUTE" and take_enabled else None)

        break_even_price, break_even_activation_price = _compute_break_even_prices(fill_price, side_text, cfg)

    def _close_position(
        ts_ns: int,
        fill_price_raw: float,
        reason: str,
        *,
        ambiguous: bool = False,
        exit_row_index: Optional[int] = None,
        extrema_end_row: Optional[int] = None,
    ) -> None:
        nonlocal balance, pos_side, qty, entry_price, entry_fill_price_raw, entry_time_ns, entry_row_index, entry_reason
        nonlocal fee_entry, stop_price, take_profit_price, break_even_price, break_even_activation_price, trade_id

        if pos_side == 0 or entry_time_ns is None:
            return

        side_text = "LONG" if pos_side > 0 else "SHORT"
        action = "SELL" if pos_side > 0 else "BUY"
        fill_price = _apply_slippage(float(fill_price_raw), action, slippage_bps)
        gross = (fill_price - entry_price) * qty if pos_side > 0 else (entry_price - fill_price) * qty
        exit_notional = qty * fill_price
        fee_exit = fee_rate * exit_notional
        net = gross - fee_entry - fee_exit
        balance += gross - fee_exit
        trade_id += 1

        trade_high, trade_low = _compiled_trade_extrema(
            entry_fill_price_raw=float(entry_fill_price_raw),
            entry_row_index=entry_row_index,
            end_row_inclusive=extrema_end_row,
            high_arr=high_arr,
            low_arr=low_arr,
        )
        if pos_side > 0:
            raw_mfe = max(0.0, float(trade_high - entry_price))
            raw_mae = min(0.0, float(trade_low - entry_price))
        else:
            raw_mfe = max(0.0, float(entry_price - trade_low))
            raw_mae = min(0.0, float(entry_price - trade_high))

        entry_notional = qty * entry_price
        entry_time = _timestamp_from_ns(entry_time_ns)
        exit_time = _timestamp_from_ns(ts_ns)
        duration_min = int((ts_ns - entry_time_ns) // 60_000_000_000)
        trades.append(
            Trade(
                trade_id=trade_id,
                side=side_text,
                entry_time=entry_time,
                entry_price=float(entry_price),
                exit_time=exit_time,
                exit_price=float(fill_price),
                qty=float(qty),
                entry_notional=float(entry_notional),
                exit_notional=float(exit_notional),
                gross_pnl=float(gross),
                fees=float(fee_entry + fee_exit),
                net_pnl=float(net),
                return_pct=float((net / entry_notional) * 100.0) if entry_notional > 0 else 0.0,
                duration_min=int(duration_min),
                mfe=float(raw_mfe),
                mae=float(raw_mae),
                mfe_pct=float((raw_mfe / entry_price) * 100.0) if entry_price > 0 else 0.0,
                mae_pct=float((raw_mae / entry_price) * 100.0) if entry_price > 0 else 0.0,
                entry_reason=str(entry_reason),
                exit_reason=str(reason),
                entry_rule_trace="",
                exit_rule_trace="",
                entry_snapshot={},
                exit_snapshot={},
                ambiguous_exit=bool(ambiguous),
                engine_type=FAST_BAR_ENGINE,
                fill_source="BAR_OPEN" if reason.startswith("Reverse") or reason.endswith("Exit") or reason == "Force Close (End)" or reason.endswith("Entry") else "INTRABAR",
                stop_loss_price=float(stop_price) if stop_price is not None else None,
                take_profit_price=float(take_profit_price) if take_profit_price is not None else None,
                entry_row_index=int(entry_row_index) if entry_row_index is not None else None,
                exit_row_index=int(exit_row_index) if exit_row_index is not None else None,
            )
        )

        pos_side = 0
        qty = 0.0
        entry_price = 0.0
        entry_fill_price_raw = 0.0
        entry_time_ns = None
        entry_row_index = None
        entry_reason = ""
        fee_entry = 0.0
        stop_price = None
        take_profit_price = None
        break_even_price = None
        break_even_activation_price = None

    current_row = int(start_i)
    t0 = time.perf_counter()
    while current_row <= int(end_i):
        if pos_side == 0:
            signal_row, side_code = _next_flat_open_signal(current_row)
            if signal_row is None:
                peak_equity, max_drawdown_pct = _append_compiled_equity_segment(
                    start_row=current_row,
                    end_row=int(end_i),
                    final_row=int(end_i),
                    pos_side=0,
                    balance=float(balance),
                    qty=0.0,
                    entry_price=0.0,
                    close_arr=close_arr,
                    idx_ns=idx_ns,
                    sample_stride=sample_stride,
                    peak_equity=peak_equity,
                    max_drawdown_pct=max_drawdown_pct,
                    equity_time_ns=equity_time_ns,
                    equity_vals=equity_vals,
                )
                break

            exec_row = int(signal_row) + 1
            flat_end = min(int(end_i), exec_row - 1)
            if flat_end >= current_row:
                peak_equity, max_drawdown_pct = _append_compiled_equity_segment(
                    start_row=current_row,
                    end_row=flat_end,
                    final_row=int(end_i),
                    pos_side=0,
                    balance=float(balance),
                    qty=0.0,
                    entry_price=0.0,
                    close_arr=close_arr,
                    idx_ns=idx_ns,
                    sample_stride=sample_stride,
                    peak_equity=peak_equity,
                    max_drawdown_pct=max_drawdown_pct,
                    equity_time_ns=equity_time_ns,
                    equity_vals=equity_vals,
                )
            if exec_row > int(end_i):
                break
            _open_position("LONG" if side_code > 0 else "SHORT", exec_row, "Long Entry" if side_code > 0 else "Short Entry")
            current_row = exec_row
            continue

        signal_row, action_code = _next_position_action(current_row, pos_side)
        rule_exec_row = (int(signal_row) + 1) if signal_row is not None else (int(end_i) + 1)
        search_end = min(rule_exec_row, int(end_i) + 1)
        price_exit = _find_first_compiled_price_exit(
            side="LONG" if pos_side > 0 else "SHORT",
            entry_row_index=int(entry_row_index) if entry_row_index is not None else current_row,
            end_exclusive=search_end,
            entry_price=float(entry_price),
            open_arr=open_arr,
            high_arr=high_arr,
            low_arr=low_arr,
            stop_price=stop_price,
            take_profit_price=take_profit_price,
            break_even_price=break_even_price,
            break_even_activation_price=break_even_activation_price,
            trailing_stop_pct=trailing_stop_pct,
        )

        if price_exit is not None and price_exit.row_index is not None:
            active_end = int(price_exit.row_index) - 1
            if active_end >= current_row:
                peak_equity, max_drawdown_pct = _append_compiled_equity_segment(
                    start_row=current_row,
                    end_row=active_end,
                    final_row=int(end_i),
                    pos_side=pos_side,
                    balance=float(balance),
                    qty=float(qty),
                    entry_price=float(entry_price),
                    close_arr=close_arr,
                    idx_ns=idx_ns,
                    sample_stride=sample_stride,
                    peak_equity=peak_equity,
                    max_drawdown_pct=max_drawdown_pct,
                    equity_time_ns=equity_time_ns,
                    equity_vals=equity_vals,
                )
            extrema_end = int(price_exit.row_index) - 1 if price_exit.at_open else int(price_exit.row_index)
            _close_position(
                int(idx_ns[int(price_exit.row_index)]),
                float(price_exit.fill_price_raw),
                str(price_exit.reason),
                ambiguous=bool(price_exit.ambiguous),
                exit_row_index=int(price_exit.row_index),
                extrema_end_row=extrema_end,
            )
            current_row = int(price_exit.row_index)
            continue

        if rule_exec_row <= int(end_i):
            active_end = rule_exec_row - 1
            if active_end >= current_row:
                peak_equity, max_drawdown_pct = _append_compiled_equity_segment(
                    start_row=current_row,
                    end_row=active_end,
                    final_row=int(end_i),
                    pos_side=pos_side,
                    balance=float(balance),
                    qty=float(qty),
                    entry_price=float(entry_price),
                    close_arr=close_arr,
                    idx_ns=idx_ns,
                    sample_stride=sample_stride,
                    peak_equity=peak_equity,
                    max_drawdown_pct=max_drawdown_pct,
                    equity_time_ns=equity_time_ns,
                    equity_vals=equity_vals,
                )
            exec_open = _bar_open_price(rule_exec_row)
            if action_code == 2:
                _close_position(
                    int(idx_ns[rule_exec_row]),
                    exec_open,
                    "Reverse to Short",
                    exit_row_index=rule_exec_row,
                    extrema_end_row=rule_exec_row - 1,
                )
                _open_position("SHORT", rule_exec_row, "Reverse to Short")
            elif action_code == -2:
                _close_position(
                    int(idx_ns[rule_exec_row]),
                    exec_open,
                    "Reverse to Long",
                    exit_row_index=rule_exec_row,
                    extrema_end_row=rule_exec_row - 1,
                )
                _open_position("LONG", rule_exec_row, "Reverse to Long")
            elif action_code == 1:
                _close_position(
                    int(idx_ns[rule_exec_row]),
                    exec_open,
                    "Long Exit",
                    exit_row_index=rule_exec_row,
                    extrema_end_row=rule_exec_row - 1,
                )
            elif action_code == -1:
                _close_position(
                    int(idx_ns[rule_exec_row]),
                    exec_open,
                    "Short Exit",
                    exit_row_index=rule_exec_row,
                    extrema_end_row=rule_exec_row - 1,
                )
            current_row = rule_exec_row
            continue

        peak_equity, max_drawdown_pct = _append_compiled_equity_segment(
            start_row=current_row,
            end_row=int(end_i),
            final_row=int(end_i),
            pos_side=pos_side,
            balance=float(balance),
            qty=float(qty),
            entry_price=float(entry_price),
            close_arr=close_arr,
            idx_ns=idx_ns,
            sample_stride=sample_stride,
            peak_equity=peak_equity,
            max_drawdown_pct=max_drawdown_pct,
            equity_time_ns=equity_time_ns,
            equity_vals=equity_vals,
        )
        final_price = float(close_arr[end_i]) if np.isfinite(close_arr[end_i]) else float(open_arr[end_i])
        _close_position(
            int(idx_ns[end_i]),
            final_price,
            "Force Close (End)",
            exit_row_index=int(end_i),
            extrema_end_row=int(end_i),
        )
        break

    timing_simulation_sec = time.perf_counter() - t0

    equity_curve = pd.DataFrame(
        {
            "time": pd.to_datetime(np.array(equity_time_ns, dtype=np.int64), utc=True, errors="coerce"),
            "equity": equity_vals,
        }
    )
    if not equity_curve.empty:
        equity_curve = equity_curve.set_index("time")

    summary = _make_summary(
        cfg=cfg,
        start=replay_plan.start,
        end=replay_plan.end,
        trades=trades,
        ending_balance=float(balance),
        max_drawdown_pct=float(max_drawdown_pct),
        plan_engine=FAST_BAR_ENGINE,
    )
    summary["strategy_plan_engine"] = replay_plan.strategy_plan_engine
    summary["strategy_plan_notes"] = list(replay_plan.strategy_plan_notes)
    summary["strategy_plan_blockers"] = list(replay_plan.strategy_plan_blockers)
    summary["timing_prepare_streams_sec"] = 0.0
    summary["timing_compile_signals_sec"] = 0.0
    summary["timing_simulation_sec"] = float(timing_simulation_sec)
    summary["timing_total_sec"] = float(time.perf_counter() - t_start_total)
    summary["compiled_plan_reused"] = True
    summary["compiled_plan_prepare_streams_sec"] = float(replay_plan.timing_prepare_streams_sec)
    summary["compiled_plan_compile_signals_sec"] = float(replay_plan.timing_compile_signals_sec)
    summary["compiled_replay_mode"] = "event_driven"

    next_replay_context = replay_context
    if next_replay_context is None:
        next_replay_context = _compiled_replay_context(
            plan=plan,
            rules_model=rules_model,
            tab_group_join_mode=tab_group_join_mode,
            group_rule_join_mode=group_rule_join_mode,
            entry_filters=entry_filters,
        )

    return BacktestResult(
        config=BacktestConfig(**asdict(cfg)),
        start=replay_plan.start,
        end=replay_plan.end,
        trades=trades,
        equity_curve=equity_curve,
        summary=summary,
        replay_context=dict(next_replay_context or {}),
    )


def run_backtest_compiled_plan(
    plan: CompiledBarPlan,
    *,
    cfg: BacktestConfig,
    rules_model: Mapping[str, Sequence[Sequence[Any]]],
    tab_group_join_mode: Mapping[str, str],
    group_rule_join_mode: Mapping[str, Sequence[str]],
    entry_filters: Optional[Mapping[str, Any]] = None,
    reuse_compiled_plan: bool = False,
    replay_context: Optional[Mapping[str, Any]] = None,
) -> BacktestResult:
    t_start_total = time.perf_counter()
    idx = plan.idx
    idx_ns = idx.asi8
    df1 = plan.df_1m
    row_count = len(df1)
    start_i = max(1, int(idx.searchsorted(plan.start, side="left")))
    end_i = min(row_count - 1, int(idx.searchsorted(plan.end, side="right") - 1))
    if end_i < start_i:
        raise ValueError("No candles in requested compiled backtest window.")

    open_arr = plan.open_arr
    high_arr = plan.high_arr
    low_arr = plan.low_arr
    close_arr = plan.close_arr
    decision_mask = plan.decision_mask
    compiled_tabs = plan.compiled_tabs
    empty_signal = np.zeros(row_count, dtype=bool)
    long_entry_arr = compiled_tabs.get("Long Entry", empty_signal)
    long_exit_arr = compiled_tabs.get("Long Exit", empty_signal)
    short_entry_arr = compiled_tabs.get("Short Entry", empty_signal)
    short_exit_arr = compiled_tabs.get("Short Exit", empty_signal)

    allow_reverse = bool(cfg.allow_reverse)
    skip_if_both_entries = bool(cfg.skip_if_both_entries)
    fee_rate = float(cfg.fee_rate)
    order_notional = float(cfg.order_notional_usdt)
    slippage_bps = float(cfg.slippage_bps)
    sample_stride = max(1, int(getattr(cfg, "equity_curve_stride", 1) or 1))

    stop_mode = str(getattr(cfg, "stop_loss_mode", "OFF") or "OFF").upper()
    take_mode = str(getattr(cfg, "take_profit_mode", "OFF") or "OFF").upper()
    stop_value = float(getattr(cfg, "stop_loss_value", getattr(cfg, "stop_loss_pct", 0.0)) or 0.0)
    take_value = float(getattr(cfg, "take_profit_value", getattr(cfg, "take_profit_pct", 0.0)) or 0.0)
    stop_pct_frac = (stop_value / 100.0) if stop_mode == "PERCENT" and stop_value > 0.0 else 0.0
    take_pct_frac = (take_value / 100.0) if take_mode == "PERCENT" and take_value > 0.0 else 0.0
    stop_abs = stop_value if stop_mode == "ABSOLUTE" and stop_value > 0.0 else 0.0
    take_abs = take_value if take_mode == "ABSOLUTE" and take_value > 0.0 else 0.0
    stop_enabled = (stop_mode == "PERCENT" and stop_pct_frac > 0.0) or (stop_mode == "ABSOLUTE" and stop_abs > 0.0)
    take_enabled = (take_mode == "PERCENT" and take_pct_frac > 0.0) or (take_mode == "ABSOLUTE" and take_abs > 0.0)

    balance = float(cfg.initial_balance)
    trade_id = 0
    peak_equity = float(cfg.initial_balance)
    max_drawdown_pct = 0.0
    equity_time_ns: List[int] = []
    equity_vals: List[float] = []
    trades: List[Trade] = []

    pos_side = 0
    qty = 0.0
    entry_price = 0.0
    entry_time_ns: Optional[int] = None
    entry_row_index: Optional[int] = None
    entry_reason = ""
    fee_entry = 0.0
    stop_price: Optional[float] = None
    take_profit_price: Optional[float] = None
    break_even_price: Optional[float] = None
    break_even_activation_price: Optional[float] = None
    break_even_armed_row_index: Optional[int] = None
    trailing_stop_pct = _trailing_stop_pct(cfg)
    trailing_stop_price: Optional[float] = None
    trailing_stop_anchor_price: Optional[float] = None
    trade_high: Optional[float] = None
    trade_low: Optional[float] = None
    scheduled_close_reason: Optional[str] = None
    scheduled_open_side: Optional[str] = None
    scheduled_open_reason: str = ""

    def _close_position(ts_ns: int, fill_price_raw: float, reason: str, *, ambiguous: bool = False, exit_row_index: Optional[int] = None) -> None:
        nonlocal balance, pos_side, qty, entry_price, entry_time_ns, entry_reason, fee_entry
        nonlocal stop_price, take_profit_price, break_even_price, break_even_activation_price, break_even_armed_row_index
        nonlocal trailing_stop_price, trailing_stop_anchor_price
        nonlocal trade_high, trade_low, trade_id, entry_row_index
        if pos_side == 0 or entry_time_ns is None:
            return
        side_text = "LONG" if pos_side > 0 else "SHORT"
        action = "SELL" if pos_side > 0 else "BUY"
        fill_price = _apply_slippage(float(fill_price_raw), action, slippage_bps)
        gross = (fill_price - entry_price) * qty if pos_side > 0 else (entry_price - fill_price) * qty
        exit_notional = qty * fill_price
        fee_exit = fee_rate * exit_notional
        net = gross - fee_entry - fee_exit
        balance += gross - fee_exit
        trade_id += 1

        raw_mfe = 0.0
        raw_mae = 0.0
        if pos_side > 0:
            raw_mfe = max(0.0, float((trade_high if trade_high is not None else entry_price) - entry_price))
            raw_mae = min(0.0, float((trade_low if trade_low is not None else entry_price) - entry_price))
        else:
            raw_mfe = max(0.0, float(entry_price - (trade_low if trade_low is not None else entry_price)))
            raw_mae = min(0.0, float(entry_price - (trade_high if trade_high is not None else entry_price)))

        entry_notional = qty * entry_price
        entry_time = _timestamp_from_ns(entry_time_ns)
        exit_time = _timestamp_from_ns(ts_ns)
        duration_min = int((ts_ns - entry_time_ns) // 60_000_000_000)
        trades.append(
            Trade(
                trade_id=trade_id,
                side=side_text,
                entry_time=entry_time,
                entry_price=float(entry_price),
                exit_time=exit_time,
                exit_price=float(fill_price),
                qty=float(qty),
                entry_notional=float(entry_notional),
                exit_notional=float(exit_notional),
                gross_pnl=float(gross),
                fees=float(fee_entry + fee_exit),
                net_pnl=float(net),
                return_pct=float((net / entry_notional) * 100.0) if entry_notional > 0 else 0.0,
                duration_min=int(duration_min),
                mfe=float(raw_mfe),
                mae=float(raw_mae),
                mfe_pct=float((raw_mfe / entry_price) * 100.0) if entry_price > 0 else 0.0,
                mae_pct=float((raw_mae / entry_price) * 100.0) if entry_price > 0 else 0.0,
                entry_reason=str(entry_reason),
                exit_reason=str(reason),
                entry_rule_trace="",
                exit_rule_trace="",
                entry_snapshot={},
                exit_snapshot={},
                ambiguous_exit=bool(ambiguous),
                engine_type=FAST_BAR_ENGINE,
                fill_source="BAR_OPEN" if reason.startswith("Reverse") or reason.endswith("Exit") or reason == "Force Close (End)" or reason.endswith("Entry") else "INTRABAR",
                stop_loss_price=float(stop_price) if stop_price is not None else None,
                take_profit_price=float(take_profit_price) if take_profit_price is not None else None,
                entry_row_index=int(entry_row_index) if entry_row_index is not None else None,
                exit_row_index=int(exit_row_index) if exit_row_index is not None else None,
            )
        )

        pos_side = 0
        qty = 0.0
        entry_price = 0.0
        entry_time_ns = None
        entry_row_index = None
        entry_reason = ""
        fee_entry = 0.0
        stop_price = None
        take_profit_price = None
        break_even_price = None
        break_even_activation_price = None
        break_even_armed_row_index = None
        trailing_stop_price = None
        trailing_stop_anchor_price = None
        trade_high = None
        trade_low = None

    def _open_position(side: str, ts_ns: int, fill_price_raw: float, reason: str, row_index: int) -> None:
        nonlocal balance, pos_side, qty, entry_price, entry_time_ns, entry_reason, fee_entry
        nonlocal stop_price, take_profit_price, break_even_price, break_even_activation_price, break_even_armed_row_index
        nonlocal trailing_stop_price, trailing_stop_anchor_price
        nonlocal trade_high, trade_low, entry_row_index
        side_text = str(side).upper()
        action = "BUY" if side_text == "LONG" else "SELL"
        fill_price = _apply_slippage(float(fill_price_raw), action, slippage_bps)
        qty = order_notional / fill_price
        fee_entry = fee_rate * (qty * fill_price)
        balance -= fee_entry
        pos_side = 1 if side_text == "LONG" else -1
        entry_price = float(fill_price)
        entry_time_ns = int(ts_ns)
        entry_row_index = int(row_index)
        entry_reason = str(reason)
        if side_text == "LONG":
            stop_price = (fill_price * (1.0 - stop_pct_frac)) if stop_mode == "PERCENT" and stop_enabled else ((fill_price - stop_abs) if stop_mode == "ABSOLUTE" and stop_enabled else None)
            take_profit_price = (fill_price * (1.0 + take_pct_frac)) if take_mode == "PERCENT" and take_enabled else ((fill_price + take_abs) if take_mode == "ABSOLUTE" and take_enabled else None)
        else:
            stop_price = (fill_price * (1.0 + stop_pct_frac)) if stop_mode == "PERCENT" and stop_enabled else ((fill_price + stop_abs) if stop_mode == "ABSOLUTE" and stop_enabled else None)
            take_profit_price = (fill_price * (1.0 - take_pct_frac)) if take_mode == "PERCENT" and take_enabled else ((fill_price - take_abs) if take_mode == "ABSOLUTE" and take_enabled else None)
        break_even_price, break_even_activation_price = _compute_break_even_prices(fill_price, side_text, cfg)
        break_even_armed_row_index = None
        trailing_stop_price, trailing_stop_anchor_price = _compute_initial_trailing_stop(fill_price, side_text, cfg)
        trade_high = float(fill_price_raw)
        trade_low = float(fill_price_raw)

    def _update_break_even_state(row_index: int, bar_high: float, bar_low: float) -> None:
        nonlocal break_even_armed_row_index
        if pos_side == 0 or break_even_armed_row_index is not None or break_even_activation_price is None:
            return
        if pos_side > 0 and float(bar_high) >= float(break_even_activation_price):
            break_even_armed_row_index = int(row_index)
        elif pos_side < 0 and float(bar_low) <= float(break_even_activation_price):
            break_even_armed_row_index = int(row_index)

    def _update_trailing_state(bar_high: float, bar_low: float) -> None:
        nonlocal trailing_stop_anchor_price, trailing_stop_price
        if pos_side == 0 or trailing_stop_pct <= 0.0:
            return
        trailing_stop_anchor_price, trailing_stop_price = _update_tick_trailing_stop_state(
            "LONG" if pos_side > 0 else "SHORT",
            float(bar_high) if pos_side > 0 else float(bar_low),
            trailing_stop_pct,
            trailing_stop_anchor_price,
            trailing_stop_price,
        )

    t0 = time.perf_counter()
    for j in range(start_i, end_i + 1):
        ts_ns = int(idx_ns[j])

        bar_open = float(open_arr[j]) if np.isfinite(open_arr[j]) else float(close_arr[j])
        bar_high = float(high_arr[j]) if np.isfinite(high_arr[j]) else bar_open
        bar_low = float(low_arr[j]) if np.isfinite(low_arr[j]) else bar_open
        bar_close = float(close_arr[j]) if np.isfinite(close_arr[j]) else bar_open

        if scheduled_close_reason and pos_side != 0:
            _close_position(ts_ns, bar_open, scheduled_close_reason, exit_row_index=j)
        if scheduled_open_side and pos_side == 0:
            _open_position(scheduled_open_side, ts_ns, bar_open, scheduled_open_reason, j)
        scheduled_close_reason = None
        scheduled_open_side = None
        scheduled_open_reason = ""

        if pos_side != 0 and entry_row_index is not None:
            carried_in = j > entry_row_index
            if carried_in:
                break_even_active = _break_even_active_for_bar(break_even_armed_row_index, j)
                gap_exit = _resolve_ohlcv_open_gap_exit(
                    "LONG" if pos_side > 0 else "SHORT",
                    bar_open,
                    stop_price,
                    take_profit_price,
                    trailing_stop_price,
                    break_even_price,
                    break_even_active,
                )
                if gap_exit is not None:
                    _close_position(ts_ns, gap_exit.fill_price, gap_exit.reason, ambiguous=gap_exit.ambiguous, exit_row_index=j)
            if pos_side != 0:
                if trade_high is None:
                    trade_high = bar_high
                else:
                    trade_high = max(float(trade_high), bar_high)
                if trade_low is None:
                    trade_low = bar_low
                else:
                    trade_low = min(float(trade_low), bar_low)
                break_even_active = _break_even_active_for_bar(break_even_armed_row_index, j)
                range_exit = _resolve_ohlcv_intrabar_range_exit(
                    "LONG" if pos_side > 0 else "SHORT",
                    bar_high,
                    bar_low,
                    stop_price,
                    take_profit_price,
                    trailing_stop_price,
                    break_even_price,
                    break_even_active,
                )
                if range_exit is not None:
                    _close_position(ts_ns, range_exit.fill_price, range_exit.reason, ambiguous=range_exit.ambiguous, exit_row_index=j)
            if pos_side != 0:
                _update_break_even_state(j, bar_high, bar_low)
                _update_trailing_state(bar_high, bar_low)

        if decision_mask[j]:
            long_entry = bool(long_entry_arr[j])
            long_exit = bool(long_exit_arr[j])
            short_entry = bool(short_entry_arr[j])
            short_exit = bool(short_exit_arr[j])
            can_schedule_next = j < end_i

            if pos_side > 0:
                if short_entry and allow_reverse and can_schedule_next:
                    scheduled_close_reason = "Reverse to Short"
                    scheduled_open_side = "SHORT"
                    scheduled_open_reason = "Reverse to Short"
                elif long_exit and can_schedule_next:
                    scheduled_close_reason = "Long Exit"
            elif pos_side < 0:
                if long_entry and allow_reverse and can_schedule_next:
                    scheduled_close_reason = "Reverse to Long"
                    scheduled_open_side = "LONG"
                    scheduled_open_reason = "Reverse to Long"
                elif short_exit and can_schedule_next:
                    scheduled_close_reason = "Short Exit"
            else:
                if long_entry and short_entry:
                    if not skip_if_both_entries:
                        pass
                elif long_entry and can_schedule_next:
                    scheduled_open_side = "LONG"
                    scheduled_open_reason = "Long Entry"
                elif short_entry and can_schedule_next:
                    scheduled_open_side = "SHORT"
                    scheduled_open_reason = "Short Entry"

        equity_now = _equity_mark(balance, pos_side, qty, entry_price, bar_close)
        peak_equity = max(float(peak_equity), float(equity_now))
        if peak_equity > 0:
            max_drawdown_pct = max(max_drawdown_pct, ((peak_equity - equity_now) / peak_equity) * 100.0)
        if (j % sample_stride == 0) or j == end_i:
            equity_time_ns.append(ts_ns)
            equity_vals.append(float(equity_now))

    timing_simulation_sec = time.perf_counter() - t0

    if pos_side != 0 and entry_time_ns is not None:
        final_price = float(close_arr[end_i]) if np.isfinite(close_arr[end_i]) else float(open_arr[end_i])
        _close_position(int(idx_ns[end_i]), final_price, "Force Close (End)", exit_row_index=end_i)

    equity_curve = pd.DataFrame(
        {
            "time": pd.to_datetime(np.array(equity_time_ns, dtype=np.int64), utc=True, errors="coerce"),
            "equity": equity_vals,
        }
    )
    if not equity_curve.empty:
        equity_curve = equity_curve.set_index("time")

    summary = _make_summary(
        cfg=cfg,
        start=plan.start,
        end=plan.end,
        trades=trades,
        ending_balance=float(balance),
        max_drawdown_pct=float(max_drawdown_pct),
        plan_engine=FAST_BAR_ENGINE,
    )
    summary["strategy_plan_engine"] = plan.strategy_plan_engine
    summary["strategy_plan_notes"] = list(plan.strategy_plan_notes)
    summary["strategy_plan_blockers"] = list(plan.strategy_plan_blockers)
    summary["timing_prepare_streams_sec"] = 0.0 if reuse_compiled_plan else float(plan.timing_prepare_streams_sec)
    summary["timing_compile_signals_sec"] = 0.0 if reuse_compiled_plan else float(plan.timing_compile_signals_sec)
    summary["timing_simulation_sec"] = float(timing_simulation_sec)
    summary["timing_total_sec"] = float(time.perf_counter() - t_start_total)
    summary["compiled_plan_reused"] = bool(reuse_compiled_plan)
    if reuse_compiled_plan:
        summary["compiled_plan_prepare_streams_sec"] = float(plan.timing_prepare_streams_sec)
        summary["compiled_plan_compile_signals_sec"] = float(plan.timing_compile_signals_sec)

    next_replay_context = replay_context
    if next_replay_context is None:
        next_replay_context = _compiled_replay_context(
            plan=plan,
            rules_model=rules_model,
            tab_group_join_mode=tab_group_join_mode,
            group_rule_join_mode=group_rule_join_mode,
            entry_filters=entry_filters,
        )

    return BacktestResult(
        config=BacktestConfig(**asdict(cfg)),
        start=plan.start,
        end=plan.end,
        trades=trades,
        equity_curve=equity_curve,
        summary=summary,
        replay_context=dict(next_replay_context or {}),
    )


def run_backtest_compiled_bar(
    *,
    symbol: str,
    streams_full: Mapping[str, pd.DataFrame],
    df_1m_full: Optional[pd.DataFrame] = None,
    start: Any,
    end: Any,
    rules_model: Mapping[str, Sequence[Sequence[Any]]],
    tab_group_join_mode: Mapping[str, str],
    group_rule_join_mode: Mapping[str, Sequence[str]],
    cfg: BacktestConfig,
    entry_filters: Optional[Mapping[str, Any]] = None,
    streams_already_htf_closed_only: bool = False,
) -> BacktestResult:
    plan = build_compiled_bar_plan(
        symbol=symbol,
        streams_full=streams_full,
        df_1m_full=df_1m_full,
        start=start,
        end=end,
        rules_model=rules_model,
        tab_group_join_mode=tab_group_join_mode,
        group_rule_join_mode=group_rule_join_mode,
        cfg=cfg,
        entry_filters=entry_filters,
        streams_already_htf_closed_only=streams_already_htf_closed_only,
    )
    return run_backtest_compiled_plan(
        plan,
        cfg=cfg,
        rules_model=rules_model,
        tab_group_join_mode=tab_group_join_mode,
        group_rule_join_mode=group_rule_join_mode,
        entry_filters=entry_filters,
        reuse_compiled_plan=False,
    )


def replay_backtest_compiled_bar(
    replay_context: Mapping[str, Any],
    *,
    cfg: BacktestConfig,
    capture_trade_details: Optional[bool] = None,
    equity_curve_stride: Optional[int] = None,
) -> BacktestResult:
    plan = replay_context.get("__compiled_bar_plan")
    if not isinstance(plan, (CompiledBarPlan, CompiledReplayPlan)):
        raise ValueError("Compiled replay context is missing a compiled bar plan.")

    next_cfg = BacktestConfig(**asdict(cfg))
    if capture_trade_details is not None:
        next_cfg.capture_trade_details = bool(capture_trade_details)
    if equity_curve_stride is not None:
        next_cfg.equity_curve_stride = max(1, int(equity_curve_stride))

    if _compiled_replay_event_driven_supported(next_cfg, capture_trade_details=capture_trade_details):
        return run_backtest_compiled_replay_event_driven(
            plan,
            cfg=next_cfg,
            rules_model=dict(replay_context.get("rules_model") or {}),
            tab_group_join_mode=dict(replay_context.get("tab_group_join_mode") or {}),
            group_rule_join_mode={k: list(v) for k, v in dict(replay_context.get("group_rule_join_mode") or {}).items()},
            entry_filters=dict(replay_context.get("entry_filters") or {}),
            replay_context=replay_context,
        )

    if not isinstance(plan, CompiledBarPlan):
        raise ValueError("Full-scan compiled replay requires a full CompiledBarPlan in replay_context.")

    result = run_backtest_compiled_plan(
        plan,
        cfg=next_cfg,
        rules_model=dict(replay_context.get("rules_model") or {}),
        tab_group_join_mode=dict(replay_context.get("tab_group_join_mode") or {}),
        group_rule_join_mode={k: list(v) for k, v in dict(replay_context.get("group_rule_join_mode") or {}).items()},
        entry_filters=dict(replay_context.get("entry_filters") or {}),
        reuse_compiled_plan=True,
        replay_context=replay_context,
    )
    try:
        result.summary["compiled_replay_mode"] = "full_scan"
    except Exception:
        pass
    return result


def run_backtest_auto(
    *,
    symbol: str,
    streams_full: Mapping[str, pd.DataFrame],
    start: Any,
    end: Any,
    rules_model: Mapping[str, Sequence[Sequence[Any]]],
    tab_group_join_mode: Mapping[str, str],
    group_rule_join_mode: Mapping[str, Sequence[str]],
    cfg: BacktestConfig,
    df_1m_full: Optional[pd.DataFrame] = None,
    entry_filters: Optional[Mapping[str, Any]] = None,
    apply_htf_closed_only: bool = False,
) -> BacktestResult:
    plan = compile_strategy_plan(
        rules_model,
        tab_group_join_mode=tab_group_join_mode,
        group_rule_join_mode=group_rule_join_mode,
        entry_filters=entry_filters,
        backtest_cfg=cfg,
    )
    transformed_streams = dict(streams_full or {})
    if bool(apply_htf_closed_only):
        transformed_streams = make_streams_htf_closed_only(transformed_streams)

    if plan.supports_fast_bar:
        return run_backtest_compiled_bar(
            symbol=symbol,
            streams_full=transformed_streams,
            df_1m_full=df_1m_full,
            start=start,
            end=end,
            rules_model=rules_model,
            tab_group_join_mode=tab_group_join_mode,
            group_rule_join_mode=group_rule_join_mode,
            cfg=cfg,
            entry_filters=entry_filters,
            streams_already_htf_closed_only=True,
        )

    from .backtest import run_backtest

    res = run_backtest(
        symbol=symbol,
        streams_full=transformed_streams,
        start=start,
        end=end,
        rules_model=rules_model,
        tab_group_join_mode=tab_group_join_mode,
        group_rule_join_mode=group_rule_join_mode,
        cfg=cfg,
        df_1m_full=df_1m_full,
        entry_filters=entry_filters,
    )
    try:
        res.summary["strategy_plan_engine"] = plan.engine
        res.summary["strategy_plan_notes"] = list(plan.notes)
        res.summary["strategy_plan_blockers"] = list(plan.blockers)
    except Exception:
        pass
    return res
