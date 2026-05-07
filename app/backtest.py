from __future__ import annotations

"""Backtest engines (bar-close and tick/aggTrades)."""

from dataclasses import dataclass, field
from typing import Any, Dict, List, Mapping, Optional, Tuple

import numpy as np
import pandas as pd

from .constants import *
from .rules import (
    Rule,
    RuleEvalContext,
    EntryFilterConfig,
    entry_filter_payload_to_dict,
    eval_tab_generic,
    eval_tab_matching_groups,
    matching_group_indices_from_results,
    normalize_entry_filter_payload,
)
from .utils_time import datetime_like_to_epoch_ms, interval_to_ms
from .data_binance import iter_aggtrades_futures_daily
from .indicators_streaming import compute_live_indicators, ema_tradingview
from .strategy_requirements import RANGE_FILTER_FIELDS


from .backtest_models import (
    BacktestConfig,
    Trade,
    _TradeLifecycle,
    BacktestResult,
    _EntryFilterBar,
    _PendingEntry,
    _ScheduledBarOrder,
    _EntryFilterStats,
)
from .backtest_execution import (
    _make_trade_lifecycle,
    _open_trade_lifecycle,
    _close_trade_lifecycle,
    _open_position_from_flat,
    _apply_slippage,
    _normalize_exit_mode,
    _compute_break_even_prices,
    _compute_initial_trailing_stop,
    _compute_stop_take_prices,
    _break_even_active_for_bar,
    _trailing_stop_pct,
    _intrabar_exit_enabled,
    _resolve_ohlcv_open_gap_exit,
    _resolve_ohlcv_intrabar_range_exit,
    _resolve_tick_level_exit,
    _update_bar_break_even_state,
    _update_bar_progress_state,
    _update_bar_trailing_stop_state,
    _evaluate_bar_failed_pullback_abort,
    _update_tick_break_even_state,
    _update_tick_trailing_stop_state,
    _build_tab_rule_trace,
    _build_non_rule_exit_trace,
)
from .backtest_reporting import (
    _make_trade_df,
    _apply_trade_reporting_metrics,
    _normalize_backtest_replay_context,
    build_backtest_replay_context,
    build_backtest_replay_strategy_signature,
)
from .backtest_planning import (
    _barsago_profile_for_tab,
    _compile_group_signal_array,
    _compile_tab_group_signal_arrays,
    _compile_rule_signal_array,
    _compile_tab_signal_array,
    _compile_tab_signal_arrays,
    _compiled_cmp_numeric,
    _compiled_numeric_array,
    _compiled_prev_array,
    _compiled_tabs_cover_plan,
    _compiled_text_array,
    _max_bars_ago_needed,
    _planned_eval_tabs,
    _rule_uses_barsago,
    _tab_uses_sequence,
)
from .backtest_frames import (
    HTF_EVENT_PULSE_COLUMNS,
    _RowSnapshotCache,
    _build_current_snapshots_for_row,
    _build_eval_snapshots_for_row,
    _freeze_backtest_frame,
    _freeze_backtest_streams,
    _prev_source_row_index_for_df,
    _range_audit_row,
    _snapshot_for_row,
    _source_bar_ids_for_df,
    make_streams_htf_closed_only,
)


_DEFAULT_EVAL_TAB_GENERIC = eval_tab_generic


def _strategy_signature_matches_replay_context(
    replay_context: Optional[Mapping[str, Any]],
    *,
    symbol: str,
    start: Any,
    end: Any,
    rules_model: Mapping[str, Any],
    tab_group_join_mode: Mapping[str, Any],
    group_rule_join_mode: Mapping[str, Any],
    entry_filters: Any,
) -> bool:
    if not isinstance(replay_context, Mapping):
        return False
    current_signature = build_backtest_replay_strategy_signature(
        symbol=symbol,
        start=start,
        end=end,
        rules_model=rules_model,
        tab_group_join_mode=tab_group_join_mode,
        group_rule_join_mode=group_rule_join_mode,
        entry_filters=entry_filters,
    )
    return str(replay_context.get("__strategy_signature") or "") == str(current_signature)

def _safe_float(v: Any) -> Optional[float]:
    try:
        f = float(v)
        return f if np.isfinite(f) else None
    except Exception:
        return None


def _rma(values: np.ndarray, length: int) -> np.ndarray:
    out = np.full(len(values), np.nan, dtype=float)
    if length <= 0 or len(values) == 0:
        return out
    prev = np.nan
    alpha = 1.0 / float(length)
    for i, v in enumerate(values):
        if not np.isfinite(v):
            out[i] = prev
            continue
        if not np.isfinite(prev):
            prev = float(v)
        else:
            prev = prev + alpha * (float(v) - prev)
        out[i] = prev
    return out


def _normalize_entry_filter_map(entry_filters: Optional[Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
    return normalize_entry_filter_payload(entry_filters)


def _entry_filter_default_cfg(cfg_map: Dict[str, Dict[str, Any]], tab_name: str) -> EntryFilterConfig:
    payload = cfg_map.get(str(tab_name), {})
    cfg = payload.get("default", EntryFilterConfig()) if isinstance(payload, dict) else EntryFilterConfig()
    return cfg if isinstance(cfg, EntryFilterConfig) else EntryFilterConfig()


def _entry_filter_cfg_for_group(cfg_map: Dict[str, Dict[str, Any]], tab_name: str, group_index: Optional[int]) -> EntryFilterConfig:
    default_cfg = _entry_filter_default_cfg(cfg_map, tab_name)
    if group_index is None:
        return default_cfg
    payload = cfg_map.get(str(tab_name), {})
    groups = payload.get("groups", {}) if isinstance(payload, dict) else {}
    if isinstance(groups, dict):
        group_cfg = groups.get(str(int(group_index)))
        if isinstance(group_cfg, EntryFilterConfig):
            return group_cfg
    return default_cfg


def _entry_filter_cfgs_for_groups(cfg_map: Dict[str, Dict[str, Any]], tab_name: str, matched_group_indices: Optional[List[int]]) -> List[EntryFilterConfig]:
    group_ids = [int(index) for index in (matched_group_indices or [])]
    if not group_ids:
        return [_entry_filter_default_cfg(cfg_map, tab_name)]
    return [_entry_filter_cfg_for_group(cfg_map, tab_name, group_index) for group_index in group_ids]


def _entry_filter_any_active(cfg_map: Dict[str, Dict[str, Any]]) -> bool:
    for tab_name in ("Long Entry", "Short Entry"):
        if _entry_filter_tab_active(cfg_map, tab_name):
            return True
    return False


def _entry_filter_tab_active(cfg_map: Dict[str, Dict[str, Any]], tab_name: str) -> bool:
    default_cfg = _entry_filter_default_cfg(cfg_map, tab_name)
    if default_cfg.is_active():
        return True
    payload = cfg_map.get(tab_name, {})
    groups = payload.get("groups", {}) if isinstance(payload, dict) else {}
    if isinstance(groups, dict):
        for cfg in groups.values():
            if isinstance(cfg, EntryFilterConfig) and cfg.is_active():
                return True
    return False


def _build_step_bars(df_1m_full: Optional["pd.DataFrame"], step_tf: str) -> "pd.DataFrame":
    if df_1m_full is None or df_1m_full.empty:
        return pd.DataFrame(columns=["open", "high", "low", "close", "atr"])

    df = df_1m_full.copy()
    if "close_time" in df.columns:
        close_idx = pd.to_datetime(df["close_time"], utc=True)
    elif isinstance(df.index, pd.DatetimeIndex):
        close_idx = pd.to_datetime(df.index, utc=True)
    else:
        return pd.DataFrame(columns=["open", "high", "low", "close", "atr"])

    close_ms = datetime_like_to_epoch_ms(close_idx)

    try:
        step_ms = int(interval_to_ms(step_tf))
    except Exception:
        step_ms = 60_000
    if step_ms <= 0:
        step_ms = 60_000

    bucket_end_ms = (((close_ms + 1) // step_ms) * step_ms) - 1
    df = df.copy()
    df["__bucket_end_ms"] = bucket_end_ms
    df["open"] = pd.to_numeric(df.get("open"), errors="coerce")
    df["high"] = pd.to_numeric(df.get("high"), errors="coerce")
    df["low"] = pd.to_numeric(df.get("low"), errors="coerce")
    df["close"] = pd.to_numeric(df.get("close"), errors="coerce")

    agg = df.groupby("__bucket_end_ms", sort=True).agg({
        "open": "first",
        "high": "max",
        "low": "min",
        "close": "last",
    })
    if agg.empty:
        return pd.DataFrame(columns=["open", "high", "low", "close", "atr"])

    high = pd.to_numeric(agg["high"], errors="coerce").astype(float).to_numpy()
    low = pd.to_numeric(agg["low"], errors="coerce").astype(float).to_numpy()
    close = pd.to_numeric(agg["close"], errors="coerce").astype(float).to_numpy()
    prev_close = np.concatenate(([np.nan], close[:-1]))
    tr = np.nanmax(np.vstack([
        high - low,
        np.abs(high - prev_close),
        np.abs(low - prev_close),
    ]), axis=0)
    tr = np.where(np.isfinite(tr), tr, high - low)
    atr = _rma(tr.astype(float), ATR_LEN)
    agg["atr"] = atr
    agg.index = pd.to_datetime(agg.index.astype(np.int64), unit="ms", utc=True)
    agg.index.name = "time"
    return agg[["open", "high", "low", "close", "atr"]]


def _bar_info_at(step_bars: Optional["pd.DataFrame"], ts: Any) -> Optional[_EntryFilterBar]:
    if step_bars is None or len(step_bars) == 0 or ts is None:
        return None
    try:
        key = pd.to_datetime(ts, utc=True)
    except Exception:
        return None
    try:
        row = step_bars.loc[key]
    except Exception:
        return None
    try:
        if isinstance(row, pd.DataFrame):
            row = row.iloc[-1]
        return _EntryFilterBar(
            time=key,
            open=float(row["open"]),
            high=float(row["high"]),
            low=float(row["low"]),
            close=float(row["close"]),
            atr=_safe_float(row.get("atr")),
        )
    except Exception:
        return None


def _entry_filter_reference_bounds(cfg: EntryFilterConfig, signal_bar: Optional[_EntryFilterBar]) -> Tuple[float, float, float]:
    if signal_bar is None:
        return 0.0, 0.0, 0.0
    cfg = (cfg or EntryFilterConfig()).normalized_copy()
    if cfg.normalized_reference_range() == "BODY_ONLY":
        upper = max(float(signal_bar.open), float(signal_bar.close))
        lower = min(float(signal_bar.open), float(signal_bar.close))
    else:
        upper = float(signal_bar.high)
        lower = float(signal_bar.low)
    span = max(0.0, upper - lower)
    return upper, lower, span


def _entry_filter_retrace_pct(
    side: str,
    cfg: EntryFilterConfig,
    signal_bar: Optional[_EntryFilterBar],
    confirm_bar: Optional[_EntryFilterBar],
) -> Optional[float]:
    if signal_bar is None or confirm_bar is None:
        return None
    upper, lower, span = _entry_filter_reference_bounds(cfg, signal_bar)
    if span <= 0.0:
        return 0.0
    touch_type = (cfg or EntryFilterConfig()).normalized_copy().normalized_touch_type()
    side_norm = str(side or "").upper().strip()
    if side_norm == "LONG":
        touched = float(confirm_bar.low) if touch_type == "WICK" else float(confirm_bar.close)
        retrace = max(0.0, upper - touched)
    else:
        touched = float(confirm_bar.high) if touch_type == "WICK" else float(confirm_bar.close)
        retrace = max(0.0, touched - lower)
    return retrace / span * 100.0


def _entry_filter_range_atr(signal_bar: Optional[_EntryFilterBar]) -> Optional[float]:
    if signal_bar is None:
        return None
    bar_range = float(signal_bar.range)
    atr = _safe_float(signal_bar.atr)
    if not atr or atr <= 0.0 or bar_range <= 0.0:
        return None
    return bar_range / atr


def _entry_filter_casebook_pullback_confirm_decision(
    side: str,
    cfg: EntryFilterConfig,
    signal_bar: Optional[_EntryFilterBar],
    confirm_bar: Optional[_EntryFilterBar],
    setup_still_valid: bool,
    *,
    signal_row_index: Optional[int] = None,
    confirm_row_index: Optional[int] = None,
    stream_1m: Optional[pd.DataFrame] = None,
) -> Tuple[str, str]:
    cfg = (cfg or EntryFilterConfig()).normalized_copy()
    if cfg.require_setup_still_valid and not bool(setup_still_valid):
        return "BLOCK", "setup-invalid"
    if str(side or "").upper().strip() != "LONG":
        return "BLOCK", "unsupported-side"
    if signal_bar is None or confirm_bar is None:
        return "WAIT", "missing-bar"
    if stream_1m is None or not isinstance(stream_1m, pd.DataFrame) or stream_1m.empty:
        return "WAIT", "missing-stream"
    if signal_row_index is None or confirm_row_index is None:
        return "WAIT", "missing-row"

    try:
        signal_pos = int(signal_row_index)
        confirm_pos = int(confirm_row_index)
    except Exception:
        return "WAIT", "missing-row"
    if signal_pos < 0 or confirm_pos <= signal_pos or confirm_pos >= len(stream_1m):
        return "WAIT", "await-pullback"

    required_columns = {
        "open",
        "high",
        "low",
        "close",
        "vidya_line",
        "vidya_state",
        "range_filter_state",
        "range_filter_phase",
        "range_filter_buy",
        "range_filter_sell",
        "vidya_trend_up",
        "vidya_trend_down",
    }
    if any(col not in stream_1m.columns for col in required_columns):
        return "BLOCK", "missing-fields"

    def _truthy_event(value: Any) -> bool:
        try:
            if pd.isna(value):
                return False
        except Exception:
            pass
        try:
            return bool(float(value))
        except Exception:
            raw = str(value or "").strip().upper()
            return raw in {"1", "TRUE", "T", "YES", "Y", "EVENT"}

    def _resolve_anchor_row_index() -> int:
        lookback = max(2, int(cfg.casebook_anchor_lookback_bars))
        start_pos = max(0, signal_pos - lookback)
        reset_floor = start_pos
        for pos in range(signal_pos, start_pos - 1, -1):
            row = stream_1m.iloc[pos]
            if pos < signal_pos and (
                _truthy_event(row.get("range_filter_sell")) or _truthy_event(row.get("vidya_trend_down"))
            ):
                reset_floor = pos + 1
                break
        for pos in range(signal_pos, reset_floor - 1, -1):
            row = stream_1m.iloc[pos]
            if not _truthy_event(row.get("vidya_trend_up")):
                continue
            for rf_pos in range(pos, reset_floor - 1, -1):
                if _truthy_event(stream_1m.iloc[rf_pos].get("range_filter_buy")):
                    return pos
        return signal_pos

    anchor_pos = _resolve_anchor_row_index()
    t0_row = stream_1m.iloc[anchor_pos]
    t0_vidya = str(t0_row.get("vidya_state") or "").upper()
    t0_rf = str(t0_row.get("range_filter_state") or "").upper()
    if cfg.casebook_require_t0_vidya_buy and t0_vidya != "BUY":
        return "BLOCK", "t0-vidya-not-buy"
    if cfg.casebook_require_t0_rf_buy and t0_rf != "BUY":
        return "BLOCK", "t0-rf-not-buy"

    t0_close = _safe_float(t0_row.get("close"))
    t0_open = _safe_float(t0_row.get("open"))
    t0_high = _safe_float(t0_row.get("high"))
    t0_low = _safe_float(t0_row.get("low"))
    if t0_close is None or t0_open is None or t0_high is None or t0_low is None:
        return "BLOCK", "missing-t0-price"
    t0_body_bottom = min(float(t0_open), float(t0_close))
    t0_full_range = max(1e-9, float(t0_high) - float(t0_low))

    pullback_limit = min(len(stream_1m) - 1, anchor_pos + int(cfg.casebook_pullback_window_bars))
    pullback_search_end = min(confirm_pos, pullback_limit)
    if anchor_pos + 1 > pullback_search_end:
        return "WAIT", "await-pullback"

    pullback_slice = stream_1m.iloc[anchor_pos + 1 : pullback_search_end + 1]
    lows = pd.to_numeric(pullback_slice["low"], errors="coerce").to_numpy(dtype=float)
    if len(lows) == 0 or not np.isfinite(lows).any():
        return "WAIT", "await-pullback"
    pullback_pos = int(anchor_pos + 1 + int(np.nanargmin(lows)))
    pullback_row = stream_1m.iloc[pullback_pos]
    pullback_low = _safe_float(pullback_row.get("low"))
    if pullback_low is None:
        return "WAIT", "await-pullback"
    if float(pullback_low) >= float(t0_body_bottom):
        if confirm_pos >= pullback_limit:
            return "BLOCK", "no-body-pierce"
        return "WAIT", "await-body-pierce"
    body_pierce_pct = ((float(t0_body_bottom) - float(pullback_low)) / float(t0_full_range)) * 100.0
    if float(body_pierce_pct) < float(cfg.min_retrace_pct):
        if confirm_pos >= pullback_limit:
            return "BLOCK", f"body-pierce:{body_pierce_pct:.1f}%"
        return "WAIT", f"await-deeper-pullback:{body_pierce_pct:.1f}%"

    vidya_line = _safe_float(pullback_row.get("vidya_line"))
    if vidya_line is None or float(pullback_low) < float(vidya_line):
        return "BLOCK", "pullback-below-vidya"
    vidya_gap_pct = ((float(pullback_low) - float(vidya_line)) / float(vidya_line)) * 100.0 if float(vidya_line) != 0.0 else None
    if vidya_gap_pct is not None and float(vidya_gap_pct) > float(cfg.casebook_max_vidya_gap_pct):
        return "BLOCK", f"vidya-gap:{vidya_gap_pct:.3f}%"

    if cfg.casebook_require_vidya_buy_through_signal:
        vidya_slice = stream_1m.iloc[anchor_pos : pullback_pos + 1]["vidya_state"].astype(str).str.upper()
        if not vidya_slice.eq("BUY").all():
            return "BLOCK", "vidya-lost-before-pullback"

    pullback_phase = str(pullback_row.get("range_filter_phase") or "").upper()
    delayed_rf_buy_phases = {str(part or "").upper().strip() for part in tuple(cfg.casebook_delayed_rf_buy_phases or ()) if str(part or "").strip()}
    simple_reclaim_phases = {str(part or "").upper().strip() for part in tuple(cfg.casebook_simple_reclaim_phases or ()) if str(part or "").strip()}
    min_signal_bars = max(0, int(cfg.casebook_min_signal_bars))
    max_signal_bars = max(0, int(cfg.casebook_max_signal_bars))

    def _signal_age_ok(pos: int) -> bool:
        age = int(pos) - int(anchor_pos)
        if min_signal_bars > 0 and age < min_signal_bars:
            return False
        if max_signal_bars > 0 and age > max_signal_bars:
            return False
        return True

    if pullback_phase in delayed_rf_buy_phases:
        if cfg.casebook_require_vidya_buy_through_signal:
            vidya_slice = stream_1m.iloc[anchor_pos : confirm_pos + 1]["vidya_state"].astype(str).str.upper()
            if not vidya_slice.eq("BUY").all():
                return "BLOCK", "vidya-lost-before-signal"
        if confirm_pos <= pullback_pos:
            return "WAIT", "await-rf-buy"
        max_signal_pos = (anchor_pos + max_signal_bars) if max_signal_bars > 0 else confirm_pos
        search_end = min(confirm_pos, max_signal_pos)
        for pos in range(pullback_pos + 1, search_end + 1):
            row = stream_1m.iloc[pos]
            if str(row.get("range_filter_state") or "").upper() != "BUY":
                continue
            if not _signal_age_ok(pos):
                continue
            return ("ALLOW", "casebook-rf-buy") if confirm_pos >= pos else ("WAIT", "await-rf-buy")
        if max_signal_bars > 0 and confirm_pos >= int(anchor_pos + max_signal_bars):
            return "BLOCK", "late-signal"
        return "WAIT", "await-rf-buy"

    if pullback_phase not in simple_reclaim_phases:
        return "BLOCK", f"unsupported-phase:{pullback_phase or 'EMPTY'}"

    reclaim_limit = min(len(stream_1m) - 1, pullback_pos + int(cfg.casebook_reclaim_window_bars))
    reclaim_search_end = min(confirm_pos, reclaim_limit)
    if pullback_pos + 1 > reclaim_search_end:
        return "WAIT", "await-reclaim"

    for pos in range(pullback_pos + 1, reclaim_search_end + 1):
        if not _signal_age_ok(pos):
            continue
        row = stream_1m.iloc[pos]
        close = _safe_float(row.get("close"))
        open_ = _safe_float(row.get("open"))
        prev_high = _safe_float(stream_1m.iloc[pos - 1].get("high")) if pos - 1 >= 0 else close
        row_vidya_line = _safe_float(row.get("vidya_line"))
        if close is None or open_ is None or prev_high is None:
            continue
        if close >= prev_high and close >= open_ and (row_vidya_line is None or close >= row_vidya_line):
            if cfg.casebook_require_vidya_buy_through_signal:
                vidya_slice = stream_1m.iloc[anchor_pos : pos + 1]["vidya_state"].astype(str).str.upper()
                if not vidya_slice.eq("BUY").all():
                    return "BLOCK", "vidya-lost-before-signal"
            return ("ALLOW", "casebook-reclaim") if confirm_pos >= pos else ("WAIT", "await-reclaim")
        if close >= float(t0_close) and close >= open_:
            if cfg.casebook_require_vidya_buy_through_signal:
                vidya_slice = stream_1m.iloc[anchor_pos : pos + 1]["vidya_state"].astype(str).str.upper()
                if not vidya_slice.eq("BUY").all():
                    return "BLOCK", "vidya-lost-before-signal"
            return ("ALLOW", "casebook-close-reclaim") if confirm_pos >= pos else ("WAIT", "await-reclaim")

    if max_signal_bars > 0 and confirm_pos >= int(anchor_pos + max_signal_bars):
        return "BLOCK", "late-signal"
    if confirm_pos >= reclaim_limit:
        return "BLOCK", "no-reclaim"
    return "WAIT", "await-reclaim"


def _entry_filter_signal_decision(side: str, cfg: EntryFilterConfig, signal_bar: Optional[_EntryFilterBar]) -> Tuple[str, str]:
    cfg = (cfg or EntryFilterConfig()).normalized_copy()
    mode = cfg.normalized_mode()
    if not cfg.is_active() or mode == "OFF":
        return "ALLOW", "off"
    if mode == "CASEBOOK_PULLBACK_V1":
        return "WAIT", "await-casebook-pullback"
    if signal_bar is None:
        return "ALLOW", "missing-bar"
    _, _, reference_span = _entry_filter_reference_bounds(cfg, signal_bar)
    if reference_span <= 0.0:
        return "ALLOW", "flat-signal"

    range_atr = _entry_filter_range_atr(signal_bar)
    if float(cfg.hard_skip_atr_mult) > 0.0 and range_atr is not None and range_atr >= float(cfg.hard_skip_atr_mult):
        return "BLOCK", f"hard-skip:{range_atr:.2f}atr"

    if mode == "ANTI_CHASE":
        if range_atr is None:
            return "ALLOW", "missing-atr"
        if range_atr < float(cfg.expansion_atr_mult):
            return "ALLOW", f"normal:{range_atr:.2f}atr"
        return "WAIT", f"expansion:{range_atr:.2f}atr"

    if mode == "RETRACE_ON_EXPANSION":
        if range_atr is None:
            return "ALLOW", "missing-atr"
        if range_atr < float(cfg.expansion_atr_mult):
            return "ALLOW", f"normal:{range_atr:.2f}atr"
        return "WAIT", f"retrace-after-expansion:{range_atr:.2f}atr"

    if mode == "RETRACE_ALWAYS":
        return "WAIT", "await-retrace"

    return "ALLOW", "off"


def _entry_filter_confirm_decision(
    side: str,
    cfg: EntryFilterConfig,
    signal_bar: Optional[_EntryFilterBar],
    confirm_bar: Optional[_EntryFilterBar],
    setup_still_valid: bool,
    *,
    signal_row_index: Optional[int] = None,
    confirm_row_index: Optional[int] = None,
    stream_1m: Optional[pd.DataFrame] = None,
) -> Tuple[str, str]:
    cfg = (cfg or EntryFilterConfig()).normalized_copy()
    mode = cfg.normalized_mode()
    if not cfg.is_active() or mode == "OFF":
        return "ALLOW", "off"
    if cfg.require_setup_still_valid and not bool(setup_still_valid):
        return "BLOCK", "setup-invalid"
    if mode == "CASEBOOK_PULLBACK_V1":
        return _entry_filter_casebook_pullback_confirm_decision(
            side,
            cfg,
            signal_bar,
            confirm_bar,
            setup_still_valid,
            signal_row_index=signal_row_index,
            confirm_row_index=confirm_row_index,
            stream_1m=stream_1m,
        )
    if signal_bar is None:
        return "ALLOW", "missing-bar"
    if confirm_bar is None:
        return ("WAIT", "missing-bar") if mode in ("RETRACE_ON_EXPANSION", "RETRACE_ALWAYS") else ("ALLOW", "missing-bar")

    sig_range = float(signal_bar.range)
    if sig_range <= 0.0:
        return "ALLOW", "flat-signal"

    if mode == "ANTI_CHASE":
        midpoint = float(signal_bar.low) + sig_range * 0.5
        if str(side).upper() == "LONG":
            retrace = max(0.0, float(signal_bar.high) - float(confirm_bar.low))
            retrace_pct = retrace / sig_range * 100.0
            if retrace_pct > float(cfg.max_retrace_pct):
                return "BLOCK", f"retrace:{retrace_pct:.1f}%"
            if cfg.require_next_close_beyond_mid and float(confirm_bar.close) < midpoint:
                return "BLOCK", "below-mid"
            return "ALLOW", f"confirmed:{retrace_pct:.1f}%"

        retrace = max(0.0, float(confirm_bar.high) - float(signal_bar.low))
        retrace_pct = retrace / sig_range * 100.0
        if retrace_pct > float(cfg.max_retrace_pct):
            return "BLOCK", f"retrace:{retrace_pct:.1f}%"
        if cfg.require_next_close_beyond_mid and float(confirm_bar.close) > midpoint:
            return "BLOCK", "above-mid"
        return "ALLOW", f"confirmed:{retrace_pct:.1f}%"

    retrace_pct = _entry_filter_retrace_pct(side, cfg, signal_bar, confirm_bar)
    if retrace_pct is None:
        return "WAIT", "missing-bar"
    if retrace_pct < float(cfg.min_retrace_pct):
        return "WAIT", f"retrace:{retrace_pct:.1f}%"
    if retrace_pct > float(cfg.max_retrace_pct):
        return "BLOCK", f"retrace:{retrace_pct:.1f}%"
    return "ALLOW", f"retraced:{retrace_pct:.1f}%"


def _combine_entry_filter_signal_decisions(
    side: str,
    cfgs: List[EntryFilterConfig],
    signal_bar: Optional[_EntryFilterBar],
) -> Tuple[str, str, int]:
    active_cfgs = [cfg.normalized_copy() for cfg in (cfgs or []) if isinstance(cfg, EntryFilterConfig) and cfg.is_active()]
    if not active_cfgs:
        return "ALLOW", "off", 0

    blocked_details: List[str] = []
    wait_details: List[str] = []
    wait_confirm_bars = 0
    for cfg in active_cfgs:
        decision, detail = _entry_filter_signal_decision(side, cfg, signal_bar)
        if decision == "BLOCK":
            blocked_details.append(str(detail))
        elif decision == "WAIT":
            wait_details.append(str(detail))
            wait_confirm_bars = max(wait_confirm_bars, int(cfg.confirm_bars))

    if blocked_details:
        return "BLOCK", "; ".join(blocked_details), 0
    if wait_details:
        return "WAIT", "; ".join(wait_details), max(1, int(wait_confirm_bars or 1))
    return "ALLOW", "pass", 0


def _combine_entry_filter_confirm_decisions(
    side: str,
    cfgs: List[EntryFilterConfig],
    signal_bar: Optional[_EntryFilterBar],
    confirm_bar: Optional[_EntryFilterBar],
    setup_still_valid: bool,
    bars_since_signal: int = 1,
    signal_row_index: Optional[int] = None,
    confirm_row_index: Optional[int] = None,
    stream_1m: Optional[pd.DataFrame] = None,
) -> Tuple[str, str]:
    active_cfgs = [cfg.normalized_copy() for cfg in (cfgs or []) if isinstance(cfg, EntryFilterConfig) and cfg.is_active()]
    if not active_cfgs:
        return "ALLOW", "off"

    blocked_details: List[str] = []
    allow_details: List[str] = []
    wait_details: List[str] = []
    age = max(0, int(bars_since_signal or 0))
    for cfg in active_cfgs:
        mode = cfg.normalized_mode()
        if age < 1:
            wait_details.append("await-next-bar")
            continue
        if mode == "ANTI_CHASE" and age < int(cfg.confirm_bars):
            wait_details.append(f"await:{age}/{int(cfg.confirm_bars)}")
            continue
        decision, detail = _entry_filter_confirm_decision(
            side,
            cfg,
            signal_bar,
            confirm_bar,
            setup_still_valid,
            signal_row_index=signal_row_index,
            confirm_row_index=confirm_row_index,
            stream_1m=stream_1m,
        )
        if decision == "WAIT" and mode != "CASEBOOK_PULLBACK_V1" and age >= int(cfg.confirm_bars):
            decision, detail = "BLOCK", f"timeout:{age}"
        if decision == "BLOCK":
            blocked_details.append(str(detail))
        elif decision == "WAIT":
            wait_details.append(str(detail))
        else:
            allow_details.append(str(detail))

    if blocked_details:
        return "BLOCK", "; ".join(blocked_details)
    if wait_details:
        return "WAIT", "; ".join(wait_details)
    if allow_details:
        return "ALLOW", "; ".join(allow_details)
    return "ALLOW", "confirmed"


def _matching_group_indices_from_compiled(tab_name: str, compiled_group_signals: Dict[str, List[np.ndarray]], row_index: int, tab_signal: bool) -> List[int]:
    if not bool(tab_signal):
        return []
    groups = compiled_group_signals.get(tab_name, [])
    matched: List[int] = []
    for gi, arr in enumerate(groups):
        try:
            if bool(arr[row_index]):
                matched.append(int(gi))
        except Exception:
            continue
    return matched


def _setup_still_valid_for_pending(matched_group_indices: List[int], group_results: List[Optional[bool]], join_groups: str) -> bool:
    group_ids = [int(idx) for idx in (matched_group_indices or [])]
    if not group_ids:
        return False
    if str(join_groups or "AND").upper().strip() == "AND":
        return all(gi < len(group_results) and group_results[gi] is True for gi in group_ids)
    return any(gi < len(group_results) and group_results[gi] is True for gi in group_ids)


def _setup_still_valid_from_matches(matched_group_indices: List[int], current_group_indices: List[int], join_groups: str) -> bool:
    group_ids = {int(idx) for idx in (matched_group_indices or [])}
    current_ids = {int(idx) for idx in (current_group_indices or [])}
    if not group_ids:
        return False
    if str(join_groups or "AND").upper().strip() == "AND":
        return group_ids.issubset(current_ids)
    return len(group_ids & current_ids) > 0


def _format_entry_reason(base: str, status: str, detail: str, confirm_bars: int = 0) -> str:
    if status == "confirmed":
        return f"{base} (Entry Filter confirmed +{int(confirm_bars)} bar{'s' if int(confirm_bars) != 1 else ''})"
    if status == "allowed":
        return f"{base} (Entry Filter pass)" if detail not in ("off", "normal", "") else base
    if status == "blocked":
        return f"{base} (Entry Filter veto: {detail})"
    return base


def _entry_filter_summary_payload(cfg_map: Dict[str, Dict[str, Any]], stats: _EntryFilterStats) -> Dict[str, Any]:
    out: Dict[str, Any] = {
        "Long Entry": EntryFilterConfig().to_dict(),
        "Short Entry": EntryFilterConfig().to_dict(),
        "stats": {
            "created": int(stats.created),
            "confirmed": int(stats.confirmed),
            "blocked": int(stats.blocked),
            "hard_skipped": int(stats.hard_skipped),
            "cancelled": int(stats.cancelled),
            "replaced": int(stats.replaced),
        },
    }
    for tab_name in ("Long Entry", "Short Entry"):
        payload = cfg_map.get(tab_name, {}) if isinstance(cfg_map, dict) else {}
        default_cfg = payload.get("default", EntryFilterConfig()) if isinstance(payload, dict) else EntryFilterConfig()
        groups = payload.get("groups", {}) if isinstance(payload, dict) else {}
        out[tab_name] = (
            default_cfg.normalized_copy().to_dict()
            if isinstance(default_cfg, EntryFilterConfig)
            else EntryFilterConfig().to_dict()
        )
        out[tab_name]["groups"] = {
            str(group_key): (
                group_cfg.normalized_copy().to_dict()
                if isinstance(group_cfg, EntryFilterConfig)
                else EntryFilterConfig.from_dict(dict(group_cfg or {})).to_dict()
            )
            for group_key, group_cfg in (groups.items() if isinstance(groups, Mapping) else [])
            if str(group_key or "").strip()
        }
    return out


def _pending_from_signal(side: str, origin: str, signal_time: Any, signal_bar: _EntryFilterBar, signal_snapshot: Optional[Dict[str, Dict[str, Any]]], signal_reason: str, signal_rule_trace: str, bars_ago: Dict[str, Any], current_step: int, confirm_bars: int, signal_snapshot_row_index: Optional[int] = None, tab_name: str = "", matched_group_indices: Optional[List[int]] = None) -> _PendingEntry:
    return _PendingEntry(
        side=str(side).upper(),
        origin=str(origin).upper(),
        tab_name=str(tab_name or ""),
        signal_time=pd.to_datetime(signal_time, utc=True),
        signal_step=int(current_step),
        signal_bar=signal_bar,
        signal_snapshot=signal_snapshot if isinstance(signal_snapshot, dict) else {},
        signal_snapshot_row_index=(int(signal_snapshot_row_index) if signal_snapshot_row_index is not None else None),
        signal_reason=str(signal_reason),
        signal_rule_trace=str(signal_rule_trace or ""),
        bars_ago=bars_ago if isinstance(bars_ago, dict) else {},
        matched_group_indices=[int(idx) for idx in (matched_group_indices or [])],
        due_step=int(current_step) + 1,
        confirm_bars=max(1, int(confirm_bars)),
    )


def _cancel_pending(pending: Dict[str, Optional[_PendingEntry]], side: str) -> None:
    try:
        pending[str(side).upper()] = None
    except Exception:
        pass


def _bool_true(x: Optional[bool]) -> bool:
    return x is True


def _snapshot_keys_for_rule_field(field_name: Any) -> List[str]:
    fld = str(field_name or "").strip()
    if not fld:
        return []
    keys: List[str] = [fld]
    if fld == "ms_cross":
        keys.extend(["macd", "signal"])
    elif fld == "macd0_cross":
        keys.append("macd")
    elif fld == "di_cross":
        keys.extend(["di_plus", "di_minus"])
    elif fld in ("frama_break_up", "frama_break_down", "frama_mid_cross"):
        keys.extend(["frama_state", "frama_break_up", "frama_break_down", "frama_mid_cross", "frama_trigger_side"])
    elif fld in ("vidya_new_liq_low", "vidya_new_liq_high"):
        keys.extend(["vidya_active_liq_low_count", "vidya_active_liq_high_count"])
    elif fld in ("range_filter_buy", "range_filter_sell"):
        keys.extend(["range_filter_state", "range_filter_cond_ini"])
    deduped: List[str] = []
    for key in keys:
        text = str(key or "").strip()
        if text and text not in deduped:
            deduped.append(text)
    return deduped


def _compare_to_field_name(compare_to: Any) -> str:
    if isinstance(compare_to, dict):
        raw = compare_to.get("field", compare_to.get("name", ""))
    else:
        raw = compare_to
    text = str(raw or "").strip()
    if text.endswith("]") and "[" in text:
        text = text.rpartition("[")[0].strip()
    return text


def _minimal_snapshot_required_fields(rules_model: Dict[str, List[List["Rule"]]]) -> List[str]:
    fields: set[str] = set()
    try:
        for groups in (rules_model or {}).values():
            for group in (groups or []):
                for rule in (group or []):
                    for key in _snapshot_keys_for_rule_field(getattr(rule, "field", "")):
                        if key:
                            fields.add(str(key))
                    compare_field = _compare_to_field_name(getattr(rule, "compare_to", None))
                    if compare_field:
                        for key in _snapshot_keys_for_rule_field(compare_field):
                            if key:
                                fields.add(str(key))
    except Exception:
        return []
    return sorted(str(field).strip() for field in fields if str(field).strip())


def _replay_eval_snapshot_signature(
    *,
    step_tf: str,
    subs: Dict[str, "pd.DataFrame"],
    required_fields: Optional[List[str]],
) -> Tuple[Any, ...]:
    tf_shapes = tuple(
        (str(tf), int(len(dft)))
        for tf, dft in sorted((subs or {}).items(), key=lambda item: str(item[0]))
        if dft is not None
    )
    fields = tuple(
        sorted(
            str(field).strip()
            for field in (required_fields or [])
            if str(field).strip()
        )
    )
    return ("eval_snapshot_pairs_v1", str(step_tf or ""), tf_shapes, fields)


def _replay_eval_snapshot_pair_cache(
    replay_context: Optional[Dict[str, Any]],
    *,
    step_tf: str,
    subs: Dict[str, "pd.DataFrame"],
    required_fields: Optional[List[str]],
) -> Optional[Dict[int, Tuple[Dict[str, Dict[str, Any]], Dict[str, Dict[str, Any]]]]]:
    if not isinstance(replay_context, dict):
        return None
    signature = _replay_eval_snapshot_signature(
        step_tf=step_tf,
        subs=subs,
        required_fields=required_fields,
    )
    cache_key = "__eval_snapshot_pair_cache"
    sig_key = "__eval_snapshot_pair_cache_signature"
    existing_cache = replay_context.get(cache_key)
    if replay_context.get(sig_key) != signature or not isinstance(existing_cache, dict):
        existing_cache = {}
        replay_context[sig_key] = signature
        replay_context[cache_key] = existing_cache
    return existing_cache


def _entry_group_results_for_tab(
    tab_name: str,
    rules_model: Dict[str, List[List["Rule"]]],
    tab_group_join_mode: Dict[str, str],
    group_rule_join_mode: Dict[str, List[str]],
    cur_by_tf: Dict[str, Dict[str, Any]],
    prev_by_tf: Dict[str, Dict[str, Any]],
    ctx: RuleEvalContext,
) -> Tuple[bool, List[int], List[Optional[bool]]]:
    if eval_tab_generic is _DEFAULT_EVAL_TAB_GENERIC:
        tab_result, matched_groups, group_results = eval_tab_matching_groups(
            tab_name,
            rules_model,
            tab_group_join_mode,
            group_rule_join_mode,
            cur_by_tf,
            prev_by_tf,
            ctx=ctx,
        )
        return (_bool_true(tab_result), matched_groups, group_results)

    tab_result = _bool_true(
        eval_tab_generic(
            tab_name,
            rules_model,
            tab_group_join_mode,
            group_rule_join_mode,
            cur_by_tf,
            prev_by_tf,
            ctx=ctx,
        )
    )
    return tab_result, [], []


def _normalize_step_timeframe(step_tf: str) -> str:
    """Normalize backtest evaluation timeframe string."""
    try:
        s = str(step_tf or "").strip()
    except Exception:
        s = ""
    if not s:
        return "1m"
    return s

def _decision_mask_for_step_tf(idx: "pd.DatetimeIndex", step_tf: str) -> "np.ndarray":
    """Return a boolean mask selecting rows where the chosen step timeframe candle closes.

    Streams in this project are indexed by 1m close_time. Binance closes are typically at
    HH:MM:59.999. We treat 'bar end' as (timestamp + 1ms) and select those ends that land
    exactly on a step timeframe boundary.
    """
    try:
        step_tf = _normalize_step_timeframe(step_tf)
        step_ms = int(interval_to_ms(step_tf))
    except Exception:
        step_tf = "1m"
        step_ms = 60_000

    # 1m: evaluate on every row (except j=0 warmup row).
    if step_tf == "1m" or step_ms <= 60_000:
        return np.ones(len(idx), dtype=bool)

    try:
        # idx is tz-aware UTC; convert ns -> ms
        ns = idx.astype("int64").to_numpy(dtype=np.int64, copy=False)
        close_ms = (ns // 1_000_000).astype(np.int64, copy=False)
    except Exception:
        close_ms = np.array([int(pd.Timestamp(t).value // 1_000_000) for t in idx], dtype=np.int64)

    # bar end in ms (close_ms is typically end-of-minute minus 1ms)
    end_ms = close_ms + 1
    return (end_ms % step_ms) == 0

def _build_ohlcv_execution_frame(df_1m_full: Optional["pd.DataFrame"],
                                target_index: "pd.DatetimeIndex",
                                fallback_close: "pd.Series") -> "pd.DataFrame":
    """Build a 1m execution frame aligned to the simulated 1m close-time index.

    Phase 3 uses this frame to separate signal time (closed candle) from OHLCV
    execution time (next 1m bar open). This helper is additive and does not touch
    protected indicator/rule math.
    """
    target_index = pd.DatetimeIndex(pd.to_datetime(target_index, utc=True))
    df_exec = pd.DataFrame(index=target_index)

    if df_1m_full is not None and not getattr(df_1m_full, "empty", True):
        try:
            df_src = df_1m_full.copy()
            if "close_time" in df_src.columns:
                ct = pd.to_datetime(df_src["close_time"], utc=True)
                df_src.index = pd.DatetimeIndex(ct)
            elif isinstance(df_src.index, pd.DatetimeIndex):
                if df_src.index.tz is None:
                    df_src.index = df_src.index.tz_localize("UTC")
                else:
                    df_src.index = df_src.index.tz_convert("UTC")
            else:
                df_src.index = target_index[:len(df_src)]
            df_exec = df_src.reindex(target_index).copy()
        except Exception:
            df_exec = pd.DataFrame(index=target_index)

    close_fallback = pd.Series(pd.to_numeric(fallback_close, errors="coerce"), index=target_index, dtype=float)
    open_fallback = close_fallback.copy()
    high_fallback = pd.concat([open_fallback, close_fallback], axis=1).max(axis=1)
    low_fallback = pd.concat([open_fallback, close_fallback], axis=1).min(axis=1)
    open_time_fallback = pd.Series(target_index - pd.Timedelta(minutes=1) + pd.Timedelta(milliseconds=1), index=target_index)

    if "open" in df_exec.columns:
        df_exec["open"] = pd.to_numeric(df_exec["open"], errors="coerce")
        df_exec["open"] = df_exec["open"].where(pd.notna(df_exec["open"]), open_fallback)
    else:
        df_exec["open"] = open_fallback

    if "high" in df_exec.columns:
        df_exec["high"] = pd.to_numeric(df_exec["high"], errors="coerce")
        df_exec["high"] = df_exec["high"].where(pd.notna(df_exec["high"]), high_fallback)
    else:
        df_exec["high"] = high_fallback

    if "low" in df_exec.columns:
        df_exec["low"] = pd.to_numeric(df_exec["low"], errors="coerce")
        df_exec["low"] = df_exec["low"].where(pd.notna(df_exec["low"]), low_fallback)
    else:
        df_exec["low"] = low_fallback

    if "open_time" in df_exec.columns:
        ot = pd.to_datetime(df_exec["open_time"], utc=True, errors="coerce")
        df_exec["open_time"] = ot.where(pd.notna(ot), open_time_fallback)
    else:
        df_exec["open_time"] = open_time_fallback

    return df_exec


def run_backtest(symbol: str,
                 streams_full: Dict[str, "pd.DataFrame"],
                 start: "pd.Timestamp",
                 end: "pd.Timestamp",
                 rules_model: Dict[str, List[List["Rule"]]],
                 tab_group_join_mode: Dict[str, str],
                 group_rule_join_mode: Dict[str, List[str]],
                 cfg: BacktestConfig,
                 progress_cb=None,
                 df_1m_full: Optional["pd.DataFrame"] = None,
                 entry_filters: Optional[Dict[str, Any]] = None,
                 replay_context: Optional[Dict[str, Any]] = None,
                 *,
                 capture_trade_details: Optional[bool] = None,
                 equity_curve_stride: Optional[int] = None) -> BacktestResult:
    frozen_streams = _freeze_backtest_streams(streams_full, end=end)
    frozen_df_1m = _freeze_backtest_frame(df_1m_full, end=end)
    normalized_replay_context: Optional[Dict[str, Any]]
    if isinstance(replay_context, dict):
        candidate_replay_context = _normalize_backtest_replay_context(
            replay_context,
            preserve_identity=True,
        )
        if _strategy_signature_matches_replay_context(
            candidate_replay_context,
            symbol=symbol,
            start=start,
            end=end,
            rules_model=rules_model,
            tab_group_join_mode=tab_group_join_mode,
            group_rule_join_mode=group_rule_join_mode,
            entry_filters=entry_filters,
        ):
            normalized_replay_context = candidate_replay_context
        else:
            normalized_replay_context = build_backtest_replay_context(
                symbol=symbol,
                streams_full=frozen_streams,
                start=start,
                end=end,
                rules_model=rules_model,
                tab_group_join_mode=tab_group_join_mode,
                group_rule_join_mode=group_rule_join_mode,
                df_1m_full=frozen_df_1m,
                entry_filters=entry_filters,
            )
    else:
        normalized_replay_context = build_backtest_replay_context(
            symbol=symbol,
            streams_full=frozen_streams,
            start=start,
            end=end,
            rules_model=rules_model,
            tab_group_join_mode=tab_group_join_mode,
            group_rule_join_mode=group_rule_join_mode,
            df_1m_full=frozen_df_1m,
            entry_filters=entry_filters,
        )
    return _run_backtest_from_frozen_inputs(
        symbol=symbol,
        streams_full=frozen_streams,
        start=start,
        end=end,
        rules_model=rules_model,
        tab_group_join_mode=tab_group_join_mode,
        group_rule_join_mode=group_rule_join_mode,
        cfg=cfg,
        progress_cb=progress_cb,
        df_1m_full=frozen_df_1m,
        entry_filters=entry_filters,
        replay_context=normalized_replay_context,
        capture_trade_details=capture_trade_details,
        equity_curve_stride=equity_curve_stride,
    )


def run_backtest_replay(replay_context: Mapping[str, Any],
                        cfg: BacktestConfig,
                        progress_cb=None,
                        *,
                        capture_trade_details: Optional[bool] = None,
                        equity_curve_stride: Optional[int] = None) -> BacktestResult:
    normalized = _normalize_backtest_replay_context(
        replay_context,
        preserve_identity=isinstance(replay_context, dict),
    )
    return _run_backtest_from_frozen_inputs(
        symbol=normalized["symbol"],
        streams_full=normalized["streams_full"],
        start=normalized["start"],
        end=normalized["end"],
        rules_model=normalized["rules_model"],
        tab_group_join_mode=normalized["tab_group_join_mode"],
        group_rule_join_mode=normalized["group_rule_join_mode"],
        cfg=cfg,
        progress_cb=progress_cb,
        df_1m_full=normalized.get("df_1m_full"),
        entry_filters=normalized.get("entry_filters"),
        replay_context=normalized,
        capture_trade_details=capture_trade_details,
        equity_curve_stride=equity_curve_stride,
    )


def _run_backtest_from_frozen_inputs(symbol: str,
                 streams_full: Dict[str, "pd.DataFrame"],
                 start: "pd.Timestamp",
                 end: "pd.Timestamp",
                 rules_model: Dict[str, List[List["Rule"]]],
                 tab_group_join_mode: Dict[str, str],
                 group_rule_join_mode: Dict[str, List[str]],
                 cfg: BacktestConfig,
                 progress_cb=None,
                 df_1m_full: Optional["pd.DataFrame"] = None,
                 entry_filters: Optional[Dict[str, Any]] = None,
                 replay_context: Optional[Dict[str, Any]] = None,
                 capture_trade_details: Optional[bool] = None,
                 equity_curve_stride: Optional[int] = None) -> BacktestResult:
    if "1m" not in streams_full or streams_full["1m"].empty:
        raise ValueError("streams_full must contain non-empty '1m' dataframe")

    full_idx = streams_full["1m"].index
    if start.tzinfo is None:
        start = start.tz_localize("UTC")
    else:
        start = start.tz_convert("UTC")
    if end.tzinfo is None:
        end = end.tz_localize("UTC")
    else:
        end = end.tz_convert("UTC")

    start_pos = int(full_idx.searchsorted(start))
    end_pos = int(full_idx.searchsorted(end, side="right") - 1)
    if end_pos <= 0 or start_pos > end_pos:
        raise ValueError("No candles in selected window (start/end).")

    lookback = max(1, _max_bars_ago_needed(rules_model))
    step_tf = _normalize_step_timeframe(getattr(cfg, "step_timeframe", "1m"))
    try:
        _step_ms = int(interval_to_ms(step_tf))
        _step_minutes = max(1, int(_step_ms // 60_000))
    except Exception:
        _step_minutes = 1
    lookback_minutes = int(max(lookback * _step_minutes, _step_minutes))

    range_start = max(0, start_pos - lookback_minutes)
    range_end = end_pos

    subs: Dict[str, pd.DataFrame] = {}
    for tf, dft in streams_full.items():
        if dft is None or dft.empty:
            continue
        subs[tf] = dft.iloc[range_start:range_end + 1]

    eval_required_fields = _minimal_snapshot_required_fields(rules_model)
    eval_snapshot_cache = _RowSnapshotCache(required_fields=eval_required_fields, max_rows_per_tf=1024)
    full_snapshot_cache = _RowSnapshotCache(required_fields=None, max_rows_per_tf=128)
    replay_eval_snapshot_pairs = _replay_eval_snapshot_pair_cache(
        replay_context,
        step_tf=step_tf,
        subs=subs,
        required_fields=eval_required_fields,
    )
    range_audit_enabled = bool((replay_context or {}).get("__enable_range_audit")) and bool(set(eval_required_fields) & set(RANGE_FILTER_FIELDS))
    default_eval_impl = str(getattr(eval_tab_generic, "__module__", "")) == "app.rules"
    active_tabs = {
        "Long Entry": bool(rules_model.get("Long Entry")) or (not default_eval_impl),
        "Long Exit": bool(rules_model.get("Long Exit")) or (not default_eval_impl),
        "Short Entry": bool(rules_model.get("Short Entry")) or (not default_eval_impl),
        "Short Exit": bool(rules_model.get("Short Exit")) or (not default_eval_impl),
    }
    sequence_tabs = {
        tab: _tab_uses_sequence(group_rule_join_mode, tab)
        for tab in active_tabs.keys()
    }
    barsago_profile_cache: Dict[str, Dict[str, Any]] = {
        tab: _barsago_profile_for_tab(rules_model, tab)
        for tab in ("Long Entry", "Long Exit", "Short Entry", "Short Exit")
    }

    fallback_close = subs["1m"].get("price")
    if fallback_close is None:
        fallback_close = subs["1m"].get("close")
        if fallback_close is not None:
            subs["1m"] = subs["1m"].copy()
            subs["1m"]["price"] = pd.to_numeric(fallback_close, errors="coerce")
            fallback_close = subs["1m"]["price"]
        else:
            fallback_close = pd.Series(index=subs["1m"].index, dtype=float)

    exec_1m = _build_ohlcv_execution_frame(
        df_1m_full=df_1m_full,
        target_index=subs["1m"].index,
        fallback_close=fallback_close,
    )
    exec_open_prices = pd.to_numeric(exec_1m.get("open"), errors="coerce").to_numpy(dtype=float, copy=False)
    exec_high_prices = pd.to_numeric(exec_1m.get("high"), errors="coerce").to_numpy(dtype=float, copy=False)
    exec_low_prices = pd.to_numeric(exec_1m.get("low"), errors="coerce").to_numpy(dtype=float, copy=False)
    exec_open_times = pd.to_datetime(exec_1m.get("open_time"), utc=True, errors="coerce")
    exec_open_times_list = list(exec_open_times)
    price_series = subs["1m"].get("price")
    if price_series is None:
        price_series = fallback_close

    capture_trade_details = bool(getattr(cfg, "capture_trade_details", True) if capture_trade_details is None else capture_trade_details)
    equity_curve_stride = max(1, int(getattr(cfg, "equity_curve_stride", 1) if equity_curve_stride is None else equity_curve_stride))

    equity = float(cfg.initial_balance)
    equity_mark = equity
    high_water = equity
    max_dd_val = 0.0
    max_dd_pct = 0.0

    pos = None
    trade_id = 0
    trades: List[Trade] = []
    eq_rows: List[dict] = []

    n = len(subs["1m"])
    if progress_cb:
        progress_cb("Backtest running…", 0)

    ctx = RuleEvalContext(step_index=0)
    try:
        ctx.ensure_max_history(max(512, int(lookback) + 32))
    except Exception:
        pass
    decision_mask = _decision_mask_for_step_tf(subs["1m"].index, step_tf)
    prev_source_row_by_tf: Dict[str, Optional[np.ndarray]] = {
        tf: _prev_source_row_index_for_df(dft, tf)
        for tf, dft in subs.items()
    }
    compiled_tab_signals: Dict[str, np.ndarray] = {}
    compiled_group_signals: Dict[str, List[np.ndarray]] = {}
    cached_compiled_ok = False
    if default_eval_impl and isinstance(replay_context, dict):
        try:
            cache_range_start = int(replay_context.get("__compiled_cache_range_start"))
            cache_range_end = int(replay_context.get("__compiled_cache_range_end"))
            cache_len_1m = int(replay_context.get("__compiled_cache_len_1m"))
            raw_tab_cache = replay_context.get("__compiled_tab_signals_cache")
            raw_group_cache = replay_context.get("__compiled_group_signals_cache")
            if (
                cache_range_start == int(range_start)
                and cache_range_end == int(range_end)
                and cache_len_1m == int(len(subs["1m"]))
                and isinstance(raw_tab_cache, dict)
                and isinstance(raw_group_cache, dict)
            ):
                compiled_tab_signals = raw_tab_cache
                compiled_group_signals = raw_group_cache
                cached_compiled_ok = True
        except Exception:
            compiled_tab_signals = {}
            compiled_group_signals = {}
            cached_compiled_ok = False
    if default_eval_impl and not cached_compiled_ok:
        try:
            compiled_tab_signals = _compile_tab_signal_arrays(
                rules_model,
                tab_group_join_mode,
                group_rule_join_mode,
                subs,
                prev_source_row_by_tf,
                len(subs["1m"]),
            )
        except Exception:
            compiled_tab_signals = {}
        try:
            compiled_group_signals = _compile_tab_group_signal_arrays(
                rules_model,
                group_rule_join_mode,
                subs,
                prev_source_row_by_tf,
                len(subs["1m"]),
            )
        except Exception:
            compiled_group_signals = {}
        if isinstance(replay_context, dict):
            try:
                replay_context["__compiled_tab_signals_cache"] = compiled_tab_signals
                replay_context["__compiled_group_signals_cache"] = compiled_group_signals
                replay_context["__compiled_cache_range_start"] = int(range_start)
                replay_context["__compiled_cache_range_end"] = int(range_end)
                replay_context["__compiled_cache_len_1m"] = int(len(subs["1m"]))
            except Exception:
                pass
    prev_decision_j: Optional[int] = None
    step_counter: int = 0

    ef_cfg = _normalize_entry_filter_map(entry_filters)
    ef_stats = _EntryFilterStats()
    active_entry_filter_tabs = {
        tab_name: _entry_filter_tab_active(ef_cfg, tab_name)
        for tab_name in ("Long Entry", "Short Entry")
    }
    active_entry_filter_tabs = {
        tab_name: _entry_filter_tab_active(ef_cfg, tab_name)
        for tab_name in ("Long Entry", "Short Entry")
    }
    step_bars = _build_step_bars(df_1m_full, step_tf) if _entry_filter_any_active(ef_cfg) else pd.DataFrame()
    pending: Dict[str, Optional[_PendingEntry]] = {"LONG": None, "SHORT": None}
    scheduled_order: Optional[_ScheduledBarOrder] = None
    range_audit_rows: List[Dict[str, Any]] = []

    def _bar_for_time(ts: Any) -> Optional[_EntryFilterBar]:
        return _bar_info_at(step_bars, ts)

    def _open_now(side: str,
                  reason: str,
                  t_now: pd.Timestamp,
                  price_now: float,
                  snap_now: Dict[str, Dict[str, Any]],
                  signal_time: Optional[Any] = None,
                  order_created_time: Optional[Any] = None,
                  fill_source: str = "BAR_OPEN",
                  entry_rule_trace: str = "",
                  row_index: Optional[int] = None) -> None:
        nonlocal pos, equity
        pos, fee_entry = _open_position_from_flat(
            side,
            t_now,
            price_now,
            snap_now,
            rules_model,
            cfg,
            reason,
            signal_time=signal_time,
            order_created_time=order_created_time,
            engine_type="OHLCV",
            fill_source=fill_source,
            entry_rule_trace=(entry_rule_trace if capture_trade_details else ""),
        )
        if row_index is not None:
            try:
                pos["entry_row_index"] = int(row_index)
            except Exception:
                pos["entry_row_index"] = None
        elif "entry_row_index" not in pos:
            pos["entry_row_index"] = None
        equity -= fee_entry
        try:
            ctx.reset_all_sequences()
        except Exception:
            pass

    def _close_now(fill_time: pd.Timestamp,
                   fill_price: float,
                   close_reason: str,
                   exit_snapshot: Dict[str, Dict[str, Any]],
                   exit_barsago: Dict[str, Any],
                   ambiguous_exit: bool = False,
                   exit_rule_trace: str = "",
                   exit_signal_time: Optional[Any] = None,
                   exit_order_created_time: Optional[Any] = None,
                   exit_row_index: Optional[int] = None) -> None:
        nonlocal pos, equity, trade_id
        if pos is None:
            return

        if pos["side"] == "LONG":
            px_exit = _apply_slippage(fill_price, "SELL", cfg.slippage_bps)
            gross = (px_exit - pos["entry_price"]) * pos["qty"]
        else:
            px_exit = _apply_slippage(fill_price, "BUY", cfg.slippage_bps)
            gross = (pos["entry_price"] - px_exit) * pos["qty"]

        exit_notional = pos["qty"] * px_exit
        fee_exit = cfg.fee_rate * exit_notional
        fees = float(pos["fee_entry"]) + float(fee_exit)
        net = gross - fees
        equity += gross - fee_exit

        trade_id += 1
        duration = int((fill_time - pos["entry_time"]).total_seconds() // 60)
        entry_notional = pos["qty"] * pos["entry_price"]
        ret_pct = (net / entry_notional * 100.0) if entry_notional > 0 else 0.0
        mfe = float(pos["mfe"])
        mae = float(pos["mae"])
        mfe_pct = (mfe / entry_notional * 100.0) if entry_notional > 0 else 0.0
        mae_pct = (mae / entry_notional * 100.0) if entry_notional > 0 else 0.0

        lifecycle_kwargs = _close_trade_lifecycle(
            pos.get("lifecycle"),
            exit_fill_time=fill_time,
            exit_fill_price=float(px_exit),
            exit_reason=str(close_reason),
            exit_snapshot=exit_snapshot,
            exit_barsago=exit_barsago,
            ambiguous_exit=ambiguous_exit,
            exit_rule_trace=(exit_rule_trace if capture_trade_details else ""),
            exit_signal_time=(exit_signal_time if exit_signal_time is not None else fill_time),
            exit_order_created_time=(exit_order_created_time if exit_order_created_time is not None else (exit_signal_time if exit_signal_time is not None else fill_time)),
        )

        trades.append(Trade(
            trade_id=trade_id,
            side=pos["side"],
            entry_time=pos["entry_time"],
            entry_price=float(pos["entry_price"]),
            exit_time=fill_time,
            exit_price=float(px_exit),
            qty=float(pos["qty"]),
            entry_notional=float(entry_notional),
            exit_notional=float(exit_notional),
            gross_pnl=float(gross),
            fees=float(fees),
            fee_entry=float(pos.get("fee_entry", 0.0)),
            fee_exit=float(fee_exit),
            net_pnl=float(net),
            return_pct=float(ret_pct),
            duration_min=duration,
            mfe=float(mfe),
            mae=float(mae),
            mfe_pct=float(mfe_pct),
            mae_pct=float(mae_pct),
            entry_reason=str(pos["entry_reason"]),
            exit_reason=str(close_reason),
            entry_rule_trace=(str(pos.get("entry_rule_trace", "") or "") if capture_trade_details else ""),
            exit_rule_trace=(str(lifecycle_kwargs.get("exit_rule_trace", "") or "") if capture_trade_details else ""),
            entry_snapshot=(pos["entry_snapshot"] if capture_trade_details else {}),
            exit_snapshot=(exit_snapshot if capture_trade_details else {}),
            entry_barsago=(pos.get("entry_barsago", {}) if capture_trade_details else {}),
            exit_barsago=(exit_barsago if capture_trade_details else {}),
            stop_loss_price=(float(pos.get("stop_loss_price")) if pos.get("stop_loss_price") is not None else None),
            take_profit_price=(float(pos.get("take_profit_price")) if pos.get("take_profit_price") is not None else None),
            entry_row_index=(int(pos.get("entry_row_index")) if pos.get("entry_row_index") is not None else None),
            exit_row_index=(int(exit_row_index) if exit_row_index is not None else None),
            **{k: v for k, v in lifecycle_kwargs.items() if k not in ("entry_rule_trace", "exit_rule_trace")},
        ))
        pos = None
        try:
            ctx.reset_all_sequences()
        except Exception:
            pass

    def _latest_closed_snapshot(fill_index: int) -> Dict[str, Dict[str, Any]]:
        idx_use = int(fill_index) - 1
        if idx_use < 0:
            return {}
        return _build_current_snapshots_for_row(
            subs,
            idx_use,
            snapshot_cache=full_snapshot_cache,
            required_fields=None,
        )

    def _materialize_snapshot(snapshot: Optional[Dict[str, Dict[str, Any]]], row_index: Optional[int]) -> Dict[str, Dict[str, Any]]:
        if isinstance(snapshot, dict) and len(snapshot) > 0:
            return snapshot
        if row_index is None:
            return {}
        idx_use = int(row_index)
        if idx_use < 0 or idx_use >= n:
            return {}
        return _build_current_snapshots_for_row(
            subs,
            idx_use,
            snapshot_cache=full_snapshot_cache,
            required_fields=None,
        )

    def _materialize_pending_snapshot(pending_entry: Optional[_PendingEntry]) -> Dict[str, Dict[str, Any]]:
        if pending_entry is None:
            return {}
        return _materialize_snapshot(
            getattr(pending_entry, "signal_snapshot", None),
            getattr(pending_entry, "signal_snapshot_row_index", None),
        )

    def _materialize_scheduled_snapshot(order: Optional[_ScheduledBarOrder]) -> Dict[str, Dict[str, Any]]:
        if order is None:
            return {}
        snap = _materialize_snapshot(
            getattr(order, "snapshot", None),
            getattr(order, "snapshot_row_index", None),
        )
        try:
            order.snapshot = snap
            order.snapshot_row_index = None
        except Exception:
            pass
        return snap

    def _schedule_open(side: str,
                       reason: str,
                       signal_time: Any,
                       order_created_time: Any,
                       signal_snapshot: Optional[Dict[str, Dict[str, Any]]],
                       fill_index: int,
                       rule_trace: str = "",
                       signal_snapshot_row_index: Optional[int] = None) -> bool:
        nonlocal scheduled_order
        if fill_index >= n:
            return False
        scheduled_order = _ScheduledBarOrder(
            kind="OPEN",
            fill_index=int(fill_index),
            side=str(side).upper(),
            reason=str(reason),
            rule_trace=str(rule_trace or ""),
            signal_time=pd.to_datetime(signal_time, utc=True) if signal_time is not None else None,
            order_created_time=pd.to_datetime(order_created_time, utc=True) if order_created_time is not None else None,
            snapshot=(signal_snapshot if isinstance(signal_snapshot, dict) else {}),
            snapshot_row_index=(int(signal_snapshot_row_index) if signal_snapshot_row_index is not None else None),
        )
        return True

    def _schedule_close(close_reason: str,
                        signal_time: Any,
                        order_created_time: Any,
                        exit_snapshot: Optional[Dict[str, Dict[str, Any]]],
                        exit_barsago: Dict[str, Any],
                        fill_index: int,
                        reverse_side: Optional[str] = None,
                        reverse_reason: str = "",
                        rule_trace: str = "",
                        reverse_rule_trace: str = "",
                        exit_snapshot_row_index: Optional[int] = None) -> bool:
        nonlocal scheduled_order
        if fill_index >= n:
            return False
        scheduled_order = _ScheduledBarOrder(
            kind="CLOSE",
            fill_index=int(fill_index),
            reason=str(close_reason),
            rule_trace=str(rule_trace or ""),
            signal_time=pd.to_datetime(signal_time, utc=True) if signal_time is not None else None,
            order_created_time=pd.to_datetime(order_created_time, utc=True) if order_created_time is not None else None,
            snapshot=(exit_snapshot if isinstance(exit_snapshot, dict) else {}),
            snapshot_row_index=(int(exit_snapshot_row_index) if exit_snapshot_row_index is not None else None),
            barsago=(exit_barsago if isinstance(exit_barsago, dict) else {}),
            reverse_side=(str(reverse_side).upper() if reverse_side else None),
            reverse_reason=str(reverse_reason or ""),
            reverse_rule_trace=str(reverse_rule_trace or ""),
        )
        return True

    def _maybe_start_pending_or_open(side: str,
                                     base_reason: str,
                                     origin: str,
                                     t_now: pd.Timestamp,
                                     price_now: float,
                                     current_step: int,
                                     signal_index: int,
                                     matched_group_indices: Optional[List[int]] = None,
                                     rule_trace: str = "") -> bool:
        nonlocal pos, equity, ef_stats
        side = str(side).upper()
        tab_name = "Long Entry" if side == "LONG" else "Short Entry"
        cfgs_side = _entry_filter_cfgs_for_groups(ef_cfg, tab_name, matched_group_indices)
        sig_bar = _bar_for_time(t_now)
        decision, detail, confirm_bars = _combine_entry_filter_signal_decisions(side, cfgs_side, sig_bar)
        if decision == "ALLOW":
            return _schedule_open(
                side,
                _format_entry_reason(base_reason, "allowed", detail),
                signal_time=t_now,
                order_created_time=t_now,
                signal_snapshot=None,
                fill_index=(signal_index + 1),
                rule_trace=rule_trace,
                signal_snapshot_row_index=signal_index,
            )
        if decision == "WAIT" and sig_bar is not None:
            pending[side] = _pending_from_signal(
                side=side,
                origin=origin,
                signal_time=t_now,
                signal_bar=sig_bar,
                signal_snapshot=None,
                signal_snapshot_row_index=signal_index,
                signal_reason=base_reason,
                signal_rule_trace=str(rule_trace or ""),
                bars_ago=barsago_profile_cache.get(tab_name, {"tab": str(tab_name), "exact": [], "within": [], "items": []}),
                current_step=current_step,
                confirm_bars=confirm_bars,
                tab_name=tab_name,
                matched_group_indices=matched_group_indices,
            )
            ef_stats.created += 1
            return False
        ef_stats.blocked += 1
        if "hard-skip" in str(detail):
            ef_stats.hard_skipped += 1
        return False

    for j in range(1, n):
        t = subs["1m"].index[j]
        price = float(price_series.iloc[j])
        exec_fill_price = float(exec_open_prices[j]) if np.isfinite(float(exec_open_prices[j])) else float(price)
        exec_bar_high = float(exec_high_prices[j]) if np.isfinite(float(exec_high_prices[j])) else max(float(exec_fill_price), float(price))
        exec_bar_low = float(exec_low_prices[j]) if np.isfinite(float(exec_low_prices[j])) else min(float(exec_fill_price), float(price))
        exec_fill_time = exec_open_times_list[j]
        if pd.isna(exec_fill_time):
            exec_fill_time = pd.Timestamp(t) - pd.Timedelta(minutes=1) + pd.Timedelta(milliseconds=1)
        else:
            exec_fill_time = pd.Timestamp(exec_fill_time)
            if exec_fill_time.tzinfo is None:
                exec_fill_time = exec_fill_time.tz_localize("UTC")
            else:
                exec_fill_time = exec_fill_time.tz_convert("UTC")

        opened_this_bar = False
        prechecked_open_gap = False

        if pos is not None and scheduled_order is not None and int(scheduled_order.fill_index) == int(j) and scheduled_order.kind == "CLOSE" and _intrabar_exit_enabled(cfg):
            break_even_active = _break_even_active_for_bar(pos.get("break_even_armed_row_index"), j)
            gap_exit = _resolve_ohlcv_open_gap_exit(
                pos["side"],
                exec_fill_price,
                pos.get("stop_loss_price"),
                pos.get("take_profit_price"),
                pos.get("trailing_stop_price"),
                pos.get("break_even_price"),
                break_even_active,
            )
            if gap_exit is not None:
                _close_now(
                    fill_time=exec_fill_time,
                    fill_price=gap_exit.fill_price,
                    close_reason=gap_exit.reason,
                    exit_snapshot=_latest_closed_snapshot(j),
                    exit_barsago={},
                    ambiguous_exit=gap_exit.ambiguous,
                    exit_rule_trace=_build_non_rule_exit_trace(gap_exit.reason),
                    exit_signal_time=exec_fill_time,
                    exit_order_created_time=exec_fill_time,
                    exit_row_index=j,
                )
                scheduled_order = None
                prechecked_open_gap = True

        if scheduled_order is not None and int(scheduled_order.fill_index) == int(j):
            if scheduled_order.kind == "OPEN" and pos is None:
                entry_signal_snapshot = _materialize_scheduled_snapshot(scheduled_order)
                _open_now(
                    scheduled_order.side,
                    scheduled_order.reason,
                    exec_fill_time,
                    exec_fill_price,
                    entry_signal_snapshot,
                    signal_time=scheduled_order.signal_time,
                    order_created_time=scheduled_order.order_created_time,
                    fill_source="BAR_OPEN",
                    entry_rule_trace=scheduled_order.rule_trace,
                    row_index=j,
                )
                opened_this_bar = True
                scheduled_order = None
            elif scheduled_order.kind == "CLOSE" and pos is not None:
                reverse_side = scheduled_order.reverse_side
                reverse_reason = scheduled_order.reverse_reason
                reverse_signal_time = scheduled_order.signal_time
                reverse_order_created_time = scheduled_order.order_created_time
                scheduled_exit_snapshot = _materialize_scheduled_snapshot(scheduled_order)
                reverse_snapshot = scheduled_exit_snapshot
                reverse_rule_trace = scheduled_order.reverse_rule_trace
                exit_signal_time = scheduled_order.signal_time
                exit_order_created_time = scheduled_order.order_created_time
                _close_now(
                    fill_time=exec_fill_time,
                    fill_price=exec_fill_price,
                    close_reason=scheduled_order.reason,
                    exit_snapshot=scheduled_exit_snapshot,
                    exit_barsago=scheduled_order.barsago,
                    ambiguous_exit=False,
                    exit_rule_trace=scheduled_order.rule_trace,
                    exit_signal_time=exit_signal_time,
                    exit_order_created_time=exit_order_created_time,
                    exit_row_index=j,
                )
                scheduled_order = None
                if reverse_side is not None:
                    _open_now(
                        reverse_side,
                        reverse_reason,
                        exec_fill_time,
                        exec_fill_price,
                        reverse_snapshot,
                        signal_time=reverse_signal_time,
                        order_created_time=reverse_order_created_time,
                        fill_source="BAR_OPEN",
                        entry_rule_trace=reverse_rule_trace,
                        row_index=j,
                    )
                    opened_this_bar = True
            else:
                scheduled_order = None

        if pos is not None and _intrabar_exit_enabled(cfg):
            if not opened_this_bar and not prechecked_open_gap:
                break_even_active = _break_even_active_for_bar(pos.get("break_even_armed_row_index"), j)
                gap_exit = _resolve_ohlcv_open_gap_exit(
                    pos["side"],
                    exec_fill_price,
                    pos.get("stop_loss_price"),
                    pos.get("take_profit_price"),
                    pos.get("trailing_stop_price"),
                    pos.get("break_even_price"),
                    break_even_active,
                )
                if gap_exit is not None:
                    _close_now(
                        fill_time=exec_fill_time,
                        fill_price=gap_exit.fill_price,
                        close_reason=gap_exit.reason,
                        exit_snapshot=_latest_closed_snapshot(j),
                        exit_barsago={},
                        ambiguous_exit=gap_exit.ambiguous,
                        exit_rule_trace=_build_non_rule_exit_trace(gap_exit.reason),
                        exit_signal_time=exec_fill_time,
                        exit_order_created_time=exec_fill_time,
                        exit_row_index=j,
                    )

        if pos is not None and _intrabar_exit_enabled(cfg):
            break_even_active = _break_even_active_for_bar(pos.get("break_even_armed_row_index"), j)
            range_exit = _resolve_ohlcv_intrabar_range_exit(
                pos["side"],
                exec_bar_high,
                exec_bar_low,
                pos.get("stop_loss_price"),
                pos.get("take_profit_price"),
                pos.get("trailing_stop_price"),
                pos.get("break_even_price"),
                break_even_active,
            )
            if range_exit is not None:
                _close_now(
                    fill_time=t,
                    fill_price=range_exit.fill_price,
                    close_reason=range_exit.reason,
                    exit_snapshot=_latest_closed_snapshot(j),
                    exit_barsago={},
                    ambiguous_exit=range_exit.ambiguous,
                    exit_rule_trace=_build_non_rule_exit_trace(range_exit.reason),
                    exit_signal_time=t,
                    exit_order_created_time=t,
                    exit_row_index=j,
                )

        if pos is not None:
            _update_bar_break_even_state(pos, j, exec_bar_high, exec_bar_low)
            _update_bar_trailing_stop_state(pos, j, exec_bar_high, exec_bar_low)
            _update_bar_progress_state(pos, exec_bar_high, exec_bar_low)

        do_eval = bool(decision_mask[j])
        cur_by_tf: Dict[str, Dict[str, Any]] = {}
        prev_by_tf: Dict[str, Dict[str, Any]] = {}
        cur_step_index: Optional[int] = None
        eval_snapshots_ready = False
        eval_tabs = {
            "Long Entry": False,
            "Long Exit": False,
            "Short Entry": False,
            "Short Exit": False,
        }
        prev_decision_before = prev_decision_j

        def _ensure_eval_snapshots() -> None:
            nonlocal cur_by_tf, prev_by_tf, eval_snapshots_ready
            if eval_snapshots_ready or not do_eval:
                return
            cached_pair = None
            if replay_eval_snapshot_pairs is not None:
                cached_pair = replay_eval_snapshot_pairs.get(int(j))
            if (
                isinstance(cached_pair, tuple)
                and len(cached_pair) == 2
                and isinstance(cached_pair[0], dict)
                and isinstance(cached_pair[1], dict)
            ):
                cur_by_tf, prev_by_tf = cached_pair
            else:
                cur_by_tf, prev_by_tf = _build_eval_snapshots_for_row(
                    subs,
                    row_index=j,
                    prev_decision_j=prev_decision_before,
                    prev_source_row_by_tf=prev_source_row_by_tf,
                    step_tf=step_tf,
                    snapshot_cache=eval_snapshot_cache,
                    required_fields=eval_required_fields,
                )
                if replay_eval_snapshot_pairs is not None:
                    replay_eval_snapshot_pairs[int(j)] = (cur_by_tf, prev_by_tf)
            ctx.step_index = int(cur_step_index if cur_step_index is not None else 0)
            try:
                ctx.observe(cur_by_tf)
            except Exception:
                pass
            try:
                if range_audit_enabled and len(range_audit_rows) < 2000:
                    for _tf in sorted(cur_by_tf.keys()):
                        if any(k in cur_by_tf.get(_tf, {}) for k in ("range_filter_state", "range_filter_buy", "range_filter_sell", "range_filter_cond_ini")):
                            range_audit_rows.append(_range_audit_row(t, _tf, cur_by_tf.get(_tf), prev_by_tf.get(_tf)))
            except Exception:
                pass
            eval_snapshots_ready = True

        if do_eval:
            cur_step_index = int(step_counter)
            eval_tabs = _planned_eval_tabs(
                active_tabs=active_tabs,
                sequence_tabs=sequence_tabs,
                pos_side=(None if pos is None else pos.get("side")),
                has_scheduled_order=bool(scheduled_order is not None),
                allow_reverse=bool(getattr(cfg, "allow_reverse", False)),
                default_eval_impl=default_eval_impl,
            )
            if not _compiled_tabs_cover_plan(eval_tabs, compiled_tab_signals):
                _ensure_eval_snapshots()
            prev_decision_j = int(j)
            step_counter += 1

        if t < start or t > end:
            continue

        le_trace_txt = ""
        lx_trace_txt = ""
        se_trace_txt = ""
        sx_trace_txt = ""
        full_cur_by_tf: Optional[Dict[str, Dict[str, Any]]] = None

        def _full_current_snapshots() -> Dict[str, Dict[str, Any]]:
            nonlocal full_cur_by_tf
            if full_cur_by_tf is None:
                full_cur_by_tf = _build_current_snapshots_for_row(
                    subs,
                    j,
                    snapshot_cache=full_snapshot_cache,
                    required_fields=None,
                )
            return full_cur_by_tf

        if not do_eval:
            long_entry = False
            long_exit = False
            short_entry = False
            short_exit = False
            long_entry_groups: List[int] = []
            short_entry_groups: List[int] = []
            long_entry_group_results: List[Optional[bool]] = []
            short_entry_group_results: List[Optional[bool]] = []
        else:
            if eval_tabs["Long Entry"] and ("Long Entry" in compiled_tab_signals) and (("Long Entry" in compiled_group_signals) or (not active_entry_filter_tabs.get("Long Entry", False))):
                long_entry = bool(compiled_tab_signals["Long Entry"][j])
                long_entry_groups = _matching_group_indices_from_compiled("Long Entry", compiled_group_signals, j, long_entry)
                long_entry_group_results = [True if gi in long_entry_groups else False for gi in range(len(rules_model.get("Long Entry", [])))]
            elif eval_tabs["Long Entry"]:
                _ensure_eval_snapshots()
                if default_eval_impl:
                    long_entry, long_entry_groups, long_entry_group_results = _entry_group_results_for_tab(
                        "Long Entry",
                        rules_model,
                        tab_group_join_mode,
                        group_rule_join_mode,
                        cur_by_tf,
                        prev_by_tf,
                        ctx,
                    )
                else:
                    long_entry = _bool_true(eval_tab_generic("Long Entry", rules_model, tab_group_join_mode, group_rule_join_mode, cur_by_tf, prev_by_tf, ctx=ctx))
                    long_entry_groups = []
                    long_entry_group_results = []
            else:
                long_entry = False
                long_entry_groups = []
                long_entry_group_results = []

            if eval_tabs["Long Exit"] and ("Long Exit" in compiled_tab_signals):
                long_exit = bool(compiled_tab_signals["Long Exit"][j])
            elif eval_tabs["Long Exit"]:
                _ensure_eval_snapshots()
                long_exit = _bool_true(eval_tab_generic("Long Exit", rules_model, tab_group_join_mode, group_rule_join_mode, cur_by_tf, prev_by_tf, ctx=ctx))
            else:
                long_exit = False

            if eval_tabs["Short Entry"] and ("Short Entry" in compiled_tab_signals) and (("Short Entry" in compiled_group_signals) or (not active_entry_filter_tabs.get("Short Entry", False))):
                short_entry = bool(compiled_tab_signals["Short Entry"][j])
                short_entry_groups = _matching_group_indices_from_compiled("Short Entry", compiled_group_signals, j, short_entry)
                short_entry_group_results = [True if gi in short_entry_groups else False for gi in range(len(rules_model.get("Short Entry", [])))]
            elif eval_tabs["Short Entry"]:
                _ensure_eval_snapshots()
                if default_eval_impl:
                    short_entry, short_entry_groups, short_entry_group_results = _entry_group_results_for_tab(
                        "Short Entry",
                        rules_model,
                        tab_group_join_mode,
                        group_rule_join_mode,
                        cur_by_tf,
                        prev_by_tf,
                        ctx,
                    )
                else:
                    short_entry = _bool_true(eval_tab_generic("Short Entry", rules_model, tab_group_join_mode, group_rule_join_mode, cur_by_tf, prev_by_tf, ctx=ctx))
                    short_entry_groups = []
                    short_entry_group_results = []
            else:
                short_entry = False
                short_entry_groups = []
                short_entry_group_results = []

            if eval_tabs["Short Exit"] and ("Short Exit" in compiled_tab_signals):
                short_exit = bool(compiled_tab_signals["Short Exit"][j])
            elif eval_tabs["Short Exit"]:
                _ensure_eval_snapshots()
                short_exit = _bool_true(eval_tab_generic("Short Exit", rules_model, tab_group_join_mode, group_rule_join_mode, cur_by_tf, prev_by_tf, ctx=ctx))
            else:
                short_exit = False

            if capture_trade_details and (long_entry or long_exit or short_entry or short_exit):
                _ensure_eval_snapshots()
            if capture_trade_details and long_entry:
                le_trace_txt = _build_tab_rule_trace("Long Entry", rules_model, tab_group_join_mode, group_rule_join_mode, cur_by_tf, prev_by_tf, ctx=ctx)
            if capture_trade_details and long_exit:
                lx_trace_txt = _build_tab_rule_trace("Long Exit", rules_model, tab_group_join_mode, group_rule_join_mode, cur_by_tf, prev_by_tf, ctx=ctx)
            if capture_trade_details and short_entry:
                se_trace_txt = _build_tab_rule_trace("Short Entry", rules_model, tab_group_join_mode, group_rule_join_mode, cur_by_tf, prev_by_tf, ctx=ctx)
            if capture_trade_details and short_exit:
                sx_trace_txt = _build_tab_rule_trace("Short Exit", rules_model, tab_group_join_mode, group_rule_join_mode, cur_by_tf, prev_by_tf, ctx=ctx)

        unreal = 0.0
        if pos is not None:
            if pos["side"] == "LONG":
                unreal = (price - pos["entry_price"]) * pos["qty"]
            else:
                unreal = (pos["entry_price"] - price) * pos["qty"]
            pos["mfe"] = max(pos["mfe"], unreal)
            pos["mae"] = min(pos["mae"], unreal)

        equity_mark = equity + unreal
        high_water = max(high_water, equity_mark)
        dd = high_water - equity_mark
        if dd > max_dd_val:
            max_dd_val = dd
            max_dd_pct = (dd / high_water * 100.0) if high_water > 0 else 0.0

        if (j % equity_curve_stride) == 0 or j == (n - 1):
            eq_rows.append({
                "time": t,
                "equity": equity,
                "equity_mark": equity_mark,
                "pos_side": None if pos is None else pos["side"],
                "price": price,
            })

        if not do_eval:
            continue

        current_step = int(cur_step_index if cur_step_index is not None else 0)
        consumed_sides: set[str] = set()

        if pos is None and scheduled_order is None:
            due_sides = [s for s, p in pending.items() if p is not None and current_step >= int(p.due_step)]
            if len(due_sides) == 2 and cfg.skip_if_both_entries:
                for s in due_sides:
                    pending[s] = None
                    ef_stats.blocked += 1
                    consumed_sides.add(s)
            else:
                for s in due_sides:
                    p = pending.get(s)
                    if p is None or pos is not None or scheduled_order is not None:
                        continue
                    pending_tab_name = getattr(p, "tab_name", ("Long Entry" if s == "LONG" else "Short Entry"))
                    pending_groups = list(getattr(p, "matched_group_indices", []) or [])
                    bars_since_signal = max(1, int(current_step) - int(getattr(p, "signal_step", current_step - 1)))
                    current_group_results = long_entry_group_results if s == "LONG" else short_entry_group_results
                    current_join_mode = tab_group_join_mode.get(pending_tab_name, "AND")
                    setup_still_valid = _setup_still_valid_for_pending(pending_groups, current_group_results, current_join_mode)
                    confirm_decision, detail = _combine_entry_filter_confirm_decisions(
                        s,
                        _entry_filter_cfgs_for_groups(ef_cfg, pending_tab_name, pending_groups),
                        p.signal_bar,
                        _bar_for_time(t),
                        setup_still_valid,
                        bars_since_signal=bars_since_signal,
                        signal_row_index=getattr(p, "signal_snapshot_row_index", None),
                        confirm_row_index=j,
                        stream_1m=subs.get("1m"),
                    )
                    if confirm_decision == "ALLOW":
                        pending[s] = None
                        consumed_sides.add(s)
                        ef_stats.confirmed += 1
                        _schedule_open(
                            s,
                            _format_entry_reason(p.signal_reason, "confirmed", detail, bars_since_signal),
                            signal_time=p.signal_time,
                            order_created_time=t,
                            signal_snapshot=_materialize_pending_snapshot(p),
                            fill_index=(j + 1),
                            rule_trace=p.signal_rule_trace,
                            signal_snapshot_row_index=getattr(p, "signal_snapshot_row_index", None),
                        )
                    elif confirm_decision == "BLOCK":
                        pending[s] = None
                        consumed_sides.add(s)
                        ef_stats.blocked += 1

        if pos is None and scheduled_order is None:
            if long_entry and pending.get("SHORT") is not None and "LONG" not in consumed_sides:
                pending["SHORT"] = None
                ef_stats.cancelled += 1
            if short_entry and pending.get("LONG") is not None and "SHORT" not in consumed_sides:
                pending["LONG"] = None
                ef_stats.cancelled += 1

            if cfg.skip_if_both_entries and long_entry and short_entry:
                pass
            elif long_entry and "LONG" not in consumed_sides and pending.get("LONG") is None:
                _maybe_start_pending_or_open("LONG", "Long Entry", "ENTRY", t, price, current_step, j, long_entry_groups, le_trace_txt)
            elif short_entry and "SHORT" not in consumed_sides and pending.get("SHORT") is None:
                _maybe_start_pending_or_open("SHORT", "Short Entry", "ENTRY", t, price, current_step, j, short_entry_groups, se_trace_txt)

        else:
            do_close = False
            close_reason = ""
            close_rule_trace = ""
            reverse_side = None
            reverse_reason = ""

            if pos is not None and scheduled_order is None:
                failure_abort = _evaluate_bar_failed_pullback_abort(pos, j, price, cfg)
                if failure_abort is not None:
                    do_close = True
                    close_reason = str(failure_abort["reason"])
                    close_rule_trace = "\n".join([
                        f"Exit cause: {close_reason}",
                        "Type: failed-pullback abort",
                        (
                            f"Rule: after {int(getattr(cfg, 'failed_pullback_abort_minutes', 0))} bars, "
                            f"abort when MFE < {float(getattr(cfg, 'failed_pullback_abort_min_mfe_pct', 0.0)):.4f}% "
                            f"and close_return <= {float(getattr(cfg, 'failed_pullback_abort_max_close_return_pct', 0.0)):.4f}%"
                        ),
                        (
                            f"Observed: mfe_pct={float(failure_abort['mfe_pct']):.4f}% "
                            f"close_return_pct={float(failure_abort['close_return_pct']):.4f}%"
                        ),
                    ])
                elif pos["side"] == "LONG":
                    if cfg.allow_reverse and short_entry:
                        do_close = True
                        close_reason = "Reverse to Short"
                        reverse_side = "SHORT"
                        reverse_reason = "Reverse to Short"
                    elif long_exit:
                        do_close = True
                        close_reason = "Long Exit"
                else:
                    if cfg.allow_reverse and long_entry:
                        do_close = True
                        close_reason = "Reverse to Long"
                        reverse_side = "LONG"
                        reverse_reason = "Reverse to Long"
                    elif short_exit:
                        do_close = True
                        close_reason = "Short Exit"

            if do_close:
                tab_for_close = ""
                if str(close_reason) == "Long Exit":
                    tab_for_close = "Long Exit"
                elif str(close_reason) == "Short Exit":
                    tab_for_close = "Short Exit"
                elif str(close_reason) == "Reverse to Short":
                    tab_for_close = "Short Entry"
                elif str(close_reason) == "Reverse to Long":
                    tab_for_close = "Long Entry"
                close_barsago = barsago_profile_cache.get(tab_for_close, {}) if tab_for_close else {}

                scheduled_reverse_side = reverse_side
                scheduled_reverse_reason = reverse_reason
                scheduled_reverse_rule_trace = se_trace_txt if reverse_side == "SHORT" else (le_trace_txt if reverse_side == "LONG" else "")
                if reverse_side is not None:
                    rev_tab = "Long Entry" if reverse_side == "LONG" else "Short Entry"
                    rev_groups = long_entry_groups if reverse_side == "LONG" else short_entry_groups
                    rev_cfgs = _entry_filter_cfgs_for_groups(ef_cfg, rev_tab, rev_groups)
                    reversal_filter_active = any(cfg.is_active() and cfg.apply_to_reversals for cfg in rev_cfgs)
                    if reversal_filter_active:
                        sig_bar = _bar_for_time(t)
                        reversal_cfgs = [
                            cfg for cfg in rev_cfgs
                            if isinstance(cfg, EntryFilterConfig) and cfg.is_active() and cfg.apply_to_reversals
                        ]
                        decision, detail, confirm_bars = _combine_entry_filter_signal_decisions(reverse_side, reversal_cfgs, sig_bar)
                        if decision == "ALLOW":
                            scheduled_reverse_reason = _format_entry_reason(reverse_reason, "allowed", detail)
                            scheduled_reverse_rule_trace = se_trace_txt if reverse_side == "SHORT" else le_trace_txt
                        elif decision == "WAIT" and sig_bar is not None:
                            pending[reverse_side] = _pending_from_signal(
                                side=reverse_side,
                                origin="REVERSE",
                                signal_time=t,
                                signal_bar=sig_bar,
                                signal_snapshot=None,
                                signal_snapshot_row_index=j,
                                signal_reason=reverse_reason,
                                signal_rule_trace=(se_trace_txt if reverse_side == "SHORT" else le_trace_txt),
                                bars_ago=barsago_profile_cache.get(rev_tab, {"tab": str(rev_tab), "exact": [], "within": [], "items": []}),
                                current_step=current_step,
                                confirm_bars=confirm_bars,
                                tab_name=rev_tab,
                                matched_group_indices=rev_groups,
                            )
                            ef_stats.created += 1
                            scheduled_reverse_side = None
                            scheduled_reverse_reason = ""
                            scheduled_reverse_rule_trace = ""
                        else:
                            ef_stats.blocked += 1
                            if "hard-skip" in str(detail):
                                ef_stats.hard_skipped += 1
                            scheduled_reverse_side = None
                            scheduled_reverse_reason = ""
                            scheduled_reverse_rule_trace = ""

                _schedule_close(
                    close_reason=close_reason,
                    signal_time=t,
                    order_created_time=t,
                    exit_snapshot=None,
                    exit_barsago=close_barsago,
                    fill_index=(j + 1),
                    reverse_side=scheduled_reverse_side,
                    reverse_reason=scheduled_reverse_reason,
                    rule_trace=(close_rule_trace or (lx_trace_txt if pos["side"] == "LONG" and long_exit else (sx_trace_txt if pos["side"] == "SHORT" and short_exit else (se_trace_txt if scheduled_reverse_side == "SHORT" else le_trace_txt)))),
                    reverse_rule_trace=scheduled_reverse_rule_trace,
                    exit_snapshot_row_index=j,
                )

        if progress_cb and j % max(1, n // 50) == 0:
            pct = int(j / max(1, n - 1) * 100)
            progress_cb(f"Backtest… {pct}%", pct)

    if pos is not None:
        t = subs["1m"].index[-1]
        final_price = float("nan")
        try:
            final_price = float(price_series.iloc[-1])
        except Exception:
            final_price = float("nan")
        if not np.isfinite(final_price):
            try:
                final_price = float(fallback_close.iloc[-1])
            except Exception:
                final_price = float("nan")
        if not np.isfinite(final_price):
            try:
                final_price = float(subs["1m"]["close"].iloc[-1])
            except Exception:
                final_price = float("nan")
        if not np.isfinite(final_price):
            try:
                final_price = float(subs["1m"]["open"].iloc[-1])
            except Exception:
                final_price = float("nan")
        if not np.isfinite(final_price):
            try:
                final_price = float(exec_open_prices[-1])
            except Exception:
                final_price = float("nan")
        if not np.isfinite(final_price):
            raise ValueError("Unable to determine a valid final OHLCV close price for force-close.")
        cur_by_tf = _build_current_snapshots_for_row(subs, len(subs["1m"]) - 1, snapshot_cache=full_snapshot_cache, required_fields=None)
        _close_now(
            fill_time=t,
            fill_price=final_price,
            close_reason="Force Close (End)",
            exit_snapshot=_full_current_snapshots(),
            exit_barsago={},
            ambiguous_exit=False,
            exit_rule_trace=_build_non_rule_exit_trace("Force Close (End)"),
            exit_signal_time=t,
            exit_order_created_time=t,
            exit_row_index=(len(subs["1m"]) - 1),
        )

    eq_df = pd.DataFrame(eq_rows)
    if not eq_df.empty:
        eq_df = eq_df.set_index("time")

    equity_end = float(equity)
    net_profit = equity_end - float(cfg.initial_balance)
    net_profit_from_trades = sum(t.net_pnl for t in trades)
    gross_profit = sum(t.net_pnl for t in trades if t.net_pnl > 0)
    gross_loss = -sum(t.net_pnl for t in trades if t.net_pnl < 0)
    gross_pnl_total = sum(t.gross_pnl for t in trades)
    fees_total = sum(t.fees for t in trades)
    fees_entry_total = sum(float(getattr(t, "fee_entry", 0.0) or 0.0) for t in trades)
    fees_exit_total = sum(float(getattr(t, "fee_exit", 0.0) or 0.0) for t in trades)
    wins = [t for t in trades if t.net_pnl > 0]
    losses = [t for t in trades if t.net_pnl < 0]

    summary = _apply_trade_reporting_metrics({
        "symbol": symbol,
        "step_timeframe": _normalize_step_timeframe(getattr(cfg, "step_timeframe", "1m")),
        "start": start,
        "end": end,
        "initial_balance": cfg.initial_balance,
        "ending_balance": equity_end,
        "net_profit": net_profit,
        "net_profit_from_trades": net_profit_from_trades,
        "gross_pnl_total": gross_pnl_total,
        "fees_total": fees_total,
        "fees_entry_total": fees_entry_total,
        "fees_exit_total": fees_exit_total,
        "return_pct": (equity_end - cfg.initial_balance) / cfg.initial_balance * 100.0 if cfg.initial_balance else 0.0,
        "trades": len(trades),
        "wins": len(wins),
        "losses": len(losses),
        "win_rate_pct": (len(wins) / len(trades) * 100.0) if trades else 0.0,
        "gross_profit": gross_profit,
        "gross_loss": gross_loss,
        "profit_factor": (gross_profit / gross_loss) if gross_loss > 0 else (float("inf") if gross_profit > 0 else 0.0),
        "avg_trade": (net_profit / len(trades)) if trades else 0.0,
        "avg_win": (sum(t.net_pnl for t in wins) / len(wins)) if wins else 0.0,
        "avg_loss": (sum(t.net_pnl for t in losses) / len(losses)) if losses else 0.0,
        "max_drawdown": max_dd_val,
        "max_drawdown_pct": max_dd_pct,
        "avg_duration_min": (sum(t.duration_min for t in trades) / len(trades)) if trades else 0.0,
        "stop_loss_pct": float(getattr(cfg, "stop_loss_pct", 0.0) or 0.0),
        "take_profit_pct": float(getattr(cfg, "take_profit_pct", 0.0) or 0.0),
        "stop_loss_mode": str(getattr(cfg, "stop_loss_mode", "PERCENT") or "PERCENT"),
        "stop_loss_value": float(getattr(cfg, "stop_loss_value", getattr(cfg, "stop_loss_pct", 0.0)) or 0.0),
        "stop_loss_timeframe": str(getattr(cfg, "stop_loss_timeframe", "1m") or "1m"),
        "stop_loss_field": str(getattr(cfg, "stop_loss_field", "") or ""),
        "stop_loss_offset_pct": float(getattr(cfg, "stop_loss_offset_pct", 0.0) or 0.0),
        "take_profit_mode": str(getattr(cfg, "take_profit_mode", "PERCENT") or "PERCENT"),
        "take_profit_value": float(getattr(cfg, "take_profit_value", getattr(cfg, "take_profit_pct", 0.0)) or 0.0),
        "take_profit_timeframe": str(getattr(cfg, "take_profit_timeframe", "1m") or "1m"),
        "take_profit_field": str(getattr(cfg, "take_profit_field", "") or ""),
        "take_profit_offset_pct": float(getattr(cfg, "take_profit_offset_pct", 0.0) or 0.0),
        "entry_filter": _entry_filter_summary_payload(ef_cfg, ef_stats),
    }, trades)

    if progress_cb:
        progress_cb("Backtest complete.", 100)

    return BacktestResult(config=cfg, start=start, end=end, trades=trades, equity_curve=eq_df, summary=summary, replay_context=replay_context)

@dataclass
class _TFBarState:
    tf: str
    tf_ms: int
    # current timeframe bucket boundaries [bucket_start, bucket_end)
    bucket_start_ms: int
    bucket_end_ms: int

    # last trade price observed (used as forming close)
    last_price: Optional[float] = None

    # Indicator history (closed bars) for seeding / fallback
    closes: List[float] = field(default_factory=list)

    # Closed-bar EMA states (TradingView-style, committed only on bar close)
    ema_fast: Optional[float] = None
    ema_slow: Optional[float] = None
    ema_sig: Optional[float] = None

    # Closed-bar MACD/Signal values
    macd_closed: Optional[float] = None
    sig_closed: Optional[float] = None

    # Previous closed-bar MACD for angle-dot (vs prev bar)
    macd_prev_bar: Optional[float] = None

    # Previous closed-bar Signal for signal-angle-dot (vs prev bar)
    sig_prev_bar: Optional[float] = None

    # PPO chain (matches compute_live_indicators: ppo_raw -> SMA(PPO_SMOOTH) -> compare last vs prev)
    ppo_raw_tail: List[float] = field(default_factory=list)   # last (PPO_SMOOTH-1) closed ppo_raw values
    dppo_closed: Optional[float] = None                        # last closed d_ppo value
    dppo_prev_bar: Optional[float] = None                      # previous closed d_ppo value

    ppo_dot_closed: Optional[str] = None  # GREEN/RED at last close (based on d_ppo slope)
    flip_age: Optional[int] = None        # candles since PPO dot flipped (closed bars)

    def _alpha(self, length: int) -> float:
        return 2.0 / (length + 1.0)

    def _is_seeded(self) -> bool:
        return (self.ema_fast is not None and np.isfinite(self.ema_fast) and
                self.ema_slow is not None and np.isfinite(self.ema_slow) and
                self.ema_sig is not None and np.isfinite(self.ema_sig))

    @staticmethod
    def _last_finite(a: np.ndarray) -> Optional[float]:
        for x in a[::-1]:
            if np.isfinite(x):
                return float(x)
        return None

    def ensure_seeded_from_closes(self):
        """Seed EMA states and PPO chain from self.closes so tick-preview math matches TradingView."""
        # Minimum number of *closed* bars needed so that TradingView-style MACD + Signal are available.
        # With ema_tradingview seeding, MACD becomes finite after MACD_SLOW bars, and Signal becomes
        # finite after MACD_SIGNAL additional MACD values. The first finite Signal is therefore at:
        #   (MACD_SLOW - 1) + (MACD_SIGNAL - 1)
        # which means we need at least (MACD_SLOW + MACD_SIGNAL - 1) closes.
        #
        # PPO (EMA-based) becomes available no later than PPO_SLOW + PPO_SMOOTH bars.
        # We also respect MIN_FULL_CLOSES to match the bar-close backtest readiness threshold.
        min_need = max(MACD_SLOW + MACD_SIGNAL - 1, PPO_SLOW + PPO_SMOOTH, MIN_FULL_CLOSES)
        if len(self.closes) < min_need:
            return

        arr = np.asarray(self.closes, dtype=float)
        if len(arr) < 3:
            return

        ema_f = ema_tradingview(arr, MACD_FAST)
        ema_s = ema_tradingview(arr, MACD_SLOW)
        macd_line = ema_f - ema_s
        sig = ema_tradingview(macd_line, MACD_SIGNAL)

        self.ema_fast = self._last_finite(ema_f)
        self.ema_slow = self._last_finite(ema_s)
        self.ema_sig = self._last_finite(sig)
        self.macd_closed = self._last_finite(macd_line)
        self.sig_closed = self._last_finite(sig)

        # macd_prev_bar for angle dot
        if len(macd_line) >= 2 and np.isfinite(macd_line[-2]):
            self.macd_prev_bar = float(macd_line[-2])


        # sig_prev_bar for signal angle dot
        if len(sig) >= 2 and np.isfinite(sig[-2]):
            self.sig_prev_bar = float(sig[-2])

        # PPO chain (raw + SMA smooth)
        with np.errstate(divide="ignore", invalid="ignore"):
            ppo_raw = (ema_f - ema_s) / ema_s * 100.0

        win = max(int(PPO_SMOOTH), 1)
        dppo = np.full_like(ppo_raw, np.nan)
        if win == 1:
            dppo[:] = ppo_raw
        else:
            for i in range(win - 1, len(ppo_raw)):
                w = ppo_raw[i - win + 1:i + 1]
                if np.all(np.isfinite(w)):
                    dppo[i] = float(np.mean(w))

        self.dppo_closed = self._last_finite(dppo)
        if len(dppo) >= 2 and np.isfinite(dppo[-2]):
            self.dppo_prev_bar = float(dppo[-2])

        # store tail of ppo_raw values (closed) for preview smoothing
        tail_len = max(win - 1, 0)
        self.ppo_raw_tail = []
        if tail_len > 0:
            # take last tail_len finite raw values up to last closed bar
            raw_finite = [float(x) for x in ppo_raw.tolist() if np.isfinite(x)]
            if len(raw_finite) >= tail_len:
                self.ppo_raw_tail = raw_finite[-tail_len:]

        # derive dot color + flip age from dppo series
        if self.dppo_closed is not None and self.dppo_prev_bar is not None:
            self.ppo_dot_closed = "GREEN" if float(self.dppo_closed) > float(self.dppo_prev_bar) else "RED"
        try:
            if len(dppo) >= 3:
                self.flip_age = _ppo_flip_age(dppo)
        except Exception:
            pass

    def _preview_macd(self, price: float) -> Tuple[Optional[float], Optional[float], Optional[float], Optional[float]]:
        """Return (ema_fast_preview, ema_slow_preview, macd_preview, sig_preview) using current price as forming close."""
        if not self._is_seeded():
            return (None, None, None, None)

        af = self._alpha(MACD_FAST)
        as_ = self._alpha(MACD_SLOW)
        ag = self._alpha(MACD_SIGNAL)

        ema_f = (af * price) + (1.0 - af) * float(self.ema_fast)
        ema_s = (as_ * price) + (1.0 - as_) * float(self.ema_slow)
        macd = ema_f - ema_s
        sig = (ag * macd) + (1.0 - ag) * float(self.ema_sig)
        return (ema_f, ema_s, macd, sig)

    def _preview_dppo(self, ema_f: float, ema_s: float) -> Optional[float]:
        """Preview d_ppo (SMA-smoothed PPO raw) for current forming candle."""
        if not np.isfinite(ema_s) or ema_s == 0:
            return None
        with np.errstate(divide="ignore", invalid="ignore"):
            ppo_raw_now = float((ema_f - ema_s) / ema_s * 100.0)

        win = max(int(PPO_SMOOTH), 1)
        if win == 1:
            return ppo_raw_now

        # need tail_len = win-1 raw values
        tail_len = win - 1
        if len(self.ppo_raw_tail) < tail_len:
            return None
        vals = self.ppo_raw_tail[-tail_len:] + [ppo_raw_now]
        if not all(np.isfinite(v) for v in vals):
            return None
        return float(sum(vals) / len(vals))

    def snapshot(self, t_ms: int, price: float) -> Dict[str, Any]:
        """Snapshot for eval_rule_generic: macd/signal/ms/angle/sig_angle/ppo/flip_age/time"""
        if not self._is_seeded():
            self.ensure_seeded_from_closes()

        ema_f, ema_s, macd, sig = self._preview_macd(price)
        if macd is None or sig is None or (not np.isfinite(macd)) or (not np.isfinite(sig)):
            return {
                "time": pd.Timestamp(t_ms, unit="ms", tz="UTC"),
                "macd": None,
                "signal": None,
                "ms": None,
                "angle": None,
                "sig_angle": None,
                "ppo": None,
                "flip_age": self.flip_age,
            }

        ms = "GREEN" if macd > sig else "RED"

        angle = None
        if self.macd_prev_bar is not None and np.isfinite(self.macd_prev_bar):
            angle = "GREEN" if macd > float(self.macd_prev_bar) else "RED"


        sig_angle = None
        if self.sig_prev_bar is not None and np.isfinite(self.sig_prev_bar):
            sig_angle = "GREEN" if sig > float(self.sig_prev_bar) else "RED"

        ppo_color = None
        if ema_f is not None and ema_s is not None:
            dppo_now = self._preview_dppo(float(ema_f), float(ema_s))
            if dppo_now is not None and self.dppo_closed is not None and np.isfinite(self.dppo_closed):
                ppo_color = "GREEN" if float(dppo_now) > float(self.dppo_closed) else "RED"
            else:
                ppo_color = self.ppo_dot_closed

        return {
            "time": pd.Timestamp(t_ms, unit="ms", tz="UTC"),
            "macd": float(macd),
            "signal": float(sig),
            "ms": ms,
            "angle": angle,
            "sig_angle": sig_angle,
            "ppo": ppo_color,
            "flip_age": self.flip_age,
        }

    def close_bar(self, close_ms: int):
        """Finalize the bar at close_ms using self.last_price."""
        if self.last_price is None:
            # advance bucket anyway
            self.bucket_start_ms = self.bucket_end_ms
            self.bucket_end_ms = self.bucket_start_ms + self.tf_ms
            return

        c = float(self.last_price)
        self.closes.append(c)
        if len(self.closes) > MAX_STORED_CLOSES:
            self.closes = self.closes[-MAX_STORED_CLOSES:]

        if self._is_seeded():
            # shift previous references
            if self.macd_closed is not None and np.isfinite(self.macd_closed):
                self.macd_prev_bar = float(self.macd_closed)
            if self.sig_closed is not None and np.isfinite(self.sig_closed):
                self.sig_prev_bar = float(self.sig_closed)
            if self.dppo_closed is not None and np.isfinite(self.dppo_closed):
                self.dppo_prev_bar = float(self.dppo_closed)

            # commit EMA
            af = self._alpha(MACD_FAST)
            as_ = self._alpha(MACD_SLOW)
            ag = self._alpha(MACD_SIGNAL)

            self.ema_fast = (af * c) + (1.0 - af) * float(self.ema_fast)
            self.ema_slow = (as_ * c) + (1.0 - as_) * float(self.ema_slow)
            macd = float(self.ema_fast) - float(self.ema_slow)
            self.macd_closed = macd
            self.ema_sig = (ag * macd) + (1.0 - ag) * float(self.ema_sig)
            self.sig_closed = float(self.ema_sig)

            # update PPO raw tail + dppo
            with np.errstate(divide="ignore", invalid="ignore"):
                ppo_raw_new = float((float(self.ema_fast) - float(self.ema_slow)) / float(self.ema_slow) * 100.0) if float(self.ema_slow) != 0 else float("nan")

            win = max(int(PPO_SMOOTH), 1)
            tail_len = max(win - 1, 0)

            # compute new dppo
            dppo_new = None
            if win == 1:
                dppo_new = ppo_raw_new if np.isfinite(ppo_raw_new) else None
            else:
                if tail_len > 0 and len(self.ppo_raw_tail) >= tail_len and np.isfinite(ppo_raw_new):
                    vals = self.ppo_raw_tail[-tail_len:] + [ppo_raw_new]
                    if all(np.isfinite(v) for v in vals):
                        dppo_new = float(sum(vals) / len(vals))

            if dppo_new is not None:
                self.dppo_closed = float(dppo_new)

            # update tail for next bar
            if tail_len > 0 and np.isfinite(ppo_raw_new):
                self.ppo_raw_tail.append(float(ppo_raw_new))
                if len(self.ppo_raw_tail) > tail_len:
                    self.ppo_raw_tail = self.ppo_raw_tail[-tail_len:]

            # dot color + flip age
            new_dot = None
            if self.dppo_closed is not None and self.dppo_prev_bar is not None and np.isfinite(self.dppo_closed) and np.isfinite(self.dppo_prev_bar):
                new_dot = "GREEN" if float(self.dppo_closed) > float(self.dppo_prev_bar) else "RED"

            if new_dot is not None:
                if self.ppo_dot_closed is None:
                    self.ppo_dot_closed = new_dot
                    self.flip_age = 0
                else:
                    if new_dot != self.ppo_dot_closed:
                        self.ppo_dot_closed = new_dot
                        self.flip_age = 0
                    else:
                        self.flip_age = 0 if self.flip_age is None else (int(self.flip_age) + 1)
        else:
            # attempt (re)seed now that we have more closes
            self.ensure_seeded_from_closes()

        # advance bucket
        self.bucket_start_ms = self.bucket_end_ms
        self.bucket_end_ms = self.bucket_start_ms + self.tf_ms

def _init_tf_states_from_warmup(df_1m_warm: "pd.DataFrame", tfs: List[str], start_ms: int) -> Dict[str, _TFBarState]:
    """Build per-timeframe close history from 1m warmup klines and seed EMA states."""
    tf_ms_map = {tf: interval_to_ms(tf) for tf in tfs}
    # start bucket boundaries based on start_ms
    out: Dict[str, _TFBarState] = {}
    for tf in tfs:
        ms = tf_ms_map[tf]
        bstart = (start_ms // ms) * ms
        out[tf] = _TFBarState(tf=tf, tf_ms=ms, bucket_start_ms=bstart, bucket_end_ms=bstart + ms)

    if df_1m_warm is None or df_1m_warm.empty:
        return out

    # Ensure chronological
    d = df_1m_warm.sort_values("open_time")
    # For each tf, build closed bar closes by taking the 1m close at each tf boundary.
    for _, row in d.iterrows():
        ot = row["open_time"]
        try:
            ot_ms = int(pd.Timestamp(ot).value // 1_000_000)
        except Exception:
            continue
        ct_ms = ot_ms + 60_000
        c = float(row["close"])
        for tf, st in out.items():
            # when 1m bar closes, update last_price
            st.last_price = c
            # close tf bar when boundary hit
            if (ct_ms % st.tf_ms) == 0:
                st.closes.append(c)
                if len(st.closes) > MAX_STORED_CLOSES:
                    st.closes = st.closes[-MAX_STORED_CLOSES:]

    # Seed all states from closes
    for st in out.values():
        st.ensure_seeded_from_closes()

    return out

def run_backtest_tick(symbol: str,
                      df_1m_full: "pd.DataFrame",
                      start: "pd.Timestamp",
                      end: "pd.Timestamp",
                      tfs: List[str],
                      rules_model: Dict[str, List[List["Rule"]]],
                      tab_group_join_mode: Dict[str, str],
                      group_rule_join_mode: Dict[str, List[str]],
                      cfg: BacktestConfig,
                      cache_dir: str = "data_cache",
                      progress_cb=None,
                      streams_full: Optional[Dict[str, "pd.DataFrame"]] = None,
                      macd_impl: str = "TRADINGVIEW",
                      entry_filters: Optional[Dict[str, Any]] = None) -> BacktestResult:
    """AggTrade backtest with candle-close-equivalent signals.

    Phase 4 standardizes this engine on the shared lifecycle foundation:
    signals are still produced from completed bars, but entries/exits fill on
    the first eligible aggregate trade strictly after the signal timestamp.
    """
    if start.tzinfo is None:
        start = start.tz_localize("UTC")
    else:
        start = start.tz_convert("UTC")
    if end.tzinfo is None:
        end = end.tz_localize("UTC")
    else:
        end = end.tz_convert("UTC")

    df_1m_full = _freeze_backtest_frame(df_1m_full, end=end)
    streams_full = _freeze_backtest_streams(streams_full or {}, end=end) if streams_full is not None else streams_full
    replay_context = None
    if streams_full is not None and isinstance(streams_full, dict) and "1m" in streams_full and streams_full.get("1m") is not None:
        try:
            replay_context = build_backtest_replay_context(
                symbol=symbol,
                streams_full=streams_full,
                start=start,
                end=end,
                rules_model=rules_model,
                tab_group_join_mode=tab_group_join_mode,
                group_rule_join_mode=group_rule_join_mode,
                df_1m_full=df_1m_full,
                entry_filters=entry_filters,
            )
        except Exception:
            replay_context = None

    start_ms = int(start.value // 1_000_000)
    end_ms = int(end.value // 1_000_000)

    def _progress(msg: str):
        if progress_cb:
            try:
                progress_cb(msg, None)
            except Exception:
                pass

    ef_cfg = _normalize_entry_filter_map(entry_filters)
    ef_stats = _EntryFilterStats()
    equity_curve_stride = max(1, int(getattr(cfg, "equity_curve_stride", 1) or 1))

    # Preferred path: use precomputed candle-close signals and only fill them on ticks.
    if streams_full is not None and isinstance(streams_full, dict) and ("1m" in streams_full) and (streams_full["1m"] is not None) and (not streams_full["1m"].empty):
        df1 = streams_full["1m"]
        if not isinstance(df1.index, pd.DatetimeIndex):
            raise ValueError("Tick backtest needs streams_full['1m'] indexed by DatetimeIndex.")

        used_tfs = [tf for tf in tfs if tf in streams_full and streams_full[tf] is not None and (not streams_full[tf].empty)]
        if "1m" not in used_tfs:
            used_tfs = ["1m"] + used_tfs

        idx = df1.index
        start_pos = int(idx.searchsorted(start))
        end_pos = int(idx.searchsorted(end, side="right") - 1)
        if end_pos <= 0 or start_pos > end_pos:
            raise ValueError("No 1m candles in selected window (start/end).")

        signal_times_ms: List[int] = []
        signal_map: Dict[int, Tuple[bool, bool, bool, bool]] = {}
        signal_entry_group_map: Dict[int, Dict[str, List[int]]] = {}
        signal_snapshot_row_map: Dict[int, int] = {}
        snap_map: Dict[int, Dict[str, Dict[str, Any]]] = {}
        trace_map: Dict[int, Dict[str, str]] = {}
        trace_snapshot_map: Dict[int, Tuple[Dict[str, Dict[str, Any]], Dict[str, Dict[str, Any]]]] = {}
        trace_ctx_map: Dict[int, RuleEvalContext] = {}
        eval_required_fields = _minimal_snapshot_required_fields(rules_model)
        eval_snapshot_cache = _RowSnapshotCache(required_fields=eval_required_fields, max_rows_per_tf=1024)
        full_snapshot_cache = _RowSnapshotCache(required_fields=None, max_rows_per_tf=128)
        range_audit_enabled = bool((replay_context or {}).get("__enable_range_audit")) and bool(set(eval_required_fields) & set(RANGE_FILTER_FIELDS))
        default_eval_impl = str(getattr(eval_tab_generic, "__module__", "")) == "app.rules"
        active_tabs = {
            "Long Entry": bool(rules_model.get("Long Entry")) or (not default_eval_impl),
            "Long Exit": bool(rules_model.get("Long Exit")) or (not default_eval_impl),
            "Short Entry": bool(rules_model.get("Short Entry")) or (not default_eval_impl),
            "Short Exit": bool(rules_model.get("Short Exit")) or (not default_eval_impl),
        }
        barsago_profile_cache: Dict[str, Dict[str, Any]] = {
            tab: _barsago_profile_for_tab(rules_model, tab)
            for tab in ("Long Entry", "Long Exit", "Short Entry", "Short Exit")
        }

        _progress("Preparing 1m-close signals for tick fills…")
        ctx = RuleEvalContext(step_index=0)
        step_tf = _normalize_step_timeframe(getattr(cfg, "step_timeframe", "1m"))
        decision_mask = _decision_mask_for_step_tf(idx, step_tf)
        prev_source_row_by_tf: Dict[str, Optional[np.ndarray]] = {
            tf: _prev_source_row_index_for_df(streams_full.get(tf), tf)
            for tf in used_tfs
        }
        compiled_tab_signals: Dict[str, np.ndarray] = {}
        compiled_group_signals: Dict[str, List[np.ndarray]] = {}
        if default_eval_impl:
            try:
                compiled_tab_signals = _compile_tab_signal_arrays(
                    rules_model,
                    tab_group_join_mode,
                    group_rule_join_mode,
                    {tf: streams_full.get(tf) for tf in used_tfs if streams_full.get(tf) is not None and not streams_full.get(tf).empty},
                    prev_source_row_by_tf,
                    len(df1),
                )
            except Exception:
                compiled_tab_signals = {}
            try:
                compiled_group_signals = _compile_tab_group_signal_arrays(
                    rules_model,
                    group_rule_join_mode,
                    {tf: streams_full.get(tf) for tf in used_tfs if streams_full.get(tf) is not None and not streams_full.get(tf).empty},
                    prev_source_row_by_tf,
                    len(df1),
                )
            except Exception:
                compiled_group_signals = {}
        prev_decision_j: Optional[int] = None
        step_counter: int = 0
        lookback = max(1, _max_bars_ago_needed(rules_model))
        try:
            _step_ms = int(interval_to_ms(step_tf))
            _step_minutes = max(1, int(_step_ms // 60_000))
        except Exception:
            _step_minutes = 1
        prep_start = max(1, start_pos - int(max(lookback * _step_minutes, _step_minutes)))
        try:
            ctx.ensure_max_history(max(512, int(lookback) + 32))
        except Exception:
            pass
        range_audit_rows: List[Dict[str, Any]] = []
        subs_tick = {tf: streams_full.get(tf) for tf in used_tfs if streams_full.get(tf) is not None and not streams_full.get(tf).empty}
        replay_eval_snapshot_pairs = _replay_eval_snapshot_pair_cache(
            replay_context,
            step_tf=step_tf,
            subs=subs_tick,
            required_fields=eval_required_fields,
        )
        for j in range(prep_start, end_pos + 1):
            if not bool(decision_mask[j]):
                continue
            t = idx[j]
            if t < start or t > end:
                continue

            cur_by_tf: Dict[str, Dict[str, Any]] = {}
            prev_by_tf: Dict[str, Dict[str, Any]] = {}
            eval_snapshots_ready = False

            def _ensure_eval_snapshots() -> None:
                nonlocal cur_by_tf, prev_by_tf, eval_snapshots_ready
                if eval_snapshots_ready:
                    return
                cached_pair = None
                if replay_eval_snapshot_pairs is not None:
                    cached_pair = replay_eval_snapshot_pairs.get(int(j))
                if (
                    isinstance(cached_pair, tuple)
                    and len(cached_pair) == 2
                    and isinstance(cached_pair[0], dict)
                    and isinstance(cached_pair[1], dict)
                ):
                    cur_by_tf, prev_by_tf = cached_pair
                else:
                    cur_by_tf, prev_by_tf = _build_eval_snapshots_for_row(
                        subs_tick,
                        row_index=j,
                        prev_decision_j=prev_decision_before,
                        prev_source_row_by_tf=prev_source_row_by_tf,
                        step_tf=step_tf,
                        snapshot_cache=eval_snapshot_cache,
                        required_fields=eval_required_fields,
                    )
                    if replay_eval_snapshot_pairs is not None:
                        replay_eval_snapshot_pairs[int(j)] = (cur_by_tf, prev_by_tf)
                ctx.step_index = int(cur_step_index)
                try:
                    ctx.observe(cur_by_tf)
                except Exception:
                    pass
                try:
                    if range_audit_enabled and len(range_audit_rows) < 2000:
                        for _tf in sorted(cur_by_tf.keys()):
                            if any(k in cur_by_tf.get(_tf, {}) for k in ("range_filter_state", "range_filter_buy", "range_filter_sell", "range_filter_cond_ini")):
                                range_audit_rows.append(_range_audit_row(t, _tf, cur_by_tf.get(_tf), prev_by_tf.get(_tf)))
                except Exception:
                    pass
                eval_snapshots_ready = True

            prev_decision_before = prev_decision_j
            cur_step_index = int(step_counter)
            ctx.step_index = int(cur_step_index)

            if active_tabs["Long Entry"] and ("Long Entry" in compiled_tab_signals) and (("Long Entry" in compiled_group_signals) or (not active_entry_filter_tabs.get("Long Entry", False))):
                long_entry = bool(compiled_tab_signals["Long Entry"][j])
                long_entry_groups = _matching_group_indices_from_compiled("Long Entry", compiled_group_signals, j, long_entry)
            elif active_tabs["Long Entry"]:
                _ensure_eval_snapshots()
                if default_eval_impl:
                    long_entry, long_entry_groups, _ = _entry_group_results_for_tab(
                        "Long Entry",
                        rules_model,
                        tab_group_join_mode,
                        group_rule_join_mode,
                        cur_by_tf,
                        prev_by_tf,
                        ctx,
                    )
                else:
                    long_entry = _bool_true(eval_tab_generic("Long Entry", rules_model, tab_group_join_mode, group_rule_join_mode, cur_by_tf, prev_by_tf, ctx=ctx))
                    long_entry_groups = []
            else:
                long_entry = False
                long_entry_groups = []

            if active_tabs["Long Exit"] and ("Long Exit" in compiled_tab_signals):
                long_exit = bool(compiled_tab_signals["Long Exit"][j])
            elif active_tabs["Long Exit"]:
                _ensure_eval_snapshots()
                long_exit = _bool_true(eval_tab_generic("Long Exit", rules_model, tab_group_join_mode, group_rule_join_mode, cur_by_tf, prev_by_tf, ctx=ctx))
            else:
                long_exit = False

            if active_tabs["Short Entry"] and ("Short Entry" in compiled_tab_signals) and (("Short Entry" in compiled_group_signals) or (not active_entry_filter_tabs.get("Short Entry", False))):
                short_entry = bool(compiled_tab_signals["Short Entry"][j])
                short_entry_groups = _matching_group_indices_from_compiled("Short Entry", compiled_group_signals, j, short_entry)
            elif active_tabs["Short Entry"]:
                _ensure_eval_snapshots()
                if default_eval_impl:
                    short_entry, short_entry_groups, _ = _entry_group_results_for_tab(
                        "Short Entry",
                        rules_model,
                        tab_group_join_mode,
                        group_rule_join_mode,
                        cur_by_tf,
                        prev_by_tf,
                        ctx,
                    )
                else:
                    short_entry = _bool_true(eval_tab_generic("Short Entry", rules_model, tab_group_join_mode, group_rule_join_mode, cur_by_tf, prev_by_tf, ctx=ctx))
                    short_entry_groups = []
            else:
                short_entry = False
                short_entry_groups = []

            if active_tabs["Short Exit"] and ("Short Exit" in compiled_tab_signals):
                short_exit = bool(compiled_tab_signals["Short Exit"][j])
            elif active_tabs["Short Exit"]:
                _ensure_eval_snapshots()
                short_exit = _bool_true(eval_tab_generic("Short Exit", rules_model, tab_group_join_mode, group_rule_join_mode, cur_by_tf, prev_by_tf, ctx=ctx))
            else:
                short_exit = False

            prev_decision_j = int(j)
            step_counter += 1

            t_ms = int(t.value // 1_000_000)
            signal_times_ms.append(t_ms)
            signal_map[t_ms] = (long_entry, long_exit, short_entry, short_exit)
            signal_entry_group_map[t_ms] = {
                "Long Entry": [int(idx) for idx in (long_entry_groups or [])],
                "Short Entry": [int(idx) for idx in (short_entry_groups or [])],
            }
            if long_entry or long_exit or short_entry or short_exit:
                signal_snapshot_row_map[t_ms] = int(j)
                if not eval_snapshots_ready:
                    _ensure_eval_snapshots()
                trace_snapshot_map[t_ms] = (cur_by_tf, prev_by_tf)
                try:
                    import copy as _copy
                    trace_ctx_map[t_ms] = _copy.deepcopy(ctx)
                except Exception:
                    trace_ctx_map[t_ms] = RuleEvalContext(step_index=int(cur_step_index))

        signal_times_ms = sorted(set(signal_times_ms))
        if not signal_times_ms:
            return BacktestResult(
                config=cfg,
                start=start,
                end=end,
                trades=[],
                equity_curve=pd.DataFrame(columns=["equity", "equity_mark"]).set_index(pd.DatetimeIndex([], tz="UTC")),
                summary={
                    "symbol": symbol,
                    "step_timeframe": _normalize_step_timeframe(getattr(cfg, "step_timeframe", "1m")),
                    "start": start,
                    "end": end,
                    "initial_balance": cfg.initial_balance,
                    "ending_balance": cfg.initial_balance,
                    "net_profit": 0.0,
                    "return_pct": 0.0,
                    "trades": 0,
                    "wins": 0,
                    "losses": 0,
                    "win_rate_pct": 0.0,
                    "gross_profit": 0.0,
                    "gross_loss": 0.0,
                    "profit_factor": 0.0,
                    "avg_trade": 0.0,
                    "avg_win": 0.0,
                    "avg_loss": 0.0,
                    "max_drawdown": 0.0,
                    "max_drawdown_pct": 0.0,
                    "avg_duration_min": 0.0,
                    "entry_filter": _entry_filter_summary_payload(ef_cfg, ef_stats),
                },
                replay_context=replay_context,
            )

        _progress("AggTrade backtest: filling on first eligible aggTrade after each signal…")
        step_bars = _build_step_bars(df_1m_full, step_tf) if _entry_filter_any_active(ef_cfg) else pd.DataFrame()
        pending: Dict[str, Optional[_PendingEntry]] = {"LONG": None, "SHORT": None}

        balance = float(cfg.initial_balance)
        equity = float(cfg.initial_balance)
        pos = 0
        entry_price = 0.0
        entry_time = None
        entry_snapshot = {}
        entry_barsago = {}
        entry_reason = ""
        entry_rule_trace_text = ""
        position_lifecycle: Optional[_TradeLifecycle] = None
        stop_loss_price: Optional[float] = None
        take_profit_price: Optional[float] = None
        break_even_price: Optional[float] = None
        break_even_activation_price: Optional[float] = None
        break_even_armed = False
        trailing_stop_pct = _trailing_stop_pct(cfg)
        trailing_stop_price: Optional[float] = None
        trailing_stop_anchor_price: Optional[float] = None
        qty = 0.0
        trade_id = 0
        trades: List[Trade] = []
        trade_high = None
        trade_low = None
        eq_times_ms: List[int] = []
        eq_balances: List[float] = []
        eq_marks: List[float] = []
        eq_sample_index: int = 0
        high_water = float(cfg.initial_balance)
        max_dd_val = 0.0
        max_dd_pct = 0.0

        def _record_tick_equity(sample_ms: int, equity_value: float, equity_mark_value: float, *, force: bool = False) -> None:
            nonlocal eq_sample_index, high_water, max_dd_val, max_dd_pct
            equity_value = float(equity_value)
            equity_mark_value = float(equity_mark_value)
            if equity_mark_value > high_water:
                high_water = equity_mark_value
            dd = high_water - equity_mark_value
            if dd > max_dd_val:
                max_dd_val = float(dd)
                max_dd_pct = float((dd / high_water) * 100.0) if high_water > 0 else 0.0
            should_store = bool(force or ((eq_sample_index % equity_curve_stride) == 0))
            eq_sample_index += 1
            if should_store:
                eq_times_ms.append(int(sample_ms))
                eq_balances.append(equity_value)
                eq_marks.append(equity_mark_value)
        sig_i = 0
        next_sig_ms = signal_times_ms[sig_i] if sig_i < len(signal_times_ms) else None
        last_tick_price = None
        last_tick_ms = None

        def _bar_for_decision(ts_ms: int) -> Optional[_EntryFilterBar]:
            try:
                return _bar_info_at(step_bars, pd.Timestamp(int(ts_ms), unit="ms", tz="UTC"))
            except Exception:
                return None

        latest_closed_signal_ms: Optional[int] = None

        def _materialize_tick_signal_snapshot(signal_ms: Optional[int]) -> Dict[str, Dict[str, Any]]:
            if signal_ms is None:
                return {}
            try:
                key = int(signal_ms)
            except Exception:
                return {}
            snap = snap_map.get(key)
            if isinstance(snap, dict) and len(snap) > 0:
                return snap
            row_index = signal_snapshot_row_map.get(key)
            if row_index is None:
                return {}
            snap = _build_current_snapshots_for_row(
                subs_tick,
                int(row_index),
                snapshot_cache=full_snapshot_cache,
                required_fields=None,
            )
            if isinstance(snap, dict) and len(snap) > 0:
                snap_map[key] = snap
                return snap
            return {}

        def _latest_closed_tick_snapshot() -> Dict[str, Dict[str, Any]]:
            return _materialize_tick_signal_snapshot(latest_closed_signal_ms)

        def _materialize_tick_signal_trace(signal_ms: Optional[int], tab_name: str) -> str:
            if signal_ms is None:
                return ""
            try:
                key = int(signal_ms)
            except Exception:
                return ""
            cached = trace_map.get(key)
            if isinstance(cached, dict) and tab_name in cached:
                return str(cached.get(tab_name, "") or "")
            signal_state = signal_map.get(key)
            if not isinstance(signal_state, tuple) or len(signal_state) != 4:
                trace_map.setdefault(key, {})[tab_name] = ""
                return ""
            enabled_by_tab = {
                "Long Entry": bool(signal_state[0]),
                "Long Exit": bool(signal_state[1]),
                "Short Entry": bool(signal_state[2]),
                "Short Exit": bool(signal_state[3]),
            }
            if not bool(enabled_by_tab.get(tab_name)):
                trace_map.setdefault(key, {})[tab_name] = ""
                return ""
            snap_pair = trace_snapshot_map.get(key)
            if not (isinstance(snap_pair, tuple) and len(snap_pair) == 2):
                row_index = signal_snapshot_row_map.get(key)
                if row_index is None:
                    trace_map.setdefault(key, {})[tab_name] = ""
                    return ""
                snap_pair = _build_eval_snapshots_for_row(
                    subs_tick,
                    row_index=int(row_index),
                    prev_decision_j=None,
                    prev_source_row_by_tf=prev_source_row_by_tf,
                    step_tf=step_tf,
                    snapshot_cache=eval_snapshot_cache,
                    required_fields=eval_required_fields,
                )
                trace_snapshot_map[key] = snap_pair
            cur_trace_by_tf, prev_trace_by_tf = snap_pair
            trace_txt = _build_tab_rule_trace(
                tab_name,
                rules_model,
                tab_group_join_mode,
                group_rule_join_mode,
                cur_trace_by_tf,
                prev_trace_by_tf,
                ctx=trace_ctx_map.get(key),
            )
            trace_map.setdefault(key, {})[tab_name] = str(trace_txt or "")
            return str(trace_txt or "")

        def _open_tick_now(side: str,
                           reason: str,
                           fill_price_raw: float,
                           tick_ms: int,
                           snap_now: Dict[str, Dict[str, Any]],
                           signal_time_ms: Optional[int] = None,
                           order_created_time_ms: Optional[int] = None,
                           entry_rule_trace: str = ""):
            nonlocal pos, entry_price, entry_time, entry_snapshot, entry_barsago, entry_reason, position_lifecycle
            nonlocal stop_loss_price, take_profit_price, break_even_price, break_even_activation_price, break_even_armed
            nonlocal trailing_stop_price, trailing_stop_anchor_price
            nonlocal qty, balance, equity, trade_high, trade_low, entry_rule_trace_text
            side = str(side).upper()
            fill_action = "BUY" if side == "LONG" else "SELL"
            fill_price = _apply_slippage(float(fill_price_raw), fill_action, cfg.slippage_bps)
            qty = cfg.order_notional_usdt / fill_price
            entry_price = fill_price
            entry_time = pd.Timestamp(int(tick_ms), unit="ms", tz="UTC")
            entry_snapshot = snap_now
            entry_barsago = barsago_profile_cache.get("Long Entry" if side == "LONG" else "Short Entry", {})
            entry_reason = reason
            entry_rule_trace_text = str(entry_rule_trace or "")
            position_lifecycle = _make_trade_lifecycle(engine_type="AGGTRADE", fill_source="AGGTRADE")
            position_lifecycle = _open_trade_lifecycle(
                position_lifecycle,
                side=side,
                signal_time=(pd.Timestamp(int(signal_time_ms), unit="ms", tz="UTC") if signal_time_ms is not None else entry_time),
                order_created_time=(pd.Timestamp(int(order_created_time_ms), unit="ms", tz="UTC") if order_created_time_ms is not None else (pd.Timestamp(int(signal_time_ms), unit="ms", tz="UTC") if signal_time_ms is not None else entry_time)),
                entry_fill_time=entry_time,
                entry_fill_price=float(fill_price),
                qty=float(qty),
                entry_reason=reason,
                entry_snapshot=snap_now,
                entry_barsago=entry_barsago,
                entry_rule_trace=entry_rule_trace_text,
            )
            stop_loss_price, take_profit_price = _compute_stop_take_prices(fill_price, side, cfg, snap_now)
            break_even_price, break_even_activation_price = _compute_break_even_prices(fill_price, side, cfg)
            break_even_armed = False
            trailing_stop_price, trailing_stop_anchor_price = _compute_initial_trailing_stop(fill_price, side, cfg)
            trade_high = float(fill_price_raw)
            trade_low = float(fill_price_raw)
            fee_entry = cfg.order_notional_usdt * cfg.fee_rate
            balance -= fee_entry
            equity = balance
            pos = 1 if side == "LONG" else -1

        def _clear_tick_open_state() -> None:
            nonlocal pos, entry_price, entry_time, entry_snapshot, entry_barsago, entry_reason, position_lifecycle
            nonlocal stop_loss_price, take_profit_price, break_even_price, break_even_activation_price, break_even_armed
            nonlocal trailing_stop_price, trailing_stop_anchor_price
            nonlocal qty, trade_high, trade_low, entry_rule_trace_text
            pos = 0
            entry_price = 0.0
            entry_time = None
            entry_snapshot = {}
            entry_barsago = {}
            entry_reason = ""
            entry_rule_trace_text = ""
            position_lifecycle = None
            stop_loss_price = None
            take_profit_price = None
            break_even_price = None
            break_even_activation_price = None
            break_even_armed = False
            trailing_stop_price = None
            trailing_stop_anchor_price = None
            qty = 0.0
            trade_high = None
            trade_low = None

        def _close_tick_position(close_reason: str, fill_price_raw: float, tick_ms: int, exit_snapshot: Dict[str, Dict[str, Any]], exit_barsago: Dict[str, Any], exit_rule_trace: str = "", exit_signal_time_ms: Optional[int] = None, exit_order_created_time_ms: Optional[int] = None) -> None:
            nonlocal balance, equity, trade_id
            if pos == 0:
                return

            exit_time = pd.Timestamp(int(tick_ms), unit="ms", tz="UTC")
            if pos == 1:
                fill_price = _apply_slippage(float(fill_price_raw), "SELL", cfg.slippage_bps)
                gross_pnl = (fill_price - entry_price) * qty
                side_label = "LONG"
                mfe = (trade_high - entry_price) if trade_high is not None else 0.0
                mae = (trade_low - entry_price) if trade_low is not None else 0.0
            else:
                fill_price = _apply_slippage(float(fill_price_raw), "BUY", cfg.slippage_bps)
                gross_pnl = (entry_price - fill_price) * qty
                side_label = "SHORT"
                mfe = (entry_price - trade_low) if trade_low is not None else 0.0
                mae = (entry_price - trade_high) if trade_high is not None else 0.0

            entry_notional = qty * entry_price
            exit_notional = qty * fill_price
            fee_exit = exit_notional * cfg.fee_rate
            fee_entry = entry_notional * cfg.fee_rate
            fees = fee_entry + fee_exit
            balance += gross_pnl - fee_exit
            equity = balance
            trade_id += 1

            mfe_pct = (mfe / entry_price) * 100.0 if entry_price else 0.0
            mae_pct = (mae / entry_price) * 100.0 if entry_price else 0.0
            dur_min = int((exit_time - entry_time).total_seconds() // 60) if entry_time is not None else 0
            lifecycle_kwargs = _close_trade_lifecycle(
                position_lifecycle,
                exit_fill_time=exit_time,
                exit_fill_price=float(fill_price),
                exit_reason=str(close_reason),
                exit_snapshot=exit_snapshot,
                exit_barsago=exit_barsago,
                ambiguous_exit=False,
                exit_rule_trace=exit_rule_trace,
                exit_signal_time=(pd.Timestamp(int(exit_signal_time_ms), unit="ms", tz="UTC") if exit_signal_time_ms is not None else exit_time),
                exit_order_created_time=(pd.Timestamp(int(exit_order_created_time_ms), unit="ms", tz="UTC") if exit_order_created_time_ms is not None else (pd.Timestamp(int(exit_signal_time_ms), unit="ms", tz="UTC") if exit_signal_time_ms is not None else exit_time)),
            )
            trades.append(Trade(
                trade_id=trade_id,
                side=side_label,
                entry_time=entry_time,
                entry_price=entry_price,
                exit_time=exit_time,
                exit_price=fill_price,
                qty=qty,
                entry_notional=entry_notional,
                exit_notional=exit_notional,
                gross_pnl=gross_pnl,
                fees=fees,
                fee_entry=fee_entry,
                fee_exit=fee_exit,
                net_pnl=gross_pnl - fees,
                return_pct=((gross_pnl - fees) / entry_notional) * 100.0 if entry_notional else 0.0,
                duration_min=dur_min,
                mfe=mfe,
                mae=mae,
                mfe_pct=mfe_pct,
                mae_pct=mae_pct,
                entry_reason=entry_reason or ("Long Entry" if side_label == "LONG" else "Short Entry"),
                exit_reason=str(close_reason),
                entry_rule_trace=str(entry_rule_trace_text or ""),
                exit_rule_trace=str(lifecycle_kwargs.get("exit_rule_trace", "") or ""),
                entry_snapshot=entry_snapshot,
                exit_snapshot=(exit_snapshot if isinstance(exit_snapshot, dict) else {}),
                entry_barsago=entry_barsago,
                exit_barsago=(exit_barsago if isinstance(exit_barsago, dict) else {}),
                stop_loss_price=(float(stop_loss_price) if stop_loss_price is not None else None),
                take_profit_price=(float(take_profit_price) if take_profit_price is not None else None),
                **{k: v for k, v in lifecycle_kwargs.items() if k not in ("entry_rule_trace", "exit_rule_trace")},
            ))
            _clear_tick_open_state()

        def _start_pending_or_open_tick(side: str,
                                        base_reason: str,
                                        origin: str,
                                        decision_ts_ms: int,
                                        tick_ms: int,
                                        price_now: float,
                                        current_step: int,
                                        matched_group_indices: Optional[List[int]] = None,
                                        rule_trace: str = ""):
            nonlocal ef_stats
            side = str(side).upper()
            tab_name = "Long Entry" if side == "LONG" else "Short Entry"
            cfgs_side = _entry_filter_cfgs_for_groups(ef_cfg, tab_name, matched_group_indices)
            sig_bar = _bar_for_decision(decision_ts_ms)
            decision, detail, confirm_bars = _combine_entry_filter_signal_decisions(side, cfgs_side, sig_bar)
            if decision == "ALLOW":
                _open_tick_now(
                    side,
                    _format_entry_reason(base_reason, "allowed", detail),
                    price_now,
                    tick_ms,
                    _materialize_tick_signal_snapshot(decision_ts_ms),
                    signal_time_ms=int(decision_ts_ms),
                    order_created_time_ms=int(decision_ts_ms),
                    entry_rule_trace=rule_trace,
                )
            elif decision == "WAIT" and sig_bar is not None:
                pending[side] = _pending_from_signal(
                    side=side,
                    origin=origin,
                    tab_name=tab_name,
                    signal_time=pd.Timestamp(int(decision_ts_ms), unit="ms", tz="UTC"),
                    signal_bar=sig_bar,
                    signal_snapshot={},
                    signal_snapshot_row_index=signal_snapshot_row_map.get(int(decision_ts_ms)),
                    signal_reason=base_reason,
                    signal_rule_trace=str(rule_trace or ""),
                    bars_ago=barsago_profile_cache.get(tab_name, {"tab": str(tab_name), "exact": [], "within": [], "items": []}),
                    matched_group_indices=matched_group_indices,
                    current_step=current_step,
                    confirm_bars=confirm_bars,
                )
                ef_stats.created += 1
            else:
                ef_stats.blocked += 1
                if "hard-skip" in str(detail):
                    ef_stats.hard_skipped += 1

        for d in iter_aggtrades_futures_daily(symbol, start, end, cache_dir=cache_dir, progress_cb=progress_cb, use_cache=True, auto_download_missing=getattr(cfg, 'tick_auto_download', False)):
            try:
                t_ms = int(d.get("T"))
                price = float(d.get("p"))
            except Exception:
                continue
            if t_ms < start_ms:
                continue
            if t_ms >= end_ms:
                break

            last_tick_price = price
            last_tick_ms = t_ms
            if pos != 0:
                if trade_high is None or price > trade_high:
                    trade_high = price
                if trade_low is None or price < trade_low:
                    trade_low = price
                trailing_stop_anchor_price, trailing_stop_price = _update_tick_trailing_stop_state(
                    "LONG" if pos == 1 else "SHORT",
                    price,
                    trailing_stop_pct,
                    trailing_stop_anchor_price,
                    trailing_stop_price,
                )
                break_even_armed = _update_tick_break_even_state(
                    "LONG" if pos == 1 else "SHORT",
                    price,
                    break_even_activation_price,
                    break_even_armed,
                )

            if pos != 0 and _intrabar_exit_enabled(cfg):
                level_reason = _resolve_tick_level_exit(
                    "LONG" if pos == 1 else "SHORT",
                    price,
                    stop_loss_price,
                    take_profit_price,
                    trailing_stop_price,
                    break_even_price,
                    break_even_armed,
                )
                if level_reason is not None:
                    _close_tick_position(level_reason, price, t_ms, (_latest_closed_tick_snapshot() or entry_snapshot), {}, _build_non_rule_exit_trace(level_reason), exit_signal_time_ms=int(t_ms), exit_order_created_time_ms=int(t_ms))

            # Phase 4: signals are evaluated on completed bars, but aggTrade fills must
            # happen on the first eligible trade strictly AFTER the signal timestamp.
            # Using '>' here prevents retroactive same-millisecond fills at the candle-close
            # timestamp itself and aligns the tick engine with the frozen execution model.
            while next_sig_ms is not None and t_ms > int(next_sig_ms):
                current_signal_ms = int(next_sig_ms)
                long_entry, long_exit, short_entry, short_exit = signal_map.get(current_signal_ms, (False, False, False, False))
                current_entry_groups = signal_entry_group_map.get(current_signal_ms, {})
                long_entry_groups = [int(idx) for idx in current_entry_groups.get("Long Entry", [])]
                short_entry_groups = [int(idx) for idx in current_entry_groups.get("Short Entry", [])]
                if current_signal_ms in signal_snapshot_row_map:
                    latest_closed_signal_ms = current_signal_ms
                cur_snap: Optional[Dict[str, Dict[str, Any]]] = None

                def _ensure_cur_snap() -> Dict[str, Dict[str, Any]]:
                    nonlocal cur_snap
                    if cur_snap is None:
                        cur_snap = _materialize_tick_signal_snapshot(current_signal_ms)
                    return cur_snap

                le_trace_txt: Optional[str] = None
                lx_trace_txt: Optional[str] = None
                se_trace_txt: Optional[str] = None
                sx_trace_txt: Optional[str] = None

                def _le_trace() -> str:
                    nonlocal le_trace_txt
                    if le_trace_txt is None:
                        le_trace_txt = _materialize_tick_signal_trace(current_signal_ms, "Long Entry")
                    return str(le_trace_txt or "")

                def _lx_trace() -> str:
                    nonlocal lx_trace_txt
                    if lx_trace_txt is None:
                        lx_trace_txt = _materialize_tick_signal_trace(current_signal_ms, "Long Exit")
                    return str(lx_trace_txt or "")

                def _se_trace() -> str:
                    nonlocal se_trace_txt
                    if se_trace_txt is None:
                        se_trace_txt = _materialize_tick_signal_trace(current_signal_ms, "Short Entry")
                    return str(se_trace_txt or "")

                def _sx_trace() -> str:
                    nonlocal sx_trace_txt
                    if sx_trace_txt is None:
                        sx_trace_txt = _materialize_tick_signal_trace(current_signal_ms, "Short Exit")
                    return str(sx_trace_txt or "")

                current_step = int(sig_i)

                unreal = 0.0
                if pos == 1:
                    unreal = (price - entry_price) * qty
                elif pos == -1:
                    unreal = (entry_price - price) * qty
                equity_mark = balance + unreal

                consumed_sides: set[str] = set()
                if pos == 0:
                    due_sides = [s for s, p in pending.items() if p is not None and current_step >= int(p.due_step)]
                    if len(due_sides) == 2 and cfg.skip_if_both_entries:
                        for s in due_sides:
                            pending[s] = None
                            ef_stats.blocked += 1
                            consumed_sides.add(s)
                    else:
                        for s in due_sides:
                            p = pending.get(s)
                            if p is None or pos != 0:
                                continue
                            pending_tab_name = getattr(p, "tab_name", ("Long Entry" if s == "LONG" else "Short Entry"))
                            pending_groups = list(getattr(p, "matched_group_indices", []) or [])
                            bars_since_signal = max(1, int(current_step) - int(getattr(p, "signal_step", current_step - 1)))
                            current_groups = long_entry_groups if s == "LONG" else short_entry_groups
                            setup_still_valid = _setup_still_valid_from_matches(
                                pending_groups,
                                current_groups,
                                tab_group_join_mode.get(pending_tab_name, "AND"),
                            )
                            decision, detail = _combine_entry_filter_confirm_decisions(
                                s,
                                _entry_filter_cfgs_for_groups(ef_cfg, pending_tab_name, pending_groups),
                                p.signal_bar,
                                _bar_for_decision(int(next_sig_ms)),
                                setup_still_valid,
                                bars_since_signal=bars_since_signal,
                                signal_row_index=getattr(p, "signal_snapshot_row_index", None),
                                confirm_row_index=j,
                                stream_1m=subs.get("1m"),
                            )
                            if decision == "ALLOW":
                                pending[s] = None
                                consumed_sides.add(s)
                                ef_stats.confirmed += 1
                                _open_tick_now(
                                    s,
                                    _format_entry_reason(p.signal_reason, "confirmed", detail, bars_since_signal),
                                    price,
                                    t_ms,
                                    _ensure_cur_snap(),
                                    signal_time_ms=int(pd.Timestamp(p.signal_time).value // 1_000_000),
                                    order_created_time_ms=int(next_sig_ms),
                                    entry_rule_trace=p.signal_rule_trace,
                                )
                            elif decision == "BLOCK":
                                pending[s] = None
                                consumed_sides.add(s)
                                ef_stats.blocked += 1

                if pos == 0:
                    if long_entry and pending.get("SHORT") is not None and "LONG" not in consumed_sides:
                        pending["SHORT"] = None
                        ef_stats.cancelled += 1
                    if short_entry and pending.get("LONG") is not None and "SHORT" not in consumed_sides:
                        pending["LONG"] = None
                        ef_stats.cancelled += 1

                    if cfg.skip_if_both_entries and long_entry and short_entry:
                        pass
                    elif long_entry and "LONG" not in consumed_sides and pending.get("LONG") is None:
                        _start_pending_or_open_tick("LONG", "Long Entry", "ENTRY", int(next_sig_ms), t_ms, price, current_step, long_entry_groups, _le_trace())
                    elif short_entry and "SHORT" not in consumed_sides and pending.get("SHORT") is None:
                        _start_pending_or_open_tick("SHORT", "Short Entry", "ENTRY", int(next_sig_ms), t_ms, price, current_step, short_entry_groups, _se_trace())

                elif pos == 1:
                    exit_now = bool(long_exit)
                    reverse_now = bool(cfg.allow_reverse and short_entry)
                    if exit_now or reverse_now:
                        close_as_reverse = bool(reverse_now)
                        exit_barsago = barsago_profile_cache.get(("Short Entry" if close_as_reverse else "Long Exit"), {})
                        _close_tick_position(("Reverse to Short" if close_as_reverse else "Long Exit"), price, t_ms, _ensure_cur_snap(), exit_barsago, (_se_trace() if close_as_reverse else _lx_trace()), exit_signal_time_ms=int(next_sig_ms), exit_order_created_time_ms=int(next_sig_ms))
                        if reverse_now:
                            rev_cfgs = _entry_filter_cfgs_for_groups(ef_cfg, "Short Entry", short_entry_groups)
                            reversal_cfgs = [cfg for cfg in rev_cfgs if cfg.is_active() and cfg.apply_to_reversals]
                            if not reversal_cfgs:
                                _open_tick_now("SHORT", "Reverse to Short", price, t_ms, _ensure_cur_snap(), signal_time_ms=int(next_sig_ms), order_created_time_ms=int(next_sig_ms), entry_rule_trace=_se_trace())
                            else:
                                _start_pending_or_open_tick("SHORT", "Reverse to Short", "REVERSE", int(next_sig_ms), t_ms, price, current_step, short_entry_groups, _se_trace())

                elif pos == -1:
                    exit_now = bool(short_exit)
                    reverse_now = bool(cfg.allow_reverse and long_entry)
                    if exit_now or reverse_now:
                        close_as_reverse = bool(reverse_now)
                        exit_barsago = barsago_profile_cache.get(("Long Entry" if close_as_reverse else "Short Exit"), {})
                        _close_tick_position(("Reverse to Long" if close_as_reverse else "Short Exit"), price, t_ms, _ensure_cur_snap(), exit_barsago, (_le_trace() if close_as_reverse else _sx_trace()), exit_signal_time_ms=int(next_sig_ms), exit_order_created_time_ms=int(next_sig_ms))
                        if reverse_now:
                            rev_cfgs = _entry_filter_cfgs_for_groups(ef_cfg, "Long Entry", long_entry_groups)
                            reversal_cfgs = [cfg for cfg in rev_cfgs if cfg.is_active() and cfg.apply_to_reversals]
                            if not reversal_cfgs:
                                _open_tick_now("LONG", "Reverse to Long", price, t_ms, _ensure_cur_snap(), signal_time_ms=int(next_sig_ms), order_created_time_ms=int(next_sig_ms), entry_rule_trace=_le_trace())
                            else:
                                _start_pending_or_open_tick("LONG", "Reverse to Long", "REVERSE", int(next_sig_ms), t_ms, price, current_step, long_entry_groups, _le_trace())

                _record_tick_equity(
                    int(current_signal_ms),
                    float(balance),
                    float(equity_mark),
                    force=bool(sig_i == (len(signal_times_ms) - 1)),
                )

                sig_i += 1
                next_sig_ms = signal_times_ms[sig_i] if sig_i < len(signal_times_ms) else None

        if pos != 0 and last_tick_price is not None and last_tick_ms is not None:
            price = float(last_tick_price)
            t_ms = int(last_tick_ms)
            _close_tick_position("Forced close (end)", price, t_ms, (_latest_closed_tick_snapshot() or entry_snapshot), {}, _build_non_rule_exit_trace("Forced close (end)"), exit_signal_time_ms=int(t_ms), exit_order_created_time_ms=int(t_ms))

        eq = pd.DataFrame(
            {
                "time": pd.to_datetime(eq_times_ms, unit="ms", utc=True),
                "equity": eq_balances,
                "equity_mark": eq_marks,
            }
        )
        if not eq.empty:
            eq = eq.drop_duplicates(subset=["time"]).sort_values("time")
            eq = eq.set_index("time")

        equity_end = float(equity)
        net_profit = equity_end - float(cfg.initial_balance)
        net_profit_from_trades = sum(t.net_pnl for t in trades)
        gross_pnl_total = sum(t.gross_pnl for t in trades)
        fees_total = sum(t.fees for t in trades)
        fees_entry_total = sum(float(getattr(t, "fee_entry", 0.0) or 0.0) for t in trades)
        fees_exit_total = sum(float(getattr(t, "fee_exit", 0.0) or 0.0) for t in trades)
        wins = [t for t in trades if t.net_pnl > 0]
        losses = [t for t in trades if t.net_pnl < 0]
        gross_profit = sum(t.net_pnl for t in wins)
        gross_loss = -sum(t.net_pnl for t in losses)

        summary = _apply_trade_reporting_metrics({
            "symbol": symbol,
            "step_timeframe": _normalize_step_timeframe(getattr(cfg, "step_timeframe", "1m")),
            "start": start,
            "end": end,
            "initial_balance": cfg.initial_balance,
            "ending_balance": equity_end,
            "net_profit": net_profit,
            "net_profit_from_trades": net_profit_from_trades,
            "gross_pnl_total": gross_pnl_total,
            "fees_total": fees_total,
            "fees_entry_total": fees_entry_total,
            "fees_exit_total": fees_exit_total,
            "return_pct": (equity_end - cfg.initial_balance) / cfg.initial_balance * 100.0 if cfg.initial_balance else 0.0,
            "trades": len(trades),
            "wins": len(wins),
            "losses": len(losses),
            "win_rate_pct": (len(wins) / len(trades) * 100.0) if trades else 0.0,
            "gross_profit": gross_profit,
            "gross_loss": gross_loss,
            "profit_factor": (gross_profit / gross_loss) if gross_loss > 0 else (float("inf") if gross_profit > 0 else 0.0),
            "avg_trade": (net_profit / len(trades)) if trades else 0.0,
            "avg_win": (sum(t.net_pnl for t in wins) / len(wins)) if wins else 0.0,
            "avg_loss": (sum(t.net_pnl for t in losses) / len(losses)) if losses else 0.0,
            "max_drawdown": max_dd_val,
            "max_drawdown_pct": max_dd_pct,
            "avg_duration_min": (sum(t.duration_min for t in trades) / len(trades)) if trades else 0.0,
            "entry_filter": _entry_filter_summary_payload(ef_cfg, ef_stats),
        }, trades)

        return BacktestResult(config=cfg, start=start, end=end, trades=trades, equity_curve=eq, summary=summary, replay_context=replay_context)

    # ------------------------------
    # Fallback path: original live-indicator-on-tick engine
    # ------------------------------
    # Keep the original behavior when streams_full isn't available.
    warm_df = df_1m_full[df_1m_full["close_time"] < start].copy() if df_1m_full is not None and (not df_1m_full.empty) else pd.DataFrame()
    tf_states = _init_tf_states_from_warmup(warm_df, tfs, start_ms)

    # initial price seed
    last_price = None
    if not warm_df.empty:
        try:
            last_price = float(warm_df.iloc[-1]["close"])
        except Exception:
            last_price = None

    balance = float(cfg.initial_balance)
    equity = float(cfg.initial_balance)
    pos = 0
    entry_price = 0.0
    entry_time = None
    entry_snapshot = {}
    position_lifecycle: Optional[_TradeLifecycle] = None
    stop_loss_price: Optional[float] = None
    take_profit_price: Optional[float] = None
    break_even_price: Optional[float] = None
    break_even_activation_price: Optional[float] = None
    break_even_armed = False
    trailing_stop_pct = _trailing_stop_pct(cfg)
    trailing_stop_price: Optional[float] = None
    trailing_stop_anchor_price: Optional[float] = None
    qty = 0.0
    trade_id = 0
    trades: List[Trade] = []

    eq_rows = []
    cur_minute = (start_ms // 60_000) * 60_000
    prev_by_tf: Dict[str, Dict[str, Any]] = {tf: {} for tf in tfs}

    def _mk_snap(t_ms: int, price: float) -> Dict[str, Dict[str, Any]]:
        cur = {}
        for tf, st in tf_states.items():
            cur[tf] = st.snapshot(t_ms, price)
        return cur

    if last_price is not None:
        prev_by_tf = _mk_snap(start_ms, float(last_price))

    _progress("Tick backtest (fallback): streaming aggTrades…")

    trade_high = None
    trade_low = None

    def _clear_fallback_open_state() -> None:
        nonlocal pos, entry_price, entry_time, entry_snapshot, position_lifecycle
        nonlocal stop_loss_price, take_profit_price, break_even_price, break_even_activation_price, break_even_armed
        nonlocal trailing_stop_price, trailing_stop_anchor_price
        nonlocal qty, trade_high, trade_low
        pos = 0
        entry_price = 0.0
        entry_time = None
        entry_snapshot = {}
        position_lifecycle = None
        stop_loss_price = None
        take_profit_price = None
        break_even_price = None
        break_even_activation_price = None
        break_even_armed = False
        trailing_stop_price = None
        trailing_stop_anchor_price = None
        qty = 0.0
        trade_high = None
        trade_low = None

    def _close_fallback_position(close_reason: str, fill_price_raw: float, tick_ms: int, exit_snapshot: Dict[str, Dict[str, Any]]) -> None:
        nonlocal balance, equity, trade_id
        if pos == 0:
            return

        exit_time = pd.Timestamp(tick_ms, unit="ms", tz="UTC")
        if pos == 1:
            fill_price = _apply_slippage(float(fill_price_raw), "SELL", cfg.slippage_bps)
            gross_pnl = (fill_price - entry_price) * qty
            side_label = "LONG"
            mfe = (trade_high - entry_price) if trade_high is not None else 0.0
            mae = (trade_low - entry_price) if trade_low is not None else 0.0
        else:
            fill_price = _apply_slippage(float(fill_price_raw), "BUY", cfg.slippage_bps)
            gross_pnl = (entry_price - fill_price) * qty
            side_label = "SHORT"
            mfe = (entry_price - trade_low) if trade_low is not None else 0.0
            mae = (entry_price - trade_high) if trade_high is not None else 0.0

        entry_notional = qty * entry_price
        exit_notional = qty * fill_price
        fee_exit = exit_notional * cfg.fee_rate
        fee_entry = entry_notional * cfg.fee_rate
        fees = fee_entry + fee_exit
        balance += gross_pnl - fee_exit
        equity = balance
        trade_id += 1

        mfe_pct = (mfe / entry_price) * 100.0 if entry_price else 0.0
        mae_pct = (mae / entry_price) * 100.0 if entry_price else 0.0
        dur_min = int((exit_time - entry_time).total_seconds() // 60) if entry_time is not None else 0
        lifecycle_kwargs = _close_trade_lifecycle(
            position_lifecycle,
            exit_fill_time=exit_time,
            exit_fill_price=float(fill_price),
            exit_reason=str(close_reason),
            exit_snapshot=(exit_snapshot if isinstance(exit_snapshot, dict) else {}),
            exit_barsago={},
            ambiguous_exit=False,
        )
        trades.append(Trade(
            trade_id=trade_id,
            side=side_label,
            entry_time=entry_time,
            entry_price=entry_price,
            exit_time=exit_time,
            exit_price=fill_price,
            qty=qty,
            entry_notional=entry_notional,
            exit_notional=exit_notional,
            gross_pnl=gross_pnl,
            fees=fees,
            fee_entry=fee_entry,
            fee_exit=fee_exit,
            net_pnl=gross_pnl - fees,
            return_pct=((gross_pnl - fees) / entry_notional) * 100.0 if entry_notional else 0.0,
            duration_min=dur_min,
            mfe=mfe,
            mae=mae,
            mfe_pct=mfe_pct,
            mae_pct=mae_pct,
            entry_reason=("Long Entry" if side_label == "LONG" else "Short Entry"),
            exit_reason=str(close_reason),
            entry_snapshot=entry_snapshot,
            exit_snapshot=(exit_snapshot if isinstance(exit_snapshot, dict) else {}),
            stop_loss_price=(float(stop_loss_price) if stop_loss_price is not None else None),
            take_profit_price=(float(take_profit_price) if take_profit_price is not None else None),
            **lifecycle_kwargs,
        ))
        _clear_fallback_open_state()

    for d in iter_aggtrades_futures_daily(symbol, start, end, cache_dir=cache_dir, progress_cb=progress_cb, use_cache=True, auto_download_missing=getattr(cfg, 'tick_auto_download', False)):
        try:
            t_ms = int(d.get("T"))
            price = float(d.get("p"))
        except Exception:
            continue
        if t_ms < start_ms:
            continue
        if t_ms >= end_ms:
            break

        prev_price_for_minute = last_price
        last_price = price

        this_min = (t_ms // 60_000) * 60_000
        minute_changed = (this_min != cur_minute)
        if minute_changed:
            eq_rows.append({"time": pd.Timestamp(cur_minute, unit="ms", tz="UTC"), "equity": float(equity)})
            cur_minute = this_min

        for st in tf_states.values():
            while t_ms >= st.bucket_end_ms:
                st.close_bar(st.bucket_end_ms)
            st.last_price = price

        if minute_changed:
            try:
                prev_by_tf = _mk_snap(int(this_min - 1), float(prev_price_for_minute) if prev_price_for_minute is not None else float(price))
            except Exception:
                pass

        cur_by_tf = _mk_snap(t_ms, price)

        if pos != 0:
            if trade_high is None or price > trade_high:
                trade_high = price
            if trade_low is None or price < trade_low:
                trade_low = price
            trailing_stop_anchor_price, trailing_stop_price = _update_tick_trailing_stop_state(
                "LONG" if pos == 1 else "SHORT",
                price,
                trailing_stop_pct,
                trailing_stop_anchor_price,
                trailing_stop_price,
            )
            break_even_armed = _update_tick_break_even_state(
                "LONG" if pos == 1 else "SHORT",
                price,
                break_even_activation_price,
                break_even_armed,
            )

        if pos != 0 and _intrabar_exit_enabled(cfg):
            level_reason = _resolve_tick_level_exit(
                "LONG" if pos == 1 else "SHORT",
                price,
                stop_loss_price,
                take_profit_price,
                trailing_stop_price,
                break_even_price,
                break_even_armed,
            )
            if level_reason is not None:
                _close_fallback_position(level_reason, price, t_ms, entry_snapshot)

        le = eval_tab_generic("Long Entry", rules_model, tab_group_join_mode, group_rule_join_mode, cur_by_tf, prev_by_tf, ctx=ctx)
        lx = eval_tab_generic("Long Exit", rules_model, tab_group_join_mode, group_rule_join_mode, cur_by_tf, prev_by_tf, ctx=ctx)
        se = eval_tab_generic("Short Entry", rules_model, tab_group_join_mode, group_rule_join_mode, cur_by_tf, prev_by_tf, ctx=ctx)
        sx = eval_tab_generic("Short Exit", rules_model, tab_group_join_mode, group_rule_join_mode, cur_by_tf, prev_by_tf, ctx=ctx)

        long_entry = _bool_true(le)
        long_exit = _bool_true(lx)
        short_entry = _bool_true(se)
        short_exit = _bool_true(sx)

        if pos == 0:
            if cfg.skip_if_both_entries and long_entry and short_entry:
                pass
            else:
                if long_entry:
                    fill_price = _apply_slippage(price, "BUY", cfg.slippage_bps)
                    qty = cfg.order_notional_usdt / fill_price
                    entry_price = fill_price
                    entry_time = pd.Timestamp(t_ms, unit="ms", tz="UTC")
                    entry_snapshot = cur_by_tf
                    position_lifecycle = _make_trade_lifecycle(engine_type="AGGTRADE", fill_source="AGGTRADE")
                    position_lifecycle = _open_trade_lifecycle(
                        position_lifecycle,
                        side="LONG",
                        signal_time=entry_time,
                        order_created_time=entry_time,
                        entry_fill_time=entry_time,
                        entry_fill_price=float(fill_price),
                        qty=float(qty),
                        entry_reason="Long Entry",
                        entry_snapshot=entry_snapshot,
                        entry_barsago={},
                    )
                    stop_loss_price, take_profit_price = _compute_stop_take_prices(fill_price, "LONG", cfg, entry_snapshot)
                    break_even_price, break_even_activation_price = _compute_break_even_prices(fill_price, "LONG", cfg)
                    break_even_armed = False
                    trailing_stop_price, trailing_stop_anchor_price = _compute_initial_trailing_stop(fill_price, "LONG", cfg)
                    trade_high = price
                    trade_low = price
                    balance -= cfg.order_notional_usdt * cfg.fee_rate
                    equity = balance
                    pos = 1
                elif short_entry:
                    fill_price = _apply_slippage(price, "SELL", cfg.slippage_bps)
                    qty = cfg.order_notional_usdt / fill_price
                    entry_price = fill_price
                    entry_time = pd.Timestamp(t_ms, unit="ms", tz="UTC")
                    entry_snapshot = cur_by_tf
                    position_lifecycle = _make_trade_lifecycle(engine_type="AGGTRADE", fill_source="AGGTRADE")
                    position_lifecycle = _open_trade_lifecycle(
                        position_lifecycle,
                        side="SHORT",
                        signal_time=entry_time,
                        order_created_time=entry_time,
                        entry_fill_time=entry_time,
                        entry_fill_price=float(fill_price),
                        qty=float(qty),
                        entry_reason="Short Entry",
                        entry_snapshot=entry_snapshot,
                        entry_barsago={},
                    )
                    stop_loss_price, take_profit_price = _compute_stop_take_prices(fill_price, "SHORT", cfg, entry_snapshot)
                    break_even_price, break_even_activation_price = _compute_break_even_prices(fill_price, "SHORT", cfg)
                    break_even_armed = False
                    trailing_stop_price, trailing_stop_anchor_price = _compute_initial_trailing_stop(fill_price, "SHORT", cfg)
                    trade_high = price
                    trade_low = price
                    balance -= cfg.order_notional_usdt * cfg.fee_rate
                    equity = balance
                    pos = -1
        elif pos == 1:
            exit_now = long_exit
            reverse_now = cfg.allow_reverse and short_entry
            if exit_now or reverse_now:
                _close_fallback_position(("Reverse to Short" if reverse_now else "Long Exit"), price, t_ms, cur_by_tf)

                if reverse_now:
                    fill2 = _apply_slippage(price, "SELL", cfg.slippage_bps)
                    qty = cfg.order_notional_usdt / fill2
                    entry_price = fill2
                    entry_time = pd.Timestamp(t_ms, unit="ms", tz="UTC")
                    entry_snapshot = cur_by_tf
                    position_lifecycle = _make_trade_lifecycle(engine_type="AGGTRADE", fill_source="AGGTRADE")
                    position_lifecycle = _open_trade_lifecycle(
                        position_lifecycle,
                        side="SHORT",
                        signal_time=pd.Timestamp(t_ms, unit="ms", tz="UTC"),
                        order_created_time=pd.Timestamp(t_ms, unit="ms", tz="UTC"),
                        entry_fill_time=pd.Timestamp(t_ms, unit="ms", tz="UTC"),
                        entry_fill_price=float(fill2),
                        qty=float(qty),
                        entry_reason="Reverse to Short",
                        entry_snapshot=entry_snapshot,
                        entry_barsago={},
                    )
                    stop_loss_price, take_profit_price = _compute_stop_take_prices(fill2, "SHORT", cfg, entry_snapshot)
                    break_even_price, break_even_activation_price = _compute_break_even_prices(fill2, "SHORT", cfg)
                    break_even_armed = False
                    trailing_stop_price, trailing_stop_anchor_price = _compute_initial_trailing_stop(fill2, "SHORT", cfg)
                    trade_high = price
                    trade_low = price
                    balance -= cfg.order_notional_usdt * cfg.fee_rate
                    equity = balance
                    pos = -1
        elif pos == -1:
            exit_now = short_exit
            reverse_now = cfg.allow_reverse and long_entry
            if exit_now or reverse_now:
                _close_fallback_position(("Reverse to Long" if reverse_now else "Short Exit"), price, t_ms, cur_by_tf)

                if reverse_now:
                    fill2 = _apply_slippage(price, "BUY", cfg.slippage_bps)
                    qty = cfg.order_notional_usdt / fill2
                    entry_price = fill2
                    entry_time = pd.Timestamp(t_ms, unit="ms", tz="UTC")
                    entry_snapshot = cur_by_tf
                    position_lifecycle = _make_trade_lifecycle(engine_type="AGGTRADE", fill_source="AGGTRADE")
                    position_lifecycle = _open_trade_lifecycle(
                        position_lifecycle,
                        side="LONG",
                        signal_time=pd.Timestamp(t_ms, unit="ms", tz="UTC"),
                        order_created_time=pd.Timestamp(t_ms, unit="ms", tz="UTC"),
                        entry_fill_time=pd.Timestamp(t_ms, unit="ms", tz="UTC"),
                        entry_fill_price=float(fill2),
                        qty=float(qty),
                        entry_reason="Reverse to Long",
                        entry_snapshot=entry_snapshot,
                        entry_barsago={},
                    )
                    stop_loss_price, take_profit_price = _compute_stop_take_prices(fill2, "LONG", cfg, entry_snapshot)
                    break_even_price, break_even_activation_price = _compute_break_even_prices(fill2, "LONG", cfg)
                    break_even_armed = False
                    trailing_stop_price, trailing_stop_anchor_price = _compute_initial_trailing_stop(fill2, "LONG", cfg)
                    trade_high = price
                    trade_low = price
                    balance -= cfg.order_notional_usdt * cfg.fee_rate
                    equity = balance
                    pos = 1

    if last_price is not None:
        eq_rows.append({"time": pd.Timestamp(cur_minute, unit="ms", tz="UTC"), "equity": float(equity)})

    eq = pd.DataFrame(eq_rows)
    if not eq.empty:
        eq = eq.drop_duplicates(subset=["time"]).sort_values("time")
        eq = eq.set_index("time")

    equity_end = float(equity)
    net_profit = equity_end - float(cfg.initial_balance)
    net_profit_from_trades = sum(t.net_pnl for t in trades)
    gross_pnl_total = sum(t.gross_pnl for t in trades)
    fees_total = sum(t.fees for t in trades)
    fees_entry_total = sum(float(getattr(t, "fee_entry", 0.0) or 0.0) for t in trades)
    fees_exit_total = sum(float(getattr(t, "fee_exit", 0.0) or 0.0) for t in trades)

    wins = [t for t in trades if t.net_pnl > 0]
    losses = [t for t in trades if t.net_pnl < 0]

    gross_profit = sum(t.net_pnl for t in wins)
    gross_loss = -sum(t.net_pnl for t in losses)

    max_dd_val = 0.0
    max_dd_pct = 0.0
    try:
        if eq is not None and (not eq.empty) and "equity" in eq.columns:
            cm = eq["equity"].cummax()
            dd = cm - eq["equity"]
            max_dd_val = float(dd.max()) if len(dd) else 0.0
            denom = cm.replace(0.0, np.nan)
            max_dd_pct = float(((dd / denom) * 100.0).max()) if len(dd) else 0.0
            if not np.isfinite(max_dd_pct):
                max_dd_pct = 0.0
    except Exception:
        pass

    summary = _apply_trade_reporting_metrics({
        "symbol": symbol,
        "step_timeframe": _normalize_step_timeframe(getattr(cfg, "step_timeframe", "1m")),
        "start": start,
        "end": end,
        "initial_balance": cfg.initial_balance,
        "ending_balance": equity_end,
        "net_profit": net_profit,
        "net_profit_from_trades": net_profit_from_trades,
        "gross_pnl_total": gross_pnl_total,
        "fees_total": fees_total,
        "fees_entry_total": fees_entry_total,
        "fees_exit_total": fees_exit_total,
        "return_pct": (equity_end - cfg.initial_balance) / cfg.initial_balance * 100.0 if cfg.initial_balance else 0.0,
        "trades": len(trades),
        "wins": len(wins),
        "losses": len(losses),
        "win_rate_pct": (len(wins) / len(trades) * 100.0) if trades else 0.0,
        "gross_profit": gross_profit,
        "gross_loss": gross_loss,
        "profit_factor": (gross_profit / gross_loss) if gross_loss > 0 else (float("inf") if gross_profit > 0 else 0.0),
        "avg_trade": (net_profit / len(trades)) if trades else 0.0,
        "avg_win": (sum(t.net_pnl for t in wins) / len(wins)) if wins else 0.0,
        "avg_loss": (sum(t.net_pnl for t in losses) / len(losses)) if losses else 0.0,
        "max_drawdown": max_dd_val,
        "max_drawdown_pct": max_dd_pct,
        "avg_duration_min": (sum(t.duration_min for t in trades) / len(trades)) if trades else 0.0,
        "stop_loss_pct": float(getattr(cfg, "stop_loss_pct", 0.0) or 0.0),
        "take_profit_pct": float(getattr(cfg, "take_profit_pct", 0.0) or 0.0),
        "stop_loss_mode": str(getattr(cfg, "stop_loss_mode", "PERCENT") or "PERCENT"),
        "stop_loss_value": float(getattr(cfg, "stop_loss_value", getattr(cfg, "stop_loss_pct", 0.0)) or 0.0),
        "stop_loss_timeframe": str(getattr(cfg, "stop_loss_timeframe", "1m") or "1m"),
        "stop_loss_field": str(getattr(cfg, "stop_loss_field", "") or ""),
        "stop_loss_offset_pct": float(getattr(cfg, "stop_loss_offset_pct", 0.0) or 0.0),
        "take_profit_mode": str(getattr(cfg, "take_profit_mode", "PERCENT") or "PERCENT"),
        "take_profit_value": float(getattr(cfg, "take_profit_value", getattr(cfg, "take_profit_pct", 0.0)) or 0.0),
        "take_profit_timeframe": str(getattr(cfg, "take_profit_timeframe", "1m") or "1m"),
        "take_profit_field": str(getattr(cfg, "take_profit_field", "") or ""),
        "take_profit_offset_pct": float(getattr(cfg, "take_profit_offset_pct", 0.0) or 0.0),
        "entry_filter": _entry_filter_summary_payload(ef_cfg, ef_stats),
    }, trades)

    return BacktestResult(
        config=cfg,
        start=start,
        end=end,
        trades=trades,
        equity_curve=eq,
        summary=summary,
        replay_context=replay_context,
    )
