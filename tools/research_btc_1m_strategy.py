from __future__ import annotations

import argparse
import json
import math
import sys
import warnings
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence, Tuple

import pandas as pd

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from app.backtest_models import BacktestConfig
from app.backtest_frames import make_streams_htf_closed_only
from app.constants import BACKTEST_MODE_BAR_1M_HTF_CLOSED_ONLY
from app.data_binance import binance_fapi_server_time_utc, fetch_klines_1m_futures
from app.ema_settings import normalize_ema_settings
from app.fast_backtest import run_backtest_auto
from app.indicators_streaming import simulate_multitf_indicators
from app.prepared_dataset import (
    build_prepared_dataset,
    find_covering_prepared_dataset_on_disk,
    save_prepared_dataset_to_disk,
    slice_df_1m_window,
    slice_streams_window,
)
from app.rules import Rule
from app.strategy_requirements import StreamRequirementSpec, compile_stream_requirements
from app.utils_time import parse_utc, required_warmup_minutes


TABS = ("Long Entry", "Long Exit", "Short Entry", "Short Exit")


@dataclass(frozen=True)
class ExitProfile:
    name: str
    stop_mode: str
    stop_value: float
    take_mode: str
    take_value: float
    break_even_after_mfe_pct: float = 0.0
    trailing_stop_pct: float = 0.0


@dataclass(frozen=True)
class StrategyCandidate:
    name: str
    thesis: str
    ema_fast: int
    ema_slow: int
    rules_model: Dict[str, List[List[Rule]]]
    tab_group_join_mode: Dict[str, str]
    group_rule_join_mode: Dict[str, List[str]]
    entry_filters: Dict[str, Any]


def _json_safe(value: Any) -> Any:
    if isinstance(value, (pd.Timestamp,)):
        return value.isoformat()
    if isinstance(value, float):
        if math.isinf(value):
            return "inf" if value > 0 else "-inf"
        if math.isnan(value):
            return None
    if isinstance(value, dict):
        return {str(k): _json_safe(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [_json_safe(v) for v in value]
    return value


def _r(
    timeframe: str,
    mode: str,
    field: str,
    op: str,
    value: Any = None,
    *,
    compare_to: Optional[Any] = None,
    is_trigger: bool = False,
    is_final_gate: bool = False,
    is_sequence_canceler: bool = False,
    bars_ago_mode: str = "OFF",
    bars_ago_n: int = 0,
    require_still_valid: bool = False,
) -> Rule:
    return Rule(
        timeframe=timeframe,
        mode=mode,
        field=field,
        op=op,
        value=value,
        compare_to=compare_to,
        is_trigger=is_trigger,
        is_final_gate=is_final_gate,
        is_sequence_canceler=is_sequence_canceler,
        bars_ago_mode=bars_ago_mode,
        bars_ago_n=bars_ago_n,
        require_still_valid=require_still_valid,
    )


def _empty_rules_model() -> Dict[str, List[List[Rule]]]:
    return {tab: [] for tab in TABS}


def _joins(entry_join: str = "AND", exit_join: str = "AND") -> Tuple[Dict[str, str], Dict[str, List[str]]]:
    return (
        {tab: "AND" for tab in TABS},
        {
            "Long Entry": [entry_join],
            "Short Entry": [entry_join],
            "Long Exit": [exit_join] if exit_join else [],
            "Short Exit": [exit_join] if exit_join else [],
        },
    )


def _ema_settings(fast: int, slow: int) -> Dict[str, Any]:
    return normalize_ema_settings(
        {
            "defaults": {"ema_1_length": int(fast), "ema_2_length": int(slow)},
            "overrides": {},
        }
    )


def _trend_gates(side: str, *, adx_1m: float, adx_5m: Optional[float], htf: Sequence[str]) -> List[Rule]:
    long = side.upper() == "LONG"
    gt = ">" if long else "<"
    gates = [
        _r("1m", "state", "close", gt, None, compare_to={"field": "ema_1"}, is_final_gate=True),
        _r("1m", "state", "close", gt, None, compare_to={"field": "ema_2"}, is_final_gate=True),
        _r("1m", "state", "ema_1", gt, None, compare_to={"field": "ema_2"}, is_final_gate=True),
        _r("1m", "state", "ema_2", gt, None, compare_to={"field": "ema_2", "bars_ago": 20}, is_final_gate=True),
        _r("1m", "state", "adx", ">=", float(adx_1m), is_final_gate=True),
    ]
    for tf in htf:
        gates.append(_r(tf, "state", "close", gt, None, compare_to={"field": "ema_2"}, is_final_gate=True))
        gates.append(_r(tf, "state", "ema_1", gt, None, compare_to={"field": "ema_2"}, is_final_gate=True))
    if adx_5m is not None:
        gates.append(_r("5m", "state", "adx", ">=", float(adx_5m), is_final_gate=True))
    return gates


def make_ema_pullback_candidate(
    *,
    name: str,
    ema_fast: int,
    ema_slow: int,
    adx_1m: float,
    adx_5m: Optional[float],
    htf: Sequence[str],
) -> StrategyCandidate:
    rules = _empty_rules_model()
    htf_label = "+".join(htf) if htf else "1m"
    rules["Long Entry"] = [[
        _r("1m", "state", "low", "<=", None, compare_to={"field": "ema_1"}),
        _r("1m", "state", "close", ">", None, compare_to={"field": "high", "bars_ago": 1}),
        *_trend_gates("LONG", adx_1m=adx_1m, adx_5m=adx_5m, htf=htf),
        _r("1m", "state", "close", "<", None, compare_to={"field": "ema_2"}, is_sequence_canceler=True),
    ]]
    rules["Short Entry"] = [[
        _r("1m", "state", "high", ">=", None, compare_to={"field": "ema_1"}),
        _r("1m", "state", "close", "<", None, compare_to={"field": "low", "bars_ago": 1}),
        *_trend_gates("SHORT", adx_1m=adx_1m, adx_5m=adx_5m, htf=htf),
        _r("1m", "state", "close", ">", None, compare_to={"field": "ema_2"}, is_sequence_canceler=True),
    ]]
    tab_join, group_join = _joins(entry_join="SEQUENCE", exit_join="")
    return StrategyCandidate(
        name=name,
        thesis=f"EMA {ema_fast}/{ema_slow} pullback: touch EMA fast, enter on previous candle break, HTF={htf_label}, ADX {adx_1m}/{adx_5m}",
        ema_fast=ema_fast,
        ema_slow=ema_slow,
        rules_model=rules,
        tab_group_join_mode=tab_join,
        group_rule_join_mode=group_join,
        entry_filters={},
    )


def make_ema_pullbreak_candidate(
    *,
    name: str,
    ema_fast: int,
    ema_slow: int,
    adx_1m: float,
    adx_5m: Optional[float],
    htf: Sequence[str],
) -> StrategyCandidate:
    rules = _empty_rules_model()
    htf_label = "+".join(htf) if htf else "1m"
    rules["Long Entry"] = [[
        _r(
            "1m",
            "state",
            "low",
            "<=",
            None,
            compare_to={"field": "ema_1", "bars_ago": 1},
            bars_ago_mode="EXACT",
            bars_ago_n=1,
        ),
        _r("1m", "state", "close", ">", None, compare_to={"field": "high", "bars_ago": 1}),
        *_trend_gates("LONG", adx_1m=adx_1m, adx_5m=adx_5m, htf=htf),
    ]]
    rules["Short Entry"] = [[
        _r(
            "1m",
            "state",
            "high",
            ">=",
            None,
            compare_to={"field": "ema_1", "bars_ago": 1},
            bars_ago_mode="EXACT",
            bars_ago_n=1,
        ),
        _r("1m", "state", "close", "<", None, compare_to={"field": "low", "bars_ago": 1}),
        *_trend_gates("SHORT", adx_1m=adx_1m, adx_5m=adx_5m, htf=htf),
    ]]
    tab_join, group_join = _joins(entry_join="AND", exit_join="")
    return StrategyCandidate(
        name=name,
        thesis=(
            f"EMA {ema_fast}/{ema_slow} previous-bar pullback: prior bar touches EMA fast, "
            f"current bar breaks prior high/low, HTF={htf_label}, ADX {adx_1m}/{adx_5m}"
        ),
        ema_fast=ema_fast,
        ema_slow=ema_slow,
        rules_model=rules,
        tab_group_join_mode=tab_join,
        group_rule_join_mode=group_join,
        entry_filters={},
    )


def _event_filters(side: str, *, adx_1m: float, adx_5m: Optional[float], htf: Sequence[str]) -> List[Rule]:
    long = side.upper() == "LONG"
    stack = "BUY" if long else "SELL"
    slope_op = ">" if long else "<"
    filters = [
        _r("1m", "state", "ema_stack", "=", stack),
        _r("1m", "state", "ema_2_slope", slope_op, 0.0),
        _r("1m", "state", "adx", ">=", float(adx_1m)),
    ]
    for tf in htf:
        filters.append(_r(tf, "state", "ema_stack", "=", stack))
        filters.append(_r(tf, "state", "ema_2_slope", slope_op, 0.0))
    if adx_5m is not None:
        filters.append(_r("5m", "state", "adx", ">=", float(adx_5m)))
    return filters


def make_event_candidate(
    *,
    name: str,
    thesis: str,
    ema_fast: int,
    ema_slow: int,
    long_event: Rule,
    short_event: Rule,
    adx_1m: float,
    adx_5m: Optional[float],
    htf: Sequence[str],
) -> StrategyCandidate:
    rules = _empty_rules_model()
    rules["Long Entry"] = [[long_event] + _event_filters("LONG", adx_1m=adx_1m, adx_5m=adx_5m, htf=htf)]
    rules["Short Entry"] = [[short_event] + _event_filters("SHORT", adx_1m=adx_1m, adx_5m=adx_5m, htf=htf)]
    tab_join, group_join = _joins(entry_join="AND", exit_join="")
    return StrategyCandidate(
        name=name,
        thesis=thesis,
        ema_fast=ema_fast,
        ema_slow=ema_slow,
        rules_model=rules,
        tab_group_join_mode=tab_join,
        group_rule_join_mode=group_join,
        entry_filters={},
    )


def _reversal_filters(
    side: str,
    *,
    adx_1m_max: float,
    rsi_limit: Optional[float],
    stoch_limit: Optional[float],
    trend_tfs: Sequence[str],
) -> List[Rule]:
    long = side.upper() == "LONG"
    stack = "BUY" if long else "SELL"
    slope_op = ">" if long else "<"
    rsi_op = "<=" if long else ">="
    stoch_op = "<=" if long else ">="
    filters = [_r("1m", "state", "adx", "<=", float(adx_1m_max))]
    if rsi_limit is not None:
        filters.append(_r("1m", "state", "rsi", rsi_op, float(rsi_limit)))
    if stoch_limit is not None:
        filters.append(_r("1m", "state", "stoch_rsi_k", stoch_op, float(stoch_limit)))
    for tf in trend_tfs:
        filters.append(_r(tf, "state", "ema_stack", "=", stack))
        filters.append(_r(tf, "state", "ema_2_slope", slope_op, 0.0))
    return filters


def make_reversal_candidate(
    *,
    name: str,
    thesis: str,
    ema_fast: int,
    ema_slow: int,
    long_event: Rule,
    short_event: Rule,
    adx_1m_max: float,
    long_rsi_max: Optional[float] = None,
    short_rsi_min: Optional[float] = None,
    long_stoch_max: Optional[float] = None,
    short_stoch_min: Optional[float] = None,
    trend_tfs: Sequence[str] = (),
) -> StrategyCandidate:
    rules = _empty_rules_model()
    rules["Long Entry"] = [[
        long_event,
        *_reversal_filters(
            "LONG",
            adx_1m_max=adx_1m_max,
            rsi_limit=long_rsi_max,
            stoch_limit=long_stoch_max,
            trend_tfs=trend_tfs,
        ),
    ]]
    rules["Short Entry"] = [[
        short_event,
        *_reversal_filters(
            "SHORT",
            adx_1m_max=adx_1m_max,
            rsi_limit=short_rsi_min,
            stoch_limit=short_stoch_min,
            trend_tfs=trend_tfs,
        ),
    ]]
    tab_join, group_join = _joins(entry_join="AND", exit_join="")
    return StrategyCandidate(
        name=name,
        thesis=thesis,
        ema_fast=ema_fast,
        ema_slow=ema_slow,
        rules_model=rules,
        tab_group_join_mode=tab_join,
        group_rule_join_mode=group_join,
        entry_filters={},
    )


def candidate_universe(kind: str = "all") -> List[StrategyCandidate]:
    kind = str(kind or "all").strip().lower()
    out: List[StrategyCandidate] = []
    for fast, slow in [(20, 200), (50, 200)]:
        if kind in ("all", "pullback"):
            for htf in [("5m",), ("5m", "15m"), ("5m", "15m", "1h")]:
                for adx_1m, adx_5m in [(18, None), (20, 18), (23, 20)]:
                    out.append(
                        make_ema_pullback_candidate(
                            name=f"ema{fast}_{slow}_pullback_adx{adx_1m}_{'none' if adx_5m is None else int(adx_5m)}_{'_'.join(htf)}",
                            ema_fast=fast,
                            ema_slow=slow,
                            adx_1m=adx_1m,
                            adx_5m=adx_5m,
                            htf=htf,
                        )
                    )
        if kind in ("all", "pullbreak"):
            for htf in [("5m",), ("5m", "15m"), ("5m", "15m", "1h")]:
                for adx_1m, adx_5m in [(15, None), (18, None), (20, 18), (23, 20)]:
                    out.append(
                        make_ema_pullbreak_candidate(
                            name=f"ema{fast}_{slow}_pullbreak_adx{adx_1m}_{'none' if adx_5m is None else int(adx_5m)}_{'_'.join(htf)}",
                            ema_fast=fast,
                            ema_slow=slow,
                            adx_1m=adx_1m,
                            adx_5m=adx_5m,
                            htf=htf,
                        )
                    )

        event_specs = [
            (
                "range",
                "Range Filter fresh event aligned with EMA trend and candle continuation.",
                _r("1m", "event", "range_filter_buy", "EVENT", None, is_trigger=True),
                _r("1m", "event", "range_filter_sell", "EVENT", None, is_trigger=True),
            ),
            (
                "vidya",
                "VIDYA trend event aligned with EMA trend, ADX and candle continuation.",
                _r("1m", "event", "vidya_trend_up", "EVENT", None, is_trigger=True),
                _r("1m", "event", "vidya_trend_down", "EVENT", None, is_trigger=True),
            ),
            (
                "frama",
                "FRAMA breakout event aligned with EMA trend, ADX and candle continuation.",
                _r("1m", "event", "frama_break_up", "EVENT", None, is_trigger=True),
                _r("1m", "event", "frama_break_down", "EVENT", None, is_trigger=True),
            ),
            (
                "macd",
                "MACD signal cross aligned with EMA trend, ADX and candle continuation.",
                _r("1m", "event", "ms_cross", "CROSS", "UP", is_trigger=True),
                _r("1m", "event", "ms_cross", "CROSS", "DOWN", is_trigger=True),
            ),
            (
                "di",
                "DI cross aligned with EMA trend, ADX and candle continuation.",
                _r("1m", "event", "di_cross", "CROSS", "UP", is_trigger=True),
                _r("1m", "event", "di_cross", "CROSS", "DOWN", is_trigger=True),
            ),
        ]
        if kind in ("all", "event"):
            for prefix, thesis, long_event, short_event in event_specs:
                for htf in [("5m",), ("5m", "15m"), ("5m", "15m", "1h")]:
                    for adx_1m, adx_5m in [(15, None), (18, None), (20, 18), (23, 20)]:
                        out.append(
                            make_event_candidate(
                                name=f"{prefix}_ema{fast}_{slow}_adx{adx_1m}_{'none' if adx_5m is None else int(adx_5m)}_{'_'.join(htf)}",
                                thesis=thesis,
                                ema_fast=fast,
                                ema_slow=slow,
                                long_event=long_event,
                                short_event=short_event,
                                adx_1m=adx_1m,
                                adx_5m=adx_5m,
                                htf=htf,
                            )
                        )
        if kind in ("all", "reversal", "reversal_fine"):
            if kind == "reversal_fine":
                trend_modes = [
                    ("trend1m", ("1m",)),
                    ("trend5m", ("5m",)),
                ]
                adx_ceiling_values = (18, 20, 22, 24)
                stoch_pairs: Sequence[Tuple[int, int]] = ()
                rsi_pairs: Sequence[Tuple[int, int]] = ((35, 65), (36, 64), (37, 63), (38, 62), (39, 61), (40, 60))
            else:
                trend_modes = [
                    ("chop", ()),
                    ("trend1m", ("1m",)),
                    ("trend5m", ("5m",)),
                    ("trend5m15m", ("5m", "15m")),
                ]
                adx_ceiling_values = (18, 22, 26)
                stoch_pairs = ((25, 75), (35, 65), (45, 55))
                rsi_pairs = ((35, 65), (40, 60), (45, 55))
            for mode_name, trend_tfs in trend_modes:
                for adx_1m_max in adx_ceiling_values:
                    for long_stoch_max, short_stoch_min in stoch_pairs:
                        out.append(
                            make_reversal_candidate(
                                name=(
                                    f"stoch_reversal_ema{fast}_{slow}_{mode_name}_"
                                    f"adxle{adx_1m_max}_k{long_stoch_max}_{short_stoch_min}"
                                ),
                                thesis=(
                                    "StochRSI K/D color turn from an stretched 1m oscillator, "
                                    f"ADX <= {adx_1m_max}, trend mode={mode_name}."
                                ),
                                ema_fast=fast,
                                ema_slow=slow,
                                long_event=_r("1m", "event", "stoch_rsi_kd", "TURNED", "GREEN", is_trigger=True),
                                short_event=_r("1m", "event", "stoch_rsi_kd", "TURNED", "RED", is_trigger=True),
                                adx_1m_max=adx_1m_max,
                                long_stoch_max=long_stoch_max,
                                short_stoch_min=short_stoch_min,
                                trend_tfs=trend_tfs,
                            )
                        )
                    for long_rsi_max, short_rsi_min in rsi_pairs:
                        out.append(
                            make_reversal_candidate(
                                name=(
                                    f"rsi_reversal_ema{fast}_{slow}_{mode_name}_"
                                    f"adxle{adx_1m_max}_rsi{long_rsi_max}_{short_rsi_min}"
                                ),
                                thesis=(
                                    "RSI smoothed-state color turn from an stretched 1m oscillator, "
                                    f"ADX <= {adx_1m_max}, trend mode={mode_name}."
                                ),
                                ema_fast=fast,
                                ema_slow=slow,
                                long_event=_r("1m", "event", "rsi_state", "TURNED", "GREEN", is_trigger=True),
                                short_event=_r("1m", "event", "rsi_state", "TURNED", "RED", is_trigger=True),
                                adx_1m_max=adx_1m_max,
                                long_rsi_max=long_rsi_max,
                                short_rsi_min=short_rsi_min,
                                trend_tfs=trend_tfs,
                            )
                        )
    return out


def exit_profiles() -> List[ExitProfile]:
    return [
        ExitProfile("pct0p15_0p25", "PERCENT", 0.15, "PERCENT", 0.25),
        ExitProfile("pct0p20_0p35", "PERCENT", 0.20, "PERCENT", 0.35),
        ExitProfile("pct0p25_0p45", "PERCENT", 0.25, "PERCENT", 0.45),
        ExitProfile("pct0p35_0p70", "PERCENT", 0.35, "PERCENT", 0.70),
        ExitProfile("atr1p0_r1p5", "ATR", 1.0, "R_MULTIPLE", 1.5),
        ExitProfile("atr1p2_r1p8", "ATR", 1.2, "R_MULTIPLE", 1.8),
        ExitProfile("atr1p5_r2p0", "ATR", 1.5, "R_MULTIPLE", 2.0),
    ]


def _cfg(profile: ExitProfile, *, order_notional: float, fee_pct: float, slippage_bps: float) -> BacktestConfig:
    return BacktestConfig(
        initial_balance=1000.0,
        order_notional_usdt=float(order_notional),
        fee_rate=float(fee_pct) / 100.0,
        slippage_bps=float(slippage_bps),
        stop_loss_pct=profile.stop_value if profile.stop_mode == "PERCENT" else 0.0,
        take_profit_pct=profile.take_value if profile.take_mode == "PERCENT" else 0.0,
        stop_loss_mode=profile.stop_mode,
        stop_loss_value=float(profile.stop_value),
        stop_loss_timeframe="1m",
        take_profit_mode=profile.take_mode,
        take_profit_value=float(profile.take_value),
        take_profit_timeframe="1m",
        break_even_after_mfe_pct=float(profile.break_even_after_mfe_pct),
        trailing_stop_pct=float(profile.trailing_stop_pct),
        allow_reverse=False,
        skip_if_both_entries=True,
        step_timeframe="1m",
        capture_trade_details=True,
        equity_curve_stride=15,
    )


def _aggregate_rules(candidates: Sequence[StrategyCandidate]) -> Dict[str, List[List[Rule]]]:
    out = _empty_rules_model()
    for cand in candidates:
        for tab, groups in cand.rules_model.items():
            out.setdefault(tab, []).extend(groups or [])
    return out


def _summary_row(name: str, exit_name: str, window: str, summary: Mapping[str, Any], trades: Sequence[Any]) -> Dict[str, Any]:
    days = 0.0
    try:
        start = pd.to_datetime(summary.get("start"), utc=True)
        end = pd.to_datetime(summary.get("end"), utc=True)
        days = max(1e-9, float((end - start).total_seconds()) / 86400.0)
    except Exception:
        days = 0.0
    trade_count = int(summary.get("trades", summary.get("num_trades", len(trades))) or 0)
    net_profit = float(summary.get("net_profit", 0.0) or 0.0)
    gross_loss = float(summary.get("gross_loss", 0.0) or 0.0)
    profit_factor = summary.get("profit_factor", 0.0)
    try:
        profit_factor = float(profit_factor)
    except Exception:
        profit_factor = 999.0 if str(profit_factor).lower() == "inf" else 0.0
    return {
        "candidate": name,
        "exit": exit_name,
        "window": window,
        "net_profit": net_profit,
        "return_pct": float(summary.get("return_pct", 0.0) or 0.0),
        "trades": trade_count,
        "trades_per_day": trade_count / days if days > 0 else 0.0,
        "wins": int(summary.get("wins", 0) or 0),
        "losses": int(summary.get("losses", 0) or 0),
        "win_rate_pct": float(summary.get("win_rate_pct", 0.0) or 0.0),
        "profit_factor": profit_factor,
        "avg_trade": float(summary.get("avg_trade", 0.0) or 0.0),
        "avg_duration_min": float(summary.get("avg_duration_min", 0.0) or 0.0),
        "max_drawdown": float(summary.get("max_drawdown", 0.0) or 0.0),
        "max_drawdown_pct": float(summary.get("max_drawdown_pct", 0.0) or 0.0),
        "gross_loss": gross_loss,
        "exit_reason_counts": dict(summary.get("exit_reason_counts", {}) or {}),
    }


def _score(train: Mapping[str, Any], validation: Mapping[str, Any]) -> float:
    train_trades = int(train.get("trades", 0) or 0)
    val_trades = int(validation.get("trades", 0) or 0)
    if train_trades < 8 or val_trades < 1:
        return -1e9
    tpd = float(train.get("trades_per_day", 0.0) or 0.0)
    cadence_penalty = abs(tpd - 1.0) * 0.35
    dd_penalty = abs(float(train.get("max_drawdown", 0.0) or 0.0)) * 0.10
    val_dd_penalty = abs(float(validation.get("max_drawdown", 0.0) or 0.0)) * 0.15
    return (
        float(train.get("net_profit", 0.0) or 0.0)
        + 1.7 * float(validation.get("net_profit", 0.0) or 0.0)
        + 0.02 * float(train.get("win_rate_pct", 0.0) or 0.0)
        + 0.05 * min(float(train.get("profit_factor", 0.0) or 0.0), 5.0)
        - cadence_penalty
        - dd_penalty
        - val_dd_penalty
    )


def _rule_to_dict(rule: Rule) -> Dict[str, Any]:
    return {
        "timeframe": rule.timeframe,
        "mode": rule.mode,
        "field": rule.field,
        "op": rule.op,
        "value": rule.value,
        "compare_to": getattr(rule, "compare_to", None),
        "is_trigger": bool(rule.is_trigger),
        "is_final_gate": bool(getattr(rule, "is_final_gate", False)),
        "is_sequence_canceler": bool(getattr(rule, "is_sequence_canceler", False)),
        "bars_ago_mode": str(getattr(rule, "bars_ago_mode", "OFF") or "OFF"),
        "bars_ago_n": int(getattr(rule, "bars_ago_n", 0) or 0),
        "require_still_valid": bool(getattr(rule, "require_still_valid", False)),
        "window_bars": int(getattr(rule, "window_bars", 1) or 1),
    }


def _candidate_preset(candidate: StrategyCandidate, profile: ExitProfile, *, symbol: str, start: pd.Timestamp, end: pd.Timestamp) -> Dict[str, Any]:
    tabs: Dict[str, Any] = {}
    for tab in TABS:
        groups = []
        joins = candidate.group_rule_join_mode.get(tab, [])
        for idx, group in enumerate(candidate.rules_model.get(tab, []) or []):
            groups.append(
                {
                    "group_key": chr(ord("A") + idx),
                    "rules_join": joins[idx] if idx < len(joins) else "OR",
                    "rules": [_rule_to_dict(rule) for rule in group],
                }
            )
        tabs[tab] = {
            "groups_join": candidate.tab_group_join_mode.get(tab, "AND"),
            "groups": groups,
            "advanced_logic": {"enabled": False, "routes": []},
        }
    return {
        "version": 1,
        "settings": {
            "symbol": symbol,
            "start": start.strftime("%Y-%m-%d %H:%M:%S"),
            "end": end.strftime("%Y-%m-%d %H:%M:%S"),
            "warmup_min": 0,
            "cache_dir": "data_cache",
            "price_source": "LAST",
            "macd_impl": "TRADINGVIEW",
            "adx_impl": "TRADINGVIEW",
            "ema_indicators": _ema_settings(candidate.ema_fast, candidate.ema_slow),
            "bt_mode": BACKTEST_MODE_BAR_1M_HTF_CLOSED_ONLY,
            "timeframes": {"1m": True, "5m": True, "15m": True, "30m": False, "1h": True},
            "stop_loss_mode": profile.stop_mode,
            "stop_loss_value": profile.stop_value,
            "take_profit_mode": profile.take_mode,
            "take_profit_value": profile.take_value,
        },
        "tabs": tabs,
        "entry_filters": candidate.entry_filters,
        "meta": {
            "strategy_note": candidate.thesis,
            "research_exit_profile": profile.name,
        },
    }


def _prepare_streams(
    *,
    candidates: Sequence[StrategyCandidate],
    profiles: Sequence[ExitProfile],
    symbol: str,
    start: pd.Timestamp,
    end: pd.Timestamp,
    timeframes: Sequence[str],
    cache_dir: Path,
    ema_fast: int,
    ema_slow: int,
) -> Tuple[pd.DataFrame, Dict[str, pd.DataFrame], StreamRequirementSpec, int]:
    ema_settings = _ema_settings(ema_fast, ema_slow)
    aggregate_rules = _aggregate_rules(candidates)
    base_cfg = _cfg(profiles[0], order_notional=100.0, fee_pct=0.04, slippage_bps=0.0)
    req_spec = compile_stream_requirements(aggregate_rules, backtest_cfg=base_cfg, include_plot_defaults=True)
    warmup = required_warmup_minutes(list(timeframes), required_fields=req_spec, ema_settings=ema_settings)
    warmup_start = start - pd.Timedelta(minutes=int(warmup))

    prepared = find_covering_prepared_dataset_on_disk(
        str(cache_dir),
        symbol=symbol,
        start=warmup_start,
        end=end,
        timeframes=list(timeframes),
        price_source="LAST",
        macd_impl="TRADINGVIEW",
        adx_impl="TRADINGVIEW",
        required_fields=req_spec,
        ema_settings=ema_settings,
        market_state_thresholds=None,
    )
    if prepared is not None:
        print(f"loaded prepared dataset for EMA {ema_fast}/{ema_slow}", flush=True)
        return (
            slice_df_1m_window(prepared.df_1m_full, warmup_start, end),
            slice_streams_window(prepared.streams_full, warmup_start, end, timeframes=list(timeframes)),
            req_spec,
            int(warmup),
        )

    df_1m = fetch_klines_1m_futures(symbol, warmup_start, end, str(cache_dir), price_source="LAST")
    if df_1m.empty:
        raise ValueError(f"No candles returned for {symbol} {warmup_start} -> {end}")
    streams = simulate_multitf_indicators(
        df_1m,
        list(timeframes),
        macd_impl="TRADINGVIEW",
        adx_impl="TRADINGVIEW",
        cache_dir=str(cache_dir),
        symbol=symbol,
        start_utc=warmup_start,
        end_utc=end,
        price_source="LAST",
        required_fields=req_spec,
        ema_settings=ema_settings,
    )
    try:
        entry = build_prepared_dataset(
            symbol=symbol,
            start=warmup_start,
            end=end,
            timeframes=list(timeframes),
            price_source="LAST",
            macd_impl="TRADINGVIEW",
            adx_impl="TRADINGVIEW",
            required_fields=req_spec,
            ema_settings=ema_settings,
            market_state_thresholds=None,
            df_1m_full=df_1m,
            streams_full=streams,
        )
        saved = save_prepared_dataset_to_disk(str(cache_dir), entry)
        if saved:
            print(f"saved prepared dataset: {Path(saved).name}", flush=True)
    except Exception as exc:
        print(f"warning: prepared dataset save skipped: {exc}", flush=True)
    return df_1m, streams, req_spec, int(warmup)


def run(args: argparse.Namespace) -> int:
    warnings.filterwarnings("ignore", category=FutureWarning)
    symbol = str(args.symbol or "BTCUSDT").upper().strip()
    cache_dir = Path(args.cache_dir or REPO_ROOT / "data_cache").resolve()
    cache_dir.mkdir(parents=True, exist_ok=True)
    timeframes = ["1m", "5m", "15m", "1h"]

    server_now = binance_fapi_server_time_utc()
    if args.validation_end:
        validation_end = parse_utc(args.validation_end)
    elif server_now is not None:
        validation_end = pd.Timestamp(server_now).floor("min")
    else:
        validation_end = pd.Timestamp.now(tz="UTC").floor("min")
    validation_start = parse_utc(args.validation_start) if args.validation_start else (validation_end.floor("D") - pd.Timedelta(days=1))
    train_end = validation_start
    train_start = parse_utc(args.train_start) if args.train_start else (train_end - pd.Timedelta(days=int(args.train_days)))
    full_start = train_start
    full_end = validation_end

    out_dir = Path(args.output_dir or REPO_ROOT / "runs" / f"btc_1m_research_{pd.Timestamp.now(tz='UTC').strftime('%Y%m%d_%H%M%S')}").resolve()
    out_dir.mkdir(parents=True, exist_ok=True)

    print(f"symbol={symbol}", flush=True)
    print(f"train={train_start} -> {train_end}", flush=True)
    print(f"validation={validation_start} -> {validation_end}", flush=True)
    print(f"output={out_dir}", flush=True)

    all_candidates = candidate_universe(args.kind)
    if args.max_candidates:
        all_candidates = all_candidates[: int(args.max_candidates)]
    profiles = exit_profiles()
    if args.max_exit_profiles:
        profiles = profiles[: int(args.max_exit_profiles)]

    leaderboard: List[Dict[str, Any]] = []
    best_payload: Optional[Dict[str, Any]] = None
    best_score = -1e18
    detail_trades: Dict[str, List[Dict[str, Any]]] = {}

    groups: Dict[Tuple[int, int], List[StrategyCandidate]] = {}
    for cand in all_candidates:
        groups.setdefault((cand.ema_fast, cand.ema_slow), []).append(cand)

    for (ema_fast, ema_slow), candidates in groups.items():
        print(f"\n=== EMA {ema_fast}/{ema_slow}: {len(candidates)} candidates ===", flush=True)
        df_1m, streams, _req, warmup = _prepare_streams(
            candidates=candidates,
            profiles=profiles,
            symbol=symbol,
            start=full_start,
            end=full_end,
            timeframes=timeframes,
            cache_dir=cache_dir,
            ema_fast=ema_fast,
            ema_slow=ema_slow,
        )
        print(f"warmup_minutes={warmup} candles={len(df_1m)}", flush=True)
        streams_eval = make_streams_htf_closed_only(streams)
        for ci, cand in enumerate(candidates, start=1):
            for profile in profiles:
                cfg = _cfg(profile, order_notional=args.order_notional, fee_pct=args.fee_pct, slippage_bps=args.slippage_bps)
                try:
                    train_res = run_backtest_auto(
                        symbol=symbol,
                        streams_full=streams_eval,
                        start=train_start,
                        end=train_end,
                        rules_model=cand.rules_model,
                        tab_group_join_mode=cand.tab_group_join_mode,
                        group_rule_join_mode=cand.group_rule_join_mode,
                        cfg=cfg,
                        df_1m_full=df_1m,
                        entry_filters=cand.entry_filters,
                        apply_htf_closed_only=False,
                    )
                    val_res = run_backtest_auto(
                        symbol=symbol,
                        streams_full=streams_eval,
                        start=validation_start,
                        end=validation_end,
                        rules_model=cand.rules_model,
                        tab_group_join_mode=cand.tab_group_join_mode,
                        group_rule_join_mode=cand.group_rule_join_mode,
                        cfg=cfg,
                        df_1m_full=df_1m,
                        entry_filters=cand.entry_filters,
                        apply_htf_closed_only=False,
                    )
                except Exception as exc:
                    leaderboard.append(
                        {
                            "candidate": cand.name,
                            "exit": profile.name,
                            "error": str(exc),
                            "score": -1e9,
                        }
                    )
                    continue

                train_row = _summary_row(cand.name, profile.name, "train", train_res.summary, train_res.trades)
                val_row = _summary_row(cand.name, profile.name, "validation", val_res.summary, val_res.trades)
                score = _score(train_row, val_row)
                row = {
                    "candidate": cand.name,
                    "exit": profile.name,
                    "score": score,
                    "thesis": cand.thesis,
                    "ema_fast": cand.ema_fast,
                    "ema_slow": cand.ema_slow,
                    "train": train_row,
                    "validation": val_row,
                }
                leaderboard.append(row)
                if score > best_score:
                    best_score = score
                    best_payload = {
                        "candidate": cand,
                        "profile": profile,
                        "row": row,
                        "preset": _candidate_preset(cand, profile, symbol=symbol, start=validation_start, end=validation_end),
                    }
                    detail_trades["best_train"] = [
                        {
                            "side": t.side,
                            "entry_time": str(t.entry_time),
                            "exit_time": str(t.exit_time),
                            "entry_price": t.entry_price,
                            "exit_price": t.exit_price,
                            "net_pnl": t.net_pnl,
                            "return_pct": t.return_pct,
                            "exit_reason": t.exit_reason,
                        }
                        for t in train_res.trades[:50]
                    ]
                    detail_trades["best_validation"] = [
                        {
                            "side": t.side,
                            "entry_time": str(t.entry_time),
                            "exit_time": str(t.exit_time),
                            "entry_price": t.entry_price,
                            "exit_price": t.exit_price,
                            "net_pnl": t.net_pnl,
                            "return_pct": t.return_pct,
                            "exit_reason": t.exit_reason,
                        }
                        for t in val_res.trades[:50]
                    ]
            if ci % 10 == 0 or ci == len(candidates):
                print(f"tested {ci}/{len(candidates)} candidates for EMA {ema_fast}/{ema_slow}", flush=True)

    leaderboard.sort(key=lambda r: float(r.get("score", -1e18) or -1e18), reverse=True)
    payload = {
        "symbol": symbol,
        "server_now_utc": str(server_now),
        "train_start": str(train_start),
        "train_end": str(train_end),
        "validation_start": str(validation_start),
        "validation_end": str(validation_end),
        "candidate_count": len(all_candidates),
        "exit_profile_count": len(profiles),
        "leaderboard": leaderboard,
        "best_trades": detail_trades,
    }
    (out_dir / "leaderboard.json").write_text(json.dumps(_json_safe(payload), indent=2, ensure_ascii=False), encoding="utf-8")

    if best_payload is not None:
        best = {
            "best": _json_safe(best_payload["row"]),
            "preset": _json_safe(best_payload["preset"]),
            "trades": _json_safe(detail_trades),
        }
        (out_dir / "best_preset.json").write_text(json.dumps(best, indent=2, ensure_ascii=False), encoding="utf-8")
        preset_bundle = {
            "meta": {"autoload_last": False, "last_preset": "BTC_1M_AI_RESEARCH_BEST"},
            "presets": {"BTC_1M_AI_RESEARCH_BEST": _json_safe(best_payload["preset"])},
        }
        (out_dir / "presets.json").write_text(json.dumps(preset_bundle, indent=2, ensure_ascii=False), encoding="utf-8")

    print("\nTop 10", flush=True)
    for row in leaderboard[:10]:
        if "error" in row:
            print(f"{row['candidate']} / {row['exit']} ERROR {row['error']}", flush=True)
            continue
        tr = row["train"]
        va = row["validation"]
        print(
            f"{row['candidate']} / {row['exit']} score={row['score']:.4f} "
            f"train pnl={tr['net_profit']:.4f} trades={tr['trades']} wr={tr['win_rate_pct']:.1f}% pf={tr['profit_factor']:.2f} tpd={tr['trades_per_day']:.2f} "
            f"val pnl={va['net_profit']:.4f} trades={va['trades']} wr={va['win_rate_pct']:.1f}% pf={va['profit_factor']:.2f}",
            flush=True,
        )
    print(f"\nSaved: {out_dir}", flush=True)
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description="Research BTCUSDT 1m strategies with the project backtest engine.")
    parser.add_argument("--symbol", default="BTCUSDT")
    parser.add_argument("--cache-dir", default=str(REPO_ROOT / "data_cache"))
    parser.add_argument("--train-start", default=None)
    parser.add_argument("--train-days", type=int, default=31)
    parser.add_argument("--validation-start", default=None)
    parser.add_argument("--validation-end", default=None)
    parser.add_argument("--order-notional", type=float, default=100.0)
    parser.add_argument("--fee-pct", type=float, default=0.04)
    parser.add_argument("--slippage-bps", type=float, default=1.0)
    parser.add_argument("--max-candidates", type=int, default=0)
    parser.add_argument("--max-exit-profiles", type=int, default=0)
    parser.add_argument("--kind", choices=["all", "event", "pullback", "pullbreak", "reversal", "reversal_fine"], default="all")
    parser.add_argument("--output-dir", default=None)
    return run(parser.parse_args())


if __name__ == "__main__":
    raise SystemExit(main())
