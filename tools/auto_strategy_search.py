from __future__ import annotations

import argparse
import json
import math
import random
import sys
import warnings
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Mapping, Optional, Sequence, Tuple

import pandas as pd

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from app.backtest_frames import make_streams_htf_closed_only
from app.data_binance import binance_fapi_server_time_utc
from app.fast_backtest import run_backtest_auto
from app.rules import Rule

from tools.research_btc_1m_strategy import (
    TABS,
    ExitProfile,
    StrategyCandidate,
    _cfg,
    _candidate_preset,
    _ema_settings,
    _empty_rules_model,
    _json_safe,
    _joins,
    _prepare_streams,
    _r,
    _summary_row,
)


@dataclass(frozen=True)
class FilterBlock:
    name: str
    thesis: str
    long_rules: Tuple[Rule, ...]
    short_rules: Tuple[Rule, ...]


@dataclass(frozen=True)
class EventSpec:
    name: str
    thesis: str
    long_event: Rule
    short_event: Rule


def _clean_number(value: float) -> str:
    text = f"{float(value):g}"
    return text.replace("-", "m").replace(".", "p")


def _op_side(side: str, long_op: str, short_op: str) -> str:
    return long_op if str(side).upper() == "LONG" else short_op


def _dir_value(side: str, long_value: Any, short_value: Any) -> Any:
    return long_value if str(side).upper() == "LONG" else short_value


def _event_specs() -> List[EventSpec]:
    return [
        EventSpec(
            "range",
            "Range Filter fresh directional event.",
            _r("1m", "event", "range_filter_buy", "EVENT", None, is_trigger=True),
            _r("1m", "event", "range_filter_sell", "EVENT", None, is_trigger=True),
        ),
        EventSpec(
            "vidya",
            "VIDYA trend flip event.",
            _r("1m", "event", "vidya_trend_up", "EVENT", None, is_trigger=True),
            _r("1m", "event", "vidya_trend_down", "EVENT", None, is_trigger=True),
        ),
        EventSpec(
            "frama",
            "FRAMA breakout event.",
            _r("1m", "event", "frama_break_up", "EVENT", None, is_trigger=True),
            _r("1m", "event", "frama_break_down", "EVENT", None, is_trigger=True),
        ),
        EventSpec(
            "macd",
            "MACD signal-line cross.",
            _r("1m", "event", "ms_cross", "CROSS", "UP", is_trigger=True),
            _r("1m", "event", "ms_cross", "CROSS", "DOWN", is_trigger=True),
        ),
        EventSpec(
            "macd0",
            "MACD zero-line cross.",
            _r("1m", "event", "macd0_cross", "CROSS", "UP", is_trigger=True),
            _r("1m", "event", "macd0_cross", "CROSS", "DOWN", is_trigger=True),
        ),
        EventSpec(
            "di",
            "DI cross.",
            _r("1m", "event", "di_cross", "CROSS", "UP", is_trigger=True),
            _r("1m", "event", "di_cross", "CROSS", "DOWN", is_trigger=True),
        ),
        EventSpec(
            "rsi_turn",
            "RSI smoothed-state color turn.",
            _r("1m", "event", "rsi_state", "TURNED", "GREEN", is_trigger=True),
            _r("1m", "event", "rsi_state", "TURNED", "RED", is_trigger=True),
        ),
        EventSpec(
            "stoch_turn",
            "StochRSI K/D color turn.",
            _r("1m", "event", "stoch_rsi_kd", "TURNED", "GREEN", is_trigger=True),
            _r("1m", "event", "stoch_rsi_kd", "TURNED", "RED", is_trigger=True),
        ),
        EventSpec(
            "ema_cross",
            "EMA fast/slow cross.",
            _r("1m", "event", "ema_cross_up", "EVENT", None, is_trigger=True),
            _r("1m", "event", "ema_cross_down", "EVENT", None, is_trigger=True),
        ),
    ]


def _ema_trend_block(name: str, tfs: Sequence[str]) -> FilterBlock:
    long_rules: List[Rule] = []
    short_rules: List[Rule] = []
    for tf in tfs:
        long_rules.extend([
            _r(tf, "state", "ema_stack", "=", "BUY"),
            _r(tf, "state", "ema_2_slope", ">", 0.0),
        ])
        short_rules.extend([
            _r(tf, "state", "ema_stack", "=", "SELL"),
            _r(tf, "state", "ema_2_slope", "<", 0.0),
        ])
    return FilterBlock(name, f"EMA stack/slope aligned on {','.join(tfs)}.", tuple(long_rules), tuple(short_rules))


def _price_vs_ema_block(tf: str, ema_field: str) -> FilterBlock:
    return FilterBlock(
        f"{tf}_price_{ema_field}",
        f"Price is on the directional side of {ema_field} on {tf}.",
        (_r(tf, "state", "close", ">", None, compare_to={"field": ema_field}),),
        (_r(tf, "state", "close", "<", None, compare_to={"field": ema_field}),),
    )


def _adx_trend_block(tf: str, threshold: float) -> FilterBlock:
    return FilterBlock(
        f"{tf}_adxge{_clean_number(threshold)}",
        f"ADX >= {threshold:g} on {tf}.",
        (_r(tf, "state", "adx", ">=", float(threshold)),),
        (_r(tf, "state", "adx", ">=", float(threshold)),),
    )


def _adx_chop_block(tf: str, threshold: float) -> FilterBlock:
    return FilterBlock(
        f"{tf}_adxle{_clean_number(threshold)}",
        f"ADX <= {threshold:g} on {tf}.",
        (_r(tf, "state", "adx", "<=", float(threshold)),),
        (_r(tf, "state", "adx", "<=", float(threshold)),),
    )


def _di_align_block(tf: str) -> FilterBlock:
    return FilterBlock(
        f"{tf}_di_align",
        f"DI spread agrees with side on {tf}.",
        (_r(tf, "state", "di_spread", ">=", 0.0),),
        (_r(tf, "state", "di_spread", "<=", 0.0),),
    )


def _osc_reversal_block(tf: str, rsi_long_max: float, rsi_short_min: float) -> FilterBlock:
    return FilterBlock(
        f"{tf}_rsi_extreme{_clean_number(rsi_long_max)}_{_clean_number(rsi_short_min)}",
        f"RSI stretched against side on {tf}.",
        (_r(tf, "state", "rsi", "<=", float(rsi_long_max)),),
        (_r(tf, "state", "rsi", ">=", float(rsi_short_min)),),
    )


def _stoch_reversal_block(tf: str, k_long_max: float, k_short_min: float) -> FilterBlock:
    return FilterBlock(
        f"{tf}_stoch_extreme{_clean_number(k_long_max)}_{_clean_number(k_short_min)}",
        f"StochRSI K stretched against side on {tf}.",
        (_r(tf, "state", "stoch_rsi_k", "<=", float(k_long_max)),),
        (_r(tf, "state", "stoch_rsi_k", ">=", float(k_short_min)),),
    )


def _rsi_momentum_block(tf: str, threshold: float = 50.0) -> FilterBlock:
    return FilterBlock(
        f"{tf}_rsi_momo{_clean_number(threshold)}",
        f"RSI confirms momentum on {tf}.",
        (_r(tf, "state", "rsi", ">=", float(threshold)),),
        (_r(tf, "state", "rsi", "<=", float(100.0 - threshold)),),
    )


def _state_align_block(tf: str, field: str, long_value: Any, short_value: Any) -> FilterBlock:
    return FilterBlock(
        f"{tf}_{field}_align",
        f"{field} agrees with side on {tf}.",
        (_r(tf, "state", field, "=", long_value),),
        (_r(tf, "state", field, "=", short_value),),
    )


def _state_same_block(tf: str, field: str, value: Any) -> FilterBlock:
    return FilterBlock(
        f"{tf}_{field}_{str(value).lower()}",
        f"{field} == {value} on {tf}.",
        (_r(tf, "state", field, "=", value),),
        (_r(tf, "state", field, "=", value),),
    )


def _numeric_same_block(tf: str, field: str, op: str, value: float) -> FilterBlock:
    return FilterBlock(
        f"{tf}_{field}{op.replace('>', 'gt').replace('<', 'lt').replace('=', 'e')}{_clean_number(value)}",
        f"{field} {op} {value:g} on {tf}.",
        (_r(tf, "state", field, op, float(value)),),
        (_r(tf, "state", field, op, float(value)),),
    )


def _filter_library(scope: str = "full") -> Tuple[List[FilterBlock], List[FilterBlock], List[FilterBlock]]:
    scope_n = str(scope or "full").strip().lower()
    regimes = [
        FilterBlock("none", "No regime filter.", tuple(), tuple()),
        _ema_trend_block("trend_1m", ("1m",)),
        _ema_trend_block("trend_5m", ("5m",)),
        _ema_trend_block("trend_15m", ("15m",)),
        _ema_trend_block("trend_5m15m", ("5m", "15m")),
        _ema_trend_block("trend_5m15m1h", ("5m", "15m", "1h")),
        _price_vs_ema_block("1m", "ema_2"),
        _price_vs_ema_block("5m", "ema_2"),
        _state_align_block("5m", "range_filter_state", "BUY", "SELL"),
        _state_align_block("5m", "vidya_state", "BUY", "SELL"),
        _state_align_block("5m", "frama_state", "BUY", "SELL"),
    ]
    if scope_n == "full":
        regimes.extend([
        _state_align_block("5m", "market_bias", "LONG", "SHORT"),
        _state_same_block("5m", "market_phase", "PULLBACK"),
        _state_same_block("5m", "market_phase", "EXPANSION"),
        _state_same_block("5m", "market_phase", "COMPRESSION"),
        ])
    momentum = [
        FilterBlock("momo_none", "No momentum/volatility filter.", tuple(), tuple()),
        _adx_trend_block("1m", 15),
        _adx_trend_block("1m", 20),
        _adx_trend_block("1m", 25),
        _adx_chop_block("1m", 18),
        _adx_chop_block("1m", 22),
        _adx_chop_block("1m", 26),
        _di_align_block("1m"),
        _rsi_momentum_block("1m", 50),
        _osc_reversal_block("1m", 35, 65),
        _osc_reversal_block("1m", 40, 60),
        _stoch_reversal_block("1m", 25, 75),
        _stoch_reversal_block("1m", 35, 65),
    ]
    context = [
        FilterBlock("ctx_none", "No context filter.", tuple(), tuple()),
    ]
    if scope_n == "full":
        context.extend([
        _state_align_block("1m", "taker_bias_state", "BUY", "SELL"),
        FilterBlock(
            "1m_taker_delta5",
            "Taker delta agrees with side by at least 5%.",
            (_r("1m", "state", "taker_delta_pct", ">=", 5.0),),
            (_r("1m", "state", "taker_delta_pct", "<=", -5.0),),
        ),
        FilterBlock(
            "1m_taker_delta10",
            "Taker delta agrees with side by at least 10%.",
            (_r("1m", "state", "taker_delta_pct", ">=", 10.0),),
            (_r("1m", "state", "taker_delta_pct", "<=", -10.0),),
        ),
        _numeric_same_block("5m", "market_confidence", ">=", 55),
        _numeric_same_block("5m", "market_confidence", ">=", 65),
        FilterBlock(
            "5m_market_dir_align",
            "Market direction score agrees with side on 5m.",
            (_r("5m", "state", "market_direction_score", ">=", 0.0),),
            (_r("5m", "state", "market_direction_score", "<=", 0.0),),
        ),
        ])
    return regimes, momentum, context


def _exit_profiles() -> List[ExitProfile]:
    return [
        ExitProfile("pct0p12_0p20", "PERCENT", 0.12, "PERCENT", 0.20),
        ExitProfile("pct0p15_0p25", "PERCENT", 0.15, "PERCENT", 0.25),
        ExitProfile("pct0p20_0p35", "PERCENT", 0.20, "PERCENT", 0.35),
        ExitProfile("pct0p25_0p45", "PERCENT", 0.25, "PERCENT", 0.45),
        ExitProfile("pct0p35_0p70", "PERCENT", 0.35, "PERCENT", 0.70),
        ExitProfile("pct0p50_1p00", "PERCENT", 0.50, "PERCENT", 1.00),
    ]


def _make_candidate(
    *,
    event: EventSpec,
    ema_fast: int,
    ema_slow: int,
    regime: FilterBlock,
    momentum: FilterBlock,
    context: FilterBlock,
) -> StrategyCandidate:
    rules = _empty_rules_model()
    long_rules = [event.long_event, *regime.long_rules, *momentum.long_rules, *context.long_rules]
    short_rules = [event.short_event, *regime.short_rules, *momentum.short_rules, *context.short_rules]
    rules["Long Entry"] = [long_rules]
    rules["Short Entry"] = [short_rules]
    tab_join, group_join = _joins(entry_join="AND", exit_join="")
    name = (
        f"{event.name}_ema{ema_fast}_{ema_slow}_"
        f"{regime.name}_{momentum.name}_{context.name}"
    )
    thesis = " | ".join([event.thesis, regime.thesis, momentum.thesis, context.thesis])
    return StrategyCandidate(
        name=name,
        thesis=thesis,
        ema_fast=int(ema_fast),
        ema_slow=int(ema_slow),
        rules_model=rules,
        tab_group_join_mode=tab_join,
        group_rule_join_mode=group_join,
        entry_filters={},
    )


def _candidate_universe(scope: str = "full") -> List[StrategyCandidate]:
    events = _event_specs()
    regimes, momentum_blocks, context_blocks = _filter_library(scope)
    ema_pairs = [(9, 21), (20, 50), (20, 200), (50, 200)]
    out: List[StrategyCandidate] = []
    for ema_fast, ema_slow in ema_pairs:
        for event in events:
            for regime in regimes:
                for momo in momentum_blocks:
                    for ctx in context_blocks:
                        if event.name in {"rsi_turn", "stoch_turn"} and momo.name in {"1m_rsi_momo50"}:
                            continue
                        out.append(
                            _make_candidate(
                                event=event,
                                ema_fast=ema_fast,
                                ema_slow=ema_slow,
                                regime=regime,
                                momentum=momo,
                                context=ctx,
                            )
                        )
    return out


def _days_between(start: pd.Timestamp, end: pd.Timestamp) -> float:
    return max(1e-9, float((pd.to_datetime(end, utc=True) - pd.to_datetime(start, utc=True)).total_seconds()) / 86400.0)


def _search_score(
    train: Mapping[str, Any],
    validation: Mapping[str, Any],
    *,
    target_trades_per_day: float,
    min_train_trades: int,
    min_validation_trades: int,
) -> float:
    train_trades = int(train.get("trades", 0) or 0)
    val_trades = int(validation.get("trades", 0) or 0)
    train_pnl = float(train.get("net_profit", 0.0) or 0.0)
    val_pnl = float(validation.get("net_profit", 0.0) or 0.0)
    train_pf = min(float(train.get("profit_factor", 0.0) or 0.0), 5.0)
    val_pf = min(float(validation.get("profit_factor", 0.0) or 0.0), 5.0)
    train_wr = float(train.get("win_rate_pct", 0.0) or 0.0)
    val_wr = float(validation.get("win_rate_pct", 0.0) or 0.0)
    train_tpd = float(train.get("trades_per_day", 0.0) or 0.0)

    score = 0.75 * train_pnl + 2.25 * val_pnl
    score += 0.08 * train_pf + 0.12 * val_pf
    score += 0.006 * train_wr + 0.010 * val_wr
    score -= 0.30 * abs(train_tpd - float(target_trades_per_day))
    score -= 0.15 * abs(float(train.get("max_drawdown", 0.0) or 0.0))
    score -= 0.25 * abs(float(validation.get("max_drawdown", 0.0) or 0.0))

    if train_trades < int(min_train_trades):
        score -= 1000.0 + 10.0 * (int(min_train_trades) - train_trades)
    if val_trades < int(min_validation_trades):
        score -= 1000.0 + 25.0 * (int(min_validation_trades) - val_trades)
    if train_pnl <= 0:
        score -= 3.0 * abs(train_pnl) + 2.0
    if val_pnl <= 0:
        score -= 6.0 * abs(val_pnl) + 4.0
    if train_tpd > max(3.0, float(target_trades_per_day) * 4.0):
        score -= (train_tpd - max(3.0, float(target_trades_per_day) * 4.0)) * 0.5
    return float(score)


def _pick_window(args: argparse.Namespace) -> Tuple[pd.Timestamp, pd.Timestamp, pd.Timestamp, pd.Timestamp, pd.Timestamp]:
    server_now = binance_fapi_server_time_utc()
    if args.end:
        end = pd.to_datetime(args.end, utc=True)
    elif server_now is not None:
        end = pd.to_datetime(server_now, utc=True).floor("min")
    else:
        end = pd.Timestamp.now(tz="UTC").floor("min")
    lookback_days = max(1, int(args.lookback_days))
    validation_days = max(0.0, float(args.validation_days))
    start = end - pd.Timedelta(days=lookback_days)
    validation_start = end - pd.Timedelta(days=validation_days)
    if validation_start <= start:
        validation_start = start + pd.Timedelta(days=max(1.0, lookback_days * 0.7))
    train_start = start
    train_end = validation_start
    return train_start, train_end, validation_start, end, pd.to_datetime(server_now, utc=True) if server_now is not None else end


def run(args: argparse.Namespace) -> int:
    warnings.filterwarnings("ignore", category=FutureWarning)
    random.seed(int(args.seed))

    symbol = str(args.symbol or "BTCUSDT").upper().strip()
    cache_dir = Path(args.cache_dir).resolve()
    train_start, train_end, validation_start, validation_end, server_now = _pick_window(args)
    full_start = train_start
    full_end = validation_end

    out_dir = Path(args.output_dir) if args.output_dir else REPO_ROOT / "runs" / f"auto_strategy_search_{symbol.lower()}_{pd.Timestamp.now(tz='UTC').strftime('%Y%m%d_%H%M%S')}"
    if not out_dir.is_absolute():
        out_dir = (REPO_ROOT / out_dir).resolve()
    out_dir.mkdir(parents=True, exist_ok=True)

    candidates = _candidate_universe(args.scope)
    generated_count = len(candidates)
    if int(args.max_candidates or 0) > 0 and len(candidates) > int(args.max_candidates):
        candidates = random.sample(candidates, int(args.max_candidates))
        candidates.sort(key=lambda c: c.name)

    profiles = _exit_profiles()
    if int(args.max_exit_profiles or 0) > 0:
        profiles = profiles[: int(args.max_exit_profiles)]

    if args.dry_run:
        payload = {
            "generated_candidates": generated_count,
            "selected_candidates": len(candidates),
            "exit_profiles": [p.name for p in profiles],
            "sample_candidates": [c.name for c in candidates[:50]],
        }
        (out_dir / "dry_run.json").write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")
        print(json.dumps(payload, indent=2, ensure_ascii=False), flush=True)
        return 0

    timeframes = ["1m", "5m", "15m", "1h"]
    min_train_trades = int(args.min_train_trades or max(3, math.floor(_days_between(train_start, train_end) * 0.35)))
    min_validation_trades = int(args.min_validation_trades or (1 if _days_between(validation_start, validation_end) >= 0.5 else 0))

    manifest = {
        "symbol": symbol,
        "server_now_utc": str(server_now),
        "train_start": str(train_start),
        "train_end": str(train_end),
        "validation_start": str(validation_start),
        "validation_end": str(validation_end),
        "objective": {
            "primary": "positive validation net profit with positive train net profit",
            "secondary": f"train trades/day close to {float(args.target_trades_per_day):g}",
            "min_train_trades": min_train_trades,
            "min_validation_trades": min_validation_trades,
            "score_formula": "0.75*train_pnl + 2.25*validation_pnl + PF/WR bonuses - cadence/drawdown/negative-OOS penalties",
        },
        "generated_candidate_count": generated_count,
        "selected_candidate_count": len(candidates),
        "scope": str(args.scope or "full"),
        "exit_profiles": [p.name for p in profiles],
        "seed": int(args.seed),
    }
    (out_dir / "search_manifest.json").write_text(json.dumps(_json_safe(manifest), indent=2, ensure_ascii=False), encoding="utf-8")

    print(f"symbol={symbol}", flush=True)
    print(f"train={train_start} -> {train_end}", flush=True)
    print(f"validation={validation_start} -> {validation_end}", flush=True)
    print(f"generated_candidates={generated_count} selected_candidates={len(candidates)} profiles={len(profiles)}", flush=True)
    print(f"objective target_tpd={float(args.target_trades_per_day):g} min_train_trades={min_train_trades} min_validation_trades={min_validation_trades}", flush=True)
    print(f"output={out_dir}", flush=True)

    leaderboard: List[Dict[str, Any]] = []
    best_payload: Optional[Dict[str, Any]] = None
    best_score = -1e18
    best_trades: Dict[str, List[Dict[str, Any]]] = {}

    groups: Dict[Tuple[int, int], List[StrategyCandidate]] = {}
    for candidate in candidates:
        groups.setdefault((candidate.ema_fast, candidate.ema_slow), []).append(candidate)

    for (ema_fast, ema_slow), group_candidates in sorted(groups.items()):
        print(f"\n=== EMA {ema_fast}/{ema_slow}: {len(group_candidates)} candidates ===", flush=True)
        df_1m, streams, _req_spec, warmup = _prepare_streams(
            candidates=group_candidates,
            profiles=profiles,
            symbol=symbol,
            start=full_start,
            end=full_end,
            timeframes=timeframes,
            cache_dir=cache_dir,
            ema_fast=ema_fast,
            ema_slow=ema_slow,
        )
        streams_eval = make_streams_htf_closed_only(streams)
        print(f"warmup_minutes={warmup} candles={len(df_1m)}", flush=True)

        for ci, candidate in enumerate(group_candidates, start=1):
            for profile in profiles:
                cfg = _cfg(profile, order_notional=args.order_notional, fee_pct=args.fee_pct, slippage_bps=args.slippage_bps)
                try:
                    train_res = run_backtest_auto(
                        symbol=symbol,
                        streams_full=streams_eval,
                        start=train_start,
                        end=train_end,
                        rules_model=candidate.rules_model,
                        tab_group_join_mode=candidate.tab_group_join_mode,
                        group_rule_join_mode=candidate.group_rule_join_mode,
                        cfg=cfg,
                        df_1m_full=df_1m,
                        entry_filters=candidate.entry_filters,
                        apply_htf_closed_only=False,
                    )
                    val_res = run_backtest_auto(
                        symbol=symbol,
                        streams_full=streams_eval,
                        start=validation_start,
                        end=validation_end,
                        rules_model=candidate.rules_model,
                        tab_group_join_mode=candidate.tab_group_join_mode,
                        group_rule_join_mode=candidate.group_rule_join_mode,
                        cfg=cfg,
                        df_1m_full=df_1m,
                        entry_filters=candidate.entry_filters,
                        apply_htf_closed_only=False,
                    )
                except Exception as exc:
                    leaderboard.append({
                        "candidate": candidate.name,
                        "exit": profile.name,
                        "error": str(exc),
                        "score": -1e18,
                    })
                    continue

                train_row = _summary_row(candidate.name, profile.name, "train", train_res.summary, train_res.trades)
                val_row = _summary_row(candidate.name, profile.name, "validation", val_res.summary, val_res.trades)
                score = _search_score(
                    train_row,
                    val_row,
                    target_trades_per_day=float(args.target_trades_per_day),
                    min_train_trades=min_train_trades,
                    min_validation_trades=min_validation_trades,
                )
                row = {
                    "candidate": candidate.name,
                    "exit": profile.name,
                    "score": score,
                    "thesis": candidate.thesis,
                    "ema_fast": candidate.ema_fast,
                    "ema_slow": candidate.ema_slow,
                    "train": train_row,
                    "validation": val_row,
                }
                leaderboard.append(row)
                if score > best_score:
                    best_score = score
                    best_payload = {
                        "candidate": candidate,
                        "profile": profile,
                        "row": row,
                        "preset": _candidate_preset(candidate, profile, symbol=symbol, start=validation_start, end=validation_end),
                    }
                    best_trades = {
                        "train": [
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
                            for t in train_res.trades[:100]
                        ],
                        "validation": [
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
                            for t in val_res.trades[:100]
                        ],
                    }
            if ci % 100 == 0 or ci == len(group_candidates):
                print(f"tested {ci}/{len(group_candidates)} candidates for EMA {ema_fast}/{ema_slow}", flush=True)

    leaderboard.sort(key=lambda r: float(r.get("score", -1e18) or -1e18), reverse=True)
    payload = {
        **manifest,
        "leaderboard": leaderboard,
        "best_trades": best_trades,
    }
    (out_dir / "leaderboard.json").write_text(json.dumps(_json_safe(payload), indent=2, ensure_ascii=False), encoding="utf-8")
    try:
        flat_rows = []
        for row in leaderboard:
            if "train" not in row:
                flat_rows.append(row)
                continue
            flat_rows.append({
                "candidate": row["candidate"],
                "exit": row["exit"],
                "score": row["score"],
                "train_net_profit": row["train"]["net_profit"],
                "train_trades": row["train"]["trades"],
                "train_trades_per_day": row["train"]["trades_per_day"],
                "train_win_rate_pct": row["train"]["win_rate_pct"],
                "train_profit_factor": row["train"]["profit_factor"],
                "validation_net_profit": row["validation"]["net_profit"],
                "validation_trades": row["validation"]["trades"],
                "validation_win_rate_pct": row["validation"]["win_rate_pct"],
                "validation_profit_factor": row["validation"]["profit_factor"],
            })
        pd.DataFrame(flat_rows).to_csv(out_dir / "leaderboard.csv", index=False)
    except Exception:
        pass

    if best_payload is not None:
        best = {
            "best": _json_safe(best_payload["row"]),
            "preset": _json_safe(best_payload["preset"]),
            "trades": _json_safe(best_trades),
        }
        (out_dir / "best_preset.json").write_text(json.dumps(best, indent=2, ensure_ascii=False), encoding="utf-8")
        preset_bundle = {
            "meta": {"autoload_last": False, "last_preset": "BTC_1M_AUTO_SEARCH_BEST"},
            "presets": {"BTC_1M_AUTO_SEARCH_BEST": _json_safe(best_payload["preset"])},
        }
        (out_dir / "presets.json").write_text(json.dumps(preset_bundle, indent=2, ensure_ascii=False), encoding="utf-8")

    print("\nTop 20", flush=True)
    for row in leaderboard[:20]:
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
    parser = argparse.ArgumentParser(description="Autonomous grammar search for BTCUSDT 1m strategies.")
    parser.add_argument("--symbol", default="BTCUSDT")
    parser.add_argument("--cache-dir", default=str(REPO_ROOT / "data_cache"))
    parser.add_argument("--end", default=None)
    parser.add_argument("--lookback-days", type=int, default=7)
    parser.add_argument("--validation-days", type=float, default=2.0)
    parser.add_argument("--order-notional", type=float, default=100.0)
    parser.add_argument("--fee-pct", type=float, default=0.04)
    parser.add_argument("--slippage-bps", type=float, default=1.0)
    parser.add_argument("--target-trades-per-day", type=float, default=1.0)
    parser.add_argument("--min-train-trades", type=int, default=0)
    parser.add_argument("--min-validation-trades", type=int, default=0)
    parser.add_argument("--max-candidates", type=int, default=1200)
    parser.add_argument("--max-exit-profiles", type=int, default=6)
    parser.add_argument("--scope", choices=["fast", "full"], default="fast")
    parser.add_argument("--seed", type=int, default=7)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--output-dir", default=None)
    return run(parser.parse_args())


if __name__ == "__main__":
    raise SystemExit(main())
