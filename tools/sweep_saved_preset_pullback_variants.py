from __future__ import annotations

import argparse
import json
import math
from copy import deepcopy
from pathlib import Path
import sys
from typing import Any, Dict, List, Tuple

import pandas as pd

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from app.backtest import make_streams_htf_closed_only, run_backtest_tick
from app.backtest_models import BacktestConfig
from app.constants import BACKTEST_MODE_BAR_1M_HTF_CLOSED_ONLY, BACKTEST_MODE_TICK
from app.data_binance import binance_fapi_server_time_utc
from app.fast_backtest import run_backtest_auto
from app.rules import EntryFilterConfig, Rule
from app.stream_bundle_loader import load_stream_bundle_library_first
from app.strategy_requirements import compile_stream_requirements
from app.utils_time import parse_utc, required_warmup_minutes
from tools.run_saved_preset import (
    _event_activity_summary,
    _event_signal_rows,
    _load_presets,
    _materialize_preset,
    _pick_preset_name,
    _resolve_cache_dir,
    _safe_round,
    _slug,
    _write_verification_bundle,
)


def _progress(msg: str, pct: Any = None) -> None:
    if pct is None:
        print(msg, flush=True)
        return
    print(f"[{pct}%] {msg}", flush=True)


def _rule(mode: str, tf: str, field: str, op: str, value: Any) -> Rule:
    return Rule(timeframe=str(tf), mode=str(mode), field=str(field), op=str(op), value=value)


def _clone_rules_model(rules_model: Dict[str, Any]) -> Dict[str, Any]:
    return deepcopy(rules_model)


def _clone_entry_filters(entry_filters: Dict[str, EntryFilterConfig]) -> Dict[str, EntryFilterConfig]:
    out: Dict[str, EntryFilterConfig] = {}
    for key, cfg in (entry_filters or {}).items():
        out[str(key)] = cfg.normalized_copy() if isinstance(cfg, EntryFilterConfig) else EntryFilterConfig.from_dict(getattr(cfg, "to_dict", lambda: {})())
    return out


def _append_entry_rules(rules_model: Dict[str, Any], long_rules: List[Rule], short_rules: List[Rule]) -> Dict[str, Any]:
    out = _clone_rules_model(rules_model)
    for tab_name, extra_rules in (("Long Entry", long_rules), ("Short Entry", short_rules)):
        if not extra_rules:
            continue
        groups = out.get(tab_name)
        if not isinstance(groups, list) or not groups:
            groups = [[]]
            out[tab_name] = groups
        first_group = groups[0]
        if not isinstance(first_group, list):
            first_group = []
            groups[0] = first_group
        first_group.extend(extra_rules)
    return out


def _baseline_entry_filters() -> Dict[str, EntryFilterConfig]:
    cfg = EntryFilterConfig(
        enabled=True,
        mode="ANTI_CHASE",
        expansion_atr_mult=1.5,
        hard_skip_atr_mult=2.5,
        confirm_bars=1,
        max_retrace_pct=25.0,
        require_next_close_beyond_mid=True,
        require_setup_still_valid=True,
        apply_to_reversals=True,
    ).normalized_copy()
    return {"Long Entry": cfg, "Short Entry": cfg}


def _variant_library() -> List[Tuple[str, List[Rule], List[Rule]]]:
    return [
        ("baseline", [], []),
        ("market_bias_align_1h", [_rule("state", "1h", "market_bias", "=", "LONG")], [_rule("state", "1h", "market_bias", "=", "SHORT")]),
        ("market_phase_pullback_1h", [_rule("state", "1h", "market_phase", "=", "PULLBACK")], [_rule("state", "1h", "market_phase", "=", "PULLBACK")]),
        ("market_state_pullback_1h", [_rule("state", "1h", "market_state", "=", "BULL_PULLBACK")], [_rule("state", "1h", "market_state", "=", "BEAR_PULLBACK")]),
        ("range_phase_weak_1h", [_rule("state", "1h", "range_filter_phase", "=", "BUY_WEAK")], [_rule("state", "1h", "range_filter_phase", "=", "SELL_WEAK")]),
        ("vidya_align_1h", [_rule("state", "1h", "vidya_state", "=", "BUY")], [_rule("state", "1h", "vidya_state", "=", "SELL")]),
        ("frama_align_1h", [_rule("state", "1h", "frama_state", "=", "BUY")], [_rule("state", "1h", "frama_state", "=", "SELL")]),
        ("adx25_di_align_1h", [_rule("state", "1h", "adx", ">=", 25.0), _rule("state", "1h", "di_spread", ">=", 0.0)], [_rule("state", "1h", "adx", ">=", 25.0), _rule("state", "1h", "di_spread", "<=", 0.0)]),
        ("market_pullback_bias_1h", [_rule("state", "1h", "market_bias", "=", "LONG"), _rule("state", "1h", "market_phase", "=", "PULLBACK")], [_rule("state", "1h", "market_bias", "=", "SHORT"), _rule("state", "1h", "market_phase", "=", "PULLBACK")]),
        ("market_pullback_plus_vidya_1h", [_rule("state", "1h", "market_phase", "=", "PULLBACK"), _rule("state", "1h", "vidya_state", "=", "BUY")], [_rule("state", "1h", "market_phase", "=", "PULLBACK"), _rule("state", "1h", "vidya_state", "=", "SELL")]),
        ("market_pullback_plus_frama_1h", [_rule("state", "1h", "market_phase", "=", "PULLBACK"), _rule("state", "1h", "frama_state", "=", "BUY")], [_rule("state", "1h", "market_phase", "=", "PULLBACK"), _rule("state", "1h", "frama_state", "=", "SELL")]),
        ("market_pullback_plus_adx25_1h", [_rule("state", "1h", "market_phase", "=", "PULLBACK"), _rule("state", "1h", "adx", ">=", 25.0)], [_rule("state", "1h", "market_phase", "=", "PULLBACK"), _rule("state", "1h", "adx", ">=", 25.0)]),
        ("market_pullback_plus_adx20_1h", [_rule("state", "1h", "market_phase", "=", "PULLBACK"), _rule("state", "1h", "adx", ">=", 20.0)], [_rule("state", "1h", "market_phase", "=", "PULLBACK"), _rule("state", "1h", "adx", ">=", 20.0)]),
        ("market_pullback_plus_adx30_1h", [_rule("state", "1h", "market_phase", "=", "PULLBACK"), _rule("state", "1h", "adx", ">=", 30.0)], [_rule("state", "1h", "market_phase", "=", "PULLBACK"), _rule("state", "1h", "adx", ">=", 30.0)]),
        ("market_pullback_plus_adx35_1h", [_rule("state", "1h", "market_phase", "=", "PULLBACK"), _rule("state", "1h", "adx", ">=", 35.0)], [_rule("state", "1h", "market_phase", "=", "PULLBACK"), _rule("state", "1h", "adx", ">=", 35.0)]),
        ("market_pullback_bias_adx25_1h", [_rule("state", "1h", "market_bias", "=", "LONG"), _rule("state", "1h", "market_phase", "=", "PULLBACK"), _rule("state", "1h", "adx", ">=", 25.0)], [_rule("state", "1h", "market_bias", "=", "SHORT"), _rule("state", "1h", "market_phase", "=", "PULLBACK"), _rule("state", "1h", "adx", ">=", 25.0)]),
        ("market_pullback_bias_adx30_1h", [_rule("state", "1h", "market_bias", "=", "LONG"), _rule("state", "1h", "market_phase", "=", "PULLBACK"), _rule("state", "1h", "adx", ">=", 30.0)], [_rule("state", "1h", "market_bias", "=", "SHORT"), _rule("state", "1h", "market_phase", "=", "PULLBACK"), _rule("state", "1h", "adx", ">=", 30.0)]),
        ("market_pullback_adx25_conf55_1h", [_rule("state", "1h", "market_phase", "=", "PULLBACK"), _rule("state", "1h", "adx", ">=", 25.0), _rule("state", "1h", "market_confidence", ">=", 55.0)], [_rule("state", "1h", "market_phase", "=", "PULLBACK"), _rule("state", "1h", "adx", ">=", 25.0), _rule("state", "1h", "market_confidence", ">=", 55.0)]),
        ("market_pullback_adx25_conf65_1h", [_rule("state", "1h", "market_phase", "=", "PULLBACK"), _rule("state", "1h", "adx", ">=", 25.0), _rule("state", "1h", "market_confidence", ">=", 65.0)], [_rule("state", "1h", "market_phase", "=", "PULLBACK"), _rule("state", "1h", "adx", ">=", 25.0), _rule("state", "1h", "market_confidence", ">=", 65.0)]),
        ("market_pullback_adx25_age12_1h", [_rule("state", "1h", "market_phase", "=", "PULLBACK"), _rule("state", "1h", "adx", ">=", 25.0), _rule("state", "1h", "market_state_age", "<=", 12.0)], [_rule("state", "1h", "market_phase", "=", "PULLBACK"), _rule("state", "1h", "adx", ">=", 25.0), _rule("state", "1h", "market_state_age", "<=", 12.0)]),
    ]


def _parse_variant_names(raw: str) -> List[str]:
    names: List[str] = []
    for chunk in str(raw or "").split(","):
        text = str(chunk or "").strip()
        if text:
            names.append(text)
    return names


def _result_row(res: Any, *, variant: str) -> Dict[str, Any]:
    summary = dict(getattr(res, "summary", {}) or {})
    trades_count = summary.get("num_trades", summary.get("trades", len(getattr(res, "trades", []) or [])))
    ef_stats = ((summary.get("entry_filter") or {}).get("stats") or {})
    return {
        "variant": str(variant),
        "ending_balance": float(summary.get("ending_balance", float("nan"))),
        "return_pct": float(summary.get("return_pct", float("nan"))),
        "num_trades": int(trades_count or 0),
        "win_rate_pct": float(summary.get("win_rate_pct", float("nan"))),
        "profit_factor": float(summary.get("profit_factor", float("nan"))),
        "max_drawdown": float(summary.get("max_drawdown", float("nan"))),
        "max_drawdown_pct": float(summary.get("max_drawdown_pct", float("nan"))),
        "strategy_plan_engine": str(summary.get("strategy_plan_engine", "")),
        "stop_loss_count": int(summary.get("stop_loss_count", 0) or 0),
        "take_profit_count": int(summary.get("take_profit_count", 0) or 0),
        "filter_created": int(ef_stats.get("created", 0) or 0),
        "filter_confirmed": int(ef_stats.get("confirmed", 0) or 0),
        "filter_blocked": int(ef_stats.get("blocked", 0) or 0),
    }


def _sort_key(row: Dict[str, Any], objective: str) -> Tuple[Any, ...]:
    primary = float(row.get(objective, float("-inf")))
    if not math.isfinite(primary):
        primary = float("-inf")
    drawdown = float(row.get("max_drawdown_pct", float("inf")))
    if not math.isfinite(drawdown):
        drawdown = float("inf")
    profit_factor = float(row.get("profit_factor", float("-inf")))
    if not math.isfinite(profit_factor):
        profit_factor = float("-inf")
    trades = int(row.get("num_trades", 0) or 0)
    return (primary, profit_factor, -drawdown, trades)


def main() -> int:
    repo_root = REPO_ROOT
    default_presets = repo_root / "app" / "presets.json"

    parser = argparse.ArgumentParser(description="Sweep pullback-style rule variants on top of a saved preset.")
    parser.add_argument("--preset", dest="preset_name", default=None)
    parser.add_argument("--presets-file", dest="presets_file", default=str(default_presets))
    parser.add_argument("--cache-dir", dest="cache_dir", default=None)
    parser.add_argument("--start", dest="start_override", default=None)
    parser.add_argument("--end", dest="end_override", default=None)
    parser.add_argument("--initial-balance", type=float, default=1000.0)
    parser.add_argument("--order-notional", type=float, default=100.0)
    parser.add_argument("--fee-pct", type=float, default=0.04)
    parser.add_argument("--slippage-bps", type=float, default=0.0)
    parser.add_argument("--stop-loss-mode", default="PERCENT")
    parser.add_argument("--stop-loss-value", type=float, default=4.0)
    parser.add_argument("--stop-loss-timeframe", default="1m")
    parser.add_argument("--stop-loss-field", default="")
    parser.add_argument("--stop-loss-offset-pct", type=float, default=0.0)
    parser.add_argument("--take-profit-mode", default="PERCENT")
    parser.add_argument("--take-profit-value", type=float, default=6.0)
    parser.add_argument("--take-profit-timeframe", default="1m")
    parser.add_argument("--take-profit-field", default="")
    parser.add_argument("--take-profit-offset-pct", type=float, default=0.0)
    parser.add_argument("--allow-reverse", dest="allow_reverse", action="store_true", default=True)
    parser.add_argument("--no-allow-reverse", dest="allow_reverse", action="store_false")
    parser.add_argument("--skip-both-entries", dest="skip_both_entries", action="store_true", default=True)
    parser.add_argument("--no-skip-both-entries", dest="skip_both_entries", action="store_false")
    parser.add_argument("--step-timeframe", default="1m")
    parser.add_argument("--capture-trade-details", action="store_true", default=False)
    parser.add_argument("--equity-curve-stride", type=int, default=15)
    parser.add_argument("--objective", default="ending_balance", choices=["ending_balance", "return_pct", "profit_factor"])
    parser.add_argument("--top-n", type=int, default=10)
    parser.add_argument("--output-dir", default=None)
    parser.add_argument("--show-server-time", action="store_true", default=False)
    parser.add_argument("--variant-names", default=None, help="Optional comma-separated subset of variant names to evaluate.")
    args = parser.parse_args()

    payload = _load_presets(Path(args.presets_file).resolve())
    preset_name = _pick_preset_name(payload, args.preset_name)
    preset = payload["presets"][preset_name]
    settings = preset.get("settings", {})
    if not isinstance(settings, dict):
        raise ValueError("Preset settings payload is invalid.")

    symbol = str(settings.get("symbol", "BTCUSDT") or "BTCUSDT").strip().upper()
    start = parse_utc(str(args.start_override or settings.get("start", "") or ""))
    end = parse_utc(str(args.end_override or settings.get("end", "") or ""))
    if end <= start:
        raise ValueError("Preset end must be after start.")

    tfs = [tf for tf, enabled in (settings.get("timeframes", {}) or {}).items() if bool(enabled)]
    if "1m" not in tfs:
        tfs = ["1m"] + tfs
    if args.step_timeframe not in tfs:
        tfs.append(args.step_timeframe)

    base_rules_model, tab_group_join_mode, group_rule_join_mode, _ = _materialize_preset(preset)
    baseline_entry_filters = _baseline_entry_filters()

    cfg = BacktestConfig(
        initial_balance=float(args.initial_balance),
        order_notional_usdt=float(args.order_notional),
        fee_rate=float(args.fee_pct) / 100.0,
        slippage_bps=float(args.slippage_bps),
        stop_loss_pct=(float(args.stop_loss_value) if str(args.stop_loss_mode).upper() == "PERCENT" else 0.0),
        take_profit_pct=(float(args.take_profit_value) if str(args.take_profit_mode).upper() == "PERCENT" else 0.0),
        stop_loss_mode=str(args.stop_loss_mode).upper(),
        stop_loss_value=float(args.stop_loss_value),
        stop_loss_timeframe=str(args.stop_loss_timeframe),
        stop_loss_field=str(args.stop_loss_field),
        stop_loss_offset_pct=float(args.stop_loss_offset_pct),
        take_profit_mode=str(args.take_profit_mode).upper(),
        take_profit_value=float(args.take_profit_value),
        take_profit_timeframe=str(args.take_profit_timeframe),
        take_profit_field=str(args.take_profit_field),
        take_profit_offset_pct=float(args.take_profit_offset_pct),
        allow_reverse=bool(args.allow_reverse),
        skip_if_both_entries=bool(args.skip_both_entries),
        step_timeframe=str(args.step_timeframe),
        capture_trade_details=bool(args.capture_trade_details),
        equity_curve_stride=max(1, int(args.equity_curve_stride)),
    )

    variant_defs = _variant_library()
    if str(args.variant_names or "").strip():
        requested_names = _parse_variant_names(str(args.variant_names))
        if requested_names:
            allowed = set(requested_names)
            variant_defs = [item for item in variant_defs if str(item[0]) in allowed]
            if not variant_defs:
                raise ValueError("No pullback variants matched --variant-names.")
    req_spec = compile_stream_requirements(
        _append_entry_rules(base_rules_model, variant_defs[-1][1], variant_defs[-1][2]),
        entry_filters=baseline_entry_filters,
        backtest_cfg=cfg,
        include_plot_defaults=True,
    )

    preset_warmup = int(settings.get("warmup_min", 0) or 0)
    needed_warmup = required_warmup_minutes(tfs, required_fields=req_spec)
    query_warmup = max(0, int(preset_warmup))
    compute_warmup = max(query_warmup, int(needed_warmup or 0))
    warmup_start = start - pd.Timedelta(minutes=query_warmup)
    compute_start = start - pd.Timedelta(minutes=compute_warmup)

    cache_dir = _resolve_cache_dir(repo_root, args.cache_dir or str(settings.get("cache_dir", "data_cache") or "data_cache"))
    cache_dir.mkdir(parents=True, exist_ok=True)

    bt_mode = str(settings.get("bt_mode", "") or "")
    price_source = str(settings.get("price_source", "LAST") or "LAST").upper()
    macd_impl = str(settings.get("macd_impl", "TRADINGVIEW") or "TRADINGVIEW").upper()
    adx_impl = str(settings.get("adx_impl", "TRADINGVIEW") or "TRADINGVIEW").upper()

    print(f"Preset: {preset_name}", flush=True)
    print(f"Symbol: {symbol}", flush=True)
    print(f"Window UTC: {start} -> {end}", flush=True)
    print(f"Mode: {bt_mode}", flush=True)
    print(f"Cache dir: {cache_dir}", flush=True)
    print(
        f"Warmup minutes: preset={preset_warmup}, indicator_required={needed_warmup}, "
        f"query_used={query_warmup}, compute_used={compute_warmup}",
        flush=True,
    )
    print(f"Variants: {len(variant_defs)}", flush=True)
    print(
        "Fixed execution setup: "
        f"stop={cfg.stop_loss_mode}:{cfg.stop_loss_value}"
        + (f" tf={cfg.stop_loss_timeframe} field={cfg.stop_loss_field} off={cfg.stop_loss_offset_pct}%" if cfg.stop_loss_mode == "FIELD" else "")
        + " | "
        f"take={cfg.take_profit_mode}:{cfg.take_profit_value}"
        + (f" tf={cfg.take_profit_timeframe} field={cfg.take_profit_field} off={cfg.take_profit_offset_pct}%" if cfg.take_profit_mode == "FIELD" else ""),
        flush=True,
    )

    if bool(args.show_server_time):
        srv = binance_fapi_server_time_utc()
        if srv is not None:
            print(f"Binance server time (UTC): {srv}", flush=True)

    bundle = load_stream_bundle_library_first(
        symbol=symbol,
        start=warmup_start,
        end=end,
        compute_start=compute_start,
        timeframes=tfs,
        cache_dir=cache_dir,
        price_source=price_source,
        macd_impl=macd_impl,
        adx_impl=adx_impl,
        required_fields=req_spec,
        progress_cb=_progress,
    )
    df_1m = bundle.df_1m
    streams_full = bundle.streams_full

    streams_bt = streams_full
    if bt_mode == BACKTEST_MODE_BAR_1M_HTF_CLOSED_ONLY:
        _progress("Preparing HTF closed-only streams...", 0)
        streams_bt = make_streams_htf_closed_only(streams_full)

    rows: List[Dict[str, Any]] = []
    results_by_variant: Dict[str, Any] = {}
    rules_by_variant: Dict[str, Dict[str, Any]] = {}
    total = len(variant_defs)

    for index, (variant_name, long_rules, short_rules) in enumerate(variant_defs, start=1):
        rules_model = _append_entry_rules(base_rules_model, long_rules, short_rules)
        entry_filters = _clone_entry_filters(baseline_entry_filters)
        if bt_mode == BACKTEST_MODE_TICK:
            res = run_backtest_tick(
                symbol=symbol,
                df_1m_full=df_1m,
                start=start,
                end=end,
                tfs=tfs,
                rules_model=rules_model,
                tab_group_join_mode=tab_group_join_mode,
                group_rule_join_mode=group_rule_join_mode,
                entry_filters=entry_filters,
                cfg=cfg,
                cache_dir=str(cache_dir),
                progress_cb=None,
                streams_full=streams_full,
                macd_impl=macd_impl,
            )
        else:
            res = run_backtest_auto(
                symbol=symbol,
                streams_full=streams_bt,
                start=start,
                end=end,
                rules_model=rules_model,
                tab_group_join_mode=tab_group_join_mode,
                group_rule_join_mode=group_rule_join_mode,
                cfg=cfg,
                df_1m_full=df_1m,
                entry_filters=entry_filters,
                apply_htf_closed_only=False,
            )
        rows.append(_result_row(res, variant=variant_name))
        results_by_variant[variant_name] = res
        rules_by_variant[variant_name] = rules_model
        if index % 4 == 0 or index == total:
            print(f"Variant progress: {index}/{total}", flush=True)

    rows_sorted = sorted(rows, key=lambda row: _sort_key(row, args.objective), reverse=True)
    top_rows = rows_sorted[: max(1, int(args.top_n))]
    best_row = rows_sorted[0]
    best_variant = str(best_row["variant"])
    best_result = results_by_variant[best_variant]
    best_rules = rules_by_variant[best_variant]

    event_activity = _event_activity_summary(best_rules, streams_full, start, end)
    signal_rows = _event_signal_rows(best_rules, streams_full, start, end)
    bundle_dir = _write_verification_bundle(
        repo_root=repo_root,
        preset_name=f"{preset_name}_{best_variant}",
        symbol=symbol,
        start=start,
        end=end,
        bt_mode=bt_mode,
        cache_dir=cache_dir,
        cfg=cfg,
        summary=dict(best_result.summary or {}),
        event_activity=event_activity,
        signal_rows=signal_rows,
        res=best_result,
    )

    runs_dir = Path(args.output_dir).resolve() if args.output_dir else (repo_root / "runs" / f"{_slug(preset_name)}_pullback_sweep_{pd.Timestamp.now(tz='UTC').strftime('%Y%m%d_%H%M%S')}")
    runs_dir.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(rows_sorted).to_csv(runs_dir / "ranked_results.csv", index=False)

    summary_payload = {
        "preset_name": preset_name,
        "symbol": symbol,
        "start": str(start),
        "end": str(end),
        "objective": str(args.objective),
        "stop_loss_mode": str(args.stop_loss_mode).upper(),
        "stop_loss_value": float(args.stop_loss_value),
        "stop_loss_timeframe": str(args.stop_loss_timeframe),
        "stop_loss_field": str(args.stop_loss_field),
        "take_profit_mode": str(args.take_profit_mode).upper(),
        "take_profit_value": float(args.take_profit_value),
        "take_profit_timeframe": str(args.take_profit_timeframe),
        "take_profit_field": str(args.take_profit_field),
        "variant_names": [str(name) for name, _long_rules, _short_rules in variant_defs],
        "top_results": top_rows,
        "best_result": best_row,
        "best_verification_bundle": str(bundle_dir),
    }
    (runs_dir / "summary.json").write_text(json.dumps(summary_payload, indent=2, ensure_ascii=False, default=str), encoding="utf-8")

    print("", flush=True)
    print("Top pullback variants", flush=True)
    for idx, row in enumerate(top_rows, start=1):
        print(
            f"{idx}. {row['variant']} "
            f"ending_balance={_safe_round(row['ending_balance'], 4)} "
            f"return_pct={_safe_round(row['return_pct'], 4)} "
            f"profit_factor={_safe_round(row['profit_factor'], 4)} "
            f"max_drawdown_pct={_safe_round(row['max_drawdown_pct'], 4)} "
            f"trades={row['num_trades']} "
            f"filter_blocked={row['filter_blocked']}",
            flush=True,
        )

    print(f"Saved ranked results: {runs_dir / 'ranked_results.csv'}", flush=True)
    print(f"Saved sweep summary: {runs_dir / 'summary.json'}", flush=True)
    print(f"Best verification bundle: {bundle_dir}", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
