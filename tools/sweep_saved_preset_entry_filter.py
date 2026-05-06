from __future__ import annotations

import argparse
import json
import math
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
from app.rules import EntryFilterConfig
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


def _parse_float_list(raw: str) -> List[float]:
    values: List[float] = []
    for chunk in str(raw or "").split(","):
        text = chunk.strip()
        if text:
            values.append(float(text))
    if not values:
        raise ValueError("Expected at least one numeric value.")
    return sorted({round(v, 8) for v in values})


def _parse_int_list(raw: str) -> List[int]:
    values: List[int] = []
    for chunk in str(raw or "").split(","):
        text = chunk.strip()
        if text:
            values.append(int(text))
    if not values:
        raise ValueError("Expected at least one integer value.")
    return sorted(set(values))


def _make_filter(enabled: bool, expansion: float, hard_skip: float, confirm_bars: int, retrace_pct: float, require_mid: bool, apply_to_reversals: bool) -> EntryFilterConfig:
    return EntryFilterConfig(
        enabled=bool(enabled),
        mode="ANTI_CHASE" if enabled else "OFF",
        expansion_atr_mult=float(expansion),
        hard_skip_atr_mult=float(hard_skip),
        confirm_bars=int(confirm_bars),
        max_retrace_pct=float(retrace_pct),
        require_next_close_beyond_mid=bool(require_mid),
        require_setup_still_valid=True,
        apply_to_reversals=bool(apply_to_reversals),
    ).normalized_copy()


def _result_row(res: Any, *, label: str, entry_filter: EntryFilterConfig) -> Dict[str, Any]:
    summary = dict(getattr(res, "summary", {}) or {})
    trades_count = summary.get("num_trades", summary.get("trades", len(getattr(res, "trades", []) or [])))
    return {
        "variant": str(label),
        "enabled": bool(entry_filter.enabled),
        "mode": str(entry_filter.mode),
        "expansion_atr_mult": float(entry_filter.expansion_atr_mult),
        "hard_skip_atr_mult": float(entry_filter.hard_skip_atr_mult),
        "confirm_bars": int(entry_filter.confirm_bars),
        "max_retrace_pct": float(entry_filter.max_retrace_pct),
        "require_next_close_beyond_mid": bool(entry_filter.require_next_close_beyond_mid),
        "apply_to_reversals": bool(entry_filter.apply_to_reversals),
        "ending_balance": float(summary.get("ending_balance", float("nan"))),
        "return_pct": float(summary.get("return_pct", float("nan"))),
        "num_trades": int(trades_count or 0),
        "win_rate_pct": float(summary.get("win_rate_pct", float("nan"))),
        "profit_factor": float(summary.get("profit_factor", float("nan"))),
        "max_drawdown": float(summary.get("max_drawdown", float("nan"))),
        "max_drawdown_pct": float(summary.get("max_drawdown_pct", float("nan"))),
        "strategy_plan_engine": str(summary.get("strategy_plan_engine", "")),
        "filter_created": int((((summary.get("entry_filter") or {}).get("stats") or {}).get("created", 0)) or 0),
        "filter_confirmed": int((((summary.get("entry_filter") or {}).get("stats") or {}).get("confirmed", 0)) or 0),
        "filter_blocked": int((((summary.get("entry_filter") or {}).get("stats") or {}).get("blocked", 0)) or 0),
        "filter_hard_skipped": int((((summary.get("entry_filter") or {}).get("stats") or {}).get("hard_skipped", 0)) or 0),
        "filter_cancelled": int((((summary.get("entry_filter") or {}).get("stats") or {}).get("cancelled", 0)) or 0),
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

    parser = argparse.ArgumentParser(description="Sweep anti-chase entry-filter variants on a saved preset.")
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
    parser.add_argument("--expansion-values", default="1.5,2.0,2.5")
    parser.add_argument("--hard-skip-values", default="2.5,3.0,3.5")
    parser.add_argument("--confirm-bars-values", default="1,2")
    parser.add_argument("--retrace-values", default="25,35,50")
    parser.add_argument("--objective", default="ending_balance", choices=["ending_balance", "return_pct", "profit_factor"])
    parser.add_argument("--top-n", type=int, default=10)
    parser.add_argument("--output-dir", default=None)
    parser.add_argument("--show-server-time", action="store_true", default=False)
    args = parser.parse_args()

    presets_file = Path(args.presets_file).resolve()
    payload = _load_presets(presets_file)
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

    rules_model, tab_group_join_mode, group_rule_join_mode, _entry_filters = _materialize_preset(preset)

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

    expansions = _parse_float_list(args.expansion_values)
    hard_skips = _parse_float_list(args.hard_skip_values)
    confirm_bars_values = _parse_int_list(args.confirm_bars_values)
    retraces = _parse_float_list(args.retrace_values)

    variants: List[Tuple[str, Dict[str, EntryFilterConfig]]] = [("off", {"Long Entry": EntryFilterConfig(), "Short Entry": EntryFilterConfig()})]
    for expansion in expansions:
        for hard_skip in hard_skips:
            if hard_skip < expansion:
                continue
            for confirm_bars in confirm_bars_values:
                for retrace in retraces:
                    label = f"anti_chase_exp{expansion:g}_hard{hard_skip:g}_wait{confirm_bars}_ret{retrace:g}"
                    cfg_filter = _make_filter(True, expansion, hard_skip, confirm_bars, retrace, True, True)
                    variants.append((label, {"Long Entry": cfg_filter, "Short Entry": cfg_filter}))

    req_spec = compile_stream_requirements(
        rules_model,
        entry_filters=variants[-1][1],
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
    print(f"Entry-filter variants: {len(variants)}", flush=True)
    print(
        "Fixed exits: "
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
    results_by_label: Dict[str, Any] = {}
    filters_by_label: Dict[str, Dict[str, EntryFilterConfig]] = {}
    total = len(variants)
    for index, (label, entry_filters) in enumerate(variants, start=1):
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
        rows.append(_result_row(res, label=label, entry_filter=entry_filters["Long Entry"]))
        results_by_label[label] = res
        filters_by_label[label] = entry_filters
        if index % 5 == 0 or index == total:
            print(f"Variant progress: {index}/{total}", flush=True)

    rows_sorted = sorted(rows, key=lambda row: _sort_key(row, args.objective), reverse=True)
    top_rows = rows_sorted[: max(1, int(args.top_n))]
    best_row = rows_sorted[0]
    best_label = str(best_row["variant"])
    best_result = results_by_label[best_label]
    best_filters = filters_by_label[best_label]

    event_activity = _event_activity_summary(rules_model, streams_full, start, end)
    signal_rows = _event_signal_rows(rules_model, streams_full, start, end)
    bundle_dir = _write_verification_bundle(
        repo_root=repo_root,
        preset_name=f"{preset_name}_{best_label}",
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

    runs_dir = Path(args.output_dir).resolve() if args.output_dir else (repo_root / "runs" / f"{_slug(preset_name)}_entry_filter_sweep_{pd.Timestamp.now(tz='UTC').strftime('%Y%m%d_%H%M%S')}")
    runs_dir.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(rows_sorted).to_csv(runs_dir / "ranked_results.csv", index=False)
    summary_payload = {
        "preset_name": preset_name,
        "symbol": symbol,
        "start": str(start),
        "end": str(end),
        "objective": str(args.objective),
        "stop_loss_value": float(args.stop_loss_value),
        "stop_loss_mode": str(args.stop_loss_mode).upper(),
        "stop_loss_timeframe": str(args.stop_loss_timeframe),
        "stop_loss_field": str(args.stop_loss_field),
        "stop_loss_offset_pct": float(args.stop_loss_offset_pct),
        "take_profit_value": float(args.take_profit_value),
        "take_profit_mode": str(args.take_profit_mode).upper(),
        "take_profit_timeframe": str(args.take_profit_timeframe),
        "take_profit_field": str(args.take_profit_field),
        "take_profit_offset_pct": float(args.take_profit_offset_pct),
        "top_results": top_rows,
        "best_result": best_row,
        "best_verification_bundle": str(bundle_dir),
    }
    (runs_dir / "summary.json").write_text(json.dumps(summary_payload, indent=2, ensure_ascii=False, default=str), encoding="utf-8")

    print("", flush=True)
    print("Top entry-filter variants", flush=True)
    for idx, row in enumerate(top_rows, start=1):
        print(
            f"{idx}. {row['variant']} "
            f"ending_balance={_safe_round(row['ending_balance'], 4)} "
            f"return_pct={_safe_round(row['return_pct'], 4)} "
            f"profit_factor={_safe_round(row['profit_factor'], 4)} "
            f"max_drawdown_pct={_safe_round(row['max_drawdown_pct'], 4)} "
            f"trades={row['num_trades']} "
            f"created={row['filter_created']} confirmed={row['filter_confirmed']} blocked={row['filter_blocked']}",
            flush=True,
        )

    print(f"Saved ranked results: {runs_dir / 'ranked_results.csv'}", flush=True)
    print(f"Saved sweep summary: {runs_dir / 'summary.json'}", flush=True)
    print(f"Best verification bundle: {bundle_dir}", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
