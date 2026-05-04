from __future__ import annotations

import argparse
from concurrent.futures import ProcessPoolExecutor, as_completed
import json
import math
import os
from pathlib import Path
import sys
import time
from typing import Any, Dict, List, Sequence, Tuple

import pandas as pd

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from app.backtest import run_backtest_tick
from app.backtest_models import BacktestConfig
from app.constants import BACKTEST_MODE_BAR_1M_HTF_CLOSED_ONLY, BACKTEST_MODE_TICK
from app.data_binance import binance_fapi_server_time_utc, fetch_klines_1m_futures
from app.fast_backtest import run_backtest_auto
from app.indicators_streaming import simulate_multitf_indicators
from app.prepared_dataset import (
    build_prepared_dataset,
    find_covering_prepared_dataset_on_disk,
    save_prepared_dataset_to_disk,
    slice_df_1m_window,
    slice_streams_window,
)
from app.research.replay import build_backtest_replay_plan, replay_backtest_result
from app.strategy_requirements import compile_stream_requirements
from app.utils_time import parse_utc, required_warmup_minutes
from tools.run_saved_preset import (
    _load_presets,
    _materialize_preset,
    _pick_preset_name,
    _resolve_cache_dir,
    _safe_round,
    _slug,
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


def _parse_windows(raw: str) -> List[Tuple[pd.Timestamp, pd.Timestamp]]:
    windows: List[Tuple[pd.Timestamp, pd.Timestamp]] = []
    for segment in str(raw or "").split(";"):
        text = segment.strip()
        if not text:
            continue
        try:
            start_txt, end_txt = text.split("|", 1)
        except ValueError as exc:
            raise ValueError(f"Invalid window segment '{text}'. Expected start|end.") from exc
        start = parse_utc(start_txt.strip())
        end = parse_utc(end_txt.strip())
        if end <= start:
            raise ValueError(f"Invalid window '{text}': end must be after start.")
        windows.append((start, end))
    if not windows:
        raise ValueError("Expected at least one window.")
    return windows


def _window_label(start: pd.Timestamp, end: pd.Timestamp) -> str:
    return f"{pd.Timestamp(start).strftime('%Y-%m-%d')}__{pd.Timestamp(end).strftime('%Y-%m-%d')}"


def _apply_exit_values(base_cfg: BacktestConfig, *, stop_value: float, take_value: float) -> BacktestConfig:
    next_cfg = BacktestConfig(**base_cfg.__dict__)
    next_cfg.stop_loss_mode = "PERCENT" if float(stop_value) > 0.0 else "OFF"
    next_cfg.stop_loss_value = float(stop_value)
    next_cfg.stop_loss_pct = float(stop_value) if float(stop_value) > 0.0 else 0.0
    next_cfg.stop_loss_timeframe = "1m"
    next_cfg.stop_loss_field = ""
    next_cfg.stop_loss_offset_pct = 0.0
    next_cfg.take_profit_mode = "PERCENT" if float(take_value) > 0.0 else "OFF"
    next_cfg.take_profit_value = float(take_value)
    next_cfg.take_profit_pct = float(take_value) if float(take_value) > 0.0 else 0.0
    next_cfg.take_profit_timeframe = "1m"
    next_cfg.take_profit_field = ""
    next_cfg.take_profit_offset_pct = 0.0
    return next_cfg


def _combo_key(stop_value: float, take_value: float) -> str:
    return f"sl_{stop_value:g}__tp_{take_value:g}"


def _sort_key(row: Dict[str, Any]) -> Tuple[Any, ...]:
    compounded = float(row.get("compounded_return_pct", float("-inf")))
    if not math.isfinite(compounded):
        compounded = float("-inf")
    positive_windows = int(row.get("positive_windows", 0) or 0)
    worst_window = float(row.get("worst_window_return_pct", float("-inf")))
    if not math.isfinite(worst_window):
        worst_window = float("-inf")
    avg_pf = float(row.get("avg_profit_factor", float("-inf")))
    if not math.isfinite(avg_pf):
        avg_pf = float("-inf")
    worst_dd = float(row.get("worst_window_max_drawdown_pct", float("inf")))
    if not math.isfinite(worst_dd):
        worst_dd = float("inf")
    return (compounded, positive_windows, worst_window, avg_pf, -worst_dd)


def _run_window_exit_sweep(task: Dict[str, Any]) -> Dict[str, Any]:
    start_t = time.perf_counter()
    symbol = str(task["symbol"])
    start = pd.to_datetime(task["start"], utc=True)
    end = pd.to_datetime(task["end"], utc=True)
    warmup = int(task["warmup"])
    tfs = list(task["tfs"])
    warmup_start = start - pd.Timedelta(minutes=warmup)
    cache_dir = Path(task["cache_dir"]).resolve()
    bt_mode = str(task["bt_mode"])
    price_source = str(task["price_source"])
    macd_impl = str(task["macd_impl"])
    adx_impl = str(task["adx_impl"])
    rules_model = dict(task["rules_model"])
    tab_group_join_mode = dict(task["tab_group_join_mode"])
    group_rule_join_mode = {k: list(v) for k, v in dict(task["group_rule_join_mode"]).items()}
    entry_filters = dict(task["entry_filters"])
    base_cfg = BacktestConfig(**dict(task["base_cfg"]))
    req_spec = task["req_spec"]
    stop_values = [float(v) for v in task["stop_values"]]
    take_values = [float(v) for v in task["take_values"]]
    combos = [(sl, tp) for sl in stop_values for tp in take_values]

    prepared_disk = find_covering_prepared_dataset_on_disk(
        str(cache_dir),
        symbol=symbol,
        start=warmup_start,
        end=end,
        timeframes=tfs,
        price_source=price_source,
        macd_impl=macd_impl,
        adx_impl=adx_impl,
        required_fields=req_spec,
        market_state_thresholds=None,
    )
    if prepared_disk is not None:
        df_1m = slice_df_1m_window(prepared_disk.df_1m_full, warmup_start, end)
        streams_full = slice_streams_window(prepared_disk.streams_full, warmup_start, end, timeframes=tfs)
    else:
        df_1m = fetch_klines_1m_futures(symbol, warmup_start, end, str(cache_dir), progress_cb=None, price_source=price_source)
        if df_1m.empty:
            raise ValueError(f"No candles returned for window {start} -> {end}.")
        streams_full = simulate_multitf_indicators(
            df_1m,
            tfs,
            None,
            macd_impl=macd_impl,
            adx_impl=adx_impl,
            cache_dir=str(cache_dir),
            symbol=symbol,
            start_utc=warmup_start,
            end_utc=end,
            price_source=price_source,
            required_fields=req_spec,
        )
        try:
            prepared_entry = build_prepared_dataset(
                symbol=symbol,
                start=warmup_start,
                end=end,
                timeframes=tfs,
                price_source=price_source,
                macd_impl=macd_impl,
                adx_impl=adx_impl,
                required_fields=req_spec,
                market_state_thresholds=None,
                df_1m_full=df_1m,
                streams_full=streams_full,
            )
            save_prepared_dataset_to_disk(str(cache_dir), prepared_entry)
        except Exception:
            pass

    if bt_mode == BACKTEST_MODE_TICK:
        base_result = run_backtest_tick(
            symbol=symbol,
            df_1m_full=df_1m,
            start=start,
            end=end,
            tfs=tfs,
            rules_model=rules_model,
            tab_group_join_mode=tab_group_join_mode,
            group_rule_join_mode=group_rule_join_mode,
            entry_filters=entry_filters,
            cfg=base_cfg,
            cache_dir=str(cache_dir),
            progress_cb=None,
            streams_full=streams_full,
            macd_impl=macd_impl,
        )
    else:
        base_result = run_backtest_auto(
            symbol=symbol,
            streams_full=streams_full,
            start=start,
            end=end,
            rules_model=rules_model,
            tab_group_join_mode=tab_group_join_mode,
            group_rule_join_mode=group_rule_join_mode,
            cfg=base_cfg,
            df_1m_full=df_1m,
            entry_filters=entry_filters,
            apply_htf_closed_only=(bt_mode == BACKTEST_MODE_BAR_1M_HTF_CLOSED_ONLY),
        )

    window_rows: List[Dict[str, Any]] = []
    combo_summaries: Dict[str, Dict[str, Any]] = {}

    base_summary = dict(base_result.summary or {})
    base_trades = int(base_summary.get("num_trades", base_summary.get("trades", 0)) or 0)
    combo_summaries[_combo_key(0.0, 0.0)] = base_summary
    window_rows.append(
        {
            "window": _window_label(start, end),
            "stop_loss_pct": 0.0,
            "take_profit_pct": 0.0,
            "return_pct": float(base_summary.get("return_pct", float("nan"))),
            "profit_factor": float(base_summary.get("profit_factor", float("nan"))),
            "max_drawdown_pct": float(base_summary.get("max_drawdown_pct", float("nan"))),
            "trades": base_trades,
            "win_rate_pct": float(base_summary.get("win_rate_pct", float("nan"))),
        }
    )

    for stop_value, take_value in combos:
        if stop_value == 0.0 and take_value == 0.0:
            continue
        next_cfg = _apply_exit_values(base_cfg, stop_value=stop_value, take_value=take_value)
        plan = build_backtest_replay_plan(
            base_result,
            next_cfg=next_cfg,
            symbol=symbol,
            start=start,
            end=end,
            rules_model=rules_model,
            tab_group_join_mode=tab_group_join_mode,
            group_rule_join_mode=group_rule_join_mode,
            entry_filters=entry_filters,
        )
        if plan.eligible:
            res = replay_backtest_result(
                base_result,
                cfg=next_cfg,
                capture_trade_details=next_cfg.capture_trade_details,
                equity_curve_stride=next_cfg.equity_curve_stride,
            )
        else:
            res = run_backtest_auto(
                symbol=symbol,
                streams_full=streams_full,
                start=start,
                end=end,
                rules_model=rules_model,
                tab_group_join_mode=tab_group_join_mode,
                group_rule_join_mode=group_rule_join_mode,
                cfg=next_cfg,
                df_1m_full=df_1m,
                entry_filters=entry_filters,
                apply_htf_closed_only=(bt_mode == BACKTEST_MODE_BAR_1M_HTF_CLOSED_ONLY),
            )
        summary = dict(res.summary or {})
        combo_summaries[_combo_key(stop_value, take_value)] = summary
        trades_count = int(summary.get("num_trades", summary.get("trades", 0)) or 0)
        window_rows.append(
            {
                "window": _window_label(start, end),
                "stop_loss_pct": float(stop_value),
                "take_profit_pct": float(take_value),
                "return_pct": float(summary.get("return_pct", float("nan"))),
                "profit_factor": float(summary.get("profit_factor", float("nan"))),
                "max_drawdown_pct": float(summary.get("max_drawdown_pct", float("nan"))),
                "trades": trades_count,
                "win_rate_pct": float(summary.get("win_rate_pct", float("nan"))),
            }
        )

    return {
        "window": _window_label(start, end),
        "start": str(start),
        "end": str(end),
        "rows": window_rows,
        "combo_summaries": combo_summaries,
        "runtime_sec": float(time.perf_counter() - start_t),
        "engine": str(base_summary.get("strategy_plan_engine", "")),
    }


def main() -> int:
    repo_root = REPO_ROOT
    default_presets = repo_root / "app" / "presets.json"

    parser = argparse.ArgumentParser(description="Sweep basic stop/take combinations across multiple windows for a saved preset.")
    parser.add_argument("--preset", dest="preset_name", default=None)
    parser.add_argument("--presets-file", dest="presets_file", default=str(default_presets))
    parser.add_argument("--cache-dir", dest="cache_dir", default=None)
    parser.add_argument(
        "--windows",
        default="2025-01-01 00:00:00|2025-04-01 00:00:00;2025-04-01 00:00:00|2025-07-01 00:00:00;2025-07-01 00:00:00|2025-10-01 00:00:00;2025-10-01 00:00:00|2026-01-01 00:00:00;2026-01-01 00:00:00|2026-03-29 10:22:00",
        help="Semicolon-separated windows formatted as start|end.",
    )
    parser.add_argument("--initial-balance", type=float, default=1000.0)
    parser.add_argument("--order-notional", type=float, default=100.0)
    parser.add_argument("--fee-pct", type=float, default=0.04)
    parser.add_argument("--slippage-bps", type=float, default=0.0)
    parser.add_argument("--allow-reverse", dest="allow_reverse", action="store_true", default=True)
    parser.add_argument("--no-allow-reverse", dest="allow_reverse", action="store_false")
    parser.add_argument("--skip-both-entries", dest="skip_both_entries", action="store_true", default=True)
    parser.add_argument("--no-skip-both-entries", dest="skip_both_entries", action="store_false")
    parser.add_argument("--step-timeframe", default="1m")
    parser.add_argument("--capture-trade-details", action="store_true", default=False)
    parser.add_argument("--equity-curve-stride", type=int, default=15)
    parser.add_argument("--stop-values", default="0,1.5,2,2.5,3,4")
    parser.add_argument("--take-values", default="0,4,5,6,8")
    parser.add_argument("--workers", type=int, default=1, help="Parallel window workers. Use 0 for auto.")
    parser.add_argument("--top-n", type=int, default=12)
    parser.add_argument("--output-dir", default=None)
    args = parser.parse_args()

    payload = _load_presets(Path(args.presets_file).resolve())
    preset_name = _pick_preset_name(payload, args.preset_name)
    preset = payload["presets"][preset_name]
    settings = preset.get("settings", {})
    if not isinstance(settings, dict):
        raise ValueError("Preset settings payload is invalid.")

    symbol = str(settings.get("symbol", "BTCUSDT") or "BTCUSDT").strip().upper()
    windows = _parse_windows(args.windows)
    tfs = [tf for tf, enabled in (settings.get("timeframes", {}) or {}).items() if bool(enabled)]
    if "1m" not in tfs:
        tfs = ["1m"] + tfs
    if args.step_timeframe not in tfs:
        tfs.append(args.step_timeframe)

    rules_model, tab_group_join_mode, group_rule_join_mode, entry_filters = _materialize_preset(preset)

    base_cfg = BacktestConfig(
        initial_balance=float(args.initial_balance),
        order_notional_usdt=float(args.order_notional),
        fee_rate=float(args.fee_pct) / 100.0,
        slippage_bps=float(args.slippage_bps),
        stop_loss_pct=0.0,
        take_profit_pct=0.0,
        stop_loss_mode="OFF",
        stop_loss_value=0.0,
        stop_loss_timeframe="1m",
        stop_loss_field="",
        stop_loss_offset_pct=0.0,
        take_profit_mode="OFF",
        take_profit_value=0.0,
        take_profit_timeframe="1m",
        take_profit_field="",
        take_profit_offset_pct=0.0,
        allow_reverse=bool(args.allow_reverse),
        skip_if_both_entries=bool(args.skip_both_entries),
        step_timeframe=str(args.step_timeframe),
        capture_trade_details=bool(args.capture_trade_details),
        equity_curve_stride=max(1, int(args.equity_curve_stride)),
    )

    req_spec = compile_stream_requirements(
        rules_model,
        entry_filters=entry_filters,
        backtest_cfg=base_cfg,
        include_plot_defaults=True,
    )

    preset_warmup = int(settings.get("warmup_min", 0) or 0)
    needed_warmup = required_warmup_minutes(tfs, required_fields=req_spec)
    warmup = max(preset_warmup, int(needed_warmup or 0))

    cache_dir = _resolve_cache_dir(repo_root, args.cache_dir or str(settings.get("cache_dir", "data_cache") or "data_cache"))
    cache_dir.mkdir(parents=True, exist_ok=True)

    bt_mode = str(settings.get("bt_mode", "") or "")
    price_source = str(settings.get("price_source", "LAST") or "LAST").upper()
    macd_impl = str(settings.get("macd_impl", "TRADINGVIEW") or "TRADINGVIEW").upper()
    adx_impl = str(settings.get("adx_impl", "TRADINGVIEW") or "TRADINGVIEW").upper()

    stop_values = _parse_float_list(args.stop_values)
    take_values = _parse_float_list(args.take_values)
    combos = [(sl, tp) for sl in stop_values for tp in take_values]

    print(f"Preset: {preset_name}", flush=True)
    print(f"Symbol: {symbol}", flush=True)
    print(f"Windows: {len(windows)}", flush=True)
    print(f"Exit grid: {len(stop_values)} x {len(take_values)} = {len(combos)} combinations", flush=True)
    print(f"Cache dir: {cache_dir}", flush=True)
    if int(args.workers) == 0:
        workers = max(1, min(len(windows), int(os.cpu_count() or 1)))
    else:
        workers = max(1, min(len(windows), int(args.workers)))
    print(f"Workers: {workers}", flush=True)

    srv = binance_fapi_server_time_utc()
    if srv is not None:
        print(f"Binance server time (UTC): {srv}", flush=True)

    all_window_rows: List[Dict[str, Any]] = []
    aggregate: Dict[Tuple[float, float], List[Dict[str, Any]]] = {combo: [] for combo in combos}
    window_run_meta: List[Dict[str, Any]] = []
    tasks = [
        {
            "symbol": symbol,
            "start": start,
            "end": end,
            "warmup": warmup,
            "tfs": list(tfs),
            "cache_dir": str(cache_dir),
            "bt_mode": bt_mode,
            "price_source": price_source,
            "macd_impl": macd_impl,
            "adx_impl": adx_impl,
            "rules_model": rules_model,
            "tab_group_join_mode": tab_group_join_mode,
            "group_rule_join_mode": group_rule_join_mode,
            "entry_filters": entry_filters,
            "base_cfg": base_cfg.__dict__,
            "req_spec": req_spec,
            "stop_values": stop_values,
            "take_values": take_values,
        }
        for start, end in windows
    ]

    results_by_window: List[Dict[str, Any]] = []
    if workers <= 1 or len(tasks) <= 1:
        for window_index, task in enumerate(tasks, start=1):
            print(f"Window {window_index}/{len(tasks)}: {task['start']} -> {task['end']}", flush=True)
            result = _run_window_exit_sweep(task)
            print(
                f"Completed window {window_index}/{len(tasks)} in {_safe_round(result['runtime_sec'], 3)}s "
                f"(engine={result.get('engine') or 'unknown'})",
                flush=True,
            )
            results_by_window.append(result)
    else:
        print("Running windows in parallel...", flush=True)
        with ProcessPoolExecutor(max_workers=workers) as executor:
            future_map = {
                executor.submit(_run_window_exit_sweep, task): (idx, task)
                for idx, task in enumerate(tasks, start=1)
            }
            completed = 0
            for future in as_completed(future_map):
                idx, task = future_map[future]
                result = future.result()
                completed += 1
                print(
                    f"Completed window {completed}/{len(tasks)} "
                    f"({task['start']} -> {task['end']}) in {_safe_round(result['runtime_sec'], 3)}s "
                    f"(engine={result.get('engine') or 'unknown'})",
                    flush=True,
                )
                results_by_window.append(result)

    results_by_window.sort(key=lambda item: str(item.get("start", "")))
    for result in results_by_window:
        window_run_meta.append(
            {
                "window": str(result.get("window", "")),
                "start": str(result.get("start", "")),
                "end": str(result.get("end", "")),
                "runtime_sec": float(result.get("runtime_sec", 0.0) or 0.0),
                "engine": str(result.get("engine", "") or ""),
            }
        )
        all_window_rows.extend(list(result.get("rows", []) or []))
        combo_summaries = dict(result.get("combo_summaries", {}) or {})
        for stop_value, take_value in combos:
            key = _combo_key(stop_value, take_value)
            summary = combo_summaries.get(key)
            if isinstance(summary, dict):
                aggregate[(stop_value, take_value)].append(summary)

    aggregate_rows: List[Dict[str, Any]] = []
    for (stop_value, take_value), summaries in aggregate.items():
        if not summaries:
            continue
        returns = [float(s.get("return_pct", float("nan"))) for s in summaries]
        pfs = [float(s.get("profit_factor", float("nan"))) for s in summaries]
        dds = [float(s.get("max_drawdown_pct", float("nan"))) for s in summaries]
        trades = [int(s.get("num_trades", s.get("trades", 0)) or 0) for s in summaries]
        growth = 1.0
        for ret in returns:
            if math.isfinite(ret):
                growth *= (1.0 + (ret / 100.0))
        aggregate_rows.append(
            {
                "stop_loss_pct": float(stop_value),
                "take_profit_pct": float(take_value),
                "windows": len(summaries),
                "compounded_return_pct": (growth - 1.0) * 100.0,
                "avg_return_pct": sum(returns) / len(returns),
                "median_return_pct": float(pd.Series(returns).median()),
                "positive_windows": sum(1 for v in returns if math.isfinite(v) and v > 0.0),
                "worst_window_return_pct": min(returns),
                "avg_profit_factor": sum(pfs) / len(pfs),
                "worst_window_max_drawdown_pct": max(dds),
                "avg_trades": sum(trades) / len(trades),
            }
        )

    aggregate_rows_sorted = sorted(aggregate_rows, key=_sort_key, reverse=True)
    top_rows = aggregate_rows_sorted[: max(1, int(args.top_n))]

    runs_dir = Path(args.output_dir).resolve() if args.output_dir else (repo_root / "runs" / f"{_slug(preset_name)}_multiwindow_exit_sweep_{pd.Timestamp.now(tz='UTC').strftime('%Y%m%d_%H%M%S')}")
    runs_dir.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(aggregate_rows_sorted).to_csv(runs_dir / "aggregate_ranked_results.csv", index=False)
    pd.DataFrame(all_window_rows).to_csv(runs_dir / "per_window_results.csv", index=False)

    summary_payload = {
        "preset_name": preset_name,
        "symbol": symbol,
        "workers": int(workers),
        "windows": [{"start": str(start), "end": str(end)} for start, end in windows],
        "window_runs": window_run_meta,
        "stop_values": stop_values,
        "take_values": take_values,
        "top_results": top_rows,
    }
    (runs_dir / "summary.json").write_text(json.dumps(summary_payload, indent=2, ensure_ascii=False, default=str), encoding="utf-8")

    print("", flush=True)
    print("Top multi-window exit combinations", flush=True)
    for idx, row in enumerate(top_rows, start=1):
        print(
            f"{idx}. stop={_safe_round(row['stop_loss_pct'], 4)}% "
            f"take={_safe_round(row['take_profit_pct'], 4)}% "
            f"compounded_return_pct={_safe_round(row['compounded_return_pct'], 4)} "
            f"positive_windows={row['positive_windows']}/{row['windows']} "
            f"avg_return_pct={_safe_round(row['avg_return_pct'], 4)} "
            f"worst_window_return_pct={_safe_round(row['worst_window_return_pct'], 4)} "
            f"avg_profit_factor={_safe_round(row['avg_profit_factor'], 4)} "
            f"worst_window_max_dd_pct={_safe_round(row['worst_window_max_drawdown_pct'], 4)}",
            flush=True,
        )

    print(f"Saved aggregate results: {runs_dir / 'aggregate_ranked_results.csv'}", flush=True)
    print(f"Saved per-window results: {runs_dir / 'per_window_results.csv'}", flush=True)
    print(f"Saved summary: {runs_dir / 'summary.json'}", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
