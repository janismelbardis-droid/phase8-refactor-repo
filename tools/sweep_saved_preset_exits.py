from __future__ import annotations

import argparse
from concurrent.futures import ProcessPoolExecutor, as_completed
import json
import math
from pathlib import Path
import sys
import tempfile
import time
from typing import Any, Dict, List, Tuple

import pandas as pd

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from app.backtest import run_backtest_tick
from app.backtest_models import BacktestConfig
from app.constants import (
    BACKTEST_MODE_BAR_1M_HTF_CLOSED_ONLY,
    BACKTEST_MODE_TICK,
)
from app.data_binance import binance_fapi_server_time_utc
from app.fast_backtest import run_backtest_auto
from app.research.replay import extract_backtest_replay_context, replay_backtest_result
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
from tools.replay_bundle_cache import (
    build_replay_bundle_spec,
    load_replay_bundle,
    save_replay_bundle,
)
from tools._replay_worker_pool import (
    choose_auto_chunk_size,
    choose_auto_workers,
    detect_cpu_counts,
    get_worker_base_cfg,
    get_worker_replay_source,
    init_worker_replay_state,
    write_worker_replay_source,
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
        if not text:
            continue
        values.append(float(text))
    cleaned = sorted({round(float(v), 8) for v in values})
    if not cleaned:
        raise ValueError("Expected at least one numeric value.")
    return list(cleaned)


def _parse_int_list(raw: str) -> List[int]:
    values: List[int] = []
    for chunk in str(raw or "").split(","):
        text = chunk.strip()
        if not text:
            continue
        values.append(int(text))
    cleaned = sorted({int(v) for v in values if int(v) > 0})
    if not cleaned:
        raise ValueError("Expected at least one positive integer value.")
    return list(cleaned)


def _apply_exit_values(cfg: BacktestConfig, *, stop_loss_value: float, take_profit_value: float) -> BacktestConfig:
    next_cfg = BacktestConfig(**cfg.__dict__)
    if float(stop_loss_value) > 0.0:
        next_cfg.stop_loss_mode = "PERCENT"
        next_cfg.stop_loss_value = float(stop_loss_value)
        next_cfg.stop_loss_pct = float(stop_loss_value)
    else:
        next_cfg.stop_loss_mode = "OFF"
        next_cfg.stop_loss_value = 0.0
        next_cfg.stop_loss_pct = 0.0

    if float(take_profit_value) > 0.0:
        next_cfg.take_profit_mode = "PERCENT"
        next_cfg.take_profit_value = float(take_profit_value)
        next_cfg.take_profit_pct = float(take_profit_value)
    else:
        next_cfg.take_profit_mode = "OFF"
        next_cfg.take_profit_value = 0.0
        next_cfg.take_profit_pct = 0.0
    return next_cfg


def _result_row(res: Any, *, stop_loss_value: float, take_profit_value: float) -> Dict[str, Any]:
    summary = dict(getattr(res, "summary", {}) or {})
    trades_count = summary.get("num_trades", summary.get("trades", len(getattr(res, "trades", []) or [])))
    return {
        "stop_loss_pct": float(stop_loss_value),
        "take_profit_pct": float(take_profit_value),
        "ending_balance": float(summary.get("ending_balance", float("nan"))),
        "return_pct": float(summary.get("return_pct", float("nan"))),
        "num_trades": int(trades_count or 0),
        "win_rate_pct": float(summary.get("win_rate_pct", float("nan"))),
        "profit_factor": float(summary.get("profit_factor", float("nan"))),
        "avg_trade_return_pct": float(summary.get("avg_trade_return_pct", float("nan"))),
        "max_drawdown": float(summary.get("max_drawdown", float("nan"))),
        "max_drawdown_pct": float(summary.get("max_drawdown_pct", float("nan"))),
        "stop_loss_count": int(summary.get("stop_loss_count", 0) or 0),
        "take_profit_count": int(summary.get("take_profit_count", 0) or 0),
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


def _replay_exit_combo(
    base_result: Any,
    base_cfg: BacktestConfig,
    *,
    stop_loss_value: float,
    take_profit_value: float,
) -> Any:
    next_cfg = _apply_exit_values(base_cfg, stop_loss_value=stop_loss_value, take_profit_value=take_profit_value)
    return replay_backtest_result(
        base_result,
        cfg=next_cfg,
        capture_trade_details=next_cfg.capture_trade_details,
        equity_curve_stride=next_cfg.equity_curve_stride,
    )


def _chunked_combos(combos: List[Tuple[float, float]], chunk_size: int) -> List[List[Tuple[float, float]]]:
    size = max(1, int(chunk_size))
    return [list(combos[idx : idx + size]) for idx in range(0, len(combos), size)]


def _ensure_price_column(frame: pd.DataFrame) -> pd.DataFrame:
    if frame is None or frame.empty or "price" in frame.columns:
        return frame
    if "close" not in frame.columns:
        return frame
    next_frame = frame.copy()
    next_frame["price"] = pd.to_numeric(next_frame["close"], errors="coerce")
    return next_frame


def _run_exit_combo_chunk(combos: List[Tuple[float, float]]) -> List[Dict[str, Any]]:
    replay_source = get_worker_replay_source()
    base_cfg = get_worker_base_cfg()
    rows: List[Dict[str, Any]] = []
    for stop_value, take_value in list(combos or []):
        res = _replay_exit_combo(
            replay_source,
            base_cfg,
            stop_loss_value=float(stop_value),
            take_profit_value=float(take_value),
        )
        rows.append(_result_row(res, stop_loss_value=float(stop_value), take_profit_value=float(take_value)))
    return rows


def _signature_value(value: Any) -> Any:
    try:
        num = float(value)
    except Exception:
        return value
    if not math.isfinite(num):
        return str(num)
    return round(num, 8)


def _rows_signature(rows: List[Dict[str, Any]]) -> Tuple[Any, ...]:
    ordered = sorted(
        list(rows or []),
        key=lambda row: (
            float(row.get("stop_loss_pct", 0.0)),
            float(row.get("take_profit_pct", 0.0)),
        ),
    )
    signature: List[Tuple[Any, ...]] = []
    for row in ordered:
        signature.append(
            (
                _signature_value(row.get("stop_loss_pct")),
                _signature_value(row.get("take_profit_pct")),
                _signature_value(row.get("ending_balance")),
                _signature_value(row.get("return_pct")),
                int(row.get("num_trades", 0) or 0),
                _signature_value(row.get("win_rate_pct")),
                _signature_value(row.get("profit_factor")),
                _signature_value(row.get("avg_trade_return_pct")),
                _signature_value(row.get("max_drawdown")),
                _signature_value(row.get("max_drawdown_pct")),
                int(row.get("stop_loss_count", 0) or 0),
                int(row.get("take_profit_count", 0) or 0),
            )
        )
    return tuple(signature)


def _normalize_worker_candidates(raw: str, *, combo_count: int, logical_cpus: int, manual_worker: int = 0) -> List[int]:
    candidates = _parse_int_list(raw)
    if int(manual_worker or 0) > 0:
        candidates.append(int(manual_worker))
    normalized: List[int] = []
    max_workers = max(1, min(int(combo_count or 1), int(logical_cpus or 1)))
    for candidate in candidates:
        next_candidate = max(1, min(max_workers, int(candidate)))
        if next_candidate not in normalized:
            normalized.append(next_candidate)
    if 1 not in normalized:
        normalized.insert(0, 1)
    return normalized or [1]


def _run_replay_grid(
    *,
    base_result: Any,
    base_cfg: BacktestConfig,
    combos_to_replay: List[Tuple[float, float]],
    workers: int,
    requested_chunk_size: int,
    show_chunk_progress: bool,
    show_bootstrap_details: bool,
) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    replay_t0 = time.perf_counter()
    rows: List[Dict[str, Any]] = []
    resolved_workers = max(1, min(len(combos_to_replay) or 1, int(workers)))
    resolved_chunk_size = 0
    bootstrap_blob_mib = 0.0

    if resolved_workers <= 1 or len(combos_to_replay) <= 1:
        for index, (stop_value, take_value) in enumerate(combos_to_replay, start=1):
            res = _replay_exit_combo(
                base_result,
                base_cfg,
                stop_loss_value=stop_value,
                take_profit_value=take_value,
            )
            rows.append(_result_row(res, stop_loss_value=stop_value, take_profit_value=take_value))
            if show_chunk_progress and (index % 10 == 0 or index == len(combos_to_replay)):
                print(f"Replay progress: {index}/{len(combos_to_replay)} combinations checked", flush=True)
    else:
        if int(requested_chunk_size) > 0:
            resolved_chunk_size = int(requested_chunk_size)
        else:
            resolved_chunk_size = choose_auto_chunk_size(len(combos_to_replay), resolved_workers)
        combo_chunks = _chunked_combos(combos_to_replay, resolved_chunk_size)
        if show_chunk_progress:
            print(f"Replay chunk size: {resolved_chunk_size}", flush=True)
            print("Running combo replay in parallel...", flush=True)
        replay_context = extract_backtest_replay_context(base_result)
        if not replay_context:
            raise ValueError("Base backtest result is missing replay_context, so parallel replay is unavailable.")
        temp_dir_ctx = tempfile.TemporaryDirectory(prefix="exit_replay_ctx_")
        replay_blob_path = Path(temp_dir_ctx.name) / "replay_context.pkl"
        # Keep the full compiled replay context for worker replays. Some compiled
        # paths now require the original CompiledBarPlan for correctness, and the
        # compact replay-only plan is no longer sufficient for exit sweeps.
        replay_context_for_workers = replay_context
        replay_blob_size = write_worker_replay_source(replay_blob_path, replay_context_for_workers)
        bootstrap_blob_mib = float(replay_blob_size / (1024 * 1024))
        if show_bootstrap_details:
            print(f"Replay worker bootstrap blob: {_safe_round(bootstrap_blob_mib, 2)} MiB", flush=True)
        completed = 0
        completed_combos = 0
        try:
            with ProcessPoolExecutor(
                max_workers=resolved_workers,
                initializer=init_worker_replay_state,
                initargs=(str(replay_blob_path), dict(base_cfg.__dict__)),
            ) as executor:
                future_map = {
                    executor.submit(_run_exit_combo_chunk, chunk): len(chunk)
                    for chunk in combo_chunks
                }
                for future in as_completed(future_map):
                    chunk_rows = future.result()
                    rows.extend(chunk_rows)
                    completed += 1
                    completed_combos += int(future_map[future])
                    if show_chunk_progress:
                        print(
                            f"Replay progress: {completed_combos}/{len(combos_to_replay)} combinations checked "
                            f"({completed}/{len(combo_chunks)} chunks)",
                            flush=True,
                        )
        finally:
            temp_dir_ctx.cleanup()

    replay_wall_sec = float(time.perf_counter() - replay_t0)
    return rows, {
        "workers": int(resolved_workers),
        "chunk_size": int(resolved_chunk_size),
        "combo_count": int(len(combos_to_replay)),
        "replay_wall_sec": replay_wall_sec,
        "bootstrap_blob_mib": float(bootstrap_blob_mib),
    }


def _benchmark_replay_workers(
    *,
    base_result: Any,
    base_cfg: BacktestConfig,
    combos_to_replay: List[Tuple[float, float]],
    worker_candidates: List[int],
    requested_chunk_size: int,
) -> Tuple[List[Dict[str, Any]], Dict[str, Any], Dict[str, Any]]:
    print(
        "Replay worker benchmark candidates: "
        + ", ".join(str(int(candidate)) for candidate in list(worker_candidates or [])),
        flush=True,
    )
    benchmark_rows: List[Dict[str, Any]] = []
    benchmark_meta: Dict[str, Any] = {}
    baseline_signature: Tuple[Any, ...] | None = None
    benchmark_runs: List[Dict[str, Any]] = []

    for worker_count in list(worker_candidates or []):
        candidate_rows, candidate_meta = _run_replay_grid(
            base_result=base_result,
            base_cfg=base_cfg,
            combos_to_replay=combos_to_replay,
            workers=int(worker_count),
            requested_chunk_size=int(requested_chunk_size),
            show_chunk_progress=False,
            show_bootstrap_details=False,
        )
        candidate_signature = _rows_signature(candidate_rows)
        if baseline_signature is None:
            baseline_signature = candidate_signature
        elif candidate_signature != baseline_signature:
            raise ValueError(f"Replay worker benchmark mismatch detected for workers={worker_count}.")
        run_payload = dict(candidate_meta)
        benchmark_runs.append(run_payload)
        print(
            f"Benchmark candidate: workers={run_payload['workers']} "
            f"chunk_size={run_payload['chunk_size']} "
            f"replay_sec={_safe_round(run_payload['replay_wall_sec'], 4)}",
            flush=True,
        )
        if not benchmark_rows or float(run_payload["replay_wall_sec"]) < float(benchmark_meta["replay_wall_sec"]):
            benchmark_rows = candidate_rows
            benchmark_meta = run_payload

    baseline_sec = float(benchmark_runs[0]["replay_wall_sec"]) if benchmark_runs else float("nan")
    for run_payload in benchmark_runs:
        replay_sec = float(run_payload["replay_wall_sec"])
        if math.isfinite(baseline_sec) and baseline_sec > 0.0:
            run_payload["speedup_vs_first"] = float(baseline_sec / replay_sec) if replay_sec > 0.0 else float("inf")
        else:
            run_payload["speedup_vs_first"] = float("nan")

    print(
        f"Benchmark winner: workers={benchmark_meta['workers']} "
        f"chunk_size={benchmark_meta['chunk_size']} "
        f"replay_sec={_safe_round(benchmark_meta['replay_wall_sec'], 4)}",
        flush=True,
    )
    return benchmark_rows, benchmark_meta, {
        "candidates": [int(candidate) for candidate in list(worker_candidates or [])],
        "runs": benchmark_runs,
        "best_workers": int(benchmark_meta.get("workers", 1) or 1),
        "best_chunk_size": int(benchmark_meta.get("chunk_size", 0) or 0),
        "best_replay_wall_sec": float(benchmark_meta.get("replay_wall_sec", float("nan"))),
    }


def main() -> int:
    repo_root = REPO_ROOT
    default_presets = repo_root / "app" / "presets.json"

    parser = argparse.ArgumentParser(
        description="Run one saved preset backtest, then replay many stop-loss / take-profit combinations."
    )
    parser.add_argument("--preset", dest="preset_name", default=None, help="Preset name to run.")
    parser.add_argument("--presets-file", dest="presets_file", default=str(default_presets), help="Path to presets.json.")
    parser.add_argument("--cache-dir", dest="cache_dir", default=None, help="Override candle/indicator cache directory.")
    parser.add_argument("--start", dest="start_override", default=None, help="Override preset start UTC.")
    parser.add_argument("--end", dest="end_override", default=None, help="Override preset end UTC.")
    parser.add_argument("--initial-balance", type=float, default=1000.0)
    parser.add_argument("--order-notional", type=float, default=100.0)
    parser.add_argument("--fee-pct", type=float, default=0.04, help="Percent per side, like the UI.")
    parser.add_argument("--slippage-bps", type=float, default=0.0)
    parser.add_argument("--allow-reverse", dest="allow_reverse", action="store_true", default=True)
    parser.add_argument("--no-allow-reverse", dest="allow_reverse", action="store_false")
    parser.add_argument("--skip-both-entries", dest="skip_both_entries", action="store_true", default=True)
    parser.add_argument("--no-skip-both-entries", dest="skip_both_entries", action="store_false")
    parser.add_argument("--step-timeframe", default="1m")
    parser.add_argument("--capture-trade-details", action="store_true", default=False)
    parser.add_argument("--equity-curve-stride", type=int, default=15)
    parser.add_argument("--stop-values", default="0,0.25,0.5,0.75,1,1.5,2,3,4", help="Comma-separated percent values.")
    parser.add_argument("--take-values", default="0,0.25,0.5,0.75,1,1.5,2,3,4,6", help="Comma-separated percent values.")
    parser.add_argument(
        "--objective",
        default="ending_balance",
        choices=["ending_balance", "return_pct", "profit_factor"],
        help="Primary ranking field.",
    )
    parser.add_argument("--top-n", type=int, default=10, help="How many best rows to print.")
    parser.add_argument("--workers", type=int, default=0, help="Parallel combo workers. Auto by default; use 1 to force single-worker.")
    parser.add_argument("--chunk-size", type=int, default=0, help="Optional combo chunk size per worker task.")
    parser.add_argument(
        "--benchmark-workers",
        action="store_true",
        default=False,
        help="Benchmark multiple replay worker counts on this exact grid and reuse the fastest result.",
    )
    parser.add_argument(
        "--benchmark-candidates",
        default="1,2,4,8,12,16",
        help="Comma-separated replay worker counts to benchmark when --benchmark-workers is enabled.",
    )
    parser.add_argument("--output-dir", default=None, help="Optional folder for the sweep outputs.")
    parser.add_argument("--show-server-time", action="store_true", default=False, help="Print Binance server time before the sweep.")
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

    stop_values = _parse_float_list(args.stop_values)
    take_values = _parse_float_list(args.take_values)
    combos = [(sl, tp) for sl in stop_values for tp in take_values]

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
    print(f"Exit grid: {len(stop_values)} stop values x {len(take_values)} take values = {len(combos)} combinations", flush=True)

    if bool(args.show_server_time):
        srv = binance_fapi_server_time_utc()
        if srv is not None:
            print(f"Binance server time (UTC): {srv}", flush=True)

    replay_bundle_spec = build_replay_bundle_spec(
        symbol=symbol,
        start=start,
        end=end,
        warmup_start=warmup_start,
        timeframes=tfs,
        bt_mode=bt_mode,
        price_source=price_source,
        macd_impl=macd_impl,
        adx_impl=adx_impl,
        required_fields=req_spec,
        rules_model=rules_model,
        tab_group_join_mode=tab_group_join_mode,
        group_rule_join_mode=group_rule_join_mode,
        entry_filters=entry_filters,
        base_cfg=base_cfg,
    )
    t_total = time.perf_counter()
    t_stage = time.perf_counter()
    replay_bundle_payload = load_replay_bundle(
        cache_dir,
        symbol=symbol,
        spec=replay_bundle_spec,
    )
    timing_replay_bundle_lookup_sec = time.perf_counter() - t_stage
    replay_source: Any = None
    cached_event_activity = None
    cached_signal_rows = None
    df_1m = None
    streams_full = None
    timing_stream_bundle_sec = 0.0
    timing_base_run_sec = 0.0
    timing_replay_bundle_save_sec = 0.0

    if replay_bundle_payload is not None:
        print(f"Loaded replay bundle: {replay_bundle_payload['spec_key'][:12]}", flush=True)
        replay_source = {"replay_context": replay_bundle_payload["replay_context"]}
        cached_event_activity = replay_bundle_payload.get("event_activity")
        cached_signal_rows = replay_bundle_payload.get("signal_rows")
        base_result = replay_bundle_payload.get("base_result_lite")
        if base_result is not None:
            print("Loaded cached base result.", flush=True)
        else:
            t_stage = time.perf_counter()
            base_result = _replay_exit_combo(
                replay_source,
                base_cfg,
                stop_loss_value=0.0,
                take_profit_value=0.0,
            )
            timing_base_run_sec = time.perf_counter() - t_stage
            t_stage = time.perf_counter()
            upgraded_replay_bundle = save_replay_bundle(
                cache_dir,
                symbol=symbol,
                spec=replay_bundle_spec,
                result=base_result,
            )
            timing_replay_bundle_save_sec = time.perf_counter() - t_stage
            if upgraded_replay_bundle is not None:
                print(f"Upgraded replay bundle with cached base result: {upgraded_replay_bundle.name}", flush=True)
    else:
        t_stage = time.perf_counter()
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
        timing_stream_bundle_sec = time.perf_counter() - t_stage
        df_1m = _ensure_price_column(bundle.df_1m)
        streams_full = bundle.streams_full
        if "1m" in streams_full:
            streams_full["1m"] = _ensure_price_column(streams_full["1m"])

        print("Running base backtest without stop loss / take profit...", flush=True)
        t_stage = time.perf_counter()
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
                progress_cb=_progress,
                streams_full=streams_full,
                macd_impl=macd_impl,
            )
        else:
            if bt_mode == BACKTEST_MODE_BAR_1M_HTF_CLOSED_ONLY:
                _progress("Planning backtest engine and preparing rule-specific streams...", 0)
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
        timing_base_run_sec = time.perf_counter() - t_stage
        t_stage = time.perf_counter()
        saved_replay_bundle = save_replay_bundle(
            cache_dir,
            symbol=symbol,
            spec=replay_bundle_spec,
            result=base_result,
        )
        timing_replay_bundle_save_sec = time.perf_counter() - t_stage
        if saved_replay_bundle is not None:
            print(f"Saved replay bundle: {saved_replay_bundle.name}", flush=True)
        replay_source = base_result

    rows: List[Dict[str, Any]] = [_result_row(base_result, stop_loss_value=0.0, take_profit_value=0.0)]
    combos_to_replay = [(sl, tp) for sl, tp in combos if not (sl == 0.0 and tp == 0.0)]

    cpu_info = detect_cpu_counts(fast=True)
    logical_cpus = int(cpu_info.get("logical") or 1)
    physical_cpus = cpu_info.get("physical")
    print(
        f"Detected CPU threads: logical={logical_cpus}"
        + (f", physical={physical_cpus}" if physical_cpus is not None else ""),
        flush=True,
    )

    replay_rows: List[Dict[str, Any]] = []
    replay_meta: Dict[str, Any] = {
        "workers": 1,
        "chunk_size": 0,
        "combo_count": int(len(combos_to_replay)),
        "replay_wall_sec": 0.0,
        "bootstrap_blob_mib": 0.0,
    }
    benchmark_payload: Dict[str, Any] | None = None

    if args.benchmark_workers and combos_to_replay:
        worker_candidates = _normalize_worker_candidates(
            str(args.benchmark_candidates),
            combo_count=len(combos_to_replay),
            logical_cpus=logical_cpus,
            manual_worker=int(args.workers),
        )
        t_stage = time.perf_counter()
        replay_rows, replay_meta, benchmark_payload = _benchmark_replay_workers(
            base_result=replay_source,
            base_cfg=base_cfg,
            combos_to_replay=combos_to_replay,
            worker_candidates=worker_candidates,
            requested_chunk_size=int(args.chunk_size),
        )
        timing_replay_grid_sec = time.perf_counter() - t_stage
        workers = int(replay_meta["workers"])
        worker_mode = "benchmark"
    else:
        if int(args.workers) == 0:
            workers = choose_auto_workers(
                len(combos_to_replay),
                logical_cpus=logical_cpus,
                physical_cpus=physical_cpus,
            )
            worker_mode = "auto"
        else:
            workers = max(1, min(len(combos_to_replay) or 1, int(args.workers)))
            worker_mode = "manual"
        if not combos_to_replay:
            workers = 1
        print(f"Replay workers: {workers} ({worker_mode})", flush=True)
        t_stage = time.perf_counter()
        replay_rows, replay_meta = _run_replay_grid(
            base_result=replay_source,
            base_cfg=base_cfg,
            combos_to_replay=combos_to_replay,
            workers=workers,
            requested_chunk_size=int(args.chunk_size),
            show_chunk_progress=True,
            show_bootstrap_details=(workers > 1 and len(combos_to_replay) > 1),
        )
        timing_replay_grid_sec = time.perf_counter() - t_stage

    if args.benchmark_workers and combos_to_replay:
        print(f"Replay workers: {replay_meta['workers']} ({worker_mode})", flush=True)
        if int(replay_meta.get("chunk_size", 0) or 0) > 0:
            print(f"Replay chunk size: {replay_meta['chunk_size']}", flush=True)
        print(f"Replay benchmark winning replay_sec: {_safe_round(replay_meta['replay_wall_sec'], 4)}", flush=True)

    rows.extend(replay_rows)

    rows_sorted = sorted(rows, key=lambda row: _sort_key(row, args.objective), reverse=True)
    top_rows = rows_sorted[: max(1, int(args.top_n))]
    best_row = rows_sorted[0]
    best_combo = (float(best_row["stop_loss_pct"]), float(best_row["take_profit_pct"]))
    best_result = base_result if best_combo == (0.0, 0.0) else _replay_exit_combo(
        replay_source,
        base_cfg,
        stop_loss_value=best_combo[0],
        take_profit_value=best_combo[1],
    )

    timing_verification_streams_sec = 0.0
    timing_verification_artifacts_sec = 0.0
    t_stage = time.perf_counter()
    if isinstance(cached_event_activity, dict) and isinstance(cached_signal_rows, list):
        print("Loaded cached verification payload.", flush=True)
        event_activity = dict(cached_event_activity)
        signal_rows = [dict(row) for row in list(cached_signal_rows) if isinstance(row, dict)]
    else:
        if streams_full is None:
            _progress("Loading stream bundle for verification artifacts...", 0)
            t_stage_streams = time.perf_counter()
            verify_bundle = load_stream_bundle_library_first(
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
            timing_verification_streams_sec = time.perf_counter() - t_stage_streams
            df_1m = _ensure_price_column(verify_bundle.df_1m)
            streams_full = verify_bundle.streams_full
            if "1m" in streams_full:
                streams_full["1m"] = _ensure_price_column(streams_full["1m"])
        event_activity = _event_activity_summary(rules_model, streams_full, start, end)
        signal_rows = _event_signal_rows(rules_model, streams_full, start, end)
        refreshed_replay_bundle = save_replay_bundle(
            cache_dir,
            symbol=symbol,
            spec=replay_bundle_spec,
            result=replay_source,
            base_result_lite=base_result,
            event_activity=event_activity,
            signal_rows=signal_rows,
        )
        if refreshed_replay_bundle is not None:
            print(f"Cached verification payload in replay bundle: {refreshed_replay_bundle.name}", flush=True)
    best_cfg = _apply_exit_values(base_cfg, stop_loss_value=best_combo[0], take_profit_value=best_combo[1])
    bundle_dir = _write_verification_bundle(
        repo_root=repo_root,
        preset_name=f"{preset_name}_best_exits",
        symbol=symbol,
        start=start,
        end=end,
        bt_mode=bt_mode,
        cache_dir=cache_dir,
        cfg=best_cfg,
        summary=dict(best_result.summary or {}),
        event_activity=event_activity,
        signal_rows=signal_rows,
        res=best_result,
    )
    timing_verification_artifacts_sec = time.perf_counter() - t_stage

    runs_dir = Path(args.output_dir).resolve() if args.output_dir else (repo_root / "runs" / f"{_slug(preset_name)}_exit_sweep_{pd.Timestamp.now(tz='UTC').strftime('%Y%m%d_%H%M%S')}")
    runs_dir.mkdir(parents=True, exist_ok=True)

    pd.DataFrame(rows_sorted).to_csv(runs_dir / "ranked_results.csv", index=False)
    summary_payload = {
        "preset_name": preset_name,
        "symbol": symbol,
        "start": str(start),
        "end": str(end),
        "objective": str(args.objective),
        "stop_values": stop_values,
        "take_values": take_values,
        "replay_workers_mode": worker_mode,
        "replay_workers_selected": int(replay_meta.get("workers", workers)),
        "replay_chunk_size": int(replay_meta.get("chunk_size", 0) or 0),
        "replay_wall_sec": float(replay_meta.get("replay_wall_sec", float("nan"))),
        "replay_bootstrap_blob_mib": float(replay_meta.get("bootstrap_blob_mib", 0.0) or 0.0),
        "timing_replay_bundle_lookup_sec": float(timing_replay_bundle_lookup_sec),
        "timing_stream_bundle_sec": float(timing_stream_bundle_sec),
        "timing_base_run_sec": float(timing_base_run_sec),
        "timing_replay_bundle_save_sec": float(timing_replay_bundle_save_sec),
        "timing_replay_grid_sec": float(timing_replay_grid_sec),
        "timing_verification_streams_sec": float(timing_verification_streams_sec),
        "timing_verification_artifacts_sec": float(timing_verification_artifacts_sec),
        "timing_total_wall_sec": float(time.perf_counter() - t_total),
        "logical_cpu_count": int(logical_cpus),
        "physical_cpu_count": int(physical_cpus) if physical_cpus is not None else None,
        "replay_worker_benchmark": benchmark_payload,
        "top_results": top_rows,
        "best_result": best_row,
        "best_verification_bundle": str(bundle_dir),
    }
    (runs_dir / "summary.json").write_text(json.dumps(summary_payload, indent=2, ensure_ascii=False, default=str), encoding="utf-8")
    if benchmark_payload is not None:
        (runs_dir / "worker_benchmark.json").write_text(
            json.dumps(benchmark_payload, indent=2, ensure_ascii=False, default=str),
            encoding="utf-8",
        )

    print("", flush=True)
    print("Top exit combinations", flush=True)
    for idx, row in enumerate(top_rows, start=1):
        print(
            f"{idx}. stop={_safe_round(row['stop_loss_pct'], 4)}% "
            f"take={_safe_round(row['take_profit_pct'], 4)}% "
            f"ending_balance={_safe_round(row['ending_balance'], 4)} "
            f"return_pct={_safe_round(row['return_pct'], 4)} "
            f"profit_factor={_safe_round(row['profit_factor'], 4)} "
            f"max_drawdown_pct={_safe_round(row['max_drawdown_pct'], 4)} "
            f"trades={row['num_trades']}",
            flush=True,
        )

    print(f"Saved ranked results: {runs_dir / 'ranked_results.csv'}", flush=True)
    print(f"Saved sweep summary: {runs_dir / 'summary.json'}", flush=True)
    if benchmark_payload is not None:
        print(f"Saved worker benchmark: {runs_dir / 'worker_benchmark.json'}", flush=True)
    print(f"Best verification bundle: {bundle_dir}", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
