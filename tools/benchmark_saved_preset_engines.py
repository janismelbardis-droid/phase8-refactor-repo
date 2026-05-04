from __future__ import annotations

import argparse
import json
import time
from pathlib import Path
import sys
from typing import Any, Dict

import pandas as pd

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from app.backtest import run_backtest
from app.backtest_models import BacktestConfig
from app.fast_backtest import run_backtest_compiled_bar
from app.prepared_dataset import (
    build_prepared_dataset,
    find_covering_prepared_dataset_on_disk,
    save_prepared_dataset_to_disk,
    slice_df_1m_window,
    slice_streams_window,
)
from app.strategy_compiler import compile_strategy_plan
from app.utils_time import parse_utc, required_warmup_minutes
from app.indicators_streaming import simulate_multitf_indicators
from app.data_binance import fetch_klines_1m_futures
from app.strategy_requirements import compile_stream_requirements
from tools.run_saved_preset import _load_presets, _materialize_preset, _pick_preset_name, _resolve_cache_dir


def _progress(msg: str, pct: Any = None) -> None:
    if pct is None:
        print(msg, flush=True)
        return
    print(f"[{pct}%] {msg}", flush=True)


def _load_windowed_streams(
    *,
    symbol: str,
    start: pd.Timestamp,
    end: pd.Timestamp,
    settings: Dict[str, Any],
    rules_model: Dict[str, Any],
    entry_filters: Dict[str, Any],
    cfg: BacktestConfig,
    repo_root: Path,
    cache_dir_arg: str | None,
) -> Dict[str, Any]:
    tfs = [tf for tf, enabled in (settings.get("timeframes", {}) or {}).items() if bool(enabled)]
    if "1m" not in tfs:
        tfs = ["1m"] + tfs
    if cfg.step_timeframe not in tfs:
        tfs.append(cfg.step_timeframe)

    req_spec = compile_stream_requirements(
        rules_model,
        entry_filters=entry_filters,
        backtest_cfg=cfg,
        include_plot_defaults=False,
    )
    preset_warmup = int(settings.get("warmup_min", 0) or 0)
    warmup = max(preset_warmup, int(required_warmup_minutes(tfs, required_fields=req_spec) or 0))
    warmup_start = start - pd.Timedelta(minutes=warmup)

    cache_dir = _resolve_cache_dir(repo_root, cache_dir_arg or str(settings.get("cache_dir", "data_cache") or "data_cache"))
    cache_dir.mkdir(parents=True, exist_ok=True)
    price_source = str(settings.get("price_source", "LAST") or "LAST").upper()
    macd_impl = str(settings.get("macd_impl", "TRADINGVIEW") or "TRADINGVIEW").upper()
    adx_impl = str(settings.get("adx_impl", "TRADINGVIEW") or "TRADINGVIEW").upper()

    prepared = find_covering_prepared_dataset_on_disk(
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
    if prepared is None:
        df_1m = fetch_klines_1m_futures(symbol, warmup_start, end, str(cache_dir), progress_cb=_progress, price_source=price_source)
        streams_full = simulate_multitf_indicators(
            df_1m,
            tfs,
            _progress,
            macd_impl=macd_impl,
            adx_impl=adx_impl,
            cache_dir=str(cache_dir),
            symbol=symbol,
            start_utc=warmup_start,
            end_utc=end,
            required_fields=req_spec,
        )
        prepared = build_prepared_dataset(
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
        save_prepared_dataset_to_disk(str(cache_dir), prepared)

    return {
        "df_1m": slice_df_1m_window(prepared.df_1m_full, warmup_start, end),
        "streams_full": slice_streams_window(prepared.streams_full, warmup_start, end, timeframes=tfs),
        "cache_dir": str(cache_dir),
    }


def _elapsed_run(fn, *args, **kwargs):
    t0 = time.perf_counter()
    result = fn(*args, **kwargs)
    return result, time.perf_counter() - t0


def _json_dumpable(value: Any) -> str:
    return json.dumps(value, indent=2, default=str)


def main() -> int:
    parser = argparse.ArgumentParser(description="Benchmark the compiled backtest engine against the legacy engine for a saved preset.")
    parser.add_argument("--preset", default=None)
    parser.add_argument("--presets-file", default=str(REPO_ROOT / "app" / "presets.json"))
    parser.add_argument("--start", required=True)
    parser.add_argument("--end", required=True)
    parser.add_argument("--cache-dir", default=None)
    parser.add_argument("--compare-legacy", action="store_true", default=False)
    parser.add_argument("--output", default=None)
    args = parser.parse_args()

    payload = _load_presets(Path(args.presets_file).resolve())
    preset_name = _pick_preset_name(payload, args.preset)
    preset = payload["presets"][preset_name]
    settings = dict(preset.get("settings", {}) or {})
    symbol = str(settings.get("symbol", "BTCUSDT") or "BTCUSDT").upper()
    start = parse_utc(args.start)
    end = parse_utc(args.end)

    rules_model, tab_group_join_mode, group_rule_join_mode, entry_filters = _materialize_preset(preset)
    cfg = BacktestConfig(
        initial_balance=1000.0,
        order_notional_usdt=100.0,
        fee_rate=0.0004,
        slippage_bps=0.0,
        stop_loss_mode="OFF",
        stop_loss_value=0.0,
        stop_loss_pct=0.0,
        take_profit_mode="OFF",
        take_profit_value=0.0,
        take_profit_pct=0.0,
        allow_reverse=True,
        skip_if_both_entries=True,
        step_timeframe="1m",
        capture_trade_details=False,
        equity_curve_stride=15,
    )

    loaded = _load_windowed_streams(
        symbol=symbol,
        start=start,
        end=end,
        settings=settings,
        rules_model=rules_model,
        entry_filters=entry_filters,
        cfg=cfg,
        repo_root=REPO_ROOT,
        cache_dir_arg=args.cache_dir,
    )
    streams_full = loaded["streams_full"]
    df_1m = loaded["df_1m"]

    plan = compile_strategy_plan(
        rules_model,
        tab_group_join_mode=tab_group_join_mode,
        group_rule_join_mode=group_rule_join_mode,
        entry_filters=entry_filters,
        backtest_cfg=cfg,
    )
    print(_json_dumpable(
        {
            "preset": preset_name,
            "symbol": symbol,
            "engine": plan.engine,
            "compiled_tabs": list(plan.compiled_tabs),
            "sequence_tabs": list(plan.sequence_tabs),
            "entry_filter_tabs": list(plan.entry_filter_tabs),
            "blockers": list(plan.blockers),
            "notes": list(plan.notes),
        }
    ), flush=True)

    fast_result = None
    fast_elapsed = None
    if plan.supports_fast_bar:
        fast_result, fast_elapsed = _elapsed_run(
            run_backtest_compiled_bar,
            symbol=symbol,
            streams_full=streams_full,
            start=start,
            end=end,
            rules_model=rules_model,
            tab_group_join_mode=tab_group_join_mode,
            group_rule_join_mode=group_rule_join_mode,
            cfg=cfg,
            entry_filters=entry_filters,
        )
        print(f"Compiled engine: {fast_elapsed:.3f}s", flush=True)
        print(_json_dumpable(fast_result.summary), flush=True)

    legacy_result = None
    legacy_elapsed = None
    if args.compare_legacy:
        legacy_result, legacy_elapsed = _elapsed_run(
            run_backtest,
            symbol=symbol,
            streams_full=streams_full,
            start=start,
            end=end,
            rules_model=rules_model,
            tab_group_join_mode=tab_group_join_mode,
            group_rule_join_mode=group_rule_join_mode,
            cfg=cfg,
            df_1m_full=df_1m,
            entry_filters=entry_filters,
        )
        print(f"Legacy engine: {legacy_elapsed:.3f}s", flush=True)
        print(_json_dumpable(legacy_result.summary), flush=True)

    output = {
        "preset": preset_name,
        "symbol": symbol,
        "plan": {
            "engine": plan.engine,
            "compiled_tabs": list(plan.compiled_tabs),
            "sequence_tabs": list(plan.sequence_tabs),
            "entry_filter_tabs": list(plan.entry_filter_tabs),
            "blockers": list(plan.blockers),
            "notes": list(plan.notes),
        },
        "fast": {
            "elapsed_sec": fast_elapsed,
            "summary": fast_result.summary if fast_result is not None else None,
        },
        "legacy": {
            "elapsed_sec": legacy_elapsed,
            "summary": legacy_result.summary if legacy_result is not None else None,
        },
    }
    if args.output:
        output_path = Path(args.output).resolve()
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_text(json.dumps(output, indent=2, default=str), encoding="utf-8")
        print(f"Saved benchmark output: {output_path}", flush=True)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
