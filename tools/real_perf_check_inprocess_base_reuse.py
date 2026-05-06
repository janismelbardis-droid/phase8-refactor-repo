from __future__ import annotations

import argparse
import json
import math
from pathlib import Path
import sys
import time
from typing import Any, Dict, List

import pandas as pd


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from app.backtest import run_backtest_tick
from app.backtest_models import BacktestConfig
from app.constants import BACKTEST_MODE_BAR_1M_HTF_CLOSED_ONLY, BACKTEST_MODE_TICK
from app.fast_backtest import run_backtest_auto
from app.stream_bundle_loader import clear_stream_bundle_memory_cache, load_stream_bundle_library_first
from app.strategy_requirements import compile_stream_requirements
from app.utils_time import parse_utc, required_warmup_minutes
from tools.run_saved_preset import (
    _load_presets,
    _materialize_preset,
    _pick_preset_name,
    _resolve_cache_dir,
)


def _safe_float(value: Any) -> Any:
    try:
        num = float(value)
    except Exception:
        return value
    if not math.isfinite(num):
        return str(num)
    return float(num)


def _progress(_msg: str, _pct: Any = None) -> None:
    return


def _single_run(
    *,
    symbol: str,
    start: pd.Timestamp,
    end: pd.Timestamp,
    query_warmup: int,
    compute_warmup: int,
    tfs: List[str],
    cache_dir: Path,
    price_source: str,
    macd_impl: str,
    adx_impl: str,
    req_spec: Any,
    rules_model: Dict[str, Any],
    tab_group_join_mode: Dict[str, str],
    group_rule_join_mode: Dict[str, List[str]],
    entry_filters: Dict[str, Any],
    cfg: BacktestConfig,
    bt_mode: str,
) -> Dict[str, Any]:
    warmup_start = start - pd.Timedelta(minutes=int(query_warmup))
    compute_start = start - pd.Timedelta(minutes=int(compute_warmup))

    load_started = time.perf_counter()
    bundle = load_stream_bundle_library_first(
        symbol=symbol,
        start=warmup_start,
        end=end,
        compute_start=compute_start,
        required_fields=req_spec,
        timeframes=tfs,
        cache_dir=cache_dir,
        price_source=price_source,
        macd_impl=macd_impl,
        adx_impl=adx_impl,
        progress_cb=_progress,
    )
    load_sec = time.perf_counter() - load_started

    run_started = time.perf_counter()
    if bt_mode == BACKTEST_MODE_TICK:
        res = run_backtest_tick(
            symbol=symbol,
            df_1m_full=bundle.df_1m,
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
            streams_full=bundle.streams_full,
            macd_impl=macd_impl,
        )
    else:
        res = run_backtest_auto(
            symbol=symbol,
            streams_full=bundle.streams_full,
            start=start,
            end=end,
            rules_model=rules_model,
            tab_group_join_mode=tab_group_join_mode,
            group_rule_join_mode=group_rule_join_mode,
            cfg=cfg,
            df_1m_full=bundle.df_1m,
            entry_filters=entry_filters,
            apply_htf_closed_only=(bt_mode == BACKTEST_MODE_BAR_1M_HTF_CLOSED_ONLY),
        )
    run_sec = time.perf_counter() - run_started

    summary = dict(getattr(res, "summary", {}) or {})
    return {
        "bundle_source": str(bundle.source or ""),
        "bundle_memory_cached": bool(getattr(bundle, "memory_cached", False)),
        "load_sec": round(load_sec, 4),
        "run_sec": round(run_sec, 4),
        "total_sec": round(load_sec + run_sec, 4),
        "ending_balance": _safe_float(summary.get("ending_balance")),
        "return_pct": _safe_float(summary.get("return_pct")),
        "num_trades": int(summary.get("num_trades", summary.get("trades", 0)) or 0),
        "strategy_plan_engine": str(summary.get("strategy_plan_engine", "")),
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="Run two in-process base runs and verify resident bundle reuse.")
    parser.add_argument("--preset", default="011")
    parser.add_argument("--presets-file", default=str(REPO_ROOT / "app" / "presets.json"))
    parser.add_argument("--start", default="2026-04-20 00:00:00+00:00")
    parser.add_argument("--end", default="2026-04-20 04:00:00+00:00")
    parser.add_argument("--output-dir", default=str(REPO_ROOT / "runs" / "perf" / "real_perf_check_inprocess_base_reuse"))
    args = parser.parse_args()

    output_dir = Path(args.output_dir).resolve()
    output_dir.mkdir(parents=True, exist_ok=True)

    payload = _load_presets(Path(args.presets_file).resolve())
    preset_name = _pick_preset_name(payload, str(args.preset or "").strip() or None)
    preset = payload["presets"][preset_name]
    settings = preset.get("settings", {})
    if not isinstance(settings, dict):
        raise ValueError("Preset settings payload is invalid.")

    symbol = str(settings.get("symbol", "BTCUSDT") or "BTCUSDT").upper()
    start = parse_utc(str(args.start))
    end = parse_utc(str(args.end))
    rules_model, tab_group_join_mode, group_rule_join_mode, entry_filters = _materialize_preset(preset)
    tfs = [
        tf
        for tf, enabled in (settings.get("timeframes", {}) or {}).items()
        if bool(enabled)
    ]
    if "1m" not in tfs:
        tfs = ["1m"] + tfs
    req_spec = compile_stream_requirements(rules_model, entry_filters=entry_filters)

    preset_warmup = int(settings.get("warmup_min", 0) or 0)
    needed_warmup = required_warmup_minutes(tfs, required_fields=req_spec)
    query_warmup = max(0, int(preset_warmup))
    compute_warmup = max(query_warmup, int(needed_warmup or 0))

    cache_dir = _resolve_cache_dir(REPO_ROOT, str(settings.get("cache_dir", "data_cache") or "data_cache"))
    cache_dir.mkdir(parents=True, exist_ok=True)

    bt_mode = str(settings.get("bt_mode", "") or "")
    price_source = str(settings.get("price_source", "LAST") or "LAST").upper()
    macd_impl = str(settings.get("macd_impl", "TRADINGVIEW") or "TRADINGVIEW").upper()
    adx_impl = str(settings.get("adx_impl", "TRADINGVIEW") or "TRADINGVIEW").upper()

    cfg = BacktestConfig(
        initial_balance=1000.0,
        order_notional_usdt=100.0,
        fee_rate=0.04 / 100.0,
        slippage_bps=0.0,
        allow_reverse=True,
        skip_if_both_entries=True,
        step_timeframe="1m",
        stop_loss_mode="PERCENT",
        stop_loss_value=0.0,
        take_profit_mode="PERCENT",
        take_profit_value=0.0,
        capture_trade_details=False,
        equity_curve_stride=15,
    )

    clear_stream_bundle_memory_cache()

    run1 = _single_run(
        symbol=symbol,
        start=start,
        end=end,
        query_warmup=query_warmup,
        compute_warmup=compute_warmup,
        tfs=tfs,
        cache_dir=cache_dir,
        price_source=price_source,
        macd_impl=macd_impl,
        adx_impl=adx_impl,
        req_spec=req_spec,
        rules_model=rules_model,
        tab_group_join_mode=tab_group_join_mode,
        group_rule_join_mode=group_rule_join_mode,
        entry_filters=entry_filters,
        cfg=cfg,
        bt_mode=bt_mode,
    )
    run2 = _single_run(
        symbol=symbol,
        start=start,
        end=end,
        query_warmup=query_warmup,
        compute_warmup=compute_warmup,
        tfs=tfs,
        cache_dir=cache_dir,
        price_source=price_source,
        macd_impl=macd_impl,
        adx_impl=adx_impl,
        req_spec=req_spec,
        rules_model=rules_model,
        tab_group_join_mode=tab_group_join_mode,
        group_rule_join_mode=group_rule_join_mode,
        entry_filters=entry_filters,
        cfg=cfg,
        bt_mode=bt_mode,
    )

    checks = [
        {
            "name": "run2_memory_cached_bundle",
            "passed": bool(run2.get("bundle_memory_cached")),
            "detail": f"run2.bundle_memory_cached={run2.get('bundle_memory_cached')}",
        },
        {
            "name": "run1_vs_run2_consistent_result",
            "passed": run1.get("ending_balance") == run2.get("ending_balance") and run1.get("num_trades") == run2.get("num_trades"),
            "detail": f"run1={run1.get('ending_balance')} / {run1.get('num_trades')} trades, run2={run2.get('ending_balance')} / {run2.get('num_trades')} trades",
        },
        {
            "name": "run2_load_faster_than_run1_load",
            "passed": float(run2.get("load_sec") or 0.0) < float(run1.get("load_sec") or 0.0),
            "detail": f"run1.load={run1.get('load_sec')}s, run2.load={run2.get('load_sec')}s",
        },
        {
            "name": "run2_total_faster_than_run1_total",
            "passed": float(run2.get("total_sec") or 0.0) < float(run1.get("total_sec") or 0.0),
            "detail": f"run1.total={run1.get('total_sec')}s, run2.total={run2.get('total_sec')}s",
        },
    ]

    summary = {
        "preset": preset_name,
        "symbol": symbol,
        "start": str(start),
        "end": str(end),
        "query_warmup": int(query_warmup),
        "compute_warmup": int(compute_warmup),
        "cache_dir": str(cache_dir),
        "run1": run1,
        "run2": run2,
        "checks": checks,
        "overall_pass": all(bool(check.get("passed")) for check in checks),
    }

    summary_path = output_dir / "summary.json"
    summary_path.write_text(json.dumps(summary, indent=2, ensure_ascii=False), encoding="utf-8")
    print(
        f"run1: source={run1['bundle_source']} memory_cached={run1['bundle_memory_cached']} load={run1['load_sec']}s total={run1['total_sec']}s",
        flush=True,
    )
    print(
        f"run2: source={run2['bundle_source']} memory_cached={run2['bundle_memory_cached']} load={run2['load_sec']}s total={run2['total_sec']}s",
        flush=True,
    )
    print(f"Saved in-process base reuse summary: {summary_path}", flush=True)
    return 0 if summary["overall_pass"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
