from __future__ import annotations

import argparse
import json
import sys
import time
from collections import defaultdict
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Dict, Iterator, List, Optional, Tuple

import pandas as pd

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from app.backtest_models import BacktestConfig
from app.data_binance import parse_utc
from app.fast_backtest import run_backtest_auto
from app.stream_bundle_loader import clear_stream_bundle_memory_cache, load_stream_bundle_library_first
from app.strategy_compiler import compile_strategy_plan
from app.strategy_requirements import compile_stream_requirements
from app.utils_time import required_warmup_minutes
from tools.run_saved_preset import (
    _load_presets,
    _materialize_preset,
    _pick_preset_name,
    _resolve_cache_dir,
)


@dataclass
class _TimingStat:
    calls: int = 0
    total_sec: float = 0.0


class _CallProfiler:
    def __init__(self) -> None:
        self.stats: Dict[str, _TimingStat] = defaultdict(_TimingStat)

    @contextmanager
    def patch(self, module: Any, attr_name: str, label: str) -> Iterator[None]:
        original = getattr(module, attr_name)

        def wrapped(*args: Any, **kwargs: Any) -> Any:
            started = time.perf_counter()
            try:
                return original(*args, **kwargs)
            finally:
                elapsed = time.perf_counter() - started
                stat = self.stats[label]
                stat.calls += 1
                stat.total_sec += elapsed

        setattr(module, attr_name, wrapped)
        try:
            yield
        finally:
            setattr(module, attr_name, original)


def _profiled_run(
    *,
    preset_name: str,
    preset: Dict[str, Any],
    repo_root: Path,
    cache_dir: Path,
    start: pd.Timestamp,
    end: pd.Timestamp,
    initial_balance: float,
    order_notional: float,
    fee_pct: float,
    slippage_bps: float,
) -> Dict[str, Any]:
    import app.backtest as backtest_mod
    import app.rules as rules_mod

    settings = preset.get("settings", {}) if isinstance(preset, dict) else {}
    symbol = str(settings.get("symbol", "BTCUSDT") or "BTCUSDT").strip().upper()
    price_source = str(settings.get("price_source", "LAST") or "LAST").strip().upper()
    macd_impl = str(settings.get("macd_impl", "TRADINGVIEW") or "TRADINGVIEW").strip().upper()
    adx_impl = str(settings.get("adx_impl", "TRADINGVIEW") or "TRADINGVIEW").strip().upper()
    bt_mode = str(settings.get("bt_mode", "Bar Close (1m candles)") or "Bar Close (1m candles)")
    timeframes = settings.get("timeframes", {}) if isinstance(settings.get("timeframes"), dict) else {}
    selected_timeframes = [str(tf) for tf, enabled in timeframes.items() if bool(enabled)]

    rules_model, tab_group_join_mode, group_rule_join_mode, entry_filters = _materialize_preset(preset)
    required_spec = compile_stream_requirements(
        rules_model,
        entry_filters=entry_filters,
        backtest_cfg=BacktestConfig(),
        include_plot_defaults=True,
    )
    plan = compile_strategy_plan(
        rules_model,
        tab_group_join_mode=tab_group_join_mode,
        group_rule_join_mode=group_rule_join_mode,
        entry_filters=entry_filters,
        backtest_cfg=BacktestConfig(),
        include_plot_defaults=False,
    )

    needed_warmup = required_warmup_minutes(selected_timeframes, required_fields=required_spec)
    query_warmup = max(0, int(settings.get("warmup_min", 0) or 0))
    compute_warmup = max(query_warmup, int(needed_warmup or 0))

    compute_start = start - pd.Timedelta(minutes=compute_warmup)
    bundle = load_stream_bundle_library_first(
        cache_dir=cache_dir,
        symbol=symbol,
        start=start,
        end=end,
        compute_start=compute_start,
        timeframes=selected_timeframes,
        price_source=price_source,
        macd_impl=macd_impl,
        adx_impl=adx_impl,
        required_fields=required_spec,
        progress_cb=None,
    )

    cfg = BacktestConfig(
        initial_balance=float(initial_balance),
        order_notional_usdt=float(order_notional),
        fee_rate=float(fee_pct) / 100.0,
        slippage_bps=float(slippage_bps),
        allow_reverse=True,
        skip_if_both_entries=True,
        step_timeframe=str(settings.get("step_timeframe", "1m") or "1m"),
        capture_trade_details=False,
        equity_curve_stride=15,
    )

    profiler = _CallProfiler()
    started = time.perf_counter()
    with profiler.patch(backtest_mod, "_entry_group_results_for_tab", "backtest._entry_group_results_for_tab"):
        with profiler.patch(backtest_mod, "_combine_entry_filter_signal_decisions", "backtest._combine_entry_filter_signal_decisions"):
            with profiler.patch(backtest_mod, "_combine_entry_filter_confirm_decisions", "backtest._combine_entry_filter_confirm_decisions"):
                with profiler.patch(backtest_mod, "eval_tab_matching_groups", "backtest.eval_tab_matching_groups"):
                    with profiler.patch(rules_mod, "eval_tab_matching_groups", "rules.eval_tab_matching_groups"):
                        with profiler.patch(rules_mod, "eval_group_generic", "rules.eval_group_generic"):
                            with profiler.patch(rules_mod, "eval_rule_generic", "rules.eval_rule_generic"):
                                result = run_backtest_auto(
                                    symbol=symbol,
                                    streams_full=bundle.streams_full,
                                    df_1m_full=bundle.df_1m,
                                    start=start,
                                    end=end,
                                    rules_model=rules_model,
                                    tab_group_join_mode=tab_group_join_mode,
                                    group_rule_join_mode=group_rule_join_mode,
                                    cfg=cfg,
                                    entry_filters=entry_filters,
                                    apply_htf_closed_only=False,
                                )
    total_wall_sec = time.perf_counter() - started

    stats_out: Dict[str, Dict[str, Any]] = {}
    for label, stat in sorted(profiler.stats.items(), key=lambda item: item[1].total_sec, reverse=True):
        share = (stat.total_sec / total_wall_sec * 100.0) if total_wall_sec > 0 else 0.0
        stats_out[label] = {
            "calls": int(stat.calls),
            "total_sec": float(stat.total_sec),
            "avg_ms": float((stat.total_sec / max(1, stat.calls)) * 1000.0),
            "share_pct": float(share),
        }

    return {
        "preset": str(preset_name),
        "symbol": symbol,
        "window": {
            "start": str(start),
            "end": str(end),
        },
        "plan": {
            "engine": str(plan.engine),
            "blockers": list(plan.blockers),
            "sequence_tabs": list(plan.sequence_tabs),
            "entry_filter_tabs": list(plan.entry_filter_tabs),
        },
        "bundle": {
            "source": str(bundle.source),
            "memory_cached": bool(getattr(bundle, "memory_cached", False)),
        },
        "result": {
            "ending_balance": float(result.summary.get("ending_balance", 0.0)),
            "num_trades": int(result.summary.get("num_trades", result.summary.get("trades", 0)) or 0),
            "exit_reason_counts": dict(result.summary.get("exit_reason_counts", {}) or {}),
        },
        "timing": {
            "total_wall_sec": float(total_wall_sec),
        },
        "profile": stats_out,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="Profile live heavy rule-engine paths for saved presets.")
    parser.add_argument("--presets-file", default=str(REPO_ROOT / "app" / "presets.json"))
    parser.add_argument("--preset", action="append", dest="presets", default=None, help="Preset name to include. Repeatable.")
    parser.add_argument("--start", required=True)
    parser.add_argument("--end", required=True)
    parser.add_argument("--cache-dir", default="data_cache")
    parser.add_argument("--initial-balance", type=float, default=1000.0)
    parser.add_argument("--order-notional", type=float, default=100.0)
    parser.add_argument("--fee-pct", type=float, default=0.04)
    parser.add_argument("--slippage-bps", type=float, default=0.0)
    parser.add_argument("--output-dir", required=True)
    args = parser.parse_args()

    presets_file = Path(args.presets_file).resolve()
    payload = _load_presets(presets_file)
    all_presets = payload.get("presets", {})
    if not isinstance(all_presets, dict) or not all_presets:
        raise ValueError("No presets found.")

    requested = list(args.presets or [])
    if not requested:
        requested = [_pick_preset_name(payload, None)]

    start = parse_utc(args.start)
    end = parse_utc(args.end)
    cache_dir = _resolve_cache_dir(REPO_ROOT, str(args.cache_dir))

    output_dir = Path(args.output_dir).resolve()
    output_dir.mkdir(parents=True, exist_ok=True)

    clear_stream_bundle_memory_cache()
    rows: List[Dict[str, Any]] = []
    for preset_name in requested:
        preset = all_presets.get(str(preset_name))
        if not isinstance(preset, dict):
            raise ValueError(f"Preset '{preset_name}' was not found.")
        print(f"Profiling preset {preset_name}...", flush=True)
        rows.append(
            _profiled_run(
                preset_name=str(preset_name),
                preset=preset,
                repo_root=REPO_ROOT,
                cache_dir=cache_dir,
                start=start,
                end=end,
                initial_balance=float(args.initial_balance),
                order_notional=float(args.order_notional),
                fee_pct=float(args.fee_pct),
                slippage_bps=float(args.slippage_bps),
            )
        )

    summary = {
        "generated_at_utc": str(pd.Timestamp.now(tz="UTC")),
        "output_dir": str(output_dir),
        "profiles": rows,
    }
    with (output_dir / "summary.json").open("w", encoding="utf-8") as fh:
        json.dump(summary, fh, ensure_ascii=False, indent=2)

    lines: List[str] = ["# Rule Engine Profile", ""]
    for row in rows:
        lines.append(f"## Preset {row['preset']}")
        lines.append("")
        lines.append(f"- Engine: `{row['plan']['engine']}`")
        lines.append(f"- Blockers: `{', '.join(row['plan']['blockers']) or 'none'}`")
        lines.append(f"- Total wall: `{row['timing']['total_wall_sec']:.4f}s`")
        lines.append(f"- Ending balance: `{row['result']['ending_balance']}`")
        lines.append(f"- Trades: `{row['result']['num_trades']}`")
        lines.append("")
        lines.append("| Function | Calls | Total sec | Avg ms | Share % |")
        lines.append("|---|---:|---:|---:|---:|")
        for label, stat in row["profile"].items():
            lines.append(
                f"| `{label}` | {int(stat['calls'])} | {float(stat['total_sec']):.4f} | {float(stat['avg_ms']):.3f} | {float(stat['share_pct']):.1f} |"
            )
        lines.append("")

    (output_dir / "report.md").write_text("\n".join(lines), encoding="utf-8")
    print(f"Saved profile summary: {output_dir / 'summary.json'}", flush=True)
    print(f"Saved profile report: {output_dir / 'report.md'}", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
