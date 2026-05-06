from __future__ import annotations

import argparse
import json
import math
from pathlib import Path
import sys
import time
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from app.backtest_models import BacktestConfig
from app.stream_bundle_loader import clear_stream_bundle_memory_cache
from app.strategy_requirements import compile_stream_requirements
from app.utils_time import parse_utc, required_warmup_minutes
from tools.run_saved_preset import (
    execute_saved_preset_job,
    _load_presets,
    _materialize_preset,
    _pick_preset_name,
    _resolve_cache_dir,
    _safe_round,
)


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


def _load_jobs_file(path: Path) -> List[Dict[str, Any]]:
    payload = json.loads(path.read_text(encoding="utf-8-sig"))
    if not isinstance(payload, list):
        raise ValueError("Jobs file must be a JSON list.")
    jobs: List[Dict[str, Any]] = []
    for item in payload:
        if isinstance(item, dict):
            jobs.append(dict(item))
    if not jobs:
        raise ValueError("Jobs file contains no valid job objects.")
    return jobs


def _normalized_job_signature(job: Dict[str, Any]) -> Tuple[str, str, str]:
    return (
        str(job.get("preset_name") or ""),
        str(job.get("start") or ""),
        str(job.get("end") or ""),
    )


def _safe_float(value: Any) -> Any:
    try:
        num = float(value)
    except Exception:
        return value
    if not math.isfinite(num):
        return str(num)
    return float(num)


def _jsonable_value(value: Any) -> Any:
    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    if isinstance(value, Path):
        return str(value)
    if isinstance(value, pd.Timestamp):
        return pd.to_datetime(value, utc=True).isoformat()
    if isinstance(value, dict):
        return {str(k): _jsonable_value(v) for k, v in sorted(value.items(), key=lambda kv: str(kv[0]))}
    if isinstance(value, (list, tuple)):
        return [_jsonable_value(v) for v in value]
    if isinstance(value, (set, frozenset)):
        return sorted((_jsonable_value(v) for v in value), key=lambda item: json.dumps(item, sort_keys=True, ensure_ascii=False, default=str))
    try:
        return value.to_dict()
    except Exception:
        return str(value)


def _bundle_affinity_key(job: Dict[str, Any]) -> str:
    req_spec = job.get("req_spec")
    required_fields = []
    required_families = []
    try:
        required_fields = sorted(str(v) for v in req_spec.normalized_fields(include_defaults=True))
    except Exception:
        pass
    try:
        required_families = sorted(str(v) for v in req_spec.normalized_families(include_defaults=True))
    except Exception:
        pass
    payload = {
        "symbol": str(job.get("symbol") or ""),
        "start": str(job.get("start") or ""),
        "end": str(job.get("end") or ""),
        "preset_warmup": int(job.get("preset_warmup", 0) or 0),
        "needed_warmup": int(job.get("needed_warmup", 0) or 0),
        "timeframes": [str(tf) for tf in list(job.get("tfs") or [])],
        "cache_dir": str(job.get("cache_dir") or ""),
        "bt_mode": str(job.get("bt_mode") or ""),
        "price_source": str(job.get("price_source") or ""),
        "macd_impl": str(job.get("macd_impl") or ""),
        "adx_impl": str(job.get("adx_impl") or ""),
        "required_fields": required_fields,
        "required_families": required_families,
    }
    return json.dumps(_jsonable_value(payload), sort_keys=True, ensure_ascii=False, separators=(",", ":"))


def _replay_affinity_key(job: Dict[str, Any], cfg: BacktestConfig) -> str:
    payload = {
        "preset_name": str(job.get("preset_name") or ""),
        "symbol": str(job.get("symbol") or ""),
        "start": str(job.get("start") or ""),
        "end": str(job.get("end") or ""),
        "bundle_affinity_key": str(job.get("bundle_affinity_key") or ""),
        "step_timeframe": str(getattr(cfg, "step_timeframe", "") or ""),
        "allow_reverse": bool(getattr(cfg, "allow_reverse", False)),
        "skip_if_both_entries": bool(getattr(cfg, "skip_if_both_entries", False)),
        "stop_loss_mode": str(getattr(cfg, "stop_loss_mode", "") or ""),
        "stop_loss_value": float(getattr(cfg, "stop_loss_value", 0.0) or 0.0),
        "take_profit_mode": str(getattr(cfg, "take_profit_mode", "") or ""),
        "take_profit_value": float(getattr(cfg, "take_profit_value", 0.0) or 0.0),
    }
    return json.dumps(_jsonable_value(payload), sort_keys=True, ensure_ascii=False, separators=(",", ":"))


def _build_cfg(*, args: argparse.Namespace) -> BacktestConfig:
    return BacktestConfig(
        initial_balance=float(args.initial_balance),
        order_notional_usdt=float(args.order_notional),
        fee_rate=float(args.fee_pct) / 100.0,
        slippage_bps=float(args.slippage_bps),
        stop_loss_pct=0.0,
        take_profit_pct=0.0,
        stop_loss_mode="PERCENT",
        stop_loss_value=0.0,
        stop_loss_timeframe="1m",
        stop_loss_field="",
        stop_loss_offset_pct=0.0,
        take_profit_mode="PERCENT",
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


def _run_single_job(
    *,
    job_index: int,
    total_jobs: int,
    preset_name: str,
    symbol: str,
    start: pd.Timestamp,
    end: pd.Timestamp,
    tfs: List[str],
    cache_dir: Path,
    bt_mode: str,
    price_source: str,
    macd_impl: str,
    adx_impl: str,
    rules_model: Dict[str, Any],
    tab_group_join_mode: Dict[str, str],
    group_rule_join_mode: Dict[str, List[str]],
    entry_filters: Dict[str, Any],
    cfg: BacktestConfig,
    req_spec: Any,
    preset_warmup: int,
    needed_warmup: int,
    replay_context: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    query_warmup = max(0, int(preset_warmup))
    compute_warmup = max(query_warmup, int(needed_warmup or 0))

    print(
        f"Job {job_index}/{total_jobs}: preset={preset_name} window={start} -> {end}",
        flush=True,
    )

    execution = execute_saved_preset_job(
        repo_root=REPO_ROOT,
        preset_name=preset_name,
        symbol=symbol,
        start=start,
        end=end,
        tfs=tfs,
        cache_dir=cache_dir,
        bt_mode=bt_mode,
        price_source=price_source,
        macd_impl=macd_impl,
        adx_impl=adx_impl,
        rules_model=rules_model,
        tab_group_join_mode=tab_group_join_mode,
        group_rule_join_mode=group_rule_join_mode,
        entry_filters=entry_filters,
        cfg=cfg,
        req_spec=req_spec,
        query_warmup=query_warmup,
        compute_warmup=compute_warmup,
        progress_cb=None,
        collect_event_activity=False,
        collect_signal_rows=False,
        write_verification_bundle=False,
        replay_context=replay_context,
    )
    bundle = execution["bundle"]
    summary = execution["summary"]
    load_sec = float(execution.get("timing_load_bundle_sec", 0.0) or 0.0)
    run_sec = float(execution.get("timing_run_engine_sec", 0.0) or 0.0)
    total_sec = float(execution.get("timing_job_total_sec", load_sec + run_sec) or (load_sec + run_sec))

    row = {
        "job_index": int(job_index),
        "preset_name": str(preset_name),
        "symbol": str(symbol),
        "start": str(start),
        "end": str(end),
        "bundle_source": str(bundle.source or ""),
        "bundle_memory_cached": bool(getattr(bundle, "memory_cached", False)),
        "query_warmup_min": int(query_warmup),
        "compute_warmup_min": int(compute_warmup),
        "load_sec": round(float(load_sec), 4),
        "run_sec": round(float(run_sec), 4),
        "total_sec": round(float(total_sec), 4),
        "ending_balance": _safe_float(summary.get("ending_balance")),
        "return_pct": _safe_float(summary.get("return_pct")),
        "num_trades": int(summary.get("num_trades", summary.get("trades", 0)) or 0),
        "profit_factor": _safe_float(summary.get("profit_factor")),
        "max_drawdown_pct": _safe_float(summary.get("max_drawdown_pct")),
        "strategy_plan_engine": str(summary.get("strategy_plan_engine", "")),
        "replay_context_cached": bool(replay_context),
    }
    row["__next_replay_context"] = execution.get("replay_context")
    print(
        "  "
        + f"bundle={row['bundle_source']}"
        + (" (memory)" if row["bundle_memory_cached"] else "")
        + f", load={row['load_sec']}s, total={row['total_sec']}s, "
        + f"ending_balance={_safe_round(row['ending_balance'], 6)}, trades={row['num_trades']}",
        flush=True,
    )
    return row


def main() -> int:
    repo_root = REPO_ROOT
    default_presets = repo_root / "app" / "presets.json"

    parser = argparse.ArgumentParser(description="Run many saved preset jobs in one persistent Python process.")
    parser.add_argument("--preset", dest="preset_name", default=None)
    parser.add_argument("--presets-file", dest="presets_file", default=str(default_presets))
    parser.add_argument("--cache-dir", dest="cache_dir", default=None)
    parser.add_argument("--start", dest="start_override", default=None)
    parser.add_argument("--end", dest="end_override", default=None)
    parser.add_argument("--windows", default=None, help="Semicolon-separated windows formatted as start|end.")
    parser.add_argument("--jobs-file", default=None, help="Optional JSON file with a list of jobs.")
    parser.add_argument("--repeat-each", type=int, default=1)
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
    parser.add_argument("--clear-memory-cache-first", action="store_true", default=True)
    parser.add_argument("--output-dir", default=None)
    args = parser.parse_args()

    payload = _load_presets(Path(args.presets_file).resolve())
    default_preset_name = _pick_preset_name(payload, args.preset_name)
    default_preset = payload["presets"][default_preset_name]

    cfg = _build_cfg(args=args)

    raw_jobs: List[Dict[str, Any]] = []
    if args.jobs_file:
        raw_jobs = _load_jobs_file(Path(args.jobs_file).resolve())
    elif args.windows:
        for start, end in _parse_windows(args.windows):
            raw_jobs.append({"preset": default_preset_name, "start": str(start), "end": str(end)})
    else:
        settings = default_preset.get("settings", {})
        raw_jobs.append(
            {
                "preset": default_preset_name,
                "start": str(args.start_override or settings.get("start", "") or ""),
                "end": str(args.end_override or settings.get("end", "") or ""),
            }
        )

    jobs_expanded: List[Dict[str, Any]] = []
    repeat_each = max(1, int(args.repeat_each or 1))
    for raw_job in raw_jobs:
        preset_name = str(raw_job.get("preset") or default_preset_name).strip() or default_preset_name
        if preset_name not in payload.get("presets", {}):
            raise ValueError(f"Preset '{preset_name}' was not found.")
        for repeat_index in range(repeat_each):
            next_job = dict(raw_job)
            next_job["preset_name"] = preset_name
            next_job["repeat_index"] = int(repeat_index)
            jobs_expanded.append(next_job)

    if bool(args.clear_memory_cache_first):
        clear_stream_bundle_memory_cache()

    prepared_presets: Dict[str, Dict[str, Any]] = {}
    for preset_name in sorted({str(job.get("preset_name") or default_preset_name) for job in jobs_expanded}):
        preset = payload["presets"][preset_name]
        settings = preset.get("settings", {})
        if not isinstance(settings, dict):
            raise ValueError(f"Preset '{preset_name}' settings payload is invalid.")
        tfs = [tf for tf, enabled in (settings.get("timeframes", {}) or {}).items() if bool(enabled)]
        if "1m" not in tfs:
            tfs = ["1m"] + tfs
        if args.step_timeframe not in tfs:
            tfs.append(args.step_timeframe)
        rules_model, tab_group_join_mode, group_rule_join_mode, entry_filters = _materialize_preset(preset)
        req_spec = compile_stream_requirements(
            rules_model,
            entry_filters=entry_filters,
            backtest_cfg=cfg,
            include_plot_defaults=True,
        )
        cache_dir = _resolve_cache_dir(
            repo_root,
            args.cache_dir or str(settings.get("cache_dir", "data_cache") or "data_cache"),
        )
        cache_dir.mkdir(parents=True, exist_ok=True)
        prepared_presets[preset_name] = {
            "settings": settings,
            "tfs": tfs,
            "rules_model": rules_model,
            "tab_group_join_mode": tab_group_join_mode,
            "group_rule_join_mode": group_rule_join_mode,
            "entry_filters": entry_filters,
            "req_spec": req_spec,
            "cache_dir": cache_dir,
            "preset_warmup": int(settings.get("warmup_min", 0) or 0),
            "needed_warmup": required_warmup_minutes(tfs, required_fields=req_spec),
        }

    prepared_jobs: List[Dict[str, Any]] = []
    for raw_job in jobs_expanded:
        preset_name = str(raw_job.get("preset_name") or default_preset_name)
        prepared = prepared_presets[preset_name]
        settings = prepared["settings"]
        start = parse_utc(str(raw_job.get("start") or args.start_override or settings.get("start", "") or ""))
        end = parse_utc(str(raw_job.get("end") or args.end_override or settings.get("end", "") or ""))
        if end <= start:
            raise ValueError(f"Preset '{preset_name}' end must be after start.")
        next_job = {
            "preset_name": preset_name,
            "start": start,
            "end": end,
            "repeat_index": int(raw_job.get("repeat_index", 0) or 0),
            "symbol": str(settings.get("symbol", "BTCUSDT") or "BTCUSDT").strip().upper(),
            "tfs": list(prepared["tfs"]),
            "cache_dir": prepared["cache_dir"],
            "bt_mode": str(settings.get("bt_mode", "") or ""),
            "price_source": str(settings.get("price_source", "LAST") or "LAST").upper(),
            "macd_impl": str(settings.get("macd_impl", "TRADINGVIEW") or "TRADINGVIEW").upper(),
            "adx_impl": str(settings.get("adx_impl", "TRADINGVIEW") or "TRADINGVIEW").upper(),
            "rules_model": prepared["rules_model"],
            "tab_group_join_mode": prepared["tab_group_join_mode"],
            "group_rule_join_mode": prepared["group_rule_join_mode"],
            "entry_filters": prepared["entry_filters"],
            "req_spec": prepared["req_spec"],
            "preset_warmup": int(prepared["preset_warmup"]),
            "needed_warmup": int(prepared["needed_warmup"]),
        }
        next_job["bundle_affinity_key"] = _bundle_affinity_key(next_job)
        prepared_jobs.append(next_job)

    prepared_jobs.sort(
        key=lambda job: (
            str(job.get("bundle_affinity_key") or ""),
            str(job.get("preset_name") or ""),
            str(job.get("start") or ""),
            str(job.get("end") or ""),
            int(job.get("repeat_index", 0) or 0),
        )
    )

    rows: List[Dict[str, Any]] = []
    replay_context_cache: Dict[str, Dict[str, Any]] = {}
    session_started = time.perf_counter()
    total_jobs = len(prepared_jobs)
    for idx, job in enumerate(prepared_jobs, start=1):
        replay_key = _replay_affinity_key(job, cfg)
        cached_replay_context = replay_context_cache.get(replay_key)
        row = _run_single_job(
            job_index=idx,
            total_jobs=total_jobs,
            preset_name=str(job["preset_name"]),
            symbol=str(job["symbol"]),
            start=pd.to_datetime(job["start"], utc=True),
            end=pd.to_datetime(job["end"], utc=True),
            tfs=list(job["tfs"]),
            cache_dir=Path(job["cache_dir"]).resolve(),
            bt_mode=str(job["bt_mode"]),
            price_source=str(job["price_source"]),
            macd_impl=str(job["macd_impl"]),
            adx_impl=str(job["adx_impl"]),
            rules_model=dict(job["rules_model"]),
            tab_group_join_mode=dict(job["tab_group_join_mode"]),
            group_rule_join_mode={k: list(v) for k, v in dict(job["group_rule_join_mode"]).items()},
            entry_filters=dict(job["entry_filters"]),
            cfg=cfg,
            req_spec=job["req_spec"],
            preset_warmup=int(job["preset_warmup"]),
            needed_warmup=int(job["needed_warmup"]),
            replay_context=cached_replay_context,
        )
        row["repeat_index"] = int(job.get("repeat_index", 0) or 0)
        row["bundle_affinity_key"] = str(job.get("bundle_affinity_key") or "")
        row["replay_affinity_key"] = replay_key
        next_replay_context = row.pop("__next_replay_context", None)
        if isinstance(next_replay_context, dict):
            replay_context_cache[replay_key] = next_replay_context
        rows.append(row)

    wall_sec = time.perf_counter() - session_started
    df = pd.DataFrame(rows)
    output_dir = Path(args.output_dir).resolve() if args.output_dir else (
        repo_root / "runs" / f"saved_preset_batch_{pd.Timestamp.now(tz='UTC').strftime('%Y%m%d_%H%M%S')}"
    )
    output_dir.mkdir(parents=True, exist_ok=True)
    df.to_csv(output_dir / "jobs.csv", index=False)

    memory_hits = int(df["bundle_memory_cached"].fillna(False).astype(bool).sum()) if not df.empty else 0
    replay_hits = int(df["replay_context_cached"].fillna(False).astype(bool).sum()) if not df.empty else 0
    summary = {
        "jobs": int(len(rows)),
        "memory_cache_hits": int(memory_hits),
        "replay_context_hits": int(replay_hits),
        "session_wall_sec": round(wall_sec, 4),
        "avg_total_sec": _safe_float(df["total_sec"].mean()) if not df.empty else None,
        "avg_load_sec": _safe_float(df["load_sec"].mean()) if not df.empty else None,
        "avg_run_sec": _safe_float(df["run_sec"].mean()) if not df.empty else None,
        "best_total_sec": _safe_float(df["total_sec"].min()) if not df.empty else None,
        "worst_total_sec": _safe_float(df["total_sec"].max()) if not df.empty else None,
        "jobs_file": str(Path(args.jobs_file).resolve()) if args.jobs_file else None,
        "windows": args.windows,
        "repeat_each": int(repeat_each),
    }
    (output_dir / "summary.json").write_text(json.dumps(summary, indent=2, ensure_ascii=False), encoding="utf-8")

    print("", flush=True)
    print(
        f"Batch finished: jobs={summary['jobs']} memory_hits={summary['memory_cache_hits']} "
        f"session_wall_sec={summary['session_wall_sec']}",
        flush=True,
    )
    print(f"Saved jobs: {output_dir / 'jobs.csv'}", flush=True)
    print(f"Saved summary: {output_dir / 'summary.json'}", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
