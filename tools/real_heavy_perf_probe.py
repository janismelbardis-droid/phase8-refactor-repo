from __future__ import annotations

import argparse
import json
import shutil
import subprocess
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List

import pandas as pd


REPO_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_START = "2025-11-01 00:00:00+00:00"
DEFAULT_END = "2026-04-20 04:00:00+00:00"
DEFAULT_TIMEFRAMES = "1m,5m,15m,30m,1h"
DEFAULT_INDICATOR_FAMILIES = "adx,atr,ema,frama,macd_lines,macd_states,ohlcv,range_filter,rsi,stoch_rsi,taker_bias,vidya"
DEFAULT_PRESETS = ("901", "902")


@dataclass(frozen=True)
class RunResult:
    label: str
    command: List[str]
    returncode: int
    wall_sec: float
    stdout_tail: List[str]
    stderr_tail: List[str]


def _run(label: str, command: List[str]) -> RunResult:
    started = time.perf_counter()
    proc = subprocess.run(
        command,
        cwd=str(REPO_ROOT),
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
        check=False,
    )
    wall = time.perf_counter() - started
    return RunResult(
        label=label,
        command=command,
        returncode=int(proc.returncode),
        wall_sec=round(wall, 4),
        stdout_tail=(proc.stdout or "").splitlines()[-120:],
        stderr_tail=(proc.stderr or "").splitlines()[-120:],
    )


def _load_json(path: Path) -> Dict[str, Any]:
    with path.open("r", encoding="utf-8") as fh:
        payload = json.load(fh)
    if not isinstance(payload, dict):
        raise ValueError(f"Expected JSON object at {path}")
    return payload


def _status(value: bool) -> str:
    return "PASS" if value else "FAIL"


def _prepare_cache(cache_dir: Path) -> None:
    cache_dir.mkdir(parents=True, exist_ok=True)
    source_ohlcv = REPO_ROOT / "data_cache" / "ohlcv_store"
    target_ohlcv = cache_dir / "ohlcv_store"
    if target_ohlcv.exists():
        shutil.rmtree(target_ohlcv)
    shutil.copytree(source_ohlcv, target_ohlcv)


def _indicator_build_command(cache_dir: Path, output_json: Path, start: str, end: str) -> List[str]:
    return [
        "python",
        str(REPO_ROOT / "scripts" / "manual" / "build_research_cache_profile.py"),
        "--symbol",
        "BTCUSDT",
        "--start",
        start,
        "--end",
        end,
        "--cache-dir",
        str(cache_dir),
        "--timeframes",
        DEFAULT_TIMEFRAMES,
        "--warmup-minutes",
        "600",
        "--indicator-families",
        DEFAULT_INDICATOR_FAMILIES,
        "--output-json",
        str(output_json),
    ]


def _saved_preset_command(preset: str, cache_dir: Path, output_json: Path, start: str, end: str) -> List[str]:
    return [
        "python",
        str(REPO_ROOT / "tools" / "run_saved_preset.py"),
        "--preset",
        preset,
        "--cache-dir",
        str(cache_dir),
        "--start",
        start,
        "--end",
        end,
        "--output-json",
        str(output_json),
    ]


def _engine_benchmark_command(preset: str, cache_dir: Path, output_json: Path, start: str, end: str) -> List[str]:
    return [
        "python",
        str(REPO_ROOT / "tools" / "benchmark_saved_preset_engines.py"),
        "--preset",
        preset,
        "--cache-dir",
        str(cache_dir),
        "--start",
        start,
        "--end",
        end,
        "--compare-legacy",
        "--output",
        str(output_json),
    ]


def _batch_command(preset: str, cache_dir: Path, output_dir: Path, start: str, end: str, repeat_each: int) -> List[str]:
    return [
        "python",
        str(REPO_ROOT / "tools" / "run_saved_preset_batch.py"),
        "--preset",
        preset,
        "--cache-dir",
        str(cache_dir),
        "--start",
        start,
        "--end",
        end,
        "--repeat-each",
        str(int(repeat_each)),
        "--output-dir",
        str(output_dir),
    ]


def _build_report(path: Path, payload: Dict[str, Any]) -> None:
    lines: List[str] = []
    lines.append("# Real Heavy Performance Probe")
    lines.append("")
    lines.append(f"- generated_utc: `{payload.get('generated_utc')}`")
    lines.append(f"- start_utc: `{payload.get('start_utc')}`")
    lines.append(f"- end_utc: `{payload.get('end_utc')}`")
    lines.append(f"- period_minutes: `{payload.get('period_minutes')}`")
    lines.append(f"- overall: `{_status(bool(payload.get('all_passed')))}`")
    lines.append("")
    lines.append("| Step | Status | Wall s | Key result |")
    lines.append("|---|---|---:|---|")
    for step in payload.get("steps", []):
        lines.append(
            f"| {step.get('label')} | {_status(bool(step.get('passed')))} | {step.get('wall_sec')} | {step.get('key_result')} |"
        )
    lines.append("")
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def main() -> int:
    parser = argparse.ArgumentParser(description="Run a heavier practical performance probe for indicator build and saved-preset backtests.")
    parser.add_argument("--start", default=DEFAULT_START)
    parser.add_argument("--end", default=DEFAULT_END)
    parser.add_argument("--repeat-each", type=int, default=10)
    parser.add_argument(
        "--output-dir",
        default="",
        help="Optional output directory. Defaults to runs/perf/real_heavy_perf_probe_<timestamp>.",
    )
    args = parser.parse_args()

    if str(args.output_dir or "").strip():
        output_dir = Path(args.output_dir).resolve()
    else:
        stamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
        output_dir = REPO_ROOT / "runs" / "perf" / f"real_heavy_perf_probe_{stamp}"
    output_dir.mkdir(parents=True, exist_ok=True)

    cache_dir = output_dir / "temp_cache"
    _prepare_cache(cache_dir)

    start_ts = pd.Timestamp(args.start)
    end_ts = pd.Timestamp(args.end)
    if start_ts.tzinfo is None:
        start_ts = start_ts.tz_localize("UTC")
    else:
        start_ts = start_ts.tz_convert("UTC")
    if end_ts.tzinfo is None:
        end_ts = end_ts.tz_localize("UTC")
    else:
        end_ts = end_ts.tz_convert("UTC")
    period_minutes = int((end_ts - start_ts).total_seconds() // 60)

    steps: List[Dict[str, Any]] = []

    indicator_cold_json = output_dir / "indicator_build_cold.json"
    indicator_hot_json = output_dir / "indicator_build_hot.json"
    cold_run = _run(
        "indicator_build_cold",
        _indicator_build_command(cache_dir, indicator_cold_json, str(start_ts), str(end_ts)),
    )
    cold_payload = _load_json(indicator_cold_json) if cold_run.returncode == 0 and indicator_cold_json.exists() else {}
    steps.append(
        {
            "label": cold_run.label,
            "passed": cold_run.returncode == 0,
            "wall_sec": cold_run.wall_sec,
            "key_result": f"indicator_build_returncode={cold_run.returncode}",
            "run": cold_run.__dict__,
            "payload": cold_payload,
        }
    )

    hot_run = _run(
        "indicator_build_hot",
        _indicator_build_command(cache_dir, indicator_hot_json, str(start_ts), str(end_ts)),
    )
    hot_payload = _load_json(indicator_hot_json) if hot_run.returncode == 0 and indicator_hot_json.exists() else {}
    steps.append(
        {
            "label": hot_run.label,
            "passed": hot_run.returncode == 0,
            "wall_sec": hot_run.wall_sec,
            "key_result": f"indicator_build_returncode={hot_run.returncode}",
            "run": hot_run.__dict__,
            "payload": hot_payload,
        }
    )

    for preset in DEFAULT_PRESETS:
        result_json = output_dir / f"saved_preset_{preset}.json"
        run = _run(
            f"saved_preset_{preset}",
            _saved_preset_command(preset, cache_dir, result_json, str(start_ts), str(end_ts)),
        )
        payload = _load_json(result_json) if run.returncode == 0 and result_json.exists() else {}
        summary = dict(payload.get("summary") or {})
        steps.append(
            {
                "label": run.label,
                "passed": run.returncode == 0,
                "wall_sec": run.wall_sec,
                "key_result": f"engine={summary.get('strategy_plan_engine')} trades={summary.get('num_trades')}",
                "run": run.__dict__,
                "payload": payload,
            }
        )

        bench_json = output_dir / f"benchmark_{preset}.json"
        bench_run = _run(
            f"benchmark_{preset}",
            _engine_benchmark_command(preset, cache_dir, bench_json, str(start_ts), str(end_ts)),
        )
        bench_payload = _load_json(bench_json) if bench_run.returncode == 0 and bench_json.exists() else {}
        fast_elapsed = dict(bench_payload.get("fast") or {}).get("elapsed_sec")
        legacy_elapsed = dict(bench_payload.get("legacy") or {}).get("elapsed_sec")
        steps.append(
            {
                "label": bench_run.label,
                "passed": bench_run.returncode == 0,
                "wall_sec": bench_run.wall_sec,
                "key_result": f"fast={fast_elapsed}s legacy={legacy_elapsed}s",
                "run": bench_run.__dict__,
                "payload": bench_payload,
            }
        )

        batch_dir = output_dir / f"batch_{preset}"
        batch_run = _run(
            f"batch_{preset}_repeat{int(args.repeat_each)}",
            _batch_command(preset, cache_dir, batch_dir, str(start_ts), str(end_ts), int(args.repeat_each)),
        )
        batch_payload = _load_json(batch_dir / "summary.json") if batch_run.returncode == 0 and (batch_dir / "summary.json").exists() else {}
        steps.append(
            {
                "label": batch_run.label,
                "passed": batch_run.returncode == 0,
                "wall_sec": batch_run.wall_sec,
                "key_result": f"jobs={batch_payload.get('jobs')} memory_hits={batch_payload.get('memory_cache_hits')}",
                "run": batch_run.__dict__,
                "payload": batch_payload,
            }
        )

    payload = {
        "generated_utc": datetime.now(timezone.utc).isoformat(),
        "repo_root": str(REPO_ROOT),
        "start_utc": start_ts.isoformat(),
        "end_utc": end_ts.isoformat(),
        "period_minutes": period_minutes,
        "cache_dir": str(cache_dir),
        "all_passed": all(bool(step.get("passed")) for step in steps),
        "steps": steps,
    }

    summary_path = output_dir / "summary.json"
    report_path = output_dir / "report.md"
    summary_path.write_text(json.dumps(payload, indent=2, ensure_ascii=False, default=str) + "\n", encoding="utf-8")
    _build_report(report_path, payload)

    latest_dir = REPO_ROOT / "runs" / "perf" / "real_heavy_perf_probe_latest"
    if latest_dir.exists():
        shutil.rmtree(latest_dir)
    shutil.copytree(output_dir, latest_dir)

    print(f"Probe output: {output_dir}", flush=True)
    print(f"Overall: {_status(bool(payload['all_passed']))}", flush=True)
    for step in steps:
        print(
            f"- {step['label']}: {_status(bool(step['passed']))} "
            f"(wall={step['wall_sec']}s, {step['key_result']})",
            flush=True,
        )

    return 0 if payload["all_passed"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
