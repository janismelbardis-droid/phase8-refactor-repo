from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
import re
import shutil
import subprocess
import sys
import time
from typing import Any, Dict, List


REPO_ROOT = Path(__file__).resolve().parents[1]


def _run_case(
    *,
    label: str,
    command: List[str],
    workdir: Path,
) -> Dict[str, Any]:
    started = time.perf_counter()
    proc = subprocess.run(
        command,
        cwd=str(workdir),
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
        check=False,
    )
    elapsed = time.perf_counter() - started
    stdout = str(proc.stdout or "")
    stderr = str(proc.stderr or "")
    combined = stdout + ("\n" + stderr if stderr else "")

    warmup_match = re.search(
        r"Warmup minutes:\s*preset=(\d+), indicator_required=(\d+), query_used=(\d+), compute_used=(\d+)",
        combined,
    )
    return {
        "label": label,
        "returncode": int(proc.returncode),
        "wall_sec": round(elapsed, 4),
        "used_replay_bundle": "Loaded replay bundle:" in combined,
        "saved_replay_bundle": "Saved replay bundle:" in combined,
        "loaded_local_indicator_store": "Loaded local indicator store." in combined,
        "loaded_cached_data": "Loaded cached data (parquet)." in combined,
        "ran_base_backtest": "Running base backtest without stop loss / take profit..." in combined,
        "warmup": {
            "preset_min": int(warmup_match.group(1)) if warmup_match else None,
            "required_min": int(warmup_match.group(2)) if warmup_match else None,
            "query_used_min": int(warmup_match.group(3)) if warmup_match else None,
            "compute_used_min": int(warmup_match.group(4)) if warmup_match else None,
        },
        "stdout_tail": stdout.splitlines()[-40:],
        "stderr_tail": stderr.splitlines()[-40:],
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="Run real active-path perf checks on base preset and exit sweep reuse.")
    parser.add_argument("--preset", default="011")
    parser.add_argument("--start", default="2026-04-20 00:00:00+00:00")
    parser.add_argument("--end", default="2026-04-20 04:00:00+00:00")
    parser.add_argument("--python", dest="python_exe", default=sys.executable)
    parser.add_argument("--output-dir", default=str(REPO_ROOT / "runs" / "perf" / "real_active_perf_check"))
    parser.add_argument("--clean-replay-cache", action="store_true", default=False)
    args = parser.parse_args()

    python_exe = str(args.python_exe)
    output_dir = Path(args.output_dir).resolve()
    output_dir.mkdir(parents=True, exist_ok=True)

    cache_dir = REPO_ROOT / "data_cache"
    replay_dir = cache_dir / "replay_bundles" / "BTCUSDT"
    if args.clean_replay_cache and replay_dir.exists():
        shutil.rmtree(replay_dir)

    base_output = output_dir / "base_run.json"
    exit_run1_dir = output_dir / "exit_run1"
    exit_run2_dir = output_dir / "exit_run2"

    cases = [
        {
            "label": "base_run_saved_preset",
            "command": [
                python_exe,
                str(REPO_ROOT / "tools" / "run_saved_preset.py"),
                "--preset",
                str(args.preset),
                "--start",
                str(args.start),
                "--end",
                str(args.end),
                "--output-json",
                str(base_output),
            ],
        },
        {
            "label": "exit_sweep_run1",
            "command": [
                python_exe,
                str(REPO_ROOT / "tools" / "sweep_saved_preset_exits.py"),
                "--preset",
                str(args.preset),
                "--start",
                str(args.start),
                "--end",
                str(args.end),
                "--stop-values",
                "0,4",
                "--take-values",
                "0,6",
                "--workers",
                "1",
                "--top-n",
                "1",
                "--output-dir",
                str(exit_run1_dir),
            ],
        },
        {
            "label": "exit_sweep_run2",
            "command": [
                python_exe,
                str(REPO_ROOT / "tools" / "sweep_saved_preset_exits.py"),
                "--preset",
                str(args.preset),
                "--start",
                str(args.start),
                "--end",
                str(args.end),
                "--stop-values",
                "0,4",
                "--take-values",
                "0,6",
                "--workers",
                "1",
                "--top-n",
                "1",
                "--output-dir",
                str(exit_run2_dir),
            ],
        },
    ]

    results: List[Dict[str, Any]] = []
    for case in cases:
        result = _run_case(
            label=str(case["label"]),
            command=list(case["command"]),
            workdir=REPO_ROOT,
        )
        results.append(result)

    payload = {
        "python_exe": python_exe,
        "repo_root": str(REPO_ROOT),
        "cache_dir": str(cache_dir),
        "replay_dir": str(replay_dir),
        "clean_replay_cache": bool(args.clean_replay_cache),
        "results": results,
    }
    (output_dir / "summary.json").write_text(
        json.dumps(payload, indent=2, ensure_ascii=False),
        encoding="utf-8",
    )

    for row in results:
        print(
            f"{row['label']}: rc={row['returncode']} wall_sec={row['wall_sec']} "
            f"query_warmup={row['warmup']['query_used_min']} "
            f"compute_warmup={row['warmup']['compute_used_min']} "
            f"local_store={row['loaded_local_indicator_store']} "
            f"saved_bundle={row['saved_replay_bundle']} "
            f"used_bundle={row['used_replay_bundle']} "
            f"ran_base_backtest={row['ran_base_backtest']}",
            flush=True,
        )
    print(f"Saved real perf summary: {output_dir / 'summary.json'}", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
