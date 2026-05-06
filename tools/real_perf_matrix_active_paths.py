from __future__ import annotations

import argparse
import json
import math
from pathlib import Path
import shutil
import subprocess
import sys
import time
from typing import Any, Dict, List, Optional


REPO_ROOT = Path(__file__).resolve().parents[1]


def _bool_marker(text: str, needle: str) -> bool:
    return needle in text


def _safe_float(value: Any) -> Optional[float]:
    try:
        num = float(value)
    except Exception:
        return None
    if not math.isfinite(num):
        return None
    return float(num)


def _safe_int(value: Any) -> Optional[int]:
    try:
        return int(value)
    except Exception:
        return None


def _approx_equal(a: Any, b: Any, tol: float = 1e-9) -> bool:
    fa = _safe_float(a)
    fb = _safe_float(b)
    if fa is None or fb is None:
        return False
    return abs(fa - fb) <= float(tol)


def _cleanup_replay_dir(replay_dir: Path) -> None:
    if replay_dir.exists():
        shutil.rmtree(replay_dir)


def _load_json(path: Path) -> Dict[str, Any]:
    with path.open("r", encoding="utf-8") as fh:
        payload = json.load(fh)
    if not isinstance(payload, dict):
        raise ValueError(f"Expected JSON object at {path}")
    return payload


def _parse_base_metrics(path: Path) -> Dict[str, Any]:
    payload = _load_json(path)
    summary = payload.get("summary", {}) if isinstance(payload.get("summary"), dict) else {}
    return {
        "ending_balance": _safe_float(summary.get("ending_balance")),
        "return_pct": _safe_float(summary.get("return_pct")),
        "num_trades": _safe_int(summary.get("num_trades", summary.get("trades"))),
        "profit_factor": _safe_float(summary.get("profit_factor")),
        "max_drawdown_pct": _safe_float(summary.get("max_drawdown_pct")),
    }


def _parse_sweep_metrics(path: Path) -> Dict[str, Any]:
    payload = _load_json(path)
    best = payload.get("best_result", {}) if isinstance(payload.get("best_result"), dict) else {}
    return {
        "ending_balance": _safe_float(best.get("ending_balance")),
        "return_pct": _safe_float(best.get("return_pct")),
        "num_trades": _safe_int(best.get("num_trades")),
        "profit_factor": _safe_float(best.get("profit_factor")),
        "max_drawdown_pct": _safe_float(best.get("max_drawdown_pct")),
        "timing_total_wall_sec": _safe_float(payload.get("timing_total_wall_sec")),
        "timing_replay_grid_sec": _safe_float(payload.get("timing_replay_grid_sec")),
        "timing_stream_bundle_sec": _safe_float(payload.get("timing_stream_bundle_sec")),
        "timing_base_run_sec": _safe_float(payload.get("timing_base_run_sec")),
        "timing_verification_streams_sec": _safe_float(payload.get("timing_verification_streams_sec")),
        "timing_verification_artifacts_sec": _safe_float(payload.get("timing_verification_artifacts_sec")),
        "replay_workers_selected": _safe_int(payload.get("replay_workers_selected")),
    }


def _run_case(
    *,
    label: str,
    command: List[str],
    workdir: Path,
    metrics_kind: str,
    artifact_path: Path,
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

    metrics: Dict[str, Any] = {}
    if proc.returncode == 0 and artifact_path.exists():
        if metrics_kind == "base":
            metrics = _parse_base_metrics(artifact_path)
        elif metrics_kind == "sweep":
            metrics = _parse_sweep_metrics(artifact_path)

    return {
        "label": label,
        "returncode": int(proc.returncode),
        "wall_sec": round(elapsed, 4),
        "artifact_path": str(artifact_path),
        "metrics_kind": metrics_kind,
        "metrics": metrics,
        "used_replay_bundle": _bool_marker(combined, "Loaded replay bundle:"),
        "saved_replay_bundle": _bool_marker(combined, "Saved replay bundle:"),
        "loaded_cached_base_result": _bool_marker(combined, "Loaded cached base result."),
        "loaded_cached_verification_payload": _bool_marker(combined, "Loaded cached verification payload."),
        "loaded_local_indicator_store": _bool_marker(combined, "Loaded local indicator store."),
        "loaded_cached_data": _bool_marker(combined, "Loaded cached data (parquet)."),
        "loaded_verification_streams": _bool_marker(combined, "Loading stream bundle for verification artifacts..."),
        "ran_base_backtest": _bool_marker(combined, "Running base backtest without stop loss / take profit..."),
        "stdout_tail": stdout.splitlines()[-60:],
        "stderr_tail": stderr.splitlines()[-60:],
    }


def _make_check(name: str, passed: bool, detail: str) -> Dict[str, Any]:
    return {
        "name": name,
        "passed": bool(passed),
        "detail": str(detail),
    }


def _format_value(value: Any) -> str:
    if isinstance(value, bool):
        return "yes" if value else "no"
    if isinstance(value, (int, float)):
        if isinstance(value, float):
            if not math.isfinite(value):
                return str(value)
            return f"{value:.4f}"
        return str(value)
    if value is None:
        return "-"
    return str(value)


def _write_markdown_report(path: Path, payload: Dict[str, Any]) -> None:
    rows = payload.get("cases", {})
    checks = payload.get("checks", [])
    lines: List[str] = []
    lines.append("# Active Path Perf Matrix")
    lines.append("")
    lines.append(f"- preset: `{payload.get('preset')}`")
    lines.append(f"- window: `{payload.get('start')}` -> `{payload.get('end')}`")
    lines.append(f"- python: `{payload.get('python_exe')}`")
    lines.append("")
    lines.append("## Cases")
    lines.append("")
    lines.append("| Case | RC | Wall s | Ending balance | Trades | Replay bundle | Cached base | Cached verification | Local store | Cached candles | Verification reload | Base rerun | Replay workers |")
    lines.append("|---|---:|---:|---:|---:|---|---|---|---|---|---|---|---:|")
    for label, row in rows.items():
        metrics = row.get("metrics", {}) or {}
        lines.append(
            "| "
            + " | ".join(
                [
                    label,
                    _format_value(row.get("returncode")),
                    _format_value(row.get("wall_sec")),
                    _format_value(metrics.get("ending_balance")),
                    _format_value(metrics.get("num_trades")),
                    _format_value(row.get("used_replay_bundle")),
                    _format_value(row.get("loaded_cached_base_result")),
                    _format_value(row.get("loaded_cached_verification_payload")),
                    _format_value(row.get("loaded_local_indicator_store")),
                    _format_value(row.get("loaded_cached_data")),
                    _format_value(row.get("loaded_verification_streams")),
                    _format_value(row.get("ran_base_backtest")),
                    _format_value(metrics.get("replay_workers_selected")),
                ]
            )
            + " |"
        )
    lines.append("")
    lines.append("## Checks")
    lines.append("")
    lines.append("| Check | Status | Detail |")
    lines.append("|---|---|---|")
    for check in checks:
        lines.append(
            f"| {check.get('name')} | {'PASS' if check.get('passed') else 'FAIL'} | {check.get('detail')} |"
        )
    lines.append("")
    lines.append("## Notes")
    lines.append("")
    lines.append("- `hot` means second run after replay bundle already exists.")
    lines.append("- `cold` means replay cache was cleaned right before the run pair.")
    lines.append("- This matrix is based on live command execution, not unit tests.")
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def main() -> int:
    parser = argparse.ArgumentParser(description="Run a real cold/hot performance matrix for active preset paths.")
    parser.add_argument("--preset", default="011")
    parser.add_argument("--start", default="2026-04-20 00:00:00+00:00")
    parser.add_argument("--end", default="2026-04-20 04:00:00+00:00")
    parser.add_argument("--python", dest="python_exe", default=sys.executable)
    parser.add_argument(
        "--output-dir",
        default=str(REPO_ROOT / "runs" / "perf" / "real_perf_matrix_active_paths"),
    )
    args = parser.parse_args()

    python_exe = str(args.python_exe)
    output_dir = Path(args.output_dir).resolve()
    output_dir.mkdir(parents=True, exist_ok=True)

    cache_dir = REPO_ROOT / "data_cache"
    replay_dir = cache_dir / "replay_bundles" / "BTCUSDT"

    base_run1_json = output_dir / "base_run1.json"
    base_run2_json = output_dir / "base_run2.json"
    short_seq_cold_dir = output_dir / "short_seq_cold"
    short_seq_hot_dir = output_dir / "short_seq_hot"
    wide_seq_cold_dir = output_dir / "wide_seq_cold"
    wide_seq_hot_dir = output_dir / "wide_seq_hot"
    wide_auto_cold_dir = output_dir / "wide_auto_cold"
    wide_auto_hot_dir = output_dir / "wide_auto_hot"

    base_cmd = [
        python_exe,
        str(REPO_ROOT / "tools" / "run_saved_preset.py"),
        "--preset",
        str(args.preset),
        "--start",
        str(args.start),
        "--end",
        str(args.end),
        "--output-json",
    ]

    short_exit_cmd_prefix = [
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
        "--top-n",
        "1",
    ]

    wide_exit_cmd_prefix = [
        python_exe,
        str(REPO_ROOT / "tools" / "sweep_saved_preset_exits.py"),
        "--preset",
        str(args.preset),
        "--start",
        str(args.start),
        "--end",
        str(args.end),
        "--stop-values",
        "0,0.25,0.5,0.75,1,1.5,2,3,4",
        "--take-values",
        "0,0.25,0.5,0.75,1,1.5,2,3,4,6",
        "--top-n",
        "1",
    ]

    cases: Dict[str, Dict[str, Any]] = {}

    cases["base_run1"] = _run_case(
        label="base_run1",
        command=base_cmd + [str(base_run1_json)],
        workdir=REPO_ROOT,
        metrics_kind="base",
        artifact_path=base_run1_json,
    )
    cases["base_run2"] = _run_case(
        label="base_run2",
        command=base_cmd + [str(base_run2_json)],
        workdir=REPO_ROOT,
        metrics_kind="base",
        artifact_path=base_run2_json,
    )

    _cleanup_replay_dir(replay_dir)
    cases["short_seq_cold"] = _run_case(
        label="short_seq_cold",
        command=short_exit_cmd_prefix + ["--workers", "1", "--output-dir", str(short_seq_cold_dir)],
        workdir=REPO_ROOT,
        metrics_kind="sweep",
        artifact_path=short_seq_cold_dir / "summary.json",
    )
    cases["short_seq_hot"] = _run_case(
        label="short_seq_hot",
        command=short_exit_cmd_prefix + ["--workers", "1", "--output-dir", str(short_seq_hot_dir)],
        workdir=REPO_ROOT,
        metrics_kind="sweep",
        artifact_path=short_seq_hot_dir / "summary.json",
    )

    _cleanup_replay_dir(replay_dir)
    cases["wide_seq_cold"] = _run_case(
        label="wide_seq_cold",
        command=wide_exit_cmd_prefix + ["--workers", "1", "--output-dir", str(wide_seq_cold_dir)],
        workdir=REPO_ROOT,
        metrics_kind="sweep",
        artifact_path=wide_seq_cold_dir / "summary.json",
    )
    cases["wide_seq_hot"] = _run_case(
        label="wide_seq_hot",
        command=wide_exit_cmd_prefix + ["--workers", "1", "--output-dir", str(wide_seq_hot_dir)],
        workdir=REPO_ROOT,
        metrics_kind="sweep",
        artifact_path=wide_seq_hot_dir / "summary.json",
    )

    _cleanup_replay_dir(replay_dir)
    cases["wide_auto_cold"] = _run_case(
        label="wide_auto_cold",
        command=wide_exit_cmd_prefix + ["--workers", "0", "--output-dir", str(wide_auto_cold_dir)],
        workdir=REPO_ROOT,
        metrics_kind="sweep",
        artifact_path=wide_auto_cold_dir / "summary.json",
    )
    cases["wide_auto_hot"] = _run_case(
        label="wide_auto_hot",
        command=wide_exit_cmd_prefix + ["--workers", "0", "--output-dir", str(wide_auto_hot_dir)],
        workdir=REPO_ROOT,
        metrics_kind="sweep",
        artifact_path=wide_auto_hot_dir / "summary.json",
    )

    checks: List[Dict[str, Any]] = []

    all_ok = all(int(row.get("returncode", 1)) == 0 for row in cases.values())
    checks.append(_make_check("all_commands_succeeded", all_ok, "Every live command returned rc=0."))

    base1 = cases["base_run1"]["metrics"]
    base2 = cases["base_run2"]["metrics"]
    checks.append(
        _make_check(
            "base_repeat_consistent",
            _approx_equal(base1.get("ending_balance"), base2.get("ending_balance"))
            and base1.get("num_trades") == base2.get("num_trades"),
            f"base1={base1.get('ending_balance')} / {base1.get('num_trades')} trades, "
            f"base2={base2.get('ending_balance')} / {base2.get('num_trades')} trades",
        )
    )

    for case_name in ("short_seq_hot", "wide_seq_hot", "wide_auto_hot"):
        row = cases[case_name]
        checks.append(
            _make_check(
                f"{case_name}_reuse_path_clean",
                bool(row.get("used_replay_bundle"))
                and bool(row.get("loaded_cached_base_result"))
                and bool(row.get("loaded_cached_verification_payload"))
                and not bool(row.get("loaded_local_indicator_store"))
                and not bool(row.get("loaded_cached_data"))
                and not bool(row.get("loaded_verification_streams"))
                and not bool(row.get("ran_base_backtest")),
                (
                    f"bundle={row.get('used_replay_bundle')} base={row.get('loaded_cached_base_result')} "
                    f"verify={row.get('loaded_cached_verification_payload')} local_store={row.get('loaded_local_indicator_store')} "
                    f"candles={row.get('loaded_cached_data')} verify_reload={row.get('loaded_verification_streams')} "
                    f"base_rerun={row.get('ran_base_backtest')}"
                ),
            )
        )

    checks.append(
        _make_check(
            "wide_auto_hot_faster_than_wide_seq_hot",
            float(cases["wide_auto_hot"]["wall_sec"]) < float(cases["wide_seq_hot"]["wall_sec"]),
            (
                f"wide_auto_hot={cases['wide_auto_hot']['wall_sec']}s vs "
                f"wide_seq_hot={cases['wide_seq_hot']['wall_sec']}s"
            ),
        )
    )

    checks.append(
        _make_check(
            "wide_hot_results_consistent_between_seq_and_auto",
            _approx_equal(
                cases["wide_seq_hot"]["metrics"].get("ending_balance"),
                cases["wide_auto_hot"]["metrics"].get("ending_balance"),
            )
            and cases["wide_seq_hot"]["metrics"].get("num_trades")
            == cases["wide_auto_hot"]["metrics"].get("num_trades"),
            (
                f"wide_seq_hot={cases['wide_seq_hot']['metrics'].get('ending_balance')} / "
                f"{cases['wide_seq_hot']['metrics'].get('num_trades')} trades, "
                f"wide_auto_hot={cases['wide_auto_hot']['metrics'].get('ending_balance')} / "
                f"{cases['wide_auto_hot']['metrics'].get('num_trades')} trades"
            ),
        )
    )

    checks.append(
        _make_check(
            "wide_seq_hot_faster_than_wide_seq_cold",
            float(cases["wide_seq_hot"]["wall_sec"]) < float(cases["wide_seq_cold"]["wall_sec"]),
            f"hot={cases['wide_seq_hot']['wall_sec']}s vs cold={cases['wide_seq_cold']['wall_sec']}s",
        )
    )
    checks.append(
        _make_check(
            "wide_auto_hot_faster_than_wide_auto_cold",
            float(cases["wide_auto_hot"]["wall_sec"]) < float(cases["wide_auto_cold"]["wall_sec"]),
            f"hot={cases['wide_auto_hot']['wall_sec']}s vs cold={cases['wide_auto_cold']['wall_sec']}s",
        )
    )

    payload = {
        "preset": str(args.preset),
        "start": str(args.start),
        "end": str(args.end),
        "python_exe": python_exe,
        "repo_root": str(REPO_ROOT),
        "cache_dir": str(cache_dir),
        "replay_dir": str(replay_dir),
        "cases": cases,
        "checks": checks,
        "overall_pass": all(bool(check.get("passed")) for check in checks),
    }

    summary_json = output_dir / "summary.json"
    summary_md = output_dir / "report.md"
    summary_json.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")
    _write_markdown_report(summary_md, payload)

    for label, row in cases.items():
        metrics = row.get("metrics", {}) or {}
        print(
            f"{label}: rc={row['returncode']} wall={row['wall_sec']}s "
            f"bundle={row['used_replay_bundle']} cached_base={row['loaded_cached_base_result']} "
            f"cached_verify={row['loaded_cached_verification_payload']} "
            f"local_store={row['loaded_local_indicator_store']} "
            f"balance={metrics.get('ending_balance')} trades={metrics.get('num_trades')}",
            flush=True,
        )
    print(f"Saved perf matrix summary: {summary_json}", flush=True)
    print(f"Saved perf matrix report: {summary_md}", flush=True)
    if not payload["overall_pass"]:
        print("One or more perf matrix checks failed.", flush=True)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
