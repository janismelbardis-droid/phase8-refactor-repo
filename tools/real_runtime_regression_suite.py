from __future__ import annotations

import argparse
import csv
import json
import math
from datetime import datetime, timezone
from pathlib import Path
import shutil
import subprocess
import sys
import time
from typing import Any, Dict, List, Optional


REPO_ROOT = Path(__file__).resolve().parents[1]

EXPECTED_BATCH_BALANCE_901 = 999.9639765918721
EXPECTED_PULLBACK_BASELINE_BALANCE_902 = 1000.5298811911158


def _safe_float(value: Any) -> Optional[float]:
    try:
        num = float(value)
    except Exception:
        return None
    if not math.isfinite(num):
        return float(num)
    return float(num)


def _safe_int(value: Any) -> Optional[int]:
    try:
        return int(value)
    except Exception:
        return None


def _approx_equal(left: Any, right: Any, tol: float = 1e-9) -> bool:
    a = _safe_float(left)
    b = _safe_float(right)
    if a is None or b is None:
        return False
    if math.isnan(a) or math.isnan(b):
        return False
    return abs(a - b) <= float(tol)


def _bool_marker(text: str, needle: str) -> bool:
    return needle in text


def _load_json(path: Path) -> Dict[str, Any]:
    with path.open("r", encoding="utf-8") as fh:
        payload = json.load(fh)
    if not isinstance(payload, dict):
        raise ValueError(f"Expected JSON object at {path}")
    return payload


def _load_csv_rows(path: Path) -> List[Dict[str, str]]:
    with path.open("r", encoding="utf-8", newline="") as fh:
        return [dict(row) for row in csv.DictReader(fh)]


def _make_check(name: str, passed: bool, detail: str) -> Dict[str, Any]:
    return {
        "name": str(name),
        "passed": bool(passed),
        "detail": str(detail),
    }


def _run_command(*, label: str, command: List[str], workdir: Path) -> Dict[str, Any]:
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
    wall_sec = time.perf_counter() - started
    stdout = str(proc.stdout or "")
    stderr = str(proc.stderr or "")
    combined = stdout + ("\n" + stderr if stderr else "")
    return {
        "label": str(label),
        "command": command,
        "returncode": int(proc.returncode),
        "wall_sec": round(wall_sec, 4),
        "stdout_tail": stdout.splitlines()[-120:],
        "stderr_tail": stderr.splitlines()[-120:],
        "markers": {
            "loaded_cached_data": _bool_marker(combined, "Loaded cached data (parquet)."),
            "loaded_local_indicator_store": _bool_marker(combined, "Loaded local indicator store."),
            "loaded_replay_bundle": _bool_marker(combined, "Loaded replay bundle:"),
            "loaded_cached_base_result": _bool_marker(combined, "Loaded cached base result."),
            "loaded_cached_verification_payload": _bool_marker(combined, "Loaded cached verification payload."),
            "saved_replay_bundle": _bool_marker(combined, "Saved replay bundle:"),
        },
    }


def _parity_case(*, python_exe: str, output_dir: Path) -> Dict[str, Any]:
    case_dir = output_dir / "parity_suite"
    command = [
        python_exe,
        str(REPO_ROOT / "tools" / "real_backtest_parity_suite.py"),
        "--output-dir",
        str(case_dir),
    ]
    run = _run_command(label="parity_suite", command=command, workdir=REPO_ROOT)
    summary_path = case_dir / "summary.json"
    summary = _load_json(summary_path) if run["returncode"] == 0 and summary_path.exists() else {}
    checks = [
        _make_check("parity_returncode_zero", run["returncode"] == 0, f"returncode={run['returncode']}"),
        _make_check("parity_summary_exists", summary_path.exists(), str(summary_path)),
        _make_check("parity_all_passed", bool(summary.get("all_passed")), f"all_passed={summary.get('all_passed')}"),
    ]
    return {
        "label": "parity_suite",
        "run": run,
        "summary_path": str(summary_path),
        "summary": summary,
        "checks": checks,
    }


def _trade_parity_case(*, python_exe: str, output_dir: Path) -> Dict[str, Any]:
    case_dir = output_dir / "trade_parity_suite"
    command = [
        python_exe,
        str(REPO_ROOT / "tools" / "real_trade_parity_suite.py"),
        "--output-dir",
        str(case_dir),
    ]
    run = _run_command(label="trade_parity_suite", command=command, workdir=REPO_ROOT)
    summary_path = case_dir / "summary.json"
    summary = _load_json(summary_path) if run["returncode"] == 0 and summary_path.exists() else {}
    checks = [
        _make_check("trade_parity_returncode_zero", run["returncode"] == 0, f"returncode={run['returncode']}"),
        _make_check("trade_parity_summary_exists", summary_path.exists(), str(summary_path)),
        _make_check("trade_parity_all_passed", bool(summary.get("all_passed")), f"all_passed={summary.get('all_passed')}"),
    ]
    return {
        "label": "trade_parity_suite",
        "run": run,
        "summary_path": str(summary_path),
        "summary": summary,
        "checks": checks,
    }


def _batch_case(*, python_exe: str, output_dir: Path) -> Dict[str, Any]:
    case_dir = output_dir / "batch_repeat5"
    command = [
        python_exe,
        str(REPO_ROOT / "tools" / "run_saved_preset_batch.py"),
        "--preset",
        "901",
        "--start",
        "2026-04-20 00:00:00+00:00",
        "--end",
        "2026-04-20 04:00:00+00:00",
        "--repeat-each",
        "5",
        "--output-dir",
        str(case_dir),
    ]
    run = _run_command(label="batch_repeat5", command=command, workdir=REPO_ROOT)
    summary_path = case_dir / "summary.json"
    jobs_path = case_dir / "jobs.csv"
    summary = _load_json(summary_path) if run["returncode"] == 0 and summary_path.exists() else {}
    rows = _load_csv_rows(jobs_path) if run["returncode"] == 0 and jobs_path.exists() else []

    balances = [_safe_float(row.get("ending_balance")) for row in rows]
    trades = [_safe_int(row.get("num_trades")) for row in rows]
    engines = [str(row.get("strategy_plan_engine") or "") for row in rows]
    memory_hits = sum(str(row.get("bundle_memory_cached") or "").strip().lower() == "true" for row in rows)
    load_secs = [_safe_float(row.get("load_sec")) for row in rows]

    checks = [
        _make_check("batch_returncode_zero", run["returncode"] == 0, f"returncode={run['returncode']}"),
        _make_check("batch_summary_exists", summary_path.exists(), str(summary_path)),
        _make_check("batch_jobs_csv_exists", jobs_path.exists(), str(jobs_path)),
        _make_check("batch_job_count", _safe_int(summary.get("jobs")) == 5 and len(rows) == 5, f"summary_jobs={summary.get('jobs')} rows={len(rows)}"),
        _make_check("batch_memory_cache_hits", _safe_int(summary.get("memory_cache_hits")) == 4 and memory_hits == 4, f"summary_hits={summary.get('memory_cache_hits')} row_hits={memory_hits}"),
        _make_check("batch_replay_context_hits", _safe_int(summary.get("replay_context_hits")) == 4, f"replay_context_hits={summary.get('replay_context_hits')}"),
        _make_check("batch_balances_match_expected", all(_approx_equal(balance, EXPECTED_BATCH_BALANCE_901) for balance in balances), f"balances={balances}"),
        _make_check("batch_trade_counts_stable", all(trade == 1 for trade in trades), f"trades={trades}"),
        _make_check("batch_engines_stable", all(engine == "legacy_generic" for engine in engines), f"engines={engines}"),
        _make_check(
            "batch_memory_reuse_reduces_load",
            len(load_secs) >= 2 and load_secs[0] is not None and load_secs[1] is not None and float(load_secs[1]) < float(load_secs[0]),
            f"load_secs={load_secs}",
        ),
    ]
    return {
        "label": "batch_repeat5",
        "run": run,
        "summary_path": str(summary_path),
        "jobs_path": str(jobs_path),
        "summary": summary,
        "rows": rows,
        "checks": checks,
    }


def _entry_filter_case(*, python_exe: str, output_dir: Path) -> Dict[str, Any]:
    case_dir = output_dir / "entry_filter_probe"
    command = [
        python_exe,
        str(REPO_ROOT / "tools" / "sweep_saved_preset_entry_filter.py"),
        "--preset",
        "010",
        "--start",
        "2026-04-20 00:00:00+00:00",
        "--end",
        "2026-04-20 04:00:00+00:00",
        "--expansion-values",
        "1.5",
        "--hard-skip-values",
        "2.5",
        "--confirm-bars-values",
        "1",
        "--retrace-values",
        "25,35",
        "--top-n",
        "5",
        "--output-dir",
        str(case_dir),
    ]
    run = _run_command(label="entry_filter_probe", command=command, workdir=REPO_ROOT)
    summary_path = case_dir / "summary.json"
    summary = _load_json(summary_path) if run["returncode"] == 0 and summary_path.exists() else {}
    top_results = list(summary.get("top_results") or [])
    best_result = dict(summary.get("best_result") or {})
    top_engines = [str((row or {}).get("strategy_plan_engine") or "") for row in top_results if isinstance(row, dict)]

    checks = [
        _make_check("entry_filter_returncode_zero", run["returncode"] == 0, f"returncode={run['returncode']}"),
        _make_check("entry_filter_summary_exists", summary_path.exists(), str(summary_path)),
        _make_check("entry_filter_loaded_cached_data", bool(run["markers"]["loaded_cached_data"]), "stdout contains cached candle marker"),
        _make_check("entry_filter_loaded_local_store", bool(run["markers"]["loaded_local_indicator_store"]), "stdout contains local indicator store marker"),
        _make_check("entry_filter_best_variant", str(best_result.get("variant") or "") == "off", f"best_variant={best_result.get('variant')}"),
        _make_check("entry_filter_best_balance", _approx_equal(best_result.get("ending_balance"), 1000.0), f"best_balance={best_result.get('ending_balance')}"),
        _make_check("entry_filter_best_trades", _safe_int(best_result.get("num_trades")) == 0, f"best_trades={best_result.get('num_trades')}"),
        _make_check("entry_filter_compiled_path", bool(top_engines) and all(engine == "compiled_bar" for engine in top_engines), f"engines={top_engines}"),
    ]
    return {
        "label": "entry_filter_probe",
        "run": run,
        "summary_path": str(summary_path),
        "summary": summary,
        "checks": checks,
    }


def _pullback_variant_case(*, python_exe: str, output_dir: Path) -> Dict[str, Any]:
    case_dir = output_dir / "pullback_variant_probe"
    requested_names = [
        "baseline",
        "market_bias_align_1h",
        "market_phase_pullback_1h",
    ]
    command = [
        python_exe,
        str(REPO_ROOT / "tools" / "sweep_saved_preset_pullback_variants.py"),
        "--preset",
        "902",
        "--start",
        "2026-04-20 00:00:00+00:00",
        "--end",
        "2026-04-20 04:00:00+00:00",
        "--variant-names",
        ",".join(requested_names),
        "--top-n",
        "5",
        "--output-dir",
        str(case_dir),
    ]
    run = _run_command(label="pullback_variant_probe", command=command, workdir=REPO_ROOT)
    summary_path = case_dir / "summary.json"
    summary = _load_json(summary_path) if run["returncode"] == 0 and summary_path.exists() else {}
    top_results = list(summary.get("top_results") or [])
    best_result = dict(summary.get("best_result") or {})
    variant_names = [str(name or "") for name in list(summary.get("variant_names") or [])]
    top_engines = [str((row or {}).get("strategy_plan_engine") or "") for row in top_results if isinstance(row, dict)]

    checks = [
        _make_check("pullback_variant_returncode_zero", run["returncode"] == 0, f"returncode={run['returncode']}"),
        _make_check("pullback_variant_summary_exists", summary_path.exists(), str(summary_path)),
        _make_check("pullback_variant_loaded_cached_data", bool(run["markers"]["loaded_cached_data"]), "stdout contains cached candle marker"),
        _make_check("pullback_variant_loaded_local_store", bool(run["markers"]["loaded_local_indicator_store"]), "stdout contains local indicator store marker"),
        _make_check("pullback_variant_requested_names", variant_names == requested_names, f"variant_names={variant_names}"),
        _make_check("pullback_variant_best_variant", str(best_result.get("variant") or "") == "baseline", f"best_variant={best_result.get('variant')}"),
        _make_check(
            "pullback_variant_best_balance",
            _approx_equal(best_result.get("ending_balance"), EXPECTED_PULLBACK_BASELINE_BALANCE_902),
            f"best_balance={best_result.get('ending_balance')}",
        ),
        _make_check("pullback_variant_best_trades", _safe_int(best_result.get("num_trades")) == 1, f"best_trades={best_result.get('num_trades')}"),
        _make_check("pullback_variant_generic_path_with_filters", bool(top_engines) and all(engine == "legacy_generic" for engine in top_engines), f"engines={top_engines}"),
    ]
    return {
        "label": "pullback_variant_probe",
        "run": run,
        "summary_path": str(summary_path),
        "summary": summary,
        "checks": checks,
    }


def _write_report(path: Path, payload: Dict[str, Any]) -> None:
    lines: List[str] = []
    lines.append("# Real Runtime Regression Suite")
    lines.append("")
    lines.append(f"- generated_utc: `{payload.get('generated_utc')}`")
    lines.append(f"- python: `{payload.get('python_exe')}`")
    lines.append(f"- overall: `{'PASS' if payload.get('all_passed') else 'FAIL'}`")
    lines.append("")
    lines.append("## Cases")
    lines.append("")
    lines.append("| Case | RC | Wall s | Key result | Cached candles | Local store | Replay bundle | Cached base | Cached verification |")
    lines.append("|---|---:|---:|---|---|---|---|---|---|")
    for case in payload.get("cases", []):
        run = case.get("run", {}) or {}
        markers = run.get("markers", {}) or {}
        case_name = str(case.get("label") or "")
        key_result = "-"
        summary = case.get("summary", {}) or {}
        if case_name in {"parity_suite", "trade_parity_suite"}:
            key_result = f"all_passed={summary.get('all_passed')}"
        elif case_name == "batch_repeat5":
            key_result = (
                f"memory_cache_hits={summary.get('memory_cache_hits')} "
                f"replay_context_hits={summary.get('replay_context_hits')} "
                f"jobs={summary.get('jobs')}"
            )
        elif case_name in {"entry_filter_probe", "pullback_variant_probe"}:
            best_result = summary.get("best_result", {}) if isinstance(summary.get("best_result"), dict) else {}
            key_result = (
                f"best={best_result.get('variant')} "
                f"bal={best_result.get('ending_balance')} "
                f"trades={best_result.get('num_trades')}"
            )
        lines.append(
            "| "
            + " | ".join(
                [
                    case_name,
                    str(run.get("returncode")),
                    str(run.get("wall_sec")),
                    str(key_result),
                    "yes" if markers.get("loaded_cached_data") else "no",
                    "yes" if markers.get("loaded_local_indicator_store") else "no",
                    "yes" if markers.get("loaded_replay_bundle") else "no",
                    "yes" if markers.get("loaded_cached_base_result") else "no",
                    "yes" if markers.get("loaded_cached_verification_payload") else "no",
                ]
            )
            + " |"
        )
    lines.append("")
    lines.append("## Checks")
    lines.append("")
    lines.append("| Check | Status | Detail |")
    lines.append("|---|---|---|")
    for check in payload.get("checks", []):
        lines.append(f"| {check.get('name')} | {'PASS' if check.get('passed') else 'FAIL'} | {check.get('detail')} |")
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def main() -> int:
    parser = argparse.ArgumentParser(description="Run a live regression suite for saved-preset runtime and sweep paths.")
    parser.add_argument("--python", dest="python_exe", default=sys.executable)
    parser.add_argument("--output-dir", default=None)
    args = parser.parse_args()

    stamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    output_dir = (
        Path(args.output_dir).resolve()
        if args.output_dir
        else (REPO_ROOT / "runs" / "perf" / f"real_runtime_regression_suite_{stamp}")
    )
    output_dir.mkdir(parents=True, exist_ok=True)

    cases = [
        _parity_case(python_exe=str(args.python_exe), output_dir=output_dir),
        _trade_parity_case(python_exe=str(args.python_exe), output_dir=output_dir),
        _batch_case(python_exe=str(args.python_exe), output_dir=output_dir),
        _entry_filter_case(python_exe=str(args.python_exe), output_dir=output_dir),
        _pullback_variant_case(python_exe=str(args.python_exe), output_dir=output_dir),
    ]

    checks: List[Dict[str, Any]] = []
    for case in cases:
        checks.extend(list(case.get("checks") or []))

    all_passed = all(bool(check.get("passed")) for check in checks)
    payload = {
        "generated_utc": datetime.now(timezone.utc).isoformat(),
        "python_exe": str(args.python_exe),
        "repo_root": str(REPO_ROOT),
        "all_passed": bool(all_passed),
        "cases": cases,
        "checks": checks,
    }

    summary_path = output_dir / "summary.json"
    report_path = output_dir / "report.md"
    summary_path.write_text(json.dumps(payload, indent=2, ensure_ascii=False, default=str), encoding="utf-8")
    _write_report(report_path, payload)

    latest_dir = REPO_ROOT / "runs" / "perf" / "real_runtime_regression_suite_latest"
    if latest_dir.exists():
        shutil.rmtree(latest_dir)
    shutil.copytree(output_dir, latest_dir)

    print(f"Overall: {'PASS' if all_passed else 'FAIL'}", flush=True)
    print(f"Saved summary JSON: {summary_path}", flush=True)
    print(f"Saved report MD: {report_path}", flush=True)
    print(f"Updated latest snapshot: {latest_dir}", flush=True)
    return 0 if all_passed else 1


if __name__ == "__main__":
    raise SystemExit(main())
