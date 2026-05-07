from __future__ import annotations

import argparse
import json
import shutil
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List

import pandas as pd


REPO_ROOT = Path(__file__).resolve().parents[1]
WORKSPACE_ROOT = REPO_ROOT.parents[1]
CASEBOOK_ROOT = WORKSPACE_ROOT / "research_exports" / "trade_entry_casebook"
SOURCE_CASE_REPORT = CASEBOOK_ROOT / "preset_drafts" / "backtest_case_period_0001_0005.json"
SOURCE_PRESETS = CASEBOOK_ROOT / "preset_drafts" / "research_case_family_presets_v1.json"


def _load_json(path: Path) -> Dict[str, Any]:
    with path.open("r", encoding="utf-8") as fh:
        payload = json.load(fh)
    if not isinstance(payload, dict):
        raise ValueError(f"Expected JSON object at {path}")
    return payload


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
    return {
        "label": str(label),
        "command": command,
        "returncode": int(proc.returncode),
        "wall_sec": round(wall_sec, 4),
        "stdout_tail": stdout.splitlines()[-120:],
        "stderr_tail": stderr.splitlines()[-120:],
    }


def _make_check(name: str, passed: bool, detail: str) -> Dict[str, Any]:
    return {
        "name": str(name),
        "passed": bool(passed),
        "detail": str(detail),
    }


def _runtime_case(*, python_exe: str, output_dir: Path) -> Dict[str, Any]:
    case_dir = output_dir / "runtime_regression"
    command = [
        python_exe,
        str(REPO_ROOT / "tools" / "real_runtime_regression_suite.py"),
        "--output-dir",
        str(case_dir),
    ]
    run = _run_command(label="runtime_regression", command=command, workdir=REPO_ROOT)
    summary_path = case_dir / "summary.json"
    summary = _load_json(summary_path) if run["returncode"] == 0 and summary_path.exists() else {}
    checks = [
        _make_check("runtime_returncode_zero", run["returncode"] == 0, f"returncode={run['returncode']}"),
        _make_check("runtime_summary_exists", summary_path.exists(), str(summary_path)),
        _make_check("runtime_all_passed", bool(summary.get("all_passed")), f"all_passed={summary.get('all_passed')}"),
    ]
    return {
        "label": "runtime_regression",
        "run": run,
        "summary_path": str(summary_path),
        "summary": summary,
        "checks": checks,
    }


def _oracle_case(*, python_exe: str, output_dir: Path) -> Dict[str, Any]:
    case_dir = output_dir / "reference_oracle"
    command = [
        python_exe,
        str(REPO_ROOT / "scripts" / "manual" / "verify_casebook_reference_engine.py"),
        "--output-root",
        str(case_dir),
        "--strict",
    ]
    run = _run_command(label="reference_oracle", command=command, workdir=REPO_ROOT)
    summary_path = case_dir / "exact_reference_engine_0001_0005.json"
    summary = _load_json(summary_path) if run["returncode"] == 0 and summary_path.exists() else {}
    results = list(summary.get("results") or [])
    exact_matches = int(summary.get("exact_fill_matches") or 0)
    checks = [
        _make_check("oracle_returncode_zero", run["returncode"] == 0, f"returncode={run['returncode']}"),
        _make_check("oracle_summary_exists", summary_path.exists(), str(summary_path)),
        _make_check("oracle_exact_count", exact_matches == len(results) == 5, f"exact_fill_matches={exact_matches} total={len(results)}"),
    ]
    return {
        "label": "reference_oracle",
        "run": run,
        "summary_path": str(summary_path),
        "summary": summary,
        "checks": checks,
    }


def _ts(value: Any) -> pd.Timestamp:
    ts = pd.Timestamp(value)
    if ts.tzinfo is None:
        return ts.tz_localize("UTC")
    return ts.tz_convert("UTC")


def _main_engine_casebook_case(*, python_exe: str, output_dir: Path, oracle_summary: Dict[str, Any]) -> Dict[str, Any]:
    case_dir = output_dir / "main_engine_casebook_exactness"
    case_dir.mkdir(parents=True, exist_ok=True)
    report_json = case_dir / "backtest_case_period_0001_0005.json"
    report_md = case_dir / "backtest_case_period_0001_0005.md"
    shutil.copy2(SOURCE_CASE_REPORT, report_json)
    command = [
        python_exe,
        str(REPO_ROOT / "scripts" / "manual" / "rebuild_case_window_report.py"),
        "--report-json",
        str(report_json),
        "--report-md",
        str(report_md),
        "--presets-path",
        str(SOURCE_PRESETS),
        "--repo-root",
        str(REPO_ROOT),
    ]
    run = _run_command(label="main_engine_casebook_exactness", command=command, workdir=REPO_ROOT)
    report = _load_json(report_json) if run["returncode"] == 0 and report_json.exists() else {}
    cases = list(report.get("cases") or [])
    presets = dict(report.get("presets") or {})
    oracle_by_case = {
        str(row.get("case_id") or ""): dict(row)
        for row in list(oracle_summary.get("results") or [])
        if isinstance(row, dict)
    }

    case_rows: List[Dict[str, Any]] = []
    exact_count = 0
    for case in cases:
        case_id = str(case.get("case_id") or "")
        family = str(case.get("preset_family") or "")
        window_start = _ts(case.get("t_rf_utc"))
        window_end = _ts(case.get("window_end_utc"))
        family_presets = [
            (preset_name, dict(block))
            for preset_name, block in presets.items()
            if str(dict(block).get("preset_family") or "") == family
        ]
        family_preset_names = [name for name, _ in family_presets]
        expected = oracle_by_case.get(case_id, {})
        expected_fill = str(expected.get("expected_fill_open_ts") or "")
        actual_entries: List[str] = []
        preset_name = ""
        strategy_plan_engine = ""
        total_preset_trades = None
        if len(family_presets) == 1:
            preset_name, block = family_presets[0]
            strategy_plan_engine = str((dict(block.get("summary") or {})).get("strategy_plan_engine") or "")
            total_preset_trades = dict(block.get("summary") or {}).get("num_trades")
            for raw in list(block.get("entry_times_utc") or []):
                try:
                    entry_ts = _ts(raw)
                except Exception:
                    continue
                if window_start <= entry_ts <= window_end:
                    actual_entries.append(entry_ts.isoformat())
        actual_fill = actual_entries[0] if len(actual_entries) == 1 else ""
        exact = bool(len(actual_entries) == 1 and actual_fill == expected_fill)
        if exact:
            exact_count += 1
        case_rows.append(
            {
                "case_id": case_id,
                "family": family,
                "preset_name": preset_name,
                "family_preset_names": family_preset_names,
                "strategy_plan_engine": strategy_plan_engine,
                "expected_fill_open_ts": expected_fill,
                "actual_entries_in_window": actual_entries,
                "actual_entries_in_window_count": len(actual_entries),
                "actual_fill_open_ts": actual_fill,
                "total_preset_trades": total_preset_trades,
                "exact_fill_match": exact,
            }
        )

    checks = [
        _make_check("main_engine_returncode_zero", run["returncode"] == 0, f"returncode={run['returncode']}"),
        _make_check("main_engine_report_exists", report_json.exists(), str(report_json)),
        _make_check("main_engine_cases_count", len(cases) == 5, f"cases={len(cases)}"),
        _make_check("main_engine_exact_casebook_count", exact_count == len(cases) == 5, f"exact_fill_matches={exact_count} total={len(cases)}"),
    ]
    for row in case_rows:
        checks.append(
            _make_check(
                f"case_{row['case_id']}_exact_fill",
                bool(row["exact_fill_match"]),
                f"expected={row['expected_fill_open_ts']} actual={row['actual_fill_open_ts']} entries={row['actual_entries_in_window']}",
            )
        )
    return {
        "label": "main_engine_casebook_exactness",
        "run": run,
        "report_json": str(report_json),
        "report_md": str(report_md),
        "report": report,
        "case_rows": case_rows,
        "exact_fill_matches": exact_count,
        "checks": checks,
    }


def _write_report(path: Path, payload: Dict[str, Any]) -> None:
    lines: List[str] = []
    lines.append("# Real Engine Certification Suite")
    lines.append("")
    lines.append(f"- generated_utc: `{payload.get('generated_utc')}`")
    lines.append(f"- python: `{payload.get('python_exe')}`")
    lines.append(f"- git_head: `{payload.get('git_head')}`")
    lines.append(f"- overall: `{'PASS' if payload.get('all_passed') else 'FAIL'}`")
    lines.append("")
    lines.append("## Layers")
    lines.append("")
    lines.append("| Layer | RC | Wall s | Key result |")
    lines.append("|---|---:|---:|---|")
    for case in payload.get("cases", []):
        label = str(case.get("label") or "")
        run = dict(case.get("run") or {})
        key_result = "-"
        if label == "runtime_regression":
            summary = dict(case.get("summary") or {})
            key_result = f"all_passed={summary.get('all_passed')}"
        elif label == "reference_oracle":
            summary = dict(case.get("summary") or {})
            key_result = f"exact_fill_matches={summary.get('exact_fill_matches')}"
        elif label == "main_engine_casebook_exactness":
            key_result = f"exact_fill_matches={case.get('exact_fill_matches')}"
        lines.append(f"| {label} | {run.get('returncode')} | {run.get('wall_sec')} | {key_result} |")
    lines.append("")
    lines.append("## Main Engine Casebook Exactness")
    lines.append("")
    lines.append("| Case | Family | Preset | Engine | Expected fill UTC | Actual fill UTC | Entries in window | Exact |")
    lines.append("|---|---|---|---|---|---|---:|---|")
    main_case = next((case for case in payload.get("cases", []) if str(case.get("label") or "") == "main_engine_casebook_exactness"), None)
    if isinstance(main_case, dict):
        for row in list(main_case.get("case_rows") or []):
            lines.append(
                f"| {row.get('case_id')} | {row.get('family')} | {row.get('preset_name')} | {row.get('strategy_plan_engine')} | {row.get('expected_fill_open_ts')} | {row.get('actual_fill_open_ts')} | {row.get('actual_entries_in_window_count')} | {row.get('exact_fill_match')} |"
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
    parser = argparse.ArgumentParser(description="Run the full engine certification suite: runtime regression, reference oracle, and main-engine exactness.")
    parser.add_argument("--python", dest="python_exe", default=sys.executable)
    parser.add_argument("--output-dir", default=None)
    args = parser.parse_args()

    stamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    output_dir = (
        Path(args.output_dir).resolve()
        if args.output_dir
        else (REPO_ROOT / "runs" / "perf" / f"real_engine_certification_suite_{stamp}")
    )
    output_dir.mkdir(parents=True, exist_ok=True)

    git_head = ""
    try:
        git_head = (
            subprocess.run(
                ["git", "-C", str(REPO_ROOT), "rev-parse", "--short", "HEAD"],
                capture_output=True,
                text=True,
                encoding="utf-8",
                errors="replace",
                check=False,
            ).stdout.strip()
        )
    except Exception:
        git_head = ""

    runtime_case = _runtime_case(python_exe=str(args.python_exe), output_dir=output_dir)
    oracle_case = _oracle_case(python_exe=str(args.python_exe), output_dir=output_dir)
    main_engine_case = _main_engine_casebook_case(
        python_exe=str(args.python_exe),
        output_dir=output_dir,
        oracle_summary=dict(oracle_case.get("summary") or {}),
    )

    cases = [runtime_case, oracle_case, main_engine_case]
    checks: List[Dict[str, Any]] = []
    for case in cases:
        checks.extend(list(case.get("checks") or []))
    all_passed = all(bool(check.get("passed")) for check in checks)

    payload = {
        "generated_utc": datetime.now(timezone.utc).isoformat(),
        "python_exe": str(args.python_exe),
        "repo_root": str(REPO_ROOT),
        "git_head": git_head,
        "all_passed": bool(all_passed),
        "cases": cases,
        "checks": checks,
    }

    summary_path = output_dir / "summary.json"
    report_path = output_dir / "report.md"
    summary_path.write_text(json.dumps(payload, indent=2, ensure_ascii=False, default=str), encoding="utf-8")
    _write_report(report_path, payload)

    latest_dir = REPO_ROOT / "runs" / "perf" / "real_engine_certification_suite_latest"
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
