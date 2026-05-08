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


REPO_ROOT = Path(__file__).resolve().parents[1]


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
        "combined_tail": (stdout + ("\n" + stderr if stderr else "")).splitlines()[-120:],
    }


def _make_check(name: str, passed: bool, detail: str) -> Dict[str, Any]:
    return {
        "name": str(name),
        "passed": bool(passed),
        "detail": str(detail),
    }


def _status(value: bool) -> str:
    return "PASS" if value else "FAIL"


def _git_head() -> str:
    proc = subprocess.run(
        ["git", "rev-parse", "--short", "HEAD"],
        cwd=str(REPO_ROOT),
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
        check=False,
    )
    if proc.returncode != 0:
        return ""
    return str(proc.stdout or "").strip()


def _resolve_pytest_command(python_exe: str) -> List[str]:
    venv_pytest = REPO_ROOT / ".venv" / "Scripts" / "pytest.exe"
    if venv_pytest.exists():
        return [str(venv_pytest)]
    discovered = shutil.which("pytest")
    if discovered:
        return [str(discovered)]
    return [python_exe, "-m", "pytest"]


def _refresh_latest_copy(output_dir: Path) -> Path:
    latest_dir = REPO_ROOT / "runs" / "perf" / "real_backtest_system_certification_suite_latest"
    if latest_dir.exists():
        shutil.rmtree(latest_dir)
    shutil.copytree(output_dir, latest_dir)
    return latest_dir


def _summary_layer(
    *,
    label: str,
    run: Dict[str, Any],
    summary_path: Path,
    expected_flag: str = "all_passed",
    key_result: str = "",
) -> Dict[str, Any]:
    summary = _load_json(summary_path) if run["returncode"] == 0 and summary_path.exists() else {}
    checks = [
        _make_check(f"{label}_returncode_zero", run["returncode"] == 0, f"returncode={run['returncode']}"),
        _make_check(f"{label}_summary_exists", summary_path.exists(), str(summary_path)),
        _make_check(
            f"{label}_{expected_flag}",
            bool(summary.get(expected_flag)),
            f"{expected_flag}={summary.get(expected_flag)}",
        ),
    ]
    return {
        "label": label,
        "run": run,
        "summary_path": str(summary_path),
        "summary": summary,
        "checks": checks,
        "all_passed": all(bool(check.get("passed")) for check in checks),
        "key_result": key_result or f"{expected_flag}={summary.get(expected_flag)}",
    }


def _range_filter_oracle_case(*, python_exe: str, output_dir: Path) -> Dict[str, Any]:
    case_dir = output_dir / "range_filter_oracle"
    command = [
        python_exe,
        str(REPO_ROOT / "scripts" / "manual" / "verify_range_filter_oracle.py"),
        "--output-root",
        str(case_dir),
        "--strict",
    ]
    run = _run_command(label="range_filter_oracle", command=command, workdir=REPO_ROOT)
    summary_path = case_dir / "range_filter_oracle_verification.json"
    summary = _load_json(summary_path) if run["returncode"] == 0 and summary_path.exists() else {}
    results = list(summary.get("results") or [])
    exact_windows = int(summary.get("exact_windows") or 0)
    checks = [
        _make_check("range_filter_oracle_returncode_zero", run["returncode"] == 0, f"returncode={run['returncode']}"),
        _make_check("range_filter_oracle_summary_exists", summary_path.exists(), str(summary_path)),
        _make_check(
            "range_filter_oracle_exact_windows",
            exact_windows == len(results) == 5,
            f"exact_windows={exact_windows} total={len(results)}",
        ),
    ]
    return {
        "label": "range_filter_oracle",
        "run": run,
        "summary_path": str(summary_path),
        "summary": summary,
        "checks": checks,
        "all_passed": all(bool(check.get("passed")) for check in checks),
        "key_result": f"exact_windows={exact_windows}/{len(results)}",
    }


def _backtest_parity_case(*, python_exe: str, output_dir: Path) -> Dict[str, Any]:
    case_dir = output_dir / "backtest_parity"
    run = _run_command(
        label="backtest_parity",
        command=[
            python_exe,
            str(REPO_ROOT / "tools" / "real_backtest_parity_suite.py"),
            "--output-dir",
            str(case_dir),
        ],
        workdir=REPO_ROOT,
    )
    return _summary_layer(
        label="backtest_parity",
        run=run,
        summary_path=case_dir / "summary.json",
    )


def _trade_parity_case(*, python_exe: str, output_dir: Path) -> Dict[str, Any]:
    case_dir = output_dir / "trade_parity"
    run = _run_command(
        label="trade_parity",
        command=[
            python_exe,
            str(REPO_ROOT / "tools" / "real_trade_parity_suite.py"),
            "--output-dir",
            str(case_dir),
        ],
        workdir=REPO_ROOT,
    )
    return _summary_layer(
        label="trade_parity",
        run=run,
        summary_path=case_dir / "summary.json",
    )


def _causality_case(*, python_exe: str, output_dir: Path) -> Dict[str, Any]:
    case_dir = output_dir / "causality_suite"
    run = _run_command(
        label="causality_suite",
        command=[
            python_exe,
            str(REPO_ROOT / "tools" / "real_backtest_causality_suite.py"),
            "--output-dir",
            str(case_dir),
        ],
        workdir=REPO_ROOT,
    )
    return _summary_layer(
        label="causality_suite",
        run=run,
        summary_path=case_dir / "summary.json",
    )


def _replay_context_safety_case(*, python_exe: str, output_dir: Path) -> Dict[str, Any]:
    case_dir = output_dir / "replay_context_safety"
    run = _run_command(
        label="replay_context_safety",
        command=[
            python_exe,
            str(REPO_ROOT / "tools" / "real_replay_context_safety_suite.py"),
            "--output-dir",
            str(case_dir),
        ],
        workdir=REPO_ROOT,
    )
    return _summary_layer(
        label="replay_context_safety",
        run=run,
        summary_path=case_dir / "summary.json",
    )


def _runtime_regression_case(*, python_exe: str, output_dir: Path) -> Dict[str, Any]:
    case_dir = output_dir / "runtime_regression"
    run = _run_command(
        label="runtime_regression",
        command=[
            python_exe,
            str(REPO_ROOT / "tools" / "real_runtime_regression_suite.py"),
            "--output-dir",
            str(case_dir),
        ],
        workdir=REPO_ROOT,
    )
    return _summary_layer(
        label="runtime_regression",
        run=run,
        summary_path=case_dir / "summary.json",
    )


def _pytest_contract_case(
    *,
    label: str,
    python_exe: str,
    output_dir: Path,
    targets: List[str],
) -> Dict[str, Any]:
    case_dir = output_dir / label
    case_dir.mkdir(parents=True, exist_ok=True)
    pytest_command = _resolve_pytest_command(python_exe)
    run = _run_command(
        label=label,
        command=[*pytest_command, *targets, "-q"],
        workdir=REPO_ROOT,
    )
    combined_tail = list(run.get("combined_tail") or [])
    last_line = next((line.strip() for line in reversed(combined_tail) if str(line).strip()), "")
    summary = {
        "targets": targets,
        "last_line": last_line,
    }
    checks = [
        _make_check(f"{label}_returncode_zero", run["returncode"] == 0, f"returncode={run['returncode']}"),
        _make_check(f"{label}_pytest_passed", run["returncode"] == 0, last_line or "pytest returned non-zero"),
    ]
    summary_path = case_dir / "summary.json"
    summary_path.write_text(json.dumps(summary, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
    return {
        "label": label,
        "run": run,
        "summary_path": str(summary_path),
        "summary": summary,
        "checks": checks,
        "all_passed": all(bool(check.get("passed")) for check in checks),
        "key_result": last_line or "pytest bundle",
    }


def _write_report(path: Path, payload: Dict[str, Any]) -> None:
    lines: List[str] = []
    lines.append("# Real Backtest System Certification Suite")
    lines.append("")
    lines.append(f"- generated_utc: `{payload.get('generated_utc')}`")
    lines.append(f"- python: `{payload.get('python_exe')}`")
    lines.append(f"- git_head: `{payload.get('git_head')}`")
    lines.append(f"- overall: `{_status(bool(payload.get('all_passed')))}`")
    lines.append("")
    lines.append("## Ladder")
    lines.append("")
    lines.append("| Step | Layer | Status | Wall s | Key result |")
    lines.append("|---:|---|---|---:|---|")
    for idx, case in enumerate(payload.get("cases", []), start=1):
        run = dict(case.get("run") or {})
        lines.append(
            f"| {idx} | {case.get('label')} | {_status(bool(case.get('all_passed')))} | {run.get('wall_sec')} | {case.get('key_result')} |"
        )
    lines.append("")
    lines.append("## Checks")
    lines.append("")
    for case in payload.get("cases", []):
        lines.append(f"### {case.get('label')}")
        lines.append("")
        lines.append("| Check | Status | Detail |")
        lines.append("|---|---|---|")
        for check in case.get("checks", []):
            lines.append(
                f"| {check.get('name')} | {_status(bool(check.get('passed')))} | {check.get('detail')} |"
            )
        lines.append("")
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def main() -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Run a stepwise technical certification ladder for the backtest engine: "
            "indicator math, backtest parity, trade parity, replay safety, runtime regression, "
            "and live/downloader contracts."
        )
    )
    parser.add_argument("--python", dest="python_exe", default=sys.executable)
    parser.add_argument(
        "--output-dir",
        default="",
        help="Optional output directory. Defaults to runs/perf/real_backtest_system_certification_suite_<timestamp>.",
    )
    args = parser.parse_args()

    if str(args.output_dir or "").strip():
        output_dir = Path(args.output_dir).resolve()
    else:
        stamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
        output_dir = REPO_ROOT / "runs" / "perf" / f"real_backtest_system_certification_suite_{stamp}"
    output_dir.mkdir(parents=True, exist_ok=True)

    cases = [
        _range_filter_oracle_case(python_exe=str(args.python_exe), output_dir=output_dir),
        _backtest_parity_case(python_exe=str(args.python_exe), output_dir=output_dir),
        _trade_parity_case(python_exe=str(args.python_exe), output_dir=output_dir),
        _causality_case(python_exe=str(args.python_exe), output_dir=output_dir),
        _replay_context_safety_case(python_exe=str(args.python_exe), output_dir=output_dir),
        _runtime_regression_case(python_exe=str(args.python_exe), output_dir=output_dir),
        _pytest_contract_case(
            label="core_backtest_contracts",
            python_exe=str(args.python_exe),
            output_dir=output_dir,
            targets=[
                "tests/test_fast_backtest.py",
                "tests/test_backtest_replay.py",
                "tests/test_entry_filter_retrace.py",
            ],
        ),
        _pytest_contract_case(
            label="live_downloader_contracts",
            python_exe=str(args.python_exe),
            output_dir=output_dir,
            targets=[
                "tests/test_ui_v2_live.py",
                "tests/test_ui_v2_downloader.py",
            ],
        ),
    ]
    all_passed = all(bool(case.get("all_passed")) for case in cases)

    payload = {
        "generated_utc": datetime.now(timezone.utc).isoformat(),
        "python_exe": str(args.python_exe),
        "repo_root": str(REPO_ROOT),
        "git_head": _git_head(),
        "all_passed": all_passed,
        "cases": cases,
    }

    summary_path = output_dir / "summary.json"
    report_path = output_dir / "report.md"
    summary_path.write_text(json.dumps(payload, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
    _write_report(report_path, payload)
    latest_dir = _refresh_latest_copy(output_dir)

    print(f"Suite output: {output_dir}", flush=True)
    print(f"Latest copy: {latest_dir}", flush=True)
    print(f"Overall: {_status(all_passed)}", flush=True)
    for idx, case in enumerate(cases, start=1):
        print(
            f"{idx}. {case['label']}: {_status(bool(case.get('all_passed')))} "
            f"(wall={case['run'].get('wall_sec')}s, {case.get('key_result')})",
            flush=True,
        )

    return 0 if all_passed else 1


if __name__ == "__main__":
    raise SystemExit(main())
