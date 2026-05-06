from __future__ import annotations

import argparse
import json
import math
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
import subprocess
import sys
import time
from typing import Any, Dict, List, Optional


REPO_ROOT = Path(__file__).resolve().parents[1]


@dataclass(frozen=True)
class ParityCase:
    label: str
    preset: str
    start: str
    end: str
    expected_engine: str
    expected_ending_balance: float
    expected_num_trades: int
    expected_exit_reason_counts: Dict[str, int]
    expected_plan_blockers: List[str]


CASES: List[ParityCase] = [
    ParityCase(
        label="compiled_entry_filter_no_trade",
        preset="010",
        start="2026-04-20 00:00:00+00:00",
        end="2026-04-20 04:00:00+00:00",
        expected_engine="compiled_bar",
        expected_ending_balance=1000.0,
        expected_num_trades=0,
        expected_exit_reason_counts={},
        expected_plan_blockers=[],
    ),
    ParityCase(
        label="sequence_pullback_force_close",
        preset="901",
        start="2026-04-20 00:00:00+00:00",
        end="2026-04-20 04:00:00+00:00",
        expected_engine="compiled_bar",
        expected_ending_balance=999.9639765918721,
        expected_num_trades=1,
        expected_exit_reason_counts={"Force Close (End)": 1},
        expected_plan_blockers=[],
    ),
    ParityCase(
        label="sequence_long_exit_then_force_close",
        preset="902",
        start="2026-04-20 00:00:00+00:00",
        end="2026-04-20 04:00:00+00:00",
        expected_engine="compiled_bar",
        expected_ending_balance=1000.4445208379417,
        expected_num_trades=2,
        expected_exit_reason_counts={"Long Exit": 1, "Force Close (End)": 1},
        expected_plan_blockers=[],
    ),
]


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


def _approx_equal(left: Any, right: Any, tol: float = 1e-9) -> bool:
    a = _safe_float(left)
    b = _safe_float(right)
    if a is None or b is None:
        return False
    return abs(a - b) <= float(tol)


def _load_json(path: Path) -> Dict[str, Any]:
    with path.open("r", encoding="utf-8") as fh:
        payload = json.load(fh)
    if not isinstance(payload, dict):
        raise ValueError(f"Expected JSON object at {path}")
    return payload


def _bool_marker(text: str, needle: str) -> bool:
    return needle in text


def _normalize_reason_counts(value: Any) -> Dict[str, int]:
    if not isinstance(value, dict):
        return {}
    normalized: Dict[str, int] = {}
    for key, raw in value.items():
        name = str(key or "").strip()
        if not name:
            continue
        count = _safe_int(raw)
        normalized[name] = 0 if count is None else count
    return normalized


def _normalize_string_list(value: Any) -> List[str]:
    if not isinstance(value, list):
        return []
    out: List[str] = []
    for item in value:
        text = str(item or "").strip()
        if text:
            out.append(text)
    return out


def _sorted_strings(values: List[str]) -> List[str]:
    return sorted((str(v or "").strip() for v in values if str(v or "").strip()))


def _run_case(
    *,
    case: ParityCase,
    python_exe: str,
    output_dir: Path,
) -> Dict[str, Any]:
    case_dir = output_dir / case.label
    case_dir.mkdir(parents=True, exist_ok=True)
    result_json = case_dir / "result.json"

    cmd = [
        python_exe,
        str(REPO_ROOT / "tools" / "run_saved_preset.py"),
        "--preset",
        case.preset,
        "--start",
        case.start,
        "--end",
        case.end,
        "--output-json",
        str(result_json),
    ]

    started = time.perf_counter()
    proc = subprocess.run(
        cmd,
        cwd=str(REPO_ROOT),
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

    payload: Dict[str, Any] = {}
    if proc.returncode == 0 and result_json.exists():
        payload = _load_json(result_json)

    summary = payload.get("summary", {}) if isinstance(payload.get("summary"), dict) else {}
    ending_balance = _safe_float(summary.get("ending_balance"))
    num_trades = _safe_int(summary.get("num_trades", summary.get("trades")))
    engine = str(summary.get("strategy_plan_engine", "") or "")
    exit_reason_counts = _normalize_reason_counts(summary.get("exit_reason_counts"))
    plan_blockers = _normalize_string_list(summary.get("strategy_plan_blockers"))
    expected_plan_blockers = _sorted_strings(case.expected_plan_blockers)
    actual_plan_blockers = _sorted_strings(plan_blockers)

    checks = [
        {
            "name": "returncode_zero",
            "passed": proc.returncode == 0,
            "detail": f"returncode={proc.returncode}",
        },
        {
            "name": "ending_balance_match",
            "passed": _approx_equal(ending_balance, case.expected_ending_balance),
            "detail": f"expected={case.expected_ending_balance} actual={ending_balance}",
        },
        {
            "name": "num_trades_match",
            "passed": num_trades == case.expected_num_trades,
            "detail": f"expected={case.expected_num_trades} actual={num_trades}",
        },
        {
            "name": "strategy_plan_engine_match",
            "passed": engine == case.expected_engine,
            "detail": f"expected={case.expected_engine} actual={engine}",
        },
        {
            "name": "exit_reason_counts_match",
            "passed": exit_reason_counts == case.expected_exit_reason_counts,
            "detail": f"expected={case.expected_exit_reason_counts} actual={exit_reason_counts}",
        },
        {
            "name": "strategy_plan_blockers_match",
            "passed": actual_plan_blockers == expected_plan_blockers,
            "detail": f"expected={expected_plan_blockers} actual={actual_plan_blockers}",
        },
        {
            "name": "loaded_cached_data",
            "passed": _bool_marker(combined, "Loaded cached data (parquet)."),
            "detail": "stdout contains cached candle reuse marker",
        },
        {
            "name": "loaded_local_indicator_store",
            "passed": _bool_marker(combined, "Loaded local indicator store."),
            "detail": "stdout contains local indicator store reuse marker",
        },
    ]

    return {
        "label": case.label,
        "preset": case.preset,
        "start": case.start,
        "end": case.end,
        "returncode": int(proc.returncode),
        "wall_sec": round(wall_sec, 4),
        "result_json": str(result_json),
        "summary": {
            "strategy_plan_engine": engine,
            "ending_balance": ending_balance,
            "num_trades": num_trades,
            "exit_reason_counts": exit_reason_counts,
            "strategy_plan_blockers": actual_plan_blockers,
        },
        "markers": {
            "loaded_cached_data": _bool_marker(combined, "Loaded cached data (parquet)."),
            "loaded_local_indicator_store": _bool_marker(combined, "Loaded local indicator store."),
        },
        "checks": checks,
        "all_passed": all(bool(check.get("passed")) for check in checks),
        "stdout_tail": stdout.splitlines()[-80:],
        "stderr_tail": stderr.splitlines()[-80:],
    }


def _status(value: bool) -> str:
    return "PASS" if value else "FAIL"


def _fmt(value: Any) -> str:
    if isinstance(value, float):
        if not math.isfinite(value):
            return str(value)
        return f"{value:.6f}"
    if value is None:
        return "-"
    return str(value)


def _write_report(path: Path, payload: Dict[str, Any]) -> None:
    cases = payload.get("cases", [])
    lines: List[str] = []
    lines.append("# Real Backtest Parity Suite")
    lines.append("")
    lines.append(f"- generated_utc: `{payload.get('generated_utc')}`")
    lines.append(f"- python: `{payload.get('python_exe')}`")
    lines.append(f"- overall_status: `{_status(bool(payload.get('all_passed')) == True)}`")
    lines.append("")
    lines.append("## Cases")
    lines.append("")
    lines.append("| Case | Preset | Status | Wall s | Engine | Ending balance | Trades | Exit reasons | Cached candles | Local store |")
    lines.append("|---|---|---|---:|---|---:|---:|---|---|---|")
    for case in cases:
        summary = case.get("summary", {}) or {}
        markers = case.get("markers", {}) or {}
        lines.append(
            "| "
            + " | ".join(
                [
                    str(case.get("label")),
                    str(case.get("preset")),
                    _status(bool(case.get("all_passed"))),
                    _fmt(case.get("wall_sec")),
                    _fmt(summary.get("strategy_plan_engine")),
                    _fmt(summary.get("ending_balance")),
                    _fmt(summary.get("num_trades")),
                    json.dumps(summary.get("exit_reason_counts", {}), ensure_ascii=False, sort_keys=True),
                    _status(bool(markers.get("loaded_cached_data"))),
                    _status(bool(markers.get("loaded_local_indicator_store"))),
                ]
            )
            + " |"
        )
    lines.append("")
    lines.append("## Checks")
    lines.append("")
    for case in cases:
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
    parser = argparse.ArgumentParser(description="Run a live backtest parity suite against fixed control cases.")
    parser.add_argument("--python", dest="python_exe", default=sys.executable)
    parser.add_argument(
        "--output-dir",
        default="",
        help="Optional output directory. Defaults to runs/perf/real_backtest_parity_suite_<timestamp>.",
    )
    args = parser.parse_args()

    if str(args.output_dir or "").strip():
        output_dir = Path(args.output_dir).resolve()
    else:
        stamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
        output_dir = REPO_ROOT / "runs" / "perf" / f"real_backtest_parity_suite_{stamp}"
    output_dir.mkdir(parents=True, exist_ok=True)

    case_results = [_run_case(case=case, python_exe=str(args.python_exe), output_dir=output_dir) for case in CASES]
    all_passed = all(bool(case.get("all_passed")) for case in case_results)

    payload = {
        "generated_utc": datetime.now(timezone.utc).isoformat(),
        "python_exe": str(args.python_exe),
        "repo_root": str(REPO_ROOT),
        "all_passed": all_passed,
        "cases": case_results,
    }

    summary_path = output_dir / "summary.json"
    report_path = output_dir / "report.md"
    summary_path.write_text(json.dumps(payload, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
    _write_report(report_path, payload)

    print(f"Suite output: {output_dir}", flush=True)
    print(f"Overall: {_status(all_passed)}", flush=True)
    for case in case_results:
        print(
            f"- {case['label']}: {_status(bool(case.get('all_passed')))} "
            f"(engine={case['summary'].get('strategy_plan_engine')}, "
            f"ending_balance={case['summary'].get('ending_balance')}, "
            f"trades={case['summary'].get('num_trades')})",
            flush=True,
        )

    return 0 if all_passed else 1


if __name__ == "__main__":
    raise SystemExit(main())
