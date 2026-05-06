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
class ExpectedTrade:
    trade_id: int
    side: str
    entry_time: str
    exit_time: str
    entry_price: float
    exit_price: float
    net_pnl: float
    entry_reason: str
    exit_reason: str


@dataclass(frozen=True)
class TradeParityCase:
    label: str
    preset: str
    start: str
    end: str
    expected_engine: str
    expected_plan_blockers: List[str]
    expected_trades: List[ExpectedTrade]


CASES: List[TradeParityCase] = [
    TradeParityCase(
        label="preset_901_trade_parity",
        preset="901",
        start="2026-04-20 00:00:00+00:00",
        end="2026-04-20 04:00:00+00:00",
        expected_engine="legacy_generic",
        expected_plan_blockers=["sequence_groups"],
        expected_trades=[
            ExpectedTrade(
                trade_id=1,
                side="LONG",
                entry_time="2026-04-20 03:47:00+00:00",
                exit_time="2026-04-20 03:59:59.999000+00:00",
                entry_price=74555.3,
                exit_price=74588.1,
                net_pnl=-0.036023408127922624,
                entry_reason="Long Entry (Entry Filter confirmed +1 bar)",
                exit_reason="Force Close (End)",
            ),
        ],
    ),
    TradeParityCase(
        label="preset_902_trade_parity",
        preset="902",
        start="2026-04-20 00:00:00+00:00",
        end="2026-04-20 04:00:00+00:00",
        expected_engine="compiled_bar",
        expected_plan_blockers=[],
        expected_trades=[
            ExpectedTrade(
                trade_id=1,
                side="LONG",
                entry_time="2026-04-20 00:11:00+00:00",
                exit_time="2026-04-20 02:05:00+00:00",
                entry_price=74001.2,
                exit_price=74452.7,
                net_pnl=0.5298811911158197,
                entry_reason="Long Entry",
                exit_reason="Long Exit",
            ),
            ExpectedTrade(
                trade_id=2,
                side="LONG",
                entry_time="2026-04-20 03:55:00+00:00",
                exit_time="2026-04-20 03:59:59.999000+00:00",
                entry_price=74592.1,
                exit_price=74588.1,
                net_pnl=-0.08536035317412971,
                entry_reason="Long Entry",
                exit_reason="Force Close (End)",
            ),
        ],
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


def _normalize_string_list(value: Any) -> List[str]:
    if not isinstance(value, list):
        return []
    out: List[str] = []
    for item in value:
        text = str(item or "").strip()
        if text:
            out.append(text)
    return sorted(out)


def _bool_marker(text: str, needle: str) -> bool:
    return needle in text


def _run_case(*, case: TradeParityCase, python_exe: str, output_dir: Path) -> Dict[str, Any]:
    case_dir = output_dir / case.label
    case_dir.mkdir(parents=True, exist_ok=True)
    result_json = case_dir / "result.json"
    command = [
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
        command,
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
    actual_trades = list(payload.get("sample_trades") or [])
    plan_blockers = _normalize_string_list(summary.get("strategy_plan_blockers"))
    expected_plan_blockers = sorted(case.expected_plan_blockers)

    checks: List[Dict[str, Any]] = [
        {
            "name": "returncode_zero",
            "passed": proc.returncode == 0,
            "detail": f"returncode={proc.returncode}",
        },
        {
            "name": "result_json_exists",
            "passed": result_json.exists(),
            "detail": str(result_json),
        },
        {
            "name": "engine_match",
            "passed": str(summary.get("strategy_plan_engine", "") or "") == case.expected_engine,
            "detail": f"expected={case.expected_engine} actual={summary.get('strategy_plan_engine')}",
        },
        {
            "name": "plan_blockers_match",
            "passed": plan_blockers == expected_plan_blockers,
            "detail": f"expected={expected_plan_blockers} actual={plan_blockers}",
        },
        {
            "name": "trade_count_match",
            "passed": len(actual_trades) == len(case.expected_trades),
            "detail": f"expected={len(case.expected_trades)} actual={len(actual_trades)}",
        },
        {
            "name": "loaded_cached_data",
            "passed": _bool_marker(combined, "Loaded cached data (parquet)."),
            "detail": "stdout contains cached candle marker",
        },
        {
            "name": "loaded_local_indicator_store",
            "passed": _bool_marker(combined, "Loaded local indicator store."),
            "detail": "stdout contains local indicator store reuse marker",
        },
    ]

    for index, expected_trade in enumerate(case.expected_trades):
        actual = actual_trades[index] if index < len(actual_trades) and isinstance(actual_trades[index], dict) else {}
        prefix = f"trade_{index + 1}"
        checks.extend(
            [
                {
                    "name": f"{prefix}_trade_id",
                    "passed": _safe_int(actual.get("trade_id")) == expected_trade.trade_id,
                    "detail": f"expected={expected_trade.trade_id} actual={actual.get('trade_id')}",
                },
                {
                    "name": f"{prefix}_side",
                    "passed": str(actual.get("side", "") or "") == expected_trade.side,
                    "detail": f"expected={expected_trade.side} actual={actual.get('side')}",
                },
                {
                    "name": f"{prefix}_entry_time",
                    "passed": str(actual.get("entry_time", "") or "") == expected_trade.entry_time,
                    "detail": f"expected={expected_trade.entry_time} actual={actual.get('entry_time')}",
                },
                {
                    "name": f"{prefix}_exit_time",
                    "passed": str(actual.get("exit_time", "") or "") == expected_trade.exit_time,
                    "detail": f"expected={expected_trade.exit_time} actual={actual.get('exit_time')}",
                },
                {
                    "name": f"{prefix}_entry_price",
                    "passed": _approx_equal(actual.get("entry_price"), expected_trade.entry_price),
                    "detail": f"expected={expected_trade.entry_price} actual={actual.get('entry_price')}",
                },
                {
                    "name": f"{prefix}_exit_price",
                    "passed": _approx_equal(actual.get("exit_price"), expected_trade.exit_price),
                    "detail": f"expected={expected_trade.exit_price} actual={actual.get('exit_price')}",
                },
                {
                    "name": f"{prefix}_net_pnl",
                    "passed": _approx_equal(actual.get("net_pnl"), expected_trade.net_pnl, tol=1e-6),
                    "detail": f"expected={expected_trade.net_pnl} actual={actual.get('net_pnl')}",
                },
                {
                    "name": f"{prefix}_entry_reason",
                    "passed": str(actual.get("entry_reason", "") or "") == expected_trade.entry_reason,
                    "detail": f"expected={expected_trade.entry_reason} actual={actual.get('entry_reason')}",
                },
                {
                    "name": f"{prefix}_exit_reason",
                    "passed": str(actual.get("exit_reason", "") or "") == expected_trade.exit_reason,
                    "detail": f"expected={expected_trade.exit_reason} actual={actual.get('exit_reason')}",
                },
            ]
        )

    all_passed = all(bool(check.get("passed")) for check in checks)
    return {
        "label": case.label,
        "preset": case.preset,
        "returncode": int(proc.returncode),
        "wall_sec": round(wall_sec, 4),
        "result_json": str(result_json),
        "summary": {
            "strategy_plan_engine": str(summary.get("strategy_plan_engine", "") or ""),
            "strategy_plan_blockers": plan_blockers,
            "sample_trades": actual_trades,
        },
        "checks": checks,
        "all_passed": all_passed,
        "stdout_tail": stdout.splitlines()[-80:],
        "stderr_tail": stderr.splitlines()[-80:],
        "markers": {
            "loaded_cached_data": _bool_marker(combined, "Loaded cached data (parquet)."),
            "loaded_local_indicator_store": _bool_marker(combined, "Loaded local indicator store."),
        },
    }


def _write_report(path: Path, payload: Dict[str, Any]) -> None:
    lines: List[str] = []
    lines.append("# Real Trade Parity Suite")
    lines.append("")
    lines.append(f"- generated_utc: `{payload.get('generated_utc')}`")
    lines.append(f"- python: `{payload.get('python_exe')}`")
    lines.append(f"- overall: `{'PASS' if payload.get('all_passed') else 'FAIL'}`")
    lines.append("")
    lines.append("## Cases")
    lines.append("")
    lines.append("| Case | Preset | RC | Wall s | Engine | Trades | Cached candles | Local store |")
    lines.append("|---|---|---:|---:|---|---:|---|---|")
    for case in payload.get("cases", []):
        summary = case.get("summary", {}) or {}
        markers = case.get("markers", {}) or {}
        sample_trades = list(summary.get("sample_trades") or [])
        lines.append(
            "| "
            + " | ".join(
                [
                    str(case.get("label") or ""),
                    str(case.get("preset") or ""),
                    str(case.get("returncode")),
                    str(case.get("wall_sec")),
                    str(summary.get("strategy_plan_engine") or ""),
                    str(len(sample_trades)),
                    "yes" if markers.get("loaded_cached_data") else "no",
                    "yes" if markers.get("loaded_local_indicator_store") else "no",
                ]
            )
            + " |"
        )
    lines.append("")
    lines.append("## Checks")
    lines.append("")
    lines.append("| Check | Status | Detail |")
    lines.append("|---|---|---|")
    for case in payload.get("cases", []):
        lines.append(f"| `{case.get('label')}` |  |  |")
        for check in case.get("checks", []):
            lines.append(
                f"| {check.get('name')} | {'PASS' if check.get('passed') else 'FAIL'} | {check.get('detail')} |"
            )
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def main() -> int:
    parser = argparse.ArgumentParser(description="Run live trade-by-trade parity checks for critical presets.")
    parser.add_argument("--python", dest="python_exe", default=sys.executable)
    parser.add_argument("--output-dir", default=None)
    args = parser.parse_args()

    stamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    output_dir = (
        Path(args.output_dir).resolve()
        if args.output_dir
        else (REPO_ROOT / "runs" / "perf" / f"real_trade_parity_suite_{stamp}")
    )
    output_dir.mkdir(parents=True, exist_ok=True)

    cases = [_run_case(case=case, python_exe=str(args.python_exe), output_dir=output_dir) for case in CASES]
    all_passed = all(bool(case.get("all_passed")) for case in cases)
    payload = {
        "generated_utc": datetime.now(timezone.utc).isoformat(),
        "python_exe": str(args.python_exe),
        "repo_root": str(REPO_ROOT),
        "all_passed": bool(all_passed),
        "cases": cases,
    }
    summary_path = output_dir / "summary.json"
    report_path = output_dir / "report.md"
    summary_path.write_text(json.dumps(payload, indent=2, ensure_ascii=False, default=str), encoding="utf-8")
    _write_report(report_path, payload)

    latest_dir = REPO_ROOT / "runs" / "perf" / "real_trade_parity_suite_latest"
    if latest_dir.exists():
        for child in latest_dir.iterdir():
            if child.is_file():
                child.unlink()
            else:
                import shutil
                shutil.rmtree(child)
    else:
        latest_dir.mkdir(parents=True, exist_ok=True)
    for child in output_dir.iterdir():
        target = latest_dir / child.name
        if child.is_file():
            target.write_bytes(child.read_bytes())
        else:
            import shutil
            shutil.copytree(child, target, dirs_exist_ok=True)

    print(f"Overall: {'PASS' if all_passed else 'FAIL'}", flush=True)
    print(f"Saved summary JSON: {summary_path}", flush=True)
    print(f"Saved report MD: {report_path}", flush=True)
    print(f"Updated latest snapshot: {latest_dir}", flush=True)
    return 0 if all_passed else 1


if __name__ == "__main__":
    raise SystemExit(main())
