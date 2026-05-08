from __future__ import annotations

import argparse
import json
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
import sys
from typing import Any, Dict, List

import pandas as pd


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from app.services.ui_v2_runtime import BacktestRuntimeService, RunOverrides


@dataclass(frozen=True)
class CausalityCase:
    label: str
    preset: str
    start: str
    end: str


CASES: List[CausalityCase] = [
    CausalityCase(
        label="compiled_no_trade",
        preset="010",
        start="2026-04-20 00:00:00+00:00",
        end="2026-04-20 04:00:00+00:00",
    ),
    CausalityCase(
        label="compiled_pullback_single_trade",
        preset="901",
        start="2026-04-20 00:00:00+00:00",
        end="2026-04-20 04:00:00+00:00",
    ),
    CausalityCase(
        label="compiled_pullback_two_trades",
        preset="902",
        start="2026-04-20 00:00:00+00:00",
        end="2026-04-20 04:00:00+00:00",
    ),
]


def _ts(value: str) -> pd.Timestamp:
    ts = pd.Timestamp(value)
    if ts.tzinfo is None:
        return ts.tz_localize("UTC")
    return ts.tz_convert("UTC")


def _make_check(name: str, passed: bool, detail: str) -> Dict[str, Any]:
    return {
        "name": str(name),
        "passed": bool(passed),
        "detail": str(detail),
    }


def _trade_entry_signature(trade: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "side": str(trade.get("side") or ""),
        "entry_time": str(trade.get("entry_time") or ""),
        "entry_price": float(trade.get("entry_price") or 0.0),
        "entry_reason": str(trade.get("entry_reason") or ""),
    }


def _collect_entry_signatures(trades: List[Dict[str, Any]], end_ts: pd.Timestamp) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    for trade in trades:
        entry_time = _ts(str(trade.get("entry_time") or ""))
        if entry_time < end_ts:
            rows.append(_trade_entry_signature(trade))
    return rows


def _checkpoint_grid(case: CausalityCase, full_trades: List[Dict[str, Any]]) -> List[pd.Timestamp]:
    start = _ts(case.start)
    end = _ts(case.end)
    checkpoints: set[pd.Timestamp] = set()

    current = start + pd.Timedelta(minutes=10)
    while current < end:
        checkpoints.add(current)
        current += pd.Timedelta(minutes=10)

    for trade in full_trades:
        entry_time = _ts(str(trade.get("entry_time") or ""))
        before = entry_time
        after = entry_time + pd.Timedelta(minutes=1)
        if start < before < end:
            checkpoints.add(before)
        if start < after < end:
            checkpoints.add(after)

    return sorted(checkpoints)


def _run_case(case: CausalityCase, output_dir: Path) -> Dict[str, Any]:
    svc = BacktestRuntimeService(repo_root=REPO_ROOT)
    full_run = svc.run_saved_preset(case.preset, RunOverrides(start=case.start, end=case.end))
    full_payload = svc.list_run_trades(preset_name=case.preset)
    full_trades = list(full_payload.get("trades") or [])
    full_summary = dict((full_run or {}).get("summary") or {})

    checkpoints = _checkpoint_grid(case, full_trades)
    failures: List[Dict[str, Any]] = []
    checkpoint_rows: List[Dict[str, Any]] = []

    for checkpoint in checkpoints:
        svc.run_saved_preset(case.preset, RunOverrides(start=case.start, end=checkpoint.isoformat()))
        prefix_payload = svc.list_run_trades(preset_name=case.preset)
        prefix_trades = list(prefix_payload.get("trades") or [])

        expected_entries = _collect_entry_signatures(full_trades, checkpoint)
        actual_entries = [_trade_entry_signature(trade) for trade in prefix_trades]
        passed = expected_entries == actual_entries
        row = {
            "checkpoint_end_utc": checkpoint.isoformat(),
            "expected_entry_count": len(expected_entries),
            "actual_entry_count": len(actual_entries),
            "passed": passed,
        }
        checkpoint_rows.append(row)
        if not passed:
            failures.append(
                {
                    "checkpoint_end_utc": checkpoint.isoformat(),
                    "expected_entries": expected_entries,
                    "actual_entries": actual_entries,
                }
            )

    checks = [
        _make_check("strategy_plan_engine_compiled_bar", str(full_summary.get("strategy_plan_engine") or "") == "compiled_bar", f"engine={full_summary.get('strategy_plan_engine')}"),
        _make_check("all_prefix_checkpoints_causal", len(failures) == 0, f"failures={len(failures)} checkpoints={len(checkpoints)}"),
    ]

    return {
        "label": case.label,
        "preset": case.preset,
        "start": case.start,
        "end": case.end,
        "summary": full_summary,
        "full_trades": [_trade_entry_signature(trade) for trade in full_trades],
        "checkpoint_rows": checkpoint_rows,
        "failures": failures,
        "checks": checks,
        "all_passed": all(bool(check.get("passed")) for check in checks),
    }


def _write_report(path: Path, payload: Dict[str, Any]) -> None:
    lines: List[str] = []
    lines.append("# Real Backtest Causality Suite")
    lines.append("")
    lines.append(f"- generated_utc: `{payload.get('generated_utc')}`")
    lines.append(f"- overall: `{'PASS' if payload.get('all_passed') else 'FAIL'}`")
    lines.append("")
    lines.append("| Case | Preset | Status | Checkpoints | Failures | Trades |")
    lines.append("|---|---|---|---:|---:|---:|")
    for case in payload.get("cases", []):
        lines.append(
            f"| {case.get('label')} | {case.get('preset')} | {'PASS' if case.get('all_passed') else 'FAIL'} | {len(case.get('checkpoint_rows') or [])} | {len(case.get('failures') or [])} | {len(case.get('full_trades') or [])} |"
        )
    lines.append("")
    for case in payload.get("cases", []):
        lines.append(f"## {case.get('label')}")
        lines.append("")
        lines.append("| Check | Status | Detail |")
        lines.append("|---|---|---|")
        for check in case.get("checks", []):
            lines.append(f"| {check.get('name')} | {'PASS' if check.get('passed') else 'FAIL'} | {check.get('detail')} |")
        lines.append("")
        if case.get("failures"):
            lines.append("### Failures")
            lines.append("")
            for failure in case.get("failures", []):
                lines.append(f"- `{failure['checkpoint_end_utc']}`")
                lines.append(f"  expected: `{json.dumps(failure['expected_entries'], ensure_ascii=False)}`")
                lines.append(f"  actual: `{json.dumps(failure['actual_entries'], ensure_ascii=False)}`")
            lines.append("")
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def main() -> int:
    parser = argparse.ArgumentParser(description="Check prefix-causality invariance of compiled saved-preset backtests.")
    parser.add_argument(
        "--output-dir",
        default="",
        help="Optional output directory. Defaults to runs/perf/real_backtest_causality_suite_<timestamp>.",
    )
    args = parser.parse_args()

    if str(args.output_dir or "").strip():
        output_dir = Path(args.output_dir).resolve()
    else:
        stamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
        output_dir = REPO_ROOT / "runs" / "perf" / f"real_backtest_causality_suite_{stamp}"
    output_dir.mkdir(parents=True, exist_ok=True)

    case_results = [_run_case(case, output_dir) for case in CASES]
    payload = {
        "generated_utc": datetime.now(timezone.utc).isoformat(),
        "repo_root": str(REPO_ROOT),
        "all_passed": all(bool(case.get("all_passed")) for case in case_results),
        "cases": case_results,
    }

    summary_path = output_dir / "summary.json"
    report_path = output_dir / "report.md"
    summary_path.write_text(json.dumps(payload, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
    _write_report(report_path, payload)

    latest_dir = REPO_ROOT / "runs" / "perf" / "real_backtest_causality_suite_latest"
    if latest_dir.exists():
        import shutil

        shutil.rmtree(latest_dir)
    import shutil

    shutil.copytree(output_dir, latest_dir)

    print(f"Suite output: {output_dir}", flush=True)
    print(f"Overall: {'PASS' if payload['all_passed'] else 'FAIL'}", flush=True)
    for case in case_results:
        print(
            f"- {case['label']}: {'PASS' if case['all_passed'] else 'FAIL'} "
            f"(checkpoints={len(case['checkpoint_rows'])}, failures={len(case['failures'])})",
            flush=True,
        )

    return 0 if payload["all_passed"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
