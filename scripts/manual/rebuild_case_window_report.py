from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

import pandas as pd

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from app.services.ui_v2_runtime import BacktestRuntimeService, RunOverrides


def default_workspace_root() -> Path:
    script_path = Path(__file__).resolve()
    return script_path.parents[4]


def default_repo_root() -> Path:
    script_path = Path(__file__).resolve()
    return script_path.parents[2]


def default_casebook_root() -> Path:
    return default_workspace_root() / "research_exports" / "trade_entry_casebook"


def default_report_json() -> Path:
    return default_casebook_root() / "preset_drafts" / "backtest_case_period_0001_0005.json"


def default_presets_path() -> Path:
    return default_casebook_root() / "preset_drafts" / "research_case_family_presets_v1.json"


def default_report_md(report_json: Path) -> Path:
    return report_json.with_suffix(".md")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Rebuild the research case-window backtest report from the current preset pack."
    )
    parser.add_argument("--report-json", type=Path, default=default_report_json())
    parser.add_argument("--report-md", type=Path, default=None)
    parser.add_argument("--presets-path", type=Path, default=default_presets_path())
    parser.add_argument("--repo-root", type=Path, default=default_repo_root())
    return parser


def _load_json(path: Path) -> dict[str, Any]:
    with path.open("r", encoding="utf-8") as handle:
        payload = json.load(handle)
    if not isinstance(payload, dict):
        raise ValueError(f"Expected JSON object at {path}")
    return payload


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    with path.open("w", encoding="utf-8") as handle:
        json.dump(payload, handle, indent=2, ensure_ascii=False)
        handle.write("\n")


def _collect_case_matches(
    *,
    cases: list[dict[str, Any]],
    entry_times: list[pd.Timestamp],
    preset_family: str,
) -> list[dict[str, Any]]:
    matches: list[dict[str, Any]] = []
    for case in cases:
        window_start = pd.Timestamp(case["t_rf_utc"])
        t0 = pd.Timestamp(case["t0_utc"])
        window_end = pd.Timestamp(case["window_end_utc"])
        entries_in_window = [str(entry_time) for entry_time in entry_times if window_start <= entry_time <= window_end]
        nearest_entry = None
        nearest_diff = None
        if entry_times:
            nearest = min(entry_times, key=lambda entry_time: abs((entry_time - t0).total_seconds()))
            nearest_entry = str(nearest)
            nearest_diff = round((nearest - t0).total_seconds() / 60.0, 3)
        matches.append(
            {
                "case_id": str(case["case_id"]),
                "expected_family": str(case["preset_family"]),
                "window_start_utc": str(case["t_rf_utc"]),
                "t0_utc": str(case["t0_utc"]),
                "window_end_utc": str(case["window_end_utc"]),
                "entries_in_window": entries_in_window,
                "entries_in_window_count": len(entries_in_window),
                "nearest_entry_to_t0_utc": nearest_entry,
                "nearest_entry_to_t0_diff_min": nearest_diff,
                "family_match": bool(str(case["preset_family"]) == preset_family and len(entries_in_window) > 0),
            }
        )
    return matches


def _build_markdown(report: dict[str, Any], presets_path: Path) -> str:
    lines: list[str] = []
    period = dict(report.get("period") or {})
    lines.append("# Backtest Report For Case Window 0001-0005")
    lines.append("")
    lines.append(f"- Window start (UTC): `{period.get('start_utc', '')}`")
    lines.append(f"- Window end (UTC): `{period.get('end_utc', '')}`")
    lines.append(f"- Preset pack: `{presets_path.name}`")
    lines.append("")

    presets = dict(report.get("presets") or {})
    for preset_name in sorted(presets.keys()):
        block = dict(presets[preset_name] or {})
        summary = dict(block.get("summary") or {})
        entry_filter_long = dict(block.get("entry_filter_long") or {})
        lines.append(f"## Preset {preset_name} ({block.get('preset_family', '')})")
        lines.append("")
        lines.append(f"- Trades: `{summary.get('trades', summary.get('num_trades'))}`")
        lines.append(f"- Wins / Losses: `{summary.get('wins')}` / `{summary.get('losses')}`")
        lines.append(f"- Win rate: `{summary.get('win_rate_pct')}`")
        lines.append(f"- Profit factor: `{summary.get('profit_factor')}`")
        lines.append(f"- Max drawdown %: `{summary.get('max_drawdown_pct')}`")
        lines.append(f"- Entry filter active (Long): `{entry_filter_long.get('enabled')}`")
        lines.append(f"- Entry filter mode (Long): `{entry_filter_long.get('mode')}`")
        lines.append(f"- Verification bundle: `{block.get('verification_bundle')}`")
        lines.append("")
        lines.append("Case window hits:")
        lines.append("")
        for match in list(block.get("case_matches") or []):
            lines.append(
                f"- `{match['case_id']}` expected `{match['expected_family']}`: "
                f"entries in window = `{match['entries_in_window_count']}`; "
                f"nearest diff to T0 = `{match['nearest_entry_to_t0_diff_min']}` min"
            )
        lines.append("")
    return "\n".join(lines).rstrip() + "\n"


def rebuild_case_window_report(
    *,
    report_json: Path,
    report_md: Path,
    presets_path: Path,
    repo_root: Path,
) -> dict[str, Any]:
    report = _load_json(report_json)
    period = dict(report.get("period") or {})
    start_utc = str(period.get("start_utc") or "")
    end_utc = str(period.get("end_utc") or "")
    if not start_utc or not end_utc:
        raise ValueError("Report JSON must contain period.start_utc and period.end_utc.")

    cases = list(report.get("cases") or [])
    presets = dict(report.get("presets") or {})
    svc = BacktestRuntimeService(presets_path=presets_path, repo_root=repo_root)

    for preset_name, block_raw in presets.items():
        block = dict(block_raw or {})
        run = svc.run_saved_preset(str(preset_name), RunOverrides(start=start_utc, end=end_utc))
        trades_payload = svc.list_run_trades(preset_name=str(preset_name))
        trades = list(trades_payload.get("trades") or [])
        entry_times = sorted(pd.Timestamp(trade["entry_time"]) for trade in trades)
        summary = dict(run.get("summary") or {})
        entry_filter_long = dict((summary.get("entry_filter") or {}).get("Long Entry") or {})
        case_matches = _collect_case_matches(
            cases=cases,
            entry_times=entry_times,
            preset_family=str(block.get("preset_family") or ""),
        )
        block["verification_bundle"] = str(run.get("verification_bundle") or "")
        block["entry_filter_long"] = entry_filter_long
        block["summary"] = summary
        block["entry_times_utc"] = [str(entry_time) for entry_time in entry_times]
        block["case_matches"] = case_matches
        block["trade_count_from_csv"] = len(entry_times)
        presets[preset_name] = block

    report["presets"] = presets
    _write_json(report_json, report)
    report_md.write_text(_build_markdown(report, presets_path), encoding="utf-8")
    return report


def main() -> int:
    args = build_parser().parse_args()
    report_json = args.report_json.resolve()
    report_md = (args.report_md.resolve() if args.report_md else default_report_md(report_json))
    presets_path = args.presets_path.resolve()
    repo_root = args.repo_root.resolve()

    report = rebuild_case_window_report(
        report_json=report_json,
        report_md=report_md,
        presets_path=presets_path,
        repo_root=repo_root,
    )

    presets = dict(report.get("presets") or {})
    compact = {
        preset_name: {
            "trades": (dict(block.get("summary") or {}).get("num_trades") or dict(block.get("summary") or {}).get("trades")),
            "verification_bundle": block.get("verification_bundle"),
        }
        for preset_name, block in presets.items()
    }
    print(json.dumps({"report_json": str(report_json), "report_md": str(report_md), "presets": compact}, indent=2, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
