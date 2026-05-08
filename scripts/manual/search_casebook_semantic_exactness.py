from __future__ import annotations

import argparse
import json
import tempfile
from copy import deepcopy
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable

import pandas as pd

REPO_ROOT = Path(__file__).resolve().parents[2]
WORKSPACE_ROOT = Path(__file__).resolve().parents[4]
CASEBOOK_ROOT = WORKSPACE_ROOT / "research_exports" / "trade_entry_casebook"
DEFAULT_PACK = CASEBOOK_ROOT / "preset_drafts" / "research_case_family_presets_casebook_filter_v1_common_shell_v3.json"
DEFAULT_CASE_REPORT = CASEBOOK_ROOT / "preset_drafts" / "backtest_case_period_0001_0005.json"
DEFAULT_ORACLE = CASEBOOK_ROOT / "presentation" / "exact_reference_engine_0001_0005" / "exact_reference_engine_0001_0005.json"
DEFAULT_OUTPUT_ROOT = CASEBOOK_ROOT / "presentation" / "semantic_exactness_search"

import sys

if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from app.services.ui_v2_runtime import BacktestRuntimeService, RunOverrides


@dataclass(frozen=True)
class SearchFamily:
    preset_name: str
    family: str


FAMILIES: tuple[SearchFamily, ...] = (
    SearchFamily("901", "P1"),
    SearchFamily("902", "P2"),
    SearchFamily("903", "P3"),
)


def _load_json(path: Path) -> dict[str, Any]:
    with path.open("r", encoding="utf-8") as handle:
        payload = json.load(handle)
    if not isinstance(payload, dict):
        raise ValueError(f"Expected JSON object at {path}")
    return payload


def _ts(value: str) -> pd.Timestamp:
    ts = pd.Timestamp(value)
    if ts.tzinfo is None:
        return ts.tz_localize("UTC")
    return ts.tz_convert("UTC")


def _phase_variants(family: str) -> list[tuple[list[str], list[str]]]:
    if family == "P1":
        return [
            (["BUY_WEAK", "NEUTRAL"], []),
            (["BUY_WEAK"], []),
            (["NEUTRAL"], []),
            (["BUY_WEAK", "NEUTRAL"], ["SELL_WEAK", "SELL_STRONG"]),
        ]
    if family == "P2":
        return [
            (["BUY_WEAK"], []),
            (["BUY_WEAK", "NEUTRAL"], []),
            (["BUY_WEAK"], ["SELL_WEAK", "SELL_STRONG"]),
        ]
    return [
        ([], ["SELL_WEAK", "SELL_STRONG"]),
        (["BUY_WEAK", "NEUTRAL"], ["SELL_WEAK", "SELL_STRONG"]),
    ]


def _min_retrace_variants(family: str) -> list[float]:
    if family == "P1":
        return [35.0, 40.0, 50.0]
    if family == "P2":
        return [80.0, 90.0, 100.0]
    return [30.0, 40.0, 50.0]


def _signal_window_variants(family: str) -> list[tuple[int, int]]:
    if family == "P1":
        return [(0, 3), (0, 4), (1, 4), (2, 4), (3, 4), (0, 5)]
    if family == "P2":
        return [(6, 18), (6, 24), (6, 30), (8, 24), (10, 24), (12, 30)]
    return [(12, 30), (16, 32), (20, 40), (24, 40)]


def _reclaim_window_variants(family: str) -> list[int]:
    if family == "P2":
        return [4, 6, 8]
    return [4]


def _iter_candidate_configs(family: str) -> Iterable[dict[str, Any]]:
    for min_retrace in _min_retrace_variants(family):
        for min_signal_bars, max_signal_bars in _signal_window_variants(family):
            for reclaim_window_bars in _reclaim_window_variants(family):
                for simple_phases, delayed_phases in _phase_variants(family):
                    yield {
                        "min_retrace_pct": float(min_retrace),
                        "casebook_min_signal_bars": int(min_signal_bars),
                        "casebook_max_signal_bars": int(max_signal_bars),
                        "casebook_reclaim_window_bars": int(reclaim_window_bars),
                        "casebook_simple_reclaim_phases": list(simple_phases),
                        "casebook_delayed_rf_buy_phases": list(delayed_phases),
                    }


def _evaluate_family(
    *,
    base_pack: dict[str, Any],
    period: dict[str, Any],
    expected_by_case: dict[str, pd.Timestamp],
    cases_by_family: dict[str, list[dict[str, Any]]],
    family_cfg: SearchFamily,
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    family_cases = list(cases_by_family.get(family_cfg.family) or [])
    candidates = list(_iter_candidate_configs(family_cfg.family))
    with tempfile.TemporaryDirectory() as td:
        temp_root = Path(td)
        for idx, candidate in enumerate(candidates, start=1):
            payload = deepcopy(base_pack)
            filt = payload["presets"][family_cfg.preset_name]["entry_filters"]["Long Entry"]
            for key, value in candidate.items():
                filt[key] = value
            temp_pack = temp_root / f"{family_cfg.preset_name}.json"
            temp_pack.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")

            svc = BacktestRuntimeService(presets_path=temp_pack, repo_root=REPO_ROOT)
            svc.run_saved_preset(
                family_cfg.preset_name,
                RunOverrides(start=str(period["start_utc"]), end=str(period["end_utc"])),
            )
            trades_payload = svc.list_run_trades(preset_name=family_cfg.preset_name)
            entry_times = sorted(_ts(str(trade["entry_time"])) for trade in list(trades_payload.get("trades") or []))

            case_results: list[dict[str, Any]] = []
            exact_count = 0
            for case in family_cases:
                case_id = str(case["case_id"])
                window_start = _ts(str(case["t_rf_utc"]))
                window_end = _ts(str(case["window_end_utc"]))
                in_window = [entry for entry in entry_times if window_start <= entry <= window_end]
                actual_fill = in_window[0] if len(in_window) == 1 else None
                expected_fill = expected_by_case[case_id]
                exact = bool(actual_fill is not None and actual_fill == expected_fill)
                if exact:
                    exact_count += 1
                case_results.append(
                    {
                        "case_id": case_id,
                        "expected_fill_open_ts": expected_fill.isoformat(),
                        "actual_fill_open_ts": actual_fill.isoformat() if actual_fill is not None else "",
                        "entries_in_window": len(in_window),
                        "exact_fill_match": exact,
                    }
                )

            row = {
                "preset_name": family_cfg.preset_name,
                "family": family_cfg.family,
                "exact_count": exact_count,
                "case_count": len(family_cases),
                "candidate": candidate,
                "case_results": case_results,
            }
            rows.append(row)
            if idx == 1 or idx == len(candidates) or idx % 20 == 0:
                print(
                    f"{family_cfg.family}: candidate {idx}/{len(candidates)} exact={exact_count}/{len(family_cases)}",
                    flush=True,
                )

    rows.sort(
        key=lambda row: (
            -int(row["exact_count"]),
            float(dict(row["candidate"]).get("min_retrace_pct", 0.0)),
            int(dict(row["candidate"]).get("casebook_min_signal_bars", 0)),
            int(dict(row["candidate"]).get("casebook_max_signal_bars", 0)),
            int(dict(row["candidate"]).get("casebook_reclaim_window_bars", 0)),
            json.dumps(dict(row["candidate"]), ensure_ascii=False, sort_keys=True),
        )
    )
    return rows


def _build_markdown(results: list[dict[str, Any]]) -> str:
    lines: list[str] = []
    lines.append("# Casebook Semantic Exactness Search")
    lines.append("")
    lines.append("| Family | Preset | Exact | Candidate |")
    lines.append("|---|---|---:|---|")
    for row in results:
        lines.append(
            f"| {row['family']} | {row['preset_name']} | {row['exact_count']}/{row['case_count']} | `{json.dumps(row['candidate'], ensure_ascii=False, sort_keys=True)}` |"
        )
    lines.append("")
    for row in results:
        lines.append(f"## {row['family']} / {row['preset_name']}")
        lines.append("")
        lines.append(f"- Best exactness: `{row['exact_count']}/{row['case_count']}`")
        lines.append(f"- Candidate: `{json.dumps(row['candidate'], ensure_ascii=False, sort_keys=True)}`")
        lines.append("")
        lines.append("| Case | Expected fill UTC | Actual fill UTC | Entries in window | Exact |")
        lines.append("|---|---|---|---:|---|")
        for case in list(row["case_results"]):
            lines.append(
                f"| {case['case_id']} | {case['expected_fill_open_ts']} | {case['actual_fill_open_ts'] or '-'} | {case['entries_in_window']} | {case['exact_fill_match']} |"
            )
        lines.append("")
    return "\n".join(lines).rstrip() + "\n"


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Search semantic casebook filter configs family-by-family on the current engine.")
    parser.add_argument("--pack", type=Path, default=DEFAULT_PACK)
    parser.add_argument("--case-report", type=Path, default=DEFAULT_CASE_REPORT)
    parser.add_argument("--oracle", type=Path, default=DEFAULT_ORACLE)
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT)
    parser.add_argument(
        "--family",
        action="append",
        choices=[cfg.family for cfg in FAMILIES],
        help="Optional family selector. Can be passed multiple times.",
    )
    return parser


def main() -> int:
    args = build_parser().parse_args()
    pack = _load_json(args.pack.resolve())
    case_report = _load_json(args.case_report.resolve())
    oracle = _load_json(args.oracle.resolve())

    expected_by_case = {
        str(row["case_id"]): _ts(str(row["expected_fill_open_ts"]))
        for row in list(oracle.get("results") or [])
    }
    cases_by_family: dict[str, list[dict[str, Any]]] = {}
    for case in list(case_report.get("cases") or []):
        cases_by_family.setdefault(str(case["preset_family"]), []).append(dict(case))

    selected_families = set(args.family or [])
    family_list = [cfg for cfg in FAMILIES if not selected_families or cfg.family in selected_families]

    best_rows: list[dict[str, Any]] = []
    for family_cfg in family_list:
        rows = _evaluate_family(
            base_pack=pack,
            period=dict(case_report["period"]),
            expected_by_case=expected_by_case,
            cases_by_family=cases_by_family,
            family_cfg=family_cfg,
        )
        if rows:
            best_rows.append(rows[0])

    payload = {
        "generated_utc": datetime.now(timezone.utc).isoformat(),
        "pack": str(args.pack.resolve()),
        "case_report": str(args.case_report.resolve()),
        "oracle": str(args.oracle.resolve()),
        "results": best_rows,
    }

    output_root = args.output_root.resolve()
    output_root.mkdir(parents=True, exist_ok=True)
    json_path = output_root / "casebook_semantic_exactness_search.json"
    md_path = output_root / "casebook_semantic_exactness_search.md"
    json_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    md_path.write_text(_build_markdown(best_rows), encoding="utf-8")

    print(json.dumps({"output_json": str(json_path), "output_md": str(md_path)}, ensure_ascii=False, indent=2))
    for row in best_rows:
        print(
            f"{row['family']} {row['preset_name']}: {row['exact_count']}/{row['case_count']} exact "
            f"{json.dumps(row['candidate'], ensure_ascii=False, sort_keys=True)}",
            flush=True,
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
