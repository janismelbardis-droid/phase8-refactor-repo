from __future__ import annotations

import argparse
import copy
import json
import subprocess
import sys
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

CURRENT_DIR = Path(__file__).resolve().parent
if str(CURRENT_DIR) not in sys.path:
    sys.path.insert(0, str(CURRENT_DIR))


WORKSPACE_ROOT = Path(__file__).resolve().parents[4]
CASEBOOK_ROOT = WORKSPACE_ROOT / "research_exports" / "trade_entry_casebook"
BASE_DIR = CASEBOOK_ROOT / "ensemble_stage1_core_context_v1"
BUILD_PORTFOLIO_SCRIPT = CURRENT_DIR / "build_combined_case_window_portfolio.py"

DEFAULT_BASE_PACK = BASE_DIR / "curated_two_exit_sweep_case_window_v2" / "preset_packs" / "be035_only.json"
DEFAULT_CASE_REPORT = BASE_DIR / "curated_two_case_window_report.json"
DEFAULT_PREV_MONTH_REPORT = BASE_DIR / "curated_two_prev_month_report.json"
DEFAULT_OUTPUT_DIR = CASEBOOK_ROOT / "entry_depth_confirmation_search_v1"

P1_NAME = "901__5m_taker_delta_pos"
P2_NAME = "902__5m_taker_delta_pos"


def _load_json(path: Path) -> dict[str, Any]:
    with path.open("r", encoding="utf-8") as handle:
        payload = json.load(handle)
    if not isinstance(payload, dict):
        raise ValueError(f"Expected JSON object at {path}")
    return payload


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as handle:
        json.dump(payload, handle, indent=2, ensure_ascii=False)
        handle.write("\n")


def _entry_filter(
    *,
    enabled: bool,
    mode: str,
    confirm_bars: int,
    reference_range: str,
    touch_type: str,
    min_retrace_pct: float,
    max_retrace_pct: float,
    require_setup_still_valid: bool,
) -> dict[str, Any]:
    return {
        "enabled": bool(enabled),
        "mode": str(mode),
        "expansion_atr_mult": 2.0,
        "hard_skip_atr_mult": 0.0,
        "confirm_bars": int(confirm_bars),
        "reference_range": str(reference_range),
        "touch_type": str(touch_type),
        "min_retrace_pct": float(min_retrace_pct),
        "max_retrace_pct": float(max_retrace_pct),
        "require_next_close_beyond_mid": True,
        "require_setup_still_valid": bool(require_setup_still_valid),
        "apply_to_reversals": True,
        "groups": {},
    }


def _set_long_entry_filter(pack: dict[str, Any], preset_name: str, cfg: dict[str, Any]) -> None:
    presets = dict(pack.get("presets") or {})
    preset = dict(presets.get(preset_name) or {})
    entry_filters = dict(preset.get("entry_filters") or {})
    short_cfg = dict(entry_filters.get("Short Entry") or {})
    entry_filters["Long Entry"] = copy.deepcopy(cfg)
    entry_filters["Short Entry"] = short_cfg
    preset["entry_filters"] = entry_filters
    presets[preset_name] = preset
    pack["presets"] = presets


def _candidate_variants() -> list[dict[str, Any]]:
    p1_current = _entry_filter(
        enabled=True,
        mode="RETRACE_ALWAYS",
        confirm_bars=6,
        reference_range="FULL_CANDLE",
        touch_type="WICK",
        min_retrace_pct=35.0,
        max_retrace_pct=100.0,
        require_setup_still_valid=False,
    )
    p1_current_valid = dict(p1_current)
    p1_current_valid["require_setup_still_valid"] = True

    p1_body_close_35_85 = _entry_filter(
        enabled=True,
        mode="RETRACE_ALWAYS",
        confirm_bars=6,
        reference_range="BODY_ONLY",
        touch_type="CLOSE",
        min_retrace_pct=35.0,
        max_retrace_pct=85.0,
        require_setup_still_valid=True,
    )
    p1_body_close_50_100 = _entry_filter(
        enabled=True,
        mode="RETRACE_ALWAYS",
        confirm_bars=6,
        reference_range="BODY_ONLY",
        touch_type="CLOSE",
        min_retrace_pct=50.0,
        max_retrace_pct=100.0,
        require_setup_still_valid=True,
    )
    p2_full_wick_35_85 = _entry_filter(
        enabled=True,
        mode="RETRACE_ALWAYS",
        confirm_bars=3,
        reference_range="FULL_CANDLE",
        touch_type="WICK",
        min_retrace_pct=35.0,
        max_retrace_pct=85.0,
        require_setup_still_valid=True,
    )
    p2_body_close_35_100 = _entry_filter(
        enabled=True,
        mode="RETRACE_ALWAYS",
        confirm_bars=6,
        reference_range="BODY_ONLY",
        touch_type="CLOSE",
        min_retrace_pct=35.0,
        max_retrace_pct=100.0,
        require_setup_still_valid=True,
    )
    p2_body_close_50_100 = _entry_filter(
        enabled=True,
        mode="RETRACE_ALWAYS",
        confirm_bars=6,
        reference_range="BODY_ONLY",
        touch_type="CLOSE",
        min_retrace_pct=50.0,
        max_retrace_pct=100.0,
        require_setup_still_valid=True,
    )

    return [
        {
            "label": "baseline_be035",
            "description": "Current baseline: P1 retrace gate, P2 open.",
            "filters": {},
        },
        {
            "label": "p1_require_alive",
            "description": "Keep P1 depth gate but require setup to stay alive while waiting.",
            "filters": {P1_NAME: p1_current_valid},
        },
        {
            "label": "p1_body_close_35_85",
            "description": "P1 only: pullback into signal body with close confirmation, 35-85%.",
            "filters": {P1_NAME: p1_body_close_35_85},
        },
        {
            "label": "p1_body_close_50_100",
            "description": "P1 only: deeper body-close pullback, 50-100%.",
            "filters": {P1_NAME: p1_body_close_50_100},
        },
        {
            "label": "p2_full_wick_35_85",
            "description": "P2 only: add moderate full-candle wick retrace gate, 35-85%.",
            "filters": {P2_NAME: p2_full_wick_35_85},
        },
        {
            "label": "p2_body_close_35_100",
            "description": "P2 only: require body-close pullback confirmation, 35-100%.",
            "filters": {P2_NAME: p2_body_close_35_100},
        },
        {
            "label": "both_moderate",
            "description": "P1 alive-check plus P2 moderate retrace gate.",
            "filters": {
                P1_NAME: p1_current_valid,
                P2_NAME: p2_full_wick_35_85,
            },
        },
        {
            "label": "both_body_close",
            "description": "Both presets require body-close confirmation before entry.",
            "filters": {
                P1_NAME: p1_body_close_35_85,
                P2_NAME: p2_body_close_35_100,
            },
        },
        {
            "label": "both_body_close_deep",
            "description": "Both presets require deeper body-close pullbacks before entry.",
            "filters": {
                P1_NAME: p1_body_close_50_100,
                P2_NAME: p2_body_close_50_100,
            },
        },
    ]


def _collect_case_hits(result: dict[str, Any]) -> list[str]:
    hits: set[str] = set()
    for trade in list(result.get("portfolio_trades") or []):
        for case_id in list(trade.get("case_hits") or []):
            hits.add(str(case_id))
    return sorted(hits)


def _variant_sort_key(row: dict[str, Any]) -> tuple[Any, ...]:
    prev = dict(row.get("prev_month") or {})
    case = dict(row.get("case_window") or {})
    return (
        int(case.get("case_hit_count") or 0),
        float(prev.get("net_profit") or -9999.0),
        float(prev.get("profit_factor") or -9999.0),
        -float(prev.get("max_drawdown_pct") or 9999.0),
        -int(prev.get("trades") or 0),
    )


def _summary_row(result: dict[str, Any]) -> dict[str, Any]:
    summary = dict(result.get("portfolio_summary") or {})
    return {
        "trades": int(summary.get("trades") or 0),
        "wins": int(summary.get("wins") or 0),
        "losses": int(summary.get("losses") or 0),
        "win_rate_pct": summary.get("win_rate_pct"),
        "net_profit": summary.get("net_profit"),
        "profit_factor": summary.get("profit_factor"),
        "max_drawdown_pct": summary.get("max_drawdown_pct"),
    }


def _run_portfolio_build(
    *,
    report_json: Path,
    presets_path: Path,
    output_json: Path,
) -> dict[str, Any]:
    output_md = output_json.with_suffix(".md")
    output_json.parent.mkdir(parents=True, exist_ok=True)
    output_md.parent.mkdir(parents=True, exist_ok=True)
    subprocess.run(
        [
            sys.executable,
            str(BUILD_PORTFOLIO_SCRIPT),
            "--report-json",
            str(report_json),
            "--presets-path",
            str(presets_path),
            "--repo-root",
            str(REPO_ROOT),
            "--output-json",
            str(output_json),
            "--output-md",
            str(output_md),
        ],
        check=True,
        cwd=str(REPO_ROOT),
    )
    return _load_json(output_json)


def _build_markdown(rows: list[dict[str, Any]], output_dir: Path) -> str:
    lines: list[str] = []
    lines.append("# Entry Depth + Confirmation Search")
    lines.append("")
    lines.append(f"- Output root: `{output_dir}`")
    lines.append(f"- Candidates tested: `{len(rows)}`")
    lines.append("")
    lines.append("| Rank | Candidate | Case hits | Case PF | Prev PF | Prev net | Prev DD % | Prev trades |")
    lines.append("|---:|---|---:|---:|---:|---:|---:|---:|")
    for idx, row in enumerate(rows, start=1):
        case = dict(row.get("case_window") or {})
        prev = dict(row.get("prev_month") or {})
        lines.append(
            f"| {idx} | {row['label']} | {case.get('case_hit_count')} | {case.get('profit_factor')} | "
            f"{prev.get('profit_factor')} | {prev.get('net_profit')} | {prev.get('max_drawdown_pct')} | {prev.get('trades')} |"
        )
    lines.append("")
    lines.append("## Notes")
    lines.append("")
    for idx, row in enumerate(rows, start=1):
        case = dict(row.get("case_window") or {})
        prev = dict(row.get("prev_month") or {})
        lines.append(
            f"- `{idx}. {row['label']}`: {row['description']} "
            f"(hits: {','.join(list(case.get('case_hits') or [])) or '-'}, prev net: {prev.get('net_profit')}, prev PF: {prev.get('profit_factor')})"
        )
    lines.append("")
    return "\n".join(lines)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Search small targeted entry-depth and confirmation variants on the curated two-preset baseline.")
    parser.add_argument("--base-pack", type=Path, default=DEFAULT_BASE_PACK)
    parser.add_argument("--case-report", type=Path, default=DEFAULT_CASE_REPORT)
    parser.add_argument("--prev-month-report", type=Path, default=DEFAULT_PREV_MONTH_REPORT)
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    parser.add_argument("--min-case-hits", type=int, default=5)
    return parser


def main() -> int:
    args = build_parser().parse_args()
    base_pack_path = Path(args.base_pack).resolve()
    case_report_path = Path(args.case_report).resolve()
    prev_month_report_path = Path(args.prev_month_report).resolve()
    output_dir = Path(args.output_dir).resolve()
    min_case_hits = max(0, int(args.min_case_hits))
    output_dir.mkdir(parents=True, exist_ok=True)

    base_pack = _load_json(base_pack_path)
    rows: list[dict[str, Any]] = []

    for idx, candidate in enumerate(_candidate_variants(), start=1):
        label = str(candidate["label"])
        print(f"[{idx}] {label}", flush=True)
        pack = copy.deepcopy(base_pack)
        for preset_name, cfg in dict(candidate.get("filters") or {}).items():
            _set_long_entry_filter(pack, preset_name, cfg)

        pack_meta = dict(pack.get("meta") or {})
        pack_meta["source"] = "entry_depth_confirmation_search_v1"
        pack_meta["last_preset"] = P1_NAME
        pack_meta["variant_label"] = label
        pack["meta"] = pack_meta

        variant_pack_path = output_dir / "preset_packs" / f"{label}.json"
        _write_json(variant_pack_path, pack)

        case_output_json = output_dir / "case_window" / f"{label}.json"
        prev_output_json = output_dir / "prev_month" / f"{label}.json"

        case_result = _run_portfolio_build(
            report_json=case_report_path,
            presets_path=variant_pack_path,
            output_json=case_output_json,
        )

        case_summary = _summary_row(case_result)
        case_hits = _collect_case_hits(case_result)
        case_summary["case_hits"] = case_hits
        case_summary["case_hit_count"] = len(case_hits)

        prev_summary: dict[str, Any]
        if len(case_hits) >= min_case_hits:
            prev_result = _run_portfolio_build(
                report_json=prev_month_report_path,
                presets_path=variant_pack_path,
                output_json=prev_output_json,
            )
            prev_summary = _summary_row(prev_result)
        else:
            prev_summary = {
                "trades": 0,
                "wins": 0,
                "losses": 0,
                "win_rate_pct": None,
                "net_profit": None,
                "profit_factor": None,
                "max_drawdown_pct": None,
                "skipped_prev_month": True,
                "skip_reason": f"case_hit_count<{min_case_hits}",
            }

        row = {
            "label": label,
            "description": str(candidate["description"]),
            "pack_path": str(variant_pack_path),
            "case_window": case_summary,
            "prev_month": prev_summary,
        }
        rows.append(row)

        ranked = sorted(rows, key=_variant_sort_key, reverse=True)
        summary_payload = {
            "base_pack": str(base_pack_path),
            "case_report": str(case_report_path),
            "prev_month_report": str(prev_month_report_path),
            "rows": ranked,
            "best": ranked[0] if ranked else None,
        }
        _write_json(output_dir / "summary.json", summary_payload)
        (output_dir / "summary.md").write_text(_build_markdown(ranked, output_dir), encoding="utf-8")

    ranked = sorted(rows, key=_variant_sort_key, reverse=True)
    print(json.dumps({"summary_json": str(output_dir / "summary.json"), "best": ranked[0] if ranked else None}, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
