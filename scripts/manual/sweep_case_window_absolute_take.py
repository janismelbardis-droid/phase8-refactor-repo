from __future__ import annotations

import argparse
import copy
import json
import sys
from pathlib import Path
from typing import Any

CURRENT_DIR = Path(__file__).resolve().parent
if str(CURRENT_DIR) not in sys.path:
    sys.path.insert(0, str(CURRENT_DIR))

from build_combined_case_window_portfolio import build_combined_case_window_portfolio


ABS_TAKE_VALUES = [80.0, 120.0, 160.0, 240.0, 320.0, 480.0]


def default_workspace_root() -> Path:
    return Path(__file__).resolve().parents[4]


def default_repo_root() -> Path:
    return Path(__file__).resolve().parents[2]


def default_casebook_root() -> Path:
    return default_workspace_root() / "research_exports" / "trade_entry_casebook"


def default_report_json() -> Path:
    return default_casebook_root() / "preset_drafts" / "backtest_case_period_0001_0005.json"


def default_base_presets_path() -> Path:
    return default_casebook_root() / "preset_drafts" / "research_case_family_presets_v1.json"


def default_output_root() -> Path:
    return default_casebook_root() / "take_experiments"


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Sweep pure absolute take-profit models over the case window 0001-0005."
    )
    parser.add_argument("--report-json", type=Path, default=default_report_json())
    parser.add_argument("--base-presets-path", type=Path, default=default_base_presets_path())
    parser.add_argument("--repo-root", type=Path, default=default_repo_root())
    parser.add_argument("--output-root", type=Path, default=default_output_root())
    return parser


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


def _empty_exit_tab() -> dict[str, Any]:
    return {
        "groups_join": "AND",
        "groups": [],
        "advanced_logic": {
            "enabled": False,
            "routes": [],
        },
    }


def _apply_absolute_take_variant(base_pack: dict[str, Any], abs_take_value: float) -> dict[str, Any]:
    pack = copy.deepcopy(base_pack)
    presets = dict(pack.get("presets") or {})
    for preset_name, preset_raw in presets.items():
        preset = dict(preset_raw or {})
        settings = dict(preset.get("settings") or {})
        tabs = dict(preset.get("tabs") or {})
        meta = dict(preset.get("meta") or {})

        settings["stop_loss_pct"] = 0.0
        settings["stop_loss_mode"] = "OFF"
        settings["stop_loss_value"] = 0.0
        settings["stop_loss_field"] = ""
        settings["take_profit_pct"] = 0.0
        settings["take_profit_mode"] = "ABSOLUTE"
        settings["take_profit_value"] = float(abs_take_value)
        settings["take_profit_field"] = ""
        settings["break_even_after_mfe_pct"] = 0.0
        settings["trailing_stop_pct"] = 0.0

        tabs["Long Exit"] = _empty_exit_tab()

        meta["take_experiment"] = {
            "mode": "ABSOLUTE",
            "take_profit_value": float(abs_take_value),
            "long_exit_mode": "disabled_except_take_profit_and_force_close",
        }

        preset["settings"] = settings
        preset["tabs"] = tabs
        preset["meta"] = meta
        presets[preset_name] = preset

    meta_root = dict(pack.get("meta") or {})
    meta_root["take_experiment"] = {
        "mode": "ABSOLUTE",
        "take_values": list(ABS_TAKE_VALUES),
        "long_exit_mode": "disabled_except_take_profit_and_force_close",
    }
    pack["meta"] = meta_root
    pack["presets"] = presets
    return pack


def _collect_hit_cases(result: dict[str, Any]) -> list[str]:
    cases: set[str] = set()
    for trade in list(result.get("portfolio_trades") or []):
        for case_id in list(trade.get("case_hits") or []):
            cases.add(str(case_id))
    return sorted(cases)


def _row_sort_key(row: dict[str, Any]) -> tuple[Any, Any, Any]:
    return (
        -float(row.get("net_profit") or 0.0),
        -float(row.get("profit_factor") or 0.0),
        float(row.get("max_drawdown_pct") or 9999.0),
    )


def _build_markdown(rows: list[dict[str, Any]], output_root: Path) -> str:
    lines: list[str] = []
    lines.append("# Absolute Take Sweep For Case Window 0001-0005")
    lines.append("")
    lines.append(f"- Output root: `{output_root}`")
    lines.append(f"- ABS take values tested: `{', '.join(str(int(v)) for v in ABS_TAKE_VALUES)}`")
    lines.append("- Long Exit rules are disabled in these variants; only absolute TP and force-close-at-window-end remain.")
    lines.append("")
    lines.append("| Rank | ABS Take | Hit cases | Trades | Win rate % | Net profit | Profit factor | Max DD % | Force closes |")
    lines.append("|---:|---:|---|---:|---:|---:|---:|---:|---:|")
    for rank, row in enumerate(rows, start=1):
        hits = ",".join(list(row.get("hit_cases") or [])) or "-"
        lines.append(
            f"| {rank} | {row['abs_take_value']} | {hits} | {row['selected_trades']} | {row['win_rate_pct']} | "
            f"{row['net_profit']} | {row['profit_factor']} | {row['max_drawdown_pct']} | {row['force_close_count']} |"
        )
    lines.append("")
    return "\n".join(lines).rstrip() + "\n"


def sweep_absolute_take(
    *,
    report_json: Path,
    base_presets_path: Path,
    repo_root: Path,
    output_root: Path,
) -> dict[str, Any]:
    base_pack = _load_json(base_presets_path)
    output_root.mkdir(parents=True, exist_ok=True)
    preset_pack_root = output_root / "preset_packs"
    variant_report_root = output_root / "variant_reports"
    rows: list[dict[str, Any]] = []

    for index, abs_take in enumerate(ABS_TAKE_VALUES, start=1):
        variant_id = f"abs_take_{int(abs_take)}"
        print(f"[{index}/{len(ABS_TAKE_VALUES)}] Running absolute take variant: {variant_id}")
        variant_pack = _apply_absolute_take_variant(base_pack, abs_take)
        variant_preset_path = preset_pack_root / f"{variant_id}.json"
        _write_json(variant_preset_path, variant_pack)

        variant_output_json = variant_report_root / f"{variant_id}.json"
        variant_output_md = variant_report_root / f"{variant_id}.md"
        variant_output_json.parent.mkdir(parents=True, exist_ok=True)
        variant_output_md.parent.mkdir(parents=True, exist_ok=True)

        result = build_combined_case_window_portfolio(
            report_json=report_json,
            presets_path=variant_preset_path,
            repo_root=repo_root,
            output_json=variant_output_json,
            output_md=variant_output_md,
        )

        portfolio = dict(result.get("portfolio_summary") or {})
        row = {
            "variant_id": variant_id,
            "abs_take_value": float(abs_take),
            "hit_cases": _collect_hit_cases(result),
            "selected_trades": int(portfolio.get("trades") or 0),
            "win_rate_pct": portfolio.get("win_rate_pct"),
            "net_profit": portfolio.get("net_profit"),
            "profit_factor": portfolio.get("profit_factor"),
            "max_drawdown_pct": portfolio.get("max_drawdown_pct"),
            "force_close_count": int(portfolio.get("force_close_count") or 0),
            "variant_preset_path": str(variant_preset_path),
            "variant_output_json": str(variant_output_json),
            "variant_output_md": str(variant_output_md),
        }
        rows.append(row)

    ranked_rows = sorted(rows, key=_row_sort_key)
    summary = {
        "report_json": str(report_json),
        "base_presets_path": str(base_presets_path),
        "abs_take_values": list(ABS_TAKE_VALUES),
        "rows": ranked_rows,
        "best_variant": dict(ranked_rows[0]) if ranked_rows else None,
    }
    summary_json = output_root / "absolute_take_sweep_0001_0005.json"
    summary_md = output_root / "absolute_take_sweep_0001_0005.md"
    _write_json(summary_json, summary)
    summary_md.write_text(_build_markdown(ranked_rows, output_root), encoding="utf-8")
    print(json.dumps({"summary_json": str(summary_json), "summary_md": str(summary_md), "best_variant": summary.get("best_variant")}, indent=2, ensure_ascii=False))
    return summary


def main() -> int:
    args = build_parser().parse_args()
    sweep_absolute_take(
        report_json=args.report_json.resolve(),
        base_presets_path=args.base_presets_path.resolve(),
        repo_root=args.repo_root.resolve(),
        output_root=args.output_root.resolve(),
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
