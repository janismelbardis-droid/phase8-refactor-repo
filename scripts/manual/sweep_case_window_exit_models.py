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


def default_workspace_root() -> Path:
    return Path(__file__).resolve().parents[4]


def default_repo_root() -> Path:
    return Path(__file__).resolve().parents[2]


def default_casebook_root() -> Path:
    return default_workspace_root() / "research_exports" / "trade_entry_casebook"


def default_base_report_json() -> Path:
    return default_casebook_root() / "preset_drafts" / "backtest_case_period_0001_0005.json"


def default_base_presets_path() -> Path:
    return default_casebook_root() / "preset_drafts" / "research_case_family_presets_v1.json"


def default_output_root() -> Path:
    return default_casebook_root() / "exit_experiments"


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Sweep rule-based and percent-based exit models over the research case window 0001-0005."
    )
    parser.add_argument("--report-json", type=Path, default=default_base_report_json())
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


def _event_rule(field: str, *, timeframe: str = "1m") -> dict[str, Any]:
    return {
        "timeframe": timeframe,
        "mode": "event",
        "field": field,
        "op": "EVENT",
        "value": None,
        "is_trigger": True,
        "is_final_gate": False,
        "bars_ago_mode": "OFF",
        "bars_ago_n": 0,
        "window_bars": 1,
        "require_still_valid": False,
        "is_sequence_canceler": False,
    }


def _state_rule(field: str, op: str, value: Any, *, timeframe: str = "1m") -> dict[str, Any]:
    return {
        "timeframe": timeframe,
        "mode": "state",
        "field": field,
        "op": op,
        "value": value,
        "is_trigger": False,
        "is_final_gate": False,
        "bars_ago_mode": "OFF",
        "bars_ago_n": 0,
        "window_bars": 1,
        "require_still_valid": False,
        "is_sequence_canceler": False,
    }


def _single_group_exit_tab(*rules: dict[str, Any], rules_join: str = "OR") -> dict[str, Any]:
    return {
        "groups_join": "AND",
        "groups": [
            {
                "group_key": "A",
                "rules_join": rules_join,
                "rules": list(rules),
            }
        ],
        "advanced_logic": {
            "enabled": False,
            "routes": [],
        },
    }


EXIT_VARIANTS: list[dict[str, Any]] = [
    {
        "id": "baseline_rf_or_vidya",
        "label": "Baseline RF or VIDYA Flip",
        "type": "baseline",
        "description": "Current research exit: range_filter_sell OR vidya_trend_down.",
    },
    {
        "id": "range_only",
        "label": "Range Filter Only",
        "type": "rule_exit",
        "description": "Exit only when range_filter_sell fires.",
        "long_exit_tab": _single_group_exit_tab(_event_rule("range_filter_sell")),
    },
    {
        "id": "vidya_only",
        "label": "VIDYA Flip Only",
        "type": "rule_exit",
        "description": "Exit only when vidya_trend_down fires.",
        "long_exit_tab": _single_group_exit_tab(_event_rule("vidya_trend_down")),
    },
    {
        "id": "vidya_slope_le_0",
        "label": "VIDYA Slope <= 0",
        "type": "rule_exit",
        "description": "Interpret slow/weakening VIDYA as exit once slope goes non-positive.",
        "long_exit_tab": _single_group_exit_tab(_state_rule("vidya_slope", "<=", 0.0), rules_join="AND"),
    },
    {
        "id": "taker_bias_sell",
        "label": "Taker Bias SELL",
        "type": "rule_exit",
        "description": "Exit when candle-level taker bias flips to SELL.",
        "long_exit_tab": _single_group_exit_tab(_state_rule("taker_bias_state", "=", "SELL"), rules_join="AND"),
    },
    {
        "id": "taker_delta_lt_0",
        "label": "Delta Volume < 0",
        "type": "rule_exit",
        "description": "Exit when taker delta volume turns negative.",
        "long_exit_tab": _single_group_exit_tab(_state_rule("taker_delta_volume", "<", 0.0), rules_join="AND"),
    },
    {
        "id": "be020_only",
        "label": "BE After 0.20",
        "type": "settings_overlay",
        "description": "Keep the current rule exit and move stop to break-even after 0.20% MFE.",
        "settings_patch": {
            "stop_loss_pct": 0.0,
            "stop_loss_mode": "OFF",
            "stop_loss_value": 0.0,
            "take_profit_pct": 0.0,
            "take_profit_mode": "OFF",
            "take_profit_value": 0.0,
            "break_even_after_mfe_pct": 0.20,
            "trailing_stop_pct": 0.0,
        },
    },
    {
        "id": "be025_only",
        "label": "BE After 0.25",
        "type": "settings_overlay",
        "description": "Keep the current rule exit and move stop to break-even after 0.25% MFE.",
        "settings_patch": {
            "stop_loss_pct": 0.0,
            "stop_loss_mode": "OFF",
            "stop_loss_value": 0.0,
            "take_profit_pct": 0.0,
            "take_profit_mode": "OFF",
            "take_profit_value": 0.0,
            "break_even_after_mfe_pct": 0.25,
            "trailing_stop_pct": 0.0,
        },
    },
    {
        "id": "be035_only",
        "label": "BE After 0.35",
        "type": "settings_overlay",
        "description": "Keep the current rule exit and move stop to break-even after 0.35% MFE.",
        "settings_patch": {
            "stop_loss_pct": 0.0,
            "stop_loss_mode": "OFF",
            "stop_loss_value": 0.0,
            "take_profit_pct": 0.0,
            "take_profit_mode": "OFF",
            "take_profit_value": 0.0,
            "break_even_after_mfe_pct": 0.35,
            "trailing_stop_pct": 0.0,
        },
    },
    {
        "id": "fade_slope_and_delta_1m",
        "label": "1m Slope + Delta Fade",
        "type": "rule_exit",
        "description": "Exit when 1m VIDYA slope turns non-positive and 1m taker delta turns negative together.",
        "long_exit_tab": _single_group_exit_tab(
            _state_rule("vidya_slope", "<=", 0.0),
            _state_rule("taker_delta_volume", "<", 0.0),
            rules_join="AND",
        ),
    },
    {
        "id": "fade_slope_and_delta_5m",
        "label": "1m Slope + 5m Delta Fade",
        "type": "rule_exit",
        "description": "Exit when 1m VIDYA slope softens and 5m taker delta turns negative.",
        "long_exit_tab": _single_group_exit_tab(
            _state_rule("vidya_slope", "<=", 0.0),
            _state_rule("taker_delta_volume", "<", 0.0, timeframe="5m"),
            rules_join="AND",
        ),
    },
    {
        "id": "be025_fade_slope_and_delta_1m",
        "label": "BE 0.25 + 1m Fade",
        "type": "hybrid_exit",
        "description": "Move to break-even after 0.25% MFE, then exit on 1m slope and 1m delta fade.",
        "long_exit_tab": _single_group_exit_tab(
            _state_rule("vidya_slope", "<=", 0.0),
            _state_rule("taker_delta_volume", "<", 0.0),
            rules_join="AND",
        ),
        "settings_patch": {
            "stop_loss_pct": 0.0,
            "stop_loss_mode": "OFF",
            "stop_loss_value": 0.0,
            "take_profit_pct": 0.0,
            "take_profit_mode": "OFF",
            "take_profit_value": 0.0,
            "break_even_after_mfe_pct": 0.25,
            "trailing_stop_pct": 0.0,
        },
    },
    {
        "id": "be035_fade_slope_and_delta_1m",
        "label": "BE 0.35 + 1m Fade",
        "type": "hybrid_exit",
        "description": "Move to break-even after 0.35% MFE, then exit on 1m slope and 1m delta fade.",
        "long_exit_tab": _single_group_exit_tab(
            _state_rule("vidya_slope", "<=", 0.0),
            _state_rule("taker_delta_volume", "<", 0.0),
            rules_join="AND",
        ),
        "settings_patch": {
            "stop_loss_pct": 0.0,
            "stop_loss_mode": "OFF",
            "stop_loss_value": 0.0,
            "take_profit_pct": 0.0,
            "take_profit_mode": "OFF",
            "take_profit_value": 0.0,
            "break_even_after_mfe_pct": 0.35,
            "trailing_stop_pct": 0.0,
        },
    },
    {
        "id": "be025_fade_slope_and_delta_5m",
        "label": "BE 0.25 + 5m Delta Fade",
        "type": "hybrid_exit",
        "description": "Move to break-even after 0.25% MFE, then exit on 1m slope plus 5m negative delta.",
        "long_exit_tab": _single_group_exit_tab(
            _state_rule("vidya_slope", "<=", 0.0),
            _state_rule("taker_delta_volume", "<", 0.0, timeframe="5m"),
            rules_join="AND",
        ),
        "settings_patch": {
            "stop_loss_pct": 0.0,
            "stop_loss_mode": "OFF",
            "stop_loss_value": 0.0,
            "take_profit_pct": 0.0,
            "take_profit_mode": "OFF",
            "take_profit_value": 0.0,
            "break_even_after_mfe_pct": 0.25,
            "trailing_stop_pct": 0.0,
        },
    },
    {
        "id": "be035_fade_slope_and_delta_5m",
        "label": "BE 0.35 + 5m Delta Fade",
        "type": "hybrid_exit",
        "description": "Move to break-even after 0.35% MFE, then exit on 1m slope plus 5m negative delta.",
        "long_exit_tab": _single_group_exit_tab(
            _state_rule("vidya_slope", "<=", 0.0),
            _state_rule("taker_delta_volume", "<", 0.0, timeframe="5m"),
            rules_join="AND",
        ),
        "settings_patch": {
            "stop_loss_pct": 0.0,
            "stop_loss_mode": "OFF",
            "stop_loss_value": 0.0,
            "take_profit_pct": 0.0,
            "take_profit_mode": "OFF",
            "take_profit_value": 0.0,
            "break_even_after_mfe_pct": 0.35,
            "trailing_stop_pct": 0.0,
        },
    },
    {
        "id": "tp050_sl025",
        "label": "TP 0.50 / SL 0.25",
        "type": "settings_overlay",
        "description": "Keep baseline rule exit, add 0.25% stop and 0.50% take-profit.",
        "settings_patch": {
            "stop_loss_pct": 0.25,
            "stop_loss_mode": "PERCENT",
            "stop_loss_value": 0.25,
            "take_profit_pct": 0.50,
            "take_profit_mode": "PERCENT",
            "take_profit_value": 0.50,
            "break_even_after_mfe_pct": 0.0,
            "trailing_stop_pct": 0.0,
        },
    },
    {
        "id": "tp075_sl025_be025",
        "label": "TP 0.75 / SL 0.25 / BE 0.25",
        "type": "settings_overlay",
        "description": "Keep baseline rule exit, add 0.25% stop, 0.75% TP, and break-even after 0.25% MFE.",
        "settings_patch": {
            "stop_loss_pct": 0.25,
            "stop_loss_mode": "PERCENT",
            "stop_loss_value": 0.25,
            "take_profit_pct": 0.75,
            "take_profit_mode": "PERCENT",
            "take_profit_value": 0.75,
            "break_even_after_mfe_pct": 0.25,
            "trailing_stop_pct": 0.0,
        },
    },
    {
        "id": "tp100_sl035_be025_tr025",
        "label": "TP 1.00 / SL 0.35 / BE 0.25 / TR 0.25",
        "type": "settings_overlay",
        "description": "Keep baseline rule exit, add wider TP with break-even and trailing stop support.",
        "settings_patch": {
            "stop_loss_pct": 0.35,
            "stop_loss_mode": "PERCENT",
            "stop_loss_value": 0.35,
            "take_profit_pct": 1.00,
            "take_profit_mode": "PERCENT",
            "take_profit_value": 1.00,
            "break_even_after_mfe_pct": 0.25,
            "trailing_stop_pct": 0.25,
        },
    },
]


def _apply_variant(base_pack: dict[str, Any], variant: dict[str, Any]) -> dict[str, Any]:
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
        settings["take_profit_mode"] = "OFF"
        settings["take_profit_value"] = 0.0
        settings["take_profit_field"] = ""
        settings["break_even_after_mfe_pct"] = 0.0
        settings["trailing_stop_pct"] = 0.0

        if "long_exit_tab" in variant:
            tabs["Long Exit"] = copy.deepcopy(dict(variant["long_exit_tab"]))
        if "settings_patch" in variant:
            settings.update(dict(variant["settings_patch"]))
        if variant["type"] not in {"baseline", "rule_exit", "settings_overlay", "hybrid_exit"}:
            raise ValueError(f"Unsupported variant type: {variant['type']}")

        meta["exit_variant_id"] = str(variant["id"])
        meta["exit_variant_label"] = str(variant["label"])
        meta["exit_variant_description"] = str(variant["description"])

        preset["settings"] = settings
        preset["tabs"] = tabs
        preset["meta"] = meta
        presets[preset_name] = preset

    meta_root = dict(pack.get("meta") or {})
    meta_root["exit_experiment"] = {
        "variant_id": str(variant["id"]),
        "variant_label": str(variant["label"]),
        "variant_description": str(variant["description"]),
    }
    pack["meta"] = meta_root
    pack["presets"] = presets
    return pack


def _summary_value(summary: dict[str, Any], key: str) -> Any:
    return summary.get(key)


def _collect_hit_cases(result: dict[str, Any]) -> list[str]:
    cases: set[str] = set()
    for trade in list(result.get("portfolio_trades") or []):
        for case_id in list(trade.get("case_hits") or []):
            cases.add(str(case_id))
    return sorted(cases)


def _variant_row(variant: dict[str, Any], result: dict[str, Any]) -> dict[str, Any]:
    portfolio = dict(result.get("portfolio_summary") or {})
    raw = dict(result.get("raw_signal_summary") or {})
    per_preset = dict(result.get("per_preset") or {})
    hit_cases = _collect_hit_cases(result)
    return {
        "variant_id": str(variant["id"]),
        "label": str(variant["label"]),
        "type": str(variant["type"]),
        "description": str(variant["description"]),
        "hit_cases": hit_cases,
        "hit_case_count": len(hit_cases),
        "selected_trades": int(portfolio.get("trades") or 0),
        "wins": int(portfolio.get("wins") or 0),
        "losses": int(portfolio.get("losses") or 0),
        "win_rate_pct": portfolio.get("win_rate_pct"),
        "net_profit": portfolio.get("net_profit"),
        "profit_factor": portfolio.get("profit_factor"),
        "max_drawdown_pct": portfolio.get("max_drawdown_pct"),
        "return_pct": portfolio.get("return_pct"),
        "skipped_overlaps": len(list(result.get("skipped_overlaps") or [])),
        "raw_net_profit": raw.get("net_profit"),
        "raw_profit_factor": raw.get("profit_factor"),
        "p1_net_profit": dict(per_preset.get("901") or {}).get("summary_adjusted", {}).get("net_profit"),
        "p2_net_profit": dict(per_preset.get("902") or {}).get("summary_adjusted", {}).get("net_profit"),
        "p3_net_profit": dict(per_preset.get("903") or {}).get("summary_adjusted", {}).get("net_profit"),
    }


def _row_sort_key(row: dict[str, Any]) -> tuple[Any, Any, Any, Any]:
    net_profit = float(row.get("net_profit") or 0.0)
    profit_factor = float(row.get("profit_factor") or 0.0)
    max_dd = float(row.get("max_drawdown_pct") or 9999.0)
    hits = int(row.get("hit_case_count") or 0)
    return (-net_profit, -profit_factor, max_dd, -hits)


def _build_markdown(rows: list[dict[str, Any]], output_root: Path) -> str:
    lines: list[str] = []
    lines.append("# Exit Model Sweep For Case Window 0001-0005")
    lines.append("")
    lines.append(f"- Output root: `{output_root}`")
    lines.append(f"- Variants tested: `{len(rows)}`")
    lines.append("")
    lines.append("| Rank | Variant | Type | Hit cases | Trades | Win rate % | Net profit | Profit factor | Max DD % | P1 | P2 | P3 |")
    lines.append("|---:|---|---|---|---:|---:|---:|---:|---:|---:|---:|---:|")
    for rank, row in enumerate(rows, start=1):
        hits = ",".join(list(row.get("hit_cases") or [])) or "-"
        lines.append(
            f"| {rank} | {row['label']} | {row['type']} | {hits} | {row['selected_trades']} | "
            f"{row['win_rate_pct']} | {row['net_profit']} | {row['profit_factor']} | {row['max_drawdown_pct']} | "
            f"{row['p1_net_profit']} | {row['p2_net_profit']} | {row['p3_net_profit']} |"
        )
    lines.append("")
    lines.append("## Notes")
    lines.append("")
    for rank, row in enumerate(rows, start=1):
        lines.append(
            f"- `{rank}. {row['label']}`: {row['description']} "
            f"(hits: {','.join(list(row.get('hit_cases') or [])) or '-'}, raw net: {row['raw_net_profit']}, skipped overlaps: {row['skipped_overlaps']})"
        )
    lines.append("")
    return "\n".join(lines).rstrip() + "\n"


def sweep_exit_models(
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

    for index, variant in enumerate(EXIT_VARIANTS, start=1):
        print(f"[{index}/{len(EXIT_VARIANTS)}] Running exit variant: {variant['id']}")
        variant_pack = _apply_variant(base_pack, variant)
        variant_preset_path = preset_pack_root / f"{variant['id']}.json"
        _write_json(variant_preset_path, variant_pack)

        variant_output_json = variant_report_root / f"{variant['id']}.json"
        variant_output_md = variant_report_root / f"{variant['id']}.md"
        variant_output_json.parent.mkdir(parents=True, exist_ok=True)
        variant_output_md.parent.mkdir(parents=True, exist_ok=True)
        result = build_combined_case_window_portfolio(
            report_json=report_json,
            presets_path=variant_preset_path,
            repo_root=repo_root,
            output_json=variant_output_json,
            output_md=variant_output_md,
        )
        row = _variant_row(variant, result)
        row["variant_preset_path"] = str(variant_preset_path)
        row["variant_output_json"] = str(variant_output_json)
        row["variant_output_md"] = str(variant_output_md)
        rows.append(row)

    ranked_rows = sorted(rows, key=_row_sort_key)
    summary = {
        "report_json": str(report_json),
        "base_presets_path": str(base_presets_path),
        "variant_count": len(rows),
        "rows": ranked_rows,
        "best_variant": dict(ranked_rows[0]) if ranked_rows else None,
    }
    summary_json = output_root / "exit_model_sweep_0001_0005.json"
    summary_md = output_root / "exit_model_sweep_0001_0005.md"
    _write_json(summary_json, summary)
    summary_md.write_text(_build_markdown(ranked_rows, output_root), encoding="utf-8")
    print(json.dumps({"summary_json": str(summary_json), "summary_md": str(summary_md), "best_variant": summary.get("best_variant")}, indent=2, ensure_ascii=False))
    return summary


def main() -> int:
    args = build_parser().parse_args()
    sweep_exit_models(
        report_json=args.report_json.resolve(),
        base_presets_path=args.base_presets_path.resolve(),
        repo_root=args.repo_root.resolve(),
        output_root=args.output_root.resolve(),
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
