from __future__ import annotations

import argparse
import csv
import json
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pandas as pd


CURRENT_DIR = Path(__file__).resolve().parent
if str(CURRENT_DIR) not in sys.path:
    sys.path.insert(0, str(CURRENT_DIR))


@dataclass(frozen=True)
class VariantSpec:
    label: str
    report_json: Path


def default_workspace_root() -> Path:
    return Path(__file__).resolve().parents[4]


def default_casebook_root() -> Path:
    return default_workspace_root() / "research_exports" / "trade_entry_casebook"


def default_report_json() -> Path:
    return default_casebook_root() / "preset_drafts" / "backtest_case_period_0001_0005.json"


def default_output_root() -> Path:
    return default_casebook_root() / "presentation" / "case_window_0001_0005_trade_audit"


def default_variants() -> list[VariantSpec]:
    root = default_casebook_root()
    return [
        VariantSpec(
            label="Baseline RF or VIDYA",
            report_json=root / "exit_experiments" / "variant_reports" / "baseline_rf_or_vidya.json",
        ),
        VariantSpec(
            label="ABS TP 320 / SL 160",
            report_json=root / "take_experiments_abs_stop" / "variant_reports" / "abs_take_320_stop_160.json",
        ),
        VariantSpec(
            label="ABS TP 320 / SL 240",
            report_json=root / "take_experiments_abs_stop" / "variant_reports" / "abs_take_320_stop_240.json",
        ),
    ]


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Build a readable audit report for case window 0001-0005."
    )
    parser.add_argument("--report-json", type=Path, default=default_report_json())
    parser.add_argument("--output-root", type=Path, default=default_output_root())
    return parser


def _load_json(path: Path) -> dict[str, Any]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError(f"Expected JSON object at {path}")
    return payload


def _fmt_num(value: Any, digits: int = 6) -> str:
    if value is None:
        return ""
    try:
        numeric = float(value)
    except Exception:
        return str(value)
    return f"{numeric:.{digits}f}".rstrip("0").rstrip(".")


def _fmt_pct(value: Any) -> str:
    return _fmt_num(value, digits=6)


def _fmt_ts(value: Any) -> str:
    if not value:
        return ""
    ts = pd.Timestamp(value)
    if ts.tzinfo is None:
        ts = ts.tz_localize("UTC")
    else:
        ts = ts.tz_convert("UTC")
    return ts.strftime("%Y-%m-%d %H:%M:%S UTC")


def _sorted_cases(report_payload: dict[str, Any]) -> list[dict[str, Any]]:
    return sorted((report_payload.get("cases") or []), key=lambda row: str(row.get("case_id") or ""))


def _trade_case_hits(trade: dict[str, Any]) -> list[str]:
    return [str(case_id) for case_id in list(trade.get("case_hits") or [])]


def _trade_category(trade: dict[str, Any]) -> str:
    return "OUR" if _trade_case_hits(trade) else "EXTRA"


def _join_case_hits(trade: dict[str, Any]) -> str:
    hits = _trade_case_hits(trade)
    return ",".join(hits) if hits else ""


def _entry_cell(trade: dict[str, Any]) -> str:
    entry = _fmt_ts(trade.get("entry_time"))
    preset = str(trade.get("preset_name") or "")
    pnl = _fmt_num(trade.get("net_pnl"))
    exit_reason = str(trade.get("exit_reason") or "")
    return f"{entry} [{preset}] pnl={pnl} {exit_reason}".strip()


def _build_variant_summary(
    variant_label: str,
    variant_payload: dict[str, Any],
    case_ids: list[str],
) -> dict[str, Any]:
    trades = list(variant_payload.get("portfolio_trades") or [])
    cases_hit = sorted({case_id for trade in trades for case_id in _trade_case_hits(trade)})
    missing = [case_id for case_id in case_ids if case_id not in cases_hit]
    matched_trades = [trade for trade in trades if _trade_case_hits(trade)]
    extra_trades = [trade for trade in trades if not _trade_case_hits(trade)]
    summary = dict(variant_payload.get("portfolio_summary") or {})
    return {
        "variant": variant_label,
        "cases_hit_count": len(cases_hit),
        "cases_hit_list": ",".join(cases_hit),
        "missing_cases_list": ",".join(missing),
        "matched_case_trades": len(matched_trades),
        "extra_trades": len(extra_trades),
        "selected_trades": len(trades),
        "skipped_overlaps": len(list(variant_payload.get("skipped_overlaps") or [])),
        "win_rate_pct": summary.get("win_rate_pct"),
        "net_profit": summary.get("net_profit"),
        "profit_factor": summary.get("profit_factor"),
        "max_drawdown_pct": summary.get("max_drawdown_pct"),
    }


def _build_case_matrix_rows(
    cases: list[dict[str, Any]],
    variants: list[tuple[VariantSpec, dict[str, Any]]],
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for case in cases:
        case_id = str(case.get("case_id") or "")
        row: dict[str, Any] = {
            "case_id": case_id,
            "preset_family": str(case.get("preset_family") or ""),
            "t_rf_utc": _fmt_ts(case.get("t_rf_utc")),
            "t0_utc": _fmt_ts(case.get("t0_utc")),
            "window_end_utc": _fmt_ts(case.get("window_end_utc")),
        }
        for spec, payload in variants:
            hits = [
                trade for trade in list(payload.get("portfolio_trades") or [])
                if case_id in _trade_case_hits(trade)
            ]
            if hits:
                row[spec.label] = "<br>".join(_entry_cell(trade) for trade in hits)
            else:
                row[spec.label] = "MISS"
        rows.append(row)
    return rows


def _build_trade_rows(variants: list[tuple[VariantSpec, dict[str, Any]]]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for spec, payload in variants:
        for trade in list(payload.get("portfolio_trades") or []):
            rows.append(
                {
                    "variant": spec.label,
                    "category": _trade_category(trade),
                    "case_hits": _join_case_hits(trade),
                    "preset_name": str(trade.get("preset_name") or ""),
                    "preset_family": str(trade.get("preset_family") or ""),
                    "entry_time": _fmt_ts(trade.get("entry_time")),
                    "exit_time": _fmt_ts(trade.get("exit_time")),
                    "exit_reason": str(trade.get("exit_reason") or ""),
                    "net_pnl": _fmt_num(trade.get("net_pnl")),
                    "nearest_case_id": str(trade.get("nearest_case_id") or ""),
                    "nearest_case_t0_diff_min": _fmt_num(trade.get("nearest_case_t0_diff_min"), digits=2),
                }
            )
    rows.sort(key=lambda row: (row["variant"], row["entry_time"], row["preset_name"]))
    return rows


def _write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if not rows:
        path.write_text("", encoding="utf-8")
        return
    fieldnames = list(rows[0].keys())
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def _markdown_table(rows: list[dict[str, Any]], columns: list[str]) -> list[str]:
    lines = [
        "| " + " | ".join(columns) + " |",
        "|" + "|".join("---" for _ in columns) + "|",
    ]
    for row in rows:
        lines.append("| " + " | ".join(str(row.get(column, "")) for column in columns) + " |")
    return lines


def _build_markdown(
    report_json: Path,
    variants: list[tuple[VariantSpec, dict[str, Any]]],
    cases: list[dict[str, Any]],
    overview_rows: list[dict[str, Any]],
    case_matrix_rows: list[dict[str, Any]],
    trade_rows: list[dict[str, Any]],
) -> str:
    lines: list[str] = []
    lines.append("# Case Window 0001-0005 Trade Audit")
    lines.append("")
    lines.append(f"- Case source: `{report_json}`")
    lines.append(f"- Variants audited: `{len(variants)}`")
    lines.append(f"- Manual cases in window: `{len(cases)}`")
    lines.append("")
    lines.append("## Variant Overview")
    lines.append("")
    lines.extend(
        _markdown_table(
            overview_rows,
            [
                "variant",
                "cases_hit_count",
                "cases_hit_list",
                "missing_cases_list",
                "matched_case_trades",
                "extra_trades",
                "selected_trades",
                "skipped_overlaps",
                "net_profit",
                "profit_factor",
                "max_drawdown_pct",
            ],
        )
    )
    lines.append("")
    lines.append("## Manual Case Matrix")
    lines.append("")
    case_columns = ["case_id", "preset_family", "t_rf_utc", "t0_utc", "window_end_utc"] + [spec.label for spec, _ in variants]
    lines.extend(_markdown_table(case_matrix_rows, case_columns))
    lines.append("")
    lines.append("## Selected Trades")
    lines.append("")
    lines.extend(
        _markdown_table(
            trade_rows,
            [
                "variant",
                "category",
                "case_hits",
                "preset_name",
                "preset_family",
                "entry_time",
                "exit_time",
                "exit_reason",
                "net_pnl",
                "nearest_case_id",
                "nearest_case_t0_diff_min",
            ],
        )
    )
    lines.append("")
    return "\n".join(lines)


def build_case_window_trade_audit(
    *,
    report_json: Path,
    output_root: Path,
    variants: list[VariantSpec],
) -> dict[str, Any]:
    report_payload = _load_json(report_json)
    cases = _sorted_cases(report_payload)
    case_ids = [str(case.get("case_id") or "") for case in cases]

    variant_payloads: list[tuple[VariantSpec, dict[str, Any]]] = []
    for spec in variants:
        variant_payloads.append((spec, _load_json(spec.report_json)))

    overview_rows = [
        _build_variant_summary(spec.label, payload, case_ids)
        for spec, payload in variant_payloads
    ]
    case_matrix_rows = _build_case_matrix_rows(cases, variant_payloads)
    trade_rows = _build_trade_rows(variant_payloads)

    output_root.mkdir(parents=True, exist_ok=True)
    md_path = output_root / "case_window_0001_0005_trade_audit.md"
    overview_csv = output_root / "case_window_0001_0005_variant_overview.csv"
    case_matrix_csv = output_root / "case_window_0001_0005_case_matrix.csv"
    trades_csv = output_root / "case_window_0001_0005_selected_trades.csv"

    md_path.write_text(
        _build_markdown(
            report_json=report_json,
            variants=variant_payloads,
            cases=cases,
            overview_rows=overview_rows,
            case_matrix_rows=case_matrix_rows,
            trade_rows=trade_rows,
        ),
        encoding="utf-8",
    )
    _write_csv(overview_csv, overview_rows)
    _write_csv(case_matrix_csv, case_matrix_rows)
    _write_csv(trades_csv, trade_rows)

    return {
        "markdown": str(md_path),
        "overview_csv": str(overview_csv),
        "case_matrix_csv": str(case_matrix_csv),
        "trades_csv": str(trades_csv),
        "variant_count": len(variant_payloads),
        "case_count": len(cases),
        "trade_rows": len(trade_rows),
    }


def main() -> int:
    args = build_parser().parse_args()
    result = build_case_window_trade_audit(
        report_json=args.report_json.resolve(),
        output_root=args.output_root.resolve(),
        variants=default_variants(),
    )
    print(json.dumps(result, indent=2, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
