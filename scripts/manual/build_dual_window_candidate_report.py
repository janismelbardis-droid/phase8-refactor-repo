from __future__ import annotations

import json
from pathlib import Path
from typing import Any

CURRENT_DIR = Path(__file__).resolve().parent
REPO_ROOT = Path(__file__).resolve().parents[2]
WORKSPACE_ROOT = Path(__file__).resolve().parents[4]

if str(REPO_ROOT) not in __import__("sys").path:
    __import__("sys").path.insert(0, str(REPO_ROOT))
if str(CURRENT_DIR) not in __import__("sys").path:
    __import__("sys").path.insert(0, str(CURRENT_DIR))

from build_combined_case_window_portfolio import build_combined_case_window_portfolio


def casebook_root() -> Path:
    return WORKSPACE_ROOT / "research_exports" / "trade_entry_casebook"


def base_pack_path() -> Path:
    return casebook_root() / "take_experiments_abs_stop" / "preset_packs" / "abs_take_320_stop_160.json"


def current_window_report() -> Path:
    return casebook_root() / "preset_drafts" / "backtest_case_period_0001_0005.json"


def oos_window_report() -> Path:
    return casebook_root() / "oos_prev_window_0001_0005_matchlen" / "backtest_prev_window_matchlen.json"


def current_window_base_result() -> Path:
    return casebook_root() / "take_experiments_abs_stop" / "variant_reports" / "abs_take_320_stop_160.json"


def oos_window_base_result() -> Path:
    return casebook_root() / "oos_prev_window_0001_0005_matchlen" / "abs_take_320_stop_160_prev_window.json"


def output_root() -> Path:
    return casebook_root() / "dual_window_candidate_v1"


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


def _safe_float(value: Any) -> float | None:
    try:
        if value in (None, ""):
            return None
        return float(value)
    except Exception:
        return None


def _round(value: Any, digits: int = 6) -> Any:
    parsed = _safe_float(value)
    if parsed is None:
        return value
    return round(parsed, digits)


def _apply_candidate_modifications(base_pack: dict[str, Any]) -> dict[str, Any]:
    pack = json.loads(json.dumps(base_pack))

    p2 = dict((pack.get("presets") or {}).get("902") or {})
    p2_tabs = dict(p2.get("tabs") or {})
    p2_long_entry = dict(p2_tabs.get("Long Entry") or {})
    p2_groups = list(p2_long_entry.get("groups") or [])
    p2_groups.append(
        {
            "group_key": "C",
            "rules_join": "AND",
            "rules": [
                {
                    "timeframe": "5m",
                    "mode": "state",
                    "field": "range_filter_state",
                    "op": "!=",
                    "value": "BUY",
                    "is_trigger": False,
                    "is_final_gate": False,
                    "bars_ago_mode": "OFF",
                    "bars_ago_n": 0,
                    "window_bars": 1,
                    "require_still_valid": False,
                    "is_sequence_canceler": False,
                },
                {
                    "timeframe": "1m",
                    "mode": "state",
                    "field": "flip_age",
                    "op": ">=",
                    "value": 2,
                    "is_trigger": False,
                    "is_final_gate": False,
                    "bars_ago_mode": "OFF",
                    "bars_ago_n": 0,
                    "window_bars": 1,
                    "require_still_valid": False,
                    "is_sequence_canceler": False,
                },
                {
                    "timeframe": "1m",
                    "mode": "state",
                    "field": "flip_age",
                    "op": "<=",
                    "value": 15,
                    "is_trigger": False,
                    "is_final_gate": False,
                    "bars_ago_mode": "OFF",
                    "bars_ago_n": 0,
                    "window_bars": 1,
                    "require_still_valid": False,
                    "is_sequence_canceler": False,
                },
            ],
        }
    )
    p2_long_entry["groups"] = p2_groups
    p2_tabs["Long Entry"] = p2_long_entry
    p2["tabs"] = p2_tabs
    pack["presets"]["902"] = p2

    p3 = dict((pack.get("presets") or {}).get("903") or {})
    p3_tabs = dict(p3.get("tabs") or {})
    p3_long_entry = dict(p3_tabs.get("Long Entry") or {})
    p3_groups = list(p3_long_entry.get("groups") or [])
    p3_groups.append(
        {
            "group_key": "C",
            "rules_join": "AND",
            "rules": [
                {
                    "timeframe": "15m",
                    "mode": "state",
                    "field": "range_filter_state",
                    "op": "=",
                    "value": "SELL",
                    "is_trigger": False,
                    "is_final_gate": False,
                    "bars_ago_mode": "OFF",
                    "bars_ago_n": 0,
                    "window_bars": 1,
                    "require_still_valid": False,
                    "is_sequence_canceler": False,
                }
            ],
        }
    )
    p3_long_entry["groups"] = p3_groups
    p3_tabs["Long Entry"] = p3_long_entry
    p3["tabs"] = p3_tabs
    pack["presets"]["903"] = p3

    meta = dict(pack.get("meta") or {})
    meta["dual_window_candidate_v1"] = {
        "goal": "Keep all five manual cases while making both the research window and previous equal-length OOS window profitable.",
        "changes": [
            "P2 adds 5m range_filter_state != BUY.",
            "P2 adds 1m flip_age between 2 and 15.",
            "P3 adds 15m range_filter_state = SELL.",
        ],
    }
    pack["meta"] = meta
    return pack


def _case_hit_ids(result: dict[str, Any]) -> list[str]:
    case_ids: set[str] = set()
    for trade in list(result.get("portfolio_trades") or []):
        for case_id in list(trade.get("case_hits") or []):
            case_ids.add(str(case_id))
    return sorted(case_ids)


def _trade_rows(result: dict[str, Any]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for trade in list(result.get("portfolio_trades") or []):
        rows.append(
            {
                "preset": str(trade.get("preset_name") or ""),
                "entry_time": str(trade.get("entry_time") or ""),
                "net_pnl": _round(trade.get("net_pnl")),
                "exit_reason": str(trade.get("exit_reason") or ""),
                "case_hits": list(trade.get("case_hits") or []),
            }
        )
    return rows


def _markdown_table(headers: list[str], rows: list[list[Any]]) -> list[str]:
    out = [
        "| " + " | ".join(headers) + " |",
        "| " + " | ".join("---" for _ in headers) + " |",
    ]
    for row in rows:
        out.append("| " + " | ".join(str(cell) for cell in row) + " |")
    return out


def _build_summary_markdown(
    *,
    current_base: dict[str, Any],
    current_candidate: dict[str, Any],
    oos_base: dict[str, Any],
    oos_candidate: dict[str, Any],
    candidate_pack_path: Path,
) -> str:
    lines: list[str] = []
    lines.append("# Dual Window Candidate v1")
    lines.append("")
    lines.append(f"- Candidate pack: `{candidate_pack_path}`")
    lines.append("- Exit model: `ABS TP 320 / SL 160`")
    lines.append("- Goal: keep all five manual cases and make both windows positive.")
    lines.append("")
    lines.append("## Rule Changes")
    lines.append("")
    lines.append("- `902 / P2` adds `5m range_filter_state != BUY`.")
    lines.append("- `902 / P2` adds `1m flip_age >= 2` and `1m flip_age <= 15`.")
    lines.append("- `903 / P3` adds `15m range_filter_state = SELL`.")
    lines.append("")
    lines.append("## Window Comparison")
    lines.append("")
    lines.extend(
        _markdown_table(
            ["window", "variant", "trades", "net_profit", "return_pct", "wins", "losses", "case_hits"],
            [
                [
                    "case_window",
                    "base",
                    current_base["portfolio_summary"]["trades"],
                    _round(current_base["portfolio_summary"]["net_profit"]),
                    _round(current_base["portfolio_summary"]["return_pct"]),
                    current_base["portfolio_summary"]["wins"],
                    current_base["portfolio_summary"]["losses"],
                    ",".join(_case_hit_ids(current_base)),
                ],
                [
                    "case_window",
                    "candidate_v1",
                    current_candidate["portfolio_summary"]["trades"],
                    _round(current_candidate["portfolio_summary"]["net_profit"]),
                    _round(current_candidate["portfolio_summary"]["return_pct"]),
                    current_candidate["portfolio_summary"]["wins"],
                    current_candidate["portfolio_summary"]["losses"],
                    ",".join(_case_hit_ids(current_candidate)),
                ],
                [
                    "oos_prev",
                    "base",
                    oos_base["portfolio_summary"]["trades"],
                    _round(oos_base["portfolio_summary"]["net_profit"]),
                    _round(oos_base["portfolio_summary"]["return_pct"]),
                    oos_base["portfolio_summary"]["wins"],
                    oos_base["portfolio_summary"]["losses"],
                    "-",
                ],
                [
                    "oos_prev",
                    "candidate_v1",
                    oos_candidate["portfolio_summary"]["trades"],
                    _round(oos_candidate["portfolio_summary"]["net_profit"]),
                    _round(oos_candidate["portfolio_summary"]["return_pct"]),
                    oos_candidate["portfolio_summary"]["wins"],
                    oos_candidate["portfolio_summary"]["losses"],
                    "-",
                ],
            ],
        )
    )
    lines.append("")
    lines.append("## Current Window Selected Trades")
    lines.append("")
    lines.extend(
        _markdown_table(
            ["preset", "entry_time", "net_pnl", "exit_reason", "case_hits"],
            [
                [
                    row["preset"],
                    row["entry_time"],
                    row["net_pnl"],
                    row["exit_reason"],
                    ",".join(row["case_hits"]) or "-",
                ]
                for row in _trade_rows(current_candidate)
            ],
        )
    )
    lines.append("")
    lines.append("## OOS Selected Trades")
    lines.append("")
    lines.extend(
        _markdown_table(
            ["preset", "entry_time", "net_pnl", "exit_reason"],
            [
                [row["preset"], row["entry_time"], row["net_pnl"], row["exit_reason"]]
                for row in _trade_rows(oos_candidate)
            ],
        )
    )
    lines.append("")
    lines.append("## Interpretation")
    lines.append("")
    lines.append("- `P2` was too loose and was admitting chase-style continuations after the reclaim shell was already stale.")
    lines.append("- Blocking `P2` when `5m range_filter_state` is already `BUY` removes those chases while keeping the manual `0004` and `0005` style sweeps.")
    lines.append("- Bounding `P2` with `1m flip_age` keeps the reclaim in a live middle zone and cuts both too-early and too-stale triggers.")
    lines.append("- `P3` works best when the higher structure is still deteriorated; `15m range_filter_state = SELL` preserved `0003` and removed the bullish-structure re-entries that were failing.")
    lines.append("")
    return "\n".join(lines).rstrip() + "\n"


def build_dual_window_candidate_report() -> dict[str, Any]:
    out_root = output_root()
    out_root.mkdir(parents=True, exist_ok=True)

    base_pack = _load_json(base_pack_path())
    candidate_pack = _apply_candidate_modifications(base_pack)
    candidate_pack_path = out_root / "abs_take_320_stop_160_dual_window_candidate_v1.json"
    _write_json(candidate_pack_path, candidate_pack)

    current_candidate_json = out_root / "case_window_candidate_v1.json"
    current_candidate_md = out_root / "case_window_candidate_v1.md"
    oos_candidate_json = out_root / "oos_prev_candidate_v1.json"
    oos_candidate_md = out_root / "oos_prev_candidate_v1.md"

    current_candidate = build_combined_case_window_portfolio(
        report_json=current_window_report(),
        presets_path=candidate_pack_path,
        repo_root=REPO_ROOT,
        output_json=current_candidate_json,
        output_md=current_candidate_md,
    )
    oos_candidate = build_combined_case_window_portfolio(
        report_json=oos_window_report(),
        presets_path=candidate_pack_path,
        repo_root=REPO_ROOT,
        output_json=oos_candidate_json,
        output_md=oos_candidate_md,
    )

    current_base = _load_json(current_window_base_result())
    oos_base = _load_json(oos_window_base_result())

    summary_md = out_root / "dual_window_candidate_v1_summary.md"
    summary_md.write_text(
        _build_summary_markdown(
            current_base=current_base,
            current_candidate=current_candidate,
            oos_base=oos_base,
            oos_candidate=oos_candidate,
            candidate_pack_path=candidate_pack_path,
        ),
        encoding="utf-8",
    )

    summary_json = out_root / "dual_window_candidate_v1_summary.json"
    payload = {
        "candidate_pack_path": str(candidate_pack_path),
        "current_window_base": current_base.get("portfolio_summary"),
        "current_window_candidate": current_candidate.get("portfolio_summary"),
        "current_window_case_hits": _case_hit_ids(current_candidate),
        "oos_window_base": oos_base.get("portfolio_summary"),
        "oos_window_candidate": oos_candidate.get("portfolio_summary"),
        "current_window_selected_trades": _trade_rows(current_candidate),
        "oos_window_selected_trades": _trade_rows(oos_candidate),
    }
    _write_json(summary_json, payload)
    return payload


def main() -> int:
    payload = build_dual_window_candidate_report()
    print(json.dumps(payload, indent=2, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
