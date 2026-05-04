from __future__ import annotations

import argparse
import copy
import json
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable

import pandas as pd

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from app.services.ui_v2_runtime import BacktestRuntimeService, RunOverrides


WORKSPACE_ROOT = Path(__file__).resolve().parents[4]
CASEBOOK_ROOT = WORKSPACE_ROOT / "research_exports" / "trade_entry_casebook"

DEFAULT_SOURCE_PACK = CASEBOOK_ROOT / "preset_drafts" / "research_case_family_presets_v1.json"
DEFAULT_OUTPUT_DIR = CASEBOOK_ROOT / "ensemble_stage1_v1"

CASE_WINDOW_REPORT = CASEBOOK_ROOT / "preset_drafts" / "backtest_case_period_0001_0005.json"
PREV_EQUAL_REPORT = CASEBOOK_ROOT / "user_pullback_hypothesis_v4_buyweak_only" / "oos_prev_equal_report.json"
PREV2_REPORT = CASEBOOK_ROOT / "user_pullback_hypothesis_v4_buyweak_only" / "oos_prev2_report.json"
PREV3_REPORT = CASEBOOK_ROOT / "user_pullback_hypothesis_v4_buyweak_only" / "oos_prev3_report.json"
PREV_MONTH_REPORT = CASEBOOK_ROOT / "user_pullback_hypothesis_v4_buyweak_only" / "prev_month_report.json"

WINDOW_REPORTS = {
    "case_window": {"report_path": CASE_WINDOW_REPORT, "case_weight": 1.0},
    "oos_prev_equal": {"report_path": PREV_EQUAL_REPORT, "case_weight": 0.0},
    "oos_prev2": {"report_path": PREV2_REPORT, "case_weight": 0.0},
    "oos_prev3": {"report_path": PREV3_REPORT, "case_weight": 0.0},
}

DEFAULT_WINDOWS = ("case_window", "oos_prev_equal", "oos_prev2", "oos_prev3")
DEFAULT_SEED_PRESETS = ("901", "902", "903")


@dataclass(frozen=True)
class CandidateFilter:
    label: str
    rules: tuple[dict[str, Any], ...]


def _load_json(path: Path) -> dict[str, Any]:
    with path.open("r", encoding="utf-8") as handle:
        payload = json.load(handle)
    if not isinstance(payload, dict):
        raise ValueError(f"Expected JSON object at {path}")
    return payload


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as handle:
        json.dump(payload, handle, ensure_ascii=False, indent=2)
        handle.write("\n")


def _csv_items(raw: str | None) -> list[str]:
    parts = [part.strip() for part in str(raw or "").split(",")]
    return [part for part in parts if part]


def _slug(text: str) -> str:
    cleaned = []
    for ch in str(text or "").strip().lower():
        if ch.isalnum():
            cleaned.append(ch)
        else:
            cleaned.append("_")
    while "__" in "".join(cleaned):
        cleaned = list("".join(cleaned).replace("__", "_"))
    return "".join(cleaned).strip("_") or "base"


def _state_rule(*, timeframe: str, field: str, op: str, value: Any) -> dict[str, Any]:
    return {
        "timeframe": str(timeframe),
        "mode": "state",
        "field": str(field),
        "op": str(op),
        "value": value,
        "is_trigger": False,
        "is_final_gate": False,
        "bars_ago_mode": "OFF",
        "bars_ago_n": 0,
        "window_bars": 1,
        "require_still_valid": False,
        "is_sequence_canceler": False,
    }


def _candidate_filters() -> list[CandidateFilter]:
    return [
        CandidateFilter("5m_vidya_buy", (_state_rule(timeframe="5m", field="vidya_state", op="=", value="BUY"),)),
        CandidateFilter("15m_vidya_buy", (_state_rule(timeframe="15m", field="vidya_state", op="=", value="BUY"),)),
        CandidateFilter("30m_vidya_buy", (_state_rule(timeframe="30m", field="vidya_state", op="=", value="BUY"),)),
        CandidateFilter("5m_range_buy", (_state_rule(timeframe="5m", field="range_filter_state", op="=", value="BUY"),)),
        CandidateFilter("15m_range_buy", (_state_rule(timeframe="15m", field="range_filter_state", op="=", value="BUY"),)),
        CandidateFilter("30m_range_buy", (_state_rule(timeframe="30m", field="range_filter_state", op="=", value="BUY"),)),
        CandidateFilter("5m_ms_green", (_state_rule(timeframe="5m", field="ms", op="=", value="GREEN"),)),
        CandidateFilter("15m_ms_green", (_state_rule(timeframe="15m", field="ms", op="=", value="GREEN"),)),
        CandidateFilter("30m_ms_green", (_state_rule(timeframe="30m", field="ms", op="=", value="GREEN"),)),
        CandidateFilter("15m_market_bias_long", (_state_rule(timeframe="15m", field="market_bias", op="=", value="LONG"),)),
        CandidateFilter("30m_market_bias_long", (_state_rule(timeframe="30m", field="market_bias", op="=", value="LONG"),)),
        CandidateFilter("1h_market_bias_long", (_state_rule(timeframe="1h", field="market_bias", op="=", value="LONG"),)),
        CandidateFilter("15m_market_phase_pullback", (_state_rule(timeframe="15m", field="market_phase", op="=", value="PULLBACK"),)),
        CandidateFilter("30m_market_phase_pullback", (_state_rule(timeframe="30m", field="market_phase", op="=", value="PULLBACK"),)),
        CandidateFilter("1h_market_phase_pullback", (_state_rule(timeframe="1h", field="market_phase", op="=", value="PULLBACK"),)),
        CandidateFilter("15m_bull_pullback", (_state_rule(timeframe="15m", field="market_state", op="=", value="BULL_PULLBACK"),)),
        CandidateFilter("30m_bull_pullback", (_state_rule(timeframe="30m", field="market_state", op="=", value="BULL_PULLBACK"),)),
        CandidateFilter("1h_bull_pullback", (_state_rule(timeframe="1h", field="market_state", op="=", value="BULL_PULLBACK"),)),
        CandidateFilter("15m_ema_spread_pos", (_state_rule(timeframe="15m", field="ema_spread_pct", op=">=", value=0.0),)),
        CandidateFilter("30m_ema_spread_pos", (_state_rule(timeframe="30m", field="ema_spread_pct", op=">=", value=0.0),)),
        CandidateFilter("1h_ema_spread_pos", (_state_rule(timeframe="1h", field="ema_spread_pct", op=">=", value=0.0),)),
        CandidateFilter("15m_macd_pos", (_state_rule(timeframe="15m", field="macd", op=">=", value=0.0),)),
        CandidateFilter("30m_macd_pos", (_state_rule(timeframe="30m", field="macd", op=">=", value=0.0),)),
        CandidateFilter("1h_macd_pos", (_state_rule(timeframe="1h", field="macd", op=">=", value=0.0),)),
        CandidateFilter(
            "1h_adx25_di_positive",
            (
                _state_rule(timeframe="1h", field="adx", op=">=", value=25.0),
                _state_rule(timeframe="1h", field="di_spread", op=">=", value=0.0),
            ),
        ),
        CandidateFilter("1m_taker_buy", (_state_rule(timeframe="1m", field="taker_bias_state", op="=", value="BUY"),)),
        CandidateFilter("5m_taker_buy", (_state_rule(timeframe="5m", field="taker_bias_state", op="=", value="BUY"),)),
        CandidateFilter("1m_taker_delta_pos", (_state_rule(timeframe="1m", field="taker_delta_volume", op=">=", value=0.0),)),
        CandidateFilter("5m_taker_delta_pos", (_state_rule(timeframe="5m", field="taker_delta_volume", op=">=", value=0.0),)),
        CandidateFilter("1m_taker_buy_share52", (_state_rule(timeframe="1m", field="taker_buy_share_pct", op=">=", value=52.0),)),
        CandidateFilter("5m_taker_buy_share52", (_state_rule(timeframe="5m", field="taker_buy_share_pct", op=">=", value=52.0),)),
        CandidateFilter(
            "5m_vidya_buy__15m_market_bias_long",
            (
                _state_rule(timeframe="5m", field="vidya_state", op="=", value="BUY"),
                _state_rule(timeframe="15m", field="market_bias", op="=", value="LONG"),
            ),
        ),
        CandidateFilter(
            "5m_range_buy__15m_market_bias_long",
            (
                _state_rule(timeframe="5m", field="range_filter_state", op="=", value="BUY"),
                _state_rule(timeframe="15m", field="market_bias", op="=", value="LONG"),
            ),
        ),
        CandidateFilter(
            "5m_vidya_buy__1h_market_bias_long",
            (
                _state_rule(timeframe="5m", field="vidya_state", op="=", value="BUY"),
                _state_rule(timeframe="1h", field="market_bias", op="=", value="LONG"),
            ),
        ),
        CandidateFilter(
            "5m_taker_buy__15m_vidya_buy",
            (
                _state_rule(timeframe="5m", field="taker_bias_state", op="=", value="BUY"),
                _state_rule(timeframe="15m", field="vidya_state", op="=", value="BUY"),
            ),
        ),
        CandidateFilter(
            "5m_taker_delta_pos__15m_vidya_buy",
            (
                _state_rule(timeframe="5m", field="taker_delta_volume", op=">=", value=0.0),
                _state_rule(timeframe="15m", field="vidya_state", op="=", value="BUY"),
            ),
        ),
        CandidateFilter(
            "15m_vidya_buy__1h_bull_pullback",
            (
                _state_rule(timeframe="15m", field="vidya_state", op="=", value="BUY"),
                _state_rule(timeframe="1h", field="market_state", op="=", value="BULL_PULLBACK"),
            ),
        ),
        CandidateFilter(
            "15m_range_buy__1h_bull_pullback",
            (
                _state_rule(timeframe="15m", field="range_filter_state", op="=", value="BUY"),
                _state_rule(timeframe="1h", field="market_state", op="=", value="BULL_PULLBACK"),
            ),
        ),
    ]


def _window_specs(labels: Iterable[str]) -> list[dict[str, Any]]:
    windows: list[dict[str, Any]] = []
    for label in labels:
        if label not in WINDOW_REPORTS:
            raise ValueError(f"Unknown window label '{label}'.")
        row = dict(WINDOW_REPORTS[label])
        row["label"] = label
        windows.append(row)
    if not windows:
        raise ValueError("At least one window label is required.")
    return windows


def _candidate_preset_name(seed_name: str, label: str | None) -> str:
    return f"{seed_name}__{_slug(label or 'base')}"


def _clone_preset_with_filter(
    *,
    seed_preset: dict[str, Any],
    filter_spec: CandidateFilter | None,
    suffix: str,
) -> dict[str, Any]:
    preset = copy.deepcopy(seed_preset)
    tabs = preset.setdefault("tabs", {})
    long_entry = tabs.setdefault(
        "Long Entry",
        {"groups_join": "AND", "groups": [], "advanced_logic": {"enabled": False, "routes": []}},
    )
    groups = list(long_entry.get("groups") or [])
    if filter_spec is not None:
        groups.append(
            {
                "group_key": f"MTF_{suffix}",
                "rules_join": "AND",
                "rules": [copy.deepcopy(rule) for rule in filter_spec.rules],
            }
        )
    long_entry["groups"] = groups

    meta = dict(preset.get("meta") or {})
    meta["ensemble_filter_label"] = str(filter_spec.label if filter_spec is not None else "base")
    meta["ensemble_seed"] = str(meta.get("ensemble_seed") or "")
    preset["meta"] = meta
    return preset


def _read_entry_times(bundle_dir: Path) -> list[pd.Timestamp]:
    trades_path = bundle_dir / "trades.csv"
    if not trades_path.exists():
        return []
    df = pd.read_csv(trades_path)
    if "entry_time" not in df.columns or df.empty:
        return []
    entry_times: list[pd.Timestamp] = []
    for value in df["entry_time"].tolist():
        try:
            entry_times.append(pd.Timestamp(value))
        except Exception:
            continue
    return sorted(entry_times)


def _case_hits_for_window(
    *,
    report_payload: dict[str, Any],
    expected_family: str,
    entry_times: list[pd.Timestamp],
) -> tuple[int, list[str]]:
    cases = list(report_payload.get("cases") or [])
    hits: list[str] = []
    for case in cases:
        if str(case.get("preset_family") or "") != expected_family:
            continue
        start_ts = pd.Timestamp(case["t_rf_utc"])
        end_ts = pd.Timestamp(case["window_end_utc"])
        if any(start_ts <= entry_ts <= end_ts for entry_ts in entry_times):
            hits.append(str(case["case_id"]))
    return len(hits), hits


def _safe_float(value: Any) -> float | None:
    try:
        number = float(value)
    except Exception:
        return None
    if pd.isna(number):
        return None
    return number


def _score_candidate(row: dict[str, Any]) -> float:
    case_hits = int(row.get("case_hits", 0) or 0)
    positive_windows = int(row.get("positive_windows", 0) or 0)
    total_net = float(row.get("total_net_profit", 0.0) or 0.0)
    total_dd = float(row.get("total_drawdown_pct", 0.0) or 0.0)
    total_pf_bonus = float(row.get("total_pf_bonus", 0.0) or 0.0)
    trade_penalty = float(row.get("total_trade_penalty", 0.0) or 0.0)
    return (case_hits * 0.30) + (positive_windows * 0.08) + total_net + total_pf_bonus - (0.40 * total_dd) - trade_penalty


def _build_selected_report(
    *,
    source_report: dict[str, Any],
    selected_rows: list[dict[str, Any]],
) -> dict[str, Any]:
    report = {
        "period": dict(source_report.get("period") or {}),
        "cases": list(source_report.get("cases") or []),
        "presets": {},
    }
    for row in selected_rows:
        report["presets"][str(row["preset_name"])] = {
            "preset_family": str(row["expected_family"]),
            "label": str(row["candidate_label"]),
            "source_seed": str(row["seed_name"]),
        }
    return report


def _build_markdown(evaluation: dict[str, Any]) -> str:
    lines: list[str] = []
    lines.append("# Ensemble Stage 1")
    lines.append("")
    lines.append(f"- Source pack: `{evaluation.get('source_pack')}`")
    lines.append(f"- Candidate pack: `{evaluation.get('candidate_pack')}`")
    lines.append(f"- Selected pack: `{evaluation.get('selected_pack')}`")
    lines.append(
        f"- Progress: `{evaluation.get('completed_candidates', 0)}` / "
        f"`{evaluation.get('total_candidates', 0)}` candidates"
    )
    lines.append(f"- Windows: `{', '.join(list(evaluation.get('window_labels') or []))}`")
    lines.append("")
    lines.append("## Selected Presets")
    lines.append("")
    for row in list(evaluation.get("selected_presets") or []):
        lines.append(
            f"- `{row['preset_name']}` {row['expected_family']} / `{row['candidate_label']}`: "
            f"score `{row['score']}`, case hits `{row['case_hits']}`, positive windows `{row['positive_windows']}`"
        )
    if not list(evaluation.get("selected_presets") or []):
        lines.append("- No presets selected yet.")
    lines.append("")
    lines.append("## Top Candidates")
    lines.append("")
    for row in list(evaluation.get("top_candidates") or []):
        lines.append(
            f"- `{row['preset_name']}` {row['expected_family']} / `{row['candidate_label']}`: "
            f"score `{row['score']}`, net `{row['total_net_profit']}`, dd `{row['total_drawdown_pct']}`, "
            f"case hits `{row['case_hits']}`"
        )
    if not list(evaluation.get("top_candidates") or []):
        lines.append("- No completed candidates yet.")
    lines.append("")
    return "\n".join(lines)


def _row_covers_windows(row: dict[str, Any], window_labels: set[str]) -> bool:
    windows = {
        str(window.get("label") or "")
        for window in list(row.get("windows") or [])
        if isinstance(window, dict) and str(window.get("label") or "")
    }
    return window_labels.issubset(windows)


def _build_candidate_manifest(
    *,
    source_pack: dict[str, Any],
    base_case_report: dict[str, Any],
    seed_presets: list[str],
    filter_labels: set[str] | None,
) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    source_presets = dict(source_pack.get("presets") or {})
    selected_filters = _candidate_filters()
    if filter_labels:
        selected_filters = [flt for flt in selected_filters if flt.label in filter_labels]

    candidate_pack = {
        "meta": {
            "autoload_last": False,
            "last_preset": "",
            "source": "ensemble_stage1_v1_candidates",
            "version_note": "Stage 1 ensemble search: casebook families plus optional HTF/orderflow filters.",
        },
        "presets": {},
    }

    manifest_rows: list[dict[str, Any]] = []
    base_presets_map = dict(base_case_report.get("presets") or {})
    include_base = filter_labels is None or "base" in filter_labels

    for seed_name in seed_presets:
        if seed_name not in source_presets:
            raise KeyError(f"Seed preset '{seed_name}' not found in source pack.")
        seed_preset = dict(source_presets[seed_name])
        expected_family = str(base_presets_map.get(seed_name, {}).get("preset_family") or "")

        if include_base:
            preset_name = _candidate_preset_name(seed_name, "base")
            preset = _clone_preset_with_filter(seed_preset=seed_preset, filter_spec=None, suffix="BASE")
            preset.setdefault("meta", {})["ensemble_seed"] = seed_name
            candidate_pack["presets"][preset_name] = preset
            manifest_rows.append(
                {
                    "preset_name": preset_name,
                    "seed_name": seed_name,
                    "expected_family": expected_family,
                    "candidate_label": "base",
                    "filter_label": None,
                }
            )

        for filter_spec in selected_filters:
            preset_name = _candidate_preset_name(seed_name, filter_spec.label)
            preset = _clone_preset_with_filter(
                seed_preset=seed_preset,
                filter_spec=filter_spec,
                suffix=filter_spec.label,
            )
            preset.setdefault("meta", {})["ensemble_seed"] = seed_name
            candidate_pack["presets"][preset_name] = preset
            manifest_rows.append(
                {
                    "preset_name": preset_name,
                    "seed_name": seed_name,
                    "expected_family": expected_family,
                    "candidate_label": filter_spec.label,
                    "filter_label": filter_spec.label,
                }
            )

    manifest_rows.sort(key=lambda item: (str(item["seed_name"]), str(item["candidate_label"])))
    return candidate_pack, manifest_rows


def _select_rows(successful_rows: list[dict[str, Any]], *, top_per_family: int) -> list[dict[str, Any]]:
    selected_rows: list[dict[str, Any]] = []
    for family in ("P1", "P2", "P3"):
        family_rows = [
            row
            for row in successful_rows
            if str(row.get("expected_family") or "") == family
            and int(row.get("case_hits") or 0) >= 1
            and int(row.get("positive_windows") or 0) >= 1
        ]
        selected_rows.extend(family_rows[:top_per_family])
    selected_rows.sort(key=lambda item: (str(item.get("expected_family") or ""), -float(item.get("score") or 0.0)))
    return selected_rows


def _persist_outputs(
    *,
    rows_by_name: dict[str, dict[str, Any]],
    manifest_rows: list[dict[str, Any]],
    candidate_pack: dict[str, Any],
    source_pack_path: Path,
    candidate_pack_path: Path,
    selected_pack_path: Path,
    eval_json_path: Path,
    eval_md_path: Path,
    case_report_path: Path,
    prev_month_report_path: Path,
    base_case_report: dict[str, Any],
    base_prev_month_report: dict[str, Any],
    window_labels: list[str],
    top_per_family: int,
    run_options: dict[str, Any],
) -> dict[str, Any]:
    ordered_rows = [rows_by_name[row["preset_name"]] for row in manifest_rows if row["preset_name"] in rows_by_name]
    successful_rows = [row for row in ordered_rows if not row.get("error")]
    successful_rows.sort(
        key=lambda item: (float(item.get("score") or -999.0), float(item.get("total_net_profit") or -999.0)),
        reverse=True,
    )
    selected_rows = _select_rows(successful_rows, top_per_family=top_per_family)
    selected_names = {str(row["preset_name"]) for row in selected_rows}

    selected_pack = {
        "meta": {
            "autoload_last": False,
            "last_preset": (selected_rows[0]["preset_name"] if selected_rows else ""),
            "source": "ensemble_stage1_v1_selected",
            "version_note": "Selected narrow presets after incremental Stage 1 sweep.",
        },
        "presets": {
            preset_name: candidate_pack["presets"][preset_name]
            for preset_name in sorted(selected_names)
            if preset_name in candidate_pack["presets"]
        },
    }
    _write_json(selected_pack_path, selected_pack)

    case_report_payload = _build_selected_report(source_report=base_case_report, selected_rows=selected_rows)
    prev_month_report_payload = _build_selected_report(source_report=base_prev_month_report, selected_rows=selected_rows)
    _write_json(case_report_path, case_report_payload)
    _write_json(prev_month_report_path, prev_month_report_payload)

    completed = len(ordered_rows)
    total = len(manifest_rows)
    pending = [row["preset_name"] for row in manifest_rows if row["preset_name"] not in rows_by_name]
    evaluation = {
        "source_pack": str(source_pack_path),
        "candidate_pack": str(candidate_pack_path),
        "selected_pack": str(selected_pack_path),
        "selected_case_report": str(case_report_path),
        "selected_prev_month_report": str(prev_month_report_path),
        "window_labels": window_labels,
        "run_options": run_options,
        "completed_candidates": completed,
        "total_candidates": total,
        "pending_candidates": pending,
        "selected_presets": selected_rows,
        "top_candidates": successful_rows[:12],
        "all_candidates": ordered_rows,
    }
    _write_json(eval_json_path, evaluation)
    eval_md_path.write_text(_build_markdown(evaluation), encoding="utf-8")
    return evaluation


def _evaluate_candidate(
    *,
    runtime: BacktestRuntimeService,
    row: dict[str, Any],
    windows: list[dict[str, Any]],
) -> dict[str, Any]:
    preset_name = str(row["preset_name"])
    expected_family = str(row["expected_family"])

    total_net_profit = 0.0
    total_drawdown_pct = 0.0
    total_pf_bonus = 0.0
    total_trade_penalty = 0.0
    positive_windows = 0
    case_hits = 0
    case_hit_ids: list[str] = []
    window_summaries: list[dict[str, Any]] = []
    last_error: str | None = None

    for window in windows:
        report_payload = _load_json(Path(window["report_path"]))
        period = dict(report_payload.get("period") or {})
        start_utc = str(period.get("start_utc") or "")
        end_utc = str(period.get("end_utc") or "")
        try:
            payload = runtime.run_saved_preset(
                preset_name,
                RunOverrides(start=start_utc, end=end_utc),
            )
            summary = dict(payload.get("summary") or {})
            bundle_dir = Path(str(payload.get("verification_bundle") or ""))
            entry_times = _read_entry_times(bundle_dir)

            net_profit = float(_safe_float(summary.get("net_profit")) or 0.0)
            max_dd_pct = float(_safe_float(summary.get("max_drawdown_pct")) or 0.0)
            profit_factor = _safe_float(summary.get("profit_factor"))
            trade_count = int(summary.get("num_trades") or summary.get("trades") or 0)

            total_net_profit += net_profit
            total_drawdown_pct += max_dd_pct
            if net_profit > 0.0:
                positive_windows += 1
            if trade_count > 12:
                total_trade_penalty += (trade_count - 12) * 0.01
            if profit_factor is not None:
                total_pf_bonus += min(max(profit_factor - 1.0, 0.0), 0.6) * 0.10

            hits_list: list[str] = []
            if float(window["case_weight"] or 0.0) > 0.0:
                hits_count, hits_list = _case_hits_for_window(
                    report_payload=report_payload,
                    expected_family=expected_family,
                    entry_times=entry_times,
                )
                case_hits += hits_count
                case_hit_ids.extend(hits_list)

            window_summaries.append(
                {
                    "label": str(window["label"]),
                    "start_utc": start_utc,
                    "end_utc": end_utc,
                    "net_profit": round(net_profit, 6),
                    "profit_factor": round(profit_factor, 6) if profit_factor is not None else None,
                    "max_drawdown_pct": round(max_dd_pct, 6),
                    "num_trades": trade_count,
                    "case_hits": hits_list,
                    "verification_bundle": str(bundle_dir),
                }
            )
        except Exception as exc:
            last_error = str(exc)
            window_summaries.append(
                {
                    "label": str(window["label"]),
                    "error": str(exc),
                }
            )

    candidate_row = dict(row)
    candidate_row.update(
        {
            "case_hits": int(case_hits),
            "case_hit_ids": sorted(set(case_hit_ids)),
            "positive_windows": int(positive_windows),
            "total_net_profit": round(total_net_profit, 6),
            "total_drawdown_pct": round(total_drawdown_pct, 6),
            "total_pf_bonus": round(total_pf_bonus, 6),
            "total_trade_penalty": round(total_trade_penalty, 6),
            "windows": window_summaries,
            "error": last_error,
        }
    )
    candidate_row["score"] = round(_score_candidate(candidate_row), 6)
    return candidate_row


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Incremental ensemble search for the 5-trade pullback casebook using narrow HTF/orderflow filters."
    )
    parser.add_argument("--source-pack", type=Path, default=DEFAULT_SOURCE_PACK)
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    parser.add_argument("--seed-presets", default="901,902,903")
    parser.add_argument("--window-labels", default="case_window,oos_prev_equal,oos_prev2,oos_prev3")
    parser.add_argument("--filter-labels", default="", help="Optional comma-separated filter labels. Use 'base' to include the raw family preset.")
    parser.add_argument("--candidate-offset", type=int, default=0)
    parser.add_argument("--candidate-limit", type=int, default=0, help="0 means run every candidate in the manifest.")
    parser.add_argument("--top-per-family", type=int, default=2)
    parser.add_argument("--checkpoint-every", type=int, default=1)
    parser.add_argument("--resume", action="store_true")
    parser.add_argument("--rerun-errors", action="store_true")
    return parser


def main() -> None:
    args = _build_parser().parse_args()

    source_pack_path = Path(args.source_pack).resolve()
    output_dir = Path(args.output_dir).resolve()
    output_dir.mkdir(parents=True, exist_ok=True)

    candidate_pack_path = output_dir / "ensemble_stage1_v1.json"
    selected_pack_path = output_dir / "ensemble_stage1_selected.json"
    eval_json_path = output_dir / "ensemble_stage1_eval.json"
    eval_md_path = output_dir / "ensemble_stage1_eval.md"
    case_report_path = output_dir / "case_window_report.json"
    prev_month_report_path = output_dir / "prev_month_report.json"

    seed_presets = _csv_items(args.seed_presets) or list(DEFAULT_SEED_PRESETS)
    window_labels = _csv_items(args.window_labels) or list(DEFAULT_WINDOWS)
    filter_labels = set(_csv_items(args.filter_labels)) or None
    windows = _window_specs(window_labels)

    source_pack = _load_json(source_pack_path)
    base_case_report = _load_json(CASE_WINDOW_REPORT)
    base_prev_month_report = _load_json(PREV_MONTH_REPORT)

    candidate_pack, manifest_rows = _build_candidate_manifest(
        source_pack=source_pack,
        base_case_report=base_case_report,
        seed_presets=seed_presets,
        filter_labels=filter_labels,
    )
    _write_json(candidate_pack_path, candidate_pack)

    offset = max(0, int(args.candidate_offset))
    limit = int(args.candidate_limit)
    run_rows = manifest_rows[offset:]
    if limit > 0:
        run_rows = run_rows[:limit]

    existing_rows: dict[str, dict[str, Any]] = {}
    if args.resume and eval_json_path.exists():
        try:
            prior_eval = _load_json(eval_json_path)
            for prior_row in list(prior_eval.get("all_candidates") or []):
                if not isinstance(prior_row, dict):
                    continue
                name = str(prior_row.get("preset_name") or "")
                if not name:
                    continue
                existing_rows[name] = dict(prior_row)
        except Exception:
            existing_rows = {}

    run_options = {
        "seed_presets": seed_presets,
        "window_labels": window_labels,
        "filter_labels": sorted(filter_labels) if filter_labels else None,
        "candidate_offset": offset,
        "candidate_limit": limit,
        "checkpoint_every": int(max(1, args.checkpoint_every)),
        "resume": bool(args.resume),
        "rerun_errors": bool(args.rerun_errors),
    }

    _persist_outputs(
        rows_by_name=existing_rows,
        manifest_rows=manifest_rows,
        candidate_pack=candidate_pack,
        source_pack_path=source_pack_path,
        candidate_pack_path=candidate_pack_path,
        selected_pack_path=selected_pack_path,
        eval_json_path=eval_json_path,
        eval_md_path=eval_md_path,
        case_report_path=case_report_path,
        prev_month_report_path=prev_month_report_path,
        base_case_report=base_case_report,
        base_prev_month_report=base_prev_month_report,
        window_labels=window_labels,
        top_per_family=max(1, int(args.top_per_family)),
        run_options=run_options,
    )

    runtime = BacktestRuntimeService(presets_path=candidate_pack_path)
    completed_since_checkpoint = 0
    requested_windows = set(window_labels)

    for row in run_rows:
        preset_name = str(row["preset_name"])
        existing = existing_rows.get(preset_name)
        if existing is not None:
            if (not args.rerun_errors) and (not existing.get("error")) and _row_covers_windows(existing, requested_windows):
                print(f"[resume] {preset_name} already covers {', '.join(window_labels)}", flush=True)
                continue
            if args.rerun_errors or existing.get("error") or not _row_covers_windows(existing, requested_windows):
                print(f"[rerun] {preset_name}", flush=True)

        print(f"[search] {preset_name} {row['candidate_label']}", flush=True)
        evaluated = _evaluate_candidate(runtime=runtime, row=row, windows=windows)
        existing_rows[preset_name] = evaluated
        completed_since_checkpoint += 1

        if completed_since_checkpoint >= max(1, int(args.checkpoint_every)):
            _persist_outputs(
                rows_by_name=existing_rows,
                manifest_rows=manifest_rows,
                candidate_pack=candidate_pack,
                source_pack_path=source_pack_path,
                candidate_pack_path=candidate_pack_path,
                selected_pack_path=selected_pack_path,
                eval_json_path=eval_json_path,
                eval_md_path=eval_md_path,
                case_report_path=case_report_path,
                prev_month_report_path=prev_month_report_path,
                base_case_report=base_case_report,
                base_prev_month_report=base_prev_month_report,
                window_labels=window_labels,
                top_per_family=max(1, int(args.top_per_family)),
                run_options=run_options,
            )
            completed_since_checkpoint = 0

    evaluation = _persist_outputs(
        rows_by_name=existing_rows,
        manifest_rows=manifest_rows,
        candidate_pack=candidate_pack,
        source_pack_path=source_pack_path,
        candidate_pack_path=candidate_pack_path,
        selected_pack_path=selected_pack_path,
        eval_json_path=eval_json_path,
        eval_md_path=eval_md_path,
        case_report_path=case_report_path,
        prev_month_report_path=prev_month_report_path,
        base_case_report=base_case_report,
        base_prev_month_report=base_prev_month_report,
        window_labels=window_labels,
        top_per_family=max(1, int(args.top_per_family)),
        run_options=run_options,
    )

    print(str(candidate_pack_path))
    print(str(selected_pack_path))
    print(str(case_report_path))
    print(str(prev_month_report_path))
    print(str(eval_json_path))
    print(
        f"completed={evaluation.get('completed_candidates', 0)} "
        f"total={evaluation.get('total_candidates', 0)}",
        flush=True,
    )


if __name__ == "__main__":
    main()
