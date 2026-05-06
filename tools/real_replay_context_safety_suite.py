from __future__ import annotations

import argparse
import copy
import json
import math
from datetime import datetime, timezone
from pathlib import Path
import shutil
import sys
import time
from typing import Any, Dict, List

import pandas as pd


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from app.backtest import BacktestConfig
from app.backtest_reporting import build_backtest_replay_strategy_signature
from app.fast_backtest import run_backtest_auto
from app.research.replay import replay_backtest_result
from app.rules import EntryFilterConfig
from app.strategy_requirements import compile_stream_requirements
from app.utils_time import parse_utc, required_warmup_minutes
from tools.run_saved_preset import (
    _load_presets,
    _materialize_preset,
    _resolve_cache_dir,
    execute_saved_preset_job,
)


def _safe_float(value: Any) -> float | None:
    try:
        number = float(value)
    except Exception:
        return None
    if not math.isfinite(number):
        return None
    return float(number)


def _approx_equal(left: Any, right: Any, tol: float = 1e-9) -> bool:
    a = _safe_float(left)
    b = _safe_float(right)
    if a is None or b is None:
        return False
    return abs(a - b) <= float(tol)


def _make_check(name: str, passed: bool, detail: str) -> Dict[str, Any]:
    return {
        "name": str(name),
        "passed": bool(passed),
        "detail": str(detail),
    }


def _trade_signature(result: Any) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    for trade in list(getattr(result, "trades", []) or []):
        rows.append(
            {
                "side": str(getattr(trade, "side", "") or ""),
                "entry_time": str(getattr(trade, "entry_time", "") or ""),
                "exit_time": str(getattr(trade, "exit_time", "") or ""),
                "entry_price": _safe_float(getattr(trade, "entry_price", None)),
                "exit_price": _safe_float(getattr(trade, "exit_price", None)),
                "net_pnl": _safe_float(getattr(trade, "net_pnl", None)),
                "entry_reason": str(getattr(trade, "entry_reason", "") or ""),
                "exit_reason": str(getattr(trade, "exit_reason", "") or ""),
            }
        )
    return rows


def _results_match(left: Any, right: Any) -> bool:
    left_summary = dict(getattr(left, "summary", {}) or {})
    right_summary = dict(getattr(right, "summary", {}) or {})
    if not _approx_equal(left_summary.get("ending_balance"), right_summary.get("ending_balance")):
        return False
    if int(left_summary.get("num_trades", 0) or 0) != int(right_summary.get("num_trades", 0) or 0):
        return False
    left_trades = _trade_signature(left)
    right_trades = _trade_signature(right)
    if len(left_trades) != len(right_trades):
        return False
    for left_trade, right_trade in zip(left_trades, right_trades):
        if left_trade["side"] != right_trade["side"]:
            return False
        if left_trade["entry_time"] != right_trade["entry_time"]:
            return False
        if left_trade["exit_time"] != right_trade["exit_time"]:
            return False
        if not _approx_equal(left_trade["entry_price"], right_trade["entry_price"]):
            return False
        if not _approx_equal(left_trade["exit_price"], right_trade["exit_price"]):
            return False
        if not _approx_equal(left_trade["net_pnl"], right_trade["net_pnl"], tol=1e-6):
            return False
        if left_trade["entry_reason"] != right_trade["entry_reason"]:
            return False
        if left_trade["exit_reason"] != right_trade["exit_reason"]:
            return False
    return True


def _default_cfg() -> BacktestConfig:
    return BacktestConfig(
        initial_balance=1000.0,
        order_notional_usdt=100.0,
        fee_rate=0.0004,
        slippage_bps=0.0,
        stop_loss_pct=0.0,
        take_profit_pct=0.0,
        stop_loss_mode="PERCENT",
        stop_loss_value=0.0,
        stop_loss_timeframe="1m",
        stop_loss_field="",
        stop_loss_offset_pct=0.0,
        take_profit_mode="PERCENT",
        take_profit_value=0.0,
        take_profit_timeframe="1m",
        take_profit_field="",
        take_profit_offset_pct=0.0,
        allow_reverse=True,
        skip_if_both_entries=True,
        step_timeframe="1m",
        capture_trade_details=True,
        equity_curve_stride=60,
    )


def _load_base_case(*, preset_name: str, start_text: str, end_text: str) -> Dict[str, Any]:
    presets_path = REPO_ROOT / "app" / "presets.json"
    payload = _load_presets(presets_path)
    preset = payload["presets"][preset_name]
    settings = preset.get("settings", {})
    if not isinstance(settings, dict):
        raise ValueError(f"Preset '{preset_name}' settings payload is invalid.")

    symbol = str(settings.get("symbol", "BTCUSDT") or "BTCUSDT").strip().upper()
    start = parse_utc(start_text)
    end = parse_utc(end_text)
    tfs = [tf for tf, enabled in (settings.get("timeframes", {}) or {}).items() if bool(enabled)]
    if "1m" not in tfs:
        tfs = ["1m"] + tfs
    if "1m" not in tfs:
        tfs.append("1m")

    rules_model, tab_group_join_mode, group_rule_join_mode, entry_filters = _materialize_preset(preset)
    cfg = _default_cfg()
    req_spec = compile_stream_requirements(
        rules_model,
        entry_filters=entry_filters,
        backtest_cfg=cfg,
        include_plot_defaults=True,
    )
    preset_warmup = int(settings.get("warmup_min", 0) or 0)
    needed_warmup = int(required_warmup_minutes(tfs, required_fields=req_spec) or 0)
    query_warmup = max(0, preset_warmup)
    compute_warmup = max(query_warmup, needed_warmup)
    cache_dir = _resolve_cache_dir(REPO_ROOT, str(settings.get("cache_dir", "data_cache") or "data_cache"))
    cache_dir.mkdir(parents=True, exist_ok=True)

    execution = execute_saved_preset_job(
        repo_root=REPO_ROOT,
        preset_name=preset_name,
        symbol=symbol,
        start=start,
        end=end,
        tfs=tfs,
        cache_dir=cache_dir,
        bt_mode=str(settings.get("bt_mode", "") or ""),
        price_source=str(settings.get("price_source", "LAST") or "LAST").upper(),
        macd_impl=str(settings.get("macd_impl", "TRADINGVIEW") or "TRADINGVIEW").upper(),
        adx_impl=str(settings.get("adx_impl", "TRADINGVIEW") or "TRADINGVIEW").upper(),
        rules_model=rules_model,
        tab_group_join_mode=tab_group_join_mode,
        group_rule_join_mode=group_rule_join_mode,
        entry_filters=entry_filters,
        cfg=cfg,
        req_spec=req_spec,
        query_warmup=query_warmup,
        compute_warmup=compute_warmup,
        collect_event_activity=False,
        collect_signal_rows=False,
        write_verification_bundle=False,
    )
    return {
        "preset_name": preset_name,
        "symbol": symbol,
        "start": start,
        "end": end,
        "tfs": tfs,
        "settings": settings,
        "cfg": cfg,
        "rules_model": rules_model,
        "tab_group_join_mode": tab_group_join_mode,
        "group_rule_join_mode": group_rule_join_mode,
        "entry_filters": entry_filters,
        "execution": execution,
    }


def _run_modified_case(
    *,
    label: str,
    base_case: Dict[str, Any],
    modified_rules_model: Dict[str, Any],
    modified_entry_filters: Dict[str, Any],
    expected_engine: str | None,
) -> Dict[str, Any]:
    base_result = base_case["execution"]["result"]
    base_replay_context = copy.deepcopy(getattr(base_result, "replay_context", None) or {})
    base_signature = str(base_replay_context.get("__strategy_signature") or "")

    modified_result = run_backtest_auto(
        symbol=str(base_case["symbol"]),
        streams_full=dict(base_case["execution"]["streams_full"] or {}),
        start=base_case["start"],
        end=base_case["end"],
        rules_model=modified_rules_model,
        tab_group_join_mode=dict(base_case["tab_group_join_mode"]),
        group_rule_join_mode={k: list(v) for k, v in dict(base_case["group_rule_join_mode"]).items()},
        cfg=base_case["cfg"],
        df_1m_full=base_case["execution"]["df_1m"],
        entry_filters=modified_entry_filters,
        apply_htf_closed_only=False,
        replay_context=base_replay_context,
    )
    modified_summary = dict(modified_result.summary or {})
    modified_replay_context = getattr(modified_result, "replay_context", None) or {}
    modified_signature = str(modified_replay_context.get("__strategy_signature") or "")
    expected_signature = build_backtest_replay_strategy_signature(
        symbol=base_case["symbol"],
        start=base_case["start"],
        end=base_case["end"],
        rules_model=modified_rules_model,
        tab_group_join_mode=base_case["tab_group_join_mode"],
        group_rule_join_mode=base_case["group_rule_join_mode"],
        entry_filters=modified_entry_filters,
    )

    replayed_result = replay_backtest_result(modified_result)
    replayed_summary = dict(replayed_result.summary or {})

    checks = [
        _make_check(
            f"{label}_signature_changed",
            bool(base_signature) and modified_signature != base_signature,
            f"base={base_signature} modified={modified_signature}",
        ),
        _make_check(
            f"{label}_signature_matches_modified_strategy",
            modified_signature == expected_signature,
            f"expected={expected_signature} actual={modified_signature}",
        ),
        _make_check(
            f"{label}_replay_matches_modified_result",
            _results_match(modified_result, replayed_result),
            (
            f"modified_balance={modified_summary.get('ending_balance')} "
                f"replayed_balance={replayed_summary.get('ending_balance')}"
            ),
        ),
    ]
    if str(expected_engine or "").strip():
        checks.insert(
            0,
            _make_check(
                f"{label}_engine_match",
                str(modified_summary.get("strategy_plan_engine") or "") == str(expected_engine),
                f"expected={expected_engine} actual={modified_summary.get('strategy_plan_engine')}",
            ),
        )
    return {
        "label": label,
        "base_signature": base_signature,
        "modified_signature": modified_signature,
        "expected_signature": expected_signature,
        "modified_summary": modified_summary,
        "replayed_summary": replayed_summary,
        "modified_trade_signature": _trade_signature(modified_result),
        "replayed_trade_signature": _trade_signature(replayed_result),
        "checks": checks,
    }


def _write_report(path: Path, payload: Dict[str, Any]) -> None:
    lines: List[str] = []
    lines.append("# Replay Context Safety Suite")
    lines.append("")
    lines.append(f"- generated_utc: `{payload.get('generated_utc')}`")
    lines.append(f"- overall: `{'PASS' if payload.get('all_passed') else 'FAIL'}`")
    lines.append("")
    lines.append("## Cases")
    lines.append("")
    lines.append("| Case | Engine | Ending balance | Trades | Signature changed | Replay matched |")
    lines.append("|---|---|---:|---:|---|---|")
    for case in payload.get("cases", []):
        summary = dict(case.get("modified_summary") or {})
        checks = {str(check.get("name") or ""): bool(check.get("passed")) for check in list(case.get("checks") or [])}
        label = str(case.get("label") or "")
        lines.append(
            "| "
            + " | ".join(
                [
                    label,
                    str(summary.get("strategy_plan_engine") or ""),
                    str(summary.get("ending_balance")),
                    str(summary.get("num_trades")),
                    "yes" if checks.get(f"{label}_signature_changed") else "no",
                    "yes" if checks.get(f"{label}_replay_matches_modified_result") else "no",
                ]
            )
            + " |"
        )
    lines.append("")
    lines.append("## Checks")
    lines.append("")
    lines.append("| Check | Status | Detail |")
    lines.append("|---|---|---|")
    for check in payload.get("checks", []):
        lines.append(f"| {check.get('name')} | {'PASS' if check.get('passed') else 'FAIL'} | {check.get('detail')} |")
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def main() -> int:
    parser = argparse.ArgumentParser(description="Verify replay_context is rebuilt safely when strategy semantics change.")
    parser.add_argument("--output-dir", default=None)
    args = parser.parse_args()

    stamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    output_dir = (
        Path(args.output_dir).resolve()
        if args.output_dir
        else (REPO_ROOT / "runs" / "perf" / f"real_replay_context_safety_suite_{stamp}")
    )
    output_dir.mkdir(parents=True, exist_ok=True)

    started = time.perf_counter()
    case_901 = _load_base_case(
        preset_name="901",
        start_text="2026-04-20 00:00:00+00:00",
        end_text="2026-04-20 04:00:00+00:00",
    )
    modified_filters_901 = {
        "Long Entry": EntryFilterConfig(),
        "Short Entry": EntryFilterConfig(),
    }
    generic_case = _run_modified_case(
        label="generic_entry_filter_change",
        base_case=case_901,
        modified_rules_model=copy.deepcopy(case_901["rules_model"]),
        modified_entry_filters=modified_filters_901,
        expected_engine=None,
    )

    case_902 = _load_base_case(
        preset_name="902",
        start_text="2026-04-20 00:00:00+00:00",
        end_text="2026-04-20 04:00:00+00:00",
    )
    modified_rules_902 = copy.deepcopy(case_902["rules_model"])
    modified_rules_902["Long Exit"] = []
    compiled_case = _run_modified_case(
        label="compiled_rule_change",
        base_case=case_902,
        modified_rules_model=modified_rules_902,
        modified_entry_filters=copy.deepcopy(case_902["entry_filters"]),
        expected_engine="compiled_bar",
    )

    cases = [generic_case, compiled_case]
    checks: List[Dict[str, Any]] = []
    for case in cases:
        checks.extend(list(case.get("checks") or []))
    all_passed = all(bool(check.get("passed")) for check in checks)

    payload = {
        "generated_utc": datetime.now(timezone.utc).isoformat(),
        "repo_root": str(REPO_ROOT),
        "all_passed": bool(all_passed),
        "wall_sec": round(time.perf_counter() - started, 4),
        "cases": cases,
        "checks": checks,
    }

    summary_path = output_dir / "summary.json"
    report_path = output_dir / "report.md"
    summary_path.write_text(json.dumps(payload, indent=2, ensure_ascii=False, default=str), encoding="utf-8")
    _write_report(report_path, payload)

    latest_dir = REPO_ROOT / "runs" / "perf" / "real_replay_context_safety_suite_latest"
    if latest_dir.exists():
        shutil.rmtree(latest_dir)
    shutil.copytree(output_dir, latest_dir)

    print(f"Overall: {'PASS' if all_passed else 'FAIL'}", flush=True)
    print(f"Saved summary JSON: {summary_path}", flush=True)
    print(f"Saved report MD: {report_path}", flush=True)
    print(f"Updated latest snapshot: {latest_dir}", flush=True)
    return 0 if all_passed else 1


if __name__ == "__main__":
    raise SystemExit(main())
