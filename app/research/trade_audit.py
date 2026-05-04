from __future__ import annotations

"""Trade-level backtest audit reporting helpers.

This module turns a BacktestResult or a saved backtest report payload into a
trade-by-trade audit pack that preserves:
- core trade metrics
- entry/exit snapshots
- parsed entry/exit rule traces
- run threshold/config pack

The output is intentionally JSON-friendly so it can be passed into later search
and optimization workflows.
"""

from dataclasses import asdict, is_dataclass
from pathlib import Path
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence, Tuple, Union
import gzip
import json
import re

import pandas as pd

from ..backtest import BacktestConfig, BacktestResult, Trade
from ..domain.market_state import DEFAULT_MARKET_STATE_THRESHOLDS, market_state_thresholds_dict


_TRACE_TAB_RE = re.compile(r"^Tab:\s*(.*?)\s*\|\s*Result:\s*(TRUE|FALSE)\s*\|\s*GroupJoin:\s*(.*)$", re.IGNORECASE)
_TRACE_GROUP_RE = re.compile(r"^Group\s+(\d+):\s*Join=(.*?)\s*\|\s*Result=(TRUE|FALSE)$", re.IGNORECASE)
_TRACE_RULE_RE = re.compile(r"^\s*(\d+)\.(\d+)\s+(TRUE|FALSE)((?:\s+\[[^\]]+\])*)\s*::\s*(.*)$", re.IGNORECASE)
_TRACE_TF_RE = re.compile(r"^TF\[(.*?)\]\s+cur=(.*?)\s*\|\s*prev=(.*)$", re.IGNORECASE)


AuditInput = Union[BacktestResult, Mapping[str, Any]]


def _safe_jsonable(value: Any) -> Any:
    if value is None:
        return None
    if isinstance(value, (str, int, float, bool)):
        return value
    if isinstance(value, pd.Timestamp):
        return pd.to_datetime(value, utc=True).isoformat()
    if isinstance(value, Mapping):
        return {str(k): _safe_jsonable(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [_safe_jsonable(v) for v in value]
    if is_dataclass(value):
        return _safe_jsonable(asdict(value))
    try:
        return json.loads(json.dumps(value, default=str))
    except Exception:
        return str(value)


def _bool_text(v: Any) -> bool:
    return str(v or "").strip().upper() == "TRUE"


def parse_formatted_tab_trace(trace_text: Any) -> Dict[str, Any]:
    text = str(trace_text or "").strip()
    if not text:
        return {}

    lines = [line.rstrip() for line in text.splitlines() if str(line).strip()]
    out: Dict[str, Any] = {
        "tab": "",
        "result": False,
        "groups_join": "",
        "groups": [],
        "tf_snapshots": {},
        "raw_text": text,
    }
    current_group: Optional[Dict[str, Any]] = None
    current_rule: Optional[Dict[str, Any]] = None

    for raw_line in lines:
        line = raw_line.strip("\n")
        m = _TRACE_TAB_RE.match(line)
        if m:
            out["tab"] = str(m.group(1) or "")
            out["result"] = _bool_text(m.group(2))
            out["groups_join"] = str(m.group(3) or "")
            current_group = None
            current_rule = None
            continue
        m = _TRACE_TF_RE.match(line)
        if m:
            out["tf_snapshots"][str(m.group(1) or "")] = {
                "current_meta_text": str(m.group(2) or ""),
                "previous_meta_text": str(m.group(3) or ""),
            }
            continue
        m = _TRACE_GROUP_RE.match(line)
        if m:
            current_group = {
                "group_index": int(m.group(1)),
                "join_mode": str(m.group(2) or ""),
                "result": _bool_text(m.group(3)),
                "rules": [],
            }
            out["groups"].append(current_group)
            current_rule = None
            continue
        m = _TRACE_RULE_RE.match(line)
        if m:
            badge_text = str(m.group(4) or "")
            current_rule = {
                "group_index": int(m.group(1)),
                "rule_index": int(m.group(2)),
                "result": _bool_text(m.group(3)),
                "is_trigger": "[TRIGGER]" in badge_text.upper(),
                "is_final_gate": "[FINAL GATE]" in badge_text.upper(),
                "is_sequence_canceler": "[SEQ CANCEL]" in badge_text.upper(),
                "label": str(m.group(5) or ""),
                "detail_text": "",
            }
            if current_group is not None:
                current_group["rules"].append(current_rule)
            continue
        if current_rule is not None and raw_line.startswith("      "):
            current_rule["detail_text"] = str(line)
            continue
        if current_group is not None and raw_line.startswith("  Sequence progress:"):
            current_group["sequence_progress"] = str(line).strip()
            continue
        if current_group is not None and raw_line.startswith("  Sequence reset:"):
            current_group["sequence_reset"] = str(line).strip()
            continue

    return out


def _coerce_trade_mapping(trade: Any) -> Dict[str, Any]:
    if isinstance(trade, Mapping):
        return dict(trade)
    if is_dataclass(trade):
        return asdict(trade)
    out: Dict[str, Any] = {}
    for name in (
        "trade_id", "side", "entry_time", "entry_price", "exit_time", "exit_price", "qty",
        "entry_notional", "exit_notional", "gross_pnl", "fees", "fee_entry", "fee_exit", "net_pnl",
        "return_pct", "duration_min", "mfe", "mae", "mfe_pct", "mae_pct", "entry_reason",
        "exit_reason", "entry_rule_trace", "exit_rule_trace", "entry_snapshot", "exit_snapshot",
        "entry_barsago", "exit_barsago", "signal_time", "order_created_time", "entry_fill_time",
        "exit_signal_time", "exit_order_created_time", "exit_fill_time", "engine_type", "fill_source",
        "ambiguous_exit", "stop_loss_price", "take_profit_price",
    ):
        out[name] = getattr(trade, name, None)
    return out


def _extract_time(value: Any) -> Optional[str]:
    if value in (None, "", pd.NaT):
        return None
    try:
        return pd.to_datetime(value, utc=True).isoformat()
    except Exception:
        return str(value)


def _market_context(snapshot: Any) -> Dict[str, Any]:
    if not isinstance(snapshot, Mapping):
        return {}
    out: Dict[str, Any] = {}
    for tf, snap in snapshot.items():
        if not isinstance(snap, Mapping):
            continue
        out[str(tf)] = {
            "market_state": snap.get("market_state"),
            "market_phase": snap.get("market_phase"),
            "market_bias": snap.get("market_bias"),
            "market_confidence": snap.get("market_confidence"),
            "market_compression_maturity": snap.get("market_compression_maturity"),
            "market_breakout_readiness": snap.get("market_breakout_readiness"),
            "market_breakout_confirmation": snap.get("market_breakout_confirmation"),
            "market_breakout_quality": snap.get("market_breakout_quality"),
            "market_breakout_fail_rate": snap.get("market_breakout_fail_rate"),
            "market_pressure_bias": snap.get("market_pressure_bias"),
            "market_delta_strength": snap.get("market_delta_strength"),
            "market_volume_support": snap.get("market_volume_support"),
            "market_direction_evidence": snap.get("market_direction_evidence"),
            "vwap_relation": snap.get("vwap_relation"),
        }
    return out


def _audit_stats(parsed: Dict[str, Any]) -> Dict[str, int]:
    groups = parsed.get("groups", []) if isinstance(parsed, dict) else []
    passed = 0
    failed = 0
    triggers = 0
    for group in groups:
        for rule in group.get("rules", []):
            if bool(rule.get("result")):
                passed += 1
            else:
                failed += 1
            if bool(rule.get("is_trigger")):
                triggers += 1
    return {
        "groups": len(groups),
        "rules_passed": passed,
        "rules_failed": failed,
        "trigger_rules": triggers,
    }


def extract_threshold_pack(config: Optional[BacktestConfig] = None,
                           summary: Optional[Mapping[str, Any]] = None,
                           market_thresholds: Optional[Mapping[str, Any]] = None) -> Dict[str, Any]:
    cfg = config or BacktestConfig()
    out = {
        "backtest_config": {
            "initial_balance": float(getattr(cfg, "initial_balance", 0.0) or 0.0),
            "order_notional_usdt": float(getattr(cfg, "order_notional_usdt", 0.0) or 0.0),
            "fee_rate": float(getattr(cfg, "fee_rate", 0.0) or 0.0),
            "slippage_bps": float(getattr(cfg, "slippage_bps", 0.0) or 0.0),
            "stop_loss_pct": float(getattr(cfg, "stop_loss_pct", 0.0) or 0.0),
            "take_profit_pct": float(getattr(cfg, "take_profit_pct", 0.0) or 0.0),
            "stop_loss_mode": str(getattr(cfg, "stop_loss_mode", "PERCENT") or "PERCENT"),
            "stop_loss_value": float(getattr(cfg, "stop_loss_value", getattr(cfg, "stop_loss_pct", 0.0)) or 0.0),
            "stop_loss_timeframe": str(getattr(cfg, "stop_loss_timeframe", "1m") or "1m"),
            "stop_loss_field": str(getattr(cfg, "stop_loss_field", "") or ""),
            "stop_loss_offset_pct": float(getattr(cfg, "stop_loss_offset_pct", 0.0) or 0.0),
            "take_profit_mode": str(getattr(cfg, "take_profit_mode", "PERCENT") or "PERCENT"),
            "take_profit_value": float(getattr(cfg, "take_profit_value", getattr(cfg, "take_profit_pct", 0.0)) or 0.0),
            "take_profit_timeframe": str(getattr(cfg, "take_profit_timeframe", "1m") or "1m"),
            "take_profit_field": str(getattr(cfg, "take_profit_field", "") or ""),
            "take_profit_offset_pct": float(getattr(cfg, "take_profit_offset_pct", 0.0) or 0.0),
            "allow_reverse": bool(getattr(cfg, "allow_reverse", False)),
            "skip_if_both_entries": bool(getattr(cfg, "skip_if_both_entries", False)),
            "step_timeframe": str(getattr(cfg, "step_timeframe", "1m") or "1m"),
        },
        "market_state_thresholds": _safe_jsonable(dict(market_thresholds or market_state_thresholds_dict(DEFAULT_MARKET_STATE_THRESHOLDS))),
    }
    if isinstance(summary, Mapping) and isinstance(summary.get("entry_filter"), Mapping):
        out["entry_filter"] = _safe_jsonable(summary.get("entry_filter"))
    return out


def _bt_result_parts(bt_result: AuditInput) -> Tuple[Optional[BacktestConfig], Dict[str, Any], List[Dict[str, Any]], Dict[str, Any], Optional[str], Optional[str]]:
    if isinstance(bt_result, BacktestResult):
        return bt_result.config, dict(bt_result.summary or {}), [_coerce_trade_mapping(t) for t in (bt_result.trades or [])], {}, _extract_time(bt_result.start), _extract_time(bt_result.end)
    if isinstance(bt_result, Mapping):
        payload = dict(bt_result)
        if isinstance(payload.get("result"), Mapping):
            payload = dict(payload.get("result") or {})
        cfg_data = payload.get("config", {}) if isinstance(payload.get("config"), Mapping) else {}
        cfg = BacktestConfig(
            initial_balance=float(cfg_data.get("initial_balance", 0.0) or 0.0),
            order_notional_usdt=float(cfg_data.get("order_notional_usdt", 0.0) or 0.0),
            fee_rate=float(cfg_data.get("fee_rate", 0.0) or 0.0),
            slippage_bps=float(cfg_data.get("slippage_bps", 0.0) or 0.0),
            stop_loss_pct=float(cfg_data.get("stop_loss_pct", 0.0) or 0.0),
            take_profit_pct=float(cfg_data.get("take_profit_pct", 0.0) or 0.0),
            stop_loss_mode=str(cfg_data.get("stop_loss_mode", "PERCENT") or "PERCENT"),
            stop_loss_value=float(cfg_data.get("stop_loss_value", cfg_data.get("stop_loss_pct", 0.0)) or 0.0),
            stop_loss_timeframe=str(cfg_data.get("stop_loss_timeframe", "1m") or "1m"),
            stop_loss_field=str(cfg_data.get("stop_loss_field", "") or ""),
            stop_loss_offset_pct=float(cfg_data.get("stop_loss_offset_pct", 0.0) or 0.0),
            take_profit_mode=str(cfg_data.get("take_profit_mode", "PERCENT") or "PERCENT"),
            take_profit_value=float(cfg_data.get("take_profit_value", cfg_data.get("take_profit_pct", 0.0)) or 0.0),
            take_profit_timeframe=str(cfg_data.get("take_profit_timeframe", "1m") or "1m"),
            take_profit_field=str(cfg_data.get("take_profit_field", "") or ""),
            take_profit_offset_pct=float(cfg_data.get("take_profit_offset_pct", 0.0) or 0.0),
            allow_reverse=bool(cfg_data.get("allow_reverse", False)),
            skip_if_both_entries=bool(cfg_data.get("skip_if_both_entries", False)),
        )
        return cfg, dict(payload.get("summary") or {}), [dict(x) for x in (payload.get("trades") or []) if isinstance(x, Mapping)], payload, _extract_time(payload.get("start")), _extract_time(payload.get("end"))
    raise TypeError("Unsupported backtest audit input")


def build_trade_audit_record(trade: Any,
                             threshold_pack: Optional[Dict[str, Any]] = None,
                             include_snapshots: bool = True) -> Dict[str, Any]:
    td = _coerce_trade_mapping(trade)
    entry_parsed = parse_formatted_tab_trace(td.get("entry_rule_trace"))
    exit_parsed = parse_formatted_tab_trace(td.get("exit_rule_trace"))
    entry_snapshot = td.get("entry_snapshot") if isinstance(td.get("entry_snapshot"), Mapping) else {}
    exit_snapshot = td.get("exit_snapshot") if isinstance(td.get("exit_snapshot"), Mapping) else {}
    record = {
        "trade_id": td.get("trade_id"),
        "side": td.get("side"),
        "entry_time": _extract_time(td.get("entry_time")),
        "exit_time": _extract_time(td.get("exit_time")),
        "signal_time": _extract_time(td.get("signal_time")),
        "order_created_time": _extract_time(td.get("order_created_time")),
        "entry_fill_time": _extract_time(td.get("entry_fill_time")),
        "exit_signal_time": _extract_time(td.get("exit_signal_time")),
        "exit_order_created_time": _extract_time(td.get("exit_order_created_time")),
        "exit_fill_time": _extract_time(td.get("exit_fill_time")),
        "entry_price": td.get("entry_price"),
        "exit_price": td.get("exit_price"),
        "qty": td.get("qty"),
        "entry_notional": td.get("entry_notional"),
        "exit_notional": td.get("exit_notional"),
        "gross_pnl": td.get("gross_pnl"),
        "fees": td.get("fees"),
        "fee_entry": td.get("fee_entry"),
        "fee_exit": td.get("fee_exit"),
        "net_pnl": td.get("net_pnl"),
        "return_pct": td.get("return_pct"),
        "duration_min": td.get("duration_min"),
        "mfe": td.get("mfe"),
        "mae": td.get("mae"),
        "mfe_pct": td.get("mfe_pct"),
        "mae_pct": td.get("mae_pct"),
        "engine_type": td.get("engine_type"),
        "fill_source": td.get("fill_source"),
        "ambiguous_exit": bool(td.get("ambiguous_exit", False)),
        "stop_loss_price": td.get("stop_loss_price"),
        "take_profit_price": td.get("take_profit_price"),
        "entry_reason": td.get("entry_reason"),
        "exit_reason": td.get("exit_reason"),
        "entry_barsago": _safe_jsonable(td.get("entry_barsago") if isinstance(td.get("entry_barsago"), Mapping) else {}),
        "exit_barsago": _safe_jsonable(td.get("exit_barsago") if isinstance(td.get("exit_barsago"), Mapping) else {}),
        "entry_rule_audit": {
            **entry_parsed,
            "stats": _audit_stats(entry_parsed),
            "raw_text": str(td.get("entry_rule_trace") or ""),
        },
        "exit_rule_audit": {
            **exit_parsed,
            "stats": _audit_stats(exit_parsed),
            "raw_text": str(td.get("exit_rule_trace") or ""),
        },
        "entry_market_context": _market_context(entry_snapshot),
        "exit_market_context": _market_context(exit_snapshot),
        "threshold_pack": _safe_jsonable(threshold_pack or {}),
    }
    if include_snapshots:
        record["entry_snapshot"] = _safe_jsonable(entry_snapshot)
        record["exit_snapshot"] = _safe_jsonable(exit_snapshot)
    return record


def build_trade_audit_report(bt_result: AuditInput,
                             market_thresholds: Optional[Mapping[str, Any]] = None,
                             include_snapshots: bool = True) -> Dict[str, Any]:
    cfg, summary, trades, raw_payload, start, end = _bt_result_parts(bt_result)
    threshold_pack = extract_threshold_pack(cfg, summary=summary, market_thresholds=market_thresholds)
    trade_records = [build_trade_audit_record(td, threshold_pack=threshold_pack, include_snapshots=include_snapshots) for td in trades]
    return {
        "report_type": "trade_audit",
        "report_version": 1,
        "start": start,
        "end": end,
        "run_summary": _safe_jsonable(summary),
        "threshold_pack": threshold_pack,
        "trade_count": len(trade_records),
        "trades": trade_records,
        "source_payload_meta": {
            "has_result_wrapper": bool(isinstance(bt_result, Mapping) and isinstance(bt_result.get("result") if isinstance(bt_result, Mapping) else None, Mapping)),
            "source_keys": sorted(list(raw_payload.keys())) if isinstance(raw_payload, Mapping) else [],
        },
    }


def trade_audit_dataframe(bt_result: AuditInput) -> pd.DataFrame:
    report = build_trade_audit_report(bt_result, include_snapshots=False)
    rows: List[Dict[str, Any]] = []
    for trade in report.get("trades", []):
        entry_ctx = trade.get("entry_market_context", {}) if isinstance(trade.get("entry_market_context", {}), Mapping) else {}
        entry_primary = next(iter(entry_ctx.values()), {}) if entry_ctx else {}
        exit_ctx = trade.get("exit_market_context", {}) if isinstance(trade.get("exit_market_context", {}), Mapping) else {}
        exit_primary = next(iter(exit_ctx.values()), {}) if exit_ctx else {}
        rows.append({
            "trade_id": trade.get("trade_id"),
            "side": trade.get("side"),
            "entry_time": trade.get("entry_time"),
            "exit_time": trade.get("exit_time"),
            "entry_price": trade.get("entry_price"),
            "exit_price": trade.get("exit_price"),
            "net_pnl": trade.get("net_pnl"),
            "return_pct": trade.get("return_pct"),
            "duration_min": trade.get("duration_min"),
            "entry_reason": trade.get("entry_reason"),
            "exit_reason": trade.get("exit_reason"),
            "entry_rules_passed": (trade.get("entry_rule_audit", {}).get("stats", {}) or {}).get("rules_passed"),
            "entry_rules_failed": (trade.get("entry_rule_audit", {}).get("stats", {}) or {}).get("rules_failed"),
            "exit_rules_passed": (trade.get("exit_rule_audit", {}).get("stats", {}) or {}).get("rules_passed"),
            "exit_rules_failed": (trade.get("exit_rule_audit", {}).get("stats", {}) or {}).get("rules_failed"),
            "entry_market_state": entry_primary.get("market_state"),
            "entry_market_phase": entry_primary.get("market_phase"),
            "entry_breakout_readiness": entry_primary.get("market_breakout_readiness"),
            "entry_breakout_confirmation": entry_primary.get("market_breakout_confirmation"),
            "exit_market_state": exit_primary.get("market_state"),
            "exit_market_phase": exit_primary.get("market_phase"),
        })
    return pd.DataFrame(rows)


def write_trade_audit_report(output_path: Union[str, Path],
                             bt_result: AuditInput,
                             market_thresholds: Optional[Mapping[str, Any]] = None,
                             include_snapshots: bool = True) -> Path:
    out = Path(output_path)
    out.parent.mkdir(parents=True, exist_ok=True)
    report = build_trade_audit_report(bt_result, market_thresholds=market_thresholds, include_snapshots=include_snapshots)
    out.write_text(json.dumps(report, indent=2, ensure_ascii=False, default=str), encoding="utf-8")
    return out


def write_trade_audit_csv(output_path: Union[str, Path], bt_result: AuditInput) -> Path:
    out = Path(output_path)
    out.parent.mkdir(parents=True, exist_ok=True)
    df = trade_audit_dataframe(bt_result)
    df.to_csv(out, index=False)
    return out


def load_backtest_report_payload(path: Union[str, Path]) -> Dict[str, Any]:
    fp = Path(path)
    if fp.suffix.lower() == ".gz":
        with gzip.open(fp, "rt", encoding="utf-8") as fh:
            return json.load(fh)
    return json.loads(fp.read_text(encoding="utf-8"))


__all__ = [
    "build_trade_audit_record",
    "build_trade_audit_report",
    "trade_audit_dataframe",
    "write_trade_audit_report",
    "write_trade_audit_csv",
    "load_backtest_report_payload",
    "parse_formatted_tab_trace",
    "extract_threshold_pack",
]
