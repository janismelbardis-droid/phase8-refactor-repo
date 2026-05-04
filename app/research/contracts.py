from __future__ import annotations

"""Live/backtest support-contract helpers for phase 7.

The goal of this module is to make research boundaries explicit without
changing trading behavior. It classifies which outputs are backtest-supported
versus live-only advisory enrichments and provides replay-capture payloads for
future research on live order-flow features.
"""

from dataclasses import asdict, dataclass, field
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence

from ..domain.market_state import coerce_mapping

VALIDATED_MARKET_FEATURES: Sequence[str] = (
    "market_state",
    "market_phase",
    "market_bias",
    "market_confidence",
    "market_summary",
    "market_structure",
    "market_breakout_quality",
    "market_breakout_readiness",
    "market_breakout_confirmation",
    "market_breakout_trigger_ready",
    "market_breakout_confirmed",
    "market_pressure_bias",
)

ADVISORY_ORDERFLOW_FEATURES: Sequence[str] = (
    "of_tape_quality",
    "of_tape_confidence",
    "of_dom_quality",
    "of_dom_confidence",
    "of_data_quality",
    "of_data_confidence",
    "of_watch_allowed",
    "of_confirmation_allowed",
    "of_quality_note",
    "of_dom_sync_state_pretty",
    "orderflow_status",
    "orderflow_status_reason",
)

ORDERFLOW_CAPTURE_KEYS: Sequence[str] = (
    "of_dom_source",
    "of_last_trade_ts_ms",
    "of_dom_depth_age_ms",
    "of_dom_snapshot_age_ms",
    "of_dom_resync_required",
    "of_dom_local_book_synced",
    "of_dom_partial_connected",
    "of_dom_sync_state",
    "of_tape_quality",
    "of_tape_confidence",
    "of_dom_quality",
    "of_dom_confidence",
    "of_data_quality",
    "of_data_confidence",
    "of_watch_allowed",
    "of_confirmation_allowed",
    "orderflow_status",
    "orderflow_status_reason",
)

MARKET_CAPTURE_KEYS: Sequence[str] = (
    "market_state",
    "market_phase",
    "market_bias",
    "market_confidence",
    "market_summary",
    "market_breakout_quality",
    "market_breakout_readiness",
    "market_breakout_confirmation",
)


@dataclass(frozen=True)
class SupportContract:
    runtime: str = "unknown"
    support_mode: str = "validated_only"
    validated_features: List[str] = field(default_factory=list)
    advisory_features: List[str] = field(default_factory=list)
    experimental_features: List[str] = field(default_factory=list)
    research_ready: bool = True
    replay_capture_ready: bool = False
    boundary_note: str = ""

    def as_dict(self) -> Dict[str, Any]:
        return asdict(self)

    def as_overlay(self, *, prefix: str = "support") -> Dict[str, Any]:
        return {
            f"{prefix}_runtime": self.runtime,
            f"{prefix}_mode": self.support_mode,
            f"{prefix}_validated_features": list(self.validated_features),
            f"{prefix}_advisory_features": list(self.advisory_features),
            f"{prefix}_experimental_features": list(self.experimental_features),
            f"{prefix}_research_ready": bool(self.research_ready),
            f"{prefix}_replay_capture_ready": bool(self.replay_capture_ready),
            f"{prefix}_boundary_note": self.boundary_note,
        }


@dataclass(frozen=True)
class ReplayCaptureRecord:
    runtime: str
    symbol: str
    timeframe: str
    ts_ms: Optional[int]
    price: Optional[float]
    market: Dict[str, Any] = field(default_factory=dict)
    orderflow: Dict[str, Any] = field(default_factory=dict)

    def as_dict(self) -> Dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class ContractMatrix:
    row_count: int
    by_runtime: Dict[str, int]
    by_mode: Dict[str, int]
    advisory_rows: int
    validated_only_rows: int
    replay_capture_ready_rows: int

    def as_dict(self) -> Dict[str, Any]:
        return asdict(self)


def _coerce_text(value: Any, default: str = "") -> str:
    try:
        return str(value if value is not None else default)
    except Exception:
        return default


def _coerce_bool(value: Any) -> bool:
    try:
        return bool(value)
    except Exception:
        return False


def _coerce_int(value: Any) -> Optional[int]:
    try:
        if value is None:
            return None
        return int(value)
    except Exception:
        return None


def _coerce_float(value: Any) -> Optional[float]:
    try:
        if value is None:
            return None
        return float(value)
    except Exception:
        return None


def _normalize_runtime(value: Any) -> str:
    text = _coerce_text(value, "").strip().lower()
    if text in {"live", "backtest", "research"}:
        return text
    return "unknown"


def _present_features(row: Mapping[str, Any], names: Sequence[str]) -> List[str]:
    present: List[str] = []
    for name in names:
        if name in row and row.get(name) not in (None, ""):
            present.append(name)
    return present


def build_support_contract(row: Mapping[str, Any], *, runtime: Optional[str] = None) -> SupportContract:
    payload = coerce_mapping(row)
    runtime_norm = _normalize_runtime(runtime or payload.get("runtime") or payload.get("support_runtime"))

    validated = _present_features(payload, VALIDATED_MARKET_FEATURES)
    advisory = _present_features(payload, ADVISORY_ORDERFLOW_FEATURES)
    experimental: List[str] = []

    orderflow_status = _coerce_text(payload.get("orderflow_status"), "").strip().lower()
    orderflow_present = bool(advisory or any(key.startswith("of_") for key in payload.keys()) or orderflow_status)
    replay_ready = orderflow_present and runtime_norm == "live"

    if runtime_norm == "backtest":
        advisory = []
        replay_ready = False
    elif runtime_norm == "research" and orderflow_present:
        advisory = sorted(set(advisory + ["orderflow_replay_review"]))

    if orderflow_status in {"failed", "partial", "degraded"}:
        experimental.append("degraded_orderflow_overlay")

    if runtime_norm == "live" and orderflow_present:
        support_mode = "validated_plus_advisory"
        boundary_note = (
            "Market-state outputs are backtest-supported; order-flow outputs remain live-only advisory "
            "until replay-backed research is complete."
        )
        research_ready = False
    elif runtime_norm == "backtest":
        support_mode = "validated_only"
        boundary_note = "Backtest path contains validated market-state outputs only."
        research_ready = True
    elif runtime_norm == "research" and orderflow_present:
        support_mode = "research_review"
        boundary_note = "Research rows may include replayed order-flow fields under review."
        research_ready = True
    else:
        support_mode = "validated_only"
        boundary_note = "No live-only advisory fields detected."
        research_ready = True

    return SupportContract(
        runtime=runtime_norm,
        support_mode=support_mode,
        validated_features=sorted(set(validated)),
        advisory_features=sorted(set(advisory)),
        experimental_features=sorted(set(experimental)),
        research_ready=bool(research_ready),
        replay_capture_ready=bool(replay_ready),
        boundary_note=boundary_note,
    )


def build_contract_overlay(row: Mapping[str, Any], *, runtime: Optional[str] = None, prefix: str = "support") -> Dict[str, Any]:
    contract = build_support_contract(row, runtime=runtime)
    return contract.as_overlay(prefix=prefix)


def apply_contract_overlay(row: Mapping[str, Any], *, runtime: Optional[str] = None, prefix: str = "support") -> Dict[str, Any]:
    payload = dict(coerce_mapping(row))
    payload.update(build_contract_overlay(payload, runtime=runtime, prefix=prefix))
    return payload


def build_replay_capture_record(row: Mapping[str, Any], *, runtime: Optional[str] = None) -> ReplayCaptureRecord:
    payload = coerce_mapping(row)
    runtime_norm = _normalize_runtime(runtime or payload.get("runtime") or payload.get("support_runtime") or "live")
    ts_ms = _coerce_int(payload.get("ts_ms") or payload.get("close_time_ms") or payload.get("bar_close_time_ms"))
    price = _coerce_float(payload.get("price") if payload.get("price") is not None else payload.get("close"))
    market = {key: payload.get(key) for key in MARKET_CAPTURE_KEYS if key in payload}
    orderflow = {key: payload.get(key) for key in ORDERFLOW_CAPTURE_KEYS if key in payload}
    return ReplayCaptureRecord(
        runtime=runtime_norm,
        symbol=_coerce_text(payload.get("symbol"), ""),
        timeframe=_coerce_text(payload.get("timeframe") or payload.get("tf"), ""),
        ts_ms=ts_ms,
        price=price,
        market=market,
        orderflow=orderflow,
    )


def build_replay_capture_batch(rows: Iterable[Mapping[str, Any]], *, runtime: Optional[str] = None) -> List[Dict[str, Any]]:
    return [build_replay_capture_record(row, runtime=runtime).as_dict() for row in rows]


def build_contract_matrix(rows: Iterable[Mapping[str, Any]], *, runtime: Optional[str] = None) -> ContractMatrix:
    runtime_counts: Dict[str, int] = {}
    mode_counts: Dict[str, int] = {}
    advisory_rows = 0
    validated_only_rows = 0
    replay_ready_rows = 0
    total = 0
    for row in rows:
        contract = build_support_contract(row, runtime=runtime)
        total += 1
        runtime_counts[contract.runtime] = runtime_counts.get(contract.runtime, 0) + 1
        mode_counts[contract.support_mode] = mode_counts.get(contract.support_mode, 0) + 1
        if contract.advisory_features:
            advisory_rows += 1
        if contract.support_mode == "validated_only":
            validated_only_rows += 1
        if contract.replay_capture_ready:
            replay_ready_rows += 1
    return ContractMatrix(
        row_count=total,
        by_runtime=runtime_counts,
        by_mode=mode_counts,
        advisory_rows=advisory_rows,
        validated_only_rows=validated_only_rows,
        replay_capture_ready_rows=replay_ready_rows,
    )
