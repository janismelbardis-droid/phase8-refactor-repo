from __future__ import annotations

"""Helpers that keep live order-flow enrichments explicitly advisory."""

from typing import Any, Dict, Mapping, Optional

from ...research.contracts import (
    apply_contract_overlay,
    build_replay_capture_record,
)
from ...domain.market_state import coerce_mapping


ORDERFLOW_BRIDGE_VERSION = "phase7"


def attach_live_contract_overlay(snapshot: Mapping[str, Any], *, runtime: str = "live") -> Dict[str, Any]:
    payload = dict(coerce_mapping(snapshot))
    payload.update(apply_contract_overlay(payload, runtime=runtime))
    payload.setdefault("support_bridge_version", ORDERFLOW_BRIDGE_VERSION)
    return payload


def build_live_replay_capture(snapshot: Mapping[str, Any], *, runtime: str = "live") -> Dict[str, Any]:
    record = build_replay_capture_record(snapshot, runtime=runtime).as_dict()
    record["bridge_version"] = ORDERFLOW_BRIDGE_VERSION
    return record


def merge_live_advisory_payload(
    snapshot: Mapping[str, Any],
    advisory_overlay: Optional[Mapping[str, Any]] = None,
    *,
    runtime: str = "live",
) -> Dict[str, Any]:
    payload = dict(coerce_mapping(snapshot))
    if advisory_overlay:
        payload.update(coerce_mapping(advisory_overlay))
    payload = attach_live_contract_overlay(payload, runtime=runtime)
    return payload
