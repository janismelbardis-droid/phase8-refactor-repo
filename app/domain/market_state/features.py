from __future__ import annotations

"""Typed input adapters for the market-state pipeline."""

from typing import Any, Dict

from .schema import BarSnapshot, IndicatorSnapshot, MarketFeatures, MarketStateInputs, coerce_mapping


def build_bar_snapshot(snapshot: Any) -> BarSnapshot:
    return BarSnapshot.from_mapping(coerce_mapping(snapshot))


def build_indicator_snapshot(snapshot: Any) -> IndicatorSnapshot:
    return IndicatorSnapshot.from_mapping(coerce_mapping(snapshot))


def build_market_features(snapshot: Any) -> MarketFeatures:
    return MarketFeatures.from_mapping(coerce_mapping(snapshot))


def build_market_state_inputs(snapshot: Any, prev_state: Any = None) -> MarketStateInputs:
    return MarketStateInputs.from_payloads(snapshot, prev_state=prev_state)


def merge_market_snapshot(base: Any, *parts: Any) -> Dict[str, Any]:
    out = coerce_mapping(base)
    for part in parts:
        out.update(coerce_mapping(part))
    return out
