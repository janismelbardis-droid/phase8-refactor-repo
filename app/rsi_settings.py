from __future__ import annotations

from typing import Any, Dict, Optional, Sequence, Tuple

from .constants import RSI_LENGTH, RSI_SMOOTHING
from .parameter_indicator_settings import (
    coerce_int_fields,
    normalize_parameter_indicator_settings,
    parameter_indicator_settings_signature,
    resolve_parameter_indicator_settings,
)

RSI_FIELD_ORDER = ("length", "smoothing")


def coerce_rsi_config(raw_config: Any, fallback: Optional[Dict[str, int]] = None) -> Dict[str, int]:
    base = dict(fallback or {"length": RSI_LENGTH, "smoothing": RSI_SMOOTHING})
    return coerce_int_fields(raw_config, base)


def normalize_rsi_settings(raw: Any) -> Dict[str, Any]:
    return normalize_parameter_indicator_settings(
        raw,
        root_key="rsi_indicators",
        coerce_config=coerce_rsi_config,
    )


def resolve_rsi_settings(raw: Any, timeframe: Optional[str] = None) -> Dict[str, int]:
    return resolve_parameter_indicator_settings(
        raw,
        root_key="rsi_indicators",
        coerce_config=coerce_rsi_config,
        timeframe=timeframe,
    )


def rsi_settings_signature(raw: Any, *, timeframes: Optional[Sequence[str]] = None) -> Tuple[Tuple[str, int, int], ...]:
    return parameter_indicator_settings_signature(
        raw,
        root_key="rsi_indicators",
        coerce_config=coerce_rsi_config,
        field_order=RSI_FIELD_ORDER,
        timeframes=timeframes,
    )


def rsi_required_warmup_bars(raw: Any, *, timeframes: Optional[Sequence[str]] = None) -> int:
    signature = rsi_settings_signature(raw, timeframes=timeframes)
    max_need = 0
    for _label, length, smoothing in signature:
        try:
            max_need = max(max_need, int(length) + int(smoothing) + 4)
        except Exception:
            continue
    return max(0, int(max_need))
