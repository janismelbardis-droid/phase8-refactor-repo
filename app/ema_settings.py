from __future__ import annotations

from typing import Any, Dict, Optional, Sequence, Tuple

from .parameter_indicator_settings import (
    coerce_int_fields,
    normalize_parameter_indicator_settings,
    parameter_indicator_settings_signature,
    resolve_parameter_indicator_settings,
)

DEFAULT_EMA_1_LENGTH = 9
DEFAULT_EMA_2_LENGTH = 21
EMA_FIELD_ORDER = ("ema_1_length", "ema_2_length")


def coerce_ema_pair(raw_pair: Any, fallback: Optional[Dict[str, int]] = None) -> Dict[str, int]:
    base = dict(fallback or {"ema_1_length": DEFAULT_EMA_1_LENGTH, "ema_2_length": DEFAULT_EMA_2_LENGTH})
    return coerce_int_fields(raw_pair, base)


def normalize_ema_settings(raw: Any) -> Dict[str, Any]:
    return normalize_parameter_indicator_settings(
        raw,
        root_key="ema_indicators",
        coerce_config=coerce_ema_pair,
    )


def resolve_ema_settings(raw: Any, timeframe: Optional[str] = None) -> Dict[str, int]:
    return resolve_parameter_indicator_settings(
        raw,
        root_key="ema_indicators",
        coerce_config=coerce_ema_pair,
        timeframe=timeframe,
    )


def ema_settings_signature(raw: Any, *, timeframes: Optional[Sequence[str]] = None) -> Tuple[Tuple[str, int, int], ...]:
    return parameter_indicator_settings_signature(
        raw,
        root_key="ema_indicators",
        coerce_config=coerce_ema_pair,
        field_order=EMA_FIELD_ORDER,
        timeframes=timeframes,
    )


def ema_required_warmup_bars(raw: Any, *, timeframes: Optional[Sequence[str]] = None) -> int:
    signature = ema_settings_signature(raw, timeframes=timeframes)
    max_length = 0
    for _label, fast, slow in signature:
        try:
            max_length = max(max_length, int(fast), int(slow))
        except Exception:
            continue
    return max(0, int(max_length))
