from __future__ import annotations

import json
from typing import Any, Callable, Dict, Mapping, Optional, Sequence, Tuple

from .constants import ALL_TIMEFRAMES


CoerceConfigFn = Callable[[Any, Optional[Dict[str, int]]], Dict[str, int]]


def ordered_timeframe_key(tf: str) -> int:
    try:
        return ALL_TIMEFRAMES.index(tf)
    except Exception:
        return 999


def coerce_int_fields(
    raw_config: Any,
    field_defaults: Mapping[str, int],
    *,
    minimum: int = 1,
) -> Dict[str, int]:
    config = raw_config if isinstance(raw_config, dict) else {}
    result: Dict[str, int] = {}
    for field_name, fallback in field_defaults.items():
        try:
            value = max(int(minimum), int(config.get(field_name, fallback) or fallback))
        except Exception:
            value = int(fallback)
        result[str(field_name)] = int(value)
    return result


def normalize_parameter_indicator_settings(
    raw: Any,
    *,
    root_key: str,
    coerce_config: CoerceConfigFn,
) -> Dict[str, Any]:
    source = raw
    if isinstance(source, str):
        text = source.strip()
        if text:
            try:
                decoded = json.loads(text)
                if isinstance(decoded, dict):
                    source = decoded
            except Exception:
                pass
    if isinstance(source, dict) and isinstance(source.get(root_key), dict):
        source = source.get(root_key)
    raw_map = source if isinstance(source, dict) else {}
    defaults_source = raw_map.get("defaults") if isinstance(raw_map.get("defaults"), dict) else raw_map
    defaults = coerce_config(defaults_source, None)
    overrides_raw = raw_map.get("overrides") if isinstance(raw_map.get("overrides"), dict) else {}
    overrides: Dict[str, Dict[str, int]] = {}
    for raw_tf, raw_config in overrides_raw.items():
        tf = str(raw_tf or "").strip()
        if not tf:
            continue
        overrides[tf] = coerce_config(raw_config, defaults)
    return {
        "defaults": defaults,
        "overrides": overrides,
    }


def resolve_parameter_indicator_settings(
    raw: Any,
    *,
    root_key: str,
    coerce_config: CoerceConfigFn,
    timeframe: Optional[str] = None,
) -> Dict[str, int]:
    normalized = normalize_parameter_indicator_settings(raw, root_key=root_key, coerce_config=coerce_config)
    tf = str(timeframe or "").strip()
    if tf and tf in normalized["overrides"]:
        return dict(normalized["overrides"][tf])
    return dict(normalized["defaults"])


def parameter_indicator_settings_signature(
    raw: Any,
    *,
    root_key: str,
    coerce_config: CoerceConfigFn,
    field_order: Sequence[str],
    timeframes: Optional[Sequence[str]] = None,
) -> Tuple[Tuple[Any, ...], ...]:
    normalized = normalize_parameter_indicator_settings(raw, root_key=root_key, coerce_config=coerce_config)
    entries = [("default", *[int(normalized["defaults"][field_name]) for field_name in field_order])]
    if timeframes:
        seen = set()
        ordered = []
        for raw_tf in timeframes:
            tf = str(raw_tf or "").strip()
            if not tf or tf in seen:
                continue
            seen.add(tf)
            ordered.append(tf)
        ordered.sort(key=lambda item: (ordered_timeframe_key(item), item))
    else:
        ordered = sorted(normalized["overrides"].keys(), key=lambda item: (ordered_timeframe_key(item), item))
    for tf in ordered:
        config = resolve_parameter_indicator_settings(
            normalized,
            root_key=root_key,
            coerce_config=coerce_config,
            timeframe=tf,
        )
        entries.append((str(tf), *[int(config[field_name]) for field_name in field_order]))
    return tuple(entries)
