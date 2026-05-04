from __future__ import annotations

"""Presentation helpers extracted from the Tk UI shell.

These helpers are intentionally pure or near-pure so Phase 1 can reduce
`VisualApp` complexity without changing UI behavior.
"""

from typing import Any, Dict, Optional, Tuple

import numpy as np

from ..constants import C_DIM, C_GREEN, C_RED, C_TEXT
from ..utils_time import interval_to_ms


def color_from_bool(flag: Any) -> str:
    return C_GREEN if bool(flag) else C_DIM


def market_state_palette(label: Any) -> Tuple[str, str, str]:
    s = str(label or "").upper().strip()
    if s.startswith("BULL_"):
        return (C_GREEN, "#eafff5", "#a7e8ca")
    if s.startswith("BEAR_"):
        return (C_RED, "#fff0f2", "#f3b3bb")
    if s == "COMPRESSION":
        return ("#22577a", "#eef6fb", "#bfd7ea")
    if s == "TRANSITION":
        return ("#9a6700", "#fff8e6", "#efd48a")
    return (C_DIM, "#f4f4f4", "#d4d4d4")


def market_state_playbook(label: Any) -> str:
    s = str(label or "").upper().strip()
    mapping = {
        "BULL_EXPANSION": "Playbook: breakout or continuation longs.",
        "BULL_PULLBACK": "Playbook: long pullback / re-confirmation only.",
        "BULL_EXHAUSTION": "Playbook: protect gains; avoid fresh chase longs.",
        "BEAR_EXPANSION": "Playbook: breakout or continuation shorts.",
        "BEAR_PULLBACK": "Playbook: short pullback / re-confirmation only.",
        "BEAR_EXHAUSTION": "Playbook: protect gains; avoid fresh chase shorts.",
        "TRANSITION": "Playbook: wait for alignment; reduce aggression.",
        "COMPRESSION": "Playbook: no-trade / wait for expansion.",
    }
    return mapping.get(s, "Waiting for data.")


def market_state_thesis(label: Any) -> str:
    s = str(label or "").upper().strip()
    mapping = {
        "BULL_EXPANSION": "Directional alignment is bullish and still pressing. Momentum and structure are supportive.",
        "BULL_PULLBACK": "Higher-timeframe bias remains bullish, but the move is cooling. Look for a controlled long pullback rather than a chase.",
        "BULL_EXHAUSTION": "Bullish bias still exists on paper, but impulse quality is fading. New entries should be selective.",
        "BEAR_EXPANSION": "Directional alignment is bearish and still pressing. Momentum and structure are supportive for shorts.",
        "BEAR_PULLBACK": "Higher-timeframe bias remains bearish, but the move is cooling. Look for a controlled short pullback rather than a chase.",
        "BEAR_EXHAUSTION": "Bearish bias still exists on paper, but impulse quality is fading. New entries should be selective.",
        "TRANSITION": "Inputs are mixed. Bias is unstable, so the app should favor patience over prediction.",
        "COMPRESSION": "Range and momentum are compressed. This is a low-edge zone until expansion returns.",
    }
    return mapping.get(s, "No market-state thesis available yet.")


def range_event_age(range_event_last_bar_ms: Optional[Dict[str, Dict[str, Any]]], tf: str, current_bar_ms: Optional[int], side: str) -> Optional[int]:
    try:
        cur = int(current_bar_ms) if current_bar_ms is not None else None
    except Exception:
        cur = None
    if cur is None:
        return None
    try:
        event_map = dict(range_event_last_bar_ms or {})
        last = event_map.get(str(tf), {}).get(str(side).lower())
        if last is None:
            return None
        last = int(last)
    except Exception:
        return None
    try:
        tf_ms = int(interval_to_ms(tf))
        if tf_ms <= 0:
            return None
        return int(max(0, (cur - last) // tf_ms))
    except Exception:
        return None


def format_range_event_age_cell(range_event_last_bar_ms: Optional[Dict[str, Dict[str, Any]]], tf: str, current_bar_ms: Optional[int], side: str, fired_now: bool) -> Tuple[str, str]:
    side_norm = str(side).lower()
    if bool(fired_now):
        return ("0", C_GREEN if side_norm == "buy" else C_RED)
    age = range_event_age(range_event_last_bar_ms, tf, current_bar_ms, side)
    if age is None:
        return ("-", C_DIM)
    if int(age) == 0:
        return ("0", C_GREEN if side_norm == "buy" else C_RED)
    return (str(int(age)), C_TEXT if int(age) <= 9999 else C_DIM)


def safe_label_text(value: Any, default: str = "-") -> str:
    try:
        text = str(value)
    except Exception:
        return default
    return text if text.strip() else default


def finite_number_text(value: Any, fmt: str = ".6f", default: str = "-") -> str:
    try:
        fv = float(value)
        if np.isfinite(fv):
            return format(fv, fmt)
    except Exception:
        pass
    return default
