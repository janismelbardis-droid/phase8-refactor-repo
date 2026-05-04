from __future__ import annotations

from typing import Any, Dict, Optional

import math

TAKER_BIAS_NEUTRAL_DELTA_PCT = 5.0


def _as_float(value: Any) -> Optional[float]:
    try:
        num = float(value)
    except Exception:
        return None
    if not math.isfinite(num):
        return None
    return num


def _as_int(value: Any) -> Optional[int]:
    try:
        num = int(float(value))
    except Exception:
        return None
    return num


def compute_taker_bias_payload(
    volume: Any,
    taker_buy_volume: Any,
    trade_count: Any = None,
) -> Dict[str, Any]:
    total_volume = _as_float(volume)
    buy_volume = _as_float(taker_buy_volume)
    trades = _as_int(trade_count)

    if total_volume is None or total_volume <= 0.0:
        return {
            "taker_buy_volume": None,
            "taker_sell_volume": None,
            "taker_delta_volume": None,
            "taker_delta_pct": None,
            "taker_buy_share_pct": None,
            "taker_trade_count": trades,
            "taker_bias_state": None,
        }

    buy_volume = 0.0 if buy_volume is None else max(0.0, min(float(buy_volume), float(total_volume)))
    sell_volume = max(0.0, float(total_volume) - float(buy_volume))
    delta_volume = float(buy_volume) - float(sell_volume)
    delta_pct = (float(delta_volume) / float(total_volume)) * 100.0 if total_volume > 0.0 else None
    buy_share_pct = (float(buy_volume) / float(total_volume)) * 100.0 if total_volume > 0.0 else None

    state = "NEUTRAL"
    if delta_pct is not None:
        if delta_pct > TAKER_BIAS_NEUTRAL_DELTA_PCT:
            state = "BUY"
        elif delta_pct < -TAKER_BIAS_NEUTRAL_DELTA_PCT:
            state = "SELL"

    return {
        "taker_buy_volume": float(buy_volume),
        "taker_sell_volume": float(sell_volume),
        "taker_delta_volume": float(delta_volume),
        "taker_delta_pct": float(delta_pct) if delta_pct is not None else None,
        "taker_buy_share_pct": float(buy_share_pct) if buy_share_pct is not None else None,
        "taker_trade_count": trades,
        "taker_bias_state": state,
    }
