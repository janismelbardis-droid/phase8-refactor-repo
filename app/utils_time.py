from __future__ import annotations

"""Time helpers (UTC parsing, interval conversions, warmup sizing)."""

from typing import Any, List, Optional

import numpy as np
import pandas as pd

from .constants import *
from .strategy_requirements import (
    FAMILY_ADX,
    FAMILY_EMA,
    FAMILY_ATR,
    FAMILY_FRAMA,
    FAMILY_MACD_LINES,
    FAMILY_MACD_STATES,
    FAMILY_MARKET_AUG,
    FAMILY_OHLCV,
    FAMILY_RSI,
    FAMILY_RANGE_FILTER,
    FAMILY_STOCH_RSI,
    FAMILY_TAKER_BIAS,
    FAMILY_VIDYA,
    normalize_required_families,
)
from .ema_settings import ema_required_warmup_bars
from .rsi_settings import rsi_required_warmup_bars

def utc_now_floor_min() -> "pd.Timestamp":
    # Avoid tz_localize on already tz-aware timestamps
    return pd.Timestamp.now(tz="UTC").floor("min")

def parse_utc(s: str) -> "pd.Timestamp":
    s = s.strip()
    if not s:
        raise ValueError("Empty datetime")
    ts = pd.Timestamp(s)
    if ts.tzinfo is None:
        ts = ts.tz_localize("UTC")
    else:
        ts = ts.tz_convert("UTC")
    return ts

def interval_to_ms(tf: str) -> int:
    tf = tf.strip()
    if tf.endswith("m"):
        return int(tf[:-1]) * 60_000
    if tf.endswith("h"):
        return int(tf[:-1]) * 3_600_000
    if tf.endswith("d"):
        return int(tf[:-1]) * 86_400_000
    raise ValueError(f"Unsupported interval: {tf}")

def datetime_like_to_epoch_ms(values: Any) -> "np.ndarray":
    """Return UTC epoch-millisecond values for a datetime-like Series/Index.

    This preserves the existing integer conversion semantics used by the backtest
    engine while avoiding deprecated ``Series.view`` / ``DatetimeIndex.view``
    calls on newer pandas versions. ``NaT`` continues to round-trip to the same
    sentinel-based integer value as before.
    """
    try:
        if isinstance(values, pd.Series):
            ns = values.astype("int64", copy=False).to_numpy(dtype=np.int64, copy=False)
        elif isinstance(values, pd.DatetimeIndex):
            ns = values.astype("int64", copy=False).to_numpy(dtype=np.int64, copy=False)
        else:
            coerced = pd.to_datetime(values, utc=True)
            if isinstance(coerced, pd.Series):
                ns = coerced.astype("int64", copy=False).to_numpy(dtype=np.int64, copy=False)
            elif isinstance(coerced, pd.DatetimeIndex):
                ns = coerced.astype("int64", copy=False).to_numpy(dtype=np.int64, copy=False)
            else:
                ns = np.asarray(coerced, dtype="datetime64[ns]").astype(np.int64, copy=False)
        return (ns // 1_000_000).astype(np.int64, copy=False)
    except Exception:
        return np.array([int(pd.Timestamp(x).value // 1_000_000) for x in values], dtype=np.int64)


def required_warmup_minutes(
    timeframes: List[str],
    required_fields: Optional[Any] = None,
    *,
    ema_settings: Optional[Any] = None,
    rsi_settings: Optional[Any] = None,
) -> int:
    """Compute a conservative warmup window in minutes.

    When ``required_fields`` is omitted, we preserve the legacy behavior and size for the full
    built-in indicator universe. When it is provided, we derive the warmup from the actual
    indicator families needed by the active strategy/request.
    """
    try:
        tfs = [tf for tf in (timeframes or []) if isinstance(tf, str) and tf.strip()]
    except Exception:
        tfs = []
    if not tfs:
        return 0

    fams = normalize_required_families(required_fields, include_defaults=True)

    req_bars = 0
    if FAMILY_MACD_LINES in fams:
        req_bars = max(req_bars, int(max(MACD_SLOW + MACD_SIGNAL - 1, MIN_FULL_CLOSES)))
    if FAMILY_MACD_STATES in fams:
        req_bars = max(req_bars, int(max(PPO_SLOW + PPO_SMOOTH, MIN_FULL_CLOSES)))
    if FAMILY_ADX in fams:
        req_bars = max(req_bars, int(max(MIN_FULL_CLOSES, ADX_LEN * 3)))
    if FAMILY_ATR in fams:
        req_bars = max(req_bars, int(max(MIN_FULL_CLOSES, ATR_LEN * 3)))
    if FAMILY_EMA in fams:
        req_bars = max(req_bars, int(max(1, ema_required_warmup_bars(ema_settings, timeframes=tfs))))
    if FAMILY_RSI in fams:
        req_bars = max(req_bars, int(max(MIN_FULL_CLOSES, rsi_required_warmup_bars(rsi_settings if rsi_settings is not None else ema_settings, timeframes=tfs))))
    if FAMILY_STOCH_RSI in fams:
        req_bars = max(
            req_bars,
            int(
                max(
                    MIN_FULL_CLOSES,
                    STOCH_RSI_RSI_LENGTH + STOCH_RSI_STOCH_LENGTH + STOCH_RSI_SMOOTH_K + STOCH_RSI_SMOOTH_D + 8,
                )
            ),
        )
    if FAMILY_TAKER_BIAS in fams:
        req_bars = max(req_bars, 1)
    if FAMILY_RANGE_FILTER in fams:
        req_bars = max(req_bars, int(max(400, int(RANGE_FILTER_PER) * 4)))
    if FAMILY_FRAMA in fams:
        req_bars = max(req_bars, int(max(250, 200 + int(FRAMA_LEN) + 8)))
    if FAMILY_VIDYA in fams:
        req_bars = max(req_bars, int(max(250, 200 + int(VIDYA_LENGTH) + int(VIDYA_MOMENTUM) + 8)))
    if FAMILY_MARKET_AUG in fams:
        req_bars = max(req_bars, int(max(250, 200 + int(VIDYA_LENGTH) + int(VIDYA_MOMENTUM) + 8, 200 + int(FRAMA_LEN) + 8, int(RANGE_FILTER_PER) * 4)))
    if FAMILY_OHLCV in fams and req_bars <= 0:
        req_bars = max(req_bars, 1)

    max_req = 0
    for tf in tfs:
        try:
            mins = int(interval_to_ms(tf) // 60_000)
            max_req = max(max_req, int(req_bars * mins))
        except Exception:
            continue

    if max_req > 0:
        max_req += int(max(15, min(240, max_req * 0.05)))
    return int(max_req)
