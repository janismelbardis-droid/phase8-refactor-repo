from __future__ import annotations

"""Constants shared across the app.

This file was split from the original monolithic script:
  backtest_ready_v4_23_trade_inspector_v9_adx_di_fix_rules_FIXED.py

Behavior is intended to remain identical; only code organization changed.
"""

C_GREEN = "#0ecb81"
C_RED   = "#f6465d"
C_DIM   = "#8b949e"
C_AMBER = "#f3ba2f"
C_TEXT  = "#e6edf3"
C_BG_APP = "#0b1016"
C_BG_PANEL = "#11161d"
C_BG_ELEVATED = "#161b22"
C_BG_INPUT = "#0f141a"
C_BG_OK = "#0f2d22"
C_BG_NO = "#381a22"
C_BG_NEU = "#161b22"
C_BORDER_ACTIVE = "#58a6ff"
C_BORDER_IDLE = "#30363d"
DOT_CHAR = "●"
ALL_TIMEFRAMES = ["1m", "5m", "15m", "30m", "1h"]  # 30m -> 1h
PPO_FAST = 12
PPO_SLOW = 26
PPO_SMOOTH = 2
MACD_FAST = 12
MACD_SLOW = 26
MACD_SIGNAL = 9
ADX_LEN = 14  # Wilder/TradingView default
ATR_LEN = 14  # TradingView ATR default (ta.atr uses ta.rma(tr, len))
FRAMA_LEN = 26
FRAMA_BANDS_DISTANCE = 1.5
VIDYA_LENGTH = 10
VIDYA_MOMENTUM = 20
VIDYA_BAND_DISTANCE = 2.0
VIDYA_PIVOT_LEFT = 3
VIDYA_PIVOT_RIGHT = 3
RANGE_FILTER_PER = 100
RANGE_FILTER_MULT = 3.0
STOCH_RSI_SMOOTH_K = 3
STOCH_RSI_SMOOTH_D = 3
STOCH_RSI_RSI_LENGTH = 14
STOCH_RSI_STOCH_LENGTH = 14
RSI_LENGTH = 14
RSI_SMOOTHING = 3
MIN_FULL_CLOSES = 30
MAX_STORED_CLOSES = 9000
FAPI_BASE = "https://fapi.binance.com"
FAPI_KLINES = "/fapi/v1/klines"
FAPI_TICKER_PRICE = "/fapi/v1/ticker/price"
FAPI_MARK_KLINES = "/fapi/v1/markPriceKlines"
FAPI_INDEX_KLINES = "/fapi/v1/indexPriceKlines"
FAPI_PREMIUM_INDEX = "/fapi/v1/premiumIndex"  # markPrice + indexPrice snapshot
PRICE_SOURCES = ["LAST", "MARK", "INDEX"]          # Binance Futures
MACD_IMPLS = ["TRADINGVIEW", "TALIB"]              # Indicator math
ADX_IMPLS = ["TRADINGVIEW", "PINE_SMA_DX"]         # ADX math (TRADINGVIEW = Wilder/RMA(DX), PINE_SMA_DX = Pine script SMA(DX))
REQUEST_TIMEOUT = 30
QUEUE_POLL_MS = 150
LIVE_COMPUTE_INTERVAL_SEC = 0.20
LIVE_UI_RENDER_MS = 500
LIVE_REST_FALLBACK_SEC = 1.0
LIVE_WS_STALE_SEC = 3.0
LIVE_PREFILL_BARS = {
    "1m": 7000,
    "5m": 3500,
    "15m": 2000,
    "30m": 1700,
    "1h": 1400,
}

# How many CLOSED bars per timeframe to seed into the UI after prefill.
# This controls how far back STATE Bars-Ago can look immediately in LIVE mode.
LIVE_SEED_CLOSED_BARS = 1024
BINANCE_KLINE_MAX_LIMIT_TRY = 1500
RULE_TABS = ["Long Entry", "Long Exit", "Short Entry", "Short Exit"]
# Backtest modes shown in the UI.
#
# "Bar Close (1m candles)": evaluate rules every 1m close using multi-timeframe indicator *previews*
# (higher-TF MACD/PPO use forming-close semantics, matching LIVE by default).
#
# "Bar Close (1m, HTF closed only)": evaluate rules every 1m close, but higher-TF snapshots only
# update when that higher-TF candle actually closes (forward-filled between closes).
#
# "Tick (aggTrades)": evaluate on each aggTrade tick (heavy, but closest to real-time execution).
# Backtest mode labels (strings are part of preset files; keep stable or support aliases).
BACKTEST_STEP_TFS = ["1m", "5m", "15m"]  # Base timeframe for backtest evaluation steps

BACKTEST_MODE_BAR_1M = "Bar Close (1m candles)"
# Alias requested: treat higher-TF indicators as "forming" (updated each 1m close), i.e. mark-to-market.
BACKTEST_MODE_BAR_1M_HTF_FORMING = "Bar Close (1m, HTF forming)"
# Closed-only variant: higher-TF indicator columns only update on TF closes (more conservative; no intrabar HTF repaint).
BACKTEST_MODE_BAR_1M_HTF_CLOSED_ONLY = "Bar Close (1m, HTF closed only)"
BACKTEST_MODE_TICK = "Tick (aggTrades)"

BACKTEST_MODES = [
    BACKTEST_MODE_BAR_1M,
    BACKTEST_MODE_BAR_1M_HTF_FORMING,
    BACKTEST_MODE_BAR_1M_HTF_CLOSED_ONLY,
    BACKTEST_MODE_TICK,
]

# User-facing conceptual mapping for the backtesting foundation.
# Keep BACKTEST_MODE_* strings stable for backwards compatibility with presets,
# and surface the clearer labels through the UI.
BACKTEST_MODE_EXECUTION_FOUNDATION = {
    BACKTEST_MODE_BAR_1M: "OHLCV Backtest",
    BACKTEST_MODE_BAR_1M_HTF_FORMING: "OHLCV Backtest",
    BACKTEST_MODE_BAR_1M_HTF_CLOSED_ONLY: "OHLCV Backtest",
    BACKTEST_MODE_TICK: "AggTrade Backtest",
}

BACKTEST_MODE_SIGNAL_BEHAVIOR = {
    BACKTEST_MODE_BAR_1M: "1m closed-bar evaluation with the standard 1m candle path",
    BACKTEST_MODE_BAR_1M_HTF_FORMING: "1m closed-bar evaluation with HTF preview/forming updates",
    BACKTEST_MODE_BAR_1M_HTF_CLOSED_ONLY: "1m closed-bar evaluation with HTF closed-only snapshots",
    BACKTEST_MODE_TICK: "bar-driven signals with aggregate-trade execution sequence",
}

BACKTEST_MODE_SUMMARY = {
    BACKTEST_MODE_BAR_1M: "Fast OHLCV approximation. Signals are evaluated on closed bars and execution uses the OHLCV bar path.",
    BACKTEST_MODE_BAR_1M_HTF_FORMING: "Fast OHLCV approximation with higher-timeframe forming/preview behavior updated on each 1m close.",
    BACKTEST_MODE_BAR_1M_HTF_CLOSED_ONLY: "Fast OHLCV approximation with higher-timeframe snapshots updating only when that higher timeframe actually closes.",
    BACKTEST_MODE_TICK: "Realistic AggTrade path. Signals still come from bars, but entries and exits use the aggregate-trade sequence.",
}
BINANCE_FAPI_BASE = "https://fapi.binance.com"
AGGTRADES_DAILY_SUBDIR = "aggtrades_futures_daily"
INDICATOR_CACHE_VERSION = 12
INDICATOR_CACHE_SUBDIR = "indicator_streams"
