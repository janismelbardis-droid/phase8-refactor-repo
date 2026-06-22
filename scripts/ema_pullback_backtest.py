"""Standalone EMA pullback backtest. Reads only the cached parquet candles
under data_cache/ohlcv_store - no dependency on the app/ engine or presets.

Strategy (5m, BTCUSDT):
  trend   : close > EMA200 -> long bias, close < EMA200 -> short bias
  confirm : close > EMA50 (long) / close < EMA50 (short)
  trigger : prior bar dipped to/through EMA20 (low <= EMA20 for long,
            high >= EMA20 for short), current bar closes back beyond EMA20
  exit    : SL 0.3% / TP 0.6% from entry, or early exit if close crosses
            EMA50 against the position
"""
from __future__ import annotations

import sys
from pathlib import Path

import numpy as np
import pandas as pd

REPO_ROOT = Path(__file__).resolve().parents[1]

SYMBOL = "BTCUSDT"
PRICE_SOURCE = "LAST"
EMA_FAST, EMA_MID, EMA_SLOW = 20, 50, 200
SL_PCT = 0.003
TP_PCT = 0.006
FEE_PCT = 0.0004  # 0.04% per side


def load_1m_candles(start: pd.Timestamp, end: pd.Timestamp) -> pd.DataFrame:
    store_dir = REPO_ROOT / "data_cache" / "ohlcv_store" / SYMBOL / PRICE_SOURCE / "1m"
    frames = []
    for day in pd.date_range(start.normalize(), end.normalize(), freq="1D"):
        path = store_dir / f"{SYMBOL}_{PRICE_SOURCE}_{day.strftime('%Y%m%d')}.parquet"
        if path.exists():
            frames.append(pd.read_parquet(path))
    if not frames:
        raise FileNotFoundError(f"No cached candles found in {store_dir} for requested window")
    df = pd.concat(frames, ignore_index=True).drop_duplicates("open_time").sort_values("open_time")
    df["open_time"] = pd.to_datetime(df["open_time"], utc=True)
    df = df[(df["open_time"] >= start) & (df["open_time"] <= end)]
    return df.set_index("open_time")


def resample_5m(df_1m: pd.DataFrame) -> pd.DataFrame:
    agg = {"open": "first", "high": "max", "low": "min", "close": "last", "volume": "sum"}
    df_5m = df_1m.resample("5min", label="left", closed="left").agg(agg).dropna()
    return df_5m


def add_indicators(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df["ema_fast"] = df["close"].ewm(span=EMA_FAST, adjust=False).mean()
    df["ema_mid"] = df["close"].ewm(span=EMA_MID, adjust=False).mean()
    df["ema_slow"] = df["close"].ewm(span=EMA_SLOW, adjust=False).mean()
    return df


def run_backtest(df: pd.DataFrame) -> tuple[pd.DataFrame, dict]:
    trades = []
    position = None  # dict: side, entry_price, entry_time, sl, tp

    rows = df.reset_index().to_dict("records")
    for i in range(1, len(rows)):
        prev, cur = rows[i - 1], rows[i]

        if position is not None:
            exit_price, exit_reason = None, None
            if position["side"] == "long":
                if cur["low"] <= position["sl"]:
                    exit_price, exit_reason = position["sl"], "SL"
                elif cur["high"] >= position["tp"]:
                    exit_price, exit_reason = position["tp"], "TP"
                elif cur["close"] < cur["ema_mid"]:
                    exit_price, exit_reason = cur["close"], "TREND_FLIP"
            else:
                if cur["high"] >= position["sl"]:
                    exit_price, exit_reason = position["sl"], "SL"
                elif cur["low"] <= position["tp"]:
                    exit_price, exit_reason = position["tp"], "TP"
                elif cur["close"] > cur["ema_mid"]:
                    exit_price, exit_reason = cur["close"], "TREND_FLIP"

            if exit_price is not None:
                gross = (exit_price - position["entry_price"]) / position["entry_price"]
                if position["side"] == "short":
                    gross = -gross
                net = gross - 2 * FEE_PCT
                trades.append({
                    "side": position["side"],
                    "entry_time": position["entry_time"],
                    "exit_time": cur["open_time"],
                    "entry_price": position["entry_price"],
                    "exit_price": exit_price,
                    "exit_reason": exit_reason,
                    "return_pct": net,
                })
                position = None

        if position is None:
            long_trend = cur["close"] > cur["ema_slow"] and cur["close"] > cur["ema_mid"]
            long_trigger = prev["low"] <= prev["ema_fast"] and cur["close"] > cur["ema_fast"]
            short_trend = cur["close"] < cur["ema_slow"] and cur["close"] < cur["ema_mid"]
            short_trigger = prev["high"] >= prev["ema_fast"] and cur["close"] < cur["ema_fast"]

            if long_trend and long_trigger:
                entry_price = cur["close"]
                position = {
                    "side": "long", "entry_price": entry_price, "entry_time": cur["open_time"],
                    "sl": entry_price * (1 - SL_PCT), "tp": entry_price * (1 + TP_PCT),
                }
            elif short_trend and short_trigger:
                entry_price = cur["close"]
                position = {
                    "side": "short", "entry_price": entry_price, "entry_time": cur["open_time"],
                    "sl": entry_price * (1 + SL_PCT), "tp": entry_price * (1 - TP_PCT),
                }

    return pd.DataFrame(trades), {}


def summarize(trades_df: pd.DataFrame) -> dict:
    if trades_df.empty:
        return {"trades": 0}

    trades_df["equity_mult"] = (1 + trades_df["return_pct"]).cumprod()
    wins = trades_df["return_pct"] > 0
    running_max = trades_df["equity_mult"].cummax()
    drawdown = (trades_df["equity_mult"] - running_max) / running_max

    return {
        "trades": len(trades_df),
        "win_rate_pct": round(100 * wins.mean(), 2),
        "net_return_pct": round(100 * (trades_df["equity_mult"].iloc[-1] - 1), 2),
        "avg_trade_pct": round(100 * trades_df["return_pct"].mean(), 4),
        "max_drawdown_pct": round(100 * drawdown.min(), 2),
        "sl_exits": int((trades_df["exit_reason"] == "SL").sum()),
        "tp_exits": int((trades_df["exit_reason"] == "TP").sum()),
        "trend_flip_exits": int((trades_df["exit_reason"] == "TREND_FLIP").sum()),
    }


def main() -> None:
    end = pd.Timestamp(sys.argv[2] if len(sys.argv) > 2 else "2026-06-21 23:59:00", tz="UTC")
    start = pd.Timestamp(sys.argv[1] if len(sys.argv) > 1 else "2026-05-21 23:59:00", tz="UTC")

    warmup_days = max(2, (EMA_SLOW * 5) // (60 * 24) + 1)  # bars needed for EMA200 on 5m
    df_1m = load_1m_candles(start - pd.Timedelta(days=warmup_days), end)
    df_5m = add_indicators(resample_5m(df_1m))

    trades_df, _ = run_backtest(df_5m[df_5m.index <= end])
    if not trades_df.empty:
        trades_df = trades_df[trades_df["entry_time"] >= start].reset_index(drop=True)
    stats = summarize(trades_df)

    print(f"Window: {start} -> {end}")
    print(f"5m bars in window: {len(df_5m[(df_5m.index >= start) & (df_5m.index <= end)])}")
    print("\nStats:")
    for k, v in stats.items():
        print(f"  {k}: {v}")

    if not trades_df.empty:
        out_csv = REPO_ROOT / "scripts" / "ema_pullback_trades.csv"
        trades_df.to_csv(out_csv, index=False)
        print(f"\nTrades written to {out_csv}")


if __name__ == "__main__":
    main()
