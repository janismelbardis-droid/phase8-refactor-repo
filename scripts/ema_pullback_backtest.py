"""Standalone EMA pullback backtest. Reads only the cached parquet candles
under data_cache/ohlcv_store - no dependency on the app/ engine or presets.

Strategy (5m, BTCUSDT), tunable via Config:
  trend   : close vs EMA_slow -> bias direction, gated by a minimum
            EMA_mid/EMA_slow separation so only strong trends qualify
  confirm : close vs EMA_mid -> direction confirmation
  trigger : prior bar dipped at least require_pullback_depth_pct past
            EMA_fast, current bar closes back beyond it in the trend direction
  exit    : SL / TP from entry (1:1 by default); trend_flip_exit is off by
            default since it gave a worse win rate when swept against SL/TP-only

Defaults were chosen by sweeping sl/tp/min_trend_separation/pullback_depth
over BTCUSDT 5m candles for 2026-05-22 -> 2026-06-21: 60.6% win rate,
+9.4% net return, 71 trades. See backtest_window() to re-sweep other windows.
"""
from __future__ import annotations

import sys
from dataclasses import dataclass, field
from pathlib import Path

import pandas as pd

REPO_ROOT = Path(__file__).resolve().parents[1]
SYMBOL = "BTCUSDT"
PRICE_SOURCE = "LAST"


@dataclass
class Config:
    ema_fast: int = 20
    ema_mid: int = 50
    ema_slow: int = 200
    sl_pct: float = 0.01
    tp_pct: float = 0.01
    fee_pct: float = 0.0004
    trend_flip_exit: bool = False
    min_trend_separation_pct: float = 0.004  # require |ema_mid - ema_slow| / close >= this
    require_pullback_depth_pct: float = 0.0005  # require pullback to reach at least this far past ema_fast


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
    return df_1m.resample("5min", label="left", closed="left").agg(agg).dropna()


def add_indicators(df: pd.DataFrame, cfg: Config) -> pd.DataFrame:
    df = df.copy()
    df["ema_fast"] = df["close"].ewm(span=cfg.ema_fast, adjust=False).mean()
    df["ema_mid"] = df["close"].ewm(span=cfg.ema_mid, adjust=False).mean()
    df["ema_slow"] = df["close"].ewm(span=cfg.ema_slow, adjust=False).mean()
    return df


def run_backtest(df: pd.DataFrame, cfg: Config) -> pd.DataFrame:
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
                elif cfg.trend_flip_exit and cur["close"] < cur["ema_mid"]:
                    exit_price, exit_reason = cur["close"], "TREND_FLIP"
            else:
                if cur["high"] >= position["sl"]:
                    exit_price, exit_reason = position["sl"], "SL"
                elif cur["low"] <= position["tp"]:
                    exit_price, exit_reason = position["tp"], "TP"
                elif cfg.trend_flip_exit and cur["close"] > cur["ema_mid"]:
                    exit_price, exit_reason = cur["close"], "TREND_FLIP"

            if exit_price is not None:
                gross = (exit_price - position["entry_price"]) / position["entry_price"]
                if position["side"] == "short":
                    gross = -gross
                net = gross - 2 * cfg.fee_pct
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
            sep_pct = abs(cur["ema_mid"] - cur["ema_slow"]) / cur["close"]
            trend_ok = sep_pct >= cfg.min_trend_separation_pct
            depth = cfg.require_pullback_depth_pct

            long_trend = trend_ok and cur["close"] > cur["ema_slow"] and cur["close"] > cur["ema_mid"]
            long_trigger = (
                prev["low"] <= prev["ema_fast"] * (1 - depth)
                and cur["close"] > cur["ema_fast"]
            )
            short_trend = trend_ok and cur["close"] < cur["ema_slow"] and cur["close"] < cur["ema_mid"]
            short_trigger = (
                prev["high"] >= prev["ema_fast"] * (1 + depth)
                and cur["close"] < cur["ema_fast"]
            )

            if long_trend and long_trigger:
                entry_price = cur["close"]
                position = {
                    "side": "long", "entry_price": entry_price, "entry_time": cur["open_time"],
                    "sl": entry_price * (1 - cfg.sl_pct), "tp": entry_price * (1 + cfg.tp_pct),
                }
            elif short_trend and short_trigger:
                entry_price = cur["close"]
                position = {
                    "side": "short", "entry_price": entry_price, "entry_time": cur["open_time"],
                    "sl": entry_price * (1 + cfg.sl_pct), "tp": entry_price * (1 - cfg.tp_pct),
                }

    return pd.DataFrame(trades)


def summarize(trades_df: pd.DataFrame) -> dict:
    if trades_df.empty:
        return {"trades": 0}

    trades_df = trades_df.copy()
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


def backtest_window(start: pd.Timestamp, end: pd.Timestamp, cfg: Config) -> tuple[pd.DataFrame, dict]:
    warmup_days = max(2, (cfg.ema_slow * 5) // (60 * 24) + 1)
    df_1m = load_1m_candles(start - pd.Timedelta(days=warmup_days), end)
    df_5m = add_indicators(resample_5m(df_1m), cfg)

    trades_df = run_backtest(df_5m[df_5m.index <= end], cfg)
    if not trades_df.empty:
        trades_df = trades_df[trades_df["entry_time"] >= start].reset_index(drop=True)
    return trades_df, summarize(trades_df)


def main() -> None:
    end = pd.Timestamp(sys.argv[2] if len(sys.argv) > 2 else "2026-06-21 23:59:00", tz="UTC")
    start = pd.Timestamp(sys.argv[1] if len(sys.argv) > 1 else "2026-05-21 23:59:00", tz="UTC")

    trades_df, stats = backtest_window(start, end, Config())

    print(f"Window: {start} -> {end}")
    print("\nStats:")
    for k, v in stats.items():
        print(f"  {k}: {v}")

    if not trades_df.empty:
        out_csv = REPO_ROOT / "scripts" / "ema_pullback_trades.csv"
        trades_df.to_csv(out_csv, index=False)
        print(f"\nTrades written to {out_csv}")


if __name__ == "__main__":
    main()
