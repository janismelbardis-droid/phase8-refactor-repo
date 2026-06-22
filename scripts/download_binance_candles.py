"""Download Binance Futures klines for a symbol/interval/date range and save as CSV.

Usage:
    python scripts/download_binance_candles.py --symbol BTCUSDT --intervals 1m,5m,15m,1h --days 365
"""
from __future__ import annotations

import argparse
import os
import time

import pandas as pd
import requests

FAPI_BASE = "https://api.binance.us"
FAPI_KLINES = "/api/v3/klines"
OUT_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "data_cache", "ohlcv_export")


def download_klines(symbol: str, interval: str, start_ms: int, end_ms: int) -> pd.DataFrame:
    sess = requests.Session()
    rows = []
    cur = start_ms
    while cur < end_ms:
        params = {"symbol": symbol, "interval": interval, "startTime": cur, "endTime": end_ms, "limit": 1500}
        for attempt in range(5):
            r = sess.get(FAPI_BASE + FAPI_KLINES, params=params, timeout=20)
            if r.status_code == 429:
                time.sleep(2.0)
                continue
            r.raise_for_status()
            break
        batch = r.json()
        if not batch:
            break
        rows.extend(batch)
        last_open = int(batch[-1][0])
        if last_open <= cur:
            break
        cur = last_open + 1
        if last_open >= end_ms:
            break
        time.sleep(0.05)

    if not rows:
        return pd.DataFrame(columns=["open_time", "open", "high", "low", "close", "volume", "close_time"])

    df = pd.DataFrame(rows, columns=[
        "open_time", "open", "high", "low", "close", "volume", "close_time",
        "quote_volume", "trade_count", "taker_buy_volume", "taker_buy_quote_volume", "ignore",
    ])
    df = df.drop(columns=["ignore"]).drop_duplicates(subset=["open_time"]).sort_values("open_time").reset_index(drop=True)
    df["open_time"] = pd.to_datetime(df["open_time"], unit="ms", utc=True)
    df["close_time"] = pd.to_datetime(df["close_time"], unit="ms", utc=True)
    for c in ["open", "high", "low", "close", "volume", "quote_volume", "trade_count", "taker_buy_volume", "taker_buy_quote_volume"]:
        df[c] = pd.to_numeric(df[c], errors="coerce")
    return df


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--symbol", default="BTCUSDT")
    parser.add_argument("--intervals", default="1m,5m,15m,1h")
    parser.add_argument("--days", type=int, default=365)
    args = parser.parse_args()

    end_utc = pd.Timestamp.now(tz="UTC").floor("min")
    start_utc = end_utc - pd.Timedelta(days=args.days)
    start_ms = int(start_utc.value // 1_000_000)
    end_ms = int(end_utc.value // 1_000_000)

    symbol_dir = os.path.join(OUT_DIR, args.symbol.upper())
    os.makedirs(symbol_dir, exist_ok=True)

    for interval in args.intervals.split(","):
        interval = interval.strip()
        print(f"Downloading {args.symbol} {interval} from {start_utc} to {end_utc} ...")
        df = download_klines(args.symbol.upper(), interval, start_ms, end_ms)
        out_path = os.path.join(symbol_dir, f"{args.symbol.upper()}_{interval}.parquet")
        df.to_parquet(out_path, index=False, compression="zstd")
        print(f"  saved {len(df)} rows -> {out_path}")


if __name__ == "__main__":
    main()
