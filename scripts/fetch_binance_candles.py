#!/usr/bin/env python3
"""Download Binance Futures candles for one or more intervals and save as parquet.

Run this on a machine with network access to Binance (fapi.binance.com),
then commit the resulting files under data_cache/candles/ and push.

Usage:
    python scripts/fetch_binance_candles.py
    python scripts/fetch_binance_candles.py --symbol BTCUSDT --intervals 1m,15m,1h --days 365
"""
from __future__ import annotations

import argparse
import os
import time

import pandas as pd
import requests

FAPI_BASE = "https://fapi.binance.com"
KLINES_PATH = "/fapi/v1/klines"
MAX_LIMIT = 1500

COLUMNS = [
    "open_time", "open", "high", "low", "close", "volume", "close_time",
    "quote_volume", "trade_count", "taker_buy_volume", "taker_buy_quote_volume", "ignore",
]


def _interval_to_ms(interval: str) -> int:
    unit = interval[-1]
    value = int(interval[:-1])
    mult = {"m": 60_000, "h": 3_600_000, "d": 86_400_000}[unit]
    return value * mult


def fetch_klines(symbol: str, interval: str, start_utc: pd.Timestamp, end_utc: pd.Timestamp) -> pd.DataFrame:
    step_ms = _interval_to_ms(interval) * MAX_LIMIT
    start_ms = int(start_utc.value // 1_000_000)
    end_ms = int(end_utc.value // 1_000_000)

    frames = []
    cursor = start_ms
    session = requests.Session()
    while cursor <= end_ms:
        window_end = min(cursor + step_ms, end_ms)
        params = {
            "symbol": symbol.upper(),
            "interval": interval,
            "startTime": cursor,
            "endTime": window_end,
            "limit": MAX_LIMIT,
        }
        for attempt in range(5):
            try:
                resp = session.get(FAPI_BASE + KLINES_PATH, params=params, timeout=15)
                resp.raise_for_status()
                rows = resp.json()
                break
            except Exception as exc:
                if attempt == 4:
                    raise
                time.sleep(2 ** attempt)
        if not rows:
            cursor = window_end + 1
            continue
        frames.append(pd.DataFrame(rows, columns=COLUMNS))
        last_open = int(rows[-1][0])
        cursor = last_open + _interval_to_ms(interval)
        time.sleep(0.25)  # stay well under Binance rate limits

    if not frames:
        return pd.DataFrame(columns=COLUMNS)

    df = pd.concat(frames, ignore_index=True).drop_duplicates(subset=["open_time"])
    df["open_time"] = pd.to_datetime(df["open_time"], unit="ms", utc=True)
    df["close_time"] = pd.to_datetime(df["close_time"], unit="ms", utc=True)
    for col in ["open", "high", "low", "close", "volume", "quote_volume", "taker_buy_volume", "taker_buy_quote_volume"]:
        df[col] = pd.to_numeric(df[col], errors="coerce")
    df["trade_count"] = pd.to_numeric(df["trade_count"], errors="coerce").astype("Int64")
    df = df.drop(columns=["ignore"]).sort_values("open_time").reset_index(drop=True)
    return df


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--symbol", default="BTCUSDT")
    parser.add_argument("--intervals", default="1m,15m,1h", help="Comma-separated Binance intervals")
    parser.add_argument("--days", type=int, default=365)
    parser.add_argument("--out-dir", default=os.path.join("data_cache", "candles"))
    args = parser.parse_args()

    end_utc = pd.Timestamp.now(tz="UTC").floor("min")
    start_utc = end_utc - pd.Timedelta(days=args.days)
    os.makedirs(args.out_dir, exist_ok=True)

    for interval in [i.strip() for i in args.intervals.split(",") if i.strip()]:
        print(f"Fetching {args.symbol} {interval} from {start_utc} to {end_utc} ...")
        df = fetch_klines(args.symbol, interval, start_utc, end_utc)
        fname = (
            f"{args.symbol.upper()}_{interval}_"
            f"{start_utc.strftime('%Y%m%d')}_{end_utc.strftime('%Y%m%d')}.parquet"
        )
        out_path = os.path.join(args.out_dir, fname)
        df.to_parquet(out_path, index=False)
        print(f"  -> {len(df)} rows saved to {out_path}")


if __name__ == "__main__":
    main()
