"""Backfill a year of 1m futures candles from the data.binance.vision daily ZIP archive.

fapi.binance.com is geo-blocked from this environment, and the data-api.binance.vision
REST mirror only serves spot klines. The data.binance.vision static archive serves the
real USD-M futures daily klines and is not geo-blocked, so we use it here to seed
data_cache/ohlcv_store directly in the layout app/data_binance.py expects.
"""
from __future__ import annotations

import io
import sys
import zipfile
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pandas as pd
import requests

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT))

from app.data_binance import KLINE_COLUMNS, _write_local_1m_store  # noqa: E402

ARCHIVE_URL = "https://data.binance.vision/data/futures/um/daily/klines/{symbol}/1m/{symbol}-1m-{date}.zip"
RAW_COLUMNS = [
    "open_time", "open", "high", "low", "close", "volume", "close_time",
    "quote_volume", "trade_count", "taker_buy_volume", "taker_buy_quote_volume", "ignore",
]


def fetch_day(symbol: str, day: datetime) -> pd.DataFrame | None:
    url = ARCHIVE_URL.format(symbol=symbol, date=day.strftime("%Y-%m-%d"))
    resp = requests.get(url, timeout=30)
    if resp.status_code != 200:
        return None
    with zipfile.ZipFile(io.BytesIO(resp.content)) as zf:
        csv_name = zf.namelist()[0]
        with zf.open(csv_name) as fh:
            first_line = fh.readline()
            has_header = first_line.strip().split(b",")[0] == b"open_time"
        with zf.open(csv_name) as fh:
            df = pd.read_csv(fh, header=0 if has_header else None, names=RAW_COLUMNS)
    df["open_time"] = pd.to_datetime(df["open_time"], unit="ms", utc=True)
    df["close_time"] = pd.to_datetime(df["close_time"], unit="ms", utc=True)
    return df[KLINE_COLUMNS]


def main() -> None:
    symbol = sys.argv[1] if len(sys.argv) > 1 else "BTCUSDT"
    price_source = "LAST"
    days = int(sys.argv[2]) if len(sys.argv) > 2 else 365
    cache_dir = str(REPO_ROOT / "data_cache")

    today = datetime.now(timezone.utc).date()
    ok, missing = 0, 0
    for i in range(days, 0, -1):
        day = datetime.combine(today - timedelta(days=i), datetime.min.time(), tzinfo=timezone.utc)
        df = fetch_day(symbol, day)
        if df is None or df.empty:
            missing += 1
            print(f"[skip] {day.date()} not available")
            continue
        _write_local_1m_store(cache_dir, symbol, price_source, df)
        ok += 1
        print(f"[ok]   {day.date()} -> {len(df)} rows")

    print(f"\nDone. {ok} days written, {missing} days missing, symbol={symbol}, price_source={price_source}")


if __name__ == "__main__":
    main()
