#!/usr/bin/env python3
"""Download BTCUSDT 1m candle history and store it compactly for git.

Why this script exists
----------------------
The live Binance REST API (fapi.binance.com) is geo-blocked in some
environments (HTTP 451), so it cannot be used everywhere. Binance's public
historical archive ``data.binance.vision`` (served via CloudFront) is reachable
from both the cloud session and a normal local machine, and it is far cheaper
for bulk history (one zip per month/day instead of thousands of REST calls).

This script downloads candle history (any timeframe) from a start date up to
"now" and writes it as compact **yearly parquet files** (zstd) that are small
enough to commit to git.

Typical use (run on YOUR machine, then push):

    python tools/fetch_candle_history.py                              # 1m (default)
    python tools/fetch_candle_history.py --timeframes 1m,5m,15m,1h,4h --push

Restore the compact files into the app's data_cache layout so the desktop app
and backtests can read them:

    python tools/fetch_candle_history.py restore

Output layout (git-tracked):

    market_data/<SYMBOL>/<PRICE_SOURCE>/1m/<SYMBOL>_<PRICE_SOURCE>_1m_<YEAR>.parquet
"""
from __future__ import annotations

import argparse
import calendar
import datetime as dt
import io
import os
import subprocess
import time
import urllib.error
import urllib.request
import zipfile
from pathlib import Path
from typing import List, Optional

import pandas as pd

REPO_ROOT = Path(__file__).resolve().parents[1]

VISION_BASE = "https://data.binance.vision/data/futures/um"

# Full schema (matches app.data_binance.KLINE_COLUMNS so the restored daily files
# drop straight into the existing reader).
KLINE_COLUMNS = [
    "open_time", "close_time", "open", "high", "low", "close", "volume",
    "quote_volume", "trade_count", "taker_buy_volume", "taker_buy_quote_volume",
]

# Compact (default) schema: just the core OHLCV. ~2x smaller on disk. close_time
# and the extra volume columns are reconstructed/zero-filled during `restore`,
# which matches how the app already treats those extras (optional, default 0.0).
COMPACT_COLUMNS = ["open_time", "open", "high", "low", "close", "volume"]
_EXTRA_COLUMNS = ["quote_volume", "trade_count", "taker_buy_volume", "taker_buy_quote_volume"]

# Raw Binance-vision CSV column order (12 columns, last is "ignore").
_RAW_COLUMNS = [
    "open_time", "open", "high", "low", "close", "volume", "close_time",
    "quote_volume", "trade_count", "taker_buy_volume", "taker_buy_quote_volume",
    "ignore",
]


def _log(msg: str) -> None:
    print(msg, flush=True)


def _rel(path: Path) -> str:
    """Path relative to the repo root when possible, else the path as-is."""
    try:
        return str(path.relative_to(REPO_ROOT))
    except ValueError:
        return str(path)


# --------------------------------------------------------------------------- #
# download helpers
# --------------------------------------------------------------------------- #
def _http_get(url: str, timeout: int = 60, attempts: int = 5) -> Optional[bytes]:
    """GET bytes. Returns None on 404, raises on other persistent failures."""
    delay = 1.0
    last_exc: Optional[BaseException] = None
    for attempt in range(1, attempts + 1):
        try:
            req = urllib.request.Request(url, headers={"User-Agent": "candle-history/1.0"})
            with urllib.request.urlopen(req, timeout=timeout) as resp:
                return resp.read()
        except urllib.error.HTTPError as exc:
            if exc.code == 404:
                return None
            last_exc = exc
        except Exception as exc:  # noqa: BLE001 - network is best-effort
            last_exc = exc
        if attempt < attempts:
            time.sleep(min(20.0, delay))
            delay *= 2
    raise RuntimeError(f"GET failed after {attempts} attempts: {url} ({last_exc})")


def _cached_zip(url: str, work_dir: Path) -> Optional[bytes]:
    """Return the zip bytes, caching them under work_dir so re-runs are incremental."""
    name = url.rsplit("/", 1)[-1]
    cache_path = work_dir / name
    if cache_path.exists() and cache_path.stat().st_size > 0:
        return cache_path.read_bytes()
    data = _http_get(url)
    if data is None:
        return None
    tmp = cache_path.with_suffix(cache_path.suffix + ".tmp")
    tmp.write_bytes(data)
    os.replace(tmp, cache_path)
    return data


def _parse_zip_csv(blob: bytes) -> pd.DataFrame:
    with zipfile.ZipFile(io.BytesIO(blob)) as zf:
        csv_name = zf.namelist()[0]
        with zf.open(csv_name) as fh:
            raw = pd.read_csv(fh, header=None, names=_RAW_COLUMNS)
    # Newer archives prepend a header row; coercing open_time to numeric drops it.
    raw["open_time"] = pd.to_numeric(raw["open_time"], errors="coerce")
    raw = raw.dropna(subset=["open_time"]).reset_index(drop=True)
    if raw.empty:
        return pd.DataFrame(columns=KLINE_COLUMNS)

    # Binance switched archive timestamps from ms to microseconds in 2025.
    # Detect per-chunk by magnitude (ms ~ 1e12-1e13, us ~ 1e15-1e16).
    unit = "us" if raw["open_time"].max() >= 1e15 else "ms"
    out = pd.DataFrame()
    out["open_time"] = pd.to_datetime(raw["open_time"].astype("int64"), unit=unit, utc=True)
    out["close_time"] = pd.to_datetime(
        pd.to_numeric(raw["close_time"], errors="coerce").astype("int64"), unit=unit, utc=True
    )
    for col in ("open", "high", "low", "close", "volume", "quote_volume",
                "trade_count", "taker_buy_volume", "taker_buy_quote_volume"):
        out[col] = pd.to_numeric(raw[col], errors="coerce")
    return out[KLINE_COLUMNS]


def _normalize(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame(columns=KLINE_COLUMNS)
    out = df.dropna(subset=["open_time", "open", "high", "low", "close"]).copy()
    out = out.drop_duplicates(subset=["open_time"], keep="last")
    out = out.sort_values("open_time").reset_index(drop=True)
    return out


# --------------------------------------------------------------------------- #
# month / range fetching
# --------------------------------------------------------------------------- #
def _fetch_month(symbol: str, interval: str, year: int, month: int, today: dt.date, work_dir: Path) -> pd.DataFrame:
    """Fetch one calendar month of `interval` klines. Prefer the monthly zip; fall back to daily zips."""
    pieces: List[pd.DataFrame] = []
    monthly_url = f"{VISION_BASE}/monthly/klines/{symbol}/{interval}/{symbol}-{interval}-{year:04d}-{month:02d}.zip"
    blob = _cached_zip(monthly_url, work_dir)
    if blob is not None:
        _log(f"  monthly {interval} {year:04d}-{month:02d}")
        pieces.append(_parse_zip_csv(blob))
    else:
        # No monthly archive yet (current/most-recent month): use daily files.
        days_in_month = calendar.monthrange(year, month)[1]
        for day in range(1, days_in_month + 1):
            d = dt.date(year, month, day)
            if d >= today:  # today's archive is not published yet
                break
            daily_url = f"{VISION_BASE}/daily/klines/{symbol}/{interval}/{symbol}-{interval}-{d.isoformat()}.zip"
            dblob = _cached_zip(daily_url, work_dir)
            if dblob is None:
                continue
            _log(f"  daily {interval} {d.isoformat()}")
            pieces.append(_parse_zip_csv(dblob))
    if not pieces:
        return pd.DataFrame(columns=KLINE_COLUMNS)
    return _normalize(pd.concat(pieces, ignore_index=True))


def fetch(symbol: str, price_source: str, interval: str, start: dt.date, work_dir: Path,
          out_root: Path, full: bool = False) -> List[Path]:
    today = dt.datetime.now(dt.timezone.utc).date()
    out_dir = out_root / symbol / price_source / interval
    out_dir.mkdir(parents=True, exist_ok=True)
    work_dir.mkdir(parents=True, exist_ok=True)
    keep = KLINE_COLUMNS if full else COMPACT_COLUMNS

    written: List[Path] = []
    for year in range(start.year, today.year + 1):
        first_month = start.month if year == start.year else 1
        last_month = today.month if year == today.year else 12
        year_pieces: List[pd.DataFrame] = []
        for month in range(first_month, last_month + 1):
            year_pieces.append(_fetch_month(symbol, interval, year, month, today, work_dir))
        year_df = _normalize(pd.concat(year_pieces, ignore_index=True)) if year_pieces else pd.DataFrame(columns=KLINE_COLUMNS)
        if year_df.empty:
            _log(f"{interval} {year}: no data")
            continue
        out_path = out_dir / f"{symbol}_{price_source}_{interval}_{year}.parquet"
        year_df[keep].to_parquet(out_path, index=False, compression="zstd")
        size_mb = out_path.stat().st_size / 1e6
        _log(f"{interval} {year}: {len(year_df):,} candles -> {_rel(out_path)} ({size_mb:.1f} MB)")
        written.append(out_path)
    return written


# --------------------------------------------------------------------------- #
# restore into the app's data_cache/ohlcv_store layout
# --------------------------------------------------------------------------- #
def restore(symbol: str, price_source: str, out_root: Path, cache_dir: Path) -> int:
    src_dir = out_root / symbol / price_source / "1m"
    files = sorted(src_dir.glob(f"{symbol}_{price_source}_1m_*.parquet"))
    if not files:
        _log(f"No compact files found under {src_dir}. Run fetch first.")
        return 0
    store_dir = cache_dir / "ohlcv_store" / symbol / price_source / "1m"
    store_dir.mkdir(parents=True, exist_ok=True)
    total_days = 0
    for f in files:
        df = _normalize(pd.read_parquet(f))
        if df.empty:
            continue
        # Rebuild the full app schema from compact storage.
        if "close_time" not in df.columns:
            df["close_time"] = df["open_time"] + pd.Timedelta(milliseconds=59_999)
        for col in _EXTRA_COLUMNS:
            if col not in df.columns:
                df[col] = 0.0
        df = df[KLINE_COLUMNS]
        day = pd.to_datetime(df["open_time"], utc=True).dt.normalize()
        for day_value, group in df.groupby(day, sort=True):
            day_str = pd.to_datetime(day_value, utc=True).strftime("%Y%m%d")
            base = f"{symbol}_{price_source}_{day_str}"
            group.reset_index(drop=True).to_parquet(store_dir / f"{base}.parquet", index=False, compression="zstd")
            total_days += 1
    _log(f"Restored {total_days:,} daily files into {_rel(store_dir)}")
    return total_days


# --------------------------------------------------------------------------- #
# git push
# --------------------------------------------------------------------------- #
def _git(*args: str) -> str:
    return subprocess.run(["git", *args], cwd=REPO_ROOT, check=True,
                          capture_output=True, text=True).stdout.strip()


def git_push(paths: List[Path], message: str) -> None:
    if not paths:
        _log("Nothing to push.")
        return
    branch = _git("rev-parse", "--abbrev-ref", "HEAD")
    _git("add", *[str(p) for p in paths])
    status = _git("status", "--porcelain")
    if not status.strip():
        _log("No changes to commit.")
        return
    _git("commit", "-m", message)
    for attempt, backoff in enumerate((2, 4, 8, 16, 0), start=1):
        try:
            _git("push", "-u", "origin", branch)
            _log(f"Pushed to origin/{branch}")
            return
        except subprocess.CalledProcessError as exc:
            _log(f"push attempt {attempt} failed: {exc.stderr or exc}")
            if backoff:
                time.sleep(backoff)
    raise RuntimeError("git push failed after retries")


# --------------------------------------------------------------------------- #
# cli
# --------------------------------------------------------------------------- #
def main(argv: Optional[List[str]] = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("command", nargs="?", default="fetch", choices=["fetch", "restore"],
                        help="fetch = download history from Binance; restore = expand 1m into data_cache for the app")
    parser.add_argument("--symbol", default="BTCUSDT")
    parser.add_argument("--price-source", default="LAST", choices=["LAST", "MARK", "INDEX"])
    parser.add_argument("--start", default="2020-01-01", help="start date YYYY-MM-DD (default 2020-01-01)")
    parser.add_argument("--timeframes", default="1m",
                        help="comma-separated timeframes to download (e.g. 1m,5m,15m,1h,4h)")
    parser.add_argument("--out-dir", default=str(REPO_ROOT / "market_data"),
                        help="git-tracked output root for compact yearly parquet")
    parser.add_argument("--work-dir", default=str(REPO_ROOT / ".candle_cache"),
                        help="scratch dir for raw downloaded zips (not committed)")
    parser.add_argument("--cache-dir", default=os.environ.get("TRADE_INSPECTOR_CACHE_DIR") or str(REPO_ROOT / "data_cache"),
                        help="app data_cache dir (used by restore)")
    parser.add_argument("--full", action="store_true",
                        help="store all 11 kline columns (quote/taker volumes) instead of compact OHLCV")
    parser.add_argument("--push", action="store_true", help="git add/commit/push the generated files after fetch")
    args = parser.parse_args(argv)

    symbol = args.symbol.upper()
    price_source = args.price_source.upper()
    out_root = Path(args.out_dir)

    if args.command == "restore":
        restore(symbol, price_source, out_root, Path(args.cache_dir))
        return 0

    timeframes = [t.strip() for t in args.timeframes.split(",") if t.strip()]
    start = dt.date.fromisoformat(args.start)
    written: List[Path] = []
    for tf in timeframes:
        _log(f"Fetching {symbol} {price_source} {tf} candles from {start} to today via data.binance.vision")
        written += fetch(symbol, price_source, tf, start, Path(args.work_dir), out_root, full=args.full)
    if not written:
        _log("No files written.")
        return 1
    total_mb = sum(p.stat().st_size for p in written) / 1e6
    _log(f"Done. {len(written)} file(s), {total_mb:.1f} MB total under {_rel(out_root)}/")
    if args.push:
        git_push(written, f"Add {symbol} {price_source} {args.timeframes} candle history (downloaded from Binance)")
    else:
        _log("Review the files, then commit & push (or re-run with --push).")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
