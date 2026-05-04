from __future__ import annotations

"""Binance Futures REST + aggTrades cache utilities."""

import logging
import os
import json
import gzip
import time
import hashlib
import re
from typing import Optional, Any, Dict, Iterator, Tuple, List

import pandas as pd
import numpy as np
import requests

from .constants import *
from .utils_time import parse_utc, interval_to_ms

logger = logging.getLogger(__name__)

_SERVER_TIME_CACHE = {"ts": None, "at": 0.0}
_KLINE_CACHE_RE = re.compile(
    r"^FUTURES_(?P<symbol>[A-Z0-9]+)_1m_(?P<price_source>[A-Z]+)_(?P<start>\d{14})_(?P<end>\d{14})\.(?P<ext>parquet|csv)$"
)

LOCAL_KLINE_STORE_SUBDIR = "ohlcv_store"
EXACT_KLINE_CACHE_SUBDIR = "ohlcv_exact"
KLINE_BASE_COLUMNS = ["open_time", "close_time", "open", "high", "low", "close", "volume"]
KLINE_EXTRA_COLUMNS = ["quote_volume", "trade_count", "taker_buy_volume", "taker_buy_quote_volume"]
KLINE_COLUMNS = [*KLINE_BASE_COLUMNS, *KLINE_EXTRA_COLUMNS]


def _parse_kline_cache_name(path: str) -> Optional[Dict[str, Any]]:
    try:
        name = os.path.basename(path)
        m = _KLINE_CACHE_RE.match(name)
        if not m:
            return None
        return {
            "path": path,
            "symbol": str(m.group("symbol") or "").upper(),
            "price_source": str(m.group("price_source") or "").upper(),
            "start_utc": pd.to_datetime(m.group("start"), format="%Y%m%d%H%M%S", utc=True),
            "end_utc": pd.to_datetime(m.group("end"), format="%Y%m%d%H%M%S", utc=True),
            "ext": str(m.group("ext") or "").lower(),
        }
    except Exception:
        return None


def _read_cached_1m_frame(path: str) -> Optional["pd.DataFrame"]:
    try:
        if str(path).lower().endswith(".parquet"):
            df = pd.read_parquet(path)
        else:
            df = pd.read_csv(path)
        if df is None or df.empty:
            return pd.DataFrame(columns=KLINE_COLUMNS)
        df = df.copy()
        df["open_time"] = pd.to_datetime(df["open_time"], utc=True)
        df["close_time"] = pd.to_datetime(df["close_time"], utc=True)
        for c in ["open", "high", "low", "close", "volume", "quote_volume", "trade_count", "taker_buy_volume", "taker_buy_quote_volume"]:
            if c in df.columns:
                df[c] = pd.to_numeric(df[c], errors="coerce")
        for c in KLINE_EXTRA_COLUMNS:
            if c not in df.columns:
                df[c] = 0.0
        return df[KLINE_COLUMNS].dropna(subset=KLINE_BASE_COLUMNS[:-1]).reset_index(drop=True)
    except Exception:
        return None


def _slice_cached_1m_frame(df: "pd.DataFrame", start_utc: "pd.Timestamp", end_utc: "pd.Timestamp") -> "pd.DataFrame":
    if df is None or df.empty:
        return pd.DataFrame(columns=KLINE_COLUMNS)
    start_utc = pd.to_datetime(start_utc, utc=True)
    end_utc = pd.to_datetime(end_utc, utc=True)
    out = df.loc[(df["open_time"] >= start_utc) & (df["open_time"] <= end_utc)].copy()
    if out.empty:
        return out
    return out.reset_index(drop=True)


def _exact_1m_window_cache_key(
    symbol: str,
    start_utc: "pd.Timestamp",
    end_utc: "pd.Timestamp",
    price_source: str,
) -> str:
    return (
        f"FUTURES_{str(symbol or '').upper()}_1m_{str(price_source or 'LAST').upper()}_"
        f"{pd.to_datetime(start_utc, utc=True).strftime('%Y%m%d%H%M%S')}_"
        f"{pd.to_datetime(end_utc, utc=True).strftime('%Y%m%d%H%M%S')}"
    )


def _exact_1m_window_cache_dir(cache_dir: str, symbol: str, price_source: str) -> str:
    return os.path.join(
        str(cache_dir or "data_cache"),
        EXACT_KLINE_CACHE_SUBDIR,
        str(symbol or "").upper(),
        str(price_source or "LAST").upper(),
        "1m",
    )


def _exact_1m_window_paths(
    cache_dir: str,
    symbol: str,
    start_utc: "pd.Timestamp",
    end_utc: "pd.Timestamp",
    price_source: str,
) -> Tuple[str, str]:
    key = _exact_1m_window_cache_key(symbol, start_utc, end_utc, price_source)
    folder = _exact_1m_window_cache_dir(cache_dir, symbol, price_source)
    return (os.path.join(folder, f"{key}.parquet"), os.path.join(folder, f"{key}.csv"))


def _legacy_exact_1m_window_paths(
    cache_dir: str,
    symbol: str,
    start_utc: "pd.Timestamp",
    end_utc: "pd.Timestamp",
    price_source: str,
) -> Tuple[str, str]:
    key = _exact_1m_window_cache_key(symbol, start_utc, end_utc, price_source)
    return (os.path.join(str(cache_dir or "data_cache"), f"{key}.parquet"), os.path.join(str(cache_dir or "data_cache"), f"{key}.csv"))


def _exact_1m_cache_search_dirs(cache_dir: str, symbol: str, price_source: str) -> List[str]:
    dirs: List[str] = []
    seen: set[str] = set()
    for folder in (
        _exact_1m_window_cache_dir(cache_dir, symbol, price_source),
        str(cache_dir or "data_cache"),
    ):
        norm = os.path.abspath(folder)
        if norm in seen:
            continue
        seen.add(norm)
        dirs.append(folder)
    return dirs


def _find_covering_1m_cache(
    cache_dir: str,
    symbol: str,
    start_utc: "pd.Timestamp",
    end_utc: "pd.Timestamp",
    price_source: str,
) -> Optional[Tuple[str, "pd.Timestamp", "pd.Timestamp"]]:
    symbol = str(symbol or "").upper()
    price_source = str(price_source or "LAST").upper()
    best: Optional[Tuple[str, pd.Timestamp, pd.Timestamp]] = None
    best_span: Optional[int] = None
    for folder in _exact_1m_cache_search_dirs(cache_dir, symbol, price_source):
        try:
            names = os.listdir(folder)
        except Exception:
            continue
        for name in names:
            meta = _parse_kline_cache_name(os.path.join(folder, name))
            if not meta:
                continue
            if meta["symbol"] != symbol or meta["price_source"] != price_source:
                continue
            s = pd.to_datetime(meta["start_utc"], utc=True)
            e = pd.to_datetime(meta["end_utc"], utc=True)
            if s > start_utc or e < end_utc:
                continue
            span = int((e.value - s.value) // 1_000_000)
            if best is None or best_span is None or span < best_span:
                best = (meta["path"], s, e)
                best_span = span
    return best


def _write_cached_1m_frame(df: "pd.DataFrame", parquet_path: str, csv_path: str) -> None:
    os.makedirs(os.path.dirname(parquet_path), exist_ok=True)
    try:
        df.to_parquet(parquet_path, index=False)
    except Exception as exc:
        logger.debug("Parquet cache write failed, using CSV: %s (%s)", parquet_path, exc)
        os.makedirs(os.path.dirname(csv_path), exist_ok=True)
        df.to_csv(csv_path, index=False)


def _normalize_1m_frame(df: Optional["pd.DataFrame"]) -> "pd.DataFrame":
    if df is None:
        return pd.DataFrame(columns=KLINE_COLUMNS)
    if not isinstance(df, pd.DataFrame) or df.empty:
        return pd.DataFrame(columns=KLINE_COLUMNS)
    out = df.copy()
    out["open_time"] = pd.to_datetime(out["open_time"], utc=True, errors="coerce")
    out["close_time"] = pd.to_datetime(out["close_time"], utc=True, errors="coerce")
    for c in ["open", "high", "low", "close", "volume", "quote_volume", "trade_count", "taker_buy_volume", "taker_buy_quote_volume"]:
        if c in out.columns:
            out[c] = pd.to_numeric(out[c], errors="coerce")
    for c in KLINE_EXTRA_COLUMNS:
        if c not in out.columns:
            out[c] = 0.0
    out = out[KLINE_COLUMNS].dropna(subset=["open_time", "close_time", "open", "high", "low", "close"]).copy()
    if out.empty:
        return pd.DataFrame(columns=KLINE_COLUMNS)
    out = out.drop_duplicates(subset=["open_time"], keep="last").sort_values("open_time").reset_index(drop=True)
    if "volume" not in out.columns:
        out["volume"] = 0.0
    out["volume"] = pd.to_numeric(out["volume"], errors="coerce").fillna(0.0)
    for c in ("quote_volume", "trade_count", "taker_buy_volume", "taker_buy_quote_volume"):
        out[c] = pd.to_numeric(out.get(c), errors="coerce").fillna(0.0)
    return out


def _merge_1m_frames(*frames: Optional["pd.DataFrame"]) -> "pd.DataFrame":
    usable = [f for f in frames if isinstance(f, pd.DataFrame) and not f.empty]
    if not usable:
        return pd.DataFrame(columns=KLINE_COLUMNS)
    merged = pd.concat([_normalize_1m_frame(f) for f in usable], ignore_index=True)
    return _normalize_1m_frame(merged)


def _local_1m_store_dir(cache_dir: str, symbol: str, price_source: str) -> str:
    folder = os.path.join(str(cache_dir or "data_cache"), LOCAL_KLINE_STORE_SUBDIR, str(symbol or "").upper(), str(price_source or "LAST").upper(), "1m")
    os.makedirs(folder, exist_ok=True)
    return folder


def _iter_utc_days(start_utc: "pd.Timestamp", end_utc: "pd.Timestamp") -> Iterator["pd.Timestamp"]:
    cur = pd.to_datetime(start_utc, utc=True).normalize()
    stop = pd.to_datetime(end_utc, utc=True).normalize()
    while cur <= stop:
        yield cur
        cur = cur + pd.Timedelta(days=1)


def _local_1m_store_day_paths(cache_dir: str, symbol: str, price_source: str, day_utc: "pd.Timestamp") -> Tuple[str, str]:
    folder = _local_1m_store_dir(cache_dir, symbol, price_source)
    base = f"{str(symbol or '').upper()}_{str(price_source or 'LAST').upper()}_{pd.to_datetime(day_utc, utc=True).strftime('%Y%m%d')}"
    return (os.path.join(folder, base + '.parquet'), os.path.join(folder, base + '.csv'))


def _load_local_1m_store_window(cache_dir: str, symbol: str, start_utc: "pd.Timestamp", end_utc: "pd.Timestamp", price_source: str) -> "pd.DataFrame":
    pieces: List[pd.DataFrame] = []
    for day in _iter_utc_days(start_utc, end_utc):
        pq_path, csv_path = _local_1m_store_day_paths(cache_dir, symbol, price_source, day)
        path = pq_path if os.path.exists(pq_path) else (csv_path if os.path.exists(csv_path) else None)
        if path is None:
            continue
        df = _read_cached_1m_frame(path)
        if df is None or df.empty:
            continue
        pieces.append(df)
    merged = _merge_1m_frames(*pieces)
    return _slice_cached_1m_frame(merged, start_utc, end_utc)


def _write_local_1m_store(cache_dir: str, symbol: str, price_source: str, df: "pd.DataFrame") -> None:
    norm = _normalize_1m_frame(df)
    if norm.empty:
        return
    day_series = pd.to_datetime(norm["open_time"], utc=True).dt.normalize()
    for day_value, group in norm.groupby(day_series, sort=True):
        day = pd.to_datetime(day_value, utc=True)
        pq_path, csv_path = _local_1m_store_day_paths(cache_dir, symbol, price_source, day)
        existing = None
        if os.path.exists(pq_path):
            existing = _read_cached_1m_frame(pq_path)
        elif os.path.exists(csv_path):
            existing = _read_cached_1m_frame(csv_path)
        merged = _merge_1m_frames(existing, group)
        _write_cached_1m_frame(merged, pq_path, csv_path)


def _find_missing_1m_ranges(start_utc: "pd.Timestamp", end_utc: "pd.Timestamp", df_local: Optional["pd.DataFrame"]) -> List[Tuple["pd.Timestamp", "pd.Timestamp"]]:
    start_ts = pd.to_datetime(start_utc, utc=True).floor('min')
    end_ts = pd.to_datetime(end_utc, utc=True).floor('min')
    if end_ts < start_ts:
        return []
    expected = pd.date_range(start_ts, end_ts, freq='1min', tz='UTC')
    if len(expected) == 0:
        return []
    have: set[int] = set()
    if isinstance(df_local, pd.DataFrame) and not df_local.empty and 'open_time' in df_local.columns:
        try:
            vals = pd.to_datetime(df_local['open_time'], utc=True, errors='coerce').dropna().astype('int64').tolist()
            have = set(int(v) for v in vals)
        except Exception:
            have = set()
    missing: List[Tuple[pd.Timestamp, pd.Timestamp]] = []
    run_start: Optional[pd.Timestamp] = None
    prev_missing: Optional[pd.Timestamp] = None
    for ts in expected:
        key = int(ts.value)
        if key not in have:
            if run_start is None:
                run_start = ts
            prev_missing = ts
        elif run_start is not None and prev_missing is not None:
            missing.append((run_start, prev_missing))
            run_start = None
            prev_missing = None
    if run_start is not None and prev_missing is not None:
        missing.append((run_start, prev_missing))
    return missing


def _download_klines_1m_range(
    symbol: str,
    start_utc: "pd.Timestamp",
    end_utc: "pd.Timestamp",
    *,
    price_source: str,
    progress_cb=None,
    progress_prefix: str = "Downloading klines",
) -> "pd.DataFrame":
    price_source = (price_source or "LAST").upper()
    endpoint = FAPI_KLINES if price_source == "LAST" else (FAPI_MARK_KLINES if price_source == "MARK" else FAPI_INDEX_KLINES)
    start_ms = int(pd.to_datetime(start_utc, utc=True).timestamp() * 1000)
    end_ms = int(pd.to_datetime(end_utc, utc=True).timestamp() * 1000)
    if end_ms < start_ms:
        return pd.DataFrame(columns=KLINE_COLUMNS)

    all_rows: List[list] = []
    cur = start_ms
    limit = 1000
    sess = requests.Session()

    total_minutes = max(1, int((end_ms - start_ms) / 60_000) + 1)
    est_calls = max(1, total_minutes // 1000 + (1 if total_minutes % 1000 else 0))
    calls_done = 0

    while True:
        params = {"symbol": symbol.upper(), "interval": "1m", "startTime": cur, "endTime": end_ms, "limit": limit}
        batch = _binance_get_json_with_retries(
            sess,
            FAPI_BASE + endpoint,
            params=params,
            timeout=REQUEST_TIMEOUT,
            progress_cb=progress_cb,
            context="klines",
        )
        if not batch:
            break

        all_rows.extend(batch)
        last_open = batch[-1][0]
        cur = int(last_open) + 60_000
        calls_done += 1

        if progress_cb:
            pct = min(95, int((calls_done / max(1, est_calls)) * 95))
            progress_cb(f"{progress_prefix}... ({calls_done}/{est_calls})", pct)

        if int(last_open) >= end_ms:
            break
        time.sleep(0.03)

    if not all_rows:
        return pd.DataFrame(columns=KLINE_COLUMNS)

    rows_out: List[dict] = []
    for row in all_rows:
        try:
            ot = int(row[0])
            op = float(row[1])
            hi = float(row[2])
            lo = float(row[3])
            cl = float(row[4])
            vol = 0.0
            if len(row) > 5:
                try:
                    vol = float(row[5])
                except Exception:
                    vol = 0.0
            ct = int(row[6]) if len(row) > 6 else (ot + 60_000 - 1)
            quote_vol = float(row[7]) if len(row) > 7 else 0.0
            trade_count = float(row[8]) if len(row) > 8 else 0.0
            taker_buy_vol = float(row[9]) if len(row) > 9 else 0.0
            taker_buy_quote_vol = float(row[10]) if len(row) > 10 else 0.0
            rows_out.append(
                {
                    "open_time": ot,
                    "close_time": ct,
                    "open": op,
                    "high": hi,
                    "low": lo,
                    "close": cl,
                    "volume": vol,
                    "quote_volume": quote_vol,
                    "trade_count": trade_count,
                    "taker_buy_volume": taker_buy_vol,
                    "taker_buy_quote_volume": taker_buy_quote_vol,
                }
            )
        except Exception:
            continue

    df = pd.DataFrame(rows_out)
    if df.empty:
        return pd.DataFrame(columns=KLINE_COLUMNS)
    df["open_time"] = pd.to_datetime(df["open_time"], unit="ms", utc=True)
    df["close_time"] = pd.to_datetime(df["close_time"], unit="ms", utc=True)
    for c in ["open", "high", "low", "close", "volume", "quote_volume", "trade_count", "taker_buy_volume", "taker_buy_quote_volume"]:
        df[c] = pd.to_numeric(df[c], errors="coerce")
    return _normalize_1m_frame(df)


def _binance_get_json_with_retries(
    sess: "requests.Session",
    url: str,
    *,
    params: Dict[str, Any],
    timeout: float,
    progress_cb=None,
    context: str = "request",
    max_attempts: int = 6,
):
    delay = 0.8
    last_error: Optional[BaseException] = None
    for attempt in range(1, max_attempts + 1):
        try:
            r = sess.get(url, params=params, timeout=timeout)
            if r.status_code == 429:
                last_error = RuntimeError(f"HTTP 429 during {context}")
                if progress_cb:
                    try:
                        progress_cb(f"Rate limited (429) on {context} — backing off…", None)
                    except Exception:
                        pass
            else:
                r.raise_for_status()
                return r.json()
        except Exception as exc:
            last_error = exc
            if attempt >= max_attempts:
                raise
            if progress_cb:
                try:
                    progress_cb(f"{context.capitalize()} error ({attempt}/{max_attempts}) — retrying…", None)
                except Exception:
                    pass
        if attempt >= max_attempts:
            break
        time.sleep(min(5.0, delay))
        delay *= 1.8
    if last_error is not None:
        raise last_error
    raise RuntimeError(f"{context} failed without a response")

def binance_fapi_server_time_utc(force: bool = False, timeout: float = 5.0) -> Optional["pd.Timestamp"]:
    """Return Binance Futures server time as UTC Timestamp.
    Cached briefly to avoid repeated network calls.
    Returns None if the request fails.
    """
    try:
        now = time.time()
        if (not force) and _SERVER_TIME_CACHE["ts"] is not None and (now - float(_SERVER_TIME_CACHE["at"]) < 10.0):
            return _SERVER_TIME_CACHE["ts"]
        r = requests.get(FAPI_BASE + "/fapi/v1/time", timeout=timeout)
        r.raise_for_status()
        js = r.json()
        ms = int(js.get("serverTime"))
        ts = pd.Timestamp(ms, unit="ms", tz="UTC")
        _SERVER_TIME_CACHE["ts"] = ts
        _SERVER_TIME_CACHE["at"] = now
        return ts
    except Exception:
        return None

def fetch_recent_closes_futures(symbol: str, interval: str, bars: int, log_cb=None, price_source: str = "LAST") -> List[float]:
    """Fetch recent CLOSED candle closes from Binance Futures (pages backwards)."""
    price_source = (price_source or "LAST").upper()
    if price_source not in PRICE_SOURCES:
        price_source = "LAST"
    endpoint = FAPI_KLINES if price_source == "LAST" else (FAPI_MARK_KLINES if price_source == "MARK" else FAPI_INDEX_KLINES)

    max_limit = BINANCE_KLINE_MAX_LIMIT_TRY
    sess = requests.Session()
    closes: List[float] = []
    end_ms: Optional[int] = None
    remaining = bars
    tried_fallback = False

    while remaining > 0:
        lim = min(max_limit, remaining)
        params = {"symbol": symbol.upper(), "interval": interval, "limit": lim}
        if end_ms is not None:
            params["endTime"] = int(end_ms)

        try:
            data = _binance_get_json_with_retries(
                sess,
                FAPI_BASE + endpoint,
                params=params,
                timeout=REQUEST_TIMEOUT,
                progress_cb=(lambda msg, _pct=None: log_cb(msg)) if log_cb else None,
                context=f"{interval} prefill",
            )
        except Exception:
            if (not tried_fallback) and max_limit > 1000:
                tried_fallback = True
                max_limit = 1000
                if log_cb:
                    log_cb(f"[Live] Prefill: limit {BINANCE_KLINE_MAX_LIMIT_TRY} rejected, fallback to 1000")
                continue
            raise

        if not data:
            break
        # Keep ONLY closed candles.
        # Binance returns the currently-forming candle only in the most-recent batch (when endTime is not set).
        # When paging backwards with endTime, *all* returned candles are closed and must be kept.
        data_closed = data
        if end_ms is None and data:
            try:
                now_ms = int(time.time() * 1000)
                # kline schema: [open_time, open, high, low, close, volume, close_time, ...]
                last_close_time = int(data[-1][6])
                if last_close_time > now_ms:
                    data_closed = data[:-1]
            except Exception:
                # If schema differs or parsing fails, fall back to keeping all rows
                data_closed = data
        if not data_closed:
            break
        batch_closes = [float(x[4]) for x in data_closed]
        closes = batch_closes + closes

        first_open = int(data_closed[0][0])
        end_ms = first_open - 1

        remaining = bars - len(closes)
        if log_cb:
            log_cb(f"[Live] Prefill {interval} ({price_source}): got {len(closes)}/{bars}")

        time.sleep(0.05)

    if len(closes) > bars:
        closes = closes[-bars:]
    return closes

def fetch_klines_1m_futures(
    symbol: str,
    start_utc: "pd.Timestamp",
    end_utc: "pd.Timestamp",
    cache_dir: str = "data_cache",
    progress_cb=None,
    price_source: str = "LAST",
) -> "pd.DataFrame":
    """
    Download 1m candles for Binance Futures.

    price_source:
      - "LAST"  -> /fapi/v1/klines
      - "MARK"  -> /fapi/v1/markPriceKlines
      - "INDEX" -> /fapi/v1/indexPriceKlines
    """
    price_source = (price_source or "LAST").upper()
    if price_source not in PRICE_SOURCES:
        price_source = "LAST"

    start_utc = pd.to_datetime(start_utc, utc=True)
    end_utc = pd.to_datetime(end_utc, utc=True)
    os.makedirs(cache_dir, exist_ok=True)

    cache_path, cache_csv_path = _exact_1m_window_paths(cache_dir, symbol, start_utc, end_utc, price_source)
    legacy_cache_path, legacy_cache_csv_path = _legacy_exact_1m_window_paths(cache_dir, symbol, start_utc, end_utc, price_source)

    for path in (cache_path, cache_csv_path, legacy_cache_path, legacy_cache_csv_path):
        if not os.path.exists(path):
            continue
        try:
            if progress_cb:
                fmt = "parquet" if str(path).lower().endswith(".parquet") else "csv"
                progress_cb(f"Loaded cached data ({fmt}).", 100)
            df = _read_cached_1m_frame(path)
            if df is not None:
                if os.path.abspath(path) not in {os.path.abspath(cache_path), os.path.abspath(cache_csv_path)}:
                    try:
                        _write_cached_1m_frame(df, cache_path, cache_csv_path)
                    except Exception as exc:
                        logger.debug("Failed migrating legacy exact kline cache %s: %s", path, exc)
                return df
        except Exception as exc:
            logger.debug("Failed reading exact cache %s: %s", path, exc)

    covering = _find_covering_1m_cache(cache_dir, symbol, start_utc, end_utc, price_source)
    if covering is not None:
        cover_path, _cover_start, _cover_end = covering
        df_cover = _read_cached_1m_frame(cover_path)
        if df_cover is not None and not df_cover.empty:
            sliced = _slice_cached_1m_frame(df_cover, start_utc, end_utc)
            if not sliced.empty:
                try:
                    if progress_cb:
                        progress_cb(
                            f"Loaded covering cache {os.path.basename(cover_path)} and sliced requested window.",
                            100,
                        )
                    _write_cached_1m_frame(sliced, cache_path, cache_csv_path)
                    _write_local_1m_store(cache_dir, symbol, price_source, sliced)
                except Exception as exc:
                    logger.warning(
                        "Could not persist sliced klines from covering cache (data still returned): %s",
                        exc,
                        exc_info=True,
                    )
                return sliced

    local_df = _load_local_1m_store_window(cache_dir, symbol, start_utc, end_utc, price_source)
    missing_ranges = _find_missing_1m_ranges(start_utc, end_utc, local_df)
    try:
        expected_rows = max(1, len(pd.date_range(start_utc.floor('min'), end_utc.floor('min'), freq='1min', tz='UTC')))
    except Exception:
        expected_rows = max(1, len(local_df))
    if progress_cb and not local_df.empty:
        try:
            pct_local = int(min(100, max(0, round((len(local_df) / max(1, expected_rows)) * 100))))
            progress_cb(f"Loaded local candle store ({pct_local}% coverage).", min(50, pct_local))
        except Exception:
            pass
    if not missing_ranges:
        out = _slice_cached_1m_frame(local_df, start_utc, end_utc)
        if not out.empty:
            try:
                _write_cached_1m_frame(out, cache_path, cache_csv_path)
            except Exception as exc:
                logger.warning(
                    "Could not write exact-window cache from local candle store: %s",
                    exc,
                    exc_info=True,
                )
            if progress_cb:
                progress_cb("Loaded local candle store.", 100)
            return out

    pieces: List[pd.DataFrame] = [local_df] if isinstance(local_df, pd.DataFrame) and not local_df.empty else []
    total_missing = len(missing_ranges)
    for seg_i, (seg_start, seg_end) in enumerate(missing_ranges, start=1):
        if progress_cb:
            progress_cb(
                f"Downloading missing candle segment {seg_i}/{total_missing} from Binance ({price_source})...",
                1,
            )
        segment_df = _download_klines_1m_range(
            symbol,
            seg_start,
            seg_end,
            price_source=price_source,
            progress_cb=progress_cb,
            progress_prefix=f"Downloading missing segment {seg_i}/{total_missing}",
        )
        if segment_df is not None and not segment_df.empty:
            pieces.append(segment_df)
            try:
                _write_local_1m_store(cache_dir, symbol, price_source, segment_df)
            except Exception as exc:
                logger.warning("Could not append downloaded segment to local OHLCV store: %s", exc, exc_info=True)

    merged = _merge_1m_frames(*pieces)
    out = _slice_cached_1m_frame(merged, start_utc, end_utc)
    still_missing = _find_missing_1m_ranges(start_utc, end_utc, out)
    if still_missing:
        raise RuntimeError(f"Missing 1m candle coverage after download for {symbol} {price_source}: {still_missing[:3]}")

    _write_cached_1m_frame(out, cache_path, cache_csv_path)
    try:
        _write_local_1m_store(cache_dir, symbol, price_source, out)
    except Exception as exc:
        logger.warning("Could not update local OHLCV store after download: %s", exc, exc_info=True)
    if progress_cb:
        progress_cb("Download complete.", 100)
    return out

def fetch_recent_ohlc_futures(symbol: str, interval: str, bars: int, log_cb=None, price_source: str = "LAST") -> "pd.DataFrame":
    """Fetch recent CLOSED candles (OHLCV) from Binance Futures.

    Returns DataFrame with candle OHLCV plus Binance kline extras such as trade count
    and taker-buy volumes when the endpoint provides them.

    Volume is required by stateful indicators such as the BigBeluga VIDYA port. The older
    implementation accidentally dropped Binance's kline volume column during REST prefill,
    so live-prefilled trend-volume counters started from zero history on startup.
    """
    price_source = (price_source or "LAST").upper()
    if price_source not in PRICE_SOURCES:
        price_source = "LAST"

    # endpoint selection mirrors fetch_recent_closes_futures
    if price_source == "LAST":
        endpoint = FAPI_KLINES
    elif price_source == "MARK":
        endpoint = FAPI_MARK_KLINES
    else:
        endpoint = FAPI_INDEX_KLINES

    limit = 1500
    need = int(max(0, bars))
    if need == 0:
        return pd.DataFrame(columns=KLINE_COLUMNS)

    sess = requests.Session()
    out_rows: List[Dict[str, Any]] = []

    end_ms = None  # paginate backwards
    while need > 0:
        page = min(limit, need)
        params = {"symbol": symbol.upper(), "interval": interval, "limit": page}
        if end_ms is not None:
            params["endTime"] = int(end_ms)

        try:
            r = sess.get(FAPI_BASE + endpoint, params=params, timeout=15)
            r.raise_for_status()
            data = r.json()
        except Exception as e:
            if log_cb:
                log_cb(f"[REST] OHLC fetch error: {e}")
            break

        if not data:
            break

        # Keep ONLY closed candles.
        # Binance includes the currently-forming candle only in the most-recent batch (when endTime is not set).
        # When paging backwards with endTime, all returned candles are closed and must be kept.
        data_closed = data
        if end_ms is None and data:
            try:
                now_ms = int(time.time() * 1000)
                # kline schema: [open_time, open, high, low, close, volume, close_time, ...]
                last_close_time = int(data[-1][6])
                if last_close_time > now_ms:
                    data_closed = data[:-1]
            except Exception:
                data_closed = data
        if not data_closed:
            break

        # Binance returns oldest->newest; we are paginating backwards by endTime, so keep them and shift end_ms.
        for row in data_closed:
            try:
                ot = int(row[0])
                op = float(row[1])
                hi = float(row[2])
                lo = float(row[3])
                cl = float(row[4])
                vol = float(row[5]) if len(row) > 5 else 0.0
                ct = int(row[6]) if len(row) > 6 else (ot + interval_to_ms(interval) - 1)
                quote_vol = float(row[7]) if len(row) > 7 else 0.0
                trade_count = float(row[8]) if len(row) > 8 else 0.0
                taker_buy_vol = float(row[9]) if len(row) > 9 else 0.0
                taker_buy_quote_vol = float(row[10]) if len(row) > 10 else 0.0
                out_rows.append(
                    {
                        "open_time": ot,
                        "close_time": ct,
                        "open": op,
                        "high": hi,
                        "low": lo,
                        "close": cl,
                        "volume": vol,
                        "quote_volume": quote_vol,
                        "trade_count": trade_count,
                        "taker_buy_volume": taker_buy_vol,
                        "taker_buy_quote_volume": taker_buy_quote_vol,
                    }
                )
            except Exception:
                continue

        end_ms = int(data_closed[0][0]) - 1
        need = int(max(0, bars - len(out_rows)))

        if len(data) < page:
            break

    if not out_rows:
        return pd.DataFrame(columns=KLINE_COLUMNS)

    df = pd.DataFrame(out_rows)
    df = df.drop_duplicates(subset=["open_time"]).sort_values("open_time").reset_index(drop=True)
    df["open_time"] = pd.to_datetime(df["open_time"], unit="ms", utc=True)
    df["close_time"] = pd.to_datetime(df["close_time"], unit="ms", utc=True)
    for c in ["open", "high", "low", "close", "volume", "quote_volume", "trade_count", "taker_buy_volume", "taker_buy_quote_volume"]:
        df[c] = pd.to_numeric(df[c], errors="coerce")
    df = df.dropna().reset_index(drop=True)
    # Drop the most recent candle if it is still forming (Binance REST returns the in-progress bar).
    try:
        now_utc = pd.Timestamp.now(tz="UTC")
        df = df[df["close_time"] <= now_utc].reset_index(drop=True)
    except Exception:
        pass
    return df

def _ensure_dir(path: str):
    try:
        os.makedirs(path, exist_ok=True)
    except Exception:
        pass

def iter_aggtrades_futures(symbol: str,
                           start_utc: "pd.Timestamp",
                           end_utc: "pd.Timestamp",
                           cache_dir: str = "data_cache",
                           progress_cb=None,
                           use_cache: bool = True):
    """Iterate Binance Futures aggTrades between [start_utc, end_utc).

    Yields dicts with keys: T (ms), p (price str), q (qty str), a (aggTradeId), m (is_buyer_maker)
    Uses JSONL.GZ cache for exact (symbol,start,end) windows to avoid re-downloading.

    NOTE: Futures aggTrades timestamps are millisecond precision.
    """
    symbol = symbol.upper().strip()
    if start_utc.tzinfo is None:
        start_utc = start_utc.tz_localize("UTC")
    else:
        start_utc = start_utc.tz_convert("UTC")
    if end_utc.tzinfo is None:
        end_utc = end_utc.tz_localize("UTC")
    else:
        end_utc = end_utc.tz_convert("UTC")

    start_ms = int(start_utc.value // 1_000_000)
    end_ms = int(end_utc.value // 1_000_000)

    _ensure_dir(cache_dir)
    cache_path = os.path.join(cache_dir, f"aggtrades_{symbol}_{start_ms}_{end_ms}.jsonl.gz")

    def _emit_progress(msg: str):
        if progress_cb:
            try:
                progress_cb(msg, None)
            except Exception:
                pass

    if use_cache and os.path.isfile(cache_path):
        # The cache can occasionally be created empty (e.g., interrupted download / network error).
        # If that happens, using it would silently yield 0 ticks -> 0 trades. Guard against it.
        try:
            if os.path.getsize(cache_path) < 60:
                raise OSError("aggTrades cache too small")
        except Exception:
            pass
        else:
            _emit_progress(f"Loading aggTrades cache: {os.path.basename(cache_path)}")
            yielded = 0
            try:
                with gzip.open(cache_path, "rt", encoding="utf-8") as f:
                    for line in f:
                        line = line.strip()
                        if not line:
                            continue
                        try:
                            d = json.loads(line)
                            # guard window
                            t = int(d.get("T", 0))
                            if t < start_ms:
                                continue
                            if t >= end_ms:
                                break
                            yielded += 1
                            yield d
                        except Exception:
                            continue
            except Exception:
                yielded = 0

            if yielded > 0:
                return
            _emit_progress("aggTrades cache was empty/invalid — re-downloading…")

    # Download + stream-write cache
    _emit_progress("Downloading aggTrades (tick data)…")
    url = f"{BINANCE_FAPI_BASE}/fapi/v1/aggTrades"

    headers = {"User-Agent": "macd-tool/1.0"}
    next_from_id = None
    wrote = 0

    with gzip.open(cache_path, "wt", encoding="utf-8") as out:
        # Safety: ensure we always start at or after start_ms
        current_start_ms = start_ms

        while True:
            params = {"symbol": symbol, "limit": 1000}
            # Prefer fromId pagination once we have it (more robust than startTime when many trades share same ms)
            if next_from_id is not None:
                params["fromId"] = int(next_from_id)
            else:
                params["startTime"] = int(current_start_ms)

            # IMPORTANT (Binance Futures):
            # If you pass BOTH startTime and endTime, Binance returns HTTP 400 when the span is >= 1 hour.
            # For multi-hour/day tick backtests we therefore avoid sending endTime and stop client-side once T >= end_ms.
            try:
                r = requests.get(url, params=params, headers=headers, timeout=20)
                # Fallback: if Binance rejects a startTime-only request with 400 (rare),
                # retry with a 1-hour endTime window to satisfy the API constraint.
                if r.status_code == 400 and ("startTime" in params) and ("endTime" not in params):
                    try:
                        params2 = dict(params)
                        params2["endTime"] = int(min(end_ms, int(params2["startTime"]) + 3_599_000))
                        r = requests.get(url, params=params2, headers=headers, timeout=20)
                    except Exception:
                        pass
                if r.status_code == 429:

                    # rate limit: brief backoff
                    _emit_progress("Rate limited (429) on aggTrades — backing off briefly…")
                    time.sleep(0.8)
                    continue
                r.raise_for_status()
                data = r.json()
            except Exception as e:
                _emit_progress(f"aggTrades download error: {e} — retrying…")
                time.sleep(1.0)
                continue

            if not data:
                break

            # Ensure sorted
            try:
                data.sort(key=lambda x: int(x.get("a", 0)))
            except Exception:
                pass

            for d in data:
                try:
                    t = int(d.get("T", 0))
                except Exception:
                    continue
                if t < start_ms:
                    continue
                if t >= end_ms:
                    # stop early
                    return
                # stream write + yield
                out.write(json.dumps(d) + "\n")
                wrote += 1
                yield d

            last = data[-1]
            try:
                last_id = int(last.get("a"))
                next_from_id = last_id + 1
            except Exception:
                # fallback: advance time by last timestamp + 1ms
                try:
                    current_start_ms = int(last.get("T", current_start_ms)) + 1
                    next_from_id = None
                except Exception:
                    break

            if wrote and wrote % 200000 == 0:
                _emit_progress(f"Downloaded {wrote:,} aggTrades…")

def _aggtrades_meta_path(cache_path: str) -> str:
    if cache_path.endswith(".jsonl.gz"):
        return cache_path[:-len(".jsonl.gz")] + ".meta.json"
    return cache_path + ".meta.json"


def _aggtrades_cache_day_status(cache_dir: str, symbol: str, day_utc: "pd.Timestamp") -> Tuple[bool, str]:
    cache_path = _aggtrades_day_path(cache_dir, symbol, day_utc)
    if not os.path.isfile(cache_path):
        return False, "missing"
    meta_path = _aggtrades_meta_path(cache_path)
    if not os.path.isfile(meta_path):
        return False, "no_meta"
    try:
        with open(meta_path, "r", encoding="utf-8") as f:
            meta = json.load(f)
    except Exception:
        return False, "bad_meta"

    status = str(meta.get("status") or meta.get("state") or "").upper().strip()
    verify_status = str(meta.get("verify_status") or "").upper().strip()
    day_s = day_utc.floor("D").strftime("%Y-%m-%d")
    today_s = pd.Timestamp.now(tz="UTC").strftime("%Y-%m-%d")

    if verify_status.startswith("CORRUPT"):
        return False, verify_status.lower()
    if status == "EMPTY" or verify_status == "EMPTY":
        return False, "empty"
    if day_s == today_s:
        if status in ("OPEN", "FINAL") or verify_status in ("OPEN", "FINAL"):
            return True, status or verify_status or "open"
        return False, (status or verify_status or "unusable").lower()
    if status == "FINAL" or verify_status == "FINAL":
        return True, "final"
    return False, (status or verify_status or "unusable").lower()


def _aggtrades_day_path(cache_dir: str, symbol: str, day_utc: "pd.Timestamp") -> str:
    """Return daily cache file path for UTC day (00:00..24:00)."""
    symbol = symbol.upper().strip()
    if day_utc.tzinfo is None:
        day_utc = day_utc.tz_localize("UTC")
    else:
        day_utc = day_utc.tz_convert("UTC")
    day_utc = day_utc.floor("D")
    day_str = day_utc.strftime("%Y-%m-%d")
    folder = os.path.join(cache_dir, AGGTRADES_DAILY_SUBDIR, symbol)
    _ensure_dir(folder)
    return os.path.join(folder, f"{day_str}.jsonl.gz")

def _iter_day_range_utc(start_utc: "pd.Timestamp", end_utc: "pd.Timestamp") -> List["pd.Timestamp"]:
    """List UTC day starts that intersect [start_utc, end_utc)."""
    if start_utc.tzinfo is None:
        start_utc = start_utc.tz_localize("UTC")
    else:
        start_utc = start_utc.tz_convert("UTC")
    if end_utc.tzinfo is None:
        end_utc = end_utc.tz_localize("UTC")
    else:
        end_utc = end_utc.tz_convert("UTC")
    d0 = start_utc.floor("D")
    d1 = (end_utc - pd.Timedelta(microseconds=1)).floor("D") if end_utc > start_utc else d0
    days = []
    cur = d0
    while cur <= d1:
        days.append(cur)
        cur = cur + pd.Timedelta(days=1)
    return days

def download_aggtrades_futures_day(symbol: str,
                                  day_start_utc: "pd.Timestamp",
                                  cache_dir: str = "data_cache",
                                  progress_cb=None,
                                  force: bool = False) -> str:
    """Download ONE UTC day of aggTrades into the daily cache. Returns cache path.

    The file is written atomically (tmp -> rename) so interrupted downloads won't poison cache.
    """
    symbol = symbol.upper().strip()
    if day_start_utc.tzinfo is None:
        day_start_utc = day_start_utc.tz_localize("UTC")
    else:
        day_start_utc = day_start_utc.tz_convert("UTC")
    day_start_utc = day_start_utc.floor("D")
    day_end_utc = day_start_utc + pd.Timedelta(days=1)

    _ensure_dir(cache_dir)
    out_path = _aggtrades_day_path(cache_dir, symbol, day_start_utc)
    if (not force) and os.path.isfile(out_path):
        return out_path

    tmp_path = out_path + ".tmp"

    start_ms = int(day_start_utc.value // 1_000_000)
    end_ms = int(day_end_utc.value // 1_000_000)

    def _emit(msg: str):
        if progress_cb:
            try:
                progress_cb(msg, None)
            except Exception:
                pass

    _emit(f"Downloading aggTrades day {symbol} {day_start_utc.strftime('%Y-%m-%d')} …")
    url = f"{BINANCE_FAPI_BASE}/fapi/v1/aggTrades"
    headers = {"User-Agent": "macd-tool/1.0"}

    next_from_id = None
    current_start_ms = start_ms
    wrote = 0

    try:
        os.makedirs(os.path.dirname(out_path), exist_ok=True)
    except Exception:
        pass

    with gzip.open(tmp_path, "wt", encoding="utf-8") as out:
        while True:
            params = {"symbol": symbol, "limit": 1000}
            if next_from_id is not None:
                params["fromId"] = int(next_from_id)
            else:
                params["startTime"] = int(current_start_ms)

            try:
                r = requests.get(url, params=params, headers=headers, timeout=20)
                if r.status_code == 400 and ("startTime" in params) and ("endTime" not in params):
                    params2 = dict(params)
                    params2["endTime"] = int(min(end_ms, int(params2["startTime"]) + 3_599_000))
                    r = requests.get(url, params=params2, headers=headers, timeout=20)
                if r.status_code == 429:
                    _emit("Rate limited (429) on aggTrades — backing off briefly…")
                    time.sleep(0.8)
                    continue
                r.raise_for_status()
                data = r.json()
            except Exception as e:
                _emit(f"aggTrades day download error: {e} — retrying…")
                time.sleep(1.0)
                continue

            if not data:
                break

            try:
                data.sort(key=lambda x: int(x.get("a", 0)))
            except Exception:
                pass

            reached_end = False
            for d in data:
                try:
                    t = int(d.get("T", 0))
                except Exception:
                    continue
                if t < start_ms:
                    continue
                if t >= end_ms:
                    reached_end = True
                    break
                out.write(json.dumps(d) + "\n")
                wrote += 1

            if reached_end:
                break

            last = data[-1]
            try:
                last_id = int(last.get("a"))
                next_from_id = last_id + 1
            except Exception:
                try:
                    current_start_ms = int(last.get("T", current_start_ms)) + 1
                    next_from_id = None
                except Exception:
                    break

            if wrote and wrote % 200000 == 0:
                _emit(f"Downloaded {wrote:,} aggTrades for day {day_start_utc.strftime('%Y-%m-%d')} …")

    try:
        if os.path.isfile(out_path):
            try:
                os.remove(out_path)
            except Exception:
                pass
        os.replace(tmp_path, out_path)
    except Exception:
        try:
            os.replace(tmp_path, out_path)
        except Exception:
            pass

    _emit(f"Saved daily aggTrades cache: {os.path.basename(out_path)} ({wrote:,} ticks)")
    return out_path

def iter_aggtrades_futures_daily(symbol: str,
                                start_utc: "pd.Timestamp",
                                end_utc: "pd.Timestamp",
                                cache_dir: str = "data_cache",
                                progress_cb=None,
                                use_cache: bool = True,
                                auto_download_missing: bool = False,
                                force_redownload: bool = False):
    """Iterate Binance Futures aggTrades between [start_utc, end_utc) using a DAILY cache.

    Cache layout:
        {cache_dir}/aggtrades_futures_daily/{SYMBOL}/{YYYY-MM-DD}.jsonl.gz

    This avoids creating a new giant cache file for every backtest window.
    """
    symbol = symbol.upper().strip()
    if start_utc.tzinfo is None:
        start_utc = start_utc.tz_localize("UTC")
    else:
        start_utc = start_utc.tz_convert("UTC")
    if end_utc.tzinfo is None:
        end_utc = end_utc.tz_localize("UTC")
    else:
        end_utc = end_utc.tz_convert("UTC")

    start_ms = int(start_utc.value // 1_000_000)
    end_ms = int(end_utc.value // 1_000_000)

    def _emit(msg: str):
        if progress_cb:
            try:
                progress_cb(msg, None)
            except Exception:
                pass

    days = _iter_day_range_utc(start_utc, end_utc)
    missing = []
    for day in days:
        usable, reason = _aggtrades_cache_day_status(cache_dir, symbol, day)
        if (not use_cache) or force_redownload or (not usable):
            missing.append((day.strftime("%Y-%m-%d"), reason))

    if missing:
        if auto_download_missing:
            for day_str, _reason in missing:
                day = pd.Timestamp(day_str + " 00:00:00", tz="UTC")
                download_aggtrades_futures_day(symbol, day, cache_dir=cache_dir, progress_cb=progress_cb, force=True)
        else:
            raise ValueError(
                "Unavailable or unverified tick cache days for tick backtest:\n"
                + "\n".join(f" - {d} ({reason})" for d, reason in missing)
                + "\n\nOpen Tick Manager to verify/repair them (or enable 'Tick: auto-download missing days')."
            )

    for day in days:
        p = _aggtrades_day_path(cache_dir, symbol, day)
        if not os.path.isfile(p):
            continue
        _emit(f"Loading daily aggTrades: {symbol} {day.strftime('%Y-%m-%d')}")
        try:
            with gzip.open(p, "rt", encoding="utf-8") as f:
                for line in f:
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        d = json.loads(line)
                        t = int(d.get("T", 0))
                        if t < start_ms:
                            continue
                        if t >= end_ms:
                            break
                        yield d
                    except Exception:
                        continue
        except Exception as e:
            _emit(f"Error reading {os.path.basename(p)}: {e}")
            continue
