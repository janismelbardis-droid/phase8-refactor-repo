from __future__ import annotations

from collections import OrderedDict
from dataclasses import dataclass
import hashlib
import json
import os
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Tuple

import pandas as pd

from .data_binance import _exact_1m_window_paths, fetch_klines_1m_futures
from .indicators_streaming import (
    _iter_utc_days,
    _load_local_indicator_store_window,
    _local_indicator_store_day_paths,
    simulate_multitf_indicators,
)


ProgressCallback = Optional[Callable[[str, Optional[float]], None]]


@dataclass
class StreamBundleLoadResult:
    df_1m: pd.DataFrame
    streams_full: Dict[str, pd.DataFrame]
    source: str
    memory_cached: bool = False


@dataclass
class _StreamBundleMemoryCacheEntry:
    result: StreamBundleLoadResult


_STREAM_BUNDLE_MEMORY_CACHE_MAX = 8
_STREAM_BUNDLE_MEMORY_CACHE: "OrderedDict[str, _StreamBundleMemoryCacheEntry]" = OrderedDict()


def clear_stream_bundle_memory_cache() -> None:
    _STREAM_BUNDLE_MEMORY_CACHE.clear()


def _copy_stream_bundle_result(result: StreamBundleLoadResult, *, memory_cached: bool) -> StreamBundleLoadResult:
    return StreamBundleLoadResult(
        df_1m=result.df_1m.copy(deep=True),
        streams_full={str(tf): df.copy(deep=True) for tf, df in dict(result.streams_full or {}).items()},
        source=str(result.source or ""),
        memory_cached=bool(memory_cached),
    )


def _jsonable_cache_value(value: Any) -> Any:
    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    if isinstance(value, Path):
        return str(value)
    if isinstance(value, pd.Timestamp):
        return pd.to_datetime(value, utc=True).isoformat()
    if isinstance(value, dict):
        return {str(k): _jsonable_cache_value(v) for k, v in sorted(value.items(), key=lambda kv: str(kv[0]))}
    if isinstance(value, (list, tuple)):
        return [_jsonable_cache_value(v) for v in value]
    if isinstance(value, (set, frozenset)):
        return sorted((_jsonable_cache_value(v) for v in value), key=lambda item: json.dumps(item, sort_keys=True, ensure_ascii=False, default=str))
    return str(value)


def _stat_file_token(path: str) -> Optional[Tuple[str, int, int]]:
    try:
        st = os.stat(path)
    except Exception:
        return None
    return (os.path.abspath(path), int(st.st_size), int(getattr(st, "st_mtime_ns", int(st.st_mtime * 1_000_000_000))))


def _exact_candle_cache_token(
    cache_dir: Path,
    symbol: str,
    start: pd.Timestamp,
    end: pd.Timestamp,
    price_source: str,
) -> Optional[Tuple[str, int, int]]:
    pq_path, csv_path = _exact_1m_window_paths(str(cache_dir), str(symbol or "").upper(), start, end, price_source)
    for path in (pq_path, csv_path):
        token = _stat_file_token(path)
        if token is not None:
            return token
    return None


def _local_indicator_store_window_tokens(
    *,
    cache_dir: Path,
    symbol: str,
    start: pd.Timestamp,
    end: pd.Timestamp,
    timeframes: List[str],
    price_source: str,
    macd_impl: str,
    adx_impl: str,
    required_fields: Any,
    ema_settings: Optional[Any],
    rsi_settings: Optional[Any],
) -> Optional[List[Tuple[str, int, int]]]:
    tokens: List[Tuple[str, int, int]] = []
    for day in _iter_utc_days(start, end):
        pq_path, meta_path = _local_indicator_store_day_paths(
            str(cache_dir),
            str(symbol or "").upper(),
            day,
            timeframes=list(timeframes or []),
            price_source=str(price_source or "LAST").upper(),
            macd_impl=str(macd_impl or "TRADINGVIEW").upper(),
            adx_impl=str(adx_impl or "TRADINGVIEW").upper(),
            required_fields=required_fields,
            ema_settings=ema_settings,
            rsi_settings=rsi_settings,
            market_state_thresholds=None,
        )
        pq_token = _stat_file_token(pq_path)
        meta_token = _stat_file_token(meta_path)
        if pq_token is None or meta_token is None:
            return None
        tokens.append(pq_token)
        tokens.append(meta_token)
    return tokens


def _stream_bundle_memory_cache_key(
    *,
    symbol: str,
    start: pd.Timestamp,
    end: pd.Timestamp,
    compute_start: pd.Timestamp,
    timeframes: List[str],
    cache_dir: Path,
    price_source: str,
    macd_impl: str,
    adx_impl: str,
    required_fields: Any,
    ema_settings: Optional[Any],
    rsi_settings: Optional[Any],
    candle_token: Optional[Tuple[str, int, int]],
    local_store_tokens: Optional[List[Tuple[str, int, int]]],
) -> str:
    payload = {
        "symbol": str(symbol or "").upper(),
        "start": pd.to_datetime(start, utc=True).isoformat(),
        "end": pd.to_datetime(end, utc=True).isoformat(),
        "compute_start": pd.to_datetime(compute_start, utc=True).isoformat(),
        "timeframes": [str(tf) for tf in list(timeframes or [])],
        "cache_dir": str(Path(cache_dir).resolve()),
        "price_source": str(price_source or "LAST").upper(),
        "macd_impl": str(macd_impl or "TRADINGVIEW").upper(),
        "adx_impl": str(adx_impl or "TRADINGVIEW").upper(),
        "required_fields": _jsonable_cache_value(required_fields),
        "ema_settings": _jsonable_cache_value(ema_settings),
        "rsi_settings": _jsonable_cache_value(rsi_settings),
        "candle_token": _jsonable_cache_value(candle_token),
        "local_store_tokens": _jsonable_cache_value(local_store_tokens),
    }
    raw = json.dumps(payload, sort_keys=True, ensure_ascii=False, separators=(",", ":"))
    return hashlib.sha1(raw.encode("utf-8")).hexdigest()


def _stream_bundle_memory_cache_get(key: str) -> Optional[StreamBundleLoadResult]:
    entry = _STREAM_BUNDLE_MEMORY_CACHE.get(str(key or ""))
    if entry is None:
        return None
    _STREAM_BUNDLE_MEMORY_CACHE.move_to_end(str(key or ""))
    return _copy_stream_bundle_result(entry.result, memory_cached=True)


def _stream_bundle_memory_cache_put(key: str, result: StreamBundleLoadResult) -> None:
    cache_key = str(key or "")
    _STREAM_BUNDLE_MEMORY_CACHE[cache_key] = _StreamBundleMemoryCacheEntry(
        result=_copy_stream_bundle_result(result, memory_cached=False)
    )
    _STREAM_BUNDLE_MEMORY_CACHE.move_to_end(cache_key)
    while len(_STREAM_BUNDLE_MEMORY_CACHE) > int(_STREAM_BUNDLE_MEMORY_CACHE_MAX):
        _STREAM_BUNDLE_MEMORY_CACHE.popitem(last=False)


def _slice_df_window(df: pd.DataFrame, start: pd.Timestamp, end: pd.Timestamp) -> pd.DataFrame:
    if df.empty or "open_time" not in df.columns:
        return df.copy()
    open_time = pd.to_datetime(df["open_time"], utc=True, errors="coerce")
    mask = (open_time >= pd.to_datetime(start, utc=True)) & (open_time <= pd.to_datetime(end, utc=True))
    return df.loc[mask].reset_index(drop=True).copy()


def _slice_streams_window(
    streams: Dict[str, pd.DataFrame],
    start: pd.Timestamp,
    end: pd.Timestamp,
    *,
    timeframes: List[str],
) -> Dict[str, pd.DataFrame]:
    out: Dict[str, pd.DataFrame] = {}
    start_ts = pd.to_datetime(start, utc=True)
    end_ts = pd.to_datetime(end, utc=True)
    allowed = {str(tf) for tf in list(timeframes or []) if str(tf or "").strip()}
    for tf, df in dict(streams or {}).items():
        tf_name = str(tf or "")
        if allowed and tf_name not in allowed:
            continue
        if not isinstance(df, pd.DataFrame) or df.empty or "open_time" not in df.columns:
            out[tf_name] = df.copy() if isinstance(df, pd.DataFrame) else df
            continue
        open_time = pd.to_datetime(df["open_time"], utc=True, errors="coerce")
        mask = (open_time >= start_ts) & (open_time <= end_ts)
        out[tf_name] = df.loc[mask].reset_index(drop=True).copy()
    return out


def load_stream_bundle_library_first(
    *,
    symbol: str,
    start: pd.Timestamp,
    end: pd.Timestamp,
    compute_start: Optional[pd.Timestamp] = None,
    timeframes: List[str],
    cache_dir: Path,
    price_source: str,
    macd_impl: str,
    adx_impl: str,
    required_fields: Any,
    ema_settings: Optional[Any] = None,
    rsi_settings: Optional[Any] = None,
    progress_cb: ProgressCallback = None,
) -> StreamBundleLoadResult:
    query_start = pd.to_datetime(start, utc=True)
    query_end = pd.to_datetime(end, utc=True)
    compute_from = pd.to_datetime(compute_start if compute_start is not None else start, utc=True)
    if progress_cb:
        progress_cb("Loading candles from cache or exchange...", 5)
    df_1m = fetch_klines_1m_futures(
        str(symbol or "").upper(),
        query_start,
        query_end,
        str(cache_dir),
        progress_cb=progress_cb,
        price_source=str(price_source or "LAST").upper(),
    )
    if df_1m.empty:
        raise ValueError("No candles returned for the requested window.")

    candle_token = _exact_candle_cache_token(
        cache_dir=Path(cache_dir),
        symbol=str(symbol or "").upper(),
        start=query_start,
        end=query_end,
        price_source=str(price_source or "LAST").upper(),
    )
    local_store_tokens = _local_indicator_store_window_tokens(
        cache_dir=Path(cache_dir),
        symbol=str(symbol or "").upper(),
        start=query_start,
        end=query_end,
        timeframes=list(timeframes or []),
        price_source=str(price_source or "LAST").upper(),
        macd_impl=str(macd_impl or "TRADINGVIEW").upper(),
        adx_impl=str(adx_impl or "TRADINGVIEW").upper(),
        required_fields=required_fields,
        ema_settings=ema_settings,
        rsi_settings=rsi_settings,
    )
    memory_cache_key = None
    if candle_token is not None and local_store_tokens is not None:
        memory_cache_key = _stream_bundle_memory_cache_key(
            symbol=str(symbol or "").upper(),
            start=query_start,
            end=query_end,
            compute_start=compute_from,
            timeframes=list(timeframes or []),
            cache_dir=Path(cache_dir),
            price_source=str(price_source or "LAST").upper(),
            macd_impl=str(macd_impl or "TRADINGVIEW").upper(),
            adx_impl=str(adx_impl or "TRADINGVIEW").upper(),
            required_fields=required_fields,
            ema_settings=ema_settings,
            rsi_settings=rsi_settings,
            candle_token=candle_token,
            local_store_tokens=local_store_tokens,
        )
        cached = _stream_bundle_memory_cache_get(memory_cache_key)
        if cached is not None:
            if progress_cb:
                progress_cb("Loaded local indicator store.", 100)
            return cached

    streams_full = _load_local_indicator_store_window(
        str(cache_dir),
        str(symbol or "").upper(),
        query_start,
        query_end,
        timeframes=list(timeframes or []),
        price_source=str(price_source or "LAST").upper(),
        macd_impl=str(macd_impl or "TRADINGVIEW").upper(),
        adx_impl=str(adx_impl or "TRADINGVIEW").upper(),
        required_fields=required_fields,
        ema_settings=ema_settings,
        rsi_settings=rsi_settings,
        market_state_thresholds=None,
    )
    if streams_full is not None:
        result = StreamBundleLoadResult(
            df_1m=df_1m,
            streams_full=streams_full,
            source="local_indicator_store",
        )
        if memory_cache_key:
            _stream_bundle_memory_cache_put(memory_cache_key, result)
        if progress_cb:
            progress_cb("Loaded local indicator store.", 100)
        return result

    if progress_cb:
        progress_cb("Calculating indicator streams...", 20)
    df_compute = fetch_klines_1m_futures(
        str(symbol or "").upper(),
        compute_from,
        query_end,
        str(cache_dir),
        progress_cb=progress_cb,
        price_source=str(price_source or "LAST").upper(),
    )
    if df_compute.empty:
        raise ValueError("No candles returned for the requested compute window.")

    streams_computed = simulate_multitf_indicators(
        df_compute,
        list(timeframes or []),
        progress_cb,
        macd_impl=str(macd_impl or "TRADINGVIEW").upper(),
        adx_impl=str(adx_impl or "TRADINGVIEW").upper(),
        cache_dir=str(cache_dir),
        symbol=str(symbol or "").upper(),
        start_utc=compute_from,
        end_utc=query_end,
        price_source=str(price_source or "LAST").upper(),
        required_fields=required_fields,
        ema_settings=ema_settings,
        rsi_settings=rsi_settings,
    )
    if compute_from != query_start:
        df_1m = _slice_df_window(df_compute, query_start, query_end)
        streams_full = _slice_streams_window(
            streams_computed,
            query_start,
            query_end,
            timeframes=list(timeframes or []),
        )
    else:
        df_1m = df_compute
        streams_full = streams_computed
    return StreamBundleLoadResult(
        df_1m=df_1m,
        streams_full=streams_full,
        source="fresh_indicator_build",
    )
