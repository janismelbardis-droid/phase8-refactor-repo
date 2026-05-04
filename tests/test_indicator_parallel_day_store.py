from __future__ import annotations

import json
import os
from concurrent.futures import Future
from pathlib import Path

import numpy as np
import pandas as pd

import app.indicators_streaming as indicators_mod
from app.indicators_streaming import (
    _indicator_cache_paths,
    _local_indicator_store_day_paths,
    simulate_multitf_indicators,
)


def _sample_two_day_1m() -> pd.DataFrame:
    open_times = pd.date_range("2026-03-28 00:00:00", periods=2 * 24 * 60, freq="1min", tz="UTC")
    close_times = open_times + pd.Timedelta(seconds=59)
    base = np.linspace(100.0, 130.0, len(open_times))
    return pd.DataFrame(
        {
            "open_time": open_times,
            "close_time": close_times,
            "open": base,
            "high": base + 0.5,
            "low": base - 0.5,
            "close": base + 0.1,
            "volume": np.linspace(10.0, 20.0, len(open_times)),
        }
    )


def _sample_multi_day_1m(day_count: int) -> pd.DataFrame:
    open_times = pd.date_range("2026-03-28 00:00:00", periods=day_count * 24 * 60, freq="1min", tz="UTC")
    close_times = open_times + pd.Timedelta(seconds=59)
    base = np.linspace(100.0, 100.0 + float(day_count * 20), len(open_times))
    return pd.DataFrame(
        {
            "open_time": open_times,
            "close_time": close_times,
            "open": base,
            "high": base + 0.5,
            "low": base - 0.5,
            "close": base + 0.1,
            "volume": np.linspace(10.0, 20.0, len(open_times)),
        }
    )


def test_simulate_multitf_indicators_materializes_parallel_day_store(tmp_path: Path) -> None:
    cache_dir = tmp_path / "data_cache"
    cache_dir.mkdir(parents=True, exist_ok=True)
    symbol = "BTCUSDT"
    df = _sample_two_day_1m()
    start_utc = pd.Timestamp("2026-03-28 00:00:00", tz="UTC")
    end_utc = pd.Timestamp("2026-03-29 23:59:59", tz="UTC")
    required_fields = {"price", "ema_1", "ema_2", "ema_stack", "ema_cross_up", "ema_cross_down"}
    tracker = {"used": False, "submitted": 0, "max_workers": 0}

    class _FakeExecutor:
        def __init__(self, max_workers=None, *args, **kwargs):
            tracker["used"] = True
            tracker["max_workers"] = int(max_workers or 0)

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

        def submit(self, fn, *args, **kwargs):
            tracker["submitted"] += 1
            future: Future = Future()
            try:
                future.set_result(fn(*args, **kwargs))
            except Exception as exc:  # pragma: no cover - surfaced by future.result()
                future.set_exception(exc)
            return future

    original_executor = indicators_mod.ProcessPoolExecutor
    old_env = {
        "BT_INDICATOR_PARALLEL_MIN_ROWS": os.environ.get("BT_INDICATOR_PARALLEL_MIN_ROWS"),
        "BT_INDICATOR_PARALLEL_MIN_DAYS": os.environ.get("BT_INDICATOR_PARALLEL_MIN_DAYS"),
        "BT_INDICATOR_MAX_WORKERS": os.environ.get("BT_INDICATOR_MAX_WORKERS"),
        "BT_INDICATOR_DISABLE_PARALLEL": os.environ.get("BT_INDICATOR_DISABLE_PARALLEL"),
    }
    try:
        indicators_mod.ProcessPoolExecutor = _FakeExecutor
        os.environ["BT_INDICATOR_PARALLEL_MIN_ROWS"] = "0"
        os.environ["BT_INDICATOR_PARALLEL_MIN_DAYS"] = "2"
        os.environ["BT_INDICATOR_MAX_WORKERS"] = "2"
        os.environ["BT_INDICATOR_DISABLE_PARALLEL"] = "0"

        streams = simulate_multitf_indicators(
            df,
            ["1m"],
            cache_dir=str(cache_dir),
            symbol=symbol,
            start_utc=start_utc,
            end_utc=end_utc,
            price_source="LAST",
            required_fields=required_fields,
            ema_settings={"defaults": {"ema_1_length": 9, "ema_2_length": 21}},
        )
    finally:
        indicators_mod.ProcessPoolExecutor = original_executor
        for key, value in old_env.items():
            if value is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = value

    assert tracker["used"] is True
    assert tracker["submitted"] == 2
    assert tracker["max_workers"] == 2
    assert "1m" in streams
    assert "ema_1" in streams["1m"].columns
    assert "ema_2" in streams["1m"].columns
    assert len(streams["1m"]) == len(df)

    exact_pq, exact_meta = _indicator_cache_paths(
        str(cache_dir),
        symbol,
        start_utc,
        end_utc,
        ["1m"],
        "LAST",
        "TRADINGVIEW",
        "TRADINGVIEW",
        required_fields=required_fields,
        ema_settings={"defaults": {"ema_1_length": 9, "ema_2_length": 21}},
    )
    assert Path(exact_pq).is_file()
    exact_payload = json.loads(Path(exact_meta).read_text(encoding="utf-8"))
    assert exact_payload["build_mode"] == "parallel_day_store"
    assert exact_payload["build_stats"]["missing_days"] == 2
    assert exact_payload["build_stats"]["workers"] == 2
    assert exact_payload["timings"]["local_store_materialize_sec"] >= 0.0

    for day in ("2026-03-28", "2026-03-29"):
        day_pq, day_meta = _local_indicator_store_day_paths(
            str(cache_dir),
            symbol,
            pd.Timestamp(day, tz="UTC"),
            timeframes=["1m"],
            price_source="LAST",
            macd_impl="TRADINGVIEW",
            adx_impl="TRADINGVIEW",
            required_fields=required_fields,
            ema_settings={"defaults": {"ema_1_length": 9, "ema_2_length": 21}},
            market_state_thresholds=None,
        )
        assert Path(day_pq).is_file()
        payload = json.loads(Path(day_meta).read_text(encoding="utf-8"))
        assert payload["build_mode"] == "parallel_day_store"


def test_simulate_multitf_indicators_materializes_parallel_chunk_store(tmp_path: Path) -> None:
    cache_dir = tmp_path / "data_cache"
    cache_dir.mkdir(parents=True, exist_ok=True)
    symbol = "BTCUSDT"
    df = _sample_multi_day_1m(4)
    start_utc = pd.Timestamp("2026-03-28 00:00:00", tz="UTC")
    end_utc = pd.Timestamp("2026-03-31 23:59:59", tz="UTC")
    required_fields = {"price", "ema_1", "ema_2", "ema_stack", "ema_cross_up", "ema_cross_down"}
    tracker = {"used": False, "submitted": 0, "max_workers": 0}

    class _FakeExecutor:
        def __init__(self, max_workers=None, *args, **kwargs):
            tracker["used"] = True
            tracker["max_workers"] = int(max_workers or 0)

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

        def submit(self, fn, *args, **kwargs):
            tracker["submitted"] += 1
            future: Future = Future()
            try:
                future.set_result(fn(*args, **kwargs))
            except Exception as exc:  # pragma: no cover
                future.set_exception(exc)
            return future

    original_executor = indicators_mod.ProcessPoolExecutor
    old_env = {
        "BT_INDICATOR_PARALLEL_MIN_ROWS": os.environ.get("BT_INDICATOR_PARALLEL_MIN_ROWS"),
        "BT_INDICATOR_PARALLEL_MIN_DAYS": os.environ.get("BT_INDICATOR_PARALLEL_MIN_DAYS"),
        "BT_INDICATOR_MAX_WORKERS": os.environ.get("BT_INDICATOR_MAX_WORKERS"),
        "BT_INDICATOR_DISABLE_PARALLEL": os.environ.get("BT_INDICATOR_DISABLE_PARALLEL"),
        "BT_INDICATOR_MAX_CHUNK_DAYS": os.environ.get("BT_INDICATOR_MAX_CHUNK_DAYS"),
    }
    try:
        indicators_mod.ProcessPoolExecutor = _FakeExecutor
        os.environ["BT_INDICATOR_PARALLEL_MIN_ROWS"] = "0"
        os.environ["BT_INDICATOR_PARALLEL_MIN_DAYS"] = "2"
        os.environ["BT_INDICATOR_MAX_WORKERS"] = "2"
        os.environ["BT_INDICATOR_DISABLE_PARALLEL"] = "0"
        os.environ["BT_INDICATOR_MAX_CHUNK_DAYS"] = "2"

        streams = simulate_multitf_indicators(
            df,
            ["1m"],
            cache_dir=str(cache_dir),
            symbol=symbol,
            start_utc=start_utc,
            end_utc=end_utc,
            price_source="LAST",
            required_fields=required_fields,
            ema_settings={"defaults": {"ema_1_length": 9, "ema_2_length": 21}},
        )
    finally:
        indicators_mod.ProcessPoolExecutor = original_executor
        for key, value in old_env.items():
            if value is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = value

    assert tracker["used"] is True
    assert tracker["submitted"] == 2
    assert tracker["max_workers"] == 2
    assert "1m" in streams
    assert len(streams["1m"]) == len(df)

    exact_pq, exact_meta = _indicator_cache_paths(
        str(cache_dir),
        symbol,
        start_utc,
        end_utc,
        ["1m"],
        "LAST",
        "TRADINGVIEW",
        "TRADINGVIEW",
        required_fields=required_fields,
        ema_settings={"defaults": {"ema_1_length": 9, "ema_2_length": 21}},
    )
    assert Path(exact_pq).is_file()
    exact_payload = json.loads(Path(exact_meta).read_text(encoding="utf-8"))
    assert exact_payload["build_mode"] == "parallel_chunk_store"
    assert exact_payload["build_stats"]["missing_days"] == 4
    assert exact_payload["build_stats"]["task_count"] == 2
    assert exact_payload["build_stats"]["chunk_days"] == 2
    assert exact_payload["build_stats"]["workers"] == 2
    assert len(exact_payload["build_stats"]["day_reports"]) == 4

    for day in ("2026-03-28", "2026-03-29", "2026-03-30", "2026-03-31"):
        day_pq, day_meta = _local_indicator_store_day_paths(
            str(cache_dir),
            symbol,
            pd.Timestamp(day, tz="UTC"),
            timeframes=["1m"],
            price_source="LAST",
            macd_impl="TRADINGVIEW",
            adx_impl="TRADINGVIEW",
            required_fields=required_fields,
            ema_settings={"defaults": {"ema_1_length": 9, "ema_2_length": 21}},
            market_state_thresholds=None,
        )
        assert Path(day_pq).is_file()
        payload = json.loads(Path(day_meta).read_text(encoding="utf-8"))
        assert payload["build_mode"] == "parallel_chunk_store"
