from __future__ import annotations

import json
import tempfile
import warnings
from pathlib import Path
from unittest.mock import patch

import numpy as np
import pandas as pd

import app.indicators_streaming as indicators_mod
from app.indicators_streaming import (
    INDICATOR_CACHE_VERSION,
    _load_indicator_cache,
    _save_indicator_cache,
    simulate_multitf_indicators_fast,
)


def _sample_1m(periods: int = 64) -> pd.DataFrame:
    idx = pd.date_range("2026-01-01T00:00:00Z", periods=periods, freq="1min", tz="UTC")
    base = np.linspace(100.0, 112.0, periods)
    return pd.DataFrame(
        {
            "open_time": [ts - pd.Timedelta(minutes=1) + pd.Timedelta(milliseconds=1) for ts in idx],
            "close_time": list(idx),
            "open": base,
            "high": base + 0.8,
            "low": base - 0.7,
            "close": base + 0.2,
            "volume": np.linspace(10.0, 25.0, periods),
            "trade_count": np.linspace(50, 120, periods).astype(int),
            "taker_buy_volume": np.linspace(5.0, 14.0, periods),
        }
    )


def test_indicator_cache_roundtrip_avoids_fragmentation_warnings() -> None:
    df_1m = _sample_1m()
    streams = simulate_multitf_indicators_fast(df_1m, ["1m", "5m"])

    with tempfile.TemporaryDirectory() as tmpdir:
        cache_path = Path(tmpdir) / "indicator_cache.pkl"
        meta_path = Path(tmpdir) / "indicator_cache.meta.json"
        meta = {
            "report_version": INDICATOR_CACHE_VERSION,
            "timeframes": ["1m", "5m"],
        }

        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always", pd.errors.PerformanceWarning)
            _save_indicator_cache(str(cache_path), str(meta_path), meta, streams)
            loaded = _load_indicator_cache(str(cache_path), str(meta_path))

    perf_warnings = [w for w in caught if issubclass(w.category, pd.errors.PerformanceWarning)]
    assert perf_warnings == []
    assert loaded is not None
    assert set(loaded.keys()) == {"1m", "5m"}

    for tf in ("1m", "5m"):
        original = streams[tf]
        roundtrip = loaded[tf]
        assert list(original.index) == list(roundtrip.index)
        for col in [
            "price",
            "macd",
            "signal",
            "ms",
            "angle",
            "ppo",
            "flip_age",
            "adx",
            "atr",
            "vidya_line",
            "market_state",
            "market_confidence",
            "range_filter_line",
        ]:
            pd.testing.assert_series_equal(original[col], roundtrip[col], check_names=False)


def test_indicator_cache_save_uses_unique_tmp_files_and_retries_replace() -> None:
    df_1m = _sample_1m(periods=16)
    streams = simulate_multitf_indicators_fast(df_1m, ["1m"])

    with tempfile.TemporaryDirectory() as tmpdir:
        cache_path = Path(tmpdir) / "indicator_cache.pkl"
        meta_path = Path(tmpdir) / "indicator_cache.meta.json"
        meta = {
            "report_version": INDICATOR_CACHE_VERSION,
            "timeframes": ["1m"],
        }

        replace_attempts = {}
        seen_src = []
        real_replace = indicators_mod.os.replace

        def flaky_replace(src: str, dst: str) -> None:
            seen_src.append(src)
            key = str(dst)
            replace_attempts[key] = replace_attempts.get(key, 0) + 1
            if replace_attempts[key] == 1:
                raise PermissionError(32, "sharing violation", key)
            real_replace(src, dst)

        with patch("app.indicators_streaming.os.replace", side_effect=flaky_replace):
            _save_indicator_cache(str(cache_path), str(meta_path), meta, streams)

        assert any(str(src).endswith(".tmp") for src in seen_src)
        unique_src = {str(src) for src in seen_src}
        assert len(unique_src) == 2
        assert cache_path.is_file()
        assert meta_path.is_file()
        loaded = _load_indicator_cache(str(cache_path), str(meta_path))
        assert loaded is not None
