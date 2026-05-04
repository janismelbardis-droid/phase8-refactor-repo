from __future__ import annotations

import os
import tempfile
import unittest
from unittest.mock import patch

import pandas as pd

from app.indicators_streaming import (
    _load_local_indicator_store_window,
    _local_indicator_store_day_paths,
    _save_local_indicator_store,
    simulate_multitf_indicators,
)


class Phase9PerfSafetyTests(unittest.TestCase):
    def _sample_df_1m(self, start: str = "2026-01-01T00:00:00Z", periods: int = 8) -> pd.DataFrame:
        idx = pd.date_range(start, periods=periods, freq="1min", tz="UTC")
        return pd.DataFrame(
            {
                "open_time": idx,
                "close_time": idx + pd.Timedelta(seconds=59),
                "open": [100.0 + i for i in range(periods)],
                "high": [100.5 + i for i in range(periods)],
                "low": [99.5 + i for i in range(periods)],
                "close": [100.2 + i for i in range(periods)],
                "volume": [10.0] * periods,
            }
        )

    def _sample_streams(self, start: str = "2026-01-01T00:00:00Z", periods: int = 8):
        idx = pd.date_range(start, periods=periods, freq="1min", tz="UTC")
        df_1m = pd.DataFrame(
            {
                "price": [100.0 + i for i in range(periods)],
                "macd": [0.1 * i for i in range(periods)],
                "signal": [0.05 * i for i in range(periods)],
                "ms": ["GREEN"] * periods,
                "angle": ["GREEN"] * periods,
                "sig_angle": ["GREEN"] * periods,
                "ppo": ["GREEN"] * periods,
                "flip_age": list(range(periods)),
                "adx": [20.0 + i for i in range(periods)],
                "atr": [1.0 + 0.1 * i for i in range(periods)],
                "di_plus": [10.0 + i for i in range(periods)],
                "di_minus": [5.0 + i for i in range(periods)],
                "di_spread": [5.0] * periods,
                "adx_angle": ["GREEN"] * periods,
                "atr_angle": ["GREEN"] * periods,
            },
            index=idx,
        )
        df_5m = df_1m.resample("5min").last()
        return {"1m": df_1m, "5m": df_5m}

    def test_local_indicator_store_roundtrip_supports_subset_window(self) -> None:
        streams = self._sample_streams(periods=12)
        with tempfile.TemporaryDirectory() as tmp:
            _save_local_indicator_store(
                tmp,
                "BTCUSDT",
                streams,
                timeframes=["1m", "5m"],
                price_source="LAST",
                macd_impl="TRADINGVIEW",
                adx_impl="TRADINGVIEW",
                required_fields={"macd", "signal", "atr"},
                market_state_thresholds=None,
            )
            day_pq, day_meta = _local_indicator_store_day_paths(
                tmp,
                "BTCUSDT",
                pd.Timestamp("2026-01-01T00:00:00Z"),
                timeframes=["1m", "5m"],
                price_source="LAST",
                macd_impl="TRADINGVIEW",
                adx_impl="TRADINGVIEW",
                required_fields={"macd", "signal", "atr"},
                market_state_thresholds=None,
            )
            self.assertTrue(os.path.exists(day_pq))
            self.assertTrue(os.path.exists(day_meta))

            loaded = _load_local_indicator_store_window(
                tmp,
                "BTCUSDT",
                pd.Timestamp("2026-01-01T00:02:00Z"),
                pd.Timestamp("2026-01-01T00:09:00Z"),
                timeframes=["1m", "5m"],
                price_source="LAST",
                macd_impl="TRADINGVIEW",
                adx_impl="TRADINGVIEW",
                required_fields={"macd", "signal", "atr"},
                market_state_thresholds=None,
            )
            self.assertIsNotNone(loaded)
            self.assertEqual(len(loaded["1m"]), 8)
            self.assertEqual(float(loaded["1m"].iloc[0]["macd"]), 0.2)
            self.assertEqual(list(loaded.keys()), ["1m", "5m"])

    def test_simulate_multitf_indicators_uses_local_indicator_store_without_recompute(self) -> None:
        df_1m = self._sample_df_1m(periods=8)
        streams = self._sample_streams(periods=8)
        with tempfile.TemporaryDirectory() as tmp:
            _save_local_indicator_store(
                tmp,
                "ETHUSDT",
                streams,
                timeframes=["1m", "5m"],
                price_source="LAST",
                macd_impl="TRADINGVIEW",
                adx_impl="TRADINGVIEW",
                required_fields={"macd", "signal", "atr"},
                market_state_thresholds=None,
            )
            with patch("app.indicators_streaming.simulate_multitf_indicators_fast", side_effect=AssertionError("should not recompute")):
                out = simulate_multitf_indicators(
                    df_1m,
                    ["1m", "5m"],
                    cache_dir=tmp,
                    symbol="ETHUSDT",
                    start_utc=pd.Timestamp("2026-01-01T00:00:00Z"),
                    end_utc=pd.Timestamp("2026-01-01T00:07:00Z"),
                    price_source="LAST",
                    macd_impl="TRADINGVIEW",
                    adx_impl="TRADINGVIEW",
                    required_fields={"macd", "signal", "atr"},
                )
            self.assertEqual(len(out["1m"]), 8)
            self.assertAlmostEqual(float(out["1m"].iloc[-1]["atr"]), 1.7, places=9)


if __name__ == "__main__":
    unittest.main()
