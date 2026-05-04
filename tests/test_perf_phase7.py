from __future__ import annotations

import tempfile
import unittest

import pandas as pd

from app.domain.market_state import MarketStateThresholds
from app.prepared_dataset import (
    build_prepared_dataset,
    find_covering_prepared_dataset_on_disk,
    save_prepared_dataset_to_disk,
    slice_df_1m_window,
    slice_streams_window,
)


class Phase7PerfSafetyTests(unittest.TestCase):
    def _sample_df(self, start: str = "2026-01-01T00:00:00Z", periods: int = 12) -> pd.DataFrame:
        idx = pd.date_range(start, periods=periods, freq="1min", tz="UTC")
        return pd.DataFrame(
            {
                "open_time": idx,
                "close_time": idx + pd.Timedelta(seconds=59),
                "open": [100.0 + i for i in range(len(idx))],
                "high": [100.5 + i for i in range(len(idx))],
                "low": [99.5 + i for i in range(len(idx))],
                "close": [100.2 + i for i in range(len(idx))],
                "volume": [10.0] * len(idx),
            }
        )

    def _sample_streams(self, start: str = "2026-01-01T00:00:00Z", periods: int = 12):
        idx = pd.date_range(start, periods=periods, freq="1min", tz="UTC")
        df_1m = pd.DataFrame(
            {
                "price": [100.0 + i for i in range(len(idx))],
                "macd": [0.1 * i for i in range(len(idx))],
                "signal": [0.05 * i for i in range(len(idx))],
                "market_state": ["BULL_EXPANSION"] * len(idx),
            },
            index=idx,
        )
        df_5m = df_1m.resample("5min").last()
        return {"1m": df_1m, "5m": df_5m}

    def test_prepared_dataset_roundtrip_on_disk_supports_covering_subset(self) -> None:
        df_1m = self._sample_df()
        streams = self._sample_streams()
        entry = build_prepared_dataset(
            symbol="BTCUSDT",
            start=df_1m["open_time"].min(),
            end=df_1m["open_time"].max(),
            timeframes=["1m", "5m"],
            price_source="LAST",
            macd_impl="TRADINGVIEW",
            adx_impl="TRADINGVIEW",
            required_fields={"macd", "signal", "atr", "market_state"},
            market_state_thresholds=MarketStateThresholds(direction_long_min=4),
            df_1m_full=df_1m,
            streams_full=streams,
        )
        with tempfile.TemporaryDirectory() as tmp:
            saved = save_prepared_dataset_to_disk(tmp, entry)
            self.assertIsNotNone(saved)
            found = find_covering_prepared_dataset_on_disk(
                tmp,
                symbol="BTCUSDT",
                start=pd.Timestamp("2026-01-01T00:03:00Z"),
                end=pd.Timestamp("2026-01-01T00:08:00Z"),
                timeframes=["1m"],
                price_source="LAST",
                macd_impl="TRADINGVIEW",
                adx_impl="TRADINGVIEW",
                required_fields={"macd", "signal"},
                market_state_thresholds=None,
            )
            self.assertIsNotNone(found)
            self.assertEqual(found.symbol, "BTCUSDT")
            self.assertEqual(found.timeframes, ("1m", "5m"))
            sliced_df = slice_df_1m_window(found.df_1m_full, pd.Timestamp("2026-01-01T00:03:00Z"), pd.Timestamp("2026-01-01T00:08:00Z"))
            self.assertEqual(len(sliced_df), 6)
            self.assertEqual(float(sliced_df.iloc[0]["close"]), 103.2)
            sliced_streams = slice_streams_window(found.streams_full, pd.Timestamp("2026-01-01T00:03:00Z"), pd.Timestamp("2026-01-01T00:08:00Z"), timeframes=["1m"])
            self.assertEqual(list(sliced_streams.keys()), ["1m"])
            self.assertEqual(len(sliced_streams["1m"]), 6)
            self.assertEqual(str(sliced_streams["1m"].index.tz), "UTC")

    def test_market_aug_threshold_mismatch_blocks_disk_reuse(self) -> None:
        df_1m = self._sample_df(periods=8)
        streams = self._sample_streams(periods=8)
        entry = build_prepared_dataset(
            symbol="ETHUSDT",
            start=df_1m["open_time"].min(),
            end=df_1m["open_time"].max(),
            timeframes=["1m"],
            price_source="LAST",
            macd_impl="TRADINGVIEW",
            adx_impl="TRADINGVIEW",
            required_fields={"market_state"},
            market_state_thresholds=MarketStateThresholds(direction_long_min=4),
            df_1m_full=df_1m,
            streams_full=streams,
        )
        with tempfile.TemporaryDirectory() as tmp:
            save_prepared_dataset_to_disk(tmp, entry)
            found = find_covering_prepared_dataset_on_disk(
                tmp,
                symbol="ETHUSDT",
                start=pd.Timestamp("2026-01-01T00:01:00Z"),
                end=pd.Timestamp("2026-01-01T00:06:00Z"),
                timeframes=["1m"],
                price_source="LAST",
                macd_impl="TRADINGVIEW",
                adx_impl="TRADINGVIEW",
                required_fields={"market_state"},
                market_state_thresholds=MarketStateThresholds(direction_long_min=5),
            )
            self.assertIsNone(found)

    def test_ema_threshold_mismatch_blocks_disk_reuse(self) -> None:
        df_1m = self._sample_df(periods=8)
        streams = self._sample_streams(periods=8)
        entry = build_prepared_dataset(
            symbol="ETHUSDT",
            start=df_1m["open_time"].min(),
            end=df_1m["open_time"].max(),
            timeframes=["1m", "5m"],
            price_source="LAST",
            macd_impl="TRADINGVIEW",
            adx_impl="TRADINGVIEW",
            required_fields={"ema_1", "ema_2", "ema_stack"},
            ema_settings={"defaults": {"ema_1_length": 9, "ema_2_length": 21}},
            market_state_thresholds=None,
            df_1m_full=df_1m,
            streams_full=streams,
        )
        with tempfile.TemporaryDirectory() as tmp:
            save_prepared_dataset_to_disk(tmp, entry)
            found = find_covering_prepared_dataset_on_disk(
                tmp,
                symbol="ETHUSDT",
                start=pd.Timestamp("2026-01-01T00:01:00Z"),
                end=pd.Timestamp("2026-01-01T00:06:00Z"),
                timeframes=["1m"],
                price_source="LAST",
                macd_impl="TRADINGVIEW",
                adx_impl="TRADINGVIEW",
                required_fields={"ema_1", "ema_2", "ema_stack"},
                ema_settings={"defaults": {"ema_1_length": 50, "ema_2_length": 200}},
                market_state_thresholds=None,
            )
            self.assertIsNone(found)


if __name__ == "__main__":
    unittest.main()
