from __future__ import annotations

import os
import tempfile
import unittest
from unittest.mock import patch

import pandas as pd

from app.data_binance import (
    _exact_1m_window_paths,
    _local_1m_store_day_paths,
    _read_cached_1m_frame,
    fetch_klines_1m_futures,
)


class _FakeResponse:
    def __init__(self, payload, status_code: int = 200):
        self._payload = payload
        self.status_code = status_code

    def raise_for_status(self):
        if self.status_code >= 400:
            raise RuntimeError(f"status={self.status_code}")

    def json(self):
        return self._payload


class Phase8PerfSafetyTests(unittest.TestCase):
    def _sample_df(self, start: str = "2026-01-01T00:00:00Z", periods: int = 6) -> pd.DataFrame:
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

    def test_fetch_klines_uses_local_candle_store_without_network(self) -> None:
        df = self._sample_df(periods=6)
        with tempfile.TemporaryDirectory() as tmp:
            pq_path, csv_path = _local_1m_store_day_paths(tmp, "BTCUSDT", "LAST", pd.Timestamp("2026-01-01T00:00:00Z"))
            df.to_csv(csv_path, index=False)

            start = pd.Timestamp("2026-01-01T00:01:00Z")
            end = pd.Timestamp("2026-01-01T00:04:00Z")
            with patch("requests.Session.get", side_effect=AssertionError("network should not be used")):
                out = fetch_klines_1m_futures("BTCUSDT", start, end, cache_dir=tmp, price_source="LAST")

            self.assertEqual(len(out), 4)
            self.assertEqual(float(out.iloc[0]["close"]), 101.2)
            exact_parquet, exact_csv = _exact_1m_window_paths(tmp, "BTCUSDT", start, end, "LAST")
            self.assertTrue(os.path.exists(exact_parquet) or os.path.exists(exact_csv))
            self.assertTrue(os.path.exists(csv_path) or os.path.exists(pq_path))

    def test_fetch_klines_gap_fills_only_missing_minutes_and_updates_local_store(self) -> None:
        full_df = self._sample_df(periods=5)
        local_df = full_df.iloc[:3].copy()
        with tempfile.TemporaryDirectory() as tmp:
            pq_path, csv_path = _local_1m_store_day_paths(tmp, "ETHUSDT", "LAST", pd.Timestamp("2026-01-01T00:00:00Z"))
            local_df.to_csv(csv_path, index=False)

            calls = {"count": 0, "params": []}

            def fake_get(_self, url, params=None, timeout=None):
                calls["count"] += 1
                calls["params"].append(dict(params or {}))
                self.assertEqual(int(params["startTime"]), int(pd.Timestamp("2026-01-01T00:03:00Z").timestamp() * 1000))
                self.assertEqual(int(params["endTime"]), int(pd.Timestamp("2026-01-01T00:04:00Z").timestamp() * 1000))
                payload = []
                for _, row in full_df.iloc[3:].iterrows():
                    payload.append([
                        int(pd.Timestamp(row["open_time"]).timestamp() * 1000),
                        str(row["open"]),
                        str(row["high"]),
                        str(row["low"]),
                        str(row["close"]),
                        str(row["volume"]),
                        int(pd.Timestamp(row["close_time"]).timestamp() * 1000),
                    ])
                return _FakeResponse(payload)

            with patch("requests.Session.get", new=fake_get), patch("time.sleep", return_value=None):
                out = fetch_klines_1m_futures(
                    "ETHUSDT",
                    pd.Timestamp("2026-01-01T00:00:00Z"),
                    pd.Timestamp("2026-01-01T00:04:00Z"),
                    cache_dir=tmp,
                    price_source="LAST",
                )

            self.assertEqual(calls["count"], 1)
            self.assertEqual(len(out), 5)
            self.assertEqual(float(out.iloc[-1]["close"]), 104.2)
            store_path = pq_path if os.path.exists(pq_path) else csv_path
            stored = _read_cached_1m_frame(store_path)
            self.assertIsNotNone(stored)
            self.assertEqual(len(stored), 5)
            self.assertEqual(list(stored["open_time"]), list(full_df["open_time"]))


if __name__ == "__main__":
    unittest.main()
