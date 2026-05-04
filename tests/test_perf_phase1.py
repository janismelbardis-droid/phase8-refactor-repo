from __future__ import annotations

import json
import os
import tempfile
import unittest
from unittest.mock import patch

import pandas as pd
import requests

from app.constants import INDICATOR_CACHE_VERSION
from app.data_binance import _exact_1m_window_paths, fetch_klines_1m_futures
from app.indicators_streaming import _indicator_cache_paths, simulate_multitf_indicators


class _FakeResponse:
    def __init__(self, payload, status_code: int = 200):
        self._payload = payload
        self.status_code = status_code

    def raise_for_status(self):
        if self.status_code >= 400:
            raise requests.HTTPError(f"status={self.status_code}")

    def json(self):
        return self._payload


class Phase1PerfSafetyTests(unittest.TestCase):
    def test_fetch_klines_reuses_covering_cache_and_persists_slice(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            idx = pd.date_range("2026-01-01T00:00:00Z", periods=10, freq="1min", tz="UTC")
            df = pd.DataFrame(
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
            cache_name = "FUTURES_BTCUSDT_1m_LAST_20260101000000_20260101000900.csv"
            df.to_csv(os.path.join(tmp, cache_name), index=False)

            start = pd.Timestamp("2026-01-01T00:02:00Z")
            end = pd.Timestamp("2026-01-01T00:05:00Z")

            with patch("requests.Session.get", side_effect=AssertionError("network should not be used")):
                out = fetch_klines_1m_futures("BTCUSDT", start, end, cache_dir=tmp, price_source="LAST")

            self.assertEqual(list(out["open_time"]), list(idx[2:6]))
            # _write_cached_1m_frame prefers parquet when the engine is available; csv is fallback only.
            pq_path, csv_path = _exact_1m_window_paths(tmp, "BTCUSDT", start, end, "LAST")
            self.assertTrue(
                os.path.exists(pq_path) or os.path.exists(csv_path),
                "expected sliced window to be persisted as .parquet or .csv",
            )

    def test_fetch_klines_retries_transient_connection_errors(self) -> None:
        start = pd.Timestamp("2026-01-01T00:00:00Z")
        end = pd.Timestamp("2026-01-01T00:00:00Z")
        payload = [[
            int(start.timestamp() * 1000),
            "100.0",
            "101.0",
            "99.0",
            "100.5",
            "12.0",
            int((start + pd.Timedelta(seconds=59)).timestamp() * 1000),
        ]]
        calls = {"count": 0}

        def fake_get(_self, url, params=None, timeout=None):
            calls["count"] += 1
            if calls["count"] < 3:
                raise requests.exceptions.ConnectionError("boom")
            return _FakeResponse(payload)

        with tempfile.TemporaryDirectory() as tmp, patch("requests.Session.get", new=fake_get), patch("time.sleep", return_value=None):
            out = fetch_klines_1m_futures("BTCUSDT", start, end, cache_dir=tmp, price_source="LAST")

        self.assertEqual(calls["count"], 3)
        self.assertEqual(len(out), 1)
        self.assertEqual(float(out.iloc[0]["close"]), 100.5)

    def test_indicator_cache_reuses_covering_window(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            symbol = "BTCUSDT"
            timeframes = ["1m"]
            price_source = "LAST"
            macd_impl = "TRADINGVIEW"
            adx_impl = "TRADINGVIEW"
            cover_start = pd.Timestamp("2026-01-01T00:00:00Z")
            cover_end = pd.Timestamp("2026-01-01T00:09:00Z")
            req_start = pd.Timestamp("2026-01-01T00:02:00Z")
            req_end = pd.Timestamp("2026-01-01T00:05:00Z")

            pq_cover, meta_cover = _indicator_cache_paths(
                tmp, symbol, cover_start, cover_end, timeframes, price_source, macd_impl, adx_impl
            )
            idx = pd.date_range(cover_start, cover_end, freq="1min", tz="UTC")
            wide = pd.DataFrame(
                {
                    "1m__price": [100.0 + i for i in range(len(idx))],
                    "1m__macd": [0.1 * i for i in range(len(idx))],
                    "1m__signal": [0.05 * i for i in range(len(idx))],
                    "1m__ema_1": [100.0 + i for i in range(len(idx))],
                    "1m__ema_2": [99.5 + i for i in range(len(idx))],
                    "1m__ema_1_slope": [0.5] * len(idx),
                    "1m__ema_2_slope": [0.25] * len(idx),
                    "1m__ema_spread": [0.5] * len(idx),
                    "1m__ema_spread_pct": [0.5] * len(idx),
                    "1m__ema_stack": ["BUY"] * len(idx),
                    "1m__ema_1_angle_state": ["UP"] * len(idx),
                    "1m__ema_2_angle_state": ["UP"] * len(idx),
                    "1m__ema_cross_up_i": [0] * len(idx),
                    "1m__ema_cross_down_i": [0] * len(idx),
                    "1m__open": [100.0 + i for i in range(len(idx))],
                    "1m__high": [100.5 + i for i in range(len(idx))],
                    "1m__low": [99.5 + i for i in range(len(idx))],
                    "1m__close": [100.2 + i for i in range(len(idx))],
                    "1m__volume": [10.0] * len(idx),
                    "1m__close_time": idx,
                },
                index=idx,
            )
            wide.to_pickle(pq_cover)
            with open(meta_cover, "w", encoding="utf-8") as f:
                json.dump(
                    {
                        "report_version": INDICATOR_CACHE_VERSION,
                        "symbol": symbol,
                        "start_utc": cover_start.isoformat(),
                        "end_utc": cover_end.isoformat(),
                        "timeframes": timeframes,
                        "price_source": price_source,
                        "macd_impl": macd_impl,
                        "adx_impl": adx_impl,
                        "required_families": ["adx", "atr", "ema", "frama", "macd_lines", "macd_states", "market_aug", "ohlcv", "range_filter", "rsi", "stoch_rsi", "vidya"],
                        "ema_profile_signature": [["default", 9, 21], ["1m", 9, 21]],
                        "rsi_profile_signature": [["default", 14, 3], ["1m", 14, 3]],
                    },
                    f,
                )

            df_req = pd.DataFrame(
                {
                    "open_time": pd.date_range(req_start, req_end, freq="1min", tz="UTC"),
                    "close_time": pd.date_range(req_start, req_end, freq="1min", tz="UTC") + pd.Timedelta(seconds=59),
                    "open": [1.0] * 4,
                    "high": [1.0] * 4,
                    "low": [1.0] * 4,
                    "close": [1.0] * 4,
                    "volume": [1.0] * 4,
                }
            )

            with patch("app.indicators_streaming.pd.read_parquet", side_effect=lambda path: pd.read_pickle(path)), patch(
                "app.indicators_streaming.simulate_multitf_indicators_fast",
                side_effect=AssertionError("fast compute should not run when a covering cache exists"),
            ):
                streams = simulate_multitf_indicators(
                    df_req,
                    timeframes,
                    cache_dir=tmp,
                    symbol=symbol,
                    start_utc=req_start,
                    end_utc=req_end,
                    price_source=price_source,
                    macd_impl=macd_impl,
                    adx_impl=adx_impl,
                )

            self.assertIn("1m", streams)
            out = streams["1m"]
            self.assertEqual(list(out.index), list(idx[2:6]))
            self.assertEqual(float(out.iloc[0]["price"]), 102.0)


if __name__ == "__main__":
    unittest.main()
