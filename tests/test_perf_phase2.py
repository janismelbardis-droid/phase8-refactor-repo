from __future__ import annotations

import json
import tempfile
import unittest
from unittest.mock import patch

import pandas as pd

from app.constants import INDICATOR_CACHE_VERSION
from app.indicators_streaming import _indicator_cache_paths, simulate_multitf_indicators
from app.rules import Rule
from app.strategy_requirements import compile_stream_requirements
from app.utils_time import required_warmup_minutes


class _NoHeavyState:
    def update(self, *args, **kwargs):
        raise AssertionError("heavy state update should not run for core-only requests")

    def snapshot(self):
        raise AssertionError("heavy state snapshot should not run for core-only requests")


class Phase2PerfSafetyTests(unittest.TestCase):
    def _sample_df(self) -> pd.DataFrame:
        idx = pd.date_range("2026-01-01T00:00:00Z", periods=8, freq="1min", tz="UTC")
        return pd.DataFrame(
            {
                "open_time": idx,
                "close_time": idx + pd.Timedelta(seconds=59),
                "open": [100.0 + i for i in range(len(idx))],
                "high": [100.4 + i for i in range(len(idx))],
                "low": [99.6 + i for i in range(len(idx))],
                "close": [100.2 + i for i in range(len(idx))],
                "volume": [10.0] * len(idx),
            }
        )

    def test_dependency_warmup_is_smaller_for_simple_rules(self) -> None:
        rules = {
            "Long Entry": [[Rule(timeframe="1m", mode="state", field="macd", op=">", value=0)]],
            "Long Exit": [],
            "Short Entry": [],
            "Short Exit": [],
        }
        spec = compile_stream_requirements(rules, include_plot_defaults=True)
        full_req = required_warmup_minutes(["1m"])
        selective_req = required_warmup_minutes(["1m"], required_fields=spec)
        self.assertGreater(full_req, selective_req)
        self.assertGreaterEqual(selective_req, 30)

    def test_core_only_requests_skip_heavy_indicator_updates(self) -> None:
        df = self._sample_df()
        with patch("app.indicators_streaming.make_frama_state", return_value=_NoHeavyState()), \
             patch("app.indicators_streaming.make_vidya_state", return_value=_NoHeavyState()), \
             patch("app.indicators_streaming.make_range_filter_state", return_value=_NoHeavyState()), \
             patch("app.indicators_streaming._augment_stream_with_market_state", side_effect=AssertionError("market state augment should not run")):
            streams = simulate_multitf_indicators(
                df,
                ["1m"],
                required_fields={"macd", "signal"},
                use_cache=False,
            )
        out = streams["1m"]
        self.assertIn("macd", out.columns)
        self.assertIn("signal", out.columns)
        self.assertNotIn("market_state", out.columns)

    def test_default_requests_skip_market_state_augmentation(self) -> None:
        df = self._sample_df()
        with patch(
            "app.indicators_streaming._augment_stream_with_market_state",
            side_effect=AssertionError("market state augment should not run for default requests"),
        ):
            streams = simulate_multitf_indicators(
                df,
                ["1m"],
                required_fields=None,
                use_cache=False,
            )
        out = streams["1m"]
        self.assertIn("macd", out.columns)
        self.assertNotIn("market_state", out.columns)

    def test_covering_full_cache_can_serve_selective_request(self) -> None:
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
                    "1m__ms_cross": [False] * len(idx),
                    "1m__macd0_cross": [False] * len(idx),
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
                        "required_families": ["core", "adx", "atr", "frama", "vidya", "range_filter", "market_aug"],
                    },
                    f,
                )

            df_req = self._sample_df().iloc[2:6].reset_index(drop=True)
            with patch("app.indicators_streaming.pd.read_parquet", side_effect=lambda path: pd.read_pickle(path)), patch(
                "app.indicators_streaming.simulate_multitf_indicators_fast",
                side_effect=AssertionError("fast compute should not run when a covering full cache exists"),
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
                    required_fields={"price"},
                )

            out = streams["1m"]
            self.assertEqual(list(out.index), list(idx[2:6]))
            self.assertEqual(float(out.iloc[0]["price"]), 102.0)


if __name__ == "__main__":
    unittest.main()
