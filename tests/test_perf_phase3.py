from __future__ import annotations

import unittest

import pandas as pd

from app.backtest import BacktestConfig, BacktestResult
from app.domain.market_state import MarketStateThresholds
from app.prepared_dataset import (
    build_prepared_dataset,
    find_covering_prepared_dataset,
    market_threshold_signature,
    slice_df_1m_window,
    slice_streams_window,
)
from app.research.replay import build_backtest_replay_plan
from app.rules import Rule


class Phase3PerfSafetyTests(unittest.TestCase):
    def _sample_df(self, start: str = "2026-01-01T00:00:00Z", periods: int = 10) -> pd.DataFrame:
        idx = pd.date_range(start, periods=periods, freq="1min", tz="UTC")
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

    def _sample_streams(self, start: str = "2026-01-01T00:00:00Z", periods: int = 10):
        idx = pd.date_range(start, periods=periods, freq="1min", tz="UTC")
        df = pd.DataFrame(
            {
                "price": [100.0 + i for i in range(len(idx))],
                "macd": [0.1 * i for i in range(len(idx))],
                "signal": [0.05 * i for i in range(len(idx))],
            },
            index=idx,
        )
        return {"1m": df, "5m": df.resample("5min").last()}

    def test_prepared_dataset_covering_lookup_allows_subset_request(self) -> None:
        df_1m = self._sample_df()
        streams = self._sample_streams()
        entry = build_prepared_dataset(
            symbol="BTCUSDT",
            start=pd.Timestamp("2026-01-01T00:00:00Z"),
            end=pd.Timestamp("2026-01-01T00:09:00Z"),
            timeframes=["1m", "5m"],
            price_source="LAST",
            macd_impl="TRADINGVIEW",
            adx_impl="TRADINGVIEW",
            required_fields={"macd", "signal", "adx", "atr"},
            market_state_thresholds=None,
            df_1m_full=df_1m,
            streams_full=streams,
        )
        found = find_covering_prepared_dataset(
            [entry],
            symbol="BTCUSDT",
            start=pd.Timestamp("2026-01-01T00:02:00Z"),
            end=pd.Timestamp("2026-01-01T00:05:00Z"),
            timeframes=["1m"],
            price_source="LAST",
            macd_impl="TRADINGVIEW",
            adx_impl="TRADINGVIEW",
            required_fields={"macd", "signal"},
            market_state_thresholds=None,
        )
        self.assertIs(found, entry)

        sliced_df = slice_df_1m_window(found.df_1m_full, pd.Timestamp("2026-01-01T00:02:00Z"), pd.Timestamp("2026-01-01T00:05:00Z"))
        self.assertEqual(len(sliced_df), 4)
        self.assertEqual(float(sliced_df.iloc[0]["close"]), 102.2)

        sliced_streams = slice_streams_window(found.streams_full, pd.Timestamp("2026-01-01T00:02:00Z"), pd.Timestamp("2026-01-01T00:05:00Z"), timeframes=["1m"])
        self.assertEqual(list(sliced_streams.keys()), ["1m"])
        self.assertEqual(len(sliced_streams["1m"]), 4)
        self.assertEqual(float(sliced_streams["1m"].iloc[-1]["price"]), 105.0)

    def test_market_aug_reuse_requires_matching_thresholds(self) -> None:
        df_1m = self._sample_df()
        streams = self._sample_streams()
        entry = build_prepared_dataset(
            symbol="BTCUSDT",
            start=pd.Timestamp("2026-01-01T00:00:00Z"),
            end=pd.Timestamp("2026-01-01T00:09:00Z"),
            timeframes=["1m"],
            price_source="LAST",
            macd_impl="TRADINGVIEW",
            adx_impl="TRADINGVIEW",
            required_fields={"market_state"},
            market_state_thresholds=MarketStateThresholds(direction_long_min=4),
            df_1m_full=df_1m,
            streams_full=streams,
        )
        self.assertIsNotNone(market_threshold_signature(MarketStateThresholds(direction_long_min=4)))
        found = find_covering_prepared_dataset(
            [entry],
            symbol="BTCUSDT",
            start=pd.Timestamp("2026-01-01T00:02:00Z"),
            end=pd.Timestamp("2026-01-01T00:05:00Z"),
            timeframes=["1m"],
            price_source="LAST",
            macd_impl="TRADINGVIEW",
            adx_impl="TRADINGVIEW",
            required_fields={"market_state"},
            market_state_thresholds=MarketStateThresholds(direction_long_min=5),
        )
        self.assertIsNone(found)

    def test_ema_reuse_requires_matching_lengths(self) -> None:
        df_1m = self._sample_df()
        streams = self._sample_streams()
        entry = build_prepared_dataset(
            symbol="BTCUSDT",
            start=pd.Timestamp("2026-01-01T00:00:00Z"),
            end=pd.Timestamp("2026-01-01T00:09:00Z"),
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
        found = find_covering_prepared_dataset(
            [entry],
            symbol="BTCUSDT",
            start=pd.Timestamp("2026-01-01T00:02:00Z"),
            end=pd.Timestamp("2026-01-01T00:05:00Z"),
            timeframes=["1m"],
            price_source="LAST",
            macd_impl="TRADINGVIEW",
            adx_impl="TRADINGVIEW",
            required_fields={"ema_1", "ema_2", "ema_stack"},
            ema_settings={"defaults": {"ema_1_length": 50, "ema_2_length": 200}},
            market_state_thresholds=None,
        )
        self.assertIsNone(found)

    def test_replay_plan_accepts_cost_only_config_updates(self) -> None:
        rules = {
            "Long Entry": [[Rule(timeframe="1m", mode="state", field="macd", op=">", value=0)]],
            "Long Exit": [],
            "Short Entry": [],
            "Short Exit": [],
        }
        replay_context = {
            "symbol": "BTCUSDT",
            "start": pd.Timestamp("2026-01-01T00:00:00Z"),
            "end": pd.Timestamp("2026-01-01T01:00:00Z"),
            "rules_model": rules,
            "tab_group_join_mode": {"Long Entry": "AND", "Long Exit": "AND", "Short Entry": "AND", "Short Exit": "AND"},
            "group_rule_join_mode": {"Long Entry": ["AND"], "Long Exit": [], "Short Entry": [], "Short Exit": []},
            "entry_filters": {},
            "streams_full": {"1m": pd.DataFrame(index=pd.date_range("2026-01-01T00:00:00Z", periods=2, freq="1min", tz="UTC"))},
            "df_1m_full": self._sample_df(periods=2),
        }
        result = BacktestResult(
            config=BacktestConfig(),
            start=pd.Timestamp("2026-01-01T00:00:00Z"),
            end=pd.Timestamp("2026-01-01T01:00:00Z"),
            trades=[],
            equity_curve=pd.DataFrame(),
            summary={},
            replay_context=replay_context,
        )
        next_cfg = BacktestConfig(initial_balance=2500.0, order_notional_usdt=250.0, fee_rate=0.0002, slippage_bps=4.0)
        plan = build_backtest_replay_plan(
            result,
            next_cfg=next_cfg,
            symbol="BTCUSDT",
            start=pd.Timestamp("2026-01-01T00:00:00Z"),
            end=pd.Timestamp("2026-01-01T01:00:00Z"),
            rules_model=rules,
            tab_group_join_mode=replay_context["tab_group_join_mode"],
            group_rule_join_mode=replay_context["group_rule_join_mode"],
            entry_filters={},
        )
        self.assertTrue(plan.eligible)
        self.assertIn("cost-config", plan.reason)
        self.assertEqual(plan.config_updates["initial_balance"], 2500.0)
        self.assertEqual(plan.config_updates["order_notional_usdt"], 250.0)


if __name__ == "__main__":
    unittest.main()
