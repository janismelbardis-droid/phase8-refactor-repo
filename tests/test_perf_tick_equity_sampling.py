from __future__ import annotations

import unittest
from typing import List
from unittest.mock import patch

import pandas as pd

from app.backtest import BacktestConfig, run_backtest_tick
from app.rules import Rule


class TickEquitySamplingPerfTests(unittest.TestCase):
    def _sample_stream(self) -> pd.DataFrame:
        idx = pd.date_range("2026-01-01T00:00:00Z", periods=8, freq="1min", tz="UTC")
        return pd.DataFrame(
            {
                "open": [100.0, 100.2, 100.5, 101.0, 99.0, 98.0, 102.0, 103.0],
                "high": [100.2, 100.4, 100.7, 101.2, 99.2, 98.2, 102.2, 103.2],
                "low": [99.8, 100.0, 100.3, 100.8, 98.8, 97.8, 101.8, 102.8],
                "close": [100.0, 100.2, 100.5, 101.0, 99.0, 98.0, 102.0, 103.0],
                "price": [100.0, 100.2, 100.5, 101.0, 99.0, 98.0, 102.0, 103.0],
                "open_time": [ts - pd.Timedelta(minutes=1) + pd.Timedelta(milliseconds=1) for ts in idx],
                "close_time": list(idx),
                "volume": [10.0] * len(idx),
                "macd": [-1.0, -0.8, -0.4, 0.3, 0.1, 0.0, 0.2, 0.4],
                "signal": [-0.9, -0.7, -0.3, 0.1, 0.05, 0.02, 0.1, 0.2],
                "ms": ["RED", "RED", "RED", "GREEN", "GREEN", "GREEN", "GREEN", "GREEN"],
            },
            index=idx,
        )

    def _cfg(self, *, stride: int = 1) -> BacktestConfig:
        return BacktestConfig(
            initial_balance=1000.0,
            order_notional_usdt=100.0,
            fee_rate=0.0,
            slippage_bps=0.0,
            stop_loss_mode="OFF",
            take_profit_mode="OFF",
            step_timeframe="1m",
            equity_curve_stride=stride,
        )

    def _joins(self, rules_model):
        tab_join = {name: "AND" for name in rules_model.keys()}
        group_join = {name: (["OR"] if rules_model[name] else []) for name in rules_model.keys()}
        return tab_join, group_join

    def _ticks(self, df: pd.DataFrame) -> List[dict]:
        ticks = []
        for ts, price in zip(df.index[2:], df["close"].iloc[2:]):
            ticks.append({"T": int((ts + pd.Timedelta(seconds=1)).value // 1_000_000), "p": str(float(price))})
        return ticks

    def test_tick_prepared_path_equity_stride_reduces_curve_density(self) -> None:
        df = self._sample_stream()
        rules_model = {
            "Long Entry": [],
            "Long Exit": [],
            "Short Entry": [],
            "Short Exit": [],
        }
        tab_join, group_join = self._joins(rules_model)
        ticks = self._ticks(df)

        with patch("app.backtest.iter_aggtrades_futures_daily", return_value=ticks):
            result_full = run_backtest_tick(
                symbol="BTCUSDT",
                df_1m_full=df.copy(),
                start=df.index[0],
                end=df.index[-1],
                tfs=["1m"],
                rules_model=rules_model,
                tab_group_join_mode=tab_join,
                group_rule_join_mode=group_join,
                cfg=self._cfg(stride=1),
                streams_full={"1m": df.copy()},
            )

        with patch("app.backtest.iter_aggtrades_futures_daily", return_value=ticks):
            result_sparse = run_backtest_tick(
                symbol="BTCUSDT",
                df_1m_full=df.copy(),
                start=df.index[0],
                end=df.index[-1],
                tfs=["1m"],
                rules_model=rules_model,
                tab_group_join_mode=tab_join,
                group_rule_join_mode=group_join,
                cfg=self._cfg(stride=3),
                streams_full={"1m": df.copy()},
            )

        self.assertEqual(len(result_full.trades), 0)
        self.assertEqual(len(result_sparse.trades), 0)
        self.assertGreater(len(result_full.equity_curve), len(result_sparse.equity_curve))
        self.assertEqual(len(result_full.equity_curve), 6)
        self.assertEqual(len(result_sparse.equity_curve), 2)
        self.assertEqual(float(result_full.summary.get("max_drawdown", 0.0)), 0.0)
        self.assertEqual(float(result_sparse.summary.get("max_drawdown", 0.0)), 0.0)

    def test_tick_prepared_path_stride_keeps_summary_metrics_exact(self) -> None:
        df = self._sample_stream()
        rules_model = {
            "Long Entry": [[Rule(timeframe="1m", mode="event", field="ms_cross", op="CROSS", value="UP")]],
            "Long Exit": [],
            "Short Entry": [],
            "Short Exit": [],
        }
        tab_join, group_join = self._joins(rules_model)
        ticks = self._ticks(df)

        with patch("app.backtest.iter_aggtrades_futures_daily", return_value=ticks):
            result_full = run_backtest_tick(
                symbol="BTCUSDT",
                df_1m_full=df.copy(),
                start=df.index[0],
                end=df.index[-1],
                tfs=["1m"],
                rules_model=rules_model,
                tab_group_join_mode=tab_join,
                group_rule_join_mode=group_join,
                cfg=self._cfg(stride=1),
                streams_full={"1m": df.copy()},
            )

        with patch("app.backtest.iter_aggtrades_futures_daily", return_value=ticks):
            result_sparse = run_backtest_tick(
                symbol="BTCUSDT",
                df_1m_full=df.copy(),
                start=df.index[0],
                end=df.index[-1],
                tfs=["1m"],
                rules_model=rules_model,
                tab_group_join_mode=tab_join,
                group_rule_join_mode=group_join,
                cfg=self._cfg(stride=3),
                streams_full={"1m": df.copy()},
            )

        self.assertEqual(len(result_full.trades), 1)
        self.assertEqual(len(result_sparse.trades), 1)
        self.assertEqual(result_full.trades[0].entry_time, result_sparse.trades[0].entry_time)
        self.assertAlmostEqual(result_full.trades[0].entry_price, result_sparse.trades[0].entry_price, places=9)
        self.assertAlmostEqual(float(result_full.summary["ending_balance"]), float(result_sparse.summary["ending_balance"]), places=9)
        self.assertAlmostEqual(float(result_full.summary["net_profit"]), float(result_sparse.summary["net_profit"]), places=9)
        self.assertAlmostEqual(float(result_full.summary["max_drawdown"]), float(result_sparse.summary["max_drawdown"]), places=9)
        self.assertAlmostEqual(float(result_full.summary["max_drawdown_pct"]), float(result_sparse.summary["max_drawdown_pct"]), places=9)
        self.assertGreater(len(result_full.equity_curve), len(result_sparse.equity_curve))


if __name__ == "__main__":
    unittest.main()
