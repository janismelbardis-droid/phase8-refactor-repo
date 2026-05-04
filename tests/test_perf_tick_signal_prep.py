from __future__ import annotations

import unittest
from collections import Counter
from unittest.mock import patch

import pandas as pd

from app.backtest import BacktestConfig, run_backtest_tick
from app.rules import Rule


class TickSignalPrepPerfTests(unittest.TestCase):
    def _sample_stream(self) -> pd.DataFrame:
        idx = pd.date_range("2026-01-01T00:00:00Z", periods=8, freq="1min", tz="UTC")
        return pd.DataFrame(
            {
                "open": [100.0, 100.5, 101.0, 101.5, 102.0, 102.5, 103.0, 103.5],
                "high": [100.2, 100.7, 101.2, 101.7, 102.2, 102.7, 103.2, 103.7],
                "low": [99.8, 100.3, 100.8, 101.3, 101.8, 102.3, 102.8, 103.3],
                "close": [100.0, 100.5, 101.0, 101.5, 102.0, 102.5, 103.0, 103.5],
                "price": [100.0, 100.5, 101.0, 101.5, 102.0, 102.5, 103.0, 103.5],
                "open_time": [ts - pd.Timedelta(minutes=1) + pd.Timedelta(milliseconds=1) for ts in idx],
                "close_time": list(idx),
                "volume": [10.0] * len(idx),
                "macd": [-1.0, -0.5, 0.2, 0.3, 0.8, 0.4, 0.2, -0.2],
                "signal": [-0.8, -0.4, 0.1, 0.2, 0.3, 0.35, 0.3, 0.1],
                "ms": ["RED", "RED", "GREEN", "GREEN", "GREEN", "GREEN", "GREEN", "RED"],
            },
            index=idx,
        )

    def _cfg(self) -> BacktestConfig:
        return BacktestConfig(
            initial_balance=1000.0,
            order_notional_usdt=100.0,
            fee_rate=0.0,
            slippage_bps=0.0,
            stop_loss_mode="OFF",
            take_profit_mode="OFF",
            step_timeframe="1m",
        )

    def _joins(self, rules_model):
        tab_join = {name: "AND" for name in rules_model.keys()}
        group_join = {name: (["OR"] if rules_model[name] else []) for name in rules_model.keys()}
        return tab_join, group_join

    def test_tick_backtest_skips_eval_snapshots_when_compiled_tabs_stay_false(self) -> None:
        df = self._sample_stream()
        rules_model = {
            "Long Entry": [[Rule(timeframe="1m", mode="state", field="macd", op=">", value=9999)]],
            "Long Exit": [],
            "Short Entry": [],
            "Short Exit": [],
        }
        tab_join, group_join = self._joins(rules_model)

        with patch("app.backtest.iter_aggtrades_futures_daily", return_value=[]), \
             patch("app.backtest._build_eval_snapshots_for_row", side_effect=AssertionError("compiled idle tick path should skip eval snapshots")):
            result = run_backtest_tick(
                symbol="BTCUSDT",
                df_1m_full=df.copy(),
                start=df.index[0],
                end=df.index[-1],
                tfs=["1m"],
                rules_model=rules_model,
                tab_group_join_mode=tab_join,
                group_rule_join_mode=group_join,
                cfg=self._cfg(),
                streams_full={"1m": df.copy()},
            )
        self.assertEqual(len(result.trades), 0)

    def test_tick_backtest_barsago_rule_falls_back_to_legacy_eval(self) -> None:
        df = self._sample_stream()
        rules_model = {
            "Long Entry": [[Rule(timeframe="1m", mode="event", field="ms_cross", op="CROSS", value="UP", bars_ago_mode="EXACT", bars_ago_n=1)]],
            "Long Exit": [],
            "Short Entry": [],
            "Short Exit": [],
        }
        tab_join, group_join = self._joins(rules_model)
        calls: Counter = Counter()

        def fake_eval(tab_name, rules_model, tab_group_join_mode, group_rule_join_mode, cur_by_tf, prev_by_tf, ctx=None):
            calls[str(tab_name)] += 1
            return False

        fake_eval.__module__ = "app.rules"

        with patch("app.backtest.iter_aggtrades_futures_daily", return_value=[]), \
             patch("app.backtest.eval_tab_generic", new=fake_eval):
            result = run_backtest_tick(
                symbol="BTCUSDT",
                df_1m_full=df.copy(),
                start=df.index[0],
                end=df.index[-1],
                tfs=["1m"],
                rules_model=rules_model,
                tab_group_join_mode=tab_join,
                group_rule_join_mode=group_join,
                cfg=self._cfg(),
                streams_full={"1m": df.copy()},
            )
        self.assertEqual(len(result.trades), 0)
        self.assertGreater(calls["Long Entry"], 0)

    def test_tick_backtest_compiled_signal_still_builds_snapshot_and_trace_on_fire(self) -> None:
        df = self._sample_stream()
        rules_model = {
            "Long Entry": [[Rule(timeframe="1m", mode="event", field="ms_cross", op="CROSS", value="UP")]],
            "Long Exit": [],
            "Short Entry": [],
            "Short Exit": [],
        }
        tab_join, group_join = self._joins(rules_model)
        ticks = [
            {"T": int((df.index[2] + pd.Timedelta(seconds=1)).value // 1_000_000), "p": "101.1"},
            {"T": int((df.index[-1] - pd.Timedelta(seconds=1)).value // 1_000_000), "p": "103.4"},
        ]

        with patch("app.backtest.iter_aggtrades_futures_daily", return_value=ticks), \
             patch("app.backtest._build_tab_rule_trace", return_value="compiled-trace"):
            result = run_backtest_tick(
                symbol="BTCUSDT",
                df_1m_full=df.copy(),
                start=df.index[0],
                end=df.index[-1],
                tfs=["1m"],
                rules_model=rules_model,
                tab_group_join_mode=tab_join,
                group_rule_join_mode=group_join,
                cfg=self._cfg(),
                streams_full={"1m": df.copy()},
            )

        self.assertEqual(len(result.trades), 1)
        trade = result.trades[0]
        self.assertEqual(trade.entry_rule_trace, "compiled-trace")
        self.assertIn("1m", trade.entry_snapshot)
        self.assertEqual(trade.entry_snapshot["1m"].get("ms"), "GREEN")


if __name__ == "__main__":
    unittest.main()
