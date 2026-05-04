from __future__ import annotations

import unittest

import pandas as pd

from app.backtest import BacktestConfig, run_backtest
from app.research.replay import replay_backtest_result
from app.rules import Rule


class Phase11FastBacktestTests(unittest.TestCase):
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

    def _rules(self):
        return {
            "Long Entry": [[Rule(timeframe="1m", mode="event", field="ms_cross", op="CROSS", value="UP")]],
            "Long Exit": [[Rule(timeframe="1m", mode="state", field="macd", op=">", value=0.75)]],
            "Short Entry": [],
            "Short Exit": [],
        }

    def _joins(self, rules_model):
        tab_join = {name: "AND" for name in rules_model.keys()}
        group_join = {name: (["OR"] if rules_model[name] else []) for name in rules_model.keys()}
        return tab_join, group_join

    def test_fast_mode_keeps_trades_light_and_samples_equity_curve(self) -> None:
        df = self._sample_stream()
        rules_model = self._rules()
        tab_join, group_join = self._joins(rules_model)
        cfg = BacktestConfig(
            initial_balance=1000.0,
            order_notional_usdt=100.0,
            fee_rate=0.0,
            slippage_bps=0.0,
            stop_loss_mode="OFF",
            take_profit_mode="OFF",
            step_timeframe="1m",
        )
        result = run_backtest(
            symbol="BTCUSDT",
            streams_full={"1m": df.copy()},
            df_1m_full=df.copy(),
            start=df.index[0],
            end=df.index[-1],
            rules_model=rules_model,
            tab_group_join_mode=tab_join,
            group_rule_join_mode=group_join,
            cfg=cfg,
            capture_trade_details=False,
            equity_curve_stride=3,
        )
        self.assertEqual(len(result.trades), 1)
        trade = result.trades[0]
        self.assertEqual(trade.entry_snapshot, {})
        self.assertEqual(trade.exit_snapshot, {})
        self.assertEqual(trade.entry_rule_trace, "")
        self.assertEqual(trade.exit_rule_trace, "")
        self.assertIsNotNone(trade.entry_row_index)
        self.assertIsNotNone(trade.exit_row_index)
        self.assertLess(len(result.equity_curve), len(df))

    def test_replay_can_keep_trades_light_too(self) -> None:
        df = self._sample_stream()
        rules_model = self._rules()
        tab_join, group_join = self._joins(rules_model)
        cfg = BacktestConfig(
            initial_balance=1000.0,
            order_notional_usdt=100.0,
            fee_rate=0.0,
            slippage_bps=0.0,
            stop_loss_mode="OFF",
            take_profit_mode="OFF",
            step_timeframe="1m",
        )
        base = run_backtest(
            symbol="BTCUSDT",
            streams_full={"1m": df.copy()},
            df_1m_full=df.copy(),
            start=df.index[0],
            end=df.index[-1],
            rules_model=rules_model,
            tab_group_join_mode=tab_join,
            group_rule_join_mode=group_join,
            cfg=cfg,
            capture_trade_details=True,
            equity_curve_stride=1,
        )
        replayed = replay_backtest_result(base, cfg=cfg, capture_trade_details=False, equity_curve_stride=4)
        self.assertEqual(len(replayed.trades), 1)
        trade = replayed.trades[0]
        self.assertEqual(trade.entry_snapshot, {})
        self.assertEqual(trade.exit_snapshot, {})
        self.assertEqual(trade.entry_rule_trace, "")
        self.assertEqual(trade.exit_rule_trace, "")
        self.assertIsNotNone(trade.entry_row_index)
        self.assertIsNotNone(trade.exit_row_index)
        self.assertLess(len(replayed.equity_curve), len(base.equity_curve))


if __name__ == "__main__":
    unittest.main()
