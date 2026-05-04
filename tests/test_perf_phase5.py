from __future__ import annotations

import unittest
from collections import Counter
from unittest.mock import patch

import pandas as pd

from app.backtest import BacktestConfig, _planned_eval_tabs, run_backtest


class Phase5PerfSafetyTests(unittest.TestCase):
    def test_planned_eval_tabs_flat_only_entries_without_sequence(self) -> None:
        plan = _planned_eval_tabs(
            active_tabs={
                "Long Entry": True,
                "Long Exit": True,
                "Short Entry": True,
                "Short Exit": True,
            },
            sequence_tabs={
                "Long Entry": False,
                "Long Exit": False,
                "Short Entry": False,
                "Short Exit": False,
            },
            pos_side=None,
            has_scheduled_order=False,
            allow_reverse=False,
            default_eval_impl=True,
        )
        self.assertEqual(
            plan,
            {
                "Long Entry": True,
                "Long Exit": False,
                "Short Entry": True,
                "Short Exit": False,
            },
        )

    def test_planned_eval_tabs_long_position_only_long_exit_without_reverse(self) -> None:
        plan = _planned_eval_tabs(
            active_tabs={
                "Long Entry": True,
                "Long Exit": True,
                "Short Entry": True,
                "Short Exit": True,
            },
            sequence_tabs={
                "Long Entry": False,
                "Long Exit": False,
                "Short Entry": False,
                "Short Exit": False,
            },
            pos_side="LONG",
            has_scheduled_order=False,
            allow_reverse=False,
            default_eval_impl=True,
        )
        self.assertEqual(
            plan,
            {
                "Long Entry": False,
                "Long Exit": True,
                "Short Entry": False,
                "Short Exit": False,
            },
        )

    def _sample_stream(self) -> pd.DataFrame:
        idx = pd.date_range("2026-01-01T00:00:00Z", periods=5, freq="1min", tz="UTC")
        return pd.DataFrame(
            {
                "open": [100.0, 101.0, 102.0, 103.0, 104.0],
                "high": [100.5, 101.5, 102.5, 103.5, 104.5],
                "low": [99.5, 100.5, 101.5, 102.5, 103.5],
                "close": [100.0, 101.0, 102.0, 103.0, 104.0],
                "price": [100.0, 101.0, 102.0, 103.0, 104.0],
                "open_time": [ts - pd.Timedelta(minutes=1) + pd.Timedelta(milliseconds=1) for ts in idx],
                "close_time": list(idx),
                "volume": [10.0] * len(idx),
                "macd": [0.0, 0.1, 0.2, 0.3, 0.4],
                "signal": [0.0, 0.05, 0.1, 0.15, 0.2],
            },
            index=idx,
        )

    def _run_counting_backtest(self, *, sequence_long_exit: bool = False) -> Counter:
        df = self._sample_stream()
        cfg = BacktestConfig(
            initial_balance=1000.0,
            order_notional_usdt=100.0,
            fee_rate=0.0,
            slippage_bps=0.0,
            stop_loss_mode="OFF",
            take_profit_mode="OFF",
            step_timeframe="1m",
            allow_reverse=False,
        )
        marker = object()
        rules_model = {
            "Long Entry": [[marker]],
            "Long Exit": [[marker]],
            "Short Entry": [[marker]],
            "Short Exit": [[marker]],
        }
        join_mode = {name: "AND" for name in rules_model.keys()}
        group_join_mode = {
            name: (["SEQUENCE"] if (sequence_long_exit and name == "Long Exit") else ["OR"])
            for name in rules_model.keys()
        }

        calls: Counter = Counter()

        def fake_eval(tab_name, rules_model, tab_group_join_mode, group_rule_join_mode, cur_by_tf, prev_by_tf, ctx=None):
            calls[str(tab_name)] += 1
            return False

        fake_eval.__module__ = "app.rules"

        with patch("app.backtest.eval_tab_generic", new=fake_eval):
            run_backtest(
                symbol="BTCUSDT",
                streams_full={"1m": df.copy()},
                start=df.index[0],
                end=df.index[-1],
                rules_model=rules_model,
                tab_group_join_mode=join_mode,
                group_rule_join_mode=group_join_mode,
                cfg=cfg,
                df_1m_full=df.copy(),
            )
        return calls

    def test_bar_backtest_skips_irrelevant_exit_tabs_while_flat(self) -> None:
        calls = self._run_counting_backtest(sequence_long_exit=False)
        self.assertGreater(calls["Long Entry"], 0)
        self.assertGreater(calls["Short Entry"], 0)
        self.assertEqual(calls["Long Exit"], 0)
        self.assertEqual(calls["Short Exit"], 0)

    def test_sequence_tabs_still_evaluate_while_flat_for_safety(self) -> None:
        calls = self._run_counting_backtest(sequence_long_exit=True)
        self.assertGreater(calls["Long Entry"], 0)
        self.assertGreater(calls["Short Entry"], 0)
        self.assertGreater(calls["Long Exit"], 0)
        self.assertEqual(calls["Short Exit"], 0)

    def test_bar_backtest_reverses_into_short_when_opposite_entry_matches_exit_bar(self) -> None:
        idx = pd.date_range("2026-01-01T00:00:00Z", periods=6, freq="1min", tz="UTC")
        df = pd.DataFrame(
            {
                "open": [100.0, 101.0, 102.0, 103.0, 104.0, 105.0],
                "high": [100.5, 101.5, 102.5, 103.5, 104.5, 105.5],
                "low": [99.5, 100.5, 101.5, 102.5, 103.5, 104.5],
                "close": [100.0, 101.0, 102.0, 103.0, 104.0, 105.0],
                "price": [100.0, 101.0, 102.0, 103.0, 104.0, 105.0],
                "open_time": [ts - pd.Timedelta(minutes=1) + pd.Timedelta(milliseconds=1) for ts in idx],
                "close_time": list(idx),
                "volume": [10.0] * len(idx),
                "macd": [0.0] * len(idx),
                "signal": [0.0] * len(idx),
            },
            index=idx,
        )
        cfg = BacktestConfig(
            initial_balance=1000.0,
            order_notional_usdt=100.0,
            fee_rate=0.0,
            slippage_bps=0.0,
            stop_loss_mode="OFF",
            take_profit_mode="OFF",
            step_timeframe="1m",
            allow_reverse=True,
            skip_if_both_entries=True,
        )
        marker = object()
        rules_model = {
            "Long Entry": [[marker]],
            "Long Exit": [[marker]],
            "Short Entry": [[marker]],
            "Short Exit": [[marker]],
        }
        join_mode = {name: "AND" for name in rules_model.keys()}
        group_join_mode = {name: ["OR"] for name in rules_model.keys()}

        def fake_eval(tab_name, rules_model, tab_group_join_mode, group_rule_join_mode, cur_by_tf, prev_by_tf, ctx=None):
            step = int(getattr(ctx, "step_index", -1) or 0)
            if tab_name == "Long Entry":
                return step == 0
            if tab_name == "Long Exit":
                return step == 1
            if tab_name == "Short Entry":
                return step == 1
            if tab_name == "Short Exit":
                return step == 2
            return False

        fake_eval.__module__ = "app.rules"

        with patch("app.backtest.eval_tab_generic", new=fake_eval):
            res = run_backtest(
                symbol="BTCUSDT",
                streams_full={"1m": df.copy()},
                start=df.index[0],
                end=df.index[-1],
                rules_model=rules_model,
                tab_group_join_mode=join_mode,
                group_rule_join_mode=group_join_mode,
                cfg=cfg,
                df_1m_full=df.copy(),
            )

        self.assertEqual(len(res.trades), 2)
        self.assertEqual(res.trades[0].side, "LONG")
        self.assertEqual(res.trades[0].exit_reason, "Reverse to Short")
        self.assertEqual(res.trades[1].side, "SHORT")
        self.assertEqual(res.trades[1].entry_reason, "Reverse to Short")
        self.assertEqual(res.trades[1].exit_reason, "Short Exit")


if __name__ == "__main__":
    unittest.main()
