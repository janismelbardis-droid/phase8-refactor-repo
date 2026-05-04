from __future__ import annotations

import unittest
from unittest.mock import patch

import pandas as pd

from app.backtest import BacktestConfig, run_backtest
from app.research.replay import build_backtest_replay_plan, extract_backtest_replay_context, replay_backtest_result


class BacktestReplayTests(unittest.TestCase):
    def _sample_stream(self) -> pd.DataFrame:
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
            },
            index=idx,
        )
        return df

    def _fake_eval(self, tab_name, rules_model, tab_group_join_mode, group_rule_join_mode, cur_by_tf, prev_by_tf, ctx=None):
        ts = str((cur_by_tf.get("1m") or {}).get("snapshot_time") or "")
        if tab_name == "Long Entry" and ts == "2026-01-01T00:01:00+00:00":
            return True
        return False

    def test_run_backtest_attaches_replay_context_and_replay_reuses_cached_inputs(self) -> None:
        df = self._sample_stream()
        cfg = BacktestConfig(
            initial_balance=1000.0,
            order_notional_usdt=100.0,
            fee_rate=0.0,
            slippage_bps=0.0,
            stop_loss_mode="OFF",
            take_profit_mode="PERCENT",
            take_profit_value=1.0,
            step_timeframe="1m",
        )
        rules_model = {name: [] for name in ("Long Entry", "Long Exit", "Short Entry", "Short Exit")}
        join_mode = {name: "AND" for name in rules_model.keys()}
        group_join_mode = {name: [] for name in rules_model.keys()}

        with patch("app.backtest.eval_tab_generic", side_effect=self._fake_eval), patch(
            "app.backtest._build_tab_rule_trace", side_effect=lambda *args, **kwargs: "trace"
        ):
            result = run_backtest(
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
            self.assertEqual(len(result.trades), 1)
            self.assertEqual(result.trades[0].exit_reason, "Take Profit")
            replay_context = extract_backtest_replay_context(result)
            self.assertIsInstance(replay_context, dict)
            self.assertEqual(replay_context["symbol"], "BTCUSDT")
            self.assertIn("1m", replay_context["streams_full"])

            with patch("app.backtest._freeze_backtest_streams", side_effect=AssertionError("freeze should not run during replay")), patch(
                "app.backtest._freeze_backtest_frame", side_effect=AssertionError("frame freeze should not run during replay")
            ):
                replayed = replay_backtest_result(result, config_updates={"take_profit_value": 5.0})

        self.assertEqual(len(replayed.trades), 1)
        self.assertEqual(replayed.trades[0].exit_reason, "Force Close (End)")
        self.assertGreater(replayed.trades[0].exit_time, result.trades[0].exit_time)
        self.assertIsNotNone(replayed.replay_context)

    def test_replay_plan_only_allows_exit_config_changes(self) -> None:
        df = self._sample_stream()
        cfg = BacktestConfig(
            initial_balance=1000.0,
            order_notional_usdt=100.0,
            fee_rate=0.0,
            slippage_bps=0.0,
            stop_loss_mode="OFF",
            take_profit_mode="PERCENT",
            take_profit_value=1.0,
            step_timeframe="1m",
        )
        rules_model = {name: [] for name in ("Long Entry", "Long Exit", "Short Entry", "Short Exit")}
        join_mode = {name: "AND" for name in rules_model.keys()}
        group_join_mode = {name: [] for name in rules_model.keys()}

        with patch("app.backtest.eval_tab_generic", side_effect=self._fake_eval), patch(
            "app.backtest._build_tab_rule_trace", side_effect=lambda *args, **kwargs: "trace"
        ):
            result = run_backtest(
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

        exit_only_cfg = BacktestConfig(**{**cfg.__dict__, "take_profit_value": 5.0, "break_even_after_mfe_pct": 1.0, "trailing_stop_pct": 0.75})
        exit_only_plan = build_backtest_replay_plan(
            result,
            next_cfg=exit_only_cfg,
            symbol="BTCUSDT",
            start=df.index[0],
            end=df.index[-1],
            rules_model=rules_model,
            tab_group_join_mode=join_mode,
            group_rule_join_mode=group_join_mode,
            entry_filters={},
        )
        self.assertTrue(exit_only_plan.eligible)
        self.assertEqual(exit_only_plan.reason, "exit-config-only")
        self.assertEqual(exit_only_plan.config_updates, {"take_profit_value": 5.0, "break_even_after_mfe_pct": 1.0, "trailing_stop_pct": 0.75})

        structural_cfg = BacktestConfig(**{**cfg.__dict__, "allow_reverse": False})
        structural_plan = build_backtest_replay_plan(
            result,
            next_cfg=structural_cfg,
            symbol="BTCUSDT",
            start=df.index[0],
            end=df.index[-1],
            rules_model=rules_model,
            tab_group_join_mode=join_mode,
            group_rule_join_mode=group_join_mode,
            entry_filters={},
        )
        self.assertFalse(structural_plan.eligible)
        self.assertIn("allow_reverse", structural_plan.reason)


    def test_replay_requires_context(self) -> None:
        empty = type("Dummy", (), {"replay_context": None, "config": BacktestConfig()})()
        with self.assertRaises(ValueError):
            replay_backtest_result(empty)  # type: ignore[arg-type]

    def test_break_even_after_mfe_replays_as_exit_only_change(self) -> None:
        idx = pd.date_range("2026-01-01T00:00:00Z", periods=6, freq="1min", tz="UTC")
        df = pd.DataFrame(
            {
                "open": [100.0, 100.0, 100.0, 100.0, 99.8, 99.7],
                "high": [100.2, 100.3, 101.6, 100.4, 100.0, 99.9],
                "low": [99.8, 99.9, 100.2, 99.7, 99.5, 99.4],
                "close": [100.0, 100.0, 101.2, 99.9, 99.8, 99.7],
                "price": [100.0, 100.0, 101.2, 99.9, 99.8, 99.7],
                "open_time": [ts - pd.Timedelta(minutes=1) + pd.Timedelta(milliseconds=1) for ts in idx],
                "close_time": list(idx),
                "volume": [10.0] * len(idx),
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
            break_even_after_mfe_pct=0.0,
            step_timeframe="1m",
        )
        rules_model = {name: [] for name in ("Long Entry", "Long Exit", "Short Entry", "Short Exit")}
        join_mode = {name: "AND" for name in rules_model.keys()}
        group_join_mode = {name: [] for name in rules_model.keys()}

        with patch("app.backtest.eval_tab_generic", side_effect=self._fake_eval), patch(
            "app.backtest._build_tab_rule_trace", side_effect=lambda *args, **kwargs: "trace"
        ):
            base = run_backtest(
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
            replayed = replay_backtest_result(base, config_updates={"break_even_after_mfe_pct": 1.0})
        self.assertEqual(len(replayed.trades), 1)
        self.assertEqual(replayed.trades[0].exit_reason, "Break Even")
        self.assertEqual(int(replayed.summary.get("break_even_count", 0)), 1)

    def test_trailing_stop_replays_as_exit_only_change(self) -> None:
        idx = pd.date_range("2026-01-01T00:00:00Z", periods=6, freq="1min", tz="UTC")
        df = pd.DataFrame(
            {
                "open": [100.0, 100.0, 100.0, 100.9, 100.4, 100.2],
                "high": [100.2, 100.3, 102.0, 101.1, 100.5, 100.3],
                "low": [99.8, 99.9, 100.4, 100.7, 100.1, 100.0],
                "close": [100.0, 100.0, 101.8, 100.8, 100.3, 100.1],
                "price": [100.0, 100.0, 101.8, 100.8, 100.3, 100.1],
                "open_time": [ts - pd.Timedelta(minutes=1) + pd.Timedelta(milliseconds=1) for ts in idx],
                "close_time": list(idx),
                "volume": [10.0] * len(idx),
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
            break_even_after_mfe_pct=0.0,
            trailing_stop_pct=0.0,
            step_timeframe="1m",
        )
        rules_model = {name: [] for name in ("Long Entry", "Long Exit", "Short Entry", "Short Exit")}
        join_mode = {name: "AND" for name in rules_model.keys()}
        group_join_mode = {name: [] for name in rules_model.keys()}

        with patch("app.backtest.eval_tab_generic", side_effect=self._fake_eval), patch(
            "app.backtest._build_tab_rule_trace", side_effect=lambda *args, **kwargs: "trace"
        ):
            base = run_backtest(
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
            replayed = replay_backtest_result(base, config_updates={"trailing_stop_pct": 1.0})
        self.assertEqual(len(replayed.trades), 1)
        self.assertEqual(replayed.trades[0].exit_reason, "Trailing Stop")
        self.assertEqual(int(replayed.summary.get("trailing_stop_count", 0)), 1)


if __name__ == "__main__":
    unittest.main()
