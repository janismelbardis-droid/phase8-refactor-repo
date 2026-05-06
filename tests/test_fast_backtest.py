from __future__ import annotations

import unittest

import pandas as pd

from app.backtest import run_backtest
from app.backtest_models import BacktestConfig
from app.fast_backtest import run_backtest_compiled_bar
from app.research.replay import build_backtest_replay_plan, replay_backtest_result
from app.rules import Rule
from app.strategy_compiler import FAST_BAR_ENGINE, FALLBACK_ENGINE, compile_strategy_plan


class FastBacktestTests(unittest.TestCase):
    def _sample_stream(self) -> pd.DataFrame:
        idx = pd.date_range("2026-01-01T00:00:00Z", periods=10, freq="1min", tz="UTC")
        close = [100.0, 102.0, 104.0, 103.0, 101.0, 99.0, 97.0, 98.0, 100.0, 102.0]
        open_ = [100.0, 100.0, 102.0, 104.0, 103.0, 101.0, 99.0, 97.0, 98.0, 100.0]
        high = [max(o, c) + 0.5 for o, c in zip(open_, close)]
        low = [min(o, c) - 0.5 for o, c in zip(open_, close)]
        return pd.DataFrame(
            {
                "open": open_,
                "high": high,
                "low": low,
                "close": close,
                "price": close,
                "open_time": [ts - pd.Timedelta(minutes=1) + pd.Timedelta(milliseconds=1) for ts in idx],
                "close_time": list(idx),
                "volume": [10.0] * len(idx),
                "macd": [-1.0, 1.0, 1.0, 1.0, -1.0, -1.0, -1.0, 1.0, 1.0, 1.0],
                "signal": [0.0] * len(idx),
                "ms": ["RED", "GREEN", "GREEN", "GREEN", "RED", "RED", "RED", "GREEN", "GREEN", "GREEN"],
                "angle": ["RED", "GREEN", "GREEN", "GREEN", "RED", "RED", "RED", "GREEN", "GREEN", "GREEN"],
                "sig_angle": ["RED", "GREEN", "GREEN", "GREEN", "RED", "RED", "RED", "GREEN", "GREEN", "GREEN"],
                "ppo": ["RED", "GREEN", "GREEN", "GREEN", "RED", "RED", "RED", "GREEN", "GREEN", "GREEN"],
                "flip_age": [0.0] * len(idx),
            },
            index=idx,
        )

    def _rules_model(self):
        long_rule = Rule(timeframe="1m", mode="state", field="macd", op=">", value=0)
        short_rule = Rule(timeframe="1m", mode="state", field="macd", op="<", value=0)
        return {
            "Long Entry": [[long_rule]],
            "Long Exit": [[short_rule]],
            "Short Entry": [[short_rule]],
            "Short Exit": [[long_rule]],
        }

    def test_strategy_plan_compiles_safe_one_sided_sequence(self) -> None:
        rules_model = self._rules_model()
        plan = compile_strategy_plan(
            rules_model,
            tab_group_join_mode={name: "AND" for name in rules_model},
            group_rule_join_mode={name: (["SEQUENCE"] if name == "Long Entry" else ["OR"]) for name in rules_model},
            entry_filters={},
            backtest_cfg=BacktestConfig(stop_loss_mode="OFF", take_profit_mode="OFF"),
        )
        self.assertEqual(plan.engine, FAST_BAR_ENGINE)
        self.assertNotIn("sequence_groups", plan.blockers)

    def test_strategy_plan_keeps_sequence_with_active_entry_filter_on_generic_engine(self) -> None:
        rules_model = self._rules_model()
        plan = compile_strategy_plan(
            rules_model,
            tab_group_join_mode={name: "AND" for name in rules_model},
            group_rule_join_mode={name: (["SEQUENCE"] if name == "Long Entry" else ["OR"]) for name in rules_model},
            entry_filters={
                "Long Entry": {
                    "enabled": True,
                    "mode": "RETRACE_ALWAYS",
                    "expansion_atr_mult": 2.0,
                    "hard_skip_atr_mult": 0.0,
                    "confirm_bars": 1,
                    "reference_range": "FULL_CANDLE",
                    "touch_type": "WICK",
                    "min_retrace_pct": 35.0,
                    "max_retrace_pct": 100.0,
                    "require_next_close_beyond_mid": False,
                    "require_setup_still_valid": False,
                    "apply_to_reversals": True,
                    "groups": {
                        "0": {
                            "enabled": True,
                            "mode": "RETRACE_ALWAYS",
                            "expansion_atr_mult": 2.0,
                            "hard_skip_atr_mult": 0.0,
                            "confirm_bars": 1,
                            "reference_range": "FULL_CANDLE",
                            "touch_type": "WICK",
                            "min_retrace_pct": 35.0,
                            "max_retrace_pct": 100.0,
                            "require_next_close_beyond_mid": False,
                            "require_setup_still_valid": False,
                            "apply_to_reversals": True,
                        }
                    },
                }
            },
            backtest_cfg=BacktestConfig(stop_loss_mode="OFF", take_profit_mode="OFF"),
        )
        self.assertEqual(plan.engine, FALLBACK_ENGINE)
        self.assertIn("entry_filters", plan.blockers)

    def test_strategy_plan_compiles_safe_sequence_with_default_only_entry_filter(self) -> None:
        rules_model = self._rules_model()
        plan = compile_strategy_plan(
            rules_model,
            tab_group_join_mode={name: "AND" for name in rules_model},
            group_rule_join_mode={name: (["SEQUENCE"] if name == "Long Entry" else ["OR"]) for name in rules_model},
            entry_filters={
                "Long Entry": {
                    "enabled": True,
                    "mode": "RETRACE_ALWAYS",
                    "expansion_atr_mult": 2.0,
                    "hard_skip_atr_mult": 0.0,
                    "confirm_bars": 1,
                    "reference_range": "FULL_CANDLE",
                    "touch_type": "WICK",
                    "min_retrace_pct": 35.0,
                    "max_retrace_pct": 100.0,
                    "require_next_close_beyond_mid": False,
                    "require_setup_still_valid": False,
                    "apply_to_reversals": True,
                }
            },
            backtest_cfg=BacktestConfig(
                stop_loss_mode="OFF",
                take_profit_mode="OFF",
                allow_reverse=False,
            ),
        )
        self.assertEqual(plan.engine, FAST_BAR_ENGINE)
        self.assertEqual(tuple(plan.blockers), tuple())

    def test_strategy_plan_compiles_safe_sequence_with_reverse_entry_filter(self) -> None:
        long_trigger = Rule(timeframe="1m", mode="state", field="macd", op=">", value=0)
        long_gate = Rule(timeframe="1m", mode="state", field="ms", op="=", value="GREEN")
        short_rule = Rule(timeframe="1m", mode="state", field="macd", op="<", value=0)
        rules_model = {
            "Long Entry": [[long_trigger, long_gate]],
            "Long Exit": [],
            "Short Entry": [[short_rule]],
            "Short Exit": [],
        }
        entry_filters = {
            "Long Entry": {
                "enabled": True,
                "mode": "RETRACE_ALWAYS",
                "expansion_atr_mult": 2.0,
                "hard_skip_atr_mult": 0.0,
                "confirm_bars": 1,
                "reference_range": "FULL_CANDLE",
                "touch_type": "WICK",
                "min_retrace_pct": 0.0,
                "max_retrace_pct": 100.0,
                "require_next_close_beyond_mid": False,
                "require_setup_still_valid": False,
                "apply_to_reversals": True,
            },
            "Short Entry": {
                "enabled": True,
                "mode": "RETRACE_ALWAYS",
                "expansion_atr_mult": 2.0,
                "hard_skip_atr_mult": 0.0,
                "confirm_bars": 1,
                "reference_range": "FULL_CANDLE",
                "touch_type": "WICK",
                "min_retrace_pct": 0.0,
                "max_retrace_pct": 100.0,
                "require_next_close_beyond_mid": False,
                "require_setup_still_valid": False,
                "apply_to_reversals": True,
            },
        }
        plan = compile_strategy_plan(
            rules_model,
            tab_group_join_mode={name: "AND" for name in rules_model},
            group_rule_join_mode={
                "Long Entry": ["SEQUENCE"],
                "Long Exit": [],
                "Short Entry": ["OR"],
                "Short Exit": [],
            },
            entry_filters=entry_filters,
            backtest_cfg=BacktestConfig(
                stop_loss_mode="OFF",
                take_profit_mode="OFF",
                allow_reverse=True,
            ),
        )
        self.assertEqual(plan.engine, FAST_BAR_ENGINE)
        self.assertEqual(tuple(plan.blockers), tuple())

    def test_compiled_engine_matches_legacy_for_simple_state_rules(self) -> None:
        df = self._sample_stream()
        rules_model = self._rules_model()
        cfg = BacktestConfig(
            initial_balance=1000.0,
            order_notional_usdt=100.0,
            fee_rate=0.0,
            slippage_bps=0.0,
            stop_loss_mode="OFF",
            take_profit_mode="OFF",
            allow_reverse=True,
            skip_if_both_entries=True,
            step_timeframe="1m",
            capture_trade_details=False,
            equity_curve_stride=1,
        )
        join_mode = {name: "AND" for name in rules_model}
        group_rule_join_mode = {name: ["OR"] for name in rules_model}

        plan = compile_strategy_plan(
            rules_model,
            tab_group_join_mode=join_mode,
            group_rule_join_mode=group_rule_join_mode,
            entry_filters={},
            backtest_cfg=cfg,
        )
        self.assertEqual(plan.engine, FAST_BAR_ENGINE)

        legacy = run_backtest(
            symbol="BTCUSDT",
            streams_full={"1m": df.copy()},
            start=df.index[0],
            end=df.index[-1],
            rules_model=rules_model,
            tab_group_join_mode=join_mode,
            group_rule_join_mode=group_rule_join_mode,
            cfg=cfg,
            df_1m_full=df.copy(),
            entry_filters={},
        )
        compiled = run_backtest_compiled_bar(
            symbol="BTCUSDT",
            streams_full={"1m": df.copy()},
            start=df.index[0],
            end=df.index[-1],
            rules_model=rules_model,
            tab_group_join_mode=join_mode,
            group_rule_join_mode=group_rule_join_mode,
            cfg=cfg,
            entry_filters={},
        )

        self.assertEqual([t.side for t in compiled.trades], [t.side for t in legacy.trades])
        self.assertEqual([t.entry_reason for t in compiled.trades], [t.entry_reason for t in legacy.trades])
        self.assertEqual([t.exit_reason for t in compiled.trades], [t.exit_reason for t in legacy.trades])
        self.assertAlmostEqual(float(compiled.summary["ending_balance"]), float(legacy.summary["ending_balance"]), places=8)
        self.assertAlmostEqual(float(compiled.summary["return_pct"]), float(legacy.summary["return_pct"]), places=8)

    def test_compiled_engine_uses_raw_ohlc_when_stream_only_has_indicator_columns(self) -> None:
        df = self._sample_stream()
        stream_only = df.drop(columns=["open", "high", "low", "close", "open_time", "close_time", "volume"])
        rules_model = self._rules_model()
        cfg = BacktestConfig(
            initial_balance=1000.0,
            order_notional_usdt=100.0,
            fee_rate=0.0,
            slippage_bps=0.0,
            stop_loss_mode="OFF",
            take_profit_mode="OFF",
            allow_reverse=True,
            skip_if_both_entries=True,
            step_timeframe="1m",
            capture_trade_details=False,
            equity_curve_stride=1,
        )
        join_mode = {name: "AND" for name in rules_model}
        group_rule_join_mode = {name: ["OR"] for name in rules_model}

        legacy = run_backtest(
            symbol="BTCUSDT",
            streams_full={"1m": df.copy()},
            start=df.index[0],
            end=df.index[-1],
            rules_model=rules_model,
            tab_group_join_mode=join_mode,
            group_rule_join_mode=group_rule_join_mode,
            cfg=cfg,
            df_1m_full=df.copy(),
            entry_filters={},
        )
        compiled = run_backtest_compiled_bar(
            symbol="BTCUSDT",
            streams_full={"1m": stream_only.copy()},
            df_1m_full=df.copy(),
            start=df.index[0],
            end=df.index[-1],
            rules_model=rules_model,
            tab_group_join_mode=join_mode,
            group_rule_join_mode=group_rule_join_mode,
            cfg=cfg,
            entry_filters={},
        )

        self.assertEqual([t.side for t in compiled.trades], [t.side for t in legacy.trades])
        self.assertEqual([t.entry_reason for t in compiled.trades], [t.entry_reason for t in legacy.trades])
        self.assertEqual([t.exit_reason for t in compiled.trades], [t.exit_reason for t in legacy.trades])
        self.assertAlmostEqual(float(compiled.summary["ending_balance"]), float(legacy.summary["ending_balance"]), places=8)
        self.assertAlmostEqual(float(compiled.summary["return_pct"]), float(legacy.summary["return_pct"]), places=8)
        self.assertTrue(all(pd.notna(trade.entry_price) for trade in compiled.trades))
        self.assertTrue(all(pd.notna(trade.exit_price) for trade in compiled.trades))
        self.assertTrue(all(pd.notna(trade.qty) for trade in compiled.trades))
        self.assertTrue(all(pd.notna(trade.net_pnl) for trade in compiled.trades))
        self.assertTrue(pd.notna(compiled.summary["ending_balance"]))
        self.assertTrue(pd.notna(compiled.summary["return_pct"]))

    def test_compiled_engine_matches_legacy_for_sequence_with_default_only_entry_filter(self) -> None:
        df = self._sample_stream()
        long_trigger = Rule(timeframe="1m", mode="state", field="macd", op=">", value=0)
        long_gate = Rule(timeframe="1m", mode="state", field="ms", op="=", value="GREEN")
        long_exit = Rule(timeframe="1m", mode="state", field="macd", op="<", value=0)
        rules_model = {
            "Long Entry": [[long_trigger, long_gate]],
            "Long Exit": [[long_exit]],
            "Short Entry": [],
            "Short Exit": [],
        }
        join_mode = {name: "AND" for name in rules_model}
        group_rule_join_mode = {
            "Long Entry": ["SEQUENCE"],
            "Long Exit": ["OR"],
            "Short Entry": [],
            "Short Exit": [],
        }
        entry_filters = {
            "Long Entry": {
                "enabled": True,
                "mode": "RETRACE_ALWAYS",
                "expansion_atr_mult": 2.0,
                "hard_skip_atr_mult": 0.0,
                "confirm_bars": 3,
                "reference_range": "FULL_CANDLE",
                "touch_type": "WICK",
                "min_retrace_pct": 0.0,
                "max_retrace_pct": 100.0,
                "require_next_close_beyond_mid": False,
                "require_setup_still_valid": False,
                "apply_to_reversals": True,
            }
        }
        cfg = BacktestConfig(
            initial_balance=1000.0,
            order_notional_usdt=100.0,
            fee_rate=0.0,
            slippage_bps=0.0,
            stop_loss_mode="OFF",
            take_profit_mode="OFF",
            allow_reverse=False,
            skip_if_both_entries=True,
            step_timeframe="1m",
            capture_trade_details=False,
            equity_curve_stride=1,
        )

        plan = compile_strategy_plan(
            rules_model,
            tab_group_join_mode=join_mode,
            group_rule_join_mode=group_rule_join_mode,
            entry_filters=entry_filters,
            backtest_cfg=cfg,
        )
        self.assertEqual(plan.engine, FAST_BAR_ENGINE)

        legacy = run_backtest(
            symbol="BTCUSDT",
            streams_full={"1m": df.copy()},
            start=df.index[0],
            end=df.index[-1],
            rules_model=rules_model,
            tab_group_join_mode=join_mode,
            group_rule_join_mode=group_rule_join_mode,
            cfg=cfg,
            df_1m_full=df.copy(),
            entry_filters=entry_filters,
        )
        compiled = run_backtest_compiled_bar(
            symbol="BTCUSDT",
            streams_full={"1m": df.copy()},
            start=df.index[0],
            end=df.index[-1],
            rules_model=rules_model,
            tab_group_join_mode=join_mode,
            group_rule_join_mode=group_rule_join_mode,
            cfg=cfg,
            entry_filters=entry_filters,
        )

        self.assertEqual([t.side for t in compiled.trades], [t.side for t in legacy.trades])
        self.assertEqual([t.entry_time for t in compiled.trades], [t.entry_time for t in legacy.trades])
        self.assertEqual([t.exit_time for t in compiled.trades], [t.exit_time for t in legacy.trades])
        self.assertEqual([t.entry_reason for t in compiled.trades], [t.entry_reason for t in legacy.trades])
        self.assertEqual([t.exit_reason for t in compiled.trades], [t.exit_reason for t in legacy.trades])
        self.assertAlmostEqual(float(compiled.summary["ending_balance"]), float(legacy.summary["ending_balance"]), places=8)

    def test_compiled_engine_matches_legacy_for_sequence_with_reverse_entry_filter(self) -> None:
        df = self._sample_stream()
        long_trigger = Rule(timeframe="1m", mode="state", field="macd", op=">", value=0)
        long_gate = Rule(timeframe="1m", mode="state", field="ms", op="=", value="GREEN")
        short_rule = Rule(timeframe="1m", mode="state", field="macd", op="<", value=0)
        rules_model = {
            "Long Entry": [[long_trigger, long_gate]],
            "Long Exit": [],
            "Short Entry": [[short_rule]],
            "Short Exit": [],
        }
        join_mode = {name: "AND" for name in rules_model}
        group_rule_join_mode = {
            "Long Entry": ["SEQUENCE"],
            "Long Exit": [],
            "Short Entry": ["OR"],
            "Short Exit": [],
        }
        entry_filters = {
            "Long Entry": {
                "enabled": True,
                "mode": "RETRACE_ALWAYS",
                "expansion_atr_mult": 2.0,
                "hard_skip_atr_mult": 0.0,
                "confirm_bars": 1,
                "reference_range": "FULL_CANDLE",
                "touch_type": "WICK",
                "min_retrace_pct": 0.0,
                "max_retrace_pct": 100.0,
                "require_next_close_beyond_mid": False,
                "require_setup_still_valid": False,
                "apply_to_reversals": True,
            },
            "Short Entry": {
                "enabled": True,
                "mode": "RETRACE_ALWAYS",
                "expansion_atr_mult": 2.0,
                "hard_skip_atr_mult": 0.0,
                "confirm_bars": 1,
                "reference_range": "FULL_CANDLE",
                "touch_type": "WICK",
                "min_retrace_pct": 0.0,
                "max_retrace_pct": 100.0,
                "require_next_close_beyond_mid": False,
                "require_setup_still_valid": False,
                "apply_to_reversals": True,
            },
        }
        cfg = BacktestConfig(
            initial_balance=1000.0,
            order_notional_usdt=100.0,
            fee_rate=0.0,
            slippage_bps=0.0,
            stop_loss_mode="OFF",
            take_profit_mode="OFF",
            allow_reverse=True,
            skip_if_both_entries=True,
            step_timeframe="1m",
            capture_trade_details=False,
            equity_curve_stride=1,
        )

        plan = compile_strategy_plan(
            rules_model,
            tab_group_join_mode=join_mode,
            group_rule_join_mode=group_rule_join_mode,
            entry_filters=entry_filters,
            backtest_cfg=cfg,
        )
        self.assertEqual(plan.engine, FAST_BAR_ENGINE)

        legacy = run_backtest(
            symbol="BTCUSDT",
            streams_full={"1m": df.copy()},
            start=df.index[0],
            end=df.index[-1],
            rules_model=rules_model,
            tab_group_join_mode=join_mode,
            group_rule_join_mode=group_rule_join_mode,
            cfg=cfg,
            df_1m_full=df.copy(),
            entry_filters=entry_filters,
        )
        compiled = run_backtest_compiled_bar(
            symbol="BTCUSDT",
            streams_full={"1m": df.copy()},
            start=df.index[0],
            end=df.index[-1],
            rules_model=rules_model,
            tab_group_join_mode=join_mode,
            group_rule_join_mode=group_rule_join_mode,
            cfg=cfg,
            entry_filters=entry_filters,
        )

        self.assertEqual([t.side for t in compiled.trades], [t.side for t in legacy.trades])
        self.assertEqual([t.entry_time for t in compiled.trades], [t.entry_time for t in legacy.trades])
        self.assertEqual([t.exit_time for t in compiled.trades], [t.exit_time for t in legacy.trades])
        self.assertEqual([t.entry_reason for t in compiled.trades], [t.entry_reason for t in legacy.trades])
        self.assertEqual([t.exit_reason for t in compiled.trades], [t.exit_reason for t in legacy.trades])
        self.assertAlmostEqual(float(compiled.summary["ending_balance"]), float(legacy.summary["ending_balance"]), places=8)

    def test_compiled_replay_matches_fresh_compiled_run_for_exit_changes(self) -> None:
        df = self._sample_stream()
        rules_model = self._rules_model()
        join_mode = {name: "AND" for name in rules_model}
        group_rule_join_mode = {name: ["OR"] for name in rules_model}
        base_cfg = BacktestConfig(
            initial_balance=1000.0,
            order_notional_usdt=100.0,
            fee_rate=0.0,
            slippage_bps=0.0,
            stop_loss_mode="OFF",
            take_profit_mode="OFF",
            allow_reverse=True,
            skip_if_both_entries=True,
            step_timeframe="1m",
            capture_trade_details=False,
            equity_curve_stride=1,
        )

        base_result = run_backtest_compiled_bar(
            symbol="BTCUSDT",
            streams_full={"1m": df.copy()},
            start=df.index[0],
            end=df.index[-1],
            rules_model=rules_model,
            tab_group_join_mode=join_mode,
            group_rule_join_mode=group_rule_join_mode,
            cfg=base_cfg,
            entry_filters={},
        )

        next_cfg = BacktestConfig(**base_cfg.__dict__)
        next_cfg.stop_loss_mode = "PERCENT"
        next_cfg.stop_loss_value = 1.0
        next_cfg.stop_loss_pct = 1.0
        next_cfg.take_profit_mode = "PERCENT"
        next_cfg.take_profit_value = 2.0
        next_cfg.take_profit_pct = 2.0

        replay_plan = build_backtest_replay_plan(
            base_result,
            next_cfg=next_cfg,
            symbol="BTCUSDT",
            start=df.index[0],
            end=df.index[-1],
            rules_model=rules_model,
            tab_group_join_mode=join_mode,
            group_rule_join_mode=group_rule_join_mode,
            entry_filters={},
        )
        self.assertTrue(replay_plan.eligible)

        replayed = replay_backtest_result(
            base_result,
            cfg=next_cfg,
            capture_trade_details=False,
            equity_curve_stride=1,
        )
        fresh = run_backtest_compiled_bar(
            symbol="BTCUSDT",
            streams_full={"1m": df.copy()},
            start=df.index[0],
            end=df.index[-1],
            rules_model=rules_model,
            tab_group_join_mode=join_mode,
            group_rule_join_mode=group_rule_join_mode,
            cfg=next_cfg,
            entry_filters={},
        )

        self.assertTrue(bool(replayed.summary.get("compiled_plan_reused")))
        self.assertAlmostEqual(float(replayed.summary["ending_balance"]), float(fresh.summary["ending_balance"]), places=8)
        self.assertAlmostEqual(float(replayed.summary["return_pct"]), float(fresh.summary["return_pct"]), places=8)
        self.assertEqual([t.exit_reason for t in replayed.trades], [t.exit_reason for t in fresh.trades])

    def test_compiled_engine_supports_break_even_after_mfe(self) -> None:
        df = self._sample_stream()
        rules_model = self._rules_model()
        join_mode = {name: "AND" for name in rules_model}
        group_rule_join_mode = {name: ["OR"] for name in rules_model}
        cfg = BacktestConfig(
            initial_balance=1000.0,
            order_notional_usdt=100.0,
            fee_rate=0.0,
            slippage_bps=0.0,
            stop_loss_mode="OFF",
            take_profit_mode="OFF",
            break_even_after_mfe_pct=1.0,
            allow_reverse=True,
            skip_if_both_entries=True,
            step_timeframe="1m",
            capture_trade_details=False,
            equity_curve_stride=1,
        )

        legacy = run_backtest(
            symbol="BTCUSDT",
            streams_full={"1m": df.copy()},
            start=df.index[0],
            end=df.index[-1],
            rules_model=rules_model,
            tab_group_join_mode=join_mode,
            group_rule_join_mode=group_rule_join_mode,
            cfg=cfg,
            df_1m_full=df.copy(),
            entry_filters={},
        )
        compiled = run_backtest_compiled_bar(
            symbol="BTCUSDT",
            streams_full={"1m": df.copy()},
            start=df.index[0],
            end=df.index[-1],
            rules_model=rules_model,
            tab_group_join_mode=join_mode,
            group_rule_join_mode=group_rule_join_mode,
            cfg=cfg,
            entry_filters={},
        )

        self.assertTrue(any(trade.exit_reason == "Break Even" for trade in legacy.trades))
        self.assertEqual([t.exit_reason for t in compiled.trades], [t.exit_reason for t in legacy.trades])
        self.assertEqual(int(compiled.summary.get("break_even_count", 0)), int(legacy.summary.get("break_even_count", 0)))

    def test_compiled_engine_supports_trailing_stop(self) -> None:
        df = self._sample_stream()
        long_rule = Rule(timeframe="1m", mode="state", field="macd", op=">", value=0)
        rules_model = {
            "Long Entry": [[long_rule]],
            "Long Exit": [],
            "Short Entry": [],
            "Short Exit": [],
        }
        join_mode = {name: "AND" for name in rules_model}
        group_rule_join_mode = {name: ["OR"] for name in rules_model}
        cfg = BacktestConfig(
            initial_balance=1000.0,
            order_notional_usdt=100.0,
            fee_rate=0.0,
            slippage_bps=0.0,
            stop_loss_mode="OFF",
            take_profit_mode="OFF",
            trailing_stop_pct=1.0,
            allow_reverse=False,
            skip_if_both_entries=True,
            step_timeframe="1m",
            capture_trade_details=False,
            equity_curve_stride=1,
        )

        legacy = run_backtest(
            symbol="BTCUSDT",
            streams_full={"1m": df.copy()},
            start=df.index[0],
            end=df.index[-1],
            rules_model=rules_model,
            tab_group_join_mode=join_mode,
            group_rule_join_mode=group_rule_join_mode,
            cfg=cfg,
            df_1m_full=df.copy(),
            entry_filters={},
        )
        compiled = run_backtest_compiled_bar(
            symbol="BTCUSDT",
            streams_full={"1m": df.copy()},
            start=df.index[0],
            end=df.index[-1],
            rules_model=rules_model,
            tab_group_join_mode=join_mode,
            group_rule_join_mode=group_rule_join_mode,
            cfg=cfg,
            entry_filters={},
        )

        self.assertTrue(any(trade.exit_reason == "Trailing Stop" for trade in legacy.trades))
        self.assertEqual([t.exit_reason for t in compiled.trades], [t.exit_reason for t in legacy.trades])
        self.assertEqual(int(compiled.summary.get("trailing_stop_count", 0)), int(legacy.summary.get("trailing_stop_count", 0)))


if __name__ == "__main__":
    unittest.main()
