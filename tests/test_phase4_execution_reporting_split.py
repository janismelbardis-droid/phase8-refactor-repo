from __future__ import annotations

import unittest

import pandas as pd

from app import backtest as bt
from app import backtest_execution as exec_mod
from app import backtest_reporting as report_mod
from app.backtest import BacktestConfig, Trade


class Phase4ExecutionReportingSplitTests(unittest.TestCase):
    def test_backtest_module_reexports_split_helpers(self) -> None:
        self.assertIs(bt._apply_slippage, exec_mod._apply_slippage)
        self.assertIs(bt._compute_stop_take_prices, exec_mod._compute_stop_take_prices)
        self.assertIs(bt._build_tab_rule_trace, exec_mod._build_tab_rule_trace)
        self.assertIs(bt._make_trade_df, report_mod._make_trade_df)
        self.assertIs(bt.build_backtest_replay_context, report_mod.build_backtest_replay_context)

    def test_reporting_helpers_preserve_trade_metrics(self) -> None:
        trade = Trade(
            trade_id=7,
            side="LONG",
            entry_time=pd.Timestamp("2026-01-01T00:00:00Z"),
            entry_price=100.0,
            exit_time=pd.Timestamp("2026-01-01T00:05:00Z"),
            exit_price=105.0,
            qty=1.0,
            entry_notional=100.0,
            exit_notional=105.0,
            gross_pnl=5.0,
            fees=0.3,
            net_pnl=4.7,
            return_pct=4.7,
            duration_min=5,
            mfe=6.0,
            mae=-1.0,
            mfe_pct=6.0,
            mae_pct=-1.0,
            entry_reason="Long Entry",
            exit_reason="Take Profit",
            entry_rule_trace="entry-trace",
            exit_rule_trace="exit-trace",
            entry_snapshot={"1m": {"close": 100.0}},
            exit_snapshot={"1m": {"close": 105.0}},
            fee_entry=0.1,
            fee_exit=0.2,
            signal_time=pd.Timestamp("2026-01-01T00:00:00Z"),
            order_created_time=pd.Timestamp("2026-01-01T00:00:30Z"),
            entry_fill_time=pd.Timestamp("2026-01-01T00:01:00Z"),
            exit_fill_time=pd.Timestamp("2026-01-01T00:05:00Z"),
            engine_type="OHLCV",
            fill_source="BAR",
            ambiguous_exit=False,
            stop_loss_price=98.0,
            take_profit_price=105.0,
        )

        df = bt._make_trade_df([trade])
        self.assertEqual(list(df["trade_id"]), [7])
        self.assertAlmostEqual(float(df.loc[0, "signal_to_entry_sec"]), 60.0)
        self.assertAlmostEqual(float(df.loc[0, "order_to_entry_sec"]), 30.0)
        self.assertAlmostEqual(float(df.loc[0, "entry_to_exit_sec"]), 240.0)

        summary = bt._apply_trade_reporting_metrics({"net_profit": 4.7}, [trade])
        self.assertEqual(summary["engine_type_counts"], {"OHLCV": 1})
        self.assertEqual(summary["fill_source_counts"], {"BAR": 1})
        self.assertEqual(summary["exit_reason_counts"], {"Take Profit": 1})
        self.assertEqual(summary["take_profit_count"], 1)
        self.assertAlmostEqual(summary["avg_signal_to_entry_sec"], 60.0)
        self.assertAlmostEqual(summary["avg_order_to_entry_sec"], 30.0)
        self.assertAlmostEqual(summary["avg_entry_to_exit_sec"], 240.0)

    def test_replay_context_builder_keeps_copy_semantics(self) -> None:
        idx = pd.date_range("2026-01-01T00:00:00Z", periods=2, freq="1min", tz="UTC")
        streams = {"1m": pd.DataFrame({"close": [100.0, 101.0]}, index=idx)}
        rules_model = {"Long Entry": [[{"mode": "state"}]], "Long Exit": [], "Short Entry": [], "Short Exit": []}
        entry_filters = {"Long Entry": {"enabled": True}}

        ctx = bt.build_backtest_replay_context(
            symbol="BTCUSDT",
            streams_full=streams,
            start=idx[0],
            end=idx[-1],
            rules_model=rules_model,
            tab_group_join_mode={"Long Entry": "AND", "Long Exit": "AND", "Short Entry": "AND", "Short Exit": "AND"},
            group_rule_join_mode={"Long Entry": ["AND"], "Long Exit": [], "Short Entry": [], "Short Exit": []},
            df_1m_full=streams["1m"],
            entry_filters=entry_filters,
        )

        rules_model["Long Entry"][0].append({"mode": "mutated"})
        entry_filters["Long Entry"]["enabled"] = False

        self.assertEqual(len(ctx["rules_model"]["Long Entry"][0]), 1)
        self.assertTrue(ctx["entry_filters"]["Long Entry"]["enabled"])
        self.assertTrue(ctx["streams_full"]["1m"].equals(streams["1m"]))

    def test_stop_take_and_lifecycle_helpers_stay_callable(self) -> None:
        cfg = BacktestConfig(
            stop_loss_mode="PERCENT",
            stop_loss_value=1.0,
            take_profit_mode="R_MULTIPLE",
            take_profit_value=2.0,
        )
        stop_price, take_profit_price = bt._compute_stop_take_prices(100.0, "LONG", cfg, {"1m": {"atr": 1.5}})
        self.assertAlmostEqual(float(stop_price), 99.0)
        self.assertAlmostEqual(float(take_profit_price), 102.0)

        life = bt._make_trade_lifecycle(engine_type="OHLCV", fill_source="BAR")
        life = bt._open_trade_lifecycle(
            life,
            side="LONG",
            signal_time="2026-01-01T00:00:00Z",
            order_created_time="2026-01-01T00:00:30Z",
            entry_fill_time="2026-01-01T00:01:00Z",
            entry_fill_price=100.0,
            qty=1.25,
            entry_reason="Long Entry",
            entry_snapshot={"1m": {"close": 100.0}},
            entry_barsago={"Long Entry": {}},
            entry_rule_trace="trace",
        )
        payload = bt._close_trade_lifecycle(
            life,
            exit_fill_time="2026-01-01T00:03:00Z",
            exit_fill_price=101.0,
            exit_reason="Take Profit",
            exit_snapshot={"1m": {"close": 101.0}},
            exit_barsago={"Long Exit": {}},
            ambiguous_exit=False,
            exit_rule_trace="exit-trace",
            exit_signal_time="2026-01-01T00:02:30Z",
            exit_order_created_time="2026-01-01T00:02:30Z",
        )
        self.assertEqual(payload["engine_type"], "OHLCV")
        self.assertEqual(payload["fill_source"], "BAR")
        self.assertEqual(payload["entry_rule_trace"], "trace")
        self.assertEqual(payload["exit_rule_trace"], "exit-trace")


if __name__ == "__main__":
    unittest.main()
