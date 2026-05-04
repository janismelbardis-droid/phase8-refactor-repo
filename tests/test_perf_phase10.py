from __future__ import annotations

import unittest
from collections import Counter
from unittest.mock import patch

import pandas as pd

from app.backtest import BacktestConfig, _compile_tab_signal_array, _prev_source_row_index_for_df, run_backtest
from app.rules import Rule


class Phase10PerfSafetyTests(unittest.TestCase):
    def _sample_stream(self) -> pd.DataFrame:
        idx = pd.date_range("2026-01-01T00:00:00Z", periods=6, freq="1min", tz="UTC")
        return pd.DataFrame(
            {
                "open": [100.0, 101.0, 102.0, 103.0, 104.0, 105.0],
                "high": [100.5, 101.5, 102.5, 103.5, 104.5, 105.5],
                "low": [99.5, 100.5, 101.5, 102.5, 103.5, 104.5],
                "close": [100.0, 101.0, 102.0, 103.0, 104.0, 105.0],
                "price": [100.0, 101.0, 102.0, 103.0, 104.0, 105.0],
                "open_time": [ts - pd.Timedelta(minutes=1) + pd.Timedelta(milliseconds=1) for ts in idx],
                "close_time": list(idx),
                "volume": [10.0] * len(idx),
                "macd": [-1.0, -0.5, 0.2, 0.6, 0.7, 0.8],
                "signal": [0.0, 0.0, 0.1, 0.2, 0.3, 0.4],
                "ms": ["RED", "RED", "GREEN", "GREEN", "GREEN", "GREEN"],
            },
            index=idx,
        )

    def test_compile_tab_signal_array_matches_simple_macd_cross(self) -> None:
        df = self._sample_stream()
        rule = Rule(timeframe="1m", mode="event", field="ms_cross", op="CROSS", value="UP")
        rules_model = {"Long Entry": [[rule]]}
        tab_join = {"Long Entry": "AND"}
        group_join = {"Long Entry": ["OR"]}
        prev_map = {"1m": _prev_source_row_index_for_df(df, "1m")}
        arr = _compile_tab_signal_array("Long Entry", rules_model, tab_join, group_join, {"1m": df}, prev_map, len(df))
        self.assertIsNotNone(arr)
        self.assertEqual(arr.tolist(), [False, False, True, False, False, False])

    def test_bar_backtest_skips_eval_snapshots_when_actionable_tabs_are_compiled(self) -> None:
        df = self._sample_stream()
        cfg = BacktestConfig(
            initial_balance=1000.0,
            order_notional_usdt=100.0,
            fee_rate=0.0,
            slippage_bps=0.0,
            stop_loss_mode="OFF",
            take_profit_mode="OFF",
            step_timeframe="1m",
        )
        # Compilable state rule that never fires, so no traces or snapshots are needed.
        rules_model = {
            "Long Entry": [[Rule(timeframe="1m", mode="state", field="macd", op=">", value=9999)]],
            "Long Exit": [],
            "Short Entry": [],
            "Short Exit": [],
        }
        join_mode = {name: "AND" for name in rules_model.keys()}
        group_join_mode = {name: ["OR"] if rules_model[name] else [] for name in rules_model.keys()}

        with patch("app.backtest._build_eval_snapshots_for_row", side_effect=AssertionError("compiled path should skip eval snapshots")):
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
        self.assertEqual(len(result.trades), 0)

    def test_barsago_rule_falls_back_to_legacy_eval(self) -> None:
        df = self._sample_stream()
        cfg = BacktestConfig(
            initial_balance=1000.0,
            order_notional_usdt=100.0,
            fee_rate=0.0,
            slippage_bps=0.0,
            stop_loss_mode="OFF",
            take_profit_mode="OFF",
            step_timeframe="1m",
        )
        rules_model = {
            "Long Entry": [[Rule(timeframe="1m", mode="event", field="ms_cross", op="CROSS", value="UP", bars_ago_mode="EXACT", bars_ago_n=1)]],
            "Long Exit": [],
            "Short Entry": [],
            "Short Exit": [],
        }
        join_mode = {name: "AND" for name in rules_model.keys()}
        group_join_mode = {name: ["OR"] if rules_model[name] else [] for name in rules_model.keys()}

        calls: Counter = Counter()

        def fake_eval(tab_name, rules_model, tab_group_join_mode, group_rule_join_mode, cur_by_tf, prev_by_tf, ctx=None):
            calls[str(tab_name)] += 1
            return False

        fake_eval.__module__ = "app.rules"

        with patch("app.backtest.eval_tab_generic", new=fake_eval):
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
        self.assertEqual(len(result.trades), 0)
        self.assertGreater(calls["Long Entry"], 0)


if __name__ == "__main__":
    unittest.main()
