from __future__ import annotations

import unittest

import numpy as np
import pandas as pd

from app import backtest as bt
from app.backtest_planning import compiled_rule_support
from app.indicators_streaming import _indicator_streams_have_required_fields
from app.prepared_dataset import PreparedDataset, _entry_has_required_fields
from app.rules import Rule


class Phase3BacktestSplitTests(unittest.TestCase):
    def test_public_helpers_are_reexported_from_split_modules(self) -> None:
        self.assertEqual(bt._compile_tab_signal_array.__module__, "app.backtest_planning")
        self.assertEqual(bt._planned_eval_tabs.__module__, "app.backtest_planning")
        self.assertEqual(bt._freeze_backtest_streams.__module__, "app.backtest_frames")
        self.assertEqual(bt.make_streams_htf_closed_only.__module__, "app.backtest_frames")

    def test_compiled_tab_signal_array_behavior_is_preserved(self) -> None:
        idx = pd.date_range("2026-01-01T00:00:00Z", periods=4, freq="1min", tz="UTC")
        stream = pd.DataFrame(
            {
                "price": [100.0, 101.0, 102.0, 103.0],
                "ms": ["RED", "GREEN", "GREEN", "RED"],
            },
            index=idx,
        )
        rules_model = {
            "Long Entry": [[Rule(timeframe="1m", mode="state", field="ms", op="==", value="GREEN")]],
            "Long Exit": [],
            "Short Entry": [],
            "Short Exit": [],
        }
        prev_idx = {"1m": bt._prev_source_row_index_for_df(stream, "1m")}
        arr = bt._compile_tab_signal_array(
            "Long Entry",
            rules_model,
            {"Long Entry": "AND", "Long Exit": "AND", "Short Entry": "AND", "Short Exit": "AND"},
            {"Long Entry": ["AND"], "Long Exit": [], "Short Entry": [], "Short Exit": []},
            {"1m": stream},
            prev_idx,
            len(stream),
        )
        self.assertIsNotNone(arr)
        self.assertEqual(arr.dtype, np.dtype(bool))
        self.assertEqual(arr.tolist(), [False, True, True, False])

    def test_compiled_signal_array_supports_ema_cross_and_taker_bias(self) -> None:
        idx = pd.date_range("2026-01-01T00:00:00Z", periods=4, freq="1min", tz="UTC")
        stream = pd.DataFrame(
            {
                "ema_cross_up": [False, True, True, False],
                "taker_bias_state": ["SELL", "BUY", "BUY", "NEUTRAL"],
                "taker_delta_pct": [-10.0, 12.0, 18.0, 1.0],
            },
            index=idx,
        )
        prev_idx = {"1m": bt._prev_source_row_index_for_df(stream, "1m")}
        cross_rule = Rule(timeframe="1m", mode="event", field="ema_cross_up", op="EVENT", value=None)
        bias_rule = Rule(timeframe="1m", mode="state", field="taker_bias_state", op="=", value="BUY")
        delta_rule = Rule(timeframe="1m", mode="state", field="taker_delta_pct", op=">=", value=10.0)

        cross_arr = bt._compile_rule_signal_array(cross_rule, {"1m": stream}, prev_idx, len(stream))
        bias_arr = bt._compile_rule_signal_array(bias_rule, {"1m": stream}, prev_idx, len(stream))
        delta_arr = bt._compile_rule_signal_array(delta_rule, {"1m": stream}, prev_idx, len(stream))

        self.assertIsNotNone(cross_arr)
        self.assertIsNotNone(bias_arr)
        self.assertIsNotNone(delta_arr)
        self.assertEqual(cross_arr.tolist(), [False, True, False, False])
        self.assertEqual(bias_arr.tolist(), [False, True, True, False])
        self.assertEqual(delta_arr.tolist(), [False, True, True, False])

    def test_field_compare_rules_compile_to_vector_signal_array(self) -> None:
        idx = pd.date_range("2026-01-01T00:00:00Z", periods=4, freq="1min", tz="UTC")
        stream = pd.DataFrame(
            {
                "close": [100.0, 101.0, 102.0, 103.0],
                "ema_2": [99.0, 100.0, 101.0, 102.0],
            },
            index=idx,
        )
        rule = Rule(timeframe="1m", mode="state", field="close", op=">", value=None, compare_to={"field": "ema_2", "bars_ago": 2})
        rules_model = {
            "Long Entry": [[rule]],
            "Long Exit": [],
            "Short Entry": [],
            "Short Exit": [],
        }
        self.assertEqual(bt._max_bars_ago_needed(rules_model), 2)
        self.assertEqual(compiled_rule_support(rule), (True, "compiled"))
        arr = bt._compile_rule_signal_array(rule, {"1m": stream}, {"1m": bt._prev_source_row_index_for_df(stream, "1m")}, len(stream))
        self.assertIsNotNone(arr)
        self.assertEqual(arr.tolist(), [False, False, True, True])

    def test_exact_barsago_state_rules_compile_to_vector_signal_array(self) -> None:
        idx = pd.date_range("2026-01-01T00:00:00Z", periods=4, freq="1min", tz="UTC")
        stream = pd.DataFrame(
            {
                "low": [100.0, 99.0, 101.0, 98.0],
                "ema_1": [100.5, 100.0, 100.0, 100.0],
            },
            index=idx,
        )
        rule = Rule(
            timeframe="1m",
            mode="state",
            field="low",
            op="<=",
            value=None,
            compare_to={"field": "ema_1", "bars_ago": 1},
            bars_ago_mode="EXACT",
            bars_ago_n=1,
        )
        arr = bt._compile_rule_signal_array(rule, {"1m": stream}, {"1m": bt._prev_source_row_index_for_df(stream, "1m")}, len(stream))
        self.assertIsNotNone(arr)
        self.assertEqual(arr.tolist(), [False, True, True, False])

    def test_cache_validation_rejects_nan_only_required_ohlcv_fields(self) -> None:
        idx = pd.date_range("2026-01-01T00:00:00Z", periods=3, freq="1min", tz="UTC")
        stream = pd.DataFrame(
            {
                "price": [100.0, 101.0, 102.0],
                "open": [99.0, 100.0, 101.0],
                "high": [np.nan, np.nan, np.nan],
                "low": [98.0, 99.0, 100.0],
                "close": [100.0, 101.0, 102.0],
                "volume": [1.0, 1.0, 1.0],
            },
            index=idx,
        )

        self.assertFalse(_indicator_streams_have_required_fields({"1m": stream}, {"high"}))
        stream.loc[idx[1], "high"] = 102.0
        self.assertTrue(_indicator_streams_have_required_fields({"1m": stream}, {"high"}))

        entry = PreparedDataset(
            symbol="BTCUSDT",
            start=idx[0],
            end=idx[-1],
            timeframes=("1m",),
            price_source="LAST",
            macd_impl="TRADINGVIEW",
            adx_impl="TRADINGVIEW",
            required_families=frozenset({"ohlcv"}),
            ema_profile_signature=None,
            rsi_profile_signature=None,
            market_threshold_signature=None,
            df_1m_full=pd.DataFrame(),
            streams_full={"1m": stream},
        )
        self.assertTrue(_entry_has_required_fields(entry, {"high"}))
        entry.streams_full["1m"]["high"] = np.nan
        self.assertFalse(_entry_has_required_fields(entry, {"high"}))

    def test_barsago_profile_and_htf_closed_only_remain_available(self) -> None:
        rules_model = {
            "Long Entry": [[
                Rule(
                    timeframe="5m",
                    mode="event",
                    field="ms_cross",
                    op="CROSS",
                    value="UP",
                    is_trigger=True,
                    bars_ago_mode="WITHIN",
                    bars_ago_n=2,
                    require_still_valid=True,
                )
            ]]
        }
        profile = bt._barsago_profile_for_tab(rules_model, "Long Entry")
        self.assertEqual(profile["within"], [2])
        self.assertEqual(profile["items"][0]["field"], "ms_cross")

        idx = pd.date_range("2026-01-01T00:00:59.999Z", periods=6, freq="1min", tz="UTC")
        htf = pd.DataFrame(
            {
                "price": [100, 101, 102, 103, 104, 105],
                "ms": ["RED", "GREEN", "GREEN", "GREEN", "RED", "RED"],
                "frama_break_up": [False, False, False, False, True, False],
                "ema_cross_up": [False, False, False, False, True, False],
                "ema_cross_down": [False, False, False, False, False, False],
            },
            index=idx,
        )
        streams = {"1m": htf.copy(), "5m": htf.copy()}
        closed_only = bt.make_streams_htf_closed_only(streams)
        self.assertIn("5m", closed_only)
        self.assertIn("snapshot_bar_ms", closed_only["5m"].columns)
        self.assertFalse(bool(closed_only["5m"].iloc[5]["frama_break_up"]))
        self.assertTrue(bool(closed_only["5m"].iloc[4]["frama_break_up"]))
        self.assertFalse(bool(closed_only["5m"].iloc[5]["ema_cross_up"]))
        self.assertTrue(bool(closed_only["5m"].iloc[4]["ema_cross_up"]))


if __name__ == "__main__":
    unittest.main()
