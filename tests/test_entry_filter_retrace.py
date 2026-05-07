from __future__ import annotations

import unittest
from unittest.mock import patch

import pandas as pd

from app.backtest import (
    BacktestConfig,
    _EntryFilterBar,
    _entry_filter_confirm_decision,
    _entry_filter_signal_decision,
    run_backtest,
)
from app.rules import EntryFilterConfig


class EntryFilterRetraceTests(unittest.TestCase):
    def test_entry_filter_config_normalizes_retrace_alias_and_zero_hard_skip(self) -> None:
        cfg = EntryFilterConfig.from_dict(
            {
                "enabled": True,
                "mode": "require_pullback",
                "hard_skip_atr_mult": 0,
                "reference_range": "body_only",
                "touch_type": "close",
                "min_retrace_pct": 40,
                "max_retrace_pct": 90,
            }
        )
        self.assertEqual(cfg.normalized_mode(), "RETRACE_ALWAYS")
        self.assertEqual(cfg.normalized_reference_range(), "BODY_ONLY")
        self.assertEqual(cfg.normalized_touch_type(), "CLOSE")
        self.assertEqual(cfg.hard_skip_atr_mult, 0.0)

    def test_retrace_confirm_uses_min_and_max_thresholds(self) -> None:
        cfg = EntryFilterConfig(
            enabled=True,
            mode="RETRACE_ALWAYS",
            reference_range="FULL_CANDLE",
            touch_type="WICK",
            min_retrace_pct=50.0,
            max_retrace_pct=100.0,
            hard_skip_atr_mult=0.0,
        ).normalized_copy()
        signal_bar = _EntryFilterBar(
            time=pd.Timestamp("2026-01-01T00:01:00Z"),
            open=100.0,
            high=110.0,
            low=100.0,
            close=109.0,
            atr=2.0,
        )
        wait_bar = _EntryFilterBar(
            time=pd.Timestamp("2026-01-01T00:02:00Z"),
            open=109.0,
            high=109.5,
            low=107.0,
            close=108.0,
            atr=2.0,
        )
        allow_bar = _EntryFilterBar(
            time=pd.Timestamp("2026-01-01T00:03:00Z"),
            open=108.0,
            high=108.5,
            low=104.0,
            close=105.0,
            atr=2.0,
        )
        block_bar = _EntryFilterBar(
            time=pd.Timestamp("2026-01-01T00:04:00Z"),
            open=105.0,
            high=106.0,
            low=98.0,
            close=99.0,
            atr=2.0,
        )

        self.assertEqual(_entry_filter_signal_decision("LONG", cfg, signal_bar)[0], "WAIT")
        self.assertEqual(
            _entry_filter_confirm_decision("LONG", cfg, signal_bar, wait_bar, True),
            ("WAIT", "retrace:30.0%"),
        )
        self.assertEqual(
            _entry_filter_confirm_decision("LONG", cfg, signal_bar, allow_bar, True),
            ("ALLOW", "retraced:60.0%"),
        )
        self.assertEqual(
            _entry_filter_confirm_decision("LONG", cfg, signal_bar, block_bar, True),
            ("BLOCK", "retrace:120.0%"),
        )

    def test_casebook_pullback_confirm_simple_reclaim(self) -> None:
        cfg = EntryFilterConfig(
            enabled=True,
            mode="CASEBOOK_PULLBACK_V1",
            min_retrace_pct=0.0,
            casebook_pullback_window_bars=6,
            casebook_reclaim_window_bars=4,
            casebook_max_vidya_gap_pct=0.25,
            casebook_require_t0_vidya_buy=True,
            casebook_require_t0_rf_buy=True,
            casebook_require_vidya_buy_through_signal=True,
            require_setup_still_valid=False,
        ).normalized_copy()
        idx = pd.date_range("2026-01-01T00:00:00Z", periods=4, freq="1min", tz="UTC")
        stream = pd.DataFrame(
            {
                "open": [100.0, 109.0, 108.0, 100.0],
                "high": [110.0, 109.5, 102.0, 104.0],
                "low": [100.0, 107.0, 98.0, 99.5],
                "close": [109.0, 108.0, 101.0, 103.0],
                "vidya_line": [99.8, 99.0, 97.9, 98.5],
                "vidya_state": ["BUY", "BUY", "BUY", "BUY"],
                "range_filter_state": ["BUY", "BUY", "BUY", "BUY"],
                "range_filter_phase": ["BUY_STRONG", "BUY_WEAK", "BUY_WEAK", "BUY_WEAK"],
                "range_filter_buy": [1, 0, 0, 0],
                "range_filter_sell": [0, 0, 0, 0],
                "vidya_trend_up": [1, 0, 0, 0],
                "vidya_trend_down": [0, 0, 0, 0],
            },
            index=idx,
        )
        signal_bar = _EntryFilterBar(time=idx[0], open=100.0, high=110.0, low=100.0, close=109.0, atr=2.0)
        confirm_bar = _EntryFilterBar(time=idx[3], open=100.0, high=104.0, low=99.5, close=103.0, atr=2.0)

        self.assertEqual(_entry_filter_signal_decision("LONG", cfg, signal_bar)[0], "WAIT")
        self.assertEqual(
            _entry_filter_confirm_decision(
                "LONG",
                cfg,
                signal_bar,
                confirm_bar,
                True,
                signal_row_index=0,
                confirm_row_index=3,
                stream_1m=stream,
            ),
            ("ALLOW", "casebook-reclaim"),
        )

    def test_casebook_pullback_confirm_delayed_rf_buy(self) -> None:
        cfg = EntryFilterConfig(
            enabled=True,
            mode="CASEBOOK_PULLBACK_V1",
            min_retrace_pct=0.0,
            casebook_pullback_window_bars=8,
            casebook_reclaim_window_bars=4,
            casebook_max_vidya_gap_pct=0.25,
            casebook_require_t0_vidya_buy=True,
            casebook_require_t0_rf_buy=True,
            casebook_require_vidya_buy_through_signal=True,
            require_setup_still_valid=False,
        ).normalized_copy()
        idx = pd.date_range("2026-01-01T00:00:00Z", periods=5, freq="1min", tz="UTC")
        stream = pd.DataFrame(
            {
                "open": [100.0, 108.0, 106.0, 101.0, 102.0],
                "high": [110.0, 108.5, 106.5, 102.0, 103.0],
                "low": [100.0, 102.0, 99.0, 100.0, 101.0],
                "close": [109.0, 103.0, 100.5, 101.5, 102.5],
                "vidya_line": [99.8, 99.2, 98.9, 99.4, 100.2],
                "vidya_state": ["BUY", "BUY", "BUY", "BUY", "BUY"],
                "range_filter_state": ["BUY", "SELL", "SELL", "SELL", "BUY"],
                "range_filter_phase": ["BUY_STRONG", "SELL_WEAK", "SELL_WEAK", "SELL_WEAK", "BUY_WEAK"],
                "range_filter_buy": [1, 0, 0, 0, 0],
                "range_filter_sell": [0, 1, 0, 0, 0],
                "vidya_trend_up": [1, 0, 0, 0, 0],
                "vidya_trend_down": [0, 0, 0, 0, 0],
            },
            index=idx,
        )
        signal_bar = _EntryFilterBar(time=idx[0], open=100.0, high=110.0, low=100.0, close=109.0, atr=2.0)
        confirm_bar = _EntryFilterBar(time=idx[4], open=102.0, high=103.0, low=101.0, close=102.5, atr=2.0)

        self.assertEqual(
            _entry_filter_confirm_decision(
                "LONG",
                cfg,
                signal_bar,
                confirm_bar,
                True,
                signal_row_index=0,
                confirm_row_index=4,
                stream_1m=stream,
            ),
            ("ALLOW", "casebook-rf-buy"),
        )

    def test_casebook_pullback_backtracks_anchor_before_late_signal_row(self) -> None:
        cfg = EntryFilterConfig(
            enabled=True,
            mode="CASEBOOK_PULLBACK_V1",
            min_retrace_pct=0.0,
            casebook_anchor_lookback_bars=8,
            casebook_pullback_window_bars=8,
            casebook_reclaim_window_bars=4,
            casebook_max_vidya_gap_pct=0.25,
            casebook_require_t0_vidya_buy=True,
            casebook_require_t0_rf_buy=True,
            casebook_require_vidya_buy_through_signal=True,
            require_setup_still_valid=False,
        ).normalized_copy()
        idx = pd.date_range("2026-01-01T00:00:00Z", periods=8, freq="1min", tz="UTC")
        stream = pd.DataFrame(
            {
                "open": [100.0, 100.5, 109.0, 108.0, 101.0, 100.2, 100.4, 100.6],
                "high": [100.7, 101.0, 110.0, 108.3, 102.0, 101.0, 103.0, 104.0],
                "low": [99.9, 100.3, 100.0, 99.4, 99.6, 99.8, 100.2, 100.5],
                "close": [100.5, 100.8, 109.0, 101.0, 100.1, 100.5, 102.5, 103.5],
                "vidya_line": [99.7, 99.9, 99.8, 99.2, 99.3, 99.4, 99.8, 100.1],
                "vidya_state": ["BUY"] * 8,
                "range_filter_state": ["BUY", "BUY", "BUY", "BUY", "BUY", "BUY", "BUY", "BUY"],
                "range_filter_phase": ["BUY_STRONG", "BUY_STRONG", "BUY_STRONG", "BUY_WEAK", "BUY_WEAK", "BUY_WEAK", "BUY_WEAK", "BUY_WEAK"],
                "range_filter_buy": [0, 1, 0, 0, 0, 0, 0, 0],
                "range_filter_sell": [0] * 8,
                "vidya_trend_up": [0, 0, 1, 0, 0, 0, 0, 0],
                "vidya_trend_down": [0] * 8,
            },
            index=idx,
        )
        signal_bar = _EntryFilterBar(time=idx[5], open=100.2, high=101.0, low=99.8, close=100.5, atr=2.0)
        confirm_bar = _EntryFilterBar(time=idx[7], open=100.6, high=104.0, low=100.5, close=103.5, atr=2.0)

        self.assertEqual(
            _entry_filter_confirm_decision(
                "LONG",
                cfg,
                signal_bar,
                confirm_bar,
                True,
                signal_row_index=5,
                confirm_row_index=7,
                stream_1m=stream,
            ),
            ("ALLOW", "casebook-reclaim"),
        )

    def test_run_backtest_retrace_filter_waits_across_multiple_bars(self) -> None:
        idx = pd.date_range("2026-01-01T00:00:00Z", periods=7, freq="1min", tz="UTC")
        df = pd.DataFrame(
            {
                "open": [100.0, 100.0, 109.0, 108.0, 106.0, 107.0, 108.0],
                "high": [100.5, 110.0, 109.5, 108.5, 107.0, 108.0, 109.0],
                "low": [99.5, 100.0, 107.0, 104.0, 105.0, 106.0, 107.0],
                "close": [100.0, 109.0, 108.0, 105.0, 106.0, 107.0, 108.0],
                "price": [100.0, 109.0, 108.0, 105.0, 106.0, 107.0, 108.0],
                "open_time": [ts - pd.Timedelta(minutes=1) + pd.Timedelta(milliseconds=1) for ts in idx],
                "close_time": list(idx),
                "volume": [10.0] * len(idx),
                "market_state": ["FLAT"] * len(idx),
                "market_phase": ["BALANCE"] * len(idx),
            },
            index=idx,
        )

        def eval_side(tab_name, rules_model, tab_group_join_mode, group_rule_join_mode, cur_by_tf, prev_by_tf, ctx=None):
            ts = str((cur_by_tf.get("1m") or {}).get("snapshot_time") or "")
            if tab_name == "Long Entry":
                return ts == "2026-01-01T00:01:00+00:00"
            if tab_name == "Long Exit":
                return ts == "2026-01-01T00:05:00+00:00"
            return False

        step_bars = df[["open", "high", "low", "close"]].copy()
        step_bars["atr"] = 1.0
        step_bars.index = pd.to_datetime(df["close_time"], utc=True)
        step_bars.index.name = "time"

        cfg = BacktestConfig(
            initial_balance=1000.0,
            order_notional_usdt=100.0,
            fee_rate=0.0,
            slippage_bps=0.0,
            stop_loss_mode="OFF",
            take_profit_mode="OFF",
            step_timeframe="1m",
        )
        rules_model = {name: [] for name in ("Long Entry", "Long Exit", "Short Entry", "Short Exit")}
        join_mode = {name: "AND" for name in rules_model.keys()}
        group_join_mode = {name: [] for name in rules_model.keys()}
        entry_filters = {
            "Long Entry": EntryFilterConfig(
                enabled=True,
                mode="RETRACE_ALWAYS",
                reference_range="FULL_CANDLE",
                touch_type="WICK",
                min_retrace_pct=50.0,
                max_retrace_pct=100.0,
                confirm_bars=3,
                hard_skip_atr_mult=0.0,
                require_setup_still_valid=False,
            ),
            "Short Entry": EntryFilterConfig(),
        }

        with patch("app.backtest.eval_tab_generic", side_effect=eval_side), \
             patch("app.backtest._build_step_bars", return_value=step_bars), \
             patch("app.backtest._build_tab_rule_trace", return_value="trace"):
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
                entry_filters=entry_filters,
            )

        self.assertEqual(len(result.trades), 1)
        self.assertIn("Entry Filter confirmed +2 bars", result.trades[0].entry_reason)
        stats = result.summary.get("entry_filter", {}).get("stats", {})
        self.assertEqual(result.summary.get("entry_filter", {}).get("Long Entry", {}).get("mode"), "RETRACE_ALWAYS")
        self.assertEqual(stats.get("created"), 1)
        self.assertEqual(stats.get("confirmed"), 1)
        self.assertEqual(stats.get("blocked"), 0)


if __name__ == "__main__":
    unittest.main()
