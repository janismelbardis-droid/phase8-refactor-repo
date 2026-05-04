from __future__ import annotations

import unittest
from unittest.mock import patch

import pandas as pd

from app.backtest import BacktestConfig, run_backtest
from app.rules import EntryFilterConfig, Rule


class Phase6PerfSafetyTests(unittest.TestCase):
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
                "macd": [0.0, 1.0, 1.1, 1.2, 1.3, 1.4],
                "signal": [0.0, 0.5, 0.6, 0.7, 0.8, 0.9],
                "market_state": ["BULL_EXPANSION"] * len(idx),
                "market_phase": ["EXPANSION"] * len(idx),
            },
            index=idx,
        )

    def _entry_only_eval(self, tab_name, rules_model, tab_group_join_mode, group_rule_join_mode, cur_by_tf, prev_by_tf, ctx=None):
        ts = str((cur_by_tf.get("1m") or {}).get("snapshot_time") or "")
        return tab_name == "Long Entry" and ts == "2026-01-01T00:01:00+00:00"

    def _step_bars(self, df: pd.DataFrame) -> pd.DataFrame:
        out = df[["open", "high", "low", "close"]].copy()
        out["atr"] = 1.0
        out.index = pd.to_datetime(df["close_time"], utc=True)
        out.index.name = "time"
        return out

    def _group_filter_signal_decision(self, side, cfg, signal_bar):
        return ("WAIT", "expansion") if bool(getattr(cfg, "enabled", False)) else ("ALLOW", "off")

    def _group_filter_confirm_decision(self, side, cfg, signal_bar, confirm_bar, setup_still_valid):
        return ("BLOCK", "setup-invalid") if bool(getattr(cfg, "enabled", False)) else ("ALLOW", "confirmed")

    def test_pending_entry_blocked_does_not_build_full_snapshots(self) -> None:
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
        rules_model = {name: [] for name in ("Long Entry", "Long Exit", "Short Entry", "Short Exit")}
        join_mode = {name: "AND" for name in rules_model.keys()}
        group_join_mode = {name: [] for name in rules_model.keys()}
        entry_filters = {
            "Long Entry": EntryFilterConfig(enabled=True, mode="ANTI_CHASE", confirm_bars=1),
            "Short Entry": EntryFilterConfig(),
        }

        full_snapshot_calls = 0

        def counting_full_snapshot(*args, **kwargs):
            nonlocal full_snapshot_calls
            full_snapshot_calls += 1
            return {"1m": {"market_state": "BULL_EXPANSION", "snapshot_time": df.index[1]}}

        with patch("app.backtest.eval_tab_generic", side_effect=self._entry_only_eval), \
             patch("app.backtest._entry_filter_signal_decision", return_value=("WAIT", "expansion")), \
             patch("app.backtest._entry_filter_confirm_decision", return_value=("BLOCK", "setup-invalid")), \
             patch("app.backtest._build_step_bars", return_value=self._step_bars(df)), \
             patch("app.backtest._build_current_snapshots_for_row", side_effect=counting_full_snapshot):
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

        self.assertEqual(len(result.trades), 0)
        self.assertEqual(full_snapshot_calls, 0)
        self.assertEqual(result.summary.get("entry_filter", {}).get("stats", {}).get("created"), 1)
        self.assertEqual(result.summary.get("entry_filter", {}).get("stats", {}).get("blocked"), 1)

    def test_confirmed_pending_entry_materializes_full_signal_snapshot_on_fill(self) -> None:
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
        rules_model = {name: [] for name in ("Long Entry", "Long Exit", "Short Entry", "Short Exit")}
        join_mode = {name: "AND" for name in rules_model.keys()}
        group_join_mode = {name: [] for name in rules_model.keys()}
        entry_filters = {
            "Long Entry": EntryFilterConfig(enabled=True, mode="ANTI_CHASE", confirm_bars=1),
            "Short Entry": EntryFilterConfig(),
        }

        with patch("app.backtest.eval_tab_generic", side_effect=self._entry_only_eval), \
             patch("app.backtest._entry_filter_signal_decision", return_value=("WAIT", "expansion")), \
             patch("app.backtest._entry_filter_confirm_decision", return_value=("ALLOW", "confirmed:10.0%")), \
             patch("app.backtest._build_step_bars", return_value=self._step_bars(df)), \
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
        trade = result.trades[0]
        snap = trade.entry_snapshot.get("1m", {})
        self.assertEqual(snap.get("market_state"), "BULL_EXPANSION")
        self.assertEqual(snap.get("market_phase"), "EXPANSION")
        self.assertEqual(snap.get("macd"), 1.0)
        self.assertEqual(result.summary.get("entry_filter", {}).get("stats", {}).get("confirmed"), 1)

    def test_group_specific_entry_filter_blocks_only_matched_group(self) -> None:
        df = self._sample_stream().copy()
        df["market_state"] = ["FLAT", "BULL_EXPANSION", "FLAT", "FLAT", "FLAT", "FLAT"]
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
            "Long Entry": [
                [Rule("1m", "state", "market_state", "=", "BULL_EXPANSION")],
                [Rule("1m", "state", "market_state", "=", "BEAR_EXPANSION")],
            ],
            "Long Exit": [],
            "Short Entry": [],
            "Short Exit": [],
        }
        join_mode = {"Long Entry": "OR", "Long Exit": "AND", "Short Entry": "AND", "Short Exit": "AND"}
        group_join_mode = {"Long Entry": ["OR", "OR"], "Long Exit": [], "Short Entry": [], "Short Exit": []}
        entry_filters = {
            "Long Entry": {
                "enabled": False,
                "mode": "OFF",
                "groups": {
                    "0": EntryFilterConfig(enabled=True, mode="ANTI_CHASE", confirm_bars=1),
                },
            },
            "Short Entry": EntryFilterConfig(),
        }

        with patch("app.backtest._entry_filter_signal_decision", side_effect=self._group_filter_signal_decision), \
             patch("app.backtest._entry_filter_confirm_decision", side_effect=self._group_filter_confirm_decision), \
             patch("app.backtest._build_step_bars", return_value=self._step_bars(df)), \
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

        self.assertEqual(len(result.trades), 0)
        self.assertEqual(result.summary.get("entry_filter", {}).get("stats", {}).get("created"), 1)
        self.assertEqual(result.summary.get("entry_filter", {}).get("stats", {}).get("blocked"), 1)
        self.assertTrue(result.summary.get("entry_filter", {}).get("Long Entry", {}).get("groups", {}).get("0", {}).get("enabled"))

    def test_group_specific_entry_filter_does_not_leak_to_other_group(self) -> None:
        df = self._sample_stream().copy()
        df["market_state"] = ["FLAT", "BULL_EXPANSION", "FLAT", "FLAT", "FLAT", "FLAT"]
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
            "Long Entry": [
                [Rule("1m", "state", "market_state", "=", "BULL_EXPANSION")],
                [Rule("1m", "state", "market_state", "=", "BEAR_EXPANSION")],
            ],
            "Long Exit": [],
            "Short Entry": [],
            "Short Exit": [],
        }
        join_mode = {"Long Entry": "OR", "Long Exit": "AND", "Short Entry": "AND", "Short Exit": "AND"}
        group_join_mode = {"Long Entry": ["OR", "OR"], "Long Exit": [], "Short Entry": [], "Short Exit": []}
        entry_filters = {
            "Long Entry": {
                "enabled": False,
                "mode": "OFF",
                "groups": {
                    "1": EntryFilterConfig(enabled=True, mode="ANTI_CHASE", confirm_bars=1),
                },
            },
            "Short Entry": EntryFilterConfig(),
        }

        with patch("app.backtest._entry_filter_signal_decision", side_effect=self._group_filter_signal_decision), \
             patch("app.backtest._entry_filter_confirm_decision", side_effect=self._group_filter_confirm_decision), \
             patch("app.backtest._build_step_bars", return_value=self._step_bars(df)), \
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
        self.assertEqual(result.trades[0].entry_reason, "Long Entry")
        self.assertEqual(result.summary.get("entry_filter", {}).get("stats", {}).get("created"), 0)


if __name__ == "__main__":
    unittest.main()
