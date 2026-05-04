from __future__ import annotations

import unittest
from unittest.mock import patch

import pandas as pd

from app.backtest import BacktestConfig, run_backtest
from app.rules import snapshot_from_stream_row


class _GuardRow(dict):
    def __init__(self, *args, forbidden=None, **kwargs):
        super().__init__(*args, **kwargs)
        self.forbidden = set(forbidden or set())

    def get(self, key, default=None):
        if key in self.forbidden:
            raise AssertionError(f"unexpected field lookup: {key}")
        return super().get(key, default)


class Phase4PerfSafetyTests(unittest.TestCase):
    def test_selective_snapshot_only_reads_requested_fields(self) -> None:
        row = _GuardRow(
            {
                "macd": 1.25,
                "signal": 0.75,
                "close_time": pd.Timestamp("2026-01-01T00:00:00Z"),
                "is_closed": True,
            },
            forbidden={
                "adx",
                "frama_state",
                "vidya_state",
                "range_filter_state",
                "market_state",
            },
        )
        snap = snapshot_from_stream_row(row, required_fields={"macd", "signal"})
        self.assertEqual(snap["macd"], 1.25)
        self.assertEqual(snap["signal"], 0.75)
        self.assertIn("snapshot_time", snap)
        self.assertNotIn("adx", snap)
        self.assertNotIn("market_state", snap)

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

    def _fake_eval(self, tab_name, rules_model, tab_group_join_mode, group_rule_join_mode, cur_by_tf, prev_by_tf, ctx=None):
        ts = str((cur_by_tf.get("1m") or {}).get("snapshot_time") or "")
        if tab_name == "Long Entry" and ts == "2026-01-01T00:01:00+00:00":
            return True
        return False

    def test_bar_backtest_keeps_full_trade_snapshots(self) -> None:
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
        trade = result.trades[0]
        snap = trade.entry_snapshot.get("1m", {})
        self.assertEqual(snap.get("market_state"), "BULL_EXPANSION")
        self.assertEqual(snap.get("market_phase"), "EXPANSION")
        self.assertEqual(snap.get("macd"), 1.0)
        self.assertIn("snapshot_time", snap)


if __name__ == "__main__":
    unittest.main()
