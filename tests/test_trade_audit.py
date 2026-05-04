from __future__ import annotations

import unittest
import pandas as pd

from tests.common import ROOT
from app.backtest import BacktestConfig, BacktestResult, Trade
from app.research.trade_audit import (
    build_trade_audit_report,
    parse_formatted_tab_trace,
    trade_audit_dataframe,
)


ENTRY_TRACE = """Tab: Long Entry | Result: TRUE | GroupJoin: AND
TF[1m] cur={'bar_time': '2026-01-01T00:00:00+00:00'} | prev={'bar_time': '2025-12-31T23:59:00+00:00'}
Group 1: Join=AND | Result=TRUE
  1.1 TRUE [TRIGGER] :: 1m market_breakout_confirmation >= 0.6
      cur=0.71 | 0.71 >= 0.6 -> True
  1.2 FALSE :: 1m market_volume_support >= 0.4
      cur=0.39 | 0.39 >= 0.4 -> False
"""

EXIT_TRACE = """Tab: Long Exit | Result: TRUE | GroupJoin: OR
Group 1: Join=OR | Result=TRUE
  1.1 TRUE :: 1m trend_broken == True
      cur=True | True == True -> True
"""


class TradeAuditTests(unittest.TestCase):
    def _sample_result(self) -> BacktestResult:
        trade = Trade(
            trade_id=1,
            side="LONG",
            entry_time=pd.Timestamp("2026-01-01T00:00:00Z"),
            entry_price=100.0,
            exit_time=pd.Timestamp("2026-01-01T00:10:00Z"),
            exit_price=101.0,
            qty=1.0,
            entry_notional=100.0,
            exit_notional=101.0,
            gross_pnl=1.0,
            fees=0.1,
            net_pnl=0.9,
            return_pct=0.9,
            duration_min=10,
            mfe=1.5,
            mae=-0.3,
            mfe_pct=1.5,
            mae_pct=-0.3,
            entry_reason="Long Entry",
            exit_reason="Long Exit",
            entry_rule_trace=ENTRY_TRACE,
            exit_rule_trace=EXIT_TRACE,
            entry_snapshot={"1m": {"market_state": "BREAKOUT_CONFIRM_LONG", "market_phase": "EXPANSION", "market_breakout_readiness": 0.68, "market_breakout_confirmation": 0.71}},
            exit_snapshot={"1m": {"market_state": "BULL_EXHAUSTION", "market_phase": "EXHAUSTION"}},
        )
        return BacktestResult(
            config=BacktestConfig(initial_balance=1000.0, order_notional_usdt=100.0),
            start=pd.Timestamp("2026-01-01T00:00:00Z"),
            end=pd.Timestamp("2026-01-01T01:00:00Z"),
            trades=[trade],
            equity_curve=pd.DataFrame(),
            summary={"symbol": "BTCUSDT", "entry_filter": {"Long Entry": {"enabled": True}}},
        )

    def test_parse_formatted_tab_trace(self) -> None:
        parsed = parse_formatted_tab_trace(ENTRY_TRACE)
        self.assertEqual(parsed["tab"], "Long Entry")
        self.assertTrue(parsed["result"])
        self.assertEqual(len(parsed["groups"]), 1)
        self.assertEqual(len(parsed["groups"][0]["rules"]), 2)
        self.assertTrue(parsed["groups"][0]["rules"][0]["is_trigger"])
        self.assertFalse(parsed["groups"][0]["rules"][1]["result"])

    def test_parse_formatted_tab_trace_keeps_sequence_reset_line(self) -> None:
        trace = """Tab: Long Entry | Result: FALSE | GroupJoin: AND
Group 1: Join=SEQUENCE | Result=FALSE
  Sequence progress: completed=0/2 | next_step=1
  Sequence reset: invalidated by opposite SHORT trigger while building LONG setup
"""
        parsed = parse_formatted_tab_trace(trace)
        self.assertEqual(
            parsed["groups"][0]["sequence_reset"],
            "Sequence reset: invalidated by opposite SHORT trigger while building LONG setup",
        )

    def test_parse_formatted_tab_trace_reads_sequence_canceler_badge(self) -> None:
        trace = """Tab: Long Entry | Result: FALSE | GroupJoin: AND
Group 1: Join=SEQUENCE | Result=FALSE
  1.1 FALSE [SEQ CANCEL] :: 1m PPO = RED [SEQ CANCEL]
      cur=GREEN | GREEN = RED -> False
"""
        parsed = parse_formatted_tab_trace(trace)
        self.assertTrue(parsed["groups"][0]["rules"][0]["is_sequence_canceler"])

    def test_build_trade_audit_report(self) -> None:
        report = build_trade_audit_report(self._sample_result(), include_snapshots=False)
        self.assertEqual(report["trade_count"], 1)
        trade = report["trades"][0]
        self.assertEqual(trade["entry_market_context"]["1m"]["market_state"], "BREAKOUT_CONFIRM_LONG")
        self.assertEqual(trade["entry_rule_audit"]["stats"]["rules_passed"], 1)
        self.assertEqual(trade["entry_rule_audit"]["stats"]["rules_failed"], 1)
        self.assertIn("market_state_thresholds", report["threshold_pack"])

    def test_trade_audit_dataframe(self) -> None:
        df = trade_audit_dataframe(self._sample_result())
        self.assertEqual(len(df), 1)
        self.assertEqual(df.iloc[0]["entry_market_state"], "BREAKOUT_CONFIRM_LONG")
        self.assertEqual(int(df.iloc[0]["entry_rules_failed"]), 1)


if __name__ == "__main__":
    unittest.main()
