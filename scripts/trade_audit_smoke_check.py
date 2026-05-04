from __future__ import annotations

from pathlib import Path
import sys
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from app.backtest import BacktestConfig, BacktestResult, Trade  # noqa: E402
from app.research.trade_audit import build_trade_audit_report, trade_audit_dataframe  # noqa: E402


def main() -> int:
    result = BacktestResult(
        config=BacktestConfig(initial_balance=1000.0, order_notional_usdt=100.0),
        start=pd.Timestamp("2026-01-01T00:00:00Z"),
        end=pd.Timestamp("2026-01-01T01:00:00Z"),
        trades=[
            Trade(
                trade_id=1,
                side="LONG",
                entry_time=pd.Timestamp("2026-01-01T00:00:00Z"),
                entry_price=100.0,
                exit_time=pd.Timestamp("2026-01-01T00:05:00Z"),
                exit_price=101.0,
                qty=1.0,
                entry_notional=100.0,
                exit_notional=101.0,
                gross_pnl=1.0,
                fees=0.1,
                net_pnl=0.9,
                return_pct=0.9,
                duration_min=5,
                mfe=1.1,
                mae=-0.2,
                mfe_pct=1.1,
                mae_pct=-0.2,
                entry_reason="Long Entry",
                exit_reason="Long Exit",
                entry_rule_trace="Tab: Long Entry | Result: TRUE | GroupJoin: AND\nGroup 1: Join=AND | Result=TRUE\n  1.1 TRUE :: 1m close > ema\n      cur=101 | prev=100",
                exit_rule_trace="Tab: Long Exit | Result: TRUE | GroupJoin: OR\nGroup 1: Join=OR | Result=TRUE\n  1.1 TRUE :: 1m close < ema\n      cur=100 | prev=101",
                entry_snapshot={"1m": {"market_state": "BULL_EXPANSION", "market_phase": "EXPANSION"}},
                exit_snapshot={"1m": {"market_state": "BULL_EXHAUSTION", "market_phase": "EXHAUSTION"}},
            )
        ],
        equity_curve=pd.DataFrame(),
        summary={"symbol": "BTCUSDT"},
    )
    report = build_trade_audit_report(result, include_snapshots=False)
    df = trade_audit_dataframe(result)
    assert report["trade_count"] == 1
    assert len(df) == 1
    print("trade audit smoke check passed")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
