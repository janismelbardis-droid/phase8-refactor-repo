from __future__ import annotations

import py_compile
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

TARGETS = [
    ROOT / "app" / "backtest.py",
    ROOT / "app" / "backtest_models.py",
    ROOT / "app" / "backtest_execution.py",
    ROOT / "app" / "backtest_reporting.py",
    ROOT / "tests" / "test_phase4_execution_reporting_split.py",
]

for path in TARGETS:
    py_compile.compile(str(path), doraise=True)
    print(f"OK  {path.relative_to(ROOT)}")

from app import backtest as bt
from app import backtest_execution as exec_mod
from app import backtest_reporting as report_mod
from app.backtest import BacktestConfig
from app.backtest_models import Trade
import pandas as pd

assert bt._apply_slippage.__module__ == "app.backtest_execution"
assert bt._make_trade_df.__module__ == "app.backtest_reporting"
assert bt._apply_slippage is exec_mod._apply_slippage
assert bt._make_trade_df is report_mod._make_trade_df

cfg = BacktestConfig(stop_loss_mode="PERCENT", stop_loss_value=1.0, take_profit_mode="R_MULTIPLE", take_profit_value=2.0)
stop_price, take_profit_price = bt._compute_stop_take_prices(100.0, "LONG", cfg, {"1m": {"atr": 1.5}})
assert round(float(stop_price), 8) == 99.0
assert round(float(take_profit_price), 8) == 102.0

trade = Trade(
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
    mfe=1.2,
    mae=-0.5,
    mfe_pct=1.2,
    mae_pct=-0.5,
    entry_reason="Long Entry",
    exit_reason="Take Profit",
    entry_rule_trace="entry",
    exit_rule_trace="exit",
    entry_snapshot={"1m": {"close": 100.0}},
    exit_snapshot={"1m": {"close": 101.0}},
    signal_time=pd.Timestamp("2026-01-01T00:00:00Z"),
    order_created_time=pd.Timestamp("2026-01-01T00:00:30Z"),
    entry_fill_time=pd.Timestamp("2026-01-01T00:01:00Z"),
    exit_fill_time=pd.Timestamp("2026-01-01T00:05:00Z"),
    engine_type="OHLCV",
    fill_source="BAR",
)
df = bt._make_trade_df([trade])
assert list(df["trade_id"]) == [1]
summary = bt._apply_trade_reporting_metrics({}, [trade])
assert summary["engine_type_counts"] == {"OHLCV": 1}
assert summary["exit_reason_counts"] == {"Take Profit": 1}
print("Phase 4 execution/reporting split smoke check passed")
