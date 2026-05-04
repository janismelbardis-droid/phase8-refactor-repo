from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

import pandas as pd


@dataclass
class BacktestConfig:
    initial_balance: float = 1000.0
    order_notional_usdt: float = 100.0
    fee_rate: float = 0.0004          # fraction (0.0004 = 0.04%)
    slippage_bps: float = 0.0         # basis points
    stop_loss_pct: float = 0.0        # legacy percent field; preserved for compatibility
    take_profit_pct: float = 0.0      # legacy percent field; preserved for compatibility
    break_even_after_mfe_pct: float = 0.0  # arm break-even after favorable move of X%
    trailing_stop_pct: float = 0.0        # trail from best favorable price by X%
    failed_pullback_abort_minutes: int = 0
    failed_pullback_abort_min_mfe_pct: float = 0.0
    failed_pullback_abort_max_close_return_pct: float = 0.0
    stop_loss_mode: str = "PERCENT"  # OFF / PERCENT / ABSOLUTE / ATR / FIELD
    stop_loss_value: float = 0.0      # % / absolute distance / ATR multiple depending on mode
    stop_loss_timeframe: str = "1m"  # timeframe for ATR/FIELD lookup
    stop_loss_field: str = ""        # snapshot field when mode=FIELD
    stop_loss_offset_pct: float = 0.0 # FIELD mode only; extend level away from entry by this %
    take_profit_mode: str = "PERCENT" # OFF / PERCENT / ABSOLUTE / ATR / FIELD / R_MULTIPLE
    take_profit_value: float = 0.0     # % / absolute distance / ATR multiple / R multiple depending on mode
    take_profit_timeframe: str = "1m" # timeframe for ATR/FIELD lookup
    take_profit_field: str = ""       # snapshot field when mode=FIELD
    take_profit_offset_pct: float = 0.0 # FIELD mode only; extend level away from entry by this %
    allow_reverse: bool = True
    skip_if_both_entries: bool = True
    tick_auto_download: bool = False  # if True, tick backtest downloads missing daily caches
    tick_cache_partition: str = "daily"  # "daily" (recommended)
    step_timeframe: str = "1m"      # backtest evaluation step timeframe (1m/5m/15m...)
    capture_trade_details: bool = True  # when False, keep backtest summary/trades light and materialize details on demand
    equity_curve_stride: int = 1        # sample every N bars into equity_curve (summary metrics stay exact)


@dataclass
class Trade:
    trade_id: int
    side: str  # LONG/SHORT
    entry_time: pd.Timestamp
    entry_price: float
    exit_time: pd.Timestamp
    exit_price: float
    qty: float

    entry_notional: float
    exit_notional: float

    gross_pnl: float
    fees: float
    net_pnl: float
    return_pct: float

    duration_min: int
    mfe: float
    mae: float
    mfe_pct: float
    mae_pct: float

    entry_reason: str
    exit_reason: str
    entry_rule_trace: str
    exit_rule_trace: str

    entry_snapshot: Dict[str, Dict[str, Any]]
    exit_snapshot: Dict[str, Dict[str, Any]]

    # Optional: visualization/debug info for Bars-Ago constraints at entry/exit.
    # Stored as JSON-serializable dicts (see _barsago_profile_for_tab).
    entry_barsago: Dict[str, Any] = field(default_factory=dict)
    exit_barsago: Dict[str, Any] = field(default_factory=dict)
    # Fee breakdown (per side). Optional; stored for reporting clarity.
    fee_entry: float = 0.0
    fee_exit: float = 0.0
    # Phase 2 lifecycle timing fields. These are additive and preserve existing reporting.
    signal_time: Optional[pd.Timestamp] = None
    order_created_time: Optional[pd.Timestamp] = None
    entry_fill_time: Optional[pd.Timestamp] = None
    exit_signal_time: Optional[pd.Timestamp] = None
    exit_order_created_time: Optional[pd.Timestamp] = None
    exit_fill_time: Optional[pd.Timestamp] = None
    engine_type: str = ""
    fill_source: str = ""
    ambiguous_exit: bool = False
    stop_loss_price: Optional[float] = None
    take_profit_price: Optional[float] = None
    entry_row_index: Optional[int] = None
    exit_row_index: Optional[int] = None


@dataclass
class _ResolvedLevelExit:
    reason: str
    fill_price: float
    at_open: bool = False
    ambiguous: bool = False


@dataclass
class _TradeLifecycle:
    """Shared timing/lifecycle record for a single backtest position.

    Phase 2 introduces this object so signal generation, order creation,
    entry fill, and exit fill are tracked separately without changing the
    protected indicator math or rule math.
    """

    state: str = "FLAT"
    side: str = ""
    signal_time: Optional[pd.Timestamp] = None
    order_created_time: Optional[pd.Timestamp] = None
    entry_fill_time: Optional[pd.Timestamp] = None
    exit_signal_time: Optional[pd.Timestamp] = None
    exit_order_created_time: Optional[pd.Timestamp] = None
    exit_fill_time: Optional[pd.Timestamp] = None
    entry_fill_price: Optional[float] = None
    exit_fill_price: Optional[float] = None
    qty: float = 0.0
    entry_reason: str = ""
    exit_reason: str = ""
    entry_rule_trace: str = ""
    exit_rule_trace: str = ""
    engine_type: str = ""
    fill_source: str = ""
    ambiguous_exit: bool = False
    stop_loss_price: Optional[float] = None
    take_profit_price: Optional[float] = None
    entry_snapshot: Dict[str, Dict[str, Any]] = field(default_factory=dict)
    exit_snapshot: Dict[str, Dict[str, Any]] = field(default_factory=dict)
    entry_barsago: Dict[str, Any] = field(default_factory=dict)
    exit_barsago: Dict[str, Any] = field(default_factory=dict)


@dataclass
class BacktestResult:
    config: BacktestConfig
    start: pd.Timestamp
    end: pd.Timestamp
    trades: List[Trade]
    equity_curve: pd.DataFrame
    summary: Dict[str, Any]
    replay_context: Optional[Dict[str, Any]] = None


@dataclass
class _EntryFilterBar:
    time: pd.Timestamp
    open: float
    high: float
    low: float
    close: float
    atr: Optional[float]

    @property
    def range(self) -> float:
        try:
            return max(0.0, float(self.high) - float(self.low))
        except Exception:
            return 0.0


@dataclass
class _PendingEntry:
    side: str                        # LONG / SHORT
    origin: str                      # ENTRY / REVERSE
    tab_name: str                    # Long Entry / Short Entry
    signal_time: pd.Timestamp
    signal_step: int
    signal_bar: _EntryFilterBar
    signal_snapshot: Dict[str, Dict[str, Any]]
    signal_snapshot_row_index: Optional[int]
    signal_reason: str
    signal_rule_trace: str
    bars_ago: Dict[str, Any]
    matched_group_indices: List[int]
    due_step: int
    confirm_bars: int


@dataclass
class _ScheduledBarOrder:
    kind: str                        # OPEN / CLOSE
    fill_index: int
    side: str = ""
    reason: str = ""
    rule_trace: str = ""
    signal_time: Optional[pd.Timestamp] = None
    order_created_time: Optional[pd.Timestamp] = None
    snapshot: Dict[str, Dict[str, Any]] = field(default_factory=dict)
    snapshot_row_index: Optional[int] = None
    barsago: Dict[str, Any] = field(default_factory=dict)
    reverse_side: Optional[str] = None
    reverse_reason: str = ""
    reverse_rule_trace: str = ""


@dataclass
class _EntryFilterStats:
    created: int = 0
    confirmed: int = 0
    blocked: int = 0
    hard_skipped: int = 0
    cancelled: int = 0
    replaced: int = 0
