# Audit fixes applied

This project copy contains the following targeted backtest/reporting fixes:

1. `app/rules.py`
   - `snapshot_from_stream_row()` now returns `macd`.
   - `snapshot_from_stream_row()` now returns `is_closed` with a safe default.

2. `app/backtest.py`
   - Fixed `_pending_from_signal()` to pass `signal_rule_trace` into `_PendingEntry`.
   - Added explicit exit lifecycle timestamps:
     - `exit_signal_time`
     - `exit_order_created_time`
   - Fixed reversal path bug where `scheduled_order.reverse_rule_trace` was accessed after `scheduled_order = None`.
   - Non-rule OHLCV exits (gap/range/forced-close) now use the latest closed snapshot instead of incorrectly reusing `entry_snapshot`.
   - Tick non-rule exits now prefer the latest closed snapshot instead of incorrectly reusing `entry_snapshot`.
   - Rule exits in both bar and tick paths now preserve exit signal/order timestamps.

3. `app/ui_app.py`
   - JSON export/import now preserves the new exit timing fields.
   - Trade inspector now shows entry and exit timing separately:
     - signal time
     - order created time
     - fill time
   - Exit indicator columns are labeled as `X sig` to clarify that indicator values belong to the exit signal snapshot, not necessarily the exit fill candle.

## Validation

Validated with:

- `python -m py_compile app/rules.py app/backtest.py app/ui_app.py`
- direct smoke tests for:
  - `snapshot_from_stream_row()` returning MACD
  - `_pending_from_signal()` constructing successfully with trace preservation
