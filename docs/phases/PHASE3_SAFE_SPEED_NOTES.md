# Phase 3 Safe Speed Notes

This phase keeps the strategy logic intact and focuses on cutting repeated preparation work.

## What changed

- Added a small **in-memory prepared dataset cache** for history/backtest runs.
  - Reuses already-fetched 1m candles and already-computed indicator streams.
  - Allows a larger prepared window to satisfy a smaller request.
  - Allows a richer prepared feature set to satisfy a smaller feature request.
  - Requires matching market-state thresholds when market-state features are involved.
- History and backtest workers now try **prepared dataset reuse first** before downloading candles or recomputing indicators.
- Expanded replay eligibility to include safe **cost/sizing-only config changes**:
  - `initial_balance`
  - `order_notional_usdt`
  - `fee_rate`
  - `slippage_bps`
- Backtest status text now shows when the run came from a **prepared cache**.

## Safety constraints

- No trade-entry or trade-exit rule logic was rewritten.
- No indicator math was changed.
- Reuse only happens when:
  - symbol matches
  - request window is covered
  - requested timeframes are a subset of prepared timeframes
  - requested indicator families are a subset of prepared families
  - MACD/ADX implementation and price source match
  - market-state thresholds match when market-state fields are required

## Files added/changed

- `app/prepared_dataset.py`
- `app/ui_app.py`
- `app/research/replay.py`
- `tests/test_perf_phase3.py`

## Validation

- `python -m unittest tests.test_perf_phase1 tests.test_perf_phase2 tests.test_perf_phase3 tests.test_backtest_replay`
- `python -m py_compile app/prepared_dataset.py app/ui_app.py app/research/replay.py tests/test_perf_phase3.py`
