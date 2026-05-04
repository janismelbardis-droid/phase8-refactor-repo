# Phase 7 Safe Speed Notes

This phase adds a **persistent prepared dataset store** for backtest/history preparation.

## What changed
- Added disk-backed prepared dataset save/load helpers in `app/prepared_dataset.py`
- Prepared datasets now persist:
  - full 1m candle frame for the prepared window
  - full computed indicator streams per timeframe
  - metadata describing symbol, range, timeframes, price source, MACD/ADX impls, required indicator families, and market-state threshold signature
- Backtest/history now try this order:
  1. in-memory prepared dataset reuse
  2. disk prepared dataset reuse
  3. existing candle + indicator preparation path
- Newly prepared history/backtest windows are stored to disk for future runs

## Safety constraints
- Indicator math is unchanged
- Backtest trade logic is unchanged
- Replay logic is unchanged
- Reuse only happens when the prepared dataset safely covers the request
- Market-state-dependent prepared datasets only match when threshold signatures match

## What this phase is and is not
This is a **persistent prepared-data layer**.
It is **not yet** a chunked master candle database or a full "gap-fill only" market data engine.

## Validation
- `python -m unittest tests.test_perf_phase3 tests.test_perf_phase7 tests.test_backtest_replay`
- `python -m py_compile app/prepared_dataset.py app/ui_app.py tests/test_perf_phase7.py`
