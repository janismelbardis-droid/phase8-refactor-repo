# Phase 4 — Backtest execution/reporting structural split

This phase is a structure-only refactor.

## What changed

- added `app/backtest_models.py` for shared backtest dataclasses and internal execution/reporting payload models
- added `app/backtest_execution.py` for:
  - lifecycle helpers
  - slippage helpers
  - stop loss / take profit resolution helpers
  - OHLCV / tick level exit helpers
  - rule-trace helpers
- added `app/backtest_reporting.py` for:
  - trade dataframe export helper
  - trade reporting summary metrics helper
  - replay-context normalization / builder
- kept `app/backtest.py` as the public facade so existing imports still work

## What stayed untouched

- indicator formulas
- rule math
- bars-ago behavior
- filter behavior
- sequence behavior
- OHLCV backtest execution behavior
- tick-based tester availability and behavior
- replay entry points and result shape

## Why this phase exists

The previous phase split planning and frame/snapshot handling. This phase finishes the next safe boundary by separating:

- execution mechanics
- reporting / replay-context preparation
- shared backtest models

That makes the remaining engine work easier to reason about without forcing a behavior rewrite.

## Validation

Validated with:

- `python -m py_compile app/backtest.py app/backtest_models.py app/backtest_execution.py app/backtest_reporting.py`
- `python scripts/phase4_execution_reporting_split_smoke_check.py`
- full test suite
