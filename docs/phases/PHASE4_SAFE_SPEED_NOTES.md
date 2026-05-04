# Phase 4 Safe Speed Notes

This phase focuses on reducing Python object churn in the backtest hot path without changing strategy math or trade rules.

## What changed

- added **slim rule-eval snapshots** for common indicator-only backtest fields
- added a **small bounded row-snapshot cache** so repeated previous-bar / HTF source-bar lookups reuse normalized snapshots
- kept **full snapshots for stored trades** so audits, inspectors, and saved reports still retain rich entry/exit context
- skipped empty tab evaluation when the default rule engine is in use
- for tick-prep, only store full snapshots/traces when a signal actually fires

## Safety guardrails

- any unsupported or unknown requested field automatically falls back to the legacy full snapshot path
- trade entry/exit snapshots still use the full schema
- test compatibility with patched `eval_tab_generic` is preserved

## Expected effect

This does not change indicator formulas or order fill behavior. It is meant to speed up backtests by making each decision step cheaper, especially for longer 1m candle runs with a small active indicator set.
