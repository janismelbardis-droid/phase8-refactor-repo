# Phase 5 Safe Speed Notes

This phase trims unnecessary rule-tab evaluation work in the bar backtest hot path without changing trade logic.

## What changed

- Added a state-aware tab evaluation plan for bar backtests.
- While flat, the engine now skips ordinary exit-tab evaluation.
- While in a long position, the engine now skips ordinary long-entry / short-exit evaluation.
- While in a short position, the engine now skips ordinary short-entry / long-exit evaluation.
- While a scheduled order is already pending, the engine skips ordinary extra tab evaluation for that bar.
- Tabs using `SEQUENCE` group logic are **not** skipped, even when they are not immediately actionable, so sequence state remains safe.
- If the tab evaluator is monkeypatched or replaced, the engine falls back to the legacy “evaluate every active tab” behavior.

## Why this is safe

- No indicator formulas changed.
- No order scheduling / fill logic changed.
- No trade snapshot/report shape changed.
- `SEQUENCE` tabs keep their previous evaluation cadence.
- Non-default evaluator implementations keep the old behavior.

## Expected benefit

Most ordinary strategies do not use sequence groups. For those strategies, bar-mode backtests avoid a large share of unnecessary tab evaluations on every decision bar.
