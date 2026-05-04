# Tick signal prep safe speed notes

This pass trims unnecessary work in the tick-based backtest signal-preparation stage without changing signal or fill logic.

## What changed

- Tick signal preparation now reuses the same compiled rule-array fast path already used by the bar backtest when a tab is simple enough to compile safely.
- When all active tabs for a decision bar are covered by compiled arrays and none of them fire, the tick engine now skips building full evaluation snapshots for that bar.
- If a compiled signal does fire, the engine still materializes the normal evaluation snapshots before building traces and full signal snapshots, so trade details stay intact.
- Bars-ago / unsupported rule families still fall back to the legacy snapshot-based evaluator.
- The tick path now also reuses a prebuilt `subs_tick` mapping instead of rebuilding the same timeframe dictionary on every decision bar.

## Why this is safe

- No indicator formulas changed.
- No bars-ago logic changed.
- No sequence logic changed.
- No entry / exit / fill pricing logic changed.
- Tick-based testing remains available.
- Fired signals still keep the same trace + snapshot behavior.

## Validation

Validated with:

- `python scripts/phase_tick_signal_prep_smoke_check.py`
- targeted tick-prep performance safety tests
- selected regression/perf tests for phases 0, 3, 4, 10, and 11
