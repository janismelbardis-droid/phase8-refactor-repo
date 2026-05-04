# Phase 6 — Market State Outcome Diagnostics

Added in this phase:

- Market-state outcome diagnostics report driven by the currently loaded backtest result.
- Aggregation by timeframe using entry snapshots already attached to trades.
- Ranked outcome tables by entry market state.
- Ranked outcome tables by entry market phase.
- Entry-state to exit-state transition matrix per timeframe.
- Outcome bucket counts (WIN / LOSS / FLAT) per entry state.
- Main UI button: `State Outcomes…`
- Preset visualization window button: `State Outcomes…`

What stays untouched:

- VIDYA core formula
- FRAMA
- Range Filter
- Existing backtest execution logic
- Existing market-state board
- Existing threshold lab
- Existing preset visualization report

Notes:

- The diagnostics report evaluates the currently loaded backtest against the currently selected or unsaved preset context.
- It is intentionally read-only and does not modify thresholds, rules, or historical data.
- This phase adds synthesis / diagnostics only, not new trading rules.
