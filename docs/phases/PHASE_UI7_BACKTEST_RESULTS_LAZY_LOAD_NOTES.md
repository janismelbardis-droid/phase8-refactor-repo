# PHASE UI7 — Backtest results lazy-load

This phase keeps the backtest rules, math, indicators, bars-ago behavior, filters, sequences, replay behavior, and tick tester mechanics unchanged.

What changed:
- The Backtest tab now opens its results area with a lightweight placeholder.
- The heavy Summary text widget and Trades tree are built only when a backtest result or loaded report is rendered.
- Existing backtest actions still go through the same render path, so behavior stays the same once results exist.

Why this is safe:
- Only Tk widget construction timing changed.
- No execution logic, formulas, or decision semantics changed.
