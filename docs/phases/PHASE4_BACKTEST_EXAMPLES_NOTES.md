Phase 4: real backtest examples + market-state diagnostics

What was added:
- Preset Visualization report can now include real trade examples from the currently loaded backtest.
- Examples show entry/exit timing, PnL, reasons, and entry market-state summaries by timeframe.
- Market state details window now shows classifier diagnostics:
  - direction score
  - energy score
  - alignment bonus
  - normalized MACD gap
- Market-state score fields are now carried through live snapshots and cached historical streams.
- Indicator cache version bumped to 11 so old cache files do not hide the new diagnostics fields.

What was intentionally not done in this phase:
- No automatic threshold optimizer.
- No changes to VIDYA / FRAMA / Range Filter core formulas.
- No removal of existing features.

Purpose:
- make the preset visualization less abstract by grounding it in actual backtest trades;
- make threshold tuning easier later by exposing the internal market-state scores.
