# Phase 5 — Market State Threshold Lab

This phase adds a session-local threshold tuning workflow for the Market State classifier.

## Added

- **Threshold Lab** button under the Market State Board
- editable thresholds for:
  - VIDYA angle up/down/strong
  - VIDYA delta-volume support/weak thresholds
  - direction-score long/short limits
  - compression direction/gap thresholds
  - confidence base
- **Preview window** with diagnostics before applying changes
- diagnostics summarize:
  - live/latest preview by timeframe
  - loaded History or Backtest dataset label distribution by timeframe
  - latest label/confidence per timeframe
- **Apply to session**:
  - refreshes Market State cards
  - rebuilds market-state columns on loaded history streams
  - rebuilds market-state columns on loaded backtest streams
  - refreshes backtest rendering and preset visualization examples that rely on trade snapshots
- Market State details popup now shows the **active classifier thresholds**
- live/current and closed snapshots are re-labeled using the active session thresholds

## Preserved

- VIDYA core formula
- FRAMA core formula
- Range Filter core formula
- rule engine behavior
- backtest execution logic
- existing preset visualization/report flow

## Scope

Threshold changes are **session-local** and affect only the synthetic Market State classifier layer.
Indicator math stays unchanged.
