# Phase 11 — Summary-first backtest UX + explicit exit/cost replay

This phase keeps indicator math and backtest trade math intact while making the normal backtest path cheaper and more explicit.

## What changed

### 1. Explicit UI replay action
A new **Apply exits / costs only** button now calls the existing replay engine directly for bar-mode backtests when the current request is replay-eligible.

This is intended for quick iteration on:
- stop loss / take profit settings
- fee / slippage
- initial balance / order notional

### 2. Fast backtest mode (lazy trade details)
A new **Fast backtest mode (lazy trade details)** checkbox defaults to ON.

When enabled, the engine still computes summary metrics and trades exactly, but it does **not** eagerly store heavy per-trade snapshots/traces during the main run.

Instead, trades keep lightweight row references so details can be materialized later from the cached/prepared streams.

### 3. Lighter equity curve sampling
Fast mode samples the saved `equity_curve` (for UI plotting) instead of appending a rich equity row for every minute bar. Summary metrics and drawdown are still computed on the exact internal path.

### 4. On-demand trade details
Double-clicking a trade (or using the new **Generate selected trade details** button) now materializes missing snapshots from the current backtest streams on demand.

If a backtest was run in fast mode, traces are shown with a clear placeholder message rather than pretending full traces were captured.

### 5. Manual heavy report generation
A new **Trade audit…** button exports the trade-audit pack only when requested, instead of making every backtest pay for that work.

## Safety boundaries
- indicator formulas unchanged
- rule math unchanged
- fill logic unchanged
- TP/SL logic unchanged
- unsupported/legacy full-detail behavior still works when fast mode is disabled

## Known limitation
Fast mode can reconstruct snapshots on demand from stored streams, but it does **not** recreate full original per-rule traces after the fact. For exact trade traces, rerun with fast mode turned off.
