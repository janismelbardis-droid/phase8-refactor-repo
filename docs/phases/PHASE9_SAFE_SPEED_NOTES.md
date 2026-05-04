# Phase 9 — Safe speed: local persistent indicator store

This phase keeps indicator math and backtest trade logic intact.

## What changed

- added a persistent **daily chunked local indicator store** under `cache_dir/indicator_store/...`
- `simulate_multitf_indicators(...)` now tries, in order:
  1. exact indicator cache
  2. covering indicator cache
  3. local persistent indicator store
  4. full recompute only if needed
- newly computed indicator streams are now also written into the local daily indicator store
- exact-window indicator cache behavior stays in place for compatibility

## Why this helps

Repeated backtests on overlapping windows can now reuse already prepared indicator streams even after app restarts, without requiring an exact prior request-window cache hit.

## Safety

- no indicator formulas changed
- no backtest execution rules changed
- no report schema changed
- exact-window cache behavior is preserved
