# Phase 2 Safe Speed Patch

This phase focuses on reducing backtest preparation time without changing strategy logic.

## Changes

- Added strategy dependency analysis (`app/strategy_requirements.py`)
- Backtest/history warmup now sizes from active indicator families instead of the full indicator universe
- Indicator simulation now skips heavy per-row updates when those indicator families are not needed
- Indicator cache keys now include the required feature profile, and larger/full caches can satisfy smaller/selective requests
- Backtest/history indicator computation now uses the current market-state thresholds only when market-state fields are actually required
- Removed eager market-state rebuild after history/backtest completion; on-demand tools can still rebuild later

## Safety notes

- Unknown/custom fields fall back to the full indicator profile
- Existing backtest logic, fills, and rule evaluation are unchanged
- Cache/profile changes are additive and isolated by cache-key signature

## Validation

- Existing phase-1 perf safety tests pass
- Existing replay tests pass
- Added phase-2 tests for selective warmup, selective compute, and selective cache reuse
