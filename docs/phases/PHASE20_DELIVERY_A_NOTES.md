# PHASE20 Delivery A — Safe Local L2 Foundation

This delivery implements the first two workflow phases only:

1. Freeze/protect the current live order-flow architecture with feature flags and rollback modes.
2. Replace snapshot-only DOM handling with a true local L2 book path built from Binance futures REST depth snapshot + diff-depth updates.

## What changed

- **No backtest changes**.
- **No indicator math changes** for Range Filter, FRAMA, VIDYA, MACD/PPO, ATR, ADX.
- Existing market-evaluation and breakout logic remain in place.
- Live order-flow now supports the following modes:
  - `OFF`
  - `TAPE_ONLY`
  - `PARTIAL_DOM`
  - `LOCAL_L2`
  - `AUTO` (default)

## Default behavior

`AUTO` mode is the new safe default:
- uses aggTrade tape
- subscribes to diff-depth `@depth@100ms`
- keeps a true local L2 book using snapshot + diffs
- also keeps partial-depth fallback `@depth20@100ms` until local L2 sync succeeds

## New backstage fields

The OF popup now shows:
- mode
- DOM source
- sync state
- resync-needed flag
- snapshot ID / last update IDs
- buffered / applied local-book events
- partial fallback status
- depth/snapshot ages

## Not included yet

This delivery does **not** yet add:
- stale-data gating / confidence locks
- boundary-aware acceptance logic
- replay logging
- tighter trigger integration

Those are reserved for the later workflow phases.
