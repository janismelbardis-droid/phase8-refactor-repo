# PHASE21 Delivery B — Freshness / Integrity / Confidence Gating

This delivery implements the third workflow phase only:

3. Freshness, integrity, and fault safety for the live order-flow layer.

## What changed

- **No backtest changes**.
- **No indicator math changes** for Range Filter, FRAMA, VIDYA, MACD/PPO, ATR, ADX.
- Existing market-evaluation and breakout logic remain in place.
- The live order-flow layer now evaluates whether tape/DOM data is fresh enough to be trusted before it can confirm breakouts.

## New safety logic

### Tape freshness
- tracks the age of the latest aggTrade update
- classifies tape quality as:
  - `GOOD`
  - `DEGRADED`
  - `STALE`
  - `NO_DATA`
  - `DISABLED`

### DOM freshness / integrity
- tracks depth age and snapshot age
- respects local-L2 sync state and resync-required state
- classifies DOM quality as:
  - `GOOD`
  - `DEGRADED`
  - `STALE`
  - `NO_DATA`
  - `INVALID`
  - `DISABLED`

### Confirmation locks
- **watch** states are blocked if the available order-flow data is stale/missing
- **confirm** states are blocked if the required tape/DOM data is stale, missing, unsynced, or invalid
- in `AUTO` / `LOCAL_L2`, hard confirmation now requires a healthy synced local L2 book
- in `PARTIAL_DOM`, partial-depth can still be used, but with lower confidence

## New backstage fields

The OF popup now shows:
- overall order-flow data quality
- overall confidence %
- tape age / tape quality
- DOM quality / DOM confidence
- whether watch is allowed
- whether confirmation is allowed
- watch/confirm lock states and reasons

## Why this matters

This phase prevents the live order-flow layer from quietly confirming breakouts with:
- stale tape
- stale DOM
- damaged local-book continuity
- startup states before local L2 sync is ready

## Not included yet

This delivery does **not** yet add:
- boundary-aware acceptance logic
- replay/event logging
- deeper trigger integration

Those are reserved for later workflow phases.
