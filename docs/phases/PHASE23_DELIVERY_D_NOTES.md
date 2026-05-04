# Phase 23 — Delivery D: Order-Flow Replay Trail & Event Snapshots

This delivery adds replay-friendly order-flow event logging without touching backtest or indicator math.

## Scope
- **No changes to backtest**
- **No changes to Range Filter / FRAMA / VIDYA math**
- Live-only diagnostics and replay trail for the order-flow / boundary breakout layer

## What was added
- Per-timeframe in-memory order-flow event trail (`maxlen=400`)
- Compact event snapshots whenever the important OF/boundary/filter state changes
- Stored event fields include:
  - timestamp
  - event type
  - market breakout state / bias
  - OF state / bias
  - quality / confidence
  - boundary state
  - filter gate / pass
  - delta / progress / DOM pressure
  - concise summary and reason text
- New `LiveEngine.get_orderflow_event_history(tf, limit=...)`
- OF popup now shows:
  - event count
  - last event type
  - last event summary
- New **Trail…** popup window:
  - recent events newest-first
  - why confirm/watch passed or failed
  - compact replay/debug trail

## Intent
This phase is meant to make the live microstructure layer auditable:
- why a watch passed
- why a confirm was blocked
- when a boundary failed
- what the engine believed at the time

## Files changed
- `app/live_engine.py`
- `app/ui_app.py`

## Files intentionally untouched
- `app/backtest.py`
- `app/indicators_streaming.py`
- `app/rules.py`
