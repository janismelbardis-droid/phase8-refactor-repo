# Phase 24 — Delivery E: Controlled live order-flow integration

This phase keeps all previous functionality intact and does **not** touch backtest or indicator math.

## What was added

- A new live-only order-flow integration policy with three modes:
  - `ADVISORY`
  - `SOFT`
  - `HARD`
- Default mode is `SOFT` to preserve the previous live behavior as closely as possible.
- New environment variable: `PHASE24_OF_INTEGRATION_MODE`

## Mode behavior

- `ADVISORY`
  - order flow never blocks or enables the breakout trigger
  - it only annotates the live state
- `SOFT`
  - confirmed breakout needs OF pass
  - watch / ready states can still stay live on structure alone or get elevated by OF pass
- `HARD`
  - live breakout trigger requires both structural readiness and OF pass whenever a gate is active

## New live fields

- `market_breakout_trigger_ready_raw`
- `market_breakout_trigger_ready_filtered`
- `of_integration_mode`
- `of_integration_effect`
- `of_integration_blocked`
- `of_integration_pass`
- `of_integration_note`

## UI

- The timeframe breakout line now shows the integration mode and blocked/pass hint.
- Market-state detail popup now shows raw-vs-live breakout readiness and the integration note.
- Order-flow detail popup now shows integration mode/effect, blocked state, and raw-vs-live readiness.
- Event trail entries now include integration effect in their summary.

## Safety

- `backtest.py` unchanged
- indicator math unchanged
- all changes are live-only
