# PHASE 22 — Delivery C: Boundary-Aware Breakout Engine

This phase implements the next step from the workflow plan:
- keep backtest untouched
- keep indicator math untouched
- keep all existing live/breakout/order-flow features intact
- add boundary-aware breakout evaluation on top of the existing live market-state and order-flow layers

## What was added

### 1) Live compression boundary context
A live boundary context is now derived from the existing closed-OHLC ring buffer per timeframe.
It exposes a structural compression box using a multi-bar window, including:
- boundary high / low
- width in price and ATR terms
- distance to upper/lower boundary
- upper/lower touch counts
- near-boundary / outside-boundary state
- a compact boundary note for UI diagnostics

### 2) Boundary-aware order-flow evaluator
A new pure evaluator in `orderflow_engine.py` ties tape/DOM confirmation to the actual compression edge.
It adds:
- boundary watch long / short
- boundary confirm long / short
- boundary acceptance long / short
- failed breakout re-entry detection
- outside-hold counters for price staying outside the box

### 3) Live filter integration
The live breakout filter now requires the move to happen at the correct boundary:
- watch needs pressure at the correct edge
- confirm needs price outside the edge with hold + OF support
- failed re-entry blocks confirmation
- notes now explain boundary reasons, not only tape/DOM reasons

### 4) UI diagnostics
The market-state and OF popups now show the new boundary evaluation:
- boundary state / bias
- boundary high / low
- width ATR
- distance to upper/lower boundary
- touch counts
- outside-hold counters
- failed breakout flags
- boundary note

## Important safety guarantees
- `app/backtest.py` was not modified
- indicator math was not modified
- rule logic was not modified
- this phase only touches:
  - `app/orderflow_engine.py`
  - `app/live_engine.py`
  - `app/ui_app.py`

## Remaining future phases
This phase does **not** yet add:
- replay logging / event history
- historical OF validation tooling
- soft vs hard trigger modes in the broader live filter stack

Those remain for later deliveries.
