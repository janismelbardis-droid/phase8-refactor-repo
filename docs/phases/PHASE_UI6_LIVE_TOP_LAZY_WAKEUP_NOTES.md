# Phase UI6 — Live Top Lazy Wake-up

## Goal
Reduce the GUI's wake-up cost on startup without changing any trading math, indicator math, rule semantics, bars-ago behavior, filter behavior, sequence behavior, replay behavior, or tick-tester mechanics.

## What changed
- Split the Live tab's top area into lazy-built surfaces:
  - breakout alert banner
  - signal / indicator table
- Startup now creates lightweight placeholders first.
- The top surfaces wake up after the main window appears.
- Starting Live explicitly ensures those surfaces are built before the engine starts, so behavior stays intact.
- Breakout banner updates can also force the lazy build safely if a breakout alert arrives before the scheduled wake-up finishes.

## Safety
- No indicator formulas changed.
- No backtest formulas changed.
- No rule logic changed.
- No bars-ago, filter, or sequence semantics changed.
- Tick-based tester remains unchanged.

## Validation
- Added `tests/test_ui_phase6_live_top_lazy_wakeup.py`
- Added `scripts/phase_ui6_live_top_lazy_wakeup_smoke_check.py`
- Full suite passed after the change.
