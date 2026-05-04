# Phase UI-5 — Live workspace lazy load

This phase keeps trading logic, indicator math, rule semantics, bars-ago behavior, filters,
sequences, replay behavior, and tick-based tester mechanics unchanged.

## What changed

- The lower Live workspace notebook now starts with lightweight placeholders.
- The `RULE BUILDER` tab is no longer fully built during initial `LIVE` tab construction.
  It wakes up after the main window appears.
- The `LOG` and `BREAKOUT ALERTS` tabs now build only on first access.
- Live log lines are buffered before the Log widget exists, then replayed into the widget
  when it is built.
- Recorded breakout alerts are rehydrated into the alert-history text widget when that tab
  is built later.

## Why this is safe

- No indicator formulas changed.
- No backtest formulas changed.
- No rule evaluation semantics changed.
- Only Tk widget construction timing changed.

## Validation

- Added `tests/test_ui_phase5_live_workspace_lazy_load.py`
- Added `scripts/phase_ui5_live_workspace_lazy_load_smoke_check.py`
