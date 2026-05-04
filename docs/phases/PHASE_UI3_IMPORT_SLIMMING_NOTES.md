# Phase UI3 — Import slimming / cold-start reduction

This phase reduces GUI cold-start cost without changing any trading math, indicator math, rule semantics, bars-ago behavior, filters, sequences, replay behavior, or tick-tester mechanics.

## What changed

- `app/ui_app.py`
  - removed eager imports of heavy runtime modules from module import time
  - added lazy runtime wrappers for:
    - `app.backtest`
    - `app.live_engine`
    - `app.ui_plot`
    - `app.ui_dialogs`
    - `app.ui_widgets`
    - `app.preset_visualizer`
    - `app.ui.inspectors`
    - `app.ui.dialogs`
    - `app.research`
    - `app.indicators_streaming`
    - `matplotlib`
  - session market-state thresholds are now initialized lazily instead of during `VisualApp()` construction

## Safety / parity intent

- no formulas were changed
- no thresholds were changed
- no rule evaluation behavior was changed
- no entry / exit mechanics were changed
- no tick path was removed
- only import timing and initialization order were changed

## Validation added

- `tests/test_ui_phase3_import_slimming.py`
- `scripts/phase_ui3_import_slimming_smoke_check.py`

## Expected user-facing effect

- the main app window should appear with less cold-start overhead
- heavy runtime stacks are loaded only when the user actually opens or uses those parts of the app
