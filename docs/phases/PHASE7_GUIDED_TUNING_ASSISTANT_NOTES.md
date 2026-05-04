# Phase 7 — Guided Threshold Tuning Assistant

This phase adds a backtest-driven tuning assistant for the Market State classifier.

## Added
- `build_guided_threshold_tuning_report(...)` in `app/preset_visualizer.py`
- `suggest_market_state_thresholds(...)` heuristic suggestion engine
- `Tuning Assistant…` button on the Market State Board
- `Tuning Assistant…` button inside the State Outcomes window
- assistant window with:
  - copy/save text report
  - suggested threshold profile
  - reasons / warnings / rule-layer hints
  - apply suggested thresholds directly to the current session
  - open suggested values in Threshold Preview / Threshold Lab flow

## Preserved
- existing indicator formulas
- VIDYA / FRAMA / Range Filter behavior
- backtest execution
- preset visualization
- state outcomes report
- threshold lab

## Validation done
- `python -m py_compile run_app.py app/*.py`
- smoke import for guided report builder

## Notes
This assistant is intentionally heuristic, not an optimizer. It suggests *session-level threshold adjustments* based on the currently loaded backtest and does not rewrite indicator formulas.
