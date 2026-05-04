# Phase 3 — Preset Visualization + Explainability

This phase adds a human-readable layer on top of raw rule groups.

## Added

- Preset note / thesis field persisted inside the preset JSON.
- `Visualize…` action for presets.
- Auto-generated **Preset Visualization Report** window with:
  - archetype detection
  - one-line intent summary
  - storyboard / ideal path
  - failure path
  - rule groups rewritten into plain English bullets
  - entry filter summary
- Double-click on preset combo opens the visualization report.
- Preset note preview is shown next to the preset controls.
- Market-state thresholds were moved into a dedicated config structure in `indicators_streaming.py` so later tuning does not require chasing magic numbers through the classifier.

## Intention

The goal is to make presets readable again after time passes.
Instead of remembering why a rule tree existed, the app can now regenerate the strategy idea in words.

## Kept intact

- Core VIDYA calculation
- FRAMA
- Range Filter
- Existing rule engine behavior
- Existing backtest path
- Market State Board from phase 2
