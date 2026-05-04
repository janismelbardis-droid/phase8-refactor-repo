# Phase UI-2 — Rule Builder Lazy Load Notes

This phase reduces GUI startup weight without changing trading math, indicator math,
rule semantics, bars-ago behavior, filters, sequences, replay, or tick testing.

## What changed

- Rule-builder tabs now start as lightweight placeholders.
- Only the initially selected rule tab is built during startup.
- Other rule tabs build on first selection.
- Off-screen tabs keep their rule data model in memory exactly as before; only the
  Tk widgets are deferred until needed.
- Rebuild/status helpers now tolerate tabs that have not yet been materialized.

## Safety / parity intent

- No formulas changed.
- No rule evaluation changed.
- No entry/exit semantics changed.
- No bars-ago / filter / sequence semantics changed.

## Validation

- Added `tests/test_ui_phase2_rule_builder_lazy_load.py`
- Added `scripts/phase_ui2_rule_builder_lazy_load_smoke_check.py`
