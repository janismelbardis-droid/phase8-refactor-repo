# Phase 3 — Backtest structural split notes

This phase is a **structure-only** refactor of the bar-close backtest module.

## Goal

Reduce `app/backtest.py` size and separate responsibilities without changing:

- rule math
- indicator math
- bars-ago behavior
- sequence behavior
- filter behavior
- replay behavior
- tick backtest availability

## What moved

### `app/backtest_planning.py`
Planning and compiled-signal helpers:

- bars-ago profiling helpers
- sequence-aware tab planning helpers
- compiled rule/group/tab signal array helpers

### `app/backtest_frames.py`
Frame/snapshot helpers:

- deterministic dataframe freezing
- previous source-bar mapping
- row snapshot cache
- current/previous snapshot builders
- HTF closed-only stream shaping

## What stayed in `app/backtest.py`

- public engine entrypoints
- config/result/trade dataclasses
- trade lifecycle helpers
- execution logic
- replay/reporting assembly
- tick backtest engine

## Compatibility strategy

`app/backtest.py` keeps the same public import surface by re-exporting the moved helpers.
Existing imports such as `from app.backtest import _compile_tab_signal_array` still work.

## Validation focus

- parity-focused regression tests
- freeze/replay tests
- performance safety tests from prior phases
- dedicated structural split smoke test
