# Backtest Certification

This repository separates two different questions:

1. Is the backtest engine technically correct?
2. Does a given strategy or casebook family match a desired research example exactly?

Those are not the same thing.

## Technical certification

The engine is considered technically healthy when the following ladder passes:

```bash
python tools/real_backtest_system_certification_suite.py
```

That ladder currently checks:

1. Independent `Range Filter` oracle parity.
2. Backtest parity on fixed control windows.
3. Trade-by-trade parity on fixed control windows.
4. Prefix-causality invariance.
5. Replay-context safety.
6. Runtime regression.
7. Core backtest contract tests.
8. Live/downloader contract tests.

The most recent output is mirrored to:

`runs/perf/real_backtest_system_certification_suite_latest/`

## Causality

A technically correct backtest must be causal.

That means already-formed entries must not change simply because more candles were appended on the right side of the chart. The dedicated causality check is:

```bash
python tools/real_backtest_causality_suite.py
```

If this suite fails, the engine may be using future information and should not be trusted.

## Strategy semantics

Casebook exactness is a separate layer above the engine.

If technical certification passes but a strategy family still misses a desired research example, that is treated as a strategy-semantics gap unless there is evidence that the certification ladder is insufficient.

Examples:

- A family may need a different pullback classification rule.
- A family may need a different reclaim definition.
- A family may rely on an oracle-style lookahead rule that is not causal and should not be moved into live or main-engine logic.

## Practical rule

Use this decision rule:

- `technical certification PASS` and `strategy exactness FAIL`:
  treat it as a strategy-semantics problem.
- `technical certification FAIL`:
  treat it as an engine problem.

This keeps core engine work and strategy modeling work separate.
