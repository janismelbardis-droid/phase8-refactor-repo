# Tick lazy signal snapshot safe speed notes

This pass keeps the tick-based tester intact while trimming unnecessary work around fired signals that never become trades.

## What changed

- Tick signal preparation now stores a **lazy row reference** for fired signal bars instead of eagerly materializing a full multi-timeframe snapshot.
- The tick engine now materializes the full snapshot **only when it is actually needed**:
  - an entry fills
  - a rule-based exit/reversal fills
  - a non-rule close needs the latest closed signal snapshot
- Bars-ago inspector metadata is now cached once per tab for both bar and tick engines instead of being rebuilt repeatedly.

## What stayed the same

- rule evaluation logic
- indicator math
- bars-ago semantics
- filter behavior
- sequence behavior
- tick fill mechanics
- replay/report shapes

## Validation

- added `tests/test_perf_tick_lazy_signal_snapshots.py`
- existing tick signal prep and replay tests still pass
- full suite passed after the change
