# Phase Final Hardening Notes

This pass keeps all trading math, indicator math, rule semantics, bars-ago behavior, filters, sequences, replay behavior, and tick tester mechanics unchanged.

Changes in this phase:

- Removed deprecated pandas datetime integer conversion usage from backtest helpers by routing those conversions through a shared utility.
- Added release-manifest generation so each clean repo zip can ship with a root `MANIFEST.txt` listing included project files.
- Added targeted parity tests for the datetime conversion helper and manifest generation.

Why this is safe:

- Datetime conversion parity is tested against the previous integer conversion behavior.
- No rule logic, indicator formulas, order mechanics, or reporting formulas changed.
- The manifest is packaging metadata only; it does not affect runtime behavior.
