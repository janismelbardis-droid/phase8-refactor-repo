Determinism patch focus
=======================

This patch is intentionally narrow:

- Freeze historical backtest inputs into local, sorted, duplicate-free copies.
- Build current/previous snapshots from the frozen local streams only.
- Prefer previous real source-TF bars for mixed-timeframe event evaluation.
- Add a small Range audit trail into backtest summaries (`range_audit_rows`).
- Make SEQUENCE progression require a later source bar before advancing to the next step.

Changed files
-------------

- `app/backtest.py`
- `app/rules.py`

Important note
--------------

This patch is aimed at removing backtest nondeterminism / nearby-wrong-candle behavior.
It does not claim the strategy is now perfect. The new `range_audit_rows` summary payload is there so exact missing badges can be inspected from a single run without guessing from trades alone.
