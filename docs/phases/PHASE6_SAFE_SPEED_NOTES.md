# Phase 6 Safe Speed Notes

This phase trims wasted full-snapshot work around delayed bar-mode entries and scheduled bar orders.

## What changed

- Pending entry signals now keep a **lazy snapshot reference** (`row_index`) instead of eagerly building a full snapshot pack.
- Scheduled bar opens/closes now also keep a **lazy signal snapshot reference** and only materialize the full snapshot if the order actually fills.
- If a pending signal is later blocked/cancelled, the full snapshot work is skipped entirely.
- Reverse-entry pending signals use the same lazy snapshot path.
- Safety tests were added for:
  - blocked pending entries avoiding full snapshot builds
  - confirmed pending entries still materializing a full entry snapshot when the trade fills

## Why this is safe

- No indicator formulas changed.
- No execution pricing / fill logic changed.
- Trade objects still keep the same full `entry_snapshot` / `exit_snapshot` structure once a fill happens.
- Lazy snapshot references are resolved back through the existing full-snapshot builder before the trade is recorded.

## Expected benefit

Strategies that generate many delayed or filtered entry signals no longer pay the cost of building full multi-timeframe snapshots for signals that never become trades.
