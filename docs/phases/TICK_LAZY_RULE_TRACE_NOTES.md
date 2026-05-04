# Tick Lazy Rule Trace Safe Speed Notes

This phase keeps the tick backtester behavior intact while removing eager trace work from signal preparation.

## What changed

- prepared tick signal prep no longer builds tab rule-trace strings eagerly for every fired signal bar
- fired signal bars now keep the already-built evaluation snapshots plus a frozen rule-eval context copy
- tab traces are materialized lazily only when a signal is actually consumed by:
  - immediate entry
  - pending-entry confirmation
  - rule-based exit
  - reversal
- per-signal trace strings are cached after first materialization so repeated use on the same signal bar stays stable

## What did not change

- rule evaluation logic
- indicator math
- bars-ago semantics
- filter behavior
- sequence behavior
- tick fill mechanics
- trade output shape

## Why this is safe

The consumed trade still receives the same trace payload, but bars that never turn into an action no longer pay to build trace strings upfront.
