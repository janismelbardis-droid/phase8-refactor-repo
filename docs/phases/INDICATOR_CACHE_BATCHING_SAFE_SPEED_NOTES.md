# Indicator Cache Batching Safe Speed Notes

This phase keeps indicator math, rule semantics, bars-ago behavior, filters, sequences, replay, and tick-based backtesting unchanged.

Changes in this phase:
- batched the precomputed market-state column attach inside `app/indicators_streaming.py`
- switched market-state row walking away from `iterrows()` to tuple-based row decoding without changing snapshot contents
- batched indicator cache save packing into a single frame build
- batched indicator cache load reconstruction into a single frame build

Why this is safe:
- formulas are unchanged
- thresholds are unchanged
- only DataFrame assembly and row iteration mechanics were changed

Validation:
- dedicated cache round-trip test added
- existing market-state batching test still covers no-fragmentation behavior
- full regression suite should stay green before accepting the phase
