# Phase 10 Safe Speed Notes

This phase adds a conservative compiled-signal fast path for common bar-mode rule evaluation.

What changed:
- common non-SEQUENCE, non-Bars-Ago rules can now compile into per-row boolean arrays
- bar-mode backtest can reuse those arrays instead of calling the generic evaluator every decision row
- when every actionable tab on a bar is compilable, the engine skips per-row eval snapshot building unless a trace is actually needed
- unsupported rules automatically fall back to the legacy evaluator

Safety boundaries:
- indicator math unchanged
- indicator streams unchanged
- trade execution and fill logic unchanged
- SEQUENCE groups stay on the old path
- Bars-Ago rules stay on the old path
- traces still use the legacy trace builder
