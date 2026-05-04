Phase 12 — SEQUENCE final-bar gate filters

What this adds
- Rules inside a SEQUENCE group can now be marked as `Final-Bar Gate`.
- Final-Bar Gate rules do not advance sequence progress.
- They are checked only on the bar where the final non-gate sequence step completes.
- If the gate does not pass on that completion bar, the sequence opportunity is dropped and the sequence resets.

UI behavior
- In a SEQUENCE group, right-click any rule chip and choose `Set Final-Bar Gate`.
- The chip label shows `[FINAL GATE]`.
- Sequence status shows `+G<n>` when the group has gate filters.

Safety
- Indicator math untouched.
- Backtest execution math untouched.
- Existing groups without Final-Bar Gate rules behave as before.
- Outside SEQUENCE groups, the flag is ignored.
