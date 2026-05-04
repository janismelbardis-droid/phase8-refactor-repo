Warmup / preload reliability patch
=================================

This patch keeps the project behavior intact while making auto-warmup more conservative.

What changed
------------
- `app/utils_time.py` now sizes warmup using the heaviest built-in indicators, not only MACD/PPO.
- The auto warmup now accounts for:
  - Range Filter
  - FRAMA Channel
  - VIDYA
  - MACD / PPO

Design choice
-------------
This patch intentionally prefers reliability over smaller downloads.
It uses one simple global rule based on the selected timeframes and the largest safe bar requirement.

Practical effect
----------------
- Early backtest / history bars should be much less likely to miss signals due to insufficient preload.
- Range Filter, FRAMA, and VIDYA should start from a more settled state.
- This does not replace the deterministic backtest fixes; it complements them.
