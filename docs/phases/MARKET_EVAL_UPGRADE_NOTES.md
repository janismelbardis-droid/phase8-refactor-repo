# Market Evaluation Upgrade Notes

This patch upgrades the five timeframe market-evaluation cards without changing the underlying indicator math.

What stayed intact:
- Range Filter math
- FRAMA / FRAMA Channel math
- VIDYA math
- MACD / PPO / ATR / ADX indicator math
- backtest and rule-engine structure outside the existing prior patches

What changed:
- richer stream data exposed to the market-evaluation layer:
  - OHLCV context per timeframe stream
  - FRAMA numeric channel fields in the multi-timeframe streams
- smarter market-state feature extraction:
  - width vs ATR
  - slope strength and slope acceleration/deceleration
  - containment and candle overlap / rotationality
  - breakout hold vs breakout failure
  - direction agreement / conflict across existing indicators
  - VIDYA delta strength / agreement / divergence
  - relative candle volume context
- upgraded market-state scoring:
  - compression
  - transition
  - trend
  - pullback
  - exhaustion
- the cards now receive:
  - market texture
  - short and long situation summaries
  - breakout quality / containment / dynamic context in the detail panel

Touched files:
- app/indicators_streaming.py
- app/live_engine.py
- app/ui_app.py
