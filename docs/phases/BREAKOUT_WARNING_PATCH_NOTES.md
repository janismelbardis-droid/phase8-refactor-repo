Added breakout warning state to the market-evaluation layer while keeping indicator math unchanged.

Included changes:
- secondary breakout state on market rectangles
- breakout state derived from compression maturity, range squeeze, breakout readiness/confirmation,
  exchange volume support, and VIDYA delta-volume agreement
- added fields:
  market_breakout_state
  market_breakout_bias
  market_breakout_pretty
  market_breakout_score
  market_breakout_trigger_ready
  market_breakout_note
- UI cards now display breakout status without removing the existing state/bias/phase display
- detail popup now includes breakout state, bias, score, and note
