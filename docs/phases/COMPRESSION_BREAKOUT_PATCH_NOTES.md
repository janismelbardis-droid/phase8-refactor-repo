Compression and breakout market-evaluation patch

What changed:
- Preserved all original indicator math.
- Upgraded only the market-evaluation layer in app/indicators_streaming.py.
- Compression now uses a longer structural window and explicit squeeze detection.
- Transition is penalized when mature compression is present without real breakout confirmation.
- Added compression maturity, swing squeeze, range contraction, breakout readiness, and breakout confirmation features.
- Pressure compression now requires directional delta support plus boundary pressure inside compression.

New derived fields:
- market_range_contraction
- market_swing_squeeze
- market_compression_maturity
- market_breakout_readiness
- market_breakout_confirmation

Goal:
- obvious squeezing / coiling structures should resolve to COMPRESSION more often
- TRANSITION should be reserved for real reorganisation or release
- breakout context should depend on price escape + volume + delta support
