# MS + RF Overlap Preset Cases

These are research preset sketches derived from the standalone `market structure + range filter + stoch RSI` overlap probe. They are not live-engine integrations; they are case descriptions for further preset design.

## Core idea

Do not trade every `ChoCh`. Use market structure only as context, then wait for a pullback reset and only trade re-entry during the active overlap session.

## Case A: Bullish overlap continuation

- Structural bias: latest `ChoCh ↑` is still active.
- Time window: `11:00-16:00 UTC`.
- Freshness: entry must happen within `25` bars after the `ChoCh ↑`.
- Pullback: price must revisit the active structural line / `ATR TS`.
- Reset: `Range Filter` must print a counter reset (`SELL` seen) and then a `NEUTRAL` state before the re-entry.
- Confirm: first fresh `Range Filter BUY`.
- Stoch RSI: `%K > %D` and `%K <= 60`.
- Entry: next bar open after the confirm candle.
- Exit: trailing stop only.

## Case B: Bearish overlap continuation

- Structural bias: latest `ChoCh ↓` is still active.
- Time window: `11:00-16:00 UTC`.
- Freshness: entry must happen within `25` bars after the `ChoCh ↓`.
- Pullback: price must revisit the active structural line / `ATR TS`.
- Reset: `Range Filter` must print a counter reset (`BUY` seen) and then a `NEUTRAL` state before the re-entry.
- Confirm: first fresh `Range Filter SELL`.
- Stoch RSI: `%K < %D` and `%K >= 40`.
- Entry: next bar open after the confirm candle.
- Exit: trailing stop only.

## Exclusions

- Ignore all entries outside `11:00-16:00 UTC`.
- Ignore re-entries later than `25` bars after the last `ChoCh`.
- Ignore setups where `Range Filter` did not reset through counter-state and `NEUTRAL`.
- Ignore setups where Stoch RSI is already stretched beyond the allowed re-entry zone.

## Why this case family matters

The unrestricted version of the same family was close to flat but still negative. Restricting it to the overlap session and early post-`ChoCh` re-entries improved the standalone probe into a positive zone.
