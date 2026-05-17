# Bundle to Preset Case Map

This note maps the uploaded `RF_COMBINED24` research bundle and the new standalone overlap probe into concrete strategy/preset cases.

## 1. Old stable strategy from the bundle

Source bundle summary:

- [README.md](C:/Users/Lenovo/Downloads/RF_COMBINED24_FULL_RESEARCH_BUNDLE_2026_05_04.zip)
- `Combined24 = Base RF body-pullback + 1m emergency exit + pre-entry 1m directional-efficiency cancel + conditional 16h -> 24h time extension`

### What it really is

This is not a simple entry preset.

It is a multi-layer execution strategy:

1. `5m Range Filter` flip provides the parent signal.
2. Entry is a `5m body pullback limit`, not market.
3. `1m pre-entry cancel` can veto the fill.
4. `1m emergency exit` can close a bad live trade before hard stop.
5. `16h -> 24h extension` changes holding logic for profitable trades.

### Why it matters

This bundle proves the old RF family already had a durable positive edge on long history:

- `Combined24 x1`: `+775.05%`, `PF 1.755`, `DD -18.00%`, `516 trades`

So the bundle is not junk. It is a real old working branch.

### What can be expressed as preset fields now

Only part of it:

- `range_filter_buy / range_filter_sell`
- `stoch_rsi_kd`
- `vidya_state / vidya_trend_up / vidya_trend_down`
- session gating

### What cannot be expressed as a simple preset alone

- body-pullback limit entry geometry
- pre-fill 1m directional-efficiency cancel
- 1m emergency management after fill
- 16h -> 24h conditional extension

Those are execution-layer mechanics, not just entry conditions.

## 2. New overlap case family from the standalone probe

Standalone probe result:

- best current case: `ms_rf_stoch_confirm_overlap_early25`
- `+0.60%`
- `21 trades`
- `PF 4.013`
- `DD 0.05%`

This case is not universal 1m trading. It is a narrow continuation case.

## 3. Case descriptions

### Case A — Overlap bullish continuation

- Market structure already flipped bullish.
- Entry happens early: within `25` bars after the structural flip.
- Only active during `11:00-16:00 UTC`.
- Price makes a pullback toward the active structure line.
- `Range Filter` resets against the trade and passes through `NEUTRAL`.
- Then `Range Filter BUY` returns.
- `Stoch RSI` confirms re-acceleration:
  - `%K > %D`
  - `%K <= 60`
- Entry: next bar open after the confirm.
- Exit: trailing stop.

### Case B — Overlap bearish continuation

- Market structure already flipped bearish.
- Entry happens within `25` bars after the structural flip.
- Only active during `11:00-16:00 UTC`.
- Price pulls back upward toward the active structure line.
- `Range Filter` resets against the trade and passes through `NEUTRAL`.
- Then `Range Filter SELL` returns.
- `Stoch RSI` confirms downside re-acceleration:
  - `%K < %D`
  - `%K >= 40`
- Entry: next bar open after the confirm.
- Exit: trailing stop.

## 4. Preset skeleton we can express now

### Long entry skeleton

These fields are available in the current engine:

- `range_filter_buy` event on `1m`
- `range_filter_state = BUY`
- `stoch_rsi_kd = GREEN`
- optional `stoch_rsi_k <= 60`
- optional `vidya_state != SELL`
- optional session filter `11:00-16:00 UTC`

### Short entry skeleton

- `range_filter_sell` event on `1m`
- `range_filter_state = SELL`
- `stoch_rsi_kd = RED`
- optional `stoch_rsi_k >= 40`
- optional `vidya_state != BUY`
- optional session filter `11:00-16:00 UTC`

## 5. What is still missing for a true preset reproduction

The best overlap case still depends on standalone structural context:

- `bars since ChoCh <= 25`
- pullback toward the active structure line
- reset happened after the structure flip, not at any random time

That means:

- we can describe the case in preset language now,
- but exact reproduction still needs either:
  - structure fields exposed into the runtime,
  - or a dedicated wrapper around the preset engine.

## 6. Practical recommendation

Do not mix the two branches conceptually:

- `Combined24` is an old `5m RF body-pullback` strategy with execution layers.
- `Overlap early25` is a new `1m structure continuation` case family.

Use them as separate research tracks until one of them is intentionally integrated into the main runtime.
