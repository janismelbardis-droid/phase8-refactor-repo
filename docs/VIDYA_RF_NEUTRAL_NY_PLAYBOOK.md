# VIDYA + RF Neutral NY Playbook

## Core Idea

Use only two main engines for the signal:

- `VIDYA` defines regime.
- `Range Filter` defines timing.

Do not enter on the first impulse bar.  
Wait for:

1. active `VIDYA` regime,
2. `Range Filter` reset into `NEUTRAL`,
3. first directional `Range Filter` re-entry,
4. no veto from BigBeluga market structure,
5. entry not trapped at bad previous-day New York levels.

## Hard Veto Rules

- Never long directly into `previous New York day high (PDH)`.
- Never short directly into `previous New York day low (PDL)`.
- If BigBeluga structure is bearish, do not long.
- If BigBeluga structure is bullish, do not short.
- If `VIDYA` does not agree with the side, do not enter.
- If there was no immediate `NEUTRAL` reset before the re-entry, do not enter.

## Long Playbook

### Long Support Reclaim

Use this when:

- `VIDYA state = BUY`
- `Range Filter` was `BUY`, then went `NEUTRAL`, then prints first new `BUY`
- BigBeluga structure is not vetoing the long
- entry is **above previous NY low**
- entry is still **below previous NY midpoint**
- price is still close enough to `VIDYA`

Interpretation:

- the market held support,
- reset momentum without fully breaking regime,
- and resumed higher from the lower half of the previous NY range.

Avoid:

- longs near `PDH`
- longs already extended above previous NY midpoint unless a different breakout playbook is confirmed separately

## Short Playbook

### Short Mid Breakdown

Use this when:

- `VIDYA state = SELL`
- `Range Filter` was `SELL`, then went `NEUTRAL`, then prints first new `SELL`
- BigBeluga structure is not vetoing the short
- entry is **below previous NY midpoint**

This is the strongest recent family in the last `~36h` audit.

### Short Acceptance Below PDL

Stronger subset:

- all the rules above
- plus entry is already **below previous NY low**

Interpretation:

- support is already lost,
- the neutral reset failed,
- continuation resumes under the prior day low.

## Management

Base management template:

- initial stop:
  - behind the reset swing
  - or behind `VIDYA`
  - plus a small ATR buffer
- move to `break-even` after `+1R`
- after that:
  - trail,
  - or exit on opposite `Range Filter` flip,
  - or exit if `VIDYA` loses the side

## What Recent Audit Showed

From:

- [report.md](C:/Users/Lenovo/Desktop/BTRESOURCE/phase8_ui_phase7_backtest_results_lazy_load-main/phase8_refactor_repo/runs/perf/vidya_rf_neutral_reentry_latest/report.md)

Recent results over the latest `~36h`:

- all `VIDYA`-aligned neutral re-entry candidates: `25`
- after BigBeluga veto: `17`
- `short_below_prev_mid_ms_align`:
  - `11` candidates
  - `9` reached at least `1R`
  - average `MFE = 5.882R`
- `short_below_prev_low_ms_align`:
  - `6` candidates
  - `5` reached at least `1R`
  - average `MFE = 5.826R`
- `long_support_reclaim_ms_align`:
  - `1` clean candidate
  - reached `4.245R`

## Practical Conclusion

The current evidence does **not** support “trade every RF re-entry”.

The current evidence **does** support:

- `VIDYA regime`
- `RF neutral reset`
- `BigBeluga veto`
- `NY previous-day level awareness`

The short side is currently much cleaner than the long side.  
The long side needs stricter context and should not be chased near `PDH`.
