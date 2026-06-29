# Regime-change research — ATR Fibonacci (#2) on BTCUSDT 5m

What happens on the 5-minute chart **after a #2 regime change**, and is there a
way to make money — where to put stops and takes? This is the result of a full
systematic sweep, not hand-picked cells.

- Data: committed `market_data/BTCUSDT/LAST/{5m,15m}`, 2020-01 → 2026-06 (gap-free).
- Engine: `tools/research/regime_change_research.py` (runs in the cloud OR on the
  laptop against a bigger cache — identical logic).
- Grid: 3 entry setups × 2 sides × 6 TP × 5 SL × 5 time-stops × 16 filters =
  **14,400 combinations**, ~13,200 scored with ≥150 trades.
- Scoring: net expectancy **after fees**, win-rate, profit factor, and a
  robustness check = fraction of years the config is net positive.

## Headline

1. **The naive ideas do NOT work.**
   - Buying the pocket pullback blindly: 73% of pockets see the #2 trend flip → negative.
   - Fading the pocket (trading the 73% "break"): also **negative** after fees.
     The 73% flip rate is a whippy *state* change, not a profitable move
     (MFE ≈ MAE ≈ 0.8% → efficient).

2. **There IS a small, year-consistent edge — on the CONTINUATION side.**
   The top of the zero-fee ranking is a coherent cluster (not scattered noise):
   **enter WITH the trend on a pocket / fib-0.5 pullback, in a high-volatility /
   rejection-wick / volume-spike context, wide ATR targets, long holds.**

   | setup  | side | filter  | TP×ATR | SL×ATR | time | n | exp/trade | PF | yrs+ |
   |--------|------|---------|--------|--------|------|------|-----------|------|------|
   | pocket | cont | volHI   | 4.0 | 3.0 | 24h | 1295 | +0.080% | 1.15 | 71% |
   | pocket | cont | rejWick | 4.0 | 3.0 | 24h | 494  | +0.073% | 1.22 | **100%** |
   | fib50  | cont | volSpike| 4.0 | 3.0 | 24h | 1716 | +0.065% | 1.19 | 71% |

   (expectancy shown at **zero** fee, to isolate the raw signal)

3. **The edge is thinner than the fee.** Raw ≈ +0.06–0.08%/trade, PF ≈ 1.15–1.22.
   - Taker round-trip ≈ 0.08% → net **≈ 0** (no robust winners).
   - Maker round-trip ≈ 0.04% (limit entries) → best survives at **+0.030%/trade,
     PF 1.06, positive in 71% of years**. Marginal — slippage / missed limit
     fills threaten it.

## What this means for actually making money

The signal carries real, year-robust directional information; it dies on
execution cost. Two concrete levers (neither is blind tinkering):

- **Maker execution.** A pullback-continuation entry naturally fits a resting
  limit order below market, so maker fills are realistic. That alone flips the
  best config to net positive.
- **Order-flow / tick refinement.** The most useful filters (volatility,
  rejection wick, volume spike) are crude bar proxies for order flow. Real
  aggTrades delta (the basis of #3 Volumatic) should sharpen
  continuation-vs-break and lift PF above the taker fee. This needs the tick
  cache — run the same engine on the laptop where that data lives.

## What does NOT help

Across the full grid, these filters did **not** produce a robust fee-surviving
edge on their own: 15m alignment, single-indicator confluence (#1 or #3),
session-of-day. The edge lives in the *volatility/rejection/volume* context plus
the continuation direction, not in higher-timeframe agreement.

## Reproduce

```bash
python tools/research/regime_change_research.py --fee 0.08   # realistic taker
python tools/research/regime_change_research.py --fee 0.04   # maker / limit
python tools/research/regime_change_research.py --fee 0.0    # raw signal
# full ranked grid -> tools/research/output/all_results.csv
```
