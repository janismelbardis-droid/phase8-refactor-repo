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

## Stage 3 — market structure (the real lift)

Lagging-indicator filters were weak. Adding price-action **mechanics** —
swing pivots (HH/HL/LH/LL), Break of Structure, liquidity sweeps, position in
range — produced edges that survive **out-of-sample**.

Method (`tools/research/structure_edge.py`): SELECT configs on **2020-2023**,
keep only those also positive on **2024-2026**. The framework correctly killed
an in-sample star (`posHigh`: IS +0.074 → OOS −0.096), proving it discriminates
signal from overfit.

Two interpretable survivors (net at maker 0.04% round-trip):

| setup  | side | filter | TP×ATR | SL×ATR | time | OOS exp | OOS PF | per-year |
|--------|------|--------|--------|--------|------|---------|--------|----------|
| flip   | cont | **noBOS** | 2.0 | 3.0 | 8h  | +0.093% | 1.58 | **+ in 6/7 yrs** |
| flip   | cont | noBOS  | 4.0 | 3.0 | 24h | +0.201% | 1.84 | + in 5/7 yrs |
| pocket | cont | **sweep** | 3.0 | 3.0 | 8h  | +0.124% | 1.65 | + in 6/7 yrs |

- **noBOS-flip**: enter with the trend on a regime change that has **not yet**
  broken the prior swing (still has room to structure) — don't chase the
  exhausted break.
- **sweep-pocket**: a pocket pullback that follows a **liquidity sweep**
  (stop-hunt) continues the trend.

Caveats: per-trade edge is modest (~+0.03…+0.10%), per-year samples small
(n≈13–47), maker fills / slippage not modelled, BTC-only. A real but modest
foundation — next: combine the two signals, confirm with order-flow (tick), and
widen the sample to other symbols.

## Stage 4 — MFE/MAE profile and the timeframe/cost insight

Full characterization of every regime change (`tools/research/mfe_profile.py`):
- Nearly every flip produces a move: **MFE median ≈ 4.9 ATR**, 88% reach ≥1 ATR,
  77% reach ≥2 ATR; median time-to-peak ≈ 32 bars. But **MAE ≈ MFE** (you suffer
  almost as much against you first), and **no bar-close context** (BOS/sweep/
  structure/confluence/position) separates the win-rate — all cluster 53–58%.
- The win-rate is, however, **remarkably stable year to year** (~57%).

Why 5m doesn't pay: ATR on 5m ≈ 0.19%, so a 0.04% fee ≈ **0.2 ATR per round
trip** — a fifth of the move you are harvesting. The stable edge is real but
smaller than the cost at this timeframe.

**The stable, fee-surviving configuration is on 1h** (`mfe_profile` logic at 1h;
ATR ≈ 0.77%, fee ≈ 0.05 ATR — negligible):

| TF | entry | exit | win | ROI (1% risk) | maxDD | drop-top-10 |
|----|-------|------|-----|---------------|-------|-------------|
| 1h | regime change | fixed **TP1.5 / SL2 ATR** | 61% | +11% | **7%** | still **+3%** |

Per-year win 62/54/63/58/62/70/53% — consistent. Unlike the trail-to-flip trend
variant (+867% but −27% after dropping 10 trades, all in 2020), this fixed
harvest is **smooth and not outlier-dependent** — the stable-equity profile.
Absolute return is modest at 1% risk; it scales by **breadth** (a basket of
perps: `tools/research/portfolio_harvest_1h.py`) and sizing (7% DD leaves room),
not by leverage on one symbol.

## Stage 5 — the validated baseline strategy (BTC 15m flip)

Dropping ATR and the multi-filter machinery, the simplest version is the
strongest. Rule: on **every** #2 regime flip, enter in the flip direction; fixed
**TP +3% / SL -2%**; if neither hits, exit at the next flip (ride the regime);
risk a fixed fraction per trade. No ATR, no trend filter (EMA50/200 did not
separate MFE), no clustering. `tools/research/strategy_15m_flip.py`.

Result (978 trades, 2020-2026, cost 0.08% = full taker, risk 1%/trade):
**ROI +214% (~19%/yr), max drawdown 11%, PF 1.28.**

Why we trust it (autonomous stress tests):
- **Per year:** positive 2020-2024 (+8…+43%), flat 2025-2026 (+0…+1%, low-vol).
- **Outlier-robust:** drop the top 20 trades -> still +135%.
- **Parameter plateau, not a fit:** every cell of TP[2-4] x SL[1.5-3] is
  positive (+123…+300%), almost all positive in 6-7/7 years — a broad smooth
  surface, the signature of a real edge.
- **Cost-robust:** +282% @0.04% maker, +214% @0.08% taker, +158% @0.12%
  (taker+slippage); only breaks near 0.30%/trade.

Scaling is by risk %, at the cost of drawdown: 1%->~19%/yr/11%DD,
2%->~40%/yr/22%DD, 3%->~70%/yr/29%DD. Known weakness: fixed % targets sag when
volatility halves (2025-2026); a structure-based (swing-level) target is the
ATR-free way to adapt — open next step.

## Reproduce

```bash
python tools/research/regime_change_research.py --fee 0.08   # realistic taker
python tools/research/regime_change_research.py --fee 0.04   # maker / limit
python tools/research/regime_change_research.py --fee 0.0    # raw signal
python tools/research/structure_edge.py --fee 0.04           # market-structure, IS/OOS
# grids -> tools/research/output/{all_results,structure_results}.csv
```
