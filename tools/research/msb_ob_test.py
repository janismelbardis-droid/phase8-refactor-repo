"""
Lookahead-safe backtest of the MSB-OB indicator (EmreKb) on BTCUSDT.

Faithful, real-time port:
  - ZigZag swings are confirmed ONLY when the trend changes (no repaint / no peek).
  - MSB (market structure break) is registered on the bar the condition is met.
  - The Order Block zone is built from a PAST candle; a trade is taken only when
    price RETURNS into the zone on a LATER bar. Every price comparison uses bars
    at or before the decision bar — no future data.

Two questions:
  A) MSB as a signal: forward move after the break (in the break direction).
  B) Do the OB boxes "work": when price returns to the OB, does it bounce
     (reach a target) more often than it breaks (close through the box)?
"""
from __future__ import annotations
import argparse, os, numpy as np, pandas as pd, sys
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import regime_change_research as base

ZZ = 9
FIB = 0.33


def build(df):
    h = df["high"].to_numpy(float); l = df["low"].to_numpy(float)
    o = df["open"].to_numpy(float); c = df["close"].to_numpy(float)
    n = len(df)
    rmax = pd.Series(h).rolling(ZZ).max().to_numpy()
    rmin = pd.Series(l).rolling(ZZ).min().to_numpy()
    to_up = h >= rmax        # current high is a ZZ-bar high (known at bar i)
    to_dn = l <= rmin

    trend = 1
    pmin = l[0]; pmin_i = 0; pmax = h[0]; pmax_i = 0
    lows = []; highs = []          # confirmed swings (price, idx), appended at flips
    market = 1
    msb = []                       # (bar_idx, dir, ob_lo, ob_hi)
    start = ZZ
    for i in range(start, n):
        if l[i] < pmin: pmin = l[i]; pmin_i = i
        if h[i] > pmax: pmax = h[i]; pmax_i = i
        nt = -1 if (trend == 1 and to_dn[i]) else (1 if (trend == -1 and to_up[i]) else trend)
        if nt != trend:
            if nt == 1:            # downtrend ended -> confirm swing LOW
                lows.append((pmin, pmin_i)); pmax = h[i]; pmax_i = i
            else:                  # uptrend ended -> confirm swing HIGH
                highs.append((pmax, pmax_i)); pmin = l[i]; pmin_i = i
            trend = nt
            # ---- MSB check, only with CONFIRMED swings known at bar i ----
            if len(lows) >= 2 and len(highs) >= 1:
                (l0, l0i) = lows[-1]; (l1, l1i) = lows[-2]; (h0, h0i) = highs[-1]
                if market == 1 and l0 < l1 and l0 < l1 - abs(h0 - l1) * FIB:
                    market = -1
                    ob_lo, ob_hi = _last_candle(o, c, h, l, h0i, l0i, bullish=False)
                    msb.append((i, -1, ob_lo, ob_hi))
            if len(highs) >= 2 and len(lows) >= 1:
                (h0, h0i) = highs[-1]; (h1, h1i) = highs[-2]; (l0, l0i) = lows[-1]
                if market == -1 and h0 > h1 and h0 > h1 + abs(h1 - l0) * FIB:
                    market = 1
                    ob_lo, ob_hi = _last_candle(o, c, h, l, h1i, l0i, bullish=True)
                    msb.append((i, 1, ob_lo, ob_hi))
    return msb


def _last_candle(o, c, h, l, a, b, bullish):
    """OB = last opposite candle in [min(a,b), max(a,b)]: bearish for bullish MSB."""
    lo, hi = min(a, b), max(a, b)
    pick = lo
    for j in range(lo, hi + 1):
        if bullish and o[j] > c[j]: pick = j      # last bearish candle
        if (not bullish) and o[j] < c[j]: pick = j  # last bullish candle
    return l[pick], h[pick]


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--tf", default="15m"); ap.add_argument("--rr", type=float, default=2.0)
    ap.add_argument("--cost", type=float, default=0.08); ap.add_argument("--horizon", type=int, default=200)
    a = ap.parse_args()
    df = base.load(a.tf, os.path.join(base.REPO, "market_data"))
    h = df["high"].to_numpy(float); l = df["low"].to_numpy(float); c = df["close"].to_numpy(float)
    year = df["open_time"].dt.year.to_numpy(); n = len(df)
    msb = build(df)
    print(f"{a.tf}: MSB events = {len(msb)}  (bull {sum(1 for m in msb if m[1]==1)}, bear {sum(1 for m in msb if m[1]==-1)})")

    # ---------- A) MSB as directional signal: forward move from the break bar ----------
    rowsA = []
    for (m, d, _, _) in msb:
        end = min(m + a.horizon, n - 1); hi = h[m+1:end+1]; lo = l[m+1:end+1]
        if len(hi) == 0: continue
        e = c[m]
        mfe = (hi.max()/e-1)*100 if d == 1 else (1-lo.min()/e)*100
        mae = (1-lo.min()/e)*100 if d == 1 else (hi.max()/e-1)*100
        rowsA.append((int(year[m]), mfe, mae))
    A = pd.DataFrame(rowsA, columns=["year", "mfe", "mae"])
    print(f"\nA) MSB directional (forward {a.horizon} bars, % in break direction):")
    print(f"   med MFE {A['mfe'].median():.2f}%  med MAE {A['mae'].median():.2f}%  MFE>MAE in {(A['mfe']>A['mae']).mean()*100:.0f}% of breaks")

    # ---------- B) OB retest: enter on RETURN to the box, RR target vs box-fail ----------
    rowsB = []
    for (m, d, ob_lo, ob_hi) in msb:
        if ob_hi <= ob_lo: continue
        # find first RETURN into the OB zone after the break (lookahead-safe: r>m)
        r = -1
        for k in range(m+1, min(m + a.horizon, n-1)+1):
            if d == 1 and l[k] <= ob_hi and h[k] >= ob_lo: r = k; break   # price dipped into bull OB
            if d == -1 and h[k] >= ob_lo and l[k] <= ob_hi: r = k; break  # rallied into bear OB
        if r < 0: continue
        if d == 1:
            entry = ob_hi; stop = ob_lo; risk = entry - stop; tp = entry + a.rr*risk
        else:
            entry = ob_lo; stop = ob_hi; risk = stop - entry; tp = entry - a.rr*risk
        if risk <= 0: continue
        out = None
        for k in range(r+1, min(r + a.horizon, n-1)+1):
            if d == 1:
                if c[k] < stop: out = -1; break          # box failed (close beyond)
                if h[k] >= tp: out = +1; break           # bounce hit target
            else:
                if c[k] > stop: out = -1; break
                if l[k] <= tp: out = +1; break
        if out is None: continue
        pnl = (a.rr if out == 1 else -1.0)              # in R, before cost
        cost_R = a.cost / (abs(risk)/entry*100)          # cost as fraction of 1R
        rowsB.append((int(year[m]), out, pnl - cost_R))
    B = pd.DataFrame(rowsB, columns=["year", "out", "R"])
    retest_rate = len(B) / max(len(msb), 1) * 100
    if len(B):
        be = 1/(1+a.rr)*100
        print(f"\nB) OB retest (RR {a.rr}:1, cost {a.cost}%):")
        print(f"   из {len(msb)} MSB цена вернулась в OB и сделка состоялась: {len(B)} ({retest_rate:.0f}%)")
        print(f"   BOUNCE (тейк) {(B['out']==1).mean()*100:.0f}%  vs BREAK (стоп) {(B['out']==-1).mean()*100:.0f}%   (breakeven {be:.0f}%)")
        print(f"   средний результат: {B['R'].mean():+.3f}R/сделку")
        print(f"   {'год':>4} {'сделок':>7} {'bounce%':>8} {'ср.R':>7}")
        for y, g in B.groupby("year"):
            print(f"   {y:>4} {len(g):>7} {(g['out']==1).mean()*100:>7.0f}% {g['R'].mean():>+6.3f}")
    else:
        print("\nB) нет состоявшихся ретестов")


if __name__ == "__main__":
    main()
