"""
Honest strategy BATTERY on BTC 5m: many distinct mechanisms, one rigorous
harness (lookahead-safe signals, ATR-based TP/SL first-touch, fee, per-year).

Every signal is computed from data up to bar i; entry at c[i]; exits scanned on
later bars only. A strategy "works" only if best config is net-positive AND
positive in most years after fees.
"""
from __future__ import annotations
import argparse, os, numpy as np, pandas as pd, sys
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import regime_change_research as base

HOR = 48


def sim(sig, c, h, l, atr, year, tp, sl, cost):
    """sig: array of (i, dir). Return per-trade net R and years."""
    n = len(c); nets = []; yrs = []; last = -1
    for (i, d) in sig:
        if i <= last: continue
        e = c[i]; a = atr[i]
        if a <= 0: continue
        end = min(i + HOR, n - 1); exitp = c[end]; xo = end
        tpx = e + d * tp * a; slx = e - d * sl * a
        for k in range(i + 1, end + 1):
            if d == 1:
                if l[k] <= slx: exitp = slx; xo = k; break
                if h[k] >= tpx: exitp = tpx; xo = k; break
            else:
                if h[k] >= slx: exitp = slx; xo = k; break
                if l[k] <= tpx: exitp = tpx; xo = k; break
        net = d * (exitp / e - 1) * 100 - cost
        risk = sl * a / e * 100
        nets.append(net / risk); yrs.append(int(year[i])); last = xo
    return np.array(nets), np.array(yrs)


def evaluate(name, sig, c, h, l, atr, year, cost):
    best = None
    for tp in [1.0, 2.0, 3.0]:
        for sl in [1.0, 2.0]:
            R, Y = sim(sig, c, h, l, atr, year, tp, sl, cost)
            if len(R) < 300: continue
            pyw = pd.Series(R).groupby(Y).mean()
            pos = (pyw > 0).sum(); ny = pyw.size
            avg = R.mean()
            if best is None or avg > best["avg"]:
                best = dict(tp=tp, sl=sl, n=len(R), avg=avg, win=(R > 0).mean()*100, pos=pos, ny=ny, pyw=pyw)
    if best:
        flag = "  <== ВСЕ ГОДЫ +" if best["pos"] == best["ny"] else ("  <== плюс "+str(best['pos'])+"/"+str(best['ny']) if best['avg']>0 else "")
        print(f"  {name:22s} TP{best['tp']}/SL{best['sl']}  n{best['n']:>6}  win {best['win']:>3.0f}%  avgR {best['avg']:+.3f}{flag}")
        return best["avg"] > 0 and best["pos"] >= best["ny"] - 1
    print(f"  {name:22s} — мало сделок")
    return False


def main():
    ap = argparse.ArgumentParser(); ap.add_argument("--cost", type=float, default=0.08); a = ap.parse_args()
    d = base.load("5m", os.path.join(base.REPO, "market_data"))
    c = d["close"].to_numpy(float); h = d["high"].to_numpy(float); l = d["low"].to_numpy(float)
    o = d["open"].to_numpy(float); v = d["volume"].to_numpy(float); year = d["open_time"].dt.year.to_numpy()
    n = len(c)
    # ATR(100)
    pc = pd.Series(c).shift(1)
    tr = pd.concat([pd.Series(h)-pd.Series(l), (pd.Series(h)-pc).abs(), (pd.Series(l)-pc).abs()], axis=1).max(axis=1)
    atr = tr.ewm(alpha=1/100, adjust=False).mean().to_numpy()
    hi20 = pd.Series(h).rolling(20).max().shift(1).to_numpy(); lo20 = pd.Series(l).rolling(20).min().shift(1).to_numpy()
    sma20 = pd.Series(c).rolling(20).mean().to_numpy(); sd20 = pd.Series(c).rolling(20).std().to_numpy()
    vz = ((pd.Series(v)-pd.Series(v).rolling(100).mean())/(pd.Series(v).rolling(100).std()+1e-9)).to_numpy()
    up = (c > o); dn = (c < o)
    atr_pct = pd.Series(atr/c); atr_lo = (atr_pct < atr_pct.rolling(200).quantile(0.25)).to_numpy()
    print(f"BTC 5m strategy battery, fee {a.cost}%. Ищем: avgR>0 И плюс почти каждый год.\n")

    S = {}
    # 1) Donchian breakout (momentum)
    S["Donchian breakout"] = [(i, 1) for i in range(21, n) if h[i] > hi20[i]] + \
                             [(i, -1) for i in range(21, n) if l[i] < lo20[i]]
    # 2) Donchian reversal (fade the break)
    S["Donchian reversal"] = [(i, -1) for i in range(21, n) if h[i] > hi20[i]] + \
                             [(i, 1) for i in range(21, n) if l[i] < lo20[i]]
    # 3) Bollinger reversion
    S["Bollinger reversion"] = [(i, 1) for i in range(21, n) if c[i] < sma20[i]-2*sd20[i]] + \
                               [(i, -1) for i in range(21, n) if c[i] > sma20[i]+2*sd20[i]]
    # 4) Bollinger breakout
    S["Bollinger breakout"] = [(i, 1) for i in range(21, n) if c[i] > sma20[i]+2*sd20[i]] + \
                              [(i, -1) for i in range(21, n) if c[i] < sma20[i]-2*sd20[i]]
    # 5) 3 candles same dir -> continuation
    S["3-candle momentum"] = [(i, 1) for i in range(3, n) if up[i] and up[i-1] and up[i-2]] + \
                             [(i, -1) for i in range(3, n) if dn[i] and dn[i-1] and dn[i-2]]
    # 6) 3 candles -> fade
    S["3-candle reversal"] = [(i, -1) for i in range(3, n) if up[i] and up[i-1] and up[i-2]] + \
                             [(i, 1) for i in range(3, n) if dn[i] and dn[i-1] and dn[i-2]]
    # 7) volume spike continuation
    S["vol-spike continuation"] = [(i, 1) for i in range(1, n) if vz[i] > 2 and up[i]] + \
                                  [(i, -1) for i in range(1, n) if vz[i] > 2 and dn[i]]
    # 8) volume spike reversal
    S["vol-spike reversal"] = [(i, -1) for i in range(1, n) if vz[i] > 2 and up[i]] + \
                              [(i, 1) for i in range(1, n) if vz[i] > 2 and dn[i]]
    # 9) volatility squeeze -> breakout
    S["squeeze breakout"] = [(i, 1) for i in range(21, n) if atr_lo[i] and h[i] > hi20[i]] + \
                            [(i, -1) for i in range(21, n) if atr_lo[i] and l[i] < lo20[i]]
    # 10) fade N-bar extreme (range reversal)
    S["range reversal(20)"] = [(i, -1) for i in range(21, n) if h[i] >= hi20[i]] + \
                              [(i, 1) for i in range(21, n) if l[i] <= lo20[i]]

    winners = []
    for name, sig in S.items():
        sig = sorted(sig)
        ok = evaluate(name, sig, c, h, l, atr, year, a.cost)
        if ok: winners.append(name)
    print("\n" + ("РОБАСТНЫЕ (плюс + почти каждый год): " + ", ".join(winners) if winners
                  else "Ни одна не прошла порог (avgR>0 и плюс почти каждый год)."))


if __name__ == "__main__":
    main()
