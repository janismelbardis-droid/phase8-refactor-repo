"""
Dead-simple EMA50 / EMA200 strategies on BTC. Lookahead-safe, fees, per-year,
vs buy-and-hold. Position decided at bar close, earns the NEXT bar's return.

  V1 golden-cross long-only : long when EMA50>EMA200, flat otherwise
  V2 golden/death stop-&-rev : long when EMA50>EMA200, short when below
  V3 trend + pullback        : uptrend -> buy dip to EMA50 (TP/SL), mirror short
"""
from __future__ import annotations
import argparse, os, numpy as np, pandas as pd, sys
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import regime_change_research as base


def maxdd(e): p = np.maximum.accumulate(e); return float((1 - e/p).max()*100)


def mtm(pos, ret, year, side_cost):
    """mark-to-market equity from a position series (pos[i] earns ret[i+1])."""
    n = len(ret); eq = 1.0; cur = np.empty(n)
    prev = 0.0
    for i in range(n):
        # cost when position changes (charged at bar i on the new position)
        eq *= (1 - side_cost/100 * abs(pos[i]-prev))
        eq *= (1 + pos[i]*ret[i])       # pos[i] already shifted (decided at i-1)
        cur[i] = eq; prev = pos[i]
    return cur


def report(name, cur, year, bh=None):
    roi = (cur[-1]-1)*100; dd = maxdd(np.concatenate([[1.0], cur]))
    print(f"\n{name}: ROI {roi:+.0f}%  maxDD {dd:.0f}%")
    s = 1.0
    for y in range(2020, 2027):
        m = (year == y)
        if not m.any(): continue
        e1 = cur[m][-1]; e0 = cur[np.where(m)[0][0]-1] if np.where(m)[0][0] > 0 else 1.0
        b = ""
        if bh is not None:
            bh1 = bh[m][-1]; bh0 = bh[np.where(m)[0][0]-1] if np.where(m)[0][0] > 0 else 1.0
            b = f"   (buy&hold {(bh1/bh0-1)*100:+.0f}%)"
        print(f"   {y}: {(e1/e0-1)*100:+7.0f}%{b}")
        s = e1


def main():
    ap = argparse.ArgumentParser(); ap.add_argument("--tf", default="5m")
    ap.add_argument("--side", type=float, default=0.04, help="fee per side %%")
    a = ap.parse_args()
    d = base.load(a.tf, os.path.join(base.REPO, "market_data"))
    c = d["close"].to_numpy(float); h = d["high"].to_numpy(float); l = d["low"].to_numpy(float)
    year = d["open_time"].dt.year.to_numpy()
    ema50 = pd.Series(c).ewm(span=50, adjust=False).mean().to_numpy()
    ema200 = pd.Series(c).ewm(span=200, adjust=False).mean().to_numpy()
    ret = np.zeros(len(c)); ret[1:] = c[1:]/c[:-1] - 1
    bull = ema50 > ema200

    # position decided at bar i, applied to ret[i+1] -> shift signal by 1
    sig_long = np.where(bull, 1.0, 0.0)
    sig_ls = np.where(bull, 1.0, -1.0)
    pos_long = np.concatenate([[0.0], sig_long[:-1]])   # shift: no lookahead
    pos_ls = np.concatenate([[0.0], sig_ls[:-1]])

    bh = np.cumprod(1+ret)
    print(f"BTC {a.tf}, EMA50/EMA200, fee {a.side}%/side. (buy&hold для сравнения)")
    report("V1 golden-cross LONG-ONLY", mtm(pos_long, ret, year, a.side), year, bh)
    report("V2 golden/death LONG-SHORT", mtm(pos_ls, ret, year, a.side), year, bh)
    print(f"\nBUY & HOLD целиком: ROI {(bh[-1]-1)*100:+.0f}%  maxDD {maxdd(np.concatenate([[1.0],bh])):.0f}%")

    # V3 trend + pullback to EMA50, TP/SL in ATR
    pc = pd.Series(c).shift(1)
    tr = pd.concat([pd.Series(h)-pd.Series(l),(pd.Series(h)-pc).abs(),(pd.Series(l)-pc).abs()],axis=1).max(axis=1)
    atr = tr.ewm(alpha=1/100, adjust=False).mean().to_numpy()
    n=len(c); trades=[]; last=-1
    for i in range(200,n):
        if i<=last: continue
        # uptrend + price dipped to/below EMA50 this bar (touched from above)
        if bull[i] and l[i]<=ema50[i] and c[i-1]>ema50[i-1]:
            d_=1
        elif (not bull[i]) and h[i]>=ema50[i] and c[i-1]<ema50[i-1]:
            d_=-1
        else: continue
        e=c[i]; A=atr[i]; end=min(i+96,n-1); tp=e+d_*2*A; sl=e-d_*1.5*A; exitp=c[end]; xo=end
        for k in range(i+1,end+1):
            if d_==1:
                if l[k]<=sl: exitp=sl;xo=k;break
                if h[k]>=tp: exitp=tp;xo=k;break
            else:
                if h[k]>=sl: exitp=sl;xo=k;break
                if l[k]<=tp: exitp=tp;xo=k;break
        net=d_*(exitp/e-1)*100 - 2*a.side
        trades.append((int(year[i]), net/(1.5*A/e*100))); last=xo
    T=pd.DataFrame(trades,columns=['year','R'])
    eq=1.0;cur=[]
    for r in T['R']: eq*=(1+0.01*r);cur.append(eq)
    roi=(eq-1)*100
    print(f"\nV3 trend+pullback to EMA50 (TP2/SL1.5 ATR, risk1%): ROI {roi:+.0f}%  сделок {len(T)}  win {(T['R']>0).mean()*100:.0f}%")
    s=1.0
    for y,g in T.groupby('year'):
        gg=1.0
        for r in g['R']: gg*=(1+0.01*r)
        print(f"   {y}: {(gg-1)*100:+6.0f}%  (n{len(g)})")


if __name__ == "__main__":
    main()
