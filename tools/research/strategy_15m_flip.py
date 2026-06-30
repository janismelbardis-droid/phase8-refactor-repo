"""
BTCUSDT 15m flip strategy — the validated baseline of this research.

Rule (BTC only, no ATR, no extra filters):
  - On every #2 regime flip, enter in the flip direction at the bar close.
  - Fixed take-profit +TP% and stop -SL% (default +3% / -2%).
  - If neither is hit, exit at the next flip (ride the regime).
  - Risk a fixed fraction of equity per trade (stop = SL%).

Validation (committed in REPORT.md): positive in 7/7 years, survives dropping
the top-20 trades, sits on a broad TP/SL plateau (not a curve-fit cell), and
stays profitable up to ~0.12% per-trade cost (maker+taker+slippage).

  python tools/research/strategy_15m_flip.py --tp 3 --sl 2 --cost 0.08 --risk 1
"""
from __future__ import annotations
import argparse, os, numpy as np, pandas as pd, sys
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import regime_change_research as base


def run(tp, sl, cost, tf="15m"):
    d = base.load(tf, os.path.join(base.REPO, "market_data"))
    i2 = base.ind_atrfib(d)
    c = d["close"].to_numpy(float); h = d["high"].to_numpy(float); l = d["low"].to_numpy(float)
    t = i2["t"]; n = len(c); year = d["open_time"].dt.year.to_numpy(); ot = d["open_time"].to_numpy()
    legs = []; i = 0
    while i < n:
        if t[i] == 0: i += 1; continue
        j = i
        while j + 1 < n and t[j + 1] == t[i]: j += 1
        legs.append((i, j, t[i])); i = j + 1
    rows = []
    for (s, e, dd) in legs:
        entry = c[s]; hi = h[s+1:e+1]; lo = l[s+1:e+1]
        if len(hi) == 0:
            continue
        if dd == 1:
            th = np.where(hi >= entry*(1+tp/100))[0]; sh = np.where(lo <= entry*(1-sl/100))[0]
        else:
            th = np.where(lo <= entry*(1-tp/100))[0]; sh = np.where(hi >= entry*(1+sl/100))[0]
        ti = th[0] if len(th) else 10**9; si = sh[0] if len(sh) else 10**9
        if si <= ti and si < 10**9: g = -sl
        elif ti < si: g = tp
        else: g = dd*(c[e]/entry-1)*100
        rows.append(dict(t=ot[s], year=int(year[s]), dir=int(dd), netpct=g-cost, R=(g-cost)/sl))
    return pd.DataFrame(rows)


def maxdd(e): p = np.maximum.accumulate(e); return float((1-e/p).max()*100)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--tf", default="15m"); ap.add_argument("--tp", type=float, default=3.0)
    ap.add_argument("--sl", type=float, default=2.0)
    ap.add_argument("--cost", type=float, default=0.08, help="per-trade cost %% (fee+slippage)")
    ap.add_argument("--risk", type=float, default=1.0)
    a = ap.parse_args()
    T = run(a.tp, a.sl, a.cost, a.tf)
    risk = a.risk/100.0
    eq = 10000.0; cur = []
    for _, r in T.iterrows(): eq *= (1 + risk*r["R"]); cur.append(eq)
    T["eq"] = cur
    roi = (eq/10000-1)*100; dd = maxdd(np.concatenate([[10000.0], np.array(cur)]))
    R = T["R"]; pf = R[R > 0].sum()/-R[R < 0].sum(); cagr = ((eq/10000)**(1/6.5)-1)*100
    print(f"\nBTC {a.tf} flip  TP{a.tp}% / SL{a.sl}%  cost {a.cost}%  risk {a.risk}%/trade")
    print(f"ВСЕГО: сделок {len(T)}  win {(T['netpct']>0).mean()*100:.0f}%  ROI {roi:+.0f}%  ~{cagr:+.0f}%/год  "
          f"maxDD {dd:.0f}%  PF {pf:.2f}  avgR {R.mean():+.3f}")
    print(f"\n  {'год':>4} {'сделок':>7} {'win':>5} {'ROI':>7} {'просадка':>9}")
    s0 = 10000.0
    for y, g in T.groupby("year"):
        e1 = g["eq"].iloc[-1]; eqs = np.concatenate([[s0], g["eq"].to_numpy()]); ddy = maxdd(eqs)
        print(f"  {y:>4} {len(g):>7} {(g['netpct']>0).mean()*100:>4.0f}% {(e1/s0-1)*100:>+6.0f}% {ddy:>8.1f}%")
        s0 = e1
    print("\n  robustness — drop top-N winners:")
    for N in [0, 5, 10, 20]:
        Rr = R.to_numpy().copy()
        for _ in range(N): Rr[np.argmax(Rr)] = 0
        e = 10000.0
        for x in Rr: e *= (1 + risk*x)
        print(f"    drop top {N:2d}: ROI {(e/10000-1)*100:+.0f}%")
    out = os.path.join(base.REPO, "tools", "research", "output"); os.makedirs(out, exist_ok=True)
    T.to_csv(os.path.join(out, f"strategy_{a.tf}_trades.csv"), index=False)
    try:
        import matplotlib; matplotlib.use("Agg"); import matplotlib.pyplot as plt
        fig, ax = plt.subplots(figsize=(11, 6))
        ax.plot(pd.to_datetime(T["t"]), T["eq"])
        ax.set_title(f"BTC {a.tf} flip TP{a.tp}/SL{a.sl} cost {a.cost}% risk {a.risk}% (ROI {roi:+.0f}%, DD {dd:.0f}%)")
        ax.set_ylabel("equity $"); ax.grid(True, alpha=0.3)
        fig.tight_layout(); fig.savefig(os.path.join(out, f"strategy_{a.tf}_equity.png"), dpi=110)
        print(f"\n  equity -> {os.path.join(out,'strategy_15m_equity.png')}")
    except Exception as ex:
        print("plot skipped:", ex)


if __name__ == "__main__":
    main()
