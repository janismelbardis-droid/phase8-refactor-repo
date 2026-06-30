"""
BTCUSDT flip strategy — CORRECTED (no lookahead).

Rule (BTC only, no ATR, no extra filters):
  - On every #2 regime flip, enter in the flip direction at the bar close.
  - Fixed take-profit +TP% and stop -SL% (default +3% / -2%).
  - If neither is hit, exit when the NEXT flip is CONFIRMED, i.e. at c[e+1]
    (the bar where the trend actually changes) — NOT c[e]. Exiting at c[e] is a
    one-bar lookahead that dodges the adverse bar which causes every flip.

IMPORTANT: an earlier version exited at c[e] and reported huge returns
(15m +282%, 5m +712%). Those were a LOOKAHEAD ARTIFACT. With the honest exit:
  - 5m  TP3/SL2 @0.08%: ROI -47% (loses).
  - 15m TP3/SL2 @0.08%: ROI +26% (~4%/yr, PF 1.06, 27% DD) — too thin to trade.
i.e. no real edge. See REPORT.md "Correction".

  python tools/research/strategy_15m_flip.py --tf 15m --tp 3 --sl 2 --cost 0.08
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
        # NO LOOKAHEAD: the flip is only confirmed at the close of bar e+1 (where t
        # changes), so we may hold and exit there, not at c[e]. Scan TP/SL through e+1.
        ee = min(e + 1, n - 1)
        entry = c[s]; hi = h[s+1:ee+1]; lo = l[s+1:ee+1]
        if len(hi) == 0:
            continue
        if dd == 1:
            th = np.where(hi >= entry*(1+tp/100))[0]; sh = np.where(lo <= entry*(1-sl/100))[0]
        else:
            th = np.where(lo <= entry*(1-tp/100))[0]; sh = np.where(hi >= entry*(1+sl/100))[0]
        ti = th[0] if len(th) else 10**9; si = sh[0] if len(sh) else 10**9
        if si <= ti and si < 10**9: g = -sl
        elif ti < si: g = tp
        else: g = dd*(c[ee]/entry-1)*100        # exit at the confirmed-flip bar, not c[e]
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
