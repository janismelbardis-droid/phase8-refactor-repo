"""
Portfolio trend system: noBOS-flip entry + trail-to-regime-flip exit across a
basket of liquid perps (5m). Trend-following needs breadth -- this tests whether
diversification removes the single-symbol 3-trade dependency.

Reads BTCUSDT from market_data/ and the rest from data_cache/basket/ (gitignored).
"""
from __future__ import annotations
import argparse, glob, os, numpy as np, pandas as pd, sys
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import regime_change_research as base
import structure_edge as se
import exit_systems as ex


def lean_events(df, i2):
    """noBOS flip events (continuation) using only #2 trend + swings."""
    c = df["close"].to_numpy(float); t = i2["t"]; atrv = i2["atr"]
    n = len(c); year = df["open_time"].dt.year.to_numpy()
    sh1, sh2, sl1, sl2 = se.swings(df)
    legs = []; i = 0
    while i < n:
        if t[i] == 0: i += 1; continue
        j = i
        while j + 1 < n and t[j + 1] == t[i]: j += 1
        legs.append((i, t[i])); i = j + 1
    rows = []
    for (s, d) in legs:
        if s < 1:
            continue
        bos = (d == 1 and not np.isnan(sh1[s]) and c[s] > sh1[s]) or \
              (d == -1 and not np.isnan(sl1[s]) and c[s] < sl1[s])
        if bos:
            continue                      # keep only noBOS
        rows.append(dict(k=int(s), dir=int(d), atr=float(atrv[s]), year=int(year[s])))
    return pd.DataFrame(rows)


def symbol_trades(df, sl, fee):
    i2 = base.ind_atrfib(df)
    ev = lean_events(df, i2)
    c = df["close"].to_numpy(float); h = df["high"].to_numpy(float); l = df["low"].to_numpy(float); t = i2["t"]
    ot = df["open_time"].to_numpy()
    last_exit = -1; trades = []
    for _, r in ev.iterrows():
        k = int(r["k"])
        if k <= last_exit:
            continue
        td = int(r["dir"])
        net, slr = ex.sim_trail2flip(c, h, l, t, k, td, float(r["atr"]), sl, fee)
        # find exit bar index for time ordering / overlap
        last_exit = k + 1
        trades.append(dict(entry_t=ot[k], R=net / (slr * 100), net=net, year=int(r["year"])))
    return pd.DataFrame(trades)


def max_dd(e):
    p = np.maximum.accumulate(e); return float((1 - e / p).max() * 100)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--fee", type=float, default=0.04)
    ap.add_argument("--risk", type=float, default=0.5, help="%% equity risked per trade")
    ap.add_argument("--sl", type=float, default=2.0)
    args = ap.parse_args()

    files = {}
    btc = sorted(glob.glob(os.path.join(base.REPO, "market_data/BTCUSDT/LAST/5m/*.parquet")))
    if btc: files["BTCUSDT"] = btc
    for d in sorted(glob.glob(os.path.join(base.REPO, "data_cache/basket/*/LAST/5m"))):
        sym = d.split("/")[-3]
        fs = sorted(glob.glob(os.path.join(d, "*.parquet")))
        if fs: files[sym] = fs
    print(f"symbols: {list(files)}")

    all_tr = []
    for sym, fs in files.items():
        df = pd.concat([pd.read_parquet(f) for f in fs], ignore_index=True)
        df = df.sort_values("open_time").drop_duplicates("open_time").reset_index(drop=True)
        df["open_time"] = pd.to_datetime(df["open_time"], utc=True)
        if len(df) < 5000:
            print(f"  {sym}: too little data ({len(df)}), skip"); continue
        tr = symbol_trades(df, args.sl, args.fee)
        tr["sym"] = sym
        all_tr.append(tr)
        print(f"  {sym}: {len(df):>7} bars -> {len(tr):>4} trades  sumR {tr['R'].sum():+.1f}")

    T = pd.concat(all_tr, ignore_index=True).sort_values("entry_t").reset_index(drop=True)
    risk = args.risk / 100.0
    eq = 10000.0; curve = []
    for _, r in T.iterrows():
        eq *= (1 + risk * r["R"]); curve.append(eq)
    T["eq"] = curve
    roi = (eq / 10000 - 1) * 100
    dd = max_dd(np.concatenate([[10000.0], np.array(curve)]))
    R = T["R"]; pf = R[R > 0].sum() / -R[R < 0].sum()

    print(f"\n=========== PORTFOLIO ({len(files)} symbols, risk {args.risk}%/trade) ===========")
    print(f"  trades {len(T)}  win {(T['net']>0).mean()*100:.0f}%  ROI {roi:+.0f}%  maxDD {dd:.0f}%  "
          f"PF {pf:.2f}  sumR {R.sum():+.0f}  avgR {R.mean():+.2f}")
    print("\n  per-year:")
    s = 10000.0
    for y, g in T.groupby("year"):
        e1 = g["eq"].iloc[-1]
        print(f"    {y}: trades {len(g):4d}  win {(g['net']>0).mean()*100:3.0f}%  "
              f"sumR {g['R'].sum():+6.1f}  ROI {(e1/s-1)*100:+7.0f}%")
        s = e1

    print("\n  ROBUSTNESS — drop top-N winners (vs single-symbol where top-3 killed it):")
    for N in [0, 3, 5, 10, 20]:
        Rr = R.to_numpy().copy()
        for _ in range(N):
            Rr[np.argmax(Rr)] = 0
        e = 10000.0
        for x in Rr: e *= (1 + risk * x)
        print(f"    drop top {N:2d}: ROI {(e/10000-1)*100:+8.0f}%")
    topshare = np.sort(R.to_numpy())[::-1][:10].sum() / R.sum() * 100
    print(f"  top-10 trades = {topshare:.0f}% of total R (lower = more robust)")

    out = os.path.join(base.REPO, "tools", "research", "output")
    T.to_csv(os.path.join(out, "portfolio_trades.csv"), index=False)
    try:
        import matplotlib; matplotlib.use("Agg"); import matplotlib.pyplot as plt
        fig, ax = plt.subplots(figsize=(11, 6))
        ax.plot(pd.to_datetime(T["entry_t"]), T["eq"]); ax.set_yscale("log")
        ax.set_title(f"Portfolio trend (noBOS-flip + trail-to-flip), {len(files)} perps, risk {args.risk}%/trade")
        ax.set_ylabel("equity $ (log)"); ax.grid(True, alpha=0.3)
        fig.tight_layout(); fig.savefig(os.path.join(out, "portfolio_equity.png"), dpi=110)
        print(f"\n  equity curve -> {os.path.join(out,'portfolio_equity.png')}")
    except Exception as e:
        print("plot skipped:", e)


if __name__ == "__main__":
    main()
