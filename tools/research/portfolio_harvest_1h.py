"""
Portfolio of the STABLE harvest: #2 regime-change entry, fixed TP/SL in ATR,
on 1h, across a basket of perps. Goal = smooth equity (high win-rate, low DD,
no outlier dependence), scaled by breadth rather than leverage.

BTCUSDT 1h from market_data/, basket 1h from data_cache/basket/ (gitignored).
"""
from __future__ import annotations
import argparse, glob, os, numpy as np, pandas as pd, sys
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import regime_change_research as base

H = 48  # max bars in trade (1h -> 2 days)


def harvest_trades(df, tp, sl, fee):
    i2 = base.ind_atrfib(df)
    c = df["close"].to_numpy(float); h = df["high"].to_numpy(float); l = df["low"].to_numpy(float)
    t = i2["t"]; atrv = i2["atr"]; ot = df["open_time"].to_numpy(); yr = df["open_time"].dt.year.to_numpy()
    n = len(c)
    legs = []; i = 0
    while i < n:
        if t[i] == 0: i += 1; continue
        j = i
        while j + 1 < n and t[j + 1] == t[i]: j += 1
        legs.append((i, t[i])); i = j + 1
    trades = []; le = -1
    for (k, d) in legs:
        if k <= le or k < 1:
            continue
        e = c[k]; end = min(k + H, n - 1); tpr = tp * atrv[k] / e; slr = sl * atrv[k] / e
        if d > 0:
            th = np.where(h[k + 1:end + 1] >= e * (1 + tpr))[0]; sh = np.where(l[k + 1:end + 1] <= e * (1 - slr))[0]
        else:
            th = np.where(l[k + 1:end + 1] <= e * (1 - tpr))[0]; sh = np.where(h[k + 1:end + 1] >= e * (1 + slr))[0]
        ti = th[0] if len(th) else 10**9; si = sh[0] if len(sh) else 10**9
        if si <= ti and si < 10**9:
            g = -slr; le = k + 1 + si
        elif ti < si:
            g = tpr; le = k + 1 + ti
        else:
            g = d * (c[end] / e - 1); le = end
        net = g * 100 - fee
        trades.append(dict(entry_t=ot[k], R=net / (slr * 100), net=net, year=int(yr[k])))
    return pd.DataFrame(trades)


def load_1h(path_glob):
    fs = sorted(glob.glob(path_glob))
    if not fs:
        return None
    df = pd.concat([pd.read_parquet(f) for f in fs], ignore_index=True)
    df = df.sort_values("open_time").drop_duplicates("open_time").reset_index(drop=True)
    df["open_time"] = pd.to_datetime(df["open_time"], utc=True)
    return df


def max_dd(e):
    p = np.maximum.accumulate(e); return float((1 - e / p).max() * 100)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--tp", type=float, default=1.5); ap.add_argument("--sl", type=float, default=2.0)
    ap.add_argument("--fee", type=float, default=0.04); ap.add_argument("--risk", type=float, default=1.0)
    args = ap.parse_args()
    risk = args.risk / 100.0

    syms = {"BTCUSDT": os.path.join(base.REPO, "market_data/BTCUSDT/LAST/1h/*.parquet")}
    for d in sorted(glob.glob(os.path.join(base.REPO, "data_cache/basket/*/LAST/1h"))):
        syms[d.split("/")[-3]] = os.path.join(d, "*.parquet")

    all_tr = []
    for sym, g in syms.items():
        df = load_1h(g)
        if df is None or len(df) < 3000:
            print(f"  {sym}: skip ({0 if df is None else len(df)} bars)"); continue
        tr = harvest_trades(df, args.tp, args.sl, args.fee); tr["sym"] = sym
        all_tr.append(tr)
        print(f"  {sym}: {len(df)} bars -> {len(tr)} trades  win {(tr['net']>0).mean()*100:.0f}%  sumR {tr['R'].sum():+.0f}")

    T = pd.concat(all_tr, ignore_index=True).sort_values("entry_t").reset_index(drop=True)
    eq = 10000.0; curve = []
    for _, r in T.iterrows():
        eq *= (1 + risk * r["R"]); curve.append(eq)
    T["eq"] = curve
    roi = (eq / 10000 - 1) * 100; dd = max_dd(np.concatenate([[10000.0], np.array(curve)]))
    R = T["R"]; pf = R[R > 0].sum() / -R[R < 0].sum()
    print(f"\n===== PORTFOLIO 1h harvest TP{args.tp}/SL{args.sl}  ({len(syms)} symbols, risk {args.risk}%/trade) =====")
    print(f"  trades {len(T)}  win {(T['net']>0).mean()*100:.0f}%  ROI {roi:+.0f}%  maxDD {dd:.0f}%  PF {pf:.2f}  avgR {R.mean():+.2f}")
    print("\n  ROBUSTNESS — drop top-N winners (stable system barely moves):")
    for N in [0, 5, 10, 20, 40]:
        Rr = R.to_numpy().copy()
        for _ in range(N): Rr[np.argmax(Rr)] = 0
        e = 10000.0
        for x in Rr: e *= (1 + risk * x)
        print(f"    drop top {N:2d}: ROI {(e/10000-1)*100:+8.0f}%")
    print("\n  per-year:")
    s = 10000.0
    for y, g in T.groupby("year"):
        e1 = g["eq"].iloc[-1]
        print(f"    {y}: trades {len(g):4d}  win {(g['net']>0).mean()*100:3.0f}%  ROI {(e1/s-1)*100:+6.0f}%")
        s = e1

    out = os.path.join(base.REPO, "tools", "research", "output"); os.makedirs(out, exist_ok=True)
    T.to_csv(os.path.join(out, "portfolio_1h_trades.csv"), index=False)
    try:
        import matplotlib; matplotlib.use("Agg"); import matplotlib.pyplot as plt
        fig, ax = plt.subplots(figsize=(11, 6))
        ax.plot(pd.to_datetime(T["entry_t"]), T["eq"])
        ax.set_title(f"Portfolio 1h stable harvest TP{args.tp}/SL{args.sl}, {len(syms)} perps, risk {args.risk}%/trade "
                     f"(ROI {roi:+.0f}%, DD {dd:.0f}%)")
        ax.set_ylabel("equity $"); ax.grid(True, alpha=0.3)
        fig.tight_layout(); fig.savefig(os.path.join(out, "portfolio_1h_equity.png"), dpi=110)
        print(f"\n  equity -> {os.path.join(out,'portfolio_1h_equity.png')}")
    except Exception as ex:
        print("plot skipped:", ex)


if __name__ == "__main__":
    main()
