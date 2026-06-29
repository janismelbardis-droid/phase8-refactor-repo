"""
Anatomy of every BTCUSDT #2 regime flip, grouped by CHARACTER, with per-cluster
behaviour AND a per-year money table (trades / win / ROI / max drawdown) for the
best character. Works on any base timeframe (--base): higher/lower TF context is
shifted accordingly. BTC only.

  python tools/research/flip_anatomy.py --base 5m  --fee 0.04
  python tools/research/flip_anatomy.py --base 15m --fee 0.04
"""
from __future__ import annotations
import argparse, os, numpy as np, pandas as pd, sys
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import regime_change_research as base
from sklearn.preprocessing import StandardScaler
from sklearn.cluster import KMeans

CTX = {  # base -> (ltf, [htf...]) as (tf, minutes)
    "5m":  (("1m", 1),  [("15m", 15), ("1h", 60), ("4h", 240)]),
    "15m": (("5m", 5),  [("1h", 60), ("4h", 240)]),
    "1h":  (("15m", 15), [("4h", 240)]),
}
H = 96  # forward horizon in base bars


def htf_on(dbase, tf, minutes):
    dtf = base.load(tf, os.path.join(base.REPO, "market_data"))
    itf = base.ind_atrfib(dtf)
    ext = (dtf["close"].to_numpy() - itf["basis"]) / itf["atr"]
    df = pd.DataFrame({"avail": dtf["open_time"] + pd.Timedelta(minutes=minutes),
                       "t": itf["t"], "ext": ext}).sort_values("avail")
    m = pd.merge_asof(dbase[["open_time"]].sort_values("open_time"), df,
                      left_on="open_time", right_on="avail", direction="backward")
    return m["t"].fillna(0).to_numpy(), m["ext"].fillna(0).to_numpy()


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--base", default="5m", choices=list(CTX))
    ap.add_argument("--fee", type=float, default=0.04)
    ap.add_argument("--k", type=int, default=6)
    args = ap.parse_args()
    (ltf, ltf_min), htfs = CTX[args.base]

    d = base.load(args.base, os.path.join(base.REPO, "market_data"))
    i2 = base.ind_atrfib(d)
    c = d["close"].to_numpy(float); h = d["high"].to_numpy(float); l = d["low"].to_numpy(float)
    o = d["open"].to_numpy(float); v = d["volume"].to_numpy(float)
    t = i2["t"]; atrv = i2["atr"]; basis = i2["basis"]; n = len(c)
    hour = d["open_time"].dt.hour.to_numpy(); year = d["open_time"].dt.year.to_numpy()

    htf_t = {}; htf_e = {}
    for tf, mins in htfs:
        htf_t[tf], htf_e[tf] = htf_on(d, tf, mins)

    dl = base.load(ltf, os.path.join(base.REPO, "market_data"))
    cl_ = dl["close"].to_numpy(float)
    retl = np.diff(np.log(cl_), prepend=np.log(cl_[0]))
    win_ltf = max(3, 15 // ltf_min)
    l_mom = pd.Series(retl).rolling(win_ltf).sum().to_numpy()
    l_vol = pd.Series(retl).rolling(win_ltf).std().to_numpy()
    otl = dl["open_time"].values.astype("datetime64[ns]")
    otb = d["open_time"].values.astype("datetime64[ns]")
    idxl = np.searchsorted(otl, otb, side="right") - 1

    rng = np.maximum(h - l, 1e-9)
    body = (c - o) / rng; uwick = (h - np.maximum(o, c)) / rng; lwick = (np.minimum(o, c) - l) / rng
    vz = ((pd.Series(v) - pd.Series(v).rolling(100).mean()) / (pd.Series(v).rolling(100).std() + 1e-9)).to_numpy()
    atr_pct = atrv / c

    legs = []; i = 0
    while i < n:
        if t[i] == 0: i += 1; continue
        j = i
        while j + 1 < n and t[j + 1] == t[i]: j += 1
        legs.append((i, j, t[i])); i = j + 1

    near_tf = htfs[0][0]  # the nearest higher TF (the "pullback" TF)
    far_tf = htfs[-1][0]
    rows = []
    for li in range(1, len(legs)):
        s, e, dd = legs[li]; ps, pe, _ = legs[li - 1]
        if s < 30 or atrv[s] <= 0:
            continue
        a = atrv[s]
        prior_bars = pe - ps + 1; prior_move = abs(c[pe] - c[ps]) / a
        extension = dd * (c[s] - basis[s]) / a; runup = dd * (c[s] - c[s - 6]) / a
        W = 12
        recent_pullback = (c[s] - l[s - W:s].min()) / a if dd == 1 else (h[s - W:s].max() - c[s]) / a
        j1 = idxl[s]
        mom1 = dd * l_mom[j1] / atr_pct[s] if not np.isnan(l_mom[j1]) else 0.0
        vol1 = l_vol[j1] / np.nanmedian(l_vol) if not np.isnan(l_vol[j1]) else 1.0
        feat = dict(prior_bars=prior_bars, prior_move=prior_move, extension=extension,
                    runup=runup, recent_pullback=recent_pullback, l_mom=mom1, l_vol=vol1,
                    atr_pct=atr_pct[s] * 100, vz=vz[s] if not np.isnan(vz[s]) else 0.0,
                    body=dd * body[s], rejwick=(lwick[s] if dd == 1 else uwick[s]))
        for tf, _ in htfs:
            feat[f"t_{tf}"] = dd * np.sign(htf_t[tf][s])
        feat["htf_conf"] = sum(feat[f"t_{tf}"] for tf, _ in htfs)
        feat["e_far"] = dd * htf_e[far_tf][s]
        # forward MFE/MAE + harvest
        end = min(s + H, n - 1); hh = h[s + 1:end + 1]; ll = l[s + 1:end + 1]
        if len(hh) == 0:
            continue
        if dd == 1:
            mfe = (hh.max() - c[s]) / a; mae = (c[s] - ll.min()) / a
            th = np.where(hh >= c[s] + 1.5 * a)[0]; sh = np.where(ll <= c[s] - 2.0 * a)[0]
        else:
            mfe = (c[s] - ll.min()) / a; mae = (hh.max() - c[s]) / a
            th = np.where(ll <= c[s] - 1.5 * a)[0]; sh = np.where(hh >= c[s] + 2.0 * a)[0]
        ti = th[0] if len(th) else 10**9; si = sh[0] if len(sh) else 10**9
        hwin = 1 if ti < si else (0 if si < 10**9 else np.nan)
        feat.update(year=int(year[s]), dir=int(dd), atr_pct_raw=atr_pct[s] * 100,
                    mfe=mfe, mae=mae, hwin=hwin)
        rows.append(feat)
    F = pd.DataFrame(rows).dropna(subset=["hwin"]).reset_index(drop=True)
    FEATURES = [c for c in F.columns if c not in ("year", "dir", "atr_pct_raw", "mfe", "mae", "hwin")]
    print(f"\nbase {args.base}: flips {len(F)}  | features {len(FEATURES)}  | fee≈{args.fee/ (F['atr_pct_raw'].median()):.2f} ATR")
    X = StandardScaler().fit_transform(F[FEATURES].fillna(0).values)
    F["cl"] = KMeans(n_clusters=args.k, n_init=10, random_state=0).fit_predict(X)
    print(f"baseline harvest-win {F['hwin'].mean()*100:.0f}%  medMFE {F['mfe'].median():.2f}/MAE {F['mae'].median():.2f}\n")

    def equity_table(g, label):
        ap_ = g["atr_pct_raw"].to_numpy(); win = g["hwin"].to_numpy()
        net = np.where(win == 1, 1.5 * ap_, -2.0 * ap_) - args.fee; R = net / (2.0 * ap_)
        eq = 10000.0; cur = []
        for r in R: eq *= (1 + 0.01 * r); cur.append(eq)
        g = g.assign(eq=cur)
        roi = (eq / 10000 - 1) * 100; e = np.concatenate([[10000.0], cur]); dd = (1 - e / np.maximum.accumulate(e)).max() * 100
        pf = R[R > 0].sum() / -R[R < 0].sum() if (R < 0).any() else 9
        print(f"  ==== {label} (fee {args.fee}%, risk 1%) ====")
        print(f"   ВСЕГО: сделок {len(g)}  win {(win==1).mean()*100:.0f}%  ROI {roi:+.0f}%  maxDD {dd:.0f}%  PF {pf:.2f}  avgR {R.mean():+.3f}")
        print(f"   {'год':>4} {'сделок':>7} {'win':>5} {'ROI':>7} {'просадка':>9}")
        s0 = 10000.0
        for y, gg in g.groupby("year"):
            e1 = gg["eq"].iloc[-1]; eqs = np.concatenate([[s0], gg["eq"].to_numpy()]); ddy = (1 - eqs / np.maximum.accumulate(eqs)).max() * 100
            print(f"   {y:>4} {len(gg):>7} {(gg['hwin']==1).mean()*100:>4.0f}% {(e1/s0-1)*100:>+6.0f}% {ddy:>8.1f}%")
            s0 = e1

    prof = []
    for k in range(args.k):
        g = F[F["cl"] == k]
        ap_ = g["atr_pct_raw"].to_numpy(); win = g["hwin"].to_numpy()
        avgR = (np.where(win == 1, 1.5 * ap_, -2.0 * ap_) - args.fee) / (2.0 * ap_)
        z = {f: (g[f].mean() - F[f].mean()) / (F[f].std() + 1e-9) for f in FEATURES}
        top = sorted(z.items(), key=lambda kv: -abs(kv[1]))[:4]
        prof.append(dict(cl=k, n=len(g), win=win.mean() * 100, avgR=avgR.mean(),
                         desc=", ".join(f"{a}{'↑' if b>0 else '↓'}" for a, b in top)))
    P = pd.DataFrame(prof).sort_values("avgR", ascending=False)
    print("===== CHARACTERS sorted by net avgR (after fee) =====")
    for _, r in P.iterrows():
        print(f"  cl{int(r['cl'])} n={int(r['n']):4d}  win {r['win']:.0f}%  netAvgR {r['avgR']:+.3f}  | {r['desc']}")
    print()
    bestcl = int(P.iloc[0]["cl"])
    equity_table(F[F["cl"] == bestcl], f"BEST cluster {bestcl}")
    F.to_csv(os.path.join(base.REPO, "tools", "research", "output", f"flip_anatomy_{args.base}.csv"), index=False)


if __name__ == "__main__":
    main()
