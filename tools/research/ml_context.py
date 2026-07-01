"""
Context-aware model for #2 regime flips (BTC 15m) — the honest "reads context"
robot: rich pre-flip features -> gradient boosting -> expanding WALK-FORWARD
(train on all prior years, test the next; each test year is unseen).

Label (lookahead-safe): entering in the flip direction, does price reach +TP%
before -SL% within `horizon` bars (fixed price levels — no flip-exit trick).
All FEATURES use only data up to and including the flip bar.
"""
from __future__ import annotations
import argparse, os, math, numpy as np, pandas as pd, sys
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import regime_change_research as base
from sklearn.ensemble import HistGradientBoostingClassifier
from sklearn.inspection import permutation_importance

TP, SL, HOR = 2.0, 2.0, 200


def htf_on(dbase, tf, minutes):
    dtf = base.load(tf, os.path.join(base.REPO, "market_data")); itf = base.ind_atrfib(dtf)
    ext = (dtf["close"].to_numpy() - itf["basis"]) / itf["atr"]
    df = pd.DataFrame({"avail": dtf["open_time"] + pd.Timedelta(minutes=minutes),
                       "t": itf["t"], "ext": ext}).sort_values("avail")
    m = pd.merge_asof(dbase[["open_time"]].sort_values("open_time"), df,
                      left_on="open_time", right_on="avail", direction="backward")
    return m["t"].fillna(0).to_numpy(), m["ext"].fillna(0).to_numpy()


def features():
    d = base.load("15m", os.path.join(base.REPO, "market_data")); i2 = base.ind_atrfib(d)
    c = d["close"].to_numpy(float); h = d["high"].to_numpy(float); l = d["low"].to_numpy(float)
    o = d["open"].to_numpy(float); v = d["volume"].to_numpy(float)
    t = i2["t"]; atrv = i2["atr"]; basis = i2["basis"]; n = len(c)
    year = d["open_time"].dt.year.to_numpy()
    t1h, e1h = htf_on(d, "1h", 60); t4h, e4h = htf_on(d, "4h", 240)
    d1 = base.load("1m", os.path.join(base.REPO, "market_data")); c1 = d1["close"].to_numpy(float)
    ret1 = np.diff(np.log(c1), prepend=np.log(c1[0]))
    m1_mom = pd.Series(ret1).rolling(15).sum().to_numpy(); m1_vol = pd.Series(ret1).rolling(15).std().to_numpy()
    idx1 = np.searchsorted(d1["open_time"].values.astype("datetime64[ns]"),
                           d["open_time"].values.astype("datetime64[ns]"), side="right") - 1
    rng = np.maximum(h - l, 1e-9); body = (c - o)/rng
    uw = (h - np.maximum(o, c))/rng; lw = (np.minimum(o, c) - l)/rng
    vz = ((pd.Series(v)-pd.Series(v).rolling(100).mean())/(pd.Series(v).rolling(100).std()+1e-9)).to_numpy()
    atrp = atrv/c

    legs = []; i = 0
    while i < n:
        if t[i] == 0: i += 1; continue
        j = i
        while j+1 < n and t[j+1] == t[i]: j += 1
        legs.append((i, j, t[i])); i = j+1

    rows = []
    for li in range(1, len(legs)):
        s, e, dd = legs[li]; ps, pe, _ = legs[li-1]
        if s < 30 or atrv[s] <= 0: continue
        a = atrv[s]
        f = dict(
            prior_bars=pe-ps+1, prior_move=abs(c[pe]-c[ps])/a,
            extension=dd*(c[s]-basis[s])/a, runup=dd*(c[s]-c[s-6])/a,
            recent_pullback=((c[s]-l[s-12:s].min())/a if dd == 1 else (h[s-12:s].max()-c[s])/a),
            atr_pct=atrp[s]*100, vz=(vz[s] if not math.isnan(vz[s]) else 0.0),
            body=dd*body[s], rejwick=(lw[s] if dd == 1 else uw[s]),
            t1h=dd*np.sign(t1h[s]), t4h=dd*np.sign(t4h[s]), e4h=dd*e4h[s],
            m1_mom=(dd*m1_mom[idx1[s]]/atrp[s] if not math.isnan(m1_mom[idx1[s]]) else 0.0),
            m1_vol=(m1_vol[idx1[s]]/np.nanmedian(m1_vol) if not math.isnan(m1_vol[idx1[s]]) else 1.0),
            hour=d["open_time"].iloc[s].hour,
        )
        # label: first-touch +TP before -SL in flip direction, within HOR
        end = min(s+HOR, n-1); hi = h[s+1:end+1]; lo = l[s+1:end+1]
        if len(hi) == 0: continue
        if dd == 1:
            th = np.where(hi >= c[s]*(1+TP/100))[0]; sh = np.where(lo <= c[s]*(1-SL/100))[0]
        else:
            th = np.where(lo <= c[s]*(1-TP/100))[0]; sh = np.where(hi >= c[s]*(1+SL/100))[0]
        ti = th[0] if len(th) else 10**9; si = sh[0] if len(sh) else 10**9
        win = 1 if ti < si else 0
        f["y"] = win; f["year"] = int(year[s])
        rows.append(f)
    return pd.DataFrame(rows)


def main():
    ap = argparse.ArgumentParser(); ap.add_argument("--cost", type=float, default=0.08)
    ap.add_argument("--thr", type=float, default=0.55); a = ap.parse_args()
    F = features()
    feats = [c for c in F.columns if c not in ("y", "year")]
    base_rate = F["y"].mean()*100
    print(f"flips {len(F)}  features {len(feats)}  base win-rate (+{TP}/-{SL}) {base_rate:.1f}%\n")

    # expanding walk-forward: train on year<Y, test on Y
    oos = []
    for Y in [2022, 2023, 2024, 2025, 2026]:
        tr = F[F.year < Y]; te = F[F.year == Y]
        if len(te) < 20 or tr["y"].nunique() < 2: continue
        m = HistGradientBoostingClassifier(max_depth=3, max_iter=200, learning_rate=0.05,
                                           l2_regularization=1.0, random_state=0)
        m.fit(tr[feats], tr["y"])
        p = m.predict_proba(te[feats])[:, 1]
        g = te.copy(); g["p"] = p; oos.append(g)
    O = pd.concat(oos, ignore_index=True)

    # OOS quality
    from sklearn.metrics import roc_auc_score
    auc = roc_auc_score(O["y"], O["p"])
    print(f"OOS AUC = {auc:.3f}  (0.50 = бесполезно, >0.55 = что-то есть)\n")

    # trading: take flips where model P(win) > threshold; RR=TP/SL, cost per trade
    def pnl(df):
        r = np.where(df["y"] == 1, TP, -SL) - a.cost
        return r/SL   # R-multiple
    print(f"{'год':>5} | {'ВСЕ флипы':^22} | {'ВЫБОР модели p>'+str(a.thr):^24}")
    print(f"{'':>5} | {'n':>5} {'win%':>6} {'ср.R':>7} | {'n':>5} {'win%':>6} {'ср.R':>7}")
    for Y, g in O.groupby("year"):
        sel = g[g["p"] > a.thr]
        ra = pnl(g); rs = pnl(sel) if len(sel) else np.array([0.0])
        print(f"{Y:>5} | {len(g):>5} {g['y'].mean()*100:>5.0f}% {ra.mean():>+6.3f} | "
              f"{len(sel):>5} {sel['y'].mean()*100 if len(sel) else 0:>5.0f}% {rs.mean():>+6.3f}")
    allsel = O[O["p"] > a.thr]
    print(f"\nИТОГО OOS: все {len(O)} флипов win {O['y'].mean()*100:.1f}% avgR {pnl(O).mean():+.3f}  |  "
          f"выбор модели {len(allsel)} флипов win {allsel['y'].mean()*100:.1f}% avgR {pnl(allsel).mean():+.3f}")

    # feature importance (permutation, on pooled OOS)
    mfull = HistGradientBoostingClassifier(max_depth=3, max_iter=200, learning_rate=0.05,
                                           l2_regularization=1.0, random_state=0).fit(F[feats], F["y"])
    imp = permutation_importance(mfull, F[feats], F["y"], n_repeats=5, random_state=0)
    order = np.argsort(imp.importances_mean)[::-1]
    print("\nважность признаков (какой контекст реально что-то даёт):")
    for i in order[:8]:
        print(f"  {feats[i]:>16}: {imp.importances_mean[i]:+.4f}")


if __name__ == "__main__":
    main()
