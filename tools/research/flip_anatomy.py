"""
Anatomy of every BTCUSDT #2 regime flip (5m), then group by CHARACTER and
profile each group's behaviour. No binary single-filter testing: build a rich
multi-timeframe context per flip, cluster into characters, then see how each
character behaves and what the good ones have in common.

Context per flip (no lookahead):
  WHY / BEFORE : prior-regime length & size, extension from mean, impulse into
                 the flip, how deep price pulled back just before flipping.
  HIGHER TF    : 15m / 1h / 4h #2 trend alignment + 4h extension.
  LOWER TF     : 1m momentum and 1m volatility inside the flip.
  CHARACTER    : volatility regime, volume spike, candle body/wick, session.

Behaviour (label, NOT clustered on): forward MFE/MAE, continuation rate,
fixed-harvest win-rate and its per-year stability.
"""
from __future__ import annotations
import os, numpy as np, pandas as pd, sys
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import regime_change_research as base
from sklearn.preprocessing import StandardScaler
from sklearn.cluster import KMeans

H = 96   # forward horizon (8h)


def htf_trend_on5(d5, tf, minutes):
    dtf = base.load(tf, os.path.join(base.REPO, "market_data"))
    itf = base.ind_atrfib(dtf)
    ext = ((dtf["close"].to_numpy() - itf["basis"]) / itf["atr"])
    df = pd.DataFrame({"avail": dtf["open_time"] + pd.Timedelta(minutes=minutes),
                       "t": itf["t"], "ext": ext}).sort_values("avail")
    m = pd.merge_asof(d5[["open_time"]].sort_values("open_time"), df,
                      left_on="open_time", right_on="avail", direction="backward")
    return m["t"].fillna(0).to_numpy(), m["ext"].fillna(0).to_numpy()


def main():
    d5 = base.load("5m", os.path.join(base.REPO, "market_data"))
    i2 = base.ind_atrfib(d5)
    c = d5["close"].to_numpy(float); h = d5["high"].to_numpy(float); l = d5["low"].to_numpy(float)
    o = d5["open"].to_numpy(float); v = d5["volume"].to_numpy(float)
    t = i2["t"]; atrv = i2["atr"]; basis = i2["basis"]; n = len(c)
    hour = d5["open_time"].dt.hour.to_numpy(); year = d5["open_time"].dt.year.to_numpy()

    # HTF context
    t15, e15 = htf_trend_on5(d5, "15m", 15)
    t1h, e1h = htf_trend_on5(d5, "1h", 60)
    t4h, e4h = htf_trend_on5(d5, "4h", 240)

    # LTF (1m) precomputed momentum & vol, mapped by time
    d1 = base.load("1m", os.path.join(base.REPO, "market_data"))
    c1 = d1["close"].to_numpy(float)
    ret1 = np.diff(np.log(c1), prepend=np.log(c1[0]))
    m1_mom = pd.Series(ret1).rolling(15).sum().to_numpy()          # 15-min log return
    m1_vol = pd.Series(ret1).rolling(15).std().to_numpy()          # 15-min realized vol
    ot1 = d1["open_time"].values.astype("datetime64[ns]")
    ot5 = d5["open_time"].values.astype("datetime64[ns]")
    idx1 = np.searchsorted(ot1, ot5, side="right") - 1             # last closed 1m bar at/just before 5m close

    rng = np.maximum(h - l, 1e-9)
    body = (c - o) / rng
    uwick = (h - np.maximum(o, c)) / rng
    lwick = (np.minimum(o, c) - l) / rng
    vz = ((pd.Series(v) - pd.Series(v).rolling(100).mean()) / (pd.Series(v).rolling(100).std() + 1e-9)).to_numpy()
    atr_pct = atrv / c

    # legs / flips
    legs = []; i = 0
    while i < n:
        if t[i] == 0: i += 1; continue
        j = i
        while j + 1 < n and t[j + 1] == t[i]: j += 1
        legs.append((i, j, t[i])); i = j + 1

    rows = []
    for li in range(1, len(legs)):
        s, e, d = legs[li]
        ps, pe, pd_ = legs[li - 1]
        if s < 30 or atrv[s] <= 0:
            continue
        a = atrv[s]
        # WHY / BEFORE
        prior_bars = pe - ps + 1
        prior_move = abs(c[pe] - c[ps]) / a
        extension = d * (c[s] - basis[s]) / a
        runup = d * (c[s] - c[s - 6]) / a
        W = 12
        if d == 1:
            recent_pullback = (c[s] - l[s - W:s].min()) / a      # how far above recent low
        else:
            recent_pullback = (h[s - W:s].max() - c[s]) / a
        # LTF
        j1 = idx1[s]
        mom1 = d * m1_mom[j1] / atr_pct[s] if not np.isnan(m1_mom[j1]) else 0.0
        vol1 = (m1_vol[j1] / np.nanmedian(m1_vol)) if not np.isnan(m1_vol[j1]) else 1.0
        # forward behaviour
        end = min(s + H, n - 1)
        hh = h[s + 1:end + 1]; ll = l[s + 1:end + 1]
        if len(hh) == 0:
            continue
        if d == 1:
            mfe = (hh.max() - c[s]) / a; mae = (c[s] - ll.min()) / a
        else:
            mfe = (c[s] - ll.min()) / a; mae = (hh.max() - c[s]) / a
        # continuation = reach +2ATR favorable before -2ATR adverse (path)
        tp = 2.0 * a; sl = 2.0 * a
        if d == 1:
            th = np.where(hh >= c[s] + tp)[0]; sh = np.where(ll <= c[s] - sl)[0]
        else:
            th = np.where(ll <= c[s] - tp)[0]; sh = np.where(hh >= c[s] + sl)[0]
        ti = th[0] if len(th) else 10**9; si = sh[0] if len(sh) else 10**9
        cont = 1 if ti < si else 0
        # harvest TP1.5/SL2 win
        tph = 1.5 * a; slh = 2.0 * a
        if d == 1:
            th2 = np.where(hh >= c[s] + tph)[0]; sh2 = np.where(ll <= c[s] - slh)[0]
        else:
            th2 = np.where(ll <= c[s] - tph)[0]; sh2 = np.where(hh >= c[s] + slh)[0]
        ti2 = th2[0] if len(th2) else 10**9; si2 = sh2[0] if len(sh2) else 10**9
        hwin = 1 if (ti2 < si2) else (0 if si2 < 10**9 else np.nan)

        rows.append(dict(
            year=int(year[s]), dir=int(d),
            prior_bars=prior_bars, prior_move=prior_move, extension=extension,
            runup=runup, recent_pullback=recent_pullback,
            t15=d * t15[s], t1h=d * t1h[s], t4h=d * t4h[s],
            htf_conf=d * (np.sign(t15[s]) + np.sign(t1h[s]) + np.sign(t4h[s])),
            e4h=d * e4h[s], m1_mom=mom1, m1_vol=vol1,
            atr_pct=atr_pct[s] * 100, vz=vz[s] if not np.isnan(vz[s]) else 0.0,
            body=d * body[s], rejwick=(lwick[s] if d == 1 else uwick[s]),
            hour=int(hour[s]),
            mfe=mfe, mae=mae, cont=cont, hwin=hwin))
    F = pd.DataFrame(rows).dropna(subset=["hwin"]).reset_index(drop=True)
    print(f"flips analysed: {len(F)}")

    FEATURES = ["prior_bars", "prior_move", "extension", "runup", "recent_pullback",
                "t15", "t1h", "t4h", "htf_conf", "e4h", "m1_mom", "m1_vol",
                "atr_pct", "vz", "body", "rejwick"]
    X = StandardScaler().fit_transform(F[FEATURES].fillna(0).values)
    K = 6
    F["cl"] = KMeans(n_clusters=K, n_init=10, random_state=0).fit_predict(X)

    base_hwin = F["hwin"].mean() * 100
    base_cont = F["cont"].mean() * 100
    print(f"baseline: harvest-win {base_hwin:.0f}%  continuation {base_cont:.0f}%  medMFE {F['mfe'].median():.2f}  medMAE {F['mae'].median():.2f}\n")

    rep = []
    for cl in range(K):
        g = F[F["cl"] == cl]
        # distinguishing features: standardized mean
        z = {f: (g[f].mean() - F[f].mean()) / (F[f].std() + 1e-9) for f in FEATURES}
        top = sorted(z.items(), key=lambda kv: -abs(kv[1]))[:4]
        desc = ", ".join(f"{k}{'↑' if val>0 else '↓'}" for k, val in top)
        # per-year stability of harvest win
        yr_w = [g[g["year"] == y]["hwin"].mean() * 100 for y in range(2020, 2027) if (g["year"] == y).sum() >= 10]
        yr_consistent = np.std(yr_w) if yr_w else 99
        rep.append(dict(cl=cl, n=len(g), hwin=g["hwin"].mean() * 100, cont=g["cont"].mean() * 100,
                        medMFE=g["mfe"].median(), medMAE=g["mae"].median(),
                        mfe_mae=g["mfe"].median() / max(g["mae"].median(), 0.1),
                        yr_sd=yr_consistent, desc=desc))
    R = pd.DataFrame(rep).sort_values("hwin", ascending=False)
    pd.set_option("display.width", 220)
    print("===== FLIP CHARACTERS (groups), sorted by harvest win-rate =====")
    for _, r in R.iterrows():
        print(f"\n  cluster {int(r['cl'])}  n={int(r['n'])}  | harvest-WIN {r['hwin']:.0f}%  continuation {r['cont']:.0f}%  "
              f"MFE/MAE {r['mfe_mae']:.2f} (med {r['medMFE']:.1f}/{r['medMAE']:.1f})  yr-SD {r['yr_sd']:.0f}")
        print(f"     character: {r['desc']}")

    # readable per-year for the best and worst cluster
    best = int(R.iloc[0]["cl"]); worst = int(R.iloc[-1]["cl"])
    for tag, cl in [("BEST", best), ("WORST", worst)]:
        g = F[F["cl"] == cl]
        pys = " ".join(f"{y}:{g[g['year']==y]['hwin'].mean()*100:.0f}%(n{(g['year']==y).sum()})" for y in range(2020, 2027) if (g['year']==y).sum() >= 5)
        print(f"\n  {tag} cluster {cl} harvest-win by year: {pys}")

    F.to_csv(os.path.join(base.REPO, "tools", "research", "output", "flip_anatomy.csv"), index=False)
    print("\n(per-flip table -> tools/research/output/flip_anatomy.csv)")


if __name__ == "__main__":
    main()
