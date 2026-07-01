"""
Full MFE/MAE characterization of every #2 regime change on BTCUSDT 5m.

Answers, for (nearly) every regime flip: how much does it give (MFE), how much
does it go against first (MAE), how long to the favorable peak, and how this
splits by CONTEXT (BOS / sweep / structure / volatility / leg age / confluence).

Goal: find contexts where a reliable MFE can be harvested with a HIGH WIN-RATE
target -> stable, low-variance equity (not fat-tail lottery).
"""
from __future__ import annotations
import argparse, os, numpy as np, pandas as pd, sys
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import regime_change_research as base
import structure_edge as se

H = 96  # characterization horizon (8h on 5m)


def excursions(ev, c, h, l):
    k = ev["k"].to_numpy(); d = ev["dir"].to_numpy(); a = ev["atr"].to_numpy()
    n = len(c); mfe = np.full(len(k), np.nan); mae = np.full(len(k), np.nan); tpk = np.full(len(k), np.nan)
    for i in range(len(k)):
        ki = k[i]; e = c[ki]; end = min(ki + H, n - 1)
        hh = h[ki + 1:end + 1]; ll = l[ki + 1:end + 1]
        if len(hh) == 0:
            continue
        if d[i] > 0:
            fav = (hh.max() - e) / a[i]; adv = (e - ll.min()) / a[i]
            tpk[i] = int(np.argmax(hh)) + 1
        else:
            fav = (e - ll.min()) / a[i]; adv = (hh.max() - e) / a[i]
            tpk[i] = int(np.argmin(ll)) + 1
        mfe[i] = fav; mae[i] = adv
    ev = ev.copy(); ev["mfe"] = mfe; ev["mae"] = mae; ev["tpk"] = tpk
    return ev.dropna(subset=["mfe"])


def first_touch_win(ev, c, h, l, tp, sl):
    """win = TP (tp*ATR favorable) reached before SL (sl*ATR adverse), within H."""
    k = ev["k"].to_numpy(); d = ev["dir"].to_numpy(); a = ev["atr"].to_numpy()
    n = len(c); win = np.zeros(len(k), bool); resolved = np.zeros(len(k), bool)
    for i in range(len(k)):
        ki = k[i]; e = c[ki]; end = min(ki + H, n - 1)
        tpr = tp * a[i]; slr = sl * a[i]
        if d[i] > 0:
            th = np.where(h[ki + 1:end + 1] >= e + tpr)[0]; sh = np.where(l[ki + 1:end + 1] <= e - slr)[0]
        else:
            th = np.where(l[ki + 1:end + 1] <= e - tpr)[0]; sh = np.where(h[ki + 1:end + 1] >= e + slr)[0]
        ti = th[0] if len(th) else 10**9; si = sh[0] if len(sh) else 10**9
        if ti < si:
            win[i] = True; resolved[i] = True
        elif si < 10**9:
            resolved[i] = True
    return win, resolved


def main():
    ap = argparse.ArgumentParser(); ap.add_argument("--setup", default="flip")
    args = ap.parse_args()
    d5 = base.load("5m", os.path.join(base.REPO, "market_data"))
    i2 = base.ind_atrfib(d5); i1 = base.ind_frama(d5); i3 = base.ind_vidya(d5)
    d15 = base.load("15m", os.path.join(base.REPO, "market_data")); i15 = base.ind_atrfib(d15)
    t15df = pd.DataFrame({"avail": d15["open_time"] + pd.Timedelta(minutes=15), "t15": i15["t"]}).sort_values("avail")
    t15 = pd.merge_asof(d5[["open_time"]].sort_values("open_time"), t15df,
                        left_on="open_time", right_on="avail", direction="backward")["t15"].fillna(0).to_numpy()
    ev = se.build(d5, i2, i1, i3, t15)
    ev = ev[ev["setup"] == args.setup].reset_index(drop=True)
    c = d5["close"].to_numpy(float); h = d5["high"].to_numpy(float); l = d5["low"].to_numpy(float)
    ev = excursions(ev, c, h, l)
    print(f"\n#2 '{args.setup}' events with MFE/MAE: {len(ev)}   (horizon {H} bars = {H*5//60}h)")

    print("\n=== MFE / MAE distribution (in ATR units) ===")
    for name, col in [("MFE (favorable)", "mfe"), ("MAE (adverse first)", "mae")]:
        q = ev[col].quantile([.25, .5, .75, .9]).to_numpy()
        print(f"  {name:20s}: p25={q[0]:.2f}  median={q[1]:.2f}  p75={q[2]:.2f}  p90={q[3]:.2f}  mean={ev[col].mean():.2f}")
    print(f"  median bars-to-MFE-peak: {ev['tpk'].median():.0f}  ({ev['tpk'].median()*5:.0f} min)")
    print("\n  share of flips reaching MFE >= X*ATR:")
    for x in [0.5, 1.0, 1.5, 2.0, 3.0]:
        print(f"    >= {x:.1f} ATR : {(ev['mfe']>=x).mean()*100:5.1f}%")

    print("\n=== WIN-RATE surface: P(TP before SL)  rows=TP*ATR cols=SL*ATR ===")
    TPs = [0.5, 0.75, 1.0, 1.5, 2.0]; SLs = [0.75, 1.0, 1.5, 2.0]
    print("   TP\\SL |" + "".join(f"{s:>8.2f}" for s in SLs))
    grid = {}
    for tp in TPs:
        cells = []
        for sl in SLs:
            win, res = first_touch_win(ev, c, h, l, tp, sl)
            wr = win[res].mean() * 100 if res.any() else np.nan
            grid[(tp, sl)] = (win, res, wr)
            cells.append(wr)
        print(f"  {tp:>5.2f} |" + "".join(f"{x:7.1f}%" for x in cells))

    # choose a high-hit, RR-aware harvest: TP small, SL a bit wider, then check context + per-year
    tp, sl = 1.0, 1.5
    win, res = grid[(tp, sl)][:2]
    ev2 = ev.copy(); ev2["win"] = win; ev2["res"] = res
    rr = tp / sl
    print(f"\n=== Harvest TP={tp}*ATR / SL={sl}*ATR  (RR {rr:.2f}; breakeven win = {100/(1+rr):.0f}%) ===")
    base_wr = ev2.loc[ev2["res"], "win"].mean() * 100
    print(f"  overall win-rate (resolved): {base_wr:.1f}%   n={ev2['res'].sum()}")
    print("\n  win-rate by CONTEXT (resolved trades):")
    ctx = {
        "BOS": ev2["bos"] == 1, "noBOS": ev2["bos"] == 0,
        "sweep": ev2["sweep"] == 1,
        "structAlign": ev2["struct_aligned"] == 1, "structAgainst": (ev2["struct"] != 0) & (ev2["struct_aligned"] == 0),
        "conf3of3": (ev2["conf1"] + ev2["conf3"] + ev2["conf15"]) == 3,
        "conf0": (ev2["conf1"] + ev2["conf3"] + ev2["conf15"]) == 0,
        "bigDisp>1.5": ev2["disp"] > 1.5, "smallDisp<0.7": ev2["disp"] < 0.7,
        "posLow<0.33": ev2["pos"] < 0.33, "posHigh>0.67": ev2["pos"] > 0.67,
    }
    for name, m in ctx.items():
        g = ev2[m & ev2["res"]]
        if len(g) >= 60:
            print(f"    {name:16s}: win {g['win'].mean()*100:5.1f}%   medMFE {ev2[m]['mfe'].median():.2f}   n={len(g)}")

    print("\n  per-year win-rate (stability check, all flips):")
    for y, g in ev2[ev2["res"]].groupby("year"):
        print(f"    {y}: win {g['win'].mean()*100:5.1f}%   n={len(g)}")


if __name__ == "__main__":
    main()
