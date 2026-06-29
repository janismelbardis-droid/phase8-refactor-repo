"""
Exit design test: the real lever for a trend system.

Same structure entries, but compare EXITS:
  1. fixed   : TP/SL in ATR (what we had — caps winners)
  2. trail2flip : initial ATR stop, otherwise ride until #2 regime flips against us
  3. chandelier : ATR trailing stop from the best price since entry

Let winners run -> fat right tail. Reports per-year ROI / max DD and the
R-multiple distribution (where trend systems actually make money).
"""
from __future__ import annotations
import argparse, os, numpy as np, pandas as pd, sys
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import regime_change_research as base
import structure_edge as se


def sim_fixed(c, h, l, t, k, td, atr, sl, tp, fee, maxbars=2000):
    e = c[k]; slr = sl * atr / e; tpr = tp * atr / e
    end = min(k + maxbars, len(c) - 1)
    if td > 0:
        sp, tpx = e * (1 - slr), e * (1 + tpr)
        sh = np.where(l[k + 1:end + 1] <= sp)[0]; th = np.where(h[k + 1:end + 1] >= tpx)[0]
    else:
        sp, tpx = e * (1 + slr), e * (1 - tpr)
        sh = np.where(h[k + 1:end + 1] >= sp)[0]; th = np.where(l[k + 1:end + 1] <= tpx)[0]
    si = sh[0] if len(sh) else 10**9; ti = th[0] if len(th) else 10**9
    if si <= ti and si < 10**9: g = -slr
    elif ti < si: g = tpr
    else: g = td * (c[end] / e - 1)
    return g * 100 - fee, slr


def sim_trail2flip(c, h, l, t, k, td, atr, sl, fee):
    """Initial ATR stop; otherwise hold until #2 regime flips against position."""
    e = c[k]; slr = sl * atr / e; d = td  # continuation: td == regime dir
    sp = e * (1 - slr) if td > 0 else e * (1 + slr)
    n = len(c)
    for m in range(k + 1, n):
        if td > 0:
            if l[m] <= sp:
                return (sp / e - 1) * 100 - fee, slr
        else:
            if h[m] >= sp:
                return (e / sp - 1) * 100 * -1 * -1 - fee, slr  # short stop
        if t[m] == -d:                      # regime flipped against us -> exit at close
            g = td * (c[m] / e - 1)
            return g * 100 - fee, slr
    g = td * (c[-1] / e - 1)
    return g * 100 - fee, slr


def sim_chandelier(c, h, l, t, k, td, atr, sl, ch, fee):
    """Initial ATR stop, then trail ch*ATR from the best price reached."""
    e = c[k]; slr = sl * atr / e; n = len(c)
    if td > 0:
        peak = h[k]; stop = e * (1 - slr)
        for m in range(k + 1, n):
            if l[m] <= stop:
                return (stop / e - 1) * 100 - fee, slr
            peak = max(peak, h[m]); stop = max(stop, peak - ch * atr)
        return (c[-1] / e - 1) * 100 - fee, slr
    else:
        trough = l[k]; stop = e * (1 + slr)
        for m in range(k + 1, n):
            if h[m] >= stop:
                return (e / stop - 1) * 100 - fee, slr
            trough = min(trough, l[m]); stop = min(stop, trough + ch * atr)
        return (e / c[-1] - 1) * 100 - fee, slr


def equity(trades, risk=0.01, init=10000.0):
    eq = init; out = []
    for net_pct, slr, year, k in trades:
        R = net_pct / (slr * 100)
        eq *= (1 + risk * R)
        out.append((year, R, net_pct, eq, k))
    return pd.DataFrame(out, columns=["year", "R", "net_pct", "eq", "k"])


def max_dd(e):
    p = np.maximum.accumulate(e); return float((1 - e / p).max() * 100)


def run_system(ev, c, h, l, t, setup, filt, sl, exit_kind, fee, **kw):
    evs = ev[ev["setup"] == setup].reset_index(drop=True)
    m = se.STRUCT_FILTERS[filt](evs)
    evs = evs[m].sort_values("k").reset_index(drop=True)
    last_exit = -1; trades = []
    for _, r in evs.iterrows():
        k = int(r["k"])
        if k <= last_exit:
            continue
        td = int(r["dir"]); atr = float(r["atr"])
        if exit_kind == "fixed":
            net, slr = sim_fixed(c, h, l, t, k, td, atr, sl, kw["tp"], fee)
            # approximate exit bar for overlap control
            last_exit = k + 1
        elif exit_kind == "trail2flip":
            net, slr = sim_trail2flip(c, h, l, t, k, td, atr, sl, fee)
            last_exit = k + 1
        else:
            net, slr = sim_chandelier(c, h, l, t, k, td, atr, sl, kw["ch"], fee)
            last_exit = k + 1
        trades.append((net, slr, int(r["year"]), k))
    eqdf = equity(trades, kw.get("risk", 0.01))
    return eqdf


def main():
    ap = argparse.ArgumentParser(); ap.add_argument("--fee", type=float, default=0.04)
    args = ap.parse_args()
    d5 = base.load("5m", os.path.join(base.REPO, "market_data"))
    i2 = base.ind_atrfib(d5); i1 = base.ind_frama(d5); i3 = base.ind_vidya(d5)
    d15 = base.load("15m", os.path.join(base.REPO, "market_data")); i15 = base.ind_atrfib(d15)
    t15df = pd.DataFrame({"avail": d15["open_time"] + pd.Timedelta(minutes=15), "t15": i15["t"]}).sort_values("avail")
    t15 = pd.merge_asof(d5[["open_time"]].sort_values("open_time"), t15df,
                        left_on="open_time", right_on="avail", direction="backward")["t15"].fillna(0).to_numpy()
    ev = se.build(d5, i2, i1, i3, t15)
    c = d5["close"].to_numpy(float); h = d5["high"].to_numpy(float); l = d5["low"].to_numpy(float); t = i2["t"]

    SETUPS = [("sweep-pocket", "pocket", "sweep"), ("noBOS-flip", "flip", "noBOS"), ("all-flip", "flip", "ALL")]
    for name, setup, filt in SETUPS:
        print(f"\n############### {name}  ({setup}/{filt}) ###############")
        for exit_kind, kw in [("fixed", {"tp": 3.0}), ("trail2flip", {}), ("chandelier", {"ch": 4.0})]:
            eqdf = run_system(ev, c, h, l, t, setup, filt, 2.0, exit_kind, args.fee, **kw)
            if eqdf.empty:
                continue
            roi = (eqdf["eq"].iloc[-1] / 10000 - 1) * 100
            dd = max_dd(np.concatenate([[10000.0], eqdf["eq"].to_numpy()]))
            win = (eqdf["net_pct"] > 0).mean() * 100
            R = eqdf["R"]
            pf = R[R > 0].sum() / -R[R < 0].sum() if (R < 0).any() else np.inf
            big = (R > 3).mean() * 100; maxR = R.max()
            print(f"  {exit_kind:11s}: trades {len(eqdf):4d}  win {win:4.0f}%  ROI {roi:+8.1f}%  "
                  f"maxDD {dd:4.0f}%  PF {pf:4.2f}  avgR {R.mean():+.2f}  >3R {big:3.0f}%  maxR {maxR:4.1f}")


if __name__ == "__main__":
    main()
