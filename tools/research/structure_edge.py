"""
Market-structure edge search around #2 regime changes (BTCUSDT 5m).

Adds price-action MECHANICS as conditioning features (the stuff that actually
carries information, vs lagging indicator states):
  - swing pivots -> HH/HL/LH/LL structure regime
  - Break of Structure (BOS): the flip takes out the prior swing in trend dir
  - Liquidity sweep / stop-hunt: flip follows a wick beyond the opposite swing
    that then reclaims (failed break -> reversal)
  - position within recent range, displacement vs ATR

Anti-overfit: IN-SAMPLE = 2020-2023, OUT-OF-SAMPLE = 2024-2026. We SELECT on IS
and only keep configs that ALSO hold on OOS (and across years).

  python tools/research/structure_edge.py --fee 0.04
"""
from __future__ import annotations
import argparse, os, numpy as np, pandas as pd
import regime_change_research as base   # reuse io + indicator ports

L = R = 5            # swing pivot fractal (left/right bars)
RANGE_N = 96         # range window for position-in-range (8h)
SWEEP_W = 12         # bars to look back for a liquidity sweep before the flip


def swings(df):
    """Return per-bar last-two confirmed swing highs/lows (no lookahead)."""
    high, low = df["high"], df["low"]
    rmax = high.rolling(L + R + 1).max()
    rmin = low.rolling(L + R + 1).min()
    ph = (high == rmax.shift(-R)).to_numpy()   # pivot high at i, confirmed at i+R
    pl = (low == rmin.shift(-R)).to_numpy()
    h = high.to_numpy(); l = low.to_numpy(); n = len(df)
    conf = {}
    for i in range(n):
        if i - R >= 0:
            if ph[i - R]:
                conf.setdefault(i, []).append(("H", h[i - R]))
            if pl[i - R]:
                conf.setdefault(i, []).append(("L", l[i - R]))
    sh1 = np.full(n, np.nan); sh2 = np.full(n, np.nan)
    sl1 = np.full(n, np.nan); sl2 = np.full(n, np.nan)
    cur_h, cur_l = [], []
    for i in range(n):
        for typ, px in conf.get(i, []):
            if typ == "H":
                cur_h.append(px)
            else:
                cur_l.append(px)
        if len(cur_h) >= 1: sh1[i] = cur_h[-1]
        if len(cur_h) >= 2: sh2[i] = cur_h[-2]
        if len(cur_l) >= 1: sl1[i] = cur_l[-1]
        if len(cur_l) >= 2: sl2[i] = cur_l[-2]
    return sh1, sh2, sl1, sl2


def build(df, i2, i1, i3, t15):
    c = df["close"].to_numpy(float); h = df["high"].to_numpy(float); l = df["low"].to_numpy(float)
    t = i2["t"]; atrv = i2["atr"]; plo = i2["plo"]; phi = i2["phi"]
    n = len(c); year = df["open_time"].dt.year.to_numpy()
    sh1, sh2, sl1, sl2 = swings(df)
    roll_hi = pd.Series(h).rolling(RANGE_N).max().to_numpy()
    roll_lo = pd.Series(l).rolling(RANGE_N).min().to_numpy()
    in_pocket = (t != 0) & (c >= plo) & (c <= phi)

    legs = []; i = 0
    while i < n:
        if t[i] == 0: i += 1; continue
        j = i
        while j + 1 < n and t[j + 1] == t[i]: j += 1
        legs.append((i, j, t[i])); i = j + 1

    rows = []
    for (s, e, d) in legs:
        for setup, k in (("flip", s), ("pocket", _first_pocket(in_pocket, s, e))):
            if k is None or k < 1:
                continue
            # structure regime (HH/HL vs LH/LL), known at k
            bull = (sh1[k] > sh2[k]) and (sl1[k] > sl2[k])
            bear = (sh1[k] < sh2[k]) and (sl1[k] < sl2[k])
            struct = 1 if bull else (-1 if bear else 0)
            # BOS: flip closes beyond the prior swing in trend direction
            bos = (d == 1 and not np.isnan(sh1[k]) and c[k] > sh1[k]) or \
                  (d == -1 and not np.isnan(sl1[k]) and c[k] < sl1[k])
            # liquidity sweep before flip: took out opposite swing then reclaimed
            lo_w = l[max(0, k - SWEEP_W):k + 1].min(); hi_w = h[max(0, k - SWEEP_W):k + 1].max()
            sweep = (d == 1 and not np.isnan(sl1[k]) and lo_w < sl1[k] and c[k] > sl1[k]) or \
                    (d == -1 and not np.isnan(sh1[k]) and hi_w > sh1[k] and c[k] < sh1[k])
            rng = (roll_hi[k] - roll_lo[k])
            pos = (c[k] - roll_lo[k]) / rng if rng > 0 else 0.5
            disp = abs(c[k] - c[max(0, k - 6)]) / atrv[k] if atrv[k] > 0 else 0.0
            rows.append(dict(
                setup=setup, k=int(k), dir=int(d), atr=float(atrv[k]), year=int(year[k]),
                struct=struct, struct_aligned=int(struct == d), bos=int(bool(bos)),
                sweep=int(bool(sweep)), pos=float(pos), disp=float(disp),
                conf15=int(np.sign(t15[k]) == d),
                conf1=int(i1["t"][k] == d), conf3=int(i3["t"][k] == d)))
    return pd.DataFrame(rows)


def _first_pocket(inp, s, e):
    for k in range(s + 1, e + 1):
        if inp[k]:
            return k
    return None


STRUCT_FILTERS = {
    "ALL": lambda ev: np.ones(len(ev), bool),
    "BOS": lambda ev: ev["bos"].to_numpy() == 1,
    "noBOS": lambda ev: ev["bos"].to_numpy() == 0,
    "sweep": lambda ev: ev["sweep"].to_numpy() == 1,
    "noSweep": lambda ev: ev["sweep"].to_numpy() == 0,
    "structAlign": lambda ev: ev["struct_aligned"].to_numpy() == 1,
    "structAgainst": lambda ev: (ev["struct"].to_numpy() != 0) & (ev["struct_aligned"].to_numpy() == 0),
    "BOS+struct": lambda ev: (ev["bos"].to_numpy() == 1) & (ev["struct_aligned"].to_numpy() == 1),
    "sweep+against": lambda ev: (ev["sweep"].to_numpy() == 1) & (ev["struct"].to_numpy() == -ev["dir"].to_numpy()),
    "posLow": lambda ev: ev["pos"].to_numpy() < 0.33,
    "posHigh": lambda ev: ev["pos"].to_numpy() > 0.67,
    "bigDisp": lambda ev: ev["disp"].to_numpy() > 1.5,
    "BOS+bigDisp": lambda ev: (ev["bos"].to_numpy() == 1) & (ev["disp"].to_numpy() > 1.5),
}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--data", default=os.path.join(base.REPO, "market_data"))
    ap.add_argument("--fee", type=float, default=0.04)
    ap.add_argument("--min-is", type=int, default=80)
    args = ap.parse_args()

    print("loading + indicators + swings ...", flush=True)
    d5 = base.load("5m", args.data)
    i2 = base.ind_atrfib(d5); i1 = base.ind_frama(d5); i3 = base.ind_vidya(d5)
    d15 = base.load("15m", args.data); i15 = base.ind_atrfib(d15)
    t15df = pd.DataFrame({"avail": d15["open_time"] + pd.Timedelta(minutes=15), "t15": i15["t"]}).sort_values("avail")
    t15 = pd.merge_asof(d5[["open_time"]].sort_values("open_time"), t15df,
                        left_on="open_time", right_on="avail", direction="backward")["t15"].fillna(0).to_numpy()

    ev = build(d5, i2, i1, i3, t15)
    print(f"events: {len(ev)}  by setup={ev['setup'].value_counts().to_dict()}", flush=True)
    print(f"  BOS rate={ev['bos'].mean():.2f}  sweep rate={ev['sweep'].mean():.2f}  structAlign={ev['struct_aligned'].mean():.2f}")

    c = d5["close"].to_numpy(float); h = d5["high"].to_numpy(float); l = d5["low"].to_numpy(float)
    SIDES = {"cont": +1, "fade": -1}
    TP = [1.0, 2.0, 3.0, 4.0]; SL = [1.0, 1.5, 2.0, 3.0]; TS = [24, 96, 288]
    IS_YEARS = {2020, 2021, 2022, 2023}

    res = []
    for setup in ["flip", "pocket"]:
        evs = ev[ev["setup"] == setup].reset_index(drop=True)
        yr = evs["year"].to_numpy(); is_mask = np.isin(yr, list(IS_YEARS))
        for sname, sv in SIDES.items():
            for tp in TP:
                for sl in SL:
                    for ts in TS:
                        pnl = base.simulate(evs, c, h, l, max(TS), sv, tp, sl, ts, args.fee)
                        for fname, ffn in STRUCT_FILTERS.items():
                            m = ffn(evs)
                            pis = pnl[m & is_mask]; poos = pnl[m & ~is_mask]
                            if len(pis) < args.min_is or len(poos) < 40:
                                continue
                            res.append(dict(
                                setup=setup, side=sname, filter=fname, tp=tp, sl=sl, ts=ts,
                                n_is=len(pis), exp_is=pis.mean(), win_is=(pis > 0).mean() * 100,
                                n_oos=len(poos), exp_oos=poos.mean(), win_oos=(poos > 0).mean() * 100,
                                pf_oos=(poos[poos > 0].sum() / -poos[poos < 0].sum()) if (poos < 0).any() else np.inf))
    R = pd.DataFrame(res)
    out = os.path.join(base.REPO, "tools", "research", "output"); os.makedirs(out, exist_ok=True)
    R.sort_values("exp_is", ascending=False).to_csv(os.path.join(out, "structure_results.csv"), index=False)

    # SURVIVORS: selected on IS (exp>0 after fee), confirmed on OOS (exp>0)
    surv = R[(R["exp_is"] > 0.01) & (R["exp_oos"] > 0.01)].copy()
    surv = surv.sort_values("exp_oos", ascending=False)

    def show(df, title, k=20):
        print(f"\n===== {title} ({len(df)}) =====")
        if df.empty:
            print("  (none)"); return
        cols = ["setup", "side", "filter", "tp", "sl", "ts", "n_is", "exp_is", "n_oos", "exp_oos", "win_oos", "pf_oos"]
        with pd.option_context("display.width", 220):
            print(df[cols].head(k).to_string(index=False, formatters={
                "exp_is": lambda x: f"{x:+.3f}", "exp_oos": lambda x: f"{x:+.3f}",
                "win_oos": lambda x: f"{x:.1f}", "pf_oos": lambda x: f"{x:.2f}"}))

    print(f"\nscored configs: {len(R)}  (selected on 2020-2023, validated on 2024-2026)")
    show(R.sort_values("exp_is", ascending=False), "Best on IN-SAMPLE (may be overfit)")
    show(surv, "SURVIVORS: positive IN-SAMPLE *and* OUT-OF-SAMPLE")


if __name__ == "__main__":
    main()
