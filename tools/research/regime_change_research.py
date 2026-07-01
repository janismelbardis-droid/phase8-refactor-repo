"""
Regime-change research engine (BTCUSDT, 5m).

Centered on ATR Fibonacci Trend Envelopes (#2) regime changes. Systematically
sweeps tens of thousands of (entry setup x side x filters x TP/SL/time-stop)
combinations, scores net expectancy AFTER FEES, and guards against
over-fitting by requiring per-year consistency.

Runs anywhere the committed 5m/15m candles exist (cloud OR your laptop). On the
laptop you can point --data at a bigger cache; the logic is identical.

Indicators ported from the three BigBeluga Pine scripts:
  #1 FRAMA Channel        (confluence filter)
  #2 ATR Fibonacci        (regime / primary)
  #3 Volumatic VIDYA      (confluence filter)

Usage:
  python tools/research/regime_change_research.py --fee 0.08 --min-trades 150
"""
from __future__ import annotations
import argparse, glob, os, sys, math
import numpy as np
import pandas as pd

# ----------------------------------------------------------------------------- params
REPO = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# #2 ATR Fibonacci
F2_MA, F2_ATR, F2_MULT = 100, 100, 3.0
G1, G2 = 0.618, 0.786
# #1 FRAMA
F1_N, F1_DIST, F1_VOL = 26, 1.5, 200
# #3 VIDYA
F3_LEN, F3_MOM, F3_BANDDIST, F3_ATR = 10, 20, 2.0, 200


# ----------------------------------------------------------------------------- io / ta
def load(tf, data_root):
    fs = sorted(glob.glob(f"{data_root}/BTCUSDT/LAST/{tf}/*.parquet"))
    if not fs:
        sys.exit(f"no {tf} parquet under {data_root}")
    df = pd.concat([pd.read_parquet(f) for f in fs], ignore_index=True)
    df = df.sort_values("open_time").drop_duplicates("open_time").reset_index(drop=True)
    df["open_time"] = pd.to_datetime(df["open_time"], utc=True)
    return df


def rma(s, n):
    return s.ewm(alpha=1.0 / n, adjust=False).mean()


def atr(df, n):
    pc = df["close"].shift(1)
    tr = pd.concat([df["high"] - df["low"], (df["high"] - pc).abs(), (df["low"] - pc).abs()], axis=1).max(axis=1)
    return rma(tr, n)


def sticky_trend(close, upper, lower):
    """t=+1 after close crosses up the upper band, -1 after crossing down lower; sticky."""
    n = len(close); t = np.zeros(n, np.int8); cur = 0
    for i in range(1, n):
        if close[i] > upper[i] and close[i - 1] <= upper[i - 1]:
            cur = 1
        elif close[i] < lower[i] and close[i - 1] >= lower[i - 1]:
            cur = -1
        t[i] = cur
    return t


# ----- #2 ATR Fibonacci -----
def ind_atrfib(df):
    basis = rma(df["close"], F2_MA).to_numpy()
    a = atr(df, F2_ATR).to_numpy()
    up = basis + a * F2_MULT; lo = basis - a * F2_MULT
    c = df["close"].to_numpy()
    t = sticky_trend(c, up, lo)
    rng = up - lo
    f50 = np.where(t == 1, lo + rng * 0.5, up - rng * 0.5)
    f618 = np.where(t == 1, lo + rng * (1 - G1), up - rng * (1 - G1))
    f786 = np.where(t == 1, lo + rng * (1 - G2), up - rng * (1 - G2))
    plo, phi = np.minimum(f618, f786), np.maximum(f618, f786)
    return dict(t=t, atr=a, upper=up, lower=lo, f50=f50, plo=plo, phi=phi, basis=basis)


# ----- #1 FRAMA -----
def ind_frama(df):
    high, low = df["high"], df["low"]
    price = ((high + low) / 2).to_numpy()
    vol = (high - low).rolling(F1_VOL).mean().to_numpy()
    h = high.to_numpy(); l = low.to_numpy()
    half = F1_N // 2
    hh1 = high.rolling(half).max().to_numpy(); ll1 = low.rolling(half).min().to_numpy()
    hh2 = high.shift(half).rolling(half).max().to_numpy(); ll2 = low.shift(half).rolling(half).min().to_numpy()
    hh3 = high.rolling(F1_N).max().to_numpy(); ll3 = low.rolling(F1_N).min().to_numpy()
    N1 = (hh1 - ll1) / half; N2 = (hh2 - ll2) / half; N3 = (hh3 - ll3) / F1_N
    with np.errstate(divide="ignore", invalid="ignore"):
        dim = (np.log(N1 + N2) - np.log(N3)) / math.log(2)
    alpha = np.exp(-4.6 * (dim - 1))
    alpha = np.clip(np.nan_to_num(alpha, nan=1.0), 0.01, 1.0)
    n = len(price); filt = np.full(n, np.nan)
    prev = price[0]
    for i in range(n):
        p = price[i]
        if math.isnan(prev): prev = p
        filt[i] = alpha[i] * p + (1 - alpha[i]) * prev
        prev = filt[i]
    filt = pd.Series(filt).rolling(5).mean().to_numpy()
    up = filt + vol * F1_DIST; lo = filt - vol * F1_DIST
    hlc3 = ((df["high"] + df["low"] + df["close"]) / 3).to_numpy()
    t = sticky_trend(hlc3, up, lo)
    return dict(t=t)


# ----- #3 VIDYA -----
def ind_vidya(df):
    src = df["close"].to_numpy()
    mom = np.diff(src, prepend=src[0])
    pos = pd.Series(np.where(mom >= 0, mom, 0.0)).rolling(F3_MOM).sum().to_numpy()
    neg = pd.Series(np.where(mom >= 0, 0.0, -mom)).rolling(F3_MOM).sum().to_numpy()
    with np.errstate(divide="ignore", invalid="ignore"):
        cmo = np.abs(100 * (pos - neg) / (pos + neg))
    cmo = np.nan_to_num(cmo, nan=0.0)
    alpha = 2.0 / (F3_LEN + 1)
    n = len(src); vid = np.zeros(n); prev = 0.0
    for i in range(n):
        k = alpha * cmo[i] / 100.0
        prev = k * src[i] + (1 - k) * prev
        vid[i] = prev
    vid = pd.Series(vid).rolling(15).mean().to_numpy()
    a = atr(df, F3_ATR).to_numpy()
    up = vid + a * F3_BANDDIST; lo = vid - a * F3_BANDDIST
    t = sticky_trend(src, up, lo)
    return dict(t=t)


# ----------------------------------------------------------------------------- events
def build_events(d5, i2, i1, i3, t15_on5):
    """Return DataFrame of candidate entries with per-event features."""
    c = d5["close"].to_numpy(float); h = d5["high"].to_numpy(float); l = d5["low"].to_numpy(float)
    o = d5["open"].to_numpy(float); v = d5["volume"].to_numpy(float)
    t = i2["t"]; atrv = i2["atr"]; plo = i2["plo"]; phi = i2["phi"]; f50 = i2["f50"]
    n = len(c)
    hour = d5["open_time"].dt.hour.to_numpy()
    year = d5["open_time"].dt.year.to_numpy()
    rng = np.maximum(h - l, 1e-9)
    body = (c - o) / rng
    uwick = (h - np.maximum(o, c)) / rng
    lwick = (np.minimum(o, c) - l) / rng
    vz = ((pd.Series(v) - pd.Series(v).rolling(100).mean()) / (pd.Series(v).rolling(100).std() + 1e-9)).to_numpy()
    atr_pct = atrv / c
    atr_hi = atr_pct > np.nanmedian(atr_pct)  # vol regime split

    # legs
    legs = []; i = 0
    while i < n:
        if t[i] == 0: i += 1; continue
        j = i
        while j + 1 < n and t[j + 1] == t[i]: j += 1
        legs.append((i, j, t[i])); i = j + 1

    in_pocket = (t != 0) & (c >= plo) & (c <= phi)

    rows = []
    for (s, e, d) in legs:
        leg_len = e - s + 1
        # setup A: enter at flip bar
        rows.append(_feat("flip", s, d, s, leg_len, c, h, l, atrv, atr_pct, atr_hi,
                          body, uwick, lwick, vz, hour, year, i1, i3, t15_on5, f50, plo, phi))
        # setup B: first pocket touch
        touch = -1
        for k in range(s + 1, e + 1):
            if in_pocket[k]: touch = k; break
        if touch != -1:
            rows.append(_feat("pocket", touch, d, s, leg_len, c, h, l, atrv, atr_pct, atr_hi,
                              body, uwick, lwick, vz, hour, year, i1, i3, t15_on5, f50, plo, phi))
        # setup C: first touch of fib0.5 (shallower pullback)
        f50t = -1
        for k in range(s + 1, e + 1):
            if d == 1 and l[k] <= f50[k]:
                f50t = k; break
            if d == -1 and h[k] >= f50[k]:
                f50t = k; break
        if f50t != -1:
            rows.append(_feat("fib50", f50t, d, s, leg_len, c, h, l, atrv, atr_pct, atr_hi,
                              body, uwick, lwick, vz, hour, year, i1, i3, t15_on5, f50, plo, phi))
    return pd.DataFrame(rows)


def _feat(setup, k, d, legstart, leglen, c, h, l, atrv, atr_pct, atr_hi,
          body, uwick, lwick, vz, hour, year, i1, i3, t15, f50, plo, phi):
    conf1 = int(i1["t"][k] == d)
    conf3 = int(i3["t"][k] == d)
    conf15 = int(np.sign(t15[k]) == d)
    return dict(setup=setup, k=int(k), dir=int(d), atr=float(atrv[k]),
                leg_age=int(k - legstart), atr_hi=bool(atr_hi[k]),
                body=float(body[k]), uwick=float(uwick[k]), lwick=float(lwick[k]),
                vz=float(vz[k]) if not math.isnan(vz[k]) else 0.0,
                hour=int(hour[k]), year=int(year[k]),
                conf1=conf1, conf3=conf3, conf15=conf15,
                conf_n=conf1 + conf3 + conf15)


# ----------------------------------------------------------------------------- sweep
def simulate(ev, c, h, l, Wmax, side, tp_mult, sl_mult, ts, fee):
    """Vectorized first-touch TP/SL+timestop P&L (% of entry, after fee) per event.
    side: +1 trade WITH #2 trend (continuation), -1 AGAINST (fade)."""
    k = ev["k"].to_numpy(); d = ev["dir"].to_numpy(); a = ev["atr"].to_numpy()
    entry = c[k]
    trade_dir = side * d                      # +1 long, -1 short
    tp_ret = tp_mult * a / entry              # % target
    sl_ret = sl_mult * a / entry              # % stop
    n_ev = len(k)
    pnl = np.empty(n_ev)
    for i in range(n_ev):
        ki = k[i]; td = trade_dir[i]
        end = min(ki + ts, len(c) - 1)
        hi = h[ki + 1:end + 1]; lo = l[ki + 1:end + 1]
        e = entry[i]
        if td > 0:   # long
            tp_px = e * (1 + tp_ret[i]); sl_px = e * (1 - sl_ret[i])
            tp_hit = np.where(hi >= tp_px)[0]; sl_hit = np.where(lo <= sl_px)[0]
        else:        # short
            tp_px = e * (1 - tp_ret[i]); sl_px = e * (1 + sl_ret[i])
            tp_hit = np.where(lo <= tp_px)[0]; sl_hit = np.where(hi >= sl_px)[0]
        ti = tp_hit[0] if len(tp_hit) else 10 ** 9
        si = sl_hit[0] if len(sl_hit) else 10 ** 9
        if si <= ti and si < 10 ** 9:
            pnl[i] = -sl_ret[i] * 100          # conservative: SL first on tie
        elif ti < si:
            pnl[i] = tp_ret[i] * 100
        else:                                   # timeout, mark to market
            pnl[i] = td * (c[end] / e - 1) * 100
    return pnl - fee  # fee already in % round-trip


FILTERS = {
    "ALL": lambda ev: np.ones(len(ev), bool),
    "conf15": lambda ev: ev["conf15"].to_numpy() == 1,
    "anti15": lambda ev: ev["conf15"].to_numpy() == 0,
    "conf1": lambda ev: ev["conf1"].to_numpy() == 1,
    "conf3": lambda ev: ev["conf3"].to_numpy() == 1,
    "conf2of3": lambda ev: ev["conf_n"].to_numpy() >= 2,
    "conf3of3": lambda ev: ev["conf_n"].to_numpy() == 3,
    "conf0": lambda ev: ev["conf_n"].to_numpy() == 0,
    "volHI": lambda ev: ev["atr_hi"].to_numpy(),
    "volLO": lambda ev: ~ev["atr_hi"].to_numpy(),
    "legYoung": lambda ev: ev["leg_age"].to_numpy() <= 20,
    "legOld": lambda ev: ev["leg_age"].to_numpy() > 20,
    "rejWick": lambda ev: (ev["dir"].to_numpy() == 1) & (ev["lwick"].to_numpy() > 0.4)
                          | (ev["dir"].to_numpy() == -1) & (ev["uwick"].to_numpy() > 0.4),
    "volSpike": lambda ev: ev["vz"].to_numpy() > 1.0,
    "sessUS": lambda ev: (ev["hour"].to_numpy() >= 13) & (ev["hour"].to_numpy() < 21),
    "sessASIA": lambda ev: (ev["hour"].to_numpy() >= 0) & (ev["hour"].to_numpy() < 8),
}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--data", default=os.path.join(REPO, "market_data"))
    ap.add_argument("--fee", type=float, default=0.08, help="round-trip fee in %% (default 0.08)")
    ap.add_argument("--min-trades", type=int, default=150)
    ap.add_argument("--out", default=os.path.join(REPO, "tools", "research", "output"))
    args = ap.parse_args()

    print("loading + indicators ...", flush=True)
    d5 = load("5m", args.data)
    i2 = ind_atrfib(d5); i1 = ind_frama(d5); i3 = ind_vidya(d5)
    d15 = load("15m", args.data); i15 = ind_atrfib(d15)
    t15df = pd.DataFrame({"avail": d15["open_time"] + pd.Timedelta(minutes=15), "t15": i15["t"]}).sort_values("avail")
    m = pd.merge_asof(d5[["open_time"]].sort_values("open_time"), t15df,
                      left_on="open_time", right_on="avail", direction="backward")
    t15_on5 = m["t15"].fillna(0).to_numpy()

    c = d5["close"].to_numpy(float); h = d5["high"].to_numpy(float); l = d5["low"].to_numpy(float)
    ev_all = build_events(d5, i2, i1, i3, t15_on5)
    print(f"events: {len(ev_all)}  (by setup: {ev_all['setup'].value_counts().to_dict()})", flush=True)

    SIDES = {"cont": +1, "fade": -1}
    TP = [0.5, 1.0, 1.5, 2.0, 3.0, 4.0]   # x ATR
    SL = [0.5, 1.0, 1.5, 2.0, 3.0]        # x ATR
    TS = [12, 24, 48, 96, 288]            # bars (1h..24h)
    Wmax = max(TS)
    years = sorted(d5["open_time"].dt.year.unique())

    results = []
    n_combos = 0
    for setup in ["flip", "pocket", "fib50"]:
        evs = ev_all[ev_all["setup"] == setup].reset_index(drop=True)
        yr = evs["year"].to_numpy()
        for sname, sv in SIDES.items():
            for tp in TP:
                for sl in SL:
                    for ts in TS:
                        pnl_full = simulate(evs, c, h, l, Wmax, sv, tp, sl, ts, args.fee)
                        for fname, ffn in FILTERS.items():
                            mask = ffn(evs)
                            n_combos += 1
                            pnl = pnl_full[mask]
                            if len(pnl) < args.min_trades:
                                continue
                            wins = pnl[pnl > 0].sum(); losses = -pnl[pnl < 0].sum()
                            pf = wins / losses if losses > 0 else np.inf
                            # per-year positivity (robustness)
                            ym = yr[mask]
                            yr_pos = 0; yr_tot = 0
                            for y in years:
                                ys = pnl[ym == y]
                                if len(ys) >= 20:
                                    yr_tot += 1; yr_pos += int(ys.mean() > 0)
                            robust = yr_pos / yr_tot if yr_tot else 0.0
                            results.append(dict(
                                setup=setup, side=sname, tp=tp, sl=sl, ts=ts, filter=fname,
                                n=len(pnl), exp=pnl.mean(), win=(pnl > 0).mean() * 100,
                                pf=pf, total=pnl.sum(), robust=robust, yr_tot=yr_tot))
    R = pd.DataFrame(results)
    print(f"combinations evaluated: {n_combos:,}  | scored configs (>= {args.min_trades} trades): {len(R):,}", flush=True)

    os.makedirs(args.out, exist_ok=True)
    R.sort_values("exp", ascending=False).to_csv(os.path.join(args.out, "all_results.csv"), index=False)

    # robust winners: positive expectancy after fees, consistent across most years
    rob = R[(R["exp"] > 0) & (R["robust"] >= 0.7) & (R["n"] >= args.min_trades)].copy()
    rob = rob.sort_values(["exp", "pf"], ascending=False)

    def show(df, title, k=15):
        print(f"\n===== {title} =====")
        if df.empty:
            print("  (none)"); return
        cols = ["setup", "side", "filter", "tp", "sl", "ts", "n", "exp", "win", "pf", "robust"]
        with pd.option_context("display.width", 200, "display.max_columns", None):
            print(df[cols].head(k).to_string(index=False,
                  formatters={"exp": lambda x: f"{x:+.3f}", "win": lambda x: f"{x:.1f}",
                              "pf": lambda x: f"{x:.2f}", "robust": lambda x: f"{x:.2f}"}))

    show(R.sort_values("exp", ascending=False), "TOP 15 by net expectancy/trade (after fees)")
    show(rob, "ROBUST winners (exp>0 AND positive in >=70% of years)")
    print(f"\nfull grid -> {os.path.join(args.out,'all_results.csv')}")


if __name__ == "__main__":
    main()
