"""
Readable equity / performance report for a surviving structure edge.

Sequential execution (one position at a time, no overlap), risk-based sizing:
each trade risks RISK% of equity (stop = SL x ATR), so max drawdown is meaningful.
Maker fee. Prints a per-year table (trades / win% / ROI / max DD) and saves an
equity-curve PNG.

  python tools/research/equity_report.py
"""
from __future__ import annotations
import argparse, os, numpy as np, pandas as pd, sys
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import regime_change_research as base
import structure_edge as se
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt


def one_trade(c, h, l, k, trade_dir, atr, tp, sl, ts, fee):
    e = c[k]
    tp_ret = tp * atr / e; sl_ret = sl * atr / e
    end = min(k + ts, len(c) - 1)
    if trade_dir > 0:
        tp_px, sl_px = e * (1 + tp_ret), e * (1 - sl_ret)
        th = np.where(h[k + 1:end + 1] >= tp_px)[0]; sh = np.where(l[k + 1:end + 1] <= sl_px)[0]
    else:
        tp_px, sl_px = e * (1 - tp_ret), e * (1 + sl_ret)
        th = np.where(l[k + 1:end + 1] <= tp_px)[0]; sh = np.where(h[k + 1:end + 1] >= sl_px)[0]
    ti = th[0] if len(th) else 10 ** 9
    si = sh[0] if len(sh) else 10 ** 9
    if si <= ti and si < 10 ** 9:
        gross = -sl_ret; exit_off = si
    elif ti < si:
        gross = tp_ret; exit_off = ti
    else:
        exit_off = end - (k + 1); gross = trade_dir * (c[end] / e - 1)
    net_pct = gross * 100 - fee
    R = net_pct / (sl_ret * 100)
    return net_pct, R, k + 1 + int(exit_off)


def run(ev, c, h, l, setup, side, filt, tp, sl, ts, fee, risk, init=10000.0):
    evs = ev[ev["setup"] == setup].reset_index(drop=True)
    m = se.STRUCT_FILTERS[filt](evs)
    evs = evs[m].sort_values("k").reset_index(drop=True)
    sv = +1 if side == "cont" else -1
    eq = init; last_exit = -1; curve = []; trades = []
    for _, r in evs.iterrows():
        k = int(r["k"])
        if k <= last_exit:           # no overlap
            continue
        td = sv * int(r["dir"])
        net_pct, R, xidx = one_trade(c, h, l, k, td, float(r["atr"]), tp, sl, ts, fee)
        eq *= (1 + risk * R)
        last_exit = xidx
        trades.append(dict(year=int(r["year"]), dir=td, R=R, net_pct=net_pct, eq=eq, k=k))
        curve.append(eq)
    return pd.DataFrame(trades)


def max_dd(equity):
    peak = np.maximum.accumulate(equity)
    return float((1 - equity / peak).max() * 100)


def per_year_table(tr, init=10000.0):
    rows = []
    eq_start = init
    for y, g in tr.groupby("year"):
        eq_end = g["eq"].iloc[-1]
        roi = (eq_end / eq_start - 1) * 100
        # intra-year drawdown on the running equity (incl. starting point)
        eqs = np.concatenate([[eq_start], g["eq"].to_numpy()])
        dd = max_dd(eqs)
        wins = (g["net_pct"] > 0).mean() * 100
        rows.append(dict(year=y, trades=len(g), win=wins, roi=roi, maxDD=dd,
                         avgR=g["R"].mean(), eq_end=eq_end))
        eq_start = eq_end
    return pd.DataFrame(rows)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--fee", type=float, default=0.04)
    ap.add_argument("--risk", type=float, default=1.0, help="%% equity risked per trade")
    args = ap.parse_args()
    risk = args.risk / 100.0

    d5 = base.load("5m", os.path.join(base.REPO, "market_data"))
    i2 = base.ind_atrfib(d5); i1 = base.ind_frama(d5); i3 = base.ind_vidya(d5)
    d15 = base.load("15m", os.path.join(base.REPO, "market_data")); i15 = base.ind_atrfib(d15)
    t15df = pd.DataFrame({"avail": d15["open_time"] + pd.Timedelta(minutes=15), "t15": i15["t"]}).sort_values("avail")
    t15 = pd.merge_asof(d5[["open_time"]].sort_values("open_time"), t15df,
                        left_on="open_time", right_on="avail", direction="backward")["t15"].fillna(0).to_numpy()
    ev = se.build(d5, i2, i1, i3, t15)
    c = d5["close"].to_numpy(float); h = d5["high"].to_numpy(float); l = d5["low"].to_numpy(float)

    CONFIGS = [
        ("A  noBOS-flip steady", "flip", "cont", "noBOS", 2.0, 3.0, 96),
        ("B  noBOS-flip wide",   "flip", "cont", "noBOS", 4.0, 3.0, 288),
        ("C  sweep-pocket",      "pocket", "cont", "sweep", 3.0, 3.0, 96),
    ]
    print(f"\nSizing: risk {args.risk:.0f}% equity/trade, stop = SL x ATR, maker fee {args.fee}% round-trip, "
          f"one position at a time, start $10,000\n")

    fig, ax = plt.subplots(figsize=(11, 6))
    for label, setup, side, filt, tp, sl, ts in CONFIGS:
        tr = run(ev, c, h, l, setup, side, filt, tp, sl, ts, args.fee, risk)
        pyt = per_year_table(tr)
        total_roi = (tr["eq"].iloc[-1] / 10000 - 1) * 100
        odd = max_dd(np.concatenate([[10000.0], tr["eq"].to_numpy()]))
        wins = (tr["net_pct"] > 0); pf = tr.loc[wins, "net_pct"].sum() / -tr.loc[~wins, "net_pct"].sum()
        print(f"================= {label}  (TP{tp}/SL{sl} ATR, {ts*5//60}h max hold) =================")
        print(f"  {setup} / {'long' if side=='cont' else 'fade'}-with-trend  |  total trades {len(tr)}")
        with pd.option_context("display.width", 200):
            print(pyt.to_string(index=False, formatters={
                "win": lambda x: f"{x:4.0f}%", "roi": lambda x: f"{x:+7.1f}%",
                "maxDD": lambda x: f"{x:5.1f}%", "avgR": lambda x: f"{x:+.2f}",
                "eq_end": lambda x: f"${x:,.0f}"}))
        print(f"  OVERALL: ROI {total_roi:+.1f}%  | max drawdown {odd:.1f}%  | win {wins.mean()*100:.0f}%  "
              f"| profit factor {pf:.2f}  | avg {tr['R'].mean():+.2f}R/trade\n")
        # equity curve over time
        t_idx = d5["open_time"].to_numpy()[tr["k"].to_numpy()]
        ax.plot(t_idx, tr["eq"].to_numpy(), label=f"{label}  (ROI {total_roi:+.0f}%, DD {odd:.0f}%)")

    ax.set_yscale("log"); ax.set_title("Equity — structure edges, BTCUSDT 5m, risk 1%/trade, maker fee")
    ax.set_ylabel("equity $ (log)"); ax.legend(); ax.grid(True, alpha=0.3)
    out = os.path.join(base.REPO, "tools", "research", "output", "equity_curve.png")
    fig.tight_layout(); fig.savefig(out, dpi=110)
    print(f"equity curve -> {out}")


if __name__ == "__main__":
    main()
