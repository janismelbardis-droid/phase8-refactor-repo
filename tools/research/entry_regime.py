"""
Regime-routed entry around #2 flips (the user's model):

  - if the flip ALREADY came after a substantial pullback (it swept the opposite
    side / is not extended) -> enter AT the flip.
  - if the flip is fresh / extended (no pullback yet) -> WAIT for the first
    pullback of P*ATR, then enter; skip if no pullback comes.

Compares: (1) naive enter-at-flip, (2) always-wait-pullback, (3) regime-routed.
Runs on 1h (fee-viable). BTC from market_data, basket from data_cache/basket.
"""
from __future__ import annotations
import argparse, glob, os, numpy as np, pandas as pd, sys
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import regime_change_research as base
import structure_edge as se

SWEEP_W = 12        # window to detect the pre-flip liquidity sweep
WAIT = 24           # bars allowed to wait for the first pullback
H = 48              # max bars in trade


def flip_events(df):
    i2 = base.ind_atrfib(df)
    c = df["close"].to_numpy(float); h = df["high"].to_numpy(float); l = df["low"].to_numpy(float)
    t = i2["t"]; atrv = i2["atr"]; yr = df["open_time"].dt.year.to_numpy(); n = len(c)
    sh1, sh2, sl1, sl2 = se.swings(df)
    legs = []; i = 0
    while i < n:
        if t[i] == 0: i += 1; continue
        j = i
        while j + 1 < n and t[j + 1] == t[i]: j += 1
        legs.append((i, t[i])); i = j + 1
    ev = []
    for (k, d) in legs:
        if k < 1:
            continue
        lo_w = l[max(0, k - SWEEP_W):k + 1].min(); hi_w = h[max(0, k - SWEEP_W):k + 1].max()
        sweep = (d == 1 and not np.isnan(sl1[k]) and lo_w < sl1[k] and c[k] > sl1[k]) or \
                (d == -1 and not np.isnan(sh1[k]) and hi_w > sh1[k] and c[k] < sh1[k])
        disp = abs(c[k] - c[max(0, k - 6)]) / atrv[k] if atrv[k] > 0 else 0.0
        ev.append((k, d, bool(sweep), disp, int(yr[k])))
    return ev, c, h, l, t, atrv, n


def trade_from(c, h, l, entry_idx, entry_px, d, atr, tp, sl, n, fee):
    e = entry_px; end = min(entry_idx + H, n - 1); tpr = tp * atr / e; slr = sl * atr / e
    if d > 0:
        th = np.where(h[entry_idx + 1:end + 1] >= e * (1 + tpr))[0]; sh = np.where(l[entry_idx + 1:end + 1] <= e * (1 - slr))[0]
    else:
        th = np.where(l[entry_idx + 1:end + 1] <= e * (1 - tpr))[0]; sh = np.where(h[entry_idx + 1:end + 1] >= e * (1 + slr))[0]
    ti = th[0] if len(th) else 10**9; si = sh[0] if len(sh) else 10**9
    if si <= ti and si < 10**9:
        g = -slr; xo = si
    elif ti < si:
        g = tpr; xo = ti
    else:
        g = d * (c[end] / e - 1); xo = end - (entry_idx + 1)
    return g * 100 - fee, slr, entry_idx + 1 + int(xo)


def run(ev_pack, mode, P, tp, sl, fee):
    ev, c, h, l, t, atrv, n = ev_pack
    trades = []; le = -1
    for (k, d, sweep, disp, yr) in ev:
        if k <= le:
            continue
        atr = atrv[k]
        enter_at_flip = (mode == "naive") or (mode == "routed" and sweep)
        if enter_at_flip:
            net, slr, xidx = trade_from(c, h, l, k, c[k], d, atr, tp, sl, n, fee)
        else:
            # wait for first pullback P*ATR against flip dir within WAIT bars
            jfill = -1; ent = None
            for m in range(k + 1, min(k + WAIT, n - 1) + 1):
                if d > 0 and l[m] <= c[k] - P * atr: ent = c[k] - P * atr; jfill = m; break
                if d < 0 and h[m] >= c[k] + P * atr: ent = c[k] + P * atr; jfill = m; break
            if jfill < 0:
                continue
            net, slr, xidx = trade_from(c, h, l, jfill, ent, d, atr, tp, sl, n, fee)
        le = xidx
        trades.append((yr, net / (slr * 100), net))
    return pd.DataFrame(trades, columns=["year", "R", "net"])


def report(T, label, risk=0.01):
    if T.empty:
        print(f"  {label}: no trades"); return
    eq = 10000.0; cur = []
    for _, r in T.iterrows(): eq *= (1 + risk * r["R"]); cur.append(eq)
    roi = (eq / 10000 - 1) * 100
    e = np.concatenate([[10000.0], cur]); dd = (1 - e / np.maximum.accumulate(e)).max() * 100
    R = T["R"]; pf = R[R > 0].sum() / -R[R < 0].sum() if (R < 0).any() else 9
    drops = []
    for N in [5, 10]:
        Rr = R.to_numpy().copy()
        for _ in range(N): Rr[np.argmax(Rr)] = 0
        ee = 10000.0
        for x in Rr: ee *= (1 + risk * x)
        drops.append((ee / 10000 - 1) * 100)
    pys = " ".join(f"{y}:{(g['net']>0).mean()*100:.0f}" for y, g in T.groupby("year"))
    print(f"  {label:22s}: n{len(T):4d} win{(T['net']>0).mean()*100:3.0f}% ROI{roi:+5.0f}% DD{dd:3.0f}% PF{pf:.2f} "
          f"avgR{R.mean():+.2f} | dropTop5/10 {drops[0]:+.0f}/{drops[1]:+.0f}% | yr-win {pys}")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--tf", default="1h"); ap.add_argument("--tp", type=float, default=1.5)
    ap.add_argument("--sl", type=float, default=2.0); ap.add_argument("--P", type=float, default=1.0)
    ap.add_argument("--fee", type=float, default=0.04)
    a = ap.parse_args()
    df = base.load(a.tf, os.path.join(base.REPO, "market_data"))
    ev_pack = flip_events(df)
    swr = np.mean([1 if e[2] else 0 for e in ev_pack[0]]) * 100
    print(f"\n{a.tf} BTC: {len(ev_pack[0])} flips, sweep(post-pullback) regime = {swr:.0f}% of flips")
    print(f"harvest TP{a.tp}/SL{a.sl}, pullback {a.P} ATR, fee {a.fee}%\n")
    report(run(ev_pack, "naive", a.P, a.tp, a.sl, a.fee), "1) naive @flip")
    report(run(ev_pack, "wait", a.P, a.tp, a.sl, a.fee), "2) always wait pullback")
    report(run(ev_pack, "routed", a.P, a.tp, a.sl, a.fee), "3) ROUTED (sweep@flip,else wait)")


if __name__ == "__main__":
    main()
