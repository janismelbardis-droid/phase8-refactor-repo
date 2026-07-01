"""
Mean-reversion on BTC 5m (the unexplored opposite of everything tried so far).

Idea: 5m mostly chops -> fade stretches away from a moving average.
z = (close - SMA(n)) / std(n). Enter AGAINST the stretch when z crosses +/-Z,
exit back at the mean (z->0) as target, or a wider z as stop, or a time cap.

Lookahead-safe: signal on bar close (z uses only past), entry at that close,
exits scanned on later bars. Fee per round trip. Reported per year.
"""
from __future__ import annotations
import argparse, os, numpy as np, pandas as pd, sys
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import regime_change_research as base


def run(c, h, l, sma, sd, year, n_, Z, Zstop, hor, cost):
    z = (c - sma) / (sd + 1e-9)
    n = len(c); out = []
    i = n_ + 1
    last_exit = -1
    while i < n:
        if i <= last_exit or np.isnan(z[i]) or np.isnan(z[i-1]):
            i += 1; continue
        d = 0
        if z[i-1] > -Z and z[i] <= -Z: d = 1      # stretched down -> fade long
        elif z[i-1] < Z and z[i] >= Z: d = -1     # stretched up -> fade short
        if d == 0:
            i += 1; continue
        entry = c[i]; end = min(i + hor, n - 1); exitp = c[end]
        for k in range(i + 1, end + 1):
            zk = (c[k] - sma[k]) / (sd[k] + 1e-9)
            if d == 1:
                if l[k] <= entry * (1 - 0):  # placeholder no fixed stop; use z-stop
                    pass
                if zk >= 0: exitp = sma[k]; end = k; break        # target: back to mean
                if zk <= -Zstop: exitp = c[k]; end = k; break     # stop: stretched further
            else:
                if zk <= 0: exitp = sma[k]; end = k; break
                if zk >= Zstop: exitp = c[k]; end = k; break
        net = d * (exitp / entry - 1) * 100 - cost
        out.append((int(year[i]), net)); last_exit = end
        i = end + 1
    return pd.DataFrame(out, columns=["year", "net"])


def main():
    ap = argparse.ArgumentParser(); ap.add_argument("--cost", type=float, default=0.08)
    a = ap.parse_args()
    d = base.load("5m", os.path.join(base.REPO, "market_data"))
    c = d["close"].to_numpy(float); h = d["high"].to_numpy(float); l = d["low"].to_numpy(float)
    year = d["open_time"].dt.year.to_numpy()
    print(f"BTC 5m mean-reversion (z-score), fee {a.cost}%. Positive avg-net%/trade every year = edge.\n")
    print(f"{'n':>4} {'Z':>4} {'Zstop':>6} {'hor':>4} | {'trades':>7} {'win%':>5} {'avgNet%':>8} | per-year avgNet%")
    best = None
    for n_ in [50, 100, 200]:
        sma = pd.Series(c).rolling(n_).mean().to_numpy()
        sd = pd.Series(c).rolling(n_).std().to_numpy()
        for Z in [2.0, 2.5, 3.0]:
            for Zstop in [3.5, 4.5]:
                for hor in [50, 100]:
                    T = run(c, h, l, sma, sd, year, n_, Z, Zstop, hor, a.cost)
                    if len(T) < 200: continue
                    pyw = T.groupby("year")["net"].mean()
                    pos_years = (pyw > 0).sum(); ny = pyw.size
                    line = " ".join(f"{y%100}:{v:+.2f}" for y, v in pyw.items())
                    flag = "  <== все годы +" if pos_years == ny else ""
                    avg = T["net"].mean()
                    print(f"{n_:>4} {Z:>4} {Zstop:>6} {hor:>4} | {len(T):>7} {(T['net']>0).mean()*100:>4.0f}% {avg:>+7.3f} | {line}{flag}")
                    if avg > 0 and (best is None or avg > best[0]):
                        best = (avg, n_, Z, Zstop, hor, pos_years, ny)
    if best:
        print(f"\nЛучшая по avgNet: n{best[1]} Z{best[2]} Zstop{best[3]} hor{best[4]}  "
              f"avg {best[0]:+.3f}%  плюс {best[5]}/{best[6]} лет")


if __name__ == "__main__":
    main()
