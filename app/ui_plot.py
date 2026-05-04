from __future__ import annotations

"""Matplotlib figure builders used by the Tk UI."""

import matplotlib.dates as mdates
import matplotlib.pyplot as plt
import pandas as pd

from .constants import C_BG_INPUT, C_BG_PANEL, C_BORDER_IDLE, C_DIM, C_GREEN, C_TEXT


def build_figure(tf: str, dft: "pd.DataFrame") -> "plt.Figure":
    x = dft.index.to_pydatetime()
    price = dft["price"].values
    macd = dft["macd"].values
    sig = dft["signal"].values

    fig = plt.Figure(figsize=(11.5, 7.5), dpi=100)
    fig.patch.set_facecolor(C_BG_PANEL)
    gs = fig.add_gridspec(2, 1, height_ratios=[2.0, 2.2], hspace=0.12)
    ax_price = fig.add_subplot(gs[0, 0])
    ax_macd = fig.add_subplot(gs[1, 0], sharex=ax_price)

    for ax in (ax_price, ax_macd):
        ax.set_facecolor(C_BG_INPUT)
        ax.tick_params(colors=C_DIM)
        for spine in ax.spines.values():
            spine.set_color(C_BORDER_IDLE)

    fig.suptitle(f"{tf} - Price + MACD/Signal", fontsize=12, color=C_TEXT)

    ax_price.plot(x, price, linewidth=1.0, color=C_TEXT)
    ax_price.set_ylabel("Price", color=C_TEXT)
    ax_price.grid(True, alpha=0.25)

    ax_macd.plot(x, macd, linewidth=1.2, label="MACD", color=C_GREEN)
    ax_macd.plot(x, sig, linewidth=1.0, alpha=0.9, label="Signal", color=C_TEXT)
    ax_macd.axhline(0, linewidth=1.0, alpha=0.5, color=C_DIM)
    ax_macd.set_ylabel("MACD", color=C_TEXT)
    ax_macd.grid(True, alpha=0.25)
    legend = ax_macd.legend(loc="upper left")
    if legend is not None:
        try:
            legend.get_frame().set_facecolor(C_BG_PANEL)
            legend.get_frame().set_edgecolor(C_BORDER_IDLE)
            for txt in legend.get_texts():
                txt.set_color(C_TEXT)
        except Exception:
            pass

    ax_macd.xaxis.set_major_formatter(mdates.DateFormatter("%Y-%m-%d\n%H:%M"))
    for lbl in ax_price.get_xticklabels():
        lbl.set_visible(False)
    return fig
