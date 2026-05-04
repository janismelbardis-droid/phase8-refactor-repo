from __future__ import annotations

"""Backtest charts, trade tree, reports, trade inspector, and render path."""

import gzip
import json
import os
import re
import threading
import traceback
from typing import Any, Dict, List, Optional, Tuple

import matplotlib.dates as mdates
import numpy as np
import pandas as pd
import tkinter as tk
from tkinter import filedialog, messagebox, ttk

import matplotlib.pyplot as plt
from matplotlib.backends.backend_tkagg import FigureCanvasTkAgg, NavigationToolbar2Tk
from matplotlib.collections import LineCollection

from ...backtest import BacktestConfig, BacktestResult, Trade
from ...constants import *
from ...indicators_streaming import macd_series, simulate_multitf_indicators_fast, slice_and_decimate
from ...research.replay import build_backtest_replay_plan
from ...research.trade_audit import write_trade_audit_csv, write_trade_audit_report
from ...rules import Rule
from ...ui_plot import build_figure


class BacktestChartsMixin:
    """Matplotlib backtest UI: OHLC/MACD/equity, trade selection, reports, inspector."""

    def _bt_schedule_chart_redraw(self, reset_cache: bool = False):
        """Throttle chart redraws so UI stays responsive when sliding/typing."""
        try:
            if reset_cache:
                try:
                    self._bt_ohlc_cache = {}
                except Exception:
                    pass
            if getattr(self, "_bt_chart_redraw_job", None) is not None:
                try:
                    self.after_cancel(self._bt_chart_redraw_job)
                except Exception:
                    pass
            self._bt_chart_redraw_job = self.after(60, self._bt_safe_redraw_chart)
        except Exception:
            try:
                self._bt_safe_redraw_chart()
            except Exception:
                pass

    def _bt_safe_redraw_chart(self):
        try:
            self._bt_draw_chart()
        except Exception as e:
            try:
                self.status_var.set(f"Chart error: {e}")
            except Exception:
                pass
            try:
                # Never crash the app due to charting.
                print("\n[Chart error]\n", traceback.format_exc())
            except Exception:
                pass

    def _bt_on_chart_mode_changed(self):
        # Reset view position when switching modes for a predictable experience.
        try:
            if hasattr(self, "bt_chart_pos_var"):
                self.bt_chart_pos_var.set(0.0)
        except Exception:
            pass
        self._bt_schedule_chart_redraw(reset_cache=False)

    def _bt_reset_view_controls(self):
        """Reset chart controls to safe defaults."""
        try:
            if hasattr(self, "bt_chart_pos_var"):
                self.bt_chart_pos_var.set(0.0)
        except Exception:
            pass
        try:
            if hasattr(self, "bt_chart_bars_var"):
                # Keep user's preference if reasonable; otherwise default to 600.
                v = int(self.bt_chart_bars_var.get())
                if v < 100:
                    self.bt_chart_bars_var.set(600)
        except Exception:
            pass
        self._bt_schedule_chart_redraw(reset_cache=False)

    def _bt_get_chart_ohlc(self, tf: str) -> "pd.DataFrame":
        """Return an OHLC dataframe indexed by close_time for the given TF, cached."""
        tf = (tf or "1m").strip()
        cache = getattr(self, "_bt_ohlc_cache", None)
        if cache is None:
            cache = {}
            self._bt_ohlc_cache = cache
        if tf in cache:
            return cache[tf]

        if not getattr(self, "hist_df_1m_full", None) is not None:
            cache[tf] = pd.DataFrame()
            return cache[tf]

        df = self.hist_df_1m_full
        if df is None or df.empty:
            cache[tf] = pd.DataFrame()
            return cache[tf]

        # Use close_time as the index for consistent alignment.
        dfx = df.copy()
        try:
            dfx["close_time"] = pd.to_datetime(dfx["close_time"], utc=True)
        except Exception:
            pass
        try:
            dfx = dfx.sort_values("close_time")
        except Exception:
            pass

        try:
            dfx = dfx.set_index("close_time", drop=False)
        except Exception:
            cache[tf] = pd.DataFrame()
            return cache[tf]

        # Restrict to the backtest window (no need to plot warmup).
        try:
            if self.bt_result is not None:
                st = pd.to_datetime(self.bt_result.start, utc=True)
                en = pd.to_datetime(self.bt_result.end, utc=True)
                dfx = dfx.loc[(dfx.index >= st) & (dfx.index <= en)]
        except Exception:
            pass

        if dfx.empty:
            cache[tf] = pd.DataFrame()
            return cache[tf]

        if tf == "1m":
            out = dfx[["open", "high", "low", "close"]].copy()
        else:
            # Resample from 1m to the requested TF.
            rule = None
            try:
                ms = interval_to_ms(tf)
                mins = int(ms // 60_000)
                rule = f"{mins}T"
            except Exception:
                rule = None
            if not rule:
                cache[tf] = pd.DataFrame()
                return cache[tf]
            try:
                out = dfx[["open", "high", "low", "close"]].resample(rule, label="right", closed="right").agg(
                    {"open": "first", "high": "max", "low": "min", "close": "last"}
                )
            except Exception:
                out = pd.DataFrame()

        try:
            out = out.dropna()
        except Exception:
            pass

        cache[tf] = out
        return out

    def _bt_set_view_center_time(self, center_ts: "pd.Timestamp"):
        """Center the window around a timestamp using the current TF/bars controls."""
        try:
            if center_ts is None:
                return
            tf = (self.bt_chart_tf_var.get() if hasattr(self, "bt_chart_tf_var") else "1m") or "1m"
            ohlc = self._bt_get_chart_ohlc(tf)
            if ohlc is None or ohlc.empty:
                return
            bars = int(self.bt_chart_bars_var.get()) if hasattr(self, "bt_chart_bars_var") else 600
            bars = max(100, min(5000, bars))
            # Find nearest index
            try:
                idx = int(ohlc.index.get_indexer([pd.to_datetime(center_ts, utc=True)], method="nearest")[0])
            except Exception:
                # fallback: linear scan
                idx = int(max(0, min(len(ohlc) - 1, len(ohlc) // 2)))
            off = idx - bars // 2
            max_off = max(0, len(ohlc) - bars)
            off = max(0, min(max_off, off))
            try:
                self.bt_chart_pos_var.set(float(off))
            except Exception:
                pass
        except Exception:
            pass

    def _bt_center_on_selected_trade(self):
        """Center chart window on the currently selected trade (if any)."""
        if not self.bt_result or not self.bt_result.trades:
            return
        tid = getattr(self, "bt_selected_trade_id", None)
        if tid is None:
            # fallback to current tree selection
            try:
                sel = self.bt_tree.selection()
                if sel:
                    tid = int(sel[0])
            except Exception:
                tid = None
        if tid is None:
            return
        tr = next((t for t in self.bt_result.trades if int(getattr(t, "trade_id", -1)) == int(tid)), None)
        if tr is None:
            return
        try:
            self._bt_set_view_center_time(pd.to_datetime(tr.entry_time, utc=True))
            self._bt_schedule_chart_redraw(reset_cache=False)
        except Exception:
            pass

    def _bt_draw_chart_candles(self, *, popup: bool = False):
        """Fast windowed candle chart for backtest inspection."""
        # Select target axes/canvas depending on main vs popup.
        if popup:
            ax_p = getattr(self, "_bt_popup_price_ax", None)
            ax_m = getattr(self, "_bt_popup_macd_ax", None)
            canvas = getattr(self, "_bt_popup_canvas", None)
        else:
            ax_p = getattr(self, "bt_price_ax", None)
            ax_m = getattr(self, "bt_macd_ax", None)
            canvas = getattr(self, "bt_price_canvas", None)

        if ax_p is None or ax_m is None or canvas is None:
            return

        if not self.bt_result:
            try:
                ax_p.clear(); ax_m.clear()
                ax_p.set_title("No backtest results yet.")
                canvas.draw_idle()
            except Exception:
                pass
            return

        tf = (self.bt_chart_tf_var.get() if hasattr(self, "bt_chart_tf_var") else "1m") or "1m"
        ohlc = self._bt_get_chart_ohlc(tf)
        if ohlc is None or ohlc.empty:
            try:
                ax_p.clear(); ax_m.clear()
                ax_p.set_title("No candle data in range.")
                canvas.draw_idle()
            except Exception:
                pass
            return

        # Bars/view
        try:
            bars = int(self.bt_chart_bars_var.get())
        except Exception:
            bars = 600
        bars = max(100, min(5000, bars))

        max_off = max(0, len(ohlc) - bars)
        # Update slider range (best-effort)
        try:
            if hasattr(self, "bt_chart_pos_scale"):
                self.bt_chart_pos_scale.configure(to=max_off)
        except Exception:
            pass

        try:
            off = int(round(float(self.bt_chart_pos_var.get())))
        except Exception:
            off = 0
        off = max(0, min(max_off, off))
        try:
            if abs(float(self.bt_chart_pos_var.get()) - float(off)) > 0.01:
                self.bt_chart_pos_var.set(float(off))
        except Exception:
            pass

        view = ohlc.iloc[off:off + bars]
        if view.empty:
            return

        # Update window label
        try:
            if hasattr(self, "bt_chart_window_label"):
                t0 = view.index[0].strftime("%Y-%m-%d %H:%M")
                t1 = view.index[-1].strftime("%Y-%m-%d %H:%M")
                self.bt_chart_window_label.configure(text=f"{tf}  {t0} → {t1}")
        except Exception:
            pass

        ax_p.clear(); ax_m.clear()

        # Prepare candle vectors
        try:
            xdt = view.index.to_pydatetime()
            x = mdates.date2num(xdt)
            o = view["open"].astype(float).values
            h = view["high"].astype(float).values
            l = view["low"].astype(float).values
            c = view["close"].astype(float).values

            # Wicks (single artist)
            ax_p.vlines(x, l, h, linewidth=0.6, alpha=0.65)

            # Bodies (cap to keep it fast)
            ms = interval_to_ms(tf)
            width = (ms / 86_400_000.0) * 0.78  # days
            try:
                up = c >= o
                colors = [C_GREEN if u else C_RED for u in up]
            except Exception:
                colors = C_GREEN
            ax_p.bar(x, (c - o), bottom=o, width=width, align="center", color=colors, edgecolor=colors, linewidth=0.0, alpha=0.85)
        except Exception:
            # Fallback: close line
            try:
                ax_p.plot(view.index.to_pydatetime(), view["close"].astype(float).values, linewidth=1.0, alpha=0.9)
            except Exception:
                pass

        # MACD panel: compute on closes with a bit of lookback for stability.
        try:
            impl = (self.macd_impl_var.get() or "TRADINGVIEW").upper()
        except Exception:
            impl = "TRADINGVIEW"
        try:
            # Use a small lookback window to stabilize the EMA state.
            lookback = max(200, min(5000, off + bars))
            seg2 = ohlc.iloc[max(0, (off + bars) - lookback):(off + bars)]
            closes2 = seg2["close"].astype(float).values
            macd_line, sig_line, hist = macd_series(closes2, impl)
            # Keep only last 'bars' points aligned with view.
            macd_line = macd_line[-len(view):]
            sig_line = sig_line[-len(view):]
            hist = hist[-len(view):]
            ax_m.plot(view.index.to_pydatetime(), macd_line, linewidth=1.0, label="MACD")
            ax_m.plot(view.index.to_pydatetime(), sig_line, linewidth=1.0, label="Signal")
            ax_m.plot(view.index.to_pydatetime(), hist, linewidth=0.9, alpha=0.35, label="Hist")
            ax_m.axhline(0.0, linewidth=0.8, alpha=0.5)
        except Exception:
            pass

        # Trade overlays only for trades that intersect the visible window
        try:
            res = self.bt_result
            t0 = pd.to_datetime(view.index[0], utc=True)
            t1 = pd.to_datetime(view.index[-1], utc=True)

            if popup:
                # reset popup mappings
                self._bt_popup_entry_ids = []
                self._bt_popup_exit_ids = []
                self._bt_popup_trade_ids_for_collection = []
            else:
                self.bt_entry_ids = []
                self.bt_exit_ids = []
                self.bt_trade_ids_for_collection = []

            segments = []
            seg_colors = []
            ent_x, ent_y, ent_c = [], [], []
            ex_x, ex_y, ex_c = [], [], []

            for tr in res.trades:
                et = pd.to_datetime(tr.entry_time, utc=True)
                xt = pd.to_datetime(tr.exit_time, utc=True)
                if xt < t0 or et > t1:
                    continue

                col = C_GREEN if tr.net_pnl >= 0 else C_RED
                try:
                    x1 = mdates.date2num(et.to_pydatetime())
                    x2 = mdates.date2num(xt.to_pydatetime())
                    segments.append([(x1, float(tr.entry_price)), (x2, float(tr.exit_price))])
                    seg_colors.append(col)
                    if popup:
                        self._bt_popup_trade_ids_for_collection.append(tr.trade_id)
                    else:
                        self.bt_trade_ids_for_collection.append(tr.trade_id)
                except Exception:
                    pass

                try:
                    ent_x.append(et.to_pydatetime()); ent_y.append(float(tr.entry_price)); ent_c.append(col)
                    ex_x.append(xt.to_pydatetime()); ex_y.append(float(tr.exit_price)); ex_c.append(col)
                    if popup:
                        self._bt_popup_entry_ids.append(tr.trade_id)
                        self._bt_popup_exit_ids.append(tr.trade_id)
                    else:
                        self.bt_entry_ids.append(tr.trade_id)
                        self.bt_exit_ids.append(tr.trade_id)
                except Exception:
                    pass

            if segments:
                try:
                    from matplotlib.collections import LineCollection
                    lc = LineCollection(segments, colors=seg_colors, linewidths=1.8, alpha=0.9, zorder=6)
                    try:
                        lc.set_picker(5)
                    except Exception:
                        pass
                    ax_p.add_collection(lc)
                    if popup:
                        self._bt_popup_trade_line_collection = lc
                    else:
                        self.bt_trade_line_collection = lc
                except Exception:
                    pass

            if ent_x:
                sc1 = ax_p.scatter(ent_x, ent_y, c=ent_c, marker="^", s=60, picker=6, zorder=7, label="Entry")
                sc2 = ax_p.scatter(ex_x, ex_y, c=ex_c, marker="v", s=60, picker=6, zorder=7, label="Exit")
                if popup:
                    self._bt_popup_entry_scatter = sc1
                    self._bt_popup_exit_scatter = sc2
                else:
                    self.bt_entry_scatter = sc1
                    self.bt_exit_scatter = sc2
        except Exception:
            pass

        ax_p.grid(True, alpha=0.20)
        ax_m.grid(True, alpha=0.18)
        ax_p.set_ylabel("Price")
        ax_m.set_ylabel("MACD")
        ax_m.set_xlabel("Time (UTC)")
        try:
            sym = self.bt_result.summary.get("symbol", "")
        except Exception:
            sym = ""
        ax_p.set_title(f"{sym} — Backtest | Trades: {len(self.bt_result.trades)}  (view: {tf})")

        locator = mdates.AutoDateLocator(minticks=4, maxticks=10)
        try:
            fmt = mdates.ConciseDateFormatter(locator)
            ax_m.xaxis.set_major_locator(locator)
            ax_m.xaxis.set_major_formatter(fmt)
        except Exception:
            ax_m.xaxis.set_major_locator(locator)
            ax_m.xaxis.set_major_formatter(mdates.DateFormatter("%m-%d\n%H:%M"))

        ax_p.tick_params(labelbottom=False)

        try:
            _safe_axis_legend(ax_p, loc="upper left")
        except Exception:
            pass
        try:
            _safe_axis_legend(ax_m, loc="upper left")
        except Exception:
            pass

        # Re-apply highlight if a trade is selected (without auto-zoom).
        try:
            if getattr(self, "bt_selected_trade_id", None) is not None:
                self._bt_select_trade_by_id(int(self.bt_selected_trade_id), from_chart=False, open_details=False, auto_zoom=False)
        except Exception:
            pass
        try:
            canvas.draw_idle()
        except Exception:
            pass
    def _bt_draw_chart(self):
        """Draw backtest chart.

        Default is a fast candle-by-candle *windowed* viewer to avoid freezes/crashes on large datasets.
        You can switch to LINE mode for the old full-range view (heavier).
        """
        try:
            self._ensure_bt_price_chart_built()
        except Exception:
            pass
        try:
            mode = (self.bt_chart_mode_var.get() if hasattr(self, "bt_chart_mode_var") else "CANDLES")
        except Exception:
            mode = "CANDLES"
        mode_u = str(mode).strip().upper()

        if mode_u.startswith("LINE"):
            try:
                self._bt_draw_chart_line_legacy()
            except Exception:
                pass
        else:
            try:
                self._bt_draw_chart_candles(popup=False)
            except Exception:
                pass

        # Note: Removed heavy full-range pop-out syncing. Use Trade Inspector instead.
    def _bt_draw_chart_line_legacy(self):
        """Legacy full-range line chart (can be heavy on large ranges)."""
        if not hasattr(self, "bt_price_ax") or not hasattr(self, "bt_macd_ax"):
            return

        ax_p = self.bt_price_ax
        ax_m = self.bt_macd_ax

        if not self.bt_result or not self.bt_streams_full or "1m" not in self.bt_streams_full:
            try:
                ax_p.clear()
                ax_m.clear()
                ax_p.set_title("No backtest results yet.")
                ax_m.set_title("")
                self.bt_price_canvas.draw_idle()
            except Exception:
                pass
            return

        res = self.bt_result
        df1 = self.bt_streams_full["1m"]
        seg_full = df1.loc[(df1.index >= res.start) & (df1.index <= res.end)]
        if seg_full.empty:
            ax_p.clear()
            ax_m.clear()
            ax_p.set_title("No chart data in range.")
            self.bt_price_canvas.draw_idle()
            return

        seg = seg_full
        # Keep interactive zoom/pan smooth.
        # Hard-cap the number of plotted points; too many points will freeze Tk on zoom/pan.
        max_points = 8000
        if len(seg) > max_points:
            step = int(len(seg) // max_points) + 1
            seg = seg.iloc[::step]
        ax_p.clear()
        ax_m.clear()

        x = seg.index.to_pydatetime()
        y = seg["price"].astype(float).values

        ax_p.plot(x, y, linewidth=1.0, color="#222222", alpha=0.95, label="Price")

        # MACD panel (use the same implementation as the backtest)
        try:
            if "macd" in seg.columns and "signal" in seg.columns:
                macd_line = seg["macd"].astype(float).values
                sig_line = seg["signal"].astype(float).values
                hist = macd_line - sig_line
            else:
                impl = (self.macd_impl_var.get() or "TRADINGVIEW").upper()
                macd_line, sig_line, hist = macd_series(y, impl)

            ax_m.plot(x, macd_line, linewidth=1.0, label="MACD")
            ax_m.plot(x, sig_line, linewidth=1.0, label="Signal")
            use_bars = bool(getattr(self, "bt_hist_bars_var", tk.BooleanVar(value=False)).get())
            # Bars are visually nice but can be very slow with thousands of points (Tk freezes on zoom/pan).
            # Default to a lightweight line representation unless the user explicitly enables bars.
            if use_bars and len(x) <= 8000:
                try:
                    ax_m.bar(x, hist, width=0.0008, alpha=0.22, align="center")
                except Exception:
                    pass
            else:
                try:
                    ax_m.plot(x, hist, linewidth=0.9, alpha=0.35, label="Hist")
                except Exception:
                    pass
            ax_m.axhline(0.0, linewidth=0.8, alpha=0.5)
        except Exception:
            pass

        # reset mappings
        self.bt_line_to_trade_id = {}
        self.bt_trade_id_to_line = {}
        self.bt_entry_ids = []
        self.bt_exit_ids = []

        ent_x, ent_y, ent_c = [], [], []
        ex_x, ex_y, ex_c = [], [], []

        show_vlines = bool(getattr(self, "bt_show_vlines_var", tk.BooleanVar(value=False)).get())

        # Trade overlays (fast path)
        trade_count = len(res.trades)

        # Always use a single LineCollection for trade segments.
        # Creating one Line2D per trade is what typically freezes Tk on zoom/pan.
        self.bt_trade_line_collection = None
        self.bt_trade_ids_for_collection = []
        self.bt_trade_highlight_line = None

        ent_x, ent_y, ent_c = [], [], []
        ex_x, ex_y, ex_c = [], [], []

        show_vlines = bool(getattr(self, "bt_show_vlines_var", tk.BooleanVar(value=False)).get())
        max_markers = 3000
        marker_step = (int(trade_count // max_markers) + 1) if trade_count > max_markers else 1

        segments = []
        seg_colors = []

        for k, t in enumerate(res.trades):
            col = C_GREEN if t.net_pnl >= 0 else C_RED

            # Trade segment (always)
            try:
                x1 = mdates.date2num(t.entry_time.to_pydatetime())
                x2 = mdates.date2num(t.exit_time.to_pydatetime())
                segments.append([(x1, t.entry_price), (x2, t.exit_price)])
                seg_colors.append(col)
                self.bt_trade_ids_for_collection.append(t.trade_id)
            except Exception:
                pass

            # Markers (downsample if huge)
            if marker_step == 1 or (k % marker_step) == 0:
                try:
                    ent_x.append(t.entry_time.to_pydatetime())
                    ent_y.append(t.entry_price)
                    ent_c.append(col)
                    self.bt_entry_ids.append(t.trade_id)

                    ex_x.append(t.exit_time.to_pydatetime())
                    ex_y.append(t.exit_price)
                    ex_c.append(col)
                    self.bt_exit_ids.append(t.trade_id)
                except Exception:
                    pass

            # Vertical lines are very expensive; only draw when trade count is modest.
            if show_vlines and trade_count <= 250:
                try:
                    ax_p.axvline(t.entry_time.to_pydatetime(), linewidth=0.8, alpha=0.18)
                    ax_p.axvline(t.exit_time.to_pydatetime(), linewidth=0.8, alpha=0.18)
                    ax_m.axvline(t.entry_time.to_pydatetime(), linewidth=0.8, alpha=0.10)
                    ax_m.axvline(t.exit_time.to_pydatetime(), linewidth=0.8, alpha=0.10)
                except Exception:
                    pass

        if segments:
            try:
                from matplotlib.collections import LineCollection
                lc = LineCollection(segments, colors=seg_colors, linewidths=1.6, alpha=0.85, zorder=4)
                try:
                    lc.set_picker(5)
                except Exception:
                    pass
                try:
                    lc.set_rasterized(True)
                except Exception:
                    pass
                ax_p.add_collection(lc)
                self.bt_trade_line_collection = lc
            except Exception:
                self.bt_trade_line_collection = None

        if ent_x:
            self.bt_entry_scatter = ax_p.scatter(ent_x, ent_y, c=ent_c, marker="^", s=55, picker=6, zorder=6, label="Entry")
            try:
                self.bt_entry_scatter.set_rasterized(True)
            except Exception:
                pass
        else:
            self.bt_entry_scatter = None

        if ex_x:
            self.bt_exit_scatter = ax_p.scatter(ex_x, ex_y, c=ex_c, marker="v", s=55, picker=6, zorder=6, label="Exit")
            try:
                self.bt_exit_scatter.set_rasterized(True)
            except Exception:
                pass
        else:
            self.bt_exit_scatter = None

        ax_p.grid(True, alpha=0.22)
        ax_m.grid(True, alpha=0.18)

        ax_p.set_ylabel("Price")
        ax_m.set_ylabel("MACD")
        ax_m.set_xlabel("Time (UTC)")

        ax_p.set_title(f"{res.summary.get('symbol','')} — Backtest (1m) | Trades: {len(res.trades)}")

        locator = mdates.AutoDateLocator(minticks=5, maxticks=10)
        try:
            fmt = mdates.ConciseDateFormatter(locator)
            ax_m.xaxis.set_major_locator(locator)
            ax_m.xaxis.set_major_formatter(fmt)
        except Exception:
            ax_m.xaxis.set_major_locator(locator)
            ax_m.xaxis.set_major_formatter(mdates.DateFormatter("%m-%d\n%H:%M"))

        ax_p.tick_params(labelbottom=False)

        try:
            _safe_axis_legend(ax_p, loc="upper left")
        except Exception:
            pass
        try:
            _safe_axis_legend(ax_m, loc="upper left")
        except Exception:
            pass

        try:
            self.bt_chart_full_xlim = ax_p.get_xlim()
        except Exception:
            self.bt_chart_full_xlim = None

        if self.bt_selected_trade_id is not None:
            self._bt_select_trade_by_id(self.bt_selected_trade_id, from_chart=False, open_details=False, auto_zoom=False)

        try:
            self.bt_price_canvas.draw_idle()
        except Exception:
            pass



    def _bt_draw_equity(self):
        """Draw mark-to-market equity curve."""
        try:
            self._ensure_bt_equity_chart_built()
        except Exception:
            pass
        if not hasattr(self, "bt_equity_ax"):
            return
        ax = self.bt_equity_ax
        ax.clear()

        if not self.bt_result:
            ax.set_title("No backtest results yet.")
            try:
                self.bt_equity_canvas.draw_idle()
            except Exception:
                pass
            return

        res = self.bt_result
        eq = res.equity_curve
        if eq is None or eq.empty:
            ax.set_title("No equity curve available.")
            try:
                self.bt_equity_canvas.draw_idle()
            except Exception:
                pass
            return

        seg = eq.loc[(eq.index >= res.start) & (eq.index <= res.end)]
        if seg.empty:
            seg = eq

        if len(seg) > 30000:
            step = int(len(seg) // 30000) + 1
            seg = seg.iloc[::step]

        x = seg.index.to_pydatetime()
        y = seg.get("equity_mark", seg.iloc[:, 0]).astype(float).values

        ax.plot(x, y, linewidth=1.1, label="Equity (mark-to-market)")
        ax.grid(True, alpha=0.22)
        ax.set_title(f"{res.summary.get('symbol','')} — Equity Curve | Ending: {res.summary.get('ending_balance', 0):.2f}")
        ax.set_ylabel("USDT")
        ax.set_xlabel("Time (UTC)")

        locator = mdates.AutoDateLocator(minticks=5, maxticks=10)
        try:
            fmt = mdates.ConciseDateFormatter(locator)
            ax.xaxis.set_major_locator(locator)
            ax.xaxis.set_major_formatter(fmt)
        except Exception:
            ax.xaxis.set_major_locator(locator)
            ax.xaxis.set_major_formatter(mdates.DateFormatter("%m-%d\n%H:%M"))

        try:
            _safe_axis_legend(ax, loc="upper left")
        except Exception:
            pass

        try:
            self.bt_equity_canvas.draw_idle()
        except Exception:
            pass



    def _bt_open_chart_popup(self):
        """Open a separate (larger) window for the Price + Trades + MACD chart.

        This helps inspection and also avoids the main notebook needing constant resizing/zooming.
        """
        try:
            if getattr(self, "_bt_popup_win", None) is not None and self._bt_popup_win.winfo_exists():
                try:
                    self._bt_popup_win.lift()
                except Exception:
                    pass
                return
        except Exception:
            pass

        try:
            win = tk.Toplevel(self)
            win.title("Backtest Chart — Pop-out")
            win.geometry("1500x900")
            win.minsize(1100, 650)
        except Exception:
            return

        outer = ttk.Frame(win)
        outer.pack(side="top", fill="both", expand=True)

        ctl = ttk.Frame(outer, padding=(10, 8))
        ctl.pack(side="top", fill="x")
        ttk.Button(ctl, text="Reset view", command=self._bt_reset_view).pack(side="left")
        ttk.Label(ctl, text="Tip: Use toolbar (Home/Pan/Zoom) or mouse wheel.", foreground="#666").pack(side="left", padx=12)
        ttk.Button(ctl, text="Close", command=self._bt_close_chart_popup).pack(side="right")

        fig = plt.Figure(figsize=(14.8, 8.6), dpi=100)
        gs = fig.add_gridspec(nrows=2, ncols=1, height_ratios=[3.2, 1.4], hspace=0.06)
        ax_p = fig.add_subplot(gs[0, 0])
        ax_m = fig.add_subplot(gs[1, 0], sharex=ax_p)

        canvas = FigureCanvasTkAgg(fig, master=outer)
        canvas.draw()
        canvas.get_tk_widget().pack(side="top", fill="both", expand=True)

        toolbar = NavigationToolbar2Tk(canvas, outer)
        toolbar.update()
        toolbar.pack(side="bottom", fill="x")

        # Store popup references
        self._bt_popup_win = win
        self._bt_popup_fig = fig
        self._bt_popup_price_ax = ax_p
        self._bt_popup_macd_ax = ax_m
        self._bt_popup_canvas = canvas
        self._bt_popup_toolbar = toolbar

        # Popup chart mappings (separate from the main chart)
        self._bt_popup_entry_scatter = None
        self._bt_popup_exit_scatter = None
        self._bt_popup_entry_ids = []
        self._bt_popup_exit_ids = []

        # Trade overlay objects (popup-only; important to reset on every redraw to avoid growth/freezes).
        self._bt_popup_trade_line_collection = None
        self._bt_popup_trade_ids_for_collection = []
        self._bt_popup_highlight_line = None
        self._bt_popup_line_to_trade_id = {}
        self._bt_popup_trade_id_to_line = {}
        self._bt_popup_chart_full_xlim = None


        # Scroll + pick support in popup too
        canvas.mpl_connect("pick_event", self._bt_on_popup_pick)
        canvas.mpl_connect("scroll_event", self._bt_on_popup_scroll)

        try:
            win.protocol("WM_DELETE_WINDOW", self._bt_close_chart_popup)
        except Exception:
            pass

        # Draw immediately
        try:
            self._bt_draw_chart_popup()
        except Exception:
            pass

    def _bt_close_chart_popup(self):
        """Close the pop-out chart window (if open)."""
        try:
            win = getattr(self, "_bt_popup_win", None)
            if win is not None and win.winfo_exists():
                win.destroy()
        except Exception:
            pass
        self._bt_popup_win = None
        self._bt_popup_fig = None
        self._bt_popup_price_ax = None
        self._bt_popup_macd_ax = None
        self._bt_popup_canvas = None
        self._bt_popup_toolbar = None

        self._bt_popup_entry_scatter = None
        self._bt_popup_exit_scatter = None
        self._bt_popup_entry_ids = []
        self._bt_popup_exit_ids = []
        self._bt_popup_line_to_trade_id = {}
        self._bt_popup_trade_id_to_line = {}
        self._bt_popup_chart_full_xlim = None


    def _bt_on_popup_scroll(self, event):
        """Mouse-wheel zoom on the pop-out chart."""
        try:
            if event is None:
                return
            ax_p = getattr(self, "_bt_popup_price_ax", None)
            ax_m = getattr(self, "_bt_popup_macd_ax", None)
            canvas = getattr(self, "_bt_popup_canvas", None)
            if ax_p is None or ax_m is None or canvas is None:
                return
            if event.inaxes not in (ax_p, ax_m):
                return
            if event.xdata is None:
                return

            cur_xlim = ax_p.get_xlim()
            if cur_xlim[0] == cur_xlim[1]:
                return

            base_scale = 1.20
            if getattr(event, "button", None) == "up":
                scale = 1.0 / base_scale
            else:
                scale = base_scale

            xdata = float(event.xdata)
            width = (cur_xlim[1] - cur_xlim[0]) * scale
            relx = (cur_xlim[1] - xdata) / (cur_xlim[1] - cur_xlim[0])
            new_xlim = (xdata - width * (1 - relx), xdata + width * relx)

            ax_p.set_xlim(new_xlim)
            ax_m.set_xlim(new_xlim)

            # Throttle redraws
            try:
                if getattr(self, "_bt_popup_scroll_job", None) is not None:
                    self.after_cancel(self._bt_popup_scroll_job)
            except Exception:
                self._bt_popup_scroll_job = None
            try:
                self._bt_popup_scroll_job = self.after(25, canvas.draw_idle)
            except Exception:
                try:
                    canvas.draw_idle()
                except Exception:
                    pass
        except Exception:
            pass

    def _bt_on_popup_pick(self, event):
        """Pick handler for the pop-out chart."""
        if not self.bt_result:
            return

        tid = None

        try:
            if getattr(self, "_bt_popup_entry_scatter", None) is not None and event.artist == self._bt_popup_entry_scatter:
                if hasattr(event, "ind") and len(event.ind):
                    idx = int(event.ind[0])
                    if 0 <= idx < len(getattr(self, "_bt_popup_entry_ids", [])):
                        tid = int(self._bt_popup_entry_ids[idx])
            elif getattr(self, "_bt_popup_exit_scatter", None) is not None and event.artist == self._bt_popup_exit_scatter:
                if hasattr(event, "ind") and len(event.ind):
                    idx = int(event.ind[0])
                    if 0 <= idx < len(getattr(self, "_bt_popup_exit_ids", [])):
                        tid = int(self._bt_popup_exit_ids[idx])
        except Exception:
            tid = None

        # trade lines (LineCollection fast path)
        if tid is None:
            try:
                lc = getattr(self, "_bt_popup_trade_line_collection", None)
                ids = getattr(self, "_bt_popup_trade_ids_for_collection", [])
                if lc is not None and event.artist == lc and hasattr(event, "ind") and len(event.ind):
                    idx = int(event.ind[0])
                    if 0 <= idx < len(ids):
                        tid = int(ids[idx])
            except Exception:
                tid = None

        if tid is None:
            try:
                tid = getattr(self, "_bt_popup_line_to_trade_id", {}).get(event.artist)
            except Exception:
                tid = None

        if tid is None:
            return

        open_details = bool(getattr(getattr(event, "mouseevent", None), "dblclick", False))
        self._bt_select_trade_by_id(tid, from_chart=True, open_details=open_details)

    def _bt_draw_chart_popup(self):
        """Draw chart into pop-out window (if open). Uses the same mode as the main chart."""
        try:
            win = getattr(self, "_bt_popup_win", None)
            if win is None or (hasattr(win, "winfo_exists") and not win.winfo_exists()):
                return
        except Exception:
            return

        try:
            mode = (self.bt_chart_mode_var.get() if hasattr(self, "bt_chart_mode_var") else "CANDLES")
        except Exception:
            mode = "CANDLES"
        mode_u = str(mode).strip().upper()

        if mode_u.startswith("LINE"):
            try:
                self._bt_draw_chart_popup_line_legacy()
            except Exception:
                pass
        else:
            try:
                self._bt_draw_chart_candles(popup=True)
            except Exception:
                pass
    def _bt_draw_chart_popup_line_legacy(self):
        """Legacy full-range line chart for the pop-out window."""
        try:
            win = getattr(self, "_bt_popup_win", None)
            if win is None or (hasattr(win, "winfo_exists") and not win.winfo_exists()):
                return
        except Exception:
            return

        ax_p = getattr(self, "_bt_popup_price_ax", None)
        ax_m = getattr(self, "_bt_popup_macd_ax", None)
        canvas = getattr(self, "_bt_popup_canvas", None)
        if ax_p is None or ax_m is None or canvas is None:
            return

        # Replicate main chart drawing logic, but store mappings on popup attributes.
        if not self.bt_result or not self.bt_streams_full or "1m" not in self.bt_streams_full:
            try:
                ax_p.clear()
                ax_m.clear()
                ax_p.set_title("No backtest results yet.")
                ax_m.set_title("")
                canvas.draw_idle()
            except Exception:
                pass
            return

        res = self.bt_result
        df1 = self.bt_streams_full["1m"]
        seg_full = df1.loc[(df1.index >= res.start) & (df1.index <= res.end)]
        if seg_full.empty:
            ax_p.clear()
            ax_m.clear()
            ax_p.set_title("No chart data in range.")
            canvas.draw_idle()
            return

        seg = seg_full
        # Hard-cap plotted points for responsiveness.
        max_points = 8000
        if len(seg) > max_points:
            step = int(len(seg) // max_points) + 1
            seg = seg.iloc[::step]
        ax_p.clear()
        ax_m.clear()

        x = seg.index.to_pydatetime()
        y = seg["price"].astype(float).values

        ax_p.plot(x, y, linewidth=1.0, color="#222222", alpha=0.95, label="Price")

        # MACD panel (use the same implementation as the backtest)
        try:
            if "macd" in seg.columns and "signal" in seg.columns:
                macd_line = seg["macd"].astype(float).values
                sig_line = seg["signal"].astype(float).values
                hist = macd_line - sig_line
            else:
                impl = (self.macd_impl_var.get() or "TRADINGVIEW").upper()
                macd_line, sig_line, hist = macd_series(y, impl)

            ax_m.plot(x, macd_line, linewidth=1.0, label="MACD")
            ax_m.plot(x, sig_line, linewidth=1.0, label="Signal")

            use_bars = bool(getattr(self, "bt_hist_bars_var", tk.BooleanVar(value=False)).get())
            if use_bars and len(x) <= 8000:
                try:
                    ax_m.bar(x, hist, width=0.0008, alpha=0.22, align="center")
                except Exception:
                    pass
            else:
                try:
                    ax_m.plot(x, hist, linewidth=0.9, alpha=0.35, label="Hist")
                except Exception:
                    pass

            ax_m.axhline(0.0, linewidth=0.8, alpha=0.5)
        except Exception:
            pass

        # reset mappings
        self._bt_popup_line_to_trade_id = {}
        self._bt_popup_trade_id_to_line = {}
        self._bt_popup_entry_ids = []
        self._bt_popup_exit_ids = []

        ent_x, ent_y, ent_c = [], [], []
        ex_x, ex_y, ex_c = [], [], []

        show_vlines = bool(getattr(self, "bt_show_vlines_var", tk.BooleanVar(value=False)).get())

        # Trade overlays (fast path)
        trade_count = len(res.trades)

        # Always use a single LineCollection for trade segments.

        # Reset popup trade overlay containers (avoid growth on redraw)
        self._bt_popup_trade_line_collection = None
        self._bt_popup_trade_ids_for_collection = []
        self._bt_popup_highlight_line = None

        show_vlines = bool(getattr(self, "bt_show_vlines_var", tk.BooleanVar(value=False)).get())
        max_markers = 3000
        marker_step = (int(trade_count // max_markers) + 1) if trade_count > max_markers else 1

        segments = []
        seg_colors = []

        for k, t in enumerate(res.trades):
            col = C_GREEN if t.net_pnl >= 0 else C_RED

            # Trade segment (always)
            try:
                x1 = mdates.date2num(t.entry_time.to_pydatetime())
                x2 = mdates.date2num(t.exit_time.to_pydatetime())
                segments.append([(x1, t.entry_price), (x2, t.exit_price)])
                seg_colors.append(col)
                self._bt_popup_trade_ids_for_collection.append(t.trade_id)
            except Exception:
                pass

            # Markers (downsample if huge)
            if marker_step == 1 or (k % marker_step) == 0:
                try:
                    ent_x.append(t.entry_time.to_pydatetime())
                    ent_y.append(t.entry_price)
                    ent_c.append(col)
                    self._bt_popup_entry_ids.append(t.trade_id)

                    ex_x.append(t.exit_time.to_pydatetime())
                    ex_y.append(t.exit_price)
                    ex_c.append(col)
                    self._bt_popup_exit_ids.append(t.trade_id)
                except Exception:
                    pass

            # Vertical lines are very expensive; only draw when trade count is modest.
            if show_vlines and trade_count <= 250:
                try:
                    ax_p.axvline(t.entry_time.to_pydatetime(), linewidth=0.8, alpha=0.18)
                    ax_p.axvline(t.exit_time.to_pydatetime(), linewidth=0.8, alpha=0.18)
                    ax_m.axvline(t.entry_time.to_pydatetime(), linewidth=0.8, alpha=0.10)
                    ax_m.axvline(t.exit_time.to_pydatetime(), linewidth=0.8, alpha=0.10)
                except Exception:
                    pass

        if segments:
            try:
                from matplotlib.collections import LineCollection
                lc = LineCollection(segments, colors=seg_colors, linewidths=1.6, alpha=0.85, zorder=4)
                try:
                    lc.set_picker(5)
                except Exception:
                    pass
                try:
                    lc.set_rasterized(True)
                except Exception:
                    pass
                ax_p.add_collection(lc)
                self._bt_popup_trade_line_collection = lc
            except Exception:
                self._bt_popup_trade_line_collection = None

        if ent_x:
            self._bt_popup_entry_scatter = ax_p.scatter(ent_x, ent_y, c=ent_c, marker="^", s=55, picker=6, zorder=6, label="Entry")
            try:
                self._bt_popup_entry_scatter.set_rasterized(True)
            except Exception:
                pass
        else:
            self._bt_popup_entry_scatter = None

        if ex_x:
            self._bt_popup_exit_scatter = ax_p.scatter(ex_x, ex_y, c=ex_c, marker="v", s=55, picker=6, zorder=6, label="Exit")
            try:
                self._bt_popup_exit_scatter.set_rasterized(True)
            except Exception:
                pass
        else:
            self._bt_popup_exit_scatter = None

        ax_p.grid(True, alpha=0.22)
        ax_m.grid(True, alpha=0.18)

        ax_p.set_ylabel("Price")
        ax_m.set_ylabel("MACD")
        ax_m.set_xlabel("Time (UTC)")

        ax_p.set_title(f"{res.summary.get('symbol','')} — Backtest (1m) | Trades: {len(res.trades)}")

        locator = mdates.AutoDateLocator(minticks=5, maxticks=10)
        try:
            fmt = mdates.ConciseDateFormatter(locator)
            ax_m.xaxis.set_major_locator(locator)
            ax_m.xaxis.set_major_formatter(fmt)
        except Exception:
            ax_m.xaxis.set_major_locator(locator)
            ax_m.xaxis.set_major_formatter(mdates.DateFormatter("%m-%d\n%H:%M"))

        ax_p.tick_params(labelbottom=False)

        try:
            _safe_axis_legend(ax_p, loc="upper left")
        except Exception:
            pass
        try:
            _safe_axis_legend(ax_m, loc="upper left")
        except Exception:
            pass

        try:
            self._bt_popup_chart_full_xlim = ax_p.get_xlim()
        except Exception:
            self._bt_popup_chart_full_xlim = None


        # If a trade is selected, apply highlighting in the popup too.
        if getattr(self, "bt_selected_trade_id", None) is not None:
            try:
                tid = int(self.bt_selected_trade_id)
                if getattr(self, "_bt_popup_trade_line_collection", None) is not None:
                    try:
                        if getattr(self, "_bt_popup_highlight_line", None) is not None:
                            self._bt_popup_highlight_line.remove()
                    except Exception:
                        pass

                    tsel = None
                    try:
                        for _t in self.bt_result.trades:
                            if int(getattr(_t, "trade_id", -1)) == tid:
                                tsel = _t
                                break
                    except Exception:
                        tsel = None

                    if tsel is not None:
                        col = C_GREEN if tsel.net_pnl >= 0 else C_RED
                        ln, = ax_p.plot(
                            [tsel.entry_time.to_pydatetime(), tsel.exit_time.to_pydatetime()],
                            [tsel.entry_price, tsel.exit_price],
                            linewidth=3.4,
                            color=col,
                            alpha=1.0,
                            zorder=8
                        )
                        self._bt_popup_highlight_line = ln
                else:
                    for _tid, ln in list(getattr(self, "_bt_popup_trade_id_to_line", {}).items()):
                        if ln is None:
                            continue
                        if int(_tid) == tid:
                            ln.set_linewidth(3.4)
                            ln.set_alpha(1.0)
                            ln.set_zorder(7)
                        else:
                            ln.set_linewidth(1.7)
                            ln.set_alpha(0.80)
                            ln.set_zorder(4)
            except Exception:
                pass

        try:
            canvas.draw_idle()
        except Exception:
            pass


    def _bt_on_chart_scroll(self, event):
        """Mouse-wheel zoom on the price/MACD chart (makes it easier to inspect entries/exits)."""
        try:
            if event is None:
                return
            if not hasattr(self, "bt_price_ax") or not hasattr(self, "bt_macd_ax"):
                return
            if event.inaxes not in (self.bt_price_ax, self.bt_macd_ax):
                return
            if event.xdata is None:
                return

            ax = self.bt_price_ax
            cur_xlim = ax.get_xlim()
            if cur_xlim[0] == cur_xlim[1]:
                return

            base_scale = 1.20
            if getattr(event, "button", None) == "up":
                scale = 1.0 / base_scale
            else:
                scale = base_scale

            xdata = float(event.xdata)
            width = (cur_xlim[1] - cur_xlim[0]) * scale
            relx = (cur_xlim[1] - xdata) / (cur_xlim[1] - cur_xlim[0])

            new_xlim = (xdata - width * (1 - relx), xdata + width * relx)
            ax.set_xlim(new_xlim)
            self.bt_macd_ax.set_xlim(new_xlim)

            # Throttle redraws: wheel can fire many events; drawing a dense chart each time can freeze Tk.
            try:
                if getattr(self, "_bt_scroll_draw_job", None) is not None:
                    self.after_cancel(self._bt_scroll_draw_job)
            except Exception:
                self._bt_scroll_draw_job = None
            try:
                self._bt_scroll_draw_job = self.after(25, self.bt_price_canvas.draw_idle)
            except Exception:
                try:
                    self.bt_price_canvas.draw_idle()
                except Exception:
                    pass
        except Exception:
            pass

    def _bt_on_chart_pick(self, event):
        """Pick handler: click markers/lines to select trade; double-click to open details."""
        if not self.bt_result:
            return

        tid = None

        # scatter points (entries/exits)
        try:
            if self.bt_entry_scatter is not None and event.artist == self.bt_entry_scatter:
                if hasattr(event, "ind") and len(event.ind):
                    idx = int(event.ind[0])
                    if 0 <= idx < len(self.bt_entry_ids):
                        tid = int(self.bt_entry_ids[idx])
            elif self.bt_exit_scatter is not None and event.artist == self.bt_exit_scatter:
                if hasattr(event, "ind") and len(event.ind):
                    idx = int(event.ind[0])
                    if 0 <= idx < len(self.bt_exit_ids):
                        tid = int(self.bt_exit_ids[idx])
        except Exception:
            tid = None

        # trade lines (LineCollection fast path)
        if tid is None:
            try:
                lc = getattr(self, "bt_trade_line_collection", None)
                ids = getattr(self, "bt_trade_ids_for_collection", [])
                if lc is not None and event.artist == lc and hasattr(event, "ind") and len(event.ind):
                    idx = int(event.ind[0])
                    if 0 <= idx < len(ids):
                        tid = int(ids[idx])
            except Exception:
                tid = None

        # individual line segments
        if tid is None:
            try:
                tid = self.bt_line_to_trade_id.get(event.artist)
            except Exception:
                tid = None

        if tid is None:
            return

        open_details = bool(getattr(getattr(event, "mouseevent", None), "dblclick", False))
        self._bt_select_trade_by_id(tid, from_chart=True, open_details=open_details)

    def _bt_select_trade_by_id(self, trade_id: int, from_chart: bool = False, open_details: bool = False, auto_zoom: Optional[bool] = None):
        """Select a trade in the table and highlight it on the chart."""
        if not self.bt_result:
            return

        self.bt_selected_trade_id = int(trade_id)

        # Highlight in tree
        iid = str(trade_id)
        try:
            # Guard against triggering inspector opens from programmatic selection.
            self._bt_programmatic_trade_select = True
            self.bt_tree.selection_set(iid)
            self.bt_tree.focus(iid)
            self.bt_tree.see(iid)
            if from_chart:
                self.bt_tree.focus_set()
        except Exception:
            pass
        finally:
            try:
                self._bt_programmatic_trade_select = False
            except Exception:
                pass

        # Highlight in chart (selected trade)
        try:
            if getattr(self, "bt_trade_line_collection", None) is not None:
                # Collection mode: avoid looping over thousands of Line2D objects.
                try:
                    if getattr(self, "bt_trade_highlight_line", None) is not None:
                        self.bt_trade_highlight_line.remove()
                except Exception:
                    pass

                tsel = None
                try:
                    for _t in self.bt_result.trades:
                        if int(getattr(_t, "trade_id", -1)) == int(trade_id):
                            tsel = _t
                            break
                except Exception:
                    tsel = None

                if tsel is not None:
                    col = C_GREEN if tsel.net_pnl >= 0 else C_RED
                    ln, = self.bt_price_ax.plot(
                        [tsel.entry_time.to_pydatetime(), tsel.exit_time.to_pydatetime()],
                        [tsel.entry_price, tsel.exit_price],
                        linewidth=3.4,
                        color=col,
                        alpha=1.0,
                        zorder=8
                    )
                    self.bt_trade_highlight_line = ln

                self.bt_price_canvas.draw_idle()
            else:
                # Individual line mode
                for tid, ln in list(getattr(self, "bt_trade_id_to_line", {}).items()):
                    if ln is None:
                        continue
                    if int(tid) == int(trade_id):
                        ln.set_linewidth(3.4)
                        ln.set_alpha(1.0)
                        ln.set_zorder(7)
                    else:
                        ln.set_linewidth(1.7)
                        ln.set_alpha(0.80)
                        ln.set_zorder(4)
                self.bt_price_canvas.draw_idle()
        except Exception:
            pass

        # Also highlight in pop-out chart (if open)
        try:
            canvas = getattr(self, "_bt_popup_canvas", None)
            axp = getattr(self, "_bt_popup_price_ax", None)
            if canvas is not None and axp is not None:
                if getattr(self, "_bt_popup_trade_line_collection", None) is not None:
                    try:
                        if getattr(self, "_bt_popup_highlight_line", None) is not None:
                            self._bt_popup_highlight_line.remove()
                    except Exception:
                        pass

                    tsel = None
                    try:
                        for _t in self.bt_result.trades:
                            if int(getattr(_t, "trade_id", -1)) == int(trade_id):
                                tsel = _t
                                break
                    except Exception:
                        tsel = None

                    if tsel is not None:
                        col = C_GREEN if tsel.net_pnl >= 0 else C_RED
                        ln2, = axp.plot(
                            [tsel.entry_time.to_pydatetime(), tsel.exit_time.to_pydatetime()],
                            [tsel.entry_price, tsel.exit_price],
                            linewidth=3.4,
                            color=col,
                            alpha=1.0,
                            zorder=8
                        )
                        self._bt_popup_highlight_line = ln2

                    try:
                        canvas.draw_idle()
                    except Exception:
                        pass
                else:
                    for tid2, ln2 in list(getattr(self, "_bt_popup_trade_id_to_line", {}).items()):
                        if ln2 is None:
                            continue
                        if int(tid2) == int(trade_id):
                            ln2.set_linewidth(3.4)
                            ln2.set_alpha(1.0)
                            ln2.set_zorder(7)
                        else:
                            ln2.set_linewidth(1.7)
                            ln2.set_alpha(0.80)
                            ln2.set_zorder(4)
                    try:
                        canvas.draw_idle()
                    except Exception:
                        pass
        except Exception:
            pass

        # Auto zoom (unless user disabled)
        if auto_zoom is None:
            auto_zoom = bool(getattr(self, "bt_autofocus_var", tk.BooleanVar(value=True)).get())
        if auto_zoom:
            tr = next((t for t in self.bt_result.trades if t.trade_id == trade_id), None)
            if tr:
                self._bt_zoom_to_trade(tr)

        if open_details:
            tr = next((t for t in self.bt_result.trades if t.trade_id == trade_id), None)
            if tr:
                self._open_trade_detail(tr)
    def _on_trade_select(self, event=None):
        """Keep trade selection lightweight: remember the selected trade id and (optionally) open inspector.

        NOTE: Do NOT redraw / zoom the overview chart here. Redrawing large matplotlib canvases from a Treeview
        selection callback can freeze Tk on some systems.
        """
        if not self.bt_result:
            return
        # Ignore programmatic selection changes (e.g., selection set from chart-click).
        try:
            if bool(getattr(self, "_bt_programmatic_trade_select", False)):
                return
        except Exception:
            pass

        sel = self.bt_tree.selection()
        if not sel:
            return
        try:
            tid = int(sel[0])
        except Exception:
            return

        try:
            self.bt_selected_trade_id = tid
        except Exception:
            pass

        # Enable the "Inspect" button once we have a valid selection.
        try:
            if hasattr(self, "btn_inspect_trade") and self.btn_inspect_trade is not None:
                self.btn_inspect_trade.configure(state="normal")
        except Exception:
            pass

        # Optional auto-open lightweight Trade Inspector (debounced).
        try:
            if hasattr(self, "bt_auto_inspect_var") and bool(self.bt_auto_inspect_var.get()):
                self._bt_schedule_trade_inspector_open(tid)
        except Exception:
            pass

    def _bt_zoom_to_trade(self, trade: Trade, pad_minutes: int = 40):
        if not self.bt_streams_full or "1m" not in self.bt_streams_full:
            return
        if not hasattr(self, "bt_price_ax") or not hasattr(self, "bt_macd_ax"):
            return

        ax_p = self.bt_price_ax
        ax_m = self.bt_macd_ax

        dfp = self.bt_streams_full["1m"][["price"]]
        start = trade.entry_time - pd.Timedelta(minutes=pad_minutes)
        end = trade.exit_time + pd.Timedelta(minutes=pad_minutes)

        # Cap points to keep Tk responsive (tick backtests can be extremely dense)
        seg = slice_and_decimate(dfp, start, end, max_points=6000)
        if seg.empty:
            return

        xs = seg.index.to_pydatetime()
        ys = seg["price"].astype(float).values

        try:
            ax_p.set_xlim(xs[0], xs[-1])
            ax_m.set_xlim(xs[0], xs[-1])


            ymin = float(np.nanmin(ys))
            ymax = float(np.nanmax(ys))
            if np.isfinite(ymin) and np.isfinite(ymax) and ymax > ymin:
                pad = (ymax - ymin) * 0.06
                ax_p.set_ylim(ymin - pad, ymax + pad)

            self.bt_price_canvas.draw_idle()
        except Exception:
            pass
    def _bt_reset_view(self):
        """Reset charts to full backtest range."""
        try:
            self._bt_draw_chart()
        except Exception:
            pass
        try:
            self._bt_draw_equity()
        except Exception:
            pass
    def _bt_clear_results(self):
        # Close inspector to avoid stale references
        try:
            self._bt_close_trade_inspector()
        except Exception:
            pass

        self.bt_result = None
        self.bt_streams_full = None

        # clear table
        try:
            for iid in self.bt_tree.get_children():
                self.bt_tree.delete(iid)
        except Exception:
            pass

        # clear summary
        try:
            self.bt_summary_txt.configure(state="normal")
            self.bt_summary_txt.delete("1.0", "end")
            self.bt_summary_txt.configure(state="disabled")
        except Exception:
            pass

        # clear charts
        try:
            if hasattr(self, "bt_price_ax") and hasattr(self, "bt_macd_ax"):
                self.bt_price_ax.clear()
                self.bt_macd_ax.clear()
                self.bt_price_ax.set_title("No backtest results yet.")
                self.bt_price_canvas.draw_idle()
        except Exception:
            pass

        try:
            if hasattr(self, "bt_equity_ax"):
                self.bt_equity_ax.clear()
                self.bt_equity_ax.set_title("No backtest results yet.")
                self.bt_equity_canvas.draw_idle()
        except Exception:
            pass

        try:
            self.btn_export_csv.config(state="disabled")
            self.btn_clear_bt.config(state="disabled")
            try:
                self.btn_inspect_trade.config(state="disabled")
            except Exception:
                pass
            try:
                self.btn_save_report.config(state="disabled")
            except Exception:
                pass
            try:
                self.btn_generate_trade_detail.config(state="disabled")
            except Exception:
                pass
            try:
                self.btn_trade_audit.config(state="disabled")
            except Exception:
                pass
            try:
                self.btn_replay_backtest.config(state="disabled")
            except Exception:
                pass
        except Exception:
            pass
    def _bt_export_csv(self):
        if not self.bt_result:
            messagebox.showerror("Export", "No backtest results to export.")
            return
        df = _make_trade_df(self.bt_result.trades)
        if df.empty:
            messagebox.showerror("Export", "No trades to export.")
            return

        default_name = f"backtest_{self.bt_result.summary.get('symbol','SYMBOL')}_{self.bt_result.start.strftime('%Y%m%d_%H%M')}_{self.bt_result.end.strftime('%Y%m%d_%H%M')}.csv"
        path = filedialog.asksaveasfilename(
            title="Export trades CSV",
            defaultextension=".csv",
            initialfile=default_name,
            filetypes=[("CSV", "*.csv"), ("All files", "*.*")]
        )
        if not path:
            return
        try:
            df.to_csv(path, index=False)
            messagebox.showinfo("Export", f"Saved:\n{path}")
        except Exception as e:
            messagebox.showerror("Export failed", str(e))


    # -------------------------
    # Backtest report (save/load)
    # -------------------------
    def _bt_report_dir(self) -> str:
        """Default directory for saved backtest reports."""
        try:
            base = os.path.dirname(os.path.abspath(__file__))
        except Exception:
            base = os.getcwd()
        out = os.path.join(base, "backtest_reports")
        try:
            os.makedirs(out, exist_ok=True)
        except Exception:
            pass
        return out

    @staticmethod
    def _bt_json_safe_report(v: Any) -> Any:
        """Make objects JSON-serializable (timestamps, numpy scalars, NaN/inf)."""
        try:
            if isinstance(v, pd.Timestamp):
                return pd.to_datetime(v, utc=True).isoformat()
        except Exception:
            pass
        try:
            if isinstance(v, (np.generic,)):
                return v.item()
        except Exception:
            pass
        if isinstance(v, float):
            try:
                if not np.isfinite(v):
                    return None
            except Exception:
                return None
        return v

    def _bt_result_to_report_dict(self, res: BacktestResult) -> Dict[str, Any]:
        cfg = res.config
        # Equity curve
        eq = res.equity_curve.copy() if hasattr(res, "equity_curve") and res.equity_curve is not None else pd.DataFrame()
        eq_payload = {"index": [], "columns": [], "data": []}
        try:
            if not eq.empty:
                eq_payload["index"] = [pd.to_datetime(x, utc=True).isoformat() for x in eq.index]
                eq_payload["columns"] = list(eq.columns)
                data_rows = []
                for row in eq.to_numpy().tolist():
                    clean = []
                    for x in row:
                        try:
                            if x is None or (isinstance(x, float) and (not np.isfinite(x))):
                                clean.append(None)
                            elif pd.isna(x):
                                clean.append(None)
                            else:
                                clean.append(self._bt_json_safe_report(x))
                        except Exception:
                            clean.append(None)
                    data_rows.append(clean)
                eq_payload["data"] = data_rows
        except Exception:
            pass

        # Trades
        trades_payload = []
        for t in res.trades or []:
            try:
                trades_payload.append({
                    "trade_id": int(t.trade_id),
                    "side": str(t.side),
                    "entry_time": pd.to_datetime(t.entry_time, utc=True).isoformat(),
                    "entry_price": float(t.entry_price),
                    "exit_time": pd.to_datetime(t.exit_time, utc=True).isoformat(),
                    "exit_price": float(t.exit_price),
                    "qty": float(t.qty),
                    "entry_notional": float(t.entry_notional),
                    "exit_notional": float(t.exit_notional),
                    "gross_pnl": float(t.gross_pnl),
                    "fees": float(t.fees),
                    "fee_entry": float(getattr(t, "fee_entry", 0.0) or 0.0),
                    "fee_exit": float(getattr(t, "fee_exit", 0.0) or 0.0),
                    "net_pnl": float(t.net_pnl),
                    "return_pct": float(t.return_pct),
                    "duration_min": int(t.duration_min),
                    "mfe": float(t.mfe),
                    "mae": float(t.mae),
                    "mfe_pct": float(t.mfe_pct),
                    "mae_pct": float(t.mae_pct),
                    "entry_reason": str(t.entry_reason),
                    "exit_reason": str(t.exit_reason),
                    "entry_rule_trace": str(getattr(t, "entry_rule_trace", "") or ""),
                    "exit_rule_trace": str(getattr(t, "exit_rule_trace", "") or ""),
                    "signal_time": (pd.to_datetime(getattr(t, "signal_time", None), utc=True).isoformat() if getattr(t, "signal_time", None) is not None else None),
                    "order_created_time": (pd.to_datetime(getattr(t, "order_created_time", None), utc=True).isoformat() if getattr(t, "order_created_time", None) is not None else None),
                    "entry_fill_time": (pd.to_datetime(getattr(t, "entry_fill_time", None), utc=True).isoformat() if getattr(t, "entry_fill_time", None) is not None else None),
                    "exit_signal_time": (pd.to_datetime(getattr(t, "exit_signal_time", None), utc=True).isoformat() if getattr(t, "exit_signal_time", None) is not None else None),
                    "exit_order_created_time": (pd.to_datetime(getattr(t, "exit_order_created_time", None), utc=True).isoformat() if getattr(t, "exit_order_created_time", None) is not None else None),
                    "exit_fill_time": (pd.to_datetime(getattr(t, "exit_fill_time", None), utc=True).isoformat() if getattr(t, "exit_fill_time", None) is not None else None),
                    "engine_type": str(getattr(t, "engine_type", "") or ""),
                    "fill_source": str(getattr(t, "fill_source", "") or ""),
                    "ambiguous_exit": bool(getattr(t, "ambiguous_exit", False)),
                    "stop_loss_price": (float(getattr(t, "stop_loss_price", 0.0)) if getattr(t, "stop_loss_price", None) is not None else None),
                    "take_profit_price": (float(getattr(t, "take_profit_price", 0.0)) if getattr(t, "take_profit_price", None) is not None else None),
                    "entry_row_index": (int(getattr(t, "entry_row_index")) if getattr(t, "entry_row_index", None) is not None else None),
                    "exit_row_index": (int(getattr(t, "exit_row_index")) if getattr(t, "exit_row_index", None) is not None else None),
                    "entry_snapshot": t.entry_snapshot if isinstance(t.entry_snapshot, dict) else {},
                    "exit_snapshot": t.exit_snapshot if isinstance(t.exit_snapshot, dict) else {},
                    "entry_barsago": getattr(t, "entry_barsago", {}) if isinstance(getattr(t, "entry_barsago", {}), dict) else {},
                    "exit_barsago": getattr(t, "exit_barsago", {}) if isinstance(getattr(t, "exit_barsago", {}), dict) else {},
                })
            except Exception:
                continue

        # Summary (sanitize for JSON)
        summary = {}
        try:
            for k, v in (res.summary or {}).items():
                summary[str(k)] = self._bt_json_safe_report(v)
        except Exception:
            pass

        entry_filters_payload = {}
        try:
            if isinstance((res.summary or {}).get("entry_filter"), dict):
                entry_filters_payload = dict((res.summary or {}).get("entry_filter"))
            else:
                entry_filters_payload = {
                    "Long Entry": self._entry_filter_for_tab("Long Entry").to_dict(),
                    "Short Entry": self._entry_filter_for_tab("Short Entry").to_dict(),
                }
        except Exception:
            entry_filters_payload = {}

        return {
            "config": {
                "initial_balance": float(cfg.initial_balance),
                "order_notional_usdt": float(cfg.order_notional_usdt),
                "fee_rate": float(cfg.fee_rate),
                "slippage_bps": float(cfg.slippage_bps),
                "stop_loss_pct": float(getattr(cfg, "stop_loss_pct", 0.0) or 0.0),
                "take_profit_pct": float(getattr(cfg, "take_profit_pct", 0.0) or 0.0),
                "stop_loss_mode": str(getattr(cfg, "stop_loss_mode", "PERCENT") or "PERCENT"),
                "stop_loss_value": float(getattr(cfg, "stop_loss_value", getattr(cfg, "stop_loss_pct", 0.0)) or 0.0),
                "stop_loss_timeframe": str(getattr(cfg, "stop_loss_timeframe", "1m") or "1m"),
                "stop_loss_field": str(getattr(cfg, "stop_loss_field", "") or ""),
                "stop_loss_offset_pct": float(getattr(cfg, "stop_loss_offset_pct", 0.0) or 0.0),
                "take_profit_mode": str(getattr(cfg, "take_profit_mode", "PERCENT") or "PERCENT"),
                "take_profit_value": float(getattr(cfg, "take_profit_value", getattr(cfg, "take_profit_pct", 0.0)) or 0.0),
                "take_profit_timeframe": str(getattr(cfg, "take_profit_timeframe", "1m") or "1m"),
                "take_profit_field": str(getattr(cfg, "take_profit_field", "") or ""),
                "take_profit_offset_pct": float(getattr(cfg, "take_profit_offset_pct", 0.0) or 0.0),
                "allow_reverse": bool(cfg.allow_reverse),
                "skip_if_both_entries": bool(cfg.skip_if_both_entries),
                "tick_auto_download": bool(getattr(cfg, "tick_auto_download", False)),
                "tick_cache_partition": str(getattr(cfg, "tick_cache_partition", "daily")),
            },
            "start": pd.to_datetime(res.start, utc=True).isoformat(),
            "end": pd.to_datetime(res.end, utc=True).isoformat(),
            "summary": summary,
            "entry_filters": entry_filters_payload,
            "trades": trades_payload,
            "equity_curve": eq_payload,
        }

    def _bt_result_from_report_dict(self, d: Dict[str, Any]) -> BacktestResult:
        cfgd = d.get("config", {}) if isinstance(d, dict) else {}
        cfg = BacktestConfig(
            initial_balance=float(cfgd.get("initial_balance", 1000.0)),
            order_notional_usdt=float(cfgd.get("order_notional_usdt", 100.0)),
            fee_rate=float(cfgd.get("fee_rate", 0.0004)),
            slippage_bps=float(cfgd.get("slippage_bps", 0.0)),
            stop_loss_pct=float(cfgd.get("stop_loss_pct", 0.0)),
            take_profit_pct=float(cfgd.get("take_profit_pct", 0.0)),
            stop_loss_mode=str(cfgd.get("stop_loss_mode", "PERCENT") or "PERCENT"),
            stop_loss_value=float(cfgd.get("stop_loss_value", cfgd.get("stop_loss_pct", 0.0)) or 0.0),
            stop_loss_timeframe=str(cfgd.get("stop_loss_timeframe", "1m") or "1m"),
            stop_loss_field=str(cfgd.get("stop_loss_field", "") or ""),
            stop_loss_offset_pct=float(cfgd.get("stop_loss_offset_pct", 0.0) or 0.0),
            take_profit_mode=str(cfgd.get("take_profit_mode", "PERCENT") or "PERCENT"),
            take_profit_value=float(cfgd.get("take_profit_value", cfgd.get("take_profit_pct", 0.0)) or 0.0),
            take_profit_timeframe=str(cfgd.get("take_profit_timeframe", "1m") or "1m"),
            take_profit_field=str(cfgd.get("take_profit_field", "") or ""),
            take_profit_offset_pct=float(cfgd.get("take_profit_offset_pct", 0.0) or 0.0),
            allow_reverse=bool(cfgd.get("allow_reverse", True)),
            skip_if_both_entries=bool(cfgd.get("skip_if_both_entries", True)),
        )
        try:
            cfg.tick_auto_download = bool(cfgd.get("tick_auto_download", False))
        except Exception:
            pass
        try:
            cfg.tick_cache_partition = str(cfgd.get("tick_cache_partition", "daily"))
        except Exception:
            pass

        start = pd.to_datetime(d.get("start"), utc=True)
        end = pd.to_datetime(d.get("end"), utc=True)

        trades = []
        for td in d.get("trades", []) if isinstance(d.get("trades", []), list) else []:
            try:
                trades.append(Trade(
                    trade_id=int(td.get("trade_id", 0)),
                    side=str(td.get("side", "")),
                    entry_time=pd.to_datetime(td.get("entry_time"), utc=True),
                    entry_price=float(td.get("entry_price", 0.0)),
                    exit_time=pd.to_datetime(td.get("exit_time"), utc=True),
                    exit_price=float(td.get("exit_price", 0.0)),
                    qty=float(td.get("qty", 0.0)),
                    entry_notional=float(td.get("entry_notional", 0.0)),
                    exit_notional=float(td.get("exit_notional", 0.0)),
                    gross_pnl=float(td.get("gross_pnl", 0.0)),
                    fees=float(td.get("fees", 0.0)),
                    fee_entry=float(td.get("fee_entry", 0.0)),
                    fee_exit=float(td.get("fee_exit", 0.0)),
                    net_pnl=float(td.get("net_pnl", 0.0)),
                    return_pct=float(td.get("return_pct", 0.0)),
                    duration_min=int(td.get("duration_min", 0)),
                    mfe=float(td.get("mfe", 0.0)),
                    mae=float(td.get("mae", 0.0)),
                    mfe_pct=float(td.get("mfe_pct", 0.0)),
                    mae_pct=float(td.get("mae_pct", 0.0)),
                    entry_reason=str(td.get("entry_reason", "")),
                    exit_reason=str(td.get("exit_reason", "")),
                    entry_rule_trace=str(td.get("entry_rule_trace", "") or ""),
                    exit_rule_trace=str(td.get("exit_rule_trace", "") or ""),
                    entry_snapshot=td.get("entry_snapshot", {}) if isinstance(td.get("entry_snapshot", {}), dict) else {},
                    exit_snapshot=td.get("exit_snapshot", {}) if isinstance(td.get("exit_snapshot", {}), dict) else {},
                    entry_barsago=td.get("entry_barsago", {}) if isinstance(td.get("entry_barsago", {}), dict) else {},
                    exit_barsago=td.get("exit_barsago", {}) if isinstance(td.get("exit_barsago", {}), dict) else {},
                    signal_time=(pd.to_datetime(td.get("signal_time"), utc=True) if td.get("signal_time") else None),
                    order_created_time=(pd.to_datetime(td.get("order_created_time"), utc=True) if td.get("order_created_time") else None),
                    entry_fill_time=(pd.to_datetime(td.get("entry_fill_time"), utc=True) if td.get("entry_fill_time") else None),
                    exit_signal_time=(pd.to_datetime(td.get("exit_signal_time"), utc=True) if td.get("exit_signal_time") else None),
                    exit_order_created_time=(pd.to_datetime(td.get("exit_order_created_time"), utc=True) if td.get("exit_order_created_time") else None),
                    exit_fill_time=(pd.to_datetime(td.get("exit_fill_time"), utc=True) if td.get("exit_fill_time") else None),
                    engine_type=str(td.get("engine_type", "") or ""),
                    fill_source=str(td.get("fill_source", "") or ""),
                    ambiguous_exit=bool(td.get("ambiguous_exit", False)),
                    stop_loss_price=(float(td.get("stop_loss_price")) if td.get("stop_loss_price") is not None else None),
                    take_profit_price=(float(td.get("take_profit_price")) if td.get("take_profit_price") is not None else None),
                ))
            except Exception:
                continue

        eq_payload = d.get("equity_curve", {}) if isinstance(d.get("equity_curve", {}), dict) else {}
        eq = pd.DataFrame()
        try:
            idx = [pd.to_datetime(x, utc=True) for x in eq_payload.get("index", [])]
            cols = list(eq_payload.get("columns", []))
            data = eq_payload.get("data", [])
            if idx and cols and isinstance(data, list):
                eq = pd.DataFrame(data, columns=cols, index=pd.DatetimeIndex(idx, tz="UTC"))
        except Exception:
            eq = pd.DataFrame()

        summary = d.get("summary", {}) if isinstance(d.get("summary", {}), dict) else {}
        return BacktestResult(config=cfg, start=start, end=end, trades=trades, equity_curve=eq, summary=summary)

    def _bt_save_report(self):
        if not self.bt_result:
            messagebox.showerror("Save report", "No backtest result to save.")
            return

        sym = ""
        try:
            sym = str(self.bt_result.summary.get("symbol", "") or "")
        except Exception:
            sym = ""

        default_name = f"bt_report_{sym or 'SYMBOL'}_{pd.to_datetime(self.bt_result.start, utc=True).strftime('%Y%m%d_%H%M')}_{pd.to_datetime(self.bt_result.end, utc=True).strftime('%Y%m%d_%H%M')}.json.gz"

        path = filedialog.asksaveasfilename(
            title="Save backtest report",
            defaultextension=".json.gz",
            initialdir=self._bt_report_dir(),
            initialfile=default_name,
            filetypes=[("Gzipped JSON", "*.json.gz"), ("JSON", "*.json"), ("All files", "*.*")]
        )
        if not path:
            return

        meta = {
            "report_version": 1,
            "created_at_utc": pd.Timestamp.utcnow().tz_localize("UTC").isoformat(),
            "symbol": sym,
            "mode": str(getattr(self, "_bt_last_mode", "") or ""),
            "timeframes": list(getattr(self, "_bt_last_tfs", []) or []),
            "warmup_min": int(getattr(self, "_bt_last_warmup", 0) or 0),
            "cache_dir": str(getattr(self, "_bt_last_cache_dir", "data_cache") or "data_cache"),
            "price_source": str(getattr(self, "_bt_last_price_source", "LAST") or "LAST"),
            "macd_impl": str(getattr(self, "_bt_last_macd_impl", "TRADINGVIEW") or "TRADINGVIEW"),
            "adx_impl": str(getattr(self, "_bt_last_adx_impl", "TRADINGVIEW") or "TRADINGVIEW"),
        }

        payload = {"meta": meta, "result": self._bt_result_to_report_dict(self.bt_result)}

        try:
            if path.lower().endswith(".gz"):
                with gzip.open(path, "wt", encoding="utf-8") as f:
                    json.dump(payload, f, indent=2, ensure_ascii=False, default=self._bt_json_safe_report)
            else:
                with open(path, "w", encoding="utf-8") as f:
                    json.dump(payload, f, indent=2, ensure_ascii=False, default=self._bt_json_safe_report)
            messagebox.showinfo("Save report", f"Saved:\n{path}")
        except Exception as e:
            messagebox.showerror("Save report failed", str(e))

    def _bt_load_report(self):
        self._ensure_backtest_tab_built()
        path = filedialog.askopenfilename(
            title="Load backtest report",
            initialdir=self._bt_report_dir(),
            filetypes=[("Gzipped JSON", "*.json.gz"), ("JSON", "*.json"), ("All files", "*.*")]
        )
        if not path:
            return

        try:
            if path.lower().endswith(".gz"):
                with gzip.open(path, "rt", encoding="utf-8") as f:
                    payload = json.load(f)
            else:
                with open(path, "r", encoding="utf-8") as f:
                    payload = json.load(f)
        except Exception as e:
            messagebox.showerror("Load report failed", str(e))
            return

        if not isinstance(payload, dict) or "result" not in payload:
            messagebox.showerror("Load report", "Invalid report file (missing 'result').")
            return

        meta = payload.get("meta", {}) if isinstance(payload.get("meta", {}), dict) else {}
        try:
            self._bt_last_symbol = str(meta.get("symbol", "") or "")
            self._bt_last_mode = str(meta.get("mode", "") or "")
            self._bt_last_tfs = list(meta.get("timeframes", []) or [])
            self._bt_last_warmup = int(meta.get("warmup_min", 0) or 0)
            self._bt_last_cache_dir = str(meta.get("cache_dir", "data_cache") or "data_cache")
            self._bt_last_price_source = str(meta.get("price_source", "LAST") or "LAST")
            self._bt_last_macd_impl = str(meta.get("macd_impl", "TRADINGVIEW") or "TRADINGVIEW")
            self._bt_last_adx_impl = str(meta.get("adx_impl", "TRADINGVIEW") or "TRADINGVIEW")
        except Exception:
            pass

        try:
            res = self._bt_result_from_report_dict(payload.get("result", {}))
        except Exception as e:
            messagebox.showerror("Load report", f"Could not parse report:\n{e}")
            return

        # Render without streams (charts will show a note; Trade Inspector can fetch on demand).
        self._render_backtest(res, streams_full=None)
        try:
            self.status_var.set(f"Loaded report: {os.path.basename(path)}")
        except Exception:
            pass

    def _trade_row_index_for_time(self, ts: Any) -> Optional[int]:
        try:
            if not self.bt_streams_full or "1m" not in self.bt_streams_full:
                return None
            idx = self.bt_streams_full["1m"].index
            if idx is None or len(idx) == 0:
                return None
            t = pd.to_datetime(ts, utc=True)
            pos = int(idx.searchsorted(t))
            if pos >= len(idx):
                pos = len(idx) - 1
            if pos > 0:
                try:
                    prev_ts = idx[pos - 1]
                    cur_ts = idx[pos]
                    if abs((cur_ts - t).total_seconds()) >= abs((t - prev_ts).total_seconds()):
                        pos = pos - 1
                except Exception:
                    pass
            return int(max(0, min(len(idx) - 1, pos)))
        except Exception:
            return None

    def _materialize_trade_detail_payload(self, trade: Trade) -> bool:
        if trade is None:
            return False
        try:
            have_entry = bool(isinstance(getattr(trade, "entry_snapshot", None), dict) and trade.entry_snapshot)
            have_exit = bool(isinstance(getattr(trade, "exit_snapshot", None), dict) and trade.exit_snapshot)
            if have_entry and have_exit and str(getattr(trade, "entry_rule_trace", "") or "").strip() and str(getattr(trade, "exit_rule_trace", "") or "").strip():
                return True
        except Exception:
            pass
        if not self.bt_streams_full:
            return False
        entry_row = getattr(trade, "entry_row_index", None)
        exit_row = getattr(trade, "exit_row_index", None)
        if entry_row is None:
            entry_row = self._trade_row_index_for_time(getattr(trade, "entry_fill_time", None) or trade.entry_time)
        if exit_row is None:
            exit_row = self._trade_row_index_for_time(getattr(trade, "exit_signal_time", None) or trade.exit_time)
        entry_snapshot = {}
        exit_snapshot = {}
        try:
            for tf, dft in (self.bt_streams_full or {}).items():
                if dft is None or dft.empty:
                    continue
                if entry_row is not None and 0 <= int(entry_row) < len(dft):
                    try:
                        entry_snapshot[str(tf)] = snapshot_from_stream_row(dft.iloc[int(entry_row)], required_fields=None)
                    except Exception:
                        pass
                if exit_row is not None and 0 <= int(exit_row) < len(dft):
                    try:
                        exit_snapshot[str(tf)] = snapshot_from_stream_row(dft.iloc[int(exit_row)], required_fields=None)
                    except Exception:
                        pass
        except Exception:
            return False
        if entry_snapshot and (not isinstance(getattr(trade, "entry_snapshot", None), dict) or not trade.entry_snapshot):
            trade.entry_snapshot = entry_snapshot
        if exit_snapshot and (not isinstance(getattr(trade, "exit_snapshot", None), dict) or not trade.exit_snapshot):
            trade.exit_snapshot = exit_snapshot
        placeholder = "Not captured during fast backtest mode. Use Trade audit… or rerun with fast mode off if you need full per-rule traces."
        try:
            if not str(getattr(trade, "entry_rule_trace", "") or "").strip():
                trade.entry_rule_trace = placeholder
            if not str(getattr(trade, "exit_rule_trace", "") or "").strip():
                trade.exit_rule_trace = placeholder
        except Exception:
            pass
        return bool(entry_snapshot or exit_snapshot)

    def _bt_generate_selected_trade_detail(self):
        if not self.bt_result or not self.bt_result.trades:
            return
        tid = None
        try:
            sel = self.bt_tree.selection()
            if sel:
                tid = int(sel[0])
        except Exception:
            tid = None
        if tid is None:
            tid = getattr(self, "bt_selected_trade_id", None)
        if tid is None:
            return
        trade = next((t for t in self.bt_result.trades if int(getattr(t, "trade_id", -1)) == int(tid)), None)
        if trade is None:
            return
        ok = self._materialize_trade_detail_payload(trade)
        if ok:
            try:
                self.status_var.set(f"Trade #{trade.trade_id} details materialized on demand.")
            except Exception:
                pass
        else:
            messagebox.showinfo("Generate trade details", "Could not materialize detailed snapshots from the current backtest data.")

    def _bt_export_trade_audit(self):
        if not self.bt_result:
            return
        path = filedialog.asksaveasfilename(
            parent=self,
            title="Save trade audit report",
            defaultextension=".json",
            filetypes=[("JSON files", "*.json"), ("CSV files", "*.csv"), ("All files", "*.*")],
            initialfile="trade_audit_report.json",
        )
        if not path:
            return
        try:
            for tr in (self.bt_result.trades or []):
                self._materialize_trade_detail_payload(tr)
            market_th = self._market_state_thresholds_obj()
            if str(path).lower().endswith(".csv"):
                out = write_trade_audit_csv(path, self.bt_result)
            else:
                out = write_trade_audit_report(path, self.bt_result, market_thresholds=market_th, include_snapshots=True)
            self.status_var.set(f"Trade audit saved: {os.path.basename(str(out))}")
        except Exception as exc:
            messagebox.showerror("Trade audit", str(exc))

    def _on_trade_double_click(self, event=None):
        if not self.bt_result:
            return
        sel = self.bt_tree.selection()
        if not sel:
            return
        iid = sel[0]
        try:
            tid = int(iid)
        except Exception:
            return

        # If user double-clicks, open the full detail window only (cancel any pending inspector open).
        try:
            if getattr(self, "_bt_inspector_job", None) is not None:
                try:
                    self.after_cancel(self._bt_inspector_job)
                except Exception:
                    pass
                self._bt_inspector_job = None
        except Exception:
            pass
        trade = next((t for t in self.bt_result.trades if t.trade_id == tid), None)
        if not trade:
            return
        self._open_trade_detail(trade)

    def _open_trade_detail(self, trade: Trade):
        try:
            self._materialize_trade_detail_payload(trade)
        except Exception:
            pass
        win = tk.Toplevel(self)
        win.title(f"Trade #{trade.trade_id} — {trade.side}")
        win.geometry("1150x820")

        top = ttk.Frame(win, padding=10)
        top.pack(side="top", fill="x")

        def fmt_ts(ts: pd.Timestamp) -> str:
            try:
                return ts.strftime("%Y-%m-%d %H:%M:%S")
            except Exception:
                return str(ts)

        signal_time = pd.to_datetime(getattr(trade, "signal_time", None), utc=True) if getattr(trade, "signal_time", None) is not None else pd.NaT
        order_created_time = pd.to_datetime(getattr(trade, "order_created_time", None), utc=True) if getattr(trade, "order_created_time", None) is not None else pd.NaT
        entry_fill_time = pd.to_datetime(getattr(trade, "entry_fill_time", None), utc=True) if getattr(trade, "entry_fill_time", None) is not None else pd.NaT
        exit_signal_time = pd.to_datetime(getattr(trade, "exit_signal_time", None), utc=True) if getattr(trade, "exit_signal_time", None) is not None else pd.NaT
        exit_order_created_time = pd.to_datetime(getattr(trade, "exit_order_created_time", None), utc=True) if getattr(trade, "exit_order_created_time", None) is not None else pd.NaT
        exit_fill_time = pd.to_datetime(getattr(trade, "exit_fill_time", None), utc=True) if getattr(trade, "exit_fill_time", None) is not None else pd.NaT
        sig_to_ent = "n/a"
        try:
            if pd.notna(signal_time) and pd.notna(entry_fill_time):
                sig_to_ent = f"{float((entry_fill_time - signal_time).total_seconds()):.1f} s"
        except Exception:
            sig_to_ent = "n/a"
        lines = [
            f"Side: {trade.side}    Engine: {str(getattr(trade, 'engine_type', '') or '')}    Fill source: {str(getattr(trade, 'fill_source', '') or '')}",
            f"Entry signal time: {(fmt_ts(signal_time) if pd.notna(signal_time) else 'n/a')}    Entry order created: {(fmt_ts(order_created_time) if pd.notna(order_created_time) else 'n/a')}",
            f"Entry fill time: {(fmt_ts(entry_fill_time) if pd.notna(entry_fill_time) else fmt_ts(trade.entry_time))}",
            f"Exit signal time: {(fmt_ts(exit_signal_time) if pd.notna(exit_signal_time) else 'n/a')}    Exit order created: {(fmt_ts(exit_order_created_time) if pd.notna(exit_order_created_time) else 'n/a')}",
            f"Exit fill time: {(fmt_ts(exit_fill_time) if pd.notna(exit_fill_time) else fmt_ts(trade.exit_time))}",
            f"Signal→Entry: {sig_to_ent}    Ambiguous exit: {'YES' if bool(getattr(trade, 'ambiguous_exit', False)) else 'NO'}",
            f"Entry: {fmt_ts(trade.entry_time)}  @ {trade.entry_price:.6f}",
            f"Exit : {fmt_ts(trade.exit_time)}  @ {trade.exit_price:.6f}",
            f"SL: {('{:.6f}'.format(float(getattr(trade, 'stop_loss_price'))) if getattr(trade, 'stop_loss_price', None) is not None else 'n/a')}    TP: {('{:.6f}'.format(float(getattr(trade, 'take_profit_price'))) if getattr(trade, 'take_profit_price', None) is not None else 'n/a')}",
            f"Qty: {trade.qty:.6f}    Entry Notional: {trade.entry_notional:.2f}    Exit Notional: {trade.exit_notional:.2f}",
            f"Gross PnL: {trade.gross_pnl:.2f}    Fees: {trade.fees:.2f}    Net PnL: {trade.net_pnl:.2f}    Return: {trade.return_pct:.2f}%",
            f"Duration: {trade.duration_min} min    MFE: {trade.mfe:.2f} ({trade.mfe_pct:.2f}%)    MAE: {trade.mae:.2f} ({trade.mae_pct:.2f}%)",
            f"Entry reason: {trade.entry_reason}    Exit reason: {trade.exit_reason}",
            "",
            "ENTRY RULE TRACE:",
            str(getattr(trade, "entry_rule_trace", "") or "n/a"),
            "",
            "EXIT RULE TRACE:",
            str(getattr(trade, "exit_rule_trace", "") or "n/a"),
        ]
        txt = tk.Text(top, height=10, font=("Consolas", 10))
        txt.pack(side="top", fill="x")
        txt.insert("1.0", "\n".join(lines))
        txt.configure(state="disabled")

        nb = ttk.Notebook(win)
        nb.pack(side="top", fill="both", expand=True, padx=10, pady=10)

        tab_ind = ttk.Frame(nb)
        tab_chart = ttk.Frame(nb)
        nb.add(tab_ind, text="Indicators")
        nb.add(tab_chart, text="Chart (Price + MACD)")

        # Indicators table (entry vs exit snapshots)
        cols = ["TF", "ms (E)", "angle (E)", "sig_angle (E)", "ppo (E)", "flip_age (E)", "macd (E)", "signal (E)",
                "ms (X sig)", "angle (X sig)", "sig_angle (X sig)", "ppo (X sig)", "flip_age (X sig)", "macd (X sig)", "signal (X sig)"]
        tv = ttk.Treeview(tab_ind, columns=cols, show="headings", height=16)
        for c in cols:
            tv.heading(c, text=c)
            tv.column(c, width=90 if c != "TF" else 60, anchor="center" if c == "TF" else "e")
        vs = ttk.Scrollbar(tab_ind, orient="vertical", command=tv.yview)
        hs = ttk.Scrollbar(tab_ind, orient="horizontal", command=tv.xview)
        tv.configure(yscrollcommand=vs.set, xscrollcommand=hs.set)
        tv.grid(row=0, column=0, sticky="nsew")
        vs.grid(row=0, column=1, sticky="ns")
        hs.grid(row=1, column=0, sticky="ew")
        tab_ind.grid_rowconfigure(0, weight=1)
        tab_ind.grid_columnconfigure(0, weight=1)

        tfs = sorted(set(list(trade.entry_snapshot.keys()) + list(trade.exit_snapshot.keys())))
        for tf in tfs:
            e = trade.entry_snapshot.get(tf, {})
            x = trade.exit_snapshot.get(tf, {})
            row = [
                tf,
                str(e.get("ms")), str(e.get("angle")), str(e.get("sig_angle")), str(e.get("ppo")), str(e.get("flip_age")),
                "" if e.get("macd") is None else f"{float(e.get('macd')):.6f}",
                "" if e.get("signal") is None else f"{float(e.get('signal')):.6f}",
                str(x.get("ms")), str(x.get("angle")), str(x.get("sig_angle")), str(x.get("ppo")), str(x.get("flip_age")),
                "" if x.get("macd") is None else f"{float(x.get('macd')):.6f}",
                "" if x.get("signal") is None else f"{float(x.get('signal')):.6f}",
            ]
            tv.insert("", "end", values=row)

                # Chart tab: show 1m price + MACD around the trade (with padding)
        holder = ttk.Frame(tab_chart, padding=10)
        holder.pack(side="top", fill="both", expand=True)

        if not (self.bt_streams_full and "1m" in self.bt_streams_full):
            ttk.Label(holder, text="Chart unavailable (no backtest stream cached).").pack(padx=20, pady=20)
            return

        df1 = self.bt_streams_full["1m"]
        pad = pd.Timedelta(minutes=60)
        start = trade.entry_time - pad
        end = trade.exit_time + pad

        # Keep UI responsive: slice/decimate in a worker thread.
        loading = ttk.Label(holder, text="Loading chart…", foreground="#666")
        loading.pack(padx=20, pady=20)

        def _render_chart(seg: "pd.DataFrame"):
            try:
                loading.destroy()
            except Exception:
                pass

            if seg is None or seg.empty:
                ttk.Label(holder, text="No chart data in range.").pack(padx=20, pady=20)
                return

            fig = plt.Figure(figsize=(11.2, 6.8), dpi=100)
            gs = fig.add_gridspec(nrows=2, ncols=1, height_ratios=[3.2, 1.4], hspace=0.06)
            ax_p = fig.add_subplot(gs[0, 0])
            ax_m = fig.add_subplot(gs[1, 0], sharex=ax_p)

            try:
                x = seg.index.to_pydatetime()
            except Exception:
                x = []
            try:
                y = seg["price"].astype(float).values if "price" in seg.columns else seg["close"].astype(float).values
            except Exception:
                try:
                    y = seg["close"].astype(float).values
                except Exception:
                    y = []

            try:
                ax_p.plot(x, y, linewidth=1.1, label="Price")
            except Exception:
                pass

            # Entry/exit visuals
            try:
                ent_t = pd.to_datetime(trade.entry_time, utc=True).to_pydatetime()
                ex_t = pd.to_datetime(trade.exit_time, utc=True).to_pydatetime()
                ax_p.axvline(ent_t, linewidth=1.0, alpha=0.35)
                ax_p.axvline(ex_t, linewidth=1.0, alpha=0.35)
                ax_p.scatter([ent_t], [float(trade.entry_price)], marker="^", s=70, zorder=6, label="Entry")
                ax_p.scatter([ex_t], [float(trade.exit_price)], marker="v", s=70, zorder=6, label="Exit")
                try:
                    ax_p.plot([ent_t, ex_t], [float(trade.entry_price), float(trade.exit_price)], linewidth=2.0, alpha=0.70)
                except Exception:
                    pass
            except Exception:
                pass

            # MACD on the same displayed series (use the same implementation as backtest)
            try:
                if "macd" in seg.columns and "signal" in seg.columns:
                    macd_line = seg["macd"].astype(float).values
                    sig_line = seg["signal"].astype(float).values
                    hist = macd_line - sig_line
                else:
                    impl = (self.macd_impl_var.get() or "TRADINGVIEW").upper()
                    macd_line, sig_line, hist = macd_series(np.asarray(y, dtype=float), impl)

                ax_m.plot(x, macd_line, linewidth=1.0, label="MACD")
                ax_m.plot(x, sig_line, linewidth=1.0, label="Signal")
                try:
                    ax_m.bar(x, hist, width=0.0008, alpha=0.25, align="center")
                except Exception:
                    try:
                        ax_m.plot(x, hist, linewidth=0.9, alpha=0.35, label="Hist")
                    except Exception:
                        pass
                ax_m.axhline(0.0, linewidth=0.8, alpha=0.5)
            except Exception:
                pass

            ax_p.grid(True, alpha=0.22)
            ax_m.grid(True, alpha=0.18)
            ax_p.set_title(f"Trade #{trade.trade_id} — {trade.side} — Price + MACD (1m)")
            ax_p.set_ylabel("Price")
            ax_m.set_ylabel("MACD")
            ax_m.set_xlabel("Time (UTC)")
            ax_p.tick_params(labelbottom=False)

            locator = mdates.AutoDateLocator(minticks=5, maxticks=10)
            try:
                fmt = mdates.ConciseDateFormatter(locator)
                ax_m.xaxis.set_major_locator(locator)
                ax_m.xaxis.set_major_formatter(fmt)
            except Exception:
                ax_m.xaxis.set_major_locator(locator)
                ax_m.xaxis.set_major_formatter(mdates.DateFormatter("%m-%d\\n%H:%M"))

            try:
                _safe_axis_legend(ax_p, loc="upper left")
            except Exception:
                pass
            try:
                _safe_axis_legend(ax_m, loc="upper left")
            except Exception:
                pass

            canvas = FigureCanvasTkAgg(fig, master=holder)
            canvas.draw()
            canvas.get_tk_widget().pack(side="top", fill="both", expand=True)
            toolbar = NavigationToolbar2Tk(canvas, holder)
            toolbar.update()
            toolbar.pack(side="bottom", fill="x")

        def _worker():
            try:
                seg_local = slice_and_decimate(df1, start, end, max_points=12000)
            except Exception:
                seg_local = pd.DataFrame()
            try:
                self.after(0, lambda: _render_chart(seg_local))
            except Exception:
                _render_chart(seg_local)

        threading.Thread(target=_worker, daemon=True).start()

    # -------------------------
    # Trade Inspector (lightweight per-trade popup)
    # -------------------------
    def _bt_inspect_selected_trade(self):
        """Open the lightweight Trade Inspector for the selected trade (if any)."""
        if not self.bt_result or not self.bt_result.trades:
            return
        tid = None
        try:
            sel = self.bt_tree.selection()
            if sel:
                tid = int(sel[0])
        except Exception:
            tid = None
        if tid is None:
            tid = getattr(self, "bt_selected_trade_id", None)
        if tid is None:
            return
        tr = next((t for t in self.bt_result.trades if int(getattr(t, "trade_id", -1)) == int(tid)), None)
        if tr is None:
            return
        self._bt_open_trade_inspector(tr)

    def _bt_schedule_trade_inspector_open(self, trade_id: int):
        """Debounce inspector opens so dragging/keyboard selection doesn't spam windows."""
        try:
            if getattr(self, "_bt_inspector_job", None) is not None:
                try:
                    self.after_cancel(self._bt_inspector_job)
                except Exception:
                    pass
            self._bt_inspector_job = self.after(120, lambda: self._bt_open_trade_inspector_by_id(trade_id))
        except Exception:
            # best-effort fallback
            try:
                self._bt_open_trade_inspector_by_id(trade_id)
            except Exception:
                pass

    def _bt_open_trade_inspector_by_id(self, trade_id: int):
        if not self.bt_result:
            return
        tr = next((t for t in self.bt_result.trades if int(getattr(t, "trade_id", -1)) == int(trade_id)), None)
        if tr is None:
            return
        self._bt_open_trade_inspector(tr)

    def _bt_close_trade_inspector(self):
        try:
            win = getattr(self, "_bt_inspector_win", None)
            if win is not None and win.winfo_exists():
                win.destroy()
        except Exception:
            pass
        self._bt_inspector_win = None
        self._bt_inspector_canvas = None
        self._bt_inspector_toolbar = None
        self._bt_inspector_fig = None
        self._bt_inspector_ax_p = None
        self._bt_inspector_ax_m = None
        self._bt_inspector_ax_di = None
        self._bt_inspector_info = None
        self._bt_inspector_tf_combo = None
        self._bt_inspector_current_trade = None
        self._bt_inspector_current_tf = None
        self._bt_inspector_status_var = None




    def _bt_on_inspector_tf_changed(self):
        """When the MACD TF selection changes, redraw the current trade fragment (async)."""
        tr = getattr(self, "_bt_inspector_current_trade", None)
        if tr is None:
            return
        # Debounce rapid UI changes.
        try:
            job = getattr(self, "_bt_inspector_tf_job", None)
            if job is not None:
                try:
                    self.after_cancel(job)
                except Exception:
                    pass
        except Exception:
            pass
        try:
            self._bt_inspector_tf_job = self.after(80, lambda: self._bt_update_trade_inspector(tr))
        except Exception:
            try:
                self._bt_update_trade_inspector(tr)
            except Exception:
                pass

    def _bt_ensure_trade_inspector(self):
        """Create (or focus) the Trade Inspector window."""
        try:
            win = getattr(self, "_bt_inspector_win", None)
            if win is not None and win.winfo_exists():
                try:
                    win.deiconify()
                    win.lift()
                except Exception:
                    pass
                return win
        except Exception:
            pass

        win = tk.Toplevel(self)
        win.title("Trade Inspector — Price + MACD (fragment)")
        win.geometry("1200x860")
        try:
            win.minsize(980, 720)
        except Exception:
            pass

        try:
            win.protocol("WM_DELETE_WINDOW", self._bt_close_trade_inspector)
        except Exception:
            pass

        # Top info + controls
        top = ttk.Frame(win, padding=10)
        top.pack(side="top", fill="x")

        ctl = ttk.Frame(top)
        ctl.pack(side="top", fill="x")

        # Inspector controls (keep simple; defaults chosen for stability)
        if not hasattr(self, "bt_inspector_pad_var"):
            self.bt_inspector_pad_var = tk.IntVar(value=60)
        if not hasattr(self, "bt_inspector_maxpts_var"):
            self.bt_inspector_maxpts_var = tk.IntVar(value=12000)

        if not hasattr(self, "bt_inspector_macdtf_var"):
            self.bt_inspector_macdtf_var = tk.StringVar(value="1m")


        if not hasattr(self, "bt_inspector_show_barsago_var"):
            self.bt_inspector_show_barsago_var = tk.BooleanVar(value=True)

        ttk.Label(ctl, text="Padding (min):").pack(side="left")
        ttk.Spinbox(ctl, from_=0, to=720, increment=10, textvariable=self.bt_inspector_pad_var, width=6).pack(side="left", padx=(6, 14))
        ttk.Label(ctl, text="Max points:").pack(side="left")
        ttk.Spinbox(ctl, from_=2000, to=30000, increment=1000, textvariable=self.bt_inspector_maxpts_var, width=7).pack(side="left", padx=(6, 14))
        ttk.Checkbutton(ctl, text="BarsAgo marks", variable=self.bt_inspector_show_barsago_var, command=lambda: self._bt_on_inspector_tf_changed()).pack(side="left", padx=(0, 14))

        ttk.Label(ctl, text="MACD TF:").pack(side="left")
        tf_combo = ttk.Combobox(ctl, textvariable=self.bt_inspector_macdtf_var, state="readonly", width=6)
        tf_combo.pack(side="left", padx=(6, 14))
        try:
            tf_combo["values"] = ("1m",)
        except Exception:
            pass
        try:
            tf_combo.bind("<<ComboboxSelected>>", lambda _e: self._bt_on_inspector_tf_changed())
        except Exception:
            pass
        ttk.Button(ctl, text="Redraw", command=self._bt_inspect_selected_trade).pack(side="left", padx=(0, 10))
        ttk.Button(ctl, text="Close", command=self._bt_close_trade_inspector).pack(side="right")

        self._bt_inspector_status_var = tk.StringVar(value="Ready")
        ttk.Label(ctl, textvariable=self._bt_inspector_status_var, foreground="#666").pack(side="right", padx=(0, 12))

        info = tk.Text(top, height=6, font=("Consolas", 10))
        info.pack(side="top", fill="x", pady=(8, 0))
        info.configure(state="disabled")

        # Chart area
        holder = ttk.Frame(win, padding=(10, 6, 10, 10))
        holder.pack(side="top", fill="both", expand=True)

        fig = plt.Figure(figsize=(11.2, 6.8), dpi=100)
        gs = fig.add_gridspec(nrows=2, ncols=1, height_ratios=[3.2, 1.4], hspace=0.06)
        ax_p = fig.add_subplot(gs[0, 0])
        ax_m = fig.add_subplot(gs[1, 0], sharex=ax_p)

        canvas = FigureCanvasTkAgg(fig, master=holder)
        canvas.draw()
        canvas.get_tk_widget().pack(side="top", fill="both", expand=True)

        toolbar = NavigationToolbar2Tk(canvas, holder)
        toolbar.update()
        toolbar.pack(side="bottom", fill="x")

        # Store
        self._bt_inspector_win = win
        self._bt_inspector_fig = fig
        self._bt_inspector_ax_p = ax_p
        self._bt_inspector_ax_m = ax_m
        self._bt_inspector_ax_di = None
        self._bt_inspector_canvas = canvas
        self._bt_inspector_toolbar = toolbar
        self._bt_inspector_info = info
        self._bt_inspector_tf_combo = tf_combo

        # request-id for async fetch
        self._bt_inspector_req_id = 0

        return win

    def _bt_open_trade_inspector(self, trade: Trade):
        """Show a per-trade fragment chart in a reusable popup window."""
        self._bt_ensure_trade_inspector()

        # Remember current trade for TF switching.
        self._bt_inspector_current_trade = trade

        # Update available TFs in the inspector combobox (best-effort).
        try:
            combo = getattr(self, "_bt_inspector_tf_combo", None)
            if combo is not None:
                tfs = []
                try:
                    if self.bt_streams_full:
                        tfs = list(self.bt_streams_full.keys())
                except Exception:
                    tfs = []
                if not tfs:
                    try:
                        tfs = [t.strip() for t in str(self.tf_var.get()).split(",") if t.strip()]
                    except Exception:
                        tfs = []
                if "1m" not in tfs:
                    tfs = ["1m"] + tfs
                # sort by interval size
                def _k(tf: str) -> int:
                    try:
                        return int(interval_to_ms(tf))
                    except Exception:
                        return 10**18
                tfs = sorted(list(dict.fromkeys(tfs)), key=_k)
                try:
                    combo["values"] = tuple(tfs)
                except Exception:
                    pass
                # ensure selected value exists
                try:
                    cur = str(getattr(self, "bt_inspector_macdtf_var", tk.StringVar(value="1m")).get() or "1m")
                except Exception:
                    cur = "1m"
                if cur not in tfs:
                    cur = "1m" if "1m" in tfs else (tfs[0] if tfs else "1m")
                    try:
                        self.bt_inspector_macdtf_var.set(cur)
                    except Exception:
                        pass
        except Exception:
            pass

        try:
            win = getattr(self, "_bt_inspector_win", None)
            if win is not None and win.winfo_exists():
                win.title(f"Trade Inspector — #{trade.trade_id} — {trade.side}")
                try:
                    win.lift()
                except Exception:
                    pass
        except Exception:
            pass

        self._bt_update_trade_inspector(trade)

    def _bt_update_trade_inspector(self, trade: Trade):
        ax_p = getattr(self, "_bt_inspector_ax_p", None)
        ax_m = getattr(self, "_bt_inspector_ax_m", None)
        canvas = getattr(self, "_bt_inspector_canvas", None)
        info = getattr(self, "_bt_inspector_info", None)
        if ax_p is None or ax_m is None or canvas is None or info is None:
            return


        # Selected MACD timeframe for the inspector (defaults to 1m)
        try:
            tf_sel = str(getattr(self, "bt_inspector_macdtf_var", tk.StringVar(value="1m")).get() or "1m").strip()
        except Exception:
            tf_sel = "1m"
        if not tf_sel:
            tf_sel = "1m"
        self._bt_inspector_current_tf = tf_sel

        # Update info text
        def fmt_ts(ts: pd.Timestamp) -> str:
            try:
                return pd.to_datetime(ts, utc=True).strftime("%Y-%m-%d %H:%M:%S")
            except Exception:
                return str(ts)

        try:
            fee_rate = 0.0
            try:
                if getattr(self, "bt_result", None) is not None and getattr(self.bt_result, "config", None) is not None:
                    fee_rate = float(getattr(self.bt_result.config, "fee_rate", 0.0) or 0.0)
            except Exception:
                fee_rate = 0.0

            fee_entry = float(getattr(trade, "fee_entry", 0.0) or 0.0)
            fee_exit = float(getattr(trade, "fee_exit", 0.0) or 0.0)

            # If the report was loaded from an older JSON (no breakdown), reconstruct from notionals.
            try:
                if fee_rate and (fee_entry == 0.0) and (trade.entry_notional is not None):
                    fee_entry = float(trade.entry_notional) * fee_rate
            except Exception:
                pass
            try:
                if fee_rate and (fee_exit == 0.0) and (trade.exit_notional is not None):
                    fee_exit = float(trade.exit_notional) * fee_rate
            except Exception:
                pass

            fees_total = float(getattr(trade, "fees", 0.0) or 0.0)
            try:
                if fees_total == 0.0 and (fee_entry or fee_exit):
                    fees_total = float(fee_entry) + float(fee_exit)
            except Exception:
                pass

            lines = [
                f"Trade #{trade.trade_id} — {trade.side}",
                f"View MACD TF: {getattr(self, '_bt_inspector_current_tf', '1m')}",
                f"Entry: {fmt_ts(trade.entry_time)}  @ {trade.entry_price:.6f}",
                f"Exit : {fmt_ts(trade.exit_time)}  @ {trade.exit_price:.6f}",
                f"Qty: {trade.qty:.6f}    Entry Notional: {trade.entry_notional:.2f}    Exit Notional: {trade.exit_notional:.2f}",
                f"Gross PnL: {trade.gross_pnl:.2f}    Fees: {fees_total:.2f} (entry {fee_entry:.2f} + exit {fee_exit:.2f})    Net PnL: {trade.net_pnl:.2f}    Return: {trade.return_pct:.2f}%",
                f"Net PnL definition: net = gross − (entry fee + exit fee).",
                f"Duration: {trade.duration_min} min    MFE: {trade.mfe:.2f} ({trade.mfe_pct:.2f}%)    MAE: {trade.mae:.2f} ({trade.mae_pct:.2f}%)",
                f"Entry reason: {trade.entry_reason}    Exit reason: {trade.exit_reason}",
                "",
                "ENTRY RULE TRACE:",
                str(getattr(trade, "entry_rule_trace", "") or "n/a"),
                "",
                "EXIT RULE TRACE:",
                str(getattr(trade, "exit_rule_trace", "") or "n/a"),
            ]

            # DI+/DI- (from ADX) values in the selected TF snapshot (if present)
            try:
                tfv = str(getattr(self, "_bt_inspector_current_tf", "1m") or "1m")
            except Exception:
                tfv = "1m"
            try:
                e_snap = (trade.entry_snapshot or {}).get(tfv, {}) if isinstance(getattr(trade, "entry_snapshot", {}), dict) else {}
            except Exception:
                e_snap = {}
            try:
                x_snap = (trade.exit_snapshot or {}).get(tfv, {}) if isinstance(getattr(trade, "exit_snapshot", {}), dict) else {}
            except Exception:
                x_snap = {}

            try:
                e_dp = e_snap.get("di_plus")
                e_dm = e_snap.get("di_minus")
                x_dp = x_snap.get("di_plus")
                x_dm = x_snap.get("di_minus")

                e_dp_s = "-" if e_dp is None else f"{float(e_dp):.3f}"
                e_dm_s = "-" if e_dm is None else f"{float(e_dm):.3f}"
                x_dp_s = "-" if x_dp is None else f"{float(x_dp):.3f}"
                x_dm_s = "-" if x_dm is None else f"{float(x_dm):.3f}"

                lines.append(f"DI+ / DI- ({tfv}) — Entry: {e_dp_s} / {e_dm_s}    Exit: {x_dp_s} / {x_dm_s}")
            except Exception:
                pass

            # Bars-Ago info (if present in this backtest result)
            try:
                eb = getattr(trade, "entry_barsago", {}) or {}
                xb = getattr(trade, "exit_barsago", {}) or {}
                e_items = eb.get("items", []) if isinstance(eb, dict) else []
                x_items = xb.get("items", []) if isinstance(xb, dict) else []
                if e_items:
                    lines.append("BarsAgo entry:")
                    for it in e_items[:20]:
                        try:
                            lines.append(f"  - {it.get('label')}")
                        except Exception:
                            pass
                    if len(e_items) > 20:
                        lines.append(f"  … +{len(e_items) - 20} more")
                if x_items:
                    lines.append("BarsAgo exit:")
                    for it in x_items[:20]:
                        try:
                            lines.append(f"  - {it.get('label')}")
                        except Exception:
                            pass
                    if len(x_items) > 20:
                        lines.append(f"  … +{len(x_items) - 20} more")
            except Exception:
                pass

            info.configure(state="normal")
            info.delete("1.0", "end")
            info.insert("1.0", "\n".join(lines))
            info.configure(state="disabled")
        except Exception:
            pass

        # Determine fragment window
        try:
            pad_m = int(getattr(self, "bt_inspector_pad_var", tk.IntVar(value=60)).get())
        except Exception:
            pad_m = 60
        pad_m = max(0, min(24 * 60, pad_m))

        try:
            max_pts = int(getattr(self, "bt_inspector_maxpts_var", tk.IntVar(value=12000)).get())
        except Exception:
            max_pts = 12000
        max_pts = max(2000, min(30000, max_pts))

        start = pd.to_datetime(trade.entry_time, utc=True) - pd.Timedelta(minutes=pad_m)
        end = pd.to_datetime(trade.exit_time, utc=True) + pd.Timedelta(minutes=pad_m)

        # Bump request-id for async fetch
        try:
            rid = int(getattr(self, "_bt_inspector_req_id", 0)) + 1
        except Exception:
            rid = 1
        self._bt_inspector_req_id = rid

        # Prefer cached backtest stream for the selected MACD TF; otherwise we will fetch on demand.
        df1 = None
        try:
            if self.bt_streams_full and tf_sel in self.bt_streams_full:
                df1 = self.bt_streams_full[tf_sel]
            elif self.bt_streams_full and "1m" in self.bt_streams_full:
                df1 = self.bt_streams_full["1m"]
        except Exception:
            df1 = None

        if df1 is not None:
            # Even with cached streams, slicing/decimation can be heavy on large datasets.
            # Do it in a worker thread to keep the UI responsive.
            try:
                if self._bt_inspector_status_var is not None:
                    self._bt_inspector_status_var.set("Preparing segment…")
            except Exception:
                pass

            def worker_slice(req_id: int):
                try:
                    seg_local = slice_and_decimate(df1, start, end, max_points=max_pts)
                except Exception:
                    seg_local = pd.DataFrame()
                # Return to UI thread
                try:
                    self.after(0, lambda: self._bt_inspector_apply_segment(trade, seg_local, req_id))
                except Exception:
                    # Best-effort fallback
                    self._bt_inspector_apply_segment(trade, seg_local, req_id)

            th = threading.Thread(target=worker_slice, args=(rid,), daemon=True)
            th.start()
            return

        # No stream cached => fetch in background (best-effort)
        try:
            if self._bt_inspector_status_var is not None:
                self._bt_inspector_status_var.set("Fetching candles…")
        except Exception:
            pass

        def worker_fetch(req_id: int, tf_view: str):
            try:
                sym = ""
                try:
                    if self.bt_result is not None:
                        sym = str(self.bt_result.summary.get("symbol", "") or "")
                except Exception:
                    sym = ""
                if not sym:
                    sym = str(getattr(self, "_bt_last_symbol", "") or "")
                if not sym:
                    raise ValueError("No symbol available for on-demand fetch.")

                cache_dir = str(getattr(self, "_bt_last_cache_dir", "data_cache") or "data_cache")
                price_source = str(getattr(self, "_bt_last_price_source", "LAST") or "LAST")
                macd_impl = str(getattr(self, "_bt_last_macd_impl", "TRADINGVIEW") or "TRADINGVIEW")
                adx_impl = str(getattr(self, "_bt_last_adx_impl", "TRADINGVIEW") or "TRADINGVIEW")

                # Fetch with extra lookback so MACD state is stable.
                lookback_m = max(240, pad_m + 200)
                fetch_start = start - pd.Timedelta(minutes=lookback_m)
                fetch_end = end

                raw = _get_data_binance().fetch_klines_1m_futures(
                    sym, fetch_start, fetch_end, cache_dir=cache_dir, progress_cb=None, price_source=price_source
                )
                if raw is None or raw.empty:
                    seg_local = pd.DataFrame()
                else:
                    raw = raw.copy()
                    try:
                        raw["close_time"] = pd.to_datetime(raw["close_time"], utc=True)
                    except Exception:
                        pass
                    try:
                        raw = raw.sort_values("close_time")
                    except Exception:
                        pass
                    try:
                        raw = raw.set_index("close_time", drop=False)
                    except Exception:
                        pass
                    # Build indicator stream for the selected view TF (matches backtest semantics).
                    try:
                        streams = simulate_multitf_indicators_fast(raw, [tf_view], progress_cb=None, macd_impl=macd_impl, adx_impl=adx_impl)
                        df_view = streams.get(tf_view) or streams.get("1m")
                    except Exception:
                        df_view = None

                    if df_view is None or getattr(df_view, "empty", True):
                        # Fallback: at least provide price + 1m MACD
                        try:
                            raw["price"] = pd.to_numeric(raw["close"], errors="coerce")
                        except Exception:
                            raw["price"] = raw["close"]
                        try:
                            closes = raw["price"].astype(float).values
                            macd_line, sig_line, _hist = macd_series(closes, macd_impl)
                            raw["macd"] = macd_line
                            raw["signal"] = sig_line
                        except Exception:
                            pass
                        df_view = raw

                    seg_local = slice_and_decimate(df_view, start, end, max_points=max_pts)


                # Return to UI thread
                self.after(0, lambda: self._bt_inspector_apply_segment(trade, seg_local, req_id))
            except Exception as e:
                self.after(0, lambda: self._bt_inspector_show_error(str(e), req_id))

        th = threading.Thread(target=worker_fetch, args=(rid, tf_sel), daemon=True)
        th.start()

    def _bt_inspector_show_error(self, msg: str, req_id: int):
        try:
            if int(getattr(self, "_bt_inspector_req_id", 0)) != int(req_id):
                return
        except Exception:
            pass
        try:
            if self._bt_inspector_status_var is not None:
                self._bt_inspector_status_var.set("Error")
        except Exception:
            pass
        ax_p = getattr(self, "_bt_inspector_ax_p", None)
        ax_m = getattr(self, "_bt_inspector_ax_m", None)
        canvas = getattr(self, "_bt_inspector_canvas", None)
        if ax_p is None or ax_m is None or canvas is None:
            return

        # Remove optional DI axis (if present) so we don't stack axes across redraws.
        try:
            ax_di = getattr(self, "_bt_inspector_ax_di", None)
            if ax_di is not None:
                ax_di.remove()
        except Exception:
            pass
        self._bt_inspector_ax_di = None
        try:
            ax_p.clear(); ax_m.clear()
            ax_p.set_title(f"Trade Inspector — {msg}")
            canvas.draw_idle()
        except Exception:
            pass

    def _bt_inspector_apply_segment(self, trade: Trade, seg: "pd.DataFrame", req_id: int):
        """Render the segment in the inspector (only if still current)."""
        try:
            if int(getattr(self, "_bt_inspector_req_id", 0)) != int(req_id):
                return
        except Exception:
            # If we can't validate, still try.
            pass

        ax_p = getattr(self, "_bt_inspector_ax_p", None)
        ax_m = getattr(self, "_bt_inspector_ax_m", None)
        canvas = getattr(self, "_bt_inspector_canvas", None)
        if ax_p is None or ax_m is None or canvas is None:
            return

        # Remove optional DI axis (if present) so we don't stack axes across redraws.
        try:
            ax_di = getattr(self, "_bt_inspector_ax_di", None)
            if ax_di is not None:
                ax_di.remove()
        except Exception:
            pass
        self._bt_inspector_ax_di = None

        if seg is None or seg.empty:
            try:
                if self._bt_inspector_status_var is not None:
                    self._bt_inspector_status_var.set("No data")
            except Exception:
                pass
            try:
                ax_p.clear(); ax_m.clear()
                ax_p.set_title("No chart data in range.")
                canvas.draw_idle()
            except Exception:
                pass
            return

        try:
            if self._bt_inspector_status_var is not None:
                self._bt_inspector_status_var.set("Ready")
        except Exception:
            pass

        ax_p.clear(); ax_m.clear()

        try:
            x = seg.index.to_pydatetime()
        except Exception:
            x = []
        try:
            y = seg["price"].astype(float).values if "price" in seg.columns else seg["close"].astype(float).values
        except Exception:
            try:
                y = seg["close"].astype(float).values
            except Exception:
                y = []

        try:
            ax_p.plot(x, y, linewidth=1.1, label="Price")
        except Exception:
            pass

        # Entry/exit visuals
        try:
            ent_t = pd.to_datetime(trade.entry_time, utc=True).to_pydatetime()
            ex_t = pd.to_datetime(trade.exit_time, utc=True).to_pydatetime()
            ax_p.axvline(ent_t, linewidth=1.0, alpha=0.35)
            ax_p.axvline(ex_t, linewidth=1.0, alpha=0.35)
            ax_p.scatter([ent_t], [float(trade.entry_price)], marker="^", s=70, zorder=6, label="Entry")
            ax_p.scatter([ex_t], [float(trade.exit_price)], marker="v", s=70, zorder=6, label="Exit")
            try:
                ax_p.plot([ent_t, ex_t], [float(trade.entry_price), float(trade.exit_price)], linewidth=2.0, alpha=0.70)
            except Exception:
                pass
        except Exception:
            pass


        # Bars-Ago visual markers (entry/exit)
        try:
            show_marks = True
            try:
                show_marks = bool(getattr(self, "bt_inspector_show_barsago_var", tk.BooleanVar(value=True)).get())
            except Exception:
                show_marks = True

            if show_marks:
                try:
                    t_min = pd.to_datetime(seg.index.min(), utc=True)
                    t_max = pd.to_datetime(seg.index.max(), utc=True)
                except Exception:
                    t_min = None
                    t_max = None

                def _in_range(ts: "pd.Timestamp") -> bool:
                    try:
                        if t_min is not None and ts < t_min:
                            return False
                        if t_max is not None and ts > t_max:
                            return False
                    except Exception:
                        return True
                    return True

                def _draw_profile(base_time: Any, prof: Any, linestyle: str = "--", span_alpha: float = 0.06):
                    if prof is None or not isinstance(prof, dict):
                        return
                    try:
                        exact = prof.get("exact", []) or []
                        within = prof.get("within", []) or []
                    except Exception:
                        exact = []
                        within = []

                    # WITHIN window: draw the largest window only (avoids visual clutter)
                    try:
                        if within:
                            nmax = int(max(within))
                            if nmax >= 0:
                                base_ts = pd.to_datetime(base_time, utc=True)
                                ts0 = base_ts - pd.Timedelta(minutes=nmax)
                                ts1 = base_ts
                                if _in_range(ts0) or _in_range(ts1):
                                    ax_p.axvspan(ts0.to_pydatetime(), ts1.to_pydatetime(), alpha=span_alpha)
                                    ax_m.axvspan(ts0.to_pydatetime(), ts1.to_pydatetime(), alpha=span_alpha)
                    except Exception:
                        pass

                    # EXACT offsets: vertical dashed lines
                    try:
                        for n in sorted({int(x) for x in exact if int(x) >= 0}):
                            base_ts = pd.to_datetime(base_time, utc=True)
                            ts = base_ts - pd.Timedelta(minutes=int(n))
                            if not _in_range(ts):
                                continue
                            ax_p.axvline(ts.to_pydatetime(), linestyle=linestyle, linewidth=0.9, alpha=0.25)
                            ax_m.axvline(ts.to_pydatetime(), linestyle=linestyle, linewidth=0.9, alpha=0.25)
                    except Exception:
                        pass

                # Entry profile
                try:
                    ent_time = pd.to_datetime(trade.entry_time, utc=True)
                    _draw_profile(ent_time, getattr(trade, "entry_barsago", {}), linestyle="--", span_alpha=0.06)
                except Exception:
                    pass
                # Exit profile
                try:
                    ex_time = pd.to_datetime(trade.exit_time, utc=True)
                    _draw_profile(ex_time, getattr(trade, "exit_barsago", {}), linestyle=":", span_alpha=0.04)
                except Exception:
                    pass
        except Exception:
            pass

        # MACD: prefer precomputed columns for consistency
        try:
            if "macd" in seg.columns and "signal" in seg.columns:
                macd_line = seg["macd"].astype(float).values
                sig_line = seg["signal"].astype(float).values
                hist = macd_line - sig_line
            else:
                impl = (self.macd_impl_var.get() or "TRADINGVIEW").upper()
                macd_line, sig_line, hist = macd_series(np.asarray(y, dtype=float), impl)

            ax_m.plot(x, macd_line, linewidth=1.0, label="MACD")
            ax_m.plot(x, sig_line, linewidth=1.0, label="Signal")
            try:
                ax_m.plot(x, hist, linewidth=0.9, alpha=0.35, label="Hist")
            except Exception:
                pass
            ax_m.axhline(0.0, linewidth=0.8, alpha=0.5)
        except Exception:
            pass

        # ADX components: DI+ / DI- (if available in the selected TF stream)
        # Plot them on a secondary Y axis so their 0..100 scale doesn't distort MACD.
        try:
            have_di = False
            try:
                have_di = ("di_plus" in seg.columns) and ("di_minus" in seg.columns)
            except Exception:
                have_di = False

            if have_di:
                ax_di = ax_m.twinx()
                self._bt_inspector_ax_di = ax_di

                try:
                    di_p = seg["di_plus"].astype(float).values
                except Exception:
                    di_p = None
                try:
                    di_m = seg["di_minus"].astype(float).values
                except Exception:
                    di_m = None

                if di_p is not None:
                    ax_di.plot(x, di_p, linewidth=1.0, alpha=0.80, label="DI+")
                if di_m is not None:
                    ax_di.plot(x, di_m, linewidth=1.0, alpha=0.80, label="DI-")

                try:
                    ax_di.set_ylim(0.0, 100.0)
                except Exception:
                    pass
                try:
                    ax_di.set_ylabel("DI (+/-)")
                except Exception:
                    pass
                try:
                    ax_di
                except Exception:
                    pass
        except Exception:
            pass

        ax_p.grid(True, alpha=0.22)
        ax_m.grid(True, alpha=0.18)
        ax_p.set_title(f"Trade #{trade.trade_id} — {trade.side} — Price + MACD (fragment) — TF: {getattr(self, '_bt_inspector_current_tf', '1m')}")
        ax_p.set_ylabel("Price")
        ax_m.set_ylabel("MACD")
        ax_m.set_xlabel("Time (UTC)")
        ax_p.tick_params(labelbottom=False)

        locator = mdates.AutoDateLocator(minticks=5, maxticks=10)
        try:
            fmt = mdates.ConciseDateFormatter(locator)
            ax_m.xaxis.set_major_locator(locator)
            ax_m.xaxis.set_major_formatter(fmt)
        except Exception:
            ax_m.xaxis.set_major_locator(locator)
            ax_m.xaxis.set_major_formatter(mdates.DateFormatter("%m-%d\n%H:%M"))

        try:
            _safe_axis_legend(ax_p, loc="upper left")
        except Exception:
            pass
        try:
            _safe_axis_legend(ax_m, loc="upper left")
        except Exception:
            pass

        try:
            canvas.draw_idle()
        except Exception:
            pass

    def _render_backtest(self, res: BacktestResult, streams_full: Optional[Dict[str, pd.DataFrame]] = None):
        self._ensure_backtest_tab_built()
        self._ensure_bt_results_surface_built()
        self.bt_result = res
        self.bt_streams_full = streams_full
        # Reset chart caches/view for new results (avoid huge redraws).
        try:
            self._bt_ohlc_cache = {}
        except Exception:
            pass
        try:
            if hasattr(self, "bt_chart_pos_var"):
                self.bt_chart_pos_var.set(0.0)
        except Exception:
            pass

        # draw charts
        have_stream = False
        try:
            have_stream = (self.bt_streams_full is not None) and ("1m" in self.bt_streams_full)
        except Exception:
            have_stream = False

        if have_stream:
            try:
                self._bt_draw_chart()
            except Exception:
                pass
        else:
            # Don't crash if chart data isn't available (e.g., report loaded without candles).
            try:
                if hasattr(self, "bt_price_ax") and hasattr(self, "bt_macd_ax"):
                    self.bt_price_ax.clear()
                    self.bt_macd_ax.clear()
                    self.bt_price_ax.set_title("Overview chart not loaded. Use the Trades table → Trade Inspector (it can fetch on demand).")
                    self.bt_price_canvas.draw_idle()
            except Exception:
                pass

        try:
            self._bt_draw_equity()
        except Exception:
            pass

        # summary
        s = res.summary
        gross_pnl_total = float(s.get("gross_pnl_total", 0.0) or 0.0)
        fees_total = float(s.get("fees_total", 0.0) or 0.0)
        fees_entry_total = float(s.get("fees_entry_total", 0.0) or 0.0)
        fees_exit_total = float(s.get("fees_exit_total", 0.0) or 0.0)
        net_profit_from_trades = float(s.get("net_profit_from_trades", 0.0) or 0.0)
        engine_counts = s.get("engine_type_counts", {}) if isinstance(s.get("engine_type_counts", {}), dict) else {}
        fill_source_counts = s.get("fill_source_counts", {}) if isinstance(s.get("fill_source_counts", {}), dict) else {}
        exit_reason_counts = s.get("exit_reason_counts", {}) if isinstance(s.get("exit_reason_counts", {}), dict) else {}
        engine_counts_txt = ", ".join(f"{k}={v}" for k, v in sorted(engine_counts.items())) if engine_counts else "n/a"
        fill_source_counts_txt = ", ".join(f"{k}={v}" for k, v in sorted(fill_source_counts.items())) if fill_source_counts else "n/a"
        exit_reason_counts_txt = ", ".join(f"{k}={v}" for k, v in sorted(exit_reason_counts.items())) if exit_reason_counts else "n/a"
        lines = [
            f"Symbol: {s.get('symbol')}",
            f"Range: {res.start.strftime('%Y-%m-%d %H:%M:%S')} → {res.end.strftime('%Y-%m-%d %H:%M:%S')} (UTC)",
            "",
            f"Initial balance: {s.get('initial_balance'):.2f}",
            f"Ending balance : {s.get('ending_balance'):.2f}",
            f"Net PnL (after fees): {s.get('net_profit'):.2f}   ({s.get('return_pct'):.2f}%)",
            f"Gross PnL (before fees): {gross_pnl_total:.2f}",
            f"Fees total: {fees_total:.2f}   (entry {fees_entry_total:.2f} + exit {fees_exit_total:.2f})",
            f"Cross-check: sum(trade net PnL) = {net_profit_from_trades:.2f}",
            f"Net definition: net = gross − fees (entry+exit) = Ending − Initial",
            "",
            f"Trades: {s.get('trades')}   Wins: {s.get('wins')}   Losses: {s.get('losses')}   Win rate: {s.get('win_rate_pct'):.2f}%",
            f"Profit factor: {s.get('profit_factor'):.3f}   Avg trade: {s.get('avg_trade'):.2f}",
            f"Avg win: {s.get('avg_win'):.2f}   Avg loss: {s.get('avg_loss'):.2f}",
            f"Max drawdown: {s.get('max_drawdown'):.2f}   ({s.get('max_drawdown_pct'):.2f}%)",
            f"Avg duration: {s.get('avg_duration_min'):.1f} min   Avg entry→exit: {float(s.get('avg_entry_to_exit_sec', 0.0) or 0.0):.1f} s",
            f"Avg signal→entry: {float(s.get('avg_signal_to_entry_sec', 0.0) or 0.0):.1f} s   Avg order→entry: {float(s.get('avg_order_to_entry_sec', 0.0) or 0.0):.1f} s",
            f"Execution foundations: {engine_counts_txt}",
            f"Fill sources: {fill_source_counts_txt}",
            f"Exit reasons: {exit_reason_counts_txt}",
            f"SL exits: {int(s.get('stop_loss_count', 0) or 0)}   TP exits: {int(s.get('take_profit_count', 0) or 0)}   Ambiguous exits: {int(s.get('ambiguous_exit_count', 0) or 0)}",
            "",
            f"Config: notional={res.config.order_notional_usdt:.2f}  fee={res.config.fee_rate*100:.4f}%/side  slippage={res.config.slippage_bps:.2f} bps  SL={str(getattr(res.config, 'stop_loss_mode', 'PERCENT') or 'PERCENT')}:{float(getattr(res.config, 'stop_loss_value', getattr(res.config, 'stop_loss_pct', 0.0)) or 0.0):.4f} tf={str(getattr(res.config, 'stop_loss_timeframe', '1m') or '1m')} field={str(getattr(res.config, 'stop_loss_field', '') or '')} adj={float(getattr(res.config, 'stop_loss_offset_pct', 0.0) or 0.0):.4f}%  TP={str(getattr(res.config, 'take_profit_mode', 'PERCENT') or 'PERCENT')}:{float(getattr(res.config, 'take_profit_value', getattr(res.config, 'take_profit_pct', 0.0)) or 0.0):.4f} tf={str(getattr(res.config, 'take_profit_timeframe', '1m') or '1m')} field={str(getattr(res.config, 'take_profit_field', '') or '')} adj={float(getattr(res.config, 'take_profit_offset_pct', 0.0) or 0.0):.4f}%  reverse={res.config.allow_reverse}  skipBothEntries={res.config.skip_if_both_entries}",
        ]
        try:
            self.bt_summary_txt.configure(state="normal")
            self.bt_summary_txt.delete("1.0", "end")
            self.bt_summary_txt.insert("1.0", "\n".join(lines))
            self.bt_summary_txt.configure(state="disabled")
        except Exception:
            pass

        # table
        try:
            for iid in self.bt_tree.get_children():
                self.bt_tree.delete(iid)
        except Exception:
            pass

        for t in res.trades:
            try:
                signal_time = pd.to_datetime(getattr(t, "signal_time", None), utc=True) if getattr(t, "signal_time", None) is not None else pd.NaT
                entry_fill_time = pd.to_datetime(getattr(t, "entry_fill_time", None), utc=True) if getattr(t, "entry_fill_time", None) is not None else pd.NaT
                signal_to_entry_sec = ""
                try:
                    if pd.notna(signal_time) and pd.notna(entry_fill_time):
                        signal_to_entry_sec = f"{float((entry_fill_time - signal_time).total_seconds()):.1f}"
                except Exception:
                    signal_to_entry_sec = ""
                self.bt_tree.insert(
                    "",
                    "end",
                    iid=str(t.trade_id),
                    values=[
                        t.trade_id,
                        str(getattr(t, "engine_type", "") or ""),
                        str(getattr(t, "fill_source", "") or ""),
                        t.side,
                        (signal_time.strftime("%Y-%m-%d %H:%M:%S") if pd.notna(signal_time) else ""),
                        t.entry_time.strftime("%Y-%m-%d %H:%M:%S"),
                        t.exit_time.strftime("%Y-%m-%d %H:%M:%S"),
                        f"{t.entry_price:.6f}",
                        f"{t.exit_price:.6f}",
                        (f"{float(getattr(t, 'stop_loss_price')):.6f}" if getattr(t, "stop_loss_price", None) is not None else ""),
                        (f"{float(getattr(t, 'take_profit_price')):.6f}" if getattr(t, "take_profit_price", None) is not None else ""),
                        f"{t.net_pnl:.2f}",
                        f"{t.return_pct:.2f}",
                        t.duration_min,
                        signal_to_entry_sec,
                        t.entry_reason,
                        t.exit_reason,
                        ("YES" if bool(getattr(t, "ambiguous_exit", False)) else ""),
                    ],
                )
            except Exception:
                pass

        try:
            self.btn_export_csv.config(state="normal" if res.trades else "disabled")
            self.btn_clear_bt.config(state="normal")
            try:
                self.btn_inspect_trade.config(state="normal" if res.trades else "disabled")
            except Exception:
                pass
            try:
                self.btn_save_report.config(state="normal")
            except Exception:
                pass
            try:
                self.btn_generate_trade_detail.config(state="normal" if res.trades else "disabled")
            except Exception:
                pass
            try:
                self.btn_trade_audit.config(state="normal" if res.trades else "disabled")
            except Exception:
                pass
            try:
                if replay_plan := build_backtest_replay_plan(
                    self.bt_result,
                    next_cfg=self.bt_result.config,
                    symbol=str(self.bt_result.summary.get("symbol") or ""),
                    start=self.bt_result.start,
                    end=self.bt_result.end,
                    rules_model=self.rules,
                    tab_group_join_mode=self.tab_group_join_mode,
                    group_rule_join_mode=self.group_rule_join_mode,
                    entry_filters=self.entry_filters,
                ):
                    self.btn_replay_backtest.config(state="normal" if replay_plan.eligible else "disabled")
            except Exception:
                pass
        except Exception:
            pass

        # jump to tab
        try:
            self.main_nb.select(self.tab_bt)
        except Exception:
            pass


    def _bt_set_default_sash(self, *, show_trades: bool = False):
        """Ensure the Backtest Trades area remains visible on smaller screens.

        Backtest tab uses a vertical PanedWindow (Charts + Summary/Trades). Some window managers
        initialize the sash in a way that collapses the bottom pane, which makes it look like
        the trades list is "missing". This helper nudges the sash to a sensible default.
        """
        try:
            pw = getattr(self, "bt_pw", None)
            if pw is None:
                return
            # Let geometry settle first.
            try:
                self.update_idletasks()
            except Exception:
                pass
            h = int(pw.winfo_height() or 0)
            if h <= 50:
                return

            # sashpos is the Y coordinate of the divider from the top of the PanedWindow.
            # Smaller value -> more space for the bottom pane (trades).
            if show_trades:
                frac = 0.48
            else:
                frac = 0.62
            pos = max(80, min(h - 80, int(h * frac)))
            try:
                pw.sashpos(0, pos)
            except Exception:
                pass
        except Exception:
            pass

