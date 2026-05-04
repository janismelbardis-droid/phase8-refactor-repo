from __future__ import annotations

"""Popup/dialog helpers extracted from the Tk UI shell."""

from typing import Any

import tkinter as tk
from tkinter import messagebox, ttk

from ..constants import C_BG_NEU, C_TEXT


def _report_popup_error(app: Any, title: str, exc: Exception) -> None:
        msg = str(exc).strip() or exc.__class__.__name__
        try:
            status_var = getattr(app, "status_var", None)
            if status_var is not None:
                status_var.set(f"{title} failed: {msg}")
        except Exception:
            pass
        try:
            parent = app if isinstance(app, tk.Misc) else None
            messagebox.showerror(title, msg, parent=parent)
        except Exception:
            pass


def bind_popup_text_scroll(txt: tk.Text) -> None:
        def _scroll_y(units: int):
            try:
                txt.yview_scroll(int(units), "units")
            except Exception:
                pass
            return "break"

        def _scroll_x(units: int):
            try:
                txt.xview_scroll(int(units), "units")
            except Exception:
                pass
            return "break"

        def _mousewheel(event):
            delta = getattr(event, "delta", 0)
            if delta:
                step = int(-1 * (delta / 120))
                if step == 0:
                    step = -1 if delta > 0 else 1
                return _scroll_y(step)
            num = getattr(event, "num", None)
            if num == 4:
                return _scroll_y(-1)
            if num == 5:
                return _scroll_y(1)
            return None

        def _shift_mousewheel(event):
            delta = getattr(event, "delta", 0)
            if delta:
                step = int(-1 * (delta / 120))
                if step == 0:
                    step = -1 if delta > 0 else 1
                return _scroll_x(step)
            num = getattr(event, "num", None)
            if num == 4:
                return _scroll_x(-1)
            if num == 5:
                return _scroll_x(1)
            return None

        try:
            txt.bind("<MouseWheel>", _mousewheel)
            txt.bind("<Shift-MouseWheel>", _shift_mousewheel)
            txt.bind("<Button-4>", _mousewheel)
            txt.bind("<Button-5>", _mousewheel)
            txt.bind("<Shift-Button-4>", _shift_mousewheel)
            txt.bind("<Shift-Button-5>", _shift_mousewheel)
        except Exception:
            pass


def make_popup_text_widget(app: Any, parent, *, width: int = 120, height: int = 34, wrap: str = "none") -> tk.Text:
        holder = ttk.Frame(parent)
        holder.pack(fill="both", expand=True)
        try:
            holder.rowconfigure(0, weight=1)
            holder.columnconfigure(0, weight=1)
        except Exception:
            pass
        txt = tk.Text(
            holder,
            wrap=wrap,
            width=width,
            height=height,
            bg=C_BG_NEU,
            fg=C_TEXT,
            insertbackground=C_TEXT,
            font=("Consolas", 10),
            relief="solid",
            bd=1,
            state="disabled",
            cursor="arrow",
        )
        txt.grid(row=0, column=0, sticky="nsew")
        ysb = ttk.Scrollbar(holder, orient="vertical", command=txt.yview)
        ysb.grid(row=0, column=1, sticky="ns")
        xsb = ttk.Scrollbar(holder, orient="horizontal", command=txt.xview)
        xsb.grid(row=1, column=0, sticky="ew")
        txt.configure(yscrollcommand=ysb.set, xscrollcommand=xsb.set)
        bind_popup_text_scroll(txt)
        return txt


def set_popup_text_content(txt: tk.Text, content: str, *, preserve_view: bool = True) -> None:
        content_txt = str(content or "").rstrip()
        if not content_txt:
            content_txt = "No detail text available yet.\n\nWait for a populated live/latest snapshot, then press Refresh."
        try:
            x_view = txt.xview()[0] if preserve_view else 0.0
            y_view = txt.yview()[0] if preserve_view else 0.0
        except Exception:
            x_view = 0.0
            y_view = 0.0
        try:
            txt.config(state="normal")
            txt.delete("1.0", "end")
            txt.insert("1.0", content_txt)
            txt.config(state="disabled")
            txt.xview_moveto(x_view)
            txt.yview_moveto(y_view)
        except Exception:
            pass


def show_orderflow_trail(app: Any, tf: str):
        tf = str(tf)
        try:
            if not hasattr(app, "_orderflow_trail_windows"):
                app._orderflow_trail_windows = {}
            win = app._orderflow_trail_windows.get(tf)
            if win is None or (hasattr(win, "winfo_exists") and not win.winfo_exists()):
                win = tk.Toplevel(app)
                win.title(f"Order Flow Trail - {tf}")
                win.transient(app)
                win.resizable(True, True)
                frm = ttk.Frame(win, padding=10)
                frm.pack(fill="both", expand=True)
                try:
                    frm.rowconfigure(0, weight=1)
                    frm.columnconfigure(0, weight=1)
                except Exception:
                    pass
                txt = make_popup_text_widget(app, frm, width=120, height=34, wrap="none")
                btns = ttk.Frame(frm)
                btns.pack(fill="x", pady=(8, 0))
                ttk.Button(btns, text="Refresh", command=lambda tf=tf: app._show_orderflow_trail(tf)).pack(side="left")
                ttk.Button(btns, text="OF...", command=lambda tf=tf: show_orderflow_details(app, tf)).pack(side="left", padx=(6, 0))
                ttk.Button(btns, text="Close", command=win.destroy).pack(side="right")
                win._content_text = txt
                app._orderflow_trail_windows[tf] = win
            set_popup_text_content(win._content_text, app._build_orderflow_trail_text(tf), preserve_view=True)
            win.deiconify()
            win.lift()
            win.focus_force()
        except Exception as exc:
            _report_popup_error(app, f"Order Flow Trail ({tf})", exc)


def show_orderflow_details(app: Any, tf: str):
        tf = str(tf)
        try:
            if not hasattr(app, "_orderflow_detail_windows"):
                app._orderflow_detail_windows = {}
            win = app._orderflow_detail_windows.get(tf)
            if win is None or (hasattr(win, "winfo_exists") and not win.winfo_exists()):
                win = tk.Toplevel(app)
                win.title(f"Order Flow Details - {tf}")
                win.transient(app)
                win.resizable(True, True)
                frm = ttk.Frame(win, padding=10)
                frm.pack(fill="both", expand=True)
                try:
                    frm.rowconfigure(0, weight=1)
                    frm.columnconfigure(0, weight=1)
                except Exception:
                    pass
                txt = make_popup_text_widget(app, frm, width=122, height=34, wrap="none")
                try:
                    win.geometry("1180x760")
                    win.minsize(900, 560)
                except Exception:
                    pass
                btns = ttk.Frame(frm)
                btns.pack(fill="x", pady=(8, 0))
                ttk.Button(btns, text="Refresh", command=lambda tf=tf: app._show_orderflow_details(tf)).pack(side="left")
                ttk.Button(btns, text="Trail...", command=lambda tf=tf: show_orderflow_trail(app, tf)).pack(side="left", padx=(6, 0))
                ttk.Button(btns, text="Market State...", command=lambda tf=tf: show_market_state_details(app, tf)).pack(side="left", padx=(6, 0))
                ttk.Button(btns, text="Close", command=win.destroy).pack(side="right")
                win._content_text = txt
                app._orderflow_detail_windows[tf] = win
            set_popup_text_content(win._content_text, app._build_orderflow_detail_text(tf), preserve_view=True)
            win.deiconify()
            win.lift()
            win.focus_force()
        except Exception as exc:
            _report_popup_error(app, f"Order Flow Details ({tf})", exc)


def show_market_state_details(app: Any, tf: str):
        tf = str(tf)
        try:
            if not hasattr(app, "_market_state_detail_windows"):
                app._market_state_detail_windows = {}
            win = app._market_state_detail_windows.get(tf)
            if win is None or (hasattr(win, "winfo_exists") and not win.winfo_exists()):
                win = tk.Toplevel(app)
                win.title(f"Market State Details - {tf}")
                win.transient(app)
                win.resizable(True, True)
                frm = ttk.Frame(win, padding=10)
                frm.pack(fill="both", expand=True)
                try:
                    frm.rowconfigure(0, weight=1)
                    frm.columnconfigure(0, weight=1)
                except Exception:
                    pass
                txt = make_popup_text_widget(app, frm, width=122, height=34, wrap="none")
                try:
                    win.geometry("1180x760")
                    win.minsize(900, 560)
                except Exception:
                    pass
                btns = ttk.Frame(frm)
                btns.pack(fill="x", pady=(8, 0))
                ttk.Button(btns, text="Refresh", command=lambda tf=tf: app._show_market_state_details(tf)).pack(side="left")
                ttk.Button(btns, text="Order Flow...", command=lambda tf=tf: show_orderflow_details(app, tf)).pack(side="left", padx=(6, 0))
                ttk.Button(btns, text="Threshold Lab...", command=app._open_market_state_threshold_lab).pack(side="left", padx=(6, 0))
                ttk.Button(btns, text="Close", command=win.destroy).pack(side="right")
                win._content_text = txt
                app._market_state_detail_windows[tf] = win
            try:
                detail_text = app._build_market_state_detail_text(tf)
            except Exception as exc:
                detail_text = f"Timeframe: {tf}\n\nMarket-state detail rendering failed.\n\nReason: {exc}"
            set_popup_text_content(win._content_text, detail_text, preserve_view=True)
            win.deiconify()
            win.lift()
            win.focus_force()
        except Exception as exc:
            _report_popup_error(app, f"Market State Details ({tf})", exc)
