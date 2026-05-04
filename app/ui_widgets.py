from __future__ import annotations

"""Reusable Tk widgets used by the main UI."""

import tkinter as tk
from tkinter import ttk
from typing import Any, Callable, Dict, List, Optional

from .constants import *
from .rules import Rule

class LegendWindow(tk.Toplevel):
    """Clickable legend for symbols and rule semantics used in the app."""

    def __init__(self, parent):
        super().__init__(parent)
        self.title("Legend — Symbols & Rule Semantics")
        self.geometry("900x560")
        self.minsize(760, 460)
        try:
            self.grab_set()
        except Exception:
            pass

        outer = ttk.Frame(self, padding=10)
        outer.pack(side="top", fill="both", expand=True)

        ttk.Label(
            outer,
            text="Click an item to see its meaning. (This legend matches the LIVE grid, Rule Builder chips, Backtest status, and the stop/target controls in Backtest settings.)",
            foreground="#555"
        ).pack(side="top", anchor="w", pady=(0, 10))

        pw = ttk.Panedwindow(outer, orient="horizontal")
        pw.pack(side="top", fill="both", expand=True)

        left = ttk.Frame(pw)
        right = ttk.Frame(pw)
        pw.add(left, weight=1)
        pw.add(right, weight=3)

        self.lst = tk.Listbox(left, height=22, exportselection=False)
        self.lst.pack(side="top", fill="both", expand=True)

        self.txt = tk.Text(right, wrap="word", font=("Segoe UI", 10))
        self.txt.pack(side="top", fill="both", expand=True)
        self.txt.configure(state="disabled")

        # Items (title -> description)
        self.items = [
            ("Backtest foundations", """The app now thinks about backtesting in two execution foundations.

OHLCV Backtest = fast approximation using candle data. Entries are scheduled after the signal candle, and stop/target exits are approximated intrabar from OHLC.

AggTrade Backtest = realistic validation using downloaded aggregate trades. Entries and exits are resolved from the real trade sequence after the signal time."""),
            ("Backtest mode mapping", """Current UI mode labels are still the old ones. Read them like this:

• Bar Close (...) modes -> OHLCV Backtest foundation
• Tick (aggTrades) -> AggTrade Backtest foundation

So the dropdown label may look familiar, while the behavior underneath is now more structured."""),
            ("Stop/Target modes", """Stop mode and Target mode choose HOW the exit price is calculated.

OFF = disabled
PERCENT = percent away from entry
ABSOLUTE = fixed raw price distance from entry
ATR = volatility-based distance using ATR multiple
FIELD = use an indicator-derived numeric level
R_MULTIPLE = target only; uses stop distance multiplied by R value"""),
            ("Stop/Target value", """The Value box means different things depending on the selected mode.

PERCENT: value is percent distance from entry. Example 0.5 = 0.5%.
ABSOLUTE: value is raw price distance. Example 1.25 means 1.25 price units.
ATR: value is ATR multiple. Example 1.5 = 1.5 x ATR.
FIELD: value is usually not the important part; the field name matters most.
R_MULTIPLE: target value is the reward multiple. Example 2.0 = target at 2R."""),
            ("Stop/Target TF", """TF means which timeframe snapshot to use for ATR or FIELD lookups.

Examples:
• Stop TF = 1m and field = vidya_lower -> use the 1m vidya_lower value
• Target TF = 5m and field = range_filter_upper -> use the 5m range_filter_upper value

For simple PERCENT and ABSOLUTE exits, TF is usually not important."""),
            ("Stop/Target field", """FIELD mode uses a numeric level already present in the entry snapshot.

The field selector now shows common built-in names in a dropdown. You can still type an advanced internal field manually if you know it.

Common choices:
atr
frama
frama_upper
frama_lower
vidya_line
vidya_upper
vidya_lower
vidya_nearest_liq_low
vidya_nearest_liq_high
range_filter_line
range_filter_upper
range_filter_lower

A valid stop/target field must also make directional sense: long stop should be below entry, long target above entry, short stop above entry, short target below entry."""),
            ("Stop/Target adj (%)", """Adj (%) nudges a FIELD-based level by a percent offset.

Use it when you do not want the stop or target placed exactly on the raw indicator level.

Example: stop field = vidya_lower and adj = 0.1 means place the stop slightly beyond that level instead of exactly on it."""),
            ("Why ATR exists", """ATR was added because it is a standard volatility-based exit distance. It widens in fast conditions and tightens in quiet conditions.

ATR is optional. It is not meant to replace your structural levels. In this app, FIELD-based exits can often be more natural because they can use FRAMA, VIDYA, Range Filter, or liquidity-related levels directly."""),
            ("Practical exit examples", """Simple starter setup:
• Stop mode = PERCENT, value = 0.4
• Target mode = PERCENT, value = 0.8

Structure-based example for LONGS:
• Stop mode = FIELD, TF = 1m, field = vidya_lower
• Target mode = FIELD, TF = 1m, field = vidya_nearest_liq_high

Structure stop + R target:
• Stop mode = FIELD, TF = 1m, field = vidya_lower
• Target mode = R_MULTIPLE, value = 1.5 or 2.0

For SHORTS, mirror the logic with upper/lower levels reversed."""),
            ("● Dot", """A dot cell is a binary state: GREEN or RED.

Green means the condition is TRUE for that candle; Red means FALSE."""),
            ("M/S dot", """M/S = MACD above/below Signal on that timeframe.

GREEN: MACD > Signal
RED: MACD <= Signal"""),
            ("Angle dot", """Angle = MACD rising/falling vs the previous candle (same timeframe).

GREEN: MACD_now > MACD_prev
RED: MACD_now <= MACD_prev"""),
            ("PPO dot", """PPO dot shows the slope direction of PPO on that timeframe.

GREEN: PPO rising
RED: PPO falling"""),
            ("FlipAge", """FlipAge = number of candles since the PPO dot last flipped color (on that timeframe).

Example: FlipAge=0 means it flipped on the current candle; FlipAge=1 means 1 candle ago."""),
            ("★ (star) Trigger", """★ marks an EVENT rule as a TRIGGER.

Execution/backtest: the trigger is TRUE only on the candle where the event happens (a pulse).
Monitoring/live: after a trigger fires, the group can show FIRED while filters stay valid (sticky)."""),
            ("SEQUENCE group", """SEQUENCE means the rules in that group must complete in order across time.

Example:
• rule 1 happens first
• later rule 2 happens
• later rule 3 happens

Only when the final rule completes does the group become READY.

Unlike AND/OR, SEQUENCE remembers progress from one bar to the next."""),
            ("Group status: WAIT", """WAIT means the group can't be evaluated yet (not enough data / warmup / missing previous bar for an event)."""),
            ("Group status: NO", """NO means the FILTERS are failing (blocked)."""),
            ("Group status: ARMED", """ARMED means the FILTERS pass, but the TRIGGER has not fired on this candle.

This is a 'setup ready, waiting for signal' state."""),
            ("Group status: READY", """READY means: trigger fired on THIS candle AND filters pass.

This is the candle where an entry/exit can happen in backtest."""),
            ("Group status: FIRED", """FIRED means: trigger fired previously and is latched for monitoring while filters remain passing.

FIRED (Xm) shows how many minutes ago it triggered."""),
            ("!= (not equal)", """!= means 'not equal to'.

Example: M/S != GREEN means 'M/S is anything except GREEN'.
If the field has only GREEN/RED, this equals 'M/S is RED'.
If the field can be unknown (warmup), then '!= GREEN' may also include unknown."""),
            ("EVENT: TURNED", """TURNED GREEN/RED is an event that happens only when the dot changed into that color on the current candle."""),
            ("EVENT: FLIPPED", """FLIPPED is an event that happens only on the candle where the dot changes color (either direction)."""),
            ("EVENT: CROSS", """MACD×SIG CROSS UP/DOWN happens only on the candle where MACD crosses the Signal line (up or down)."""),
            ("Backtest chart markers", """▲ Entry marker, ▼ Exit marker. Trade lines are colored by trade net PnL (green=profit, red=loss).

Use toolbar (Home/Pan/Zoom) or mouse wheel to zoom."""),
        ]

        for title, _ in self.items:
            self.lst.insert("end", title)

        self.lst.bind("<<ListboxSelect>>", self._on_select)
        self.lst.selection_set(0)
        self._show(0)

        btns = ttk.Frame(outer)
        btns.pack(side="bottom", fill="x", pady=(10, 0))
        ttk.Button(btns, text="Close", command=self.destroy).pack(side="right")

    def _on_select(self, event=None):
        sel = self.lst.curselection()
        if not sel:
            return
        self._show(int(sel[0]))

    def _show(self, idx: int):
        try:
            title, desc = self.items[idx]
        except Exception:
            return
        self.txt.configure(state="normal")
        self.txt.delete("1.0", "end")
        self.txt.insert("1.0", f"{title}\n\n{desc}")
        self.txt.configure(state="disabled")

class Chip(tk.Frame):
    def __init__(self, parent, text: str, on_remove, on_edit):
        super().__init__(parent, bd=1, relief="solid", bg=C_BG_NEU, highlightthickness=0)
        self._on_remove = on_remove
        self._on_edit = on_edit

        self.rule = None  # Rule associated with this chip
        self.lbl = tk.Label(self, text=text, bg=C_BG_NEU, fg=C_TEXT, padx=6, pady=2, cursor="hand2")
        self.lbl.pack(side="left")
        self.btn = tk.Label(self, text=" ✕ ", bg=C_BG_NEU, fg=C_DIM, cursor="hand2")
        self.btn.pack(side="right")

        self.lbl.bind("<Button-1>", lambda e: self._on_edit())
        self.btn.bind("<Button-1>", lambda e: self._on_remove())

    def set_state(self, satisfied: Optional[bool]):
        if satisfied is None:
            bg = C_BG_NEU
            fg = C_DIM
        else:
            bg = C_BG_OK if satisfied else C_BG_NO
            fg = C_TEXT
        self.config(bg=bg)
        self.lbl.config(bg=bg, fg=fg)
        self.btn.config(bg=bg)

    # Backwards-compat alias (older revisions used set_satisfied)
    def set_satisfied(self, satisfied: Optional[bool]):
        self.set_state(satisfied)

class GroupUI(tk.Frame):
    def __init__(self, parent, tab_name: str, idx: int, join_mode: str,
                 on_join_change, on_set_active, on_remove_group,
                 entry_filter_text: Optional[str] = None,
                 on_edit_entry_filter: Optional[Callable[[], None]] = None):
        super().__init__(parent, bd=2, relief="groove", bg="white", highlightthickness=2, highlightbackground=C_BORDER_IDLE)
        self.tab_name = tab_name
        self.idx = idx
        self.on_set_active = on_set_active
        self.on_remove_group = on_remove_group
        self.on_join_change = on_join_change
        self._on_edit_entry_filter = on_edit_entry_filter

        top = tk.Frame(self, bg="white")
        top.pack(side="top", fill="x")

        self.title = tk.Label(top, text=f"Group {idx+1} (Rules: {join_mode})", bg="white", fg=C_TEXT, font=("Segoe UI", 9, "bold"))
        self.title.pack(side="left", padx=8, pady=6)

        self.status = tk.Label(top, text="—", bg="white", fg=C_DIM, font=("Segoe UI", 9, "bold"))
        self.status.pack(side="left", padx=8)

        # Backwards-compat alias: earlier revisions used `status_lbl`.
        # Some code paths call set_group_status(), which expects this attribute.
        self.status_lbl = self.status

        self.entry_filter_badge = tk.Label(top, text="", bg="white", fg="#666666", font=("Segoe UI", 8, "bold"), padx=6, pady=2)
        self.entry_filter_badge.pack(side="right", padx=(8, 0))
        self.entry_filter_btn = None
        if on_edit_entry_filter is not None:
            self.entry_filter_btn = ttk.Button(top, text="Entry Filter…", command=on_edit_entry_filter)
            self.entry_filter_btn.pack(side="right", padx=(8, 0), pady=4)

        ttk.Label(top, text="Rules:", background="white").pack(side="right", padx=(6, 0))
        self.join_var = tk.StringVar(value=join_mode)
        cmb = ttk.Combobox(top, textvariable=self.join_var, values=["AND", "OR", "SEQUENCE"], width=10, state="readonly")
        cmb.pack(side="right", padx=6, pady=4)
        cmb.bind("<<ComboboxSelected>>", lambda e: self._join_changed())

        btn_rm = tk.Button(top, text="Remove", command=self.on_remove_group, padx=8)
        btn_rm.pack(side="right", padx=8, pady=4)

        # chip strip with horizontal scroll
        strip = tk.Frame(self, bg="white")
        strip.pack(side="top", fill="x", padx=8, pady=(0, 8))

        self.canvas = tk.Canvas(strip, height=40, bg="white", highlightthickness=0)
        self.canvas.pack(side="top", fill="x", expand=True)

        self.scroll = tk.Scrollbar(strip, orient="horizontal", command=self.canvas.xview)
        self.scroll.pack(side="bottom", fill="x")
        self.canvas.configure(xscrollcommand=self.scroll.set)

        self.inner = tk.Frame(self.canvas, bg="white")
        self.inner_id = self.canvas.create_window((0, 0), window=self.inner, anchor="nw")

        def _on_inner_config(event=None):
            self.canvas.configure(scrollregion=self.canvas.bbox("all"))
        self.inner.bind("<Configure>", _on_inner_config)

        def _on_canvas_config(event):
            self.canvas.itemconfigure(self.inner_id, height=event.height)
        self.canvas.bind("<Configure>", _on_canvas_config)

        # select group by clicking header/frame
        for w in (self, top, self.title, self.status):
            w.bind("<Button-1>", lambda e: self.on_set_active())

        self.chips: List[Chip] = []
        self.set_entry_filter(entry_filter_text, active=False)


    def set_entry_filter(self, text: Optional[str], active: bool = False):
        txt = (str(text or "")).strip()
        if not txt:
            try:
                self.entry_filter_badge.config(text="")
            except Exception:
                pass
            return
        bg = "#eef3ff" if active else "#f4f4f4"
        fg = "#1f4acc" if active else "#666666"
        try:
            self.entry_filter_badge.config(text=txt, bg=bg, fg=fg)
        except Exception:
            pass

    def _join_changed(self):
        mode = self.join_var.get()
        self.title.config(text=f"Group {self.idx+1} (Rules: {mode})")
        self.on_join_change(mode)

    def set_active(self, active: bool):
        self.config(highlightbackground=C_BORDER_ACTIVE if active else C_BORDER_IDLE)

    def set_group_status(self, satisfied: Optional[Any]):
        """satisfied can be None, bool, or a status string (ARMED/FIRED/READY/NO/WAIT)."""
        if satisfied is None:
            status = "WAIT"
        elif isinstance(satisfied, bool):
            status = "READY" if satisfied else "NO"
        else:
            status = str(satisfied).strip()

        up = status.upper()
        if up.startswith("READY"):
            self.status_lbl.config(text="STATUS: READY", bg=C_GREEN, fg="white")
        elif up.startswith("FIRED"):
            self.status_lbl.config(text=f"STATUS: {status}", bg=C_GREEN, fg="white")
        elif up.startswith("ARMED"):
            self.status_lbl.config(text="STATUS: ARMED", bg=C_AMBER, fg="black")
        elif up.startswith("SEQ"):
            self.status_lbl.config(text=f"STATUS: {status}", bg="#4f46e5", fg="white")
        elif up.startswith("NO"):
            self.status_lbl.config(text="STATUS: NO", bg=C_RED, fg="white")
        else:
            self.status_lbl.config(text="STATUS: WAIT", bg="#777", fg="white")

    def add_chip(self, chip: Chip):
        chip.pack(side="left", padx=4, pady=4)
        self.chips.append(chip)
