from __future__ import annotations

"""Tk dialogs for creating/editing rules."""

import tkinter as tk
from tkinter import ttk, messagebox, simpledialog
from typing import Any, Optional, List, Tuple

from .constants import *
from .rules import Rule, EntryFilterConfig

class DotRuleDialog(tk.Toplevel):
    """Binary (GREEN/RED) state rule editor."""
    def __init__(self, parent, title: str, default_op: str, default_val: str):
        super().__init__(parent)
        self.title(title)
        self.resizable(False, False)
        self.result: Optional[Tuple[str, str]] = None
        self.grab_set()

        frm = ttk.Frame(self, padding=12)
        frm.pack(fill="both", expand=True)

        ttk.Label(frm, text="Operator:").grid(row=0, column=0, sticky="w")
        self.op_var = tk.StringVar(value=default_op)
        ttk.Combobox(frm, textvariable=self.op_var, values=["=", "!="], width=6, state="readonly").grid(
            row=0, column=1, padx=8, sticky="w"
        )

        ttk.Label(frm, text="Value:").grid(row=1, column=0, sticky="w", pady=(10, 0))
        self.val_var = tk.StringVar(value=default_val)
        ttk.Combobox(frm, textvariable=self.val_var, values=["GREEN", "RED"], width=10, state="readonly").grid(
            row=1, column=1, padx=8, sticky="w", pady=(10, 0)
        )

        btns = ttk.Frame(frm)
        btns.grid(row=2, column=0, columnspan=2, pady=(14, 0), sticky="e")
        ttk.Button(btns, text="Cancel", command=self._cancel).pack(side="right", padx=6)
        ttk.Button(btns, text="OK", command=self._ok).pack(side="right")

        self.bind("<Return>", lambda e: self._ok())
        self.bind("<Escape>", lambda e: self._cancel())

    def _ok(self):
        op = self.op_var.get().strip()
        val = self.val_var.get().strip()
        self.result = (op, val)
        self.destroy()

    def _cancel(self):
        self.result = None
        self.destroy()

class EventRuleDialog(tk.Toplevel):
    """Binary event rule editor (TURNED GREEN/RED, FLIPPED) or CROSS UP/DOWN."""
    def __init__(self, parent, title: str, choices: List[str], default_choice: str):
        super().__init__(parent)
        self.title(title)
        self.resizable(False, False)
        self.result: Optional[str] = None
        self.grab_set()

        frm = ttk.Frame(self, padding=12)
        frm.pack(fill="both", expand=True)

        ttk.Label(frm, text="Event:").grid(row=0, column=0, sticky="w")
        self.var = tk.StringVar(value=default_choice)
        ttk.Combobox(frm, textvariable=self.var, values=choices, width=18, state="readonly").grid(
            row=0, column=1, padx=8, sticky="w"
        )

        btns = ttk.Frame(frm)
        btns.grid(row=1, column=0, columnspan=2, pady=(14, 0), sticky="e")
        ttk.Button(btns, text="Cancel", command=self._cancel).pack(side="right", padx=6)
        ttk.Button(btns, text="OK", command=self._ok).pack(side="right")

        self.bind("<Return>", lambda e: self._ok())
        self.bind("<Escape>", lambda e: self._cancel())

    def _ok(self):
        self.result = self.var.get()
        self.destroy()

    def _cancel(self):
        self.result = None
        self.destroy()



class BarsAgoDialog(tk.Toplevel):
    """Configure bars-ago constraint for an EVENT rule.

    Modes:
      - OFF: rule behaves like a classic event (true only when it fires now)
      - EXACT N: true when the event last fired exactly N steps ago (N=0 means now)
      - WITHIN N: true when the event last fired within the last N steps (includes 0..N)
    """
    def __init__(self, parent, title: str, default_mode: str = "OFF", default_n: int = 0, default_require_now: bool = False):
        super().__init__(parent)
        self.title(title)
        self.resizable(False, False)
        self.result: Optional[Tuple[str, int, bool]] = None
        self.grab_set()

        frm = ttk.Frame(self, padding=12)
        frm.pack(fill="both", expand=True)

        mode = (default_mode or "OFF").upper().strip()
        if mode not in ("OFF", "EXACT", "WITHIN"):
            mode = "OFF"

        self.mode_var = tk.StringVar(value=mode)
        self.n_var = tk.IntVar(value=max(0, int(default_n or 0)))
        self.require_var = tk.BooleanVar(value=bool(default_require_now))

        ttk.Label(frm, text="Bars-ago mode:").grid(row=0, column=0, sticky="w")

        modes = ttk.Frame(frm)
        modes.grid(row=1, column=0, columnspan=2, sticky="w", pady=(6, 0))

        rb_off = ttk.Radiobutton(modes, text="OFF (only when event fires now)", value="OFF", variable=self.mode_var, command=self._sync)
        rb_ex = ttk.Radiobutton(modes, text="EXACT", value="EXACT", variable=self.mode_var, command=self._sync)
        rb_wi = ttk.Radiobutton(modes, text="WITHIN last", value="WITHIN", variable=self.mode_var, command=self._sync)
        rb_off.grid(row=0, column=0, columnspan=3, sticky="w")
        rb_ex.grid(row=1, column=0, sticky="w", pady=(6, 0))
        rb_wi.grid(row=2, column=0, sticky="w", pady=(6, 0))

        self.n_spin = ttk.Spinbox(modes, from_=0, to=999, textvariable=self.n_var, width=6)
        self.n_spin.grid(row=1, column=1, padx=(10, 0), sticky="w")

        ttk.Label(modes, text="bars").grid(row=1, column=2, padx=(6, 0), sticky="w")

        self.n_spin2 = ttk.Spinbox(modes, from_=0, to=999, textvariable=self.n_var, width=6)
        self.n_spin2.grid(row=2, column=1, padx=(10, 0), sticky="w")
        ttk.Label(modes, text="bars").grid(row=2, column=2, padx=(6, 0), sticky="w")

        # Keep only one spinbox enabled depending on selection
        self._sync()

        self.require_chk = ttk.Checkbutton(frm, text="Require still valid now (at entry)", variable=self.require_var)
        self.require_chk.grid(row=2, column=0, columnspan=2, sticky="w", pady=(10, 0))

        btns = ttk.Frame(frm)
        btns.grid(row=3, column=0, columnspan=2, pady=(14, 0), sticky="e")
        ttk.Button(btns, text="Cancel", command=self._cancel).pack(side="right", padx=6)
        ttk.Button(btns, text="OK", command=self._ok).pack(side="right")

        self.bind("<Return>", lambda e: self._ok())
        self.bind("<Escape>", lambda e: self._cancel())

    def _sync(self):
        m = (self.mode_var.get() or "OFF").upper().strip()
        if m == "EXACT":
            self.n_spin.state(["!disabled"])
            self.n_spin2.state(["disabled"])
        elif m == "WITHIN":
            self.n_spin.state(["disabled"])
            self.n_spin2.state(["!disabled"])
        else:
            self.n_spin.state(["disabled"])
            self.n_spin2.state(["disabled"])

        # Only meaningful when bars-ago is enabled
        try:
            if m == "OFF":
                self.require_chk.state(["disabled"])
            else:
                self.require_chk.state(["!disabled"])
        except Exception:
            pass

    def _ok(self):
        m = (self.mode_var.get() or "OFF").upper().strip()
        n = max(0, int(self.n_var.get() or 0))
        if m not in ("OFF", "EXACT", "WITHIN"):
            m = "OFF"
        self.result = (m, n, bool(self.require_var.get()))
        self.destroy()

    def _cancel(self):
        self.result = None
        self.destroy()

class NumericRuleDialog(tk.Toplevel):
    def __init__(self, parent, title: str, default_op: str, default_value: str):
        super().__init__(parent)
        self.title(title)
        self.resizable(False, False)
        self.result: Optional[Tuple[str, str]] = None
        self.grab_set()

        frm = ttk.Frame(self, padding=12)
        frm.pack(fill="both", expand=True)

        ttk.Label(frm, text="Operator:").grid(row=0, column=0, sticky="w")
        self.op_var = tk.StringVar(value=default_op)
        ttk.Combobox(frm, textvariable=self.op_var, values=["<", "<=", "=", ">=", ">", "!="], width=6, state="readonly").grid(
            row=0, column=1, padx=8, sticky="w"
        )

        ttk.Label(frm, text="Value:").grid(row=1, column=0, sticky="w", pady=(10, 0))
        self.val_var = tk.StringVar(value=default_value)
        ttk.Entry(frm, textvariable=self.val_var, width=16).grid(row=1, column=1, padx=8, sticky="w", pady=(10, 0))

        btns = ttk.Frame(frm)
        btns.grid(row=2, column=0, columnspan=2, pady=(14, 0), sticky="e")
        ttk.Button(btns, text="Cancel", command=self._cancel).pack(side="right", padx=6)
        ttk.Button(btns, text="OK", command=self._ok).pack(side="right")

        self.bind("<Return>", lambda e: self._ok())
        self.bind("<Escape>", lambda e: self._cancel())

    def _ok(self):
        op = self.op_var.get().strip()
        val = self.val_var.get().strip()
        if not val:
            messagebox.showerror("Error", "Value cannot be empty.")
            return
        self.result = (op, val)
        self.destroy()

    def _cancel(self):
        self.result = None
        self.destroy()


class EntryFilterDialog(tk.Toplevel):
    """Compact editor for the per-entry-tab execution filter."""

    def __init__(self, parent, title: str, cfg: Optional[EntryFilterConfig] = None):
        super().__init__(parent)
        self.title(title)
        self.resizable(False, False)
        self.result: Optional[EntryFilterConfig] = None
        self.grab_set()

        cfg = (cfg or EntryFilterConfig()).normalized_copy()

        frm = ttk.Frame(self, padding=12)
        frm.pack(fill="both", expand=True)

        self.enabled_var = tk.BooleanVar(value=bool(cfg.enabled))
        self.mode_var = tk.StringVar(value=str(cfg.mode or "OFF"))
        self.expansion_var = tk.DoubleVar(value=float(cfg.expansion_atr_mult))
        self.hard_skip_var = tk.DoubleVar(value=float(cfg.hard_skip_atr_mult))
        self.confirm_bars_var = tk.IntVar(value=int(cfg.confirm_bars))
        self.max_retrace_var = tk.DoubleVar(value=float(cfg.max_retrace_pct))
        self.midpoint_var = tk.BooleanVar(value=bool(cfg.require_next_close_beyond_mid))
        self.setup_valid_var = tk.BooleanVar(value=bool(cfg.require_setup_still_valid))
        self.reversals_var = tk.BooleanVar(value=bool(cfg.apply_to_reversals))

        ttk.Checkbutton(frm, text="Enable entry filter", variable=self.enabled_var, command=self._sync).grid(row=0, column=0, columnspan=4, sticky="w")

        ttk.Label(frm, text="Mode:").grid(row=1, column=0, sticky="w", pady=(10, 0))
        ttk.Combobox(frm, textvariable=self.mode_var, values=["OFF", "ANTI_CHASE"], width=18, state="readonly").grid(row=1, column=1, sticky="w", padx=8, pady=(10, 0))

        hint = ttk.Label(frm, text="Anti-chase only delays expansion candles. Normal entries keep current behavior.", foreground="#666")
        hint.grid(row=2, column=0, columnspan=4, sticky="w", pady=(8, 0))

        row = 3
        ttk.Label(frm, text="Expansion threshold (ATR×):").grid(row=row, column=0, sticky="w", pady=(10, 0))
        ttk.Entry(frm, textvariable=self.expansion_var, width=10).grid(row=row, column=1, sticky="w", padx=8, pady=(10, 0))

        ttk.Label(frm, text="Hard skip threshold (ATR×):").grid(row=row, column=2, sticky="w", pady=(10, 0))
        ttk.Entry(frm, textvariable=self.hard_skip_var, width=10).grid(row=row, column=3, sticky="w", padx=8, pady=(10, 0))

        row += 1
        ttk.Label(frm, text="Confirmation bars:").grid(row=row, column=0, sticky="w", pady=(10, 0))
        ttk.Spinbox(frm, from_=1, to=20, textvariable=self.confirm_bars_var, width=8).grid(row=row, column=1, sticky="w", padx=8, pady=(10, 0))

        ttk.Label(frm, text="Max retrace into signal (%):").grid(row=row, column=2, sticky="w", pady=(10, 0))
        ttk.Entry(frm, textvariable=self.max_retrace_var, width=10).grid(row=row, column=3, sticky="w", padx=8, pady=(10, 0))

        row += 1
        ttk.Checkbutton(frm, text="Require confirmation close beyond signal midpoint", variable=self.midpoint_var).grid(row=row, column=0, columnspan=4, sticky="w", pady=(10, 0))

        row += 1
        ttk.Checkbutton(frm, text="Require setup still valid on confirmation bar", variable=self.setup_valid_var).grid(row=row, column=0, columnspan=4, sticky="w", pady=(6, 0))

        row += 1
        ttk.Checkbutton(frm, text="Apply filter to reverse entries too", variable=self.reversals_var).grid(row=row, column=0, columnspan=4, sticky="w", pady=(6, 0))

        row += 1
        foot = ttk.Label(frm, text="The filter evaluates only after the rules already gave a green light on the current entry bar. Bars Ago stays untouched.", foreground="#666")
        foot.grid(row=row, column=0, columnspan=4, sticky="w", pady=(10, 0))

        btns = ttk.Frame(frm)
        btns.grid(row=row + 1, column=0, columnspan=4, pady=(14, 0), sticky="e")
        ttk.Button(btns, text="Cancel", command=self._cancel).pack(side="right", padx=6)
        ttk.Button(btns, text="OK", command=self._ok).pack(side="right")

        self.bind("<Return>", lambda e: self._ok())
        self.bind("<Escape>", lambda e: self._cancel())
        self._sync()

    def _sync(self):
        active = bool(self.enabled_var.get()) and str(self.mode_var.get() or "OFF").upper() != "OFF"
        state = (["!disabled"] if active else ["disabled"])
        # ttk widgets do not expose a uniform iteration API we can trust by type here,
        # so toggle by walking children and keeping the main switches enabled.
        for child in self.winfo_children():
            pass

    def _ok(self):
        try:
            cfg = EntryFilterConfig(
                enabled=bool(self.enabled_var.get()),
                mode=str(self.mode_var.get() or "OFF"),
                expansion_atr_mult=float(self.expansion_var.get()),
                hard_skip_atr_mult=float(self.hard_skip_var.get()),
                confirm_bars=int(self.confirm_bars_var.get()),
                max_retrace_pct=float(self.max_retrace_var.get()),
                require_next_close_beyond_mid=bool(self.midpoint_var.get()),
                require_setup_still_valid=bool(self.setup_valid_var.get()),
                apply_to_reversals=bool(self.reversals_var.get()),
            ).normalized_copy()
        except Exception:
            messagebox.showerror("Invalid value", "Please check the entry filter numbers.")
            return
        if cfg.hard_skip_atr_mult < cfg.expansion_atr_mult:
            messagebox.showerror("Invalid value", "Hard skip threshold must be greater than or equal to the expansion threshold.")
            return
        self.result = cfg
        self.destroy()

    def _cancel(self):
        self.result = None
        self.destroy()


class MarketStateThresholdsDialog(tk.Toplevel):
    """Editable market-state threshold dialog.

    Returns a dict[str, str] in ``result`` when OK is pressed.
    """

    FIELD_SPECS = [
        ("vidya_angle_up_deg", "VIDYA angle UP ≥", "°"),
        ("vidya_angle_down_deg", "VIDYA angle DOWN ≤", "°"),
        ("vidya_angle_strong_deg", "VIDYA angle STRONG ≥", "°"),
        ("vidya_delta_support_pct", "VIDYA ΔVol support ≥", "%"),
        ("vidya_delta_weak_pct", "VIDYA ΔVol weak <", "%"),
        ("direction_long_min", "Direction score LONG ≥", "pts"),
        ("direction_short_max", "Direction score SHORT ≤", "pts"),
        ("compression_direction_abs_max", "Compression |direction| ≤", "pts"),
        ("compression_macd_gap_norm_max", "Compression MACD gap ≤", "norm"),
        ("confidence_base", "Confidence base", "pts"),
        ("expansion_confirmation_min", "Expansion breakout confirm ≥", "frac"),
        ("expansion_pullback_floor", "Expansion demote → Pullback floor", "pts"),
        ("expansion_exhaustion_cap", "Expansion exhaustion cap", "pts"),
        ("expansion_max_weakness_score", "Expansion max weakness", "pts"),
    ]

    def __init__(self, parent, values: dict[str, object], *, title: str = "Market State Threshold Lab"):
        super().__init__(parent)
        self.title(title)
        self.resizable(False, False)
        self.result: Optional[dict[str, str]] = None
        self.grab_set()

        frm = ttk.Frame(self, padding=12)
        frm.pack(fill="both", expand=True)

        ttk.Label(
            frm,
            text="Tune how Market State labels are assigned. These values do not change indicator formulas; they only change the state classifier.",
            foreground="#666",
            justify="left",
            wraplength=620,
        ).grid(row=0, column=0, columnspan=4, sticky="w")

        self.vars: dict[str, tk.StringVar] = {}
        row = 1
        for idx, (key, label, unit) in enumerate(self.FIELD_SPECS):
            col = 0 if idx % 2 == 0 else 2
            if col == 0 and idx > 0:
                row += 1
            ttk.Label(frm, text=f"{label}:").grid(row=row, column=col, sticky="w", pady=(10, 0))
            var = tk.StringVar(value=str(values.get(key, "")))
            self.vars[key] = var
            ttk.Entry(frm, textvariable=var, width=12).grid(row=row, column=col + 1, sticky="w", padx=8, pady=(10, 0))
            ttk.Label(frm, text=unit, foreground="#666").grid(row=row, column=col + 1, sticky="e", padx=(0, 6), pady=(10, 0))

        hint = ttk.Label(
            frm,
            text=(
                "Typical workflow: Preview → inspect diagnostics on your loaded history/backtest → Apply to current session only. "
                "Reset restores the built-in defaults."
            ),
            foreground="#666",
            justify="left",
            wraplength=620,
        )
        hint.grid(row=row + 1, column=0, columnspan=4, sticky="w", pady=(12, 0))

        btns = ttk.Frame(frm)
        btns.grid(row=row + 2, column=0, columnspan=4, pady=(16, 0), sticky="e")
        ttk.Button(btns, text="Reset Defaults", command=self._reset_defaults).pack(side="left")
        ttk.Button(btns, text="Cancel", command=self._cancel).pack(side="right", padx=6)
        ttk.Button(btns, text="OK", command=self._ok).pack(side="right")

        self.bind("<Return>", lambda e: self._ok())
        self.bind("<Escape>", lambda e: self._cancel())

    def _reset_defaults(self):
        from .indicators_streaming import market_state_thresholds_dict

        defaults = market_state_thresholds_dict()
        for key, var in self.vars.items():
            try:
                var.set(str(defaults.get(key, "")))
            except Exception:
                pass

    def _ok(self):
        self.result = {key: var.get().strip() for key, var in self.vars.items()}
        self.destroy()

    def _cancel(self):
        self.result = None
        self.destroy()
