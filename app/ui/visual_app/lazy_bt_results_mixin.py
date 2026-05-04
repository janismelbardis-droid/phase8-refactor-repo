from __future__ import annotations

import tkinter as tk
from tkinter import ttk


class LazyBtResultsMixin:
    """Lazy-built summary + trades table on the backtest tab (Phase UI7)."""

    def _build_bt_results_surface(self, parent) -> None:
        sum_box = ttk.LabelFrame(parent, text='Summary', padding=10)
        sum_box.pack(side='top', fill='x')
        self.bt_summary_txt = tk.Text(sum_box, height=7, state='disabled', font=('Consolas', 10))
        self.bt_summary_txt.pack(side='top', fill='x', expand=False)

        tbl_box = ttk.LabelFrame(parent, text='Trades (click to inspect; double-click for full details)', padding=10)
        tbl_box.pack(side='top', fill='both', expand=True, pady=(10, 0))
        cols = ['ID', 'Engine', 'Fill Src', 'Side', 'Signal Time', 'Entry Time', 'Exit Time', 'Entry', 'Exit', 'SL', 'TP', 'Net PnL', 'Return %', 'Dur (m)', 'Sig→Ent (s)', 'Entry Reason', 'Exit Reason', 'Ambig']
        self.bt_tree = ttk.Treeview(tbl_box, columns=cols, show='headings', height=16)
        for c in cols:
            self.bt_tree.heading(c, text=c)
        self.bt_tree.column('ID', width=50, anchor='center')
        self.bt_tree.column('Engine', width=90, anchor='center')
        self.bt_tree.column('Fill Src', width=90, anchor='center')
        self.bt_tree.column('Side', width=70, anchor='center')
        self.bt_tree.column('Signal Time', width=160, anchor='w')
        self.bt_tree.column('Entry Time', width=160, anchor='w')
        self.bt_tree.column('Exit Time', width=160, anchor='w')
        self.bt_tree.column('Entry', width=90, anchor='e')
        self.bt_tree.column('Exit', width=90, anchor='e')
        self.bt_tree.column('SL', width=90, anchor='e')
        self.bt_tree.column('TP', width=90, anchor='e')
        self.bt_tree.column('Net PnL', width=90, anchor='e')
        self.bt_tree.column('Return %', width=80, anchor='e')
        self.bt_tree.column('Dur (m)', width=70, anchor='e')
        self.bt_tree.column('Sig→Ent (s)', width=95, anchor='e')
        self.bt_tree.column('Entry Reason', width=160, anchor='w')
        self.bt_tree.column('Exit Reason', width=160, anchor='w')
        self.bt_tree.column('Ambig', width=70, anchor='center')
        vs = ttk.Scrollbar(tbl_box, orient='vertical', command=self.bt_tree.yview)
        hs = ttk.Scrollbar(tbl_box, orient='horizontal', command=self.bt_tree.xview)
        self.bt_tree.configure(yscrollcommand=vs.set, xscrollcommand=hs.set)
        self.bt_tree.grid(row=0, column=0, sticky='nsew')
        vs.grid(row=0, column=1, sticky='ns')
        hs.grid(row=1, column=0, sticky='ew')
        tbl_box.grid_rowconfigure(0, weight=1)
        tbl_box.grid_columnconfigure(0, weight=1)
        self.bt_tree.bind('<Double-1>', self._on_trade_double_click)
        self.bt_tree.bind('<<TreeviewSelect>>', self._on_trade_select)

    def _ensure_bt_results_surface_built(self) -> None:
        try:
            if bool(getattr(self, '_bt_results_surface_built', False)):
                return
        except Exception:
            pass
        parent = getattr(self, '_bt_results_parent', None)
        if parent is None:
            return
        placeholder = getattr(self, '_bt_results_placeholder', None)
        if placeholder is not None:
            try:
                placeholder.destroy()
            except Exception:
                pass
            self._bt_results_placeholder = None
        self._build_bt_results_surface(parent)
        self._bt_results_surface_built = True
