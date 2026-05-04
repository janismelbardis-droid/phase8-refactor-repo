from __future__ import annotations

from typing import Tuple

import tkinter as tk
from tkinter import ttk


class ScrollAndScreenMixin:
    """Scrollable frames, mousewheel binding, live table scroll, and initial window geometry."""

    def _create_scrollable_frame(self, parent, *, xscroll: bool = False, yscroll: bool = True, padding: int = 0) -> Tuple[ttk.Frame, ttk.Frame, tk.Canvas]:
        outer = ttk.Frame(parent)
        canvas = tk.Canvas(outer, highlightthickness=0, bd=0)
        canvas.grid(row=0, column=0, sticky="nsew")
        outer.grid_rowconfigure(0, weight=1)
        outer.grid_columnconfigure(0, weight=1)

        if yscroll:
            vsb = ttk.Scrollbar(outer, orient="vertical", command=canvas.yview)
            vsb.grid(row=0, column=1, sticky="ns")
            canvas.configure(yscrollcommand=vsb.set)
        if xscroll:
            hsb = ttk.Scrollbar(outer, orient="horizontal", command=canvas.xview)
            hsb.grid(row=1, column=0, sticky="ew")
            canvas.configure(xscrollcommand=hsb.set)

        inner = ttk.Frame(canvas, padding=padding)
        win = canvas.create_window((0, 0), window=inner, anchor="nw")

        def _sync_scrollregion(event=None):
            try:
                canvas.configure(scrollregion=canvas.bbox("all"))
            except Exception:
                pass

        def _sync_width(event):
            try:
                canvas.itemconfigure(win, width=event.width)
            except Exception:
                pass

        inner.bind("<Configure>", _sync_scrollregion)
        canvas.bind("<Configure>", _sync_width)

        def _wheel_units(event):
            delta = getattr(event, "delta", 0)
            if delta:
                try:
                    return int(-1 * (delta / 120)) or (-1 if delta > 0 else 1)
                except Exception:
                    return -1 if delta > 0 else 1
            num = getattr(event, "num", None)
            if num == 4:
                return -1
            if num == 5:
                return 1
            return 0

        def _on_wheel(event):
            units = _wheel_units(event)
            if units and yscroll:
                try:
                    canvas.yview_scroll(units, "units")
                except Exception:
                    pass
            return "break"

        def _on_shift_wheel(event):
            units = _wheel_units(event)
            if units and xscroll:
                try:
                    canvas.xview_scroll(units, "units")
                except Exception:
                    pass
            return "break"

        for widget in (outer, canvas, inner):
            try:
                widget.bind("<MouseWheel>", _on_wheel, add="+")
                widget.bind("<Shift-MouseWheel>", _on_shift_wheel, add="+")
                widget.bind("<Button-4>", _on_wheel, add="+")
                widget.bind("<Button-5>", _on_wheel, add="+")
            except Exception:
                pass

        return outer, inner, canvas

    def _bind_mousewheel_handlers(self, root, *, y_handler=None, x_handler=None):
        if root is None:
            return

        def _wheel_units(event):
            delta = getattr(event, "delta", 0)
            if delta:
                try:
                    return int(-1 * (delta / 120)) or (-1 if delta > 0 else 1)
                except Exception:
                    return -1 if delta > 0 else 1
            num = getattr(event, "num", None)
            if num == 4:
                return -1
            if num == 5:
                return 1
            return 0

        def _bind_tree(widget):
            try:
                already = bool(getattr(widget, '_ui_mwheel_bound', False))
            except Exception:
                already = False
            if not already:
                if y_handler is not None:
                    try:
                        widget.bind('<MouseWheel>', lambda e: y_handler(_wheel_units(e)), add='+')
                        widget.bind('<Button-4>', lambda e: y_handler(-1), add='+')
                        widget.bind('<Button-5>', lambda e: y_handler(1), add='+')
                    except Exception:
                        pass
                if x_handler is not None:
                    try:
                        widget.bind('<Shift-MouseWheel>', lambda e: x_handler(_wheel_units(e)), add='+')
                    except Exception:
                        pass
                try:
                    widget._ui_mwheel_bound = True
                except Exception:
                    pass
            try:
                children = widget.winfo_children()
            except Exception:
                children = []
            for child in children:
                _bind_tree(child)

        _bind_tree(root)

    def _scroll_live_table_y(self, units: int):
        try:
            canvas = getattr(self, '_live_table_canvas', None)
            if canvas is not None and units:
                canvas.yview_scroll(int(units), 'units')
        except Exception:
            pass
        return 'break'

    def _scroll_live_table_x(self, units: int):
        try:
            canvas = getattr(self, '_live_table_canvas', None)
            if canvas is not None and units:
                canvas.xview_scroll(int(units), 'units')
        except Exception:
            pass
        return 'break'

    def _scroll_rule_groups_canvas(self, canvas, units: int):
        try:
            if canvas is not None and units:
                canvas.yview_scroll(int(units), 'units')
        except Exception:
            pass
        return 'break'

    def _configure_window_for_screen(self) -> None:
        try:
            self.update_idletasks()
        except Exception:
            pass
        try:
            sw = int(self.winfo_screenwidth() or 0)
            sh = int(self.winfo_screenheight() or 0)
        except Exception:
            sw, sh = 1550, 1000

        width = max(1180, min(sw - 40, int(sw * (0.93 if sw >= 2200 else 0.96))))
        height = max(780, min(sh - 80, int(sh * (0.90 if sh >= 1200 else 0.92))))
        try:
            x = max(8, (sw - width) // 2)
            y = max(8, (sh - height) // 2)
            self.geometry(f"{width}x{height}+{x}+{y}")
        except Exception:
            pass

        compact = bool(sw < 2200 or sh < 1250)
        for fn, val in [
            (getattr(self, '_set_breakout_alert_collapsed', None), compact),
            (getattr(self, '_set_backtest_structure_collapsed', None), compact),
            (getattr(self, '_set_backtest_settings_collapsed', None), compact),
        ]:
            try:
                if callable(fn):
                    fn(val)
            except Exception:
                pass
        try:
            self.after(180, self._live_set_default_sash)
        except Exception:
            pass
        try:
            self.after(260, lambda: self._bt_set_default_sash(show_trades=True))
        except Exception:
            pass
