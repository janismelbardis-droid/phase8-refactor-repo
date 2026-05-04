from __future__ import annotations

"""Lazy live tab / main notebook / market-state popup shell helpers."""

import tkinter as tk
from tkinter import ttk


class LiveShellMixin:
    """Live layout refresh, lazy workspace/top builds, main tab selection, market-state board popup."""

    def _queue_live_layout_refresh(self) -> None:
        try:
            job = getattr(self, '_live_layout_refresh_job', None)
            if job is not None:
                self.after_cancel(job)
        except Exception:
            pass
        try:
            self._live_layout_refresh_job = self.after(40, self._refresh_live_layout)
        except Exception:
            self._live_layout_refresh_job = None

    def _refresh_live_layout(self) -> None:
        self._live_layout_refresh_job = None
        try:
            self.update_idletasks()
        except Exception:
            pass
        try:
            canvas = getattr(self, '_live_top_canvas', None)
            if canvas is not None:
                canvas.configure(scrollregion=canvas.bbox('all'))
        except Exception:
            pass
        try:
            self._live_set_default_sash()
        except Exception:
            pass

    def _live_set_default_sash(self) -> None:
        try:
            pw = getattr(self, 'live_pane', None)
            top_outer = getattr(self, '_live_top_outer', None)
            bot = getattr(self, '_live_bottom_frame', None)
            if pw is None or top_outer is None or bot is None:
                return
            self.update_idletasks()
            h = int(pw.winfo_height() or 0)
            if h <= 80:
                return
            compact = bool((self.winfo_width() or 0) < 2200 or (self.winfo_height() or 0) < 1250)
            top_req = int(top_outer.winfo_reqheight() or 0)
            bot_req = int(bot.winfo_reqheight() or 0)
            top_min = 180 if compact else 260
            bottom_min = 360 if compact else 260
            if bot_req > 0:
                bottom_min = max(bottom_min, min(bot_req, int(h * (0.68 if compact else 0.50))))
            max_top = max(top_min, h - bottom_min)
            if compact:
                preferred = max(top_min, min(max_top, min(top_req or int(h * 0.34), int(h * 0.42))))
            else:
                preferred = max(top_min, min(max_top, max(top_req, int(h * 0.48))))
            pos = max(top_min, min(max_top, preferred))
            pw.sashpos(0, pos)
        except Exception:
            pass

    def _set_breakout_alert_collapsed(self, collapsed: bool) -> None:
        collapsed = bool(collapsed)
        try:
            self.breakout_alert_collapsed_var.set(collapsed)
        except Exception:
            pass
        outer = getattr(self, '_breakout_alert_outer', None)
        btn = getattr(self, '_breakout_alert_toggle_btn', None)
        toolbar_btn = getattr(self, 'btn_breakout_panel_toggle', None)
        table_outer = getattr(self, '_live_table_outer', None)
        state_outer = getattr(self, '_market_state_outer', None)
        if outer is not None:
            try:
                if collapsed:
                    if outer.winfo_manager():
                        outer.pack_forget()
                else:
                    if not outer.winfo_manager():
                        before = state_outer if state_outer is not None and state_outer.winfo_manager() else table_outer
                        pack_kwargs = dict(side='top', fill='x', padx=6, pady=(6, 0))
                        if before is not None:
                            outer.pack(before=before, **pack_kwargs)
                        else:
                            outer.pack(**pack_kwargs)
            except Exception:
                pass
        for widget in (btn, toolbar_btn):
            if widget is not None:
                try:
                    widget.configure(text=("Show" if collapsed else "Hide") + " breakout")
                except Exception:
                    pass
        self._queue_live_layout_refresh()

    def _toggle_breakout_alert(self) -> None:
        cur = False
        try:
            cur = bool(self.breakout_alert_collapsed_var.get())
        except Exception:
            cur = False
        self._set_breakout_alert_collapsed(not cur)

    def _close_market_state_board_popup(self) -> None:
        try:
            self.market_state_collapsed_var.set(True)
        except Exception:
            pass
        win = getattr(self, '_market_state_popup', None)
        if win is not None:
            try:
                win.withdraw()
            except Exception:
                pass

    def _ensure_market_state_popup_built(self) -> None:
        built = False
        try:
            built = bool(getattr(self, '_market_state_popup_built', False))
        except Exception:
            built = False
        if built:
            return
        self._build_market_state_popup()

    def _ensure_backtest_tab_built(self) -> None:
        built = False
        try:
            built = bool(getattr(self, '_backtest_tab_built', False))
        except Exception:
            built = False
        if built:
            return
        placeholder = getattr(self, '_bt_lazy_placeholder', None)
        if placeholder is not None:
            try:
                placeholder.destroy()
            except Exception:
                pass
            self._bt_lazy_placeholder = None
        self._build_backtest_tab()

    def _ensure_rule_tab_built(self, tab_name: str) -> None:
        try:
            built = bool(getattr(self, '_rule_tabs_built', {}).get(tab_name, False))
        except Exception:
            built = False
        if built:
            return
        frm = None
        try:
            frm = getattr(self, 'rules_tab_frames', {}).get(tab_name)
        except Exception:
            frm = None
        if frm is None:
            return
        placeholder = None
        try:
            placeholder = getattr(self, '_rule_tab_placeholders', {}).get(tab_name)
        except Exception:
            placeholder = None
        if placeholder is not None:
            try:
                placeholder.destroy()
            except Exception:
                pass
            try:
                self._rule_tab_placeholders[tab_name] = None
            except Exception:
                pass
        self._build_rule_tab(frm, tab_name)
        try:
            self._rule_tabs_built[tab_name] = True
        except Exception:
            pass
        try:
            if tab_name in getattr(self, 'tab_group_join_var', {}):
                self.tab_group_join_var[tab_name].set(self.tab_group_join_mode.get(tab_name, 'AND'))
        except Exception:
            pass
        self._ensure_at_least_one_group(tab_name)
        try:
            self._refresh_entry_filter_badges(tab_name)
        except Exception:
            pass

    def _schedule_initial_live_workspace_build(self) -> None:
        if bool(getattr(self, '_live_workspace_tabs_built', {}).get('builder', False)):
            return
        if getattr(self, '_live_workspace_build_job', None) is not None:
            return
        try:
            self._live_workspace_build_job = self.after_idle(self._build_initial_live_workspace_tab)
        except Exception:
            self._build_initial_live_workspace_tab()

    def _build_initial_live_workspace_tab(self) -> None:
        try:
            self._live_workspace_build_job = None
        except Exception:
            pass
        try:
            self._ensure_live_workspace_tab_built('builder')
        except Exception:
            pass

    def _ensure_live_workspace_tab_built(self, tab_key: str) -> None:
        try:
            built = bool(getattr(self, '_live_workspace_tabs_built', {}).get(tab_key, False))
        except Exception:
            built = False
        if built:
            return
        parent = None
        try:
            parent = getattr(self, '_live_workspace_tab_frames', {}).get(tab_key)
        except Exception:
            parent = None
        if parent is None:
            return
        placeholder = None
        try:
            placeholder = getattr(self, '_live_workspace_tab_placeholders', {}).get(tab_key)
        except Exception:
            placeholder = None
        if placeholder is not None:
            try:
                placeholder.destroy()
            except Exception:
                pass
            try:
                self._live_workspace_tab_placeholders[tab_key] = None
            except Exception:
                pass
        if tab_key == 'builder':
            self._build_rule_builder(parent)
        elif tab_key == 'log':
            self._build_log(parent)
        elif tab_key == 'alerts':
            self._build_breakout_alerts(parent)
        else:
            return
        try:
            self._live_workspace_tabs_built[tab_key] = True
        except Exception:
            pass

    def _on_live_workspace_tab_changed(self, event=None) -> None:
        tab_key = 'builder'
        try:
            nb = getattr(self, '_live_workspace_nb', None)
            if nb is None:
                return
            selected = nb.nametowidget(nb.select())
        except Exception:
            selected = getattr(self, 'tab_builder', None)
        try:
            frames = getattr(self, '_live_workspace_tab_frames', {})
            if selected is frames.get('log'):
                tab_key = 'log'
            elif selected is frames.get('alerts'):
                tab_key = 'alerts'
        except Exception:
            tab_key = 'builder'
        try:
            self._ensure_live_workspace_tab_built(tab_key)
        except Exception:
            pass

    def _schedule_initial_live_top_build(self) -> None:
        if bool(getattr(self, '_live_breakout_alert_built', False)) and bool(getattr(self, '_live_signal_table_built', False)):
            return
        if getattr(self, '_live_top_build_job', None) is not None:
            return
        try:
            self._live_top_build_job = self.after_idle(self._build_initial_live_top_surfaces)
        except Exception:
            self._build_initial_live_top_surfaces()

    def _build_initial_live_top_surfaces(self) -> None:
        try:
            self._live_top_build_job = None
        except Exception:
            pass
        try:
            self._ensure_live_breakout_alert_built()
        except Exception:
            pass
        if bool(getattr(self, '_live_signal_table_built', False)):
            return
        try:
            self.after(40, self._ensure_live_signal_table_built)
        except Exception:
            self._ensure_live_signal_table_built()

    def _ensure_live_breakout_alert_built(self) -> None:
        if bool(getattr(self, '_live_breakout_alert_built', False)):
            return
        parent = getattr(self, '_live_breakout_parent', None)
        if parent is None:
            return
        placeholder = None
        try:
            placeholder = getattr(self, '_live_top_placeholders', {}).get('breakout')
        except Exception:
            placeholder = None
        if placeholder is not None:
            try:
                placeholder.destroy()
            except Exception:
                pass
            try:
                self._live_top_placeholders['breakout'] = None
            except Exception:
                pass
        self._build_live_breakout_alert(parent)
        self._live_breakout_alert_built = True
        try:
            collapsed = bool(self.breakout_alert_collapsed_var.get())
        except Exception:
            collapsed = False
        self._set_breakout_alert_collapsed(collapsed)

    def _ensure_live_signal_table_built(self) -> None:
        if bool(getattr(self, '_live_signal_table_built', False)):
            return
        parent = getattr(self, '_live_table_parent', None)
        if parent is None:
            return
        placeholder = None
        try:
            placeholder = getattr(self, '_live_top_placeholders', {}).get('table')
        except Exception:
            placeholder = None
        if placeholder is not None:
            try:
                placeholder.destroy()
            except Exception:
                pass
            try:
                self._live_top_placeholders['table'] = None
            except Exception:
                pass
        self._build_live_signal_table(parent)
        self._live_signal_table_built = True
        try:
            self._queue_live_layout_refresh()
        except Exception:
            pass

    def _on_main_tab_changed(self, event=None):
        try:
            nb = getattr(self, 'main_nb', None)
            if nb is None:
                return
            current = nb.select()
            if not current:
                return
            selected = nb.nametowidget(current)
            if selected is getattr(self, 'tab_bt', None):
                self._ensure_backtest_tab_built()
        except Exception:
            pass

    def _open_market_state_board_popup(self) -> None:
        win = getattr(self, '_market_state_popup', None)
        if win is None:
            try:
                self._ensure_market_state_popup_built()
            except Exception:
                return
            win = getattr(self, '_market_state_popup', None)
        if win is None:
            return
        try:
            sw = int(self.winfo_screenwidth() or 0)
            sh = int(self.winfo_screenheight() or 0)
            width = max(1180, min(sw - 80, int(sw * 0.72) if sw else 1400))
            height = max(420, min(sh - 120, int(sh * 0.42) if sh else 460))
            x = max(20, (sw - width) // 2) if sw else 40
            y = max(40, int((sh - height) * 0.12)) if sh else 60
            win.geometry(f'{width}x{height}+{x}+{y}')
        except Exception:
            pass
        try:
            self.market_state_collapsed_var.set(False)
        except Exception:
            pass
        try:
            win.deiconify()
            win.lift()
            win.focus_force()
        except Exception:
            pass
        try:
            self._refresh_market_state_cards_from_latest()
        except Exception:
            pass

    def _set_market_state_board_collapsed(self, collapsed: bool) -> None:
        if collapsed:
            self._close_market_state_board_popup()
        else:
            self._open_market_state_board_popup()

    def _toggle_market_state_board(self) -> None:
        win = getattr(self, '_market_state_popup', None)
        visible = False
        if win is not None:
            try:
                visible = bool(win.state() != 'withdrawn')
            except Exception:
                try:
                    visible = bool(win.winfo_viewable())
                except Exception:
                    visible = False
        if visible:
            self._close_market_state_board_popup()
        else:
            self._open_market_state_board_popup()

