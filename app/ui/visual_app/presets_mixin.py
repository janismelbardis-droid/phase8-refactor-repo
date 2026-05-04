from __future__ import annotations

"""Preset save/load, entry filters, and market-state preset reports."""

import json
import os
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd
import tkinter as tk
from tkinter import filedialog, messagebox, simpledialog, ttk

from ...constants import *
from ...indicators_streaming import rebuild_market_state_streams
from ...preset_visualizer import (
    build_guided_threshold_tuning_report,
    build_market_state_outcome_report,
    build_market_state_stream_audit_report,
    build_preset_report,
    infer_preset_archetype,
)
from ...rules import EntryFilterConfig, Rule
from ...ui_dialogs import EntryFilterDialog


class PresetsMixin:
    """JSON presets, entry-filter editing, visualization / diagnostics dialogs."""

    def _presets_file_path(self) -> str:
        """Presets JSON lives in `app/presets.json` (same location as before extraction)."""
        return str(Path(__file__).resolve().parents[2] / "presets.json")

    @staticmethod
    def _json_safe(v: Any) -> Any:
        try:
            # numpy scalar
            if hasattr(v, "item") and callable(getattr(v, "item")):
                return v.item()
        except Exception:
            pass
        if isinstance(v, (pd.Timestamp,)):
            try:
                return v.isoformat()
            except Exception:
                return str(v)
        return v


    def _entry_filter_for_tab(self, tab_name: str) -> EntryFilterConfig:
        cfg = self.entry_filters.get(tab_name, EntryFilterConfig())
        try:
            return cfg.normalized_copy()
        except Exception:
            return EntryFilterConfig()

    def _entry_filter_summary(self, tab_name: str) -> str:
        try:
            return self._entry_filter_for_tab(tab_name).summary()
        except Exception:
            return "Off"

    def _refresh_entry_filter_badges(self, tab_name: Optional[str] = None):
        tabs = [tab_name] if tab_name else [t for t in RULE_TABS if t in ("Long Entry", "Short Entry")]
        for t in tabs:
            try:
                cfg = self._entry_filter_for_tab(t)
                badge = cfg.badge_text()
                active = bool(cfg.is_active())
                for gui in self.group_ui.get(t, []) or []:
                    try:
                        gui.set_entry_filter(badge, active=active)
                    except Exception:
                        pass
            except Exception:
                continue

    def _edit_entry_filter(self, tab_name: str):
        if tab_name not in ("Long Entry", "Short Entry"):
            return
        dlg = EntryFilterDialog(self, f"{tab_name} — Entry Filter", self._entry_filter_for_tab(tab_name))
        self.wait_window(dlg)
        if dlg.result is None:
            return
        self.entry_filters[tab_name] = dlg.result.normalized_copy()
        self._refresh_entry_filter_badges(tab_name)
        if self.active_tab == tab_name:
            try:
                self.builder_status.set(
                    f"Active: {tab_name} → Group {self.active_group_index.get(tab_name, 0)+1}. "
                    f"Rules in group: {self.group_rule_join_mode.get(tab_name, ['OR'])[self.active_group_index.get(tab_name, 0)] if self.group_rule_join_mode.get(tab_name) else 'OR'}. "
                    f"Groups in tab: {self.tab_group_join_mode.get(tab_name, 'AND')}. "
                    f"Entry filter: {self._entry_filter_summary(tab_name)}."
                )
            except Exception:
                pass

    def _load_presets_file(self) -> Tuple[Dict[str, Any], Dict[str, Any]]:
        path = self._presets_file_path()
        if not os.path.exists(path):
            return {}, {"autoload_last": False, "last_preset": ""}
        try:
            with open(path, "r", encoding="utf-8") as f:
                data = json.load(f)
            presets = data.get("presets", {}) if isinstance(data, dict) else {}
            meta = data.get("meta", {}) if isinstance(data, dict) else {}
            if not isinstance(presets, dict):
                presets = {}
            if not isinstance(meta, dict):
                meta = {}
            meta.setdefault("autoload_last", False)
            meta.setdefault("last_preset", "")
            return presets, meta
        except Exception:
            # Corrupt file – don't crash the app.
            return {}, {"autoload_last": False, "last_preset": ""}

    def _write_presets_file(self):
        path = self._presets_file_path()
        try:
            payload = {"meta": self._presets_meta, "presets": self._presets}
            with open(path, "w", encoding="utf-8") as f:
                json.dump(payload, f, indent=2, ensure_ascii=False, default=self._json_safe)
        except Exception:
            pass

    def _refresh_preset_combo(self, select_name: Optional[str] = None):
        names = sorted(list(self._presets.keys()))
        try:
            self.preset_cmb.configure(values=names)
        except Exception:
            pass
        if select_name and select_name in self._presets:
            try:
                self.preset_var.set(select_name)
            except Exception:
                pass
        elif self.preset_var.get() not in names:
            try:
                self.preset_var.set(names[0] if names else "")
            except Exception:
                pass

    def _collect_preset_dict(self) -> Dict[str, Any]:
        """Serialize current UI + rule builder into a JSON-friendly dict."""
        settings = {
            "symbol": self.symbol_var.get(),
            "start": self.start_var.get(),
            "end": self.end_var.get(),
            "warmup_min": int(self.warmup_var.get()),
            "cache_dir": self.cache_var.get(),
            "price_source": self.price_source_var.get(),
            "macd_impl": self.macd_impl_var.get(),
            "adx_impl": self.adx_impl_var.get(),
            "live_forming": bool(self.live_forming_var.get()),
            "latch_until_flip": bool(self.latch_until_flip_var.get()),
            "bt_mode": self.bt_mode_var.get(),
            "timeframes": {tf: bool(self.tf_vars[tf].get()) for tf in ALL_TIMEFRAMES},
        }

        tabs: Dict[str, Any] = {}
        for tab in RULE_TABS:
            groups = []
            for gi, grp in enumerate(self.rules.get(tab, [])):
                join_mode = "OR"
                try:
                    join_mode = self.group_rule_join_mode.get(tab, [])[gi]
                except Exception:
                    join_mode = "OR"
                rules = []
                for r in grp:
                    rules.append({
                        "timeframe": r.timeframe,
                        "mode": r.mode,
                        "field": r.field,
                        "op": r.op,
                        "value": self._json_safe(r.value),
                        "compare_to": self._json_safe(getattr(r, "compare_to", None)),
                        "is_trigger": bool(getattr(r, "is_trigger", False)),
                        "is_final_gate": bool(getattr(r, "is_final_gate", False)),
                        "is_sequence_canceler": bool(getattr(r, "is_sequence_canceler", False)),
                        "bars_ago_mode": str(getattr(r, "bars_ago_mode", "OFF")),
                        "bars_ago_n": int(getattr(r, "bars_ago_n", 0) or 0),
                        "window_bars": int(getattr(r, "window_bars", 1) or 1),
                    })
                groups.append({"rules_join": join_mode, "rules": rules})
            if not groups:
                groups = [{"rules_join": "OR", "rules": []}]
            tabs[tab] = {
                "groups_join": self.tab_group_join_mode.get(tab, "AND"),
                "groups": groups,
            }

        entry_filters = {
            "Long Entry": self._entry_filter_for_tab("Long Entry").to_dict(),
            "Short Entry": self._entry_filter_for_tab("Short Entry").to_dict(),
        }
        meta = {
            "strategy_note": str(getattr(self, "current_preset_note", "") or "").strip(),
        }
        return {"version": 5, "settings": settings, "tabs": tabs, "entry_filters": entry_filters, "meta": meta}

    def _apply_preset_dict(self, preset: Dict[str, Any]):
        """Apply a preset dict to UI + rule models."""
        if not isinstance(preset, dict):
            return

        # Avoid trace-triggered restarts while we set multiple vars.
        prev_ui_ready = getattr(self, "_ui_ready", False)
        self._ui_ready = False

        was_live = bool(self.live_engine and getattr(self.live_engine, "running", False))
        if was_live:
            try:
                self.on_toggle_live()
            except Exception:
                pass

        settings = preset.get("settings", {}) if isinstance(preset.get("settings", {}), dict) else {}
        meta = preset.get("meta", {}) if isinstance(preset.get("meta", {}), dict) else {}
        self.current_preset_note = str(meta.get("strategy_note") or "").strip()
        try:
            if "symbol" in settings:
                self.symbol_var.set(settings.get("symbol", "BTCUSDT"))
            if "start" in settings:
                self.start_var.set(settings.get("start", self.start_var.get()))
            if "end" in settings:
                self.end_var.set(settings.get("end", self.end_var.get()))
            if "warmup_min" in settings:
                self.warmup_var.set(int(settings.get("warmup_min", int(self.warmup_var.get()))))
            if "cache_dir" in settings:
                self.cache_var.set(settings.get("cache_dir", self.cache_var.get()))
            if "price_source" in settings:
                self.price_source_var.set(settings.get("price_source", self.price_source_var.get()))
            if "macd_impl" in settings:
                self.macd_impl_var.set(settings.get("macd_impl", self.macd_impl_var.get()))
            if "adx_impl" in settings:
                try:
                    self.adx_impl_var.set(settings.get("adx_impl", self.adx_impl_var.get()))
                except Exception:
                    pass
            if "live_forming" in settings:
                self.live_forming_var.set(bool(settings.get("live_forming", True)))
            if "latch_until_flip" in settings:
                self.latch_until_flip_var.set(bool(settings.get("latch_until_flip", True)))
            if "bt_mode" in settings:
                self.bt_mode_var.set(settings.get("bt_mode", self.bt_mode_var.get()))
                try:
                    self._refresh_backtest_structure_labels()
                except Exception:
                    pass
            tf_cfg = settings.get("timeframes", {})
            if isinstance(tf_cfg, dict):
                for tf in ALL_TIMEFRAMES:
                    if tf in tf_cfg:
                        self.tf_vars[tf].set(bool(tf_cfg.get(tf, True)))
        except Exception:
            pass

        # Entry filters (tab-level execution filters for entry tabs)
        ef_cfg = preset.get("entry_filters", {}) if isinstance(preset.get("entry_filters", {}), dict) else {}
        for _tab in ("Long Entry", "Short Entry"):
            try:
                self.entry_filters[_tab] = EntryFilterConfig.from_dict(ef_cfg.get(_tab, {}))
            except Exception:
                self.entry_filters[_tab] = EntryFilterConfig()

        # Rules
        tabs = preset.get("tabs", {}) if isinstance(preset.get("tabs", {}), dict) else {}
        for tab in RULE_TABS:
            tab_cfg = tabs.get(tab, {}) if isinstance(tabs.get(tab, {}), dict) else {}
            self.tab_group_join_mode[tab] = tab_cfg.get("groups_join", self.tab_group_join_mode.get(tab, "AND"))
            try:
                if tab in getattr(self, "tab_group_join_var", {}):
                    self.tab_group_join_var[tab].set(self.tab_group_join_mode[tab])
            except Exception:
                pass

            groups_cfg = tab_cfg.get("groups", [])
            if not isinstance(groups_cfg, list) or not groups_cfg:
                groups_cfg = [{"rules_join": "OR", "rules": []}]

            self.rules[tab] = []
            self.group_rule_join_mode[tab] = []
            self.group_latch[tab] = []
            self.group_latch_meta[tab] = []
            for g in groups_cfg:
                join = "OR"
                if isinstance(g, dict):
                    join = str(g.get("rules_join", "OR") or "OR").upper().strip()
                    if join not in ("AND", "OR", "SEQUENCE"):
                        join = "OR"
                self.group_rule_join_mode[tab].append(join)
                self.group_latch[tab].append(None)
                self.group_latch_meta[tab].append(None)
                rules_list = []
                if isinstance(g, dict) and isinstance(g.get("rules", []), list):
                    for rd in g.get("rules", []):
                        if not isinstance(rd, dict):
                            continue
                        try:
                            rules_list.append(Rule(
                                timeframe=str(rd.get("timeframe", "1m")),
                                mode=str(rd.get("mode", "state")),
                                field=str(rd.get("field", "ms")),
                                op=str(rd.get("op", "=")),
                                value=rd.get("value", None),
                                is_trigger=bool(rd.get("is_trigger", False)),
                                is_final_gate=bool(rd.get("is_final_gate", False)),
                                is_sequence_canceler=bool(rd.get("is_sequence_canceler", False)),
                                bars_ago_mode=str(rd.get("bars_ago_mode", "OFF")),
                                bars_ago_n=int(rd.get("bars_ago_n", 0) or 0),
                                window_bars=int(rd.get("window_bars", 1) or 1),
                                compare_to=rd.get("compare_to"),
                            ))
                        except Exception:
                            continue
                self.rules[tab].append(rules_list)

            self.active_group_index[tab] = 0
            try:
                self._rebuild_groups_ui(tab)
                self._set_active_group(tab, 0)
            except Exception:
                pass

        # Re-seed trigger latches from history if available
        self._latch_seed_dirty = True
        try:
            self._rule_eval_ctx.reset_all_sequences()
        except Exception:
            pass
        try:
            self._seed_all_group_latches_from_history()
        except Exception:
            pass

        try:
            self._update_rule_statuses()
        except Exception:
            pass

        try:
            self._refresh_entry_filter_badges()
        except Exception:
            pass

        self._ui_ready = prev_ui_ready

        # Optionally restart live if it was running
        if was_live:
            try:
                self.on_toggle_live()
            except Exception:
                pass

    def _selected_preset_note(self) -> str:
        try:
            name = (self.preset_var.get() or "").strip()
        except Exception:
            name = ""
        if name and name in getattr(self, "_presets", {}):
            preset = self._presets.get(name) or {}
            meta = preset.get("meta", {}) if isinstance(preset, dict) and isinstance(preset.get("meta", {}), dict) else {}
            return str(meta.get("strategy_note") or "").strip()
        return str(getattr(self, "current_preset_note", "") or "").strip()

    def _refresh_preset_note_widgets(self) -> None:
        note = self._selected_preset_note()
        try:
            self.preset_note_var.set(f"Preset note: {note}" if note else "Preset note: —")
        except Exception:
            pass

    def _on_preset_combo_changed(self, event=None):
        self._refresh_preset_note_widgets()

    def on_edit_preset_note(self):
        current = str(getattr(self, "current_preset_note", "") or "")
        note = simpledialog.askstring(
            "Preset Note",
            "Short human note / thesis for this preset:\n\nExample: Wait for first pullback after bullish VIDYA turn, then enter on MACD recovery.",
            initialvalue=current,
            parent=self,
        )
        if note is None:
            return
        self.current_preset_note = str(note).strip()
        try:
            self._refresh_preset_note_widgets()
        except Exception:
            pass
        try:
            self.status_var.set("Updated preset note")
        except Exception:
            pass

    def _resolve_preset_for_visualization(self) -> Tuple[str, Dict[str, Any]]:
        try:
            name = (self.preset_var.get() or "").strip()
        except Exception:
            name = ""
        if name and name in getattr(self, "_presets", {}):
            preset = self._presets.get(name) or {}
            return name, dict(preset)
        return "Current Unsaved Preset", self._collect_preset_dict()

    def on_visualize_preset(self):
        name, preset = self._resolve_preset_for_visualization()
        report = build_preset_report(preset, name, bt_result=getattr(self, "bt_result", None))
        archetype, one_liner, _ = infer_preset_archetype(preset)

        win = tk.Toplevel(self)
        win.title(f"Preset Visualization — {name}")
        win.geometry("980x760")

        header = ttk.Frame(win, padding=(10, 8, 10, 4))
        header.pack(side="top", fill="x")
        ttk.Label(header, text=name, font=("TkDefaultFont", 12, "bold")).pack(side="left")
        ttk.Label(header, text=f"{archetype} · {one_liner}", foreground="#555").pack(side="left", padx=(10, 0))

        body = ttk.Frame(win, padding=(10, 0, 10, 10))
        body.pack(side="top", fill="both", expand=True)

        txt = tk.Text(body, wrap="word")
        y = ttk.Scrollbar(body, orient="vertical", command=txt.yview)
        txt.configure(yscrollcommand=y.set)
        txt.pack(side="left", fill="both", expand=True)
        y.pack(side="right", fill="y")
        txt.insert("1.0", report)
        txt.configure(state="disabled")

        btns = ttk.Frame(win, padding=(10, 0, 10, 10))
        btns.pack(side="bottom", fill="x")

        def _copy():
            try:
                self.clipboard_clear()
                self.clipboard_append(report)
                self.update_idletasks()
                self.status_var.set(f"Copied preset report: {name}")
            except Exception:
                pass

        def _save_txt():
            path = filedialog.asksaveasfilename(
                parent=win,
                title="Save preset report as text",
                defaultextension=".txt",
                filetypes=[("Text files", "*.txt"), ("All files", "*.*")],
                initialfile=f"{name.replace(' ', '_')}_preset_report.txt",
            )
            if not path:
                return
            try:
                with open(path, "w", encoding="utf-8") as f:
                    f.write(report)
                self.status_var.set(f"Saved preset report: {os.path.basename(path)}")
            except Exception as e:
                messagebox.showerror("Save preset report", str(e), parent=win)

        ttk.Button(btns, text="Copy", command=_copy).pack(side="left")
        ttk.Button(btns, text="Save .txt…", command=_save_txt).pack(side="left", padx=(6, 0))
        ttk.Button(btns, text="State Outcomes…", command=self.on_show_market_state_outcomes).pack(side="left", padx=(6, 0))
        ttk.Button(btns, text="Close", command=win.destroy).pack(side="right")

    def on_show_market_state_outcomes(self):
        name, preset = self._resolve_preset_for_visualization()
        report = build_market_state_outcome_report(getattr(self, "bt_result", None), preset_name=name, preset=preset)

        win = tk.Toplevel(self)
        win.title(f"Market State Outcome Diagnostics — {name}")
        win.geometry("980x760")

        header = ttk.Frame(win, padding=(10, 8, 10, 4))
        header.pack(side="top", fill="x")
        ttk.Label(header, text=name, font=("TkDefaultFont", 12, "bold")).pack(side="left")
        ttk.Label(header, text="Backtest-driven state quality report", foreground="#555").pack(side="left", padx=(10, 0))

        body = ttk.Frame(win, padding=(10, 0, 10, 10))
        body.pack(side="top", fill="both", expand=True)

        txt = tk.Text(body, wrap="none")
        y = ttk.Scrollbar(body, orient="vertical", command=txt.yview)
        x = ttk.Scrollbar(body, orient="horizontal", command=txt.xview)
        txt.configure(yscrollcommand=y.set, xscrollcommand=x.set)
        y.pack(side="right", fill="y")
        x.pack(side="bottom", fill="x")
        txt.pack(side="left", fill="both", expand=True)
        txt.insert("1.0", report)
        txt.configure(state="disabled")

        btns = ttk.Frame(win, padding=(10, 0, 10, 10))
        btns.pack(side="bottom", fill="x")

        def _copy():
            try:
                self.clipboard_clear()
                self.clipboard_append(report)
                self.update_idletasks()
                self.status_var.set(f"Copied market-state outcome report: {name}")
            except Exception:
                pass

        def _save_txt():
            path = filedialog.asksaveasfilename(
                parent=win,
                title="Save market-state diagnostics as text",
                defaultextension=".txt",
                filetypes=[("Text files", "*.txt"), ("All files", "*.*")],
                initialfile=f"{name.replace(' ', '_')}_market_state_diagnostics.txt",
            )
            if not path:
                return
            try:
                with open(path, "w", encoding="utf-8") as f:
                    f.write(report)
                self.status_var.set(f"Saved market-state diagnostics: {os.path.basename(path)}")
            except Exception as e:
                messagebox.showerror("Save market-state diagnostics", str(e), parent=win)

        ttk.Button(btns, text="Copy", command=_copy).pack(side="left")
        ttk.Button(btns, text="Save .txt…", command=_save_txt).pack(side="left", padx=(6, 0))
        ttk.Button(btns, text="Tuning Assistant…", command=self.on_show_market_state_tuning_assistant).pack(side="left", padx=(6, 0))
        ttk.Button(btns, text="State Audit…", command=self.on_show_market_state_stream_audit).pack(side="left", padx=(6, 0))
        ttk.Button(btns, text="Close", command=win.destroy).pack(side="right")


    def on_show_market_state_stream_audit(self):
        name, _preset = self._resolve_preset_for_visualization()
        source_name, streams = self._current_market_state_source_streams()
        report = build_market_state_stream_audit_report(
            rebuild_market_state_streams(streams, thresholds=self._market_state_thresholds_obj()) if streams else streams,
            source_name=source_name,
            preset_name=name,
        )

        win = tk.Toplevel(self)
        win.title(f"Market State Empty Backtest / Weakness Audit — {name}")
        win.geometry("1060x780")

        header = ttk.Frame(win, padding=(10, 8, 10, 4))
        header.pack(side="top", fill="x")
        ttk.Label(header, text=name, font=("TkDefaultFont", 12, "bold")).pack(side="left")
        ttk.Label(header, text=f"{source_name} stream audit for the rectangles", foreground="#555").pack(side="left", padx=(10, 0))

        body = ttk.Frame(win, padding=(10, 0, 10, 10))
        body.pack(side="top", fill="both", expand=True)

        txt = tk.Text(body, wrap="none")
        y = ttk.Scrollbar(body, orient="vertical", command=txt.yview)
        x = ttk.Scrollbar(body, orient="horizontal", command=txt.xview)
        txt.configure(yscrollcommand=y.set, xscrollcommand=x.set)
        y.pack(side="right", fill="y")
        x.pack(side="bottom", fill="x")
        txt.pack(side="left", fill="both", expand=True)
        txt.insert("1.0", report)
        txt.configure(state="disabled")

        btns = ttk.Frame(win, padding=(10, 0, 10, 10))
        btns.pack(side="bottom", fill="x")

        def _copy():
            try:
                self.clipboard_clear()
                self.clipboard_append(report)
                self.update_idletasks()
                self.status_var.set(f"Copied market-state stream audit: {name}")
            except Exception:
                pass

        def _save_txt():
            path = filedialog.asksaveasfilename(
                parent=win,
                title="Save market-state stream audit as text",
                defaultextension=".txt",
                filetypes=[("Text files", "*.txt"), ("All files", "*.*")],
                initialfile=f"{name.replace(' ', '_')}_market_state_stream_audit.txt",
            )
            if not path:
                return
            try:
                with open(path, "w", encoding="utf-8") as f:
                    f.write(report)
                self.status_var.set(f"Saved market-state stream audit: {os.path.basename(path)}")
            except Exception as e:
                messagebox.showerror("Save market-state stream audit", str(e), parent=win)

        ttk.Button(btns, text="Copy", command=_copy).pack(side="left")
        ttk.Button(btns, text="Save .txt…", command=_save_txt).pack(side="left", padx=(6, 0))
        ttk.Button(btns, text="Threshold Lab…", command=self._open_market_state_threshold_lab).pack(side="left", padx=(6, 0))
        ttk.Button(btns, text="Close", command=win.destroy).pack(side="right")


    def on_show_market_state_tuning_assistant(self):
        name, preset = self._resolve_preset_for_visualization()
        report, advice = build_guided_threshold_tuning_report(
            getattr(self, "bt_result", None),
            current_thresholds=getattr(self, "market_state_threshold_values", None),
            preset_name=name,
            preset=preset,
        )
        suggested_values = dict((advice or {}).get("suggested") or {})

        win = tk.Toplevel(self)
        win.title(f"Guided Threshold Tuning Assistant — {name}")
        win.geometry("980x760")

        header = ttk.Frame(win, padding=(10, 8, 10, 4))
        header.pack(side="top", fill="x")
        ttk.Label(header, text=name, font=("TkDefaultFont", 12, "bold")).pack(side="left")
        ttk.Label(header, text="Backtest-driven threshold suggestions", foreground="#555").pack(side="left", padx=(10, 0))

        body = ttk.Frame(win, padding=(10, 0, 10, 10))
        body.pack(side="top", fill="both", expand=True)

        txt = tk.Text(body, wrap="none")
        y = ttk.Scrollbar(body, orient="vertical", command=txt.yview)
        x = ttk.Scrollbar(body, orient="horizontal", command=txt.xview)
        txt.configure(yscrollcommand=y.set, xscrollcommand=x.set)
        y.pack(side="right", fill="y")
        x.pack(side="bottom", fill="x")
        txt.pack(side="left", fill="both", expand=True)
        txt.insert("1.0", report)
        txt.configure(state="disabled")

        btns = ttk.Frame(win, padding=(10, 0, 10, 10))
        btns.pack(side="bottom", fill="x")

        def _copy():
            try:
                self.clipboard_clear()
                self.clipboard_append(report)
                self.update_idletasks()
                self.status_var.set(f"Copied tuning assistant report: {name}")
            except Exception:
                pass

        def _save_txt():
            path = filedialog.asksaveasfilename(
                parent=win,
                title="Save tuning assistant report as text",
                defaultextension=".txt",
                filetypes=[("Text files", "*.txt"), ("All files", "*.*")],
                initialfile=f"{name.replace(' ', '_')}_threshold_tuning_assistant.txt",
            )
            if not path:
                return
            try:
                with open(path, "w", encoding="utf-8") as f:
                    f.write(report)
                self.status_var.set(f"Saved tuning assistant report: {os.path.basename(path)}")
            except Exception as e:
                messagebox.showerror("Save tuning assistant report", str(e), parent=win)

        ttk.Button(btns, text="Copy", command=_copy).pack(side="left")
        ttk.Button(btns, text="Save .txt…", command=_save_txt).pack(side="left", padx=(6, 0))
        if suggested_values:
            ttk.Button(btns, text="Open in Threshold Lab…", command=lambda: [win.destroy(), self._show_market_state_threshold_preview(suggested_values)]).pack(side="left", padx=(6, 0))
            ttk.Button(btns, text="Apply Suggested to Session", command=lambda: [self._apply_market_state_thresholds_session(suggested_values), win.destroy()]).pack(side="left", padx=(6, 0))
        ttk.Button(btns, text="Close", command=win.destroy).pack(side="right")


    def _init_presets(self):
        self._presets, self._presets_meta = self._load_presets_file()
        try:
            self.autoload_last_preset_var.set(bool(self._presets_meta.get("autoload_last", False)))
        except Exception:
            pass
        self._refresh_preset_combo()
        # Auto-load last preset if requested
        last = str(self._presets_meta.get("last_preset", "") or "")
        if bool(self._presets_meta.get("autoload_last", False)) and last and last in self._presets:
            try:
                self._apply_preset_dict(self._presets[last])
                self.preset_var.set(last)
            except Exception:
                pass
        try:
            self._refresh_preset_note_widgets()
        except Exception:
            pass

    def _on_autoload_last_changed(self):
        self._presets_meta["autoload_last"] = bool(self.autoload_last_preset_var.get())
        # Keep current selection as last preset
        sel = (self.preset_var.get() or "").strip()
        if sel:
            self._presets_meta["last_preset"] = sel
        self._write_presets_file()

    def on_save_preset(self):
        cur = (self.preset_var.get() or "").strip()
        name = simpledialog.askstring("Save Preset", "Preset name:", initialvalue=cur)
        if not name:
            return
        name = name.strip()
        if not name:
            return
        self._presets[name] = self._collect_preset_dict()
        self._presets_meta["last_preset"] = name
        self._presets_meta["autoload_last"] = bool(self.autoload_last_preset_var.get())
        self._write_presets_file()
        self._refresh_preset_combo(select_name=name)
        try:
            self._refresh_preset_note_widgets()
        except Exception:
            pass
        try:
            self.status_var.set(f"Saved preset: {name}")
        except Exception:
            pass

    def on_load_preset(self):
        name = (self.preset_var.get() or "").strip()
        if not name:
            messagebox.showinfo("Load Preset", "Select a preset to load.")
            return
        if name not in self._presets:
            messagebox.showerror("Load Preset", f"Preset '{name}' not found.")
            return
        self._apply_preset_dict(self._presets[name])
        self._presets_meta["last_preset"] = name
        self._write_presets_file()
        try:
            self._refresh_preset_note_widgets()
        except Exception:
            pass
        try:
            self.status_var.set(f"Loaded preset: {name}")
        except Exception:
            pass

    def on_delete_preset(self):
        name = (self.preset_var.get() or "").strip()
        if not name:
            messagebox.showinfo("Delete Preset", "Select a preset to delete.")
            return
        if name not in self._presets:
            messagebox.showerror("Delete Preset", f"Preset '{name}' not found.")
            return
        if not messagebox.askyesno("Delete Preset", f"Delete preset '{name}'?"):
            return
        try:
            del self._presets[name]
        except Exception:
            return
        if self._presets_meta.get("last_preset", "") == name:
            self._presets_meta["last_preset"] = ""
        self._write_presets_file()
        self._refresh_preset_combo()
        self.current_preset_note = ""
        try:
            self._refresh_preset_note_widgets()
        except Exception:
            pass
        try:
            self.status_var.set(f"Deleted preset: {name}")
        except Exception:
            pass
