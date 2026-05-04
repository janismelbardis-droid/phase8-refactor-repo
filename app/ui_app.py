from __future__ import annotations

"""Tkinter UI: Live inspector, strategy builder, backtest runner, trade inspector.

This module is split from the original monolithic script. The goal is to keep the
behavior identical while making the codebase manageable.
"""

import sys
import os
import json
import gzip
import time
import queue
import traceback
import threading
import subprocess
import collections
import calendar
import importlib
from functools import partial
from typing import Any, Dict, List, Optional, Tuple, TYPE_CHECKING

import tkinter as tk
from tkinter import ttk, messagebox, filedialog, simpledialog

import numpy as np
import pandas as pd

from .constants import *
from .utils_time import utc_now_floor_min, parse_utc, interval_to_ms, required_warmup_minutes
from .ui.presenters import color_from_bool, market_state_palette, market_state_playbook, market_state_thesis, range_event_age, format_range_event_age_cell

from .prepared_dataset import PreparedDataset, slice_df_1m_window, slice_streams_window

if TYPE_CHECKING:
    from .backtest import BacktestConfig, BacktestResult, Trade
    from .live_engine import LiveEngine


class _LazyModuleProxy:
    def __init__(self, loader):
        self._loader = loader

    def __getattr__(self, name):
        return getattr(self._loader(), name)


_INDICATORS_RUNTIME = None
_BACKTEST_RUNTIME = None
_LIVE_ENGINE_RUNTIME = None
_UI_PLOT_RUNTIME = None
_MPL_DATES_RUNTIME = None
_MPL_PYPLOT_RUNTIME = None
_MPL_TK_RUNTIME = None
_MPL_COLLECTIONS_RUNTIME = None
_UI_DIALOGS_RUNTIME = None
_UI_WIDGETS_RUNTIME = None
_PRESET_VISUALIZER_RUNTIME = None
_UI_INSPECTORS_RUNTIME = None
_UI_HELPER_DIALOGS_RUNTIME = None
_RESEARCH_RUNTIME = None


def _import_relative(module_name: str):
    return importlib.import_module(module_name, __package__)


def _get_indicators_runtime():
    global _INDICATORS_RUNTIME
    if _INDICATORS_RUNTIME is None:
        _INDICATORS_RUNTIME = _import_relative('.indicators_streaming')
    return _INDICATORS_RUNTIME


def _get_backtest_runtime():
    global _BACKTEST_RUNTIME
    if _BACKTEST_RUNTIME is None:
        _BACKTEST_RUNTIME = _import_relative('.backtest')
    return _BACKTEST_RUNTIME


def _get_live_engine_cls():
    global _LIVE_ENGINE_RUNTIME
    if _LIVE_ENGINE_RUNTIME is None:
        _LIVE_ENGINE_RUNTIME = _import_relative('.live_engine').LiveEngine
    return _LIVE_ENGINE_RUNTIME


def _get_ui_plot_runtime():
    global _UI_PLOT_RUNTIME
    if _UI_PLOT_RUNTIME is None:
        _UI_PLOT_RUNTIME = _import_relative('.ui_plot')
    return _UI_PLOT_RUNTIME


def _get_mpl_dates_runtime():
    global _MPL_DATES_RUNTIME
    if _MPL_DATES_RUNTIME is None:
        _MPL_DATES_RUNTIME = importlib.import_module('matplotlib.dates')
    return _MPL_DATES_RUNTIME


def _get_mpl_pyplot_runtime():
    global _MPL_PYPLOT_RUNTIME
    if _MPL_PYPLOT_RUNTIME is None:
        _MPL_PYPLOT_RUNTIME = importlib.import_module('matplotlib.pyplot')
    return _MPL_PYPLOT_RUNTIME


def _get_mpl_tk_runtime():
    global _MPL_TK_RUNTIME
    if _MPL_TK_RUNTIME is None:
        _MPL_TK_RUNTIME = importlib.import_module('matplotlib.backends.backend_tkagg')
    return _MPL_TK_RUNTIME


def _get_mpl_collections_runtime():
    global _MPL_COLLECTIONS_RUNTIME
    if _MPL_COLLECTIONS_RUNTIME is None:
        _MPL_COLLECTIONS_RUNTIME = importlib.import_module('matplotlib.collections')
    return _MPL_COLLECTIONS_RUNTIME


def _get_ui_dialogs_runtime():
    global _UI_DIALOGS_RUNTIME
    if _UI_DIALOGS_RUNTIME is None:
        _UI_DIALOGS_RUNTIME = _import_relative('.ui_dialogs')
    return _UI_DIALOGS_RUNTIME


def _get_ui_widgets_runtime():
    global _UI_WIDGETS_RUNTIME
    if _UI_WIDGETS_RUNTIME is None:
        _UI_WIDGETS_RUNTIME = _import_relative('.ui_widgets')
    return _UI_WIDGETS_RUNTIME


def _get_preset_visualizer_runtime():
    global _PRESET_VISUALIZER_RUNTIME
    if _PRESET_VISUALIZER_RUNTIME is None:
        _PRESET_VISUALIZER_RUNTIME = _import_relative('.preset_visualizer')
    return _PRESET_VISUALIZER_RUNTIME


def _get_ui_inspectors_runtime():
    global _UI_INSPECTORS_RUNTIME
    if _UI_INSPECTORS_RUNTIME is None:
        _UI_INSPECTORS_RUNTIME = _import_relative('.ui.inspectors')
    return _UI_INSPECTORS_RUNTIME


def _get_ui_helper_dialogs_runtime():
    global _UI_HELPER_DIALOGS_RUNTIME
    if _UI_HELPER_DIALOGS_RUNTIME is None:
        _UI_HELPER_DIALOGS_RUNTIME = _import_relative('.ui.dialogs')
    return _UI_HELPER_DIALOGS_RUNTIME


def _get_research_runtime():
    global _RESEARCH_RUNTIME
    if _RESEARCH_RUNTIME is None:
        _RESEARCH_RUNTIME = _import_relative('.research')
    return _RESEARCH_RUNTIME


_DATA_BINANCE = None


def _get_data_binance():
    """Lazy import: pulls in `requests` and REST helpers; not needed for a bare UI shell."""
    global _DATA_BINANCE
    if _DATA_BINANCE is None:
        from . import data_binance as _db
        _DATA_BINANCE = _db
    return _DATA_BINANCE


def _compile_stream_requirements(*args, **kwargs):
    from .strategy_requirements import compile_stream_requirements as _fn

    return _fn(*args, **kwargs)


plt = _LazyModuleProxy(_get_mpl_pyplot_runtime)
mdates = _LazyModuleProxy(_get_mpl_dates_runtime)


def FigureCanvasTkAgg(*args, **kwargs):
    return _get_mpl_tk_runtime().FigureCanvasTkAgg(*args, **kwargs)


def NavigationToolbar2Tk(*args, **kwargs):
    return _get_mpl_tk_runtime().NavigationToolbar2Tk(*args, **kwargs)


def LineCollection(*args, **kwargs):
    return _get_mpl_collections_runtime().LineCollection(*args, **kwargs)


def build_figure(*args, **kwargs):
    return _get_ui_plot_runtime().build_figure(*args, **kwargs)


def macd_series(*args, **kwargs):
    return _get_indicators_runtime().macd_series(*args, **kwargs)


def ppo_d_series(*args, **kwargs):
    return _get_indicators_runtime().ppo_d_series(*args, **kwargs)


def pretty_market_state_label(*args, **kwargs):
    return _get_indicators_runtime().pretty_market_state_label(*args, **kwargs)


def simulate_multitf_indicators(*args, **kwargs):
    return _get_indicators_runtime().simulate_multitf_indicators(*args, **kwargs)


def simulate_multitf_indicators_fast(*args, **kwargs):
    return _get_indicators_runtime().simulate_multitf_indicators_fast(*args, **kwargs)


def slice_and_decimate(*args, **kwargs):
    return _get_indicators_runtime().slice_and_decimate(*args, **kwargs)


def market_state_thresholds_dict(*args, **kwargs):
    return _get_indicators_runtime().market_state_thresholds_dict(*args, **kwargs)


def coerce_market_state_thresholds(*args, **kwargs):
    return _get_indicators_runtime().coerce_market_state_thresholds(*args, **kwargs)


def compute_market_state_snapshot(*args, **kwargs):
    return _get_indicators_runtime().compute_market_state_snapshot(*args, **kwargs)


def rebuild_market_state_streams(*args, **kwargs):
    return _get_indicators_runtime().rebuild_market_state_streams(*args, **kwargs)


def summarize_market_state_streams(*args, **kwargs):
    return _get_indicators_runtime().summarize_market_state_streams(*args, **kwargs)


def build_preset_report(*args, **kwargs):
    return _get_preset_visualizer_runtime().build_preset_report(*args, **kwargs)


def infer_preset_archetype(*args, **kwargs):
    return _get_preset_visualizer_runtime().infer_preset_archetype(*args, **kwargs)


def build_market_state_outcome_report(*args, **kwargs):
    return _get_preset_visualizer_runtime().build_market_state_outcome_report(*args, **kwargs)


def build_guided_threshold_tuning_report(*args, **kwargs):
    return _get_preset_visualizer_runtime().build_guided_threshold_tuning_report(*args, **kwargs)


def build_market_state_stream_audit_report(*args, **kwargs):
    return _get_preset_visualizer_runtime().build_market_state_stream_audit_report(*args, **kwargs)


def ui_popup_yes_no(*args, **kwargs):
    return _get_ui_inspectors_runtime().popup_yes_no(*args, **kwargs)


def ui_detail_section(*args, **kwargs):
    return _get_ui_inspectors_runtime().detail_section(*args, **kwargs)


def ui_derive_classic_ta_snapshot(*args, **kwargs):
    return _get_ui_inspectors_runtime().derive_classic_ta_snapshot(*args, **kwargs)


def ui_build_market_state_detail_text(*args, **kwargs):
    return _get_ui_inspectors_runtime().build_market_state_detail_text(*args, **kwargs)


def ui_build_orderflow_detail_text(*args, **kwargs):
    return _get_ui_inspectors_runtime().build_orderflow_detail_text(*args, **kwargs)


def ui_build_orderflow_trail_text(*args, **kwargs):
    return _get_ui_inspectors_runtime().build_orderflow_trail_text(*args, **kwargs)


def ui_bind_popup_text_scroll(*args, **kwargs):
    return _get_ui_helper_dialogs_runtime().bind_popup_text_scroll(*args, **kwargs)


def ui_make_popup_text_widget(*args, **kwargs):
    return _get_ui_helper_dialogs_runtime().make_popup_text_widget(*args, **kwargs)


def ui_set_popup_text_content(*args, **kwargs):
    return _get_ui_helper_dialogs_runtime().set_popup_text_content(*args, **kwargs)


def ui_show_orderflow_trail(*args, **kwargs):
    return _get_ui_helper_dialogs_runtime().show_orderflow_trail(*args, **kwargs)


def ui_show_orderflow_details(*args, **kwargs):
    return _get_ui_helper_dialogs_runtime().show_orderflow_details(*args, **kwargs)


def ui_show_market_state_details(*args, **kwargs):
    return _get_ui_helper_dialogs_runtime().show_market_state_details(*args, **kwargs)


def build_backtest_replay_plan(*args, **kwargs):
    return _get_research_runtime().build_backtest_replay_plan(*args, **kwargs)


def replay_backtest_result(*args, **kwargs):
    return _get_research_runtime().replay_backtest_result(*args, **kwargs)


def extract_backtest_replay_context(*args, **kwargs):
    return _get_research_runtime().extract_backtest_replay_context(*args, **kwargs)


def write_trade_audit_csv(*args, **kwargs):
    return _get_research_runtime().write_trade_audit_csv(*args, **kwargs)


def write_trade_audit_report(*args, **kwargs):
    return _get_research_runtime().write_trade_audit_report(*args, **kwargs)


def BacktestConfig(*args, **kwargs):
    return _get_backtest_runtime().BacktestConfig(*args, **kwargs)


def Trade(*args, **kwargs):
    return _get_backtest_runtime().Trade(*args, **kwargs)


def BacktestResult(*args, **kwargs):
    return _get_backtest_runtime().BacktestResult(*args, **kwargs)


def run_backtest(*args, **kwargs):
    return _get_backtest_runtime().run_backtest(*args, **kwargs)


def run_backtest_tick(*args, **kwargs):
    return _get_backtest_runtime().run_backtest_tick(*args, **kwargs)


def make_streams_htf_closed_only(*args, **kwargs):
    return _get_backtest_runtime().make_streams_htf_closed_only(*args, **kwargs)


def _make_trade_df(*args, **kwargs):
    return _get_backtest_runtime()._make_trade_df(*args, **kwargs)


def DotRuleDialog(*args, **kwargs):
    return _get_ui_dialogs_runtime().DotRuleDialog(*args, **kwargs)


def EventRuleDialog(*args, **kwargs):
    return _get_ui_dialogs_runtime().EventRuleDialog(*args, **kwargs)


def NumericRuleDialog(*args, **kwargs):
    return _get_ui_dialogs_runtime().NumericRuleDialog(*args, **kwargs)


def BarsAgoDialog(*args, **kwargs):
    return _get_ui_dialogs_runtime().BarsAgoDialog(*args, **kwargs)


def EntryFilterDialog(*args, **kwargs):
    return _get_ui_dialogs_runtime().EntryFilterDialog(*args, **kwargs)


def MarketStateThresholdsDialog(*args, **kwargs):
    return _get_ui_dialogs_runtime().MarketStateThresholdsDialog(*args, **kwargs)


def LegendWindow(*args, **kwargs):
    return _get_ui_widgets_runtime().LegendWindow(*args, **kwargs)


def Chip(*args, **kwargs):
    return _get_ui_widgets_runtime().Chip(*args, **kwargs)


def GroupUI(*args, **kwargs):
    return _get_ui_widgets_runtime().GroupUI(*args, **kwargs)


def _safe_axis_legend(ax, *args, **kwargs):
    """Draw legend only when there are visible labeled artists."""
    try:
        handles, labels = ax.get_legend_handles_labels()
    except Exception:
        return None
    filt = [(h, l) for h, l in zip(handles, labels) if isinstance(l, str) and l and not l.startswith("_")]
    if not filt:
        return None
    try:
        hh, ll = zip(*filt)
        return ax.legend(hh, ll, *args, **kwargs)
    except Exception:
        try:
            return ax.legend(*args, **kwargs)
        except Exception:
            return None

from .rules import Rule, RuleEvalContext, EntryFilterConfig, eval_rule_generic, eval_group_generic, eval_tab_generic, sequence_rule_partition, snapshot_from_stream_row

from .ui.visual_app import (
    BacktestChartsMixin,
    LazyBtResultsMixin,
    LiveShellMixin,
    PreparedDatasetMixin,
    PresetsMixin,
    ScrollAndScreenMixin,
)


class VisualApp(
    PreparedDatasetMixin,
    ScrollAndScreenMixin,
    LazyBtResultsMixin,
    LiveShellMixin,
    PresetsMixin,
    BacktestChartsMixin,
    tk.Tk,
):
    def __init__(self):
        super().__init__()
        self.title("MACD Live + Clickable Strategy Builder (Binance Futures) — AND/OR/SEQUENCE + Events")
        self.geometry("1480x980")
        self.minsize(1180, 780)
        self._apply_dark_theme()

        self.q: "queue.Queue" = queue.Queue()
        self.worker: Optional[threading.Thread] = None
        self.live_engine: Optional[Any] = None
        self._progress_started = False
        self._live_status_pending: Dict[str, Tuple[float, float, Optional[Dict[str, Any]]]] = {}
        self._live_status_render_job = None
        self._live_price_job = None
        self._live_price_last_value: Optional[float] = None
        self._live_price_last_age_sec: Optional[float] = None
        self._live_cell_cache: Dict[Tuple[str, str], Tuple[str, str]] = {}

        # Latest live states per TF
        self.latest: Dict[str, Dict[str, Any]] = {tf: {} for tf in ALL_TIMEFRAMES}
        self.prev_latest: Dict[str, Dict[str, Any]] = {tf: {} for tf in ALL_TIMEFRAMES}

        # Latest CLOSED-bar snapshots per TF (stable at candle close).
        # Used to make STATE "Bars Ago" behave like TradingView (previous *closed* candle),
        # even while the current candle is forming/repainting.
        self.latest_closed: Dict[str, Dict[str, Any]] = {tf: {} for tf in ALL_TIMEFRAMES}

        # Last seen Range Filter trigger bar per timeframe. Used purely for UI diagnostics so you
        # can see when BUY/SELL trigger pulses last fired, similar to PPO FlipAge.
        self._range_event_last_bar_ms: Dict[str, Dict[str, Optional[int]]] = {
            tf: {"buy": None, "sell": None} for tf in ALL_TIMEFRAMES
        }
        self._range_event_meta: Dict[str, Dict[str, Any]] = {
            tf: {"last_seen_bar_ms": None, "regime": 0, "current_bar_buy_seen": False, "current_bar_sell_seen": False}
            for tf in ALL_TIMEFRAMES
        }

        # Session-local market-state classifier thresholds. These do not change indicator math;
        # they only change the synthetic market-state labels and diagnostics.
        self.market_state_threshold_values: Optional[Dict[str, Any]] = None

        # Pending batches of CLOSED-bar history from the live engine (seeded after prefill).
        # We drain these gradually to keep Tk responsive.
        self._pending_closed_seed: Dict[str, collections.deque] = {tf: collections.deque() for tf in ALL_TIMEFRAMES}
        self._pending_closed_seed_job = None

        # Throttle live rule evaluations so the UI never freezes when updates are frequent.
        self._live_rule_dirty: bool = False
        self._live_rule_update_job = None

        # Stateful evaluation context for EVENT rules (supports bars-ago constraints)
        self._rule_eval_ctx = RuleEvalContext(step_index=0)
        self._rule_eval_last_ts = None

        # Rule model:
        # rules[tab] = list of groups; group is list of Rule
        self.rules: Dict[str, List[List[Rule]]] = {name: [] for name in RULE_TABS}

        # Logic modes:
        # tab_group_join_mode[tab] controls how groups combine (AND/OR)
        # group_rule_join_mode[tab][gi] controls how rules inside that group combine (AND/OR/SEQUENCE)
        self.tab_group_join_mode: Dict[str, str] = {name: "AND" for name in RULE_TABS}   # default per your request
        self.group_rule_join_mode: Dict[str, List[str]] = {name: [] for name in RULE_TABS}  # per group
        # Live latch per group (★ triggers): once fired, keep group ON while filters remain valid
        self.group_latch: Dict[str, List[Optional[pd.Timestamp]]] = {name: [] for name in RULE_TABS}
        # Extra latch metadata (for "until opposite state flips" validity):
        #   group_latch_meta[tab][gi] = (tf, state_key, state_val)
        self.group_latch_meta: Dict[str, List[Optional[Tuple[str, str, Any]]]] = {name: [] for name in RULE_TABS}

        # Per-entry-tab execution filter. This is intentionally separate from the
        # rule engine: rules decide *when* a candidate exists; the entry filter
        # decides whether we may execute that candidate now / later / never.
        self.entry_filters: Dict[str, EntryFilterConfig] = {
            "Long Entry": EntryFilterConfig(),
            "Short Entry": EntryFilterConfig(),
        }

        # When True: FIRED remains valid only while the resulting state still holds ("until opposite state flips").
        # When False: FIRED remains valid until filters fail (classic latch).
        self.latch_until_flip_var: tk.BooleanVar
        self._latch_seed_dirty: bool = False

        # UI handles for groups + chips:
        self.group_ui: Dict[str, List[Any]] = {name: [] for name in RULE_TABS}
        self.active_tab: str = "Long Entry"
        self.active_group_index: Dict[str, int] = {name: 0 for name in RULE_TABS}

        # Live rule-status bookkeeping (for on-screen STATUS updates)
        self.tab_status_lbls: Dict[str, tk.Label] = {}
        self.tab_satisfied: Dict[str, bool] = {name: False for name in RULE_TABS}

        # History in memory
        self.hist_symbol: Optional[str] = None
        self.hist_start: Optional[pd.Timestamp] = None
        self.hist_end: Optional[pd.Timestamp] = None
        self.hist_df_1m_full: Optional[pd.DataFrame] = None
        self.hist_streams_full: Optional[Dict[str, pd.DataFrame]] = None

        # Small in-memory cache of prepared datasets so repeated backtests can reuse
        # already-fetched candles and already-computed indicator streams safely.
        self._prepared_datasets: List[PreparedDataset] = []
        self._prepared_dataset_limit: int = 4

        self.current_preset_note: str = ""
        self._preset_visual_windows: Dict[str, tk.Toplevel] = {}

        # Session-local breakout alert visibility + history.
        # This is intentionally UI-only: it does not change the trading logic, it only
        # surfaces important breakout events loudly and keeps a record for later review.
        self._breakout_alert_records: List[Dict[str, Any]] = []
        self._breakout_alert_last_by_tf: Dict[str, Dict[str, Any]] = {}
        self._breakout_alert_flash_job = None
        self._breakout_alert_flash_state: bool = False
        self._breakout_alert_banner_latched: bool = False

        # Lazy-build state for heavier UI surfaces so the main window appears faster.
        self._backtest_tab_built: bool = False
        self._rule_tabs_built: Dict[str, bool] = {name: False for name in RULE_TABS}
        self._rule_tab_placeholders: Dict[str, Optional[ttk.Label]] = {name: None for name in RULE_TABS}
        self._market_state_popup_built: bool = False
        self._market_state_popup = None
        self._market_state_popup_outer = None
        self._market_state_popup_body = None
        self._state_board_canvas = None
        self._state_board_hsb = None
        self._state_board_inner = None
        self._state_board_window = None
        self._market_state_outer = None
        self._market_state_body = None
        self.market_state_collapsed_var = tk.BooleanVar(value=True)
        self.market_state_cards: Dict[str, Dict[str, Any]] = {}
        self.bt_result: Optional[Any] = None
        self.bt_streams_full: Optional[Dict[str, pd.DataFrame]] = None
        self._bt_price_chart_built: bool = False
        self._bt_equity_chart_built: bool = False
        self._bt_chart_tab_price = None
        self._bt_chart_tab_equity = None
        self._bt_price_chart_placeholder = None
        self._bt_equity_chart_placeholder = None
        self._bt_results_surface_built: bool = False
        self._bt_results_parent = None
        self._bt_results_placeholder = None
        self.bt_summary_txt = None
        self.bt_tree = None
        self._live_workspace_tabs_built: Dict[str, bool] = {'builder': False, 'log': False, 'alerts': False}
        self._live_workspace_tab_placeholders: Dict[str, Optional[ttk.Label]] = {'builder': None, 'log': None, 'alerts': None}
        self._live_workspace_build_job = None
        self._live_workspace_nb = None
        self._live_breakout_alert_built: bool = False
        self._live_signal_table_built: bool = False
        self._live_top_placeholders: Dict[str, Optional[ttk.Label]] = {'breakout': None, 'table': None}
        self._live_top_build_job = None
        self._live_breakout_parent = None
        self._live_table_parent = None
        self.live_cells: Dict[str, Dict[str, tk.Label]] = {}
        self._live_log_buffer: collections.deque[str] = collections.deque(maxlen=2500)

        self._ui_ready = False
        self._build_ui()
        try:
            self.update_idletasks()
        except Exception:
            pass
        self._set_defaults()
        self._init_presets()
        self._ui_ready = True
        self._install_traces()
        self.after(150, self._configure_window_for_screen)
        self.after(QUEUE_POLL_MS, self._poll_queue)

    def _apply_dark_theme(self) -> None:
        try:
            self.configure(bg=C_BG_APP)
            self.tk_setPalette(
                background=C_BG_PANEL,
                foreground=C_TEXT,
                activeBackground=C_BG_ELEVATED,
                activeForeground=C_TEXT,
                highlightBackground=C_BORDER_IDLE,
                highlightColor=C_BORDER_ACTIVE,
            )
        except Exception:
            pass
        try:
            self.option_add("*Background", C_BG_PANEL)
            self.option_add("*Foreground", C_TEXT)
            self.option_add("*Canvas.Background", C_BG_PANEL)
            self.option_add("*Text.Background", C_BG_INPUT)
            self.option_add("*Text.Foreground", C_TEXT)
            self.option_add("*Entry.Background", C_BG_INPUT)
            self.option_add("*Entry.Foreground", C_TEXT)
            self.option_add("*Listbox.Background", C_BG_INPUT)
            self.option_add("*Listbox.Foreground", C_TEXT)
            self.option_add("*Menu.Background", C_BG_PANEL)
            self.option_add("*Menu.Foreground", C_TEXT)
            self.option_add("*selectBackground", C_BORDER_ACTIVE)
            self.option_add("*selectForeground", C_TEXT)
        except Exception:
            pass
        try:
            style = ttk.Style(self)
            try:
                style.theme_use("clam")
            except Exception:
                pass
            style.configure(".", background=C_BG_PANEL, foreground=C_TEXT, fieldbackground=C_BG_INPUT)
            style.configure("TFrame", background=C_BG_PANEL)
            style.configure("TLabelframe", background=C_BG_PANEL, foreground=C_TEXT, bordercolor=C_BORDER_IDLE)
            style.configure("TLabelframe.Label", background=C_BG_PANEL, foreground=C_TEXT)
            style.configure("TLabel", background=C_BG_PANEL, foreground=C_TEXT)
            style.configure("TButton", background=C_BG_ELEVATED, foreground=C_TEXT, bordercolor=C_BORDER_IDLE)
            style.map("TButton", background=[("active", C_BG_NEU), ("pressed", C_BG_INPUT)])
            style.configure("TCheckbutton", background=C_BG_PANEL, foreground=C_TEXT)
            style.configure("TRadiobutton", background=C_BG_PANEL, foreground=C_TEXT)
            style.configure("TNotebook", background=C_BG_APP, borderwidth=0)
            style.configure("TNotebook.Tab", background=C_BG_PANEL, foreground=C_DIM, padding=(10, 6))
            style.map("TNotebook.Tab", background=[("selected", C_BG_ELEVATED)], foreground=[("selected", C_TEXT)])
            style.configure("Treeview", background=C_BG_INPUT, foreground=C_TEXT, fieldbackground=C_BG_INPUT)
            style.configure("Treeview.Heading", background=C_BG_ELEVATED, foreground=C_TEXT)
            style.map("Treeview", background=[("selected", C_BG_NEU)], foreground=[("selected", C_TEXT)])
            style.configure("TEntry", fieldbackground=C_BG_INPUT, foreground=C_TEXT)
            style.configure("TCombobox", fieldbackground=C_BG_INPUT, foreground=C_TEXT, arrowsize=14)
            style.configure("Horizontal.TProgressbar", background=C_GREEN, troughcolor=C_BG_INPUT)
            style.configure("Vertical.TScrollbar", background=C_BG_ELEVATED, troughcolor=C_BG_PANEL)
            style.configure("Horizontal.TScrollbar", background=C_BG_ELEVATED, troughcolor=C_BG_PANEL)
            style.configure("TPanedwindow", background=C_BG_APP)
        except Exception:
            pass
        try:
            mpl = _get_mpl_pyplot_runtime()
            mpl.rcParams.update({
                "figure.facecolor": C_BG_PANEL,
                "axes.facecolor": C_BG_INPUT,
                "savefig.facecolor": C_BG_PANEL,
                "axes.edgecolor": C_BORDER_IDLE,
                "axes.labelcolor": C_TEXT,
                "axes.titlecolor": C_TEXT,
                "xtick.color": C_DIM,
                "ytick.color": C_DIM,
                "grid.color": C_BORDER_IDLE,
                "text.color": C_TEXT,
                "legend.facecolor": C_BG_ELEVATED,
                "legend.edgecolor": C_BORDER_IDLE,
            })
        except Exception:
            pass

    def _set_backtest_structure_collapsed(self, collapsed: bool) -> None:
        collapsed = bool(collapsed)
        try:
            self.bt_structure_collapsed_var.set(collapsed)
        except Exception:
            pass
        body = getattr(self, '_bt_structure_body', None)
        btn = getattr(self, '_bt_structure_toggle_btn', None)
        if body is not None:
            try:
                if collapsed:
                    if body.winfo_manager():
                        body.pack_forget()
                else:
                    if not body.winfo_manager():
                        body.pack(side='top', fill='x', expand=True, pady=(4, 0))
            except Exception:
                pass
        if btn is not None:
            try:
                btn.configure(text=("Show" if collapsed else "Hide") + " BT structure")
            except Exception:
                pass

    def _toggle_backtest_structure(self) -> None:
        cur = False
        try:
            cur = bool(self.bt_structure_collapsed_var.get())
        except Exception:
            cur = False
        self._set_backtest_structure_collapsed(not cur)

    def _set_backtest_settings_collapsed(self, collapsed: bool) -> None:
        collapsed = bool(collapsed)
        try:
            self.bt_settings_collapsed_var.set(collapsed)
        except Exception:
            pass
        body = getattr(self, '_bt_settings_body', None)
        btn = getattr(self, '_bt_settings_toggle_btn', None)
        if body is not None:
            try:
                if collapsed:
                    if body.winfo_manager():
                        body.pack_forget()
                else:
                    if not body.winfo_manager():
                        body.pack(side='top', fill='x', expand=False, pady=(4, 0))
            except Exception:
                pass
        if btn is not None:
            try:
                btn.configure(text=("Show" if collapsed else "Hide") + " settings")
            except Exception:
                pass

    def _toggle_backtest_settings(self) -> None:
        cur = False
        try:
            cur = bool(self.bt_settings_collapsed_var.get())
        except Exception:
            cur = False
        self._set_backtest_settings_collapsed(not cur)

    def _open_workflow_help(self) -> None:
        win = tk.Toplevel(self)
        win.title("Workflow help")
        win.geometry("920x520")
        win.minsize(760, 420)
        outer = ttk.Frame(win, padding=12)
        outer.pack(side='top', fill='both', expand=True)

        ttk.Label(
            outer,
            text='Use the UI in layers: keep the signal table and rule builder on-screen, collapse diagnostics when you need space, and open deeper tools in popups.',
            justify='left',
            wraplength=860,
        ).pack(side='top', anchor='w', pady=(0, 10))

        txt = tk.Text(outer, wrap='word', height=16, font=('Segoe UI', 10))
        txt.pack(side='top', fill='both', expand=True)
        txt.insert(
            '1.0',
            'Core workflow\n'
            '• Symbol / dates / timeframes stay in the top toolbars.\n'
            '• The LIVE tab is prioritized around the signal table and the rule builder.\n'
            '• Breakout Alert stays in the main view, and Market State cards open in a separate popup window.\n\n'
            'Rule builder notes\n'
            '• Each Group combines RULES with AND / OR / SEQUENCE.\n'
            '• EVENT rules are one-candle pulses (TURNED / FLIPPED / CROSS).\n'
            '• SEQUENCE evaluates rules in order across time.\n'
            '• Right-click an EVENT chip to toggle ★ Trigger.\n'
            '• Live monitoring shows ARMED / READY / FIRED so you can track setups in real time.\n\n'
            'Popup tools\n'
            '• Legend explains symbols and execution semantics.\n'
            '• Tick Manager handles aggTrades cache coverage.\n'
            '• Threshold Lab, Tuning Assistant, and State Audit stay as dedicated popups so the main surface stays focused.\n'
        )
        txt.configure(state='disabled')

        btns = ttk.Frame(outer)
        btns.pack(side='bottom', fill='x', pady=(10, 0))
        ttk.Button(btns, text='Legend', command=self.open_legend).pack(side='left')
        ttk.Button(btns, text='Close', command=win.destroy).pack(side='right')

    def _open_workspace_tools(self) -> None:
        win = tk.Toplevel(self)
        win.title("Workspace tools")
        win.geometry("520x300")
        win.minsize(460, 250)
        outer = ttk.Frame(win, padding=12)
        outer.pack(side='top', fill='both', expand=True)

        ttk.Label(
            outer,
            text='Quick-launch the supporting tools without keeping them pinned in the main workspace.',
            justify='left',
            wraplength=460,
        ).pack(side='top', anchor='w', pady=(0, 10))

        grid = ttk.Frame(outer)
        grid.pack(side='top', fill='both', expand=True)
        actions = [
            ('Legend', self.open_legend),
            ('Tick Manager…', self.open_tick_manager),
            ('Threshold Lab…', self._open_market_state_threshold_lab),
            ('Tuning Assistant…', self.on_show_market_state_tuning_assistant),
            ('State Audit…', self.on_show_market_state_stream_audit),
            ('State Outcomes…', self.on_show_market_state_outcomes),
        ]
        for idx, (label, command) in enumerate(actions):
            r = idx // 2
            c = idx % 2
            ttk.Button(grid, text=label, command=command).grid(row=r, column=c, sticky='ew', padx=6, pady=6)
        grid.grid_columnconfigure(0, weight=1)
        grid.grid_columnconfigure(1, weight=1)
        ttk.Button(outer, text='Close', command=win.destroy).pack(side='bottom', anchor='e', pady=(10, 0))

    def _open_datetime_picker(self, target: str) -> None:
        if target not in {"start", "end"}:
            return
        var = self.start_var if target == "start" else self.end_var
        try:
            base_ts = parse_utc(str(var.get() or "")).tz_convert("UTC")
        except Exception:
            try:
                base_ts = utc_now_floor_min().tz_convert("UTC")
            except Exception:
                base_ts = pd.Timestamp.now(tz="UTC").floor("min")

        win = tk.Toplevel(self)
        win.title(f"Select {'Start' if target == 'start' else 'End'} UTC")
        try:
            win.transient(self)
            win.grab_set()
        except Exception:
            pass
        try:
            win.resizable(False, False)
        except Exception:
            pass

        view_year = tk.IntVar(value=int(base_ts.year))
        view_month = tk.IntVar(value=int(base_ts.month))
        sel_day = tk.IntVar(value=int(base_ts.day))
        hour_var = tk.IntVar(value=int(base_ts.hour))
        minute_var = tk.IntVar(value=int(base_ts.minute))
        second_var = tk.IntVar(value=int(base_ts.second))
        header_var = tk.StringVar(value="")

        outer = ttk.Frame(win, padding=10)
        outer.pack(fill="both", expand=True)

        nav = ttk.Frame(outer)
        nav.pack(fill="x")
        ttk.Button(nav, text="◀", width=4, command=lambda: _shift_month(-1)).pack(side="left")
        ttk.Label(nav, textvariable=header_var, font=("Segoe UI", 10, "bold")).pack(side="left", padx=10)
        ttk.Button(nav, text="▶", width=4, command=lambda: _shift_month(1)).pack(side="left")
        ttk.Button(nav, text="Today", command=lambda: _jump_today()).pack(side="right")

        cal_frame = ttk.Frame(outer)
        cal_frame.pack(fill="x", pady=(8, 6))
        day_grid = ttk.Frame(outer)
        day_grid.pack(fill="x")

        time_frame = ttk.LabelFrame(outer, text="UTC time", padding=8)
        time_frame.pack(fill="x", pady=(10, 0))
        ttk.Label(time_frame, text="Hour").grid(row=0, column=0, sticky="w")
        ttk.Spinbox(time_frame, from_=0, to=23, wrap=True, textvariable=hour_var, width=4, format="%02.0f").grid(row=0, column=1, padx=(4, 12), sticky="w")
        ttk.Label(time_frame, text="Minute").grid(row=0, column=2, sticky="w")
        ttk.Spinbox(time_frame, from_=0, to=59, wrap=True, textvariable=minute_var, width=4, format="%02.0f").grid(row=0, column=3, padx=(4, 12), sticky="w")
        ttk.Label(time_frame, text="Second").grid(row=0, column=4, sticky="w")
        ttk.Spinbox(time_frame, from_=0, to=59, wrap=True, textvariable=second_var, width=4, format="%02.0f").grid(row=0, column=5, padx=(4, 0), sticky="w")

        btns = ttk.Frame(outer)
        btns.pack(fill="x", pady=(10, 0))

        def _days_in_month(year: int, month: int) -> int:
            return int(calendar.monthrange(int(year), int(month))[1])

        def _sync_selected_day() -> None:
            try:
                max_day = _days_in_month(view_year.get(), view_month.get())
                cur = int(sel_day.get())
            except Exception:
                max_day = 28
                cur = 1
            if cur < 1:
                cur = 1
            if cur > max_day:
                cur = max_day
            sel_day.set(cur)

        def _render_calendar() -> None:
            for child in cal_frame.winfo_children():
                child.destroy()
            for child in day_grid.winfo_children():
                child.destroy()
            y = int(view_year.get())
            m = int(view_month.get())
            header_var.set(f"{calendar.month_name[m]} {y}")
            for idx, name in enumerate(["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]):
                ttk.Label(cal_frame, text=name, width=4, anchor="center").grid(row=0, column=idx, padx=1, pady=(0, 2))
            weeks = calendar.monthcalendar(y, m)
            _sync_selected_day()
            active_day = int(sel_day.get())
            for r, week in enumerate(weeks, start=0):
                for c, day in enumerate(week):
                    if day <= 0:
                        ttk.Label(day_grid, text="", width=4).grid(row=r, column=c, padx=1, pady=1)
                        continue
                    style = "Accent.TButton" if day == active_day else "TButton"
                    ttk.Button(day_grid, text=f"{day:02d}", width=4, style=style, command=lambda d=day: _select_day(d)).grid(row=r, column=c, padx=1, pady=1)

        def _select_day(day: int) -> None:
            sel_day.set(int(day))
            _render_calendar()

        def _shift_month(delta: int) -> None:
            y = int(view_year.get())
            m = int(view_month.get())
            m += int(delta)
            while m < 1:
                y -= 1
                m += 12
            while m > 12:
                y += 1
                m -= 12
            view_year.set(y)
            view_month.set(m)
            _sync_selected_day()
            _render_calendar()

        def _jump_today() -> None:
            try:
                now_ts = utc_now_floor_min().tz_convert("UTC")
            except Exception:
                now_ts = pd.Timestamp.now(tz="UTC").floor("min")
            view_year.set(int(now_ts.year))
            view_month.set(int(now_ts.month))
            sel_day.set(int(now_ts.day))
            hour_var.set(int(now_ts.hour))
            minute_var.set(int(now_ts.minute))
            second_var.set(int(now_ts.second))
            _render_calendar()

        def _apply() -> None:
            try:
                ts = pd.Timestamp(
                    year=int(view_year.get()),
                    month=int(view_month.get()),
                    day=int(sel_day.get()),
                    hour=int(hour_var.get()),
                    minute=int(minute_var.get()),
                    second=int(second_var.get()),
                    tz="UTC",
                )
            except Exception as exc:
                messagebox.showerror("Datetime picker", f"Invalid UTC datetime: {exc}", parent=win)
                return
            var.set(ts.strftime("%Y-%m-%d %H:%M:%S"))
            try:
                self.status_var.set(f"Set {'start' if target == 'start' else 'end'} time from picker")
            except Exception:
                pass
            try:
                win.destroy()
            except Exception:
                pass

        ttk.Button(btns, text="Apply", command=_apply).pack(side="right")
        ttk.Button(btns, text="Cancel", command=win.destroy).pack(side="right", padx=(0, 6))

        _render_calendar()
        try:
            win.wait_visibility()
            win.focus_set()
        except Exception:
            pass

    # ---------- UI ----------
    def _build_ui(self):
        top = ttk.Frame(self, padding=(8, 6, 8, 4))
        top.pack(side='top', fill='x')

        row1 = ttk.Frame(top)
        row1.pack(side='top', fill='x')
        row2 = ttk.Frame(top)
        row2.pack(side='top', fill='x', pady=(6, 0))
        row3 = ttk.Frame(top)
        row3.pack(side='top', fill='x', pady=(6, 0))

        session_box = ttk.LabelFrame(row1, text='Session', padding=8)
        session_box.pack(side='left', fill='x', expand=True, padx=(0, 6))
        session_row1 = ttk.Frame(session_box)
        session_row1.pack(side='top', fill='x')
        session_row2 = ttk.Frame(session_box)
        session_row2.pack(side='top', fill='x', pady=(6, 0))

        ttk.Label(session_row1, text='Symbol:').pack(side='left')
        self.symbol_var = tk.StringVar()
        ttk.Combobox(session_row1, textvariable=self.symbol_var, width=10, values=['BTCUSDT', 'ETHUSDT', 'SOLUSDT', 'XRPUSDT', 'BNBUSDT']).pack(side='left', padx=(6, 12))
        ttk.Label(session_row1, text='Start (UTC):').pack(side='left')
        self.start_var = tk.StringVar()
        start_box = ttk.Frame(session_row1)
        start_box.pack(side='left', padx=(6, 12))
        ttk.Entry(start_box, textvariable=self.start_var, width=16).pack(side='left')
        ttk.Button(start_box, text='Pick…', width=7, command=lambda: self._open_datetime_picker('start')).pack(side='left', padx=(4, 0))
        ttk.Label(session_row1, text='End (UTC):').pack(side='left')
        self.end_var = tk.StringVar()
        end_box = ttk.Frame(session_row1)
        end_box.pack(side='left', padx=(6, 0))
        ttk.Entry(end_box, textvariable=self.end_var, width=16).pack(side='left')
        ttk.Button(end_box, text='Pick…', width=7, command=lambda: self._open_datetime_picker('end')).pack(side='left', padx=(4, 0))

        ttk.Label(session_row2, text='Warmup (min):').pack(side='left')
        self.warmup_var = tk.IntVar(value=600)
        ttk.Spinbox(session_row2, from_=0, to=5000, textvariable=self.warmup_var, width=10).pack(side='left', padx=(6, 12))
        ttk.Label(session_row2, text='Cache dir:').pack(side='left')
        self.cache_var = tk.StringVar(value='data_cache')
        ttk.Entry(session_row2, textvariable=self.cache_var, width=22).pack(side='left', padx=(6, 0), fill='x', expand=True)

        indicator_box = ttk.LabelFrame(row1, text='Indicators', padding=8)
        indicator_box.pack(side='left', fill='x', padx=(0, 6))
        indicator_row1 = ttk.Frame(indicator_box)
        indicator_row1.pack(side='top', fill='x')
        indicator_row2 = ttk.Frame(indicator_box)
        indicator_row2.pack(side='top', fill='x', pady=(6, 0))
        ttk.Label(indicator_row1, text='Price:').pack(side='left')
        self.price_source_var = tk.StringVar(value='LAST')
        ttk.Combobox(indicator_row1, textvariable=self.price_source_var, width=8, values=PRICE_SOURCES, state='readonly').pack(side='left', padx=(6, 12))
        ttk.Label(indicator_row1, text='MACD:').pack(side='left')
        self.macd_impl_var = tk.StringVar(value='TRADINGVIEW')
        ttk.Combobox(indicator_row1, textvariable=self.macd_impl_var, width=10, values=MACD_IMPLS, state='readonly').pack(side='left', padx=(6, 12))
        ttk.Label(indicator_row1, text='ADX:').pack(side='left')
        self.adx_impl_var = tk.StringVar(value='TRADINGVIEW')
        ttk.Combobox(indicator_row1, textvariable=self.adx_impl_var, width=10, values=ADX_IMPLS, state='readonly').pack(side='left', padx=(6, 0))
        self.live_forming_var = tk.BooleanVar(value=True)
        ttk.Checkbutton(indicator_row2, text='Live uses the forming candle (tick as close)', variable=self.live_forming_var).pack(side='left')

        run_box = ttk.LabelFrame(row1, text='Run / Status', padding=8)
        run_box.pack(side='left', fill='x')
        run_row1 = ttk.Frame(run_box)
        run_row1.pack(side='top', fill='x')
        run_row2 = ttk.Frame(run_box)
        run_row2.pack(side='top', fill='x', pady=(6, 0))
        self.btn_fetch = ttk.Button(run_row1, text='Fetch & Plot (History)', command=self.on_run_history)
        self.btn_fetch.pack(side='left')
        self.btn_live = ttk.Button(run_row1, text='Start Live (Now)', command=self.on_toggle_live)
        self.btn_live.pack(side='left', padx=(6, 0))
        self.btn_backtest = ttk.Button(run_row1, text='Run Backtest', command=self.on_run_backtest)
        self.btn_backtest.pack(side='left', padx=(6, 0))
        self.btn_replay_backtest = ttk.Button(run_row2, text='Apply exits / costs only', command=self.on_replay_backtest_only, state='disabled')
        self.btn_replay_backtest.pack(side='left')
        self.progress = ttk.Progressbar(run_row2, mode='indeterminate', length=180)
        self.progress.pack(side='left')
        self.status_var = tk.StringVar(value='Ready.')
        ttk.Label(run_row2, textvariable=self.status_var, width=28, anchor='w').pack(side='left', padx=(10, 0))

        tf_box = ttk.LabelFrame(row2, text='Timeframes', padding=8)
        tf_box.pack(side='left', fill='x', expand=True, padx=(0, 6))
        self.tf_vars: Dict[str, tk.BooleanVar] = {}
        tf_canvas = tk.Canvas(tf_box, height=28, highlightthickness=0, bd=0)
        tf_canvas.pack(side='top', fill='x', expand=True)
        tf_scroll = ttk.Scrollbar(tf_box, orient='horizontal', command=tf_canvas.xview)
        tf_scroll.pack(side='bottom', fill='x')
        tf_canvas.configure(xscrollcommand=tf_scroll.set)
        tf_inner = ttk.Frame(tf_canvas)
        tf_inner_id = tf_canvas.create_window((0, 0), window=tf_inner, anchor='nw')
        tf_inner.bind('<Configure>', lambda e=None: tf_canvas.configure(scrollregion=tf_canvas.bbox('all')))
        tf_canvas.bind('<Configure>', lambda e: tf_canvas.itemconfigure(tf_inner_id, height=e.height))
        for j, tf in enumerate(ALL_TIMEFRAMES):
            v = tk.BooleanVar(value=True)
            self.tf_vars[tf] = v
            ttk.Checkbutton(tf_inner, text=tf, variable=v).grid(row=0, column=j, padx=6)

        bt_mode_box = ttk.LabelFrame(row2, text='Backtest mode', padding=8)
        bt_mode_box.pack(side='left', fill='x', padx=(0, 6))
        bt_mode_row1 = ttk.Frame(bt_mode_box)
        bt_mode_row1.pack(side='top', fill='x')
        bt_mode_row2 = ttk.Frame(bt_mode_box)
        bt_mode_row2.pack(side='top', fill='x', pady=(6, 0))
        ttk.Label(bt_mode_row1, text='BT Mode:').pack(side='left')
        self.bt_mode_var = tk.StringVar(value=BACKTEST_MODES[0])
        ttk.Combobox(bt_mode_row1, textvariable=self.bt_mode_var, width=18, values=BACKTEST_MODES, state='readonly').pack(side='left', padx=(6, 12))
        try:
            self.bt_mode_var.trace_add('write', self._on_bt_mode_var_changed)
        except Exception:
            pass
        ttk.Label(bt_mode_row1, text='BT TF:').pack(side='left')
        self.bt_step_tf_var = tk.StringVar(value=BACKTEST_STEP_TFS[0])
        ttk.Combobox(bt_mode_row1, textvariable=self.bt_step_tf_var, width=6, values=BACKTEST_STEP_TFS, state='readonly').pack(side='left', padx=(6, 0))
        self.tick_autodownload_var = tk.BooleanVar(value=False)
        ttk.Checkbutton(bt_mode_row2, text='Tick auto-download missing days', variable=self.tick_autodownload_var).pack(side='left')

        workflow_box = ttk.LabelFrame(row2, text='Workflow', padding=8)
        workflow_box.pack(side='left', fill='x')
        workflow_row1 = ttk.Frame(workflow_box)
        workflow_row1.pack(side='top', fill='x')
        workflow_row2 = ttk.Frame(workflow_box)
        workflow_row2.pack(side='top', fill='x', pady=(6, 0))
        workflow_row3 = ttk.Frame(workflow_box)
        workflow_row3.pack(side='top', fill='x', pady=(6, 0))
        self.latch_until_flip_var = tk.BooleanVar(value=True)
        ttk.Checkbutton(workflow_row1, text='Trigger validity stays latched until the opposite state flips', variable=self.latch_until_flip_var, command=lambda: self._on_latch_mode_changed()).pack(side='left', anchor='w')
        self.btn_legend = ttk.Button(workflow_row2, text='Legend', command=self.open_legend)
        self.btn_legend.pack(side='left')
        self.btn_tick_manager = ttk.Button(workflow_row2, text='Tick Manager…', command=self.open_tick_manager)
        self.btn_tick_manager.pack(side='left', padx=(6, 0))
        ttk.Button(workflow_row3, text='Tools…', command=self._open_workspace_tools).pack(side='left')
        ttk.Button(workflow_row3, text='Workflow help…', command=self._open_workflow_help).pack(side='left', padx=(6, 0))
        self.btn_breakout_panel_toggle = ttk.Button(workflow_row3, text='Hide breakout', command=self._toggle_breakout_alert)
        self.btn_breakout_panel_toggle.pack(side='left', padx=(12, 0))
        self.btn_market_board_toggle = ttk.Button(workflow_row3, text='State cards…', command=self._open_market_state_board_popup)
        self.btn_market_board_toggle.pack(side='left', padx=(6, 0))

        preset_box = ttk.LabelFrame(row3, text='Presets', padding=8)
        preset_box.pack(side='left', fill='x', expand=True, padx=(0, 6))
        preset_row1 = ttk.Frame(preset_box)
        preset_row1.pack(side='top', fill='x')
        preset_row2 = ttk.Frame(preset_box)
        preset_row2.pack(side='top', fill='x', pady=(6, 0))
        ttk.Label(preset_row1, text='Preset:').pack(side='left')
        self.preset_var = tk.StringVar(value='')
        self.preset_cmb = ttk.Combobox(preset_row1, textvariable=self.preset_var, width=18, values=[], state='readonly')
        self.preset_cmb.pack(side='left', padx=6)
        self.btn_preset_save = ttk.Button(preset_row1, text='Save…', command=self.on_save_preset)
        self.btn_preset_save.pack(side='left')
        self.btn_preset_load = ttk.Button(preset_row1, text='Load', command=self.on_load_preset)
        self.btn_preset_load.pack(side='left', padx=(6, 0))
        self.btn_preset_del = ttk.Button(preset_row1, text='Delete', command=self.on_delete_preset)
        self.btn_preset_del.pack(side='left', padx=(6, 0))
        self.btn_preset_note = ttk.Button(preset_row1, text='Note…', command=self.on_edit_preset_note)
        self.btn_preset_note.pack(side='left', padx=(6, 0))
        self.btn_preset_visualize = ttk.Button(preset_row1, text='Visualize…', command=self.on_visualize_preset)
        self.btn_preset_visualize.pack(side='left', padx=(6, 0))
        self.btn_preset_outcomes = ttk.Button(preset_row1, text='State Outcomes…', command=self.on_show_market_state_outcomes)
        self.btn_preset_outcomes.pack(side='left', padx=(6, 0))
        self.autoload_last_preset_var = tk.BooleanVar(value=False)
        ttk.Checkbutton(preset_row2, text='Auto-load last preset', variable=self.autoload_last_preset_var, command=self._on_autoload_last_changed).pack(side='left')
        self.preset_note_var = tk.StringVar(value='Preset note: —')
        ttk.Label(preset_row2, textvariable=self.preset_note_var, foreground='#666').pack(side='left', padx=(14, 0))
        try:
            self.preset_cmb.bind('<<ComboboxSelected>>', self._on_preset_combo_changed)
            self.preset_cmb.bind('<Double-Button-1>', lambda e: self.on_visualize_preset())
        except Exception:
            pass

        structure_outer = ttk.LabelFrame(row3, text='Backtest structure', padding=8)
        structure_outer.pack(side='left', fill='x')
        structure_header = ttk.Frame(structure_outer)
        structure_header.pack(side='top', fill='x')
        self.bt_structure_collapsed_var = tk.BooleanVar(value=False)
        self._bt_structure_toggle_btn = ttk.Button(structure_header, text='Hide BT structure', command=self._toggle_backtest_structure)
        self._bt_structure_toggle_btn.pack(side='left')
        ttk.Label(structure_header, text='Keep this collapsed on the laptop and expanded on the ultrawide when you need the extra execution context.', foreground='#666', justify='left', wraplength=420).pack(side='left', padx=(10, 0))
        self._bt_structure_body = ttk.Frame(structure_outer)
        self._bt_structure_body.pack(side='top', fill='x', expand=True, pady=(4, 0))
        self.bt_exec_foundation_var = tk.StringVar(value='')
        self.bt_signal_behavior_var = tk.StringVar(value='')
        self.bt_mode_summary_var = tk.StringVar(value='')
        structure_grid = ttk.Frame(self._bt_structure_body)
        structure_grid.pack(side='top', fill='x')
        ttk.Label(structure_grid, text='Execution foundation:').grid(row=0, column=0, sticky='w')
        ttk.Label(structure_grid, textvariable=self.bt_exec_foundation_var).grid(row=0, column=1, sticky='w', padx=(6, 18))
        ttk.Label(structure_grid, text='Signal behavior:').grid(row=0, column=2, sticky='w')
        ttk.Label(structure_grid, textvariable=self.bt_signal_behavior_var).grid(row=0, column=3, sticky='w', padx=(6, 0))
        ttk.Label(self._bt_structure_body, textvariable=self.bt_mode_summary_var, foreground='#666', wraplength=900, justify='left').pack(side='top', anchor='w', pady=(6, 0))
        ttk.Label(self._bt_structure_body, text='Mapping: Bar Close modes belong to OHLCV Backtest. Tick (aggTrades) belongs to AggTrade Backtest.', foreground='#666', justify='left', wraplength=900).pack(side='top', anchor='w', pady=(2, 0))
        self._refresh_backtest_structure_labels()
        self._set_backtest_structure_collapsed(False)

        self.main_nb = ttk.Notebook(self)
        self.main_nb.pack(side='top', fill='both', expand=True, padx=10, pady=6)
        self.tab_live = ttk.Frame(self.main_nb)
        self.tab_hist = ttk.Frame(self.main_nb)
        self.tab_bt = ttk.Frame(self.main_nb)
        self.main_nb.add(self.tab_live, text='LIVE (Now)')
        self.main_nb.add(self.tab_hist, text='HISTORY (Plots)')
        self.main_nb.add(self.tab_bt, text='BACKTEST')
        self.main_nb.bind('<<NotebookTabChanged>>', self._on_main_tab_changed)
        self._build_live_tab()
        self._build_history_tab()
        self._bt_lazy_placeholder = ttk.Label(
            self.tab_bt,
            text='Backtest workspace loads on first use so startup stays lighter.',
            foreground='#666',
            justify='left',
            padding=18,
        )
        self._bt_lazy_placeholder.pack(side='top', anchor='w')

    def open_legend(self):
        try:
            LegendWindow(self)
        except Exception as e:
            try:
                messagebox.showerror("Legend", str(e))
            except Exception:
                pass

    def open_tick_manager(self):
        """Launch the standalone Tick Manager (daily aggTrades cache downloader)."""
        try:
            cache_dir = (self.cache_var.get().strip() or "data_cache")
            # Ensure Tick Manager uses the SAME cache folder as the main app.
            # (We run the Tick Manager with cwd=project_root, but normalize to absolute for safety.)
            project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
            if not os.path.isabs(cache_dir):
                cache_dir = os.path.abspath(os.path.join(project_root, cache_dir))
            symbol = (self.symbol_var.get().strip().upper() or "")
            start_s = self.start_var.get().strip()
            end_s = self.end_var.get().strip()
            warmup = 0
            try:
                warmup = int(self.warmup_var.get())
            except Exception:
                warmup = 0
    
            # Prefer passing a warmup-start so the cache covers full context if needed.
            try:
                s_ts = parse_utc(start_s) - pd.Timedelta(minutes=warmup)
                start_s = s_ts.strftime("%Y-%m-%d %H:%M:%S")
            except Exception:
                pass
    
            tick_mgr = os.path.join(os.path.dirname(os.path.abspath(__file__)), "tick_manager.py")
            if not os.path.isfile(tick_mgr):
                messagebox.showerror("Tick Manager", f"tick_manager.py not found next to this script:\n{tick_mgr}")
                return
    
            cmd = [sys.executable, tick_mgr, "--cache-dir", cache_dir]
            if symbol:
                cmd += ["--symbol", symbol]
            if start_s:
                cmd += ["--start", start_s]
            if end_s:
                cmd += ["--end", end_s]
    
            subprocess.Popen(cmd, cwd=project_root)
        except Exception as e:
            try:
                messagebox.showerror("Tick Manager", str(e))
            except Exception:
                pass
    
    
    def _build_market_state_popup(self) -> None:
        win = tk.Toplevel(self)
        win.title('Market State Board')
        win.withdraw()
        try:
            win.transient(self)
        except Exception:
            pass
        try:
            win.protocol('WM_DELETE_WINDOW', self._close_market_state_board_popup)
        except Exception:
            pass
        outer = ttk.Frame(win, padding=8)
        outer.pack(fill='both', expand=True)
        header = ttk.Frame(outer)
        header.pack(side='top', fill='x')
        ttk.Label(header, text='Higher timeframes describe regime and phase; lower timeframes are for timing.', foreground='#666', justify='left').pack(side='left', fill='x', expand=True)
        ttk.Button(header, text='Refresh', command=self._refresh_market_state_cards_from_latest).pack(side='right')
        ttk.Button(header, text='Diagnostics…', command=self._open_workspace_tools).pack(side='right', padx=(0, 6))
        ttk.Button(header, text='Close', command=self._close_market_state_board_popup).pack(side='right', padx=(0, 6))

        body = ttk.Frame(outer)
        body.pack(side='top', fill='both', expand=True, pady=(6, 0))
        ttk.Label(body, text='Use State... for structure details, OF... for order-flow details, right-click to add state rules.', foreground='#666', justify='left').pack(side='top', anchor='w', pady=(0, 6))

        canvas = tk.Canvas(body, height=290, highlightthickness=0, bd=0)
        canvas.pack(side='top', fill='both', expand=True)
        hsb = ttk.Scrollbar(body, orient='horizontal', command=canvas.xview)
        hsb.pack(side='bottom', fill='x')
        canvas.configure(xscrollcommand=hsb.set)
        inner = ttk.Frame(canvas)
        window_id = canvas.create_window((0, 0), window=inner, anchor='nw')
        inner.bind('<Configure>', lambda e=None: canvas.configure(scrollregion=canvas.bbox('all')))
        canvas.bind('<Configure>', lambda e: canvas.itemconfigure(window_id, height=e.height))

        self._market_state_popup = win
        self._market_state_popup_outer = outer
        self._market_state_popup_body = body
        self._state_board_canvas = canvas
        self._state_board_hsb = hsb
        self._state_board_inner = inner
        self._state_board_window = window_id
        self._market_state_outer = None
        self._market_state_body = None
        self.market_state_collapsed_var = tk.BooleanVar(value=True)
        self.market_state_cards = {}
        for tf in ALL_TIMEFRAMES:
            card = tk.Frame(inner, width=225, height=306, bd=1, relief='solid', bg='#f4f4f4', highlightthickness=1, highlightbackground='#d4d4d4', highlightcolor='#d4d4d4', cursor='hand2')
            card.pack(side='left', padx=4, pady=2)
            card.pack_propagate(False)
            tf_lbl = tk.Label(card, text=tf, font=('Segoe UI', 10, 'bold'), fg=C_TEXT, bg='#f4f4f4', anchor='w', cursor='hand2')
            tf_lbl.pack(side='top', anchor='w', padx=8, pady=(6, 0))
            state_btn = ttk.Button(card, text='State...', width=7, command=lambda tf=tf: self._show_market_state_details(tf))
            try:
                state_btn.place(relx=1.0, x=-60, y=6, anchor='ne')
            except Exception:
                pass
            of_btn = ttk.Button(card, text='OF…', width=4, command=lambda tf=tf: self._show_orderflow_details(tf))
            try:
                of_btn.place(relx=1.0, x=-8, y=6, anchor='ne')
            except Exception:
                pass
            state_lbl = tk.Label(card, text='-', font=('Segoe UI', 11, 'bold'), fg=C_DIM, bg='#f4f4f4', anchor='w', justify='left', cursor='hand2')
            state_lbl.pack(side='top', anchor='w', padx=8, pady=(2, 0))
            bias_lbl = tk.Label(card, text='Bias: -   Phase: -', font=('Segoe UI', 9), fg=C_DIM, bg='#f4f4f4', anchor='w', justify='left', cursor='hand2')
            bias_lbl.pack(side='top', anchor='w', padx=8, pady=(2, 0))
            meta_lbl = tk.Label(card, text='Confidence: -   Age: -', font=('Segoe UI', 9), fg=C_DIM, bg='#f4f4f4', anchor='w', justify='left', cursor='hand2')
            meta_lbl.pack(side='top', anchor='w', padx=8, pady=(1, 0))
            vidya_lbl = tk.Label(card, text='VIDYA ∠: -   ΔVol: -', font=('Consolas', 9), fg=C_DIM, bg='#f4f4f4', anchor='w', justify='left', cursor='hand2')
            vidya_lbl.pack(side='top', anchor='w', padx=8, pady=(4, 0))
            breakout_lbl = tk.Label(card, text='Breakout: -', font=('Segoe UI', 8, 'bold'), fg=C_DIM, bg='#f4f4f4', anchor='w', justify='left', wraplength=206, cursor='hand2')
            breakout_lbl.pack(side='top', anchor='w', padx=8, pady=(3, 0), fill='x')
            warning_lbl = tk.Label(card, text='Warning: -', font=('Segoe UI', 8), fg=C_DIM, bg='#f4f4f4', anchor='w', justify='left', wraplength=206, cursor='hand2')
            warning_lbl.pack(side='top', anchor='w', padx=8, pady=(2, 0), fill='x')
            structure_lbl = tk.Label(card, text='Structure: -', font=('Segoe UI', 8), fg=C_DIM, bg='#f4f4f4', anchor='w', justify='left', wraplength=206, cursor='hand2')
            structure_lbl.pack(side='top', anchor='w', padx=8, pady=(2, 0), fill='x')
            pa_flag_lbl = tk.Label(card, text='PA-Vol: -', font=('Segoe UI', 8), fg=C_DIM, bg='#f4f4f4', anchor='w', justify='left', wraplength=206, cursor='hand2')
            pa_flag_lbl.pack(side='top', anchor='w', padx=8, pady=(1, 0), fill='x')
            strict_lbl = tk.Label(card, text='Strict: -', font=('Segoe UI', 8), fg=C_DIM, bg='#f4f4f4', anchor='w', justify='left', wraplength=206, cursor='hand2')
            strict_lbl.pack(side='top', anchor='w', padx=8, pady=(1, 0), fill='x')
            of_risk_lbl = tk.Label(card, text='OF Risk: -', font=('Segoe UI', 8), fg=C_DIM, bg='#f4f4f4', anchor='w', justify='left', wraplength=206, cursor='hand2')
            of_risk_lbl.pack(side='top', anchor='w', padx=8, pady=(1, 0), fill='x')
            playbook_lbl = tk.Label(card, text='Waiting for data', font=('Segoe UI', 8), fg='#555', bg='#f4f4f4', anchor='w', justify='left', wraplength=206, cursor='hand2')
            playbook_lbl.pack(side='top', anchor='w', padx=8, pady=(2, 6), fill='x')
            widgets = {'frame': card, 'tf': tf_lbl, 'state': state_lbl, 'bias': bias_lbl, 'meta': meta_lbl, 'vidya': vidya_lbl, 'breakout': breakout_lbl, 'warning': warning_lbl, 'structure': structure_lbl, 'pa_flag': pa_flag_lbl, 'strict': strict_lbl, 'of_risk': of_risk_lbl, 'playbook': playbook_lbl, 'state_btn': state_btn, 'of_btn': of_btn}
            self.market_state_cards[tf] = widgets
            for key, w in widgets.items():
                if key in {'state_btn', 'of_btn'}:
                    continue
                try:
                    w.bind('<Double-Button-1>', lambda e, tf=tf: self._show_market_state_details(tf))
                    w.bind('<Button-3>', lambda e, tf=tf: self._on_market_state_card_click(e, tf))
                    w.bind('<Button-2>', lambda e, tf=tf: self._on_market_state_card_click(e, tf))
                    w.bind('<Control-Button-1>', lambda e, tf=tf: self._on_market_state_card_click(e, tf))
                except Exception:
                    pass
        self._market_state_popup_built = True
        self._reset_market_state_cards()

    def _build_live_breakout_alert(self, parent) -> None:
        self.breakout_alert_collapsed_var = tk.BooleanVar(value=False)
        self.breakout_alert_summary_var = tk.StringVar(value='No breakout alert latched.')
        alert_header = ttk.Frame(parent)
        alert_header.pack(side='top', fill='x')
        self._breakout_alert_toggle_btn = ttk.Button(alert_header, text='Hide breakout', command=self._toggle_breakout_alert)
        self._breakout_alert_toggle_btn.pack(side='left')
        ttk.Label(alert_header, textvariable=self.breakout_alert_summary_var, foreground=C_AMBER).pack(side='left', padx=(10, 0), fill='x', expand=True)
        ttk.Button(alert_header, text='Clear alert latch', command=self._clear_breakout_alert_banner).pack(side='right')
        self._breakout_alert_body = ttk.Frame(parent)
        self._breakout_alert_body.pack(side='top', fill='x', expand=True, pady=(4, 0))

        self._breakout_alert_banner_frame = tk.Frame(self._breakout_alert_body, bg=C_BG_NEU, bd=2, relief='solid', highlightthickness=2, highlightbackground=C_BORDER_IDLE, highlightcolor=C_BORDER_IDLE)
        self._breakout_alert_banner_frame.pack(side='top', fill='x', expand=True)
        self._breakout_alert_banner_head = tk.Label(self._breakout_alert_banner_frame, text='No breakout alert latched.', font=('Segoe UI', 15, 'bold'), anchor='w', justify='left', bg=C_BG_NEU, fg=C_TEXT, padx=10, pady=6)
        self._breakout_alert_banner_head.pack(side='top', fill='x')
        self._breakout_alert_banner_detail = tk.Label(self._breakout_alert_banner_frame, text='When a live compression-release breakout turns trigger-ready, the app latches it here, beeps, flashes, and records the exact UTC time.', font=('Segoe UI', 9), anchor='w', justify='left', bg=C_BG_NEU, fg=C_DIM, wraplength=1100, padx=10, pady=0)
        self._breakout_alert_banner_detail.pack(side='top', fill='x', pady=(0, 8))

    def _build_live_signal_table(self, parent) -> None:
        price_bar = tk.Frame(parent, bg=C_BG_ELEVATED, highlightthickness=1, highlightbackground=C_BORDER_IDLE, highlightcolor=C_BORDER_IDLE)
        price_bar.pack(side='top', fill='x', pady=(0, 8))
        self.live_price_var = tk.StringVar(value='--')
        self.live_price_meta_var = tk.StringVar(value='Waiting for live price...')
        tk.Label(price_bar, text='BTCUSDT', font=('Segoe UI', 9, 'bold'), fg=C_DIM, bg=C_BG_ELEVATED, padx=10, pady=6).pack(side='left')
        tk.Label(price_bar, textvariable=self.live_price_var, font=('Consolas', 22, 'bold'), fg=C_TEXT, bg=C_BG_ELEVATED, padx=6, pady=4).pack(side='left')
        tk.Label(price_bar, textvariable=self.live_price_meta_var, font=('Segoe UI', 9), fg=C_DIM, bg=C_BG_ELEVATED, padx=10, pady=6).pack(side='left')

        ttk.Label(parent, text='Live price is shown once above. The table below is slower and lighter on purpose.', foreground=C_DIM, justify='left').pack(side='top', anchor='w', pady=(0, 6))
        table_scroll = ttk.Frame(parent)
        table_scroll.pack(side='top', fill='both', expand=True)
        self._live_table_canvas = tk.Canvas(table_scroll, highlightthickness=0, bd=0, bg=C_BG_PANEL)
        self._live_table_vsb = ttk.Scrollbar(table_scroll, orient='vertical', command=self._live_table_canvas.yview)
        self._live_table_hsb = ttk.Scrollbar(table_scroll, orient='horizontal', command=self._live_table_canvas.xview)
        self._live_table_canvas.configure(yscrollcommand=self._live_table_vsb.set, xscrollcommand=self._live_table_hsb.set)
        self._live_table_vsb.pack(side='right', fill='y')
        self._live_table_hsb.pack(side='bottom', fill='x')
        self._live_table_canvas.pack(side='left', fill='both', expand=True)
        table_box = tk.Frame(self._live_table_canvas, bg=C_BG_PANEL, padx=6, pady=6)
        self._live_table_window = self._live_table_canvas.create_window((0, 0), window=table_box, anchor='nw')
        table_box.bind('<Configure>', lambda e=None: self._live_table_canvas.configure(scrollregion=self._live_table_canvas.bbox('all')))
        self._live_table_canvas.bind('<Configure>', lambda e: self._live_table_canvas.itemconfigure(self._live_table_window, width=max(e.width, table_box.winfo_reqwidth())))

        self._bind_mousewheel_handlers(
            table_box,
            y_handler=self._scroll_live_table_y,
            x_handler=self._scroll_live_table_x,
        )
        self._bind_mousewheel_handlers(
            self._live_table_canvas,
            y_handler=self._scroll_live_table_y,
            x_handler=self._scroll_live_table_x,
        )

        self.live_cols = ['TF', 'Age(s)', 'MACD', 'Signal', 'M/S', 'Angle', 'SigAngle', 'PPO', 'FlipAge', 'ADX', 'ADXAngle', 'ATR', 'ATRAngle', 'FRAMA', 'VIDYA', 'Range', 'RBuyAge', 'RSellAge', '+DI', '-DI']
        widths = [6, 8, 10, 10, 6, 6, 8, 6, 9, 8, 8, 8, 8, 10, 9, 9, 9, 9, 8, 8]
        header_font = ('Segoe UI', 9, 'bold')
        cell_font = ('Consolas', 9)
        self.live_cells = {}
        for j, c in enumerate(self.live_cols):
            tk.Label(table_box, text=c, font=header_font, fg=C_TEXT, bg=C_BG_ELEVATED, anchor='center').grid(row=0, column=j, sticky='nsew', padx=2, pady=2)
            width_units = widths[j] if j < len(widths) else 10
            table_box.grid_columnconfigure(j, weight=1, minsize=width_units * 9)
        for i, tf in enumerate(ALL_TIMEFRAMES, start=1):
            row: Dict[str, tk.Label] = {}
            for j, c in enumerate(self.live_cols):
                txt = tf if c == 'TF' else '-'
                lbl = tk.Label(table_box, text=txt, font=cell_font, fg=C_DIM, bg=C_BG_PANEL, anchor='center', cursor='hand2')
                lbl.grid(row=i, column=j, sticky='nsew', padx=2, pady=1)
                row[c] = lbl
            self.live_cells[tf] = row
        for tf in ALL_TIMEFRAMES:
            for col in self.live_cols:
                if col == 'TF':
                    continue
                self.live_cells[tf][col].bind('<Button-3>', partial(self._on_live_cell_click, tf=tf, col=col))
                self.live_cells[tf][col].bind('<Control-Button-1>', partial(self._on_live_cell_click, tf=tf, col=col))
                if col == 'VIDYA':
                    self.live_cells[tf][col].bind('<Double-Button-1>', lambda e, tf=tf: self._show_vidya_details(tf))
                elif col == 'Range':
                    self.live_cells[tf][col].bind('<Double-Button-1>', lambda e, tf=tf: self._show_range_details(tf))

    def _build_live_tab(self):
        self.live_pane = ttk.Panedwindow(self.tab_live, orient='vertical')
        self.live_pane.pack(side='top', fill='both', expand=True)

        top_outer, top, top_canvas = self._create_scrollable_frame(self.live_pane, yscroll=True, padding=0)
        bot = ttk.Frame(self.live_pane)
        self._live_top_outer = top_outer
        self._live_top_frame = top
        self._live_top_canvas = top_canvas
        self._live_bottom_frame = bot
        self.live_pane.add(top_outer, weight=1)
        self.live_pane.add(bot, weight=5)

        alert_outer = tk.LabelFrame(top, text='Breakout alert', padx=8, pady=6, bg=C_BG_PANEL, fg=C_TEXT)
        self._breakout_alert_outer = alert_outer
        self._live_breakout_parent = alert_outer
        alert_outer.pack(side='top', fill='x', padx=6, pady=(6, 0))
        self._live_top_placeholders['breakout'] = ttk.Label(
            alert_outer,
            text='Breakout banner wakes up after the main window appears so startup stays lighter.',
            foreground='#6b5a2f',
            justify='left',
            padding=10,
        )
        self._live_top_placeholders['breakout'].pack(side='top', anchor='w', fill='x')
        self._breakout_alert_body = None
        self._breakout_alert_banner_frame = None
        self._breakout_alert_banner_head = None
        self._breakout_alert_banner_detail = None
        self._breakout_alert_toggle_btn = None
        self.breakout_alert_collapsed_var = tk.BooleanVar(value=False)
        self.breakout_alert_summary_var = tk.StringVar(value='No breakout alert latched.')

        table_outer = ttk.LabelFrame(top, text='Signal / indicator table', padding=6)
        self._live_table_outer = table_outer
        self._live_table_parent = table_outer
        table_outer.pack(side='top', fill='both', expand=True, padx=6, pady=(6, 0))
        self._live_top_placeholders['table'] = ttk.Label(
            table_outer,
            text='Signal table wakes up after the main window appears so startup stays lighter.',
            foreground='#666',
            justify='left',
            padding=18,
        )
        self._live_top_placeholders['table'].pack(side='top', anchor='w')
        self._live_table_canvas = None
        self._live_table_vsb = None
        self._live_table_hsb = None
        self._live_table_window = None

        bottom_nb = ttk.Notebook(bot)
        bottom_nb.pack(side='top', fill='both', expand=True, padx=10, pady=10)
        self._live_workspace_nb = bottom_nb
        self.tab_builder = ttk.Frame(bottom_nb)
        self.tab_log = ttk.Frame(bottom_nb)
        self.tab_alerts = ttk.Frame(bottom_nb)
        self._live_workspace_tab_frames = {
            'builder': self.tab_builder,
            'log': self.tab_log,
            'alerts': self.tab_alerts,
        }
        bottom_nb.add(self.tab_builder, text='RULE BUILDER')
        bottom_nb.add(self.tab_log, text='LOG')
        bottom_nb.add(self.tab_alerts, text='BREAKOUT ALERTS')
        self._live_workspace_tab_placeholders['builder'] = ttk.Label(
            self.tab_builder,
            text='Rule builder wakes up after the main window appears.',
            foreground='#666',
            justify='left',
            padding=18,
        )
        self._live_workspace_tab_placeholders['builder'].pack(side='top', anchor='w')
        self._live_workspace_tab_placeholders['log'] = ttk.Label(
            self.tab_log,
            text='Live log opens on first use so startup stays lighter.',
            foreground='#666',
            justify='left',
            padding=18,
        )
        self._live_workspace_tab_placeholders['log'].pack(side='top', anchor='w')
        self._live_workspace_tab_placeholders['alerts'] = ttk.Label(
            self.tab_alerts,
            text='Breakout alert history opens on first use so startup stays lighter.',
            foreground='#666',
            justify='left',
            padding=18,
        )
        self._live_workspace_tab_placeholders['alerts'].pack(side='top', anchor='w')
        try:
            bottom_nb.bind('<<NotebookTabChanged>>', self._on_live_workspace_tab_changed)
        except Exception:
            pass
        self._schedule_initial_live_top_build()
        self._schedule_initial_live_workspace_build()
        self._queue_live_layout_refresh()

    def _build_rule_builder(self, parent):
        header = ttk.Frame(parent, padding=8)
        header.pack(side="top", fill="x")

        self.builder_status = tk.StringVar(value="Pick Tab + Group. Right-click live cells to add rules (State or Event).")
        ttk.Label(header, textvariable=self.builder_status).pack(side="left")

        self.rules_nb = ttk.Notebook(parent)
        self.rules_nb.pack(side="top", fill="both", expand=True, padx=10, pady=10)
        self._bind_mousewheel_handlers(parent, y_handler=lambda units, frm=parent: self._scroll_rule_groups_canvas(getattr(frm, '_groups_canvas', None), units))
        self._bind_mousewheel_handlers(self.rules_nb, y_handler=lambda units, frm=parent: self._scroll_rule_groups_canvas(getattr(frm, '_groups_canvas', None), units))

        self.rules_tab_frames: Dict[str, ttk.Frame] = {}
        self.tab_status_labels: Dict[str, tk.Label] = {}
        self.tab_group_join_var: Dict[str, tk.StringVar] = {}

        for name in RULE_TABS:
            frm = ttk.Frame(self.rules_nb)
            self.rules_nb.add(frm, text=name)
            self.rules_tab_frames[name] = frm
            placeholder = ttk.Label(
                frm,
                text='This rule tab loads on first open so the main window appears faster.',
                foreground='#666',
                justify='left',
                padding=16,
            )
            placeholder.pack(side='top', anchor='w')
            self._rule_tab_placeholders[name] = placeholder
            self._rule_tabs_built[name] = False

        self.rules_nb.bind("<<NotebookTabChanged>>", self._on_rules_tab_change)

        initial_tab = RULE_TABS[0]
        self._ensure_rule_tab_built(initial_tab)

        self.active_tab = initial_tab
        self._set_active_group(self.active_tab, 0)

        # Backwards-compat alias (older code uses tab_status_lbls)
        self.tab_status_lbls = self.tab_status_labels

    def _build_rule_tab(self, parent, tab_name: str):
        top = ttk.Frame(parent, padding=8)
        top.pack(side="top", fill="x")

        status_lbl = tk.Label(top, text="STATUS: WAIT", fg=C_DIM, font=("Segoe UI", 9, "bold"))
        status_lbl.pack(side="left")
        self.tab_status_labels[tab_name] = status_lbl

        ttk.Label(top, text="Groups:", padding=(12, 0)).pack(side="left")
        self.tab_group_join_var[tab_name] = tk.StringVar(value=self.tab_group_join_mode[tab_name])
        cmb_groups = ttk.Combobox(top, textvariable=self.tab_group_join_var[tab_name], values=["AND", "OR"], width=4, state="readonly")
        cmb_groups.pack(side="left", padx=6)
        cmb_groups.bind("<<ComboboxSelected>>", lambda e, t=tab_name: self._on_tab_group_join_change(t))

        btn_add = ttk.Button(top, text="Add Group", command=lambda: self._add_group(tab_name))
        btn_add.pack(side="right", padx=6)
        btn_clear = ttk.Button(top, text="Clear All", command=lambda: self._clear_tab(tab_name))
        btn_clear.pack(side="right", padx=6)

        # groups container with vertical scroll
        container = tk.Frame(parent, bg=C_BG_PANEL)
        container.pack(side="top", fill="both", expand=True, padx=8, pady=8)

        canvas = tk.Canvas(container, bg=C_BG_PANEL, highlightthickness=0)
        vscroll = tk.Scrollbar(container, orient="vertical", command=canvas.yview)
        canvas.configure(yscrollcommand=vscroll.set)

        vscroll.pack(side="right", fill="y")
        canvas.pack(side="left", fill="both", expand=True)

        inner = tk.Frame(canvas, bg=C_BG_PANEL)
        inner_id = canvas.create_window((0, 0), window=inner, anchor="nw")

        def _on_inner_config(event=None):
            canvas.configure(scrollregion=canvas.bbox("all"))
        inner.bind("<Configure>", _on_inner_config)

        def _on_canvas_config(event):
            canvas.itemconfigure(inner_id, width=event.width)
        canvas.bind("<Configure>", _on_canvas_config)

        def _scroll_groups(units: int):
            try:
                canvas.yview_scroll(int(units), 'units')
            except Exception:
                pass
            return 'break'

        def _on_mousewheel(event):
            delta = getattr(event, 'delta', 0)
            if delta:
                step = int(-1 * (delta / 120)) or (-1 if delta > 0 else 1)
                return _scroll_groups(step)
            num = getattr(event, 'num', None)
            if num == 4:
                return _scroll_groups(-1)
            if num == 5:
                return _scroll_groups(1)
            return None

        for widget in (canvas, inner):
            try:
                widget.bind('<MouseWheel>', _on_mousewheel, add='+')
                widget.bind('<Button-4>', _on_mousewheel, add='+')
                widget.bind('<Button-5>', _on_mousewheel, add='+')
            except Exception:
                pass

        parent._groups_canvas = canvas  # type: ignore[attr-defined]
        parent._groups_inner = inner  # type: ignore[attr-defined]
        self._bind_mousewheel_handlers(parent, y_handler=lambda units, c=canvas: self._scroll_rule_groups_canvas(c, units))
        self._bind_mousewheel_handlers(top, y_handler=lambda units, c=canvas: self._scroll_rule_groups_canvas(c, units))
        self._bind_mousewheel_handlers(container, y_handler=lambda units, c=canvas: self._scroll_rule_groups_canvas(c, units))
        self._bind_mousewheel_handlers(canvas, y_handler=lambda units, c=canvas: self._scroll_rule_groups_canvas(c, units))
        self._bind_mousewheel_handlers(inner, y_handler=lambda units, c=canvas: self._scroll_rule_groups_canvas(c, units))

    def _format_breakout_alert_record_line(self, rec: Dict[str, Any]) -> str:
        line = (
            f"[{rec.get('event_time', '-')}] TF={rec.get('tf', '-')} DIR={rec.get('direction', '-')} "
            f"PRICE={rec.get('price', '-')} BREAKOUT={rec.get('breakout', '-')} "
            f"STATE={rec.get('state', '-')} OF={rec.get('of_risk', '-')} "
            f"VWAP={rec.get('vwap_bias', '-')} BAR={rec.get('bar_time', '-')}"
        )
        note = str(rec.get('note') or '').strip()
        if note:
            line += f" NOTE={note}"
        return line

    def _build_log(self, parent):
        lf = ttk.LabelFrame(parent, text="Live log", padding=10)
        lf.pack(side="top", fill="both", expand=True, padx=10, pady=10)
        self.txt_live = tk.Text(lf, height=14, state="disabled", font=("Consolas", 9))
        self.txt_live.pack(side="top", fill="both", expand=True)
        pending = list(getattr(self, '_live_log_buffer', []) or [])
        if pending:
            self.txt_live.configure(state="normal")
            try:
                self.txt_live.insert("end", "\n".join(str(line) for line in pending) + "\n")
                self.txt_live.see("end")
            finally:
                self.txt_live.configure(state="disabled")

    def _build_breakout_alerts(self, parent):
        outer = ttk.LabelFrame(parent, text="Recorded breakout alerts", padding=10)
        outer.pack(side="top", fill="both", expand=True, padx=10, pady=10)

        ttk.Label(
            outer,
            text="Every time a live breakout turns trigger-ready, the app writes a timestamped record here so you can verify later whether the alert fired.",
            foreground="#666",
            justify="left",
        ).pack(side="top", anchor="w", pady=(0, 6))

        btns = ttk.Frame(outer)
        btns.pack(side="top", fill="x", pady=(0, 6))
        ttk.Button(btns, text="Copy alerts", command=self._copy_breakout_alert_history).pack(side="left")
        ttk.Button(btns, text="Clear records", command=self._clear_breakout_alert_history).pack(side="left", padx=(6, 0))

        self.txt_breakout_alerts = tk.Text(outer, height=16, state="disabled", font=("Consolas", 9), wrap="none")
        self.txt_breakout_alerts.pack(side="top", fill="both", expand=True)
        records = list(getattr(self, '_breakout_alert_records', []) or [])
        if records:
            lines = [self._format_breakout_alert_record_line(rec) for rec in records]
            self.txt_breakout_alerts.configure(state="normal")
            try:
                self.txt_breakout_alerts.insert("end", "\n".join(lines) + "\n")
                self.txt_breakout_alerts.see("end")
            finally:
                self.txt_breakout_alerts.configure(state="disabled")

    def _on_bt_chart_tab_changed(self, event=None) -> None:
        try:
            nb = getattr(self, 'bt_chart_nb', None)
            if nb is None:
                return
            selected = nb.nametowidget(nb.select())
        except Exception:
            selected = getattr(self, '_bt_chart_tab_price', None)
        try:
            if selected is getattr(self, '_bt_chart_tab_equity', None):
                self._ensure_bt_equity_chart_built()
            else:
                self._ensure_bt_price_chart_built()
        except Exception:
            pass

    def _ensure_bt_price_chart_built(self) -> None:
        try:
            if bool(getattr(self, '_bt_price_chart_built', False)):
                return
        except Exception:
            pass
        parent = getattr(self, '_bt_chart_tab_price', None)
        if parent is None:
            return
        placeholder = getattr(self, '_bt_price_chart_placeholder', None)
        if placeholder is not None:
            try:
                placeholder.destroy()
            except Exception:
                pass
            self._bt_price_chart_placeholder = None

        self.bt_price_fig = plt.Figure(figsize=(12.8, 6.6), dpi=100)
        gs = self.bt_price_fig.add_gridspec(nrows=2, ncols=1, height_ratios=[3.2, 1.4], hspace=0.06)
        self.bt_price_ax = self.bt_price_fig.add_subplot(gs[0, 0])
        self.bt_macd_ax = self.bt_price_fig.add_subplot(gs[1, 0], sharex=self.bt_price_ax)
        self.bt_price_canvas = FigureCanvasTkAgg(self.bt_price_fig, master=parent)
        self.bt_price_canvas.get_tk_widget().pack(side='top', fill='both', expand=True)
        self.bt_price_toolbar = NavigationToolbar2Tk(self.bt_price_canvas, parent)
        self.bt_price_toolbar.update()
        self.bt_price_toolbar.pack(side='bottom', fill='x')
        self.bt_entry_scatter = None
        self.bt_exit_scatter = None
        self.bt_entry_ids = []
        self.bt_exit_ids = []
        self.bt_line_to_trade_id = {}
        self.bt_trade_id_to_line = {}
        self.bt_selected_trade_id = None
        self.bt_chart_full_xlim = None
        self.bt_price_canvas.mpl_connect('pick_event', self._bt_on_chart_pick)
        self.bt_price_canvas.mpl_connect('scroll_event', self._bt_on_chart_scroll)
        try:
            self.bt_price_canvas.draw_idle()
        except Exception:
            pass
        self._bt_price_chart_built = True

    def _ensure_bt_equity_chart_built(self) -> None:
        try:
            if bool(getattr(self, '_bt_equity_chart_built', False)):
                return
        except Exception:
            pass
        parent = getattr(self, '_bt_chart_tab_equity', None)
        if parent is None:
            return
        placeholder = getattr(self, '_bt_equity_chart_placeholder', None)
        if placeholder is not None:
            try:
                placeholder.destroy()
            except Exception:
                pass
            self._bt_equity_chart_placeholder = None

        self.bt_equity_fig = plt.Figure(figsize=(12.8, 4.8), dpi=100)
        self.bt_equity_ax = self.bt_equity_fig.add_subplot(111)
        self.bt_equity_canvas = FigureCanvasTkAgg(self.bt_equity_fig, master=parent)
        self.bt_equity_canvas.get_tk_widget().pack(side='top', fill='both', expand=True)
        self.bt_equity_toolbar = NavigationToolbar2Tk(self.bt_equity_canvas, parent)
        self.bt_equity_toolbar.update()
        self.bt_equity_toolbar.pack(side='bottom', fill='x')
        try:
            self.bt_equity_canvas.draw_idle()
        except Exception:
            pass
        self._bt_equity_chart_built = True

    def _build_history_tab(self):
        self.hist_nb = ttk.Notebook(self.tab_hist)
        self.hist_nb.pack(side="top", fill="both", expand=True)

        placeholder = ttk.Frame(self.hist_nb)
        ttk.Label(placeholder, text="Click 'Fetch & Plot (History)' to generate charts.", padding=20).pack()
        self.hist_nb.add(placeholder, text="(no charts yet)")

    def _build_backtest_tab(self):
        outer = ttk.Frame(self.tab_bt, padding=10)
        outer.pack(side='top', fill='both', expand=True)

        self.bt_settings_collapsed_var = tk.BooleanVar(value=False)
        settings_outer = ttk.LabelFrame(outer, text='Backtest settings / exits', padding=8)
        settings_outer.pack(side='top', fill='x')
        settings_header = ttk.Frame(settings_outer)
        settings_header.pack(side='top', fill='x')
        self._bt_settings_toggle_btn = ttk.Button(settings_header, text='Hide settings', command=self._toggle_backtest_settings)
        self._bt_settings_toggle_btn.pack(side='left')
        ttk.Label(settings_header, text='Collapse this on smaller screens once the run parameters are set.', foreground='#666', justify='left').pack(side='left', padx=(10, 0), fill='x', expand=True)
        self._bt_settings_body = ttk.Frame(settings_outer)
        self._bt_settings_body.pack(side='top', fill='x', expand=False, pady=(4, 0))
        settings = ttk.Frame(self._bt_settings_body)
        settings.pack(side='top', fill='x')

        # Vars
        self.bt_init_balance_var = tk.DoubleVar(value=1000.0)
        self.bt_notional_var = tk.DoubleVar(value=100.0)
        self.bt_fee_pct_var = tk.DoubleVar(value=0.04)         # percent
        self.bt_slippage_bps_var = tk.DoubleVar(value=0.0)     # bps
        self.bt_stop_loss_pct_var = tk.DoubleVar(value=0.0)    # generic stop value; semantics depend on mode
        self.bt_take_profit_pct_var = tk.DoubleVar(value=0.0)  # generic target value; semantics depend on mode
        self.bt_stop_loss_mode_var = tk.StringVar(value='PERCENT')
        self.bt_take_profit_mode_var = tk.StringVar(value='PERCENT')
        self.bt_stop_loss_tf_var = tk.StringVar(value='1m')
        self.bt_take_profit_tf_var = tk.StringVar(value='1m')
        self.bt_stop_loss_field_var = tk.StringVar(value='')
        self.bt_take_profit_field_var = tk.StringVar(value='')
        self.bt_exit_field_choices = [
            '', 'atr', 'frama', 'frama_upper', 'frama_lower', 'vidya_line', 'vidya_upper', 'vidya_lower',
            'vidya_nearest_liq_low', 'vidya_nearest_liq_high', 'range_filter_line', 'range_filter_upper', 'range_filter_lower',
        ]
        self.bt_stop_loss_offset_pct_var = tk.DoubleVar(value=0.0)
        self.bt_take_profit_offset_pct_var = tk.DoubleVar(value=0.0)
        self.bt_reverse_var = tk.BooleanVar(value=True)
        self.bt_skip_both_var = tk.BooleanVar(value=True)

        r = 0
        ttk.Label(settings, text='Initial balance (USDT):').grid(row=r, column=0, sticky='w')
        ttk.Entry(settings, textvariable=self.bt_init_balance_var, width=12).grid(row=r, column=1, padx=6, sticky='w')
        ttk.Label(settings, text='Order notional (USDT):').grid(row=r, column=2, sticky='w', padx=(16, 0))
        ttk.Entry(settings, textvariable=self.bt_notional_var, width=12).grid(row=r, column=3, padx=6, sticky='w')
        ttk.Label(settings, text='Fee (% per side):').grid(row=r, column=4, sticky='w', padx=(16, 0))
        ttk.Entry(settings, textvariable=self.bt_fee_pct_var, width=10).grid(row=r, column=5, padx=6, sticky='w')
        ttk.Label(settings, text='Slippage (bps):').grid(row=r, column=6, sticky='w', padx=(16, 0))
        ttk.Entry(settings, textvariable=self.bt_slippage_bps_var, width=10).grid(row=r, column=7, padx=6, sticky='w')

        r = 1
        ttk.Label(settings, text='Stop mode:').grid(row=r, column=0, sticky='w', pady=(6, 0))
        ttk.Combobox(settings, textvariable=self.bt_stop_loss_mode_var, values=['OFF', 'PERCENT', 'ABSOLUTE', 'ATR', 'FIELD'], width=12, state='readonly').grid(row=r, column=1, padx=6, sticky='w', pady=(6, 0))
        ttk.Label(settings, text='Stop value:').grid(row=r, column=2, sticky='w', padx=(16, 0), pady=(6, 0))
        ttk.Entry(settings, textvariable=self.bt_stop_loss_pct_var, width=10).grid(row=r, column=3, padx=6, sticky='w', pady=(6, 0))
        ttk.Label(settings, text='Stop TF:').grid(row=r, column=4, sticky='w', padx=(16, 0), pady=(6, 0))
        ttk.Combobox(settings, textvariable=self.bt_stop_loss_tf_var, values=list(ALL_TIMEFRAMES), width=8, state='normal').grid(row=r, column=5, padx=6, sticky='w', pady=(6, 0))
        ttk.Label(settings, text='Stop field:').grid(row=r, column=6, sticky='w', padx=(16, 0), pady=(6, 0))
        ttk.Combobox(settings, textvariable=self.bt_stop_loss_field_var, values=self.bt_exit_field_choices, width=22, state='normal').grid(row=r, column=7, padx=6, sticky='w', pady=(6, 0))

        r = 2
        ttk.Label(settings, text='Stop adj (%):').grid(row=r, column=0, sticky='w', pady=(6, 0))
        ttk.Entry(settings, textvariable=self.bt_stop_loss_offset_pct_var, width=10).grid(row=r, column=1, padx=6, sticky='w', pady=(6, 0))
        ttk.Label(settings, text='Target mode:').grid(row=r, column=2, sticky='w', padx=(16, 0), pady=(6, 0))
        ttk.Combobox(settings, textvariable=self.bt_take_profit_mode_var, values=['OFF', 'PERCENT', 'ABSOLUTE', 'ATR', 'FIELD', 'R_MULTIPLE'], width=12, state='readonly').grid(row=r, column=3, padx=6, sticky='w', pady=(6, 0))
        ttk.Label(settings, text='Target value:').grid(row=r, column=4, sticky='w', padx=(16, 0), pady=(6, 0))
        ttk.Entry(settings, textvariable=self.bt_take_profit_pct_var, width=10).grid(row=r, column=5, padx=6, sticky='w', pady=(6, 0))
        ttk.Label(settings, text='Target TF:').grid(row=r, column=6, sticky='w', padx=(16, 0), pady=(6, 0))
        ttk.Combobox(settings, textvariable=self.bt_take_profit_tf_var, values=list(ALL_TIMEFRAMES), width=8, state='normal').grid(row=r, column=7, padx=6, sticky='w', pady=(6, 0))

        r = 3
        ttk.Label(settings, text='Target field:').grid(row=r, column=0, sticky='w', pady=(6, 0))
        ttk.Combobox(settings, textvariable=self.bt_take_profit_field_var, values=self.bt_exit_field_choices, width=22, state='normal').grid(row=r, column=1, padx=6, sticky='w', pady=(6, 0))
        ttk.Label(settings, text='Target adj (%):').grid(row=r, column=2, sticky='w', padx=(16, 0), pady=(6, 0))
        ttk.Entry(settings, textvariable=self.bt_take_profit_offset_pct_var, width=10).grid(row=r, column=3, padx=6, sticky='w', pady=(6, 0))
        ttk.Checkbutton(settings, text='Allow reverse (close + open opposite on same bar)', variable=self.bt_reverse_var).grid(row=r, column=4, columnspan=2, sticky='w', pady=(6, 0))
        ttk.Checkbutton(settings, text='Skip if Long Entry + Short Entry both true', variable=self.bt_skip_both_var).grid(row=r, column=6, columnspan=2, sticky='w', pady=(6, 0))
        ttk.Label(settings, text='Top-level path is shown in the Backtest Structure panel above. Signals stay bar-based; stop loss / take profit are intrabar-active. Modes can mix percent, absolute, ATR, field-based levels, and R-multiple targets.', foreground='#666').grid(row=4, column=0, columnspan=8, sticky='w', pady=(8, 0))
        ttk.Label(settings, text='Field selector now shows common built-in names. You can pick from the dropdown or type an advanced internal field manually if needed.', foreground='#666').grid(row=5, column=0, columnspan=8, sticky='w', pady=(2, 0))
        ttk.Label(settings, text='Field examples: atr, frama, frama_upper, frama_lower, vidya_line, vidya_upper, vidya_lower, vidya_nearest_liq_low, vidya_nearest_liq_high, range_filter_line, range_filter_upper, range_filter_lower.', foreground='#666').grid(row=6, column=0, columnspan=8, sticky='w', pady=(2, 0))

        pw = ttk.Panedwindow(outer, orient='vertical')
        pw.pack(side='top', fill='both', expand=True, pady=(10, 0))
        charts = ttk.Frame(pw)
        bottom = ttk.Frame(pw)
        pw.add(charts, weight=3)
        pw.add(bottom, weight=2)
        self.bt_pw = pw
        try:
            self.after(250, lambda: self._bt_set_default_sash(show_trades=True))
        except Exception:
            pass

        chart_nb = ttk.Notebook(charts)
        chart_nb.pack(side='top', fill='both', expand=True)
        self.bt_chart_nb = chart_nb
        tab_price = ttk.Frame(chart_nb)
        tab_equity = ttk.Frame(chart_nb)
        self._bt_chart_tab_price = tab_price
        self._bt_chart_tab_equity = tab_equity
        chart_nb.add(tab_price, text='Price + Trades + MACD')
        chart_nb.add(tab_equity, text='Equity Curve')
        try:
            chart_nb.bind('<<NotebookTabChanged>>', self._on_bt_chart_tab_changed)
        except Exception:
            pass

        price_ctl = ttk.Frame(tab_price, padding=(10, 8))
        price_ctl.pack(side='top', fill='x')
        self.bt_autofocus_var = tk.BooleanVar(value=True)
        self.bt_show_vlines_var = tk.BooleanVar(value=False)
        self.bt_hist_bars_var = tk.BooleanVar(value=False)
        ttk.Checkbutton(price_ctl, text='Auto-zoom to selected trade', variable=self.bt_autofocus_var).pack(side='left')
        ttk.Checkbutton(price_ctl, text='Show entry/exit vlines', variable=self.bt_show_vlines_var, command=self._bt_draw_chart).pack(side='left', padx=10)
        ttk.Checkbutton(price_ctl, text='MACD histogram bars (slower)', variable=self.bt_hist_bars_var, command=self._bt_draw_chart).pack(side='left', padx=10)
        ttk.Button(price_ctl, text='Reset view', command=self._bt_reset_view).pack(side='left', padx=10)
        ttk.Button(price_ctl, text='Inspect selected trade', command=self._bt_inspect_selected_trade).pack(side='left', padx=10)
        ttk.Label(price_ctl, text='Zoom/pan with toolbar (Home/Pan/Zoom) or mouse wheel. Click a trade row to inspect it in a lightweight popup; double-click opens full details. In fast mode, detailed snapshots/traces are materialized only on demand.', foreground='#666', wraplength=760, justify='left').pack(side='left', padx=12)

        ctl = ttk.Frame(tab_price)
        ctl.pack(side='top', fill='x', pady=(2, 6))
        self.bt_chart_mode_var = tk.StringVar(value='CANDLES')
        ttk.Label(ctl, text='Chart:').pack(side='left')
        ttk.Combobox(ctl, textvariable=self.bt_chart_mode_var, values=['CANDLES', 'LINE'], width=10, state='readonly').pack(side='left', padx=(4, 12))
        self.bt_chart_tf_var = tk.StringVar(value='1m')
        ttk.Label(ctl, text='TF:').pack(side='left')
        ttk.Combobox(ctl, textvariable=self.bt_chart_tf_var, values=ALL_TIMEFRAMES, width=6, state='readonly').pack(side='left', padx=(4, 12))
        self.bt_chart_bars_var = tk.IntVar(value=600)
        ttk.Label(ctl, text='Bars/view:').pack(side='left')
        ttk.Spinbox(ctl, from_=100, to=5000, increment=50, textvariable=self.bt_chart_bars_var, width=7).pack(side='left', padx=(4, 12))
        self.bt_chart_pos_var = tk.DoubleVar(value=0.0)
        self.bt_chart_pos_scale = ttk.Scale(ctl, from_=0, to=0, orient='horizontal', variable=self.bt_chart_pos_var, command=lambda _v: self._bt_schedule_chart_redraw(reset_cache=False))
        self.bt_chart_pos_scale.pack(side='left', fill='x', expand=True, padx=(6, 6))
        self.bt_chart_window_label = ttk.Label(ctl, text='', foreground='#666')
        self.bt_chart_window_label.pack(side='left', padx=(8, 0))
        ttk.Button(ctl, text='Center on selected trade', command=self._bt_center_on_selected_trade).pack(side='left', padx=(12, 0))
        ttk.Button(ctl, text='Reset view', command=self._bt_reset_view_controls).pack(side='left', padx=(6, 0))
        try:
            self.bt_chart_mode_var.trace_add('write', lambda *_: self._bt_on_chart_mode_changed())
            self.bt_chart_tf_var.trace_add('write', lambda *_: self._bt_schedule_chart_redraw(reset_cache=False))
            self.bt_chart_bars_var.trace_add('write', lambda *_: self._bt_schedule_chart_redraw(reset_cache=False))
        except Exception:
            pass

        self.bt_entry_scatter = None
        self.bt_exit_scatter = None
        self.bt_entry_ids: List[int] = []
        self.bt_exit_ids: List[int] = []
        self.bt_line_to_trade_id: Dict[Any, int] = {}
        self.bt_trade_id_to_line: Dict[int, Any] = {}
        self.bt_selected_trade_id: Optional[int] = None
        self.bt_chart_full_xlim = None
        self._bt_price_chart_placeholder = ttk.Label(
            tab_price,
            text='Price chart loads on first chart draw so the backtest workspace wakes up faster.',
            foreground='#666',
            justify='left',
            padding=18,
        )
        self._bt_price_chart_placeholder.pack(side='top', anchor='w', fill='x')

        eq_ctl = ttk.Frame(tab_equity, padding=(10, 8))
        eq_ctl.pack(side='top', fill='x')
        ttk.Button(eq_ctl, text='Reset view', command=self._bt_draw_equity).pack(side='left')
        ttk.Label(eq_ctl, text='Tip: Use toolbar (Home/Pan/Zoom).', foreground='#666').pack(side='left', padx=12)
        self._bt_equity_chart_placeholder = ttk.Label(
            tab_equity,
            text='Equity chart loads on first open or redraw so the backtest workspace stays lighter.',
            foreground='#666',
            justify='left',
            padding=18,
        )
        self._bt_equity_chart_placeholder.pack(side='top', anchor='w', fill='x')

        self._bt_results_parent = ttk.Frame(bottom)
        self._bt_results_parent.pack(side='top', fill='both', expand=True)
        self._bt_results_placeholder = ttk.Label(
            self._bt_results_parent,
            text='Backtest summary and trades table load when results arrive so the workspace opens faster.',
            foreground='#666',
            justify='left',
            padding=18,
        )
        self._bt_results_placeholder.pack(side='top', anchor='w', fill='x')

        btns = ttk.Frame(bottom, padding=(0, 10, 0, 0))
        btns.pack(side='top', fill='x')
        self.bt_fast_details_var = tk.BooleanVar(value=True)
        ttk.Checkbutton(btns, text='Fast backtest mode (lazy trade details)', variable=self.bt_fast_details_var).pack(side='left')
        self.btn_export_csv = ttk.Button(btns, text='Export CSV…', command=self._bt_export_csv, state='disabled')
        self.btn_export_csv.pack(side='left', padx=(12, 0))
        self.btn_clear_bt = ttk.Button(btns, text='Clear results', command=self._bt_clear_results, state='disabled')
        self.btn_clear_bt.pack(side='left', padx=8)
        self.bt_auto_inspect_var = tk.BooleanVar(value=True)
        ttk.Checkbutton(btns, text='Inspect on click', variable=self.bt_auto_inspect_var).pack(side='left', padx=(18, 0))
        self.btn_show_trades = ttk.Button(btns, text='Show trades ↓', command=lambda: self._bt_set_default_sash(show_trades=True))
        self.btn_show_trades.pack(side='left', padx=8)
        self.btn_inspect_trade = ttk.Button(btns, text='Inspect selected trade', command=self._bt_inspect_selected_trade, state='disabled')
        self.btn_inspect_trade.pack(side='left', padx=8)
        self.btn_generate_trade_detail = ttk.Button(btns, text='Generate selected trade details', command=self._bt_generate_selected_trade_detail, state='disabled')
        self.btn_generate_trade_detail.pack(side='left', padx=8)
        self.btn_trade_audit = ttk.Button(btns, text='Trade audit…', command=self._bt_export_trade_audit, state='disabled')
        self.btn_trade_audit.pack(side='left', padx=8)
        self.btn_save_report = ttk.Button(btns, text='Save report…', command=self._bt_save_report, state='disabled')
        self.btn_save_report.pack(side='left', padx=(18, 0))
        self.btn_load_report = ttk.Button(btns, text='Load report…', command=self._bt_load_report)
        self.btn_load_report.pack(side='left', padx=8)
        self.bt_result: Optional[Any] = None
        self.bt_streams_full: Optional[Dict[str, pd.DataFrame]] = None
        self._set_backtest_settings_collapsed(False)
        self._backtest_tab_built = True

    def _set_defaults(self):
        self.symbol_var.set("BTCUSDT")
        self.price_source_var.set("LAST")
        self.macd_impl_var.set("TRADINGVIEW")
        try:
            self.adx_impl_var.set("TRADINGVIEW")
        except Exception:
            pass
        self.live_forming_var.set(True)
        now = utc_now_floor_min()
        start = now - pd.Timedelta(days=2)
        self.start_var.set(start.strftime("%Y-%m-%d %H:%M:%S"))
        self.end_var.set(now.strftime("%Y-%m-%d %H:%M:%S"))

    def _install_traces(self):
        def _cb(*_):
            if not getattr(self, "_ui_ready", False):
                return
            # Auto-restart live engine if settings changed while running
            self._restart_live_if_running("Settings changed")

        for v in [self.symbol_var, self.price_source_var, self.macd_impl_var, self.adx_impl_var, self.live_forming_var]:
            try:
                v.trace_add("write", _cb)
            except Exception:
                pass



    # ---------- Presets ----------
    def _on_bt_mode_var_changed(self, *args):
        try:
            self._refresh_backtest_structure_labels()
        except Exception:
            pass

    def _refresh_backtest_structure_labels(self):
        mode = BACKTEST_MODES[0]
        try:
            mode = self.bt_mode_var.get() if hasattr(self, "bt_mode_var") else BACKTEST_MODES[0]
        except Exception:
            mode = BACKTEST_MODES[0]
        if mode not in BACKTEST_MODES:
            mode = BACKTEST_MODES[0]
        if hasattr(self, "bt_exec_foundation_var"):
            try:
                self.bt_exec_foundation_var.set(BACKTEST_MODE_EXECUTION_FOUNDATION.get(mode, ""))
            except Exception:
                pass
        if hasattr(self, "bt_signal_behavior_var"):
            try:
                self.bt_signal_behavior_var.set(BACKTEST_MODE_SIGNAL_BEHAVIOR.get(mode, ""))
            except Exception:
                pass
        if hasattr(self, "bt_mode_summary_var"):
            try:
                self.bt_mode_summary_var.set(BACKTEST_MODE_SUMMARY.get(mode, ""))
            except Exception:
                pass

    
    # ---------- Trigger latch seeding (Option B: from prefilled history) ----------
    def _on_latch_mode_changed(self):
        # Switching latch policy can change which groups are considered FIRED/ARMED.
        self._latch_seed_dirty = True
        try:
            self._seed_all_group_latches_from_history()
        except Exception:
            pass
        try:
            self._update_rule_statuses()
        except Exception:
            pass

    def _get_live_closes_snapshot(self) -> Dict[str, List[float]]:
        eng = self.live_engine
        if not eng:
            return {}
        try:
            with eng.lock:
                return {tf: st.closes[:] for tf, st in eng.storage.items()}
        except Exception:
            return {}

    @staticmethod
    def _tf_minutes(tf: str) -> int:
        tf = str(tf).strip()
        if tf.endswith("m"):
            return int(tf[:-1])
        if tf.endswith("h"):
            return int(tf[:-1]) * 60
        return 1

    def _build_series_cache(self, tf: str, closes: List[float], macd_impl: str) -> Optional[Dict[str, Any]]:
        # Build indicator series over CLOSED candles only (no forming candle).
        try:
            if not closes or len(closes) < max(MACD_SLOW + MACD_SIGNAL + 2, MIN_FULL_CLOSES):
                # Still build partial cache; triggers may not be evaluable yet.
                pass
            np_cl = np.array(closes, dtype=float)
            macd_line, sig_line, _ = macd_series(np_cl, macd_impl)
            # PPO smoothing series (TA-Lib if available; otherwise fallback)
            d_ppo = ppo_d_series(np_cl)
            return {
                "tf": tf,
                "closes": closes,
                "macd": macd_line,
                "sig": sig_line,
                "dppo": d_ppo,
                "n": len(np_cl),
                "tf_min": self._tf_minutes(tf),
            }
        except Exception:
            return None

    @staticmethod
    def _color_from_bool(b: Optional[bool]) -> Optional[str]:
        if b is None:
            return None
        return "GREEN" if b else "RED"

    def _last_event_index_and_meta(self, rule: Rule, cache: Dict[str, Any]) -> Optional[Tuple[int, Tuple[str, str, Any]]]:
        """Return (idx, (tf, state_key, state_val)) for most recent event occurrence in cached CLOSED history."""
        tf = cache["tf"]
        macd = cache["macd"]
        sig = cache["sig"]
        dppo = cache["dppo"]
        n = cache["n"]
        if n < 3:
            return None

        # Helper masks
        macd_f = np.isfinite(macd)
        sig_f = np.isfinite(sig)
        dppo_f = np.isfinite(dppo)

        # ---- Dot events: ms/angle/sig_angle/ppo ----
        if rule.field in ("ms", "angle", "sig_angle", "ppo"):
            # Build state series for that field on CLOSED candles.
            if rule.field == "ms":
                valid = macd_f & sig_f
                state = np.full(n, np.nan)
                state[valid] = (macd[valid] > sig[valid]).astype(float)  # 1=GREEN,0=RED
            elif rule.field == "angle":
                # angle[i] compares macd[i] vs macd[i-1]
                state = np.full(n, np.nan)
                v = macd_f
                vv = v[1:] & v[:-1]
                state[1:][vv] = (macd[1:][vv] > macd[:-1][vv]).astype(float)
            elif rule.field == "sig_angle":
                # sig_angle[i] compares signal[i] vs signal[i-1]
                state = np.full(n, np.nan)
                v = sig_f
                vv = v[1:] & v[:-1]
                state[1:][vv] = (sig[1:][vv] > sig[:-1][vv]).astype(float)
            else:  # ppo
                state = np.full(n, np.nan)
                vv = dppo_f[1:] & dppo_f[:-1]
                state[1:][vv] = (dppo[1:][vv] > dppo[:-1][vv]).astype(float)

            # Need prev/current finite
            prev = state[:-1]
            cur = state[1:]
            finite = np.isfinite(prev) & np.isfinite(cur)

            if rule.op == "FLIPPED":
                idxs = np.where(finite & (prev != cur))[0] + 1
                if idxs.size == 0:
                    return None
                idx = int(idxs[-1])
                new_col = "GREEN" if float(state[idx]) >= 0.5 else "RED"
                return idx, (tf, rule.field, new_col)

            if rule.op == "TURNED":
                target = str(rule.value).upper()
                if target not in ("GREEN", "RED"):
                    return None
                target_val = 1.0 if target == "GREEN" else 0.0
                idxs = np.where(finite & (prev != target_val) & (cur == target_val))[0] + 1
                if idxs.size == 0:
                    return None
                idx = int(idxs[-1])
                return idx, (tf, rule.field, target)

            return None

        # ---- MACD×Signal cross ----
        if rule.field == "ms_cross" and rule.op == "CROSS":
            valid = macd_f & sig_f
            pm = macd[:-1]; ps = sig[:-1]
            cm = macd[1:];  cs = sig[1:]
            finite = valid[1:] & valid[:-1]
            if str(rule.value).upper() == "UP":
                idxs = np.where(finite & (pm <= ps) & (cm > cs))[0] + 1
                if idxs.size == 0:
                    return None
                idx = int(idxs[-1])
                return idx, (tf, "ms", "GREEN")
            if str(rule.value).upper() == "DOWN":
                idxs = np.where(finite & (pm >= ps) & (cm < cs))[0] + 1
                if idxs.size == 0:
                    return None
                idx = int(idxs[-1])
                return idx, (tf, "ms", "RED")
            return None

        # ---- MACD×0 cross ----
        if rule.field == "macd0_cross" and rule.op == "CROSS":
            valid = macd_f
            pm = macd[:-1]
            cm = macd[1:]
            finite = valid[1:] & valid[:-1]
            if str(rule.value).upper() == "UP":
                idxs = np.where(finite & (pm <= 0.0) & (cm > 0.0))[0] + 1
                if idxs.size == 0:
                    return None
                idx = int(idxs[-1])
                return idx, (tf, "macd0", "GREEN")
            if str(rule.value).upper() == "DOWN":
                idxs = np.where(finite & (pm >= 0.0) & (cm < 0.0))[0] + 1
                if idxs.size == 0:
                    return None
                idx = int(idxs[-1])
                return idx, (tf, "macd0", "RED")
            return None

        return None

    def _seed_all_group_latches_from_history(self):
        """Option B: seed ★trigger latch state from prefetched history (so FIRED doesn't depend on app start)."""
        if not self.live_engine:
            return
        if not getattr(self.live_engine, "prefill_done", False):
            return

        closes_by_tf = self._get_live_closes_snapshot()
        if not closes_by_tf:
            return

        now_ts = pd.Timestamp.utcnow()
        macd_impl = getattr(self.live_engine, "macd_impl", "TRADINGVIEW")

        # Cache per timeframe
        cache_by_tf: Dict[str, Dict[str, Any]] = {}
        for tf, closes in closes_by_tf.items():
            c = self._build_series_cache(tf, closes, macd_impl)
            if c is not None:
                cache_by_tf[tf] = c

        # Helper to read current state for "until opposite flips"
        def current_state(tf: str, key: str) -> Optional[str]:
            cur = (self.latest.get(tf) or {})
            if not cur:
                return None
            if key in ("ms", "angle", "sig_angle", "ppo"):
                v = cur.get(key)
                return v if v in ("GREEN", "RED") else None
            if key == "macd0":
                m = cur.get("macd")
                try:
                    mf = float(m)
                    if not np.isfinite(mf):
                        return None
                    return "GREEN" if mf > 0.0 else "RED"
                except Exception:
                    return None
            return None

        for tab_name in RULE_TABS:
            # Ensure latch lists exist & correct length
            latch_list = self.group_latch.setdefault(tab_name, [])
            meta_list = self.group_latch_meta.setdefault(tab_name, [])
            while len(latch_list) < len(self.rules[tab_name]):
                latch_list.append(None)
            while len(meta_list) < len(self.rules[tab_name]):
                meta_list.append(None)

            for gi, rules in enumerate(self.rules[tab_name]):
                triggers = [r for r in rules if (r.mode == "event" and getattr(r, "is_trigger", False))]
                if not triggers:
                    latch_list[gi] = None
                    meta_list[gi] = None
                    continue

                best_time: Optional[pd.Timestamp] = None
                best_meta: Optional[Tuple[str, str, Any]] = None

                for tr in triggers:
                    tf = tr.timeframe
                    cache = cache_by_tf.get(tf)
                    if not cache:
                        continue
                    res = self._last_event_index_and_meta(tr, cache)
                    if not res:
                        continue
                    idx, meta = res
                    n = cache["n"]
                    bars_ago = max(0, (n - 1) - idx)
                    mins_ago = bars_ago * int(cache["tf_min"])
                    t_est = now_ts - pd.Timedelta(minutes=int(mins_ago))

                    # If we are in "until opposite flips" mode, only seed if the resulting state matches current state.
                    if bool(self.latch_until_flip_var.get()):
                        tfm, keym, valm = meta
                        cur_s = current_state(tfm, keym)
                        if cur_s is not None and str(cur_s).upper() != str(valm).upper():
                            continue

                    if (best_time is None) or (t_est > best_time):
                        best_time = t_est
                        best_meta = meta

                # Keep newer existing latch if present
                existing = latch_list[gi]
                if isinstance(existing, pd.Timestamp) and isinstance(best_time, pd.Timestamp):
                    if existing > best_time:
                        best_time = existing
                        best_meta = meta_list[gi]

                latch_list[gi] = best_time
                meta_list[gi] = best_meta

        self._latch_seed_dirty = False

# ---------- Logging ----------
    def _live_log_append(self, msg: str):
        txt = self.__dict__.get('txt_live')
        if txt is None:
            try:
                self._live_log_buffer.append(str(msg))
            except Exception:
                pass
            return
        try:
            self._live_log_buffer.append(str(msg))
        except Exception:
            pass
        # Keep the Tk Text widget bounded so it doesn't slow down over time.
        txt.configure(state="normal")
        try:
            txt.insert("end", msg + "\n")
            txt.see("end")
            # Trim to last ~2500 lines
            try:
                end_line = int(str(txt.index("end-1c")).split(".")[0])
                max_lines = 2500
                if end_line > max_lines:
                    txt.delete("1.0", f"{end_line - max_lines}.0")
            except Exception:
                pass
        finally:
            txt.configure(state="disabled")

    def _append_breakout_alert_history(self, line: str) -> None:
        txt = self.__dict__.get("txt_breakout_alerts")
        if txt is None:
            return
        txt.configure(state="normal")
        try:
            txt.insert("end", line + "\n")
            txt.see("end")
            try:
                end_line = int(str(txt.index("end-1c")).split(".")[0])
                max_lines = 3000
                if end_line > max_lines:
                    txt.delete("1.0", f"{end_line - max_lines}.0")
            except Exception:
                pass
        finally:
            txt.configure(state="disabled")

    def _copy_breakout_alert_history(self) -> None:
        txt = self.__dict__.get("txt_breakout_alerts")
        if txt is None:
            return
        try:
            data = txt.get("1.0", "end-1c")
            self.clipboard_clear()
            self.clipboard_append(data)
        except Exception:
            pass

    def _clear_breakout_alert_history(self) -> None:
        self._breakout_alert_records = []
        self._breakout_alert_last_by_tf = {}
        txt = self.__dict__.get("txt_breakout_alerts")
        if txt is not None:
            txt.configure(state="normal")
            try:
                txt.delete("1.0", "end")
            finally:
                txt.configure(state="disabled")

    def _set_breakout_alert_banner(self, headline: str, detail: str = "", level: str = "idle") -> None:
        if not bool(getattr(self, '_live_breakout_alert_built', False)):
            try:
                self._ensure_live_breakout_alert_built()
            except Exception:
                pass
        frame = getattr(self, "_breakout_alert_banner_frame", None)
        head = getattr(self, "_breakout_alert_banner_head", None)
        det = getattr(self, "_breakout_alert_banner_detail", None)
        if frame is None or head is None or det is None:
            return
        palette = {
            "idle": (C_BG_NEU, C_TEXT, C_DIM, C_BORDER_IDLE),
            "long": ("#0f2d22", "#8ff0be", "#68c091", "#1ea95a"),
            "short": ("#381a22", "#ff9aa7", "#d37784", "#d63b49"),
            "alert": ("#2b2410", "#ffd36a", "#caaa4a", C_AMBER),
        }
        bg, fg, sub_fg, border = palette.get(level, palette["alert"])
        try:
            frame.config(bg=bg, highlightbackground=border, highlightcolor=border)
            head.config(text=headline, bg=bg, fg=fg)
            det.config(text=(detail or "-"), bg=bg, fg=sub_fg)
            try:
                self.breakout_alert_summary_var.set(headline)
            except Exception:
                pass
            self._breakout_alert_banner_latched = level != "idle"
        except Exception:
            pass

    def _pulse_breakout_alert_banner(self, level: str = "alert", pulses: int = 12) -> None:
        frame = getattr(self, "_breakout_alert_banner_frame", None)
        head = getattr(self, "_breakout_alert_banner_head", None)
        det = getattr(self, "_breakout_alert_banner_detail", None)
        if frame is None or head is None or det is None:
            return
        try:
            if self._breakout_alert_flash_job is not None:
                self.after_cancel(self._breakout_alert_flash_job)
        except Exception:
            pass
        self._breakout_alert_flash_job = None
        self._breakout_alert_flash_state = False

        base_map = {
            "long": ("#0f2d22", "#8ff0be", "#68c091", "#1ea95a"),
            "short": ("#381a22", "#ff9aa7", "#d37784", "#d63b49"),
            "alert": ("#2b2410", "#ffd36a", "#caaa4a", C_AMBER),
        }
        flash_map = {
            "long": ("#164432", "#b2ffd0", "#8fdcb3", "#0a8f43"),
            "short": ("#4a1f29", "#ffc0c8", "#f097a4", "#bb1220"),
            "alert": ("#433613", "#ffe082", "#d9bf72", "#d48a00"),
        }
        base = base_map.get(level, base_map["alert"])
        flash = flash_map.get(level, flash_map["alert"])

        def _step(remaining: int):
            if frame is None or head is None or det is None:
                return
            pal = flash if self._breakout_alert_flash_state else base
            bg, fg, sub_fg, border = pal
            try:
                frame.config(bg=bg, highlightbackground=border, highlightcolor=border)
                head.config(bg=bg, fg=fg)
                det.config(bg=bg, fg=sub_fg)
            except Exception:
                return
            self._breakout_alert_flash_state = not self._breakout_alert_flash_state
            if remaining > 0:
                try:
                    self._breakout_alert_flash_job = self.after(280, lambda: _step(remaining - 1))
                except Exception:
                    self._breakout_alert_flash_job = None
            else:
                self._breakout_alert_flash_job = None
                self._breakout_alert_flash_state = False
                bg, fg, sub_fg, border = base
                try:
                    frame.config(bg=bg, highlightbackground=border, highlightcolor=border)
                    head.config(bg=bg, fg=fg)
                    det.config(bg=bg, fg=sub_fg)
                except Exception:
                    pass

        _step(max(0, int(pulses)))

    def _clear_breakout_alert_banner(self) -> None:
        self._set_breakout_alert_banner(
            "No breakout alert latched.",
            "When a live compression-release breakout turns trigger-ready, the app latches it here, beeps, flashes, and records the exact UTC time.",
            level="idle",
        )

    def _register_breakout_alert(self, tf: str, snap: Dict[str, Any]) -> None:
        try:
            now_utc = pd.Timestamp.now(tz="UTC")
        except Exception:
            now_utc = None
        snap_time = snap.get("time")
        try:
            snap_ts = pd.Timestamp(snap_time) if snap_time is not None else None
            if snap_ts is not None and snap_ts.tzinfo is None:
                snap_ts = snap_ts.tz_localize("UTC")
            elif snap_ts is not None:
                snap_ts = snap_ts.tz_convert("UTC")
        except Exception:
            snap_ts = None

        def _fmt_ts(ts):
            try:
                ts2 = pd.Timestamp(ts)
                if ts2.tzinfo is None:
                    ts2 = ts2.tz_localize("UTC")
                else:
                    ts2 = ts2.tz_convert("UTC")
                return ts2.strftime("%Y-%m-%d %H:%M:%S UTC")
            except Exception:
                return "-"

        breakout_bias = str(snap.get("market_breakout_bias") or snap.get("market_bias") or "-").upper().strip() or "-"
        if breakout_bias == "LONG":
            level = "long"
            dir_txt = "LONG"
        elif breakout_bias == "SHORT":
            level = "short"
            dir_txt = "SHORT"
        else:
            level = "alert"
            dir_txt = "NEUTRAL"
        breakout_pretty = str(snap.get("market_breakout_pretty") or snap.get("market_breakout_state") or "Breakout").strip() or "Breakout"
        state_pretty = pretty_market_state_label(snap.get("market_state")) if snap.get("market_state") else "-"
        try:
            price_val = snap.get("live_price")
            if price_val is None:
                price_val = snap.get("price")
            if price_val is None:
                price_val = self.latest.get(tf, {}).get("live_price")
            price_txt = f"{float(price_val):.2f}"
        except Exception:
            price_txt = "-"
        warn = str(snap.get("market_warning_note") or snap.get("market_breakout_note") or "").strip()
        of_risk = str(snap.get("market_of_risk_flag") or "-").strip() or "-"
        vwap_bias = str(snap.get("market_vwap_bias") or "-").strip() or "-"
        event_ts_txt = _fmt_ts(now_utc) if now_utc is not None else "-"
        bar_ts_txt = _fmt_ts(snap_ts) if snap_ts is not None else "-"
        headline = f"⚡ BREAKOUT {dir_txt} • {tf} • {event_ts_txt}"
        detail = f"{breakout_pretty} | State: {state_pretty} | Price: {price_txt} | VWAP: {vwap_bias} | OF Risk: {of_risk} | Bar: {bar_ts_txt}"
        if warn:
            detail += f" | Note: {warn}"
        self._set_breakout_alert_banner(headline, detail, level=level)
        self._pulse_breakout_alert_banner(level=level)

        rec = {
            "event_time": event_ts_txt,
            "bar_time": bar_ts_txt,
            "tf": tf,
            "direction": dir_txt,
            "breakout": breakout_pretty,
            "state": state_pretty,
            "price": price_txt,
            "of_risk": of_risk,
            "vwap_bias": vwap_bias,
            "note": warn,
        }
        self._breakout_alert_records.append(rec)
        self._breakout_alert_last_by_tf[tf] = rec
        line = self._format_breakout_alert_record_line(rec)
        self._append_breakout_alert_history(line)
        self._live_log_append("[Breakout Alert] " + line)

    def set_progress(self, msg: str, pct: int):
        self.q.put(("progress", msg, pct))

    # ---------- Rule Builder core ----------
    def _on_rules_tab_change(self, event=None):
        idx = self.rules_nb.index(self.rules_nb.select())
        self.active_tab = RULE_TABS[idx]
        self._ensure_rule_tab_built(self.active_tab)
        self._ensure_at_least_one_group(self.active_tab)
        gi = min(self.active_group_index[self.active_tab], len(self.rules[self.active_tab]) - 1)
        self._set_active_group(self.active_tab, gi)

    def _on_tab_group_join_change(self, tab_name: str):
        try:
            self.tab_group_join_mode[tab_name] = self.tab_group_join_var[tab_name].get()
        except Exception:
            return
        self._rebuild_groups_ui(tab_name)
        self._update_rule_statuses()

    def _ensure_at_least_one_group(self, tab_name: str):
        if not self.rules[tab_name]:
            self.rules[tab_name] = [[]]
            self.group_rule_join_mode[tab_name] = ["OR"]  # default per your request: inside group OR
            self.group_latch[tab_name] = [None]
            self.group_latch_meta[tab_name] = [None]
            self.group_latch_meta[tab_name] = [None]
            self._rebuild_groups_ui(tab_name)
            self.active_group_index[tab_name] = 0

    def _clear_tab(self, tab_name: str):
        self.rules[tab_name] = [[]]
        self.group_rule_join_mode[tab_name] = ["OR"]
        self.group_latch[tab_name] = [None]
        self.group_latch_meta[tab_name] = [None]
        self.active_group_index[tab_name] = 0
        self._rebuild_groups_ui(tab_name)
        self._set_active_group(tab_name, 0)
        self._update_rule_statuses()

    def _add_group(self, tab_name: str):
        self.rules[tab_name].append([])
        self.group_rule_join_mode[tab_name].append("OR")
        self.group_latch[tab_name].append(None)
        self.group_latch_meta.setdefault(tab_name, []).append(None)
        self._rebuild_groups_ui(tab_name)
        self._set_active_group(tab_name, len(self.rules[tab_name]) - 1)
        self._update_rule_statuses()

    def _remove_group(self, tab_name: str, idx: int):
        if idx < 0 or idx >= len(self.rules[tab_name]):
            return
        del self.rules[tab_name][idx]
        del self.group_rule_join_mode[tab_name][idx]
        try:
            del self.group_latch[tab_name][idx]
            try:
                del self.group_latch_meta[tab_name][idx]
            except Exception:
                pass
        except Exception:
            pass
        if not self.rules[tab_name]:
            self.rules[tab_name] = [[]]
            self.group_rule_join_mode[tab_name] = ["OR"]
            self.group_latch[tab_name] = [None]
        self.active_group_index[tab_name] = min(self.active_group_index[tab_name], len(self.rules[tab_name]) - 1)
        self._rebuild_groups_ui(tab_name)
        self._set_active_group(tab_name, self.active_group_index[tab_name])
        self._update_rule_statuses()

    def _set_active_group(self, tab_name: str, idx: int):
        self.active_group_index[tab_name] = idx
        for i, gui in enumerate(self.group_ui.get(tab_name, [])):
            gui.set_active(i == idx)
        groups_join = self.tab_group_join_mode[tab_name]
        rules_join = self.group_rule_join_mode[tab_name][idx] if idx < len(self.group_rule_join_mode[tab_name]) else "OR"
        extra = ""
        if tab_name in ("Long Entry", "Short Entry"):
            try:
                extra = f" Entry filter: {self._entry_filter_summary(tab_name)}."
            except Exception:
                extra = ""
        self.builder_status.set(
            f"Active: {tab_name} → Group {idx+1}. "
            f"Rules in group: {rules_join}. Groups in tab: {groups_join}. "
            f"Right-click live cells to add State/Event rules.{extra}"
        )

    def _rebuild_groups_ui(self, tab_name: str):
        if not bool(getattr(self, '_rule_tabs_built', {}).get(tab_name, False)):
            return
        frm = self.rules_tab_frames[tab_name]
        inner: tk.Frame = frm._groups_inner  # type: ignore[attr-defined]

        try:
            self._rule_eval_ctx.reset_all_sequences()
        except Exception:
            pass

        for child in inner.winfo_children():
            child.destroy()

        self.group_ui[tab_name] = []
        groups = self.rules[tab_name]
        connector = self.tab_group_join_mode[tab_name]  # AND / OR between groups

        for i in range(len(groups)):
            if i > 0:
                conn_lbl = tk.Label(inner, text=connector, fg=C_DIM, bg=C_BG_PANEL, font=("Segoe UI", 9, "bold"))
                conn_lbl.pack(side="top", pady=(6, 6))

            join_mode = self.group_rule_join_mode[tab_name][i] if i < len(self.group_rule_join_mode[tab_name]) else "OR"

            def _make_join_cb(i=i, t=tab_name):
                def _cb(new_mode: str):
                    self.group_rule_join_mode[t][i] = new_mode
                    try:
                        self._rule_eval_ctx.reset_all_sequences()
                    except Exception:
                        pass
                    self._schedule_live_rule_update()
                return _cb

            g = GroupUI(
                inner,
                tab_name=tab_name,
                idx=i,
                join_mode=join_mode,
                on_join_change=_make_join_cb(),
                on_set_active=lambda e=None, i=i, t=tab_name: self._set_active_group(t, i),
                on_remove_group=lambda i=i, t=tab_name: self._remove_group(t, i),
                entry_filter_text=(self._entry_filter_for_tab(tab_name).badge_text() if tab_name in ("Long Entry", "Short Entry") else None),
                on_edit_entry_filter=(lambda t=tab_name: self._edit_entry_filter(t)) if tab_name in ("Long Entry", "Short Entry") else None,
            )
            g.pack(side="top", fill="x", pady=6, padx=6)
            try:
                self._bind_mousewheel_handlers(g, y_handler=lambda units, c=getattr(frm, '_groups_canvas', None): self._scroll_rule_groups_canvas(c, units))
            except Exception:
                pass
            self.group_ui[tab_name].append(g)

        # rebuild chips
        for gi, rules in enumerate(groups):
            for ri, rule in enumerate(rules):
                self._add_chip_widget(tab_name, gi, ri)

        # keep selection valid
        if self.rules[tab_name]:
            gi = min(self.active_group_index[tab_name], len(self.rules[tab_name]) - 1)
            self._set_active_group(tab_name, gi)
        try:
            self._refresh_entry_filter_badges(tab_name)
        except Exception:
            pass
        try:
            canvas = getattr(frm, '_groups_canvas', None)
            if canvas is not None:
                self.update_idletasks()
                canvas.configure(scrollregion=canvas.bbox('all'))
        except Exception:
            pass

    def _add_rule_active(self, rule: Rule):
        tab = self.active_tab
        self._ensure_at_least_one_group(tab)
        gi = self.active_group_index[tab]
        self.rules[tab][gi].append(rule)
        self._rebuild_groups_ui(tab)
        self._set_active_group(tab, gi)
        self._latch_seed_dirty = True
        try:
            self._seed_all_group_latches_from_history()
        except Exception:
            pass
        self._update_rule_statuses()

    def _add_chip_widget(self, tab_name: str, group_idx: int, rule_idx: int):
        gui = self.group_ui[tab_name][group_idx]
        rule = self.rules[tab_name][group_idx][rule_idx]

        def remove():
            try:
                del self.rules[tab_name][group_idx][rule_idx]
            except Exception:
                return
            self._rebuild_groups_ui(tab_name)
            self._update_rule_statuses()

        def edit():
            # State binary
            if rule.mode == "state" and rule.field in ("ms", "angle", "sig_angle", "ppo"):
                dlg = DotRuleDialog(self, f"Edit {rule.timeframe} {rule.field}", rule.op, str(rule.value))
                self.wait_window(dlg)
                if dlg.result:
                    op, val = dlg.result
                    rule.op = op
                    rule.value = val
                    self._rebuild_groups_ui(tab_name)
                    self._schedule_live_rule_update()
                return

            # State numeric
            if rule.mode == "state" and rule.field in ("frama_state", "vidya_state", "range_filter_state", "range_filter_phase", "vidya_angle_state", "vidya_angle_accel", "market_state", "market_bias", "market_phase"):
                if rule.field == "vidya_state":
                    choices = ["BUY", "SELL"]
                elif rule.field == "range_filter_phase":
                    choices = ["BUY_STRONG", "BUY_WEAK", "SELL_STRONG", "SELL_WEAK", "NEUTRAL"]
                elif rule.field == "vidya_angle_state":
                    choices = ["UP", "DOWN", "FLAT"]
                elif rule.field == "vidya_angle_accel":
                    choices = ["ACCEL", "DECEL", "STABLE"]
                elif rule.field == "market_state":
                    choices = ["BULL_EXPANSION", "BULL_PULLBACK", "BULL_EXHAUSTION", "BEAR_EXPANSION", "BEAR_PULLBACK", "BEAR_EXHAUSTION", "TRANSITION", "COMPRESSION"]
                elif rule.field == "market_bias":
                    choices = ["LONG", "SHORT", "NEUTRAL"]
                elif rule.field == "market_phase":
                    choices = ["EXPANSION", "PULLBACK", "EXHAUSTION", "TRANSITION", "COMPRESSION"]
                else:
                    choices = ["BUY", "SELL", "NEUTRAL"]
                default_val = str(rule.value) if str(rule.value) in choices else choices[0]
                dlg = EventRuleDialog(self, f"Edit {rule.timeframe} {rule.field}", choices, default_val)
                self.wait_window(dlg)
                if dlg.result:
                    rule.op = rule.op if rule.op in ("=", "!=") else "="
                    rule.value = dlg.result
                    self._rebuild_groups_ui(tab_name)
                    self._schedule_live_rule_update()
                return

            if rule.mode == "state" and rule.field in ("flip_age", "macd", "signal", "adx", "atr", "di_plus", "di_minus", "di_spread", "vidya_line", "vidya_raw", "vidya_upper", "vidya_lower", "vidya_up_volume", "vidya_down_volume", "vidya_delta_pct", "vidya_slope", "vidya_angle_deg", "vidya_angle_deg_norm", "vidya_active_liq_low_count", "vidya_active_liq_high_count", "vidya_nearest_liq_low", "vidya_nearest_liq_high", "vidya_dist_to_liq_low_pct", "vidya_dist_to_liq_high_pct", "range_filter_line", "range_filter_upper", "range_filter_lower", "range_filter_smooth_range", "range_filter_upward_count", "range_filter_downward_count", "market_confidence", "market_state_age"):
                default_val = str(rule.value)
                dlg = NumericRuleDialog(self, f"Edit {rule.timeframe} {rule.field}", rule.op, default_val)
                self.wait_window(dlg)
                if dlg.result:
                    op, val_str = dlg.result
                    rule.op = op
                    try:
                        if rule.field == "flip_age":
                            rule.value = int(float(val_str))
                        else:
                            rule.value = float(val_str)
                    except Exception:
                        messagebox.showerror("Error", "Invalid number.")
                        return
                    self._rebuild_groups_ui(tab_name)
                    self._schedule_live_rule_update()
                return

            # Event binary dots
            if rule.mode == "event" and rule.field in ("ms", "angle", "sig_angle", "ppo", "adx_angle", "atr_angle"):
                choices = ["TURNED GREEN", "TURNED RED", "FLIPPED"]
                default = "FLIPPED" if rule.op == "FLIPPED" else f"TURNED {rule.value}"
                dlg = EventRuleDialog(self, f"Edit event for {rule.timeframe} {rule.field}", choices, default)
                self.wait_window(dlg)
                if dlg.result:
                    s = dlg.result
                    if s == "FLIPPED":
                        rule.op = "FLIPPED"
                        rule.value = None
                    else:
                        _, v = s.split()
                        rule.op = "TURNED"
                        rule.value = v
                    self._rebuild_groups_ui(tab_name)
                    self._schedule_live_rule_update()
                return

            if rule.mode == "event" and rule.field in ("frama_break_up", "frama_break_down", "frama_mid_cross", "vidya_trend_up", "vidya_trend_down", "vidya_new_liq_low", "vidya_new_liq_high", "vidya_liq_low_taken", "vidya_liq_high_taken", "range_filter_buy", "range_filter_sell"):
                return

            # Event cross
            if rule.mode == "event" and rule.field == "ms_cross":
                choices = ["CROSS UP", "CROSS DOWN"]
                default = f"CROSS {rule.value}"
                dlg = EventRuleDialog(self, f"Edit MACD cross for {rule.timeframe}", choices, default)
                self.wait_window(dlg)
                if dlg.result:
                    _, v = dlg.result.split()
                    rule.op = "CROSS"
                    rule.value = v
                    self._rebuild_groups_ui(tab_name)
                    self._schedule_live_rule_update()
                return

        chip = Chip(gui.inner, rule.label(), on_remove=remove, on_edit=edit)

        chip.rule = rule
        # Context menu: toggle ★ Trigger (EVENT rules only)
        def _toggle_trigger():
            if rule.mode != "event":
                return
            rule.is_trigger = not bool(getattr(rule, "is_trigger", False))
            self._rebuild_groups_ui(tab_name)
            self._latch_seed_dirty = True
            try:
                self._seed_all_group_latches_from_history()
            except Exception:
                pass
            self._update_rule_statuses()

        def _toggle_final_gate():
            try:
                join_mode = self.group_rule_join_mode[tab_name][group_idx] if group_idx < len(self.group_rule_join_mode.get(tab_name, [])) else "OR"
            except Exception:
                join_mode = "OR"
            if str(join_mode or "OR").upper().strip() != "SEQUENCE":
                return
            rule.is_final_gate = not bool(getattr(rule, "is_final_gate", False))
            self._rebuild_groups_ui(tab_name)
            self._update_rule_statuses()


        def _edit_bars_ago():
            # Available for both STATE and EVENT rules.
            if rule is None:
                return
            try:
                default_mode = getattr(rule, "bars_ago_mode", "OFF")
                default_n = getattr(rule, "bars_ago_n", 0)
            except Exception:
                default_mode, default_n = "OFF", 0
            dlg = BarsAgoDialog(self, f"Bars Ago — {rule.timeframe} {rule.field}", default_mode=str(default_mode), default_n=int(default_n or 0), default_require_now=bool(getattr(rule, "require_still_valid", False)))
            self.wait_window(dlg)
            if dlg.result:
                m, n, need_now = dlg.result
                try:
                    rule.bars_ago_mode = str(m).upper()
                    rule.bars_ago_n = int(n)
                    rule.require_still_valid = bool(need_now)
                except Exception:
                    rule.bars_ago_mode = "OFF"
                    rule.bars_ago_n = 0
                    rule.require_still_valid = False
                self._rebuild_groups_ui(tab_name)
                self._update_rule_statuses()

        def _show_ctx_menu(e):
            menu = tk.Menu(self, tearoff=0)
            try:
                join_mode = self.group_rule_join_mode[tab_name][group_idx] if group_idx < len(self.group_rule_join_mode.get(tab_name, [])) else "OR"
            except Exception:
                join_mode = "OR"
            is_sequence_group = (str(join_mode or "OR").upper().strip() == "SEQUENCE")
            if rule.mode == "event":
                menu.add_command(
                    label=("Unset ★ Trigger" if getattr(rule, "is_trigger", False) else "Set ★ Trigger"),
                    command=_toggle_trigger
                )
            menu.add_command(label="Bars Ago…", command=_edit_bars_ago)
            if is_sequence_group:
                menu.add_separator()
                menu.add_command(
                    label=("Unset Final-Bar Gate" if getattr(rule, "is_final_gate", False) else "Set Final-Bar Gate"),
                    command=_toggle_final_gate,
                )
                menu.add_command(label="(Final-Bar Gate: checked only when the SEQUENCE completes)", state="disabled")
            else:
                menu.add_separator()
                if rule.mode == "event":
                    menu.add_command(label="(Trigger means: entry/exit only happens when this EVENT fires)", state="disabled")
                else:
                    menu.add_command(label="(Bars Ago works for STATE rules too — it checks the condition N steps back)", state="disabled")
            try:
                menu.tk_popup(e.x_root, e.y_root)
            finally:
                menu.grab_release()

        # Windows / Linux right-click
        chip.lbl.bind("<Button-3>", _show_ctx_menu)
        chip.bind("<Button-3>", _show_ctx_menu)
        # macOS two-finger click often reports Button-2
        chip.lbl.bind("<Button-2>", _show_ctx_menu)
        chip.bind("<Button-2>", _show_ctx_menu)

        gui.add_chip(chip)

    def _show_vidya_details(self, tf: str):
        tf = str(tf)
        latest = (self.latest.get(tf, {}) if hasattr(self, "latest") else {}) or {}
        if not hasattr(self, "_vidya_detail_windows"):
            self._vidya_detail_windows = {}
        win = self._vidya_detail_windows.get(tf)
        if win is None or (hasattr(win, "winfo_exists") and not win.winfo_exists()):
            win = tk.Toplevel(self)
            win.title(f"VIDYA Details — {tf}")
            win.transient(self)
            win.resizable(False, False)
            frm = ttk.Frame(win, padding=10)
            frm.pack(fill="both", expand=True)
            lbl = tk.Label(frm, justify="left", anchor="w", font=("Consolas", 10), fg=C_TEXT)
            lbl.pack(fill="both", expand=True)
            btns = ttk.Frame(frm)
            btns.pack(fill="x", pady=(8, 0))
            ttk.Button(btns, text="Refresh", command=lambda tf=tf: self._show_vidya_details(tf)).pack(side="left")
            ttk.Button(btns, text="Close", command=win.destroy).pack(side="right")
            win._content_label = lbl
            self._vidya_detail_windows[tf] = win
        try:
            state = latest.get("vidya_state") or "-"
            def _fmt_num(v, fmt=".3f"):
                try:
                    fv = float(v)
                    if np.isfinite(fv):
                        return format(fv, fmt)
                except Exception:
                    pass
                return "-"
            def _fmt_int(v):
                try:
                    fv = float(v)
                    if np.isfinite(fv):
                        return str(int(round(fv)))
                except Exception:
                    pass
                return "-"
            lines = [
                f"Timeframe: {tf}",
                f"State: {state}",
                f"Trend Up Event: {'YES' if latest.get('vidya_trend_up') else 'no'}",
                f"Trend Down Event: {'YES' if latest.get('vidya_trend_down') else 'no'}",
                "",
                f"Buy Volume: {_fmt_num(latest.get('vidya_up_volume'))}",
                f"Sell Volume: {_fmt_num(latest.get('vidya_down_volume'))}",
                f"Delta %: {_fmt_num(latest.get('vidya_delta_pct'))}",
                "",
                f"Active LIQ Lows: {_fmt_int(latest.get('vidya_active_liq_low_count'))}",
                f"Active LIQ Highs: {_fmt_int(latest.get('vidya_active_liq_high_count'))}",
                f"Nearest LIQ Low: {_fmt_num(latest.get('vidya_nearest_liq_low'))}",
                f"Nearest LIQ High: {_fmt_num(latest.get('vidya_nearest_liq_high'))}",
                f"Dist to LIQ Low %: {_fmt_num(latest.get('vidya_dist_to_liq_low_pct'))}",
                f"Dist to LIQ High %: {_fmt_num(latest.get('vidya_dist_to_liq_high_pct'))}",
                "",
                f"New LIQ Low: {'YES' if latest.get('vidya_new_liq_low') else 'no'}",
                f"New LIQ High: {'YES' if latest.get('vidya_new_liq_high') else 'no'}",
                f"LIQ Low Taken: {'YES' if latest.get('vidya_liq_low_taken') else 'no'}",
                f"LIQ High Taken: {'YES' if latest.get('vidya_liq_high_taken') else 'no'}",
                f"Last Taken Low Price: {_fmt_num(latest.get('vidya_last_liq_low_taken_price'))}",
                f"Last Taken High Price: {_fmt_num(latest.get('vidya_last_liq_high_taken_price'))}",
            ]
            try:
                win._content_label.config(text="\n".join(lines))
            except Exception:
                pass
            try:
                win.deiconify()
                win.lift()
                win.focus_force()
            except Exception:
                pass
        except Exception:
            pass

    def _update_range_event_memory(self, tf: str, payload: Optional[Dict[str, Any]], *, bar_ms_override: Optional[int] = None) -> None:
        """Remember Range BUY/SELL trigger bars for the age columns only.

        This memory is UI-local and must never let an older seeded bar rewind a newer live bar.
        We therefore keep the previous CondIni-style inference, but reject out-of-order bars and
        also trust the explicit engine event pulse when it is present for the current bar.
        """
        if payload is None:
            return
        tfk = str(tf)
        try:
            bucket = self._range_event_last_bar_ms.setdefault(tfk, {"buy": None, "sell": None})
            meta = self._range_event_meta.setdefault(tfk, {"last_seen_bar_ms": None, "regime": 0, "current_bar_buy_seen": False, "current_bar_sell_seen": False})
        except Exception:
            return
        try:
            bar_ms = int(bar_ms_override) if bar_ms_override is not None else None
        except Exception:
            bar_ms = None
        if bar_ms is None:
            for k in ("bar_open_time_ms", "bar_ms", "snapshot_bar_ms"):
                try:
                    raw = payload.get(k)
                    if raw is not None:
                        bar_ms = int(raw)
                        break
                except Exception:
                    continue
        if bar_ms is None:
            return

        try:
            state = str(payload.get("range_filter_state") or "").upper().strip()
        except Exception:
            state = ""
        try:
            cond_ini_raw = payload.get("range_filter_cond_ini")
            cond_ini = int(float(cond_ini_raw)) if cond_ini_raw is not None else None
        except Exception:
            cond_ini = None
        try:
            raw_buy_now = bool(payload.get("range_filter_buy", False))
        except Exception:
            raw_buy_now = False
        try:
            raw_sell_now = bool(payload.get("range_filter_sell", False))
        except Exception:
            raw_sell_now = False

        try:
            last_seen = meta.get("last_seen_bar_ms")
            last_seen_i = int(last_seen) if last_seen is not None else None
        except Exception:
            last_seen_i = None

        # Older bars can arrive late during startup because CLOSED-bar seed history is drained
        # asynchronously after live_status has already started flowing. In that case we must not
        # rewind the current-bar regime bookkeeping, but we *do* still want to backfill the most
        # recent historical BUY/SELL event timestamps so the age cells can populate immediately.
        if last_seen_i is not None and int(bar_ms) < last_seen_i:
            inferred_buy_now = bool(raw_buy_now) or ((state == "BUY") and (cond_ini == 1))
            inferred_sell_now = bool(raw_sell_now) or ((state == "SELL") and (cond_ini == -1))
            if inferred_buy_now:
                try:
                    prev_buy = bucket.get("buy")
                    prev_buy_i = int(prev_buy) if prev_buy is not None else None
                except Exception:
                    prev_buy_i = None
                if prev_buy_i is None or int(bar_ms) > prev_buy_i:
                    bucket["buy"] = int(bar_ms)
            if inferred_sell_now:
                try:
                    prev_sell = bucket.get("sell")
                    prev_sell_i = int(prev_sell) if prev_sell is not None else None
                except Exception:
                    prev_sell_i = None
                if prev_sell_i is None or int(bar_ms) > prev_sell_i:
                    bucket["sell"] = int(bar_ms)
            return

        try:
            if last_seen_i is None or last_seen_i != int(bar_ms):
                meta["last_seen_bar_ms"] = int(bar_ms)
                meta["current_bar_buy_seen"] = False
                meta["current_bar_sell_seen"] = False
        except Exception:
            meta["last_seen_bar_ms"] = int(bar_ms)
            meta["current_bar_buy_seen"] = False
            meta["current_bar_sell_seen"] = False

        try:
            regime = int(meta.get("regime", 0) or 0)
        except Exception:
            regime = 0

        # Canonical one-bar pulses from the engine always win for the current bar.
        if raw_buy_now and not bool(meta.get("current_bar_buy_seen")):
            bucket["buy"] = int(bar_ms)
            meta["current_bar_buy_seen"] = True
        if raw_sell_now and not bool(meta.get("current_bar_sell_seen")):
            bucket["sell"] = int(bar_ms)
            meta["current_bar_sell_seen"] = True

        # Keep the previous UI inference as a fallback for intra-bar refreshes where the raw pulse
        # may no longer be present but the bar-local regime transition still needs to show age 0.
        buy_now = (state == "BUY") and (regime == -1) and (cond_ini in (None, 1))
        sell_now = (state == "SELL") and (regime == 1) and (cond_ini in (None, -1))

        if buy_now and not bool(meta.get("current_bar_buy_seen")):
            bucket["buy"] = int(bar_ms)
            meta["current_bar_buy_seen"] = True
        if sell_now and not bool(meta.get("current_bar_sell_seen")):
            bucket["sell"] = int(bar_ms)
            meta["current_bar_sell_seen"] = True

        if cond_ini in (-1, 1):
            meta["regime"] = int(cond_ini)
        elif state == "BUY":
            meta["regime"] = 1
        elif state == "SELL":
            meta["regime"] = -1

    def _range_event_age(self, tf: str, current_bar_ms: Optional[int], side: str) -> Optional[int]:
        return range_event_age(getattr(self, "_range_event_last_bar_ms", {}), tf, current_bar_ms, side)

    def _format_range_event_age_cell(self, tf: str, current_bar_ms: Optional[int], side: str, fired_now: bool) -> Tuple[str, str]:
        return format_range_event_age_cell(getattr(self, "_range_event_last_bar_ms", {}), tf, current_bar_ms, side, fired_now)

    def _show_range_details(self, tf: str):
        tf = str(tf)
        latest = (self.latest.get(tf, {}) if hasattr(self, "latest") else {}) or {}
        if not hasattr(self, "_range_detail_windows"):
            self._range_detail_windows = {}
        win = self._range_detail_windows.get(tf)
        if win is None or (hasattr(win, "winfo_exists") and not win.winfo_exists()):
            win = tk.Toplevel(self)
            win.title(f"Range Details — {tf}")
            win.transient(self)
            win.resizable(False, False)
            frm = ttk.Frame(win, padding=10)
            frm.pack(fill="both", expand=True)
            lbl = tk.Label(frm, justify="left", anchor="w", font=("Consolas", 10), fg=C_TEXT)
            lbl.pack(fill="both", expand=True)
            btns = ttk.Frame(frm)
            btns.pack(fill="x", pady=(8, 0))
            ttk.Button(btns, text="Refresh", command=lambda tf=tf: self._show_range_details(tf)).pack(side="left")
            ttk.Button(btns, text="Close", command=win.destroy).pack(side="right")
            win._content_label = lbl
            self._range_detail_windows[tf] = win
        try:
            state = latest.get("range_filter_state") or "-"
            phase = latest.get("range_filter_phase") or "-"
            def _fmt_num(v, fmt=".6f"):
                try:
                    fv = float(v)
                    if np.isfinite(fv):
                        return format(fv, fmt)
                except Exception:
                    pass
                return "-"
            def _fmt_count(v):
                try:
                    fv = float(v)
                    if np.isfinite(fv):
                        return str(int(round(fv)))
                except Exception:
                    pass
                return "-"
            try:
                cur_bar_ms = latest.get("bar_ms") if isinstance(latest, dict) else None
            except Exception:
                cur_bar_ms = None
            buy_age = self._range_event_age(tf, cur_bar_ms, "buy")
            sell_age = self._range_event_age(tf, cur_bar_ms, "sell")
            badge = "BUY" if buy_age == 0 else ("SELL" if sell_age == 0 else "-")
            lines = [
                f"Timeframe: {tf}",
                f"State: {state}",
                f"Phase: {phase}",
                f"Signal Badge: {badge}",
                f"Buy Trigger Age: {buy_age if buy_age is not None else '-'} bars",
                f"Sell Trigger Age: {sell_age if sell_age is not None else '-'} bars",
                f"LongCond: {'YES' if latest.get('range_filter_long_cond') else 'no'}",
                f"ShortCond: {'YES' if latest.get('range_filter_short_cond') else 'no'}",
                f"CondIni: {_fmt_count(latest.get('range_filter_cond_ini'))}",
                f"Raw Buy Flag: {'YES' if latest.get('range_filter_buy') else 'no'}",
                f"Raw Sell Flag: {'YES' if latest.get('range_filter_sell') else 'no'}",
                "",
                f"Upward Count: {_fmt_count(latest.get('range_filter_upward_count'))}",
                f"Downward Count: {_fmt_count(latest.get('range_filter_downward_count'))}",
                "",
                f"Range Filter Line: {_fmt_num(latest.get('range_filter_line'))}",
                f"Upper Band: {_fmt_num(latest.get('range_filter_upper'))}",
                f"Lower Band: {_fmt_num(latest.get('range_filter_lower'))}",
                f"Smooth Range: {_fmt_num(latest.get('range_filter_smooth_range'))}",
            ]
            try:
                win._content_label.config(text="\n".join(lines))
            except Exception:
                pass
            try:
                win.deiconify()
                win.lift()
                win.focus_force()
            except Exception:
                pass
        except Exception:
            pass

    def _market_state_palette(self, label: Any) -> Tuple[str, str, str]:
        return market_state_palette(label)

    def _market_state_playbook(self, label: Any) -> str:
        return market_state_playbook(label)

    def _market_state_thesis(self, label: Any) -> str:
        return market_state_thesis(label)

    def _reset_market_state_cards(self) -> None:
        try:
            cards = getattr(self, "market_state_cards", {}) or {}
        except Exception:
            cards = {}
        for tf, widgets in cards.items():
            try:
                fg, bg, border = self._market_state_palette(None)
                frame = widgets.get("frame")
                if frame is not None:
                    frame.config(bg=bg, highlightbackground=border, highlightcolor=border)
                for key, default_text in (
                    ("tf", tf),
                    ("state", "-"),
                    ("bias", "Bias: -   Phase: -"),
                    ("meta", "Confidence: -   Age: -"),
                    ("vidya", "VIDYA ∠: -   ΔVol: -"),
                    ("breakout", "Breakout: -"),
                    ("warning", "Warning: -"),
                    ("structure", "Structure: -"),
                    ("pa_flag", "PA-Vol: -"),
                    ("strict", "Strict: -"),
                    ("of_risk", "OF Risk: -"),
                    ("playbook", "Waiting for data"),
                ):
                    lbl = widgets.get(key)
                    if lbl is None:
                        continue
                    lbl.config(text=default_text, bg=bg, fg=(C_TEXT if key == "tf" else ("#555" if key == "playbook" else C_DIM)))
            except Exception:
                pass

    def _market_state_thresholds_obj(self):
        return coerce_market_state_thresholds(getattr(self, "market_state_threshold_values", None))

    def _apply_market_state_thresholds_to_snapshot(self, snapshot: Optional[Dict[str, Any]], prev_state: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        snap = dict(snapshot or {})
        if not snap:
            return snap
        try:
            state = compute_market_state_snapshot(snap, prev_state=prev_state, thresholds=self._market_state_thresholds_obj())
            snap.update(state)
        except Exception:
            pass
        return snap

    def _refresh_market_state_cards_from_latest(self) -> None:
        try:
            for tf in ALL_TIMEFRAMES:
                snap = dict((self.latest.get(tf, {}) if hasattr(self, "latest") else {}) or {})
                if snap:
                    snap = self._apply_market_state_thresholds_to_snapshot(snap, prev_state=snap)
                    self.latest[tf] = snap
                self._update_market_state_card(tf, snap)
        except Exception:
            pass

    def _apply_market_state_thresholds_to_trade_result(self, res: Optional[BacktestResult]) -> None:
        if res is None:
            return
        for trade in list(getattr(res, "trades", []) or []):
            for attr in ("entry_snapshot", "exit_snapshot"):
                snap_map = getattr(trade, attr, None)
                if not isinstance(snap_map, dict):
                    continue
                for tf, snap in list(snap_map.items()):
                    if isinstance(snap, dict):
                        snap_map[tf] = self._apply_market_state_thresholds_to_snapshot(snap, prev_state=snap)

    def _current_market_state_source_streams(self) -> Tuple[str, Optional[Dict[str, pd.DataFrame]]]:
        bt = getattr(self, "bt_streams_full", None)
        if isinstance(bt, dict) and bt:
            return ("Backtest", bt)
        hist = getattr(self, "hist_streams_full", None)
        if isinstance(hist, dict) and hist:
            return ("History", hist)
        return ("None", None)

    def _build_market_state_threshold_preview_text(self, values: Optional[Dict[str, Any]] = None) -> str:
        th = coerce_market_state_thresholds(values or getattr(self, "market_state_threshold_values", None))
        th_map = market_state_thresholds_dict(th)

        lines = ["Market State Threshold Preview", "", "Thresholds:"]
        for key in (
            "vidya_angle_up_deg", "vidya_angle_down_deg", "vidya_angle_strong_deg",
            "vidya_delta_support_pct", "vidya_delta_weak_pct", "direction_long_min",
            "direction_short_max", "compression_direction_abs_max", "compression_macd_gap_norm_max",
            "confidence_base", "expansion_confirmation_min", "expansion_pullback_floor",
            "expansion_exhaustion_cap", "expansion_max_weakness_score",
        ):
            lines.append(f"- {key}: {th_map.get(key)}")

        lines.append("")
        lines.append("Live latest preview:")
        any_live = False
        for tf in ALL_TIMEFRAMES:
            snap = dict((self.latest.get(tf, {}) if hasattr(self, "latest") else {}) or {})
            if not snap:
                continue
            any_live = True
            state = compute_market_state_snapshot(snap, prev_state=snap, thresholds=th)
            lines.append(
                f"- {tf}: {pretty_market_state_label(state.get('market_state'))} | "
                f"bias {state.get('market_bias') or '-'} | phase {state.get('market_phase') or '-'} | "
                f"conf {int(state.get('market_confidence') or 0)}%"
            )
        if not any_live:
            lines.append("- No live/latest snapshots loaded yet.")

        source_name, streams = self._current_market_state_source_streams()
        lines.append("")
        lines.append(f"Loaded dataset diagnostics: {source_name}")
        if streams:
            try:
                rebuilt = rebuild_market_state_streams(streams, thresholds=th)
                summary = summarize_market_state_streams(rebuilt)
                for tf in ALL_TIMEFRAMES:
                    info = summary.get(tf)
                    if not info:
                        continue
                    bars = int(info.get("bars", 0) or 0)
                    counts = dict(info.get("states", {}) or {})
                    latest = pretty_market_state_label(info.get("latest")) if info.get("latest") else "-"
                    conf = info.get("latest_confidence")
                    top = sorted(counts.items(), key=lambda kv: (-int(kv[1]), str(kv[0])))[:3]
                    parts = []
                    for label, cnt in top:
                        pct = (100.0 * float(cnt) / float(max(1, bars)))
                        parts.append(f"{pretty_market_state_label(label)} {int(cnt)} ({pct:.1f}%)")
                    parts_txt = ", ".join(parts) if parts else "no labels"
                    conf_txt = f"{float(conf):.0f}%" if conf is not None and np.isfinite(float(conf)) else "-"
                    lines.append(f"- {tf}: bars {bars}, latest {latest} @ {conf_txt}; top states → {parts_txt}")
            except Exception as exc:
                lines.append(f"- Preview failed: {exc}")
        else:
            lines.append("- No history/backtest data is currently loaded. Preview will use live/latest only.")

        lines.append("")
        lines.append("Apply updates only to this session. Indicator formulas stay untouched; only market-state labels/diagnostics are rebuilt.")
        return "\n".join(lines)

    def _apply_market_state_thresholds_session(self, values: Optional[Dict[str, Any]] = None) -> None:
        th = coerce_market_state_thresholds(values or getattr(self, "market_state_threshold_values", None))
        self.market_state_threshold_values = market_state_thresholds_dict(th)

        # Live/latest snapshots and board.
        try:
            for tf in ALL_TIMEFRAMES:
                if isinstance(self.latest.get(tf), dict) and self.latest.get(tf):
                    self.latest[tf] = self._apply_market_state_thresholds_to_snapshot(self.latest[tf], prev_state=self.latest[tf])
                if isinstance(self.latest_closed.get(tf), dict) and self.latest_closed.get(tf):
                    self.latest_closed[tf] = self._apply_market_state_thresholds_to_snapshot(self.latest_closed[tf], prev_state=self.latest_closed[tf])
            self._refresh_market_state_cards_from_latest()
        except Exception:
            pass

        # Loaded history.
        try:
            if isinstance(getattr(self, "hist_streams_full", None), dict) and self.hist_streams_full:
                self.hist_streams_full = rebuild_market_state_streams(self.hist_streams_full, thresholds=th)
                if getattr(self, "hist_symbol", None) is not None and getattr(self, "hist_start", None) is not None and getattr(self, "hist_end", None) is not None:
                    cropped = {
                        tf: dft.loc[(dft.index >= self.hist_start) & (dft.index <= self.hist_end)].copy()
                        for tf, dft in self.hist_streams_full.items()
                        if isinstance(dft, pd.DataFrame)
                    }
                    self._render_history(self.hist_symbol, self.hist_start, self.hist_end, cropped)
        except Exception:
            pass

        # Loaded backtest streams + trade snapshots used by report visualization.
        try:
            if isinstance(getattr(self, "bt_streams_full", None), dict) and self.bt_streams_full:
                self.bt_streams_full = rebuild_market_state_streams(self.bt_streams_full, thresholds=th)
            if getattr(self, "bt_result", None) is not None:
                self._apply_market_state_thresholds_to_trade_result(self.bt_result)
                self._render_backtest(self.bt_result, getattr(self, "bt_streams_full", None))
        except Exception:
            pass

        try:
            self.status_var.set("Market-state thresholds applied to current session.")
        except Exception:
            pass

    def _show_market_state_threshold_preview(self, values: Dict[str, Any]) -> None:
        preview_txt = self._build_market_state_threshold_preview_text(values)
        win = tk.Toplevel(self)
        win.title("Market State Threshold Preview")
        win.transient(self)
        win.geometry("980x700")
        outer = ttk.Frame(win, padding=10)
        outer.pack(fill="both", expand=True)
        txt = tk.Text(outer, wrap="word", font=("Consolas", 10))
        ysb = ttk.Scrollbar(outer, orient="vertical", command=txt.yview)
        txt.configure(yscrollcommand=ysb.set)
        ysb.pack(side="right", fill="y")
        txt.pack(side="left", fill="both", expand=True)
        txt.insert("1.0", preview_txt)
        txt.configure(state="disabled")
        btns = ttk.Frame(win, padding=(10, 0, 10, 10))
        btns.pack(fill="x")
        ttk.Button(btns, text="Edit Again", command=lambda: [win.destroy(), self._open_market_state_threshold_lab()]).pack(side="left")
        ttk.Button(btns, text="Apply to Session", command=lambda: [self._apply_market_state_thresholds_session(values), win.destroy()]).pack(side="right")
        ttk.Button(btns, text="Close", command=win.destroy).pack(side="right", padx=(0, 6))

    def _open_market_state_threshold_lab(self) -> None:
        dlg = MarketStateThresholdsDialog(self, getattr(self, "market_state_threshold_values", None) or market_state_thresholds_dict())
        self.wait_window(dlg)
        if not getattr(dlg, "result", None):
            return
        self._show_market_state_threshold_preview(dlg.result)

    def _update_market_state_card(self, tf: str, snapshot: Optional[Dict[str, Any]]) -> None:
        try:
            widgets = (getattr(self, "market_state_cards", {}) or {}).get(tf)
            if not widgets:
                return
            snap = dict(snapshot or {})
            label = snap.get("market_state")
            pretty = pretty_market_state_label(label) if label else "-"
            bias = str(snap.get("market_bias") or "-").upper().strip() or "-"
            phase = str(snap.get("market_phase") or "-").upper().strip() or "-"
            conf = snap.get("market_confidence")
            age = snap.get("market_state_age")
            angle_state = str(snap.get("vidya_angle_state") or "-").upper().strip() or "-"
            angle_norm = snap.get("vidya_angle_deg_norm")
            delta_pct = snap.get("vidya_delta_pct")

            fg, bg, border = self._market_state_palette(label)
            frame = widgets.get("frame")
            if frame is not None:
                frame.config(bg=bg, highlightbackground=border, highlightcolor=border)

            def _fmt_num(v: Any, fmt: str = ".1f", suffix: str = "") -> str:
                try:
                    fv = float(v)
                    if np.isfinite(fv):
                        return f"{format(fv, fmt)}{suffix}"
                except Exception:
                    pass
                return "-"

            angle_part = angle_state
            if angle_norm is not None:
                norm_txt = _fmt_num(angle_norm, ".1f", "°")
                if norm_txt != "-":
                    angle_part = f"{angle_state} {norm_txt}" if angle_state not in ("", "-") else norm_txt
            delta_txt = _fmt_num(delta_pct, ".1f", "%")
            breakout_pretty = str(snap.get("market_breakout_pretty") or snap.get("market_breakout_state") or "-").strip() or "-"
            breakout_bias = str(snap.get("market_breakout_bias") or "-").upper().strip() or "-"
            breakout_score = _fmt_num(snap.get("market_breakout_score"), ".0f", "%")
            breakout_note = str(snap.get("market_breakout_note") or "").strip()
            of_pretty = str(snap.get("of_state_pretty") or snap.get("of_state") or "-").strip() or "-"
            of_pass = bool(snap.get("market_breakout_filter_pass") or snap.get("of_filter_pass"))
            integration_mode = str(snap.get("of_integration_mode") or "-").upper().strip() or "-"
            integration_effect = str(snap.get("of_integration_effect") or "").upper().strip()
            integration_blocked = bool(snap.get("of_integration_blocked"))
            cur_ready = bool(snap.get("market_breakout_trigger_ready"))
            breakout_line = "Breakout: -"
            if breakout_pretty not in ("", "-", "NONE"):
                breakout_line = f"Breakout: {breakout_pretty}"
                if breakout_bias not in ("", "-", "NEUTRAL"):
                    breakout_line += f" ({breakout_bias.title()})"
                if breakout_score != "-":
                    breakout_line += f"  {breakout_score}"
                if of_pretty not in ("", "-", "NONE", "NO_DATA"):
                    breakout_line += f" | {of_pretty}"
                    if of_pass:
                        breakout_line += " ✓"
                if integration_mode not in ("", "-"):
                    breakout_line += f" | {integration_mode}"
                    if integration_blocked:
                        breakout_line += " ✕"
                    elif integration_effect.endswith("PASS") or integration_effect.endswith("READY"):
                        breakout_line += " ✓"
            if cur_ready:
                breakout_line = "⚡ " + breakout_line

            header_context = str(snap.get("market_header_context") or "").strip()
            header_context = header_context.replace("|", "·") if header_context else ""
            widgets.get("tf").config(text=tf, bg=bg, fg=C_TEXT)
            widgets.get("state").config(text=pretty, bg=bg, fg=(fg if pretty != "-" else C_DIM))
            bias_line = f"Bias: {bias.title() if bias != '-' else '-'}   Phase: {phase.title() if phase != '-' else '-'}"
            if header_context:
                bias_line = f"{header_context}   {bias_line}"
            widgets.get("bias").config(text=bias_line, bg=bg, fg=C_TEXT if label else C_DIM)
            widgets.get("meta").config(text=f"Confidence: {_fmt_num(conf, '.0f', '%')}   Age: {_fmt_num(age, '.0f', 'b')}", bg=bg, fg=C_TEXT if label else C_DIM)
            widgets.get("vidya").config(text=f"VIDYA ∠: {angle_part}   ΔVol: {delta_txt}", bg=bg, fg=C_TEXT if label else C_DIM)
            if cur_ready and frame is not None:
                try:
                    frame.config(highlightbackground="#c62828", highlightcolor="#c62828")
                except Exception:
                    pass
            widgets.get("breakout").config(text=breakout_line, bg=bg, fg=(("#b71c1c" if breakout_bias == "SHORT" else "#0f7a39") if cur_ready else (fg if breakout_pretty not in ("", "-", "NONE") else C_DIM)))
            filter_note = str(snap.get("market_breakout_filter_note") or snap.get("of_filter_note") or snap.get("of_note") or "").strip()
            warning_note = str(snap.get("market_warning_note") or "").strip() or breakout_note or filter_note
            warning_text = f"Warning: {warning_note}" if warning_note else "Warning: -"
            widgets.get("warning").config(
                text=warning_text,
                bg=bg,
                fg=(fg if warning_note and (str(snap.get("market_structure_flag") or "") == "BROKEN" or str(snap.get("market_pa_vol_flag") or "") == "NO_PASS" or breakout_pretty not in ("", "-", "NONE")) else (C_TEXT if warning_note else C_DIM)),
            )
            structure_flag = str(snap.get("market_structure_flag") or "-").upper().strip() or "-"
            structure_map = {"INTACT": "Intact", "BROKEN": "Broken", "MIXED": "Mixed", "-": "-"}
            structure_text = f"Structure: {structure_map.get(structure_flag, structure_flag.title())}"
            structure_fg = fg if structure_flag == "INTACT" else (C_RED if structure_flag == "BROKEN" else C_DIM)
            widgets.get("structure").config(text=structure_text, bg=bg, fg=structure_fg)
            pa_flag = str(snap.get("market_pa_vol_flag") or "-").upper().strip() or "-"
            pa_map = {"PASS": "Pass", "NO_PASS": "No Pass", "MIXED": "Mixed", "-": "-"}
            pa_text = f"PA-Vol: {pa_map.get(pa_flag, pa_flag.title())}"
            pa_fg = fg if pa_flag == "PASS" else (C_RED if pa_flag == "NO_PASS" else C_DIM)
            widgets.get("pa_flag").config(text=pa_text, bg=bg, fg=pa_fg)
            strict_flag = str(snap.get("market_pa_strict_flag") or "-").upper().strip() or "-"
            strict_map = {"PASS": "Pass", "NO_PASS": "No Pass", "MIXED": "Mixed", "-": "-"}
            strict_text = f"Strict: {strict_map.get(strict_flag, strict_flag.title())}"
            strict_fg = fg if strict_flag == "PASS" else (C_RED if strict_flag == "NO_PASS" else C_DIM)
            widgets.get("strict").config(text=strict_text, bg=bg, fg=strict_fg)
            of_risk_flag = str(snap.get("market_of_risk_flag") or "-").upper().strip() or "-"
            of_risk_map = {"PASS": "Pass", "CAUTION": "Caution", "BLOCK": "Block", "MIXED": "Mixed", "-": "-"}
            of_risk_text = f"OF Risk: {of_risk_map.get(of_risk_flag, of_risk_flag.title())}"
            of_risk_fg = fg if of_risk_flag == "PASS" else (C_RED if of_risk_flag == "BLOCK" else (C_AMBER if of_risk_flag == "CAUTION" else C_DIM))
            widgets.get("of_risk").config(text=of_risk_text, bg=bg, fg=of_risk_fg)
            summary_short = str(snap.get("market_summary_short") or "").strip()
            playbook_text = summary_short if summary_short else self._market_state_playbook(label)
            widgets.get("playbook").config(text=playbook_text, bg=bg, fg="#555" if label else C_DIM)

            try:
                prev_ready_map = getattr(self, "_market_breakout_ready_prev", None)
                if not isinstance(prev_ready_map, dict):
                    prev_ready_map = {}
                    self._market_breakout_ready_prev = prev_ready_map
                prev_ready = bool(prev_ready_map.get(tf))
                prev_ready_map[tf] = bool(cur_ready)
                if cur_ready and not prev_ready:
                    try:
                        self.bell()
                    except Exception:
                        pass
                    try:
                        self._register_breakout_alert(tf, snap)
                    except Exception:
                        pass
                    try:
                        frame.config(highlightbackground=C_AMBER, highlightcolor=C_AMBER)
                        self.after(1200, lambda fr=frame, col=("#c62828" if bool((getattr(self, 'latest', {}) or {}).get(tf, {}).get('market_breakout_trigger_ready')) else border): fr.winfo_exists() and fr.config(highlightbackground=col, highlightcolor=col))
                    except Exception:
                        pass
            except Exception:
                pass

            # Refresh detail popup live if it is open.
            try:
                wins = getattr(self, "_market_state_detail_windows", {})
                win = wins.get(tf) if isinstance(wins, dict) else None
                if win is not None and win.winfo_exists():
                    if hasattr(win, "_content_text"):
                        self._set_popup_text_content(win._content_text, self._build_market_state_detail_text(tf), preserve_view=True)
                    elif hasattr(win, "_content_label"):
                        win._content_label.config(text=self._build_market_state_detail_text(tf))
            except Exception:
                pass
            try:
                of_wins = getattr(self, "_orderflow_detail_windows", {})
                of_win = of_wins.get(tf) if isinstance(of_wins, dict) else None
                if of_win is not None and of_win.winfo_exists():
                    if hasattr(of_win, "_content_text"):
                        self._set_popup_text_content(of_win._content_text, self._build_orderflow_detail_text(tf), preserve_view=True)
                    elif hasattr(of_win, "_content_label"):
                        of_win._content_label.config(text=self._build_orderflow_detail_text(tf))
            except Exception:
                pass
        except Exception:
            pass

    def _popup_yes_no(self, value: Any) -> str:
        return ui_popup_yes_no(value)


    def _derive_classic_ta_snapshot(self, latest: Dict[str, Any]) -> Dict[str, str]:
        return ui_derive_classic_ta_snapshot(latest)


    def _detail_section(self, title: str, lines: List[str]) -> str:
        return ui_detail_section(title, lines)


    def _build_market_state_detail_text(self, tf: str) -> str:
        return ui_build_market_state_detail_text(self, tf)


    def _build_orderflow_detail_text(self, tf: str) -> str:
        return ui_build_orderflow_detail_text(self, tf)


    def _build_orderflow_trail_text(self, tf: str) -> str:
        return ui_build_orderflow_trail_text(self, tf)


    def _bind_popup_text_scroll(self, txt: tk.Text) -> None:
        ui_bind_popup_text_scroll(txt)


    def _make_popup_text_widget(self, parent, *, width: int = 120, height: int = 34, wrap: str = "none") -> tk.Text:
        return ui_make_popup_text_widget(self, parent, width=width, height=height, wrap=wrap)


    def _set_popup_text_content(self, txt: tk.Text, content: str, *, preserve_view: bool = True) -> None:
        ui_set_popup_text_content(txt, content, preserve_view=preserve_view)


    def _show_orderflow_trail(self, tf: str):
        return ui_show_orderflow_trail(self, tf)


    def _show_orderflow_details(self, tf: str):
        return ui_show_orderflow_details(self, tf)


    def _show_market_state_details(self, tf: str):
        return ui_show_market_state_details(self, tf)


    def _on_market_state_card_click(self, event, tf: str):
        current = (self.latest.get(tf, {}) if hasattr(self, "latest") else {}) or {}
        menu = tk.Menu(self, tearoff=0)

        def add(rule: Optional[Rule]):
            if rule is not None:
                self._add_rule_active(rule)

        label = current.get("market_state")
        bias = current.get("market_bias")
        phase = current.get("market_phase")
        conf = current.get("market_confidence")
        age = current.get("market_state_age")

        if label:
            menu.add_command(label=f"Add STATE Market = {pretty_market_state_label(label)}", command=lambda l=label: add(Rule(tf, "state", "market_state", "=", l)))
            menu.add_command(label=f"Add STATE Market != {pretty_market_state_label(label)}", command=lambda l=label: add(Rule(tf, "state", "market_state", "!=", l)))
        if bias:
            menu.add_command(label=f"Add STATE Bias = {bias}", command=lambda b=bias: add(Rule(tf, "state", "market_bias", "=", b)))
        if phase:
            menu.add_command(label=f"Add STATE Phase = {phase}", command=lambda p=phase: add(Rule(tf, "state", "market_phase", "=", p)))
        if conf is not None:
            try:
                conf_val = float(conf)
            except Exception:
                conf_val = None
            if conf_val is not None and np.isfinite(conf_val):
                menu.add_command(label=f"Add STATE Confidence ≥ {int(round(conf_val))}", command=lambda v=conf_val: add(Rule(tf, "state", "market_confidence", ">=", float(int(round(v))))))
        if age is not None:
            try:
                age_val = float(age)
            except Exception:
                age_val = None
            if age_val is not None and np.isfinite(age_val):
                menu.add_command(label=f"Add STATE Age ≥ {int(round(age_val))}", command=lambda v=age_val: add(Rule(tf, "state", "market_state_age", ">=", float(int(round(v))))))

        menu.add_separator()
        menu.add_command(label="Show Market State Details", command=lambda tf=tf: self._show_market_state_details(tf))
        menu.add_command(label="Show Order Flow Details", command=lambda tf=tf: self._show_orderflow_details(tf))

        try:
            menu.tk_popup(event.x_root, event.y_root)
        finally:
            try:
                menu.grab_release()
            except Exception:
                pass

    # ---------- Clicking live cells ----------
    def _on_live_cell_click(self, event, tf: str, col: str):
        if col not in ("MACD", "Signal", "M/S", "Angle", "SigAngle", "PPO", "FlipAge", "ADX", "ADXAngle", "ATR", "ATRAngle", "FRAMA", "VIDYA", "Range", "+DI", "-DI"):
            return

        current = self.latest.get(tf, {})
        menu = tk.Menu(self, tearoff=0)

        def add(rule: Optional[Rule]):
            if rule is not None:
                self._add_rule_active(rule)

        # --------- Binary dots: M/S, Angle, PPO ----------
        if col in ("M/S", "Angle", "SigAngle", "PPO", "ADXAngle", "ATRAngle"):
            field = {"M/S": "ms", "Angle": "angle", "SigAngle": "sig_angle", "PPO": "ppo", "ADXAngle": "adx_angle", "ATRAngle": "atr_angle"}[col]
            cur = current.get(field)

            # STATE quick
            menu.add_command(label=f"Add STATE {field.upper()} = GREEN", command=lambda: add(Rule(tf, "state", field, "=", "GREEN")))
            menu.add_command(label=f"Add STATE {field.upper()} = RED",   command=lambda: add(Rule(tf, "state", field, "=", "RED")))
            menu.add_command(label=f"Add STATE {field.upper()} != GREEN", command=lambda: add(Rule(tf, "state", field, "!=", "GREEN")))
            menu.add_command(label=f"Add STATE {field.upper()} != RED",   command=lambda: add(Rule(tf, "state", field, "!=", "RED")))
            menu.add_separator()

            # STATE custom
            def _custom_state():
                dlg = DotRuleDialog(self, f"{tf} {col} STATE rule", "=", "GREEN" if cur != "GREEN" else "GREEN")
                self.wait_window(dlg)
                if dlg.result:
                    op, val = dlg.result
                    add(Rule(tf, "state", field, op, val))
            menu.add_command(label="Add STATE (custom operator)…", command=_custom_state)

            menu.add_separator()

            # EVENT quick (filters)
            menu.add_command(label=f"Add EVENT {field.upper()} TURNED GREEN", command=lambda: add(Rule(tf, "event", field, "TURNED", "GREEN")))
            menu.add_command(label=f"Add EVENT {field.upper()} TURNED RED",   command=lambda: add(Rule(tf, "event", field, "TURNED", "RED")))
            menu.add_command(label=f"Add EVENT {field.upper()} FLIPPED",      command=lambda: add(Rule(tf, "event", field, "FLIPPED", None)))

            menu.add_separator()
            # EVENT quick (★ triggers)
            menu.add_command(label=f"Add ★TRIGGER {field.upper()} TURNED GREEN", command=lambda: add(Rule(tf, "event", field, "TURNED", "GREEN", is_trigger=True)))
            menu.add_command(label=f"Add ★TRIGGER {field.upper()} TURNED RED",   command=lambda: add(Rule(tf, "event", field, "TURNED", "RED", is_trigger=True)))
            menu.add_command(label=f"Add ★TRIGGER {field.upper()} FLIPPED",      command=lambda: add(Rule(tf, "event", field, "FLIPPED", None, is_trigger=True)))

            # Extra: MACD cross up/down (especially useful when clicking M/S)
            if col == "M/S":
                menu.add_separator()
                menu.add_command(label="Add EVENT MACD×SIGNAL CROSS UP",   command=lambda: add(Rule(tf, "event", "ms_cross", "CROSS", "UP")))
                menu.add_command(label="Add EVENT MACD×SIGNAL CROSS DOWN", command=lambda: add(Rule(tf, "event", "ms_cross", "CROSS", "DOWN")))
                menu.add_separator()
                menu.add_command(label="Add ★TRIGGER MACD×SIGNAL CROSS UP",   command=lambda: add(Rule(tf, "event", "ms_cross", "CROSS", "UP", is_trigger=True)))
                menu.add_command(label="Add ★TRIGGER MACD×SIGNAL CROSS DOWN", command=lambda: add(Rule(tf, "event", "ms_cross", "CROSS", "DOWN", is_trigger=True)))

        elif col == "FRAMA":
            menu.add_command(label="Add STATE FRAMA = BUY", command=lambda: add(Rule(tf, "state", "frama_state", "=", "BUY")))
            menu.add_command(label="Add STATE FRAMA = SELL", command=lambda: add(Rule(tf, "state", "frama_state", "=", "SELL")))
            menu.add_command(label="Add STATE FRAMA = NEUTRAL", command=lambda: add(Rule(tf, "state", "frama_state", "=", "NEUTRAL")))
            menu.add_separator()
            menu.add_command(label="Add STATE FRAMA != BUY", command=lambda: add(Rule(tf, "state", "frama_state", "!=", "BUY")))
            menu.add_command(label="Add STATE FRAMA != SELL", command=lambda: add(Rule(tf, "state", "frama_state", "!=", "SELL")))
            menu.add_separator()
            menu.add_command(label="Add EVENT FRAMA BREAK UP", command=lambda: add(Rule(tf, "event", "frama_break_up", "EVENT", None)))
            menu.add_command(label="Add EVENT FRAMA BREAK DOWN", command=lambda: add(Rule(tf, "event", "frama_break_down", "EVENT", None)))
            menu.add_command(label="Add EVENT FRAMA MID CROSS", command=lambda: add(Rule(tf, "event", "frama_mid_cross", "EVENT", None)))
            menu.add_separator()
            menu.add_command(label="Add ★TRIGGER FRAMA BREAK UP", command=lambda: add(Rule(tf, "event", "frama_break_up", "EVENT", None, is_trigger=True)))
            menu.add_command(label="Add ★TRIGGER FRAMA BREAK DOWN", command=lambda: add(Rule(tf, "event", "frama_break_down", "EVENT", None, is_trigger=True)))
        elif col == "VIDYA":
            menu.add_command(label="View VIDYA Details…", command=lambda tf=tf: self._show_vidya_details(tf))
            menu.add_separator()
            menu.add_command(label="Add STATE VIDYA = BUY", command=lambda: add(Rule(tf, "state", "vidya_state", "=", "BUY")))
            menu.add_command(label="Add STATE VIDYA = SELL", command=lambda: add(Rule(tf, "state", "vidya_state", "=", "SELL")))
            menu.add_separator()
            menu.add_command(label="Add STATE VIDYA != BUY", command=lambda: add(Rule(tf, "state", "vidya_state", "!=", "BUY")))
            menu.add_command(label="Add STATE VIDYA != SELL", command=lambda: add(Rule(tf, "state", "vidya_state", "!=", "SELL")))
            menu.add_separator()
            menu.add_command(label="Add EVENT VIDYA TREND UP", command=lambda: add(Rule(tf, "event", "vidya_trend_up", "EVENT", None)))
            menu.add_command(label="Add EVENT VIDYA TREND DOWN", command=lambda: add(Rule(tf, "event", "vidya_trend_down", "EVENT", None)))
            menu.add_separator()
            menu.add_command(label="Add ★TRIGGER VIDYA TREND UP", command=lambda: add(Rule(tf, "event", "vidya_trend_up", "EVENT", None, is_trigger=True)))
            menu.add_command(label="Add ★TRIGGER VIDYA TREND DOWN", command=lambda: add(Rule(tf, "event", "vidya_trend_down", "EVENT", None, is_trigger=True)))
            menu.add_separator()
            menu.add_command(label="Add EVENT VIDYA NEW LIQ LOW", command=lambda: add(Rule(tf, "event", "vidya_new_liq_low", "EVENT", None)))
            menu.add_command(label="Add EVENT VIDYA NEW LIQ HIGH", command=lambda: add(Rule(tf, "event", "vidya_new_liq_high", "EVENT", None)))
            menu.add_command(label="Add EVENT VIDYA LIQ LOW TAKEN", command=lambda: add(Rule(tf, "event", "vidya_liq_low_taken", "EVENT", None)))
            menu.add_command(label="Add EVENT VIDYA LIQ HIGH TAKEN", command=lambda: add(Rule(tf, "event", "vidya_liq_high_taken", "EVENT", None)))
            menu.add_separator()
            menu.add_command(label="Add ★TRIGGER VIDYA NEW LIQ LOW", command=lambda: add(Rule(tf, "event", "vidya_new_liq_low", "EVENT", None, is_trigger=True)))
            menu.add_command(label="Add ★TRIGGER VIDYA NEW LIQ HIGH", command=lambda: add(Rule(tf, "event", "vidya_new_liq_high", "EVENT", None, is_trigger=True)))
            menu.add_command(label="Add ★TRIGGER VIDYA LIQ LOW TAKEN", command=lambda: add(Rule(tf, "event", "vidya_liq_low_taken", "EVENT", None, is_trigger=True)))
            menu.add_command(label="Add ★TRIGGER VIDYA LIQ HIGH TAKEN", command=lambda: add(Rule(tf, "event", "vidya_liq_high_taken", "EVENT", None, is_trigger=True)))
            menu.add_separator()
            menu.add_command(label="Add STATE VIDYA Active LIQ Low Count >= 1", command=lambda: add(Rule(tf, "state", "vidya_active_liq_low_count", ">=", 1.0)))
            menu.add_command(label="Add STATE VIDYA Active LIQ High Count >= 1", command=lambda: add(Rule(tf, "state", "vidya_active_liq_high_count", ">=", 1.0)))
            menu.add_command(label="Add STATE VIDYA Dist to LIQ Low % <= 1.0", command=lambda: add(Rule(tf, "state", "vidya_dist_to_liq_low_pct", "<=", 1.0)))
            menu.add_command(label="Add STATE VIDYA Dist to LIQ High % <= 1.0", command=lambda: add(Rule(tf, "state", "vidya_dist_to_liq_high_pct", "<=", 1.0)))
            menu.add_command(label="Add STATE VIDYA Δ% >= 0", command=lambda: add(Rule(tf, "state", "vidya_delta_pct", ">=", 0.0)))
            menu.add_command(label="Add STATE VIDYA Angle State = UP", command=lambda: add(Rule(tf, "state", "vidya_angle_state", "=", "UP")))
            menu.add_command(label="Add STATE VIDYA Angle State = DOWN", command=lambda: add(Rule(tf, "state", "vidya_angle_state", "=", "DOWN")))
            menu.add_command(label="Add STATE VIDYA Angle Accel = ACCEL", command=lambda: add(Rule(tf, "state", "vidya_angle_accel", "=", "ACCEL")))
            menu.add_command(label="Add STATE Market = Bull Pullback", command=lambda: add(Rule(tf, "state", "market_state", "=", "BULL_PULLBACK")))
            menu.add_command(label="Add STATE Market Bias = LONG", command=lambda: add(Rule(tf, "state", "market_bias", "=", "LONG")))
            menu.add_command(label="Add STATE Market Phase = PULLBACK", command=lambda: add(Rule(tf, "state", "market_phase", "=", "PULLBACK")))

            def _custom_vidya_numeric(field: str, pretty: str, default_value: str):
                dlg = NumericRuleDialog(self, f"{pretty} rule for {tf}", ">=", default_value)
                self.wait_window(dlg)
                if dlg.result:
                    op, val_str = dlg.result
                    try:
                        val = float(val_str)
                    except Exception:
                        messagebox.showerror("Error", f"{pretty} must be a number.")
                        return
                    add(Rule(tf, "state", field, op, val))

            menu.add_separator()
            menu.add_command(label="Add STATE VIDYA Δ% (custom operator)…", command=lambda: _custom_vidya_numeric("vidya_delta_pct", "VIDYA Δ%", f"{float(current.get('vidya_delta_pct')):.3f}" if current.get('vidya_delta_pct') is not None else "0"))
            menu.add_command(label="Add STATE VIDYA Slope (custom)…", command=lambda: _custom_vidya_numeric("vidya_slope", "VIDYA Slope", f"{float(current.get('vidya_slope')):.6f}" if current.get('vidya_slope') is not None else "0"))
            menu.add_command(label="Add STATE VIDYA Angle° (custom)…", command=lambda: _custom_vidya_numeric("vidya_angle_deg_norm", "VIDYA Angle° Norm", f"{float(current.get('vidya_angle_deg_norm')):.3f}" if current.get('vidya_angle_deg_norm') is not None else "0"))
            menu.add_command(label="Add STATE Market Confidence (custom)…", command=lambda: _custom_vidya_numeric("market_confidence", "Market Confidence", f"{float(current.get('market_confidence')):.0f}" if current.get('market_confidence') is not None else "50"))
            menu.add_command(label="Add STATE Market Age (custom)…", command=lambda: _custom_vidya_numeric("market_state_age", "Market State Age", f"{float(current.get('market_state_age')):.0f}" if current.get('market_state_age') is not None else "1"))
            menu.add_command(label="Add STATE VIDYA Active LIQ Low Count (custom)…", command=lambda: _custom_vidya_numeric("vidya_active_liq_low_count", "VIDYA Active LIQ Low Count", f"{float(current.get('vidya_active_liq_low_count')):.0f}" if current.get('vidya_active_liq_low_count') is not None else "1"))
            menu.add_command(label="Add STATE VIDYA Active LIQ High Count (custom)…", command=lambda: _custom_vidya_numeric("vidya_active_liq_high_count", "VIDYA Active LIQ High Count", f"{float(current.get('vidya_active_liq_high_count')):.0f}" if current.get('vidya_active_liq_high_count') is not None else "1"))
            menu.add_command(label="Add STATE VIDYA Dist to LIQ Low % (custom)…", command=lambda: _custom_vidya_numeric("vidya_dist_to_liq_low_pct", "VIDYA Dist to LIQ Low %", f"{float(current.get('vidya_dist_to_liq_low_pct')):.3f}" if current.get('vidya_dist_to_liq_low_pct') is not None else "1.0"))
            menu.add_command(label="Add STATE VIDYA Dist to LIQ High % (custom)…", command=lambda: _custom_vidya_numeric("vidya_dist_to_liq_high_pct", "VIDYA Dist to LIQ High %", f"{float(current.get('vidya_dist_to_liq_high_pct')):.3f}" if current.get('vidya_dist_to_liq_high_pct') is not None else "1.0"))
            menu.add_command(label="Add STATE VIDYA Nearest LIQ Low (custom)…", command=lambda: _custom_vidya_numeric("vidya_nearest_liq_low", "VIDYA Nearest LIQ Low", f"{float(current.get('vidya_nearest_liq_low')):.3f}" if current.get('vidya_nearest_liq_low') is not None else "0"))
            menu.add_command(label="Add STATE VIDYA Nearest LIQ High (custom)…", command=lambda: _custom_vidya_numeric("vidya_nearest_liq_high", "VIDYA Nearest LIQ High", f"{float(current.get('vidya_nearest_liq_high')):.3f}" if current.get('vidya_nearest_liq_high') is not None else "0"))

        elif col == "Range":
            menu.add_command(label="Add STATE RANGE = BUY", command=lambda: add(Rule(tf, "state", "range_filter_state", "=", "BUY")))
            menu.add_command(label="Add STATE RANGE = SELL", command=lambda: add(Rule(tf, "state", "range_filter_state", "=", "SELL")))
            menu.add_command(label="Add STATE RANGE = NEUTRAL", command=lambda: add(Rule(tf, "state", "range_filter_state", "=", "NEUTRAL")))
            menu.add_separator()
            menu.add_command(label="Add STATE RANGE PHASE = BUY_STRONG", command=lambda: add(Rule(tf, "state", "range_filter_phase", "=", "BUY_STRONG")))
            menu.add_command(label="Add STATE RANGE PHASE = BUY_WEAK", command=lambda: add(Rule(tf, "state", "range_filter_phase", "=", "BUY_WEAK")))
            menu.add_command(label="Add STATE RANGE PHASE = SELL_STRONG", command=lambda: add(Rule(tf, "state", "range_filter_phase", "=", "SELL_STRONG")))
            menu.add_command(label="Add STATE RANGE PHASE = SELL_WEAK", command=lambda: add(Rule(tf, "state", "range_filter_phase", "=", "SELL_WEAK")))
            menu.add_command(label="Add STATE RANGE PHASE = NEUTRAL", command=lambda: add(Rule(tf, "state", "range_filter_phase", "=", "NEUTRAL")))
            menu.add_separator()
            menu.add_command(label="Add STATE RANGE != BUY", command=lambda: add(Rule(tf, "state", "range_filter_state", "!=", "BUY")))
            menu.add_command(label="Add STATE RANGE != SELL", command=lambda: add(Rule(tf, "state", "range_filter_state", "!=", "SELL")))
            menu.add_command(label="Add STATE RANGE PHASE != BUY_STRONG", command=lambda: add(Rule(tf, "state", "range_filter_phase", "!=", "BUY_STRONG")))
            menu.add_command(label="Add STATE RANGE PHASE != SELL_STRONG", command=lambda: add(Rule(tf, "state", "range_filter_phase", "!=", "SELL_STRONG")))
            menu.add_separator()
            menu.add_command(label="Add EVENT RANGE BUY", command=lambda: add(Rule(tf, "event", "range_filter_buy", "EVENT", None)))
            menu.add_command(label="Add EVENT RANGE SELL", command=lambda: add(Rule(tf, "event", "range_filter_sell", "EVENT", None)))
            menu.add_separator()
            menu.add_command(label="Add ★TRIGGER RANGE BUY", command=lambda: add(Rule(tf, "event", "range_filter_buy", "EVENT", None, is_trigger=True)))
            menu.add_command(label="Add ★TRIGGER RANGE SELL", command=lambda: add(Rule(tf, "event", "range_filter_sell", "EVENT", None, is_trigger=True)))

            def _custom_range_numeric(field: str, pretty: str, default_value: str):
                dlg = NumericRuleDialog(self, f"{pretty} rule for {tf}", ">=", default_value)
                self.wait_window(dlg)
                if dlg.result:
                    op, val_str = dlg.result
                    try:
                        val = float(val_str)
                    except Exception:
                        messagebox.showerror("Error", f"{pretty} must be a number.")
                        return
                    add(Rule(tf, "state", field, op, val))

            menu.add_separator()
            menu.add_command(label="Add STATE RANGE Upward Count >= 1", command=lambda: add(Rule(tf, "state", "range_filter_upward_count", ">=", 1.0)))
            menu.add_command(label="Add STATE RANGE Downward Count >= 1", command=lambda: add(Rule(tf, "state", "range_filter_downward_count", ">=", 1.0)))
            menu.add_command(label="Add STATE RANGE Line (custom)…", command=lambda: _custom_range_numeric("range_filter_line", "Range Filter Line", f"{float(current.get('range_filter_line')):.6f}" if current.get('range_filter_line') is not None else "0"))
            menu.add_command(label="Add STATE RANGE Smooth Range (custom)…", command=lambda: _custom_range_numeric("range_filter_smooth_range", "Range Smooth Range", f"{float(current.get('range_filter_smooth_range')):.6f}" if current.get('range_filter_smooth_range') is not None else "0"))
            menu.add_command(label="Add STATE RANGE Upward Count (custom)…", command=lambda: _custom_range_numeric("range_filter_upward_count", "Range Upward Count", f"{float(current.get('range_filter_upward_count')):.0f}" if current.get('range_filter_upward_count') is not None else "1"))
            menu.add_command(label="Add STATE RANGE Downward Count (custom)…", command=lambda: _custom_range_numeric("range_filter_downward_count", "Range Downward Count", f"{float(current.get('range_filter_downward_count')):.0f}" if current.get('range_filter_downward_count') is not None else "1"))

        # --------- Numeric: FlipAge ----------
        elif col == "FlipAge":
            fa = current.get("flip_age")
            menu.add_command(label="Add STATE FlipAge <= 1", command=lambda: add(Rule(tf, "state", "flip_age", "<=", 1)))
            menu.add_command(label="Add STATE FlipAge <= 3", command=lambda: add(Rule(tf, "state", "flip_age", "<=", 3)))
            menu.add_command(label="Add STATE FlipAge <= 5", command=lambda: add(Rule(tf, "state", "flip_age", "<=", 5)))
            menu.add_separator()

            def _custom_fa():
                default_val = str(fa if fa is not None else 3)
                dlg = NumericRuleDialog(self, f"FlipAge rule for {tf}", "<=", default_val)
                self.wait_window(dlg)
                if dlg.result:
                    op, val_str = dlg.result
                    try:
                        val = int(float(val_str))
                    except Exception:
                        messagebox.showerror("Error", "FlipAge must be a number.")
                        return
                    add(Rule(tf, "state", "flip_age", op, val))
            menu.add_command(label="Add STATE FlipAge (custom operator)…", command=_custom_fa)

        # --------- Numeric: MACD / Signal ----------
        elif col in ("MACD", "Signal"):
            field = "macd" if col == "MACD" else "signal"
            menu.add_command(label=f"Add STATE {col} > 0", command=lambda: add(Rule(tf, "state", field, ">", 0.0)))
            menu.add_command(label=f"Add STATE {col} < 0", command=lambda: add(Rule(tf, "state", field, "<", 0.0)))
            menu.add_separator()

            def _custom_num():
                v = current.get(field)
                default_val = f"{float(v):.6f}" if v is not None else "0"
                dlg = NumericRuleDialog(self, f"{col} rule for {tf}", ">", default_val)
                self.wait_window(dlg)
                if dlg.result:
                    op, val_str = dlg.result
                    try:
                        val = float(val_str)
                    except Exception:
                        messagebox.showerror("Error", f"{col} must be a number.")
                        return
                    add(Rule(tf, "state", field, op, val))
            menu.add_command(label=f"Add STATE {col} (custom operator)…", command=_custom_num)

            # MACD zero-line cross (event)
            if col == "MACD":
                menu.add_separator()
                menu.add_command(label="Add EVENT MACD×0 CROSS UP",   command=lambda: add(Rule(tf, "event", "macd0_cross", "CROSS", "UP")))
                menu.add_command(label="Add EVENT MACD×0 CROSS DOWN", command=lambda: add(Rule(tf, "event", "macd0_cross", "CROSS", "DOWN")))
                menu.add_separator()
                menu.add_command(label="Add ★TRIGGER MACD×0 CROSS UP",   command=lambda: add(Rule(tf, "event", "macd0_cross", "CROSS", "UP", is_trigger=True)))
                menu.add_command(label="Add ★TRIGGER MACD×0 CROSS DOWN", command=lambda: add(Rule(tf, "event", "macd0_cross", "CROSS", "DOWN", is_trigger=True)))




        # --------- Numeric: ADX / DI+ / DI- ----------
        elif col in ("ADX", "ATR", "+DI", "-DI"):
            if col == "ADX":
                field = "adx"
                menu.add_command(label="Add STATE ADX >= 15", command=lambda: add(Rule(tf, "state", field, ">=", 15.0)))
                menu.add_command(label="Add STATE ADX >= 20", command=lambda: add(Rule(tf, "state", field, ">=", 20.0)))
                menu.add_command(label="Add STATE ADX >= 25", command=lambda: add(Rule(tf, "state", field, ">=", 25.0)))
                menu.add_command(label="Add STATE ADX >= 30", command=lambda: add(Rule(tf, "state", field, ">=", 30.0)))
                menu.add_separator()

                def _custom_adx():
                    v = current.get(field)
                    default_val = f"{float(v):.3f}" if v is not None else "20"
                    dlg = NumericRuleDialog(self, f"ADX rule for {tf}", ">=", default_val)
                    self.wait_window(dlg)
                    if dlg.result:
                        op, val_str = dlg.result
                        try:
                            val = float(val_str)
                        except Exception:
                            messagebox.showerror("Error", "ADX must be a number.")
                            return
                        add(Rule(tf, "state", field, op, val))
                menu.add_command(label="Add STATE ADX (custom operator)…", command=_custom_adx)

            elif col == "ATR":
                field = "atr"
                menu.add_command(label="Add STATE ATR >= 50", command=lambda: add(Rule(tf, "state", field, ">=", 50.0)))
                menu.add_command(label="Add STATE ATR >= 100", command=lambda: add(Rule(tf, "state", field, ">=", 100.0)))
                menu.add_command(label="Add STATE ATR >= 200", command=lambda: add(Rule(tf, "state", field, ">=", 200.0)))
                menu.add_separator()

                def _custom_atr():
                    v = current.get(field)
                    default_val = f"{float(v):.3f}" if v is not None else "100"
                    dlg = NumericRuleDialog(self, f"ATR rule for {tf}", ">=", default_val)
                    self.wait_window(dlg)
                    if dlg.result:
                        op, val_str = dlg.result
                        try:
                            val = float(val_str)
                        except Exception:
                            messagebox.showerror("Error", "ATR must be a number.")
                            return
                        add(Rule(tf, "state", field, op, val))
                menu.add_command(label="Add STATE ATR (custom operator)…", command=_custom_atr)


            else:
                field = "di_plus" if col == "+DI" else "di_minus"
                pretty = "+DI" if field == "di_plus" else "-DI"
                menu.add_command(label=f"Add STATE {pretty} >= 10", command=lambda: add(Rule(tf, "state", field, ">=", 10.0)))
                menu.add_command(label=f"Add STATE {pretty} >= 20", command=lambda: add(Rule(tf, "state", field, ">=", 20.0)))
                menu.add_command(label=f"Add STATE {pretty} >= 30", command=lambda: add(Rule(tf, "state", field, ">=", 30.0)))
                menu.add_separator()

                # Direction filters (use DI spread)
                menu.add_command(label="Add STATE DI+ > DI-", command=lambda: add(Rule(tf, "state", "di_spread", ">", 0.0)))
                menu.add_command(label="Add STATE DI- > DI+", command=lambda: add(Rule(tf, "state", "di_spread", "<", 0.0)))
                menu.add_separator()

                def _custom_di():
                    v = current.get(field)
                    default_val = f"{float(v):.3f}" if v is not None else "20"
                    dlg = NumericRuleDialog(self, f"{pretty} rule for {tf}", ">=", default_val)
                    self.wait_window(dlg)
                    if dlg.result:
                        op, val_str = dlg.result
                        try:
                            val = float(val_str)
                        except Exception:
                            messagebox.showerror("Error", f"{pretty} must be a number.")
                            return
                        add(Rule(tf, "state", field, op, val))
                menu.add_command(label=f"Add STATE {pretty} (custom operator)…", command=_custom_di)

                # DI crossover events
                menu.add_separator()
                menu.add_command(label="Add EVENT DI+×DI- CROSS UP", command=lambda: add(Rule(tf, "event", "di_cross", "CROSS", "UP")))
                menu.add_command(label="Add EVENT DI+×DI- CROSS DOWN", command=lambda: add(Rule(tf, "event", "di_cross", "CROSS", "DOWN")))
                menu.add_separator()
                menu.add_command(label="Add ★TRIGGER DI+×DI- CROSS UP", command=lambda: add(Rule(tf, "event", "di_cross", "CROSS", "UP", is_trigger=True)))
                menu.add_command(label="Add ★TRIGGER DI+×DI- CROSS DOWN", command=lambda: add(Rule(tf, "event", "di_cross", "CROSS", "DOWN", is_trigger=True)))

        try:
            menu.tk_popup(event.x_root, event.y_root)
        finally:
            menu.grab_release()

    # ---------- Rule evaluation ----------
    def _eval_rule(self, rule: Rule) -> Optional[bool]:
        # Delegate to generic evaluator (same logic used by Backtest)
        return eval_rule_generic(rule, self.latest, self.prev_latest, ctx=self._rule_eval_ctx)


    def _eval_group(self, rules: List[Rule], join_mode: str) -> Optional[bool]:
        return eval_group_generic(rules, join_mode, self.latest, self.prev_latest, ctx=self._rule_eval_ctx)


    def _eval_tab(self, tab_name: str) -> Optional[bool]:
        return eval_tab_generic(tab_name, self.rules, self.tab_group_join_mode, self.group_rule_join_mode, self.latest, self.prev_latest, ctx=self._rule_eval_ctx)


    def _update_rule_statuses(self):
        if not self.latest:
            return

        cur_by_tf = self.latest
        prev_by_tf = self.prev_latest

        # current time for latch age / bars-ago step (prefer smallest available TF)
        now_ts = None
        try:
            for _tf in ALL_TIMEFRAMES:
                snap = cur_by_tf.get(_tf)
                if isinstance(snap, dict):
                    t = snap.get("time")
                    if t is not None:
                        now_ts = t
                        break
        except Exception:
            now_ts = None

        # Advance evaluation step index when time advances (so bars-ago reflects bars, not UI refreshes)
        try:
            if now_ts is not None and now_ts != self._rule_eval_last_ts:
                self._rule_eval_ctx.step_index += 1
                self._rule_eval_last_ts = now_ts
        except Exception:
            pass

        # Record snapshot history (used by STATE Bars Ago rules)
        try:
            self._rule_eval_ctx.observe(cur_by_tf)
        except Exception:
            pass

        def eval_list(rules: List[Rule], join_mode: str) -> Optional[bool]:
            if not rules:
                return True
            vals = [eval_rule_generic(r, cur_by_tf, prev_by_tf, ctx=self._rule_eval_ctx) for r in rules]
            mode = join_mode.upper()

            if mode == "AND":
                any_none = False
                for v in vals:
                    if v is False:
                        return False
                    if v is None:
                        any_none = True
                return None if any_none else True
            # OR
            any_none = False
            any_false = False
            for v in vals:
                if v is True:
                    return True
                if v is None:
                    any_none = True
                else:
                    any_false = True
            return False if any_false else (None if any_none else False)
        def set_tab_label(tab_name: str, status: str):
            lbl = None
            try:
                lbl = self.tab_status_labels.get(tab_name)
            except Exception:
                lbl = None
            if lbl is None:
                try:
                    lbl = self.tab_status_lbls.get(tab_name)
                except Exception:
                    lbl = None
            if lbl is None:
                return
            up = status.upper()
            if up.startswith("READY") or up.startswith("FIRED"):
                lbl.config(text=f"STATUS: {status}", bg=C_GREEN, fg="white")
            elif up.startswith("ARMED"):
                lbl.config(text=f"STATUS: {status}", bg=C_AMBER, fg="black")
            elif up.startswith("NO"):
                lbl.config(text=f"STATUS: {status}", bg=C_RED, fg="white")
            else:
                lbl.config(text=f"STATUS: {status}", bg="#777", fg="white")

        for tab_name in RULE_TABS:
            group_statuses: List[str] = []
            group_bools: List[Optional[bool]] = []

            for gi, rules in enumerate(self.rules[tab_name]):
                join_mode = self.group_rule_join_mode[tab_name][gi] if gi < len(self.group_rule_join_mode[tab_name]) else "OR"
                join_mode_norm = str(join_mode or "OR").upper().strip()

                # per-chip status (events remain pulses)
                gui = None
                try:
                    gui = self.group_ui[tab_name][gi]
                    for chip in gui.chips:
                        ok = eval_rule_generic(chip.rule, cur_by_tf, prev_by_tf, ctx=self._rule_eval_ctx)
                        chip.set_satisfied(ok)
                except Exception:
                    gui = None

                # NOTE: Rule uses .mode ("state"/"event"), not .kind. A previous refactor
                # accidentally used .kind which doesn't exist, breaking status evaluation.
                triggers = [r for r in rules if (r.mode == "event" and getattr(r, "is_trigger", False))]
                filters = [r for r in rules if r not in triggers]

                latch_list = self.group_latch.get(tab_name, [])
                while len(latch_list) < len(self.rules[tab_name]):
                    latch_list.append(None)
                self.group_latch[tab_name] = latch_list

                meta_list = self.group_latch_meta.get(tab_name, [])
                while len(meta_list) < len(self.rules[tab_name]):
                    meta_list.append(None)
                self.group_latch_meta[tab_name] = meta_list

                latch_time = latch_list[gi]
                latch_meta = meta_list[gi]

                if join_mode_norm == "SEQUENCE":
                    g = eval_group_generic(rules, join_mode_norm, cur_by_tf, prev_by_tf, ctx=self._rule_eval_ctx, group_key=(tab_name, gi))
                    seq_completed = 0
                    seq_rules, gate_rules = sequence_rule_partition(rules)
                    seq_total = len(seq_rules)
                    try:
                        seq_meta = self._rule_eval_ctx.sequence_snapshot((tab_name, gi), seq_total)
                        seq_completed = int(seq_meta.get("completed_steps", 0) or 0)
                    except Exception:
                        seq_completed = 0

                    if g is None:
                        status = "WAIT"
                    elif g is True:
                        status = "READY"
                    elif seq_completed > 0 and seq_total > 0:
                        gate_txt = f" +G{len(gate_rules)}" if gate_rules else ""
                        status = f"SEQ {seq_completed}/{seq_total}{gate_txt}"
                    else:
                        status = "NO"

                    gb = g
                    latch_list[gi] = None
                    meta_list[gi] = None
                    group_statuses.append(status)
                    group_bools.append(gb)
                    if gui:
                        gui.set_group_status(status)
                    continue

                if triggers:
                    filters_ok = eval_list(filters, join_mode) if filters else True
                    trig_pulse = eval_list(triggers, "OR")

                    if filters_ok is None or trig_pulse is None:
                        status = "WAIT"
                        gb = None
                        latch_list[gi] = None
                        meta_list[gi] = None
                    elif filters_ok is False:
                        status = "NO"
                        gb = False
                        latch_list[gi] = None
                        meta_list[gi] = None
                    else:
                        # filters_ok True
                        # Identify which trigger fired this candle so we can store latch metadata.
                        fired_meta: Optional[Tuple[str, str, Any]] = None
                        if trig_pulse is True:
                            for tr in triggers:
                                ok = eval_rule_generic(tr, cur_by_tf, prev_by_tf, ctx=self._rule_eval_ctx)
                                if ok is True:
                                    if tr.field in ("ms", "angle", "sig_angle", "ppo"):
                                        if tr.op == "TURNED":
                                            v = str(tr.value).upper()
                                            if v in ("GREEN", "RED"):
                                                fired_meta = (tr.timeframe, tr.field, v)
                                        elif tr.op == "FLIPPED":
                                            cv = (cur_by_tf.get(tr.timeframe, {}) or {}).get(tr.field)
                                            if cv in ("GREEN", "RED"):
                                                fired_meta = (tr.timeframe, tr.field, cv)
                                    elif tr.field == "ms_cross":
                                        fired_meta = (tr.timeframe, "ms", "GREEN" if str(tr.value).upper() == "UP" else "RED")
                                    elif tr.field == "macd0_cross":
                                        fired_meta = (tr.timeframe, "macd0", "GREEN" if str(tr.value).upper() == "UP" else "RED")
                                    break

                        def _cur_state_for_meta(meta: Optional[Tuple[str, str, Any]]) -> Optional[str]:
                            if not meta:
                                return None
                            tfm, keym, _valm = meta
                            curm = cur_by_tf.get(tfm, {}) or {}
                            if keym in ("ms", "angle", "sig_angle", "ppo"):
                                v = curm.get(keym)
                                return v if v in ("GREEN", "RED") else None
                            if keym == "macd0":
                                m = curm.get("macd")
                                try:
                                    mf = float(m)
                                    if not np.isfinite(mf):
                                        return None
                                    return "GREEN" if mf > 0.0 else "RED"
                                except Exception:
                                    return None
                            return None

                        if trig_pulse is True:
                            status = "READY"
                            gb = True
                            latch_list[gi] = now_ts if isinstance(now_ts, pd.Timestamp) else pd.Timestamp.utcnow()
                            meta_list[gi] = fired_meta
                        else:
                            # Validate existing latch (FIRED)
                            latch_valid = False
                            if latch_time is not None:
                                if bool(self.latch_until_flip_var.get()):
                                    cs = _cur_state_for_meta(latch_meta)
                                    if cs is None:
                                        # If current state isn't ready, keep FIRED rather than dropping to ARMED.
                                        latch_valid = True
                                    else:
                                        try:
                                            latch_valid = (str(cs).upper() == str(latch_meta[2]).upper())
                                        except Exception:
                                            latch_valid = False
                                else:
                                    latch_valid = True

                            if latch_valid and latch_time is not None:
                                if isinstance(now_ts, pd.Timestamp) and isinstance(latch_time, pd.Timestamp):
                                    age_m = int(max(0, (now_ts - latch_time).total_seconds() // 60))
                                    status = f"FIRED ({age_m}m)"
                                else:
                                    status = "FIRED"
                                gb = True
                            else:
                                latch_list[gi] = None
                                meta_list[gi] = None
                                status = "ARMED"
                                gb = False
                else:
                    g = eval_list(rules, join_mode)
                    if g is None:
                        status = "WAIT"
                    elif g:
                        status = "READY"
                    else:
                        status = "NO"
                    gb = g
                    latch_list[gi] = None
                    meta_list[gi] = None

                group_statuses.append(status)
                group_bools.append(gb)
                if gui:
                    gui.set_group_status(status)

            # Combine groups for tab label (boolean uses READY/FIRED as True, ARMED as False)
            join_groups = self.tab_group_join_mode[tab_name].upper()

            def combine(vals: List[Optional[bool]], mode: str) -> Optional[bool]:
                if not vals:
                    return False
                if mode == "AND":
                    any_none = False
                    for v in vals:
                        if v is False:
                            return False
                        if v is None:
                            any_none = True
                    return None if any_none else True
                # OR
                any_none = False
                any_false = False
                for v in vals:
                    if v is True:
                        return True
                    if v is None:
                        any_none = True
                    else:
                        any_false = True
                return False if any_false else (None if any_none else False)

            tab_bool = combine(group_bools, join_groups)

            if tab_bool is None:
                tab_status = "WAIT"
            elif tab_bool is True:
                tab_status = "READY"
            else:
                tab_status = "NO"

            set_tab_label(tab_name, tab_status)
            self.tab_satisfied[tab_name] = (tab_bool is True)

        self._update_status_line()

    def _backtest_runtime_options(self) -> Tuple[bool, int]:
        fast_mode = True
        try:
            fast_mode = bool(self.bt_fast_details_var.get())
        except Exception:
            fast_mode = True
        capture_trade_details = not fast_mode
        equity_stride = 1 if not fast_mode else 15
        return capture_trade_details, equity_stride

    def on_replay_backtest_only(self):
        self._ensure_backtest_tab_built()
        if self.bt_result is None:
            messagebox.showinfo("Apply exits / costs only", "Run one backtest first.")
            return
        if str((getattr(self, "bt_mode_var", None).get() if hasattr(self, "bt_mode_var") else "") or "") == str(BACKTEST_MODE_TICK):
            messagebox.showinfo("Apply exits / costs only", "This quick replay path is available only for bar-based backtests.")
            return
        try:
            symbol = self.symbol_var.get().strip().upper()
            start = parse_utc(self.start_var.get())
            end = parse_utc(self.end_var.get())
            init_bal = float(self.bt_init_balance_var.get())
            notional = float(self.bt_notional_var.get())
            fee_pct = float(self.bt_fee_pct_var.get())
            slip_bps = float(self.bt_slippage_bps_var.get())
            stop_loss_value = float(self.bt_stop_loss_pct_var.get())
            take_profit_value = float(self.bt_take_profit_pct_var.get())
            stop_loss_mode = str(self.bt_stop_loss_mode_var.get() or "PERCENT").strip().upper()
            take_profit_mode = str(self.bt_take_profit_mode_var.get() or "PERCENT").strip().upper()
            stop_loss_tf = str(self.bt_stop_loss_tf_var.get() or "1m").strip()
            take_profit_tf = str(self.bt_take_profit_tf_var.get() or "1m").strip()
            stop_loss_field = str(self.bt_stop_loss_field_var.get() or "").strip()
            take_profit_field = str(self.bt_take_profit_field_var.get() or "").strip()
            stop_loss_offset_pct = float(self.bt_stop_loss_offset_pct_var.get())
            take_profit_offset_pct = float(self.bt_take_profit_offset_pct_var.get())
            allow_rev = bool(self.bt_reverse_var.get())
            skip_both = bool(self.bt_skip_both_var.get())
            cfg = BacktestConfig(
                initial_balance=init_bal,
                order_notional_usdt=notional,
                fee_rate=fee_pct / 100.0,
                slippage_bps=slip_bps,
                stop_loss_pct=(stop_loss_value if stop_loss_mode == "PERCENT" else 0.0),
                take_profit_pct=(take_profit_value if take_profit_mode == "PERCENT" else 0.0),
                stop_loss_mode=stop_loss_mode,
                stop_loss_value=stop_loss_value,
                stop_loss_timeframe=stop_loss_tf,
                stop_loss_field=stop_loss_field,
                stop_loss_offset_pct=stop_loss_offset_pct,
                take_profit_mode=take_profit_mode,
                take_profit_value=take_profit_value,
                take_profit_timeframe=take_profit_tf,
                take_profit_field=take_profit_field,
                take_profit_offset_pct=take_profit_offset_pct,
                allow_reverse=allow_rev,
                skip_if_both_entries=skip_both,
            )
            try:
                cfg.step_timeframe = str(getattr(self, "bt_step_tf_var", None).get() if hasattr(self, "bt_step_tf_var") else "1m").strip()
            except Exception:
                pass
            plan = build_backtest_replay_plan(
                self.bt_result,
                next_cfg=cfg,
                symbol=symbol,
                start=start,
                end=end,
                rules_model=self.rules,
                tab_group_join_mode=self.tab_group_join_mode,
                group_rule_join_mode=self.group_rule_join_mode,
                entry_filters=self.entry_filters,
            )
            if not plan or not plan.eligible:
                reason = getattr(plan, "reason", "not-eligible") if plan is not None else "not-eligible"
                messagebox.showinfo("Apply exits / costs only", f"Quick replay is not available for the current changes. Reason: {reason}")
                return
        except Exception as exc:
            messagebox.showerror("Apply exits / costs only", str(exc))
            return
        self.on_run_backtest(force_replay_only=True)

    def on_run_backtest(self, force_replay_only: bool = False):
        self._ensure_backtest_tab_built()
        if self.live_engine:
            self.on_toggle_live(force_stop=True)

        try:
            symbol = self.symbol_var.get().strip().upper()
            if not symbol:
                raise ValueError("Symbol is empty")
            start = parse_utc(self.start_var.get())
            end = parse_utc(self.end_var.get())
            if end <= start:
                raise ValueError("End must be after Start")
            # Validate range against Binance server time to avoid confusing HTTP 400 errors.
            # Tick backtests (aggTrades) cannot fetch future data.
            srv = _get_data_binance().binance_fapi_server_time_utc()
            if srv is not None:
                # allow a tiny clock skew
                if end > srv + pd.Timedelta(seconds=2):
                    raise ValueError(f"End (UTC) is in the future. Binance server time is {srv.strftime('%Y-%m-%d %H:%M:%S')} UTC")
                if start > srv + pd.Timedelta(seconds=2):
                    raise ValueError(f"Start (UTC) is in the future. Binance server time is {srv.strftime('%Y-%m-%d %H:%M:%S')} UTC")
            warmup = int(self.warmup_var.get())
            cache_dir = self.cache_var.get().strip() or "data_cache"
            tfs = [tf for tf, v in self.tf_vars.items() if v.get()]
            if not tfs:
                raise ValueError("Select at least one timeframe")
            if "1m" not in tfs:
                tfs = ["1m"] + tfs

            # Backtest evaluation timeframe (decisions executed only on this TF closes)
            try:
                bt_step_tf = str(getattr(self, "bt_step_tf_var", None).get() if hasattr(self, "bt_step_tf_var") else "1m").strip()
            except Exception:
                bt_step_tf = "1m"
            if bt_step_tf and (bt_step_tf not in tfs):
                tfs.append(bt_step_tf)

            init_bal = float(self.bt_init_balance_var.get())
            notional = float(self.bt_notional_var.get())
            fee_pct = float(self.bt_fee_pct_var.get())
            slip_bps = float(self.bt_slippage_bps_var.get())
            stop_loss_value = float(self.bt_stop_loss_pct_var.get())
            take_profit_value = float(self.bt_take_profit_pct_var.get())
            stop_loss_mode = str(self.bt_stop_loss_mode_var.get() or "PERCENT").strip().upper()
            take_profit_mode = str(self.bt_take_profit_mode_var.get() or "PERCENT").strip().upper()
            stop_loss_tf = str(self.bt_stop_loss_tf_var.get() or "1m").strip()
            take_profit_tf = str(self.bt_take_profit_tf_var.get() or "1m").strip()
            stop_loss_field = str(self.bt_stop_loss_field_var.get() or "").strip()
            take_profit_field = str(self.bt_take_profit_field_var.get() or "").strip()
            stop_loss_offset_pct = float(self.bt_stop_loss_offset_pct_var.get())
            take_profit_offset_pct = float(self.bt_take_profit_offset_pct_var.get())
            allow_rev = bool(self.bt_reverse_var.get())
            skip_both = bool(self.bt_skip_both_var.get())
            if init_bal <= 0 or notional <= 0:
                raise ValueError("Initial balance and order notional must be > 0")
            if fee_pct < 0 or slip_bps < 0 or stop_loss_value < 0 or take_profit_value < 0 or stop_loss_offset_pct < 0 or take_profit_offset_pct < 0:
                raise ValueError("Fee, slippage, stop/target values, and field adjustments cannot be negative")

            cfg = BacktestConfig(
                initial_balance=init_bal,
                order_notional_usdt=notional,
                fee_rate=fee_pct / 100.0,
                slippage_bps=slip_bps,
                stop_loss_pct=(stop_loss_value if stop_loss_mode == "PERCENT" else 0.0),
                take_profit_pct=(take_profit_value if take_profit_mode == "PERCENT" else 0.0),
                stop_loss_mode=stop_loss_mode,
                stop_loss_value=stop_loss_value,
                stop_loss_timeframe=stop_loss_tf,
                stop_loss_field=stop_loss_field,
                stop_loss_offset_pct=stop_loss_offset_pct,
                take_profit_mode=take_profit_mode,
                take_profit_value=take_profit_value,
                take_profit_timeframe=take_profit_tf,
                take_profit_field=take_profit_field,
                take_profit_offset_pct=take_profit_offset_pct,
                allow_reverse=allow_rev,
                skip_if_both_entries=skip_both,
            )
            try:
                cfg.step_timeframe = bt_step_tf
            except Exception:
                pass
            try:
                cfg.tick_auto_download = bool(self.tick_autodownload_var.get())
            except Exception:
                pass

            bt_req_spec = _compile_stream_requirements(
                self.rules,
                entry_filters=self.entry_filters,
                backtest_cfg=cfg,
                include_plot_defaults=True,
            )
            try:
                req = required_warmup_minutes(tfs, required_fields=bt_req_spec)
                if req and warmup < req:
                    old_w = warmup
                    warmup = int(req)
                    try:
                        self.warmup_var.set(warmup)
                    except Exception:
                        pass
                    try:
                        max_tf = max(tfs, key=lambda _tf: interval_to_ms(_tf))
                    except Exception:
                        max_tf = "?"
                    try:
                        self.status_var.set(f"Warmup increased: {old_w} → {warmup} min (needed for active indicators on {max_tf}).")
                    except Exception:
                        pass
            except Exception:
                pass

            mode = (getattr(self, "bt_mode_var", None).get() if hasattr(self, "bt_mode_var") else BACKTEST_MODES[0])

            # Remember last backtest inputs (for report save/load and on-demand Trade Inspector fetch)
            self._bt_last_symbol = symbol
            self._bt_last_cache_dir = cache_dir
            self._bt_last_price_source = str(self.price_source_var.get() or "LAST").upper()
            self._bt_last_macd_impl = str(self.macd_impl_var.get() or "TRADINGVIEW").upper()
            try:
                self._bt_last_adx_impl = str(self.adx_impl_var.get() or "TRADINGVIEW").upper()
            except Exception:
                self._bt_last_adx_impl = "TRADINGVIEW"
            self._bt_last_mode = str(mode)
            self._bt_last_tfs = list(tfs)
            self._bt_last_warmup = int(warmup)

        except Exception as e:
            messagebox.showerror("Input error", str(e))
            return

        self.btn_fetch.config(state="disabled")
        self.btn_live.config(state="disabled")
        self.btn_backtest.config(state="disabled")
        try:
            self.btn_replay_backtest.config(state="disabled")
        except Exception:
            pass
        try:
            self.btn_tick_manager.config(state="disabled")
        except Exception:
            pass
        self.btn_backtest.config(state="disabled")
        try:
            self.btn_replay_backtest.config(state="disabled")
        except Exception:
            pass
        try:
            self.btn_tick_manager.config(state="disabled")
        except Exception:
            pass
        self.status_var.set("Starting backtest…")
        self.progress.config(mode="indeterminate")
        self.progress.start(10)
        self._progress_started = True

        replay_request_matches_last = (
            self.bt_result is not None
            and str(mode or "") != str(BACKTEST_MODE_TICK)
            and str(getattr(self, "_bt_last_symbol", "") or "").upper() == str(symbol or "").upper()
            and str(getattr(self, "_bt_last_mode", "") or "") == str(mode or "")
            and list(getattr(self, "_bt_last_tfs", []) or []) == list(tfs or [])
            and int(getattr(self, "_bt_last_warmup", 0) or 0) == int(warmup)
            and str(getattr(self, "_bt_last_price_source", "LAST") or "LAST").upper() == str(self.price_source_var.get() or "LAST").upper()
            and str(getattr(self, "_bt_last_macd_impl", "TRADINGVIEW") or "TRADINGVIEW").upper() == str(self.macd_impl_var.get() or "TRADINGVIEW").upper()
            and str(getattr(self, "_bt_last_adx_impl", "TRADINGVIEW") or "TRADINGVIEW").upper() == str(self.adx_impl_var.get() or "TRADINGVIEW").upper()
        )
        replay_plan = build_backtest_replay_plan(
            self.bt_result,
            next_cfg=cfg,
            symbol=symbol,
            start=start,
            end=end,
            rules_model=self.rules,
            tab_group_join_mode=self.tab_group_join_mode,
            group_rule_join_mode=self.group_rule_join_mode,
            entry_filters=self.entry_filters,
        ) if replay_request_matches_last else None
        capture_trade_details, equity_curve_stride = self._backtest_runtime_options()

        def worker():
            try:
                timings: Dict[str, float] = {}
                started_at = time.perf_counter()
                market_th = self._market_state_thresholds_obj()
                price_source = str(self.price_source_var.get() or "LAST").upper()
                macd_impl = str(self.macd_impl_var.get() or "TRADINGVIEW").upper()
                adx_impl = str(self.adx_impl_var.get() or "TRADINGVIEW").upper()
                prepared_meta = {
                    "warmup_start": None,
                    "timeframes": list(tfs),
                    "price_source": price_source,
                    "macd_impl": macd_impl,
                    "adx_impl": adx_impl,
                    "required_fields": bt_req_spec,
                    "market_state_thresholds": market_th,
                    "prepared_streams_full": None,
                }
                if force_replay_only and not (replay_plan is not None and replay_plan.eligible):
                    raise ValueError("Quick replay is not available for the current changes.")

                if replay_plan is not None and replay_plan.eligible:
                    try:
                        self.set_progress("Replaying backtest with cached indicators…", 0)
                        replay_started = time.perf_counter()
                        res = replay_backtest_result(
                            self.bt_result,
                            cfg=cfg,
                            progress_cb=self.set_progress,
                            capture_trade_details=capture_trade_details,
                            equity_curve_stride=equity_curve_stride,
                        )
                        timings["replay_s"] = time.perf_counter() - replay_started
                        timings["total_s"] = time.perf_counter() - started_at
                        replay_ctx = extract_backtest_replay_context(res) or extract_backtest_replay_context(self.bt_result) or {}
                        streams_bt = replay_ctx.get("streams_full") or getattr(self, "bt_streams_full", None) or getattr(self, "hist_streams_full", None)
                        df_1m = replay_ctx.get("df_1m_full") or getattr(self, "hist_df_1m_full", None)
                        prepared_meta["warmup_start"] = start - pd.Timedelta(minutes=warmup)
                        prepared_meta["prepared_streams_full"] = streams_bt
                        self.q.put(("bt_done", res, streams_bt, symbol, start, end, df_1m, timings, prepared_meta))
                        return
                    except Exception:
                        if force_replay_only:
                            raise
                        self.set_progress("Replay failed; starting full rebuild…", 0)

                warmup_start = start - pd.Timedelta(minutes=warmup)
                prepared_meta["warmup_start"] = warmup_start

                prepared_started = time.perf_counter()
                prepared = self._find_prepared_dataset(
                    symbol=symbol,
                    start=warmup_start,
                    end=end,
                    timeframes=tfs,
                    price_source=price_source,
                    macd_impl=macd_impl,
                    adx_impl=adx_impl,
                    required_fields=bt_req_spec,
                    market_state_thresholds=market_th,
                )
                if prepared is not None:
                    self.set_progress("Reusing prepared dataset from memory…", 0)
                    df_1m = slice_df_1m_window(prepared.df_1m_full, warmup_start, end)
                    streams_full = slice_streams_window(prepared.streams_full, warmup_start, end, timeframes=(("1m",) + tuple(tfs or ())))
                    timings["prepared_reuse_s"] = time.perf_counter() - prepared_started
                    timings["fetch_s"] = 0.0
                    timings["indicators_s"] = 0.0
                else:
                    disk_started = time.perf_counter()
                    prepared_disk = self._find_prepared_dataset_on_disk(
                        cache_dir=cache_dir,
                        symbol=symbol,
                        start=warmup_start,
                        end=end,
                        timeframes=tfs,
                        price_source=price_source,
                        macd_impl=macd_impl,
                        adx_impl=adx_impl,
                        required_fields=bt_req_spec,
                        market_state_thresholds=market_th,
                    )
                    if prepared_disk is not None:
                        self.set_progress("Loading prepared dataset from disk…", 0)
                        df_1m = slice_df_1m_window(prepared_disk.df_1m_full, warmup_start, end)
                        streams_full = slice_streams_window(prepared_disk.streams_full, warmup_start, end, timeframes=(("1m",) + tuple(tfs or ())))
                        timings["prepared_reuse_s"] = time.perf_counter() - prepared_started
                        timings["prepared_disk_s"] = time.perf_counter() - disk_started
                        timings["fetch_s"] = 0.0
                        timings["indicators_s"] = 0.0
                    else:
                        timings["prepared_reuse_s"] = time.perf_counter() - prepared_started
                        timings["prepared_disk_s"] = time.perf_counter() - disk_started
                        # reuse history candles if possible, otherwise fetch.
                        df_1m = None
                        if self.hist_df_1m_full is not None and self.hist_symbol == symbol:
                            try:
                                min_ot = self.hist_df_1m_full["open_time"].min()
                                max_ct = self.hist_df_1m_full["close_time"].max()
                                if (min_ot is not None and max_ct is not None and min_ot <= warmup_start and max_ct >= end):
                                    df_1m = self.hist_df_1m_full
                            except Exception:
                                df_1m = None

                        if df_1m is None:
                            fetch_started = time.perf_counter()
                            df_1m = _get_data_binance().fetch_klines_1m_futures(
                                symbol, warmup_start, end, cache_dir, self.set_progress, price_source=price_source
                            )
                            timings["fetch_s"] = time.perf_counter() - fetch_started
                            if df_1m.empty:
                                raise ValueError("No candles returned (check dates/symbol).")
                        else:
                            timings["fetch_s"] = 0.0

                        self.set_progress("Computing indicators…", 0)
                        indicator_started = time.perf_counter()
                        streams_full = simulate_multitf_indicators(
                            df_1m,
                            tfs,
                            self.set_progress,
                            macd_impl=macd_impl,
                            adx_impl=adx_impl,
                            cache_dir=cache_dir,
                            symbol=symbol,
                            start_utc=warmup_start,
                            end_utc=end,
                            price_source=price_source,
                            required_fields=bt_req_spec,
                            market_state_thresholds=market_th,
                        )
                        timings["indicators_s"] = time.perf_counter() - indicator_started
                        persist_started = time.perf_counter()
                        persisted_path = self._persist_prepared_dataset(
                            cache_dir=cache_dir,
                            symbol=symbol,
                            start=warmup_start,
                            end=end,
                            timeframes=tfs,
                            price_source=price_source,
                            macd_impl=macd_impl,
                            adx_impl=adx_impl,
                            required_fields=bt_req_spec,
                            market_state_thresholds=market_th,
                            df_1m_full=df_1m,
                            streams_full=streams_full,
                        )
                        if persisted_path:
                            timings["prepared_persist_s"] = time.perf_counter() - persist_started

                streams_bt = streams_full
                prepared_meta["prepared_streams_full"] = streams_full

                if mode == BACKTEST_MODE_TICK:
                    # Tick backtest uses aggTrades (LAST price only)
                    self.set_progress("Running tick backtest (aggTrades)…", 0)
                    engine_started = time.perf_counter()
                    res = run_backtest_tick(
                        symbol=symbol,
                        df_1m_full=df_1m,
                        start=start,
                        end=end,
                        tfs=tfs,
                        rules_model=self.rules,
                        tab_group_join_mode=self.tab_group_join_mode,
                        group_rule_join_mode=self.group_rule_join_mode,
                        entry_filters=self.entry_filters,
                        cfg=cfg,
                        cache_dir=cache_dir,
                        progress_cb=self.set_progress,
                        streams_full=streams_full,
                        macd_impl=(self.macd_impl_var.get() or "TRADINGVIEW"),
                    )
                    timings["engine_s"] = time.perf_counter() - engine_started
                else:
                    # Bar-close backtests run on 1m candle closes.
                    if mode == BACKTEST_MODE_BAR_1M_HTF_CLOSED_ONLY:
                        # NEW: 1m-driven evaluation but higher-TF snapshots only update on TF close.
                        self.set_progress("Preparing HTF closed-only streams…", 0)
                        streams_bt = make_streams_htf_closed_only(streams_full)

                    engine_started = time.perf_counter()
                    res = run_backtest(
                        symbol=symbol,
                        streams_full=streams_bt,
                        df_1m_full=df_1m,
                        start=start,
                        end=end,
                        rules_model=self.rules,
                        tab_group_join_mode=self.tab_group_join_mode,
                        group_rule_join_mode=self.group_rule_join_mode,
                        entry_filters=self.entry_filters,
                        cfg=cfg,
                        progress_cb=self.set_progress,
                        capture_trade_details=capture_trade_details,
                        equity_curve_stride=equity_curve_stride,
                    )
                    timings["engine_s"] = time.perf_counter() - engine_started
                timings["total_s"] = time.perf_counter() - started_at
                self.q.put(("bt_done", res, streams_bt, symbol, start, end, df_1m, timings, prepared_meta))
            except Exception:
                self.q.put(("error", traceback.format_exc()))

        # IMPORTANT: run backtest in a background thread so the UI stays responsive
        # (this mirrors the History flow).
        self.worker = threading.Thread(target=worker, daemon=True)
        self.worker.start()


    # ---------- History ----------
    def on_run_history(self):
        if self.live_engine:
            self.on_toggle_live(force_stop=True)

        try:
            symbol = self.symbol_var.get().strip().upper()
            if not symbol:
                raise ValueError("Symbol is empty")
            start = parse_utc(self.start_var.get())
            end = parse_utc(self.end_var.get())
            if end <= start:
                raise ValueError("End must be after Start")
            warmup = int(self.warmup_var.get())
            cache_dir = self.cache_var.get().strip() or "data_cache"
            tfs = [tf for tf, v in self.tf_vars.items() if v.get()]
            if not tfs:
                raise ValueError("Select at least one timeframe")
            if "1m" not in tfs:
                tfs = ["1m"] + tfs

            # Backtest evaluation timeframe (decisions executed only on this TF closes)
            try:
                bt_step_tf = str(getattr(self, "bt_step_tf_var", None).get() if hasattr(self, "bt_step_tf_var") else "1m").strip()
            except Exception:
                bt_step_tf = "1m"
            if bt_step_tf and (bt_step_tf not in tfs):
                tfs.append(bt_step_tf)

            hist_req_spec = _compile_stream_requirements(self.rules, entry_filters=self.entry_filters, include_plot_defaults=True)
            try:
                req = required_warmup_minutes(tfs, required_fields=hist_req_spec)
                if req and warmup < req:
                    old_w = warmup
                    warmup = int(req)
                    try:
                        self.warmup_var.set(warmup)
                    except Exception:
                        pass
                    try:
                        max_tf = max(tfs, key=lambda _tf: interval_to_ms(_tf))
                    except Exception:
                        max_tf = "?"
                    try:
                        self.status_var.set(f"Warmup increased: {old_w} → {warmup} min (needed for active indicators on {max_tf}).")
                    except Exception:
                        pass
            except Exception:
                pass
        except Exception as e:
            messagebox.showerror("Input error", str(e))
            return

        self.btn_fetch.config(state="disabled")
        self.btn_live.config(state="disabled")
        self.btn_backtest.config(state="disabled")
        try:
            self.btn_replay_backtest.config(state="disabled")
        except Exception:
            pass
        try:
            self.btn_tick_manager.config(state="disabled")
        except Exception:
            pass
        self.status_var.set("Starting history…")
        self.progress.config(mode="indeterminate")
        self.progress.start(10)
        self._progress_started = True

        for tab_id in self.hist_nb.tabs():
            self.hist_nb.forget(tab_id)
        ph = ttk.Frame(self.hist_nb)
        ttk.Label(ph, text="Loading…", padding=20).pack()
        self.hist_nb.add(ph, text="loading")

        def worker():
            try:
                warmup_start = start - pd.Timedelta(minutes=warmup)
                market_th = self._market_state_thresholds_obj()
                price_source = str(self.price_source_var.get() or "LAST").upper()
                macd_impl = str(self.macd_impl_var.get() or "TRADINGVIEW").upper()
                adx_impl = str(self.adx_impl_var.get() or "TRADINGVIEW").upper()
                prepared = self._find_prepared_dataset(
                    symbol=symbol,
                    start=warmup_start,
                    end=end,
                    timeframes=tfs,
                    price_source=price_source,
                    macd_impl=macd_impl,
                    adx_impl=adx_impl,
                    required_fields=hist_req_spec,
                    market_state_thresholds=market_th,
                )
                if prepared is not None:
                    self.set_progress("Reusing prepared dataset from memory…", 0)
                    df_1m = slice_df_1m_window(prepared.df_1m_full, warmup_start, end)
                    streams_full = slice_streams_window(prepared.streams_full, warmup_start, end, timeframes=(("1m",) + tuple(tfs or ())))
                else:
                    prepared_disk = self._find_prepared_dataset_on_disk(
                        cache_dir=cache_dir,
                        symbol=symbol,
                        start=warmup_start,
                        end=end,
                        timeframes=tfs,
                        price_source=price_source,
                        macd_impl=macd_impl,
                        adx_impl=adx_impl,
                        required_fields=hist_req_spec,
                        market_state_thresholds=market_th,
                    )
                    if prepared_disk is not None:
                        self.set_progress("Loading prepared dataset from disk…", 0)
                        df_1m = slice_df_1m_window(prepared_disk.df_1m_full, warmup_start, end)
                        streams_full = slice_streams_window(prepared_disk.streams_full, warmup_start, end, timeframes=(("1m",) + tuple(tfs or ())))
                    else:
                        df_1m = _get_data_binance().fetch_klines_1m_futures(
                            symbol, warmup_start, end, cache_dir, self.set_progress, price_source=price_source
                        )
                        if df_1m.empty:
                            raise ValueError("No candles returned (check dates/symbol).")

                        self.set_progress("Computing indicators…", 0)
                        streams_full = simulate_multitf_indicators(
                            df_1m,
                            tfs,
                            self.set_progress,
                            macd_impl=macd_impl,
                            adx_impl=adx_impl,
                            cache_dir=cache_dir,
                            symbol=symbol,
                            start_utc=warmup_start,
                            end_utc=end,
                            price_source=price_source,
                            required_fields=hist_req_spec,
                            market_state_thresholds=market_th,
                        )
                        self._persist_prepared_dataset(
                            cache_dir=cache_dir,
                            symbol=symbol,
                            start=warmup_start,
                            end=end,
                            timeframes=tfs,
                            price_source=price_source,
                            macd_impl=macd_impl,
                            adx_impl=adx_impl,
                            required_fields=hist_req_spec,
                            market_state_thresholds=market_th,
                            df_1m_full=df_1m,
                            streams_full=streams_full,
                        )
                cropped = {tf: dft.loc[(dft.index >= start) & (dft.index <= end)].copy()
                           for tf, dft in streams_full.items()}

                prepared_meta = {
                    "warmup_start": warmup_start,
                    "timeframes": list(tfs),
                    "price_source": price_source,
                    "macd_impl": macd_impl,
                    "adx_impl": adx_impl,
                    "required_fields": hist_req_spec,
                    "market_state_thresholds": market_th,
                }
                self.q.put(("hist_done", symbol, start, end, cropped, df_1m, streams_full, prepared_meta))
            except Exception:
                self.q.put(("error", traceback.format_exc()))

        self.worker = threading.Thread(target=worker, daemon=True)
        self.worker.start()

    def _render_history(self, symbol: str, start: "pd.Timestamp", end: "pd.Timestamp", streams: Dict[str, "pd.DataFrame"]):
        for tab_id in self.hist_nb.tabs():
            self.hist_nb.forget(tab_id)

        for tf, dft in streams.items():
            tab = ttk.Frame(self.hist_nb)
            self.hist_nb.add(tab, text=tf)

            ttk.Label(
                tab,
                text=f"{symbol} | {start.strftime('%Y-%m-%d %H:%M:%S')} → {end.strftime('%Y-%m-%d %H:%M:%S')} (UTC)",
                padding=(8, 6)
            ).pack(side="top", anchor="w")

            fig = build_figure(tf, dft)
            canvas = FigureCanvasTkAgg(fig, master=tab)
            canvas.draw()
            canvas.get_tk_widget().pack(side="top", fill="both", expand=True)

            toolbar = NavigationToolbar2Tk(canvas, tab)
            toolbar.update()
            toolbar.pack(side="bottom", fill="x")


    def _update_status_line(self):
        """
        Lightweight status helper.

        We keep the short guidance text (builder_status) useful and also avoid
        older revisions crashing when this method was missing.
        """
        try:
            tab = getattr(self, "active_tab", RULE_TABS[0])
            lbl = None
            try:
                lbl = self.tab_status_labels.get(tab)
            except Exception:
                lbl = None
            status_txt = lbl.cget("text") if lbl is not None else ""
            if hasattr(self, "builder_status") and isinstance(self.builder_status, tk.StringVar):
                # Keep it short; the per-tab label is the authoritative indicator.
                self.builder_status.set(f"{tab} — {status_txt}   (Right-click live cells to add rules. Right-click an EVENT chip to toggle ★ Trigger.)")
        except Exception:
            pass

    # ---------- Live ----------
    def _stop_live_engine(self):
        if self.live_engine:
            try:
                self.live_engine.stop()
            except Exception:
                pass
            self.live_engine = None

    def _start_live_engine(self):
        try:
            symbol = self.symbol_var.get().strip().upper()
            if not symbol:
                raise ValueError("Symbol is empty")
            tfs = [tf for tf, v in self.tf_vars.items() if v.get()]
            if not tfs:
                raise ValueError("Select at least one timeframe")

            price_source = (self.price_source_var.get() or "LAST").upper()
            macd_impl = (self.macd_impl_var.get() or "TRADINGVIEW").upper()
            adx_impl = (self.adx_impl_var.get() or "TRADINGVIEW").upper()
            live_forming = bool(self.live_forming_var.get())
        except Exception as e:
            messagebox.showerror("Input error", str(e))
            return

        try:
            self._ensure_live_breakout_alert_built()
        except Exception:
            pass
        try:
            self._ensure_live_signal_table_built()
        except Exception:
            pass

        # reset display
        live_cells = getattr(self, 'live_cells', {}) or {}
        for tf in ALL_TIMEFRAMES:
            row = live_cells.get(tf) or {}
            for c, lbl in row.items():
                lbl.config(text=(tf if c == "TF" else "-"), fg=(C_TEXT if c == "TF" else C_DIM))
            self.latest[tf] = {}
            self.prev_latest[tf] = {}
            self.latest_closed[tf] = {}
            self._range_event_last_bar_ms[tf] = {"buy": None, "sell": None}
            self._range_event_meta[tf] = {"last_seen_bar_ms": None, "regime": 0, "current_bar_buy_seen": False, "current_bar_sell_seen": False}

        try:
            self._reset_market_state_cards()
        except Exception:
            pass

        # Clear trigger latches when (re)starting live, then seed from new prefetched history.
        for tab_name in RULE_TABS:
            n_groups = len(self.rules.get(tab_name, []))
            self.group_latch[tab_name] = [None for _ in range(n_groups)]
            self.group_latch_meta[tab_name] = [None for _ in range(n_groups)]
        self._latch_seed_dirty = True

        self.status_var.set("Starting live… (continuous)")
        self.btn_live.config(text="Stop Live")

        try:
            self.live_engine = _get_live_engine_cls()(
                self.q,
                symbol=symbol,
                timeframes=tfs,
                price_source=price_source,
                macd_impl=macd_impl,
                adx_impl=adx_impl,
                live_use_forming=live_forming,
            )
            self.live_engine.start()
        except Exception as e:
            self.live_engine = None
            self.btn_live.config(text="Start Live (Now)")
            self.status_var.set("Live start failed.")
            try:
                self._live_log_append(f"[Live] Start failed: {e}")
                self._live_log_append(traceback.format_exc())
            except Exception:
                pass
            try:
                messagebox.showerror("Live start failed", f"Could not start live engine.\n\n{e}")
            except Exception:
                pass
            return

    def _restart_live_if_running(self, reason: str = "Restarting"):
        if not self.live_engine:
            return
        try:
            if not self.live_engine.running:
                return
        except Exception:
            return

        # Avoid restart storms: tiny debounce via timestamp
        now = time.time()
        last = getattr(self, "_last_live_restart_ts", 0.0)
        if (now - last) < 0.7:
            return
        self._last_live_restart_ts = now

        try:
            self._live_log_append(f"[Live] {reason} → restarting with new settings…")
        except Exception:
            pass

        self._stop_live_engine()
        self._start_live_engine()

    def on_toggle_live(self, force_stop: bool = False):
        if self.live_engine or force_stop:
            self._stop_live_engine()
            self.btn_live.config(text="Start Live (Now)")
            self.status_var.set("Live stopped.")
            return
        self._start_live_engine()

    # ---------- Queue loop ----------

    def _set_live_cell(self, row: Dict[str, tk.Label], col: str, text: str, fg: str) -> None:
        lbl = row.get(col)
        if lbl is None:
            return
        key = (row.get("TF").cget("text") if row.get("TF") is not None else "?", col)
        new_val = (str(text), str(fg))
        if self._live_cell_cache.get(key) == new_val:
            return
        try:
            lbl.config(text=text, fg=fg)
            self._live_cell_cache[key] = new_val
        except Exception:
            pass

    def _queue_live_price_render(self, price: Any, age_sec: Any) -> None:
        try:
            self._live_price_last_value = float(price)
        except Exception:
            self._live_price_last_value = None
        try:
            self._live_price_last_age_sec = float(age_sec)
        except Exception:
            self._live_price_last_age_sec = None
        if getattr(self, "_live_price_job", None) is not None:
            return
        try:
            self._live_price_job = self.after(LIVE_UI_RENDER_MS, self._render_live_price)
        except Exception:
            self._live_price_job = None

    def _render_live_price(self) -> None:
        self._live_price_job = None
        val = getattr(self, "_live_price_last_value", None)
        age_sec = getattr(self, "_live_price_last_age_sec", None)
        if hasattr(self, "live_price_var"):
            try:
                self.live_price_var.set(f"{float(val):,.2f}" if val is not None and np.isfinite(float(val)) else "--")
            except Exception:
                self.live_price_var.set("--")
        if hasattr(self, "live_price_meta_var"):
            try:
                age_txt = f"Age {float(age_sec):.1f}s" if age_sec is not None and np.isfinite(float(age_sec)) else "Age -"
            except Exception:
                age_txt = "Age -"
            try:
                self.live_price_meta_var.set(age_txt + "  |  slower UI refresh for smoother rendering")
            except Exception:
                pass

    def _queue_live_status_render(self, tf: str, price: Any, age_sec: Any, payload: Optional[Dict[str, Any]]) -> None:
        try:
            self._live_status_pending[str(tf)] = (float(price), float(age_sec), payload if isinstance(payload, dict) else payload)
        except Exception:
            self._live_status_pending[str(tf)] = (price, age_sec, payload if isinstance(payload, dict) else payload)
        self._queue_live_price_render(price, age_sec)
        if getattr(self, "_live_status_render_job", None) is not None:
            return
        try:
            self._live_status_render_job = self.after(LIVE_UI_RENDER_MS, self._flush_live_status_render)
        except Exception:
            self._live_status_render_job = None

    def _flush_live_status_render(self) -> None:
        self._live_status_render_job = None
        pending = dict(getattr(self, "_live_status_pending", {}) or {})
        self._live_status_pending.clear()
        for tf in ALL_TIMEFRAMES:
            item = pending.get(tf)
            if item is None:
                continue
            try:
                price, age_sec, payload = item
                self._apply_live_status_update(tf, price, age_sec, payload)
            except Exception:
                pass

    # ---------- Live update throttling ----------
    def _schedule_live_rule_update(self):
        """Coalesce many live updates into a single rule evaluation.

        Calling _update_rule_statuses() for every websocket / compute tick can freeze Tk.
        We schedule a short delayed update instead.
        """
        try:
            self._live_rule_dirty = True
            if getattr(self, "_live_rule_update_job", None) is not None:
                return
            # 50ms is responsive while keeping the UI smooth.
            self._live_rule_update_job = self.after(50, self._run_live_rule_update)
        except Exception:
            # As a fallback, try updating immediately.
            try:
                self._update_rule_statuses()
            except Exception:
                pass

    def _run_live_rule_update(self):
        try:
            self._live_rule_update_job = None
            if not getattr(self, "_live_rule_dirty", False):
                return
            self._live_rule_dirty = False
            self._update_rule_statuses()
        except Exception:
            # Never crash Tk's main loop
            pass


    def _apply_closed_snapshot(self, tf: str, payload: Dict[str, Any]) -> None:
        """Apply a CLOSED-bar snapshot (stable at candle close) into rule-history."""
        if payload is None:
            return
        try:
            bar_ms = payload.get("bar_open_time_ms", None)
            if bar_ms is None:
                bar_ms = payload.get("bar_ms", None)
            try:
                bar_ms_i = int(bar_ms) if bar_ms is not None else None
            except Exception:
                bar_ms_i = None

            try:
                snap_time = pd.to_datetime(bar_ms_i, unit="ms", utc=True) if bar_ms_i is not None else pd.Timestamp.now(tz="UTC")
            except Exception:
                snap_time = pd.Timestamp.now(tz="UTC")

            snap = dict(payload)
            snap["time"] = snap_time
            snap["bar_ms"] = bar_ms_i
            snap["is_closed"] = True
            self.latest_closed[tf] = self._apply_market_state_thresholds_to_snapshot(snap, prev_state=self.latest_closed.get(tf))
            snap = self.latest_closed[tf]
            try:
                self._update_range_event_memory(tf, snap, bar_ms_override=bar_ms_i)
            except Exception:
                pass

            try:
                self._rule_eval_ctx.observe_bars({tf: snap})
            except Exception:
                pass
        except Exception:
            pass

    def _schedule_closed_seed_drain(self):
        try:
            if getattr(self, "_pending_closed_seed_job", None) is not None:
                return
            self._pending_closed_seed_job = self.after(1, self._drain_closed_seed)
        except Exception:
            pass

    def _refresh_live_range_age_cells(self, tf: str) -> None:
        try:
            row = self.live_cells.get(tf)
        except Exception:
            row = None
        if not row:
            return
        try:
            payload = self.latest.get(tf, {}) or {}
        except Exception:
            payload = {}
        if not payload:
            return
        try:
            bar_open_ms = payload.get("bar_open_time_ms")
            if bar_open_ms is None:
                bar_open_ms = payload.get("bar_ms")
            bar_open_ms = int(bar_open_ms) if bar_open_ms is not None else None
        except Exception:
            bar_open_ms = None
        if bar_open_ms is None:
            return
        try:
            buy_now = bool(payload.get("range_filter_buy", False))
        except Exception:
            buy_now = False
        try:
            sell_now = bool(payload.get("range_filter_sell", False))
        except Exception:
            sell_now = False
        try:
            buy_age_txt, buy_age_fg = self._format_range_event_age_cell(tf, bar_open_ms, "buy", buy_now)
            sell_age_txt, sell_age_fg = self._format_range_event_age_cell(tf, bar_open_ms, "sell", sell_now)
            row["RBuyAge"].config(text=buy_age_txt, fg=buy_age_fg)
            row["RSellAge"].config(text=sell_age_txt, fg=sell_age_fg)
        except Exception:
            pass

    def _drain_closed_seed(self):
        """Drain seeded CLOSED-bar history in small chunks (prevents UI freeze)."""
        self._pending_closed_seed_job = None
        start_t = time.perf_counter()
        max_sec = 0.02  # 20ms per drain
        max_snaps = 120
        done = 0
        try:
            for tf, dq in list(self._pending_closed_seed.items()):
                while dq and done < max_snaps and (time.perf_counter() - start_t) < max_sec:
                    payload = dq.popleft()
                    self._apply_closed_snapshot(tf, payload)
                    done += 1
        except Exception:
            pass

        if done > 0:
            try:
                for tf in list(getattr(self, "_pending_closed_seed", {}).keys()):
                    self._refresh_live_range_age_cells(tf)
            except Exception:
                pass
            self._schedule_live_rule_update()

        # Continue draining if anything remains.
        try:
            if any(len(dq) > 0 for dq in self._pending_closed_seed.values()):
                self._schedule_closed_seed_drain()
        except Exception:
            pass


    def _poll_queue(self):
        start_t = time.perf_counter()
        processed = 0
        max_items = 250
        max_sec = 0.04  # ~40ms time budget per poll to keep Tk responsive
        try:
            while processed < max_items and (time.perf_counter() - start_t) < max_sec:
                item = self.q.get_nowait()
                processed += 1
                kind = item[0]

                if kind == "progress":
                    _, msg, pct = item
                    self.status_var.set(msg)

                    # pct can be None (indeterminate progress). Never crash the UI thread.
                    if pct is None:
                        if not self._progress_started:
                            try:
                                self.progress.config(mode="indeterminate")
                                self.progress.start(10)
                                self._progress_started = True
                            except Exception:
                                pass
                        continue

                    # determinate progress
                    if self._progress_started:
                        try:
                            self.progress.stop()
                            self.progress.config(mode="determinate")
                        except Exception:
                            pass
                        self._progress_started = False

                    try:
                        p = float(pct)
                        self.progress["value"] = max(0, min(100, int(p)))
                    except Exception:
                        # ignore malformed progress values
                        pass

                elif kind == "error":
                    _, tb = item
                    print("\n--- ERROR ---\n", tb)
                    self.btn_fetch.config(state="normal")
                    self.btn_live.config(state="normal")
                    self.btn_backtest.config(state="normal")
                    try:
                        self.btn_replay_backtest.config(state="normal" if self.bt_result is not None else "disabled")
                    except Exception:
                        pass
                    if self._progress_started:
                        self.progress.stop()
                        self._progress_started = False
                    self.progress.config(mode="determinate")
                    self.progress["value"] = 0
                    self.status_var.set("Error.")
                    messagebox.showerror("Error", tb)

                elif kind == "hist_done":
                    prepared_meta = None
                    if len(item) >= 8:
                        _, symbol, start_ts, end_ts, cropped_streams, df_1m_full, streams_full, prepared_meta = item
                    else:
                        _, symbol, start_ts, end_ts, cropped_streams, df_1m_full, streams_full = item
                    self.hist_symbol = symbol
                    self.hist_start = start_ts
                    self.hist_end = end_ts
                    self.hist_df_1m_full = df_1m_full
                    self.hist_streams_full = streams_full
                    if isinstance(prepared_meta, dict):
                        self._remember_prepared_dataset(
                            symbol=symbol,
                            start=prepared_meta.get("warmup_start", start_ts),
                            end=end_ts,
                            timeframes=list(prepared_meta.get("timeframes") or []),
                            price_source=str(prepared_meta.get("price_source") or self.price_source_var.get() or "LAST"),
                            macd_impl=str(prepared_meta.get("macd_impl") or self.macd_impl_var.get() or "TRADINGVIEW"),
                            adx_impl=str(prepared_meta.get("adx_impl") or self.adx_impl_var.get() or "TRADINGVIEW"),
                            required_fields=prepared_meta.get("required_fields"),
                            market_state_thresholds=prepared_meta.get("market_state_thresholds"),
                            df_1m_full=df_1m_full,
                            streams_full=streams_full,
                        )

                    self._render_history(symbol, start_ts, end_ts, cropped_streams)

                    self.btn_fetch.config(state="normal")
                    self.btn_live.config(state="normal")
                    self.btn_backtest.config(state="normal")
                    try:
                        self.btn_replay_backtest.config(state="normal" if self.bt_result is not None else "disabled")
                    except Exception:
                        pass
                    if self._progress_started:
                        self.progress.stop()
                        self._progress_started = False
                    self.progress.config(mode="determinate")
                    self.progress["value"] = 100
                    self.status_var.set("History done. Data kept in memory for backtest step.")

                elif kind == "bt_done":
                    timings = {}
                    prepared_meta = None
                    if len(item) >= 9:
                        _, res, streams_full, symbol, start_ts, end_ts, df_1m_full, timings, prepared_meta = item
                    elif len(item) >= 8:
                        _, res, streams_full, symbol, start_ts, end_ts, df_1m_full, timings = item
                    else:
                        _, res, streams_full, symbol, start_ts, end_ts, df_1m_full = item

                    # Cache for reuse (same as history flow)
                    self.hist_symbol = symbol
                    self.hist_start = start_ts
                    self.hist_end = end_ts
                    self.hist_df_1m_full = df_1m_full
                    self.hist_streams_full = streams_full
                    self.bt_streams_full = streams_full
                    if isinstance(prepared_meta, dict):
                        self._remember_prepared_dataset(
                            symbol=symbol,
                            start=prepared_meta.get("warmup_start", start_ts),
                            end=end_ts,
                            timeframes=list(prepared_meta.get("timeframes") or []),
                            price_source=str(prepared_meta.get("price_source") or self.price_source_var.get() or "LAST"),
                            macd_impl=str(prepared_meta.get("macd_impl") or self.macd_impl_var.get() or "TRADINGVIEW"),
                            adx_impl=str(prepared_meta.get("adx_impl") or self.adx_impl_var.get() or "TRADINGVIEW"),
                            required_fields=prepared_meta.get("required_fields"),
                            market_state_thresholds=prepared_meta.get("market_state_thresholds"),
                            df_1m_full=df_1m_full,
                            streams_full=prepared_meta.get("prepared_streams_full") or streams_full,
                        )
                    try:
                        self._apply_market_state_thresholds_to_trade_result(res)
                    except Exception:
                        pass

                    self._render_backtest(res, streams_full)

                    self.btn_fetch.config(state="normal")
                    self.btn_live.config(state="normal")
                    self.btn_backtest.config(state="normal")
                    try:
                        self.btn_replay_backtest.config(state="normal" if self.bt_result is not None else "disabled")
                    except Exception:
                        pass
                    if self._progress_started:
                        self.progress.stop()
                        self._progress_started = False
                    self.progress.config(mode="determinate")
                    self.progress["value"] = 100
                    if timings:
                        parts = []
                        fetch_s = timings.get("fetch_s")
                        ind_s = timings.get("indicators_s")
                        engine_s = timings.get("engine_s")
                        replay_s = timings.get("replay_s")
                        prepared_reuse_s = timings.get("prepared_reuse_s")
                        prepared_disk_s = timings.get("prepared_disk_s")
                        prepared_persist_s = timings.get("prepared_persist_s")
                        total_s = timings.get("total_s")
                        if replay_s is not None:
                            parts.append(f"replay {replay_s:.2f}s")
                        else:
                            if fetch_s is not None:
                                parts.append(f"fetch {fetch_s:.2f}s")
                            if ind_s is not None:
                                parts.append(f"indicators {ind_s:.2f}s")
                            if prepared_reuse_s is not None and fetch_s == 0.0 and ind_s == 0.0:
                                parts.append("prepared cache")
                            if prepared_disk_s is not None and fetch_s == 0.0 and ind_s == 0.0 and prepared_disk_s > 0:
                                parts.append(f"prepared disk {prepared_disk_s:.2f}s")
                            if prepared_persist_s is not None:
                                parts.append(f"store {prepared_persist_s:.2f}s")
                            if engine_s is not None:
                                parts.append(f"engine {engine_s:.2f}s")
                        if total_s is not None:
                            parts.append(f"total {total_s:.2f}s")
                        self.status_var.set("Backtest done. " + " | ".join(parts))
                    else:
                        self.status_var.set("Backtest done.")

                elif kind == "live_log":
                    _, msg = item
                    self._live_log_append(msg)
                    try:
                        if "Prefill finished" in str(msg):
                            # Option B: seed FIRED from prefetched history.
                            self._latch_seed_dirty = True
                            self._seed_all_group_latches_from_history()
                            self._schedule_live_rule_update()
                    except Exception:
                        pass

                elif kind == "live_closed_seed":
                    _, tf, price, age_sec, payloads = item
                    if not payloads:
                        continue
                    try:
                        dq = self._pending_closed_seed.get(tf)
                        if dq is None:
                            dq = collections.deque()
                            self._pending_closed_seed[tf] = dq
                        dq.extend(list(payloads))
                        self._schedule_closed_seed_drain()
                    except Exception:
                        pass
                elif kind == "live_closed":
                    _, tf, price, age_sec, payload = item
                    if payload is None:
                        continue
                    self._apply_closed_snapshot(tf, payload)
                    self._schedule_live_rule_update()

                elif kind == "live_status":
                    _, tf, price, age_sec, payload = item
                    if tf not in self.live_cells:
                        continue
                    self._queue_live_price_render(price, age_sec)

                    # keep previous snapshot for event rules
                    prev_snapshot = self.latest.get(tf, {}).copy()
                    if prev_snapshot:
                        self.prev_latest[tf] = prev_snapshot

                    row = self.live_cells[tf]
                    set_cell = self._set_live_cell
                    try:
                        set_cell(row, "Age(s)", f"{float(age_sec):.1f}", (C_TEXT if float(age_sec) < 2 else C_RED))
                    except Exception:
                        set_cell(row, "Age(s)", "-", C_DIM)

                    if payload is None:
                        for col in ("MACD", "Signal", "M/S", "Angle", "SigAngle", "PPO", "FlipAge", "ADX", "ADXAngle", "ATR", "ATRAngle", "FRAMA", "VIDYA", "Range", "RBuyAge", "RSellAge", "+DI", "-DI"):
                            set_cell(row, col, "-", C_DIM)
                        # Keep a bar-aligned timestamp even if indicators aren't ready yet,
                        # so the rule engine's "bar clock" can still advance consistently.
                        try:
                            tf_ms = int(interval_to_ms(tf))
                            now_ms = int(pd.Timestamp.now(tz="UTC").timestamp() * 1000)
                            bar_open_ms = now_ms - (now_ms % tf_ms)
                            snap_time = pd.to_datetime(bar_open_ms, unit="ms", utc=True)
                        except Exception:
                            bar_open_ms = None
                            snap_time = pd.Timestamp.now(tz="UTC")
                        self.latest[tf] = self._apply_market_state_thresholds_to_snapshot({"time": snap_time, "bar_ms": bar_open_ms, "is_closed": False}, prev_state=self.latest.get(tf))
                        try:
                            self._update_market_state_card(tf, self.latest[tf])
                        except Exception:
                            pass
                    else:
                        macd = payload.get("macd")
                        sig = payload.get("signal")
                        ms = payload.get("ms")
                        ang = payload.get("angle")
                        sig_ang = payload.get("sig_angle")
                        ppo = payload.get("ppo")
                        fa = payload.get("flip_age")
                        adx = payload.get("adx")
                        adx_ang = payload.get("adx_angle")
                        atr = payload.get("atr")
                        atr_ang = payload.get("atr_angle")
                        frama_state = payload.get("frama_state")
                        frama_break_up = bool(payload.get("frama_break_up", False))
                        frama_break_down = bool(payload.get("frama_break_down", False))
                        frama_mid_cross = bool(payload.get("frama_mid_cross", False))
                        vidya_state = payload.get("vidya_state")
                        vidya_trend_up = bool(payload.get("vidya_trend_up", False))
                        vidya_trend_down = bool(payload.get("vidya_trend_down", False))
                        vidya_delta_pct = payload.get("vidya_delta_pct")
                        vidya_slope = payload.get("vidya_slope")
                        vidya_angle_deg = payload.get("vidya_angle_deg")
                        vidya_angle_deg_norm = payload.get("vidya_angle_deg_norm")
                        vidya_angle_state = payload.get("vidya_angle_state")
                        vidya_angle_accel = payload.get("vidya_angle_accel")
                        vidya_up_volume = payload.get("vidya_up_volume")
                        vidya_down_volume = payload.get("vidya_down_volume")
                        vidya_active_liq_low_count = payload.get("vidya_active_liq_low_count")
                        vidya_active_liq_high_count = payload.get("vidya_active_liq_high_count")
                        vidya_nearest_liq_low = payload.get("vidya_nearest_liq_low")
                        vidya_nearest_liq_high = payload.get("vidya_nearest_liq_high")
                        vidya_dist_to_liq_low_pct = payload.get("vidya_dist_to_liq_low_pct")
                        vidya_dist_to_liq_high_pct = payload.get("vidya_dist_to_liq_high_pct")
                        vidya_new_liq_low = bool(payload.get("vidya_new_liq_low", False))
                        vidya_new_liq_high = bool(payload.get("vidya_new_liq_high", False))
                        vidya_liq_low_taken = bool(payload.get("vidya_liq_low_taken", False))
                        vidya_liq_high_taken = bool(payload.get("vidya_liq_high_taken", False))
                        range_filter_state = payload.get("range_filter_state")
                        range_filter_phase = payload.get("range_filter_phase")
                        range_filter_buy = bool(payload.get("range_filter_buy", False))
                        range_filter_sell = bool(payload.get("range_filter_sell", False))
                        range_filter_line = payload.get("range_filter_line")
                        range_filter_upper = payload.get("range_filter_upper")
                        range_filter_lower = payload.get("range_filter_lower")
                        range_filter_smooth_range = payload.get("range_filter_smooth_range")
                        range_filter_upward_count = payload.get("range_filter_upward_count")
                        range_filter_downward_count = payload.get("range_filter_downward_count")
                        market_state = payload.get("market_state")
                        market_bias = payload.get("market_bias")
                        market_phase = payload.get("market_phase")
                        market_confidence = payload.get("market_confidence")
                        market_state_age = payload.get("market_state_age")
                        of_state = payload.get("of_state")
                        of_state_pretty = payload.get("of_state_pretty")
                        of_bias = payload.get("of_bias")
                        of_note = payload.get("of_note")
                        of_delta_pct = payload.get("of_delta_pct")
                        of_volume_burst = payload.get("of_volume_burst")
                        of_volume_burst_ratio = payload.get("of_volume_burst_ratio")
                        of_trade_burst = payload.get("of_trade_burst")
                        of_trade_burst_ratio = payload.get("of_trade_burst_ratio")
                        of_progress_bps = payload.get("of_progress_bps")
                        of_trade_rate = payload.get("of_trade_rate")
                        of_trade_count_short = payload.get("of_trade_count_short")
                        of_trade_count_long = payload.get("of_trade_count_long")
                        of_buy_qty_short = payload.get("of_buy_qty_short")
                        of_sell_qty_short = payload.get("of_sell_qty_short")
                        of_breakout_watch_long = bool(payload.get("of_breakout_watch_long", False))
                        of_breakout_watch_short = bool(payload.get("of_breakout_watch_short", False))
                        of_breakout_confirm_long = bool(payload.get("of_breakout_confirm_long", False))
                        of_breakout_confirm_short = bool(payload.get("of_breakout_confirm_short", False))
                        of_absorption_long = bool(payload.get("of_absorption_long", False))
                        of_absorption_short = bool(payload.get("of_absorption_short", False))
                        of_filter_score = payload.get("of_filter_score")
                        market_breakout_filter_gate = payload.get("market_breakout_filter_gate")
                        market_breakout_filter_pass = bool(payload.get("market_breakout_filter_pass", False))
                        market_breakout_filter_note = payload.get("market_breakout_filter_note")
                        of_window_short_sec = payload.get("of_window_short_sec")
                        of_window_long_sec = payload.get("of_window_long_sec")
                        of_dom_note = payload.get("of_dom_note")
                        of_dom_connected = bool(payload.get("of_dom_connected", False))
                        of_dom_state = payload.get("of_dom_state")
                        of_dom_state_pretty = payload.get("of_dom_state_pretty")
                        of_dom_bias = payload.get("of_dom_bias")
                        of_dom_pressure_pct = payload.get("of_dom_pressure_pct")
                        of_dom_pressure_accel = payload.get("of_dom_pressure_accel")
                        of_dom_top5_imbalance_pct = payload.get("of_dom_top5_imbalance_pct")
                        of_dom_top10_imbalance_pct = payload.get("of_dom_top10_imbalance_pct")
                        of_dom_spread_bps = payload.get("of_dom_spread_bps")
                        of_dom_bid_stack_ratio = payload.get("of_dom_bid_stack_ratio")
                        of_dom_ask_stack_ratio = payload.get("of_dom_ask_stack_ratio")
                        of_dom_bid_pull_ratio = payload.get("of_dom_bid_pull_ratio")
                        of_dom_ask_pull_ratio = payload.get("of_dom_ask_pull_ratio")
                        of_dom_breakout_watch_long = bool(payload.get("of_dom_breakout_watch_long", False))
                        of_dom_breakout_watch_short = bool(payload.get("of_dom_breakout_watch_short", False))
                        of_dom_breakout_confirm_long = bool(payload.get("of_dom_breakout_confirm_long", False))
                        of_dom_breakout_confirm_short = bool(payload.get("of_dom_breakout_confirm_short", False))
                        of_dom_updates_short = payload.get("of_dom_updates_short")
                        of_dom_updates_long = payload.get("of_dom_updates_long")
                        of_dom_best_bid = payload.get("of_dom_best_bid")
                        of_dom_best_ask = payload.get("of_dom_best_ask")
                        of_dom_mid = payload.get("of_dom_mid")
                        of_feature_mode = payload.get("of_feature_mode")
                        of_dom_source = payload.get("of_dom_source")
                        of_dom_sync_state = payload.get("of_dom_sync_state")
                        of_dom_resync_required = bool(payload.get("of_dom_resync_required", False))
                        of_dom_sync_note = payload.get("of_dom_sync_note")
                        of_dom_partial_fallback_enabled = bool(payload.get("of_dom_partial_fallback_enabled", False))
                        of_dom_partial_fallback_active = bool(payload.get("of_dom_partial_fallback_active", False))
                        of_dom_partial_connected = bool(payload.get("of_dom_partial_connected", False))
                        of_dom_partial_updates_total = payload.get("of_dom_partial_updates_total")
                        of_dom_local_l2_enabled = bool(payload.get("of_dom_local_l2_enabled", False))
                        of_dom_local_book_synced = bool(payload.get("of_dom_local_book_synced", False))
                        of_dom_local_snapshot_id = payload.get("of_dom_local_snapshot_id")
                        of_dom_local_last_event_u = payload.get("of_dom_local_last_event_u")
                        of_dom_local_last_event_pu = payload.get("of_dom_local_last_event_pu")
                        of_dom_local_buffered_events = payload.get("of_dom_local_buffered_events")
                        of_dom_local_updates_applied = payload.get("of_dom_local_updates_applied")
                        of_dom_book_levels_bid = payload.get("of_dom_book_levels_bid")
                        of_dom_book_levels_ask = payload.get("of_dom_book_levels_ask")
                        of_dom_depth_age_ms = payload.get("of_dom_depth_age_ms")
                        of_dom_snapshot_age_ms = payload.get("of_dom_snapshot_age_ms")
                        di_p = payload.get("di_plus")
                        di_m = payload.get("di_minus")
                        di_s = payload.get("di_spread")
                        try:
                            m_val = float(macd)
                        except Exception:
                            m_val = float("nan")
                        if not np.isfinite(m_val):
                            set_cell(row, "MACD", "-", C_DIM)
                        else:
                            set_cell(row, "MACD", f"{m_val:.1f}", C_TEXT)

                        try:
                            s_val = float(sig)
                        except Exception:
                            s_val = float("nan")
                        if not np.isfinite(s_val):
                            set_cell(row, "Signal", "-", C_DIM)
                        else:
                            set_cell(row, "Signal", f"{s_val:.1f}", C_TEXT)

                        # Dots: show a dot only when the state is known; otherwise show '-'
                        if ms in ("GREEN", "RED"):
                            set_cell(row, "M/S", DOT_CHAR, (C_GREEN if ms == "GREEN" else C_RED))
                        else:
                            set_cell(row, "M/S", "-", C_DIM)

                        if ang in ("GREEN", "RED"):
                            set_cell(row, "Angle", DOT_CHAR, (C_GREEN if ang == "GREEN" else C_RED))
                        else:
                            set_cell(row, "Angle", "-", C_DIM)

                        if sig_ang in ("GREEN", "RED"):
                            set_cell(row, "SigAngle", DOT_CHAR, (C_GREEN if sig_ang == "GREEN" else C_RED))
                        else:
                            set_cell(row, "SigAngle", "-", C_DIM)

                        if ppo in ("GREEN", "RED"):
                            set_cell(row, "PPO", DOT_CHAR, (C_GREEN if ppo == "GREEN" else C_RED))
                        else:
                            set_cell(row, "PPO", "-", C_DIM)

                        # FlipAge
                        set_cell(row, "FlipAge", str(fa) if fa is not None else "-", C_TEXT if fa is not None else C_DIM)

                        # ADX / DI (numeric)
                        try:
                            a_val = float(adx)
                        except Exception:
                            a_val = float("nan")
                        set_cell(row, "ADX", (f"{a_val:.1f}" if np.isfinite(a_val) else "-"), (C_TEXT if np.isfinite(a_val) else C_DIM))

                        # ADX angle (dot): rising/falling on closed bars
                        if adx_ang in ("GREEN", "RED"):
                            set_cell(row, "ADXAngle", DOT_CHAR, (C_GREEN if adx_ang == "GREEN" else C_RED))
                        else:
                            set_cell(row, "ADXAngle", "-", C_DIM)

                        # ATR (numeric)
                        try:
                            atr_val = float(atr)
                        except Exception:
                            atr_val = float("nan")
                        set_cell(row, "ATR", (f"{atr_val:.1f}" if np.isfinite(atr_val) else "-"), (C_TEXT if np.isfinite(atr_val) else C_DIM))

                        # ATR angle (dot): rising/falling; semantics depend on forming-candle setting (handled in engine)
                        if atr_ang in ("GREEN", "RED"):
                            set_cell(row, "ATRAngle", DOT_CHAR, (C_GREEN if atr_ang == "GREEN" else C_RED))
                        else:
                            set_cell(row, "ATRAngle", "-", C_DIM)

                        if frama_state == "BUY":
                            set_cell(row, "FRAMA", "BUY", C_GREEN)
                        elif frama_state == "SELL":
                            set_cell(row, "FRAMA", "SELL", C_RED)
                        elif frama_state == "NEUTRAL":
                            set_cell(row, "FRAMA", "NEUTRAL", C_DIM)
                        else:
                            set_cell(row, "FRAMA", "-", C_DIM)

                        if vidya_state == "BUY":
                            set_cell(row, "VIDYA", "BUY", C_GREEN)
                        elif vidya_state == "SELL":
                            set_cell(row, "VIDYA", "SELL", C_RED)
                        else:
                            set_cell(row, "VIDYA", "-", C_DIM)

                        if range_filter_state == "BUY":
                            set_cell(row, "Range", "BUY", C_GREEN)
                        elif range_filter_state == "SELL":
                            set_cell(row, "Range", "SELL", C_RED)
                        elif range_filter_state == "NEUTRAL":
                            set_cell(row, "Range", "NEUTRAL", C_DIM)
                        else:
                            set_cell(row, "Range", "-", C_DIM)

                        try:
                            bar_open_ms = payload.get("bar_open_time_ms")
                            if bar_open_ms is not None:
                                bar_open_ms = int(bar_open_ms)
                                snap_time = pd.to_datetime(bar_open_ms, unit="ms", utc=True)
                            else:
                                tf_ms = int(interval_to_ms(tf))
                                now_ms = int(pd.Timestamp.now(tz="UTC").timestamp() * 1000)
                                bar_open_ms = now_ms - (now_ms % tf_ms)
                                snap_time = pd.to_datetime(bar_open_ms, unit="ms", utc=True)
                        except Exception:
                            bar_open_ms = None
                            snap_time = pd.Timestamp.now(tz="UTC")

                        try:
                            self._update_range_event_memory(tf, payload, bar_ms_override=bar_open_ms)
                        except Exception:
                            pass
                        buy_age_txt, buy_age_fg = self._format_range_event_age_cell(tf, bar_open_ms, "buy", range_filter_buy)
                        sell_age_txt, sell_age_fg = self._format_range_event_age_cell(tf, bar_open_ms, "sell", range_filter_sell)
                        set_cell(row, "RBuyAge", buy_age_txt, buy_age_fg)
                        set_cell(row, "RSellAge", sell_age_txt, sell_age_fg)

                        try:
                            dp_val = float(di_p)
                        except Exception:
                            dp_val = float("nan")
                        set_cell(row, "+DI", (f"{dp_val:.1f}" if np.isfinite(dp_val) else "-"), (C_TEXT if np.isfinite(dp_val) else C_DIM))

                        try:
                            dm_val = float(di_m)
                        except Exception:
                            dm_val = float("nan")
                        set_cell(row, "-DI", (f"{dm_val:.1f}" if np.isfinite(dm_val) else "-"), (C_TEXT if np.isfinite(dm_val) else C_DIM))

                        # Snapshot for rule engine (bar_open_ms / snap_time resolved above)
                        self.latest[tf] = self._apply_market_state_thresholds_to_snapshot({
                            "price": (float(payload.get("price")) if payload.get("price") is not None and np.isfinite(float(payload.get("price"))) else None),
                            "macd": (m_val if np.isfinite(m_val) else None),
                            "signal": (s_val if np.isfinite(s_val) else None),
                            "ms": (ms if ms in ("GREEN", "RED") else None),
                            "angle": (ang if ang in ("GREEN", "RED") else None),
                            "sig_angle": (sig_ang if sig_ang in ("GREEN", "RED") else None),
                            "ppo": (ppo if ppo in ("GREEN", "RED") else None),
                            "flip_age": fa,
                            "adx": (a_val if np.isfinite(a_val) else None),
                            "atr": (atr_val if np.isfinite(atr_val) else None),
                            "adx_angle": (adx_ang if adx_ang in ("GREEN", "RED") else None),
                            "atr_angle": (atr_ang if atr_ang in ("GREEN", "RED") else None),
                            "frama": (float(payload.get("frama")) if payload.get("frama") is not None and np.isfinite(float(payload.get("frama"))) else None),
                            "frama_upper": (float(payload.get("frama_upper")) if payload.get("frama_upper") is not None and np.isfinite(float(payload.get("frama_upper"))) else None),
                            "frama_lower": (float(payload.get("frama_lower")) if payload.get("frama_lower") is not None and np.isfinite(float(payload.get("frama_lower"))) else None),
                            "frama_alpha": (float(payload.get("frama_alpha")) if payload.get("frama_alpha") is not None and np.isfinite(float(payload.get("frama_alpha"))) else None),
                            "frama_state": (frama_state if frama_state in ("BUY", "SELL", "NEUTRAL") else None),
                            "frama_break_up": bool(frama_break_up),
                            "frama_break_down": bool(frama_break_down),
                            "frama_mid_cross": bool(frama_mid_cross),
                            "vidya_state": (vidya_state if vidya_state in ("BUY", "SELL") else None),
                            "vidya_trend_up": bool(vidya_trend_up),
                            "vidya_trend_down": bool(vidya_trend_down),
                            "vidya_delta_pct": (float(vidya_delta_pct) if vidya_delta_pct is not None and np.isfinite(float(vidya_delta_pct)) else None),
                            "vidya_slope": (float(vidya_slope) if vidya_slope is not None and np.isfinite(float(vidya_slope)) else None),
                            "vidya_angle_deg": (float(vidya_angle_deg) if vidya_angle_deg is not None and np.isfinite(float(vidya_angle_deg)) else None),
                            "vidya_angle_deg_norm": (float(vidya_angle_deg_norm) if vidya_angle_deg_norm is not None and np.isfinite(float(vidya_angle_deg_norm)) else None),
                            "vidya_angle_state": (vidya_angle_state if vidya_angle_state in ("UP", "DOWN", "FLAT") else None),
                            "vidya_angle_accel": (vidya_angle_accel if vidya_angle_accel in ("ACCEL", "DECEL", "STABLE") else None),
                            "vidya_up_volume": (float(vidya_up_volume) if vidya_up_volume is not None and np.isfinite(float(vidya_up_volume)) else None),
                            "vidya_down_volume": (float(vidya_down_volume) if vidya_down_volume is not None and np.isfinite(float(vidya_down_volume)) else None),
                            "vidya_active_liq_low_count": (float(vidya_active_liq_low_count) if vidya_active_liq_low_count is not None and np.isfinite(float(vidya_active_liq_low_count)) else None),
                            "vidya_active_liq_high_count": (float(vidya_active_liq_high_count) if vidya_active_liq_high_count is not None and np.isfinite(float(vidya_active_liq_high_count)) else None),
                            "vidya_nearest_liq_low": (float(vidya_nearest_liq_low) if vidya_nearest_liq_low is not None and np.isfinite(float(vidya_nearest_liq_low)) else None),
                            "vidya_nearest_liq_high": (float(vidya_nearest_liq_high) if vidya_nearest_liq_high is not None and np.isfinite(float(vidya_nearest_liq_high)) else None),
                            "vidya_dist_to_liq_low_pct": (float(vidya_dist_to_liq_low_pct) if vidya_dist_to_liq_low_pct is not None and np.isfinite(float(vidya_dist_to_liq_low_pct)) else None),
                            "vidya_dist_to_liq_high_pct": (float(vidya_dist_to_liq_high_pct) if vidya_dist_to_liq_high_pct is not None and np.isfinite(float(vidya_dist_to_liq_high_pct)) else None),
                            "vidya_new_liq_low": bool(vidya_new_liq_low),
                            "vidya_new_liq_high": bool(vidya_new_liq_high),
                            "vidya_liq_low_taken": bool(vidya_liq_low_taken),
                            "vidya_liq_high_taken": bool(vidya_liq_high_taken),
                            "vidya_last_liq_low_taken_price": (float(payload.get("vidya_last_liq_low_taken_price")) if payload.get("vidya_last_liq_low_taken_price") is not None and np.isfinite(float(payload.get("vidya_last_liq_low_taken_price"))) else None),
                            "vidya_last_liq_high_taken_price": (float(payload.get("vidya_last_liq_high_taken_price")) if payload.get("vidya_last_liq_high_taken_price") is not None and np.isfinite(float(payload.get("vidya_last_liq_high_taken_price"))) else None),
                            "range_filter_state": (range_filter_state if range_filter_state in ("BUY", "SELL", "NEUTRAL") else None),
                            "range_filter_phase": (range_filter_phase if range_filter_phase in ("BUY_STRONG", "BUY_WEAK", "SELL_STRONG", "SELL_WEAK", "NEUTRAL") else None),
                            "range_filter_buy": bool(range_filter_buy),
                            "range_filter_sell": bool(range_filter_sell),
                            "range_filter_line": (float(range_filter_line) if range_filter_line is not None and np.isfinite(float(range_filter_line)) else None),
                            "range_filter_upper": (float(range_filter_upper) if range_filter_upper is not None and np.isfinite(float(range_filter_upper)) else None),
                            "range_filter_lower": (float(range_filter_lower) if range_filter_lower is not None and np.isfinite(float(range_filter_lower)) else None),
                            "range_filter_smooth_range": (float(range_filter_smooth_range) if range_filter_smooth_range is not None and np.isfinite(float(range_filter_smooth_range)) else None),
                            "range_filter_upward_count": (float(range_filter_upward_count) if range_filter_upward_count is not None and np.isfinite(float(range_filter_upward_count)) else None),
                            "range_filter_downward_count": (float(range_filter_downward_count) if range_filter_downward_count is not None and np.isfinite(float(range_filter_downward_count)) else None),
                            "market_state": (market_state if market_state in ("BULL_EXPANSION", "BULL_PULLBACK", "BULL_EXHAUSTION", "BEAR_EXPANSION", "BEAR_PULLBACK", "BEAR_EXHAUSTION", "TRANSITION", "COMPRESSION") else None),
                            "market_bias": (market_bias if market_bias in ("LONG", "SHORT", "NEUTRAL") else None),
                            "market_phase": (market_phase if market_phase in ("EXPANSION", "PULLBACK", "EXHAUSTION", "TRANSITION", "COMPRESSION") else None),
                            "market_confidence": (float(market_confidence) if market_confidence is not None and np.isfinite(float(market_confidence)) else None),
                            "market_state_age": (float(market_state_age) if market_state_age is not None and np.isfinite(float(market_state_age)) else None),
                            "of_state": (str(of_state) if of_state is not None else None),
                            "of_state_pretty": (str(of_state_pretty) if of_state_pretty is not None else None),
                            "of_bias": (str(of_bias) if of_bias is not None else None),
                            "of_note": (str(of_note) if of_note is not None else None),
                            "of_delta_pct": (float(of_delta_pct) if of_delta_pct is not None and np.isfinite(float(of_delta_pct)) else None),
                            "of_volume_burst": (float(of_volume_burst) if of_volume_burst is not None and np.isfinite(float(of_volume_burst)) else None),
                            "of_volume_burst_ratio": (float(of_volume_burst_ratio) if of_volume_burst_ratio is not None and np.isfinite(float(of_volume_burst_ratio)) else None),
                            "of_trade_burst": (float(of_trade_burst) if of_trade_burst is not None and np.isfinite(float(of_trade_burst)) else None),
                            "of_trade_burst_ratio": (float(of_trade_burst_ratio) if of_trade_burst_ratio is not None and np.isfinite(float(of_trade_burst_ratio)) else None),
                            "of_progress_bps": (float(of_progress_bps) if of_progress_bps is not None and np.isfinite(float(of_progress_bps)) else None),
                            "of_trade_rate": (float(of_trade_rate) if of_trade_rate is not None and np.isfinite(float(of_trade_rate)) else None),
                            "of_trade_count_short": (float(of_trade_count_short) if of_trade_count_short is not None and np.isfinite(float(of_trade_count_short)) else None),
                            "of_trade_count_long": (float(of_trade_count_long) if of_trade_count_long is not None and np.isfinite(float(of_trade_count_long)) else None),
                            "of_buy_qty_short": (float(of_buy_qty_short) if of_buy_qty_short is not None and np.isfinite(float(of_buy_qty_short)) else None),
                            "of_sell_qty_short": (float(of_sell_qty_short) if of_sell_qty_short is not None and np.isfinite(float(of_sell_qty_short)) else None),
                            "of_breakout_watch_long": bool(of_breakout_watch_long),
                            "of_breakout_watch_short": bool(of_breakout_watch_short),
                            "of_breakout_confirm_long": bool(of_breakout_confirm_long),
                            "of_breakout_confirm_short": bool(of_breakout_confirm_short),
                            "of_absorption_long": bool(of_absorption_long),
                            "of_absorption_short": bool(of_absorption_short),
                            "of_filter_score": (float(of_filter_score) if of_filter_score is not None and np.isfinite(float(of_filter_score)) else None),
                            "market_breakout_filter_gate": (str(market_breakout_filter_gate) if market_breakout_filter_gate is not None else None),
                            "market_breakout_filter_pass": bool(market_breakout_filter_pass),
                            "market_breakout_filter_note": (str(market_breakout_filter_note) if market_breakout_filter_note is not None else None),
                            "of_window_short_sec": (float(of_window_short_sec) if of_window_short_sec is not None and np.isfinite(float(of_window_short_sec)) else None),
                            "of_window_long_sec": (float(of_window_long_sec) if of_window_long_sec is not None and np.isfinite(float(of_window_long_sec)) else None),
                            "of_dom_note": (str(of_dom_note) if of_dom_note is not None else None),
                            "of_dom_connected": bool(of_dom_connected),
                            "of_dom_state": (str(of_dom_state) if of_dom_state is not None else None),
                            "of_dom_state_pretty": (str(of_dom_state_pretty) if of_dom_state_pretty is not None else None),
                            "of_dom_bias": (str(of_dom_bias) if of_dom_bias is not None else None),
                            "of_dom_pressure_pct": (float(of_dom_pressure_pct) if of_dom_pressure_pct is not None and np.isfinite(float(of_dom_pressure_pct)) else None),
                            "of_dom_pressure_accel": (float(of_dom_pressure_accel) if of_dom_pressure_accel is not None and np.isfinite(float(of_dom_pressure_accel)) else None),
                            "of_dom_top5_imbalance_pct": (float(of_dom_top5_imbalance_pct) if of_dom_top5_imbalance_pct is not None and np.isfinite(float(of_dom_top5_imbalance_pct)) else None),
                            "of_dom_top10_imbalance_pct": (float(of_dom_top10_imbalance_pct) if of_dom_top10_imbalance_pct is not None and np.isfinite(float(of_dom_top10_imbalance_pct)) else None),
                            "of_dom_spread_bps": (float(of_dom_spread_bps) if of_dom_spread_bps is not None and np.isfinite(float(of_dom_spread_bps)) else None),
                            "of_dom_bid_stack_ratio": (float(of_dom_bid_stack_ratio) if of_dom_bid_stack_ratio is not None and np.isfinite(float(of_dom_bid_stack_ratio)) else None),
                            "of_dom_ask_stack_ratio": (float(of_dom_ask_stack_ratio) if of_dom_ask_stack_ratio is not None and np.isfinite(float(of_dom_ask_stack_ratio)) else None),
                            "of_dom_bid_pull_ratio": (float(of_dom_bid_pull_ratio) if of_dom_bid_pull_ratio is not None and np.isfinite(float(of_dom_bid_pull_ratio)) else None),
                            "of_dom_ask_pull_ratio": (float(of_dom_ask_pull_ratio) if of_dom_ask_pull_ratio is not None and np.isfinite(float(of_dom_ask_pull_ratio)) else None),
                            "of_dom_breakout_watch_long": bool(of_dom_breakout_watch_long),
                            "of_dom_breakout_watch_short": bool(of_dom_breakout_watch_short),
                            "of_dom_breakout_confirm_long": bool(of_dom_breakout_confirm_long),
                            "of_dom_breakout_confirm_short": bool(of_dom_breakout_confirm_short),
                            "of_dom_updates_short": (float(of_dom_updates_short) if of_dom_updates_short is not None and np.isfinite(float(of_dom_updates_short)) else None),
                            "of_dom_updates_long": (float(of_dom_updates_long) if of_dom_updates_long is not None and np.isfinite(float(of_dom_updates_long)) else None),
                            "of_dom_best_bid": (float(of_dom_best_bid) if of_dom_best_bid is not None and np.isfinite(float(of_dom_best_bid)) else None),
                            "of_dom_best_ask": (float(of_dom_best_ask) if of_dom_best_ask is not None and np.isfinite(float(of_dom_best_ask)) else None),
                            "of_dom_mid": (float(of_dom_mid) if of_dom_mid is not None and np.isfinite(float(of_dom_mid)) else None),
                            "of_feature_mode": (str(of_feature_mode) if of_feature_mode is not None else None),
                            "of_dom_source": (str(of_dom_source) if of_dom_source is not None else None),
                            "of_dom_sync_state": (str(of_dom_sync_state) if of_dom_sync_state is not None else None),
                            "of_dom_resync_required": bool(of_dom_resync_required),
                            "of_dom_sync_note": (str(of_dom_sync_note) if of_dom_sync_note is not None else None),
                            "of_dom_partial_fallback_enabled": bool(of_dom_partial_fallback_enabled),
                            "of_dom_partial_fallback_active": bool(of_dom_partial_fallback_active),
                            "of_dom_partial_connected": bool(of_dom_partial_connected),
                            "of_dom_partial_updates_total": (float(of_dom_partial_updates_total) if of_dom_partial_updates_total is not None and np.isfinite(float(of_dom_partial_updates_total)) else None),
                            "of_dom_local_l2_enabled": bool(of_dom_local_l2_enabled),
                            "of_dom_local_book_synced": bool(of_dom_local_book_synced),
                            "of_dom_local_snapshot_id": (float(of_dom_local_snapshot_id) if of_dom_local_snapshot_id is not None and np.isfinite(float(of_dom_local_snapshot_id)) else None),
                            "of_dom_local_last_event_u": (float(of_dom_local_last_event_u) if of_dom_local_last_event_u is not None and np.isfinite(float(of_dom_local_last_event_u)) else None),
                            "of_dom_local_last_event_pu": (float(of_dom_local_last_event_pu) if of_dom_local_last_event_pu is not None and np.isfinite(float(of_dom_local_last_event_pu)) else None),
                            "of_dom_local_buffered_events": (float(of_dom_local_buffered_events) if of_dom_local_buffered_events is not None and np.isfinite(float(of_dom_local_buffered_events)) else None),
                            "of_dom_local_updates_applied": (float(of_dom_local_updates_applied) if of_dom_local_updates_applied is not None and np.isfinite(float(of_dom_local_updates_applied)) else None),
                            "of_dom_book_levels_bid": (float(of_dom_book_levels_bid) if of_dom_book_levels_bid is not None and np.isfinite(float(of_dom_book_levels_bid)) else None),
                            "of_dom_book_levels_ask": (float(of_dom_book_levels_ask) if of_dom_book_levels_ask is not None and np.isfinite(float(of_dom_book_levels_ask)) else None),
                            "of_dom_depth_age_ms": (float(of_dom_depth_age_ms) if of_dom_depth_age_ms is not None and np.isfinite(float(of_dom_depth_age_ms)) else None),
                            "of_dom_snapshot_age_ms": (float(of_dom_snapshot_age_ms) if of_dom_snapshot_age_ms is not None and np.isfinite(float(of_dom_snapshot_age_ms)) else None),
                            "di_plus": (dp_val if np.isfinite(dp_val) else None),
                            "di_minus": (dm_val if np.isfinite(dm_val) else None),
                            "di_spread": ((float(di_s) if di_s is not None and np.isfinite(float(di_s)) else (dp_val - dm_val if np.isfinite(dp_val) and np.isfinite(dm_val) else None))),
                            # Timestamp for latch age calculations (FIRED Xm)
                            "time": snap_time,
                            # Bar identifier (ms) for bars-ago logic
                            "bar_ms": bar_open_ms,
                            "is_closed": False,
                        }, prev_state=self.latest.get(tf))
                        try:
                            self._update_market_state_card(tf, self.latest[tf])
                        except Exception:
                            pass

                    self._schedule_live_rule_update()

        except queue.Empty:
            pass
        finally:
            self.after(QUEUE_POLL_MS, self._poll_queue)
