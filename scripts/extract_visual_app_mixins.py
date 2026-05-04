"""One-off helper to slice methods from ui_app.py into mixin modules (run from repo root)."""

from __future__ import annotations

import re
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
UI_APP = ROOT / "app" / "ui_app.py"
OUT = ROOT / "app" / "ui" / "visual_app"


def main() -> None:
    text = UI_APP.read_text(encoding="utf-8")
    lines = text.splitlines(keepends=True)

    # 1-based inclusive -> 0-based [start, end)
    slices = {
        "live_shell_mixin.py": (588, 1003),
        "presets_mixin.py": (2375, 3103),
        "backtest_charts_mixin.py": (5386, 8814),
    }

    headers = {
        "live_shell_mixin.py": LIVE_SHELL_HEADER,
        "presets_mixin.py": PRESETS_HEADER,
        "backtest_charts_mixin.py": BACKTEST_HEADER,
    }

    for name, (lo, hi) in slices.items():
        body = "".join(lines[lo - 1 : hi])
        (OUT / name).write_text(headers[name] + body + "\n", encoding="utf-8")
        print(f"Wrote {name} ({hi - lo + 1} lines)")


LIVE_SHELL_HEADER = '''from __future__ import annotations

"""Lazy live tab / main notebook / market-state popup shell helpers."""

import tkinter as tk
from tkinter import ttk

from ...constants import QUEUE_POLL_MS


class LiveShellMixin:
    """Live layout refresh, lazy workspace/top builds, main tab selection, market-state board popup."""

'''

PRESETS_HEADER = '''from __future__ import annotations

"""Preset save/load, entry filters, and market-state preset reports."""

import json
import os
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd
import tkinter as tk
from tkinter import filedialog, messagebox, simpledialog, ttk

from ...constants import ALL_TIMEFRAMES, BACKTEST_MODES, RULE_TABS
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

'''

BACKTEST_HEADER = '''from __future__ import annotations

"""Backtest charts, trade tree, reports, trade inspector, and render path."""

import gzip
import json
import os
import re
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

from ...backtest import BacktestResult, Trade
from ...constants import *
from ...indicators_streaming import slice_and_decimate
from ...rules import Rule
from ...ui_plot import build_figure

# Research / audit (same targets as ui_app lazy imports)
from ... import research as research_mod


class BacktestChartsMixin:
    """Matplotlib backtest UI: OHLC/MACD/equity, trade selection, reports, inspector."""

'''

if __name__ == "__main__":
    main()
