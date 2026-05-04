"""Extracted `VisualApp` behavior split into mixins (see `app.ui_app.VisualApp`)."""

from .backtest_charts_mixin import BacktestChartsMixin
from .lazy_bt_results_mixin import LazyBtResultsMixin
from .live_shell_mixin import LiveShellMixin
from .prepared_mixin import PreparedDatasetMixin
from .presets_mixin import PresetsMixin
from .scroll_screen_mixin import ScrollAndScreenMixin

__all__ = [
    "BacktestChartsMixin",
    "LazyBtResultsMixin",
    "LiveShellMixin",
    "PreparedDatasetMixin",
    "PresetsMixin",
    "ScrollAndScreenMixin",
]
