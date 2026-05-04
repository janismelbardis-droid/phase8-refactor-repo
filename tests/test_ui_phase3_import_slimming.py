from __future__ import annotations

import importlib
import sys
from pathlib import Path

import pandas as pd


HEAVY_PREFIXES = [
    'app.ui_app',
    'app.backtest',
    'app.live_engine',
    'app.ui_plot',
    'app.ui_dialogs',
    'app.ui_widgets',
    'app.preset_visualizer',
    'app.ui.inspectors',
    'app.ui.dialogs',
    'app.research',
    'app.indicators_streaming',
    'matplotlib',
]


def _purge_heavy_modules() -> None:
    for name in list(sys.modules):
        if any(name == prefix or name.startswith(prefix + '.') for prefix in HEAVY_PREFIXES):
            sys.modules.pop(name, None)


def test_ui_app_import_stays_cold_until_runtime_features_are_used() -> None:
    _purge_heavy_modules()

    ui_app = importlib.import_module('app.ui_app')
    ui_app = importlib.reload(ui_app)

    assert 'app.backtest' not in sys.modules
    assert 'app.live_engine' not in sys.modules
    assert 'app.ui_plot' not in sys.modules
    assert 'app.ui_dialogs' not in sys.modules
    assert 'app.ui_widgets' not in sys.modules
    assert 'app.preset_visualizer' not in sys.modules
    assert 'app.ui.inspectors' not in sys.modules
    assert 'app.ui.dialogs' not in sys.modules
    assert 'app.research' not in sys.modules
    assert 'app.indicators_streaming' not in sys.modules
    assert not any(name == 'matplotlib' or name.startswith('matplotlib.') for name in sys.modules)


def test_runtime_wrappers_load_only_the_module_they_need() -> None:
    _purge_heavy_modules()
    ui_app = importlib.import_module('app.ui_app')
    ui_app = importlib.reload(ui_app)

    thresholds = ui_app.market_state_thresholds_dict()
    assert isinstance(thresholds, dict)
    assert 'app.indicators_streaming' in sys.modules
    assert 'app.backtest' not in sys.modules
    assert 'app.ui_plot' not in sys.modules
    assert not any(name == 'matplotlib' or name.startswith('matplotlib.') for name in sys.modules)

    cfg = ui_app.BacktestConfig()
    assert cfg is not None
    assert 'app.backtest' in sys.modules
    assert 'app.ui_plot' not in sys.modules


def test_build_figure_wrapper_loads_plot_stack_on_demand() -> None:
    _purge_heavy_modules()
    ui_app = importlib.import_module('app.ui_app')
    ui_app = importlib.reload(ui_app)

    idx = pd.date_range('2025-01-01', periods=5, freq='min', tz='UTC')
    df = pd.DataFrame(
        {
            'price': [1.5, 2.5, 3.5, 4.5, 5.5],
            'macd': [0.1, 0.2, 0.15, 0.25, 0.3],
            'signal': [0.05, 0.1, 0.12, 0.18, 0.22],
        },
        index=idx,
    )

    fig = ui_app.build_figure('1m', df)
    assert fig is not None
    assert 'app.ui_plot' in sys.modules
    assert any(name == 'matplotlib' or name.startswith('matplotlib.') for name in sys.modules)
