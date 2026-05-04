from __future__ import annotations

from app import ui_app
from app.ui_app import VisualApp


class _Placeholder:
    def __init__(self) -> None:
        self.destroyed = False

    def destroy(self) -> None:
        self.destroyed = True


class _Widget:
    def __init__(self) -> None:
        self.pack_calls = []

    def pack(self, *args, **kwargs) -> None:
        self.pack_calls.append((args, kwargs))


class _Canvas:
    def __init__(self, fig, master) -> None:
        self.fig = fig
        self.master = master
        self.widget = _Widget()
        self.draw_idle_calls = 0
        self.connections = []

    def get_tk_widget(self):
        return self.widget

    def mpl_connect(self, name: str, callback) -> int:
        self.connections.append(name)
        return len(self.connections)

    def draw_idle(self) -> None:
        self.draw_idle_calls += 1


class _Toolbar:
    def __init__(self, canvas, parent) -> None:
        self.canvas = canvas
        self.parent = parent
        self.updated = 0
        self.pack_calls = []

    def update(self) -> None:
        self.updated += 1

    def pack(self, *args, **kwargs) -> None:
        self.pack_calls.append((args, kwargs))



class _Axis:
    def clear(self) -> None:
        pass

    def set_title(self, _title: str) -> None:
        pass

class _GridSpec:
    def __getitem__(self, item):
        return item


class _Figure:
    def __init__(self, *args, **kwargs) -> None:
        pass

    def add_gridspec(self, **kwargs):
        return _GridSpec()

    def add_subplot(self, *args, **kwargs):
        return object()


class _FakeNotebook:
    def __init__(self, selected_widget) -> None:
        self._selected_widget = selected_widget

    def select(self):
        return 'selected-tab'

    def nametowidget(self, _name):
        return self._selected_widget


class _Var:
    def __init__(self, value) -> None:
        self._value = value

    def get(self):
        return self._value



def test_ensure_bt_price_chart_built_builds_once_and_removes_placeholder(monkeypatch) -> None:
    app = VisualApp.__new__(VisualApp)
    app._bt_price_chart_built = False
    app._bt_chart_tab_price = object()
    app._bt_price_chart_placeholder = _Placeholder()
    app._bt_on_chart_pick = lambda *args, **kwargs: None
    app._bt_on_chart_scroll = lambda *args, **kwargs: None

    monkeypatch.setattr(ui_app, 'FigureCanvasTkAgg', lambda fig, master: _Canvas(fig, master))
    monkeypatch.setattr(ui_app, 'NavigationToolbar2Tk', lambda canvas, parent: _Toolbar(canvas, parent))
    monkeypatch.setattr(ui_app, 'plt', type('P', (), {'Figure': _Figure})())

    app._ensure_bt_price_chart_built()
    first_canvas = app.bt_price_canvas
    app._ensure_bt_price_chart_built()

    assert app._bt_price_chart_placeholder is None
    assert app._bt_price_chart_built is True
    assert app.bt_price_canvas is first_canvas
    assert sorted(app.bt_price_canvas.connections) == ['pick_event', 'scroll_event']
    assert app.bt_price_canvas.draw_idle_calls == 1
    assert app.bt_price_toolbar.updated == 1



def test_ensure_bt_equity_chart_built_builds_once_and_removes_placeholder(monkeypatch) -> None:
    app = VisualApp.__new__(VisualApp)
    app._bt_equity_chart_built = False
    app._bt_chart_tab_equity = object()
    app._bt_equity_chart_placeholder = _Placeholder()

    monkeypatch.setattr(ui_app, 'FigureCanvasTkAgg', lambda fig, master: _Canvas(fig, master))
    monkeypatch.setattr(ui_app, 'NavigationToolbar2Tk', lambda canvas, parent: _Toolbar(canvas, parent))
    monkeypatch.setattr(ui_app, 'plt', type('P', (), {'Figure': _Figure})())

    app._ensure_bt_equity_chart_built()
    first_canvas = app.bt_equity_canvas
    app._ensure_bt_equity_chart_built()

    assert app._bt_equity_chart_placeholder is None
    assert app._bt_equity_chart_built is True
    assert app.bt_equity_canvas is first_canvas
    assert app.bt_equity_canvas.draw_idle_calls == 1
    assert app.bt_equity_toolbar.updated == 1



def test_bt_chart_tab_change_builds_only_selected_surface() -> None:
    price_tab = object()
    equity_tab = object()
    app = VisualApp.__new__(VisualApp)
    app._bt_chart_tab_price = price_tab
    app._bt_chart_tab_equity = equity_tab
    calls = []

    app._ensure_bt_price_chart_built = lambda: calls.append('price')  # type: ignore[method-assign]
    app._ensure_bt_equity_chart_built = lambda: calls.append('equity')  # type: ignore[method-assign]

    app.bt_chart_nb = _FakeNotebook(price_tab)
    app._on_bt_chart_tab_changed()
    app.bt_chart_nb = _FakeNotebook(equity_tab)
    app._on_bt_chart_tab_changed()

    assert calls == ['price', 'equity']



def test_bt_draw_chart_and_equity_ensure_surfaces_before_rendering() -> None:
    app = VisualApp.__new__(VisualApp)
    chart_calls = []
    eq_calls = []

    app._ensure_bt_price_chart_built = lambda: chart_calls.append('ensure-price')  # type: ignore[method-assign]
    app._ensure_bt_equity_chart_built = lambda: eq_calls.append('ensure-equity')  # type: ignore[method-assign]
    app.bt_chart_mode_var = _Var('CANDLES')
    app._bt_draw_chart_candles = lambda popup=False: chart_calls.append(('candles', popup))  # type: ignore[method-assign]
    app._bt_draw_chart_line_legacy = lambda: chart_calls.append('line')  # type: ignore[method-assign]
    app.bt_equity_ax = _Axis()
    app.bt_equity_canvas = _Canvas(None, None)
    app.bt_result = None
    app._bt_draw_equity()
    app._bt_draw_chart()

    assert eq_calls == ['ensure-equity']
    assert chart_calls == ['ensure-price', ('candles', False)]
