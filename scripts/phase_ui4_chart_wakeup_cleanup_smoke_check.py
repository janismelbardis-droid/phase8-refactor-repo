from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from app import ui_app
from app.ui_app import VisualApp


class _Placeholder:
    def destroy(self) -> None:
        pass


class _Widget:
    def pack(self, *args, **kwargs) -> None:
        pass


class _Canvas:
    def __init__(self, fig, master) -> None:
        self.widget = _Widget()
        self.connections = []
        self.draw_idle_calls = 0

    def get_tk_widget(self):
        return self.widget

    def mpl_connect(self, name: str, callback) -> int:
        self.connections.append(name)
        return len(self.connections)

    def draw_idle(self) -> None:
        self.draw_idle_calls += 1


class _Toolbar:
    def __init__(self, canvas, parent) -> None:
        self.updated = 0

    def update(self) -> None:
        self.updated += 1

    def pack(self, *args, **kwargs) -> None:
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



def main() -> int:
    ui_app.FigureCanvasTkAgg = lambda fig, master: _Canvas(fig, master)
    ui_app.NavigationToolbar2Tk = lambda canvas, parent: _Toolbar(canvas, parent)
    ui_app.plt = type('P', (), {'Figure': _Figure})()

    app = VisualApp.__new__(VisualApp)
    app._bt_price_chart_built = False
    app._bt_equity_chart_built = False
    app._bt_chart_tab_price = object()
    app._bt_chart_tab_equity = object()
    app._bt_price_chart_placeholder = _Placeholder()
    app._bt_equity_chart_placeholder = _Placeholder()
    app._bt_on_chart_pick = lambda *args, **kwargs: None
    app._bt_on_chart_scroll = lambda *args, **kwargs: None

    app._ensure_bt_price_chart_built()
    app._ensure_bt_equity_chart_built()

    assert app._bt_price_chart_built is True
    assert app._bt_equity_chart_built is True
    assert sorted(app.bt_price_canvas.connections) == ['pick_event', 'scroll_event']
    assert app.bt_price_canvas.draw_idle_calls == 1
    assert app.bt_equity_canvas.draw_idle_calls == 1
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
