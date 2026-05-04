from __future__ import annotations

from app.ui_app import VisualApp


class _Placeholder:
    def __init__(self) -> None:
        self.destroyed = False

    def destroy(self) -> None:
        self.destroyed = True


class _Var:
    def __init__(self, value=None) -> None:
        self.value = value

    def set(self, value) -> None:
        self.value = value

    def get(self):
        return self.value


class _FakeWindow:
    def __init__(self) -> None:
        self.geometry_calls = []
        self.deiconified = 0
        self.lifted = 0
        self.focused = 0

    def geometry(self, spec: str) -> None:
        self.geometry_calls.append(spec)

    def deiconify(self) -> None:
        self.deiconified += 1

    def lift(self) -> None:
        self.lifted += 1

    def focus_force(self) -> None:
        self.focused += 1


class _FakeNotebook:
    def __init__(self, selected_widget) -> None:
        self._selected_widget = selected_widget

    def select(self):
        return 'backtest-tab-id'

    def nametowidget(self, _name):
        return self._selected_widget


def test_ensure_backtest_tab_built_builds_once_and_removes_placeholder() -> None:
    app = VisualApp.__new__(VisualApp)
    app._backtest_tab_built = False
    app._bt_lazy_placeholder = _Placeholder()
    calls = []

    def _build_backtest_tab() -> None:
        calls.append('build')
        app._backtest_tab_built = True

    app._build_backtest_tab = _build_backtest_tab  # type: ignore[method-assign]

    app._ensure_backtest_tab_built()
    app._ensure_backtest_tab_built()

    assert calls == ['build']
    assert app._bt_lazy_placeholder is None


def test_open_market_state_popup_builds_lazily_then_refreshes() -> None:
    app = VisualApp.__new__(VisualApp)
    app._market_state_popup_built = False
    app._market_state_popup = None
    app.market_state_collapsed_var = _Var(True)
    app.winfo_screenwidth = lambda: 1600  # type: ignore[method-assign]
    app.winfo_screenheight = lambda: 900  # type: ignore[method-assign]
    refresh_calls = []
    build_calls = []

    def _build_market_state_popup() -> None:
        build_calls.append('build')
        app._market_state_popup_built = True
        app._market_state_popup = _FakeWindow()

    app._build_market_state_popup = _build_market_state_popup  # type: ignore[method-assign]
    app._refresh_market_state_cards_from_latest = lambda: refresh_calls.append('refresh')  # type: ignore[method-assign]

    app._open_market_state_board_popup()
    app._open_market_state_board_popup()

    assert build_calls == ['build']
    assert refresh_calls == ['refresh', 'refresh']
    assert app.market_state_collapsed_var.get() is False
    win = app._market_state_popup
    assert isinstance(win, _FakeWindow)
    assert win.geometry_calls
    assert win.deiconified == 2
    assert win.lifted == 2
    assert win.focused == 2


def test_main_tab_change_builds_backtest_tab_only_for_backtest_selection() -> None:
    app = VisualApp.__new__(VisualApp)
    app._backtest_tab_built = False
    app._bt_lazy_placeholder = None
    app.tab_bt = object()
    app.main_nb = _FakeNotebook(app.tab_bt)
    calls = []

    def _build_backtest_tab() -> None:
        calls.append('build')
        app._backtest_tab_built = True

    app._build_backtest_tab = _build_backtest_tab  # type: ignore[method-assign]

    app._on_main_tab_changed()
    app._on_main_tab_changed()

    assert calls == ['build']
