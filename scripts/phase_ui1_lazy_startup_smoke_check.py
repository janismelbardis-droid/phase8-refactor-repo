from __future__ import annotations

from app.ui_app import VisualApp


class _Placeholder:
    def __init__(self) -> None:
        self.destroyed = False

    def destroy(self) -> None:
        self.destroyed = True


app = VisualApp.__new__(VisualApp)
app._backtest_tab_built = False
app._bt_lazy_placeholder = _Placeholder()
built = []


def _build_backtest_tab() -> None:
    built.append('ok')
    app._backtest_tab_built = True


app._build_backtest_tab = _build_backtest_tab  # type: ignore[method-assign]
app._ensure_backtest_tab_built()
app._ensure_backtest_tab_built()
assert built == ['ok']
assert app._bt_lazy_placeholder is None
print('phase ui1 lazy startup smoke check passed')
