from __future__ import annotations

from app.ui_app import VisualApp


class _Placeholder:
    def __init__(self) -> None:
        self.destroyed = False

    def destroy(self) -> None:
        self.destroyed = True


class _Widget:
    def __init__(self) -> None:
        self.pack_calls = []
        self.config_calls = []

    def pack(self, *args, **kwargs) -> None:
        self.pack_calls.append((args, kwargs))

    def config(self, **kwargs) -> None:
        self.config_calls.append(kwargs)


class _Var:
    def __init__(self, value=None) -> None:
        self.value = value

    def get(self):
        return self.value

    def set(self, value) -> None:
        self.value = value



def test_ensure_live_top_surfaces_build_once_and_remove_placeholders() -> None:
    app = VisualApp.__new__(VisualApp)
    app._live_breakout_alert_built = False
    app._live_signal_table_built = False
    app._live_breakout_parent = object()
    app._live_table_parent = object()
    app._live_top_placeholders = {'breakout': _Placeholder(), 'table': _Placeholder()}
    app.breakout_alert_collapsed_var = _Var(False)
    calls = []

    app._build_live_breakout_alert = lambda parent: calls.append(('breakout', parent))  # type: ignore[method-assign]
    app._build_live_signal_table = lambda parent: calls.append(('table', parent))  # type: ignore[method-assign]
    app._set_breakout_alert_collapsed = lambda collapsed: calls.append(('collapse', collapsed))  # type: ignore[method-assign]
    app._queue_live_layout_refresh = lambda: calls.append(('refresh', None))  # type: ignore[method-assign]

    app._ensure_live_breakout_alert_built()
    app._ensure_live_breakout_alert_built()
    app._ensure_live_signal_table_built()
    app._ensure_live_signal_table_built()

    assert calls == [
        ('breakout', app._live_breakout_parent),
        ('collapse', False),
        ('table', app._live_table_parent),
        ('refresh', None),
    ]
    assert app._live_breakout_alert_built is True
    assert app._live_signal_table_built is True
    assert app._live_top_placeholders['breakout'] is None
    assert app._live_top_placeholders['table'] is None



def test_schedule_initial_live_top_build_uses_after_idle_once() -> None:
    app = VisualApp.__new__(VisualApp)
    app._live_breakout_alert_built = False
    app._live_signal_table_built = False
    app._live_top_build_job = None
    calls = []

    def fake_after_idle(callback):
        calls.append(callback)
        return 'job-1'

    app.after_idle = fake_after_idle  # type: ignore[method-assign]

    app._schedule_initial_live_top_build()
    app._schedule_initial_live_top_build()

    assert app._live_top_build_job == 'job-1'
    assert calls == [app._build_initial_live_top_surfaces]



def test_set_breakout_alert_banner_lazy_builds_banner_before_updating() -> None:
    app = VisualApp.__new__(VisualApp)
    app._live_breakout_alert_built = False
    app.breakout_alert_summary_var = _Var('')
    app._breakout_alert_banner_latched = False

    frame = _Widget()
    head = _Widget()
    det = _Widget()

    def fake_ensure() -> None:
        app._live_breakout_alert_built = True
        app._breakout_alert_banner_frame = frame
        app._breakout_alert_banner_head = head
        app._breakout_alert_banner_detail = det

    app._ensure_live_breakout_alert_built = fake_ensure  # type: ignore[method-assign]

    app._set_breakout_alert_banner('Ready', 'Built lazily', level='long')

    assert app.breakout_alert_summary_var.get() == 'Ready'
    assert frame.config_calls
    assert head.config_calls[-1]['text'] == 'Ready'
    assert det.config_calls[-1]['text'] == 'Built lazily'
    assert app._breakout_alert_banner_latched is True
