from __future__ import annotations

import collections

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


class _Text(_Widget):
    def __init__(self, *args, **kwargs) -> None:
        super().__init__()
        self.content = ""
        self.state = kwargs.get('state', 'normal')
        self.seen = 0

    def configure(self, **kwargs) -> None:
        if 'state' in kwargs:
            self.state = kwargs['state']

    def insert(self, _index: str, text: str) -> None:
        self.content += text

    def see(self, _index: str) -> None:
        self.seen += 1


class _FakeNotebook:
    def __init__(self, selected_widget) -> None:
        self._selected_widget = selected_widget

    def select(self):
        return 'selected-tab'

    def nametowidget(self, _name):
        return self._selected_widget



def test_ensure_live_workspace_tab_built_builds_once_and_removes_placeholder() -> None:
    app = VisualApp.__new__(VisualApp)
    app._live_workspace_tabs_built = {'builder': False, 'log': False, 'alerts': False}
    app._live_workspace_tab_placeholders = {'builder': _Placeholder(), 'log': None, 'alerts': None}
    app._live_workspace_tab_frames = {'builder': object(), 'log': object(), 'alerts': object()}
    calls = []

    app._build_rule_builder = lambda parent: calls.append(('builder', parent))  # type: ignore[method-assign]
    app._build_log = lambda parent: calls.append(('log', parent))  # type: ignore[method-assign]
    app._build_breakout_alerts = lambda parent: calls.append(('alerts', parent))  # type: ignore[method-assign]

    app._ensure_live_workspace_tab_built('builder')
    app._ensure_live_workspace_tab_built('builder')

    assert calls == [('builder', app._live_workspace_tab_frames['builder'])]
    assert app._live_workspace_tabs_built['builder'] is True
    assert app._live_workspace_tab_placeholders['builder'] is None



def test_live_workspace_tab_change_builds_selected_tab() -> None:
    builder = object()
    log = object()
    alerts = object()
    app = VisualApp.__new__(VisualApp)
    app._live_workspace_tab_frames = {'builder': builder, 'log': log, 'alerts': alerts}
    calls = []
    app._ensure_live_workspace_tab_built = lambda tab_key: calls.append(tab_key)  # type: ignore[method-assign]

    app._live_workspace_nb = _FakeNotebook(log)
    app._on_live_workspace_tab_changed()
    app._live_workspace_nb = _FakeNotebook(alerts)
    app._on_live_workspace_tab_changed()
    app._live_workspace_nb = _FakeNotebook(builder)
    app._on_live_workspace_tab_changed()

    assert calls == ['log', 'alerts', 'builder']



def test_live_log_append_buffers_before_widget_and_build_log_replays(monkeypatch) -> None:
    app = VisualApp.__new__(VisualApp)
    app._live_log_buffer = collections.deque(maxlen=2500)

    app._live_log_append('hello')
    app._live_log_append('world')

    monkeypatch.setattr(ui_app.ttk, 'LabelFrame', lambda *args, **kwargs: _Widget())
    monkeypatch.setattr(ui_app.tk, 'Text', lambda *args, **kwargs: _Text(*args, **kwargs))

    app._build_log(object())

    assert list(app._live_log_buffer) == ['hello', 'world']
    assert 'hello\nworld\n' == app.txt_live.content
    assert app.txt_live.state == 'disabled'



def test_build_breakout_alerts_rehydrates_existing_records(monkeypatch) -> None:
    app = VisualApp.__new__(VisualApp)
    app._breakout_alert_records = [
        {
            'event_time': '2026-03-28 20:10:00',
            'bar_time': '2026-03-28 20:09:00',
            'tf': '5m',
            'direction': 'LONG',
            'breakout': 'Compression release',
            'state': 'Trend Up',
            'price': '123.45',
            'of_risk': 'LOW',
            'vwap_bias': 'ABOVE',
            'note': 'ready',
        }
    ]

    monkeypatch.setattr(ui_app.ttk, 'LabelFrame', lambda *args, **kwargs: _Widget())
    monkeypatch.setattr(ui_app.ttk, 'Label', lambda *args, **kwargs: _Widget())
    monkeypatch.setattr(ui_app.ttk, 'Frame', lambda *args, **kwargs: _Widget())
    monkeypatch.setattr(ui_app.ttk, 'Button', lambda *args, **kwargs: _Widget())
    monkeypatch.setattr(ui_app.tk, 'Text', lambda *args, **kwargs: _Text(*args, **kwargs))

    app._build_breakout_alerts(object())

    assert 'TF=5m DIR=LONG PRICE=123.45' in app.txt_breakout_alerts.content
    assert 'NOTE=ready' in app.txt_breakout_alerts.content
    assert app.txt_breakout_alerts.state == 'disabled'
