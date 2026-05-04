from __future__ import annotations

import collections
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from app import ui_app
from app.ui_app import VisualApp


class _Placeholder:
    def __init__(self) -> None:
        self.destroyed = False

    def destroy(self) -> None:
        self.destroyed = True


class _Widget:
    def pack(self, *args, **kwargs) -> None:
        pass


class _Text(_Widget):
    def __init__(self, *args, **kwargs) -> None:
        self.content = ""
        self.state = kwargs.get('state', 'normal')

    def configure(self, **kwargs) -> None:
        if 'state' in kwargs:
            self.state = kwargs['state']

    def insert(self, _index: str, text: str) -> None:
        self.content += text

    def see(self, _index: str) -> None:
        pass



def main() -> int:
    app = VisualApp.__new__(VisualApp)
    app._live_workspace_tabs_built = {'builder': False, 'log': False, 'alerts': False}
    app._live_workspace_tab_placeholders = {'builder': _Placeholder(), 'log': None, 'alerts': None}
    app._live_workspace_tab_frames = {'builder': object(), 'log': object(), 'alerts': object()}
    app._live_log_buffer = collections.deque(maxlen=2500)
    build_calls = []

    app._build_rule_builder = lambda parent: build_calls.append(('builder', parent))  # type: ignore[method-assign]
    app._build_log = lambda parent: build_calls.append(('log', parent))  # type: ignore[method-assign]
    app._build_breakout_alerts = lambda parent: build_calls.append(('alerts', parent))  # type: ignore[method-assign]

    app._ensure_live_workspace_tab_built('builder')
    assert build_calls == [('builder', app._live_workspace_tab_frames['builder'])]
    assert app._live_workspace_tabs_built['builder'] is True
    assert app._live_workspace_tab_placeholders['builder'] is None

    app = VisualApp.__new__(VisualApp)
    app._live_log_buffer = collections.deque(maxlen=2500)
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
    app._live_log_append('hello')

    ui_app.ttk.LabelFrame = lambda *args, **kwargs: _Widget()
    ui_app.ttk.Label = lambda *args, **kwargs: _Widget()
    ui_app.ttk.Frame = lambda *args, **kwargs: _Widget()
    ui_app.ttk.Button = lambda *args, **kwargs: _Widget()
    ui_app.tk.Text = lambda *args, **kwargs: _Text(*args, **kwargs)

    app._build_log(object())
    app._build_breakout_alerts(object())
    assert 'hello' in app.txt_live.content
    assert 'TF=5m DIR=LONG PRICE=123.45' in app.txt_breakout_alerts.content
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
