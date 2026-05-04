from __future__ import annotations

from types import SimpleNamespace

import pandas as pd

from app.ui_app import VisualApp


class _Placeholder:
    def __init__(self) -> None:
        self.destroyed = False

    def destroy(self) -> None:
        self.destroyed = True


class _TextWidget:
    def __init__(self) -> None:
        self.ops = []

    def configure(self, **kwargs) -> None:
        self.ops.append(('configure', kwargs))

    def delete(self, start, end) -> None:
        self.ops.append(('delete', start, end))

    def insert(self, start, value) -> None:
        self.ops.append(('insert', start, value))


class _TreeWidget:
    def __init__(self) -> None:
        self.rows = []

    def get_children(self):
        return []

    def delete(self, iid) -> None:
        self.rows.append(('delete', iid))

    def insert(self, parent, where, iid=None, values=None):
        self.rows.append((iid, values))


class _Var:
    def __init__(self, value=None) -> None:
        self.value = value

    def set(self, value) -> None:
        self.value = value


class _Button:
    def __init__(self) -> None:
        self.config_calls = []

    def config(self, **kwargs) -> None:
        self.config_calls.append(kwargs)



def test_ensure_bt_results_surface_builds_once_and_removes_placeholder() -> None:
    app = VisualApp.__new__(VisualApp)
    app._bt_results_surface_built = False
    app._bt_results_parent = object()
    app._bt_results_placeholder = _Placeholder()
    calls = []

    app._build_bt_results_surface = lambda parent: calls.append(parent)  # type: ignore[method-assign]

    app._ensure_bt_results_surface_built()
    app._ensure_bt_results_surface_built()

    assert calls == [app._bt_results_parent]
    assert app._bt_results_surface_built is True
    assert app._bt_results_placeholder is None



def test_render_backtest_builds_results_surface_before_populating_widgets() -> None:
    app = VisualApp.__new__(VisualApp)
    call_order = []
    app._ensure_backtest_tab_built = lambda: call_order.append('tab')  # type: ignore[method-assign]

    def _ensure_results() -> None:
        call_order.append('results')
        app.bt_summary_txt = _TextWidget()
        app.bt_tree = _TreeWidget()

    app._ensure_bt_results_surface_built = _ensure_results  # type: ignore[method-assign]
    app._bt_draw_equity = lambda: call_order.append('equity')  # type: ignore[method-assign]
    app.bt_chart_pos_var = _Var(0.0)
    app.btn_export_csv = _Button()
    app.btn_clear_bt = _Button()
    app.btn_inspect_trade = _Button()
    app.btn_save_report = _Button()
    app.btn_generate_trade_detail = _Button()
    app.btn_trade_audit = _Button()
    app.main_nb = SimpleNamespace(select=lambda _tab: None)
    app.tab_bt = object()

    trade = SimpleNamespace(
        trade_id=1,
        engine_type='bar',
        fill_source='close',
        side='LONG',
        signal_time=pd.Timestamp('2024-01-01 00:00:00', tz='UTC'),
        entry_time=pd.Timestamp('2024-01-01 00:01:00', tz='UTC'),
        exit_time=pd.Timestamp('2024-01-01 00:05:00', tz='UTC'),
        entry_fill_time=pd.Timestamp('2024-01-01 00:01:05', tz='UTC'),
        entry_price=100.0,
        exit_price=105.0,
        stop_loss_price=None,
        take_profit_price=None,
        net_pnl=5.0,
        return_pct=5.0,
        duration_min=4,
        entry_reason='Entry',
        exit_reason='Exit',
        ambiguous_exit=False,
    )
    cfg = SimpleNamespace(
        order_notional_usdt=100.0,
        fee_rate=0.0004,
        slippage_bps=0.0,
        stop_loss_mode='PERCENT',
        stop_loss_value=0.0,
        stop_loss_timeframe='1m',
        stop_loss_field='',
        stop_loss_offset_pct=0.0,
        take_profit_mode='PERCENT',
        take_profit_value=0.0,
        take_profit_timeframe='1m',
        take_profit_field='',
        take_profit_offset_pct=0.0,
        allow_reverse=True,
        skip_if_both_entries=True,
    )
    res = SimpleNamespace(
        config=cfg,
        start=pd.Timestamp('2024-01-01 00:00:00', tz='UTC'),
        end=pd.Timestamp('2024-01-01 01:00:00', tz='UTC'),
        trades=[trade],
        summary={
            'symbol': 'BTCUSDT',
            'initial_balance': 1000.0,
            'ending_balance': 1005.0,
            'net_profit': 5.0,
            'return_pct': 0.5,
            'gross_pnl_total': 5.2,
            'fees_total': 0.2,
            'fees_entry_total': 0.1,
            'fees_exit_total': 0.1,
            'net_profit_from_trades': 5.0,
            'trades': 1,
            'wins': 1,
            'losses': 0,
            'win_rate_pct': 100.0,
            'profit_factor': 0.0,
            'avg_trade': 5.0,
            'avg_win': 5.0,
            'avg_loss': 0.0,
            'max_drawdown': 0.0,
            'max_drawdown_pct': 0.0,
            'avg_duration_min': 4.0,
            'avg_entry_to_exit_sec': 240.0,
            'avg_signal_to_entry_sec': 65.0,
            'avg_order_to_entry_sec': 0.0,
            'stop_loss_count': 0,
            'take_profit_count': 0,
            'ambiguous_exit_count': 0,
        },
    )

    app._render_backtest(res, streams_full=None)

    assert call_order[:2] == ['tab', 'results']
    assert any(op[0] == 'insert' for op in app.bt_summary_txt.ops)
    assert len(app.bt_tree.rows) == 1
    assert app.bt_tree.rows[0][0] == '1'
