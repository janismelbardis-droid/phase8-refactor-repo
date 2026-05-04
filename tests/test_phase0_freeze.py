from __future__ import annotations

import unittest
from unittest.mock import patch

import pandas as pd

from app.backtest import BacktestConfig, run_backtest, run_backtest_tick
from app.research.replay import replay_backtest_result
from app.rules import Rule
from tests.common import load_json


class Phase0FreezeTests(unittest.TestCase):
    def _sample_stream(self) -> pd.DataFrame:
        idx = pd.date_range('2026-01-01T00:00:00Z', periods=8, freq='1min', tz='UTC')
        return pd.DataFrame(
            {
                'open': [100.0, 100.5, 101.0, 101.5, 102.0, 102.5, 103.0, 103.5],
                'high': [100.2, 100.7, 101.2, 101.7, 102.2, 102.7, 103.2, 103.7],
                'low': [99.8, 100.3, 100.8, 101.3, 101.8, 102.3, 102.8, 103.3],
                'close': [100.0, 100.5, 101.0, 101.5, 102.0, 102.5, 103.0, 103.5],
                'price': [100.0, 100.5, 101.0, 101.5, 102.0, 102.5, 103.0, 103.5],
                'open_time': [ts - pd.Timedelta(minutes=1) + pd.Timedelta(milliseconds=1) for ts in idx],
                'close_time': list(idx),
                'volume': [10.0] * len(idx),
                'macd': [-1.0, -0.5, 0.2, 0.3, 0.8, 0.4, 0.2, -0.2],
                'signal': [-0.8, -0.4, 0.1, 0.2, 0.3, 0.35, 0.3, 0.1],
                'ms': ['RED', 'RED', 'GREEN', 'GREEN', 'GREEN', 'GREEN', 'GREEN', 'RED'],
            },
            index=idx,
        )

    def _rules(self):
        return {
            'Long Entry': [[Rule(timeframe='1m', mode='event', field='ms_cross', op='CROSS', value='UP')]],
            'Long Exit': [[Rule(timeframe='1m', mode='state', field='macd', op='>', value=0.75)]],
            'Short Entry': [],
            'Short Exit': [],
        }

    def _joins(self, rules_model):
        tab_join = {name: 'AND' for name in rules_model.keys()}
        group_join = {name: (['OR'] if rules_model[name] else []) for name in rules_model.keys()}
        return tab_join, group_join

    def _cfg(self) -> BacktestConfig:
        return BacktestConfig(
            initial_balance=1000.0,
            order_notional_usdt=100.0,
            fee_rate=0.0,
            slippage_bps=0.0,
            stop_loss_mode='OFF',
            take_profit_mode='OFF',
            step_timeframe='1m',
        )

    def _fake_ticks(self, df: pd.DataFrame):
        idx = df.index
        ticks = [
            {'T': int((idx[2] + pd.Timedelta(seconds=1)).value // 1_000_000), 'p': '101.05'},
            {'T': int((idx[4] + pd.Timedelta(seconds=1)).value // 1_000_000), 'p': '102.05'},
            {'T': int((idx[5] + pd.Timedelta(seconds=1)).value // 1_000_000), 'p': '102.55'},
            {'T': int((idx[7] + pd.Timedelta(seconds=1)).value // 1_000_000), 'p': '103.55'},
        ]
        for tick in ticks:
            yield tick

    def _pack_result(self, result):
        trade = result.trades[0]
        return {
            'summary': {
                'ending_balance': result.summary['ending_balance'],
                'net_profit': result.summary['net_profit'],
                'trades': result.summary['trades'],
                'wins': result.summary['wins'],
                'losses': result.summary['losses'],
                'avg_duration_min': result.summary['avg_duration_min'],
                'engine_type_counts': result.summary.get('engine_type_counts', {}),
                'fill_source_counts': result.summary.get('fill_source_counts', {}),
                'avg_signal_to_entry_sec': result.summary.get('avg_signal_to_entry_sec'),
                'avg_order_to_entry_sec': result.summary.get('avg_order_to_entry_sec'),
                'avg_entry_to_exit_sec': result.summary.get('avg_entry_to_exit_sec'),
            },
            'trade': {
                'side': trade.side,
                'entry_time': trade.entry_time.isoformat(),
                'exit_time': trade.exit_time.isoformat(),
                'entry_price': trade.entry_price,
                'exit_price': trade.exit_price,
                'net_pnl': trade.net_pnl,
                'entry_reason': trade.entry_reason,
                'exit_reason': trade.exit_reason,
                'engine_type': trade.engine_type,
                'fill_source': trade.fill_source,
                'entry_row_index': trade.entry_row_index,
                'exit_row_index': trade.exit_row_index,
            },
            'equity_curve_points': len(result.equity_curve),
        }

    def test_bar_close_replay_and_tick_paths_match_frozen_goldens(self) -> None:
        golden = load_json('phase0_parity_report.json')
        df = self._sample_stream()
        rules_model = self._rules()
        tab_join, group_join = self._joins(rules_model)
        cfg = self._cfg()

        bar_close = run_backtest(
            symbol='BTCUSDT',
            streams_full={'1m': df.copy()},
            df_1m_full=df.copy(),
            start=df.index[0],
            end=df.index[-1],
            rules_model=rules_model,
            tab_group_join_mode=tab_join,
            group_rule_join_mode=group_join,
            cfg=cfg,
            capture_trade_details=True,
            equity_curve_stride=1,
        )
        replay_light = replay_backtest_result(bar_close, cfg=cfg, capture_trade_details=False, equity_curve_stride=4)
        with patch('app.backtest.iter_aggtrades_futures_daily', lambda *args, **kwargs: self._fake_ticks(df)):
            tick = run_backtest_tick(
                symbol='BTCUSDT',
                df_1m_full=df.copy(),
                streams_full={'1m': df.copy()},
                start=df.index[0],
                end=df.index[-1] + pd.Timedelta(minutes=1),
                tfs=['1m'],
                rules_model=rules_model,
                tab_group_join_mode=tab_join,
                group_rule_join_mode=group_join,
                cfg=cfg,
            )

        self.assertEqual(self._pack_result(bar_close), golden['bar_close'])
        self.assertEqual(self._pack_result(replay_light), golden['replay_light'])
        self.assertEqual(self._pack_result(tick), golden['tick'])

    def test_light_trade_capture_keeps_math_and_execution_parity(self) -> None:
        df = self._sample_stream()
        rules_model = self._rules()
        tab_join, group_join = self._joins(rules_model)
        cfg = self._cfg()

        detailed = run_backtest(
            symbol='BTCUSDT',
            streams_full={'1m': df.copy()},
            df_1m_full=df.copy(),
            start=df.index[0],
            end=df.index[-1],
            rules_model=rules_model,
            tab_group_join_mode=tab_join,
            group_rule_join_mode=group_join,
            cfg=cfg,
            capture_trade_details=True,
            equity_curve_stride=1,
        )
        light = run_backtest(
            symbol='BTCUSDT',
            streams_full={'1m': df.copy()},
            df_1m_full=df.copy(),
            start=df.index[0],
            end=df.index[-1],
            rules_model=rules_model,
            tab_group_join_mode=tab_join,
            group_rule_join_mode=group_join,
            cfg=cfg,
            capture_trade_details=False,
            equity_curve_stride=4,
        )

        self.assertEqual(len(detailed.trades), 1)
        self.assertEqual(len(light.trades), 1)
        rich_trade = detailed.trades[0]
        light_trade = light.trades[0]
        self.assertEqual(rich_trade.side, light_trade.side)
        self.assertEqual(rich_trade.entry_time, light_trade.entry_time)
        self.assertEqual(rich_trade.exit_time, light_trade.exit_time)
        self.assertEqual(rich_trade.entry_price, light_trade.entry_price)
        self.assertEqual(rich_trade.exit_price, light_trade.exit_price)
        self.assertEqual(rich_trade.net_pnl, light_trade.net_pnl)
        self.assertEqual(rich_trade.engine_type, light_trade.engine_type)
        self.assertEqual(rich_trade.fill_source, light_trade.fill_source)
        self.assertEqual(light_trade.entry_snapshot, {})
        self.assertEqual(light_trade.exit_snapshot, {})
        self.assertEqual(light_trade.entry_rule_trace, '')
        self.assertEqual(light_trade.exit_rule_trace, '')
        self.assertEqual(detailed.summary['net_profit'], light.summary['net_profit'])
        self.assertEqual(detailed.summary['ending_balance'], light.summary['ending_balance'])


if __name__ == '__main__':
    unittest.main()
