from __future__ import annotations

import os
import unittest
from concurrent.futures import Future
from unittest.mock import patch

import pandas as pd

import app.services.ui_v2_runtime as runtime_mod
from app.backtest import BacktestConfig, run_backtest
from app.backtest_models import BacktestResult
from app.services.ui_v2_runtime import BacktestRuntimeService, ExitSweepAxis, ExitSweepRequest, _expand_sweep_axis


class UiV2RuntimeExitSweepTests(unittest.TestCase):
    def _sample_stream(self) -> pd.DataFrame:
        idx = pd.date_range("2026-01-01T00:00:00Z", periods=6, freq="1min", tz="UTC")
        return pd.DataFrame(
            {
                "open": [100.0, 100.0, 100.0, 100.0, 99.8, 99.7],
                "high": [100.2, 100.3, 101.6, 100.4, 100.0, 99.9],
                "low": [99.8, 99.9, 100.2, 99.7, 99.5, 99.4],
                "close": [100.0, 100.0, 101.2, 99.9, 99.8, 99.7],
                "price": [100.0, 100.0, 101.2, 99.9, 99.8, 99.7],
                "open_time": [ts - pd.Timedelta(minutes=1) + pd.Timedelta(milliseconds=1) for ts in idx],
                "close_time": list(idx),
                "volume": [10.0] * len(idx),
            },
            index=idx,
        )

    def _fake_eval(self, tab_name, rules_model, tab_group_join_mode, group_rule_join_mode, cur_by_tf, prev_by_tf, ctx=None):
        ts = str((cur_by_tf.get("1m") or {}).get("snapshot_time") or "")
        if tab_name == "Long Entry" and ts == "2026-01-01T00:01:00+00:00":
            return True
        return False

    def test_runtime_exit_sweep_reuses_latest_run_context(self) -> None:
        df = self._sample_stream()
        cfg = BacktestConfig(
            initial_balance=1000.0,
            order_notional_usdt=100.0,
            fee_rate=0.0,
            slippage_bps=0.0,
            stop_loss_mode="OFF",
            take_profit_mode="OFF",
            break_even_after_mfe_pct=0.0,
            trailing_stop_pct=0.0,
            step_timeframe="1m",
        )
        rules_model = {name: [] for name in ("Long Entry", "Long Exit", "Short Entry", "Short Exit")}
        join_mode = {name: "AND" for name in rules_model.keys()}
        group_join_mode = {name: [] for name in rules_model.keys()}

        with patch("app.backtest.eval_tab_generic", side_effect=self._fake_eval), patch(
            "app.backtest._build_tab_rule_trace", side_effect=lambda *args, **kwargs: "trace"
        ):
            base = run_backtest(
                symbol="BTCUSDT",
                streams_full={"1m": df.copy()},
                start=df.index[0],
                end=df.index[-1],
                rules_model=rules_model,
                tab_group_join_mode=join_mode,
                group_rule_join_mode=group_join_mode,
                cfg=cfg,
                df_1m_full=df.copy(),
            )

        service = BacktestRuntimeService()
        service._store_run_artifact(
            "demo",
            base,
            {
                "summary": dict(base.summary or {}),
                "config": {"stop_loss_mode": "OFF", "take_profit_mode": "OFF", "break_even_after_mfe_pct": 0.0, "trailing_stop_pct": 0.0},
                "verification_bundle": "",
            },
            run_id="demo-run",
        )

        with patch("app.backtest.eval_tab_generic", side_effect=self._fake_eval), patch(
            "app.backtest._build_tab_rule_trace", side_effect=lambda *args, **kwargs: "trace"
        ):
            payload = service.run_exit_sweep(
                preset_name="demo",
                run_id="demo-run",
                request=ExitSweepRequest(
                    auto_run=False,
                    sort_by="net_profit",
                    top_n=5,
                    max_combinations=20,
                    stop_loss=ExitSweepAxis(include_off=True),
                    take_profit=ExitSweepAxis(start=1.0, end=2.0, step=1.0, include_off=True),
                    break_even=ExitSweepAxis(start=1.0, end=1.0, step=0.0, include_off=True),
                    trailing_stop=ExitSweepAxis(start=1.0, end=1.0, step=0.0, include_off=True),
                ),
            )

        self.assertEqual(payload["result_kind"], "exit_sweep")
        self.assertEqual(payload["run_id"], "demo-run")
        self.assertIn("exit_sweep", payload)
        self.assertEqual(int(payload["exit_sweep"]["combo_count"]), 12)
        self.assertTrue(len(payload["exit_sweep"]["rows"]) >= 1)
        self.assertIn("best_combo", payload["exit_sweep"])
        self.assertIn("trailing_stop_pct", payload["exit_sweep"]["best_combo"])

    def test_exit_sweep_axis_expansion_and_default_limit_match_ui_expectations(self) -> None:
        stop_values = _expand_sweep_axis("Stop Loss", ExitSweepAxis(start=0.5, end=2.0, step=0.25, include_off=False))
        take_values = _expand_sweep_axis("Take Profit", ExitSweepAxis(start=1.0, end=4.0, step=0.5, include_off=False))
        break_even_values = _expand_sweep_axis("Break Even", ExitSweepAxis(start=0.5, end=2.0, step=0.25, include_off=True))
        trailing_values = _expand_sweep_axis("Trailing Stop", ExitSweepAxis(start=0.5, end=2.0, step=0.25, include_off=True))

        self.assertEqual(len(stop_values), 7)
        self.assertEqual(len(take_values), 7)
        self.assertEqual(len(break_even_values), 8)
        self.assertEqual(len(trailing_values), 8)
        self.assertEqual(len(stop_values) * len(take_values) * len(break_even_values) * len(trailing_values), 3136)

        request = ExitSweepRequest()
        self.assertEqual(int(request.max_combinations), 5000)

    def test_exit_sweep_parallelizes_combo_batches(self) -> None:
        service = BacktestRuntimeService()
        tracker = {"used": False, "submitted": 0, "max_workers": 0}
        base_result = BacktestResult(
            config=BacktestConfig(
                initial_balance=1000.0,
                order_notional_usdt=100.0,
                fee_rate=0.0,
                slippage_bps=0.0,
                stop_loss_mode="OFF",
                take_profit_mode="OFF",
                break_even_after_mfe_pct=0.0,
                trailing_stop_pct=0.0,
                step_timeframe="1m",
            ),
            start=pd.Timestamp("2026-01-01 00:00:00", tz="UTC"),
            end=pd.Timestamp("2026-01-01 00:05:00", tz="UTC"),
            trades=[],
            equity_curve=pd.DataFrame({"equity": [1000.0]}, index=[pd.Timestamp("2026-01-01 00:00:00", tz="UTC")]),
            summary={},
            replay_context={},
        )

        class _FakeExecutor:
            def __init__(self, max_workers=None, *args, **kwargs):
                tracker["used"] = True
                tracker["max_workers"] = int(max_workers or 0)

            def __enter__(self):
                return self

            def __exit__(self, exc_type, exc, tb):
                return False

            def submit(self, fn, *args, **kwargs):
                tracker["submitted"] += 1
                future: Future = Future()
                try:
                    future.set_result(fn(*args, **kwargs))
                except Exception as exc:  # pragma: no cover
                    future.set_exception(exc)
                return future

        def _fake_replay(result, *, cfg=None, config_updates=None, progress_cb=None, capture_trade_details=None, equity_curve_stride=None):
            cfg = cfg or result.config
            net_profit = (
                float(getattr(cfg, "take_profit_pct", 0.0) or 0.0)
                - float(getattr(cfg, "stop_loss_pct", 0.0) or 0.0)
                + float(getattr(cfg, "break_even_after_mfe_pct", 0.0) or 0.0)
                + float(getattr(cfg, "trailing_stop_pct", 0.0) or 0.0)
            )
            return BacktestResult(
                config=cfg,
                start=result.start,
                end=result.end,
                trades=[],
                equity_curve=pd.DataFrame({"equity": [1000.0 + net_profit]}, index=[result.start]),
                summary={
                    "net_profit": net_profit,
                    "return_pct": net_profit,
                    "ending_balance": 1000.0 + net_profit,
                    "profit_factor": max(0.0, net_profit),
                    "win_rate_pct": 50.0,
                    "max_drawdown_pct": abs(min(0.0, net_profit)),
                    "num_trades": 1,
                    "stop_loss_count": 0,
                    "take_profit_count": 0,
                    "break_even_count": 0,
                    "trailing_stop_count": 0,
                    "exit_reason_counts": {"Force Close (End)": 0},
                },
                replay_context={},
            )

        original_executor = runtime_mod.ProcessPoolExecutor
        old_env = {
            "BT_EXIT_SWEEP_PARALLEL_MIN_COMBOS": os.environ.get("BT_EXIT_SWEEP_PARALLEL_MIN_COMBOS"),
            "BT_EXIT_SWEEP_MAX_WORKERS": os.environ.get("BT_EXIT_SWEEP_MAX_WORKERS"),
            "BT_EXIT_SWEEP_DISABLE_PARALLEL": os.environ.get("BT_EXIT_SWEEP_DISABLE_PARALLEL"),
        }
        try:
            runtime_mod.ProcessPoolExecutor = _FakeExecutor
            os.environ["BT_EXIT_SWEEP_PARALLEL_MIN_COMBOS"] = "4"
            os.environ["BT_EXIT_SWEEP_MAX_WORKERS"] = "2"
            os.environ["BT_EXIT_SWEEP_DISABLE_PARALLEL"] = "0"
            with patch("app.services.ui_v2_runtime.replay_backtest_result", side_effect=_fake_replay):
                payload = service._run_exit_sweep_for_result(
                    preset_name="demo",
                    run_id="demo-run",
                    base_result=base_result,
                    request=ExitSweepRequest(
                        auto_run=False,
                        sort_by="net_profit",
                        top_n=5,
                        max_combinations=50,
                        stop_loss=ExitSweepAxis(start=0.5, end=2.0, step=0.5, include_off=True),
                        take_profit=ExitSweepAxis(start=1.0, end=3.0, step=1.0, include_off=True),
                        break_even=ExitSweepAxis(include_off=True),
                        trailing_stop=ExitSweepAxis(include_off=True),
                    ),
                    progress_cb=lambda *_args, **_kwargs: None,
                )
        finally:
            runtime_mod.ProcessPoolExecutor = original_executor
            for key, value in old_env.items():
                if value is None:
                    os.environ.pop(key, None)
                else:
                    os.environ[key] = value

        self.assertEqual(payload["combo_count"], 20)
        self.assertEqual(payload["execution_mode"], "parallel_exit_sweep")
        self.assertEqual(payload["worker_count"], 2)
        self.assertGreaterEqual(int(payload["task_count"]), 2)
        self.assertGreaterEqual(int(payload["batch_size"]), 1)
        self.assertTrue(tracker["used"])
        self.assertEqual(tracker["max_workers"], 2)
        self.assertEqual(tracker["submitted"], int(payload["task_count"]))


if __name__ == "__main__":
    unittest.main()
