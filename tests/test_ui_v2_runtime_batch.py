from __future__ import annotations

import os
import unittest
from concurrent.futures import Future

import app.services.ui_v2_runtime as runtime_mod
from app.services.ui_v2_runtime import BacktestRuntimeService


class UiV2RuntimeBatchTests(unittest.TestCase):
    def test_run_saved_preset_batch_parallelizes_cases(self) -> None:
        service = BacktestRuntimeService()
        tracker = {"used": False, "submitted": 0, "max_workers": 0}

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

        def _fake_case(task):
            case_index = int(task.get("case_index") or 0)
            label = str(task.get("label") or f"Case {case_index}")
            net_profit = float(case_index * 10)
            return {
                "label": label,
                "case_index": case_index,
                "payload": {
                    "result_kind": "backtest",
                    "run_id": f"run-{case_index}",
                    "preset_name": str(task.get("preset_name") or ""),
                    "symbol": "BTCUSDT",
                    "start": "2026-01-01 00:00:00+00:00",
                    "end": "2026-01-01 01:00:00+00:00",
                    "warmup_minutes": 240,
                    "config": {"step_timeframe": "1m"},
                    "summary": {
                        "net_profit": net_profit,
                        "return_pct": net_profit,
                        "ending_balance": 1000.0 + net_profit,
                        "profit_factor": net_profit,
                        "win_rate_pct": 50.0,
                        "max_drawdown_pct": 1.0,
                        "num_trades": case_index,
                    },
                    "verification_bundle": f"C:/tmp/bundle-{case_index}",
                    "exit_sweep": None,
                    "exit_sweep_error": None,
                },
            }

        original_executor = runtime_mod.ProcessPoolExecutor
        original_case_runner = runtime_mod._run_saved_preset_batch_case
        old_env = {
            "BT_BACKTEST_BATCH_PARALLEL_MIN_CASES": os.environ.get("BT_BACKTEST_BATCH_PARALLEL_MIN_CASES"),
            "BT_BACKTEST_BATCH_MAX_WORKERS": os.environ.get("BT_BACKTEST_BATCH_MAX_WORKERS"),
            "BT_BACKTEST_BATCH_DISABLE_PARALLEL": os.environ.get("BT_BACKTEST_BATCH_DISABLE_PARALLEL"),
        }
        try:
            runtime_mod.ProcessPoolExecutor = _FakeExecutor
            runtime_mod._run_saved_preset_batch_case = _fake_case
            os.environ["BT_BACKTEST_BATCH_PARALLEL_MIN_CASES"] = "2"
            os.environ["BT_BACKTEST_BATCH_MAX_WORKERS"] = "2"
            os.environ["BT_BACKTEST_BATCH_DISABLE_PARALLEL"] = "0"
            payload = service.run_saved_preset_batch(
                preset_name="demo",
                cases=[
                    {"label": "Case A", "overrides": {"fee_pct": 0.04}},
                    {"label": "Case B", "overrides": {"fee_pct": 0.05}},
                    {"label": "Case C", "overrides": {"fee_pct": 0.06}},
                ],
                progress_cb=lambda *_args, **_kwargs: None,
            )
        finally:
            runtime_mod.ProcessPoolExecutor = original_executor
            runtime_mod._run_saved_preset_batch_case = original_case_runner
            for key, value in old_env.items():
                if value is None:
                    os.environ.pop(key, None)
                else:
                    os.environ[key] = value

        self.assertEqual(payload["result_kind"], "backtest_batch")
        self.assertEqual(payload["preset_name"], "demo")
        self.assertEqual(payload["case_count"], 3)
        self.assertEqual(payload["execution_mode"], "parallel_backtest_batch")
        self.assertEqual(payload["worker_count"], 2)
        self.assertEqual(payload["task_count"], 3)
        self.assertEqual(payload["batch_size"], 1)
        self.assertTrue(tracker["used"])
        self.assertEqual(tracker["max_workers"], 2)
        self.assertEqual(tracker["submitted"], 3)
        self.assertEqual(len(payload["cases"]), 3)
        self.assertEqual(len(payload["rows"]), 3)
        self.assertEqual(payload["best_case"]["label"], "Case C")
        self.assertEqual(payload["rows"][0]["label"], "Case C")

    def test_run_saved_preset_batch_groups_cases_into_worker_batches(self) -> None:
        service = BacktestRuntimeService()
        tracker = {"used": False, "submitted": 0, "max_workers": 0, "batch_sizes": []}

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
                tracker["batch_sizes"].append(len(list((args[0] or {}).get("cases") or [])))
                future: Future = Future()
                try:
                    future.set_result(fn(*args, **kwargs))
                except Exception as exc:  # pragma: no cover
                    future.set_exception(exc)
                return future

        def _fake_case_batch(task):
            rows = []
            for case in list(task.get("cases") or []):
                case_index = int(case.get("case_index") or 0)
                label = str(case.get("label") or f"Case {case_index}")
                net_profit = float(case_index * 10)
                rows.append(
                    {
                        "label": label,
                        "case_index": case_index,
                        "payload": {
                            "result_kind": "backtest",
                            "run_id": f"run-{case_index}",
                            "preset_name": str(task.get("preset_name") or ""),
                            "symbol": "BTCUSDT",
                            "start": "2026-01-01 00:00:00+00:00",
                            "end": "2026-01-01 01:00:00+00:00",
                            "warmup_minutes": 240,
                            "config": {"step_timeframe": "1m"},
                            "summary": {
                                "net_profit": net_profit,
                                "return_pct": net_profit,
                                "ending_balance": 1000.0 + net_profit,
                                "profit_factor": net_profit,
                                "win_rate_pct": 50.0,
                                "max_drawdown_pct": 1.0,
                                "num_trades": case_index,
                            },
                            "verification_bundle": f"C:/tmp/bundle-{case_index}",
                            "exit_sweep": None,
                            "exit_sweep_error": None,
                        },
                    }
                )
            return rows

        original_executor = runtime_mod.ProcessPoolExecutor
        original_case_batch_runner = runtime_mod._run_saved_preset_batch_case_batch
        old_env = {
            "BT_BACKTEST_BATCH_PARALLEL_MIN_CASES": os.environ.get("BT_BACKTEST_BATCH_PARALLEL_MIN_CASES"),
            "BT_BACKTEST_BATCH_MAX_WORKERS": os.environ.get("BT_BACKTEST_BATCH_MAX_WORKERS"),
            "BT_BACKTEST_BATCH_DISABLE_PARALLEL": os.environ.get("BT_BACKTEST_BATCH_DISABLE_PARALLEL"),
        }
        try:
            runtime_mod.ProcessPoolExecutor = _FakeExecutor
            runtime_mod._run_saved_preset_batch_case_batch = _fake_case_batch
            os.environ["BT_BACKTEST_BATCH_PARALLEL_MIN_CASES"] = "2"
            os.environ["BT_BACKTEST_BATCH_MAX_WORKERS"] = "2"
            os.environ["BT_BACKTEST_BATCH_DISABLE_PARALLEL"] = "0"
            payload = service.run_saved_preset_batch(
                preset_name="demo",
                cases=[
                    {"label": "Case A", "overrides": {"fee_pct": 0.04}},
                    {"label": "Case B", "overrides": {"fee_pct": 0.05}},
                    {"label": "Case C", "overrides": {"fee_pct": 0.06}},
                    {"label": "Case D", "overrides": {"fee_pct": 0.07}},
                    {"label": "Case E", "overrides": {"fee_pct": 0.08}},
                ],
                progress_cb=lambda *_args, **_kwargs: None,
            )
        finally:
            runtime_mod.ProcessPoolExecutor = original_executor
            runtime_mod._run_saved_preset_batch_case_batch = original_case_batch_runner
            for key, value in old_env.items():
                if value is None:
                    os.environ.pop(key, None)
                else:
                    os.environ[key] = value

        self.assertEqual(payload["execution_mode"], "parallel_backtest_batch")
        self.assertEqual(payload["worker_count"], 2)
        self.assertEqual(payload["task_count"], 3)
        self.assertEqual(payload["batch_size"], 2)
        self.assertTrue(tracker["used"])
        self.assertEqual(tracker["submitted"], 3)
        self.assertEqual(tracker["batch_sizes"], [2, 2, 1])
        self.assertEqual(payload["best_case"]["label"], "Case E")


if __name__ == "__main__":
    unittest.main()
