from __future__ import annotations

import os
import unittest

from app.execution_planner import choose_process_batch_plan, split_batches


class ExecutionPlannerTests(unittest.TestCase):
    def test_choose_process_batch_plan_respects_env_thresholds_and_batches(self) -> None:
        old_env = {
            "BT_TEST_PLAN_DISABLE": os.environ.get("BT_TEST_PLAN_DISABLE"),
            "BT_TEST_PLAN_MIN_ITEMS": os.environ.get("BT_TEST_PLAN_MIN_ITEMS"),
            "BT_TEST_PLAN_MAX_WORKERS": os.environ.get("BT_TEST_PLAN_MAX_WORKERS"),
        }
        try:
            os.environ["BT_TEST_PLAN_DISABLE"] = "0"
            os.environ["BT_TEST_PLAN_MIN_ITEMS"] = "2"
            os.environ["BT_TEST_PLAN_MAX_WORKERS"] = "2"
            plan = choose_process_batch_plan(
                item_count=5,
                disable_env="BT_TEST_PLAN_DISABLE",
                min_items_envs=("BT_TEST_PLAN_MIN_ITEMS",),
                default_min_items=2,
                max_workers_envs=("BT_TEST_PLAN_MAX_WORKERS",),
                sequential_mode="seq",
                parallel_mode="par",
            )
        finally:
            for key, value in old_env.items():
                if value is None:
                    os.environ.pop(key, None)
                else:
                    os.environ[key] = value

        self.assertTrue(plan.parallel)
        self.assertEqual(plan.execution_mode, "par")
        self.assertEqual(plan.worker_count, 2)
        self.assertEqual(plan.task_count, 3)
        self.assertEqual(plan.batch_size, 2)
        self.assertEqual(split_batches([1, 2, 3, 4, 5], batch_size=plan.batch_size), [[1, 2], [3, 4], [5]])

    def test_choose_process_batch_plan_falls_back_to_sequential_when_disabled(self) -> None:
        old_env = {"BT_TEST_PLAN_DISABLE": os.environ.get("BT_TEST_PLAN_DISABLE")}
        try:
            os.environ["BT_TEST_PLAN_DISABLE"] = "1"
            plan = choose_process_batch_plan(
                item_count=8,
                disable_env="BT_TEST_PLAN_DISABLE",
                min_items_envs=("BT_TEST_PLAN_MIN_ITEMS",),
                default_min_items=2,
                max_workers_envs=("BT_TEST_PLAN_MAX_WORKERS",),
                sequential_mode="seq",
                parallel_mode="par",
            )
        finally:
            for key, value in old_env.items():
                if value is None:
                    os.environ.pop(key, None)
                else:
                    os.environ[key] = value

        self.assertFalse(plan.parallel)
        self.assertEqual(plan.execution_mode, "seq")
        self.assertEqual(plan.worker_count, 1)
        self.assertEqual(plan.task_count, 8)
        self.assertEqual(plan.batch_size, 1)


if __name__ == "__main__":
    unittest.main()
