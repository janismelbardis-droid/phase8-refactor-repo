from __future__ import annotations

import math
import os
from dataclasses import dataclass
from typing import Any, List, Sequence


@dataclass(frozen=True)
class ProcessBatchPlan:
    execution_mode: str
    worker_count: int
    task_count: int
    batch_size: int
    parallel: bool


def env_flag(name: str) -> bool:
    raw = str(name or "").strip()
    if not raw:
        return False
    try:
        value = str(os.getenv(raw, "0")).strip().upper()
    except Exception:
        value = "0"
    return value in {"1", "TRUE", "YES", "Y", "ON"}


def env_int(names: Sequence[str], *, default: int, minimum: int = 0) -> int:
    for raw_name in list(names or []):
        name = str(raw_name or "").strip()
        if not name:
            continue
        try:
            value = int(str(os.getenv(name, "")).strip())
            return max(int(minimum), int(value))
        except Exception:
            continue
    return max(int(minimum), int(default))


def resolve_worker_count(item_count: int, *, env_names: Sequence[str], reserve_cpu: int = 1) -> int:
    item_count = max(0, int(item_count or 0))
    if item_count <= 0:
        return 1
    try:
        configured = env_int(env_names, default=0, minimum=0)
        if configured > 0:
            limit = configured
        else:
            cpu_total = int(os.cpu_count() or 1)
            limit = max(1, cpu_total - max(0, int(reserve_cpu or 0)))
    except Exception:
        limit = 1
    return max(1, min(item_count, limit))


def choose_process_batch_plan(
    *,
    item_count: int,
    disable_env: str,
    min_items_envs: Sequence[str],
    default_min_items: int,
    max_workers_envs: Sequence[str],
    sequential_mode: str,
    parallel_mode: str,
    allow_parallel: bool = True,
    target_tasks_per_worker: int = 2,
    reserve_cpu: int = 1,
) -> ProcessBatchPlan:
    item_count = max(0, int(item_count or 0))
    if item_count <= 0:
        return ProcessBatchPlan(
            execution_mode=str(sequential_mode or "sequential"),
            worker_count=1,
            task_count=0,
            batch_size=1,
            parallel=False,
        )

    min_items = env_int(min_items_envs, default=int(default_min_items or 1), minimum=1)
    if (not allow_parallel) or env_flag(disable_env) or item_count < min_items:
        return ProcessBatchPlan(
            execution_mode=str(sequential_mode or "sequential"),
            worker_count=1,
            task_count=item_count,
            batch_size=1,
            parallel=False,
        )

    worker_count = resolve_worker_count(item_count, env_names=max_workers_envs, reserve_cpu=reserve_cpu)
    if worker_count <= 1:
        return ProcessBatchPlan(
            execution_mode=str(sequential_mode or "sequential"),
            worker_count=1,
            task_count=item_count,
            batch_size=1,
            parallel=False,
        )

    tasks_per_worker = max(1, int(target_tasks_per_worker or 1))
    target_task_count = max(worker_count, min(item_count, worker_count * tasks_per_worker))
    batch_size = max(1, int(math.ceil(item_count / max(1, target_task_count))))
    task_count = int(math.ceil(item_count / max(1, batch_size)))
    return ProcessBatchPlan(
        execution_mode=str(parallel_mode or "parallel"),
        worker_count=worker_count,
        task_count=task_count,
        batch_size=batch_size,
        parallel=True,
    )


def split_batches(items: Sequence[Any], *, batch_size: int) -> List[List[Any]]:
    size = max(1, int(batch_size or 1))
    seq = list(items or [])
    return [seq[idx: idx + size] for idx in range(0, len(seq), size)]
