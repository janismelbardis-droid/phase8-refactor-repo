from __future__ import annotations

import math
import os
import pickle
from pathlib import Path
import subprocess
import sys
from typing import Any, Dict, Mapping, Optional

import pandas as pd

from app.backtest import make_streams_htf_closed_only
from app.backtest_models import BacktestConfig
from app.fast_backtest import (
    CompiledBarPlan,
    _compiled_replay_plan,
    build_compiled_bar_plan,
)
from app.stream_bundle_loader import load_stream_bundle_library_first
from app.strategy_compiler import FAST_BAR_ENGINE
from app.strategy_requirements import compile_stream_requirements

_WORKER_REPLAY_SOURCE: Optional[Dict[str, Any]] = None
_WORKER_BASE_CFG: Optional[BacktestConfig] = None
_CPU_INFO_CACHE_FAST: Optional[Dict[str, Optional[int]]] = None
_CPU_INFO_CACHE_EXACT: Optional[Dict[str, Optional[int]]] = None

AUTO_COMBOS_PER_PHYSICAL_WORKER = 24
AUTO_COMBOS_PER_LOGICAL_WORKER = 48
AUTO_MIN_PARALLEL_COMBOS = 32


def write_worker_replay_source(blob_path: Path, replay_context: Mapping[str, Any]) -> int:
    blob_path.parent.mkdir(parents=True, exist_ok=True)
    with blob_path.open("wb") as fh:
        pickle.dump({"replay_context": replay_context}, fh, protocol=pickle.HIGHEST_PROTOCOL)
    return int(blob_path.stat().st_size)


def compact_compiled_worker_replay_context(replay_context: Mapping[str, Any]) -> Dict[str, Any]:
    compact = dict(replay_context or {})
    plan = compact.get("__compiled_bar_plan")
    if not isinstance(plan, CompiledBarPlan):
        raise ValueError("Replay context does not contain a CompiledBarPlan.")
    compact["__compiled_bar_plan"] = _compiled_replay_plan(plan)
    return compact


def detect_cpu_counts(*, fast: bool = True) -> Dict[str, Optional[int]]:
    global _CPU_INFO_CACHE_FAST, _CPU_INFO_CACHE_EXACT
    if fast and _CPU_INFO_CACHE_FAST is not None:
        return dict(_CPU_INFO_CACHE_FAST)
    if (not fast) and _CPU_INFO_CACHE_EXACT is not None:
        return dict(_CPU_INFO_CACHE_EXACT)

    logical = max(1, int(os.cpu_count() or 1))
    if fast:
        _CPU_INFO_CACHE_FAST = {"logical": int(logical), "physical": None}
        return dict(_CPU_INFO_CACHE_FAST)

    physical: Optional[int] = None

    if sys.platform.startswith("win"):
        try:
            command = (
                "$sum = (Get-CimInstance Win32_Processor | "
                "Measure-Object -Property NumberOfCores -Sum).Sum; "
                "Write-Output $sum"
            )
            result = subprocess.run(
                ["powershell", "-NoProfile", "-Command", command],
                check=False,
                capture_output=True,
                text=True,
                timeout=5,
            )
            if result.returncode == 0:
                text = str(result.stdout or "").strip()
                if text:
                    physical = int(float(text))
        except Exception:
            physical = None

    if physical is not None:
        physical = max(1, min(int(physical), logical))

    payload = {
        "logical": int(logical),
        "physical": int(physical) if physical is not None else None,
    }
    _CPU_INFO_CACHE_EXACT = payload
    return dict(payload)


def choose_auto_workers(combo_count: int, *, logical_cpus: int, physical_cpus: Optional[int] = None) -> int:
    combos = max(0, int(combo_count))
    logical = max(1, int(logical_cpus or 1))
    physical = max(1, min(int(physical_cpus or logical), logical))

    if combos <= 1:
        return 1
    if combos < AUTO_MIN_PARALLEL_COMBOS:
        return 1

    workers = max(1, min(physical, int(math.ceil(combos / AUTO_COMBOS_PER_PHYSICAL_WORKER))))

    if logical > physical and combos >= logical * AUTO_COMBOS_PER_LOGICAL_WORKER:
        workers = max(
            workers,
            min(logical, int(math.ceil(combos / AUTO_COMBOS_PER_LOGICAL_WORKER))),
        )

    return max(1, min(workers, logical, combos))


def choose_auto_chunk_size(combo_count: int, workers: int) -> int:
    combos = max(0, int(combo_count))
    worker_count = max(1, int(workers or 1))
    if combos <= 0:
        return 1
    return max(1, int(math.ceil(combos / worker_count)))


def init_worker_replay_state(blob_path: str, base_cfg_payload: Mapping[str, Any]) -> None:
    global _WORKER_REPLAY_SOURCE, _WORKER_BASE_CFG

    with Path(blob_path).open("rb") as fh:
        replay_source = pickle.load(fh)

    if not isinstance(replay_source, dict) or "replay_context" not in replay_source:
        raise ValueError("Replay worker bootstrap payload is invalid.")

    _WORKER_REPLAY_SOURCE = replay_source
    _WORKER_BASE_CFG = BacktestConfig(**dict(base_cfg_payload))


def init_worker_compiled_replay_state(spec_payload: Mapping[str, Any], base_cfg_payload: Mapping[str, Any]) -> None:
    global _WORKER_REPLAY_SOURCE, _WORKER_BASE_CFG

    cfg = BacktestConfig(**dict(base_cfg_payload))
    spec = dict(spec_payload or {})

    symbol = str(spec.get("symbol") or "")
    start = pd.to_datetime(spec.get("start"), utc=True, errors="coerce")
    end = pd.to_datetime(spec.get("end"), utc=True, errors="coerce")
    warmup_start = pd.to_datetime(spec.get("warmup_start"), utc=True, errors="coerce")
    if pd.isna(start) or pd.isna(end) or pd.isna(warmup_start):
        raise ValueError("Compiled replay worker spec has an invalid time window.")

    tfs = [str(tf) for tf in list(spec.get("tfs") or []) if str(tf or "").strip()]
    rules_model = dict(spec.get("rules_model") or {})
    tab_group_join_mode = dict(spec.get("tab_group_join_mode") or {})
    group_rule_join_mode = {
        str(key): list(value)
        for key, value in dict(spec.get("group_rule_join_mode") or {}).items()
    }
    entry_filters = dict(spec.get("entry_filters") or {})
    cache_dir = str(spec.get("cache_dir") or "")

    req_spec = compile_stream_requirements(
        rules_model,
        entry_filters=entry_filters,
        backtest_cfg=cfg,
        include_plot_defaults=True,
    )
    bundle = load_stream_bundle_library_first(
        symbol=symbol,
        start=warmup_start,
        end=end,
        timeframes=tfs,
        cache_dir=Path(cache_dir),
        price_source=str(spec.get("price_source") or "LAST").upper(),
        macd_impl=str(spec.get("macd_impl") or "TRADINGVIEW").upper(),
        adx_impl=str(spec.get("adx_impl") or "TRADINGVIEW").upper(),
        required_fields=req_spec,
        progress_cb=None,
    )
    df_1m = bundle.df_1m
    streams_full = bundle.streams_full
    transformed_streams = dict(streams_full or {})
    if bool(spec.get("apply_htf_closed_only")):
        transformed_streams = make_streams_htf_closed_only(transformed_streams)

    plan = build_compiled_bar_plan(
        symbol=symbol,
        streams_full=transformed_streams,
        df_1m_full=df_1m,
        start=start,
        end=end,
        rules_model=rules_model,
        tab_group_join_mode=tab_group_join_mode,
        group_rule_join_mode=group_rule_join_mode,
        cfg=cfg,
        entry_filters=entry_filters,
        streams_already_htf_closed_only=True,
    )

    _WORKER_REPLAY_SOURCE = {
        "replay_context": {
            "symbol": symbol,
            "start": start,
            "end": end,
            "rules_model": rules_model,
            "tab_group_join_mode": tab_group_join_mode,
            "group_rule_join_mode": group_rule_join_mode,
            "entry_filters": entry_filters,
            "__replay_engine": FAST_BAR_ENGINE,
            "__compiled_bar_plan": plan,
        }
    }
    _WORKER_BASE_CFG = cfg


def get_worker_replay_source() -> Dict[str, Any]:
    if _WORKER_REPLAY_SOURCE is None:
        raise RuntimeError("Replay worker state has not been initialized.")
    return _WORKER_REPLAY_SOURCE


def get_worker_base_cfg() -> BacktestConfig:
    if _WORKER_BASE_CFG is None:
        raise RuntimeError("Replay worker config has not been initialized.")
    return _WORKER_BASE_CFG
