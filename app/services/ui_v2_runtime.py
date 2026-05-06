from __future__ import annotations

import hashlib
import json
import math
import os
import sys
import threading
import time
import uuid
from concurrent.futures import ProcessPoolExecutor, as_completed
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Tuple

import pandas as pd

from app.backtest import _decision_mask_for_step_tf, _normalize_step_timeframe, run_backtest_tick
from app.backtest_frames import _RowSnapshotCache, _build_eval_snapshots_for_row, _prev_source_row_index_for_df
from app.backtest_models import BacktestConfig, BacktestResult
from app.backtest_reporting import _make_trade_df
from app.constants import (
    ALL_TIMEFRAMES,
    BACKTEST_MODE_BAR_1M_HTF_CLOSED_ONLY,
    BACKTEST_MODE_TICK,
)
from app.execution_planner import (
    choose_process_batch_plan,
    env_flag as execution_env_flag,
    env_int as execution_env_int,
    resolve_worker_count,
    split_batches,
)
from app.ema_settings import (
    coerce_ema_pair as _coerce_ema_pair,
    normalize_ema_settings as _normalize_ema_settings_payload,
    resolve_ema_settings as _resolve_ema_settings_payload,
)
from app.rsi_settings import (
    coerce_rsi_config as _coerce_rsi_config,
    normalize_rsi_settings as _normalize_rsi_settings_payload,
    resolve_rsi_settings as _resolve_rsi_settings_payload,
)
from app.ema_pair import build_ema_pair_frame
from app.fast_backtest import run_backtest_auto
from app.stream_bundle_loader import load_stream_bundle_library_first
from app.research.market_context_audit import (
    build_market_context_audit_window,
    build_timeframe_context_summary,
    required_market_audit_fields,
)
from app.research.replay import replay_backtest_result
from app.rules import (
    EntryFilterConfig,
    Rule,
    RuleEvalContext,
    eval_tab_matching_groups,
    normalize_entry_filter_payload,
    sequence_rule_partition,
)
from app.strategy_requirements import compile_stream_requirements
from app.utils_time import parse_utc, required_warmup_minutes


REPO_ROOT = Path(__file__).resolve().parents[2]
PRESETS_PATH = REPO_ROOT / "app" / "presets.json"


ProgressCallback = Callable[[str, Optional[float]], None]

EXIT_SWEEP_PARALLEL_MIN_COMBOS_DEFAULT = 16
BACKTEST_BATCH_PARALLEL_MIN_CASES_DEFAULT = 2


def _null_progress(_: str, __: Optional[float] = None) -> None:
    return


def _env_flag(name: str) -> bool:
    return execution_env_flag(name)


def _spawn_parallel_supported() -> bool:
    """Return True when the current Python entrypoint is safe for Windows spawn.

    ProcessPoolExecutor on Windows uses spawn. That requires a real importable
    __main__ file. Interactive contexts such as stdin / -c / notebook cells
    often expose pseudo-files like "<stdin>", which makes worker startup fail.
    """
    try:
        main_mod = sys.modules.get("__main__")
        main_file = str(getattr(main_mod, "__file__", "") or "").strip()
    except Exception:
        main_file = ""
    if not main_file:
        return False
    if main_file.startswith("<") and main_file.endswith(">"):
        return False
    return True


def _exit_sweep_parallel_min_combos() -> int:
    return execution_env_int(
        ("BT_EXIT_SWEEP_PARALLEL_MIN_COMBOS", "EXIT_SWEEP_PARALLEL_MIN_COMBOS"),
        default=EXIT_SWEEP_PARALLEL_MIN_COMBOS_DEFAULT,
        minimum=1,
    )


def _exit_sweep_worker_count(combo_count: int) -> int:
    return resolve_worker_count(
        combo_count,
        env_names=("BT_EXIT_SWEEP_MAX_WORKERS", "EXIT_SWEEP_MAX_WORKERS"),
        reserve_cpu=1,
    )


def _backtest_batch_parallel_min_cases() -> int:
    return execution_env_int(
        ("BT_BACKTEST_BATCH_PARALLEL_MIN_CASES", "BACKTEST_BATCH_PARALLEL_MIN_CASES"),
        default=BACKTEST_BATCH_PARALLEL_MIN_CASES_DEFAULT,
        minimum=1,
    )


def _backtest_batch_worker_count(case_count: int) -> int:
    return resolve_worker_count(
        case_count,
        env_names=("BT_BACKTEST_BATCH_MAX_WORKERS", "BACKTEST_BATCH_MAX_WORKERS"),
        reserve_cpu=1,
    )


def _exit_sweep_cfg_for_combo(
    base_cfg: BacktestConfig,
    *,
    stop_loss_pct: float,
    take_profit_pct: float,
    break_even_pct: float,
    trailing_stop_pct: float,
) -> BacktestConfig:
    next_cfg = BacktestConfig(**asdict(base_cfg))

    stop_loss_pct = max(0.0, float(stop_loss_pct))
    take_profit_pct = max(0.0, float(take_profit_pct))
    break_even_pct = max(0.0, float(break_even_pct))
    trailing_stop_pct = max(0.0, float(trailing_stop_pct))

    next_cfg.stop_loss_pct = stop_loss_pct
    next_cfg.stop_loss_mode = "PERCENT" if stop_loss_pct > 0.0 else "OFF"
    next_cfg.stop_loss_value = stop_loss_pct if stop_loss_pct > 0.0 else 0.0
    if stop_loss_pct <= 0.0:
        next_cfg.stop_loss_field = ""

    next_cfg.take_profit_pct = take_profit_pct
    next_cfg.take_profit_mode = "PERCENT" if take_profit_pct > 0.0 else "OFF"
    next_cfg.take_profit_value = take_profit_pct if take_profit_pct > 0.0 else 0.0
    if take_profit_pct <= 0.0:
        next_cfg.take_profit_field = ""

    next_cfg.break_even_after_mfe_pct = break_even_pct
    next_cfg.trailing_stop_pct = trailing_stop_pct
    return next_cfg


def _exit_sweep_row_from_summary(
    *,
    stop_loss_pct: float,
    take_profit_pct: float,
    break_even_pct: float,
    trailing_stop_pct: float,
    summary: Dict[str, Any],
) -> Dict[str, Any]:
    exit_counts = dict(summary.get("exit_reason_counts") or {})
    trade_count = int(
        summary.get("num_trades")
        or summary.get("trades")
        or (int(summary.get("wins") or 0) + int(summary.get("losses") or 0))
        or 0
    )
    return {
        "stop_loss_pct": float(stop_loss_pct),
        "take_profit_pct": float(take_profit_pct),
        "break_even_after_mfe_pct": float(break_even_pct),
        "trailing_stop_pct": float(trailing_stop_pct),
        "stop_loss_label": "OFF" if float(stop_loss_pct) <= 0.0 else f"{float(stop_loss_pct):.4f}",
        "take_profit_label": "OFF" if float(take_profit_pct) <= 0.0 else f"{float(take_profit_pct):.4f}",
        "break_even_label": "OFF" if float(break_even_pct) <= 0.0 else f"{float(break_even_pct):.4f}",
        "trailing_stop_label": "OFF" if float(trailing_stop_pct) <= 0.0 else f"{float(trailing_stop_pct):.4f}",
        "net_profit": _safe_round(summary.get("net_profit"), 6),
        "return_pct": _safe_round(summary.get("return_pct"), 6),
        "ending_balance": _safe_round(summary.get("ending_balance"), 6),
        "profit_factor": _safe_round(summary.get("profit_factor"), 6),
        "win_rate_pct": _safe_round(summary.get("win_rate_pct"), 6),
        "max_drawdown_pct": _safe_round(summary.get("max_drawdown_pct"), 6),
        "num_trades": trade_count,
        "stop_loss_count": int(summary.get("stop_loss_count") or 0),
        "take_profit_count": int(summary.get("take_profit_count") or 0),
        "break_even_count": int(summary.get("break_even_count") or 0),
        "trailing_stop_count": int(summary.get("trailing_stop_count") or 0),
        "force_close_count": int(exit_counts.get("Force Close (End)") or 0),
    }


def _run_exit_sweep_combo_batch(task: Dict[str, Any]) -> List[Dict[str, Any]]:
    base_result: BacktestResult = task["base_result"]
    equity_curve_stride = max(1, int(task.get("equity_curve_stride") or 1))
    rows: List[Dict[str, Any]] = []
    for stop_loss_pct, take_profit_pct, break_even_pct, trailing_stop_pct in list(task.get("combos") or []):
        next_cfg = _exit_sweep_cfg_for_combo(
            base_result.config,
            stop_loss_pct=float(stop_loss_pct),
            take_profit_pct=float(take_profit_pct),
            break_even_pct=float(break_even_pct),
            trailing_stop_pct=float(trailing_stop_pct),
        )
        replayed = replay_backtest_result(
            base_result,
            cfg=next_cfg,
            capture_trade_details=False,
            equity_curve_stride=equity_curve_stride,
        )
        rows.append(
            _exit_sweep_row_from_summary(
                stop_loss_pct=float(stop_loss_pct),
                take_profit_pct=float(take_profit_pct),
                break_even_pct=float(break_even_pct),
                trailing_stop_pct=float(trailing_stop_pct),
                summary=dict(replayed.summary or {}),
            )
        )
    return rows


def _strip_backtest_batch_payload(payload: Dict[str, Any]) -> Dict[str, Any]:
    raw = dict(payload or {})
    return {
        "result_kind": str(raw.get("result_kind") or "backtest"),
        "run_id": str(raw.get("run_id") or ""),
        "preset_name": str(raw.get("preset_name") or ""),
        "symbol": str(raw.get("symbol") or ""),
        "start": str(raw.get("start") or ""),
        "end": str(raw.get("end") or ""),
        "warmup_minutes": int(raw.get("warmup_minutes") or 0),
        "config": dict(raw.get("config") or {}),
        "summary": dict(raw.get("summary") or {}),
        "verification_bundle": str(raw.get("verification_bundle") or ""),
        "exit_sweep": raw.get("exit_sweep"),
        "exit_sweep_error": raw.get("exit_sweep_error"),
    }


def _backtest_batch_row_from_payload(*, label: str, case_index: int, payload: Dict[str, Any]) -> Dict[str, Any]:
    summary = dict((payload or {}).get("summary") or {})
    trade_count = int(
        summary.get("num_trades")
        or summary.get("trades")
        or (int(summary.get("wins") or 0) + int(summary.get("losses") or 0))
        or 0
    )
    return {
        "label": str(label or f"Case {int(case_index)}"),
        "case_index": int(case_index),
        "run_id": str((payload or {}).get("run_id") or ""),
        "net_profit": _safe_round(summary.get("net_profit"), 6),
        "return_pct": _safe_round(summary.get("return_pct"), 6),
        "ending_balance": _safe_round(summary.get("ending_balance"), 6),
        "profit_factor": _safe_round(summary.get("profit_factor"), 6),
        "win_rate_pct": _safe_round(summary.get("win_rate_pct"), 6),
        "max_drawdown_pct": _safe_round(summary.get("max_drawdown_pct"), 6),
        "num_trades": trade_count,
        "verification_bundle": str((payload or {}).get("verification_bundle") or ""),
    }


def _run_saved_preset_batch_case(task: Dict[str, Any]) -> Dict[str, Any]:
    runtime = BacktestRuntimeService(
        presets_path=Path(str(task["presets_path"])),
        repo_root=Path(str(task["repo_root"])),
    )
    result = runtime.run_saved_preset(
        preset_name=str(task["preset_name"]),
        overrides=RunOverrides.from_payload(task.get("overrides") if isinstance(task.get("overrides"), dict) else {}),
        progress_cb=None,
    )
    return {
        "label": str(task.get("label") or ""),
        "case_index": int(task.get("case_index") or 0),
        "payload": result,
    }


def _run_saved_preset_batch_case_batch(task: Dict[str, Any]) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    for case in list(task.get("cases") or []):
        if not isinstance(case, dict):
            continue
        case_task = dict(case)
        case_task.setdefault("repo_root", task.get("repo_root"))
        case_task.setdefault("presets_path", task.get("presets_path"))
        case_task.setdefault("preset_name", task.get("preset_name"))
        rows.append(_run_saved_preset_batch_case(case_task))
    return rows


@dataclass
class SnapshotCacheEntry:
    payload: Dict[str, Any]
    created_monotonic: float
    minute_bucket: str


SNAPSHOT_REQUIRED_FIELDS = {
    "price",
    "ema_1",
    "ema_2",
    "ema_stack",
    "ema_cross_up",
    "ema_cross_down",
    "ema_1_slope",
    "ema_2_slope",
    "ema_1_angle_state",
    "ema_2_angle_state",
    "ema_spread",
    "ema_spread_pct",
    "ema_spread_delta",
    "ema_spread_pct_delta",
    "ema_spread_trend",
    "ema_stack_pressure",
    "macd",
    "signal",
    "rsi",
    "rsi_smooth",
    "rsi_state",
    "stoch_rsi_k",
    "stoch_rsi_d",
    "stoch_rsi_kd",
    "ms",
    "angle",
    "sig_angle",
    "ppo",
    "flip_age",
    "adx",
    "adx_angle",
    "atr",
    "atr_angle",
    "di_plus",
    "di_minus",
    "di_spread",
    "di_cross",
    "taker_buy_volume",
    "taker_sell_volume",
    "taker_delta_volume",
    "taker_delta_pct",
    "taker_buy_share_pct",
    "taker_trade_count",
    "taker_bias_state",
    "frama_state",
    "frama_break_up",
    "frama_break_down",
    "frama_mid_cross",
    "vidya_state",
    "vidya_line",
    "vidya_trend_up",
    "vidya_trend_down",
    "vidya_delta_pct",
    "vidya_slope",
    "vidya_angle_deg_norm",
    "vidya_angle_state",
    "vidya_angle_accel",
    "range_filter_state",
    "range_filter_phase",
    "range_filter_buy",
    "range_filter_sell",
    "range_filter_line",
    "range_filter_smooth_range",
    "range_filter_upward_count",
    "range_filter_downward_count",
}

MARKET_AUDIT_DEFAULT_BARS = 300
MARKET_AUDIT_MAX_BARS = 1200
TIMEFRAME_TO_MINUTES = {
    "1m": 1,
    "5m": 5,
    "15m": 15,
    "30m": 30,
    "1h": 60,
}


def _serialize_snapshot_value(value: Any) -> Any:
    try:
        if pd.isna(value):
            return None
    except Exception:
        pass
    if value is None:
        return None
    if isinstance(value, (str, bool)):
        return value
    if isinstance(value, int):
        return int(value)
    if isinstance(value, float):
        if math.isfinite(value):
            return float(value)
        return None
    try:
        as_ts = pd.to_datetime(value, utc=True, errors="raise")
        if pd.notna(as_ts):
            return str(as_ts)
    except Exception:
        pass
    try:
        scalar = value.item()
        if isinstance(scalar, bool):
            return bool(scalar)
        if isinstance(scalar, int):
            return int(scalar)
        if isinstance(scalar, float):
            return float(scalar) if math.isfinite(float(scalar)) else None
        return scalar
    except Exception:
        return str(value)


def _json_safe_value(value: Any) -> Any:
    try:
        if pd.isna(value):
            return None
    except Exception:
        pass
    if value is None or isinstance(value, (str, bool, int)):
        return value
    if isinstance(value, float):
        return float(value) if math.isfinite(value) else None
    if isinstance(value, dict):
        return {key: _json_safe_value(item) for key, item in value.items()}
    if isinstance(value, (list, tuple, set, frozenset)):
        return [_json_safe_value(item) for item in value]
    try:
        scalar = value.item()
    except Exception:
        scalar = None
    if scalar is not None and scalar is not value:
        return _json_safe_value(scalar)
    try:
        numeric = float(value)
    except Exception:
        return str(value)
    return numeric if math.isfinite(numeric) else None


def _latest_event_age_bars(frame: pd.DataFrame, column: str) -> Optional[int]:
    if not isinstance(frame, pd.DataFrame) or frame.empty or column not in frame.columns:
        return None
    try:
        flags = pd.Series(frame[column]).fillna(False).astype(bool).tolist()
    except Exception:
        return None
    last_hit: Optional[int] = None
    for idx, fired in enumerate(flags):
        if bool(fired):
            last_hit = idx
    if last_hit is None:
        return None
    return int((len(flags) - 1) - last_hit)


def _latest_state_age_bars(frame: pd.DataFrame, column: str) -> Optional[int]:
    if not isinstance(frame, pd.DataFrame) or frame.empty or column not in frame.columns:
        return None
    try:
        raw_values = pd.Series(frame[column]).tolist()
    except Exception:
        return None
    normalized: List[str] = []
    for value in raw_values:
        try:
            if pd.isna(value):
                normalized.append("")
                continue
        except Exception:
            pass
        normalized.append(str(value or "").strip().upper())
    if not normalized:
        return None
    current = normalized[-1]
    if not current:
        return None
    run_len = 0
    for value in reversed(normalized):
        if value != current:
            break
        run_len += 1
    if run_len <= 0:
        return None
    return int(run_len - 1)


def _latest_state_entry_age_bars(frame: pd.DataFrame, column: str, target_state: str) -> Optional[int]:
    if not isinstance(frame, pd.DataFrame) or frame.empty or column not in frame.columns:
        return None
    target = str(target_state or "").strip().upper()
    if target not in {"BUY", "SELL", "NEUTRAL"}:
        return None
    try:
        raw_values = pd.Series(frame[column]).tolist()
    except Exception:
        return None
    normalized: List[str] = []
    for value in raw_values:
        try:
            if pd.isna(value):
                normalized.append("")
                continue
        except Exception:
            pass
        normalized.append(str(value or "").strip().upper())
    if not normalized:
        return None
    prev_state = ""
    last_entry_idx: Optional[int] = None
    for idx, state in enumerate(normalized):
        if not state:
            continue
        if state == target and prev_state != target:
            last_entry_idx = idx
        prev_state = state
    if last_entry_idx is None:
        return None
    return int((len(normalized) - 1) - last_entry_idx)


def _latest_directional_event_side_age(
    frame: pd.DataFrame,
    up_column: str,
    down_column: str,
) -> Tuple[Optional[str], Optional[int]]:
    if not isinstance(frame, pd.DataFrame) or frame.empty:
        return None, None
    if up_column not in frame.columns and down_column not in frame.columns:
        return None, None
    try:
        up_flags = pd.Series(frame.get(up_column)).fillna(False).astype(bool).tolist()
    except Exception:
        up_flags = [False] * int(len(frame.index))
    try:
        down_flags = pd.Series(frame.get(down_column)).fillna(False).astype(bool).tolist()
    except Exception:
        down_flags = [False] * int(len(frame.index))
    last_side: Optional[str] = None
    last_age: Optional[int] = None
    for idx, (up_fired, down_fired) in enumerate(zip(up_flags, down_flags)):
        if bool(up_fired) and not bool(down_fired):
            last_side = "BUY"
            last_age = int((len(up_flags) - 1) - idx)
        elif bool(down_fired) and not bool(up_fired):
            last_side = "SELL"
            last_age = int((len(down_flags) - 1) - idx)
    return last_side, last_age


def _coerce_range_cond_ini(value: Any) -> Optional[int]:
    try:
        if value is None:
            return None
        return int(float(value))
    except Exception:
        return None


def _range_display_state(state: Any, phase: Any, cond_ini: Any) -> Optional[str]:
    state_text = str(state or "").strip().upper()
    phase_text = str(phase or "").strip().upper()
    cond_ini_i = _coerce_range_cond_ini(cond_ini)
    if state_text in ("BUY", "SELL", "NEUTRAL"):
        return state_text
    if cond_ini_i == 1:
        return "BUY"
    if cond_ini_i == -1:
        return "SELL"
    if phase_text.startswith("BUY"):
        return "BUY"
    if phase_text.startswith("SELL"):
        return "SELL"
    return None


def _load_presets(path: Path) -> Dict[str, Any]:
    with path.open("r", encoding="utf-8") as fh:
        payload = json.load(fh)
    if not isinstance(payload, dict):
        raise ValueError("Preset file must contain a JSON object.")
    return payload


def _pick_preset_name(payload: Dict[str, Any], requested_name: str | None) -> str:
    presets = payload.get("presets", {})
    if not isinstance(presets, dict) or not presets:
        raise ValueError("No presets found.")
    if requested_name:
        if requested_name not in presets:
            raise ValueError(f"Preset '{requested_name}' was not found.")
        return requested_name
    meta = payload.get("meta", {})
    if isinstance(meta, dict):
        last_name = str(meta.get("last_preset") or "").strip()
        if last_name in presets:
            return last_name
    return sorted(presets.keys())[0]


def _rule_from_dict(raw: Dict[str, Any]) -> Rule:
    return Rule(
        timeframe=str(raw.get("timeframe", "1m") or "1m"),
        mode=str(raw.get("mode", "event") or "event"),
        field=str(raw.get("field", "") or ""),
        op=str(raw.get("op", "EVENT") or "EVENT"),
        value=raw.get("value"),
        is_trigger=bool(raw.get("is_trigger", False)),
        bars_ago_mode=str(raw.get("bars_ago_mode", "OFF") or "OFF"),
        bars_ago_n=int(raw.get("bars_ago_n", 0) or 0),
        require_still_valid=bool(raw.get("require_still_valid", False)),
        is_final_gate=bool(raw.get("is_final_gate", False)),
        is_sequence_canceler=bool(raw.get("is_sequence_canceler", False)),
        window_bars=int(raw.get("window_bars", 1) or 1),
        compare_to=raw.get("compare_to"),
    )


def _materialize_preset(
    preset: Dict[str, Any],
) -> Tuple[Dict[str, Any], Dict[str, str], Dict[str, List[str]], Dict[str, Any]]:
    tabs = preset.get("tabs", {})
    if not isinstance(tabs, dict):
        raise ValueError("Preset tabs payload is invalid.")

    rules_model: Dict[str, Any] = {}
    tab_group_join_mode: Dict[str, str] = {}
    group_rule_join_mode: Dict[str, List[str]] = {}

    for tab_name, tab_payload in tabs.items():
        if not isinstance(tab_payload, dict):
            continue
        tab_group_join_mode[str(tab_name)] = str(tab_payload.get("groups_join", "AND") or "AND")
        groups_payload = tab_payload.get("groups", [])
        groups: List[List[Rule]] = []
        group_joins: List[str] = []
        if isinstance(groups_payload, list):
            for group_payload in groups_payload:
                if not isinstance(group_payload, dict):
                    continue
                group_joins.append(str(group_payload.get("rules_join", "OR") or "OR"))
                raw_rules = group_payload.get("rules", [])
                rules = []
                if isinstance(raw_rules, list):
                    for raw_rule in raw_rules:
                        if isinstance(raw_rule, dict):
                            rules.append(_rule_from_dict(raw_rule))
                groups.append(rules)
        rules_model[str(tab_name)] = groups
        group_rule_join_mode[str(tab_name)] = group_joins

    raw_filters = preset.get("entry_filters", {})
    normalized_filters = normalize_entry_filter_payload(raw_filters if isinstance(raw_filters, dict) else {})
    entry_filters: Dict[str, Any] = {}
    for tab_name in ("Long Entry", "Short Entry"):
        tab_payload = normalized_filters.get(tab_name, {"default": EntryFilterConfig(), "groups": {}})
        default_cfg = (
            tab_payload.get("default", EntryFilterConfig()).normalized_copy()
            if isinstance(tab_payload.get("default"), EntryFilterConfig)
            else EntryFilterConfig()
        )
        # Emit the legacy/raw payload shape expected by the backtest runtime.
        # The runtime normalizes entry filters again, so returning the already
        # normalized {"default": ..., "groups": ...} structure would silently
        # drop the tab-level default config and turn active filters OFF.
        tab_cfg = default_cfg.to_dict()
        tab_cfg["groups"] = {}
        raw_group_filters = tab_payload.get("groups", {})
        tab_groups = tabs.get(tab_name, {}).get("groups", []) if isinstance(tabs.get(tab_name, {}), dict) else []
        group_key_to_index: Dict[str, int] = {}
        if isinstance(tab_groups, list):
            for group_index, group in enumerate(tab_groups):
                try:
                    group_key = str((group or {}).get("group_key") or "").strip()
                except Exception:
                    group_key = ""
                if group_key:
                    group_key_to_index[group_key.upper()] = int(group_index)
        if isinstance(raw_group_filters, dict):
            for raw_group_key, cfg in raw_group_filters.items():
                lookup_key = str(raw_group_key or "").strip()
                if not lookup_key:
                    continue
                group_index: Optional[int] = None
                try:
                    group_index = int(lookup_key)
                except Exception:
                    group_index = group_key_to_index.get(lookup_key.upper())
                if group_index is None or group_index < 0:
                    continue
                if isinstance(cfg, EntryFilterConfig):
                    tab_cfg["groups"][str(group_index)] = cfg.normalized_copy().to_dict()
                elif isinstance(cfg, dict):
                    tab_cfg["groups"][str(group_index)] = EntryFilterConfig.from_dict(cfg).to_dict()
        entry_filters[tab_name] = tab_cfg

    return rules_model, tab_group_join_mode, group_rule_join_mode, entry_filters


def _normalize_timeframe_name(value: Any) -> str:
    tf = str(value or "").strip()
    return tf if tf in ALL_TIMEFRAMES else ""


def _ordered_timeframe_list(values: List[str]) -> List[str]:
    seen = set()
    ordered: List[str] = []
    for raw in values:
        tf = _normalize_timeframe_name(raw)
        if not tf or tf in seen:
            continue
        seen.add(tf)
        ordered.append(tf)
    return sorted(ordered, key=lambda item: ALL_TIMEFRAMES.index(item) if item in ALL_TIMEFRAMES else 999)


def _resolve_preset_timeframes(preset: Dict[str, Any]) -> List[str]:
    settings = preset.get("settings", {}) if isinstance(preset.get("settings", {}), dict) else {}
    detected: List[str] = []

    tabs = preset.get("tabs", {}) if isinstance(preset.get("tabs", {}), dict) else {}
    for tab_payload in tabs.values():
        if not isinstance(tab_payload, dict):
            continue
        groups = tab_payload.get("groups", [])
        if not isinstance(groups, list):
            continue
        for group_payload in groups:
            if not isinstance(group_payload, dict):
                continue
            raw_rules = group_payload.get("rules", [])
            if not isinstance(raw_rules, list):
                continue
            for raw_rule in raw_rules:
                tf_value = getattr(raw_rule, "timeframe", None) if isinstance(raw_rule, Rule) else None
                if tf_value is None and isinstance(raw_rule, dict):
                    tf_value = raw_rule.get("timeframe")
                detected.append(str(tf_value or ""))

    ema_settings = settings.get("ema_indicators", {}) if isinstance(settings.get("ema_indicators", {}), dict) else {}
    raw_overrides = ema_settings.get("overrides", {}) if isinstance(ema_settings.get("overrides", {}), dict) else {}
    detected.extend(str(tf or "") for tf in raw_overrides.keys())
    rsi_settings = settings.get("rsi_indicators", {}) if isinstance(settings.get("rsi_indicators", {}), dict) else {}
    raw_rsi_overrides = rsi_settings.get("overrides", {}) if isinstance(rsi_settings.get("overrides", {}), dict) else {}
    detected.extend(str(tf or "") for tf in raw_rsi_overrides.keys())

    ordered = _ordered_timeframe_list(detected)
    if ordered:
        return ordered

    legacy = [
        str(tf)
        for tf, enabled in (settings.get("timeframes", {}) or {}).items()
        if bool(enabled) and _normalize_timeframe_name(tf)
    ]
    ordered = _ordered_timeframe_list(legacy)
    return ordered or ["1m"]


def _sync_preset_timeframes(preset: Dict[str, Any]) -> List[str]:
    settings = preset.setdefault("settings", {})
    if not isinstance(settings, dict):
        preset["settings"] = {}
        settings = preset["settings"]
    timeframes = _resolve_preset_timeframes(preset)
    detected = set(timeframes)
    settings["timeframes"] = {tf: tf in detected for tf in ALL_TIMEFRAMES}
    return list(timeframes)


def _resolve_cache_dir(repo_root: Path, preferred: str) -> Path:
    first = Path(preferred)
    if not first.is_absolute():
        first = (repo_root / first).resolve()
    if first.exists():
        return first
    fallback = Path.home() / "Downloads" / "data_cache"
    if fallback.exists():
        return fallback
    return first


def _coerce_ema_lengths(raw_pair: Any, fallback: Optional[Dict[str, int]] = None) -> Dict[str, int]:
    return _coerce_ema_pair(raw_pair, fallback=fallback)


def _coerce_market_audit_bars(value: Any, default: int = MARKET_AUDIT_DEFAULT_BARS) -> int:
    try:
        bars = int(value)
    except Exception:
        bars = int(default)
    return max(1, min(MARKET_AUDIT_MAX_BARS, int(bars)))


def _timeframe_minutes(value: Any) -> int:
    tf = str(value or "").strip()
    return int(TIMEFRAME_TO_MINUTES.get(tf, 1))


def _normalize_ema_settings(settings: Dict[str, Any]) -> Dict[str, Any]:
    return _normalize_ema_settings_payload(settings)


def _resolve_ema_settings(settings: Dict[str, Any], timeframe: Optional[str] = None) -> Dict[str, int]:
    return _resolve_ema_settings_payload(settings, timeframe)


def _normalize_rsi_settings(settings: Dict[str, Any]) -> Dict[str, Any]:
    return _normalize_rsi_settings_payload(settings)


def _resolve_rsi_settings(settings: Dict[str, Any], timeframe: Optional[str] = None) -> Dict[str, int]:
    return _resolve_rsi_settings_payload(settings, timeframe)


def _augment_streams_with_ema(streams_full: Dict[str, pd.DataFrame], settings: Dict[str, Any]) -> Dict[str, pd.DataFrame]:
    out: Dict[str, pd.DataFrame] = {}
    for tf, frame in dict(streams_full or {}).items():
        if not isinstance(frame, pd.DataFrame) or frame.empty:
            out[str(tf)] = frame
            continue
        close_col = "close" if "close" in frame.columns else ("price" if "price" in frame.columns else None)
        if close_col is None:
            out[str(tf)] = frame
            continue
        close_ser = pd.to_numeric(frame[close_col], errors="coerce").astype(float)
        ema_settings = _resolve_ema_settings(settings, str(tf))
        next_frame = frame.copy()
        for key, series in build_ema_pair_frame(
            close_ser.to_numpy(dtype=float),
            frame.index,
            ema_1_length=int(ema_settings["ema_1_length"]),
            ema_2_length=int(ema_settings["ema_2_length"]),
        ).items():
            next_frame[key] = series
        out[str(tf)] = next_frame
    return out


def _build_timeframe_ohlcv_frame(df_1m: pd.DataFrame, timeframe: str) -> pd.DataFrame:
    if not isinstance(df_1m, pd.DataFrame) or df_1m.empty:
        return pd.DataFrame()
    required_cols = ("open", "high", "low", "close", "volume")
    if any(col not in df_1m.columns for col in required_cols):
        return pd.DataFrame()

    close_idx = pd.to_datetime(df_1m.get("close_time"), utc=True, errors="coerce")
    if close_idx.isna().all():
        return pd.DataFrame()
    ohlcv = pd.DataFrame(
        {
            "open": pd.to_numeric(df_1m.get("open"), errors="coerce"),
            "high": pd.to_numeric(df_1m.get("high"), errors="coerce"),
            "low": pd.to_numeric(df_1m.get("low"), errors="coerce"),
            "close": pd.to_numeric(df_1m.get("close"), errors="coerce"),
            "volume": pd.to_numeric(df_1m.get("volume"), errors="coerce"),
        }
    )
    if str(timeframe or "1m").strip() == "1m":
        ohlcv.index = close_idx
        ohlcv = ohlcv.loc[~ohlcv.index.isna()].sort_index()
        ohlcv.index.name = "__prepared_index__"
        return ohlcv

    open_idx = pd.to_datetime(df_1m.get("open_time"), utc=True, errors="coerce")
    if open_idx.isna().all():
        return pd.DataFrame()
    minutes = max(1, _timeframe_minutes(timeframe))
    bucket_open = open_idx.dt.floor(f"{minutes}min")
    work = ohlcv.assign(__bucket_open__=bucket_open).dropna(subset=["__bucket_open__"])
    if work.empty:
        return pd.DataFrame()
    grouped = work.groupby("__bucket_open__", sort=True).agg(
        {
            "open": "first",
            "high": "max",
            "low": "min",
            "close": "last",
            "volume": "sum",
        }
    )
    if grouped.empty:
        return pd.DataFrame()
    grouped.index = pd.to_datetime(grouped.index, utc=True, errors="coerce") + pd.Timedelta(minutes=minutes) - pd.Timedelta(milliseconds=1)
    grouped = grouped.loc[~grouped.index.isna()].sort_index()
    grouped.index.name = "__prepared_index__"
    return grouped


def _merge_market_audit_chart_frame(
    context_frame: pd.DataFrame,
    df_1m: pd.DataFrame,
    timeframe: str,
) -> pd.DataFrame:
    chart_frame = _build_timeframe_ohlcv_frame(df_1m, timeframe)
    if chart_frame.empty:
        return context_frame if isinstance(context_frame, pd.DataFrame) else pd.DataFrame()
    if not isinstance(context_frame, pd.DataFrame) or context_frame.empty:
        return chart_frame

    context = context_frame.copy()
    context = context.loc[~context.index.duplicated(keep="last")]
    context.index = pd.to_datetime(context.index, utc=True, errors="coerce")
    context = context.loc[~context.index.isna()].sort_index()
    if context.empty:
        return chart_frame

    context = context.reindex(chart_frame.index, method="ffill")
    context = context.drop(columns=["open", "high", "low", "close", "volume"], errors="ignore")
    merged = chart_frame.join(context, how="left")
    merged.index.name = "__prepared_index__"
    return merged


def _safe_round(value: Any, digits: int = 4) -> Any:
    try:
        num = float(value)
    except Exception:
        return value
    if not math.isfinite(num):
        return None
    return round(num, digits)


def _slug(value: str) -> str:
    cleaned = "".join(ch.lower() if ch.isalnum() else "_" for ch in str(value or "").strip())
    while "__" in cleaned:
        cleaned = cleaned.replace("__", "_")
    return cleaned.strip("_") or "run"


def _count_tab_rules(tab_payload: Dict[str, Any]) -> int:
    total = 0
    for group in (tab_payload.get("groups", []) or []):
        if not isinstance(group, dict):
            continue
        total += len(group.get("rules", []) or [])
    return total


def _event_hit_series(values: pd.Series) -> pd.Series:
    """Collapse repeated minute-aligned HTF event copies into real event pulses.

    Higher-timeframe event fields such as range-filter buy/sell are often aligned onto the
    1m grid for inspection. On those aligned frames, the same source HTF pulse can remain
    truthy across every minute of the higher-timeframe bar. For verification bundles we only
    want the *first* timestamp of each contiguous truthy run so signal counts and exported
    rows match the real event pulses the engine reacts to.
    """
    if values is None or len(values) == 0:
        return pd.Series(dtype=float)
    numeric = pd.to_numeric(values, errors="coerce").fillna(0).astype(float)
    truthy = numeric.gt(0)
    hit_mask = truthy & ~truthy.shift(1, fill_value=False)
    return numeric.loc[hit_mask]


def _event_activity_summary(
    rules_model: Dict[str, Any],
    streams_full: Dict[str, pd.DataFrame],
    start: pd.Timestamp,
    end: pd.Timestamp,
) -> Dict[str, Dict[str, Any]]:
    out: Dict[str, Dict[str, Any]] = {}
    window_start = pd.to_datetime(start, utc=True)
    window_end = pd.to_datetime(end, utc=True)
    for tab_name, groups in (rules_model or {}).items():
        if not isinstance(groups, list):
            continue
        for group in groups:
            if not isinstance(group, list):
                continue
            for rule in group:
                try:
                    if str(getattr(rule, "mode", "") or "").lower() != "event":
                        continue
                    tf = str(getattr(rule, "timeframe", "") or "")
                    field = str(getattr(rule, "field", "") or "")
                    frame = streams_full.get(tf)
                    if frame is None or frame.empty or field not in frame.columns:
                        continue
                    idx = pd.to_datetime(frame.index, utc=True, errors="coerce")
                    mask = (idx >= window_start) & (idx <= window_end)
                    if not mask.any():
                        continue
                    series = frame.loc[mask, field]
                    hits = _event_hit_series(series)
                    count = int(len(hits))
                    last_hits = hits.tail(5)
                    key = f"{tab_name} | {tf} | {field}"
                    out[key] = {
                        "count": count,
                        "sample_times": [str(ts) for ts in last_hits.index.tolist()],
                    }
                except Exception:
                    continue
    return out


def _event_signal_rows(
    rules_model: Dict[str, Any],
    streams_full: Dict[str, pd.DataFrame],
    start: pd.Timestamp,
    end: pd.Timestamp,
) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    window_start = pd.to_datetime(start, utc=True)
    window_end = pd.to_datetime(end, utc=True)
    for tab_name, groups in (rules_model or {}).items():
        if not isinstance(groups, list):
            continue
        for group_index, group in enumerate(groups, start=1):
            if not isinstance(group, list):
                continue
            for rule_index, rule in enumerate(group, start=1):
                try:
                    if str(getattr(rule, "mode", "") or "").lower() != "event":
                        continue
                    tf = str(getattr(rule, "timeframe", "") or "")
                    field = str(getattr(rule, "field", "") or "")
                    frame = streams_full.get(tf)
                    if frame is None or frame.empty or field not in frame.columns:
                        continue
                    idx = pd.to_datetime(frame.index, utc=True, errors="coerce")
                    mask = (idx >= window_start) & (idx <= window_end)
                    if not mask.any():
                        continue
                    values = frame.loc[mask, field]
                    hits = _event_hit_series(values)
                    for ts, val in hits.items():
                        rows.append(
                            {
                                "timestamp_utc": str(pd.to_datetime(ts, utc=True)),
                                "tab": str(tab_name),
                                "group_index": int(group_index),
                                "rule_index": int(rule_index),
                                "timeframe": str(tf),
                                "field": str(field),
                                "op": str(getattr(rule, "op", "") or ""),
                                "value": str(getattr(rule, "value", "") if getattr(rule, "value", None) is not None else ""),
                                "is_trigger": bool(getattr(rule, "is_trigger", False)),
                                "signal_value": _safe_round(val, 6),
                                "label": str(rule.label()),
                            }
                        )
                except Exception:
                    continue
    rows.sort(key=lambda row: (row["timestamp_utc"], row["tab"], row["group_index"], row["rule_index"]))
    return rows


def _write_verification_bundle(
    *,
    repo_root: Path,
    preset_name: str,
    symbol: str,
    start: pd.Timestamp,
    end: pd.Timestamp,
    bt_mode: str,
    cache_dir: Path,
    cfg: BacktestConfig,
    summary: Dict[str, Any],
    event_activity: Dict[str, Dict[str, Any]],
    signal_rows: List[Dict[str, Any]],
    res: Any,
) -> Path:
    runs_dir = repo_root / "runs"
    runs_dir.mkdir(parents=True, exist_ok=True)
    stamp = pd.Timestamp.now(tz="UTC").strftime("%Y%m%d_%H%M%S")
    bundle_dir = runs_dir / f"{_slug(preset_name)}_{_slug(symbol)}_{stamp}"
    bundle_dir.mkdir(parents=True, exist_ok=True)

    payload = {
        "preset_name": preset_name,
        "symbol": symbol,
        "start": str(start),
        "end": str(end),
        "mode": str(bt_mode),
        "cache_dir": str(cache_dir),
        "config": {
            "allow_reverse": bool(cfg.allow_reverse),
            "skip_if_both_entries": bool(cfg.skip_if_both_entries),
            "stop_loss_mode": str(cfg.stop_loss_mode),
            "stop_loss_value": float(cfg.stop_loss_value),
            "take_profit_mode": str(cfg.take_profit_mode),
            "take_profit_value": float(cfg.take_profit_value),
            "break_even_after_mfe_pct": float(cfg.break_even_after_mfe_pct),
            "trailing_stop_pct": float(cfg.trailing_stop_pct),
            "failed_pullback_abort_minutes": int(cfg.failed_pullback_abort_minutes),
            "failed_pullback_abort_min_mfe_pct": float(cfg.failed_pullback_abort_min_mfe_pct),
            "failed_pullback_abort_max_close_return_pct": float(cfg.failed_pullback_abort_max_close_return_pct),
            "capture_trade_details": bool(cfg.capture_trade_details),
            "equity_curve_stride": int(cfg.equity_curve_stride),
            "step_timeframe": str(cfg.step_timeframe),
        },
        "summary": summary,
        "event_activity": event_activity,
    }
    (bundle_dir / "summary.json").write_text(
        json.dumps(_json_safe_value(payload), indent=2, ensure_ascii=False, default=str, allow_nan=False),
        encoding="utf-8",
    )

    trade_df = _make_trade_df(list(res.trades or []))
    if trade_df is None or trade_df.empty:
        pd.DataFrame(
            columns=[
                "trade_id",
                "side",
                "entry_time",
                "exit_time",
                "entry_price",
                "exit_price",
                "net_pnl",
                "return_pct",
                "entry_reason",
                "exit_reason",
            ]
        ).to_csv(bundle_dir / "trades.csv", index=False)
    else:
        trade_df.to_csv(bundle_dir / "trades.csv", index=False)

    pd.DataFrame(signal_rows).to_csv(bundle_dir / "signal_events.csv", index=False)

    eq = res.equity_curve.copy() if getattr(res, "equity_curve", None) is not None else pd.DataFrame()
    if isinstance(eq, pd.DataFrame) and not eq.empty:
        eq_out = eq.copy().reset_index()
        first_col = str(eq_out.columns[0])
        eq_out = eq_out.rename(columns={first_col: "timestamp_utc"})
        eq_out.to_csv(bundle_dir / "equity_curve.csv", index=False)
    else:
        pd.DataFrame(columns=["timestamp_utc", "equity", "equity_mark"]).to_csv(bundle_dir / "equity_curve.csv", index=False)

    notes = (
        "Verification bundle\n"
        "1. Use the same symbol and window in TradingView.\n"
        "2. Compare signal markers against signal_events.csv first.\n"
        "3. Then compare trade-by-trade against trades.csv.\n"
        "4. Use summary.json only after signals and trades match.\n"
        "5. Do not fine-tune strategy settings until the signal timestamps match.\n"
    )
    (bundle_dir / "README.txt").write_text(notes, encoding="utf-8")
    return bundle_dir


def _coerce_bool(value: Any, default: bool) -> bool:
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    raw = str(value).strip().lower()
    if raw in {"1", "true", "yes", "on"}:
        return True
    if raw in {"0", "false", "no", "off"}:
        return False
    return default


def _coerce_float(value: Any, default: float) -> float:
    try:
        return float(value)
    except Exception:
        return float(default)


def _coerce_int(value: Any, default: int) -> int:
    try:
        return int(value)
    except Exception:
        return int(default)


def _sample_equity_curve(equity_curve: pd.DataFrame, limit: int = 240) -> List[Dict[str, Any]]:
    if equity_curve is None or equity_curve.empty:
        return []
    frame = equity_curve.reset_index().copy()
    first_col = str(frame.columns[0])
    frame = frame.rename(columns={first_col: "timestamp_utc"})
    if len(frame) > limit:
        step = max(1, len(frame) // limit)
        frame = frame.iloc[::step].tail(limit)
    rows: List[Dict[str, Any]] = []
    for _, row in frame.iterrows():
        rows.append(
            {
                "timestamp_utc": str(row.get("timestamp_utc")),
                "equity": _safe_round(row.get("equity"), 6),
                "equity_mark": _safe_round(row.get("equity_mark"), 6),
            }
        )
    return rows


@dataclass
class ExitSweepAxis:
    start: float = 0.0
    end: float = 0.0
    step: float = 0.0
    include_off: bool = False

    @classmethod
    def from_payload(cls, payload: Optional[Dict[str, Any]]) -> "ExitSweepAxis":
        raw = payload or {}
        return cls(
            start=_coerce_float(raw.get("start"), 0.0),
            end=_coerce_float(raw.get("end"), 0.0),
            step=_coerce_float(raw.get("step"), 0.0),
            include_off=_coerce_bool(raw.get("include_off"), False),
        )


@dataclass
class ExitSweepRequest:
    auto_run: bool = False
    sort_by: str = "net_profit"
    top_n: int = 25
    max_combinations: int = 5000
    stop_loss: ExitSweepAxis = field(default_factory=ExitSweepAxis)
    take_profit: ExitSweepAxis = field(default_factory=ExitSweepAxis)
    break_even: ExitSweepAxis = field(default_factory=ExitSweepAxis)
    trailing_stop: ExitSweepAxis = field(default_factory=ExitSweepAxis)

    @classmethod
    def from_payload(cls, payload: Optional[Dict[str, Any]]) -> Optional["ExitSweepRequest"]:
        raw = payload or {}
        if not isinstance(raw, dict) or not raw:
            return None
        return cls(
            auto_run=_coerce_bool(raw.get("auto_run"), False),
            sort_by=str(raw.get("sort_by") or "net_profit").strip().lower(),
            top_n=max(1, _coerce_int(raw.get("top_n"), 25)),
            max_combinations=max(1, _coerce_int(raw.get("max_combinations"), 5000)),
            stop_loss=ExitSweepAxis.from_payload(raw.get("stop_loss") if isinstance(raw.get("stop_loss"), dict) else {}),
            take_profit=ExitSweepAxis.from_payload(raw.get("take_profit") if isinstance(raw.get("take_profit"), dict) else {}),
            break_even=ExitSweepAxis.from_payload(raw.get("break_even") if isinstance(raw.get("break_even"), dict) else {}),
            trailing_stop=ExitSweepAxis.from_payload(raw.get("trailing_stop") if isinstance(raw.get("trailing_stop"), dict) else {}),
        )


@dataclass
class BacktestRunArtifact:
    run_id: str
    preset_name: str
    result: BacktestResult
    payload: Dict[str, Any]
    created_at_utc: str


def _coerce_sort_key(value: Any, default: str = "net_profit") -> str:
    key = str(value or default).strip().lower()
    allowed = {
        "net_profit",
        "return_pct",
        "ending_balance",
        "profit_factor",
        "win_rate_pct",
        "max_drawdown_pct",
        "num_trades",
    }
    return key if key in allowed else default


def _expand_sweep_axis(name: str, axis: ExitSweepAxis, *, max_points: int = 250) -> List[float]:
    values: List[float] = []
    start = max(0.0, float(axis.start))
    end = max(0.0, float(axis.end))
    step = max(0.0, float(axis.step))

    if start > 0.0 or end > 0.0:
        if end <= 0.0:
            end = start
        if start <= 0.0:
            start = end
        if end < start:
            raise ValueError(f"{name} sweep range is invalid: end must be >= start.")
        if step <= 0.0:
            if not math.isclose(start, end, rel_tol=0.0, abs_tol=1e-12):
                raise ValueError(f"{name} sweep range needs a positive step.")
            values.append(round(start, 8))
        else:
            current = start
            guard = 0
            while current <= (end + (step * 0.5)):
                values.append(round(current, 8))
                current += step
                guard += 1
                if guard > max_points:
                    raise ValueError(f"{name} sweep range expands to more than {max_points} values.")

    if bool(axis.include_off):
        values.insert(0, 0.0)

    deduped: List[float] = []
    seen: set[float] = set()
    for value in values:
        rounded = round(float(value), 8)
        if rounded in seen:
            continue
        seen.add(rounded)
        deduped.append(rounded)

    if not deduped:
        raise ValueError(f"{name} sweep has no values. Add a range or enable OFF.")
    return deduped


def _sample_trades_payload(trades: List[Any], limit: int = 25) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    for trade in list(trades or [])[:limit]:
        rows.append(_trade_payload(trade, include_snapshots=False))
    return rows


def _trade_payload(trade: Any, *, include_snapshots: bool = False) -> Dict[str, Any]:
    payload: Dict[str, Any] = {
        "trade_id": int(getattr(trade, "trade_id", 0) or 0),
        "side": str(getattr(trade, "side", "") or ""),
        "entry_time": str(getattr(trade, "entry_time", "") or ""),
        "entry_price": _safe_round(getattr(trade, "entry_price", None), 6),
        "exit_time": str(getattr(trade, "exit_time", "") or ""),
        "exit_price": _safe_round(getattr(trade, "exit_price", None), 6),
        "qty": _safe_round(getattr(trade, "qty", None), 6),
        "entry_notional": _safe_round(getattr(trade, "entry_notional", None), 6),
        "exit_notional": _safe_round(getattr(trade, "exit_notional", None), 6),
        "gross_pnl": _safe_round(getattr(trade, "gross_pnl", None), 6),
        "fees": _safe_round(getattr(trade, "fees", None), 6),
        "fee_entry": _safe_round(getattr(trade, "fee_entry", None), 6),
        "fee_exit": _safe_round(getattr(trade, "fee_exit", None), 6),
        "net_pnl": _safe_round(getattr(trade, "net_pnl", None), 6),
        "return_pct": _safe_round(getattr(trade, "return_pct", None), 6),
        "duration_min": int(getattr(trade, "duration_min", 0) or 0),
        "mfe": _safe_round(getattr(trade, "mfe", None), 6),
        "mae": _safe_round(getattr(trade, "mae", None), 6),
        "mfe_pct": _safe_round(getattr(trade, "mfe_pct", None), 6),
        "mae_pct": _safe_round(getattr(trade, "mae_pct", None), 6),
        "entry_reason": str(getattr(trade, "entry_reason", "") or ""),
        "exit_reason": str(getattr(trade, "exit_reason", "") or ""),
        "entry_rule_trace": str(getattr(trade, "entry_rule_trace", "") or ""),
        "exit_rule_trace": str(getattr(trade, "exit_rule_trace", "") or ""),
        "signal_time": str(getattr(trade, "signal_time", "") or ""),
        "order_created_time": str(getattr(trade, "order_created_time", "") or ""),
        "entry_fill_time": str(getattr(trade, "entry_fill_time", "") or ""),
        "exit_signal_time": str(getattr(trade, "exit_signal_time", "") or ""),
        "exit_order_created_time": str(getattr(trade, "exit_order_created_time", "") or ""),
        "exit_fill_time": str(getattr(trade, "exit_fill_time", "") or ""),
        "engine_type": str(getattr(trade, "engine_type", "") or ""),
        "fill_source": str(getattr(trade, "fill_source", "") or ""),
        "ambiguous_exit": bool(getattr(trade, "ambiguous_exit", False)),
        "stop_loss_price": _safe_round(getattr(trade, "stop_loss_price", None), 6),
        "take_profit_price": _safe_round(getattr(trade, "take_profit_price", None), 6),
        "entry_row_index": getattr(trade, "entry_row_index", None),
        "exit_row_index": getattr(trade, "exit_row_index", None),
    }
    if include_snapshots:
        payload["entry_snapshot"] = _json_safe_value(getattr(trade, "entry_snapshot", {}) or {})
        payload["exit_snapshot"] = _json_safe_value(getattr(trade, "exit_snapshot", {}) or {})
        payload["entry_barsago"] = _json_safe_value(getattr(trade, "entry_barsago", {}) or {})
        payload["exit_barsago"] = _json_safe_value(getattr(trade, "exit_barsago", {}) or {})
    return payload


def _trade_chart_event_indices(
    idx: pd.DatetimeIndex,
    trade: Any,
    *,
    entry_index: Optional[int] = None,
    exit_index: Optional[int] = None,
) -> Dict[str, Optional[int]]:
    if idx.empty:
        return {
            "signal_bar_index": None,
            "order_bar_index": None,
            "entry_bar_index": None,
            "exit_signal_bar_index": None,
            "exit_order_bar_index": None,
            "exit_bar_index": None,
        }

    def _locate_left(raw_time: Any) -> Optional[int]:
        ts = pd.to_datetime(raw_time, utc=True, errors="coerce")
        if pd.isna(ts):
            return None
        try:
            pos = int(idx.searchsorted(ts, side="left"))
        except Exception:
            return None
        if pos < 0:
            return 0
        if pos >= len(idx):
            return len(idx) - 1
        return pos

    def _locate_right(raw_time: Any) -> Optional[int]:
        ts = pd.to_datetime(raw_time, utc=True, errors="coerce")
        if pd.isna(ts):
            return None
        try:
            pos = int(idx.searchsorted(ts, side="right")) - 1
        except Exception:
            return None
        if pos < 0:
            return 0
        if pos >= len(idx):
            return len(idx) - 1
        return pos

    resolved_entry_index = entry_index if entry_index is not None else _locate_left(getattr(trade, "entry_fill_time", None))
    if resolved_entry_index is None:
        resolved_entry_index = _locate_left(getattr(trade, "entry_time", None))
    resolved_exit_index = exit_index if exit_index is not None else _locate_right(getattr(trade, "exit_fill_time", None))
    if resolved_exit_index is None:
        resolved_exit_index = _locate_right(getattr(trade, "exit_time", None))

    return {
        "signal_bar_index": _locate_left(getattr(trade, "signal_time", None)),
        "order_bar_index": _locate_left(getattr(trade, "order_created_time", None)),
        "entry_bar_index": resolved_entry_index,
        "exit_signal_bar_index": _locate_left(getattr(trade, "exit_signal_time", None)),
        "exit_order_bar_index": _locate_left(getattr(trade, "exit_order_created_time", None)),
        "exit_bar_index": resolved_exit_index,
    }


def _trade_chart_lifecycle_events(
    trade: Any,
    event_indices: Dict[str, Optional[int]],
    *,
    lo: int,
    hi: int,
) -> List[Dict[str, Any]]:
    specs = [
        ("signal", "Signal", getattr(trade, "signal_time", None), event_indices.get("signal_bar_index")),
        ("order", "Order", getattr(trade, "order_created_time", None), event_indices.get("order_bar_index")),
        ("entry", "Entry", getattr(trade, "entry_fill_time", None) or getattr(trade, "entry_time", None), event_indices.get("entry_bar_index")),
        ("exit_signal", "Exit Signal", getattr(trade, "exit_signal_time", None), event_indices.get("exit_signal_bar_index")),
        ("exit_order", "Exit Order", getattr(trade, "exit_order_created_time", None), event_indices.get("exit_order_bar_index")),
        ("exit", "Exit", getattr(trade, "exit_fill_time", None) or getattr(trade, "exit_time", None), event_indices.get("exit_bar_index")),
    ]
    events: List[Dict[str, Any]] = []
    for key, label, raw_time, absolute_index in specs:
        ts = pd.to_datetime(raw_time, utc=True, errors="coerce")
        if pd.isna(ts) and absolute_index is None:
            continue
        events.append(
            {
                "key": str(key),
                "label": str(label),
                "time_utc": (str(ts) if pd.notna(ts) else ""),
                "bar_index": (int(absolute_index) if absolute_index is not None else None),
                "window_row_index": (
                    int(absolute_index - lo)
                    if absolute_index is not None and absolute_index >= lo and absolute_index <= hi
                    else None
                ),
            }
        )
    return events


def _trade_chart_event_rank(key: str) -> int:
    raw = str(key or "").strip().lower()
    if raw.startswith("entry_seq_"):
        return 5
    if raw == "signal":
        return 10
    if raw == "order":
        return 20
    if raw == "entry":
        return 30
    if raw.startswith("exit_seq_"):
        return 35
    if raw == "exit_signal":
        return 40
    if raw == "exit_order":
        return 50
    if raw == "exit":
        return 60
    return 90


def _sort_trade_chart_events(events: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    def _key(event: Dict[str, Any]) -> Tuple[int, int, str]:
        raw_bar_index = event.get("bar_index")
        try:
            bar_index = int(raw_bar_index) if raw_bar_index is not None else 10**9
        except Exception:
            bar_index = 10**9
        return (
            bar_index,
            _trade_chart_event_rank(str(event.get("key") or "")),
            str(event.get("time_utc") or ""),
        )

    return sorted(list(events or []), key=_key)


def _trade_sequence_rule_text(rule: Any) -> str:
    timeframe = str(getattr(rule, "timeframe", "") or "").strip()
    field = str(getattr(rule, "field", "") or "").strip()
    op = str(getattr(rule, "op", "") or "").strip()
    value = getattr(rule, "value", None)
    parts = [part for part in (timeframe, field, op) if part]
    if value not in (None, ""):
        parts.append(str(value))
    return " ".join(parts).strip() or "sequence rule"


def _make_trade_sequence_event(
    *,
    prefix: str,
    tab_name: str,
    group_index: int,
    rule: Any,
    step_number: int,
    total_steps: int,
    timestamp_utc: str,
    bar_index: int,
) -> Dict[str, Any]:
    phase = "trigger" if step_number <= 1 else ("complete" if step_number >= total_steps else "step")
    if prefix == "exit":
        label_prefix = "Exit"
        short_prefix = "X"
    else:
        label_prefix = "Entry"
        short_prefix = "E"
    if phase == "trigger":
        label = f"{label_prefix} Trigger"
        short_label = f"{short_prefix}.TRIG"
        css = "is-seq-trigger"
    elif phase == "complete":
        label = f"{label_prefix} Sequence Complete"
        short_label = f"{short_prefix}.S{int(step_number)}"
        css = "is-seq-complete"
    else:
        label = f"{label_prefix} Step {int(step_number)}"
        short_label = f"{short_prefix}.S{int(step_number)}"
        css = "is-seq-step"
    rule_text = _trade_sequence_rule_text(rule)
    return {
        "key": f"{prefix}_seq_g{int(group_index)}_s{int(step_number)}",
        "label": f"{label} | {rule_text}",
        "time_utc": str(timestamp_utc or ""),
        "bar_index": int(bar_index),
        "window_row_index": None,
        "position": "top",
        "css": str(css),
        "short_label": str(short_label),
        "tab_name": str(tab_name or ""),
        "group_index": int(group_index),
        "step_number": int(step_number),
        "total_steps": int(total_steps),
        "rule_text": str(rule_text),
    }


def _trade_sequence_events_for_tab(
    artifact: "BacktestRunArtifact",
    idx_1m: pd.DatetimeIndex,
    *,
    tab_name: str,
    target_time: Any,
    prefix: str,
) -> List[Dict[str, Any]]:
    replay_context = getattr(artifact.result, "replay_context", {}) or {}
    rules_model = replay_context.get("rules_model") or {}
    tab_group_join_mode = replay_context.get("tab_group_join_mode") or {}
    group_rule_join_mode = replay_context.get("group_rule_join_mode") or {}
    streams_full = replay_context.get("streams_full")
    if not isinstance(streams_full, dict):
        return []

    groups = list(rules_model.get(tab_name) or [])
    if not groups:
        return []

    sequence_groups: Dict[int, List[Any]] = {}
    group_modes = list(group_rule_join_mode.get(tab_name) or [])
    for group_index, group_rules in enumerate(groups):
        join_mode = str(group_modes[group_index] if group_index < len(group_modes) else "OR").upper().strip()
        if join_mode != "SEQUENCE":
            continue
        seq_rules, _gate_rules = sequence_rule_partition(list(group_rules or []))
        if seq_rules:
            sequence_groups[int(group_index)] = list(seq_rules)
    if not sequence_groups:
        return []

    target_ts = pd.to_datetime(target_time, utc=True, errors="coerce")
    if pd.isna(target_ts):
        return []
    try:
        target_row = int(idx_1m.searchsorted(target_ts, side="left"))
    except Exception:
        return []
    if target_row < 0 or target_row >= len(idx_1m):
        return []

    step_tf = _normalize_step_timeframe(getattr(artifact.result.config, "step_timeframe", "1m"))
    decision_mask = _decision_mask_for_step_tf(idx_1m, step_tf)
    decision_rows = [int(i) for i in range(0, target_row + 1) if bool(decision_mask[i])]
    if not decision_rows:
        return []

    req_spec = compile_stream_requirements(
        rules_model,
        entry_filters=replay_context.get("entry_filters"),
        backtest_cfg=artifact.result.config,
        include_plot_defaults=True,
    )
    required_fields = req_spec.normalized_fields(include_defaults=True)
    available_tfs = [
        str(tf)
        for tf, frame in dict(streams_full or {}).items()
        if isinstance(frame, pd.DataFrame) and not frame.empty
    ]
    if "1m" in available_tfs:
        available_tfs = ["1m"] + [tf for tf in available_tfs if tf != "1m"]
    if not available_tfs:
        return []
    subset_streams = {
        tf: streams_full.get(tf)
        for tf in available_tfs
        if isinstance(streams_full.get(tf), pd.DataFrame) and not streams_full.get(tf).empty
    }
    prev_source_row_by_tf = {
        tf: _prev_source_row_index_for_df(subset_streams.get(tf), tf)
        for tf in available_tfs
    }

    total_steps_by_group = {group_index: len(seq_rules) for group_index, seq_rules in sequence_groups.items()}
    search_sizes: List[int] = []
    total_decision_rows = len(decision_rows)
    for raw_size in (256, 1024, 4096, 16384, total_decision_rows):
        size = min(total_decision_rows, int(raw_size))
        if size <= 0:
            continue
        if not search_sizes or size != search_sizes[-1]:
            search_sizes.append(size)

    for size in search_sizes:
        eval_rows = decision_rows[-size:]
        snapshot_cache = _RowSnapshotCache(required_fields=required_fields, max_rows_per_tf=1024)
        ctx = RuleEvalContext(step_index=0)
        try:
            ctx.ensure_max_history(max(512, min(32768, int(size) + 64)))
        except Exception:
            pass
        prev_decision_j: Optional[int] = None
        open_chain_by_group: Dict[int, List[Dict[str, Any]]] = {group_index: [] for group_index in sequence_groups}
        completed_chain_by_group: Dict[int, List[Dict[str, Any]]] = {}
        target_matched_groups: List[int] = []

        for step_index, row_index in enumerate(eval_rows):
            cur_by_tf, prev_by_tf = _build_eval_snapshots_for_row(
                subset_streams,
                row_index=int(row_index),
                prev_decision_j=prev_decision_j,
                prev_source_row_by_tf=prev_source_row_by_tf,
                step_tf=step_tf,
                snapshot_cache=snapshot_cache,
                required_fields=required_fields,
            )
            ctx.step_index = int(step_index)
            try:
                ctx.observe(cur_by_tf)
            except Exception:
                pass

            before_progress = {
                group_index: int(ctx.sequence_progress_by_group.get((tab_name, group_index), 0) or 0)
                for group_index in sequence_groups
            }

            _tab_result, matched_groups, group_results = eval_tab_matching_groups(
                tab_name,
                rules_model,
                tab_group_join_mode,
                group_rule_join_mode,
                cur_by_tf,
                prev_by_tf,
                ctx=ctx,
            )

            for group_index, seq_rules in sequence_groups.items():
                total_steps = total_steps_by_group[group_index]
                progress_before = int(before_progress.get(group_index, 0) or 0)
                if progress_before < 0 or progress_before >= total_steps:
                    progress_before = 0
                progress_after = int(ctx.sequence_progress_by_group.get((tab_name, group_index), 0) or 0)
                group_result = group_results[group_index] if group_index < len(group_results) else None
                transitioned = bool(
                    (progress_after != progress_before)
                    or (group_result is True and progress_before == max(0, total_steps - 1))
                )
                if not transitioned:
                    continue
                if progress_before == 0:
                    open_chain_by_group[group_index] = []
                step_number = int(progress_before) + 1
                open_chain_by_group[group_index].append(
                    _make_trade_sequence_event(
                        prefix=prefix,
                        tab_name=tab_name,
                        group_index=group_index,
                        rule=seq_rules[progress_before],
                        step_number=step_number,
                        total_steps=total_steps,
                        timestamp_utc=str(idx_1m[int(row_index)]),
                        bar_index=int(row_index),
                    )
                )
                if group_result is True and step_number >= total_steps:
                    completed_chain_by_group[group_index] = list(open_chain_by_group[group_index])
                    open_chain_by_group[group_index] = []

            prev_decision_j = int(row_index)
            if int(row_index) == int(target_row):
                target_matched_groups = [int(group_index) for group_index in list(matched_groups or [])]
                break

        sequence_target_groups = [group_index for group_index in target_matched_groups if group_index in sequence_groups]
        for group_index in sequence_target_groups:
            chain = completed_chain_by_group.get(group_index) or open_chain_by_group.get(group_index) or []
            if len(chain) == total_steps_by_group.get(group_index, 0):
                return list(chain)

    return []


def _trade_chart_sequence_events(
    artifact: "BacktestRunArtifact",
    idx_1m: pd.DatetimeIndex,
    trade: Any,
) -> List[Dict[str, Any]]:
    side = str(getattr(trade, "side", "") or "").upper().strip()
    if side == "LONG":
        entry_tab = "Long Entry"
        exit_tab = "Long Exit"
    elif side == "SHORT":
        entry_tab = "Short Entry"
        exit_tab = "Short Exit"
    else:
        return []

    events = _trade_sequence_events_for_tab(
        artifact,
        idx_1m,
        tab_name=entry_tab,
        target_time=getattr(trade, "signal_time", None),
        prefix="entry",
    )
    exit_events = _trade_sequence_events_for_tab(
        artifact,
        idx_1m,
        tab_name=exit_tab,
        target_time=getattr(trade, "exit_signal_time", None),
        prefix="exit",
    )
    return _sort_trade_chart_events(list(events or []) + list(exit_events or []))


def _project_chart_events_to_window(
    events: List[Dict[str, Any]],
    *,
    lo: int,
    hi: int,
) -> List[Dict[str, Any]]:
    projected: List[Dict[str, Any]] = []
    for raw_event in list(events or []):
        event = dict(raw_event or {})
        raw_bar_index = event.get("bar_index")
        try:
            absolute_index = int(raw_bar_index) if raw_bar_index is not None else None
        except Exception:
            absolute_index = None
        event["bar_index"] = absolute_index
        event["window_row_index"] = (
            int(absolute_index - lo)
            if absolute_index is not None and absolute_index >= lo and absolute_index <= hi
            else None
        )
        projected.append(event)
    return _sort_trade_chart_events(projected)


def _apply_chart_event_tags(
    rows: List[Dict[str, Any]],
    *,
    lo: int,
    projected_events: List[Dict[str, Any]],
) -> None:
    tags_by_abs_index: Dict[int, List[Dict[str, Any]]] = {}
    for event in list(projected_events or []):
        raw_bar_index = event.get("bar_index")
        try:
            bar_index = int(raw_bar_index) if raw_bar_index is not None else None
        except Exception:
            bar_index = None
        if bar_index is None:
            continue
        tags_by_abs_index.setdefault(bar_index, []).append(
            {
                "text": str(event.get("short_label") or event.get("label") or event.get("key") or "EVT"),
                "css": str(event.get("css") or "is-seq-step"),
                "position": str(event.get("position") or "top"),
            }
        )
    for row_offset, row in enumerate(rows):
        abs_index = int(lo + row_offset)
        row["event_tags"] = list(tags_by_abs_index.get(abs_index, []))


def _expand_trade_chart_window_for_events(
    lo: int,
    hi: int,
    *,
    limit: int,
    extra_events: Optional[List[Dict[str, Any]]] = None,
) -> Tuple[int, int]:
    next_lo = int(max(0, lo))
    next_hi = int(min(max(0, limit - 1), hi))
    for event in list(extra_events or []):
        raw_bar_index = event.get("bar_index")
        try:
            bar_index = int(raw_bar_index) if raw_bar_index is not None else None
        except Exception:
            bar_index = None
        if bar_index is None:
            continue
        next_lo = min(next_lo, max(0, bar_index))
        next_hi = max(next_hi, min(max(0, limit - 1), bar_index))
    return next_lo, next_hi


def _downsample_chart_rows(rows: List[Dict[str, Any]], *, max_points: int = 900) -> Tuple[List[Dict[str, Any]], int]:
    if len(rows) <= max_points:
        return rows, 1
    stride = max(1, int(math.ceil(len(rows) / max_points)))
    keep_indices = set(range(0, len(rows), stride))
    keep_indices.add(len(rows) - 1)
    for index, row in enumerate(rows):
        if any(
            bool(row.get(flag))
            for flag in (
                "is_signal_bar",
                "is_order_bar",
                "is_entry_bar",
                "is_exit_signal_bar",
                "is_exit_order_bar",
                "is_exit_bar",
            )
        ):
            keep_indices.add(index)
        if list(row.get("event_tags") or []):
            keep_indices.add(index)
    sampled = [rows[index] for index in sorted(keep_indices)]
    return sampled, stride


def _trade_chart_rows_from_compiled_plan(
    plan: Any,
    trade: Any,
    *,
    padding_bars: int = 8,
    extra_events: Optional[List[Dict[str, Any]]] = None,
) -> Dict[str, Any]:
    idx = pd.DatetimeIndex(getattr(plan, "idx", []))
    if idx.empty:
        return {"rows": [], "stride": 1, "window_start_utc": "", "window_end_utc": ""}

    start_i = getattr(trade, "entry_row_index", None)
    end_i = getattr(trade, "exit_row_index", None)
    if start_i is None or end_i is None:
        entry_time = pd.to_datetime(getattr(trade, "entry_time", None), utc=True, errors="coerce")
        exit_time = pd.to_datetime(getattr(trade, "exit_time", None), utc=True, errors="coerce")
        start_i = int(idx.searchsorted(entry_time, side="left")) if pd.notna(entry_time) else 0
        end_i = int(idx.searchsorted(exit_time, side="right")) - 1 if pd.notna(exit_time) else start_i
    start_i = max(0, min(int(start_i), len(idx) - 1))
    end_i = max(0, min(int(end_i), len(idx) - 1))
    event_indices = _trade_chart_event_indices(idx, trade, entry_index=start_i, exit_index=end_i)
    lo = max(0, min(start_i, end_i) - max(0, int(padding_bars)))
    hi = min(len(idx) - 1, max(start_i, end_i) + max(0, int(padding_bars)))
    lo, hi = _expand_trade_chart_window_for_events(lo, hi, limit=len(idx), extra_events=extra_events)
    rows: List[Dict[str, Any]] = []
    open_arr = getattr(plan, "open_arr", None)
    high_arr = getattr(plan, "high_arr", None)
    low_arr = getattr(plan, "low_arr", None)
    close_arr = getattr(plan, "close_arr", None)
    for i in range(lo, hi + 1):
        rows.append(
            {
                "timestamp_utc": str(idx[i]),
                "open": _safe_round(open_arr[i] if open_arr is not None else None, 6),
                "high": _safe_round(high_arr[i] if high_arr is not None else None, 6),
                "low": _safe_round(low_arr[i] if low_arr is not None else None, 6),
                "close": _safe_round(close_arr[i] if close_arr is not None else None, 6),
                "inside_trade": bool(i >= min(start_i, end_i) and i <= max(start_i, end_i)),
                "is_signal_bar": bool(i == event_indices.get("signal_bar_index")),
                "is_order_bar": bool(i == event_indices.get("order_bar_index")),
                "is_entry_bar": bool(i == start_i),
                "is_exit_signal_bar": bool(i == event_indices.get("exit_signal_bar_index")),
                "is_exit_order_bar": bool(i == event_indices.get("exit_order_bar_index")),
                "is_exit_bar": bool(i == end_i),
                "entry_marker_price": _safe_round(getattr(trade, "entry_price", None), 6) if i == start_i else None,
                "exit_marker_price": _safe_round(getattr(trade, "exit_price", None), 6) if i == end_i else None,
            }
        )
    projected_events = _project_chart_events_to_window(list(extra_events or []), lo=lo, hi=hi)
    _apply_chart_event_tags(rows, lo=lo, projected_events=projected_events)
    sampled, stride = _downsample_chart_rows(rows)
    return {
        "rows": sampled,
        "stride": int(stride),
        "window_start_utc": str(idx[lo]),
        "window_end_utc": str(idx[hi]),
        "events": _sort_trade_chart_events(
            _trade_chart_lifecycle_events(trade, event_indices, lo=lo, hi=hi) + projected_events
        ),
    }


def _trade_chart_rows_from_frame(
    frame: Any,
    trade: Any,
    *,
    padding_bars: int = 8,
    extra_events: Optional[List[Dict[str, Any]]] = None,
) -> Dict[str, Any]:
    if not isinstance(frame, pd.DataFrame) or frame.empty:
        return {"rows": [], "stride": 1, "window_start_utc": "", "window_end_utc": ""}

    idx = pd.to_datetime(frame.index, utc=True, errors="coerce")
    if idx.empty:
        return {"rows": [], "stride": 1, "window_start_utc": "", "window_end_utc": ""}

    entry_time = pd.to_datetime(getattr(trade, "entry_time", None), utc=True, errors="coerce")
    exit_time = pd.to_datetime(getattr(trade, "exit_time", None), utc=True, errors="coerce")
    start_i = int(idx.searchsorted(entry_time, side="left")) if pd.notna(entry_time) else 0
    end_i = int(idx.searchsorted(exit_time, side="right")) - 1 if pd.notna(exit_time) else start_i
    start_i = max(0, min(start_i, len(frame) - 1))
    end_i = max(0, min(end_i, len(frame) - 1))
    event_indices = _trade_chart_event_indices(idx, trade, entry_index=start_i, exit_index=end_i)
    lo = max(0, min(start_i, end_i) - max(0, int(padding_bars)))
    hi = min(len(frame) - 1, max(start_i, end_i) + max(0, int(padding_bars)))
    lo, hi = _expand_trade_chart_window_for_events(lo, hi, limit=len(frame), extra_events=extra_events)
    price_col = "close" if "close" in frame.columns else ("price" if "price" in frame.columns else None)
    rows: List[Dict[str, Any]] = []
    for i in range(lo, hi + 1):
        row = frame.iloc[i]
        rows.append(
            {
                "timestamp_utc": str(idx[i]),
                "open": _safe_round(row.get("open"), 6),
                "high": _safe_round(row.get("high"), 6),
                "low": _safe_round(row.get("low"), 6),
                "close": _safe_round(row.get("close") if "close" in frame.columns else row.get(price_col), 6),
                "inside_trade": bool(i >= min(start_i, end_i) and i <= max(start_i, end_i)),
                "is_signal_bar": bool(i == event_indices.get("signal_bar_index")),
                "is_order_bar": bool(i == event_indices.get("order_bar_index")),
                "is_entry_bar": bool(i == start_i),
                "is_exit_signal_bar": bool(i == event_indices.get("exit_signal_bar_index")),
                "is_exit_order_bar": bool(i == event_indices.get("exit_order_bar_index")),
                "is_exit_bar": bool(i == end_i),
                "entry_marker_price": _safe_round(getattr(trade, "entry_price", None), 6) if i == start_i else None,
                "exit_marker_price": _safe_round(getattr(trade, "exit_price", None), 6) if i == end_i else None,
            }
        )
    projected_events = _project_chart_events_to_window(list(extra_events or []), lo=lo, hi=hi)
    _apply_chart_event_tags(rows, lo=lo, projected_events=projected_events)
    sampled, stride = _downsample_chart_rows(rows)
    return {
        "rows": sampled,
        "stride": int(stride),
        "window_start_utc": str(idx[lo]),
        "window_end_utc": str(idx[hi]),
        "events": _sort_trade_chart_events(
            _trade_chart_lifecycle_events(trade, event_indices, lo=lo, hi=hi) + projected_events
        ),
    }


def _trade_chart_payload(artifact: "BacktestRunArtifact", trade: Any, *, padding_bars: int = 8) -> Dict[str, Any]:
    replay_context = getattr(artifact.result, "replay_context", {}) or {}
    extra_events: List[Dict[str, Any]] = []
    compiled_plan = replay_context.get("__compiled_bar_plan")
    if compiled_plan is not None and hasattr(compiled_plan, "idx") and hasattr(compiled_plan, "close_arr"):
        try:
            extra_events = _trade_chart_sequence_events(artifact, pd.DatetimeIndex(getattr(compiled_plan, "idx", [])), trade)
        except Exception:
            extra_events = []
        return _trade_chart_rows_from_compiled_plan(
            compiled_plan,
            trade,
            padding_bars=padding_bars,
            extra_events=extra_events,
        )
    frame = replay_context.get("df_1m_full")
    if isinstance(frame, pd.DataFrame) and not frame.empty:
        try:
            extra_events = _trade_chart_sequence_events(artifact, pd.to_datetime(frame.index, utc=True, errors="coerce"), trade)
        except Exception:
            extra_events = []
        return _trade_chart_rows_from_frame(
            frame,
            trade,
            padding_bars=padding_bars,
            extra_events=extra_events,
        )
    streams_full = replay_context.get("streams_full")
    if isinstance(streams_full, dict):
        frame = streams_full.get("1m")
        if isinstance(frame, pd.DataFrame) and not frame.empty:
            try:
                extra_events = _trade_chart_sequence_events(artifact, pd.to_datetime(frame.index, utc=True, errors="coerce"), trade)
            except Exception:
                extra_events = []
            return _trade_chart_rows_from_frame(
                frame,
                trade,
                padding_bars=padding_bars,
                extra_events=extra_events,
            )
    return {"rows": [], "stride": 1, "window_start_utc": "", "window_end_utc": ""}


@dataclass
class RunOverrides:
    start: Optional[str] = None
    end: Optional[str] = None
    cache_dir: Optional[str] = None
    initial_balance: float = 1000.0
    order_notional: float = 100.0
    fee_pct: float = 0.04
    slippage_bps: float = 0.0
    stop_loss_mode: str = "PERCENT"
    stop_loss_value: float = 0.0
    stop_loss_timeframe: str = "1m"
    stop_loss_field: str = ""
    stop_loss_offset_pct: float = 0.0
    take_profit_mode: str = "PERCENT"
    take_profit_value: float = 0.0
    take_profit_timeframe: str = "1m"
    take_profit_field: str = ""
    take_profit_offset_pct: float = 0.0
    break_even_after_mfe_pct: float = 0.0
    trailing_stop_pct: float = 0.0
    failed_pullback_abort_minutes: int = 0
    failed_pullback_abort_min_mfe_pct: float = 0.0
    failed_pullback_abort_max_close_return_pct: float = 0.0
    allow_reverse: bool = True
    skip_both_entries: bool = True
    step_timeframe: str = "1m"
    capture_trade_details: bool = False
    equity_curve_stride: int = 15
    exit_sweep: Optional[ExitSweepRequest] = None
    provided_fields: set[str] = field(default_factory=set, repr=False, compare=False)

    @classmethod
    def from_payload(cls, payload: Optional[Dict[str, Any]]) -> "RunOverrides":
        raw = payload or {}
        inst = cls()
        inst.provided_fields = {str(key) for key in raw.keys()}
        inst.start = raw.get("start") or None
        inst.end = raw.get("end") or None
        inst.cache_dir = raw.get("cache_dir") or None
        inst.initial_balance = _coerce_float(raw.get("initial_balance"), inst.initial_balance)
        inst.order_notional = _coerce_float(raw.get("order_notional"), inst.order_notional)
        inst.fee_pct = _coerce_float(raw.get("fee_pct"), inst.fee_pct)
        inst.slippage_bps = _coerce_float(raw.get("slippage_bps"), inst.slippage_bps)
        inst.stop_loss_mode = str(raw.get("stop_loss_mode") or inst.stop_loss_mode).upper()
        inst.stop_loss_value = _coerce_float(raw.get("stop_loss_value"), inst.stop_loss_value)
        inst.stop_loss_timeframe = str(raw.get("stop_loss_timeframe") or inst.stop_loss_timeframe)
        inst.stop_loss_field = str(raw.get("stop_loss_field") or inst.stop_loss_field)
        inst.stop_loss_offset_pct = _coerce_float(raw.get("stop_loss_offset_pct"), inst.stop_loss_offset_pct)
        inst.take_profit_mode = str(raw.get("take_profit_mode") or inst.take_profit_mode).upper()
        inst.take_profit_value = _coerce_float(raw.get("take_profit_value"), inst.take_profit_value)
        inst.take_profit_timeframe = str(raw.get("take_profit_timeframe") or inst.take_profit_timeframe)
        inst.take_profit_field = str(raw.get("take_profit_field") or inst.take_profit_field)
        inst.take_profit_offset_pct = _coerce_float(raw.get("take_profit_offset_pct"), inst.take_profit_offset_pct)
        inst.break_even_after_mfe_pct = _coerce_float(raw.get("break_even_after_mfe_pct"), inst.break_even_after_mfe_pct)
        inst.trailing_stop_pct = _coerce_float(raw.get("trailing_stop_pct"), inst.trailing_stop_pct)
        inst.failed_pullback_abort_minutes = max(0, _coerce_int(raw.get("failed_pullback_abort_minutes"), inst.failed_pullback_abort_minutes))
        inst.failed_pullback_abort_min_mfe_pct = _coerce_float(raw.get("failed_pullback_abort_min_mfe_pct"), inst.failed_pullback_abort_min_mfe_pct)
        inst.failed_pullback_abort_max_close_return_pct = _coerce_float(raw.get("failed_pullback_abort_max_close_return_pct"), inst.failed_pullback_abort_max_close_return_pct)
        inst.allow_reverse = _coerce_bool(raw.get("allow_reverse"), inst.allow_reverse)
        inst.skip_both_entries = _coerce_bool(raw.get("skip_both_entries"), inst.skip_both_entries)
        inst.step_timeframe = str(raw.get("step_timeframe") or inst.step_timeframe)
        inst.capture_trade_details = _coerce_bool(raw.get("capture_trade_details"), inst.capture_trade_details)
        inst.equity_curve_stride = max(1, _coerce_int(raw.get("equity_curve_stride"), inst.equity_curve_stride))
        inst.exit_sweep = ExitSweepRequest.from_payload(raw.get("exit_sweep") if isinstance(raw.get("exit_sweep"), dict) else None)
        return inst


class PresetRepository:
    def __init__(self, presets_path: Path = PRESETS_PATH) -> None:
        self.presets_path = Path(presets_path)
        self._lock = threading.Lock()

    def load_payload(self) -> Dict[str, Any]:
        with self._lock:
            return _load_presets(self.presets_path)

    def list_presets(self) -> Dict[str, Any]:
        payload = self.load_payload()
        presets = payload.get("presets", {})
        meta = payload.get("meta", {}) if isinstance(payload.get("meta", {}), dict) else {}
        items: List[Dict[str, Any]] = []
        for name, preset in presets.items():
            if not isinstance(preset, dict):
                continue
            timeframes = _resolve_preset_timeframes(preset)
            settings = preset.get("settings", {}) if isinstance(preset.get("settings", {}), dict) else {}
            tabs = preset.get("tabs", {}) if isinstance(preset.get("tabs", {}), dict) else {}
            items.append(
                {
                    "name": str(name),
                    "version": int(preset.get("version", 0) or 0),
                    "symbol": str(settings.get("symbol", "BTCUSDT") or "BTCUSDT"),
                    "start": str(settings.get("start", "") or ""),
                    "end": str(settings.get("end", "") or ""),
                    "bt_mode": str(settings.get("bt_mode", "") or ""),
                    "timeframes": timeframes,
                    "total_rules": sum(_count_tab_rules(tab) for tab in tabs.values() if isinstance(tab, dict)),
                    "tab_rule_counts": {
                        tab_name: _count_tab_rules(tab_payload)
                        for tab_name, tab_payload in tabs.items()
                        if isinstance(tab_payload, dict)
                    },
                    "strategy_note": str((preset.get("meta", {}) or {}).get("strategy_note", "") or ""),
                    "is_last_used": str(meta.get("last_preset", "") or "") == str(name),
                }
            )
        items.sort(key=lambda item: item["name"])
        return {
            "last_preset": str(meta.get("last_preset", "") or ""),
            "count": len(items),
            "items": items,
        }

    def get_preset(self, name: str) -> Dict[str, Any]:
        payload = self.load_payload()
        preset_name = _pick_preset_name(payload, name)
        preset = payload["presets"][preset_name]
        return {
            "name": preset_name,
            "meta": payload.get("meta", {}),
            "preset": preset,
        }

    def save_preset(self, name: str, preset: Dict[str, Any], *, set_last: bool = True) -> Dict[str, Any]:
        clean_name = str(name or "").strip()
        if not clean_name:
            raise ValueError("Preset name is required.")
        if not isinstance(preset, dict):
            raise ValueError("Preset payload must be a JSON object.")

        with self._lock:
            payload = _load_presets(self.presets_path)
            presets = payload.get("presets", {})
            if not isinstance(presets, dict):
                presets = {}
                payload["presets"] = presets
            presets[clean_name] = preset

            meta = payload.get("meta", {})
            if not isinstance(meta, dict):
                meta = {}
                payload["meta"] = meta
            if set_last:
                meta["last_preset"] = clean_name

            with self.presets_path.open("w", encoding="utf-8") as fh:
                json.dump(payload, fh, indent=2, ensure_ascii=False)
                fh.write("\n")

        return self.get_preset(clean_name)

    def delete_preset(self, name: str) -> Dict[str, Any]:
        clean_name = str(name or "").strip()
        if not clean_name:
            raise ValueError("Preset name is required.")

        with self._lock:
            payload = _load_presets(self.presets_path)
            presets = payload.get("presets", {})
            if not isinstance(presets, dict) or clean_name not in presets:
                raise ValueError(f"Preset '{clean_name}' was not found.")
            del presets[clean_name]

            meta = payload.get("meta", {})
            if not isinstance(meta, dict):
                meta = {}
                payload["meta"] = meta
            if str(meta.get("last_preset", "") or "") == clean_name:
                remaining = sorted(str(key) for key in presets.keys())
                meta["last_preset"] = remaining[0] if remaining else ""

            with self.presets_path.open("w", encoding="utf-8") as fh:
                json.dump(payload, fh, indent=2, ensure_ascii=False)
                fh.write("\n")

        return {
            "deleted": clean_name,
            "count": len(presets),
            "last_preset": str(meta.get("last_preset", "") or ""),
        }


class BacktestRuntimeService:
    def __init__(self, presets_path: Path = PRESETS_PATH, repo_root: Path = REPO_ROOT) -> None:
        self.presets_path = Path(presets_path)
        self.repo_root = Path(repo_root)
        self._snapshot_cache_lock = threading.Lock()
        self._snapshot_cache: Dict[Tuple[Any, ...], SnapshotCacheEntry] = {}
        self._snapshot_cache_ttl_sec = 15.0
        self._run_artifact_lock = threading.Lock()
        self._run_artifacts: Dict[str, BacktestRunArtifact] = {}
        self._latest_run_by_preset: Dict[str, str] = {}
        self._max_run_artifacts = 32

    def _load_stream_bundle(
        self,
        *,
        symbol: str,
        start: pd.Timestamp,
        end: pd.Timestamp,
        timeframes: List[str],
        cache_dir: Path,
        price_source: str,
        macd_impl: str,
        adx_impl: str,
        required_fields: Any,
        ema_settings: Optional[Any] = None,
        rsi_settings: Optional[Any] = None,
        progress_cb: Optional[ProgressCallback] = None,
    ) -> Tuple[pd.DataFrame, Dict[str, pd.DataFrame], bool]:
        bundle = load_stream_bundle_library_first(
            symbol=symbol,
            start=start,
            end=end,
            required_fields=required_fields,
            timeframes=timeframes,
            cache_dir=cache_dir,
            price_source=price_source,
            macd_impl=macd_impl,
            adx_impl=adx_impl,
            ema_settings=ema_settings,
            rsi_settings=rsi_settings,
            progress_cb=progress_cb,
        )
        return bundle.df_1m, bundle.streams_full, bundle.source == "local_indicator_store"

    def _store_run_artifact(self, preset_name: str, result: BacktestResult, payload: Dict[str, Any], *, run_id: Optional[str] = None) -> str:
        next_run_id = str(run_id or uuid.uuid4())
        artifact = BacktestRunArtifact(
            run_id=next_run_id,
            preset_name=str(preset_name or ""),
            result=result,
            payload=dict(payload or {}),
            created_at_utc=str(pd.Timestamp.now(tz="UTC")),
        )
        with self._run_artifact_lock:
            self._run_artifacts[next_run_id] = artifact
            self._latest_run_by_preset[str(preset_name or "")] = next_run_id
            if len(self._run_artifacts) > self._max_run_artifacts:
                ordered = sorted(self._run_artifacts.values(), key=lambda item: item.created_at_utc)
                overflow = max(0, len(ordered) - self._max_run_artifacts)
                for stale in ordered[:overflow]:
                    self._run_artifacts.pop(stale.run_id, None)
                    if self._latest_run_by_preset.get(stale.preset_name) == stale.run_id:
                        self._latest_run_by_preset.pop(stale.preset_name, None)
        return next_run_id

    def _get_run_artifact(self, *, preset_name: Optional[str] = None, run_id: Optional[str] = None) -> Optional[BacktestRunArtifact]:
        with self._run_artifact_lock:
            if run_id:
                return self._run_artifacts.get(str(run_id))
            if preset_name:
                latest_run_id = self._latest_run_by_preset.get(str(preset_name))
                if latest_run_id:
                    return self._run_artifacts.get(latest_run_id)
            return None

    def list_run_trades(
        self,
        *,
        preset_name: Optional[str] = None,
        run_id: Optional[str] = None,
    ) -> Dict[str, Any]:
        artifact = self._get_run_artifact(preset_name=preset_name, run_id=run_id)
        if artifact is None:
            raise ValueError("No compatible completed backtest is available for trades. Run a backtest first.")
        trades = [_trade_payload(trade, include_snapshots=False) for trade in list(artifact.result.trades or [])]
        return {
            "result_kind": "trade_list",
            "run_id": artifact.run_id,
            "preset_name": artifact.preset_name,
            "count": len(trades),
            "trades": trades,
        }

    def get_trade_detail(
        self,
        *,
        trade_id: int,
        preset_name: Optional[str] = None,
        run_id: Optional[str] = None,
        padding_bars: int = 8,
    ) -> Dict[str, Any]:
        artifact = self._get_run_artifact(preset_name=preset_name, run_id=run_id)
        if artifact is None:
            raise ValueError("No compatible completed backtest is available for trade details. Run a backtest first.")
        chosen = None
        for trade in list(artifact.result.trades or []):
            if int(getattr(trade, "trade_id", -1) or -1) == int(trade_id):
                chosen = trade
                break
        if chosen is None:
            raise ValueError(f"Trade '{trade_id}' was not found in run '{artifact.run_id}'.")
        return {
            "result_kind": "trade_detail",
            "run_id": artifact.run_id,
            "preset_name": artifact.preset_name,
            "trade": _trade_payload(chosen, include_snapshots=True),
            "chart": _trade_chart_payload(artifact, chosen, padding_bars=max(0, int(padding_bars))),
        }

    def _cfg_for_exit_combo(self, base_cfg: BacktestConfig, *, stop_loss_pct: float, take_profit_pct: float, break_even_pct: float, trailing_stop_pct: float) -> BacktestConfig:
        return _exit_sweep_cfg_for_combo(
            base_cfg,
            stop_loss_pct=stop_loss_pct,
            take_profit_pct=take_profit_pct,
            break_even_pct=break_even_pct,
            trailing_stop_pct=trailing_stop_pct,
        )

    def _sort_exit_sweep_rows(self, rows: List[Dict[str, Any]], sort_by: str) -> List[Dict[str, Any]]:
        sort_key = _coerce_sort_key(sort_by, "net_profit")
        reverse = sort_key != "max_drawdown_pct"

        def _metric(row: Dict[str, Any]) -> float:
            try:
                value = float(row.get(sort_key, 0.0) or 0.0)
            except Exception:
                value = 0.0
            if not math.isfinite(value):
                return float("-inf") if reverse else float("inf")
            return value

        return sorted(rows, key=_metric, reverse=reverse)

    def _run_exit_sweep_for_result(
        self,
        *,
        preset_name: str,
        run_id: str,
        base_result: BacktestResult,
        request: ExitSweepRequest,
        progress_cb: Optional[ProgressCallback] = None,
    ) -> Dict[str, Any]:
        progress = progress_cb or _null_progress
        sort_by = _coerce_sort_key(request.sort_by, "net_profit")
        stop_values = _expand_sweep_axis("Stop Loss", request.stop_loss)
        take_values = _expand_sweep_axis("Take Profit", request.take_profit)
        break_even_values = _expand_sweep_axis("Break Even", request.break_even)
        trailing_values = _expand_sweep_axis("Trailing Stop", request.trailing_stop)

        combo_count = len(stop_values) * len(take_values) * len(break_even_values) * len(trailing_values)
        if combo_count > max(1, int(request.max_combinations)):
            raise ValueError(f"Exit sweep expands to {combo_count} combinations, above the current limit of {int(request.max_combinations)}.")

        started = time.perf_counter()
        rows: List[Dict[str, Any]] = []
        combos: List[Tuple[float, float, float, float]] = []
        for stop_loss_pct in stop_values:
            for take_profit_pct in take_values:
                for break_even_pct in break_even_values:
                    for trailing_stop_pct in trailing_values:
                        combos.append((
                            float(stop_loss_pct),
                            float(take_profit_pct),
                            float(break_even_pct),
                            float(trailing_stop_pct),
                        ))

        stride = max(1, int(getattr(base_result.config, "equity_curve_stride", 1) or 1))
        parallel_allowed = True
        if os.name == "nt" and not _env_flag("BT_EXIT_SWEEP_FORCE_PARALLEL"):
            parallel_allowed = _spawn_parallel_supported()
        plan = choose_process_batch_plan(
            item_count=combo_count,
            disable_env="BT_EXIT_SWEEP_DISABLE_PARALLEL",
            min_items_envs=("BT_EXIT_SWEEP_PARALLEL_MIN_COMBOS", "EXIT_SWEEP_PARALLEL_MIN_COMBOS"),
            default_min_items=EXIT_SWEEP_PARALLEL_MIN_COMBOS_DEFAULT,
            max_workers_envs=("BT_EXIT_SWEEP_MAX_WORKERS", "EXIT_SWEEP_MAX_WORKERS"),
            sequential_mode="sequential_exit_sweep",
            parallel_mode="parallel_exit_sweep",
            allow_parallel=parallel_allowed,
        )
        execution_mode = plan.execution_mode
        worker_count = plan.worker_count
        task_count = plan.task_count

        progress(
            (
                f"Preparing exit sweep across {combo_count} combinations"
                + (
                    " (sequential mode: current Python session cannot safely spawn workers)"
                    if execution_mode == "sequential_exit_sweep" and os.name == "nt" and not parallel_allowed
                    else f" ({'parallel' if execution_mode == 'parallel_exit_sweep' else 'sequential'} mode)"
                )
            )
            + ".",
            90.0,
        )

        completed = 0
        if execution_mode == "parallel_exit_sweep":
            combo_batches = split_batches(combos, batch_size=plan.batch_size)
            task_count = len(combo_batches)
            try:
                with ProcessPoolExecutor(max_workers=worker_count) as executor:
                    futures = [
                        executor.submit(
                            _run_exit_sweep_combo_batch,
                            {
                                "base_result": base_result,
                                "equity_curve_stride": stride,
                                "combos": batch,
                            },
                        )
                        for batch in combo_batches
                    ]
                    for future in as_completed(futures):
                        batch_rows = list(future.result() or [])
                        rows.extend(batch_rows)
                        completed += len(batch_rows)
                        if batch_rows:
                            tail = batch_rows[-1]
                            progress_pct = 90.0 + (completed / max(1, combo_count)) * 10.0
                            progress(
                                f"Exit sweep {completed}/{combo_count}: SL={tail['stop_loss_label']} | TP={tail['take_profit_label']} | BE={tail['break_even_label']} | TR={tail['trailing_stop_label']}",
                                progress_pct,
                            )
            except Exception as exc:
                progress(
                    f"Parallel exit sweep failed ({type(exc).__name__}). Falling back to sequential mode...",
                    90.0,
                )
                rows = []
                completed = 0
                execution_mode = "sequential_exit_sweep_fallback"
                worker_count = 1
                task_count = combo_count
                for combo in combos:
                    batch_rows = _run_exit_sweep_combo_batch(
                        {
                            "base_result": base_result,
                            "equity_curve_stride": stride,
                            "combos": [combo],
                        }
                    )
                    rows.extend(batch_rows)
                    completed += len(batch_rows)
                    if batch_rows:
                        row = batch_rows[-1]
                        progress_pct = 90.0 + (completed / max(1, combo_count)) * 10.0
                        progress(
                            f"Exit sweep {completed}/{combo_count}: SL={row['stop_loss_label']} | TP={row['take_profit_label']} | BE={row['break_even_label']} | TR={row['trailing_stop_label']}",
                            progress_pct,
                        )
        else:
            task_count = combo_count
            for combo in combos:
                batch_rows = _run_exit_sweep_combo_batch(
                    {
                        "base_result": base_result,
                        "equity_curve_stride": stride,
                        "combos": [combo],
                    }
                )
                rows.extend(batch_rows)
                completed += len(batch_rows)
                if batch_rows:
                    row = batch_rows[-1]
                    progress_pct = 90.0 + (completed / max(1, combo_count)) * 10.0
                    progress(
                        f"Exit sweep {completed}/{combo_count}: SL={row['stop_loss_label']} | TP={row['take_profit_label']} | BE={row['break_even_label']} | TR={row['trailing_stop_label']}",
                        progress_pct,
                    )

        ranked_rows = self._sort_exit_sweep_rows(rows, sort_by)
        limited_rows = []
        for rank, row in enumerate(ranked_rows[: max(1, int(request.top_n))], start=1):
            next_row = dict(row)
            next_row["rank"] = int(rank)
            limited_rows.append(next_row)

        best_payload = dict(limited_rows[0]) if limited_rows else None
        best_result: Optional[BacktestResult] = None
        if best_payload is not None:
            best_cfg = self._cfg_for_exit_combo(
                base_result.config,
                stop_loss_pct=float(best_payload.get("stop_loss_pct") or 0.0),
                take_profit_pct=float(best_payload.get("take_profit_pct") or 0.0),
                break_even_pct=float(best_payload.get("break_even_after_mfe_pct") or 0.0),
                trailing_stop_pct=float(best_payload.get("trailing_stop_pct") or 0.0),
            )
            best_result = replay_backtest_result(
                base_result,
                cfg=best_cfg,
                capture_trade_details=False,
                equity_curve_stride=stride,
            )
        return _json_safe_value({
            "result_kind": "exit_sweep",
            "preset_name": str(preset_name or ""),
            "run_id": str(run_id or ""),
            "sort_by": sort_by,
            "combo_count": int(combo_count),
            "top_n": int(request.top_n),
            "execution_mode": execution_mode,
            "worker_count": int(worker_count),
            "task_count": int(task_count),
            "batch_size": int(plan.batch_size),
            "rows": limited_rows,
            "best_combo": best_payload,
            "best_preview": {
                "summary": dict(best_result.summary or {}) if best_result is not None else {},
                "sample_trades": _sample_trades_payload(list(best_result.trades or [])) if best_result is not None else [],
                "equity_curve_preview": _sample_equity_curve(best_result.equity_curve) if best_result is not None else [],
            },
            "duration_sec": round(time.perf_counter() - started, 4),
        })

    def get_preset_snapshot(self, preset_name: str) -> Dict[str, Any]:
        payload = _load_presets(self.presets_path)
        chosen_preset_name = _pick_preset_name(payload, preset_name)
        preset = payload["presets"][chosen_preset_name]
        settings = preset.get("settings", {})
        if not isinstance(settings, dict):
            raise ValueError("Preset settings payload is invalid.")
        symbol = str(settings.get("symbol", "BTCUSDT") or "BTCUSDT").strip().upper()
        tfs = _sync_preset_timeframes(preset)
        try:
            presets_mtime_ns = int(self.presets_path.stat().st_mtime_ns)
        except Exception:
            presets_mtime_ns = 0
        cache_key = (
            chosen_preset_name,
            presets_mtime_ns,
            symbol,
            tuple(tfs),
        )
        return self._build_snapshot_for_preset(chosen_preset_name, preset, cache_key=cache_key)

    def get_preset_snapshot_preview(self, preset_name: str, preset: Dict[str, Any]) -> Dict[str, Any]:
        if not isinstance(preset, dict):
            raise ValueError("Preset preview payload must be a JSON object.")
        chosen_preset_name = str(preset_name or "builder_preview").strip() or "builder_preview"
        digest = hashlib.sha1(
            json.dumps(preset, sort_keys=True, separators=(",", ":"), default=str).encode("utf-8")
        ).hexdigest()
        cache_key = ("builder_preview", chosen_preset_name, digest)
        return self._build_snapshot_for_preset(chosen_preset_name, preset, cache_key=cache_key)

    def get_preset_market_audit(
        self,
        preset_name: str,
        *,
        timeframe: str = "5m",
        bars: int = MARKET_AUDIT_DEFAULT_BARS,
        end_utc: Optional[str] = None,
    ) -> Dict[str, Any]:
        payload = _load_presets(self.presets_path)
        chosen_preset_name = _pick_preset_name(payload, preset_name)
        preset = payload["presets"][chosen_preset_name]
        return self._build_market_audit_for_preset(
            chosen_preset_name,
            preset,
            timeframe=timeframe,
            bars=bars,
            end_utc=end_utc,
        )

    def get_preset_market_audit_preview(
        self,
        preset_name: str,
        preset: Dict[str, Any],
        *,
        timeframe: str = "5m",
        bars: int = MARKET_AUDIT_DEFAULT_BARS,
        end_utc: Optional[str] = None,
    ) -> Dict[str, Any]:
        if not isinstance(preset, dict):
            raise ValueError("Preset preview payload must be a JSON object.")
        chosen_preset_name = str(preset_name or "builder_preview").strip() or "builder_preview"
        return self._build_market_audit_for_preset(
            chosen_preset_name,
            preset,
            timeframe=timeframe,
            bars=bars,
            end_utc=end_utc,
        )

    def get_market_audit_from_settings(
        self,
        session_name: str,
        settings: Dict[str, Any],
        *,
        timeframe: str = "5m",
        bars: int = MARKET_AUDIT_DEFAULT_BARS,
        end_utc: Optional[str] = None,
    ) -> Dict[str, Any]:
        if not isinstance(settings, dict):
            raise ValueError("Live market-audit settings must be a JSON object.")
        enabled = [
            str(tf)
            for tf, flag in (settings.get("timeframes", {}) or {}).items()
            if bool(flag) and _normalize_timeframe_name(tf)
        ]
        timeframes = _ordered_timeframe_list(enabled) or list(ALL_TIMEFRAMES)
        return self._build_market_audit_from_settings(
            settings,
            timeframe=timeframe,
            bars=bars,
            end_utc=end_utc,
            audit_name=str(session_name or "live_market").strip() or "live_market",
            audit_kind="live_context",
            preset_name=None,
            timeframes=timeframes,
        )

    def _build_market_audit_for_preset(
        self,
        preset_name: str,
        preset: Dict[str, Any],
        *,
        timeframe: str,
        bars: int,
        end_utc: Optional[str] = None,
    ) -> Dict[str, Any]:
        settings = preset.get("settings", {})
        if not isinstance(settings, dict):
            raise ValueError("Preset settings payload is invalid.")
        return self._build_market_audit_from_settings(
            settings,
            timeframe=timeframe,
            bars=bars,
            end_utc=end_utc,
            audit_name=str(preset_name or "").strip() or "builder_preview",
            audit_kind="preset",
            preset_name=str(preset_name or "").strip() or None,
            timeframes=_sync_preset_timeframes(preset),
        )

    def _build_market_audit_from_settings(
        self,
        settings: Dict[str, Any],
        *,
        timeframe: str,
        bars: int,
        end_utc: Optional[str] = None,
        audit_name: str,
        audit_kind: str,
        preset_name: Optional[str],
        timeframes: List[str],
    ) -> Dict[str, Any]:
        symbol = str(settings.get("symbol", "BTCUSDT") or "BTCUSDT").strip().upper()
        tfs = _ordered_timeframe_list(list(timeframes or [])) or ["1m"]
        chosen_tf = _normalize_timeframe_name(timeframe) or (tfs[0] if tfs else "1m")
        if chosen_tf not in tfs:
            tfs.append(chosen_tf)
            tfs = _ordered_timeframe_list(tfs)

        price_source = str(settings.get("price_source", "LAST") or "LAST").upper()
        macd_impl = str(settings.get("macd_impl", "TRADINGVIEW") or "TRADINGVIEW").upper()
        adx_impl = str(settings.get("adx_impl", "TRADINGVIEW") or "TRADINGVIEW").upper()
        cache_dir = _resolve_cache_dir(
            self.repo_root,
            str(settings.get("cache_dir", "data_cache") or "data_cache"),
        )
        cache_dir.mkdir(parents=True, exist_ok=True)

        audit_bars = _coerce_market_audit_bars(bars)
        audit_required_fields = required_market_audit_fields(
            (
                "market_structure_flag",
                "market_structure_note",
                "market_structure_context",
                "market_structure_basis",
                "market_structure_pivot_bias",
                "market_structure_last_high",
                "market_structure_last_low",
                "market_boundary_high",
                "market_boundary_low",
                "market_breakout_note",
            )
        )
        warmup = int(required_warmup_minutes(tfs, required_fields=audit_required_fields, ema_settings=settings, rsi_settings=settings) or 0)
        lookback_minutes = max(
            180,
            warmup + (_timeframe_minutes(chosen_tf) * (audit_bars + 12)),
        )

        resolved_end: Optional[pd.Timestamp] = None
        if str(end_utc or "").strip():
            resolved_end = parse_utc(str(end_utc or "").strip())
        if resolved_end is None:
            try:
                resolved_end = parse_utc(str(settings.get("end", "") or ""))
            except Exception:
                resolved_end = None
        if resolved_end is None:
            resolved_end = pd.Timestamp.now(tz="UTC").floor("min")
        window_end = pd.to_datetime(resolved_end, utc=True, errors="coerce")
        if pd.isna(window_end):
            raise ValueError("Unable to resolve a valid market-audit end timestamp.")
        window_end = window_end.floor("min")
        window_start = window_end - pd.Timedelta(minutes=lookback_minutes)

        df_1m, streams_full, used_prepared = self._load_stream_bundle(
            symbol=symbol,
            start=window_start,
            end=window_end,
            timeframes=tfs,
            cache_dir=cache_dir,
            price_source=price_source,
            macd_impl=macd_impl,
            adx_impl=adx_impl,
            required_fields=audit_required_fields,
            ema_settings=settings,
            rsi_settings=settings,
            progress_cb=None,
        )

        frame = streams_full.get(chosen_tf)
        audit_frame = _merge_market_audit_chart_frame(
            frame if isinstance(frame, pd.DataFrame) else pd.DataFrame(),
            df_1m,
            chosen_tf,
        )
        audit_payload = build_market_context_audit_window(
            audit_frame,
            timeframe=chosen_tf,
            max_bars=audit_bars,
            end_utc=window_end,
        )
        return {
            "result_kind": "market_context_audit",
            "preset_name": str(preset_name or "") if preset_name else None,
            "audit_target_name": str(audit_name or ""),
            "audit_target_kind": str(audit_kind or "preset"),
            "symbol": symbol,
            "timeframe": chosen_tf,
            "requested_bars": int(audit_bars),
            "analysis_window_start_utc": str(window_start),
            "analysis_window_end_utc": str(window_end),
            "analysis_lookback_minutes": int(lookback_minutes),
            "analysis_warmup_minutes": int(max(0, warmup)),
            "window_start_utc": audit_payload.get("window_start_utc"),
            "window_end_utc": audit_payload.get("window_end_utc"),
            "anchor_timestamp_utc": audit_payload.get("anchor_timestamp_utc"),
            "bar_count": int(audit_payload.get("bar_count") or 0),
            "rows": list(audit_payload.get("rows") or []),
            "markers": list(audit_payload.get("markers") or []),
            "latest_context": dict(audit_payload.get("latest_context") or {}),
            "latest_context_by_timeframe": build_timeframe_context_summary(streams_full),
            "available_timeframes": list(tfs),
            "audit_source_note": "prepared-cache" if used_prepared else "fresh-streams",
            "request_end_utc": str(window_end),
        }

    def _build_snapshot_for_preset(
        self,
        preset_name: str,
        preset: Dict[str, Any],
        *,
        cache_key: Optional[Tuple[Any, ...]] = None,
    ) -> Dict[str, Any]:
        settings = preset.get("settings", {})
        if not isinstance(settings, dict):
            raise ValueError("Preset settings payload is invalid.")

        symbol = str(settings.get("symbol", "BTCUSDT") or "BTCUSDT").strip().upper()
        tfs = _sync_preset_timeframes(preset)

        price_source = str(settings.get("price_source", "LAST") or "LAST").upper()
        macd_impl = str(settings.get("macd_impl", "TRADINGVIEW") or "TRADINGVIEW").upper()
        adx_impl = str(settings.get("adx_impl", "TRADINGVIEW") or "TRADINGVIEW").upper()
        cache_dir = _resolve_cache_dir(
            self.repo_root,
            str(settings.get("cache_dir", "data_cache") or "data_cache"),
        )
        cache_dir.mkdir(parents=True, exist_ok=True)

        warmup = int(required_warmup_minutes(tfs, required_fields=SNAPSHOT_REQUIRED_FIELDS, ema_settings=settings, rsi_settings=settings) or 0)
        snapshot_lookback = max(180, warmup + 90)

        live_end = pd.Timestamp.now(tz="UTC").floor("min")
        live_start = live_end - pd.Timedelta(minutes=snapshot_lookback)
        minute_bucket = live_end.strftime("%Y-%m-%dT%H:%M")

        if cache_key is not None:
            with self._snapshot_cache_lock:
                cached = self._snapshot_cache.get(cache_key)
                if cached is not None:
                    age_sec = time.monotonic() - cached.created_monotonic
                    if age_sec <= self._snapshot_cache_ttl_sec:
                        payload_copy = dict(cached.payload)
                        payload_copy["snapshot_cached"] = True
                        payload_copy["snapshot_refresh_mode"] = "polling"
                        payload_copy["snapshot_cache_age_sec"] = round(age_sec, 3)
                        payload_copy["snapshot_is_stale_minute"] = cached.minute_bucket != minute_bucket
                        return payload_copy

        candidates: List[Tuple[str, pd.Timestamp, pd.Timestamp]] = [("live", live_start, live_end)]
        try:
            preset_end = parse_utc(str(settings.get("end", "") or ""))
        except Exception:
            preset_end = None
        if preset_end is not None:
            preset_start = preset_end - pd.Timedelta(minutes=snapshot_lookback)
            if abs((preset_end - live_end).total_seconds()) > 60:
                candidates.append(("preset_window", preset_start, preset_end))

        last_error: Optional[Exception] = None
        for source_name, window_start, window_end in candidates:
            try:
                _, streams_full, used_prepared = self._load_stream_bundle(
                    symbol=symbol,
                    start=window_start,
                    end=window_end,
                    timeframes=tfs,
                    cache_dir=cache_dir,
                    price_source=price_source,
                    macd_impl=macd_impl,
                    adx_impl=adx_impl,
                    required_fields=SNAPSHOT_REQUIRED_FIELDS,
                    ema_settings=settings,
                    rsi_settings=settings,
                    progress_cb=None,
                )
                rows: List[Dict[str, Any]] = []
                snapshot_as_of: Optional[str] = None
                for tf in tfs:
                    frame = streams_full.get(tf)
                    if not isinstance(frame, pd.DataFrame) or frame.empty:
                        rows.append(
                            {
                                "timeframe": str(tf),
                                "timestamp_utc": None,
                                "values": {},
                            }
                        )
                        continue
                    last_idx = pd.to_datetime(frame.index[-1], utc=True, errors="coerce")
                    if pd.notna(last_idx):
                        snapshot_as_of = str(last_idx)
                    last_row = frame.iloc[-1]
                    values = {
                        field: _serialize_snapshot_value(last_row.get(field))
                        for field in SNAPSHOT_REQUIRED_FIELDS
                        if field in frame.columns
                    }
                    range_display_state = _range_display_state(
                        last_row.get("range_filter_state"),
                        last_row.get("range_filter_phase"),
                        last_row.get("range_filter_cond_ini"),
                    )
                    values["frama_state_age"] = _latest_state_age_bars(frame, "frama_state")
                    values["frama_break_up_age"] = _latest_event_age_bars(frame, "frama_break_up")
                    values["frama_break_down_age"] = _latest_event_age_bars(frame, "frama_break_down")
                    values["frama_mid_cross_age"] = _latest_event_age_bars(frame, "frama_mid_cross")
                    frama_trigger_side, frama_trigger_age = _latest_directional_event_side_age(
                        frame,
                        "frama_break_up",
                        "frama_break_down",
                    )
                    values["frama_trigger_side"] = frama_trigger_side
                    values["frama_trigger_age"] = frama_trigger_age
                    values["frama_display_age"] = frama_trigger_age
                    values["range_filter_state_age"] = _latest_state_age_bars(frame, "range_filter_state")
                    values["range_filter_buy_age"] = _latest_event_age_bars(frame, "range_filter_buy")
                    values["range_filter_sell_age"] = _latest_event_age_bars(frame, "range_filter_sell")
                    values["range_filter_display_state"] = range_display_state
                    values["range_filter_display_age"] = (
                        values["range_filter_buy_age"] if range_display_state == "BUY"
                        else (values["range_filter_sell_age"] if range_display_state == "SELL" else None)
                    )
                    rows.append(
                        {
                            "timeframe": str(tf),
                            "timestamp_utc": str(last_idx) if pd.notna(last_idx) else None,
                            "values": values,
                        }
                    )
                result = {
                    "preset_name": preset_name,
                    "symbol": symbol,
                    "snapshot_source": source_name,
                    "snapshot_source_note": "prepared-cache" if used_prepared else "fresh-streams",
                    "window_start_utc": str(window_start),
                    "window_end_utc": str(window_end),
                    "snapshot_as_of_utc": snapshot_as_of,
                    "timeframes": tfs,
                    "rows": rows,
                    "snapshot_cached": False,
                    "snapshot_refresh_mode": "polling",
                    "snapshot_cache_age_sec": 0.0,
                    "snapshot_is_stale_minute": False,
                }
                if source_name == "live" and cache_key is not None:
                    with self._snapshot_cache_lock:
                        self._snapshot_cache[cache_key] = SnapshotCacheEntry(
                            payload=result,
                            created_monotonic=time.monotonic(),
                            minute_bucket=minute_bucket,
                        )
                return result
            except Exception as exc:
                last_error = exc
                continue

        if last_error is not None:
            raise last_error
        raise ValueError("Unable to build snapshot for the selected preset.")

    def run_saved_preset(
        self,
        preset_name: str,
        overrides: Optional[RunOverrides] = None,
        progress_cb: Optional[ProgressCallback] = None,
    ) -> Dict[str, Any]:
        progress = progress_cb or _null_progress
        args = overrides or RunOverrides()
        started = time.perf_counter()

        payload = _load_presets(self.presets_path)
        chosen_preset_name = _pick_preset_name(payload, preset_name)
        preset = payload["presets"][chosen_preset_name]

        settings = preset.get("settings", {})
        if not isinstance(settings, dict):
            raise ValueError("Preset settings payload is invalid.")

        symbol = str(settings.get("symbol", "BTCUSDT") or "BTCUSDT").strip().upper()
        start = parse_utc(str(args.start or settings.get("start", "") or ""))
        end = parse_utc(str(args.end or settings.get("end", "") or ""))
        if end <= start:
            raise ValueError("Preset end must be after start.")

        tfs = _sync_preset_timeframes(preset)
        if "1m" not in tfs:
            tfs = ["1m"] + tfs
        if args.step_timeframe not in tfs:
            tfs.append(args.step_timeframe)

        rules_model, tab_group_join_mode, group_rule_join_mode, entry_filters = _materialize_preset(preset)

        default_args = RunOverrides()
        explicit_overrides = set(getattr(args, "provided_fields", set()) or set())

        def _has_override(field_name: str) -> bool:
            if field_name in explicit_overrides:
                return True
            return getattr(args, field_name) != getattr(default_args, field_name)

        def _resolve_runtime_value(field_name: str, fallback: Any) -> Any:
            if _has_override(field_name):
                return getattr(args, field_name)
            return settings.get(field_name, fallback)

        stop_loss_mode = str(_resolve_runtime_value("stop_loss_mode", args.stop_loss_mode) or args.stop_loss_mode).upper()
        stop_loss_value = float(_resolve_runtime_value("stop_loss_value", args.stop_loss_value) or 0.0)
        stop_loss_timeframe = str(_resolve_runtime_value("stop_loss_timeframe", args.stop_loss_timeframe) or args.stop_loss_timeframe)
        stop_loss_field = str(_resolve_runtime_value("stop_loss_field", args.stop_loss_field) or "")
        stop_loss_offset_pct = float(_resolve_runtime_value("stop_loss_offset_pct", args.stop_loss_offset_pct) or 0.0)
        take_profit_mode = str(_resolve_runtime_value("take_profit_mode", args.take_profit_mode) or args.take_profit_mode).upper()
        take_profit_value = float(_resolve_runtime_value("take_profit_value", args.take_profit_value) or 0.0)
        take_profit_timeframe = str(_resolve_runtime_value("take_profit_timeframe", args.take_profit_timeframe) or args.take_profit_timeframe)
        take_profit_field = str(_resolve_runtime_value("take_profit_field", args.take_profit_field) or "")
        take_profit_offset_pct = float(_resolve_runtime_value("take_profit_offset_pct", args.take_profit_offset_pct) or 0.0)
        break_even_after_mfe_pct = float(_resolve_runtime_value("break_even_after_mfe_pct", args.break_even_after_mfe_pct) or 0.0)
        trailing_stop_pct = float(_resolve_runtime_value("trailing_stop_pct", args.trailing_stop_pct) or 0.0)
        failed_pullback_abort_minutes = int(_resolve_runtime_value("failed_pullback_abort_minutes", args.failed_pullback_abort_minutes) or 0)
        failed_pullback_abort_min_mfe_pct = float(_resolve_runtime_value("failed_pullback_abort_min_mfe_pct", args.failed_pullback_abort_min_mfe_pct) or 0.0)
        failed_pullback_abort_max_close_return_pct = float(_resolve_runtime_value("failed_pullback_abort_max_close_return_pct", args.failed_pullback_abort_max_close_return_pct) or 0.0)

        cfg = BacktestConfig(
            initial_balance=float(args.initial_balance),
            order_notional_usdt=float(args.order_notional),
            fee_rate=float(args.fee_pct) / 100.0,
            slippage_bps=float(args.slippage_bps),
            stop_loss_pct=(stop_loss_value if stop_loss_mode == "PERCENT" else 0.0),
            take_profit_pct=(take_profit_value if take_profit_mode == "PERCENT" else 0.0),
            stop_loss_mode=stop_loss_mode,
            stop_loss_value=stop_loss_value,
            stop_loss_timeframe=stop_loss_timeframe,
            stop_loss_field=stop_loss_field,
            stop_loss_offset_pct=stop_loss_offset_pct,
            take_profit_mode=take_profit_mode,
            take_profit_value=take_profit_value,
            take_profit_timeframe=take_profit_timeframe,
            take_profit_field=take_profit_field,
            take_profit_offset_pct=take_profit_offset_pct,
            break_even_after_mfe_pct=break_even_after_mfe_pct,
            trailing_stop_pct=trailing_stop_pct,
            failed_pullback_abort_minutes=max(0, int(failed_pullback_abort_minutes)),
            failed_pullback_abort_min_mfe_pct=float(failed_pullback_abort_min_mfe_pct),
            failed_pullback_abort_max_close_return_pct=float(failed_pullback_abort_max_close_return_pct),
            allow_reverse=bool(args.allow_reverse),
            skip_if_both_entries=bool(args.skip_both_entries),
            step_timeframe=str(args.step_timeframe),
            capture_trade_details=bool(args.capture_trade_details),
            equity_curve_stride=max(1, int(args.equity_curve_stride)),
        )

        for extra_tf in (
            str(cfg.step_timeframe or "").strip(),
            str(cfg.stop_loss_timeframe or "").strip() if str(cfg.stop_loss_mode or "").upper() in {"FIELD", "ATR"} else "",
            str(cfg.take_profit_timeframe or "").strip() if str(cfg.take_profit_mode or "").upper() in {"FIELD", "ATR"} else "",
        ):
            if extra_tf and extra_tf not in tfs:
                tfs.append(extra_tf)

        req_spec = compile_stream_requirements(
            rules_model,
            entry_filters=entry_filters,
            backtest_cfg=cfg,
            include_plot_defaults=True,
        )

        preset_warmup = int(settings.get("warmup_min", 0) or 0)
        needed_warmup = required_warmup_minutes(tfs, required_fields=req_spec, ema_settings=settings, rsi_settings=settings)
        warmup = max(preset_warmup, int(needed_warmup or 0))
        warmup_start = start - pd.Timedelta(minutes=warmup)

        cache_dir = _resolve_cache_dir(
            self.repo_root,
            args.cache_dir or str(settings.get("cache_dir", "data_cache") or "data_cache"),
        )
        cache_dir.mkdir(parents=True, exist_ok=True)

        bt_mode = str(settings.get("bt_mode", "") or "")
        price_source = str(settings.get("price_source", "LAST") or "LAST").upper()
        macd_impl = str(settings.get("macd_impl", "TRADINGVIEW") or "TRADINGVIEW").upper()
        adx_impl = str(settings.get("adx_impl", "TRADINGVIEW") or "TRADINGVIEW").upper()

        bundle = load_stream_bundle_library_first(
            symbol=symbol,
            start=warmup_start,
            end=end,
            required_fields=req_spec,
            timeframes=tfs,
            cache_dir=cache_dir,
            price_source=price_source,
            macd_impl=macd_impl,
            adx_impl=adx_impl,
            ema_settings=settings,
            rsi_settings=settings,
            progress_cb=progress,
        )
        df_1m = bundle.df_1m
        streams_full = bundle.streams_full

        progress("Running backtest engine...", 55)
        if bt_mode == BACKTEST_MODE_TICK:
            res = run_backtest_tick(
                symbol=symbol,
                df_1m_full=df_1m,
                start=start,
                end=end,
                tfs=tfs,
                rules_model=rules_model,
                tab_group_join_mode=tab_group_join_mode,
                group_rule_join_mode=group_rule_join_mode,
                entry_filters=entry_filters,
                cfg=cfg,
                cache_dir=str(cache_dir),
                progress_cb=progress,
                streams_full=streams_full,
                macd_impl=macd_impl,
            )
        else:
            if bt_mode == BACKTEST_MODE_BAR_1M_HTF_CLOSED_ONLY:
                progress("Planning compiled backtest path...", 45)
            res = run_backtest_auto(
                symbol=symbol,
                streams_full=streams_full,
                start=start,
                end=end,
                rules_model=rules_model,
                tab_group_join_mode=tab_group_join_mode,
                group_rule_join_mode=group_rule_join_mode,
                cfg=cfg,
                df_1m_full=df_1m,
                entry_filters=entry_filters,
                apply_htf_closed_only=(bt_mode == BACKTEST_MODE_BAR_1M_HTF_CLOSED_ONLY),
            )

        progress("Collecting verification artifacts...", 85)
        summary = dict(res.summary or {})
        summary["num_trades"] = int(
            summary.get("num_trades")
            or summary.get("trades")
            or (int(summary.get("wins") or 0) + int(summary.get("losses") or 0))
            or 0
        )
        event_activity = _event_activity_summary(rules_model, streams_full, start, end)
        signal_rows = _event_signal_rows(rules_model, streams_full, start, end)
        bundle_dir = _write_verification_bundle(
            repo_root=self.repo_root,
            preset_name=chosen_preset_name,
            symbol=symbol,
            start=start,
            end=end,
            bt_mode=bt_mode,
            cache_dir=cache_dir,
            cfg=cfg,
            summary=summary,
            event_activity=event_activity,
            signal_rows=signal_rows,
            res=res,
        )
        summary.setdefault("timing_total_sec", round(time.perf_counter() - started, 4))

        run_id = str(uuid.uuid4())
        result_payload = {
            "result_kind": "backtest",
            "run_id": run_id,
            "preset_name": chosen_preset_name,
            "symbol": symbol,
            "start": str(start),
            "end": str(end),
            "warmup_minutes": warmup,
            "cache_dir": str(cache_dir),
            "config": {
                "allow_reverse": bool(cfg.allow_reverse),
                "skip_if_both_entries": bool(cfg.skip_if_both_entries),
                "stop_loss_mode": str(cfg.stop_loss_mode),
                "stop_loss_value": float(cfg.stop_loss_value),
                "take_profit_mode": str(cfg.take_profit_mode),
                "take_profit_value": float(cfg.take_profit_value),
                "break_even_after_mfe_pct": float(cfg.break_even_after_mfe_pct),
                "trailing_stop_pct": float(cfg.trailing_stop_pct),
                "failed_pullback_abort_minutes": int(cfg.failed_pullback_abort_minutes),
                "failed_pullback_abort_min_mfe_pct": float(cfg.failed_pullback_abort_min_mfe_pct),
                "failed_pullback_abort_max_close_return_pct": float(cfg.failed_pullback_abort_max_close_return_pct),
                "capture_trade_details": bool(cfg.capture_trade_details),
                "equity_curve_stride": int(cfg.equity_curve_stride),
            },
            "summary": summary,
            "event_activity": event_activity,
            "signal_rows_preview": signal_rows[:120],
            "sample_trades": _sample_trades_payload(list(res.trades or [])),
            "equity_curve_preview": _sample_equity_curve(res.equity_curve),
            "verification_bundle": str(bundle_dir),
        }

        result_payload["exit_sweep"] = None
        result_payload["exit_sweep_error"] = None
        if args.exit_sweep is not None and bool(args.exit_sweep.auto_run):
            try:
                progress("Running exit sweep across combinations...", 90)
                result_payload["exit_sweep"] = self._run_exit_sweep_for_result(
                    preset_name=chosen_preset_name,
                    run_id=run_id,
                    base_result=res,
                    request=args.exit_sweep,
                    progress_cb=progress,
                )
            except Exception as exc:
                result_payload["exit_sweep_error"] = str(exc)

        result_payload = _json_safe_value(result_payload)
        self._store_run_artifact(chosen_preset_name, res, result_payload, run_id=run_id)
        progress("Backtest complete.", 100)
        return result_payload

    def run_saved_preset_batch(
        self,
        preset_name: str,
        cases: List[Dict[str, Any]],
        progress_cb: Optional[ProgressCallback] = None,
    ) -> Dict[str, Any]:
        progress = progress_cb or _null_progress
        raw_cases = list(cases or [])
        if not raw_cases:
            raise ValueError("At least one batch case is required.")

        normalized_cases: List[Dict[str, Any]] = []
        for idx, raw_case in enumerate(raw_cases, start=1):
            if not isinstance(raw_case, dict):
                raise ValueError("Each batch case must be a JSON object.")
            label = str(raw_case.get("label") or f"Case {idx}").strip() or f"Case {idx}"
            overrides_payload = raw_case.get("overrides", raw_case)
            if not isinstance(overrides_payload, dict):
                raise ValueError("Each batch case overrides payload must be a JSON object.")
            normalized_cases.append(
                {
                    "label": label,
                    "case_index": int(idx),
                    "overrides": dict(overrides_payload),
                }
            )

        started = time.perf_counter()
        case_count = len(normalized_cases)
        plan = choose_process_batch_plan(
            item_count=case_count,
            disable_env="BT_BACKTEST_BATCH_DISABLE_PARALLEL",
            min_items_envs=("BT_BACKTEST_BATCH_PARALLEL_MIN_CASES", "BACKTEST_BATCH_PARALLEL_MIN_CASES"),
            default_min_items=BACKTEST_BATCH_PARALLEL_MIN_CASES_DEFAULT,
            max_workers_envs=("BT_BACKTEST_BATCH_MAX_WORKERS", "BACKTEST_BATCH_MAX_WORKERS"),
            sequential_mode="sequential_backtest_batch",
            parallel_mode="parallel_backtest_batch",
        )
        execution_mode = plan.execution_mode
        worker_count = plan.worker_count

        progress(f"Running backtest batch across {case_count} case(s)...", 1.0)

        completed = 0
        case_results: List[Dict[str, Any]] = []
        if execution_mode == "parallel_backtest_batch":
            case_batches = split_batches(normalized_cases, batch_size=plan.batch_size)
            with ProcessPoolExecutor(max_workers=worker_count) as executor:
                futures = [
                    executor.submit(
                        _run_saved_preset_batch_case_batch,
                        {
                            "repo_root": str(self.repo_root),
                            "presets_path": str(self.presets_path),
                            "preset_name": str(preset_name),
                            "cases": [
                                {
                                    "label": case["label"],
                                    "case_index": case["case_index"],
                                    "overrides": case["overrides"],
                                }
                                for case in batch
                            ],
                        },
                    )
                    for batch in case_batches
                ]
                for future in as_completed(futures):
                    batch_rows = list(future.result() or [])
                    for case_result in batch_rows:
                        case_result = dict(case_result or {})
                        case_results.append(case_result)
                        completed += 1
                        progress(
                            f"Completed batch case {case_result.get('label') or completed} ({completed}/{case_count}).",
                            1.0 + (completed / max(1, case_count)) * 99.0,
                        )
        else:
            for case in normalized_cases:
                case_result = _run_saved_preset_batch_case(
                    {
                        "repo_root": str(self.repo_root),
                        "presets_path": str(self.presets_path),
                        "preset_name": str(preset_name),
                        "label": case["label"],
                        "case_index": case["case_index"],
                        "overrides": case["overrides"],
                    }
                )
                case_results.append(case_result)
                completed += 1
                progress(
                    f"Completed batch case {case_result.get('label') or completed} ({completed}/{case_count}).",
                    1.0 + (completed / max(1, case_count)) * 99.0,
                )

        case_results.sort(key=lambda item: int(item.get("case_index") or 0))
        rows = [
            _backtest_batch_row_from_payload(
                label=str(case_result.get("label") or ""),
                case_index=int(case_result.get("case_index") or 0),
                payload=dict(case_result.get("payload") or {}),
            )
            for case_result in case_results
        ]
        ranked_rows = sorted(
            rows,
            key=lambda row: float(row.get("net_profit") or float("-inf")),
            reverse=True,
        )
        best_case = dict(ranked_rows[0]) if ranked_rows else None
        summarized_cases = []
        for case_result in case_results:
            payload = dict(case_result.get("payload") or {})
            summarized_cases.append(
                {
                    "label": str(case_result.get("label") or ""),
                    "case_index": int(case_result.get("case_index") or 0),
                    "result": _strip_backtest_batch_payload(payload),
                }
            )
        return _json_safe_value(
            {
                "result_kind": "backtest_batch",
                "preset_name": str(preset_name or ""),
                "case_count": int(case_count),
                "execution_mode": execution_mode,
                "worker_count": int(worker_count),
                "task_count": int(plan.task_count),
                "batch_size": int(plan.batch_size),
                "cases": summarized_cases,
                "rows": ranked_rows,
                "best_case": best_case,
                "duration_sec": round(time.perf_counter() - started, 4),
            }
        )

    def run_exit_sweep(
        self,
        *,
        preset_name: str,
        run_id: Optional[str],
        request: ExitSweepRequest,
        progress_cb: Optional[ProgressCallback] = None,
    ) -> Dict[str, Any]:
        artifact = self._get_run_artifact(preset_name=preset_name, run_id=run_id)
        if artifact is None:
            raise ValueError("No compatible completed backtest is available for exit sweep. Run a backtest first.")
        if str(preset_name or "").strip() and str(artifact.preset_name or "") != str(preset_name or ""):
            raise ValueError("The requested preset does not match the selected backtest run.")

        sweep_payload = self._run_exit_sweep_for_result(
            preset_name=artifact.preset_name,
            run_id=artifact.run_id,
            base_result=artifact.result,
            request=request,
            progress_cb=progress_cb,
        )
        with self._run_artifact_lock:
            current = self._run_artifacts.get(artifact.run_id)
            if current is not None:
                current.payload["exit_sweep"] = sweep_payload
        return {
            "result_kind": "exit_sweep",
            "run_id": artifact.run_id,
            "preset_name": artifact.preset_name,
            "base_summary": dict(artifact.payload.get("summary") or {}),
            "base_config": dict(artifact.payload.get("config") or {}),
            "verification_bundle": str(artifact.payload.get("verification_bundle") or ""),
            "exit_sweep": sweep_payload,
        }
