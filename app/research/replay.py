from __future__ import annotations

"""Replay helpers for deterministic market-state and regression checks."""

from dataclasses import asdict, dataclass, field
from typing import Any, Callable, Dict, Iterable, List, Mapping, Optional, Sequence

import pandas as pd

from ..backtest import BacktestConfig, BacktestResult, run_backtest_replay
from ..domain.market_state import coerce_mapping
from ..rules import entry_filter_payload_to_dict

STATE_SIGNATURE_KEYS = (
    "market_state",
    "market_bias",
    "market_phase",
    "market_confidence",
    "market_texture",
    "market_summary_short",
    "market_breakout_state",
    "market_breakout_bias",
    "market_breakout_score",
    "market_breakout_trigger_ready",
    "market_state_age",
    "market_state_status",
)


@dataclass(frozen=True)
class ReplayMismatch:
    index: int
    key: str
    expected: Any
    actual: Any


REPLAYABLE_EXIT_CONFIG_FIELDS: tuple[str, ...] = (
    "stop_loss_pct",
    "take_profit_pct",
    "break_even_after_mfe_pct",
    "trailing_stop_pct",
    "stop_loss_mode",
    "stop_loss_value",
    "stop_loss_timeframe",
    "stop_loss_field",
    "stop_loss_offset_pct",
    "take_profit_mode",
    "take_profit_value",
    "take_profit_timeframe",
    "take_profit_field",
    "take_profit_offset_pct",
)

REPLAYABLE_COST_CONFIG_FIELDS: tuple[str, ...] = (
    "initial_balance",
    "order_notional_usdt",
    "fee_rate",
    "slippage_bps",
)


@dataclass(frozen=True)
class BacktestReplayPlan:
    eligible: bool
    reason: str
    config_updates: Dict[str, Any] = field(default_factory=dict)


def _normalize_entry_filters_payload(entry_filters: Optional[Mapping[str, Any]]) -> Dict[str, Dict[str, Any]]:
    return entry_filter_payload_to_dict(entry_filters)


def _config_dict(cfg: BacktestConfig | Mapping[str, Any]) -> Dict[str, Any]:
    if isinstance(cfg, BacktestConfig):
        return asdict(cfg)
    if isinstance(cfg, Mapping):
        return dict(cfg)
    raise TypeError("cfg must be BacktestConfig or mapping")


def _diff_backtest_config(base_cfg: BacktestConfig | Mapping[str, Any],
                          next_cfg: BacktestConfig | Mapping[str, Any]) -> Dict[str, Any]:
    base = _config_dict(base_cfg)
    nxt = _config_dict(next_cfg)
    changed: Dict[str, Any] = {}
    for key, next_value in nxt.items():
        if base.get(key) != next_value:
            changed[key] = next_value
    return changed


def build_backtest_replay_plan(
    result: BacktestResult,
    *,
    next_cfg: BacktestConfig | Mapping[str, Any],
    symbol: Optional[str] = None,
    start: Any = None,
    end: Any = None,
    rules_model: Optional[Mapping[str, Any]] = None,
    tab_group_join_mode: Optional[Mapping[str, Any]] = None,
    group_rule_join_mode: Optional[Mapping[str, Any]] = None,
    entry_filters: Optional[Mapping[str, Any]] = None,
) -> BacktestReplayPlan:
    replay_context = extract_backtest_replay_context(result)
    if not replay_context:
        return BacktestReplayPlan(False, "missing-replay-context")

    if symbol is not None and str(symbol or "") != str(replay_context.get("symbol") or ""):
        return BacktestReplayPlan(False, "symbol-changed")

    if start is not None:
        req_start = pd.to_datetime(start, utc=True, errors="coerce")
        ctx_start = pd.to_datetime(replay_context.get("start"), utc=True, errors="coerce")
        if pd.isna(req_start) or pd.isna(ctx_start) or req_start != ctx_start:
            return BacktestReplayPlan(False, "start-changed")

    if end is not None:
        req_end = pd.to_datetime(end, utc=True, errors="coerce")
        ctx_end = pd.to_datetime(replay_context.get("end"), utc=True, errors="coerce")
        if pd.isna(req_end) or pd.isna(ctx_end) or req_end != ctx_end:
            return BacktestReplayPlan(False, "end-changed")

    if rules_model is not None and dict(rules_model or {}) != dict(replay_context.get("rules_model") or {}):
        return BacktestReplayPlan(False, "rules-changed")

    if tab_group_join_mode is not None and dict(tab_group_join_mode or {}) != dict(replay_context.get("tab_group_join_mode") or {}):
        return BacktestReplayPlan(False, "tab-join-mode-changed")

    if group_rule_join_mode is not None and dict(group_rule_join_mode or {}) != dict(replay_context.get("group_rule_join_mode") or {}):
        return BacktestReplayPlan(False, "group-join-mode-changed")

    if entry_filters is not None:
        current_filters = _normalize_entry_filters_payload(entry_filters)
        replay_filters = _normalize_entry_filters_payload(replay_context.get("entry_filters") or {})
        if current_filters != replay_filters:
            return BacktestReplayPlan(False, "entry-filters-changed")

    changed = _diff_backtest_config(result.config, next_cfg)
    replayable_fields = set(REPLAYABLE_EXIT_CONFIG_FIELDS) | set(REPLAYABLE_COST_CONFIG_FIELDS)
    disallowed = sorted(key for key in changed.keys() if key not in replayable_fields)
    if disallowed:
        return BacktestReplayPlan(False, f"non-replay-config:{','.join(disallowed)}")

    updates = {key: changed[key] for key in replayable_fields if key in changed}
    if updates:
        has_exit = any(key in REPLAYABLE_EXIT_CONFIG_FIELDS for key in updates)
        has_cost = any(key in REPLAYABLE_COST_CONFIG_FIELDS for key in updates)
        if has_exit and has_cost:
            reason = "exit-config+cost-config"
        elif has_exit:
            reason = "exit-config-only"
        elif has_cost:
            reason = "cost-config-only"
        else:
            reason = "replayable-config"
        return BacktestReplayPlan(True, reason, updates)
    return BacktestReplayPlan(True, "no-config-change", {})



def extract_backtest_replay_context(result: Any) -> Optional[Dict[str, Any]]:
    if isinstance(result, BacktestResult):
        ctx = result.replay_context
    elif isinstance(result, Mapping):
        ctx = result.get("replay_context")
    else:
        ctx = None
    return coerce_mapping(ctx) or None


def replay_backtest_result(
    result: BacktestResult,
    *,
    cfg: Optional[BacktestConfig] = None,
    config_updates: Optional[Mapping[str, Any]] = None,
    progress_cb: Optional[Callable[[str, int], None]] = None,
    capture_trade_details: Optional[bool] = None,
    equity_curve_stride: Optional[int] = None,
) -> BacktestResult:
    replay_context = extract_backtest_replay_context(result)
    if not replay_context:
        raise ValueError("BacktestResult does not contain replay_context. Run a fresh backtest first.")

    next_cfg = BacktestConfig(**asdict(cfg if cfg is not None else result.config))
    if config_updates:
        for key, value in dict(config_updates).items():
            if not hasattr(next_cfg, key):
                raise AttributeError(f"Unknown BacktestConfig field: {key}")
            setattr(next_cfg, key, value)

    if str(replay_context.get("__replay_engine") or "") == "compiled_bar":
        from ..fast_backtest import replay_backtest_compiled_bar

        return replay_backtest_compiled_bar(
            replay_context,
            cfg=next_cfg,
            capture_trade_details=capture_trade_details,
            equity_curve_stride=equity_curve_stride,
        )

    return run_backtest_replay(
        replay_context,
        next_cfg,
        progress_cb=progress_cb,
        capture_trade_details=capture_trade_details,
        equity_curve_stride=equity_curve_stride,
    )

def extract_state_signature(state: Mapping[str, Any], *, keys: Sequence[str] = STATE_SIGNATURE_KEYS) -> Dict[str, Any]:
    raw = coerce_mapping(state)
    return {key: raw.get(key) for key in keys}


def replay_market_state_sequence(
    rows: Iterable[Mapping[str, Any]],
    *,
    compute_fn: Callable[[Mapping[str, Any], Mapping[str, Any] | None], Mapping[str, Any]],
    initial_prev_state: Mapping[str, Any] | None = None,
) -> List[Dict[str, Any]]:
    prev_state = coerce_mapping(initial_prev_state)
    outputs: List[Dict[str, Any]] = []
    for row in rows:
        current = coerce_mapping(compute_fn(coerce_mapping(row), prev_state or None))
        outputs.append(current)
        prev_state = current
    return outputs


def compare_state_signatures(
    actual_states: Sequence[Mapping[str, Any]],
    expected_signatures: Sequence[Mapping[str, Any]],
    *,
    keys: Sequence[str] = STATE_SIGNATURE_KEYS,
) -> List[ReplayMismatch]:
    mismatches: List[ReplayMismatch] = []
    if len(actual_states) != len(expected_signatures):
        mismatches.append(
            ReplayMismatch(index=-1, key="__length__", expected=len(expected_signatures), actual=len(actual_states))
        )
        return mismatches
    for index, (actual, expected) in enumerate(zip(actual_states, expected_signatures)):
        actual_sig = extract_state_signature(actual, keys=keys)
        expected_sig = {key: coerce_mapping(expected).get(key) for key in keys}
        for key in keys:
            if actual_sig.get(key) != expected_sig.get(key):
                mismatches.append(
                    ReplayMismatch(index=index, key=key, expected=expected_sig.get(key), actual=actual_sig.get(key))
                )
    return mismatches


def replay_matches_expected(
    rows: Sequence[Mapping[str, Any]],
    expected_signatures: Sequence[Mapping[str, Any]],
    *,
    compute_fn: Callable[[Mapping[str, Any], Mapping[str, Any] | None], Mapping[str, Any]],
    keys: Sequence[str] = STATE_SIGNATURE_KEYS,
    initial_prev_state: Mapping[str, Any] | None = None,
) -> tuple[bool, List[ReplayMismatch], List[Dict[str, Any]]]:
    actual_states = replay_market_state_sequence(rows, compute_fn=compute_fn, initial_prev_state=initial_prev_state)
    mismatches = compare_state_signatures(actual_states, expected_signatures, keys=keys)
    return (len(mismatches) == 0, mismatches, actual_states)
