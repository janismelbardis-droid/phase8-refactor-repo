from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, List, Mapping, Optional, Sequence, Tuple

from .backtest_execution import _normalize_exit_mode
from .backtest_models import BacktestConfig
from .backtest_planning import _sequence_compile_allowed_for_tab, _tab_uses_sequence, compiled_rule_support
from .rules import EntryFilterConfig, ENTRY_FILTER_TABS, normalize_entry_filter_payload
from .strategy_requirements import StreamRequirementSpec, compile_stream_requirements

FAST_BAR_EXIT_MODES = frozenset({"OFF", "PERCENT", "ABSOLUTE"})
FAST_BAR_ENGINE = "compiled_bar"
FALLBACK_ENGINE = "legacy_generic"


@dataclass(frozen=True)
class RuleCompileInfo:
    tab_name: str
    group_index: int
    rule_index: int
    label: str
    timeframe: str
    mode: str
    field: str
    op: str
    supported: bool
    reason: str


@dataclass(frozen=True)
class StrategyCompilationPlan:
    engine: str
    fallback_engine: str
    required_spec: StreamRequirementSpec
    step_timeframe: str
    stop_loss_mode: str
    take_profit_mode: str
    active_tabs: Tuple[str, ...]
    compiled_tabs: Tuple[str, ...]
    sequence_tabs: Tuple[str, ...]
    entry_filter_tabs: Tuple[str, ...]
    rule_support: Tuple[RuleCompileInfo, ...]
    blockers: Tuple[str, ...] = field(default_factory=tuple)
    notes: Tuple[str, ...] = field(default_factory=tuple)

    @property
    def unsupported_rules(self) -> Tuple[RuleCompileInfo, ...]:
        return tuple(item for item in self.rule_support if not item.supported)

    @property
    def uses_sequence(self) -> bool:
        return bool(self.sequence_tabs)

    @property
    def uses_entry_filters(self) -> bool:
        return bool(self.entry_filter_tabs)

    @property
    def supports_fast_bar(self) -> bool:
        return self.engine == FAST_BAR_ENGINE


def _rule_label(rule: Any) -> str:
    try:
        if hasattr(rule, "label"):
            return str(rule.label())
    except Exception:
        pass
    try:
        return f"{getattr(rule, 'timeframe', '')} {getattr(rule, 'field', '')} {getattr(rule, 'op', '')}".strip()
    except Exception:
        return "rule"


def _iter_rule_infos(
    rules_model: Optional[Mapping[str, Sequence[Sequence[Any]]]],
) -> List[RuleCompileInfo]:
    out: List[RuleCompileInfo] = []
    for tab_name, groups in dict(rules_model or {}).items():
        if not isinstance(groups, Sequence):
            continue
        for group_index, group in enumerate(groups, start=1):
            if not isinstance(group, Sequence):
                continue
            for rule_index, rule in enumerate(group, start=1):
                supported, reason = compiled_rule_support(rule)
                out.append(
                    RuleCompileInfo(
                        tab_name=str(tab_name),
                        group_index=int(group_index),
                        rule_index=int(rule_index),
                        label=_rule_label(rule),
                        timeframe=str(getattr(rule, "timeframe", "") or ""),
                        mode=str(getattr(rule, "mode", "") or ""),
                        field=str(getattr(rule, "field", "") or ""),
                        op=str(getattr(rule, "op", "") or ""),
                        supported=bool(supported),
                        reason=str(reason or ""),
                    )
                )
    return out


def _active_entry_filter_tabs(entry_filters: Optional[Mapping[str, Any]]) -> Tuple[str, ...]:
    active: List[str] = []
    normalized = normalize_entry_filter_payload(entry_filters)
    for tab_name in ENTRY_FILTER_TABS:
        payload = normalized.get(tab_name, {})
        default_cfg = payload.get("default", EntryFilterConfig())
        groups = payload.get("groups", {})
        if isinstance(default_cfg, EntryFilterConfig) and default_cfg.is_active():
            active.append(tab_name)
            continue
        if isinstance(groups, Mapping) and any(
            isinstance(cfg, EntryFilterConfig) and cfg.is_active()
            for cfg in groups.values()
        ):
            active.append(tab_name)
    return tuple(active)


def _compiled_safe_entry_filter_tabs(
    entry_filters: Optional[Mapping[str, Any]],
    cfg: Optional[BacktestConfig],
) -> Tuple[str, ...]:
    normalized = normalize_entry_filter_payload(entry_filters)

    safe_tabs: List[str] = []
    for tab_name in ENTRY_FILTER_TABS:
        payload = normalized.get(tab_name, {})
        default_cfg = payload.get("default", EntryFilterConfig()) if isinstance(payload, dict) else EntryFilterConfig()
        groups = payload.get("groups", {}) if isinstance(payload, dict) else {}
        active_group_cfgs = [
            group_cfg
            for group_cfg in (groups.values() if isinstance(groups, Mapping) else [])
            if isinstance(group_cfg, EntryFilterConfig) and group_cfg.is_active()
        ]
        if active_group_cfgs:
            continue
        if not isinstance(default_cfg, EntryFilterConfig) or not default_cfg.is_active():
            continue
        safe_tabs.append(str(tab_name))

    return tuple(safe_tabs)


def compile_strategy_plan(
    rules_model: Optional[Mapping[str, Sequence[Sequence[Any]]]],
    *,
    tab_group_join_mode: Optional[Mapping[str, str]] = None,
    group_rule_join_mode: Optional[Mapping[str, Sequence[str]]] = None,
    entry_filters: Optional[Mapping[str, Any]] = None,
    backtest_cfg: Optional[BacktestConfig] = None,
    include_plot_defaults: bool = False,
) -> StrategyCompilationPlan:
    cfg = backtest_cfg or BacktestConfig()
    required_spec = compile_stream_requirements(
        dict(rules_model or {}),
        entry_filters=dict(entry_filters or {}),
        backtest_cfg=cfg,
        include_plot_defaults=bool(include_plot_defaults),
    )
    rule_support = tuple(_iter_rule_infos(rules_model))
    active_tabs = tuple(
        tab_name
        for tab_name, groups in dict(rules_model or {}).items()
        if isinstance(groups, Sequence) and len(groups) > 0
    )
    compiled_tabs = tuple(
        tab_name
        for tab_name in active_tabs
        if all(item.supported for item in rule_support if item.tab_name == tab_name)
    )
    sequence_tabs = tuple(
        tab_name
        for tab_name in active_tabs
        if _tab_uses_sequence(dict(group_rule_join_mode or {}), tab_name)
    )
    entry_filter_tabs = _active_entry_filter_tabs(entry_filters)
    compiled_entry_filter_tabs = _compiled_safe_entry_filter_tabs(entry_filters, cfg)
    if bool(getattr(cfg, "allow_reverse", False)):
        compiled_entry_filter_tabs = tuple(
            tab_name
            for tab_name in compiled_entry_filter_tabs
            if not (
                (tab_name == "Long Entry" and "Short Entry" in active_tabs)
                or (tab_name == "Short Entry" and "Long Entry" in active_tabs)
            )
        )
    compiled_sequence_tabs = tuple(
        tab_name
        for tab_name in sequence_tabs
        if _sequence_compile_allowed_for_tab(tab_name, dict(group_rule_join_mode or {}))
        and (tab_name not in entry_filter_tabs or tab_name in compiled_entry_filter_tabs)
    )
    compiled_tabs = tuple(
        tab_name
        for tab_name in active_tabs
        if all(item.supported for item in rule_support if item.tab_name == tab_name)
        and (tab_name not in sequence_tabs or tab_name in compiled_sequence_tabs)
    )
    stop_mode = _normalize_exit_mode(getattr(cfg, "stop_loss_mode", "OFF"), is_take_profit=False)
    take_mode = _normalize_exit_mode(getattr(cfg, "take_profit_mode", "OFF"), is_take_profit=True)
    step_timeframe = str(getattr(cfg, "step_timeframe", "1m") or "1m").strip() or "1m"

    blockers: List[str] = []
    notes: List[str] = []

    unsupported = [item for item in rule_support if not item.supported]
    if unsupported:
        blockers.append("unsupported_rules")
        notes.append(f"{len(unsupported)} rule(s) require generic evaluation.")
    unsupported_sequence_tabs = tuple(tab_name for tab_name in sequence_tabs if tab_name not in compiled_sequence_tabs)
    if unsupported_sequence_tabs:
        blockers.append("sequence_groups")
        if any(tab_name in entry_filter_tabs for tab_name in unsupported_sequence_tabs):
            notes.append("Sequence groups with active entry filters stay on the generic engine.")
        else:
            notes.append("Sequence groups need stateful generic evaluation.")
    unsupported_entry_filter_tabs = tuple(
        tab_name
        for tab_name in entry_filter_tabs
        if tab_name in active_tabs and tab_name not in compiled_entry_filter_tabs
    )
    if unsupported_entry_filter_tabs:
        blockers.append("entry_filters")
        notes.append("Active entry filters require the generic engine unless they match the compiled-safe default-only subset.")
    if stop_mode not in FAST_BAR_EXIT_MODES:
        blockers.append(f"stop_mode:{stop_mode}")
        notes.append(f"Stop-loss mode '{stop_mode}' is not yet supported by the compiled bar engine.")
    if take_mode not in FAST_BAR_EXIT_MODES:
        blockers.append(f"take_mode:{take_mode}")
        notes.append(f"Take-profit mode '{take_mode}' is not yet supported by the compiled bar engine.")

    engine = FAST_BAR_ENGINE if not blockers else FALLBACK_ENGINE
    if engine == FAST_BAR_ENGINE:
        if compiled_entry_filter_tabs:
            notes.append("Default-only active entry filters are handled by the compiled bar full-scan path.")
        notes.append("Rules can be compiled into aligned boolean arrays and executed by the lightweight bar engine.")

    return StrategyCompilationPlan(
        engine=engine,
        fallback_engine=FALLBACK_ENGINE,
        required_spec=required_spec,
        step_timeframe=step_timeframe,
        stop_loss_mode=stop_mode,
        take_profit_mode=take_mode,
        active_tabs=active_tabs,
        compiled_tabs=compiled_tabs,
        sequence_tabs=sequence_tabs,
        entry_filter_tabs=entry_filter_tabs,
        rule_support=rule_support,
        blockers=tuple(blockers),
        notes=tuple(notes),
    )
