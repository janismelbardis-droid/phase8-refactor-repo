from __future__ import annotations

"""Strategy dependency helpers for selective indicator preparation."""

from dataclasses import dataclass, field
from typing import Any, Dict, Iterable, List, Optional, Sequence, Set

from .constants import *

FAMILY_CORE = "core"  # legacy alias kept for backward compatibility
FAMILY_OHLCV = "ohlcv"
FAMILY_MACD_LINES = "macd_lines"
FAMILY_MACD_STATES = "macd_states"
FAMILY_ADX = "adx"
FAMILY_ATR = "atr"
FAMILY_EMA = "ema"
FAMILY_RSI = "rsi"
FAMILY_STOCH_RSI = "stoch_rsi"
FAMILY_TAKER_BIAS = "taker_bias"
FAMILY_FRAMA = "frama"
FAMILY_VIDYA = "vidya"
FAMILY_RANGE_FILTER = "range_filter"
FAMILY_MARKET_AUG = "market_aug"

ALL_FAMILIES: Set[str] = {
    FAMILY_OHLCV,
    FAMILY_MACD_LINES,
    FAMILY_MACD_STATES,
    FAMILY_ADX,
    FAMILY_ATR,
    FAMILY_EMA,
    FAMILY_RSI,
    FAMILY_STOCH_RSI,
    FAMILY_TAKER_BIAS,
    FAMILY_FRAMA,
    FAMILY_VIDYA,
    FAMILY_RANGE_FILTER,
    FAMILY_MARKET_AUG,
}
DEFAULT_COMPUTE_FAMILIES: Set[str] = set(ALL_FAMILIES.difference({FAMILY_MARKET_AUG}))

OHLCV_FIELDS: Set[str] = {"price", "open", "high", "low", "close", "volume"}
MACD_LINE_FIELDS: Set[str] = {"macd", "signal", "ms_cross", "macd0_cross"}
MACD_STATE_FIELDS: Set[str] = {"ms", "angle", "sig_angle", "ppo", "flip_age"}
ADX_FIELDS: Set[str] = {"adx", "adx_angle", "di_plus", "di_minus", "di_spread", "di_cross"}
ATR_FIELDS: Set[str] = {"atr", "atr_angle"}
EMA_FIELDS: Set[str] = {
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
}
RSI_FIELDS: Set[str] = {
    "rsi",
    "rsi_smooth",
    "rsi_state",
}
STOCH_RSI_FIELDS: Set[str] = {
    "stoch_rsi_k",
    "stoch_rsi_d",
    "stoch_rsi_kd",
}
TAKER_BIAS_FIELDS: Set[str] = {
    "taker_buy_volume",
    "taker_sell_volume",
    "taker_delta_volume",
    "taker_delta_pct",
    "taker_buy_share_pct",
    "taker_trade_count",
    "taker_bias_state",
}
FRAMA_FIELDS: Set[str] = {
    "frama", "frama_upper", "frama_lower", "frama_alpha",
    "frama_state", "frama_break_up", "frama_break_down", "frama_mid_cross",
}
VIDYA_FIELDS: Set[str] = {
    "vidya_line", "vidya_raw", "vidya_upper", "vidya_lower", "vidya_state",
    "vidya_trend_up", "vidya_trend_down", "vidya_up_volume", "vidya_down_volume",
    "vidya_delta_pct", "vidya_active_liq_low_count", "vidya_active_liq_high_count",
    "vidya_nearest_liq_low", "vidya_nearest_liq_high", "vidya_dist_to_liq_low_pct",
    "vidya_dist_to_liq_high_pct", "vidya_new_liq_low", "vidya_new_liq_high",
    "vidya_liq_low_taken", "vidya_liq_high_taken",
}
RANGE_FILTER_FIELDS: Set[str] = {
    "range_filter_line", "range_filter_upper", "range_filter_lower", "range_filter_smooth_range",
    "range_filter_state", "range_filter_phase", "range_filter_buy", "range_filter_sell",
    "range_filter_long_cond", "range_filter_short_cond", "range_filter_cond_ini",
    "range_filter_upward_count", "range_filter_downward_count",
}
MARKET_AUG_FIELDS: Set[str] = {
    "vidya_slope", "vidya_angle_deg", "vidya_angle_deg_norm", "vidya_angle_state", "vidya_angle_accel",
    "market_state", "market_bias", "market_phase", "market_confidence", "market_state_age",
    "market_direction_score", "market_energy_score", "market_alignment_bonus", "market_macd_gap_norm",
    "market_trend_state", "market_structure_state", "market_phase_state", "market_breakout_signal",
    "market_volume_context", "market_trend_age", "market_phase_age", "market_breakout_signal_age",
    "market_protected_high", "market_protected_low", "market_context_note", "market_context_reasons",
}

DEFAULT_FIELDS: Set[str] = set()

LEGACY_FAMILY_ALIASES: Dict[str, Set[str]] = {
    FAMILY_CORE: {FAMILY_OHLCV, FAMILY_MACD_LINES, FAMILY_MACD_STATES},
}

FAMILY_DEPENDENCIES: Dict[str, Set[str]] = {
    FAMILY_MACD_STATES: {FAMILY_MACD_LINES},
    FAMILY_EMA: {FAMILY_OHLCV},
    FAMILY_TAKER_BIAS: {FAMILY_OHLCV},
    FAMILY_MARKET_AUG: {
        FAMILY_OHLCV,
        FAMILY_MACD_LINES,
        FAMILY_ATR,
        FAMILY_FRAMA,
        FAMILY_VIDYA,
        FAMILY_RANGE_FILTER,
    },
}

FAMILY_TO_FIELDS: Dict[str, Set[str]] = {
    FAMILY_OHLCV: set(OHLCV_FIELDS),
    FAMILY_MACD_LINES: set(MACD_LINE_FIELDS),
    FAMILY_MACD_STATES: set(MACD_STATE_FIELDS),
    FAMILY_ADX: set(ADX_FIELDS),
    FAMILY_ATR: set(ATR_FIELDS),
    FAMILY_EMA: set(EMA_FIELDS),
    FAMILY_RSI: set(RSI_FIELDS),
    FAMILY_STOCH_RSI: set(STOCH_RSI_FIELDS),
    FAMILY_TAKER_BIAS: set(TAKER_BIAS_FIELDS),
    FAMILY_FRAMA: set(FRAMA_FIELDS),
    FAMILY_VIDYA: set(VIDYA_FIELDS),
    FAMILY_RANGE_FILTER: set(RANGE_FILTER_FIELDS),
    FAMILY_MARKET_AUG: set(MARKET_AUG_FIELDS),
    FAMILY_CORE: set(OHLCV_FIELDS | MACD_LINE_FIELDS | MACD_STATE_FIELDS),
}

FIELD_TO_FAMILY: Dict[str, str] = {}
for _fam, _fields in FAMILY_TO_FIELDS.items():
    if _fam == FAMILY_CORE:
        continue
    for _fld in _fields:
        FIELD_TO_FAMILY[_fld] = _fam


def _expand_families(families: Optional[Sequence[str]]) -> Set[str]:
    expanded: Set[str] = set()
    pending = [str(item or "").strip() for item in list(families or [])]
    while pending:
        fam = pending.pop()
        if not fam:
            continue
        if fam in LEGACY_FAMILY_ALIASES:
            pending.extend(sorted(LEGACY_FAMILY_ALIASES[fam]))
            continue
        if fam not in ALL_FAMILIES or fam in expanded:
            continue
        expanded.add(fam)
        pending.extend(sorted(FAMILY_DEPENDENCIES.get(fam, set())))
    return expanded


def _families_for_field_names(field_names: Iterable[str]) -> Optional[Set[str]]:
    fams: Set[str] = set()
    for raw in field_names:
        fld = str(raw or "").strip()
        if not fld:
            continue
        if fld in ALL_FAMILIES or fld in LEGACY_FAMILY_ALIASES:
            fams.update(_expand_families([fld]))
            continue
        fam = FIELD_TO_FAMILY.get(fld)
        if fam is not None:
            fams.update(_expand_families([fam]))
            continue
        if fld.startswith("market_"):
            fams.update(_expand_families([FAMILY_MARKET_AUG]))
            continue
        return None
    return fams


def canonicalize_family_selection(raw: Any) -> List[str]:
    if raw is None:
        return sorted(ALL_FAMILIES)
    if isinstance(raw, str):
        items = [part.strip() for part in raw.split(",")]
    elif isinstance(raw, (list, tuple, set, frozenset)):
        items = [str(part).strip() for part in raw]
    else:
        items = []
    fams = _families_for_field_names(items)
    if fams is None or not fams:
        return sorted(ALL_FAMILIES)
    return sorted(fams)


@dataclass
class StreamRequirementSpec:
    required_fields: Set[str] = field(default_factory=set)
    required_families: Set[str] = field(default_factory=set)
    requires_full_stream: bool = False
    unknown_fields: Set[str] = field(default_factory=set)

    def normalized_families(self, *, include_defaults: bool = True) -> Set[str]:
        if self.requires_full_stream:
            return set(ALL_FAMILIES)

        fams = _families_for_field_names(self.required_fields) or set()
        fams.update(_expand_families(sorted(self.required_families or set())))

        if include_defaults and DEFAULT_FIELDS:
            default_fams = _families_for_field_names(DEFAULT_FIELDS)
            if default_fams:
                fams.update(default_fams)

        return fams

    def normalized_fields(self, *, include_defaults: bool = True) -> Set[str]:
        out = set(self.required_fields or set())
        if include_defaults:
            out.update(DEFAULT_FIELDS)
        for fam in self.normalized_families(include_defaults=include_defaults):
            out.update(FAMILY_TO_FIELDS.get(fam, set()))
        if self.requires_full_stream:
            for fam in ALL_FAMILIES:
                out.update(FAMILY_TO_FIELDS.get(fam, set()))
        return out


def _iter_rules(rules_model: Optional[Dict[str, Any]]) -> Iterable[Any]:
    if not isinstance(rules_model, dict):
        return []
    out: List[Any] = []
    for groups in rules_model.values():
        if not isinstance(groups, list):
            continue
        for group in groups:
            if not isinstance(group, list):
                continue
            out.extend(group)
    return out


def _register_field(spec: StreamRequirementSpec, field_name: Any) -> None:
    fld = str(field_name or "").strip()
    if not fld:
        return
    spec.required_fields.add(fld)
    fam = FIELD_TO_FAMILY.get(fld)
    if fam is not None:
        spec.required_families.update(_expand_families([fam]))
        return
    if fld.startswith("market_"):
        spec.required_families.update(_expand_families([FAMILY_MARKET_AUG]))
        return
    spec.unknown_fields.add(fld)
    spec.requires_full_stream = True


def _register_compare_to_fields(spec: StreamRequirementSpec, rule: Any) -> None:
    try:
        raw = getattr(rule, "compare_to", None)
    except Exception:
        raw = None
    if raw is None:
        return
    if isinstance(raw, dict):
        field_name = raw.get("field") or raw.get("name")
    else:
        text = str(raw or "").strip()
        field_name = text
    text = str(field_name or "").strip()
    if text.endswith("]") and "[" in text:
        field_name = text.rpartition("[")[0].strip()
    _register_field(spec, field_name)


def compile_stream_requirements(
    rules_model: Optional[Dict[str, Any]] = None,
    *,
    entry_filters: Optional[Dict[str, Any]] = None,
    backtest_cfg: Optional[Any] = None,
    include_plot_defaults: bool = True,
) -> StreamRequirementSpec:
    spec = StreamRequirementSpec()
    if include_plot_defaults and DEFAULT_FIELDS:
        spec.required_fields.update(DEFAULT_FIELDS)
        default_fams = _families_for_field_names(DEFAULT_FIELDS)
        if default_fams:
            spec.required_families.update(default_fams)

    for rule in _iter_rules(rules_model):
        try:
            _register_field(spec, getattr(rule, "field", None))
            _register_compare_to_fields(spec, rule)
        except Exception:
            spec.requires_full_stream = True

    if isinstance(entry_filters, dict):
        def _iter_filter_payloads(payload: Any) -> Iterable[Any]:
            if payload is None:
                return
            yield payload
            if isinstance(payload, dict):
                group_payloads = payload.get("groups", {})
                if isinstance(group_payloads, dict):
                    for group_cfg in group_payloads.values():
                        yield group_cfg

        for raw_cfg in entry_filters.values():
            for cfg in _iter_filter_payloads(raw_cfg):
                try:
                    enabled = bool(getattr(cfg, "enabled", False))
                    if isinstance(cfg, dict):
                        enabled = bool(cfg.get("enabled", enabled))
                    mode = str(
                        (cfg.get("mode", "OFF") if isinstance(cfg, dict) else getattr(cfg, "mode", "OFF"))
                        or "OFF"
                    ).upper().strip()
                    hard_skip = (
                        cfg.get("hard_skip_atr_mult", 0.0)
                        if isinstance(cfg, dict)
                        else getattr(cfg, "hard_skip_atr_mult", 0.0)
                    )
                    hard_skip = float(hard_skip if hard_skip not in (None, "") else 0.0)
                except Exception:
                    enabled = False
                    mode = "OFF"
                    hard_skip = 0.0
                if enabled and (mode in ("ANTI_CHASE", "RETRACE_ON_EXPANSION") or hard_skip > 0.0):
                    spec.required_families.update(_expand_families([FAMILY_ATR]))
                    spec.required_fields.add("atr")
                if enabled and mode == "CASEBOOK_PULLBACK_V1":
                    spec.required_families.update(_expand_families([FAMILY_OHLCV, FAMILY_VIDYA, FAMILY_RANGE_FILTER]))
                    spec.required_fields.update({
                        "open",
                        "high",
                        "low",
                        "close",
                        "vidya_line",
                        "vidya_state",
                        "range_filter_state",
                        "range_filter_phase",
                    })

    cfg = backtest_cfg
    if cfg is not None:
        try:
            stop_mode = str(getattr(cfg, "stop_loss_mode", "PERCENT") or "PERCENT").upper().strip()
        except Exception:
            stop_mode = "PERCENT"
        try:
            take_mode = str(getattr(cfg, "take_profit_mode", "PERCENT") or "PERCENT").upper().strip()
        except Exception:
            take_mode = "PERCENT"
        if stop_mode == "ATR":
            spec.required_families.update(_expand_families([FAMILY_ATR]))
            spec.required_fields.add("atr")
        if take_mode == "ATR":
            spec.required_families.update(_expand_families([FAMILY_ATR]))
            spec.required_fields.add("atr")
        if stop_mode == "FIELD":
            _register_field(spec, getattr(cfg, "stop_loss_field", ""))
        if take_mode == "FIELD":
            _register_field(spec, getattr(cfg, "take_profit_field", ""))

    return spec


def normalize_required_families(required_fields: Optional[Any] = None, *, include_defaults: bool = True) -> Set[str]:
    if required_fields is None:
        fams = set(DEFAULT_COMPUTE_FAMILIES)
        if include_defaults and DEFAULT_FIELDS:
            default_fams = _families_for_field_names(DEFAULT_FIELDS)
            if default_fams:
                fams.update(default_fams)
        return fams
    if isinstance(required_fields, StreamRequirementSpec):
        return required_fields.normalized_families(include_defaults=include_defaults)
    if isinstance(required_fields, (set, frozenset, list, tuple)):
        fams = _families_for_field_names([str(v).strip() for v in required_fields if str(v).strip()])
        if fams is None:
            return set(ALL_FAMILIES)
        if include_defaults and DEFAULT_FIELDS:
            default_fams = _families_for_field_names(DEFAULT_FIELDS)
            if default_fams:
                fams.update(default_fams)
        return fams
    return set(ALL_FAMILIES)


def normalize_required_fields(required_fields: Optional[Any] = None, *, include_defaults: bool = True) -> Set[str]:
    if required_fields is None:
        out: Set[str] = set()
        for fam in DEFAULT_COMPUTE_FAMILIES:
            out.update(FAMILY_TO_FIELDS.get(fam, set()))
        if include_defaults:
            out.update(DEFAULT_FIELDS)
        return out
    if isinstance(required_fields, StreamRequirementSpec):
        return required_fields.normalized_fields(include_defaults=include_defaults)
    if isinstance(required_fields, (set, frozenset, list, tuple)):
        out = {str(v).strip() for v in required_fields if str(v).strip()}
        if include_defaults:
            out.update(DEFAULT_FIELDS)
        fams = normalize_required_families(required_fields, include_defaults=include_defaults)
        for fam in fams:
            out.update(FAMILY_TO_FIELDS.get(fam, set()))
        return out
    return normalize_required_fields(None, include_defaults=include_defaults)


def feature_signature(required_fields: Optional[Any] = None, *, include_defaults: bool = True) -> str:
    fams = sorted(normalize_required_families(required_fields, include_defaults=include_defaults))
    return ",".join(fams)
