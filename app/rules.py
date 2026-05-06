from __future__ import annotations

"""Strategy rule model and evaluation helpers."""

from dataclasses import dataclass, field
from typing import Any, Dict, List, Mapping, Optional, Tuple


from collections import deque

import numpy as np
import pandas as pd

from .constants import *
from .taker_bias import compute_taker_bias_payload
from .utils_time import interval_to_ms


_SEQUENCE_PARTITION_CACHE: Dict[Tuple[int, ...], Tuple[Tuple["Rule", ...], Tuple["Rule", ...], Tuple["Rule", ...]]] = {}
_TRIGGER_FILTER_PARTITION_CACHE: Dict[Tuple[int, ...], Tuple[Tuple["Rule", ...], Tuple["Rule", ...]]] = {}


def _rules_identity_key(rules: List["Rule"]) -> Tuple[int, ...]:
    try:
        return tuple(int(id(rule)) for rule in list(rules or []))
    except Exception:
        return tuple()


@dataclass
class EntryFilterConfig:
    """Execution-side entry filter configuration.

    The filter is evaluated *after* the rule engine has already produced an entry
    candidate for the current decision bar. It must never reinterpret rule
    offsets such as Bars Ago.

    The filter runs *after* a group/sequence has already turned TRUE on a
    specific trigger bar. It never rewrites Bars Ago logic; it only decides
    whether we enter immediately, wait for a price retrace, or block the entry.
    """

    enabled: bool = False
    mode: str = "OFF"            # OFF | ANTI_CHASE | RETRACE_ON_EXPANSION | RETRACE_ALWAYS
    expansion_atr_mult: float = 2.0
    hard_skip_atr_mult: float = 3.0
    confirm_bars: int = 3
    reference_range: str = "FULL_CANDLE"   # FULL_CANDLE | BODY_ONLY
    touch_type: str = "WICK"               # WICK | CLOSE
    min_retrace_pct: float = 50.0
    max_retrace_pct: float = 100.0
    require_next_close_beyond_mid: bool = True
    require_setup_still_valid: bool = True
    apply_to_reversals: bool = True

    @staticmethod
    def _normalize_mode_value(mode: Any) -> str:
        try:
            raw = str(mode or "OFF").upper().strip()
        except Exception:
            raw = "OFF"
        alias_map = {
            "RETRACE": "RETRACE_ALWAYS",
            "PULLBACK": "RETRACE_ALWAYS",
            "PRICE_PULLBACK": "RETRACE_ALWAYS",
            "REQUIRE_PULLBACK": "RETRACE_ALWAYS",
        }
        raw = alias_map.get(raw, raw)
        return raw if raw in ("OFF", "ANTI_CHASE", "RETRACE_ON_EXPANSION", "RETRACE_ALWAYS") else "OFF"

    @staticmethod
    def _normalize_reference_range_value(value: Any) -> str:
        try:
            raw = str(value or "FULL_CANDLE").upper().strip()
        except Exception:
            raw = "FULL_CANDLE"
        return raw if raw in ("FULL_CANDLE", "BODY_ONLY") else "FULL_CANDLE"

    @staticmethod
    def _normalize_touch_type_value(value: Any) -> str:
        try:
            raw = str(value or "WICK").upper().strip()
        except Exception:
            raw = "WICK"
        return raw if raw in ("WICK", "CLOSE") else "WICK"

    def normalized_mode(self) -> str:
        return self._normalize_mode_value(self.mode)

    def normalized_reference_range(self) -> str:
        return self._normalize_reference_range_value(self.reference_range)

    def normalized_touch_type(self) -> str:
        return self._normalize_touch_type_value(self.touch_type)

    def is_active(self) -> bool:
        return bool(self.enabled) and self.normalized_mode() != "OFF"

    def requires_atr(self) -> bool:
        cfg = self.normalized_copy()
        if not cfg.is_active():
            return False
        mode = cfg.normalized_mode()
        if mode in ("ANTI_CHASE", "RETRACE_ON_EXPANSION"):
            return True
        return float(cfg.hard_skip_atr_mult) > 0.0

    def normalized_copy(self) -> "EntryFilterConfig":
        cfg = EntryFilterConfig(**self.to_dict())
        cfg.mode = self.normalized_mode()
        cfg.reference_range = self.normalized_reference_range()
        cfg.touch_type = self.normalized_touch_type()
        try:
            cfg.expansion_atr_mult = max(0.1, float(cfg.expansion_atr_mult))
        except Exception:
            cfg.expansion_atr_mult = 2.0
        try:
            cfg.hard_skip_atr_mult = float(cfg.hard_skip_atr_mult)
        except Exception:
            cfg.hard_skip_atr_mult = 3.0
        if cfg.hard_skip_atr_mult <= 0.0:
            cfg.hard_skip_atr_mult = 0.0
        elif cfg.normalized_mode() in ("ANTI_CHASE", "RETRACE_ON_EXPANSION"):
            cfg.hard_skip_atr_mult = max(float(cfg.expansion_atr_mult), float(cfg.hard_skip_atr_mult))
        try:
            cfg.confirm_bars = max(1, int(cfg.confirm_bars))
        except Exception:
            cfg.confirm_bars = 3
        try:
            cfg.min_retrace_pct = min(100.0, max(0.0, float(cfg.min_retrace_pct)))
        except Exception:
            cfg.min_retrace_pct = 50.0
        try:
            cfg.max_retrace_pct = min(100.0, max(0.0, float(cfg.max_retrace_pct)))
        except Exception:
            cfg.max_retrace_pct = 100.0
        if float(cfg.max_retrace_pct) < float(cfg.min_retrace_pct):
            cfg.max_retrace_pct = float(cfg.min_retrace_pct)
        cfg.require_next_close_beyond_mid = bool(cfg.require_next_close_beyond_mid)
        cfg.require_setup_still_valid = bool(cfg.require_setup_still_valid)
        cfg.apply_to_reversals = bool(cfg.apply_to_reversals)
        cfg.enabled = bool(cfg.enabled)
        return cfg

    def to_dict(self) -> Dict[str, Any]:
        return {
            "enabled": bool(self.enabled),
            "mode": self.normalized_mode(),
            "expansion_atr_mult": float(self.expansion_atr_mult),
            "hard_skip_atr_mult": float(self.hard_skip_atr_mult),
            "confirm_bars": int(self.confirm_bars),
            "reference_range": self.normalized_reference_range(),
            "touch_type": self.normalized_touch_type(),
            "min_retrace_pct": float(self.min_retrace_pct),
            "max_retrace_pct": float(self.max_retrace_pct),
            "require_next_close_beyond_mid": bool(self.require_next_close_beyond_mid),
            "require_setup_still_valid": bool(self.require_setup_still_valid),
            "apply_to_reversals": bool(self.apply_to_reversals),
        }

    @classmethod
    def from_dict(cls, d: Optional[Dict[str, Any]]) -> "EntryFilterConfig":
        if not isinstance(d, dict):
            return cls()
        def _raw(name: str, default: Any) -> Any:
            value = d.get(name, default)
            return default if value in (None, "") else value
        cfg = cls(
            enabled=bool(d.get("enabled", False)),
            mode=str(d.get("mode", "OFF") or "OFF"),
            expansion_atr_mult=float(_raw("expansion_atr_mult", 2.0)),
            hard_skip_atr_mult=float(_raw("hard_skip_atr_mult", 3.0)),
            confirm_bars=int(_raw("confirm_bars", 3)),
            reference_range=str(_raw("reference_range", "FULL_CANDLE")),
            touch_type=str(_raw("touch_type", "WICK")),
            min_retrace_pct=float(_raw("min_retrace_pct", 50.0)),
            max_retrace_pct=float(_raw("max_retrace_pct", 100.0)),
            require_next_close_beyond_mid=bool(d.get("require_next_close_beyond_mid", True)),
            require_setup_still_valid=bool(d.get("require_setup_still_valid", True)),
            apply_to_reversals=bool(d.get("apply_to_reversals", True)),
        )
        return cfg.normalized_copy()

    def summary(self) -> str:
        cfg = self.normalized_copy()
        if not cfg.is_active():
            return "Off"
        if cfg.normalized_mode() == "ANTI_CHASE":
            parts = [
                "Anti-chase",
                f"{cfg.expansion_atr_mult:.2f} ATR",
                f"wait {cfg.confirm_bars} bar" + ("s" if int(cfg.confirm_bars) != 1 else ""),
                f"retrace ≤ {cfg.max_retrace_pct:.0f}%",
            ]
            if cfg.require_next_close_beyond_mid:
                parts.append("mid-close")
            return " · ".join(parts)
        if cfg.normalized_mode() in ("RETRACE_ALWAYS", "RETRACE_ON_EXPANSION"):
            parts = [
                "Retrace gate" if cfg.normalized_mode() == "RETRACE_ALWAYS" else "Expansion retrace",
                "full candle" if cfg.normalized_reference_range() == "FULL_CANDLE" else "body only",
                cfg.normalized_touch_type().lower(),
                f"{cfg.min_retrace_pct:.0f}-{cfg.max_retrace_pct:.0f}%",
                f"wait {cfg.confirm_bars} bar" + ("s" if int(cfg.confirm_bars) != 1 else ""),
            ]
            if cfg.requires_atr() and cfg.normalized_mode() == "RETRACE_ON_EXPANSION":
                parts.insert(1, f"{cfg.expansion_atr_mult:.2f} ATR")
            return " · ".join(parts)
        return cfg.normalized_mode().replace("_", " ").title()

    def badge_text(self) -> str:
        return f"ENTRY FILTER: {self.summary()}"


ENTRY_FILTER_TABS: Tuple[str, str] = ("Long Entry", "Short Entry")


def normalize_entry_filter_payload(entry_filters: Optional[Mapping[str, Any]]) -> Dict[str, Dict[str, Any]]:
    """Normalize legacy and per-group entry filter payloads.

    Output shape:
      {
        "Long Entry": {
          "default": EntryFilterConfig(...),
          "groups": {"0": EntryFilterConfig(...), ...},
        },
        "Short Entry": {...},
      }

    Legacy payloads that only stored a tab-level config continue to work; their
    values become the tab default and group overrides stay empty.
    """

    normalized: Dict[str, Dict[str, Any]] = {}
    source = entry_filters if isinstance(entry_filters, Mapping) else {}
    for tab_name in ENTRY_FILTER_TABS:
        raw_tab = source.get(tab_name)
        if isinstance(raw_tab, EntryFilterConfig):
            default_cfg = raw_tab.normalized_copy()
            raw_groups = None
        elif isinstance(raw_tab, Mapping):
            default_cfg = EntryFilterConfig.from_dict(dict(raw_tab))
            raw_groups = raw_tab.get("groups")
        else:
            default_cfg = EntryFilterConfig()
            raw_groups = None

        groups: Dict[str, EntryFilterConfig] = {}
        if isinstance(raw_groups, Mapping):
            for raw_group_key, raw_group_cfg in raw_groups.items():
                group_key = str(raw_group_key or "").strip()
                if not group_key:
                    continue
                if isinstance(raw_group_cfg, EntryFilterConfig):
                    groups[group_key] = raw_group_cfg.normalized_copy()
                elif isinstance(raw_group_cfg, Mapping):
                    groups[group_key] = EntryFilterConfig.from_dict(dict(raw_group_cfg))

        normalized[tab_name] = {
            "default": default_cfg,
            "groups": groups,
        }
    return normalized


def entry_filter_payload_to_dict(entry_filters: Optional[Mapping[str, Any]]) -> Dict[str, Dict[str, Any]]:
    normalized = normalize_entry_filter_payload(entry_filters)
    out: Dict[str, Dict[str, Any]] = {}
    for tab_name in ENTRY_FILTER_TABS:
        payload = normalized.get(tab_name, {})
        default_cfg = payload.get("default", EntryFilterConfig())
        groups = payload.get("groups", {})
        out[tab_name] = default_cfg.to_dict() if isinstance(default_cfg, EntryFilterConfig) else EntryFilterConfig().to_dict()
        if isinstance(groups, Mapping):
            out[tab_name]["groups"] = {
                str(group_key): (
                    cfg.normalized_copy().to_dict()
                    if isinstance(cfg, EntryFilterConfig)
                    else EntryFilterConfig.from_dict(dict(cfg or {})).to_dict()
                )
                for group_key, cfg in groups.items()
                if str(group_key or "").strip()
            }
        else:
            out[tab_name]["groups"] = {}
    return out


def iter_entry_filter_configs(entry_filters: Optional[Mapping[str, Any]]) -> List[EntryFilterConfig]:
    normalized = normalize_entry_filter_payload(entry_filters)
    out: List[EntryFilterConfig] = []
    for tab_name in ENTRY_FILTER_TABS:
        payload = normalized.get(tab_name, {})
        default_cfg = payload.get("default")
        if isinstance(default_cfg, EntryFilterConfig):
            out.append(default_cfg)
        groups = payload.get("groups", {})
        if isinstance(groups, Mapping):
            for cfg in groups.values():
                if isinstance(cfg, EntryFilterConfig):
                    out.append(cfg)
                elif isinstance(cfg, Mapping):
                    out.append(EntryFilterConfig.from_dict(dict(cfg)))
    return out

@dataclass
class Rule:
    timeframe: str               # "1m" / "5m" / ...
    mode: str                    # "state" or "event"
    field: str                   # "ms" | "angle" | "sig_angle" | "ppo" | "flip_age" | "macd" | "signal" | "ms_cross"
    op: str                      # state: = != < <= > >= ; event: TURNED / FLIPPED / CROSS
    value: Any                   # state: GREEN/RED or number ; event: GREEN/RED or UP/DOWN or None
    is_trigger: bool = False     # if True (EVENT rules only), this rule acts as a "trigger"; others in the group become filters.

    # Optional: for EVENT rules, allow referencing when the event happened.
    # bars_ago_mode: OFF (default), EXACT, WITHIN
    # - EXACT N: event happened exactly N steps ago (N=0 means "this step")
    # - WITHIN N: event happened within last N steps (includes 0..N)
    bars_ago_mode: str = "OFF"
    bars_ago_n: int = 0

    # If True and bars-ago mode is enabled, also require the condition to still be valid *now*.
    # For STATE rules: condition must also pass on the current step.
    # For EVENT rules: a post-event validity check is applied (e.g. CROSS UP requires macd>signal now).
    require_still_valid: bool = False

    # If True, this rule acts as a final-bar gate filter inside a SEQUENCE group.
    # It does not advance sequence progress; it is checked only when the final
    # sequence step completes. Outside SEQUENCE groups this flag is ignored.
    is_final_gate: bool = False

    # Optional: evaluate selected stateful families over the last N confirmed
    # closed bars instead of a single snapshot.
    window_bars: int = 1

    # If True, this rule acts as a SEQUENCE canceler.
    # While the sequence is already in progress, a TRUE result here resets the
    # whole sequence. Outside SEQUENCE groups this flag is ignored.
    is_sequence_canceler: bool = False

    # Optional right-hand field target for STATE rules, e.g.
    # {"field": "high", "bars_ago": 1}. When present, it replaces ``value``.
    compare_to: Optional[Any] = None

    @property
    def kind(self) -> str:
        # Backward-compat alias: older code paths may refer to ".kind"
        return self.mode


    def label(self) -> str:
        prefix = "★ " if (self.is_trigger and self.mode == "event") else ""
        gate_suffix = " [FINAL GATE]" if bool(getattr(self, "is_final_gate", False)) else ""
        cancel_suffix = " [SEQ CANCEL]" if bool(getattr(self, "is_sequence_canceler", False)) else ""
        suffix = ""
        window_suffix = ""
        try:
            m = str(getattr(self, "bars_ago_mode", "OFF") or "OFF").upper().strip()
            n_bars = int(getattr(self, "bars_ago_n", 0) or 0)
            need_now = bool(getattr(self, "require_still_valid", False))
            mark = "✓" if need_now and m not in ("OFF", "") else ""
            if m in ("EXACT", "=="):
                suffix = f" [={n_bars}{mark}]"
            elif m in ("WITHIN", "<=", "LE"):
                suffix = f" [≤{n_bars}{mark}]"
        except Exception:
            suffix = ""
        try:
            if str(getattr(self, "field", "") or "") in _TAKER_BIAS_WINDOW_FIELDS:
                window_bars = max(1, int(getattr(self, "window_bars", 1) or 1))
                if window_bars > 1:
                    window_suffix = f" [W{window_bars}]"
        except Exception:
            window_suffix = ""

        fld = {
            "ema_1": "EMA Fast",
            "ema_2": "EMA Slow",
            "ema_stack": "EMA Stack",
            "ema_cross_up": "EMA Cross Up",
            "ema_cross_down": "EMA Cross Down",
            "ema_1_slope": "EMA Fast Slope",
            "ema_2_slope": "EMA Slow Slope",
            "ema_1_angle_state": "EMA Fast Angle",
            "ema_2_angle_state": "EMA Slow Angle",
            "ema_spread": "EMA Spread",
            "ema_spread_pct": "EMA Spread %",
            "ema_spread_delta": "EMA Spread Δ",
            "ema_spread_pct_delta": "EMA Spread Δ%",
            "ema_spread_trend": "EMA Spread Trend",
            "ema_stack_pressure": "EMA Pressure",
            "ms": "M/S",
            "angle": "ANGLE",
            "sig_angle": "SIG∠",
            "ppo": "PPO",
            "flip_age": "FlipAge",
            "macd": "MACD",
            "signal": "SIGNAL",
            "rsi": "RSI",
            "rsi_smooth": "RSI Smooth",
            "rsi_state": "RSI State",
            "stoch_rsi_k": "Stoch K",
            "stoch_rsi_d": "Stoch D",
            "stoch_rsi_kd": "Stoch K/D",
            "ms_cross": "MACD×SIG",
            "macd0_cross": "MACD×0",
            "adx": "ADX",
            "atr": "ATR",
            "di_plus": "+DI",
            "di_minus": "-DI",
            "di_spread": "DIΔ",
            "adx_angle": "ADX∠",
            "atr_angle": "ATR∠",
            "di_cross": "DI×",
            "frama_state": "FRAMA",
            "frama_break_up": "FRAMA↑",
            "frama_break_down": "FRAMA↓",
            "frama_mid_cross": "FRAMA↔",
            "vidya_state": "VIDYA",
            "vidya_line": "VIDYA",
            "vidya_raw": "VIDYAraw",
            "vidya_upper": "VIDYA↑Band",
            "vidya_lower": "VIDYA↓Band",
            "vidya_up_volume": "VIDYA BuyVol",
            "vidya_down_volume": "VIDYA SellVol",
            "vidya_delta_pct": "VIDYA Δ%",
            "vidya_slope": "VIDYA Slope",
            "vidya_angle_deg": "VIDYA ∠",
            "vidya_angle_deg_norm": "VIDYA ∠N",
            "vidya_angle_state": "VIDYA ∠State",
            "vidya_angle_accel": "VIDYA ∠Accel",
            "vidya_active_liq_low_count": "VIDYA LiqL#",
            "vidya_active_liq_high_count": "VIDYA LiqH#",
            "vidya_nearest_liq_low": "VIDYA NearLiqL",
            "vidya_nearest_liq_high": "VIDYA NearLiqH",
            "vidya_dist_to_liq_low_pct": "VIDYA DistL%",
            "vidya_dist_to_liq_high_pct": "VIDYA DistH%",
            "vidya_new_liq_low": "VIDYA NewLiqL",
            "vidya_new_liq_high": "VIDYA NewLiqH",
            "vidya_liq_low_taken": "VIDYA LTaken",
            "vidya_liq_high_taken": "VIDYA HTaken",
            "vidya_trend_up": "VIDYA↑",
            "vidya_trend_down": "VIDYA↓",
            "range_filter_state": "Range",
            "range_filter_phase": "RangePhase",
            "range_filter_line": "Range",
            "range_filter_upper": "Range↑Band",
            "range_filter_lower": "Range↓Band",
            "range_filter_smooth_range": "RangeRng",
            "range_filter_upward_count": "RangeUp#",
            "range_filter_downward_count": "RangeDn#",
            "range_filter_buy": "RangeBuy",
            "range_filter_sell": "RangeSell",
            "market_state": "MktState",
            "market_bias": "MktBias",
            "market_phase": "MktPhase",
            "market_confidence": "MktConf",
            "market_state_age": "MktAge"}.get(self.field, self.field.upper())

        if self.mode == "state":
            rhs_label = _compare_to_label(getattr(self, "compare_to", None))
            rhs = rhs_label if rhs_label is not None else self.value
            return f"{self.timeframe} {fld} {self.op} {rhs}" + suffix + window_suffix + gate_suffix + cancel_suffix

        # event
        if self.field in ("ema_cross_up", "ema_cross_down", "frama_break_up", "frama_break_down", "frama_mid_cross", "vidya_trend_up", "vidya_trend_down", "vidya_new_liq_low", "vidya_new_liq_high", "vidya_liq_low_taken", "vidya_liq_high_taken", "range_filter_buy", "range_filter_sell"):
            return prefix + f"{self.timeframe} {fld} EVENT" + suffix + window_suffix + gate_suffix + cancel_suffix
        if self.op == "CROSS":
            return prefix + f"{self.timeframe} {fld} CROSS {self.value}" + suffix + window_suffix + gate_suffix + cancel_suffix
        if self.op == "FLIPPED":
            return prefix + f"{self.timeframe} {fld} FLIPPED" + suffix + window_suffix + gate_suffix + cancel_suffix
        if self.op == "TURNED":
            return prefix + f"{self.timeframe} {fld} TURNED {self.value}" + suffix + window_suffix + gate_suffix + cancel_suffix
        return prefix + f"{self.timeframe} {fld} EVENT" + suffix + window_suffix + gate_suffix + cancel_suffix


_TAKER_BIAS_WINDOW_FIELDS = {
    "taker_bias_state",
    "taker_trade_count",
    "taker_delta_pct",
    "taker_buy_share_pct",
    "taker_buy_volume",
    "taker_sell_volume",
    "taker_delta_volume",
}


def _rule_window_bars(rule: "Rule") -> int:
    try:
        return max(1, int(getattr(rule, "window_bars", 1) or 1))
    except Exception:
        return 1


def _taker_bias_history_payload(
    ctx: Optional["RuleEvalContext"],
    timeframe: str,
    end_snap: Optional[Dict[str, Any]],
    window_bars: int,
) -> Optional[Dict[str, Any]]:
    if ctx is None:
        return None
    try:
        window_bars = max(1, int(window_bars))
    except Exception:
        window_bars = 1

    tfk = str(timeframe).strip()
    dq = (
        ctx.bar_history_by_tf.get(tfk)
        or ctx.bar_history_by_tf.get(tfk.lower())
        or ctx.bar_history_by_tf.get(tfk.upper())
        or ctx.history_by_tf.get(tfk)
        or ctx.history_by_tf.get(tfk.lower())
        or ctx.history_by_tf.get(tfk.upper())
    )
    history = list(dq or [])
    if not history and isinstance(end_snap, dict) and end_snap:
        history = [dict(end_snap)]

    if not history:
        return None

    end_index = None
    try:
        end_bar_ms = ctx._snap_bar_ms(end_snap) if isinstance(end_snap, dict) else None
    except Exception:
        end_bar_ms = None
    if end_bar_ms is not None:
        for idx in range(len(history) - 1, -1, -1):
            try:
                snap_bar_ms = ctx._snap_bar_ms(history[idx])
            except Exception:
                snap_bar_ms = None
            if snap_bar_ms is not None and int(snap_bar_ms) == int(end_bar_ms):
                end_index = idx
                break
    if end_index is None:
        end_index = len(history) - 1

    start_index = end_index - window_bars + 1
    if start_index < 0:
        return None
    sample = history[start_index : end_index + 1]
    if len(sample) < window_bars:
        return None

    buy_total = 0.0
    total_volume = 0.0
    trade_total = 0
    used = 0
    for snap in sample:
        if not isinstance(snap, dict):
            return None
        try:
            buy_vol = float(snap.get("taker_buy_volume"))
            sell_vol = float(snap.get("taker_sell_volume"))
        except Exception:
            return None
        candle_total = float(buy_vol) + float(sell_vol)
        if candle_total <= 0.0:
            return None
        buy_total += float(buy_vol)
        total_volume += float(candle_total)
        try:
            trade_total += int(float(snap.get("taker_trade_count")))
        except Exception:
            pass
        used += 1

    if used < window_bars:
        return None
    payload = compute_taker_bias_payload(total_volume, buy_total, trade_total if used else None)
    payload["bars_used"] = int(used)
    payload["requested_bars"] = int(window_bars)
    return payload


@dataclass
class RuleEvalContext:
    """Stateful context for evaluating rules across steps.

    - EVENT rules: we track the last step index at which the event fired for each Rule instance.
    - STATE rules (with bars-ago constraints): we keep a rolling history of per-timeframe
      snapshots so the evaluator can check a condition N steps ago (or within a window).

    A "step" is defined by the caller (backtest mode). In the UI we advance the step index
    when the 1m timestamp changes so "bars ago" corresponds to bar progression, not UI refreshes.
    """

    step_index: int = 0
    last_event_step: Dict[int, int] = field(default_factory=dict)
    last_event_meta: Dict[int, Dict[str, Any]] = field(default_factory=dict)


    # Rolling history used by STATE rules with bars-ago constraints.
    max_history: int = 512
    history_steps: deque[int] = field(default_factory=lambda: deque(maxlen=512))
    history_by_tf: Dict[str, deque[Dict[str, Any]]] = field(default_factory=dict)

    # Optional: per-timeframe bar history keyed by a bar identifier (usually bar open time in ms).
    # Live mode provides this ("bar_ms" / "time"), backtest mode may not.
    bar_last_ms_by_tf: Dict[str, int] = field(default_factory=dict)
    bar_history_by_tf: Dict[str, deque[Dict[str, Any]]] = field(default_factory=dict)

    # Stateful progress for SEQUENCE groups. Keys are provided by eval_tab_generic as
    # (tab_name, group_index) tuples so backtest/live/UI share the exact same sequence logic.
    sequence_progress_by_group: Dict[Any, int] = field(default_factory=dict)
    sequence_last_eval_step_by_group: Dict[Any, int] = field(default_factory=dict)
    sequence_last_eval_result_by_group: Dict[Any, Optional[bool]] = field(default_factory=dict)
    sequence_last_progress_before_eval_by_group: Dict[Any, int] = field(default_factory=dict)
    sequence_last_transition_step_by_group: Dict[Any, Optional[int]] = field(default_factory=dict)
    sequence_last_transition_bar_ms_by_group: Dict[Any, Optional[int]] = field(default_factory=dict)
    sequence_start_step_by_group: Dict[Any, Optional[int]] = field(default_factory=dict)
    sequence_start_bar_ms_by_group: Dict[Any, Optional[int]] = field(default_factory=dict)
    sequence_last_reset_meta_by_group: Dict[Any, Dict[str, Any]] = field(default_factory=dict)
    latest_directional_trigger_step_by_side: Dict[str, Optional[int]] = field(default_factory=dict)
    latest_directional_trigger_bar_ms_by_side: Dict[str, Optional[int]] = field(default_factory=dict)

    def clone_for_trace(self) -> "RuleEvalContext":
        """Return an isolated copy for trace-only evaluation.

        Trace generation may re-evaluate EVENT / SEQUENCE rules and mutate context
        bookkeeping. A full deepcopy is correct but expensive; for tracing we only
        need independent mutable containers, while the stored snapshot payloads can
        be shared read-only.
        """
        return RuleEvalContext(
            step_index=int(self.step_index),
            last_event_step=dict(self.last_event_step),
            last_event_meta={
                key: (dict(value) if isinstance(value, dict) else value)
                for key, value in self.last_event_meta.items()
            },
            max_history=int(self.max_history),
            history_steps=deque(self.history_steps, maxlen=self.history_steps.maxlen),
            history_by_tf={
                tf: deque(dq, maxlen=dq.maxlen)
                for tf, dq in self.history_by_tf.items()
            },
            bar_last_ms_by_tf=dict(self.bar_last_ms_by_tf),
            bar_history_by_tf={
                tf: deque(dq, maxlen=dq.maxlen)
                for tf, dq in self.bar_history_by_tf.items()
            },
            sequence_progress_by_group=dict(self.sequence_progress_by_group),
            sequence_last_eval_step_by_group=dict(self.sequence_last_eval_step_by_group),
            sequence_last_eval_result_by_group=dict(self.sequence_last_eval_result_by_group),
            sequence_last_progress_before_eval_by_group=dict(self.sequence_last_progress_before_eval_by_group),
            sequence_last_transition_step_by_group=dict(self.sequence_last_transition_step_by_group),
            sequence_last_transition_bar_ms_by_group=dict(self.sequence_last_transition_bar_ms_by_group),
            sequence_start_step_by_group=dict(self.sequence_start_step_by_group),
            sequence_start_bar_ms_by_group=dict(self.sequence_start_bar_ms_by_group),
            sequence_last_reset_meta_by_group={
                key: (dict(value) if isinstance(value, dict) else {})
                for key, value in self.sequence_last_reset_meta_by_group.items()
            },
            latest_directional_trigger_step_by_side=dict(self.latest_directional_trigger_step_by_side),
            latest_directional_trigger_bar_ms_by_side=dict(self.latest_directional_trigger_bar_ms_by_side),
        )

    def ensure_max_history(self, new_max: int) -> None:
        """Increase history capacity if needed (keeps existing contents)."""
        try:
            new_max = int(new_max)
        except Exception:
            return
        if new_max <= self.max_history:
            return
        if new_max < 32:
            new_max = 32
        old_steps = list(self.history_steps)
        self.history_steps = deque(old_steps, maxlen=new_max)
        for tf, dq in list(self.history_by_tf.items()):
            self.history_by_tf[tf] = deque(list(dq), maxlen=new_max)
        for tf, dq in list(self.bar_history_by_tf.items()):
            self.bar_history_by_tf[tf] = deque(list(dq), maxlen=new_max)
        self.max_history = new_max

    def _snap_bar_ms(self, snap: Dict[str, Any]) -> Optional[int]:
        """Extract a stable bar identifier in milliseconds from a snapshot."""
        if not isinstance(snap, dict) or not snap:
            return None
        bm = snap.get("bar_ms", None)
        if bm is not None:
            try:
                return int(bm)
            except Exception:
                pass
        t = snap.get("time", None)
        # Pandas Timestamp -> ns
        if isinstance(t, pd.Timestamp):
            try:
                return int(t.value // 1_000_000)
            except Exception:
                return None
        # Python datetime
        try:
            if hasattr(t, "timestamp"):
                return int(float(t.timestamp()) * 1000.0)
        except Exception:
            return None
        return None

    def observe_bars(self, cur_by_tf: Dict[str, Dict[str, Any]]) -> None:
        """Record snapshots per-timeframe, advancing only when that TF's bar changes.

        This is used in live mode so STATE/EVENT "Bars Ago" refers to *bars of that rule's TF*,
        not UI refreshes or a 1m base clock.
        """
        if not cur_by_tf:
            return

        for tf, snap in cur_by_tf.items():
            tfk = str(tf).strip()
            if not isinstance(snap, dict):
                continue
            # In live mode we only record CLOSED-bar snapshots into the bar history.
            # Forming snapshots carry is_closed=False and should not affect bars-ago evaluation.
            try:
                if "is_closed" in snap and (not bool(snap.get("is_closed"))):
                    continue
            except Exception:
                pass
            bar_ms = self._snap_bar_ms(snap)
            if bar_ms is None:
                continue

            dq = self.bar_history_by_tf.get(tfk)
            if dq is None:
                dq = deque(maxlen=self.max_history)
                self.bar_history_by_tf[tfk] = dq

            last_ms = self.bar_last_ms_by_tf.get(tfk)
            if last_ms is None or int(bar_ms) != int(last_ms):
                dq.append(snap)
                self.bar_last_ms_by_tf[tfk] = int(bar_ms)
            else:
                # same bar -> overwrite last
                if len(dq) == 0:
                    dq.append(snap)
                else:
                    dq[-1] = snap

    def observe(self, cur_by_tf: Dict[str, Dict[str, Any]]) -> None:
        """Record the current snapshots for this step index.

        Call once per *step* (not per UI refresh). If called multiple times for the same step,
        we overwrite the last recorded snapshots for that step.
        """
        if not cur_by_tf:
            return

        # If snapshots carry a bar identifier, also maintain per-TF bar history.
        try:
            self.observe_bars(cur_by_tf)
        except Exception:
            pass

        step_i = int(self.step_index)
        append_new = (len(self.history_steps) == 0) or (self.history_steps[-1] != step_i)
        if append_new:
            self.history_steps.append(step_i)

        known_tfs = set(self.history_by_tf.keys()) | set(cur_by_tf.keys())
        steps_len = len(self.history_steps)

        for tf in known_tfs:
            tfk = str(tf).strip()
            dq = self.history_by_tf.get(tfk)
            if dq is None:
                dq = deque(maxlen=self.max_history)
                # pad up to previous steps (if any)
                for _ in range(max(0, steps_len - (1 if append_new else 0))):
                    dq.append({})
                self.history_by_tf[tfk] = dq

            # pad if this tf appeared later
            while len(dq) < (steps_len - (1 if append_new else 0)):
                dq.append({})

            snap = cur_by_tf.get(tfk, None)
            if snap is None:
                snap = cur_by_tf.get(tfk.lower(), None) or cur_by_tf.get(tfk.upper(), None)
            snap = snap or {}

            if append_new:
                dq.append(snap)
            else:
                while len(dq) < steps_len:
                    dq.append({})
                dq[-1] = snap

    def snapshot_ago(self, timeframe: str, steps_ago: int) -> Optional[Dict[str, Any]]:
        """Return snapshot for `timeframe` from `steps_ago` steps back (0=current)."""
        try:
            steps_ago = int(steps_ago)
        except Exception:
            return None
        if steps_ago < 0:
            return None
        tfk = str(timeframe).strip()
        dq = self.history_by_tf.get(tfk) or self.history_by_tf.get(tfk.lower()) or self.history_by_tf.get(tfk.upper())
        if dq is None:
            return None
        if len(dq) <= steps_ago:
            return None
        try:
            return dq[-1 - steps_ago]
        except Exception:
            return None

    def snapshot_bar_ago(self, timeframe: str, bars_ago: int) -> Optional[Dict[str, Any]]:
        """Return snapshot for `timeframe` from `bars_ago` bars back (0=current bar for that TF)."""
        try:
            bars_ago = int(bars_ago)
        except Exception:
            return None
        if bars_ago < 0:
            return None
        tfk = str(timeframe).strip()
        dq = self.bar_history_by_tf.get(tfk) or self.bar_history_by_tf.get(tfk.lower()) or self.bar_history_by_tf.get(tfk.upper())
        if dq is None or len(dq) <= bars_ago:
            return None
        try:
            return dq[-1 - bars_ago]
        except Exception:
            return None

    def note_event(self, rule: "Rule", meta: Optional[Dict[str, Any]] = None) -> None:
        k = id(rule)
        self.last_event_step[k] = int(self.step_index)
        if meta is not None:
            try:
                self.last_event_meta[k] = dict(meta)
            except Exception:
                self.last_event_meta[k] = {}

    def bars_ago(self, rule: "Rule") -> Optional[int]:
        """How many bars ago the EVENT last fired.

        Prefer bar-based calculation when we have per-TF bar identifiers, otherwise
        fall back to global step_index (backtest-style).
        """
        k = id(rule)
        # Bar-based (live): compare bar ids
        try:
            meta = self.last_event_meta.get(k) or {}
            fired_bar_ms = meta.get("bar_ms")
            if fired_bar_ms is not None:
                cur_bar_ms = self.bar_last_ms_by_tf.get(str(rule.timeframe).strip())
                if cur_bar_ms is not None:
                    tf_ms = int(interval_to_ms(rule.timeframe))
                    if tf_ms > 0:
                        return int(max(0, (int(cur_bar_ms) - int(fired_bar_ms)) // tf_ms))
        except Exception:
            pass

        # Step-based fallback
        if k not in self.last_event_step:
            return None
        return int(self.step_index) - int(self.last_event_step[k])

    def reset_sequence_group(self, group_key: Any) -> None:
        try:
            self.sequence_progress_by_group.pop(group_key, None)
            self.sequence_last_eval_step_by_group.pop(group_key, None)
            self.sequence_last_eval_result_by_group.pop(group_key, None)
            self.sequence_last_progress_before_eval_by_group.pop(group_key, None)
            self.sequence_last_transition_step_by_group.pop(group_key, None)
            self.sequence_last_transition_bar_ms_by_group.pop(group_key, None)
            self.sequence_start_step_by_group.pop(group_key, None)
            self.sequence_start_bar_ms_by_group.pop(group_key, None)
        except Exception:
            pass

    def reset_all_sequences(self) -> None:
        self.sequence_progress_by_group.clear()
        self.sequence_last_eval_step_by_group.clear()
        self.sequence_last_eval_result_by_group.clear()
        self.sequence_last_progress_before_eval_by_group.clear()
        self.sequence_last_transition_step_by_group.clear()
        self.sequence_last_transition_bar_ms_by_group.clear()
        self.sequence_start_step_by_group.clear()
        self.sequence_start_bar_ms_by_group.clear()
        self.sequence_last_reset_meta_by_group.clear()
        self.latest_directional_trigger_step_by_side.clear()
        self.latest_directional_trigger_bar_ms_by_side.clear()

    def note_sequence_reset(self, group_key: Any, *, reason: str, meta: Optional[Dict[str, Any]] = None) -> None:
        payload: Dict[str, Any] = {"reason": str(reason or "").strip()}
        if isinstance(meta, dict):
            for key, value in meta.items():
                payload[str(key)] = value
        self.sequence_last_reset_meta_by_group[group_key] = payload

    def sequence_snapshot(self, group_key: Any, total_steps: int = 0) -> Dict[str, Any]:
        total = max(0, int(total_steps or 0))
        next_idx = int(self.sequence_progress_by_group.get(group_key, 0) or 0)
        next_idx = max(0, min(total, next_idx))
        completed = max(0, min(total, next_idx))
        return {
            "completed_steps": completed,
            "next_step_index": next_idx,
            "total_steps": total,
            "last_eval_step": self.sequence_last_eval_step_by_group.get(group_key),
            "last_eval_result": self.sequence_last_eval_result_by_group.get(group_key),
            "progress_before_eval": self.sequence_last_progress_before_eval_by_group.get(group_key, completed),
            "last_transition_step": self.sequence_last_transition_step_by_group.get(group_key),
            "last_transition_bar_ms": self.sequence_last_transition_bar_ms_by_group.get(group_key),
            "last_reset_meta": self.sequence_last_reset_meta_by_group.get(group_key),
        }


def _cmp(op: str, a: Any, b: Any) -> bool:
    try:
        if op == "=":
            return a == b
        if op == "!=":
            return a != b
        if op == "<":
            return float(a) < float(b)
        if op == "<=":
            return float(a) <= float(b)
        if op == ">":
            return float(a) > float(b)
        if op == ">=":
            return float(a) >= float(b)
    except Exception:
        return False
    return False

def _is_nan(x: Any) -> bool:
    try:
        return bool(np.isnan(x))
    except Exception:
        return False

def _nan_to_none(x: Any) -> Any:
    return None if x is None or _is_nan(x) else x

def _snapshot_raw_value(row: Any, field: str) -> Any:
    fld = str(field or "").strip()
    if not fld:
        return None
    aliases = [fld]
    if fld == "close":
        aliases.append("price")
    elif fld == "price":
        aliases.append("close")
    for key in aliases:
        try:
            val = row.get(key)
        except Exception:
            val = None
        if val is not None and not _is_nan(val):
            return val
    return None

def _snapshot_numeric_value(row: Any, field: str) -> Any:
    val = _nan_to_none(_snapshot_raw_value(row, field))
    if val is None:
        return None
    try:
        return float(val)
    except Exception:
        return None

def _snapshot_value_from_snapshot(snap: Mapping[str, Any], field: str) -> Any:
    if not isinstance(snap, Mapping):
        return None
    fld = str(field or "").strip()
    aliases = [fld]
    if fld == "close":
        aliases.append("price")
    elif fld == "price":
        aliases.append("close")
    for key in aliases:
        val = snap.get(key)
        if val is not None and not _is_nan(val):
            return val
    return None

def _parse_field_reference(raw: Any) -> Optional[Dict[str, Any]]:
    if raw is None:
        return None
    if isinstance(raw, Mapping):
        field = str(raw.get("field") or raw.get("name") or "").strip()
        if not field:
            return None
        try:
            bars_ago = int(raw.get("bars_ago", raw.get("bars_ago_n", raw.get("offset", 0))) or 0)
        except Exception:
            bars_ago = 0
        if field.endswith("]") and "[" in field and bars_ago == 0:
            base, _, tail = field.rpartition("[")
            try:
                parsed_n = int(tail[:-1])
                field = base.strip()
                bars_ago = max(0, parsed_n)
            except Exception:
                pass
        timeframe = str(raw.get("timeframe") or raw.get("tf") or "").strip()
        return {
            "type": "field",
            "field": field,
            "bars_ago": max(0, bars_ago),
            "timeframe": timeframe,
        }
    text = str(raw or "").strip()
    if not text:
        return None
    field = text
    bars_ago = 0
    if text.endswith("]") and "[" in text:
        base, _, tail = text.rpartition("[")
        try:
            parsed_n = int(tail[:-1])
            field = base.strip()
            bars_ago = max(0, parsed_n)
        except Exception:
            field = text
            bars_ago = 0
    if not field:
        return None
    return {"type": "field", "field": field, "bars_ago": bars_ago, "timeframe": ""}

def _normalize_compare_to(compare_to: Any) -> Optional[Dict[str, Any]]:
    target = _parse_field_reference(compare_to)
    if target is None:
        return None
    if str(target.get("type") or "field").lower().strip() != "field":
        return None
    field = str(target.get("field") or "").strip()
    if not field:
        return None
    return target

def _compare_to_label(compare_to: Any) -> Optional[str]:
    target = _normalize_compare_to(compare_to)
    if target is None:
        return None
    field = str(target.get("field") or "").strip()
    bars_ago = int(target.get("bars_ago", 0) or 0)
    tf = str(target.get("timeframe") or "").strip()
    suffix = f"[{bars_ago}]" if bars_ago > 0 else ""
    return f"{tf + ':' if tf else ''}{field}{suffix}"

_SNAPSHOT_COLOR_FIELDS = {"ms", "angle", "sig_angle", "ppo", "adx_angle", "atr_angle", "rsi_state", "stoch_rsi_kd"}
_SNAPSHOT_BOOL_FIELDS = {
    "ema_cross_up", "ema_cross_down",
    "frama_break_up", "frama_break_down", "frama_mid_cross",
    "vidya_trend_up", "vidya_trend_down", "vidya_new_liq_low", "vidya_new_liq_high",
    "vidya_liq_low_taken", "vidya_liq_high_taken",
    "range_filter_buy", "range_filter_sell", "range_filter_long_cond", "range_filter_short_cond",
}
_SNAPSHOT_FLOAT_FIELDS = {
    "price", "open", "high", "low", "close", "volume",
    "ema_1", "ema_2", "ema_1_slope", "ema_2_slope", "ema_spread", "ema_spread_pct",
    "ema_spread_delta", "ema_spread_pct_delta",
    "macd", "signal", "rsi", "rsi_smooth", "stoch_rsi_k", "stoch_rsi_d", "adx", "di_plus", "di_minus", "di_spread", "atr",
    "vidya_line", "vidya_raw", "vidya_upper", "vidya_lower", "vidya_up_volume", "vidya_down_volume",
    "vidya_delta_pct", "vidya_slope", "vidya_angle_deg", "vidya_angle_deg_norm",
    "vidya_active_liq_low_count", "vidya_active_liq_high_count",
    "vidya_nearest_liq_low", "vidya_nearest_liq_high",
    "vidya_dist_to_liq_low_pct", "vidya_dist_to_liq_high_pct",
    "range_filter_line", "range_filter_upper", "range_filter_lower", "range_filter_smooth_range",
    "range_filter_upward_count", "range_filter_downward_count",
    "taker_buy_volume", "taker_sell_volume", "taker_delta_volume", "taker_delta_pct", "taker_buy_share_pct", "taker_trade_count",
    "market_confidence", "market_state_age", "market_direction_score", "market_energy_score",
    "market_alignment_bonus", "market_macd_gap_norm",
}
_SNAPSHOT_INT_FIELDS = {"flip_age"}
_SNAPSHOT_ENUM_VALUES = {
    "ema_stack": {"BUY", "SELL", "NEUTRAL"},
    "ema_1_angle_state": {"UP", "DOWN", "FLAT"},
    "ema_2_angle_state": {"UP", "DOWN", "FLAT"},
    "ema_spread_trend": {"EXPANDING", "CONTRACTING", "FLAT"},
    "ema_stack_pressure": {"BULL_ACCELERATING", "BULL_DECELERATING", "BEAR_ACCELERATING", "BEAR_DECELERATING", "NEUTRAL"},
    "frama_state": {"BUY", "SELL", "NEUTRAL"},
    "vidya_state": {"BUY", "SELL"},
    "vidya_angle_state": {"UP", "DOWN", "FLAT"},
    "vidya_angle_accel": {"ACCEL", "DECEL", "STABLE"},
    "range_filter_state": {"BUY", "SELL", "NEUTRAL"},
    "range_filter_phase": {"BUY_STRONG", "BUY_WEAK", "SELL_STRONG", "SELL_WEAK", "NEUTRAL"},
    "taker_bias_state": {"BUY", "SELL", "NEUTRAL"},
    "market_state": {"BULL_EXPANSION", "BULL_PULLBACK", "BULL_EXHAUSTION", "BEAR_EXPANSION", "BEAR_PULLBACK", "BEAR_EXHAUSTION", "TRANSITION", "COMPRESSION"},
    "market_bias": {"LONG", "SHORT", "NEUTRAL"},
    "market_phase": {"EXPANSION", "PULLBACK", "EXHAUSTION", "TRANSITION", "COMPRESSION"},
}
_SELECTIVE_SNAPSHOT_SUPPORTED_FIELDS = (
    _SNAPSHOT_COLOR_FIELDS
    | _SNAPSHOT_BOOL_FIELDS
    | _SNAPSHOT_FLOAT_FIELDS
    | _SNAPSHOT_INT_FIELDS
    | set(_SNAPSHOT_ENUM_VALUES.keys())
    | {"range_filter_cond_ini"}
)


def _snapshot_meta_from_stream_row(row: Any) -> Dict[str, Any]:
    snapshot_time = None
    for _time_key in ("snapshot_time", "time", "close_time"):
        try:
            raw_t = row.get(_time_key)
        except Exception:
            raw_t = None
        if raw_t is None:
            continue
        try:
            snapshot_time = pd.to_datetime(raw_t, utc=True, errors="coerce")
            if pd.notna(snapshot_time):
                break
            snapshot_time = None
        except Exception:
            snapshot_time = None
    if snapshot_time is None:
        try:
            if hasattr(row, "name") and row.name is not None:
                snapshot_time = pd.to_datetime(row.name, utc=True, errors="coerce")
                if pd.isna(snapshot_time):
                    snapshot_time = None
        except Exception:
            snapshot_time = None

    snapshot_bar_ms = None
    for _bar_key in ("snapshot_bar_ms", "bar_ms"):
        try:
            raw_bm = row.get(_bar_key)
        except Exception:
            raw_bm = None
        if raw_bm is None:
            continue
        try:
            if not _is_nan(raw_bm):
                snapshot_bar_ms = int(raw_bm)
                break
        except Exception:
            pass
    if snapshot_bar_ms is None:
        try:
            if snapshot_time is not None:
                snapshot_bar_ms = int(pd.Timestamp(snapshot_time).value // 1_000_000)
        except Exception:
            snapshot_bar_ms = None

    try:
        is_closed = bool(row.get("is_closed", True))
    except Exception:
        is_closed = True

    return {
        "snapshot_time": snapshot_time.isoformat() if snapshot_time is not None else None,
        "snapshot_bar_ms": snapshot_bar_ms,
        "bar_ms": snapshot_bar_ms,
        "is_closed": is_closed,
    }


def _snapshot_selective_value(row: Any, field: str) -> Any:
    raw = _snapshot_raw_value(row, field)
    val = _nan_to_none(raw)

    if field in _SNAPSHOT_COLOR_FIELDS:
        return val if val in ("GREEN", "RED") else None
    if field in _SNAPSHOT_ENUM_VALUES:
        return val if val in _SNAPSHOT_ENUM_VALUES[field] else None
    if field in _SNAPSHOT_BOOL_FIELDS:
        try:
            return bool(raw if raw is not None else False)
        except Exception:
            return False
    if field in _SNAPSHOT_INT_FIELDS:
        if val is None:
            return None
        try:
            return int(float(val))
        except Exception:
            return None
    if field in _SNAPSHOT_FLOAT_FIELDS:
        if val is None:
            return None
        try:
            return float(val)
        except Exception:
            return None
    if field == "range_filter_cond_ini":
        return val
    raise KeyError(field)


def _snapshot_from_stream_row_full(row: Any) -> Dict[str, Any]:
    """
    Convert a simulated stream row (Series/dict) into the same "latest" schema:
      macd, signal, ms, angle, sig_angle, ppo, flip_age, adx, di_plus, di_minus, di_spread
    """
    if row is None:
        return {}

    macd = _nan_to_none(row.get("macd"))
    signal = _nan_to_none(row.get("signal"))
    rsi = _nan_to_none(row.get("rsi"))
    rsi_smooth = _nan_to_none(row.get("rsi_smooth"))
    rsi_state = _nan_to_none(row.get("rsi_state"))
    stoch_rsi_k = _nan_to_none(row.get("stoch_rsi_k"))
    stoch_rsi_d = _nan_to_none(row.get("stoch_rsi_d"))
    stoch_rsi_kd = _nan_to_none(row.get("stoch_rsi_kd"))
    ms = _nan_to_none(row.get("ms"))
    angle = _nan_to_none(row.get("angle"))
    sig_angle = _nan_to_none(row.get("sig_angle"))
    ppo = _nan_to_none(row.get("ppo"))
    flip_age = _nan_to_none(row.get("flip_age"))
    ema_1 = _nan_to_none(row.get("ema_1"))
    ema_2 = _nan_to_none(row.get("ema_2"))
    ema_stack = _nan_to_none(row.get("ema_stack"))
    ema_cross_up = bool(row.get("ema_cross_up", False))
    ema_cross_down = bool(row.get("ema_cross_down", False))
    ema_1_slope = _nan_to_none(row.get("ema_1_slope"))
    ema_2_slope = _nan_to_none(row.get("ema_2_slope"))
    ema_1_angle_state = _nan_to_none(row.get("ema_1_angle_state"))
    ema_2_angle_state = _nan_to_none(row.get("ema_2_angle_state"))
    ema_spread = _nan_to_none(row.get("ema_spread"))
    ema_spread_pct = _nan_to_none(row.get("ema_spread_pct"))
    ema_spread_delta = _nan_to_none(row.get("ema_spread_delta"))
    ema_spread_pct_delta = _nan_to_none(row.get("ema_spread_pct_delta"))
    ema_spread_trend = _nan_to_none(row.get("ema_spread_trend"))
    ema_stack_pressure = _nan_to_none(row.get("ema_stack_pressure"))

    adx = _nan_to_none(row.get("adx"))
    adx_angle = _nan_to_none(row.get("adx_angle"))
    di_plus = _nan_to_none(row.get("di_plus"))
    di_minus = _nan_to_none(row.get("di_minus"))
    di_spread = _nan_to_none(row.get("di_spread"))

    atr = _nan_to_none(row.get("atr"))
    atr_angle = _nan_to_none(row.get("atr_angle"))
    frama_state = _nan_to_none(row.get("frama_state"))
    frama_break_up = bool(row.get("frama_break_up", False))
    frama_break_down = bool(row.get("frama_break_down", False))
    frama_mid_cross = bool(row.get("frama_mid_cross", False))
    vidya_line = _nan_to_none(row.get("vidya_line"))
    vidya_raw = _nan_to_none(row.get("vidya_raw"))
    vidya_upper = _nan_to_none(row.get("vidya_upper"))
    vidya_lower = _nan_to_none(row.get("vidya_lower"))
    vidya_state = _nan_to_none(row.get("vidya_state"))
    vidya_trend_up = bool(row.get("vidya_trend_up", False))
    vidya_trend_down = bool(row.get("vidya_trend_down", False))
    vidya_up_volume = _nan_to_none(row.get("vidya_up_volume"))
    vidya_down_volume = _nan_to_none(row.get("vidya_down_volume"))
    vidya_delta_pct = _nan_to_none(row.get("vidya_delta_pct"))
    vidya_slope = _nan_to_none(row.get("vidya_slope"))
    vidya_angle_deg = _nan_to_none(row.get("vidya_angle_deg"))
    vidya_angle_deg_norm = _nan_to_none(row.get("vidya_angle_deg_norm"))
    vidya_angle_state = _nan_to_none(row.get("vidya_angle_state"))
    vidya_angle_accel = _nan_to_none(row.get("vidya_angle_accel"))
    vidya_active_liq_low_count = _nan_to_none(row.get("vidya_active_liq_low_count"))
    vidya_active_liq_high_count = _nan_to_none(row.get("vidya_active_liq_high_count"))
    vidya_nearest_liq_low = _nan_to_none(row.get("vidya_nearest_liq_low"))
    vidya_nearest_liq_high = _nan_to_none(row.get("vidya_nearest_liq_high"))
    vidya_dist_to_liq_low_pct = _nan_to_none(row.get("vidya_dist_to_liq_low_pct"))
    vidya_dist_to_liq_high_pct = _nan_to_none(row.get("vidya_dist_to_liq_high_pct"))
    vidya_new_liq_low = bool(row.get("vidya_new_liq_low", False))
    vidya_new_liq_high = bool(row.get("vidya_new_liq_high", False))
    vidya_liq_low_taken = bool(row.get("vidya_liq_low_taken", False))
    vidya_liq_high_taken = bool(row.get("vidya_liq_high_taken", False))
    range_filter_line = _nan_to_none(row.get("range_filter_line"))
    range_filter_upper = _nan_to_none(row.get("range_filter_upper"))
    range_filter_lower = _nan_to_none(row.get("range_filter_lower"))
    range_filter_smooth_range = _nan_to_none(row.get("range_filter_smooth_range"))
    range_filter_state = _nan_to_none(row.get("range_filter_state"))
    range_filter_phase = _nan_to_none(row.get("range_filter_phase"))
    range_filter_buy = bool(row.get("range_filter_buy", False))
    range_filter_sell = bool(row.get("range_filter_sell", False))
    range_filter_long_cond = bool(row.get("range_filter_long_cond", False))
    range_filter_short_cond = bool(row.get("range_filter_short_cond", False))
    range_filter_cond_ini = _nan_to_none(row.get("range_filter_cond_ini"))
    range_filter_upward_count = _nan_to_none(row.get("range_filter_upward_count"))
    range_filter_downward_count = _nan_to_none(row.get("range_filter_downward_count"))
    taker_buy_volume = _nan_to_none(row.get("taker_buy_volume"))
    taker_sell_volume = _nan_to_none(row.get("taker_sell_volume"))
    taker_delta_volume = _nan_to_none(row.get("taker_delta_volume"))
    taker_delta_pct = _nan_to_none(row.get("taker_delta_pct"))
    taker_buy_share_pct = _nan_to_none(row.get("taker_buy_share_pct"))
    taker_trade_count = _nan_to_none(row.get("taker_trade_count"))
    taker_bias_state = _nan_to_none(row.get("taker_bias_state"))
    market_state = _nan_to_none(row.get("market_state"))
    market_bias = _nan_to_none(row.get("market_bias"))
    market_phase = _nan_to_none(row.get("market_phase"))
    market_confidence = _nan_to_none(row.get("market_confidence"))
    market_state_age = _nan_to_none(row.get("market_state_age"))
    market_direction_score = _nan_to_none(row.get("market_direction_score"))
    market_energy_score = _nan_to_none(row.get("market_energy_score"))
    market_alignment_bonus = _nan_to_none(row.get("market_alignment_bonus"))
    market_macd_gap_norm = _nan_to_none(row.get("market_macd_gap_norm"))

    if flip_age is not None:
        try:
            flip_age = int(float(flip_age))
        except Exception:
            flip_age = None

    # normalize colors
    if ms not in ("GREEN", "RED"):
        ms = None
    if angle not in ("GREEN", "RED"):
        angle = None
    if sig_angle not in ("GREEN", "RED"):
        sig_angle = None
    if ppo not in ("GREEN", "RED"):
        ppo = None
    if rsi_state not in ("GREEN", "RED"):
        rsi_state = None
    if stoch_rsi_kd not in ("GREEN", "RED"):
        stoch_rsi_kd = None
    if adx_angle not in ("GREEN", "RED"):
        adx_angle = None
    if atr_angle not in ("GREEN", "RED"):
        atr_angle = None
    if ema_stack not in ("BUY", "SELL", "NEUTRAL"):
        ema_stack = None
    if ema_1_angle_state not in ("UP", "DOWN", "FLAT"):
        ema_1_angle_state = None
    if ema_2_angle_state not in ("UP", "DOWN", "FLAT"):
        ema_2_angle_state = None
    if ema_spread_trend not in ("EXPANDING", "CONTRACTING", "FLAT"):
        ema_spread_trend = None
    if ema_stack_pressure not in ("BULL_ACCELERATING", "BULL_DECELERATING", "BEAR_ACCELERATING", "BEAR_DECELERATING", "NEUTRAL"):
        ema_stack_pressure = None
    if frama_state not in ("BUY", "SELL", "NEUTRAL"):
        frama_state = None
    if vidya_state not in ("BUY", "SELL"):
        vidya_state = None
    if vidya_angle_state not in ("UP", "DOWN", "FLAT"):
        vidya_angle_state = None
    if vidya_angle_accel not in ("ACCEL", "DECEL", "STABLE"):
        vidya_angle_accel = None
    if range_filter_state not in ("BUY", "SELL", "NEUTRAL"):
        range_filter_state = None
    if range_filter_phase not in ("BUY_STRONG", "BUY_WEAK", "SELL_STRONG", "SELL_WEAK", "NEUTRAL"):
        range_filter_phase = None
    if taker_bias_state not in ("BUY", "SELL", "NEUTRAL"):
        taker_bias_state = None
    if market_state not in ("BULL_EXPANSION", "BULL_PULLBACK", "BULL_EXHAUSTION", "BEAR_EXPANSION", "BEAR_PULLBACK", "BEAR_EXHAUSTION", "TRANSITION", "COMPRESSION"):
        market_state = None
    if market_bias not in ("LONG", "SHORT", "NEUTRAL"):
        market_bias = None
    if market_phase not in ("EXPANSION", "PULLBACK", "EXHAUSTION", "TRANSITION", "COMPRESSION"):
        market_phase = None

    # numeric casts
    if macd is not None:
        try:
            macd = float(macd)
        except Exception:
            macd = None
    if signal is not None:
        try:
            signal = float(signal)
        except Exception:
            signal = None
    if rsi is not None:
        try:
            rsi = float(rsi)
        except Exception:
            rsi = None
    if rsi_smooth is not None:
        try:
            rsi_smooth = float(rsi_smooth)
        except Exception:
            rsi_smooth = None
    if stoch_rsi_k is not None:
        try:
            stoch_rsi_k = float(stoch_rsi_k)
        except Exception:
            stoch_rsi_k = None
    if stoch_rsi_d is not None:
        try:
            stoch_rsi_d = float(stoch_rsi_d)
        except Exception:
            stoch_rsi_d = None
    if adx is not None:
        try:
            adx = float(adx)
        except Exception:
            adx = None
    if atr is not None:
        try:
            atr = float(atr)
        except Exception:
            atr = None
    if ema_1 is not None:
        try:
            ema_1 = float(ema_1)
        except Exception:
            ema_1 = None
    if ema_2 is not None:
        try:
            ema_2 = float(ema_2)
        except Exception:
            ema_2 = None
    if ema_1_slope is not None:
        try:
            ema_1_slope = float(ema_1_slope)
        except Exception:
            ema_1_slope = None
    if ema_2_slope is not None:
        try:
            ema_2_slope = float(ema_2_slope)
        except Exception:
            ema_2_slope = None
    if ema_spread is not None:
        try:
            ema_spread = float(ema_spread)
        except Exception:
            ema_spread = None
    if ema_spread_pct is not None:
        try:
            ema_spread_pct = float(ema_spread_pct)
        except Exception:
            ema_spread_pct = None
    if ema_spread_delta is not None:
        try:
            ema_spread_delta = float(ema_spread_delta)
        except Exception:
            ema_spread_delta = None
    if ema_spread_pct_delta is not None:
        try:
            ema_spread_pct_delta = float(ema_spread_pct_delta)
        except Exception:
            ema_spread_pct_delta = None
    if di_plus is not None:
        try:
            di_plus = float(di_plus)
        except Exception:
            di_plus = None
    if di_minus is not None:
        try:
            di_minus = float(di_minus)
        except Exception:
            di_minus = None
    if di_spread is not None:
        try:
            di_spread = float(di_spread)
        except Exception:
            di_spread = None
    if vidya_line is not None:
        try:
            vidya_line = float(vidya_line)
        except Exception:
            vidya_line = None
    if vidya_raw is not None:
        try:
            vidya_raw = float(vidya_raw)
        except Exception:
            vidya_raw = None
    if vidya_upper is not None:
        try:
            vidya_upper = float(vidya_upper)
        except Exception:
            vidya_upper = None
    if vidya_lower is not None:
        try:
            vidya_lower = float(vidya_lower)
        except Exception:
            vidya_lower = None
    if vidya_up_volume is not None:
        try:
            vidya_up_volume = float(vidya_up_volume)
        except Exception:
            vidya_up_volume = None
    if vidya_down_volume is not None:
        try:
            vidya_down_volume = float(vidya_down_volume)
        except Exception:
            vidya_down_volume = None
    if vidya_delta_pct is not None:
        try:
            vidya_delta_pct = float(vidya_delta_pct)
        except Exception:
            vidya_delta_pct = None
    if vidya_slope is not None:
        try:
            vidya_slope = float(vidya_slope)
        except Exception:
            vidya_slope = None
    if vidya_angle_deg is not None:
        try:
            vidya_angle_deg = float(vidya_angle_deg)
        except Exception:
            vidya_angle_deg = None
    if vidya_angle_deg_norm is not None:
        try:
            vidya_angle_deg_norm = float(vidya_angle_deg_norm)
        except Exception:
            vidya_angle_deg_norm = None
    if vidya_active_liq_low_count is not None:
        try:
            vidya_active_liq_low_count = float(vidya_active_liq_low_count)
        except Exception:
            vidya_active_liq_low_count = None
    if vidya_active_liq_high_count is not None:
        try:
            vidya_active_liq_high_count = float(vidya_active_liq_high_count)
        except Exception:
            vidya_active_liq_high_count = None
    if vidya_nearest_liq_low is not None:
        try:
            vidya_nearest_liq_low = float(vidya_nearest_liq_low)
        except Exception:
            vidya_nearest_liq_low = None
    if vidya_nearest_liq_high is not None:
        try:
            vidya_nearest_liq_high = float(vidya_nearest_liq_high)
        except Exception:
            vidya_nearest_liq_high = None
    if vidya_dist_to_liq_low_pct is not None:
        try:
            vidya_dist_to_liq_low_pct = float(vidya_dist_to_liq_low_pct)
        except Exception:
            vidya_dist_to_liq_low_pct = None
    if vidya_dist_to_liq_high_pct is not None:
        try:
            vidya_dist_to_liq_high_pct = float(vidya_dist_to_liq_high_pct)
        except Exception:
            vidya_dist_to_liq_high_pct = None
    if range_filter_line is not None:
        try:
            range_filter_line = float(range_filter_line)
        except Exception:
            range_filter_line = None
    if range_filter_upper is not None:
        try:
            range_filter_upper = float(range_filter_upper)
        except Exception:
            range_filter_upper = None
    if range_filter_lower is not None:
        try:
            range_filter_lower = float(range_filter_lower)
        except Exception:
            range_filter_lower = None
    if range_filter_smooth_range is not None:
        try:
            range_filter_smooth_range = float(range_filter_smooth_range)
        except Exception:
            range_filter_smooth_range = None
    if range_filter_upward_count is not None:
        try:
            range_filter_upward_count = float(range_filter_upward_count)
        except Exception:
            range_filter_upward_count = None
    if range_filter_downward_count is not None:
        try:
            range_filter_downward_count = float(range_filter_downward_count)
        except Exception:
            range_filter_downward_count = None
    if taker_buy_volume is not None:
        try:
            taker_buy_volume = float(taker_buy_volume)
        except Exception:
            taker_buy_volume = None
    if taker_sell_volume is not None:
        try:
            taker_sell_volume = float(taker_sell_volume)
        except Exception:
            taker_sell_volume = None
    if taker_delta_volume is not None:
        try:
            taker_delta_volume = float(taker_delta_volume)
        except Exception:
            taker_delta_volume = None
    if taker_delta_pct is not None:
        try:
            taker_delta_pct = float(taker_delta_pct)
        except Exception:
            taker_delta_pct = None
    if taker_buy_share_pct is not None:
        try:
            taker_buy_share_pct = float(taker_buy_share_pct)
        except Exception:
            taker_buy_share_pct = None
    if taker_trade_count is not None:
        try:
            taker_trade_count = float(taker_trade_count)
        except Exception:
            taker_trade_count = None
    if market_confidence is not None:
        try:
            market_confidence = float(market_confidence)
        except Exception:
            market_confidence = None
    if market_state_age is not None:
        try:
            market_state_age = float(market_state_age)
        except Exception:
            market_state_age = None
    if market_direction_score is not None:
        try:
            market_direction_score = float(market_direction_score)
        except Exception:
            market_direction_score = None
    if market_energy_score is not None:
        try:
            market_energy_score = float(market_energy_score)
        except Exception:
            market_energy_score = None
    if market_alignment_bonus is not None:
        try:
            market_alignment_bonus = float(market_alignment_bonus)
        except Exception:
            market_alignment_bonus = None
    if market_macd_gap_norm is not None:
        try:
            market_macd_gap_norm = float(market_macd_gap_norm)
        except Exception:
            market_macd_gap_norm = None

    # Prefer explicit snapshot metadata when present. This matters for backtest streams that
    # forward-fill higher-timeframe CLOSED values across 1m rows: the DataFrame row index keeps
    # advancing every minute, but the *source* higher-timeframe bar should remain stable until the
    # next higher-timeframe close. Falling back to row.name is still correct for ordinary streams.
    snapshot_time = None
    for _time_key in ("snapshot_time", "time", "close_time"):
        try:
            raw_t = row.get(_time_key)
        except Exception:
            raw_t = None
        if raw_t is None:
            continue
        try:
            snapshot_time = pd.to_datetime(raw_t, utc=True, errors="coerce")
            if pd.notna(snapshot_time):
                break
            snapshot_time = None
        except Exception:
            snapshot_time = None
    if snapshot_time is None:
        try:
            if hasattr(row, "name") and row.name is not None:
                snapshot_time = pd.to_datetime(row.name, utc=True, errors="coerce")
                if pd.isna(snapshot_time):
                    snapshot_time = None
        except Exception:
            snapshot_time = None

    snapshot_bar_ms = None
    for _bar_key in ("snapshot_bar_ms", "bar_ms"):
        try:
            raw_bm = row.get(_bar_key)
        except Exception:
            raw_bm = None
        if raw_bm is None:
            continue
        try:
            if not _is_nan(raw_bm):
                snapshot_bar_ms = int(raw_bm)
                break
        except Exception:
            pass
    if snapshot_bar_ms is None:
        try:
            if snapshot_time is not None:
                snapshot_bar_ms = int(pd.Timestamp(snapshot_time).value // 1_000_000)
        except Exception:
            snapshot_bar_ms = None

    return {
        "snapshot_time": snapshot_time.isoformat() if snapshot_time is not None else None,
        "snapshot_bar_ms": snapshot_bar_ms,
        "bar_ms": snapshot_bar_ms,
        "is_closed": bool(row.get("is_closed", True)),
        "price": _snapshot_numeric_value(row, "price"),
        "open": _snapshot_numeric_value(row, "open"),
        "high": _snapshot_numeric_value(row, "high"),
        "low": _snapshot_numeric_value(row, "low"),
        "close": _snapshot_numeric_value(row, "close"),
        "volume": _snapshot_numeric_value(row, "volume"),
        "ema_1": ema_1,
        "ema_2": ema_2,
        "ema_stack": ema_stack,
        "ema_cross_up": bool(ema_cross_up),
        "ema_cross_down": bool(ema_cross_down),
        "ema_1_slope": ema_1_slope,
        "ema_2_slope": ema_2_slope,
        "ema_1_angle_state": ema_1_angle_state,
        "ema_2_angle_state": ema_2_angle_state,
        "ema_spread": ema_spread,
        "ema_spread_pct": ema_spread_pct,
        "ema_spread_delta": ema_spread_delta,
        "ema_spread_pct_delta": ema_spread_pct_delta,
        "ema_spread_trend": ema_spread_trend,
        "ema_stack_pressure": ema_stack_pressure,
        "macd": macd,
        "signal": signal,
        "rsi": rsi,
        "rsi_smooth": rsi_smooth,
        "rsi_state": rsi_state,
        "stoch_rsi_k": stoch_rsi_k,
        "stoch_rsi_d": stoch_rsi_d,
        "stoch_rsi_kd": stoch_rsi_kd,
        "ms": ms,
        "angle": angle,
        "sig_angle": sig_angle,
        "ppo": ppo,
        "flip_age": flip_age,
        "adx": adx,
        "di_plus": di_plus,
        "di_minus": di_minus,
        "di_spread": di_spread,
        "adx_angle": adx_angle,
        "atr": atr,
        "atr_angle": atr_angle,
        "frama_state": frama_state,
        "frama_break_up": bool(frama_break_up),
        "frama_break_down": bool(frama_break_down),
        "frama_mid_cross": bool(frama_mid_cross),
        "vidya_line": vidya_line,
        "vidya_raw": vidya_raw,
        "vidya_upper": vidya_upper,
        "vidya_lower": vidya_lower,
        "vidya_state": vidya_state,
        "vidya_trend_up": bool(vidya_trend_up),
        "vidya_trend_down": bool(vidya_trend_down),
        "vidya_up_volume": vidya_up_volume,
        "vidya_down_volume": vidya_down_volume,
        "vidya_delta_pct": vidya_delta_pct,
        "vidya_slope": vidya_slope,
        "vidya_angle_deg": vidya_angle_deg,
        "vidya_angle_deg_norm": vidya_angle_deg_norm,
        "vidya_angle_state": vidya_angle_state,
        "vidya_angle_accel": vidya_angle_accel,
        "vidya_active_liq_low_count": vidya_active_liq_low_count,
        "vidya_active_liq_high_count": vidya_active_liq_high_count,
        "vidya_nearest_liq_low": vidya_nearest_liq_low,
        "vidya_nearest_liq_high": vidya_nearest_liq_high,
        "vidya_dist_to_liq_low_pct": vidya_dist_to_liq_low_pct,
        "vidya_dist_to_liq_high_pct": vidya_dist_to_liq_high_pct,
        "vidya_new_liq_low": bool(vidya_new_liq_low),
        "vidya_new_liq_high": bool(vidya_new_liq_high),
        "vidya_liq_low_taken": bool(vidya_liq_low_taken),
        "vidya_liq_high_taken": bool(vidya_liq_high_taken),
        "range_filter_line": range_filter_line,
        "range_filter_upper": range_filter_upper,
        "range_filter_lower": range_filter_lower,
        "range_filter_smooth_range": range_filter_smooth_range,
        "range_filter_state": range_filter_state,
        "range_filter_phase": range_filter_phase,
        "range_filter_buy": bool(range_filter_buy),
        "range_filter_sell": bool(range_filter_sell),
        "range_filter_long_cond": bool(range_filter_long_cond),
        "range_filter_short_cond": bool(range_filter_short_cond),
        "range_filter_cond_ini": range_filter_cond_ini,
        "range_filter_upward_count": range_filter_upward_count,
        "range_filter_downward_count": range_filter_downward_count,
        "taker_buy_volume": taker_buy_volume,
        "taker_sell_volume": taker_sell_volume,
        "taker_delta_volume": taker_delta_volume,
        "taker_delta_pct": taker_delta_pct,
        "taker_buy_share_pct": taker_buy_share_pct,
        "taker_trade_count": taker_trade_count,
        "taker_bias_state": taker_bias_state,
        "market_state": market_state,
        "market_bias": market_bias,
        "market_phase": market_phase,
        "market_confidence": market_confidence,
        "market_state_age": market_state_age,
        "market_direction_score": market_direction_score,
        "market_energy_score": market_energy_score,
        "market_alignment_bonus": market_alignment_bonus,
        "market_macd_gap_norm": market_macd_gap_norm,
    }


def snapshot_from_stream_row(row: Any, required_fields: Optional[Any] = None) -> Dict[str, Any]:
    """Build a normalized rule-eval snapshot.

    When ``required_fields`` is provided and only references common backtest fields,
    build a slim snapshot to reduce per-row Python object churn in hot loops.
    The function automatically falls back to the legacy full snapshot for any
    unsupported or unknown fields so behavior stays unchanged.
    """
    if row is None:
        return {}
    if required_fields is None:
        return _snapshot_from_stream_row_full(row)

    req = {str(v).strip() for v in required_fields if str(v).strip()}
    if not req:
        return _snapshot_meta_from_stream_row(row)
    if not req.issubset(_SELECTIVE_SNAPSHOT_SUPPORTED_FIELDS):
        return _snapshot_from_stream_row_full(row)

    out = _snapshot_meta_from_stream_row(row)
    for field in sorted(req):
        try:
            out[field] = _snapshot_selective_value(row, field)
        except Exception:
            return _snapshot_from_stream_row_full(row)
    return out


def eval_rule_generic(rule: "Rule",
                      cur_by_tf: Dict[str, Dict[str, Any]],
                      prev_by_tf: Dict[str, Dict[str, Any]],
                      ctx: Optional["RuleEvalContext"] = None) -> Optional[bool]:
    tfk = str(rule.timeframe).strip()
    cur = cur_by_tf.get(tfk, {}) or cur_by_tf.get(tfk.lower(), {}) or cur_by_tf.get(tfk.upper(), {}) or {}
    prev = prev_by_tf.get(tfk, {}) or prev_by_tf.get(tfk.lower(), {}) or prev_by_tf.get(tfk.upper(), {}) or {}

    def _missing(x: Any) -> bool:
        if x is None:
            return True
        try:
            return bool(not np.isfinite(float(x)))
        except Exception:
            return False

    def _cmp(op: str, a: float, b: float) -> bool:
        op = (op or "==").strip()
        if op in ("=", "=="):
            return a == b
        if op == "!=":
            return a != b
        if op == ">":
            return a > b
        if op == ">=":
            return a >= b
        if op == "<":
            return a < b
        if op == "<=":
            return a <= b
        return False

    def _comparison_rhs(lhs_snap: Dict[str, Any], lhs_bars_ago: int = 0) -> Tuple[Any, bool]:
        target = _normalize_compare_to(getattr(rule, "compare_to", None))
        if target is None:
            return getattr(rule, "value", None), False

        field = str(target.get("field") or "").strip()
        if not field:
            return None, True
        target_tf = str(target.get("timeframe") or "").strip() or tfk
        try:
            rhs_bars_ago = max(0, int(target.get("bars_ago", 0) or 0))
        except Exception:
            rhs_bars_ago = 0
        try:
            anchor_bars_ago = max(0, int(lhs_bars_ago or 0)) + rhs_bars_ago
        except Exception:
            anchor_bars_ago = rhs_bars_ago

        target_snap: Optional[Dict[str, Any]]
        if target_tf == tfk and anchor_bars_ago == max(0, int(lhs_bars_ago or 0)):
            target_snap = lhs_snap
        elif ctx is not None:
            target_snap = ctx.snapshot_bar_ago(target_tf, anchor_bars_ago) or ctx.snapshot_ago(target_tf, anchor_bars_ago)
        elif target_tf == tfk and anchor_bars_ago == 0:
            target_snap = cur
        elif target_tf == tfk and anchor_bars_ago == 1:
            target_snap = prev
        else:
            target_snap = None

        if target_snap is None:
            return None, True
        return _snapshot_value_from_snapshot(target_snap, field), True

    
    # ---------------- STATE ----------------
    if rule.mode == "state":
        taker_window_bars = _rule_window_bars(rule)
        def _eval_on(snap: Dict[str, Any], lhs_bars_ago: int = 0) -> Optional[bool]:
            if rule.field in _TAKER_BIAS_WINDOW_FIELDS:
                taker_payload = _taker_bias_history_payload(ctx, tfk, snap, taker_window_bars) if ctx is not None else None
                if taker_payload is None:
                    if ctx is None and taker_window_bars <= 1:
                        taker_payload = dict(snap or {})
                    else:
                        return None
                if rule.field == "taker_bias_state":
                    v = taker_payload.get("taker_bias_state")
                    if _missing(v):
                        return None
                    op = (rule.op or "=").strip()
                    if op in ("=", "=="):
                        return str(v) == str(rule.value)
                    if op == "!=":
                        return str(v) != str(rule.value)
                    return str(v) == str(rule.value)
                v = taker_payload.get(rule.field)
                if _missing(v):
                    return None
                rhs, is_field_compare = _comparison_rhs(snap, lhs_bars_ago)
                if _missing(rhs):
                    return None
                try:
                    return _cmp(rule.op, float(v), float(rhs))
                except Exception:
                    return None

            # Dots / colors
            if rule.field in ("ms", "angle", "sig_angle", "ppo", "adx_angle", "atr_angle", "rsi_state", "stoch_rsi_kd"):
                v = snap.get(rule.field)
                if _missing(v):
                    return None
                rhs, _ = _comparison_rhs(snap, lhs_bars_ago)
                if _missing(rhs):
                    return None
                op = (rule.op or "=").strip()
                if op in ("=", "=="):
                    return str(v) == str(rhs)
                if op == "!=":
                    return str(v) != str(rhs)
                return str(v) == str(rhs)

            if rule.field in ("ema_stack", "ema_1_angle_state", "ema_2_angle_state", "ema_spread_trend", "ema_stack_pressure", "frama_state"):
                v = snap.get(rule.field)
                if _missing(v):
                    return None
                rhs, _ = _comparison_rhs(snap, lhs_bars_ago)
                if _missing(rhs):
                    return None
                op = (rule.op or "=").strip()
                if op in ("=", "=="):
                    return str(v) == str(rhs)
                if op == "!=":
                    return str(v) != str(rhs)
                return str(v) == str(rhs)

            if rule.field == "vidya_state":
                v = snap.get("vidya_state")
                if _missing(v):
                    return None
                rhs, _ = _comparison_rhs(snap, lhs_bars_ago)
                if _missing(rhs):
                    return None
                op = (rule.op or "=").strip()
                if op in ("=", "=="):
                    return str(v) == str(rhs)
                if op == "!=":
                    return str(v) != str(rhs)
                return str(v) == str(rhs)

            if rule.field == "range_filter_state":
                v = snap.get("range_filter_state")
                if _missing(v):
                    return None
                rhs, _ = _comparison_rhs(snap, lhs_bars_ago)
                if _missing(rhs):
                    return None
                op = (rule.op or "=").strip()
                if op in ("=", "=="):
                    return str(v) == str(rhs)
                if op == "!=":
                    return str(v) != str(rhs)
                return str(v) == str(rhs)

            if rule.field == "range_filter_phase":
                v = snap.get("range_filter_phase")
                if _missing(v):
                    return None
                rhs, _ = _comparison_rhs(snap, lhs_bars_ago)
                if _missing(rhs):
                    return None
                op = (rule.op or "=").strip()
                if op in ("=", "=="):
                    return str(v) == str(rhs)
                if op == "!=":
                    return str(v) != str(rhs)
                return str(v) == str(rhs)

            if rule.field in ("vidya_angle_state", "vidya_angle_accel", "market_state", "market_bias", "market_phase"):
                v = snap.get(rule.field)
                if _missing(v):
                    return None
                rhs, _ = _comparison_rhs(snap, lhs_bars_ago)
                if _missing(rhs):
                    return None
                op = (rule.op or "=").strip()
                if op in ("=", "=="):
                    return str(v) == str(rhs)
                if op == "!=":
                    return str(v) != str(rhs)
                return str(v) == str(rhs)

            # FlipAge (integer-ish)
            if rule.field == "flip_age":
                v = snap.get("flip_age")
                if _missing(v):
                    return None
                rhs, is_field_compare = _comparison_rhs(snap, lhs_bars_ago)
                if _missing(rhs):
                    return None
                try:
                    return _cmp(rule.op, float(v), float(rhs))
                except Exception:
                    return None

            # Numeric comparisons
            if rule.field in (_SNAPSHOT_FLOAT_FIELDS | _SNAPSHOT_INT_FIELDS):
                v = _snapshot_value_from_snapshot(snap, rule.field)
                if _missing(v):
                    return None
                rhs, is_field_compare = _comparison_rhs(snap, lhs_bars_ago)
                if _missing(rhs):
                    return None
                try:
                    return _cmp(rule.op, float(v), float(rhs))
                except Exception:
                    return None

            return None

        # Optional: bars-ago constraint for STATE rules.
        try:
            m_mode = str(getattr(rule, "bars_ago_mode", "OFF") or "OFF").upper().strip()
            n_bars = int(getattr(rule, "bars_ago_n", 0) or 0)
        except Exception:
            m_mode, n_bars = "OFF", 0
        try:
            compare_target = _normalize_compare_to(getattr(rule, "compare_to", None))
            compare_bars_ago = int(compare_target.get("bars_ago", 0) or 0) if compare_target is not None else 0
        except Exception:
            compare_bars_ago = 0

        if ctx is not None and rule.field in _TAKER_BIAS_WINDOW_FIELDS:
            try:
                ctx.ensure_max_history(max(ctx.max_history, int(n_bars) + int(compare_bars_ago) + int(taker_window_bars) + 8))
            except Exception:
                pass

        if ctx is not None and (m_mode not in ("OFF", "") or compare_bars_ago > 0):
            # Ensure we keep enough history for this lookback window.
            try:
                extra_window = int(taker_window_bars) if rule.field in _TAKER_BIAS_WINDOW_FIELDS else 1
                ctx.ensure_max_history(max(ctx.max_history, int(n_bars) + int(compare_bars_ago) + int(extra_window) + 8))
            except Exception:
                pass

        if ctx is not None and m_mode in ("OFF", ""):
            return _eval_on(cur, 0)

        if ctx is not None and m_mode not in ("OFF", ""):
            need_now = bool(getattr(rule, "require_still_valid", False))

            def _need_current_and(ok_hist: Optional[bool]) -> Optional[bool]:
                if not need_now:
                    return ok_hist
                if ok_hist is not True:
                    return ok_hist
                ok_now = _eval_on(cur, 0)
                if ok_now is None:
                    return None
                return True if ok_now is True else False

            if m_mode in ("EXACT", "=="):
                if n_bars == 0:
                    return _need_current_and(_eval_on(cur, 0))
                snap = ctx.snapshot_bar_ago(tfk, n_bars) or ctx.snapshot_ago(tfk, n_bars)
                if snap is None:
                    return None
                return _need_current_and(_eval_on(snap, n_bars))

            if m_mode in ("WITHIN", "<=", "LE"):
                any_none = False
                hist_true = False
                # Check current .. N bars ago (inclusive)
                for k in range(0, max(0, n_bars) + 1):
                    snap = cur if k == 0 else (ctx.snapshot_bar_ago(tfk, k) or ctx.snapshot_ago(tfk, k))
                    if snap is None:
                        any_none = True
                        continue
                    ok = _eval_on(snap, k)
                    if ok is True:
                        hist_true = True
                        break
                    if ok is None:
                        any_none = True
                if not hist_true:
                    return None if any_none else False
                return _need_current_and(True)

            # Unknown mode -> fall back to current
            return _need_current_and(_eval_on(cur, 0))


        # Default behavior (no bars-ago)
        return _eval_on(cur, 0)

    def _latest_frama_directional_trigger_side_now() -> Optional[str]:
        direct_side = str(cur.get("frama_trigger_side") or "").strip().upper()
        if direct_side in ("BUY", "SELL"):
            return direct_side
        cur_up = bool(cur.get("frama_break_up", False))
        cur_down = bool(cur.get("frama_break_down", False))
        if cur_up and not cur_down:
            return "BUY"
        if cur_down and not cur_up:
            return "SELL"
        if ctx is None:
            return None
        tf_aliases = (tfk, tfk.lower(), tfk.upper())
        dq = None
        for key in tf_aliases:
            dq = ctx.bar_history_by_tf.get(key)
            if dq is not None:
                break
        if dq is None:
            for key in tf_aliases:
                dq = ctx.history_by_tf.get(key)
                if dq is not None:
                    break
        if dq is None:
            return None
        for snap in reversed(list(dq)):
            if not isinstance(snap, dict):
                continue
            up = bool(snap.get("frama_break_up", False))
            down = bool(snap.get("frama_break_down", False))
            if up and not down:
                return "BUY"
            if down and not up:
                return "SELL"
        return None

    def _still_valid_event() -> Optional[bool]:
        """If rule uses bars-ago, optionally require the post-event state to still be valid now."""
        need_now = bool(getattr(rule, "require_still_valid", False))
        if not need_now:
            return True
        # Dots: require the current dot state matches the post-event state.
        if rule.field in ("ms", "angle", "sig_angle", "ppo", "rsi_state", "stoch_rsi_kd"):
            cv = cur.get(rule.field)
            if _missing(cv):
                return None
            opu = str(rule.op or "").strip().upper()
            if opu in ("TURNED", "CROSS"):
                want = str(rule.value).upper()
                if want not in ("GREEN", "RED"):
                    # try from last event meta
                    meta = (ctx.last_event_meta.get(id(rule)) if ctx is not None else None) or {}
                    want = str(meta.get("post_state", "")).upper()
                if want in ("GREEN", "RED"):
                    return str(cv).upper() == want
                return True
            if opu == "FLIPPED":
                meta = (ctx.last_event_meta.get(id(rule)) if ctx is not None else None) or {}
                want = str(meta.get("post_state", "")).upper()
                if want in ("GREEN", "RED"):
                    return str(cv).upper() == want
                return True
            return True

        # Crosses: require the post-cross relationship still holds now.
        if rule.field == "ms_cross":
            cm = cur.get("macd")
            cs = cur.get("signal")
            if _missing(cm) or _missing(cs):
                return None
            v = str(rule.value).upper()
            if v in ("UP", "GREEN", "BULL", "BULLISH"):
                return float(cm) > float(cs)
            if v in ("DOWN", "RED", "BEAR", "BEARISH"):
                return float(cm) < float(cs)
            return True

        if rule.field == "macd0_cross":
            cm = cur.get("macd")
            if _missing(cm):
                return None
            v = str(rule.value).upper()
            if v == "UP":
                return float(cm) > 0.0
            if v == "DOWN":
                return float(cm) < 0.0
            return True

        if rule.field == "di_cross":
            cp = cur.get("di_plus")
            cm = cur.get("di_minus")
            if _missing(cp) or _missing(cm):
                return None
            v = str(rule.value).upper()
            if v == "UP":
                return float(cp) > float(cm)
            if v == "DOWN":
                return float(cp) < float(cm)
            return True

        if rule.field == "ema_cross_up":
            cv = cur.get("ema_stack")
            if _missing(cv):
                return None
            return str(cv).upper() == "BUY"

        if rule.field == "ema_cross_down":
            cv = cur.get("ema_stack")
            if _missing(cv):
                return None
            return str(cv).upper() == "SELL"

        if rule.field == "frama_break_up":
            cv = _latest_frama_directional_trigger_side_now()
            if _missing(cv):
                return None
            return str(cv).upper() == "BUY"

        if rule.field == "frama_break_down":
            cv = _latest_frama_directional_trigger_side_now()
            if _missing(cv):
                return None
            return str(cv).upper() == "SELL"

        if rule.field == "frama_mid_cross":
            cv = cur.get("frama_state")
            if _missing(cv):
                return None
            return str(cv).upper() == "NEUTRAL"

        if rule.field == "vidya_trend_up":
            cv = cur.get("vidya_state")
            if _missing(cv):
                return None
            return str(cv).upper() == "BUY"

        if rule.field == "vidya_trend_down":
            cv = cur.get("vidya_state")
            if _missing(cv):
                return None
            return str(cv).upper() == "SELL"

        if rule.field == "vidya_new_liq_low":
            cv = cur.get("vidya_active_liq_low_count")
            if _missing(cv):
                return None
            return float(cv) > 0.0

        if rule.field == "vidya_new_liq_high":
            cv = cur.get("vidya_active_liq_high_count")
            if _missing(cv):
                return None
            return float(cv) > 0.0

        if rule.field in ("vidya_liq_low_taken", "vidya_liq_high_taken"):
            return True

        if rule.field == "range_filter_buy":
            cv = cur.get("range_filter_state")
            if _missing(cv):
                return None
            return str(cv).upper() == "BUY"

        if rule.field == "range_filter_sell":
            cv = cur.get("range_filter_state")
            if _missing(cv):
                return None
            return str(cv).upper() == "SELL"

        return True


    # ---------------- EVENT ----------------
    if rule.mode == "event":
        # We first detect whether the event fired *this step* (based on cur vs prev).
        def _fired_now() -> Optional[bool]:
            # Dot TURNED / FLIPPED for M/S, Angle, PPO
            if rule.field in ("ms", "angle", "sig_angle", "ppo", "adx_angle", "atr_angle", "rsi_state", "stoch_rsi_kd"):
                cm = cur.get(rule.field)
                if _missing(cm):
                    return None
                pm = prev.get(rule.field)
                if _missing(pm):
                    return False  # no previous -> not fired

                op = (rule.op or "").strip().upper()
                if op == "FLIPPED":
                    return (str(pm) in ("GREEN", "RED")) and (str(cm) in ("GREEN", "RED")) and (str(pm) != str(cm))
                if op == "TURNED":
                    want = str(rule.value).upper()
                    if want not in ("GREEN", "RED"):
                        return None
                    return (str(pm).upper() != want) and (str(cm).upper() == want)

                # Backward-compat: allow CROSS GREEN/RED as TURNED
                if op == "CROSS":
                    want = str(rule.value).upper()
                    if want in ("GREEN", "RED"):
                        return (str(pm).upper() != want) and (str(cm).upper() == want)

                return None

            if rule.field in ("ema_cross_up", "ema_cross_down", "frama_break_up", "frama_break_down", "frama_mid_cross", "vidya_trend_up", "vidya_trend_down", "vidya_new_liq_low", "vidya_new_liq_high", "vidya_liq_low_taken", "vidya_liq_high_taken"):
                cv = bool(cur.get(rule.field, False))
                pv = bool(prev.get(rule.field, False))
                return bool(cv and (not pv))

            if rule.field in ("range_filter_buy", "range_filter_sell"):
                # Prefer source-bar regime reconstruction over raw copied event flags.
                # The Pine badge is based on current long/short condition plus previous CondIni.
                cur_state = str(cur.get("range_filter_state") or "").upper()
                prev_cond_ini = prev.get("range_filter_cond_ini")
                cur_cond_ini = cur.get("range_filter_cond_ini")
                try:
                    prev_ci_i = int(float(prev_cond_ini)) if prev_cond_ini is not None else None
                except Exception:
                    prev_ci_i = None
                try:
                    cur_ci_i = int(float(cur_cond_ini)) if cur_cond_ini is not None else None
                except Exception:
                    cur_ci_i = None
                if rule.field == "range_filter_buy":
                    if cur_state == "BUY" and prev_ci_i == -1:
                        return True if (cur_ci_i in (None, 1)) else False
                else:
                    if cur_state == "SELL" and prev_ci_i == 1:
                        return True if (cur_ci_i in (None, -1)) else False
                cv = bool(cur.get(rule.field, False))
                pv = bool(prev.get(rule.field, False))
                return bool(cv and (not pv))

            # MACD×Signal cross UP/DOWN (derived from numeric macd vs signal)
            if rule.field == "ms_cross":
                cm = cur.get("macd")
                cs = cur.get("signal")
                if _missing(cm) or _missing(cs):
                    return None
                pm = prev.get("macd")
                ps = prev.get("signal")
                if _missing(pm) or _missing(ps):
                    return False  # no previous -> not fired

                if str(rule.op).upper() != "CROSS":
                    return None

                v = str(rule.value).upper()
                if v in ("UP", "GREEN", "BULL", "BULLISH"):
                    return (float(pm) <= float(ps)) and (float(cm) > float(cs))
                if v in ("DOWN", "RED", "BEAR", "BEARISH"):
                    return (float(pm) >= float(ps)) and (float(cm) < float(cs))
                return None

            # MACD×0 cross UP/DOWN
            if rule.field == "macd0_cross":
                cm = cur.get("macd")
                if _missing(cm):
                    return None
                pm = prev.get("macd")
                if _missing(pm):
                    return False

                if str(rule.op).upper() != "CROSS":
                    return None
                if str(rule.value).upper() == "UP":
                    return (float(pm) <= 0.0) and (float(cm) > 0.0)
                if str(rule.value).upper() == "DOWN":
                    return (float(pm) >= 0.0) and (float(cm) < 0.0)
                return None

            # DI+×DI- cross UP/DOWN
            if rule.field == "di_cross":
                cp = cur.get("di_plus")
                cm = cur.get("di_minus")
                if _missing(cp) or _missing(cm):
                    return None
                pp = prev.get("di_plus")
                pm = prev.get("di_minus")
                if _missing(pp) or _missing(pm):
                    return False

                if str(rule.op).upper() != "CROSS":
                    return None
                if str(rule.value).upper() == "UP":
                    return (float(pp) <= float(pm)) and (float(cp) > float(cm))
                if str(rule.value).upper() == "DOWN":
                    return (float(pp) >= float(pm)) and (float(cp) < float(cm))
                return None

            return None

        fired_now = _fired_now()

        # Remember last-fired step for this rule (if provided).
        if ctx is not None and fired_now is True:
            try:
                meta: Dict[str, Any] = {}
                # Store a bar identifier when available so bars-ago can be computed in TF-bars.
                try:
                    bm = cur.get("bar_ms")
                    if bm is not None:
                        meta["bar_ms"] = int(bm)
                except Exception:
                    pass
                if rule.field in ("ms", "angle", "sig_angle", "ppo", "adx_angle", "atr_angle", "rsi_state", "stoch_rsi_kd"):
                    meta["post_state"] = str(cur.get(rule.field)).upper()
                ctx.note_event(rule, meta=meta)
            except Exception:
                pass

        # Optional: allow checking "event happened N steps ago" rather than only on the firing step.
        try:
            m = str(getattr(rule, "bars_ago_mode", "OFF") or "OFF").upper().strip()
            n_bars = int(getattr(rule, "bars_ago_n", 0) or 0)
        except Exception:
            m, n_bars = "OFF", 0

        if ctx is not None and m not in ("OFF", ""):
            ago = None
            try:
                ago = ctx.bars_ago(rule)
            except Exception:
                ago = None
            if ago is None or ago < 0:
                return False

            ok_hist: Optional[bool]
            if m in ("EXACT", "=="):
                ok_hist = (int(ago) == int(n_bars))
            elif m in ("WITHIN", "<=", "LE"):
                ok_hist = (int(ago) <= int(n_bars))
            else:
                ok_hist = fired_now

            if ok_hist is not True:
                return ok_hist

            # Optional: require the post-event state to still be valid on the current step.
            still_ok = _still_valid_event()
            if still_ok is None:
                return None
            return True if still_ok is True else False

        return fired_now

def sequence_rule_partition_full(rules: List["Rule"]) -> Tuple[Tuple["Rule", ...], Tuple["Rule", ...], Tuple["Rule", ...]]:
    """Split a SEQUENCE group into ordered steps, final-bar gates, and cancelers."""
    cache_key = _rules_identity_key(rules)
    cached = _SEQUENCE_PARTITION_CACHE.get(cache_key)
    if cached is not None:
        return cached
    seq_rules: List["Rule"] = []
    gate_rules: List["Rule"] = []
    cancel_rules: List["Rule"] = []
    for rule in list(rules or []):
        if bool(getattr(rule, "is_sequence_canceler", False)):
            cancel_rules.append(rule)
        elif bool(getattr(rule, "is_final_gate", False)):
            gate_rules.append(rule)
        else:
            seq_rules.append(rule)
    result = (tuple(seq_rules), tuple(gate_rules), tuple(cancel_rules))
    _SEQUENCE_PARTITION_CACHE[cache_key] = result
    return result


def sequence_rule_partition(rules: List["Rule"]) -> Tuple[Tuple["Rule", ...], Tuple["Rule", ...]]:
    """Split a SEQUENCE group into ordered steps and final-bar gate filters."""
    seq_rules, gate_rules, _cancel_rules = sequence_rule_partition_full(rules)
    return seq_rules, gate_rules


def trigger_filter_partition(rules: List["Rule"]) -> Tuple[Tuple["Rule", ...], Tuple["Rule", ...]]:
    cache_key = _rules_identity_key(rules)
    cached = _TRIGGER_FILTER_PARTITION_CACHE.get(cache_key)
    if cached is not None:
        return cached
    triggers: List["Rule"] = []
    filters: List["Rule"] = []
    for rule in list(rules or []):
        try:
            is_trigger = bool(getattr(rule, "is_trigger", False)) and str(getattr(rule, "mode", "") or "").lower() == "event"
        except Exception:
            is_trigger = False
        if is_trigger:
            triggers.append(rule)
        else:
            filters.append(rule)
    result = (tuple(triggers), tuple(filters))
    _TRIGGER_FILTER_PARTITION_CACHE[cache_key] = result
    return result


def _entry_sequence_side_from_group_key(group_key: Any) -> Optional[str]:
    try:
        if isinstance(group_key, tuple) and len(group_key) >= 1:
            tab_name = str(group_key[0] or "").strip().upper()
        else:
            tab_name = str(group_key or "").strip().upper()
    except Exception:
        return None
    if tab_name == "LONG ENTRY":
        return "LONG"
    if tab_name == "SHORT ENTRY":
        return "SHORT"
    return None


def _opposite_entry_side(side: Optional[str]) -> Optional[str]:
    side_norm = str(side or "").upper().strip()
    if side_norm == "LONG":
        return "SHORT"
    if side_norm == "SHORT":
        return "LONG"
    return None


def _sequence_marker_is_strictly_newer(*,
                                       candidate_step: Optional[int],
                                       candidate_bar_ms: Optional[int],
                                       reference_step: Optional[int],
                                       reference_bar_ms: Optional[int]) -> bool:
    """Return True only when the candidate marker is clearly later in time."""
    try:
        cand_bar = int(candidate_bar_ms) if candidate_bar_ms is not None else None
    except Exception:
        cand_bar = None
    try:
        ref_bar = int(reference_bar_ms) if reference_bar_ms is not None else None
    except Exception:
        ref_bar = None
    if cand_bar is not None and ref_bar is not None:
        if cand_bar != ref_bar:
            return cand_bar > ref_bar
        # Same source bar should not invalidate purely because of eval order.
        return False

    try:
        cand_step = int(candidate_step) if candidate_step is not None else None
    except Exception:
        cand_step = None
    try:
        ref_step = int(reference_step) if reference_step is not None else None
    except Exception:
        ref_step = None
    if cand_step is not None and ref_step is not None:
        return cand_step > ref_step
    return False


def eval_group_generic(rules: List["Rule"], join_mode: str,
                       cur_by_tf: Dict[str, Dict[str, Any]],
                       prev_by_tf: Dict[str, Dict[str, Any]],
                       ctx: Optional["RuleEvalContext"] = None,
                       group_key: Any = None) -> Optional[bool]:
    """
    Evaluate a group of rules with tri-state semantics:
      True/False/None ("WAIT" when data isn't ready)

    Trigger support:
      - Any EVENT rule can be marked is_trigger=True.
      - If a group contains ≥1 trigger rules, the group becomes:
            (ANY trigger fires)  AND  (all other rules pass under the group's AND/OR mode)
        Triggers always combine with OR (because they are "events").

    Sequence support:
      - join_mode == "SEQUENCE" turns the rules in the group into ordered steps.
      - Only the next waiting rule is evaluated for progression.
      - Progress is remembered in RuleEvalContext and is shared by live + backtest.
      - A completed sequence returns True on the bar where the final step completes,
        then resets to step 1 for the next opportunity.
    """

    join_mode_norm = str(join_mode or "OR").upper().strip()

    if not rules:
        return False

    def _eval_list(rs: List["Rule"], jm: str) -> Optional[bool]:
        jm_norm = str(jm or "OR").upper().strip()
        # Empty list = "no constraint"
        if not rs:
            return True

        if jm_norm == "AND":
            any_none = False
            for r in rs:
                ok = eval_rule_generic(r, cur_by_tf, prev_by_tf, ctx=ctx)
                if ok is None:
                    any_none = True
                elif not ok:
                    return False
            return None if any_none else True
        # OR
        any_none = False
        any_false = False
        for r in rs:
            ok = eval_rule_generic(r, cur_by_tf, prev_by_tf, ctx=ctx)
            if ok is True:
                return True
            if ok is None:
                any_none = True
            else:
                any_false = True
        # If nothing is True:
        #   - return False if at least one rule evaluated to False
        #   - return None only if *all* rules were None (not enough data yet)
        return False if any_false else (None if any_none else False)

    if join_mode_norm == "SEQUENCE":
        seq_rules, gate_rules, cancel_rules = sequence_rule_partition_full(rules)
        # Fallback when no stateful context is available: degrade gracefully to AND semantics.
        if ctx is None or group_key is None:
            fallback_rules = seq_rules or gate_rules or cancel_rules
            return _eval_list(fallback_rules, "AND") if fallback_rules else False

        # Degenerate-but-safe case: a SEQUENCE group with only final-gate filters behaves like
        # a normal AND filter bundle. This keeps old presets/editing mistakes from crashing.
        if not seq_rules:
            fallback_rules = gate_rules or cancel_rules
            return _eval_list(fallback_rules, "AND") if fallback_rules else False

        try:
            cur_step = int(getattr(ctx, "step_index", 0) or 0)
        except Exception:
            cur_step = 0

        last_eval_step = ctx.sequence_last_eval_step_by_group.get(group_key)
        if last_eval_step is not None and int(last_eval_step) == int(cur_step):
            return ctx.sequence_last_eval_result_by_group.get(group_key)

        total_steps = len(seq_rules)
        progress = int(ctx.sequence_progress_by_group.get(group_key, 0) or 0)
        if progress < 0 or progress >= total_steps:
            progress = 0
        ctx.sequence_last_progress_before_eval_by_group[group_key] = int(progress)

        entry_side = _entry_sequence_side_from_group_key(group_key)
        if progress > 0 and cancel_rules:
            for cancel_rule in cancel_rules:
                cancel_ok = eval_rule_generic(cancel_rule, cur_by_tf, prev_by_tf, ctx=ctx)
                if cancel_ok is True:
                    ctx.reset_sequence_group(group_key)
                    ctx.note_sequence_reset(
                        group_key,
                        reason="rule_canceler",
                        meta={
                            "rule_label": str(cancel_rule.label() if hasattr(cancel_rule, "label") else cancel_rule),
                            "rule_field": str(getattr(cancel_rule, "field", "") or ""),
                            "rule_timeframe": str(getattr(cancel_rule, "timeframe", "") or ""),
                        },
                    )
                    ctx.sequence_progress_by_group[group_key] = 0
                    ctx.sequence_last_eval_step_by_group[group_key] = int(cur_step)
                    ctx.sequence_last_eval_result_by_group[group_key] = False
                    return False

        if progress > 0 and entry_side is not None:
            opposite_side = _opposite_entry_side(entry_side)
            start_step = ctx.sequence_start_step_by_group.get(group_key)
            start_bar_ms = ctx.sequence_start_bar_ms_by_group.get(group_key)
            latest_opposite_step = ctx.latest_directional_trigger_step_by_side.get(str(opposite_side))
            latest_opposite_bar_ms = ctx.latest_directional_trigger_bar_ms_by_side.get(str(opposite_side))
            if _sequence_marker_is_strictly_newer(
                candidate_step=latest_opposite_step,
                candidate_bar_ms=latest_opposite_bar_ms,
                reference_step=start_step,
                reference_bar_ms=start_bar_ms,
            ):
                ctx.reset_sequence_group(group_key)
                ctx.note_sequence_reset(
                    group_key,
                    reason="opposite_trigger",
                    meta={
                        "entry_side": entry_side,
                        "opposite_side": opposite_side,
                        "start_step": start_step,
                        "start_bar_ms": start_bar_ms,
                        "opposite_trigger_step": latest_opposite_step,
                        "opposite_trigger_bar_ms": latest_opposite_bar_ms,
                    },
                )
                ctx.sequence_progress_by_group[group_key] = 0
                ctx.sequence_last_eval_step_by_group[group_key] = int(cur_step)
                ctx.sequence_last_eval_result_by_group[group_key] = False
                return False

        current_rule = seq_rules[progress]
        step_ok = eval_rule_generic(current_rule, cur_by_tf, prev_by_tf, ctx=ctx)

        cur_tf_snap = cur_by_tf.get(str(getattr(current_rule, "timeframe", "") or "").strip(), {}) if isinstance(cur_by_tf, dict) else {}
        cur_bar_ms = None
        if isinstance(cur_tf_snap, dict):
            for _k in ("bar_ms", "snapshot_bar_ms"):
                try:
                    _raw = cur_tf_snap.get(_k)
                    if _raw is not None:
                        cur_bar_ms = int(_raw)
                        break
                except Exception:
                    continue
        last_transition_bar_ms = ctx.sequence_last_transition_bar_ms_by_group.get(group_key)

        result: Optional[bool]
        if step_ok is True:
            # Deterministic guard: a SEQUENCE step may advance only on a *later* source bar than
            # the one that completed the previous step. This prevents mixed-TF state rules from
            # walking through several steps on repeated eval passes that still belong to the same
            # source candle.
            if cur_bar_ms is not None and last_transition_bar_ms is not None and int(cur_bar_ms) <= int(last_transition_bar_ms):
                result = False
            else:
                next_progress = progress + 1
                ctx.sequence_last_transition_step_by_group[group_key] = int(cur_step)
                ctx.sequence_last_transition_bar_ms_by_group[group_key] = (int(cur_bar_ms) if cur_bar_ms is not None else None)
                if progress == 0:
                    ctx.sequence_last_reset_meta_by_group.pop(group_key, None)
                    ctx.sequence_start_step_by_group[group_key] = int(cur_step)
                    ctx.sequence_start_bar_ms_by_group[group_key] = (int(cur_bar_ms) if cur_bar_ms is not None else None)
                    if entry_side is not None:
                        ctx.latest_directional_trigger_step_by_side[str(entry_side)] = int(cur_step)
                        ctx.latest_directional_trigger_bar_ms_by_side[str(entry_side)] = (
                            int(cur_bar_ms) if cur_bar_ms is not None else None
                        )
                if next_progress >= total_steps:
                    # Final-bar gate filters are checked only on the completion bar. If they do
                    # not pass, the completed opportunity is dropped and the sequence resets.
                    gate_ok = _eval_list(gate_rules, "AND") if gate_rules else True
                    ctx.reset_sequence_group(group_key)
                    ctx.sequence_progress_by_group[group_key] = 0
                    result = True if gate_ok is True else (None if gate_ok is None else False)
                else:
                    ctx.sequence_progress_by_group[group_key] = int(next_progress)
                    result = False
        elif step_ok is None:
            result = None
        else:
            result = False

        ctx.sequence_last_eval_step_by_group[group_key] = int(cur_step)
        ctx.sequence_last_eval_result_by_group[group_key] = result
        return result

    triggers, filters = trigger_filter_partition(rules)

    # No triggers -> classic behavior over entire group
    if not triggers:
        return _eval_list(rules, join_mode_norm)

    trig_ok = _eval_list(triggers, "OR")
    filt_ok = _eval_list(filters, join_mode_norm)

    # Combine (AND)
    if trig_ok is False or filt_ok is False:
        return False
    if trig_ok is None or filt_ok is None:
        return None
    return True

def eval_tab_generic(tab_name: str,
                     rules_model: Dict[str, List[List["Rule"]]],
                     tab_group_join_mode: Dict[str, str],
                     group_rule_join_mode: Dict[str, List[str]],
                     cur_by_tf: Dict[str, Dict[str, Any]],
                     prev_by_tf: Dict[str, Dict[str, Any]],
                     ctx: Optional["RuleEvalContext"] = None) -> Optional[bool]:
    groups = rules_model.get(tab_name, [])
    if not groups:
        return False

    join_groups = tab_group_join_mode.get(tab_name, "AND")

    if join_groups == "AND":
        any_none = False
        for gi, g in enumerate(groups):
            jm = group_rule_join_mode.get(tab_name, ["OR"])[gi] if gi < len(group_rule_join_mode.get(tab_name, [])) else "OR"
            ok = eval_group_generic(g, jm, cur_by_tf, prev_by_tf, ctx=ctx, group_key=(tab_name, gi))
            if ok is False:
                return False
            if ok is None:
                any_none = True
        return None if any_none else True
    # OR between groups
    any_none = False
    any_false = False
    for gi, g in enumerate(groups):
        jm = group_rule_join_mode.get(tab_name, ["OR"])[gi] if gi < len(group_rule_join_mode.get(tab_name, [])) else "OR"
        ok = eval_group_generic(g, jm, cur_by_tf, prev_by_tf, ctx=ctx, group_key=(tab_name, gi))
        if ok is True:
            return True
        if ok is None:
            any_none = True
        else:
            any_false = True
    return False if any_false else (None if any_none else False)


def eval_tab_group_results(tab_name: str,
                           rules_model: Dict[str, List[List["Rule"]]],
                           group_rule_join_mode: Dict[str, List[str]],
                           cur_by_tf: Dict[str, Dict[str, Any]],
                           prev_by_tf: Dict[str, Dict[str, Any]],
                           ctx: Optional["RuleEvalContext"] = None) -> List[Optional[bool]]:
    groups = rules_model.get(tab_name, [])
    if not groups:
        return []
    results: List[Optional[bool]] = []
    group_modes = group_rule_join_mode.get(tab_name, [])
    for gi, group_rules in enumerate(groups):
        join_mode = group_modes[gi] if gi < len(group_modes) else "OR"
        results.append(eval_group_generic(group_rules, join_mode, cur_by_tf, prev_by_tf, ctx=ctx, group_key=(tab_name, gi)))
    return results


def combine_tab_group_results(group_results: List[Optional[bool]], join_groups: str) -> Optional[bool]:
    if not group_results:
        return False
    join_norm = str(join_groups or "AND").upper().strip()
    if join_norm == "AND":
        any_none = False
        for result in group_results:
            if result is False:
                return False
            if result is None:
                any_none = True
        return None if any_none else True
    any_none = False
    any_false = False
    for result in group_results:
        if result is True:
            return True
        if result is None:
            any_none = True
        else:
            any_false = True
    return False if any_false else (None if any_none else False)


def matching_group_indices_from_results(group_results: List[Optional[bool]], tab_result: Optional[bool]) -> List[int]:
    if tab_result is not True:
        return []
    return [int(index) for index, result in enumerate(group_results) if result is True]


def eval_tab_matching_groups(tab_name: str,
                             rules_model: Dict[str, List[List["Rule"]]],
                             tab_group_join_mode: Dict[str, str],
                             group_rule_join_mode: Dict[str, List[str]],
                             cur_by_tf: Dict[str, Dict[str, Any]],
                             prev_by_tf: Dict[str, Dict[str, Any]],
                             ctx: Optional["RuleEvalContext"] = None) -> Tuple[Optional[bool], List[int], List[Optional[bool]]]:
    group_results = eval_tab_group_results(tab_name, rules_model, group_rule_join_mode, cur_by_tf, prev_by_tf, ctx=ctx)
    tab_result = combine_tab_group_results(group_results, tab_group_join_mode.get(tab_name, "AND"))
    return tab_result, matching_group_indices_from_results(group_results, tab_result), group_results



def _trace_bool_text(v: Any) -> str:
    if v is True:
        return "TRUE"
    if v is False:
        return "FALSE"
    return "NONE"


def _trace_jsonable(v: Any) -> Any:
    if isinstance(v, (str, int, float, bool)) or v is None:
        return v
    try:
        if np.isscalar(v):
            if isinstance(v, (np.floating, np.integer, np.bool_)):
                return v.item()
    except Exception:
        pass
    try:
        return float(v)
    except Exception:
        pass
    try:
        return str(v)
    except Exception:
        return repr(v)


def _trace_snapshot_meta(snap: Any) -> Dict[str, Any]:
    meta: Dict[str, Any] = {}
    if not isinstance(snap, dict):
        return meta

    def _pick_time(keys: List[str]) -> Any:
        for k in keys:
            try:
                v = snap.get(k)
            except Exception:
                v = None
            if v is not None:
                return v
        return None

    cur_time = _pick_time(["snapshot_time", "close_time", "time", "ts"])
    open_time = _pick_time(["open_time"])
    bar_ms = _pick_time(["bar_ms", "snapshot_bar_ms"])

    if cur_time is not None:
        meta["bar_time"] = _trace_jsonable(cur_time)
    if open_time is not None:
        meta["open_time"] = _trace_jsonable(open_time)
    if bar_ms is not None:
        meta["bar_ms"] = _trace_jsonable(bar_ms)
    return meta


def _trace_state_expression(value: Any, op: str, expected: Any, result: Any) -> str:
    return f"expr: {value} {op} {expected} -> {result}"


def _trace_rule_state_payload(rule: "Rule", cur: Dict[str, Any], prev: Dict[str, Any], ctx: Optional["RuleEvalContext"] = None) -> Dict[str, Any]:
    compare_label = _compare_to_label(getattr(rule, "compare_to", None))
    compare_target = _normalize_compare_to(getattr(rule, "compare_to", None))
    payload: Dict[str, Any] = {
        "field": str(getattr(rule, "field", "") or ""),
        "mode": str(getattr(rule, "mode", "") or ""),
        "op": str(getattr(rule, "op", "") or ""),
        "expected": _trace_jsonable(compare_label if compare_label is not None else getattr(rule, "value", None)),
        "compare_to": _trace_jsonable(compare_target) if compare_target is not None else None,
        "current": None,
        "previous": None,
        "used": None,
        "right": None,
        "current_meta": _trace_snapshot_meta(cur),
        "previous_meta": _trace_snapshot_meta(prev),
        "bars_ago_mode": str(getattr(rule, "bars_ago_mode", "OFF") or "OFF"),
        "bars_ago_n": int(getattr(rule, "bars_ago_n", 0) or 0),
        "require_still_valid": bool(getattr(rule, "require_still_valid", False)),
    }

    fld = str(getattr(rule, "field", "") or "")
    cur_key = fld
    prev_key = fld
    if fld == "frama_state":
        cur_key = prev_key = "frama_state"
    elif fld == "vidya_state":
        cur_key = prev_key = "vidya_state"
    elif fld == "range_filter_state":
        cur_key = prev_key = "range_filter_state"
    elif fld == "range_filter_phase":
        cur_key = prev_key = "range_filter_phase"

    if isinstance(cur, dict):
        payload["current"] = _trace_jsonable(_snapshot_value_from_snapshot(cur, cur_key))
    if isinstance(prev, dict):
        payload["previous"] = _trace_jsonable(_snapshot_value_from_snapshot(prev, prev_key))

    try:
        m_mode = str(getattr(rule, "bars_ago_mode", "OFF") or "OFF").upper().strip()
        n_bars = int(getattr(rule, "bars_ago_n", 0) or 0)
    except Exception:
        m_mode, n_bars = "OFF", 0

    if ctx is not None and m_mode not in ("", "OFF"):
        try:
            if m_mode in ("EXACT", "=="):
                snap = cur if n_bars == 0 else (ctx.snapshot_bar_ago(str(rule.timeframe).strip(), n_bars) or ctx.snapshot_ago(str(rule.timeframe).strip(), n_bars))
                if isinstance(snap, dict):
                    payload["used"] = _trace_jsonable(_snapshot_value_from_snapshot(snap, cur_key))
                    payload["used_meta"] = _trace_snapshot_meta(snap)
            elif m_mode in ("WITHIN", "<=", "LE"):
                vals = []
                for k in range(0, max(0, n_bars) + 1):
                    snap = cur if k == 0 else (ctx.snapshot_bar_ago(str(rule.timeframe).strip(), k) or ctx.snapshot_ago(str(rule.timeframe).strip(), k))
                    if isinstance(snap, dict):
                        vals.append({"bars_ago": int(k), "value": _trace_jsonable(_snapshot_value_from_snapshot(snap, cur_key)), "meta": _trace_snapshot_meta(snap)})
                payload["used"] = vals
        except Exception:
            pass
    if compare_target is not None:
        try:
            target_tf = str(compare_target.get("timeframe") or getattr(rule, "timeframe", "") or "").strip()
            target_bars_ago = int(compare_target.get("bars_ago", 0) or 0)
            if ctx is not None:
                rhs_snap = ctx.snapshot_bar_ago(target_tf, target_bars_ago) or ctx.snapshot_ago(target_tf, target_bars_ago)
            elif target_bars_ago == 0:
                rhs_snap = cur
            elif target_bars_ago == 1:
                rhs_snap = prev
            else:
                rhs_snap = None
            if isinstance(rhs_snap, dict):
                payload["right"] = _trace_jsonable(_snapshot_value_from_snapshot(rhs_snap, str(compare_target.get("field") or "")))
                payload["right_meta"] = _trace_snapshot_meta(rhs_snap)
        except Exception:
            pass
    return payload


def _trace_rule_event_payload(rule: "Rule", cur: Dict[str, Any], prev: Dict[str, Any]) -> Dict[str, Any]:
    fld = str(getattr(rule, "field", "") or "")
    payload: Dict[str, Any] = {
        "field": fld,
        "mode": str(getattr(rule, "mode", "") or ""),
        "op": str(getattr(rule, "op", "") or ""),
        "expected": _trace_jsonable(getattr(rule, "value", None)),
        "current": {},
        "previous": {},
        "current_meta": _trace_snapshot_meta(cur),
        "previous_meta": _trace_snapshot_meta(prev),
    }

    keys: List[str]
    if fld in ("ms", "angle", "sig_angle", "ppo", "adx_angle", "atr_angle"):
        keys = [fld]
    elif fld == "ms_cross":
        keys = ["macd", "signal"]
    elif fld == "macd0_cross":
        keys = ["macd"]
    elif fld == "di_cross":
        keys = ["di_plus", "di_minus"]
    elif fld in ("frama_break_up", "frama_break_down", "frama_mid_cross"):
        keys = ["frama_state", "frama_break_up", "frama_break_down", "frama_mid_cross", "frama_trigger_side"]
    elif fld in ("vidya_trend_up", "vidya_trend_down"):
        keys = ["vidya_state"]
    elif fld in ("vidya_new_liq_low", "vidya_new_liq_high"):
        keys = ["vidya_active_liq_low_count", "vidya_active_liq_high_count"]
    elif fld in ("vidya_liq_low_taken", "vidya_liq_high_taken"):
        keys = [fld]
    elif fld in ("range_filter_buy", "range_filter_sell"):
        keys = ["range_filter_state"]
    else:
        keys = [fld]

    for k in keys:
        try:
            payload["current"][k] = _trace_jsonable((cur or {}).get(k))
        except Exception:
            payload["current"][k] = None
        try:
            payload["previous"][k] = _trace_jsonable((prev or {}).get(k))
        except Exception:
            payload["previous"][k] = None
    return payload


def eval_rule_generic_trace(rule: "Rule",
                            cur_by_tf: Dict[str, Dict[str, Any]],
                            prev_by_tf: Dict[str, Dict[str, Any]],
                            ctx: Optional["RuleEvalContext"] = None) -> Dict[str, Any]:
    tfk = str(rule.timeframe).strip()
    cur = cur_by_tf.get(tfk, {}) or cur_by_tf.get(tfk.lower(), {}) or cur_by_tf.get(tfk.upper(), {}) or {}
    prev = prev_by_tf.get(tfk, {}) or prev_by_tf.get(tfk.lower(), {}) or prev_by_tf.get(tfk.upper(), {}) or {}
    result = eval_rule_generic(rule, cur_by_tf, prev_by_tf, ctx=ctx)
    payload = _trace_rule_state_payload(rule, cur, prev, ctx=ctx) if str(getattr(rule, "mode", "") or "") == "state" else _trace_rule_event_payload(rule, cur, prev)
    try:
        if str(getattr(rule, "mode", "") or "") == "state":
            rhs_for_expr = payload.get("right") if payload.get("compare_to") is not None else _trace_jsonable(getattr(rule, "value", None))
            payload["expression"] = _trace_state_expression(payload.get("current"), str(getattr(rule, "op", "") or ""), rhs_for_expr, result)
    except Exception:
        pass
    return {
        "label": (rule.label() if hasattr(rule, "label") else f"{getattr(rule,'timeframe','')} {getattr(rule,'field','')} {getattr(rule,'op','')} {getattr(rule,'value','')}"),
        "timeframe": tfk,
        "mode": str(getattr(rule, "mode", "") or ""),
        "field": str(getattr(rule, "field", "") or ""),
        "op": str(getattr(rule, "op", "") or ""),
        "expected": payload.get("expected", _trace_jsonable(getattr(rule, "value", None))),
        "is_trigger": bool(getattr(rule, "is_trigger", False)),
        "is_final_gate": bool(getattr(rule, "is_final_gate", False)),
        "is_sequence_canceler": bool(getattr(rule, "is_sequence_canceler", False)),
        "result": result,
        "trace": payload,
    }


def eval_group_generic_trace(group_rules: List["Rule"],
                             join_mode: str,
                             cur_by_tf: Dict[str, Dict[str, Any]],
                             prev_by_tf: Dict[str, Dict[str, Any]],
                             ctx: Optional["RuleEvalContext"] = None,
                             group_key: Any = None) -> Dict[str, Any]:
    traces = [eval_rule_generic_trace(r, cur_by_tf, prev_by_tf, ctx=ctx) for r in (group_rules or [])]
    group_result = eval_group_generic(group_rules, join_mode, cur_by_tf, prev_by_tf, ctx=ctx, group_key=group_key)
    out = {
        "join_mode": str(join_mode or "OR"),
        "result": group_result,
        "rules": traces,
    }
    if str(join_mode or "OR").upper().strip() == "SEQUENCE" and ctx is not None and group_key is not None:
        try:
            seq_rules, gate_rules = sequence_rule_partition(group_rules or [])
            out["sequence"] = ctx.sequence_snapshot(group_key, len(seq_rules or []))
            out["sequence_gate_count"] = len(gate_rules or [])
        except Exception:
            pass
    return out


def eval_tab_generic_trace(tab_name: str,
                           rules_model: Dict[str, List[List["Rule"]]],
                           tab_group_join_mode: Dict[str, str],
                           group_rule_join_mode: Dict[str, List[str]],
                           cur_by_tf: Dict[str, Dict[str, Any]],
                           prev_by_tf: Dict[str, Dict[str, Any]],
                           ctx: Optional["RuleEvalContext"] = None) -> Dict[str, Any]:
    groups = rules_model.get(tab_name, []) or []
    join_groups = str(tab_group_join_mode.get(tab_name, "AND") or "AND")
    group_traces = []
    for gi, g in enumerate(groups):
        jm_list = group_rule_join_mode.get(tab_name, []) or []
        jm = jm_list[gi] if gi < len(jm_list) else "OR"
        group_traces.append(eval_group_generic_trace(g, jm, cur_by_tf, prev_by_tf, ctx=ctx, group_key=(tab_name, gi)))
    tf_snapshots: Dict[str, Any] = {}
    try:
        for tf in sorted(set(list((cur_by_tf or {}).keys()) + list((prev_by_tf or {}).keys()))):
            tf_snapshots[str(tf)] = {
                "current": _trace_snapshot_meta((cur_by_tf or {}).get(tf, {})),
                "previous": _trace_snapshot_meta((prev_by_tf or {}).get(tf, {})),
            }
    except Exception:
        tf_snapshots = {}
    return {
        "tab": str(tab_name or ""),
        "groups_join": join_groups,
        "result": eval_tab_generic(tab_name, rules_model, tab_group_join_mode, group_rule_join_mode, cur_by_tf, prev_by_tf, ctx=ctx),
        "groups": group_traces,
        "tf_snapshots": tf_snapshots,
    }


def _trace_payload_to_text(payload: Any) -> str:
    if isinstance(payload, dict):
        parts = []
        cur = payload.get("current")
        prev = payload.get("previous")
        used = payload.get("used", None)
        right = payload.get("right", None)
        cur_meta = payload.get("current_meta")
        prev_meta = payload.get("previous_meta")
        used_meta = payload.get("used_meta")
        right_meta = payload.get("right_meta")
        expr = payload.get("expression")
        if cur not in (None, {}, []):
            parts.append(f"cur={cur}")
        if cur_meta not in (None, {}, []):
            parts.append(f"curMeta={cur_meta}")
        if prev not in (None, {}, []):
            parts.append(f"prev={prev}")
        if prev_meta not in (None, {}, []):
            parts.append(f"prevMeta={prev_meta}")
        if used not in (None, {}, []):
            parts.append(f"used={used}")
        if used_meta not in (None, {}, []):
            parts.append(f"usedMeta={used_meta}")
        if payload.get("compare_to") is not None:
            parts.append(f"right={right}")
        if right_meta not in (None, {}, []):
            parts.append(f"rightMeta={right_meta}")
        mode = str(payload.get("bars_ago_mode", "OFF") or "OFF")
        if mode not in ("", "OFF"):
            parts.append(f"barsAgo={mode}:{payload.get('bars_ago_n', 0)}")
        if bool(payload.get("require_still_valid", False)):
            parts.append("stillValid=YES")
        if expr not in (None, ""):
            parts.append(str(expr))
        return " | ".join(parts)
    return str(payload)


def format_tab_trace(trace: Dict[str, Any]) -> str:
    if not isinstance(trace, dict):
        return ""
    lines: List[str] = []
    lines.append(f"Tab: {trace.get('tab', '')} | Result: {_trace_bool_text(trace.get('result'))} | GroupJoin: {trace.get('groups_join', '')}")
    tf_snapshots = trace.get("tf_snapshots", {}) if isinstance(trace.get("tf_snapshots", {}), dict) else {}
    for tf, meta in tf_snapshots.items():
        try:
            lines.append(f"TF[{tf}] cur={meta.get('current', {})} | prev={meta.get('previous', {})}")
        except Exception:
            pass
    groups = trace.get("groups", []) if isinstance(trace.get("groups", []), list) else []
    for gi, g in enumerate(groups, start=1):
        lines.append(f"Group {gi}: Join={g.get('join_mode', '')} | Result={_trace_bool_text(g.get('result'))}")
        seq = g.get("sequence") if isinstance(g.get("sequence"), dict) else None
        if seq:
            try:
                completed = int(seq.get("completed_steps", 0) or 0)
                total = int(seq.get("total_steps", 0) or 0)
                nxt = int(seq.get("next_step_index", completed) or completed)
                gate_count = int(g.get("sequence_gate_count", 0) or 0) if isinstance(g, dict) else 0
                gate_txt = f" | final_gates={gate_count}" if gate_count > 0 else ""
                lines.append(f"  Sequence progress: completed={completed}/{total} | next_step={nxt + 1 if nxt < total else total}{gate_txt}")
                reset_meta = seq.get("last_reset_meta") if isinstance(seq.get("last_reset_meta"), dict) else None
                if reset_meta:
                    reason = str(reset_meta.get("reason") or "").strip().lower()
                    if reason == "opposite_trigger":
                        entry_side = str(reset_meta.get("entry_side") or "").upper().strip()
                        opposite_side = str(reset_meta.get("opposite_side") or "").upper().strip()
                        lines.append(
                            f"  Sequence reset: invalidated by opposite {opposite_side or '?'} trigger while building {entry_side or '?'} setup"
                        )
                    elif reason == "rule_canceler":
                        rule_label = str(reset_meta.get("rule_label") or "sequence canceler").strip()
                        lines.append(f"  Sequence reset: canceled by rule {rule_label}")
            except Exception:
                pass
        rules = g.get("rules", []) if isinstance(g.get("rules", []), list) else []
        for ri, r in enumerate(rules, start=1):
            detail = _trace_payload_to_text(r.get("trace"))
            trigger_mark = " [TRIGGER]" if bool(r.get("is_trigger", False)) else ""
            gate_mark = " [FINAL GATE]" if bool(r.get("is_final_gate", False)) else ""
            cancel_mark = " [SEQ CANCEL]" if bool(r.get("is_sequence_canceler", False)) else ""
            lines.append(f"  {gi}.{ri} {_trace_bool_text(r.get('result'))}{trigger_mark}{gate_mark}{cancel_mark} :: {r.get('label', '')}")
            if detail:
                lines.append(f"      {detail}")
    return "\n".join(lines)
