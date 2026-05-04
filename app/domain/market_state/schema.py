from __future__ import annotations

"""Typed market-state contracts for the phase 2 cleanup pass."""

from dataclasses import dataclass, field
from typing import Any, Dict, Mapping, Optional

import numpy as np


def _coerce_float(value: Any, default: Optional[float] = None) -> Optional[float]:
    try:
        if value is None:
            return default
        out = float(value)
        if np.isfinite(out):
            return out
    except Exception:
        pass
    return default


def _coerce_int(value: Any, default: Optional[int] = None) -> Optional[int]:
    try:
        if value is None:
            return default
        return int(value)
    except Exception:
        return default


def _coerce_bool(value: Any) -> bool:
    if isinstance(value, (bool, np.bool_)):
        return bool(value)
    if isinstance(value, str):
        s = value.strip().lower()
        if s in {"1", "true", "yes", "on"}:
            return True
        if s in {"0", "false", "no", "off", ""}:
            return False
    try:
        return bool(value)
    except Exception:
        return False


def _coerce_text(value: Any, default: str = "") -> str:
    try:
        out = str(value if value is not None else default)
    except Exception:
        return default
    return out


def _coerce_upper(value: Any, default: str = "") -> str:
    return _coerce_text(value, default).upper().strip() or default


def coerce_mapping(value: Any) -> Dict[str, Any]:
    if value is None:
        return {}
    if isinstance(value, Mapping):
        try:
            return dict(value)
        except Exception:
            return {}
    as_dict = getattr(value, "as_dict", None)
    if callable(as_dict):
        try:
            out = as_dict()
            if isinstance(out, Mapping):
                return dict(out)
        except Exception:
            return {}
    return {}


@dataclass(frozen=True)
class ComponentStatus:
    status: str = "ok"
    reason: str = ""
    error_type: str = ""
    degraded_feature: str = ""

    @classmethod
    def from_mapping(cls, data: Mapping[str, Any], prefix: str) -> "ComponentStatus":
        return cls(
            status=_coerce_text(data.get(f"{prefix}_status"), "ok") or "ok",
            reason=_coerce_text(data.get(f"{prefix}_status_reason"), ""),
            error_type=_coerce_text(data.get(f"{prefix}_error_type"), ""),
            degraded_feature=_coerce_text(data.get(f"{prefix}_degraded_feature"), ""),
        )

    def as_dict(self, prefix: str) -> Dict[str, Any]:
        return {
            f"{prefix}_status": self.status,
            f"{prefix}_status_reason": self.reason,
            f"{prefix}_error_type": self.error_type,
            f"{prefix}_degraded_feature": self.degraded_feature,
        }


@dataclass(frozen=True)
class BarSnapshot:
    price: Optional[float] = None
    open: Optional[float] = None
    high: Optional[float] = None
    low: Optional[float] = None
    close: Optional[float] = None
    volume: Optional[float] = None
    open_time_ms: Optional[int] = None
    close_time_ms: Optional[int] = None
    timeframe: str = ""
    symbol: str = ""

    @classmethod
    def from_mapping(cls, data: Mapping[str, Any]) -> "BarSnapshot":
        close = _coerce_float(data.get("close"), None)
        price = _coerce_float(data.get("price"), close)
        return cls(
            price=price,
            open=_coerce_float(data.get("open"), price),
            high=_coerce_float(data.get("high"), price),
            low=_coerce_float(data.get("low"), price),
            close=close if close is not None else price,
            volume=_coerce_float(data.get("volume"), None),
            open_time_ms=_coerce_int(data.get("open_time_ms") or data.get("bar_open_time_ms"), None),
            close_time_ms=_coerce_int(data.get("close_time_ms") or data.get("bar_close_time_ms") or data.get("ts_ms"), None),
            timeframe=_coerce_text(data.get("tf") or data.get("timeframe") or ""),
            symbol=_coerce_text(data.get("symbol") or ""),
        )

    def as_dict(self) -> Dict[str, Any]:
        return {
            "price": self.price,
            "open": self.open,
            "high": self.high,
            "low": self.low,
            "close": self.close,
            "volume": self.volume,
            "open_time_ms": self.open_time_ms,
            "close_time_ms": self.close_time_ms,
            "timeframe": self.timeframe,
            "symbol": self.symbol,
        }


@dataclass(frozen=True)
class IndicatorSnapshot:
    vidya_state: str = ""
    frama_state: str = ""
    range_filter_state: str = ""
    range_filter_phase: str = ""
    ms: str = ""
    angle: str = ""
    atr_angle: str = ""
    macd: Optional[float] = None
    signal: Optional[float] = None
    atr: Optional[float] = None
    vidya_delta_pct: Optional[float] = None
    vidya_angle_deg: Optional[float] = None
    vidya_angle_deg_norm: Optional[float] = None

    @classmethod
    def from_mapping(cls, data: Mapping[str, Any]) -> "IndicatorSnapshot":
        return cls(
            vidya_state=_coerce_upper(data.get("vidya_state"), ""),
            frama_state=_coerce_upper(data.get("frama_state"), ""),
            range_filter_state=_coerce_upper(data.get("range_filter_state"), ""),
            range_filter_phase=_coerce_upper(data.get("range_filter_phase"), ""),
            ms=_coerce_upper(data.get("ms"), ""),
            angle=_coerce_upper(data.get("angle"), ""),
            atr_angle=_coerce_upper(data.get("atr_angle"), ""),
            macd=_coerce_float(data.get("macd"), None),
            signal=_coerce_float(data.get("signal"), None),
            atr=_coerce_float(data.get("atr"), None),
            vidya_delta_pct=_coerce_float(data.get("vidya_delta_pct"), None),
            vidya_angle_deg=_coerce_float(data.get("vidya_angle_deg"), None),
            vidya_angle_deg_norm=_coerce_float(data.get("vidya_angle_deg_norm"), None),
        )

    def as_dict(self) -> Dict[str, Any]:
        return {
            "vidya_state": self.vidya_state,
            "frama_state": self.frama_state,
            "range_filter_state": self.range_filter_state,
            "range_filter_phase": self.range_filter_phase,
            "ms": self.ms,
            "angle": self.angle,
            "atr_angle": self.atr_angle,
            "macd": self.macd,
            "signal": self.signal,
            "atr": self.atr,
            "vidya_delta_pct": self.vidya_delta_pct,
            "vidya_angle_deg": self.vidya_angle_deg,
            "vidya_angle_deg_norm": self.vidya_angle_deg_norm,
        }


@dataclass(frozen=True)
class MarketFeatures:
    compression_score: float = 0.0
    transition_score: float = 0.0
    trend_score: float = 0.0
    pullback_score: float = 0.0
    exhaustion_score: float = 0.0
    breakout_quality: float = 0.0
    breakout_readiness: float = 0.0
    breakout_confirmation: float = 0.0
    compression_maturity: float = 0.0
    containment_rate: float = 0.0
    slope_flatness: float = 0.0
    range_contraction: float = 0.0
    swing_squeeze: float = 0.0
    delta_strength: float = 0.0
    direction_evidence: float = 0.0
    conflict_score: float = 0.0
    alignment_fraction: float = 0.0
    pressure_bias: str = "NEUTRAL"

    @classmethod
    def from_mapping(cls, data: Mapping[str, Any]) -> "MarketFeatures":
        return cls(
            compression_score=float(_coerce_float(data.get("market_compression_score"), 0.0) or 0.0),
            transition_score=float(_coerce_float(data.get("market_transition_score"), 0.0) or 0.0),
            trend_score=float(_coerce_float(data.get("market_trend_score"), 0.0) or 0.0),
            pullback_score=float(_coerce_float(data.get("market_pullback_score"), 0.0) or 0.0),
            exhaustion_score=float(_coerce_float(data.get("market_exhaustion_score"), 0.0) or 0.0),
            breakout_quality=float(_coerce_float(data.get("market_breakout_quality"), 0.0) or 0.0),
            breakout_readiness=float(_coerce_float(data.get("market_breakout_readiness"), 0.0) or 0.0),
            breakout_confirmation=float(_coerce_float(data.get("market_breakout_confirmation"), 0.0) or 0.0),
            compression_maturity=float(_coerce_float(data.get("market_compression_maturity"), 0.0) or 0.0),
            containment_rate=float(_coerce_float(data.get("market_containment_rate"), 0.0) or 0.0),
            slope_flatness=float(_coerce_float(data.get("market_slope_flatness"), 0.0) or 0.0),
            range_contraction=float(_coerce_float(data.get("market_range_contraction"), 0.0) or 0.0),
            swing_squeeze=float(_coerce_float(data.get("market_swing_squeeze"), 0.0) or 0.0),
            delta_strength=float(_coerce_float(data.get("market_delta_strength"), 0.0) or 0.0),
            direction_evidence=float(_coerce_float(data.get("market_direction_evidence"), 0.0) or 0.0),
            conflict_score=float(_coerce_float(data.get("market_conflict_score"), 0.0) or 0.0),
            alignment_fraction=float(_coerce_float(data.get("market_alignment_fraction"), 0.0) or 0.0),
            pressure_bias=_coerce_upper(data.get("market_pressure_bias"), "NEUTRAL") or "NEUTRAL",
        )

    def as_dict(self) -> Dict[str, Any]:
        return {
            "market_compression_score": self.compression_score,
            "market_transition_score": self.transition_score,
            "market_trend_score": self.trend_score,
            "market_pullback_score": self.pullback_score,
            "market_exhaustion_score": self.exhaustion_score,
            "market_breakout_quality": self.breakout_quality,
            "market_breakout_readiness": self.breakout_readiness,
            "market_breakout_confirmation": self.breakout_confirmation,
            "market_compression_maturity": self.compression_maturity,
            "market_containment_rate": self.containment_rate,
            "market_slope_flatness": self.slope_flatness,
            "market_range_contraction": self.range_contraction,
            "market_swing_squeeze": self.swing_squeeze,
            "market_delta_strength": self.delta_strength,
            "market_direction_evidence": self.direction_evidence,
            "market_conflict_score": self.conflict_score,
            "market_alignment_fraction": self.alignment_fraction,
            "market_pressure_bias": self.pressure_bias,
        }


@dataclass(frozen=True)
class BreakoutAssessment:
    state: str = "NONE"
    bias: str = "NEUTRAL"
    pretty: str = "-"
    score: int = 0
    trigger_ready: bool = False
    note: str = ""

    @classmethod
    def from_mapping(cls, data: Mapping[str, Any]) -> "BreakoutAssessment":
        return cls(
            state=_coerce_upper(data.get("market_breakout_state"), "NONE") or "NONE",
            bias=_coerce_upper(data.get("market_breakout_bias"), "NEUTRAL") or "NEUTRAL",
            pretty=_coerce_text(data.get("market_breakout_pretty"), "-"),
            score=int(_coerce_int(data.get("market_breakout_score"), 0) or 0),
            trigger_ready=_coerce_bool(data.get("market_breakout_trigger_ready")),
            note=_coerce_text(data.get("market_breakout_note"), ""),
        )

    def as_dict(self) -> Dict[str, Any]:
        return {
            "market_breakout_state": self.state,
            "market_breakout_bias": self.bias,
            "market_breakout_pretty": self.pretty,
            "market_breakout_score": self.score,
            "market_breakout_trigger_ready": self.trigger_ready,
            "market_breakout_note": self.note,
        }


@dataclass(frozen=True)
class MarketStateDecision:
    label: str = "TRANSITION"
    bias: str = "NEUTRAL"
    phase: str = "TRANSITION"
    confidence: int = 0
    age: int = 1
    pretty: str = "Transition"
    direction_score: int = 0
    energy_score: int = 0
    alignment_bonus: int = 0
    macd_gap_norm: Optional[float] = None
    texture: str = ""
    summary_short: str = ""
    summary_long: str = ""
    pressure_bias: str = "NEUTRAL"
    breakout: BreakoutAssessment = field(default_factory=BreakoutAssessment)
    status: ComponentStatus = field(default_factory=ComponentStatus)
    payload: Dict[str, Any] = field(default_factory=dict)

    @classmethod
    def from_mapping(cls, data: Mapping[str, Any]) -> "MarketStateDecision":
        return cls(
            label=_coerce_upper(data.get("market_state") or data.get("label"), "TRANSITION") or "TRANSITION",
            bias=_coerce_upper(data.get("market_bias") or data.get("bias"), "NEUTRAL") or "NEUTRAL",
            phase=_coerce_upper(data.get("market_phase") or data.get("phase"), "TRANSITION") or "TRANSITION",
            confidence=int(_coerce_int(data.get("market_confidence") or data.get("confidence"), 0) or 0),
            age=max(1, int(_coerce_int(data.get("market_state_age") or data.get("age"), 1) or 1)),
            pretty=_coerce_text(data.get("market_state_pretty") or data.get("pretty"), "Transition"),
            direction_score=int(_coerce_int(data.get("market_direction_score") or data.get("direction_score"), 0) or 0),
            energy_score=int(_coerce_int(data.get("market_energy_score") or data.get("energy_score"), 0) or 0),
            alignment_bonus=int(_coerce_int(data.get("market_alignment_bonus") or data.get("alignment_bonus"), 0) or 0),
            macd_gap_norm=_coerce_float(data.get("market_macd_gap_norm"), np.nan),
            texture=_coerce_text(data.get("market_texture") or data.get("texture"), ""),
            summary_short=_coerce_text(data.get("market_summary_short") or data.get("summary_short"), ""),
            summary_long=_coerce_text(data.get("market_summary_long") or data.get("summary_long"), ""),
            pressure_bias=_coerce_upper(data.get("market_pressure_bias"), "NEUTRAL") or "NEUTRAL",
            breakout=BreakoutAssessment.from_mapping(data),
            status=ComponentStatus.from_mapping(data, 'market_state'),
            payload=coerce_mapping(data),
        )

    def as_dict(self) -> Dict[str, Any]:
        out = dict(self.payload)
        out.update(
            {
                "market_state": self.label,
                "market_bias": self.bias,
                "market_phase": self.phase,
                "market_confidence": int(self.confidence),
                "market_state_age": int(max(1, self.age)),
                "market_state_pretty": self.pretty,
                "market_direction_score": int(self.direction_score),
                "market_energy_score": int(self.energy_score),
                "market_alignment_bonus": int(self.alignment_bonus),
                "market_macd_gap_norm": self.macd_gap_norm,
                "market_texture": self.texture,
                "market_summary_short": self.summary_short,
                "market_summary_long": self.summary_long,
                "market_pressure_bias": self.pressure_bias,
            }
        )
        out.update(self.breakout.as_dict())
        out.update(self.status.as_dict('market_state'))
        return out


@dataclass(frozen=True)
class MarketStateInputs:
    raw: Dict[str, Any] = field(default_factory=dict)
    previous_state: Dict[str, Any] = field(default_factory=dict)
    bar: BarSnapshot = field(default_factory=BarSnapshot)
    indicators: IndicatorSnapshot = field(default_factory=IndicatorSnapshot)
    features: MarketFeatures = field(default_factory=MarketFeatures)

    @classmethod
    def from_payloads(cls, snapshot: Any, prev_state: Any = None) -> "MarketStateInputs":
        raw = coerce_mapping(snapshot)
        prev = coerce_mapping(prev_state)
        return cls(
            raw=raw,
            previous_state=prev,
            bar=BarSnapshot.from_mapping(raw),
            indicators=IndicatorSnapshot.from_mapping(raw),
            features=MarketFeatures.from_mapping(raw),
        )

    def as_dict(self) -> Dict[str, Any]:
        return dict(self.raw)
