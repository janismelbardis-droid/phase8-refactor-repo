from __future__ import annotations

"""Research validation helpers for market-state evaluation.

These utilities are intentionally side-effect free so they can be used from
scripts, tests, or the UI/reporting layer without changing trading behavior.
"""

from dataclasses import asdict, dataclass
import inspect
from statistics import median
from typing import Any, Callable, Dict, Iterable, List, Mapping, MutableMapping, Optional, Sequence, Tuple

from ..domain.market_state import coerce_mapping, coerce_market_state_thresholds

DEFAULT_HORIZONS: Tuple[int, ...] = (1, 3, 5)


@dataclass(frozen=True)
class HorizonStats:
    count: int = 0
    hit_rate: float = 0.0
    mean_return: float = 0.0
    median_return: float = 0.0
    variance: float = 0.0
    avg_mfe: float = 0.0
    avg_mae: float = 0.0


@dataclass(frozen=True)
class StateStats:
    state: str
    sample_count: int
    horizons: Dict[str, Dict[str, float]]


@dataclass(frozen=True)
class PersistenceStats:
    state: str
    runs: int
    avg_run_length: float
    median_run_length: float
    max_run_length: int


@dataclass(frozen=True)
class ValidationReport:
    row_count: int
    state_statistics: Dict[str, Dict[str, Any]]
    transition_matrix: Dict[str, Dict[str, Any]]
    persistence: Dict[str, Dict[str, Any]]
    threshold_sensitivity: Dict[str, Dict[str, Any]]
    baseline_comparison: Dict[str, Dict[str, Any]]
    asset_timeframe_breakdown: Dict[str, Dict[str, Any]]

    def as_dict(self) -> Dict[str, Any]:
        return asdict(self)


def _coerce_float(value: Any, default: Optional[float] = None) -> Optional[float]:
    try:
        if value is None:
            return default
        out = float(value)
        if out == out and out not in (float("inf"), float("-inf")):
            return out
    except Exception:
        pass
    return default


def _coerce_text(value: Any, default: str = "") -> str:
    try:
        return str(value if value is not None else default)
    except Exception:
        return default


def _normalize_state(value: Any) -> str:
    text = _coerce_text(value, "").strip().upper()
    return text or "UNKNOWN"


def _normalize_timeframe(row: Mapping[str, Any]) -> str:
    return _coerce_text(row.get("timeframe") or row.get("tf") or "", "").strip() or "unknown"


def _normalize_symbol(row: Mapping[str, Any]) -> str:
    return _coerce_text(row.get("symbol") or "", "").strip() or "unknown"


def _extract_close(row: Mapping[str, Any], price_key: str = "close") -> Optional[float]:
    return _coerce_float(row.get(price_key), _coerce_float(row.get("price"), None))


def _extract_state(row: Mapping[str, Any], state_key: str = "market_state") -> str:
    return _normalize_state(row.get(state_key))


def _extract_direction_sign(row: Mapping[str, Any]) -> int:
    bias = _normalize_state(row.get("market_bias"))
    state = _extract_state(row)
    if bias == "LONG" or state.startswith("BULL"):
        return 1
    if bias == "SHORT" or state.startswith("BEAR"):
        return -1
    return 0


def _baseline_direction_sign(row: Mapping[str, Any]) -> int:
    score = 0
    for key, pos, neg in (
        ("vidya_state", "BUY", "SELL"),
        ("frama_state", "BUY", "SELL"),
        ("range_filter_state", "BUY", "SELL"),
        ("ms", "GREEN", "RED"),
        ("angle", "GREEN", "RED"),
        ("atr_angle", "GREEN", "RED"),
    ):
        value = _normalize_state(row.get(key))
        if value == pos:
            score += 1
        elif value == neg:
            score -= 1
    if score > 0:
        return 1
    if score < 0:
        return -1
    return 0


def prepare_validation_rows(
    rows: Iterable[Mapping[str, Any]],
    *,
    compute_fn: Optional[Callable[..., Mapping[str, Any]]] = None,
    thresholds: Optional[Mapping[str, Any]] = None,
) -> List[Dict[str, Any]]:
    """Normalize rows for validation.

    If rows already include market-state outputs, they are preserved. Otherwise,
    when ``compute_fn`` is provided, the rows are replayed through the compute
    function so validation can run on raw snapshots.
    """
    normalized_rows = [coerce_mapping(row) for row in rows]
    if not normalized_rows:
        return []
    if compute_fn is None:
        return [dict(row) for row in normalized_rows]

    threshold_obj = coerce_market_state_thresholds(dict(thresholds or {}))
    prepared: List[Dict[str, Any]] = []
    prev_state: Optional[Dict[str, Any]] = None
    signature = inspect.signature(compute_fn)
    accepts_thresholds = "thresholds" in signature.parameters
    for row in normalized_rows:
        if row.get("market_state"):
            current = dict(row)
        else:
            if accepts_thresholds:
                current = dict(coerce_mapping(compute_fn(dict(row), prev_state, thresholds=threshold_obj)))
            else:
                current = dict(coerce_mapping(compute_fn(dict(row), prev_state)))
            for key, value in row.items():
                current.setdefault(key, value)
        prepared.append(current)
        prev_state = current
    return prepared


def compute_forward_return_series(
    rows: Sequence[Mapping[str, Any]],
    *,
    horizons: Sequence[int] = DEFAULT_HORIZONS,
    price_key: str = "close",
) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    for index, raw_row in enumerate(rows):
        row = dict(coerce_mapping(raw_row))
        close0 = _extract_close(row, price_key=price_key)
        horizons_payload: Dict[str, Optional[float]] = {}
        mfe_payload: Dict[str, Optional[float]] = {}
        mae_payload: Dict[str, Optional[float]] = {}
        if close0 is None or close0 == 0:
            for horizon in horizons:
                key = str(int(horizon))
                horizons_payload[key] = None
                mfe_payload[key] = None
                mae_payload[key] = None
            row["forward_returns"] = horizons_payload
            row["forward_mfe"] = mfe_payload
            row["forward_mae"] = mae_payload
            out.append(row)
            continue

        for horizon in horizons:
            future_rows = rows[index + 1 : index + 1 + int(horizon)]
            key = str(int(horizon))
            if len(future_rows) < int(horizon):
                horizons_payload[key] = None
                mfe_payload[key] = None
                mae_payload[key] = None
                continue
            future_closes = [_extract_close(coerce_mapping(r), price_key=price_key) for r in future_rows]
            if any(v is None for v in future_closes):
                horizons_payload[key] = None
                mfe_payload[key] = None
                mae_payload[key] = None
                continue
            terminal = future_closes[-1]
            assert terminal is not None
            horizons_payload[key] = (terminal / close0) - 1.0
            max_close = max(v for v in future_closes if v is not None)
            min_close = min(v for v in future_closes if v is not None)
            mfe_payload[key] = (max_close / close0) - 1.0
            mae_payload[key] = (min_close / close0) - 1.0
        row["forward_returns"] = horizons_payload
        row["forward_mfe"] = mfe_payload
        row["forward_mae"] = mae_payload
        out.append(row)
    return out


def _summarize_horizon(returns: Sequence[float], mfes: Sequence[float], maes: Sequence[float]) -> HorizonStats:
    count = len(returns)
    if count == 0:
        return HorizonStats()
    mean_return = sum(returns) / count
    med_return = float(median(returns))
    variance = sum((value - mean_return) ** 2 for value in returns) / count
    hit_rate = sum(1 for value in returns if value > 0.0) / count
    avg_mfe = (sum(mfes) / len(mfes)) if mfes else 0.0
    avg_mae = (sum(maes) / len(maes)) if maes else 0.0
    return HorizonStats(
        count=count,
        hit_rate=float(hit_rate),
        mean_return=float(mean_return),
        median_return=float(med_return),
        variance=float(variance),
        avg_mfe=float(avg_mfe),
        avg_mae=float(avg_mae),
    )


def build_state_statistics(
    rows: Sequence[Mapping[str, Any]],
    *,
    horizons: Sequence[int] = DEFAULT_HORIZONS,
    state_key: str = "market_state",
) -> Dict[str, Dict[str, Any]]:
    prepared = compute_forward_return_series(rows, horizons=horizons)
    grouped: Dict[str, Dict[str, List[float]]] = {}
    counts: Dict[str, int] = {}
    for row in prepared:
        state = _extract_state(row, state_key=state_key)
        counts[state] = counts.get(state, 0) + 1
        grouped.setdefault(state, {})
        for horizon in horizons:
            key = str(int(horizon))
            grouped[state].setdefault(f"ret_{key}", [])
            grouped[state].setdefault(f"mfe_{key}", [])
            grouped[state].setdefault(f"mae_{key}", [])
            ret = coerce_mapping(row.get("forward_returns")).get(key)
            mfe = coerce_mapping(row.get("forward_mfe")).get(key)
            mae = coerce_mapping(row.get("forward_mae")).get(key)
            if ret is not None:
                grouped[state][f"ret_{key}"].append(float(ret))
            if mfe is not None:
                grouped[state][f"mfe_{key}"].append(float(mfe))
            if mae is not None:
                grouped[state][f"mae_{key}"].append(float(mae))

    report: Dict[str, Dict[str, Any]] = {}
    for state, buckets in grouped.items():
        horizons_report: Dict[str, Dict[str, float]] = {}
        for horizon in horizons:
            key = str(int(horizon))
            stats = _summarize_horizon(
                buckets.get(f"ret_{key}", []),
                buckets.get(f"mfe_{key}", []),
                buckets.get(f"mae_{key}", []),
            )
            horizons_report[key] = asdict(stats)
        report[state] = asdict(StateStats(state=state, sample_count=counts.get(state, 0), horizons=horizons_report))
    return report


def build_transition_matrix(rows: Sequence[Mapping[str, Any]], *, state_key: str = "market_state") -> Dict[str, Dict[str, Any]]:
    prepared = [coerce_mapping(row) for row in rows]
    counts: Dict[str, Dict[str, int]] = {}
    for current, nxt in zip(prepared, prepared[1:]):
        src = _extract_state(current, state_key=state_key)
        dst = _extract_state(nxt, state_key=state_key)
        counts.setdefault(src, {})
        counts[src][dst] = counts[src].get(dst, 0) + 1

    report: Dict[str, Dict[str, Any]] = {}
    for src, destinations in counts.items():
        total = sum(destinations.values())
        probabilities = {dst: (count / total if total else 0.0) for dst, count in destinations.items()}
        report[src] = {
            "count": total,
            "transitions": dict(destinations),
            "probabilities": probabilities,
        }
    return report


def build_persistence_analysis(rows: Sequence[Mapping[str, Any]], *, state_key: str = "market_state") -> Dict[str, Dict[str, Any]]:
    prepared = [coerce_mapping(row) for row in rows]
    if not prepared:
        return {}
    runs: Dict[str, List[int]] = {}
    total_flips = 0
    current_state = _extract_state(prepared[0], state_key=state_key)
    current_length = 1
    for row in prepared[1:]:
        state = _extract_state(row, state_key=state_key)
        if state == current_state:
            current_length += 1
            continue
        runs.setdefault(current_state, []).append(current_length)
        current_state = state
        current_length = 1
        total_flips += 1
    runs.setdefault(current_state, []).append(current_length)

    total_rows = len(prepared)
    flip_rate = (total_flips / max(1, total_rows - 1)) if total_rows > 1 else 0.0
    report: Dict[str, Dict[str, Any]] = {}
    for state, lengths in runs.items():
        stats = PersistenceStats(
            state=state,
            runs=len(lengths),
            avg_run_length=float(sum(lengths) / len(lengths)),
            median_run_length=float(median(lengths)),
            max_run_length=max(lengths),
        )
        payload = asdict(stats)
        payload["flip_rate_global"] = float(flip_rate)
        report[state] = payload
    return report


def build_threshold_sensitivity_report(
    rows: Sequence[Mapping[str, Any]],
    *,
    compute_fn: Optional[Callable[..., Mapping[str, Any]]] = None,
    threshold_sets: Optional[Mapping[str, Mapping[str, Any]]] = None,
) -> Dict[str, Dict[str, Any]]:
    scenarios = dict(threshold_sets or {})
    if not scenarios or compute_fn is None:
        return {}

    report: Dict[str, Dict[str, Any]] = {}
    for name, threshold_values in scenarios.items():
        prepared = prepare_validation_rows(rows, compute_fn=compute_fn, thresholds=threshold_values)
        states = [_extract_state(row) for row in prepared]
        distribution: Dict[str, int] = {}
        for state in states:
            distribution[state] = distribution.get(state, 0) + 1
        flip_count = sum(1 for left, right in zip(states, states[1:]) if left != right)
        report[str(name)] = {
            "row_count": len(prepared),
            "state_distribution": distribution,
            "flip_rate": (flip_count / max(1, len(states) - 1)) if len(states) > 1 else 0.0,
            "thresholds": dict(threshold_values),
        }
    return report


def _summarize_signed_returns(signals: Sequence[int], returns: Sequence[Optional[float]]) -> Dict[str, float]:
    pairs = [(int(signal), ret) for signal, ret in zip(signals, returns) if ret is not None and int(signal) != 0]
    if not pairs:
        return {"coverage": 0.0, "sample_count": 0, "mean_signed_return": 0.0, "hit_rate": 0.0}
    signed_returns = [signal * float(ret) for signal, ret in pairs]
    sample_count = len(signed_returns)
    hit_rate = sum(1 for value in signed_returns if value > 0.0) / sample_count
    return {
        "coverage": float(sample_count / max(1, len(returns))),
        "sample_count": sample_count,
        "mean_signed_return": float(sum(signed_returns) / sample_count),
        "hit_rate": float(hit_rate),
    }


def build_baseline_comparison(
    rows: Sequence[Mapping[str, Any]],
    *,
    horizons: Sequence[int] = DEFAULT_HORIZONS,
) -> Dict[str, Dict[str, Any]]:
    prepared = compute_forward_return_series(rows, horizons=horizons)
    market_signs = [_extract_direction_sign(row) for row in prepared]
    baseline_signs = [_baseline_direction_sign(row) for row in prepared]
    report: Dict[str, Dict[str, Any]] = {}
    for horizon in horizons:
        key = str(int(horizon))
        returns = [coerce_mapping(row.get("forward_returns")).get(key) for row in prepared]
        report[key] = {
            "market_model": _summarize_signed_returns(market_signs, returns),
            "baseline_model": _summarize_signed_returns(baseline_signs, returns),
        }
    return report


def build_asset_timeframe_breakdown(
    rows: Sequence[Mapping[str, Any]],
    *,
    state_key: str = "market_state",
) -> Dict[str, Dict[str, Any]]:
    breakdown: Dict[str, Dict[str, Any]] = {}
    for row in rows:
        payload = coerce_mapping(row)
        symbol = _normalize_symbol(payload)
        timeframe = _normalize_timeframe(payload)
        bucket_key = f"{symbol}|{timeframe}"
        bucket = breakdown.setdefault(bucket_key, {"symbol": symbol, "timeframe": timeframe, "row_count": 0, "state_distribution": {}})
        bucket["row_count"] += 1
        state = _extract_state(payload, state_key=state_key)
        bucket["state_distribution"][state] = bucket["state_distribution"].get(state, 0) + 1
    return breakdown


def build_validation_report(
    rows: Sequence[Mapping[str, Any]],
    *,
    compute_fn: Optional[Callable[..., Mapping[str, Any]]] = None,
    thresholds: Optional[Mapping[str, Any]] = None,
    threshold_sets: Optional[Mapping[str, Mapping[str, Any]]] = None,
    horizons: Sequence[int] = DEFAULT_HORIZONS,
) -> ValidationReport:
    prepared = prepare_validation_rows(rows, compute_fn=compute_fn, thresholds=thresholds)
    return ValidationReport(
        row_count=len(prepared),
        state_statistics=build_state_statistics(prepared, horizons=horizons),
        transition_matrix=build_transition_matrix(prepared),
        persistence=build_persistence_analysis(prepared),
        threshold_sensitivity=build_threshold_sensitivity_report(prepared if compute_fn is None else rows, compute_fn=compute_fn, threshold_sets=threshold_sets),
        baseline_comparison=build_baseline_comparison(prepared, horizons=horizons),
        asset_timeframe_breakdown=build_asset_timeframe_breakdown(prepared),
    )
