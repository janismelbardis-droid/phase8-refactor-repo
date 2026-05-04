from __future__ import annotations

import argparse
import json
import math
import sys
import warnings
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence, Tuple

import numpy as np
import pandas as pd

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))
WORKSPACE_ROOT = REPO_ROOT.parents[1] if len(REPO_ROOT.parents) > 1 else REPO_ROOT
DEFAULT_CASEBOOK_REPORT = (
    WORKSPACE_ROOT
    / "research_exports"
    / "trade_entry_casebook"
    / "preset_drafts"
    / "backtest_case_period_0001_0005.json"
)

from app.backtest_frames import make_streams_htf_closed_only
from app.data_binance import binance_fapi_server_time_utc, fetch_klines_1m_futures
from app.ema_settings import normalize_ema_settings
from app.indicators_streaming import simulate_multitf_indicators
from app.strategy_requirements import normalize_required_fields
from app.utils_time import parse_utc, required_warmup_minutes


@dataclass(frozen=True)
class ExitProfile:
    name: str
    stop_pct: float
    take_pct: float
    max_hold_min: int


def _json_safe(value: Any) -> Any:
    if isinstance(value, pd.Timestamp):
        return value.isoformat()
    if isinstance(value, float):
        if math.isnan(value):
            return None
        if math.isinf(value):
            return "inf" if value > 0 else "-inf"
    if isinstance(value, dict):
        return {str(k): _json_safe(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [_json_safe(v) for v in value]
    return value


def _default_exit_profiles() -> List[ExitProfile]:
    return [
        ExitProfile("scalp_0p12_0p20_60m", 0.12, 0.20, 60),
        ExitProfile("scalp_0p15_0p25_90m", 0.15, 0.25, 90),
        ExitProfile("scalp_0p20_0p35_120m", 0.20, 0.35, 120),
        ExitProfile("day_0p25_0p45_240m", 0.25, 0.45, 240),
        ExitProfile("day_0p35_0p70_480m", 0.35, 0.70, 480),
        ExitProfile("runner_0p50_1p00_720m", 0.50, 1.00, 720),
    ]


def _to_utc(value: Any) -> Optional[pd.Timestamp]:
    if value is None:
        return None
    try:
        ts = pd.to_datetime(value, utc=True, errors="coerce")
    except Exception:
        return None
    if pd.isna(ts):
        return None
    return pd.Timestamp(ts)


def _casebook_report_path(raw: str) -> Optional[Path]:
    text = str(raw or "auto").strip()
    if text.lower() in ("", "auto"):
        return DEFAULT_CASEBOOK_REPORT if DEFAULT_CASEBOOK_REPORT.exists() else None
    if text.lower() in ("none", "off", "false", "0"):
        return None
    return Path(text).expanduser().resolve()


def _load_casebook_cases(raw_path: str) -> Tuple[Optional[Path], List[Dict[str, Any]]]:
    path = _casebook_report_path(raw_path)
    if path is None or not path.exists():
        return path, []
    payload = json.loads(path.read_text(encoding="utf-8"))
    cases: List[Dict[str, Any]] = []
    for item in payload.get("cases") or []:
        if not isinstance(item, Mapping):
            continue
        t_rf = _to_utc(item.get("t_rf_utc"))
        t0 = _to_utc(item.get("t0_utc"))
        window_end = _to_utc(item.get("window_end_utc"))
        if t_rf is None or t0 is None or window_end is None:
            continue
        cases.append({
            "case_id": str(item.get("case_id") or ""),
            "preset_family": str(item.get("preset_family") or ""),
            "t_rf_utc": t_rf,
            "t0_utc": t0,
            "window_end_utc": window_end,
        })
    cases.sort(key=lambda row: row["t_rf_utc"])
    return path, cases


def _casebook_match(cases: Sequence[Mapping[str, Any]], *, signal_time: pd.Timestamp, entry_time: pd.Timestamp) -> Dict[str, Any]:
    if not cases:
        return {
            "casebook_match_count": 0,
            "casebook_match_ids": "",
            "casebook_match_families": "",
            "casebook_nearest_id": "",
            "casebook_nearest_family": "",
            "casebook_nearest_entry_diff_min": float("nan"),
        }
    matches: List[Mapping[str, Any]] = []
    for case in cases:
        t_rf = case.get("t_rf_utc")
        window_end = case.get("window_end_utc")
        if not isinstance(t_rf, pd.Timestamp) or not isinstance(window_end, pd.Timestamp):
            continue
        if (t_rf <= signal_time <= window_end) or (t_rf <= entry_time <= window_end):
            matches.append(case)

    def _diff_min(case: Mapping[str, Any]) -> float:
        t0 = case.get("t0_utc")
        if not isinstance(t0, pd.Timestamp):
            return float("inf")
        return abs((entry_time - t0).total_seconds()) / 60.0

    nearest = min(cases, key=_diff_min)
    families = sorted({str(c.get("preset_family") or "") for c in matches if c.get("preset_family")})
    ids = [str(c.get("case_id") or "") for c in matches if c.get("case_id")]
    return {
        "casebook_match_count": int(len(matches)),
        "casebook_match_ids": "|".join(ids),
        "casebook_match_families": "|".join(families),
        "casebook_nearest_id": str(nearest.get("case_id") or ""),
        "casebook_nearest_family": str(nearest.get("preset_family") or ""),
        "casebook_nearest_entry_diff_min": float(_diff_min(nearest)),
    }


def _casebook_windows(cases: Sequence[Mapping[str, Any]]) -> List[Tuple[pd.Timestamp, pd.Timestamp, str]]:
    windows: List[Tuple[pd.Timestamp, pd.Timestamp, str]] = []
    for case in cases:
        t_rf = case.get("t_rf_utc")
        window_end = case.get("window_end_utc")
        case_id = str(case.get("case_id") or "")
        if isinstance(t_rf, pd.Timestamp) and isinstance(window_end, pd.Timestamp):
            windows.append((t_rf, window_end, f"casebook:{case_id}"))
    return windows


def _window_labels(ts: pd.Timestamp, windows: Sequence[Tuple[pd.Timestamp, pd.Timestamp, str]]) -> List[str]:
    return [label for start, end, label in windows if start <= ts <= end]


def _write_casebook_cases(path: Path, cases: Sequence[Mapping[str, Any]]) -> None:
    rows = []
    for case in cases:
        rows.append({
            "case_id": case.get("case_id", ""),
            "preset_family": case.get("preset_family", ""),
            "t_rf_utc": str(case.get("t_rf_utc", "")),
            "t0_utc": str(case.get("t0_utc", "")),
            "window_end_utc": str(case.get("window_end_utc", "")),
        })
    pd.DataFrame(rows).to_csv(path, index=False)


def _parse_exit_profiles(raw: str) -> List[ExitProfile]:
    text = str(raw or "").strip()
    if not text:
        return _default_exit_profiles()
    out: List[ExitProfile] = []
    for chunk in text.split(","):
        item = chunk.strip()
        if not item:
            continue
        parts = [p.strip() for p in item.split(":")]
        if len(parts) != 3:
            raise ValueError(f"Exit profile must be stop:take:max_hold_min, got {item!r}")
        stop, take, hold = float(parts[0]), float(parts[1]), int(parts[2])
        out.append(ExitProfile(f"custom_{stop:g}_{take:g}_{hold}m".replace(".", "p"), stop, take, hold))
    return out or _default_exit_profiles()


def _bucket(value: Any, cuts: Sequence[float], labels: Sequence[str], default: str = "NA") -> str:
    try:
        x = float(value)
    except Exception:
        return default
    if not math.isfinite(x):
        return default
    for cut, label in zip(cuts, labels):
        if x < float(cut):
            return str(label)
    return str(labels[-1]) if labels else default


def _adx_bucket(value: Any) -> str:
    return _bucket(value, [15, 20, 25, 30], ["<15", "15-20", "20-25", "25-30", ">=30"])


def _rsi_bucket(value: Any) -> str:
    return _bucket(value, [30, 40, 50, 60, 70], ["<30", "30-40", "40-50", "50-60", "60-70", ">=70"])


def _state_dir(value: Any) -> str:
    text = str(value or "").strip().upper()
    if not text:
        return "NA"
    if text.startswith("BUY") or text.startswith("BULL"):
        return "BUY"
    if text.startswith("SELL") or text.startswith("BEAR"):
        return "SELL"
    if text in ("NEUTRAL", "BALANCE", "FLAT", "NONE", "NA", "NAN"):
        return "NEUTRAL"
    return text


def _side_dir(side: str) -> str:
    return "BUY" if str(side).upper() == "LONG" else "SELL"


def _align_to_side(side: str, value: Any) -> str:
    direction = _state_dir(value)
    if direction == "NA":
        return "NA"
    if direction == "NEUTRAL":
        return "NEUTRAL"
    expected = _side_dir(side)
    if direction == expected:
        return "ALIGNED"
    if direction in ("BUY", "SELL"):
        return "OPPOSED"
    return direction


def _is_neutral_range(state: Any, phase: Any) -> bool:
    return _state_dir(state) == "NEUTRAL" or _state_dir(phase) == "NEUTRAL"


def _num(row: Mapping[str, Any], field: str) -> float:
    try:
        return float(row.get(field))
    except Exception:
        return float("nan")


def _strv(row: Mapping[str, Any], field: str) -> str:
    value = row.get(field)
    if value is None:
        return ""
    try:
        if pd.isna(value):
            return ""
    except Exception:
        pass
    return str(value)


def _boolv(row: Mapping[str, Any], field: str) -> bool:
    value = row.get(field)
    if value is None:
        return False
    try:
        if pd.isna(value):
            return False
    except Exception:
        pass
    return bool(value)


def _side_return_pct(side: str, entry: float, exit_price: float) -> float:
    if not math.isfinite(entry) or entry <= 0 or not math.isfinite(exit_price):
        return float("nan")
    if side == "LONG":
        return ((exit_price - entry) / entry) * 100.0
    return ((entry - exit_price) / entry) * 100.0


def _apply_entry_slippage(side: str, price: float, slippage_bps: float) -> float:
    slip = float(slippage_bps) / 10000.0
    return float(price) * (1.0 + slip if side == "LONG" else 1.0 - slip)


def _apply_exit_slippage(side: str, raw_exit: float, slippage_bps: float) -> float:
    slip = float(slippage_bps) / 10000.0
    return float(raw_exit) * (1.0 - slip if side == "LONG" else 1.0 + slip)


def _net_pct(side: str, entry_fill: float, raw_exit: float, *, fee_pct: float, slippage_bps: float) -> float:
    exit_fill = _apply_exit_slippage(side, raw_exit, slippage_bps)
    gross_pct = _side_return_pct(side, entry_fill, exit_fill)
    if not math.isfinite(gross_pct):
        return float("nan")
    return gross_pct - (2.0 * float(fee_pct))


def _simulate_profile(
    *,
    side: str,
    entry_idx: int,
    entry_fill: float,
    raw: pd.DataFrame,
    profile: ExitProfile,
    fee_pct: float,
    slippage_bps: float,
) -> Dict[str, Any]:
    stop_frac = float(profile.stop_pct) / 100.0
    take_frac = float(profile.take_pct) / 100.0
    if side == "LONG":
        stop_raw = entry_fill * (1.0 - stop_frac)
        take_raw = entry_fill * (1.0 + take_frac)
    else:
        stop_raw = entry_fill * (1.0 + stop_frac)
        take_raw = entry_fill * (1.0 - take_frac)

    end_idx = min(len(raw) - 1, int(entry_idx) + max(1, int(profile.max_hold_min)))
    if entry_idx > end_idx:
        return {
            "exit_reason": "NO_DATA",
            "exit_time": "",
            "exit_price": float("nan"),
            "net_pct": float("nan"),
            "hold_min": 0,
            "ambiguous": False,
        }

    exit_idx = end_idx
    exit_reason = "TIME"
    raw_exit = float(raw.iloc[end_idx]["close"])
    ambiguous = False

    for j in range(int(entry_idx), end_idx + 1):
        high = float(raw.iloc[j]["high"])
        low = float(raw.iloc[j]["low"])
        if side == "LONG":
            hit_stop = low <= stop_raw
            hit_take = high >= take_raw
        else:
            hit_stop = high >= stop_raw
            hit_take = low <= take_raw
        if hit_stop or hit_take:
            exit_idx = j
            ambiguous = bool(hit_stop and hit_take)
            # Conservative intrabar ordering: when both touch on one 1m candle, count stop first.
            if hit_stop:
                exit_reason = "STOP"
                raw_exit = stop_raw
            else:
                exit_reason = "TAKE"
                raw_exit = take_raw
            break

    exit_time = raw.index[int(exit_idx)]
    return {
        "exit_reason": exit_reason,
        "exit_time": str(exit_time),
        "exit_price": float(_apply_exit_slippage(side, raw_exit, slippage_bps)),
        "raw_exit_price": float(raw_exit),
        "net_pct": float(_net_pct(side, entry_fill, raw_exit, fee_pct=fee_pct, slippage_bps=slippage_bps)),
        "hold_min": int(max(0, int(exit_idx) - int(entry_idx) + 1)),
        "ambiguous": bool(ambiguous),
    }


def _forward_stats(raw: pd.DataFrame, *, side: str, entry_idx: int, entry_fill: float, horizons: Sequence[int], fee_pct: float, slippage_bps: float) -> Dict[str, Any]:
    out: Dict[str, Any] = {}
    for horizon in horizons:
        end_idx = min(len(raw) - 1, int(entry_idx) + max(1, int(horizon)))
        if entry_idx > end_idx:
            out[f"mfe_{horizon}m_pct"] = float("nan")
            out[f"mae_{horizon}m_pct"] = float("nan")
            out[f"close_{horizon}m_net_pct"] = float("nan")
            continue
        chunk = raw.iloc[int(entry_idx): end_idx + 1]
        if side == "LONG":
            best_raw = float(chunk["high"].max())
            worst_raw = float(chunk["low"].min())
        else:
            best_raw = float(chunk["low"].min())
            worst_raw = float(chunk["high"].max())
        close_raw = float(raw.iloc[end_idx]["close"])
        out[f"mfe_{horizon}m_pct"] = float(_net_pct(side, entry_fill, best_raw, fee_pct=fee_pct, slippage_bps=slippage_bps))
        out[f"mae_{horizon}m_pct"] = float(_net_pct(side, entry_fill, worst_raw, fee_pct=0.0, slippage_bps=0.0))
        out[f"close_{horizon}m_net_pct"] = float(_net_pct(side, entry_fill, close_raw, fee_pct=fee_pct, slippage_bps=slippage_bps))
    return out


def _aggregate(rows: Sequence[Mapping[str, Any]]) -> Dict[str, Any]:
    vals = [float(r.get("net_pct", float("nan"))) for r in rows]
    vals = [v for v in vals if math.isfinite(v)]
    wins = [v for v in vals if v > 0]
    losses = [v for v in vals if v <= 0]
    gross_profit = sum(wins)
    gross_loss = abs(sum(losses))
    return {
        "count": int(len(vals)),
        "wins": int(len(wins)),
        "losses": int(len(losses)),
        "win_rate_pct": (len(wins) / len(vals) * 100.0) if vals else 0.0,
        "net_pct_sum": float(sum(vals)),
        "avg_net_pct": float(sum(vals) / len(vals)) if vals else 0.0,
        "profit_factor": float(gross_profit / gross_loss) if gross_loss > 0 else (999.0 if gross_profit > 0 else 0.0),
    }


def _cluster_score(agg: Mapping[str, Any]) -> float:
    count = int(agg.get("count", 0) or 0)
    return (
        float(agg.get("net_pct_sum", 0.0) or 0.0)
        + 0.04 * min(float(agg.get("profit_factor", 0.0) or 0.0), 5.0)
        + 0.006 * float(agg.get("win_rate_pct", 0.0) or 0.0)
        + 0.015 * min(count, 20)
    )


def _make_clusters(outcomes: pd.DataFrame, *, min_count: int) -> pd.DataFrame:
    if outcomes.empty:
        return pd.DataFrame()
    specs = [
        ("by_trigger", ["exit_profile", "trigger_type", "side"]),
        ("by_phase", ["exit_profile", "trigger_type", "side", "range_phase"]),
        ("by_pullback_kind", ["exit_profile", "trigger_type", "side", "range_pullback_kind"]),
        ("by_pullback_vidya", ["exit_profile", "side", "range_pullback_kind", "vidya_align_1m", "vidya_align_5m"]),
        ("by_ema", ["exit_profile", "trigger_type", "side", "ema_stack_1m", "ema_stack_5m"]),
        ("by_phase_ema", ["exit_profile", "trigger_type", "side", "range_phase", "ema_stack_5m"]),
        ("by_range_vidya_ema", ["exit_profile", "trigger_type", "side", "range_phase", "range_pullback_kind", "vidya_align_1m", "ema_align_5m"]),
        ("by_vd", ["exit_profile", "trigger_type", "side", "vd_align_1m", "taker_bias_state_1m"]),
        ("by_adx", ["exit_profile", "trigger_type", "side", "adx_bucket_1m", "adx_bucket_5m"]),
        ("by_hour", ["exit_profile", "trigger_type", "side", "hour_utc"]),
        ("by_rsi", ["exit_profile", "trigger_type", "side", "rsi_bucket_1m"]),
        ("by_casebook_family", ["exit_profile", "trigger_type", "side", "casebook_match_families", "range_pullback_kind"]),
        ("by_full_context", ["exit_profile", "trigger_type", "side", "range_phase", "ema_stack_5m", "adx_bucket_1m", "hour_utc"]),
    ]
    rows: List[Dict[str, Any]] = []
    for kind, cols in specs:
        if not all(c in outcomes.columns for c in cols):
            continue
        grouped = outcomes.groupby(cols, dropna=False)
        for key, group in grouped:
            records = group.to_dict("records")
            agg = _aggregate(records)
            if int(agg["count"]) < int(min_count):
                continue
            rec = {"cluster_kind": kind, **agg, "score": _cluster_score(agg)}
            if not isinstance(key, tuple):
                key = (key,)
            for col, value in zip(cols, key):
                rec[col] = value
            rows.append(rec)
    if not rows:
        return pd.DataFrame()
    df = pd.DataFrame(rows)
    return df.sort_values(["score", "net_pct_sum", "count"], ascending=[False, False, False]).reset_index(drop=True)


def _range_triggers(stream_1m: pd.DataFrame, mode: str) -> List[Tuple[int, str, str]]:
    mode_n = str(mode or "all").strip().lower()
    out: List[Tuple[int, str, str]] = []
    buy = stream_1m.get("range_filter_buy")
    sell = stream_1m.get("range_filter_sell")
    if buy is not None and mode_n in ("events", "all"):
        for idx in np.flatnonzero(pd.Series(buy).fillna(False).astype(bool).to_numpy()):
            out.append((int(idx), "LONG", "range_filter_buy"))
    if sell is not None and mode_n in ("events", "all"):
        for idx in np.flatnonzero(pd.Series(sell).fillna(False).astype(bool).to_numpy()):
            out.append((int(idx), "SHORT", "range_filter_sell"))
    if mode_n in ("conditions", "all"):
        for field, side, name in [
            ("range_filter_long_cond", "LONG", "range_filter_long_cond_edge"),
            ("range_filter_short_cond", "SHORT", "range_filter_short_cond_edge"),
        ]:
            series = stream_1m.get(field)
            if series is None:
                continue
            arr = pd.Series(series).fillna(False).astype(bool).to_numpy()
            prev = np.r_[False, arr[:-1]]
            for idx in np.flatnonzero(arr & (~prev)):
                out.append((int(idx), side, name))
    out.sort(key=lambda item: (item[0], item[1], item[2]))
    return out


def _range_pullback_context(stream_1m: pd.DataFrame, signal_idx: int, side: str, *, lookback: int = 180) -> Dict[str, Any]:
    idx = int(signal_idx)
    start = max(0, idx - max(1, int(lookback)))
    states = stream_1m.get("range_filter_state")
    phases = stream_1m.get("range_filter_phase")
    if states is None:
        states = pd.Series([""] * len(stream_1m), index=stream_1m.index)
    if phases is None:
        phases = pd.Series([""] * len(stream_1m), index=stream_1m.index)
    states_s = pd.Series(states).astype(object).reset_index(drop=True)
    phases_s = pd.Series(phases).astype(object).reset_index(drop=True)

    prev_idx = idx - 1
    prev_state = str(states_s.iloc[prev_idx] if prev_idx >= 0 else "" or "").upper()
    prev_phase = str(phases_s.iloc[prev_idx] if prev_idx >= 0 else "" or "").upper()
    side_direction = _side_dir(side)
    opposite_direction = "SELL" if side_direction == "BUY" else "BUY"

    neutral_run = 0
    cursor = prev_idx
    while cursor >= 0 and _is_neutral_range(states_s.iloc[cursor], phases_s.iloc[cursor]):
        neutral_run += 1
        cursor -= 1

    pre_neutral_state = str(states_s.iloc[cursor] if cursor >= 0 else "" or "").upper()
    pre_neutral_phase = str(phases_s.iloc[cursor] if cursor >= 0 else "" or "").upper()
    pre_neutral_dir = _state_dir(pre_neutral_phase if pre_neutral_phase else pre_neutral_state)

    neutral_flags = [
        bool(_is_neutral_range(states_s.iloc[j], phases_s.iloc[j]))
        for j in range(start, idx)
    ]
    recent_neutral_indices = [start + pos for pos, flag in enumerate(neutral_flags) if flag]
    last_neutral_idx = recent_neutral_indices[-1] if recent_neutral_indices else None

    def _seen_neutral(window: int) -> bool:
        lo = max(0, idx - int(window))
        return any(
            bool(_is_neutral_range(states_s.iloc[j], phases_s.iloc[j]))
            for j in range(lo, idx)
        )

    same_side_indices = [
        j for j in range(start, idx)
        if _state_dir(phases_s.iloc[j] if str(phases_s.iloc[j] or "").upper() else states_s.iloc[j]) == side_direction
    ]
    opposite_indices = [
        j for j in range(start, idx)
        if _state_dir(phases_s.iloc[j] if str(phases_s.iloc[j] or "").upper() else states_s.iloc[j]) == opposite_direction
    ]

    reclaim_from_neutral = neutral_run > 0
    same_side_neutral_reclaim = bool(reclaim_from_neutral and pre_neutral_dir == side_direction)
    opposite_after_neutral = bool(reclaim_from_neutral and pre_neutral_dir == opposite_direction)
    neutral_recent_60 = _seen_neutral(60)
    if same_side_neutral_reclaim:
        kind = "SAME_SIDE_NEUTRAL_RECLAIM"
    elif opposite_after_neutral:
        kind = "REVERSAL_AFTER_NEUTRAL"
    elif reclaim_from_neutral:
        kind = "FRESH_FROM_NEUTRAL"
    elif neutral_recent_60:
        kind = "NEUTRAL_RECENT_CONTINUATION"
    else:
        kind = "NO_NEUTRAL_RECENT"

    return {
        "range_prev_state": prev_state,
        "range_prev_phase": prev_phase,
        "range_pre_neutral_state": pre_neutral_state,
        "range_pre_neutral_phase": pre_neutral_phase,
        "range_pre_neutral_dir": pre_neutral_dir,
        "range_neutral_run_before_signal": int(neutral_run),
        "range_neutral_seen_5m": bool(_seen_neutral(5)),
        "range_neutral_seen_15m": bool(_seen_neutral(15)),
        "range_neutral_seen_60m": bool(neutral_recent_60),
        "range_neutral_count_180m": int(sum(neutral_flags)),
        "range_bars_since_last_neutral": float(idx - last_neutral_idx) if last_neutral_idx is not None else float("nan"),
        "range_bars_since_last_same_side": float(idx - same_side_indices[-1]) if same_side_indices else float("nan"),
        "range_bars_since_last_opposite": float(idx - opposite_indices[-1]) if opposite_indices else float("nan"),
        "range_pullback_kind": kind,
        "range_same_side_neutral_reclaim": bool(same_side_neutral_reclaim),
        "range_reversal_after_neutral": bool(opposite_after_neutral),
    }


def _row_context(streams: Mapping[str, pd.DataFrame], raw: pd.DataFrame, signal_idx: int, entry_idx: int, side: str) -> Dict[str, Any]:
    row_1m = streams["1m"].iloc[int(signal_idx)].to_dict()
    ctx: Dict[str, Any] = {
        "signal_time": str(streams["1m"].index[int(signal_idx)]),
        "entry_time": str(raw.index[int(entry_idx)]),
        "hour_utc": int(pd.Timestamp(raw.index[int(entry_idx)]).hour),
        "day_utc": str(pd.Timestamp(raw.index[int(entry_idx)]).date()),
        "entry_open": float(raw.iloc[int(entry_idx)]["open"]),
        "signal_open": float(raw.iloc[int(signal_idx)]["open"]),
        "signal_high": float(raw.iloc[int(signal_idx)]["high"]),
        "signal_low": float(raw.iloc[int(signal_idx)]["low"]),
        "signal_close": float(raw.iloc[int(signal_idx)]["close"]),
        "range_state": _strv(row_1m, "range_filter_state"),
        "range_phase": _strv(row_1m, "range_filter_phase"),
        "range_line": _num(row_1m, "range_filter_line"),
        "range_upper": _num(row_1m, "range_filter_upper"),
        "range_lower": _num(row_1m, "range_filter_lower"),
        "range_up_count": _num(row_1m, "range_filter_upward_count"),
        "range_down_count": _num(row_1m, "range_filter_downward_count"),
        "ema_stack_1m": _strv(row_1m, "ema_stack"),
        "ema_align_1m": _align_to_side(side, _strv(row_1m, "ema_stack")),
        "ema_1_1m": _num(row_1m, "ema_1"),
        "ema_2_1m": _num(row_1m, "ema_2"),
        "ema_spread_pct_1m": _num(row_1m, "ema_spread_pct"),
        "ema_spread_trend_1m": _strv(row_1m, "ema_spread_trend"),
        "ema_stack_pressure_1m": _strv(row_1m, "ema_stack_pressure"),
        "adx_1m": _num(row_1m, "adx"),
        "adx_bucket_1m": _adx_bucket(_num(row_1m, "adx")),
        "rsi_1m": _num(row_1m, "rsi"),
        "rsi_bucket_1m": _rsi_bucket(_num(row_1m, "rsi")),
        "stoch_k_1m": _num(row_1m, "stoch_rsi_k"),
        "macd_1m": _num(row_1m, "macd"),
        "di_spread_1m": _num(row_1m, "di_spread"),
        "vidya_state_1m": _strv(row_1m, "vidya_state"),
        "vidya_align_1m": _align_to_side(side, _strv(row_1m, "vidya_state")),
        "vidya_trend_up_1m": _boolv(row_1m, "vidya_trend_up"),
        "vidya_trend_down_1m": _boolv(row_1m, "vidya_trend_down"),
        "vidya_line_1m": _num(row_1m, "vidya_line"),
        "vidya_delta_pct_1m": _num(row_1m, "vidya_delta_pct"),
        "vidya_slope_1m": _num(row_1m, "vidya_slope"),
        "vidya_angle_norm_1m": _num(row_1m, "vidya_angle_deg_norm"),
        "taker_delta_pct_1m": _num(row_1m, "taker_delta_pct"),
        "taker_buy_share_pct_1m": _num(row_1m, "taker_buy_share_pct"),
        "taker_bias_state_1m": _strv(row_1m, "taker_bias_state"),
        "vd_align_1m": _align_to_side(side, _strv(row_1m, "taker_bias_state")),
    }
    ctx.update(_range_pullback_context(streams["1m"], int(signal_idx), side))
    for tf in ("5m", "15m", "1h"):
        frame = streams.get(tf)
        if not isinstance(frame, pd.DataFrame) or frame.empty:
            continue
        row = frame.iloc[int(signal_idx)].to_dict()
        ctx[f"ema_stack_{tf}"] = _strv(row, "ema_stack")
        ctx[f"ema_align_{tf}"] = _align_to_side(side, _strv(row, "ema_stack"))
        ctx[f"ema_spread_pct_{tf}"] = _num(row, "ema_spread_pct")
        ctx[f"ema_spread_trend_{tf}"] = _strv(row, "ema_spread_trend")
        ctx[f"adx_{tf}"] = _num(row, "adx")
        ctx[f"adx_bucket_{tf}"] = _adx_bucket(_num(row, "adx"))
        ctx[f"range_state_{tf}"] = _strv(row, "range_filter_state")
        ctx[f"range_phase_{tf}"] = _strv(row, "range_filter_phase")
        ctx[f"rsi_{tf}"] = _num(row, "rsi")
        ctx[f"rsi_bucket_{tf}"] = _rsi_bucket(_num(row, "rsi"))
        ctx[f"di_spread_{tf}"] = _num(row, "di_spread")
        ctx[f"vidya_state_{tf}"] = _strv(row, "vidya_state")
        ctx[f"vidya_align_{tf}"] = _align_to_side(side, _strv(row, "vidya_state"))
        ctx[f"vidya_delta_pct_{tf}"] = _num(row, "vidya_delta_pct")
        ctx[f"taker_delta_pct_{tf}"] = _num(row, "taker_delta_pct")
        ctx[f"taker_bias_state_{tf}"] = _strv(row, "taker_bias_state")
        ctx[f"vd_align_{tf}"] = _align_to_side(side, _strv(row, "taker_bias_state"))
    return ctx


def _resolve_window(args: argparse.Namespace) -> Tuple[pd.Timestamp, pd.Timestamp, pd.Timestamp]:
    server_now = binance_fapi_server_time_utc()
    now = pd.to_datetime(server_now, utc=True).floor("min") if server_now is not None else pd.Timestamp.now(tz="UTC").floor("min")
    if args.end:
        end = parse_utc(args.end)
    else:
        end = now
    if args.start:
        start = parse_utc(args.start)
    else:
        today = end.floor("D")
        start = today - pd.Timedelta(days=1)
    return start, end, now


def run(args: argparse.Namespace) -> int:
    warnings.filterwarnings("ignore", category=FutureWarning)
    symbol = str(args.symbol or "BTCUSDT").upper().strip()
    start, end, server_now = _resolve_window(args)
    casebook_path, casebook_cases = _load_casebook_cases(str(args.casebook_report))
    include_casebook_windows = bool(args.include_casebook_windows and casebook_cases)
    audit_windows: List[Tuple[pd.Timestamp, pd.Timestamp, str]] = [(start, end, "primary")]
    if include_casebook_windows:
        audit_windows.extend(_casebook_windows(casebook_cases))
    data_start = min(window[0] for window in audit_windows)
    data_end = max(window[1] for window in audit_windows)
    cache_dir = Path(args.cache_dir).resolve()
    out_dir = Path(args.output_dir) if args.output_dir else REPO_ROOT / "runs" / f"range_event_audit_{symbol.lower()}_{pd.Timestamp.now(tz='UTC').strftime('%Y%m%d_%H%M%S')}"
    if not out_dir.is_absolute():
        out_dir = (REPO_ROOT / out_dir).resolve()
    out_dir.mkdir(parents=True, exist_ok=True)

    timeframes = ["1m", "5m", "15m", "1h"]
    ema_settings = normalize_ema_settings({"defaults": {"ema_1_length": int(args.ema_fast), "ema_2_length": int(args.ema_slow)}, "overrides": {}})
    required_fields = set(normalize_required_fields({
        "price", "open", "high", "low", "close", "volume",
        "range_filter_line", "range_filter_upper", "range_filter_lower", "range_filter_smooth_range",
        "range_filter_state", "range_filter_phase", "range_filter_buy", "range_filter_sell",
        "range_filter_long_cond", "range_filter_short_cond", "range_filter_cond_ini",
        "range_filter_upward_count", "range_filter_downward_count",
        "ema_1", "ema_2", "ema_stack", "ema_1_slope", "ema_2_slope",
        "ema_spread", "ema_spread_pct", "ema_spread_trend", "ema_stack_pressure",
        "adx", "di_plus", "di_minus", "di_spread", "atr",
        "rsi", "rsi_smooth", "rsi_state", "stoch_rsi_k", "stoch_rsi_d", "stoch_rsi_kd",
        "macd", "signal", "ms", "angle", "sig_angle", "ppo",
        "frama_state", "vidya_state", "vidya_trend_up", "vidya_trend_down",
        "vidya_line", "vidya_raw", "vidya_upper", "vidya_lower",
        "vidya_delta_pct", "vidya_slope", "vidya_angle_deg", "vidya_angle_deg_norm",
        "taker_buy_volume", "taker_sell_volume", "taker_delta_volume", "taker_delta_pct",
        "taker_buy_share_pct", "taker_trade_count", "taker_bias_state",
    }, include_defaults=True))
    warmup = max(int(args.warmup_minutes), int(required_warmup_minutes(timeframes, required_fields=required_fields, ema_settings=ema_settings) or 0))
    if int(args.max_warmup_minutes) > 0:
        warmup = min(int(warmup), int(args.max_warmup_minutes))
    fetch_start = data_start - pd.Timedelta(minutes=warmup)

    print(f"symbol={symbol}", flush=True)
    print(f"primary audit window={start} -> {end} UTC", flush=True)
    if casebook_path:
        print(f"casebook={casebook_path} cases={len(casebook_cases)} include_windows={include_casebook_windows}", flush=True)
    print(f"data window={data_start} -> {data_end} UTC", flush=True)
    print(f"server_now_utc={server_now}", flush=True)
    print(f"warmup_minutes={warmup}", flush=True)
    print(f"output={out_dir}", flush=True)

    raw = fetch_klines_1m_futures(symbol, fetch_start, data_end, str(cache_dir), price_source=str(args.price_source or "LAST").upper())
    if raw.empty:
        raise ValueError(f"No candles returned for {symbol} {fetch_start} -> {data_end}")
    raw = raw.copy()
    raw.index = pd.to_datetime(raw["close_time"], utc=True, errors="coerce")
    raw = raw.loc[raw.index.notna()].sort_index()
    for col in ("open", "high", "low", "close"):
        raw[col] = pd.to_numeric(raw[col], errors="coerce").astype(float)

    streams = simulate_multitf_indicators(
        raw.reset_index(drop=True),
        timeframes,
        macd_impl="TRADINGVIEW",
        adx_impl="TRADINGVIEW",
        cache_dir=str(cache_dir),
        symbol=symbol,
        start_utc=fetch_start,
        end_utc=data_end,
        price_source=str(args.price_source or "LAST").upper(),
        use_cache=not bool(args.no_cache),
        required_fields=required_fields,
        ema_settings=ema_settings,
    )
    streams = make_streams_htf_closed_only(streams)
    for tf, frame in list(streams.items()):
        if isinstance(frame, pd.DataFrame) and not isinstance(frame.index, pd.DatetimeIndex):
            frame = frame.copy()
            frame.index = raw.index[: len(frame)]
            streams[tf] = frame

    audit_raw = raw.loc[(raw.index >= data_start) & (raw.index <= data_end)].copy()
    if audit_raw.empty:
        raise ValueError(f"No audit candles in {data_start} -> {data_end}")

    triggers: List[Tuple[int, str, str]] = []
    for item in _range_triggers(streams["1m"], args.trigger_mode):
        signal_idx = int(item[0])
        if signal_idx < 0 or signal_idx >= len(streams["1m"]):
            continue
        signal_time = pd.Timestamp(streams["1m"].index[signal_idx])
        if _window_labels(signal_time, audit_windows):
            triggers.append(item)
    profiles = _parse_exit_profiles(args.exit_profiles)
    horizons = [int(v.strip()) for v in str(args.horizons or "30,60,120,240,480").split(",") if v.strip()]

    events: List[Dict[str, Any]] = []
    outcomes: List[Dict[str, Any]] = []
    seen_event_keys: set[Tuple[int, str, str]] = set()
    event_id = 0
    for signal_idx, side, trigger_type in triggers:
        entry_idx = int(signal_idx) + 1
        if entry_idx >= len(raw):
            continue
        key = (int(signal_idx), str(side), str(trigger_type))
        if key in seen_event_keys:
            continue
        seen_event_keys.add(key)
        event_id += 1
        entry_open = float(raw.iloc[entry_idx]["open"])
        if not math.isfinite(entry_open) or entry_open <= 0:
            continue
        entry_fill = _apply_entry_slippage(side, entry_open, float(args.slippage_bps))
        signal_time = pd.Timestamp(streams["1m"].index[int(signal_idx)])
        entry_time = pd.Timestamp(raw.index[int(entry_idx)])
        ctx = _row_context(streams, raw, int(signal_idx), int(entry_idx), side)
        labels = _window_labels(signal_time, audit_windows) or _window_labels(entry_time, audit_windows)
        ctx.update({
            "audit_window_labels": "|".join(labels),
            "in_primary_window": bool("primary" in labels),
            "in_casebook_window": bool(any(str(label).startswith("casebook:") for label in labels)),
            **_casebook_match(casebook_cases, signal_time=signal_time, entry_time=entry_time),
        })
        event_row = {
            "event_id": int(event_id),
            "symbol": symbol,
            "side": side,
            "trigger_type": trigger_type,
            "signal_index": int(signal_idx),
            "entry_index": int(entry_idx),
            "entry_fill": float(entry_fill),
            **ctx,
            **_forward_stats(raw, side=side, entry_idx=entry_idx, entry_fill=entry_fill, horizons=horizons, fee_pct=float(args.fee_pct), slippage_bps=float(args.slippage_bps)),
        }
        events.append(event_row)
        for profile in profiles:
            result = _simulate_profile(
                side=side,
                entry_idx=entry_idx,
                entry_fill=entry_fill,
                raw=raw,
                profile=profile,
                fee_pct=float(args.fee_pct),
                slippage_bps=float(args.slippage_bps),
            )
            outcomes.append({
                **event_row,
                "exit_profile": profile.name,
                "stop_pct": float(profile.stop_pct),
                "take_pct": float(profile.take_pct),
                "max_hold_min": int(profile.max_hold_min),
                **result,
            })

    events_df = pd.DataFrame(events)
    outcomes_df = pd.DataFrame(outcomes)
    clusters_df = _make_clusters(outcomes_df, min_count=int(args.min_cluster_count))

    events_path = out_dir / "range_events.csv"
    outcomes_path = out_dir / "range_event_outcomes.csv"
    clusters_path = out_dir / "range_event_clusters.csv"
    casebook_cases_path = out_dir / "casebook_cases.csv"
    windows_path = out_dir / "audit_windows.csv"
    events_df.to_csv(events_path, index=False)
    outcomes_df.to_csv(outcomes_path, index=False)
    clusters_df.to_csv(clusters_path, index=False)
    _write_casebook_cases(casebook_cases_path, casebook_cases)
    pd.DataFrame([
        {"start_utc": str(window_start), "end_utc": str(window_end), "label": label}
        for window_start, window_end, label in audit_windows
    ]).to_csv(windows_path, index=False)

    summary: Dict[str, Any] = {
        "symbol": symbol,
        "server_now_utc": str(server_now),
        "start_utc": str(start),
        "end_utc": str(end),
        "data_start_utc": str(data_start),
        "data_end_utc": str(data_end),
        "fetch_start_utc": str(fetch_start),
        "warmup_minutes": int(warmup),
        "trigger_mode": str(args.trigger_mode),
        "casebook_report": str(casebook_path) if casebook_path else "",
        "casebook_case_count": int(len(casebook_cases)),
        "include_casebook_windows": bool(include_casebook_windows),
        "audit_windows": [
            {"start_utc": str(window_start), "end_utc": str(window_end), "label": label}
            for window_start, window_end, label in audit_windows
        ],
        "range_event_count": int(len(events_df)),
        "outcome_count": int(len(outcomes_df)),
        "exit_profiles": [profile.__dict__ for profile in profiles],
        "horizons": horizons,
        "files": {
            "events_csv": str(events_path),
            "outcomes_csv": str(outcomes_path),
            "clusters_csv": str(clusters_path),
            "casebook_cases_csv": str(casebook_cases_path),
            "audit_windows_csv": str(windows_path),
        },
        "by_trigger": {},
        "by_window": {},
        "by_pullback_kind": {},
        "by_profile": {},
        "top_clusters": [],
    }
    if not events_df.empty:
        summary["by_trigger"] = events_df.groupby(["trigger_type", "side"]).size().to_dict()
        if "audit_window_labels" in events_df.columns:
            summary["by_window"] = events_df.groupby("audit_window_labels").size().to_dict()
        if "range_pullback_kind" in events_df.columns:
            summary["by_pullback_kind"] = events_df.groupby(["range_pullback_kind", "side"]).size().to_dict()
    if not outcomes_df.empty:
        for profile, group in outcomes_df.groupby("exit_profile"):
            summary["by_profile"][str(profile)] = _aggregate(group.to_dict("records"))
    if not clusters_df.empty:
        summary["top_clusters"] = clusters_df.head(25).to_dict("records")

    summary_path = out_dir / "summary.json"
    summary_path.write_text(json.dumps(_json_safe(summary), indent=2, ensure_ascii=False), encoding="utf-8")

    print(f"range events={len(events_df)} outcomes={len(outcomes_df)} clusters={len(clusters_df)}", flush=True)
    print("\nExit profile summary", flush=True)
    for name, agg in summary["by_profile"].items():
        print(
            f"{name}: n={agg['count']} net={agg['net_pct_sum']:.3f}% "
            f"wr={agg['win_rate_pct']:.1f}% pf={agg['profit_factor']:.2f} avg={agg['avg_net_pct']:.3f}%",
            flush=True,
        )
    print("\nTop clusters", flush=True)
    for row in summary["top_clusters"][:10]:
        keys = [k for k in (
            "cluster_kind", "exit_profile", "trigger_type", "side", "range_phase",
            "range_pullback_kind", "vidya_align_1m", "ema_align_5m", "vd_align_1m",
            "ema_stack_5m", "adx_bucket_1m", "hour_utc",
        ) if k in row]
        label = " ".join(f"{k}={row.get(k)}" for k in keys)
        print(
            f"{label} n={row['count']} net={row['net_pct_sum']:.3f}% "
            f"wr={row['win_rate_pct']:.1f}% pf={row['profit_factor']:.2f}",
            flush=True,
        )
    print(f"\nSaved: {out_dir}", flush=True)
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description="Audit every Range Filter trigger as a potential 1m trade.")
    parser.add_argument("--symbol", default="BTCUSDT")
    parser.add_argument("--cache-dir", default=str(REPO_ROOT / "data_cache"))
    parser.add_argument("--price-source", default="LAST")
    parser.add_argument("--start", default=None, help="UTC start. Default: yesterday 00:00 UTC relative to end.")
    parser.add_argument("--end", default=None, help="UTC end. Default: Binance futures server time floored to minute.")
    parser.add_argument("--casebook-report", default="auto", help="Path to 0001-0005 casebook report, 'auto' or 'off'.")
    parser.add_argument("--include-casebook-windows", action=argparse.BooleanOptionalAction, default=True)
    parser.add_argument("--ema-fast", type=int, default=20)
    parser.add_argument("--ema-slow", type=int, default=200)
    parser.add_argument("--warmup-minutes", type=int, default=0)
    parser.add_argument("--max-warmup-minutes", type=int, default=12000)
    parser.add_argument("--trigger-mode", choices=["events", "conditions", "all"], default="all")
    parser.add_argument("--exit-profiles", default="", help="Comma list of stop:take:max_hold_min, e.g. 0.2:0.35:120,0.35:0.7:480")
    parser.add_argument("--horizons", default="30,60,120,240,480")
    parser.add_argument("--fee-pct", type=float, default=0.04)
    parser.add_argument("--slippage-bps", type=float, default=1.0)
    parser.add_argument("--min-cluster-count", type=int, default=3)
    parser.add_argument("--no-cache", action="store_true")
    parser.add_argument("--output-dir", default=None)
    return run(parser.parse_args())


if __name__ == "__main__":
    raise SystemExit(main())
