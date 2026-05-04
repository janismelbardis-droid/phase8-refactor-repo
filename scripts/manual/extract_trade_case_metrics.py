from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional

import pandas as pd


CORE_FAMILIES = {"vidya", "range_filter"}
FULL_INDICATOR_FAMILIES = {
    "adx",
    "atr",
    "macd_lines",
    "rsi",
    "stoch_rsi",
    "vidya",
    "range_filter",
}

PRICE_COLUMNS = [
    "open_time",
    "close_time",
    "open",
    "high",
    "low",
    "close",
    "volume",
    "quote_volume",
    "trade_count",
    "taker_buy_volume",
    "taker_buy_quote_volume",
]

SNAPSHOT_FIELDS = [
    "rsi",
    "rsi_smooth",
    "rsi_state",
    "stoch_rsi_k",
    "stoch_rsi_d",
    "stoch_rsi_kd",
    "macd",
    "signal",
    "ms",
    "angle",
    "sig_angle",
    "ppo",
    "flip_age",
    "adx",
    "atr",
    "di_plus",
    "di_minus",
    "di_spread",
    "adx_angle",
    "atr_angle",
    "vidya_state",
    "vidya_trend_up",
    "vidya_trend_down",
    "vidya_line",
    "vidya_upper",
    "vidya_lower",
    "vidya_delta_pct",
    "vidya_slope",
    "vidya_angle_deg",
    "vidya_angle_deg_norm",
    "range_filter_state",
    "range_filter_phase",
    "range_filter_buy",
    "range_filter_sell",
    "range_filter_line",
    "range_filter_upper",
    "range_filter_lower",
    "range_filter_smooth_range",
    "range_filter_cond_ini",
    "range_filter_upward_count",
    "range_filter_downward_count",
]

WINDOW_EXPORT_COLUMNS = [
    "ts",
    "open",
    "high",
    "low",
    "close",
    "volume",
    "quote_volume",
    "trade_count",
    "taker_buy_volume",
    "taker_buy_quote_volume",
    *SNAPSHOT_FIELDS,
]

DELTA_FIELDS = [
    "rsi",
    "rsi_minus_smooth",
    "stoch_rsi_k",
    "stoch_rsi_d",
    "stoch_k_minus_d",
    "macd",
    "signal",
    "macd_hist",
    "adx",
    "atr",
    "di_plus",
    "di_minus",
    "di_spread",
    "vidya_delta_pct",
]

MTF_TIMEFRAMES = ["5m", "30m", "1h"]
MTF_STREAM_FIELDS = [
    "price",
    "ema_1",
    "ema_2",
    "ema_spread",
    "ema_spread_pct",
    "ema_stack",
    "ema_cross_up",
    "ema_cross_down",
    "frama_state",
    "frama_break_up",
    "frama_break_down",
    *SNAPSHOT_FIELDS,
]
MTF_DELTA_FIELDS = [
    "price",
    "ema_spread",
    "ema_spread_pct",
    *DELTA_FIELDS,
]


def repo_root() -> Path:
    return Path(__file__).resolve().parents[2]


def prepared_root(root: Path, symbol: str) -> Path:
    return root / "data_cache" / "prepared_datasets" / symbol


def _safe_bool(value: Any) -> bool:
    try:
        if pd.isna(value):
            return False
    except Exception:
        pass
    return bool(value)


def _safe_float(value: Any) -> Optional[float]:
    try:
        if pd.isna(value):
            return None
    except Exception:
        pass
    try:
        return float(value)
    except Exception:
        return None


def _safe_upper(value: Any) -> Optional[str]:
    if value is None:
        return None
    try:
        if pd.isna(value):
            return None
    except Exception:
        pass
    text = str(value).strip()
    if not text:
        return None
    return text.upper()


def _iso_or_none(value: Any) -> Optional[str]:
    if value is None:
        return None
    try:
        if pd.isna(value):
            return None
    except Exception:
        pass
    try:
        return pd.Timestamp(value).isoformat()
    except Exception:
        return str(value)


def timeframe_minutes(timeframe: str) -> int:
    if timeframe.endswith("m"):
        return int(timeframe[:-1])
    if timeframe.endswith("h"):
        return int(timeframe[:-1]) * 60
    raise ValueError(f"unsupported timeframe {timeframe}")


def timeframe_floor(ts: pd.Timestamp, timeframe: str) -> pd.Timestamp:
    minutes = timeframe_minutes(timeframe)
    return ts.floor(f"{minutes}min")


def load_meta(meta_path: Path) -> Dict[str, Any]:
    return json.loads(meta_path.read_text(encoding="utf-8"))


def meta_matches(meta: Dict[str, Any], require_full: bool, required_timeframes: Optional[Iterable[str]] = None) -> bool:
    families = set(meta.get("required_families") or [])
    timeframes = set(meta.get("timeframes") or [])
    if not CORE_FAMILIES.issubset(families):
        return False
    if "1m" not in timeframes:
        return False
    if require_full and not FULL_INDICATOR_FAMILIES.issubset(families):
        return False
    required_timeframes = set(required_timeframes or [])
    if required_timeframes and not required_timeframes.issubset(timeframes):
        return False
    return True


def choose_meta(
    meta_dir: Path,
    meta_id: Optional[str],
    require_full: bool,
    required_timeframes: Optional[Iterable[str]] = None,
) -> Path:
    if meta_id:
        path = meta_dir / f"{meta_id}.meta.json"
        if path.exists():
            return path
        alt = meta_dir / meta_id
        if alt.exists():
            return alt
        raise FileNotFoundError(f"meta file not found for id {meta_id}")

    candidates: List[Path] = []
    for meta_path in meta_dir.glob("prep_*.meta.json"):
        try:
            meta = load_meta(meta_path)
        except Exception:
            continue
        if meta_matches(meta, require_full=require_full, required_timeframes=required_timeframes):
            candidates.append(meta_path)
    if not candidates:
        raise FileNotFoundError("no prepared dataset meta matched requested families/timeframe")
    candidates.sort(key=lambda path: load_meta(path).get("end_utc") or "", reverse=True)
    return candidates[0]


def load_merged_dataset(meta_dir: Path, meta: Dict[str, Any]) -> pd.DataFrame:
    df_1m_path = meta_dir / meta["df_1m_file"]
    stream_path = meta_dir / meta["stream_files"]["1m"]
    price = pd.read_parquet(df_1m_path).copy()
    stream = pd.read_parquet(stream_path).copy()
    if price.empty or stream.empty:
        raise ValueError("prepared dataset is empty")

    price["ts"] = pd.to_datetime(price["close_time"], utc=True)
    stream["ts"] = pd.to_datetime(stream["__prepared_index__"], utc=True)
    keep_price = [col for col in PRICE_COLUMNS if col in price.columns]
    merged = price[keep_price + ["ts"]].merge(stream.drop(columns=["__prepared_index__"], errors="ignore"), on="ts", how="left")
    merged = merged.sort_values("ts", kind="stable").reset_index(drop=True)
    return merged


def load_stream_frame(meta_dir: Path, meta: Dict[str, Any], timeframe: str) -> Optional[pd.DataFrame]:
    stream_file = (meta.get("stream_files") or {}).get(timeframe)
    if not stream_file:
        return None
    path = meta_dir / stream_file
    if not path.exists():
        return None
    frame = pd.read_parquet(path).copy()
    if "__prepared_index__" not in frame.columns:
        return None
    frame["ts"] = pd.to_datetime(frame["__prepared_index__"], utc=True)
    frame = frame.sort_values("ts", kind="stable").drop_duplicates(subset=["ts"], keep="last")
    return frame.set_index("ts", drop=False)


def find_prev_event_index(df: pd.DataFrame, idx: int, field: str) -> Optional[int]:
    flags = df[field].fillna(False).astype(bool)
    prior = flags.iloc[: idx + 1]
    if not prior.any():
        return None
    return int(prior[prior].index[-1])


def first_pullback_low_index(df: pd.DataFrame, anchor_idx: int, window: int) -> Optional[int]:
    start = anchor_idx + 1
    stop = min(len(df), anchor_idx + 1 + window)
    if start >= stop:
        return None
    lows = df.iloc[start:stop]["low"].astype(float)
    if lows.empty:
        return None
    return int(lows.idxmin())


def first_reclaim_index(df: pd.DataFrame, pullback_idx: Optional[int], t0_close: float, window: int) -> Optional[int]:
    if pullback_idx is None:
        return None
    start = pullback_idx + 1
    stop = min(len(df), pullback_idx + 1 + window)
    for idx in range(start, stop):
        close = float(df.at[idx, "close"])
        open_ = float(df.at[idx, "open"])
        prev_high = float(df.at[idx - 1, "high"]) if idx - 1 >= 0 else close
        vidya_line = _safe_float(df.at[idx, "vidya_line"]) if "vidya_line" in df.columns else None
        if close >= prev_high and close >= open_ and (vidya_line is None or close >= vidya_line):
            return idx
        if close >= t0_close and close >= open_:
            return idx
    return None


def indicator_snapshot_from_row(
    row: pd.Series,
    *,
    fields: Optional[Iterable[str]] = None,
    idx: Optional[int] = None,
    include_ohlc: bool = False,
) -> Dict[str, Any]:
    fields = list(fields or SNAPSHOT_FIELDS)
    payload: Dict[str, Any] = {
        "index": None if idx is None else int(idx),
        "ts": _iso_or_none(row.get("ts")),
    }
    if include_ohlc:
        payload["open"] = _safe_float(row.get("open"))
        payload["high"] = _safe_float(row.get("high"))
        payload["low"] = _safe_float(row.get("low"))
        payload["close"] = _safe_float(row.get("close"))
    for field in fields:
        if field not in row.index:
            continue
        value = row.get(field)
        if isinstance(value, (bool, str)) or value is None:
            payload[field] = value
        else:
            payload[field] = _safe_float(value)
    payload["rsi_minus_smooth"] = (
        None
        if payload.get("rsi") is None or payload.get("rsi_smooth") is None
        else float(payload["rsi"] - payload["rsi_smooth"])
    )
    payload["stoch_k_minus_d"] = (
        None
        if payload.get("stoch_rsi_k") is None or payload.get("stoch_rsi_d") is None
        else float(payload["stoch_rsi_k"] - payload["stoch_rsi_d"])
    )
    payload["macd_hist"] = (
        None
        if payload.get("macd") is None or payload.get("signal") is None
        else float(payload["macd"] - payload["signal"])
    )
    if "price" in payload:
        price = payload.get("price")
        vidya_line = payload.get("vidya_line")
        range_line = payload.get("range_filter_line")
        price_vidya = relative_distance_metrics(price, vidya_line)
        price_range = relative_distance_metrics(price, range_line)
        payload["price_to_vidya_abs"] = price_vidya["abs"]
        payload["price_to_vidya_frac"] = price_vidya["frac"]
        payload["price_to_vidya_pct"] = price_vidya["pct"]
        payload["price_to_range_line_abs"] = price_range["abs"]
        payload["price_to_range_line_frac"] = price_range["frac"]
        payload["price_to_range_line_pct"] = price_range["pct"]
    payload["state_pair"] = state_pair(payload)
    return payload


def snapshot_for_index(df: pd.DataFrame, idx: Optional[int]) -> Optional[Dict[str, Any]]:
    if idx is None or idx < 0 or idx >= len(df):
        return None
    row = df.iloc[idx]
    return indicator_snapshot_from_row(row, fields=SNAPSHOT_FIELDS, idx=idx, include_ohlc=True)


def relative_distance_metrics(value: Optional[float], reference: Optional[float]) -> Dict[str, Optional[float]]:
    if value is None or reference is None or abs(reference) < 1e-9:
        return {"abs": None, "frac": None, "pct": None}
    abs_delta = float(value - reference)
    frac = float(abs_delta / reference)
    return {
        "abs": abs_delta,
        "frac": frac,
        "pct": float(frac * 100.0),
    }


def delta_value(start: Optional[float], end: Optional[float]) -> Optional[float]:
    if start is None or end is None:
        return None
    return float(end - start)


def add_metric_triplet(summary: Dict[str, Any], prefix: str, metrics: Dict[str, Optional[float]]) -> None:
    summary[f"{prefix}_abs"] = metrics["abs"]
    summary[f"{prefix}_frac"] = metrics["frac"]
    summary[f"{prefix}_pct"] = metrics["pct"]


def slice_inclusive(df: pd.DataFrame, start_idx: Optional[int], end_idx: Optional[int]) -> Optional[pd.DataFrame]:
    if start_idx is None or end_idx is None:
        return None
    if start_idx < 0 or end_idx < start_idx or end_idx >= len(df):
        return None
    return df.iloc[start_idx : end_idx + 1].copy()


def count_state(df: pd.DataFrame, start_idx: Optional[int], end_idx: Optional[int], field: str, expected: str) -> Optional[int]:
    segment = slice_inclusive(df, start_idx, end_idx)
    if segment is None or field not in segment.columns:
        return None
    return int(segment[field].map(_safe_upper).eq(expected).sum())


def count_joint_buy(df: pd.DataFrame, start_idx: Optional[int], end_idx: Optional[int]) -> Optional[int]:
    segment = slice_inclusive(df, start_idx, end_idx)
    if segment is None:
        return None
    if "vidya_state" not in segment.columns or "range_filter_state" not in segment.columns:
        return None
    vidya_buy = segment["vidya_state"].map(_safe_upper).eq("BUY")
    range_buy = segment["range_filter_state"].map(_safe_upper).eq("BUY")
    return int((vidya_buy & range_buy).sum())


def state_pair(snapshot: Optional[Dict[str, Any]]) -> Optional[str]:
    if not snapshot:
        return None
    vidya = _safe_upper(snapshot.get("vidya_state")) or "NA"
    range_state = _safe_upper(snapshot.get("range_filter_state")) or "NA"
    return f"VIDYA_{vidya}__RF_{range_state}"


def classify_anchor_low_relation(breaks_t0_low: Optional[bool], breaks_rf_low: Optional[bool]) -> Optional[str]:
    if breaks_t0_low is None or breaks_rf_low is None:
        return None
    if breaks_t0_low and breaks_rf_low:
        return "breaks_both"
    if breaks_t0_low and not breaks_rf_low:
        return "breaks_t0_only"
    if not breaks_t0_low and breaks_rf_low:
        return "breaks_rf_only"
    return "holds_both"


def classify_pullback_survival(snapshot: Optional[Dict[str, Any]], breaks_t0_low: Optional[bool], breaks_rf_low: Optional[bool]) -> Optional[str]:
    if not snapshot:
        return None
    vidya = _safe_upper(snapshot.get("vidya_state"))
    range_state = _safe_upper(snapshot.get("range_filter_state"))
    if vidya == "BUY" and range_state == "BUY":
        if breaks_rf_low is False and breaks_t0_low is False:
            return "clean_hold"
        if breaks_rf_low is False and breaks_t0_low is True:
            return "deep_but_alive"
        if breaks_rf_low is True:
            return "anchor_broken_but_states_alive"
        return "alive_unclassified"
    if vidya == "BUY" and range_state != "BUY":
        return "range_lost"
    if vidya != "BUY" and range_state == "BUY":
        return "vidya_lost"
    return "both_states_lost"


def reclaim_speed_class(bars_to_reclaim: Optional[int]) -> Optional[str]:
    if bars_to_reclaim is None:
        return None
    if bars_to_reclaim <= 1:
        return "immediate"
    if bars_to_reclaim <= 3:
        return "fast"
    if bars_to_reclaim <= 6:
        return "delayed"
    return "late"


def mtf_stream_snapshot_for_ts(frame: Optional[pd.DataFrame], ts: Optional[str]) -> Optional[Dict[str, Any]]:
    if frame is None or ts is None:
        return None
    key = pd.Timestamp(ts)
    try:
        row = frame.loc[key]
    except KeyError:
        return None
    if isinstance(row, pd.DataFrame):
        row = row.iloc[-1]
    return indicator_snapshot_from_row(row, fields=MTF_STREAM_FIELDS, include_ohlc=False)


def active_higher_timeframe_price_context(df: pd.DataFrame, idx: Optional[int], timeframe: str) -> Optional[Dict[str, Any]]:
    if idx is None or idx < 0 or idx >= len(df):
        return None
    current = df.iloc[idx]
    current_open_time = pd.Timestamp(current["open_time"])
    bucket_start = timeframe_floor(current_open_time, timeframe)
    prev_bucket_start = bucket_start - pd.Timedelta(minutes=timeframe_minutes(timeframe))

    open_times = pd.to_datetime(df["open_time"], utc=True)
    current_mask = (open_times >= bucket_start) & (open_times <= current_open_time)
    current_seg = df.loc[current_mask]
    if current_seg.empty:
        return None

    prev_mask = (open_times >= prev_bucket_start) & (open_times < bucket_start)
    prev_seg = df.loc[prev_mask]

    open_ = _safe_float(current_seg.iloc[0]["open"])
    high = _safe_float(current_seg["high"].astype(float).max())
    low = _safe_float(current_seg["low"].astype(float).min())
    close = _safe_float(current_seg.iloc[-1]["close"])
    volume = _safe_float(current_seg["volume"].astype(float).sum()) if "volume" in current_seg.columns else None
    trade_count = _safe_float(current_seg["trade_count"].astype(float).sum()) if "trade_count" in current_seg.columns else None
    quote_volume = _safe_float(current_seg["quote_volume"].astype(float).sum()) if "quote_volume" in current_seg.columns else None

    total_bars = timeframe_minutes(timeframe)
    bars_in_candle = int(len(current_seg))
    range_abs = None if high is None or low is None else float(high - low)
    body_abs = None if open_ is None or close is None else float(abs(close - open_))
    body_signed = None if open_ is None or close is None else float(close - open_)
    upper_wick_abs = None if high is None or open_ is None or close is None else float(high - max(open_, close))
    lower_wick_abs = None if low is None or open_ is None or close is None else float(min(open_, close) - low)
    close_pos_in_range_pct = (
        None if close is None or low is None or range_abs is None or abs(range_abs) < 1e-9 else float((close - low) / range_abs * 100.0)
    )
    body_pct_of_range = (
        None if body_abs is None or range_abs is None or abs(range_abs) < 1e-9 else float(body_abs / range_abs * 100.0)
    )

    prev_high = _safe_float(prev_seg["high"].astype(float).max()) if not prev_seg.empty else None
    prev_low = _safe_float(prev_seg["low"].astype(float).min()) if not prev_seg.empty else None
    prev_close = _safe_float(prev_seg.iloc[-1]["close"]) if not prev_seg.empty else None

    current_high_vs_prev_high = relative_distance_metrics(high, prev_high)
    current_low_vs_prev_low = relative_distance_metrics(low, prev_low)
    close_vs_prev_close = relative_distance_metrics(close, prev_close)

    return {
        "timeframe": timeframe,
        "bucket_start_utc": bucket_start.isoformat(),
        "bucket_expected_close_utc": (bucket_start + pd.Timedelta(minutes=total_bars) - pd.Timedelta(milliseconds=1)).isoformat(),
        "bars_in_candle": bars_in_candle,
        "bars_total": total_bars,
        "progress_pct": float(bars_in_candle / total_bars * 100.0),
        "open": open_,
        "high": high,
        "low": low,
        "close": close,
        "volume": volume,
        "quote_volume": quote_volume,
        "trade_count": trade_count,
        "range_abs": range_abs,
        "body_abs": body_abs,
        "body_signed": body_signed,
        "upper_wick_abs": upper_wick_abs,
        "lower_wick_abs": lower_wick_abs,
        "close_pos_in_range_pct": close_pos_in_range_pct,
        "body_pct_of_range": body_pct_of_range,
        "direction": "bull" if body_signed is not None and body_signed > 0 else "bear" if body_signed is not None and body_signed < 0 else "doji",
        "prev_high": prev_high,
        "prev_low": prev_low,
        "prev_close": prev_close,
        "takes_prev_high": None if high is None or prev_high is None else bool(high > prev_high),
        "takes_prev_low": None if low is None or prev_low is None else bool(low < prev_low),
        "current_high_vs_prev_high_abs": current_high_vs_prev_high["abs"],
        "current_high_vs_prev_high_frac": current_high_vs_prev_high["frac"],
        "current_high_vs_prev_high_pct": current_high_vs_prev_high["pct"],
        "current_low_vs_prev_low_abs": current_low_vs_prev_low["abs"],
        "current_low_vs_prev_low_frac": current_low_vs_prev_low["frac"],
        "current_low_vs_prev_low_pct": current_low_vs_prev_low["pct"],
        "close_vs_prev_close_abs": close_vs_prev_close["abs"],
        "close_vs_prev_close_frac": close_vs_prev_close["frac"],
        "close_vs_prev_close_pct": close_vs_prev_close["pct"],
    }


def build_mtf_context(
    df: pd.DataFrame,
    checkpoint_indices: Dict[str, Optional[int]],
    checkpoint_snaps: Dict[str, Optional[Dict[str, Any]]],
    mtf_streams: Optional[Dict[str, Optional[pd.DataFrame]]],
) -> Dict[str, Any]:
    payload: Dict[str, Any] = {"timeframes": {}, "available_timeframes": [], "missing_timeframes": []}
    mtf_streams = mtf_streams or {}
    for timeframe in MTF_TIMEFRAMES:
        tf_payload: Dict[str, Any] = {"checkpoints": {}, "pair_deltas": {}}
        stream = mtf_streams.get(timeframe)
        if stream is None:
            payload["missing_timeframes"].append(timeframe)
        else:
            payload["available_timeframes"].append(timeframe)

        for checkpoint_name, snap in checkpoint_snaps.items():
            idx = checkpoint_indices.get(checkpoint_name)
            tf_payload["checkpoints"][checkpoint_name] = {
                "stream_snapshot": mtf_stream_snapshot_for_ts(stream, None if snap is None else snap.get("ts")),
                "price_context": active_higher_timeframe_price_context(df, idx, timeframe),
            }

        checkpoint_pairs = [
            ("rf_to_t0", "t_rf", "t0"),
            ("t0_to_pb1_low", "t0", "t_pb1_low"),
            ("pb1_low_to_reclaim", "t_pb1_low", "t_pb1_reclaim"),
            ("t0_to_reclaim", "t0", "t_pb1_reclaim"),
        ]
        for pair_name, start_name, end_name in checkpoint_pairs:
            start_stream = tf_payload["checkpoints"][start_name]["stream_snapshot"]
            end_stream = tf_payload["checkpoints"][end_name]["stream_snapshot"]
            pair_payload: Dict[str, Optional[float]] = {}
            for field in MTF_DELTA_FIELDS:
                start_value = None if not start_stream else _safe_float(start_stream.get(field))
                end_value = None if not end_stream else _safe_float(end_stream.get(field))
                pair_payload[f"{field}_delta"] = delta_value(start_value, end_value)
            tf_payload["pair_deltas"][pair_name] = pair_payload

        payload["timeframes"][timeframe] = tf_payload
    return payload


def measure_event(
    df: pd.DataFrame,
    anchor_idx: int,
    *,
    pullback_window: int,
    reclaim_window: int,
    mtf_streams: Optional[Dict[str, Optional[pd.DataFrame]]] = None,
    include_mtf: bool = False,
) -> Dict[str, Any]:
    prev_rf_idx = find_prev_event_index(df, anchor_idx, "range_filter_buy")
    pb1_low_idx = first_pullback_low_index(df, anchor_idx, pullback_window)
    t0_close = float(df.at[anchor_idx, "close"])
    pb1_reclaim_idx = first_reclaim_index(df, pb1_low_idx, t0_close, reclaim_window)

    t0 = snapshot_for_index(df, anchor_idx)
    t_rf = snapshot_for_index(df, prev_rf_idx)
    t_pb1_low = snapshot_for_index(df, pb1_low_idx)
    t_pb1_reclaim = snapshot_for_index(df, pb1_reclaim_idx)
    checkpoint_indices = {
        "t_rf": prev_rf_idx,
        "t0": anchor_idx,
        "t_pb1_low": pb1_low_idx,
        "t_pb1_reclaim": pb1_reclaim_idx,
    }
    checkpoint_snaps = {
        "t_rf": t_rf,
        "t0": t0,
        "t_pb1_low": t_pb1_low,
        "t_pb1_reclaim": t_pb1_reclaim,
    }

    t0_high = t0["high"] if t0 else None
    t0_low = t0["low"] if t0 else None
    t0_open = t0["open"] if t0 else None
    t0_close = t0["close"] if t0 else None
    rf_high = t_rf["high"] if t_rf else None
    rf_low = t_rf["low"] if t_rf else None
    rf_open = t_rf["open"] if t_rf else None
    rf_close = t_rf["close"] if t_rf else None
    pb_low = t_pb1_low["low"] if t_pb1_low else None

    def _pct(numer: Optional[float], denom: Optional[float]) -> Optional[float]:
        if numer is None or denom is None or abs(denom) < 1e-9:
            return None
        return float(numer / denom * 100.0)

    t0_range = None if t0_high is None or t0_low is None else max(t0_high - t0_low, 1e-9)
    t0_body = None if t0_open is None or t0_close is None else max(abs(t0_close - t0_open), 1e-9)
    t0_body_top = None if t0_open is None or t0_close is None else max(t0_open, t0_close)
    t0_body_bottom = None if t0_open is None or t0_close is None else min(t0_open, t0_close)

    rf_range = None if rf_high is None or rf_low is None else max(rf_high - rf_low, 1e-9)
    rf_body = None if rf_open is None or rf_close is None else max(abs(rf_close - rf_open), 1e-9)
    rf_body_top = None if rf_open is None or rf_close is None else max(rf_open, rf_close)
    rf_body_bottom = None if rf_open is None or rf_close is None else min(rf_open, rf_close)

    peak_high = None
    peak_idx = None
    if pb1_low_idx is not None:
        highs = df.iloc[anchor_idx : pb1_low_idx + 1]["high"].astype(float)
        if not highs.empty:
            peak_idx = int(highs.idxmax())
            peak_high = _safe_float(highs.loc[peak_idx])

    bars_rf_to_t0 = None if prev_rf_idx is None else int(anchor_idx - prev_rf_idx)
    bars_t0_to_pb1_low = None if pb1_low_idx is None else int(pb1_low_idx - anchor_idx)
    bars_pb1_low_to_reclaim = None if pb1_reclaim_idx is None or pb1_low_idx is None else int(pb1_reclaim_idx - pb1_low_idx)
    rf_to_t0_span = None if prev_rf_idx is None else int(anchor_idx - prev_rf_idx + 1)
    t0_to_pb1_low_span = None if pb1_low_idx is None else int(pb1_low_idx - anchor_idx + 1)

    summary: Dict[str, Any] = {
        "anchor_index": int(anchor_idx),
        "t_rf": t_rf,
        "t0": t0,
        "t_pb1_low": t_pb1_low,
        "t_pb1_reclaim": t_pb1_reclaim,
        "bars_rf_to_t0": bars_rf_to_t0,
        "bars_t0_to_pb1_low": bars_t0_to_pb1_low,
        "bars_pb1_low_to_reclaim": bars_pb1_low_to_reclaim,
        "bars_rf_to_t0_inclusive": rf_to_t0_span,
        "bars_t0_to_pb1_low_inclusive": t0_to_pb1_low_span,
        "pullback_window_bars": int(pullback_window),
        "reclaim_window_bars": int(reclaim_window),
        "peak_high_before_pb1": peak_high,
        "bars_t0_to_peak_high": None if peak_idx is None else int(peak_idx - anchor_idx),
        "pb1_depth_vs_t0_wick_pct": None if pb_low is None or t0_high is None or t0_range is None else _pct(t0_high - pb_low, t0_range),
        "pb1_depth_vs_t0_body_pct": None if pb_low is None or t0_body_top is None or t0_body is None else _pct(t0_body_top - pb_low, t0_body),
        "pb1_penetrates_t0_body": None if pb_low is None or t0_body_top is None or t0_body_bottom is None else bool(t0_body_bottom <= pb_low <= t0_body_top),
        "pb1_breaks_t0_low": None if pb_low is None or t0_low is None else bool(pb_low < t0_low),
        "pb1_depth_vs_rf_wick_pct": None if pb_low is None or rf_high is None or rf_range is None else _pct(rf_high - pb_low, rf_range),
        "pb1_depth_vs_rf_body_pct": None if pb_low is None or rf_body_top is None or rf_body is None else _pct(rf_body_top - pb_low, rf_body),
        "pb1_penetrates_rf_body": None if pb_low is None or rf_body_top is None or rf_body_bottom is None else bool(rf_body_bottom <= pb_low <= rf_body_top),
        "pb1_breaks_rf_low": None if pb_low is None or rf_low is None else bool(pb_low < rf_low),
    }
    summary["pb1_anchor_low_relation_class"] = classify_anchor_low_relation(
        summary.get("pb1_breaks_t0_low"),
        summary.get("pb1_breaks_rf_low"),
    )
    summary["t0_state_pair"] = state_pair(t0)
    summary["t_pb1_low_state_pair"] = state_pair(t_pb1_low)
    summary["t_pb1_reclaim_state_pair"] = state_pair(t_pb1_reclaim)
    summary["pb1_pullback_survival_class"] = classify_pullback_survival(
        t_pb1_low,
        summary.get("pb1_breaks_t0_low"),
        summary.get("pb1_breaks_rf_low"),
    )
    summary["reclaim_speed_class"] = reclaim_speed_class(bars_pb1_low_to_reclaim)

    point_diffs = {
        "t0_close_minus_rf_close": relative_distance_metrics(t0_close, rf_close),
        "t0_low_minus_rf_low": relative_distance_metrics(t0_low, rf_low),
        "pb1_low_minus_t0_low": relative_distance_metrics(pb_low, t0_low),
        "pb1_low_minus_rf_low": relative_distance_metrics(pb_low, rf_low),
        "pb1_low_minus_t0_close": relative_distance_metrics(pb_low, t0_close),
        "pb1_low_minus_rf_close": relative_distance_metrics(pb_low, rf_close),
        "peak_high_minus_t0_close": relative_distance_metrics(peak_high, t0_close),
        "peak_high_minus_rf_close": relative_distance_metrics(peak_high, rf_close),
    }
    for prefix, metrics in point_diffs.items():
        add_metric_triplet(summary, prefix, metrics)

    checkpoints = {
        "t0": t0,
        "t_pb1_low": t_pb1_low,
        "t_pb1_reclaim": t_pb1_reclaim,
    }
    for name, snap in checkpoints.items():
        if not snap:
            continue
        close = snap.get("close")
        low = snap.get("low")
        vidya_line = snap.get("vidya_line")
        range_line = snap.get("range_filter_line")
        vidya_lower = snap.get("vidya_lower")
        vidya_upper = snap.get("vidya_upper")
        range_lower = snap.get("range_filter_lower")
        range_upper = snap.get("range_filter_upper")
        close_vidya = relative_distance_metrics(close, vidya_line)
        low_vidya = relative_distance_metrics(low, vidya_line)
        close_range = relative_distance_metrics(close, range_line)
        low_range = relative_distance_metrics(low, range_line)

        add_metric_triplet(summary, f"{name}_close_to_vidya", close_vidya)
        add_metric_triplet(summary, f"{name}_low_to_vidya", low_vidya)
        add_metric_triplet(summary, f"{name}_close_to_range_line", close_range)
        add_metric_triplet(summary, f"{name}_low_to_range_line", low_range)
        summary[f"{name}_low_below_vidya_line"] = None if low is None or vidya_line is None else bool(low < vidya_line)
        summary[f"{name}_low_below_range_line"] = None if low is None or range_line is None else bool(low < range_line)
        summary[f"{name}_low_inside_vidya_band"] = (
            None if low is None or vidya_lower is None or vidya_upper is None else bool(vidya_lower <= low <= vidya_upper)
        )
        summary[f"{name}_low_inside_range_band"] = (
            None if low is None or range_lower is None or range_upper is None else bool(range_lower <= low <= range_upper)
        )

    if t0 and peak_high is not None:
        summary["move_up_before_pb1_abs"] = float(peak_high - t0["close"])
        summary["move_up_before_pb1_pct_of_t0_range"] = _pct(summary["move_up_before_pb1_abs"], t0_range)
        summary["move_up_before_pb1_pct_of_rf_range"] = _pct(summary["move_up_before_pb1_abs"], rf_range)
    if peak_high is not None and pb_low is not None:
        summary["pullback_from_peak_abs"] = float(peak_high - pb_low)
        summary["pullback_from_peak_pct_of_thrust"] = _pct(summary["pullback_from_peak_abs"], summary.get("move_up_before_pb1_abs"))

    summary["range_filter_buy_bars_rf_to_t0"] = count_state(df, prev_rf_idx, anchor_idx, "range_filter_state", "BUY")
    summary["range_filter_sell_bars_rf_to_t0"] = count_state(df, prev_rf_idx, anchor_idx, "range_filter_state", "SELL")
    summary["range_filter_all_buy_rf_to_t0"] = (
        None
        if summary["range_filter_buy_bars_rf_to_t0"] is None or rf_to_t0_span is None
        else bool(summary["range_filter_buy_bars_rf_to_t0"] == rf_to_t0_span)
    )
    summary["vidya_buy_bars_t0_to_pb1_low"] = count_state(df, anchor_idx, pb1_low_idx, "vidya_state", "BUY")
    summary["range_filter_buy_bars_t0_to_pb1_low"] = count_state(df, anchor_idx, pb1_low_idx, "range_filter_state", "BUY")
    summary["both_buy_bars_t0_to_pb1_low"] = count_joint_buy(df, anchor_idx, pb1_low_idx)
    summary["vidya_all_buy_t0_to_pb1_low"] = (
        None
        if summary["vidya_buy_bars_t0_to_pb1_low"] is None or t0_to_pb1_low_span is None
        else bool(summary["vidya_buy_bars_t0_to_pb1_low"] == t0_to_pb1_low_span)
    )
    summary["range_filter_all_buy_t0_to_pb1_low"] = (
        None
        if summary["range_filter_buy_bars_t0_to_pb1_low"] is None or t0_to_pb1_low_span is None
        else bool(summary["range_filter_buy_bars_t0_to_pb1_low"] == t0_to_pb1_low_span)
    )
    summary["both_all_buy_t0_to_pb1_low"] = (
        None
        if summary["both_buy_bars_t0_to_pb1_low"] is None or t0_to_pb1_low_span is None
        else bool(summary["both_buy_bars_t0_to_pb1_low"] == t0_to_pb1_low_span)
    )

    checkpoint_pairs = [
        ("rf_to_t0", t_rf, t0),
        ("t0_to_pb1_low", t0, t_pb1_low),
        ("pb1_low_to_reclaim", t_pb1_low, t_pb1_reclaim),
        ("t0_to_reclaim", t0, t_pb1_reclaim),
    ]
    for pair_name, start_snap, end_snap in checkpoint_pairs:
        for field in DELTA_FIELDS:
            start_value = None if not start_snap else _safe_float(start_snap.get(field))
            end_value = None if not end_snap else _safe_float(end_snap.get(field))
            summary[f"{field}_delta_{pair_name}"] = delta_value(start_value, end_value)

    if include_mtf:
        summary["mtf_context"] = build_mtf_context(
            df,
            checkpoint_indices=checkpoint_indices,
            checkpoint_snaps=checkpoint_snaps,
            mtf_streams=mtf_streams,
        )

    return summary


def build_scan_rows(df: pd.DataFrame, *, pullback_window: int, reclaim_window: int) -> pd.DataFrame:
    event_indices = df.index[df["vidya_trend_up"].fillna(False).astype(bool)].tolist()
    rows: List[Dict[str, Any]] = []
    for idx in event_indices:
        measured = measure_event(df, idx, pullback_window=pullback_window, reclaim_window=reclaim_window)
        rows.append(
            {
                "anchor_index": measured["anchor_index"],
                "t0_ts": measured["t0"]["ts"] if measured.get("t0") else None,
                "t_rf_ts": measured["t_rf"]["ts"] if measured.get("t_rf") else None,
                "bars_rf_to_t0": measured.get("bars_rf_to_t0"),
                "t_pb1_low_ts": measured["t_pb1_low"]["ts"] if measured.get("t_pb1_low") else None,
                "bars_t0_to_pb1_low": measured.get("bars_t0_to_pb1_low"),
                "t_pb1_reclaim_ts": measured["t_pb1_reclaim"]["ts"] if measured.get("t_pb1_reclaim") else None,
                "pb1_depth_vs_t0_wick_pct": measured.get("pb1_depth_vs_t0_wick_pct"),
                "pb1_depth_vs_t0_body_pct": measured.get("pb1_depth_vs_t0_body_pct"),
                "pb1_breaks_t0_low": measured.get("pb1_breaks_t0_low"),
                "pb1_depth_vs_rf_wick_pct": measured.get("pb1_depth_vs_rf_wick_pct"),
                "pb1_depth_vs_rf_body_pct": measured.get("pb1_depth_vs_rf_body_pct"),
                "pb1_breaks_rf_low": measured.get("pb1_breaks_rf_low"),
                "pb1_anchor_low_relation_class": measured.get("pb1_anchor_low_relation_class"),
                "pb1_pullback_survival_class": measured.get("pb1_pullback_survival_class"),
                "t_pb1_low_state_pair": measured.get("t_pb1_low_state_pair"),
                "range_filter_all_buy_rf_to_t0": measured.get("range_filter_all_buy_rf_to_t0"),
                "both_all_buy_t0_to_pb1_low": measured.get("both_all_buy_t0_to_pb1_low"),
                "move_up_before_pb1_abs": measured.get("move_up_before_pb1_abs"),
                "move_up_before_pb1_pct_of_t0_range": measured.get("move_up_before_pb1_pct_of_t0_range"),
                "pullback_from_peak_abs": measured.get("pullback_from_peak_abs"),
                "pullback_from_peak_pct_of_thrust": measured.get("pullback_from_peak_pct_of_thrust"),
                "reclaim_speed_class": measured.get("reclaim_speed_class"),
                "range_filter_state_t0": None if measured.get("t0") is None else measured["t0"].get("range_filter_state"),
                "vidya_state_t0": None if measured.get("t0") is None else measured["t0"].get("vidya_state"),
                "rsi_t0": None if measured.get("t0") is None else measured["t0"].get("rsi"),
                "stoch_k_t0": None if measured.get("t0") is None else measured["t0"].get("stoch_rsi_k"),
                "stoch_d_t0": None if measured.get("t0") is None else measured["t0"].get("stoch_rsi_d"),
                "macd_t0": None if measured.get("t0") is None else measured["t0"].get("macd"),
                "signal_t0": None if measured.get("t0") is None else measured["t0"].get("signal"),
                "adx_t0": None if measured.get("t0") is None else measured["t0"].get("adx"),
                "atr_t0": None if measured.get("t0") is None else measured["t0"].get("atr"),
            }
        )
    return pd.DataFrame(rows)


def select_window(df: pd.DataFrame, anchor_idx: int, before: int, after: int) -> pd.DataFrame:
    start = max(0, anchor_idx - before)
    stop = min(len(df), anchor_idx + after + 1)
    cols = [col for col in WINDOW_EXPORT_COLUMNS if col in df.columns]
    return df.iloc[start:stop][cols].copy()


def write_json(path: Path, payload: Dict[str, Any]) -> None:
    path.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Extract research metrics for trade-entry case analysis.")
    parser.add_argument("--symbol", default="BTCUSDT")
    parser.add_argument("--meta-id", default=None, help="Prepared dataset id, e.g. prep_8194f829636ce242 or prep_8194f829636ce242.meta.json")
    parser.add_argument("--case-dir", type=Path, required=True)
    parser.add_argument("--pullback-window", type=int, default=12)
    parser.add_argument("--reclaim-window", type=int, default=12)
    parser.add_argument("--window-before", type=int, default=12)
    parser.add_argument("--window-after", type=int, default=30)
    parser.add_argument("--event-index", type=int, action="append", default=[])
    parser.add_argument("--require-full-indicators", action="store_true")
    parser.add_argument("--require-timeframe", action="append", default=[], help="Require prepared meta to contain this timeframe, e.g. 5m")
    return parser


def main() -> int:
    args = build_parser().parse_args()

    root = repo_root()
    meta_dir = prepared_root(root, args.symbol)
    meta_path = choose_meta(
        meta_dir,
        args.meta_id,
        require_full=args.require_full_indicators,
        required_timeframes=args.require_timeframe,
    )
    meta = load_meta(meta_path)
    df = load_merged_dataset(meta_dir, meta)
    mtf_streams = {timeframe: load_stream_frame(meta_dir, meta, timeframe) for timeframe in MTF_TIMEFRAMES}

    case_dir = args.case_dir.resolve()
    exports_dir = case_dir / "exports"
    exports_dir.mkdir(parents=True, exist_ok=True)

    scan_df = build_scan_rows(df, pullback_window=args.pullback_window, reclaim_window=args.reclaim_window)
    scan_name = f"{meta_path.stem.replace('.meta', '')}_vidya_flip_scan.csv"
    scan_path = exports_dir / scan_name
    scan_df.to_csv(scan_path, index=False, encoding="utf-8")

    selected_indices: Iterable[int] = args.event_index or scan_df["anchor_index"].astype(int).tolist()
    summaries: List[Dict[str, Any]] = []
    for idx in selected_indices:
        measured = measure_event(
            df,
            int(idx),
            pullback_window=args.pullback_window,
            reclaim_window=args.reclaim_window,
            mtf_streams=mtf_streams,
            include_mtf=True,
        )
        summaries.append(measured)

        window_df = select_window(df, int(idx), before=args.window_before, after=args.window_after)
        window_path = exports_dir / f"{meta_path.stem.replace('.meta', '')}_event_{int(idx):03d}_window.csv"
        window_df.to_csv(window_path, index=False, encoding="utf-8")

        summary_path = exports_dir / f"{meta_path.stem.replace('.meta', '')}_event_{int(idx):03d}_summary.json"
        write_json(summary_path, measured)

    manifest = {
        "meta_path": str(meta_path),
        "meta": meta,
        "scan_csv": str(scan_path),
        "selected_event_indices": [int(idx) for idx in selected_indices],
        "event_count": int(len(scan_df)),
        "generated_summaries": len(summaries),
        "requested_timeframes": list(args.require_timeframe or []),
        "available_mtf_streams": [timeframe for timeframe, frame in mtf_streams.items() if frame is not None],
    }
    manifest_path = exports_dir / f"{meta_path.stem.replace('.meta', '')}_manifest.json"
    write_json(manifest_path, manifest)

    print(f"Prepared dataset: {meta_path.name}")
    print(f"Scan rows: {len(scan_df)} -> {scan_path}")
    print(f"Detailed events: {len(summaries)} -> {exports_dir}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
