from __future__ import annotations

import argparse
import csv
import io
import json
import math
import sys
import time
import urllib.parse
import urllib.request
import zipfile
from pathlib import Path
from typing import Any, Iterable

import numpy as np
import pandas as pd

REPO_ROOT = Path(__file__).resolve().parents[1]
WORKSPACE_ROOT = REPO_ROOT.parents[1] if len(REPO_ROOT.parents) > 1 else REPO_ROOT
CASEBOOK_ROOT = WORKSPACE_ROOT / "research_exports" / "trade_entry_casebook"
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


def _read_text(path: Path) -> str:
    if not path.exists():
        return ""
    return path.read_text(encoding="utf-8", errors="replace")


def _write_text(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")


def _write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def _to_utc(value: Any) -> pd.Timestamp | None:
    try:
        ts = pd.to_datetime(value, utc=True, errors="coerce")
    except Exception:
        return None
    if pd.isna(ts):
        return None
    return pd.Timestamp(ts)


def _finite(value: Any) -> float:
    try:
        out = float(value)
    except Exception:
        return float("nan")
    return out if math.isfinite(out) else float("nan")


def _json_safe(value: Any) -> Any:
    if isinstance(value, pd.Timestamp):
        return value.isoformat()
    if isinstance(value, np.generic):
        return value.item()
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


def _load_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _load_case_index() -> pd.DataFrame:
    path = CASEBOOK_ROOT / "CASE_INDEX.csv"
    if not path.exists():
        return pd.DataFrame()
    return pd.read_csv(path, dtype=str).fillna("")


def _load_case_report() -> dict[str, Any]:
    path = CASEBOOK_ROOT / "preset_drafts" / "backtest_case_period_0001_0005.json"
    if not path.exists():
        raise FileNotFoundError(path)
    return _load_json(path)


def _case_dir(case_index: pd.DataFrame, case_id: str) -> Path | None:
    if case_index.empty or "case_id" not in case_index.columns:
        return None
    rows = case_index.loc[case_index["case_id"].astype(str).str.zfill(4) == str(case_id).zfill(4)]
    if rows.empty:
        return None
    folder = str(rows.iloc[0].get("folder") or "").replace("/", "\\")
    path = CASEBOOK_ROOT / folder
    return path if path.exists() else None


def _choose_summary(case_dir: Path, case_t0: pd.Timestamp | None) -> Path | None:
    candidates = sorted((case_dir / "exports").glob("*_summary.json"))
    if not candidates:
        return None
    if case_t0 is None:
        return candidates[0]
    best_path: Path | None = None
    best_diff = float("inf")
    for path in candidates:
        try:
            payload = _load_json(path)
        except Exception:
            continue
        t0 = _to_utc((payload.get("t0") or {}).get("ts"))
        if t0 is None:
            continue
        diff = abs((t0 - case_t0).total_seconds())
        if diff < best_diff:
            best_diff = diff
            best_path = path
    return best_path or candidates[0]


def _window_for_summary(summary_path: Path) -> Path | None:
    candidate = summary_path.with_name(summary_path.name.replace("_summary.json", "_window.csv"))
    if candidate.exists():
        return candidate
    matches = sorted(summary_path.parent.glob("*_window.csv"))
    return matches[0] if matches else None


def _flatten_checkpoint(case_id: str, family: str, summary_path: Path, payload: dict[str, Any], checkpoint: str) -> dict[str, Any]:
    row = dict(payload.get(checkpoint) or {})
    out = {
        "case_id": case_id,
        "preset_family": family,
        "checkpoint": checkpoint,
        "source_summary": str(summary_path),
    }
    fields = [
        "index", "ts", "open", "high", "low", "close", "volume", "quote_volume", "trade_count",
        "rsi", "rsi_smooth", "rsi_state", "stoch_rsi_k", "stoch_rsi_d", "stoch_rsi_kd",
        "macd", "signal", "ms", "angle", "sig_angle", "ppo", "flip_age",
        "adx", "atr", "di_plus", "di_minus", "di_spread", "adx_angle", "atr_angle",
        "vidya_state", "vidya_trend_up", "vidya_trend_down", "vidya_line", "vidya_upper",
        "vidya_lower", "vidya_delta_pct", "range_filter_state", "range_filter_phase",
        "range_filter_buy", "range_filter_sell", "range_filter_line", "range_filter_upper",
        "range_filter_lower", "range_filter_smooth_range", "range_filter_cond_ini",
        "range_filter_upward_count", "range_filter_downward_count",
        "rsi_minus_smooth", "stoch_k_minus_d", "macd_hist", "state_pair",
    ]
    for field in fields:
        out[field] = row.get(field)
    return out


def _case_sequence_row(case: dict[str, Any], summary_path: Path | None, payload: dict[str, Any] | None) -> dict[str, Any]:
    out = {
        "case_id": str(case.get("case_id") or ""),
        "preset_family": str(case.get("preset_family") or ""),
        "t_rf_utc": str(case.get("t_rf_utc") or ""),
        "t0_utc": str(case.get("t0_utc") or ""),
        "window_end_utc": str(case.get("window_end_utc") or ""),
        "source_summary": str(summary_path or ""),
    }
    if not payload:
        return out
    fields = [
        "bars_rf_to_t0", "bars_t0_to_pb1_low", "bars_pb1_low_to_reclaim",
        "bars_rf_to_t0_inclusive", "bars_t0_to_pb1_low_inclusive",
        "pullback_window_bars", "reclaim_window_bars", "peak_high_before_pb1",
        "bars_t0_to_peak_high", "pb1_depth_vs_t0_wick_pct", "pb1_depth_vs_t0_body_pct",
        "pb1_penetrates_t0_body", "pb1_breaks_t0_low", "pb1_depth_vs_rf_wick_pct",
        "pb1_depth_vs_rf_body_pct", "pb1_penetrates_rf_body", "pb1_breaks_rf_low",
        "pb1_anchor_low_relation_class", "t0_state_pair", "t_pb1_low_state_pair",
        "t_pb1_reclaim_state_pair", "pb1_pullback_survival_class", "reclaim_speed_class",
        "t0_close_minus_rf_close_pct", "t0_low_minus_rf_low_pct",
        "pb1_low_minus_t0_low_pct", "pb1_low_minus_rf_low_pct",
        "peak_high_minus_t0_close_pct", "peak_high_minus_rf_close_pct",
    ]
    for field in fields:
        out[field] = payload.get(field)
    for checkpoint in ("t_rf", "t0", "t_pb1_low", "t_pb1_reclaim"):
        row = payload.get(checkpoint) or {}
        out[f"{checkpoint}_ts"] = row.get("ts")
        out[f"{checkpoint}_state_pair"] = row.get("state_pair")
        out[f"{checkpoint}_range_phase"] = row.get("range_filter_phase")
        out[f"{checkpoint}_range_state"] = row.get("range_filter_state")
        out[f"{checkpoint}_vidya_state"] = row.get("vidya_state")
        out[f"{checkpoint}_rsi"] = row.get("rsi")
        out[f"{checkpoint}_stoch_k"] = row.get("stoch_rsi_k")
        out[f"{checkpoint}_adx"] = row.get("adx")
        out[f"{checkpoint}_di_spread"] = row.get("di_spread")
    return out


def _build_casebook_tables(out_dir: Path) -> dict[str, Any]:
    case_index = _load_case_index()
    case_report = _load_case_report()
    cases = list(case_report.get("cases") or [])
    checkpoint_rows: list[dict[str, Any]] = []
    sequence_rows: list[dict[str, Any]] = []
    window_frames: list[pd.DataFrame] = []
    selected: list[dict[str, Any]] = []

    for case in cases:
        case_id = str(case.get("case_id") or "").zfill(4)
        family = str(case.get("preset_family") or "")
        cdir = _case_dir(case_index, case_id)
        t0 = _to_utc(case.get("t0_utc"))
        summary_path = _choose_summary(cdir, t0) if cdir is not None else None
        payload = _load_json(summary_path) if summary_path and summary_path.exists() else None
        sequence_rows.append(_case_sequence_row(case, summary_path, payload))
        if payload:
            for checkpoint in ("t_rf", "t0", "t_pb1_low", "t_pb1_reclaim"):
                checkpoint_rows.append(_flatten_checkpoint(case_id, family, summary_path, payload, checkpoint))
        window_path = _window_for_summary(summary_path) if summary_path else None
        if window_path and window_path.exists():
            frame = pd.read_csv(window_path)
            frame.insert(0, "case_id", case_id)
            frame.insert(1, "preset_family", family)
            frame.insert(2, "source_window", str(window_path))
            window_frames.append(frame)
        selected.append({
            "case_id": case_id,
            "preset_family": family,
            "case_dir": str(cdir or ""),
            "summary": str(summary_path or ""),
            "window": str(window_path or ""),
        })

    data_dir = out_dir / "data"
    data_dir.mkdir(parents=True, exist_ok=True)
    case_index.to_csv(data_dir / "case_index.csv", index=False)
    pd.DataFrame(sequence_rows).to_csv(data_dir / "case_sequence_matrix.csv", index=False)
    pd.DataFrame(checkpoint_rows).to_csv(data_dir / "case_checkpoint_matrix.csv", index=False)
    if window_frames:
        pd.concat(window_frames, ignore_index=True, sort=False).to_csv(data_dir / "case_windows_combined.csv", index=False)
    else:
        pd.DataFrame().to_csv(data_dir / "case_windows_combined.csv", index=False)
    _write_json(data_dir / "selected_case_sources.json", _json_safe(selected))
    return {
        "case_count": len(cases),
        "checkpoint_rows": len(checkpoint_rows),
        "selected_sources": selected,
    }


def _http_json(url: str, params: dict[str, Any] | None = None) -> Any:
    if params:
        url = url + "?" + urllib.parse.urlencode(params)
    req = urllib.request.Request(url, headers={"User-Agent": "btresource-ai-pack/1.0"})
    with urllib.request.urlopen(req, timeout=30) as resp:
        return json.loads(resp.read().decode("utf-8"))


def _server_time_utc() -> pd.Timestamp:
    try:
        payload = _http_json("https://fapi.binance.com/fapi/v1/time")
        return pd.to_datetime(int(payload["serverTime"]), unit="ms", utc=True).floor("min")
    except Exception:
        return pd.Timestamp.now(tz="UTC").floor("min")


def _fetch_futures_klines(symbol: str, start: pd.Timestamp, end: pd.Timestamp) -> pd.DataFrame:
    rows: list[list[Any]] = []
    start_ms = int(start.timestamp() * 1000)
    end_ms = int(end.timestamp() * 1000)
    while start_ms <= end_ms:
        payload = _http_json(
            "https://fapi.binance.com/fapi/v1/klines",
            {
                "symbol": symbol,
                "interval": "1m",
                "startTime": start_ms,
                "endTime": end_ms,
                "limit": 1500,
            },
        )
        if not payload:
            break
        rows.extend(payload)
        last_open = int(payload[-1][0])
        next_start = last_open + 60_000
        if next_start <= start_ms:
            break
        start_ms = next_start
        time.sleep(0.05)
    if not rows:
        return pd.DataFrame()
    cols = [
        "open_time_ms", "open", "high", "low", "close", "volume",
        "close_time_ms", "quote_volume", "trade_count",
        "taker_buy_volume", "taker_buy_quote_volume", "ignore",
    ]
    df = pd.DataFrame(rows, columns=cols)
    df["open_time"] = pd.to_datetime(df["open_time_ms"], unit="ms", utc=True)
    df["close_time"] = pd.to_datetime(df["close_time_ms"], unit="ms", utc=True)
    for col in ("open", "high", "low", "close", "volume", "quote_volume", "taker_buy_volume", "taker_buy_quote_volume"):
        df[col] = pd.to_numeric(df[col], errors="coerce")
    df["trade_count"] = pd.to_numeric(df["trade_count"], errors="coerce")
    keep = [
        "open_time", "close_time", "open", "high", "low", "close", "volume",
        "quote_volume", "trade_count", "taker_buy_volume", "taker_buy_quote_volume",
    ]
    return df[keep].drop_duplicates("close_time").sort_values("close_time").reset_index(drop=True)


def _ema(values: Iterable[float], length: int) -> np.ndarray:
    arr = np.asarray(list(values), dtype=float)
    out = np.full(arr.size, np.nan, dtype=float)
    alpha = 2.0 / (float(length) + 1.0)
    prev = np.nan
    for i, value in enumerate(arr):
        if not np.isfinite(value):
            out[i] = prev
            continue
        prev = value if not np.isfinite(prev) else (alpha * value + (1.0 - alpha) * prev)
        out[i] = prev
    return out


def _range_filter(close: np.ndarray, per: int = 100, mult: float = 3.0) -> pd.DataFrame:
    c = np.asarray(close, dtype=float)
    n = len(c)
    abs_delta = np.r_[np.nan, np.abs(np.diff(c))]
    avrng = _ema(abs_delta, per)
    smooth = _ema(avrng, per * 2 - 1)
    smrng = smooth * float(mult)
    filt = np.full(n, np.nan)
    upward = np.zeros(n)
    downward = np.zeros(n)
    phase: list[str] = []
    state: list[str] = []
    long_cond = np.zeros(n, dtype=bool)
    short_cond = np.zeros(n, dtype=bool)
    cond_ini = np.zeros(n)
    buy = np.zeros(n, dtype=bool)
    sell = np.zeros(n, dtype=bool)
    prev_filt = np.nan
    prev_cond = 0
    for i, x in enumerate(c):
        prev_filt_nz = prev_filt if np.isfinite(prev_filt) else 0.0
        if np.isfinite(x) and np.isfinite(smrng[i]):
            if x > prev_filt_nz:
                cur_filt = prev_filt_nz if (x - smrng[i]) < prev_filt_nz else x - smrng[i]
            else:
                cur_filt = prev_filt_nz if (x + smrng[i]) > prev_filt_nz else x + smrng[i]
        else:
            cur_filt = np.nan
        if np.isfinite(cur_filt) and np.isfinite(prev_filt):
            upward[i] = upward[i - 1] + 1 if i > 0 and cur_filt > prev_filt else (0 if cur_filt < prev_filt else upward[i - 1])
            downward[i] = downward[i - 1] + 1 if i > 0 and cur_filt < prev_filt else (0 if cur_filt > prev_filt else downward[i - 1])
        elif i > 0:
            upward[i] = upward[i - 1]
            downward[i] = downward[i - 1]
        prev_src = c[i - 1] if i > 0 else np.nan
        buy_strong = np.isfinite(x) and np.isfinite(cur_filt) and x > cur_filt and upward[i] > 0 and np.isfinite(prev_src) and x > prev_src
        buy_weak = np.isfinite(x) and np.isfinite(cur_filt) and x > cur_filt and upward[i] > 0 and np.isfinite(prev_src) and x < prev_src
        sell_strong = np.isfinite(x) and np.isfinite(cur_filt) and x < cur_filt and downward[i] > 0 and np.isfinite(prev_src) and x < prev_src
        sell_weak = np.isfinite(x) and np.isfinite(cur_filt) and x < cur_filt and downward[i] > 0 and np.isfinite(prev_src) and x > prev_src
        lc = bool(buy_strong or buy_weak)
        sc = bool(sell_strong or sell_weak)
        cur_phase = "BUY_STRONG" if buy_strong else ("BUY_WEAK" if buy_weak else ("SELL_STRONG" if sell_strong else ("SELL_WEAK" if sell_weak else "NEUTRAL")))
        cur_cond = 1 if lc else (-1 if sc else prev_cond)
        buy[i] = lc and prev_cond == -1
        sell[i] = sc and prev_cond == 1
        filt[i] = cur_filt
        long_cond[i] = lc
        short_cond[i] = sc
        cond_ini[i] = cur_cond
        phase.append(cur_phase)
        state.append("BUY" if lc else ("SELL" if sc else "NEUTRAL"))
        prev_filt = cur_filt
        prev_cond = int(cur_cond)
    return pd.DataFrame({
        "range_filter_line": filt,
        "range_filter_upper": filt + smrng,
        "range_filter_lower": filt - smrng,
        "range_filter_smooth_range": smrng,
        "range_filter_state": state,
        "range_filter_phase": phase,
        "range_filter_buy": buy,
        "range_filter_sell": sell,
        "range_filter_long_cond": long_cond,
        "range_filter_short_cond": short_cond,
        "range_filter_cond_ini": cond_ini,
        "range_filter_upward_count": upward,
        "range_filter_downward_count": downward,
    })


def _pullback_kind(frame: pd.DataFrame, idx: int, side: str) -> dict[str, Any]:
    start = max(0, idx - 180)
    prev = idx - 1
    neutral_run = 0
    cursor = prev
    while cursor >= 0 and str(frame.at[cursor, "range_filter_state"]) == "NEUTRAL":
        neutral_run += 1
        cursor -= 1
    side_state = "BUY" if side == "LONG" else "SELL"
    pre_state = str(frame.at[cursor, "range_filter_state"]) if cursor >= 0 else ""
    neutral_seen_60 = bool(frame.loc[max(0, idx - 60): max(0, idx - 1), "range_filter_state"].eq("NEUTRAL").any()) if idx > 0 else False
    if neutral_run > 0 and pre_state == side_state:
        kind = "SAME_SIDE_NEUTRAL_RECLAIM"
    elif neutral_run > 0 and pre_state and pre_state != side_state:
        kind = "REVERSAL_AFTER_NEUTRAL"
    elif neutral_run > 0:
        kind = "FRESH_FROM_NEUTRAL"
    elif neutral_seen_60:
        kind = "NEUTRAL_RECENT_CONTINUATION"
    else:
        kind = "NO_NEUTRAL_RECENT"
    return {
        "range_pullback_kind": kind,
        "range_neutral_run_before_signal": neutral_run,
        "range_neutral_seen_60m": neutral_seen_60,
        "range_neutral_count_180m": int(frame.loc[start: max(0, idx - 1), "range_filter_state"].eq("NEUTRAL").sum()) if idx > 0 else 0,
        "range_pre_neutral_state": pre_state,
    }


def _forward_outcomes(df: pd.DataFrame, idx: int, side: str, horizons: Iterable[int]) -> dict[str, Any]:
    entry_idx = idx + 1
    out: dict[str, Any] = {}
    if entry_idx >= len(df):
        return out
    entry = _finite(df.at[entry_idx, "open"])
    for horizon in horizons:
        end_idx = min(len(df) - 1, entry_idx + int(horizon))
        chunk = df.iloc[entry_idx: end_idx + 1]
        if chunk.empty or not np.isfinite(entry) or entry <= 0:
            out[f"mfe_{horizon}m_pct"] = None
            out[f"mae_{horizon}m_pct"] = None
            out[f"close_{horizon}m_pct"] = None
            continue
        if side == "LONG":
            mfe = (float(chunk["high"].max()) - entry) / entry * 100.0
            mae = (float(chunk["low"].min()) - entry) / entry * 100.0
            close_ret = (float(df.at[end_idx, "close"]) - entry) / entry * 100.0
        else:
            mfe = (entry - float(chunk["low"].min())) / entry * 100.0
            mae = (entry - float(chunk["high"].max())) / entry * 100.0
            close_ret = (entry - float(df.at[end_idx, "close"])) / entry * 100.0
        out[f"mfe_{horizon}m_pct"] = mfe
        out[f"mae_{horizon}m_pct"] = mae
        out[f"close_{horizon}m_pct"] = close_ret
    return out


def _build_recent_event_table(out_dir: Path, symbol: str, recent_days: int, warmup_days: int) -> dict[str, Any]:
    now = _server_time_utc()
    event_start = now.floor("D") - pd.Timedelta(days=max(1, int(recent_days) - 1))
    fetch_start = event_start - pd.Timedelta(days=max(1, int(warmup_days)))
    raw = _fetch_futures_klines(symbol, fetch_start, now)
    data_dir = out_dir / "data"
    raw_path = data_dir / f"recent_raw_{symbol.lower()}_1m.csv"
    events_path = data_dir / f"recent_range_events_{symbol.lower()}_1m.csv"
    if raw.empty:
        pd.DataFrame().to_csv(raw_path, index=False)
        pd.DataFrame().to_csv(events_path, index=False)
        return {"recent_raw_rows": 0, "recent_event_rows": 0, "recent_fetch_error": "empty"}

    raw.to_csv(raw_path, index=False)
    rf = _range_filter(raw["close"].to_numpy(dtype=float))
    raw = pd.concat([raw.reset_index(drop=True), rf], axis=1)
    raw["ema20"] = _ema(raw["close"], 20)
    raw["ema50"] = _ema(raw["close"], 50)
    raw["ema200"] = _ema(raw["close"], 200)
    raw["ema_stack_20_200"] = np.where(raw["ema20"] > raw["ema200"], "BUY", np.where(raw["ema20"] < raw["ema200"], "SELL", "NEUTRAL"))
    raw["taker_sell_volume"] = raw["volume"] - raw["taker_buy_volume"]
    raw["taker_delta_pct"] = np.where(raw["volume"] > 0, (raw["taker_buy_volume"] - raw["taker_sell_volume"]) / raw["volume"] * 100.0, np.nan)
    raw["taker_bias_state"] = np.where(raw["taker_delta_pct"] > 5, "BUY", np.where(raw["taker_delta_pct"] < -5, "SELL", "NEUTRAL"))
    vidya_status = "not_computed"
    try:
        from app.indicators_streaming import compute_vidya_series

        vid = compute_vidya_series(
            raw["open"].to_numpy(dtype=float),
            raw["high"].to_numpy(dtype=float),
            raw["low"].to_numpy(dtype=float),
            raw["close"].to_numpy(dtype=float),
            raw["volume"].to_numpy(dtype=float),
        )
        for key, values in vid.items():
            if key in {
                "vidya_line", "vidya_upper", "vidya_lower", "vidya_state",
                "vidya_trend_up", "vidya_trend_down", "vidya_delta_pct",
            }:
                raw[key] = values
        vidya_status = "computed_with_project_formula"
    except Exception as exc:
        raw["vidya_state"] = ""
        raw["vidya_trend_up"] = False
        raw["vidya_trend_down"] = False
        raw["vidya_delta_pct"] = np.nan
        raw["vidya_line"] = np.nan
        vidya_status = f"error:{type(exc).__name__}:{exc}"

    rows: list[dict[str, Any]] = []
    bool_specs = [
        ("range_filter_buy", "LONG", "range_filter_buy"),
        ("range_filter_sell", "SHORT", "range_filter_sell"),
    ]
    for idx, row in raw.iterrows():
        ts = pd.Timestamp(row["close_time"])
        if ts < event_start or ts > now:
            continue
        for field, side, trigger in bool_specs:
            if not bool(row.get(field, False)):
                continue
            entry_idx = int(idx) + 1
            if entry_idx >= len(raw):
                continue
            rec = {
                "symbol": symbol,
                "signal_time": str(ts),
                "entry_time": str(raw.at[entry_idx, "open_time"]),
                "side": side,
                "trigger_type": trigger,
                "signal_close": row.get("close"),
                "entry_open": raw.at[entry_idx, "open"],
                "range_state": row.get("range_filter_state"),
                "range_phase": row.get("range_filter_phase"),
                "ema_stack_20_200": row.get("ema_stack_20_200"),
                "ema20": row.get("ema20"),
                "ema50": row.get("ema50"),
                "ema200": row.get("ema200"),
                "vidya_state": row.get("vidya_state"),
                "vidya_trend_up": row.get("vidya_trend_up"),
                "vidya_trend_down": row.get("vidya_trend_down"),
                "vidya_delta_pct": row.get("vidya_delta_pct"),
                "vidya_line": row.get("vidya_line"),
                "taker_delta_pct": row.get("taker_delta_pct"),
                "taker_bias_state": row.get("taker_bias_state"),
            }
            rec.update(_pullback_kind(raw, int(idx), side))
            rec.update(_forward_outcomes(raw, int(idx), side, (30, 60, 120, 240)))
            rows.append(rec)
    pd.DataFrame(rows).to_csv(events_path, index=False)
    return {
        "recent_raw_rows": int(len(raw)),
        "recent_event_rows": int(len(rows)),
        "recent_event_start_utc": str(event_start),
        "recent_now_utc": str(now),
        "recent_vidya_status": vidya_status,
    }


def _build_notes(out_dir: Path) -> None:
    notes = [
        ("cognitive_event_layer_v1.md", CASEBOOK_ROOT / "cognitive_event_layer_v1.md"),
        ("case_window_trade_audit.md", CASEBOOK_ROOT / "presentation" / "case_window_0001_0005_trade_audit" / "case_window_0001_0005_trade_audit.md"),
        ("dual_window_candidate_v1_summary.md", CASEBOOK_ROOT / "dual_window_candidate_v1" / "dual_window_candidate_v1_summary.md"),
        ("entry_depth_confirmation_search_summary.md", CASEBOOK_ROOT / "entry_depth_confirmation_search_v1" / "summary.md"),
        ("failed_pullback_abort_sweep_summary.md", CASEBOOK_ROOT / "failed_pullback_abort_sweep_v1" / "summary.md"),
    ]
    combined: list[str] = []
    notes_dir = out_dir / "notes"
    notes_dir.mkdir(parents=True, exist_ok=True)
    for name, src in notes:
        text = _read_text(src)
        if not text:
            continue
        _write_text(notes_dir / name, text)
        combined.append(f"\n\n# Source: {name}\n\n{text}")
    _write_text(notes_dir / "prior_research_notes_combined.md", "\n".join(combined).strip() + "\n")


def _build_prompt(out_dir: Path) -> None:
    prompt = """You are a trading research partner. Analyze this ZIP as an intermediate AI research layer, not as a local backtest project.

Goal:
Build actionable playbook families for BTCUSDT 1m trading around Range Filter events combined with VIDYA, EMA context, and VD/taker pressure.

Files to use first:
- data/case_sequence_matrix.csv
- data/case_checkpoint_matrix.csv
- data/case_windows_combined.csv
- data/recent_range_events_btcusdt_1m.csv if present
- notes/cognitive_event_layer_v1.md
- notes/prior_research_notes_combined.md

Important framing:
- Do not assume one strategy is enough.
- Treat Range events as hypotheses, not automatic entries.
- Pullback means Range state/phase returning to NEUTRAL after a directional seed.
- Decide where hard neutral pullback is required and where it is harmful.
- Separate P1, P2, P3, and reject/no-trade families.

Required output:
1. A concise diagnosis of what the five successful manual cases have in common.
2. A table of 3-5 playbook families with:
   - entry sequence
   - mandatory context
   - optional context
   - rejection rules
   - exit/de-risk ideas
3. Classify recent_range_events rows into P1/P2/P3/reject if the file exists.
4. Identify which missing features would improve classification most.
5. Produce a JSON-like rule spec that can later be translated into the local engine.
6. Be honest about overfitting: distinguish strong evidence from hypotheses.

Do not just optimize parameters. Think like a discretionary quant researcher converting observed sequences into testable strategy grammar.
"""
    _write_text(out_dir / "PROMPT_FOR_CHATGPT.md", prompt)

    feature_dictionary = """# Feature Dictionary

## Main Tables

- `case_sequence_matrix.csv`: one row per manual case, with timing, pullback depth, state-pair survival, and reclaim quality.
- `case_checkpoint_matrix.csv`: one row per checkpoint per case: `t_rf`, `t0`, `t_pb1_low`, `t_pb1_reclaim`.
- `case_windows_combined.csv`: 1m local windows around each manual case, already containing project-computed indicators.
- `recent_range_events_btcusdt_1m.csv`: lightweight recent Range Filter events for yesterday/today, with EMA, VIDYA, VD/taker pressure, and forward MFE/MAE outcomes.

## Key Concepts

- `T_rf`: prior Range Filter seed event.
- `T0`: VIDYA regime-change anchor.
- `T_pb1_low`: first pullback low after T0.
- `T_pb1_reclaim`: first reclaim after pullback low.
- `state_pair`: compact `VIDYA_*__RF_*` state.
- `pb1_anchor_low_relation_class`: whether pullback preserved or broke the two anchor candles.
- `pb1_pullback_survival_class`: whether VIDYA/Range regime survived the pullback.
- `reclaim_speed_class`: how quickly price reclaimed after pullback.
- `range_pullback_kind`: recent event label based on Range NEUTRAL behavior before the signal.

## Current Working Families

- P1: deep neutral reclaim. Hard pullback is required.
- P2: fast range reclaim / sweep. Hard pullback is not mandatory and can be harmful.
- P3: second reclaim after a failed first attempt. Often useful when higher context is still damaged.
"""
    _write_text(out_dir / "feature_dictionary.md", feature_dictionary)

    readme = """# BTCUSDT Range + VIDYA + EMA + VD AI Research Pack

This pack is designed for server-side AI analysis in ChatGPT Advanced Data Analysis or OpenAI Code Interpreter.

It is intentionally compact. The laptop should only create this pack; the AI analysis should happen from these tables.

## Manual ChatGPT Workflow

1. Upload the ZIP file.
2. Open `PROMPT_FOR_CHATGPT.md`.
3. Ask ChatGPT to follow that prompt and inspect the CSV/MD files.

## API Workflow

Use `tools/submit_ai_research_pack.py` from the repo root:

```powershell
$env:OPENAI_API_KEY="..."
.\\.venv\\Scripts\\python.exe tools\\submit_ai_research_pack.py --zip <this_zip> --model gpt-5.5 --memory-limit 4g
```

If the OpenAI Python package is missing:

```powershell
.\\.venv\\Scripts\\python.exe -m pip install openai
```
"""
    _write_text(out_dir / "README.md", readme)


def _zip_dir(out_dir: Path) -> Path:
    zip_path = out_dir.with_suffix(".zip")
    if zip_path.exists():
        zip_path.unlink()
    with zipfile.ZipFile(zip_path, "w", compression=zipfile.ZIP_DEFLATED) as zf:
        for path in sorted(out_dir.rglob("*")):
            if path.is_file():
                zf.write(path, path.relative_to(out_dir))
    return zip_path


def build(args: argparse.Namespace) -> int:
    out_dir = Path(args.output_dir) if args.output_dir else WORKSPACE_ROOT / "research_exports" / "ai_research_pack_btc_range_vidya_v1"
    out_dir = out_dir.resolve()
    out_dir.mkdir(parents=True, exist_ok=True)

    case_meta = _build_casebook_tables(out_dir)
    recent_meta: dict[str, Any] = {}
    if bool(args.include_recent):
        try:
            recent_meta = _build_recent_event_table(out_dir, str(args.symbol).upper(), int(args.recent_days), int(args.recent_warmup_days))
        except Exception as exc:
            recent_meta = {"recent_error": f"{type(exc).__name__}: {exc}"}
            pd.DataFrame().to_csv(out_dir / "data" / f"recent_range_events_{str(args.symbol).lower()}_1m.csv", index=False)

    _build_notes(out_dir)
    _build_prompt(out_dir)
    manifest = {
        "pack": "btc_range_vidya_ema_vd_ai_research_pack",
        "created_utc": str(pd.Timestamp.now(tz="UTC")),
        "repo_root": str(REPO_ROOT),
        "casebook_root": str(CASEBOOK_ROOT),
        "purpose": "Upload to ChatGPT Advanced Data Analysis or OpenAI Code Interpreter for server-side research reasoning.",
        **case_meta,
        **recent_meta,
    }
    _write_json(out_dir / "manifest.json", _json_safe(manifest))
    zip_path = _zip_dir(out_dir)
    print(f"pack_dir={out_dir}")
    print(f"zip={zip_path}")
    print(json.dumps(_json_safe(manifest), ensure_ascii=False, indent=2))
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description="Build a compact server-side AI research pack.")
    parser.add_argument("--output-dir", default="")
    parser.add_argument("--symbol", default="BTCUSDT")
    parser.add_argument("--include-recent", action=argparse.BooleanOptionalAction, default=True)
    parser.add_argument("--recent-days", type=int, default=2)
    parser.add_argument("--recent-warmup-days", type=int, default=7)
    return build(parser.parse_args())


if __name__ == "__main__":
    raise SystemExit(main())
