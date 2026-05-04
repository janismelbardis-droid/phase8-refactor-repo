from __future__ import annotations

"""Backtest dataframe freezing, snapshot, and HTF stream helpers.

These helpers were extracted from :mod:`app.backtest` in the Phase 3
structural split so the engine module can keep runtime behavior while reducing
file size and separating snapshot mechanics from execution logic.
"""

from collections import OrderedDict
from typing import Any, Dict, Optional, Tuple

import numpy as np
import pandas as pd

from .rules import snapshot_from_stream_row
from .utils_time import datetime_like_to_epoch_ms, interval_to_ms

__all__ = [
    "HTF_EVENT_PULSE_COLUMNS",
    "_source_bar_ids_for_df",
    "_prev_source_row_index_for_df",
    "_freeze_backtest_frame",
    "_freeze_backtest_streams",
    "_RowSnapshotCache",
    "_snapshot_for_row",
    "_build_current_snapshots_for_row",
    "_build_eval_snapshots_for_row",
    "_range_audit_row",
    "make_streams_htf_closed_only",
]

# One-bar event pulse columns must *not* be forward-filled in HTF closed-only mode.
# They should only be true on the real source higher-timeframe close where the event fired.
HTF_EVENT_PULSE_COLUMNS = {
    "ema_cross_up",
    "ema_cross_down",
    "frama_break_up",
    "frama_break_down",
    "frama_mid_cross",
    "vidya_trend_up",
    "vidya_trend_down",
    "vidya_new_liq_low",
    "vidya_new_liq_high",
    "vidya_liq_low_taken",
    "vidya_liq_high_taken",
    "range_filter_buy",
    "range_filter_sell",
}

def _source_bar_ids_for_df(df: "pd.DataFrame", tf: str) -> Optional[np.ndarray]:
    """Return a stable source-bar identifier (ms) for every row in *df*.

    For ordinary aligned streams, this is the close time of the rule's *source timeframe* bar,
    not necessarily the current 1m-aligned row time. In HTF closed-only mode we prefer the
    explicit ``snapshot_bar_ms`` metadata because those rows intentionally stay frozen until the
    higher-timeframe bar closes. In forming/live-style streams we derive the source bar close from
    the aligned row index so mixed-timeframe EVENT logic can compare against the previous real
    source bar instead of the previous generic decision step.
    """
    if df is None or df.empty:
        return None
    idx = getattr(df, "index", None)
    if not isinstance(idx, pd.DatetimeIndex):
        return None

    explicit = None
    for col in ("snapshot_bar_ms", "bar_ms"):
        if col not in df.columns:
            continue
        try:
            s = pd.to_numeric(df[col], errors="coerce")
            arr = s.to_numpy(dtype=float, copy=False)
            if np.isfinite(arr).any():
                explicit = arr.copy()
                break
        except Exception:
            continue

    idx_ms = datetime_like_to_epoch_ms(idx)

    try:
        tf_ms = int(interval_to_ms(tf))
    except Exception:
        tf_ms = 60_000
    if tf_ms <= 0:
        tf_ms = 60_000

    # close_time-based series -> source TF close_time for the aligned row.
    derived = (((idx_ms + 1 + tf_ms - 1) // tf_ms) * tf_ms) - 1
    derived = derived.astype(np.int64, copy=False)

    if explicit is None:
        return derived

    mask = np.isfinite(explicit)
    out = derived.astype(np.int64, copy=True)
    try:
        out[mask] = explicit[mask].astype(np.int64, copy=False)
    except Exception:
        for i, ok in enumerate(mask):
            if ok:
                out[i] = int(explicit[i])
    return out


def _prev_source_row_index_for_df(df: "pd.DataFrame", tf: str) -> Optional[np.ndarray]:
    """For each row, return the last row index belonging to the previous distinct source bar.

    Example: for a 15m rule aligned to 1m rows, every 1m row inside the same 15m candle will map
    to the *final* row of the prior 15m candle. This is what EVENT/cross rules should compare
    against when the backtest step timeframe is lower than the rule timeframe.
    """
    ids = _source_bar_ids_for_df(df, tf)
    if ids is None:
        return None
    n = len(ids)
    prev_idx = np.full(n, -1, dtype=np.int64)
    if n == 0:
        return prev_idx

    current_id = int(ids[0])
    last_prev_row = -1
    prev_idx[0] = -1
    for j in range(1, n):
        bar_id = int(ids[j])
        if bar_id != current_id:
            last_prev_row = j - 1
            current_id = bar_id
        prev_idx[j] = last_prev_row
    return prev_idx




def _freeze_backtest_frame(df: Optional["pd.DataFrame"], *, end: Optional[pd.Timestamp] = None) -> Optional["pd.DataFrame"]:
    """Return a deterministic copy of a historical dataframe for backtests.

    Why this exists:
      - UI/live code can hold mutable dataframes that may be reused across runs.
      - duplicate / unsorted timestamps make previous-bar logic unstable.
      - when the selected end time is near the most recent candles, callers may still pass
        one extra bar beyond the requested historical window.

    The backtest should always evaluate a frozen, UTC-sorted, duplicate-free dataset.
    """
    if df is None:
        return None
    try:
        out = df.copy(deep=True)
    except Exception:
        try:
            out = df.copy()
        except Exception:
            return df

    try:
        if not isinstance(out.index, pd.DatetimeIndex):
            if "close_time" in out.columns:
                out.index = pd.to_datetime(out["close_time"], utc=True, errors="coerce")
            elif "open_time" in out.columns:
                out.index = pd.to_datetime(out["open_time"], utc=True, errors="coerce")
        else:
            if out.index.tz is None:
                out.index = out.index.tz_localize("UTC")
            else:
                out.index = out.index.tz_convert("UTC")
    except Exception:
        pass

    try:
        out = out[~pd.isna(out.index)]
    except Exception:
        pass
    try:
        out = out.sort_index(kind="stable")
    except Exception:
        try:
            out = out.sort_index()
        except Exception:
            pass
    try:
        out = out.loc[~out.index.duplicated(keep="last")]
    except Exception:
        pass

    if end is not None:
        try:
            end_utc = pd.to_datetime(end, utc=True)
            out = out.loc[out.index <= end_utc]
            if "close_time" in out.columns:
                ct = pd.to_datetime(out["close_time"], utc=True, errors="coerce")
                out = out.loc[(ct.isna()) | (ct <= end_utc)]
        except Exception:
            pass

    return out


def _freeze_backtest_streams(streams_full: Dict[str, "pd.DataFrame"], *, end: Optional[pd.Timestamp] = None) -> Dict[str, "pd.DataFrame"]:
    """Freeze every timeframe stream into a deterministic local copy."""
    out: Dict[str, pd.DataFrame] = {}
    for tf, dft in (streams_full or {}).items():
        frozen = _freeze_backtest_frame(dft, end=end)
        if frozen is not None and not frozen.empty:
            out[str(tf)] = frozen
    return out


class _RowSnapshotCache:
    """Small bounded cache for normalized dataframe-row snapshots.

    The backtest hot path frequently reuses the previous decision snapshot (and often the
    same higher-timeframe source bar) across adjacent steps. Caching a bounded number of
    normalized row snapshots cuts repeated Series->dict conversions without holding the
    whole dataset in memory.
    """

    def __init__(self, *, required_fields: Optional[Any] = None, max_rows_per_tf: int = 512):
        self.required_fields = required_fields
        self.max_rows_per_tf = max(16, int(max_rows_per_tf))
        self._by_tf: Dict[str, OrderedDict[int, Dict[str, Any]]] = {}

    def get(self, tf: str, dft: "pd.DataFrame", row_index: int) -> Dict[str, Any]:
        tfk = str(tf)
        idx = int(row_index)
        bucket = self._by_tf.setdefault(tfk, OrderedDict())
        if idx in bucket:
            snap = bucket.pop(idx)
            bucket[idx] = snap
            return snap
        snap = snapshot_from_stream_row(dft.iloc[idx], required_fields=self.required_fields)
        bucket[idx] = snap
        while len(bucket) > self.max_rows_per_tf:
            bucket.popitem(last=False)
        return snap


def _snapshot_for_row(tf: str,
                      dft: "pd.DataFrame",
                      row_index: int,
                      *,
                      snapshot_cache: Optional[_RowSnapshotCache] = None,
                      required_fields: Optional[Any] = None) -> Dict[str, Any]:
    try:
        if snapshot_cache is not None:
            return snapshot_cache.get(tf, dft, int(row_index))
        return snapshot_from_stream_row(dft.iloc[int(row_index)], required_fields=required_fields)
    except Exception:
        return {}


def _build_current_snapshots_for_row(subs: Dict[str, "pd.DataFrame"],
                                     row_index: int,
                                     *,
                                     snapshot_cache: Optional[_RowSnapshotCache] = None,
                                     required_fields: Optional[Any] = None) -> Dict[str, Dict[str, Any]]:
    out: Dict[str, Dict[str, Any]] = {}
    for tf, dft in subs.items():
        out[tf] = _snapshot_for_row(tf, dft, row_index, snapshot_cache=snapshot_cache, required_fields=required_fields)
    return out


def _build_eval_snapshots_for_row(subs: Dict[str, "pd.DataFrame"],
                                  row_index: int,
                                  prev_decision_j: Optional[int],
                                  prev_source_row_by_tf: Dict[str, Optional[np.ndarray]],
                                  step_tf: str,
                                  *,
                                  snapshot_cache: Optional[_RowSnapshotCache] = None,
                                  required_fields: Optional[Any] = None) -> Tuple[Dict[str, Dict[str, Any]], Dict[str, Dict[str, Any]]]:
    """Build current/previous snapshots for one decision row deterministically.

    The current snapshot is always taken from the frozen local dataframe row `row_index`.
    The previous snapshot for each rule timeframe prefers the previous *source timeframe* bar,
    not merely the previous generic decision step.
    """
    cur_by_tf: Dict[str, Dict[str, Any]] = {}
    prev_by_tf: Dict[str, Dict[str, Any]] = {}
    pj_default = int(prev_decision_j) if prev_decision_j is not None else (int(row_index) - 1)

    for tf, dft in subs.items():
        cur_by_tf[tf] = _snapshot_for_row(tf, dft, row_index, snapshot_cache=snapshot_cache, required_fields=required_fields)

        pj_tf = int(pj_default)
        try:
            prev_src_arr = prev_source_row_by_tf.get(tf)
            if prev_src_arr is not None and 0 <= int(row_index) < len(prev_src_arr):
                cand = int(prev_src_arr[int(row_index)])
                if cand >= 0:
                    pj_tf = cand
                elif str(tf) != str(step_tf):
                    pj_tf = -1
        except Exception:
            pass

        if pj_tf >= 0:
            prev_by_tf[tf] = _snapshot_for_row(tf, dft, pj_tf, snapshot_cache=snapshot_cache, required_fields=required_fields)
        else:
            prev_by_tf[tf] = {}

    return cur_by_tf, prev_by_tf


def _range_audit_row(step_time: Any,
                     tf: str,
                     cur_snap: Optional[Dict[str, Any]],
                     prev_snap: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    cur_snap = cur_snap or {}
    prev_snap = prev_snap or {}
    return {
        "decision_time": (pd.to_datetime(step_time, utc=True).isoformat() if step_time is not None else None),
        "timeframe": str(tf),
        "source_bar_time": str(cur_snap.get("snapshot_time") or cur_snap.get("time") or ""),
        "prev_source_bar_time": str(prev_snap.get("snapshot_time") or prev_snap.get("time") or ""),
        "state": cur_snap.get("range_filter_state"),
        "prev_state": prev_snap.get("range_filter_state"),
        "long_cond": cur_snap.get("range_filter_long_cond"),
        "short_cond": cur_snap.get("range_filter_short_cond"),
        "prev_cond_ini": prev_snap.get("range_filter_cond_ini"),
        "cond_ini": cur_snap.get("range_filter_cond_ini"),
        "buy_event": bool(cur_snap.get("range_filter_buy", False)),
        "sell_event": bool(cur_snap.get("range_filter_sell", False)),
        "bar_ms": cur_snap.get("bar_ms") or cur_snap.get("snapshot_bar_ms"),
        "prev_bar_ms": prev_snap.get("bar_ms") or prev_snap.get("snapshot_bar_ms"),
    }
def make_streams_htf_closed_only(streams_full: Dict[str, "pd.DataFrame"]) -> Dict[str, "pd.DataFrame"]:
    """Return a copy of *streams_full* where higher-TF indicator columns only update on TF closes.

    The project computes multi-TF streams using *forming-close previews* for MACD/PPO on higher
    timeframes (to match LIVE snapshots). For certain backtests, we instead want the higher-TF
    snapshots to be **closed-bar only**, i.e. values update only when that TF candle closes and
    remain constant between closes.

    We keep the 1m stream unchanged and forward-fill higher-TF indicator columns between TF
    boundaries. Price is left as-is (still tracks the 1m close), matching the existing engine.
    """
    if not streams_full or "1m" not in streams_full:
        return streams_full

    out: Dict[str, pd.DataFrame] = {}
    out["1m"] = streams_full["1m"]

    for tf, df in streams_full.items():
        if tf == "1m":
            continue
        if df is None or df.empty:
            continue

        try:
            tf_ms = int(interval_to_ms(tf))
        except Exception:
            tf_ms = 60_000

        df2 = df.copy()
        idx = df2.index
        if not isinstance(idx, pd.DatetimeIndex):
            out[tf] = df2
            continue

        # The 1m index is based on candle close_time. For Binance klines, close_time is typically
        # open_time + 60_000 - 1ms. The simulation commits TF bars based on (open_time + 60_000).
        # To replicate the same boundary condition from just close_time, use (close_ms + 1).
        idx_ms = datetime_like_to_epoch_ms(idx)

        boundary = (((idx_ms + 1) % tf_ms) == 0)

        # Preserve the *source* higher-timeframe bar identity for every 1m-aligned row.
        # Without this, snapshot_from_stream_row() falls back to the advancing 1m row index,
        # making bars-ago/history logic think a new HTF bar arrived every minute even though the
        # indicator values were intentionally frozen between true HTF closes.
        try:
            boundary_ms = pd.Series(np.where(boundary, idx_ms, np.nan), index=idx, dtype=float).ffill()
            df2["snapshot_bar_ms"] = boundary_ms
            df2["snapshot_time"] = pd.to_datetime(boundary_ms, unit="ms", utc=True, errors="coerce")
            df2["is_closed"] = boundary_ms.notna()
        except Exception:
            pass

        # Forward-fill state/numeric columns using only boundary values.
        #
        # Important: one-bar EVENT pulse columns must stay pulses. If we forward-fill them, a
        # real 15m event leaks onto later 1m/5m rows and the rule engine starts comparing against
        # repeated copied pulses instead of the true source HTF event bar. That can make some TV
        # badges disappear in mixed-timeframe backtests even when the underlying HTF indicator
        # values match perfectly.
        for col in list(df2.columns):
            if col in ("price", "snapshot_bar_ms", "snapshot_time", "is_closed"):
                continue
            try:
                s = df2[col]
                s2 = s.where(boundary)
                if col in HTF_EVENT_PULSE_COLUMNS:
                    arr = s2.to_numpy(copy=False)
                    arr = np.where(pd.isna(arr), False, arr).astype(bool, copy=False)
                    df2[col] = pd.Series(arr, index=df2.index)
                else:
                    df2[col] = s2.ffill()
            except Exception:
                # Never fail backtests due to a single column.
                pass

        out[tf] = df2

    return out
