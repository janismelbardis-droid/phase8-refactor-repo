from __future__ import annotations

import json
import math
import shutil
import sys
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any, Optional

import numpy as np
import pandas as pd


REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from app.indicators_streaming import compute_range_filter_series, compute_vidya_series


DEFAULT_OUTPUT_ROOT = REPO_ROOT / "runs" / "perf"
LATEST_MIRROR = REPO_ROOT / "runs" / "perf" / "probe_vidya_rf_mfi_deep_pullback_latest"
LOCAL_DAY_STORE = REPO_ROOT / "data_cache" / "ohlcv_store" / "BTCUSDT" / "LAST" / "1m"


@dataclass(frozen=True)
class Variant:
    name: str
    trigger_mode: str = "neutral_confirm"
    side_mode: str = "both"
    session_mode: str = "all"
    require_ctx5_align: bool = False
    require_ctx5_rf_not_opposite: bool = False
    min_reset_bars: int = 2
    max_reset_bars: int = 16
    min_neutral_bars: int = 1
    require_counter_weak: bool = False
    max_bars_since_regime: int = 90
    deep_touch_min_atr: float = -0.60
    deep_touch_max_atr: float = 0.20
    max_entry_gap_atr: float = 0.80
    require_mfi_rebound: bool = False
    require_state_flush: bool = False
    require_mfi_flush: bool = False
    require_overshoot_reclaim: bool = False
    reset_mfi_long_max: float = 40.0
    entry_mfi_long_min: float = 48.0
    reset_mfi_short_min: float = 60.0
    entry_mfi_short_max: float = 52.0
    be_arm_r: float = 0.80
    be_mfi_long: float = 65.0
    be_mfi_short: float = 35.0
    stop_buffer_atr: float = 0.10
    take_profit_r: Optional[float] = None
    partial_tp_fraction: float = 1.0
    max_hold_bars: int = 240


def _load_local_1m(start: pd.Timestamp, end: pd.Timestamp) -> pd.DataFrame:
    frames: list[pd.DataFrame] = []
    for path in sorted(LOCAL_DAY_STORE.glob("BTCUSDT_LAST_*.parquet")):
        try:
            day = pd.read_parquet(path, columns=["open_time", "open", "high", "low", "close", "volume"])
        except Exception:
            continue
        day["open_time"] = pd.to_datetime(day["open_time"], utc=True)
        day = day[(day["open_time"] >= start) & (day["open_time"] <= end)]
        if not day.empty:
            frames.append(day)
    if not frames:
        return pd.DataFrame(columns=["open_time", "open", "high", "low", "close", "volume"])
    out = pd.concat(frames, ignore_index=True)
    out = out.sort_values("open_time").drop_duplicates("open_time").reset_index(drop=True)
    return out


def _atr_rma(frame: pd.DataFrame, length: int = 14) -> pd.Series:
    high = frame["high"]
    low = frame["low"]
    close = frame["close"]
    prev_close = close.shift(1)
    tr = pd.concat(
        [
            high - low,
            (high - prev_close).abs(),
            (low - prev_close).abs(),
        ],
        axis=1,
    ).max(axis=1)
    return tr.ewm(alpha=1.0 / float(length), adjust=False).mean()


def _compute_mfi(frame: pd.DataFrame, length: int = 14) -> pd.Series:
    tp = (frame["high"] + frame["low"] + frame["close"]) / 3.0
    raw = tp * frame["volume"]
    prev_tp = tp.shift(1)
    pos = raw.where(tp > prev_tp, 0.0)
    neg = raw.where(tp < prev_tp, 0.0)
    pos_sum = pos.rolling(length, min_periods=length).sum()
    neg_sum = neg.rolling(length, min_periods=length).sum()
    ratio = pos_sum / neg_sum.replace(0.0, np.nan)
    mfi = 100.0 - (100.0 / (1.0 + ratio))
    mfi = mfi.where(~((neg_sum == 0.0) & (pos_sum > 0.0)), 100.0)
    mfi = mfi.where(~((pos_sum == 0.0) & (neg_sum > 0.0)), 0.0)
    mfi = mfi.fillna(50.0)
    return mfi.astype(float)


def _prepare_frame(raw: pd.DataFrame) -> pd.DataFrame:
    frame = raw.copy().reset_index(drop=True)
    vid = compute_vidya_series(
        frame["open"].to_numpy(dtype=float),
        frame["high"].to_numpy(dtype=float),
        frame["low"].to_numpy(dtype=float),
        frame["close"].to_numpy(dtype=float),
        frame["volume"].to_numpy(dtype=float),
    )
    rf = compute_range_filter_series(frame["close"].to_numpy(dtype=float), per=100, mult=3.0)
    frame["ts"] = pd.to_datetime(frame["open_time"], utc=True)
    frame["atr14"] = _atr_rma(frame)
    frame["mfi14"] = _compute_mfi(frame, length=14)

    for key in ("vidya_state", "vidya_line", "vidya_upper", "vidya_lower", "vidya_trend_up", "vidya_trend_down", "vidya_delta_pct"):
        frame[key] = vid[key]
    for key in (
        "range_filter_state",
        "range_filter_phase",
        "range_filter_buy",
        "range_filter_sell",
        "range_filter_line",
        "range_filter_long_cond",
        "range_filter_short_cond",
    ):
        frame[key] = rf[key]

    states = frame["vidya_state"].astype(str).fillna("")
    regime_start: list[int] = []
    current_start = 0
    prev = ""
    for i, state in enumerate(states):
        if i == 0 or state != prev:
            current_start = i
            prev = state
        regime_start.append(current_start)
    frame["vidya_regime_start_idx"] = regime_start

    frame_5m = (
        frame.set_index("ts")
        .resample("5min", label="left", closed="left")
        .agg({"open": "first", "high": "max", "low": "min", "close": "last", "volume": "sum"})
        .dropna()
        .reset_index()
    )
    vid5 = compute_vidya_series(
        frame_5m["open"].to_numpy(dtype=float),
        frame_5m["high"].to_numpy(dtype=float),
        frame_5m["low"].to_numpy(dtype=float),
        frame_5m["close"].to_numpy(dtype=float),
        frame_5m["volume"].to_numpy(dtype=float),
    )
    rf5 = compute_range_filter_series(frame_5m["close"].to_numpy(dtype=float), per=100, mult=3.0)
    frame_5m["ctx5_open"] = pd.to_datetime(frame_5m["ts"], utc=True)
    frame_5m["ctx5_vidya_state"] = vid5["vidya_state"]
    frame_5m["ctx5_range_state"] = rf5["range_filter_state"]
    frame_5m["ctx5_range_phase"] = rf5["range_filter_phase"]

    frame["ctx5_open"] = (frame["ts"] + pd.Timedelta(minutes=1)).dt.floor("5min") - pd.Timedelta(minutes=5)
    frame = frame.merge(frame_5m[["ctx5_open", "ctx5_vidya_state", "ctx5_range_state", "ctx5_range_phase"]], on="ctx5_open", how="left")
    return frame


def _utc_session_ok(ts: pd.Timestamp, mode: str) -> bool:
    hour = int(ts.hour)
    if mode == "all":
        return True
    if mode == "overlap":
        return 11 <= hour < 16
    if mode == "london":
        return 6 <= hour < 11
    if mode == "ny":
        return 13 <= hour < 20
    return True


def _reset_window(frame: pd.DataFrame, signal_idx: int, side: str, max_reset_bars: int) -> Optional[dict[str, Any]]:
    target_state = "BUY" if side == "LONG" else "SELL"
    j = signal_idx - 1
    bars = 0
    while j >= 0 and bars < max_reset_bars:
        state = str(frame.at[j, "range_filter_state"] or "")
        if state == target_state:
            break
        bars += 1
        j -= 1
    start = j + 1
    end = signal_idx - 1
    if start > end:
        return None
    window = frame.iloc[start : end + 1]
    neutral_bars = int(window["range_filter_state"].astype(str).eq("NEUTRAL").sum())
    phases = window["range_filter_phase"].astype(str).tolist()
    if side == "LONG":
        counter_weak = any(p == "SELL_WEAK" for p in phases)
        counter_strong = any(p == "SELL_STRONG" for p in phases)
        extreme_idx = int(window["low"].astype(float).idxmin())
        extreme_price = float(frame.at[extreme_idx, "low"])
        reset_mfi_extreme = float(window["mfi14"].min())
    else:
        counter_weak = any(p == "BUY_WEAK" for p in phases)
        counter_strong = any(p == "BUY_STRONG" for p in phases)
        extreme_idx = int(window["high"].astype(float).idxmax())
        extreme_price = float(frame.at[extreme_idx, "high"])
        reset_mfi_extreme = float(window["mfi14"].max())
    return {
        "start_idx": int(start),
        "end_idx": int(end),
        "bars": int(len(window)),
        "neutral_bars": int(neutral_bars),
        "counter_weak": bool(counter_weak),
        "counter_strong": bool(counter_strong),
        "extreme_idx": int(extreme_idx),
        "extreme_price": float(extreme_price),
        "reset_mfi_extreme": float(reset_mfi_extreme),
    }


def _build_candidates(frame: pd.DataFrame, variant: Variant, analysis_start: pd.Timestamp) -> list[dict[str, Any]]:
    candidates: list[dict[str, Any]] = []
    for i in range(2, len(frame) - 1):
        ts = pd.Timestamp(frame.at[i, "ts"])
        if ts < analysis_start:
            continue
        vidya_state = str(frame.at[i, "vidya_state"] or "")
        side = "LONG" if vidya_state == "BUY" else ("SHORT" if vidya_state == "SELL" else None)
        if side is None:
            continue
        if str(variant.trigger_mode) == "neutral_confirm":
            if str(frame.at[i, "range_filter_state"] or "") != "NEUTRAL":
                continue
            close = float(frame.at[i, "close"])
            open_ = float(frame.at[i, "open"])
            prev_high = float(frame.at[i - 1, "high"])
            prev_low = float(frame.at[i - 1, "low"])
            ref_col = "vidya_upper" if side == "LONG" else "vidya_lower"
            vidya_ref_now = float(frame.at[i, ref_col])
            if not math.isfinite(vidya_ref_now):
                continue
            if side == "LONG":
                if not (close > open_ and close >= prev_high and close >= vidya_ref_now):
                    continue
            else:
                if not (close < open_ and close <= prev_low and close <= vidya_ref_now):
                    continue
        else:
            side = None
            if bool(frame.at[i, "range_filter_buy"]) and vidya_state == "BUY":
                side = "LONG"
            elif bool(frame.at[i, "range_filter_sell"]) and vidya_state == "SELL":
                side = "SHORT"
            if side is None:
                continue
        if variant.side_mode == "long_only" and side != "LONG":
            continue
        if variant.side_mode == "short_only" and side != "SHORT":
            continue
        if variant.require_ctx5_align:
            ctx5_vidya = str(frame.at[i, "ctx5_vidya_state"] or "")
            if side == "LONG" and ctx5_vidya != "BUY":
                continue
            if side == "SHORT" and ctx5_vidya != "SELL":
                continue
        if variant.require_ctx5_rf_not_opposite:
            ctx5_rf = str(frame.at[i, "ctx5_range_state"] or "")
            if side == "LONG" and ctx5_rf == "SELL":
                continue
            if side == "SHORT" and ctx5_rf == "BUY":
                continue

        entry_idx = i + 1
        entry_ts = pd.Timestamp(frame.at[entry_idx, "ts"])
        if not _utc_session_ok(entry_ts, variant.session_mode):
            continue

        regime_start_idx = int(frame.at[i, "vidya_regime_start_idx"])
        bars_since_regime = int(i - regime_start_idx)
        if bars_since_regime <= 0 or bars_since_regime > int(variant.max_bars_since_regime):
            continue

        reset_signal_idx = i + 1 if str(variant.trigger_mode) == "neutral_confirm" else i
        reset = _reset_window(frame, reset_signal_idx, side=side, max_reset_bars=int(variant.max_reset_bars))
        if reset is None:
            continue
        if reset["bars"] < int(variant.min_reset_bars):
            continue
        if reset["neutral_bars"] < int(variant.min_neutral_bars):
            continue
        if variant.require_counter_weak and not reset["counter_weak"]:
            continue

        extreme_idx = int(reset["extreme_idx"])
        atr_extreme = float(frame.at[extreme_idx, "atr14"])
        if not math.isfinite(atr_extreme) or atr_extreme <= 0.0:
            continue
        ref_col = "vidya_upper" if side == "LONG" else "vidya_lower"
        invalidation_col = "vidya_lower" if side == "LONG" else "vidya_upper"
        vidya_ref_extreme = float(frame.at[extreme_idx, ref_col])
        if not math.isfinite(vidya_ref_extreme):
            continue
        if side == "LONG":
            deep_touch_atr = (reset["extreme_price"] - vidya_ref_extreme) / atr_extreme
        else:
            deep_touch_atr = (vidya_ref_extreme - reset["extreme_price"]) / atr_extreme
        if deep_touch_atr < float(variant.deep_touch_min_atr) or deep_touch_atr > float(variant.deep_touch_max_atr):
            continue

        entry_price = float(frame.at[entry_idx, "open"])
        atr_entry = float(frame.at[entry_idx, "atr14"])
        vidya_ref_entry = float(frame.at[entry_idx, ref_col])
        invalidation_ref_entry = float(frame.at[entry_idx, invalidation_col])
        if not math.isfinite(entry_price) or not math.isfinite(atr_entry) or atr_entry <= 0.0 or not math.isfinite(vidya_ref_entry):
            continue
        if side == "LONG":
            entry_gap_atr = (entry_price - vidya_ref_entry) / atr_entry
            if entry_gap_atr < -0.15 or entry_gap_atr > float(variant.max_entry_gap_atr):
                continue
        else:
            entry_gap_atr = (vidya_ref_entry - entry_price) / atr_entry
            if entry_gap_atr < -0.15 or entry_gap_atr > float(variant.max_entry_gap_atr):
                continue

        entry_mfi = float(frame.at[i, "mfi14"])
        prev_mfi = float(frame.at[i - 1, "mfi14"])
        if variant.require_mfi_rebound:
            if side == "LONG":
                if reset["reset_mfi_extreme"] > float(variant.reset_mfi_long_max):
                    continue
                if entry_mfi < float(variant.entry_mfi_long_min) or entry_mfi <= prev_mfi:
                    continue
            else:
                if reset["reset_mfi_extreme"] < float(variant.reset_mfi_short_min):
                    continue
                if entry_mfi > float(variant.entry_mfi_short_max) or entry_mfi >= prev_mfi:
                    continue

        mfi_flush = bool(
            (side == "LONG" and reset["reset_mfi_extreme"] <= float(variant.reset_mfi_long_max))
            or (side == "SHORT" and reset["reset_mfi_extreme"] >= float(variant.reset_mfi_short_min))
        )
        state_flush = bool(reset["neutral_bars"] >= 2 and reset["counter_weak"])
        overshoot_reclaim = bool(deep_touch_atr <= -0.20)
        if variant.require_state_flush and not state_flush:
            continue
        if variant.require_mfi_flush and not mfi_flush:
            continue
        if variant.require_overshoot_reclaim and not overshoot_reclaim:
            continue

        stop_anchor = min(float(frame.iloc[reset["start_idx"] : reset["end_idx"] + 1]["low"].min()), invalidation_ref_entry) if side == "LONG" else max(
            float(frame.iloc[reset["start_idx"] : reset["end_idx"] + 1]["high"].max()),
            invalidation_ref_entry,
        )
        stop_price = stop_anchor - float(variant.stop_buffer_atr) * atr_entry if side == "LONG" else stop_anchor + float(variant.stop_buffer_atr) * atr_entry
        risk_abs = entry_price - stop_price if side == "LONG" else stop_price - entry_price
        if risk_abs <= 0.0:
            continue

        candidates.append(
            {
                "signal_idx": int(i),
                "entry_idx": int(entry_idx),
                "signal_time": str(ts),
                "entry_time": str(entry_ts),
                "side": side,
                "bars_since_regime": int(bars_since_regime),
                "reset_bars": int(reset["bars"]),
                "neutral_bars": int(reset["neutral_bars"]),
                "counter_weak": bool(reset["counter_weak"]),
                "counter_strong": bool(reset["counter_strong"]),
                "state_flush": bool(state_flush),
                "mfi_flush": bool(mfi_flush),
                "overshoot_reclaim": bool(overshoot_reclaim),
                "deep_touch_atr": float(deep_touch_atr),
                "entry_gap_atr": float(entry_gap_atr),
                "entry_price": float(entry_price),
                "stop_price": float(stop_price),
                "risk_abs": float(risk_abs),
                "entry_mfi": float(entry_mfi),
                "reset_mfi_extreme": float(reset["reset_mfi_extreme"]),
                "entry_vidya_ref": float(vidya_ref_entry),
            }
        )
    return candidates


def _simulate_trade(frame: pd.DataFrame, candidate: dict[str, Any], variant: Variant, fee_rate: float, position_notional: float) -> dict[str, Any]:
    side = str(candidate["side"])
    entry_idx = int(candidate["entry_idx"])
    entry_price = float(candidate["entry_price"])
    stop_price = float(candidate["stop_price"])
    risk_abs = float(candidate["risk_abs"])
    qty = float(position_notional) / entry_price
    take_profit_price = None if variant.take_profit_r is None else (entry_price + float(variant.take_profit_r) * risk_abs if side == "LONG" else entry_price - float(variant.take_profit_r) * risk_abs)
    remaining_qty = float(qty)
    partial_realized_net = 0.0
    partial_realized_gross = 0.0
    partial_exit_fee = 0.0
    partial_tp_hit = False
    partial_tp_time = None
    partial_tp_price = None

    be_armed = False
    pending_stop = stop_price
    exit_idx = len(frame) - 1
    exit_price = float(frame.iloc[exit_idx]["close"])
    exit_reason = "force_close_end"

    max_idx = min(len(frame) - 1, entry_idx + int(variant.max_hold_bars))
    for i in range(entry_idx, max_idx + 1):
        row = frame.iloc[i]
        high = float(row["high"])
        low = float(row["low"])

        if side == "LONG":
            hit_stop = low <= pending_stop
            hit_take = (take_profit_price is not None) and (not partial_tp_hit) and high >= float(take_profit_price)
        else:
            hit_stop = high >= pending_stop
            hit_take = (take_profit_price is not None) and (not partial_tp_hit) and low <= float(take_profit_price)

        if hit_stop and hit_take:
            if float(variant.partial_tp_fraction) < 1.0:
                hit_take = True
                hit_stop = False
            else:
                exit_idx = i
                exit_price = float(pending_stop)
                exit_reason = "stop_before_target"
                break
        if hit_stop:
            exit_idx = i
            exit_price = float(pending_stop)
            exit_reason = "stop"
            break
        if hit_take:
            tp_fraction = max(0.0, min(1.0, float(variant.partial_tp_fraction)))
            tp_qty = remaining_qty * tp_fraction
            tp_price = float(take_profit_price)
            if tp_qty > 0.0:
                partial_gross = ((tp_price - entry_price) * tp_qty) if side == "LONG" else ((entry_price - tp_price) * tp_qty)
                fee_tp = abs(tp_price * tp_qty) * float(fee_rate)
                partial_realized_gross += float(partial_gross)
                partial_realized_net += float(partial_gross - fee_tp)
                partial_exit_fee += float(fee_tp)
                remaining_qty -= tp_qty
                partial_tp_hit = True
                partial_tp_time = str(pd.Timestamp(frame.at[i, "ts"]))
                partial_tp_price = float(tp_price)
                pending_stop = max(float(entry_price), pending_stop) if side == "LONG" else min(float(entry_price), pending_stop)
                be_armed = True
                if remaining_qty <= 1e-12:
                    exit_idx = i
                    exit_price = tp_price
                    exit_reason = "take_profit"
                    break

        close = float(row["close"])
        vidya_line = float(row["vidya_line"])
        mfi = float(row["mfi14"])
        favorable_r = ((high - entry_price) / risk_abs) if side == "LONG" else ((entry_price - low) / risk_abs)
        if (not be_armed) and favorable_r >= float(variant.be_arm_r):
            if (side == "LONG" and mfi >= float(variant.be_mfi_long)) or (side == "SHORT" and mfi <= float(variant.be_mfi_short)):
                pending_stop = float(entry_price)
                be_armed = True

        line_break = False
        if math.isfinite(vidya_line):
            line_break = (close < vidya_line) if side == "LONG" else (close > vidya_line)
        if line_break:
            if i + 1 < len(frame):
                exit_idx = i + 1
                exit_price = float(frame.at[i + 1, "open"])
            else:
                exit_idx = i
                exit_price = float(close)
            exit_reason = "vidya_break"
            break

        if i == max_idx:
            exit_idx = i
            exit_price = float(close)
            exit_reason = "time_stop"
            break

    runner_gross = ((exit_price - entry_price) * remaining_qty) if side == "LONG" else ((entry_price - exit_price) * remaining_qty)
    fee_entry = float(position_notional) * float(fee_rate)
    fee_exit = abs(exit_price * remaining_qty) * float(fee_rate)
    gross = partial_realized_gross + runner_gross
    net = partial_realized_net + runner_gross - fee_entry - fee_exit
    return {
        **candidate,
        "exit_idx": int(exit_idx),
        "exit_time": str(pd.Timestamp(frame.at[exit_idx, "ts"])),
        "exit_price": float(exit_price),
        "exit_reason": str(exit_reason),
        "gross_pnl": float(gross),
        "net_pnl": float(net),
        "return_pct_on_notional": float(net / float(position_notional) * 100.0),
        "be_armed": bool(be_armed),
        "take_profit_price": (None if take_profit_price is None else float(take_profit_price)),
        "partial_tp_hit": bool(partial_tp_hit),
        "partial_tp_fraction": float(variant.partial_tp_fraction),
        "partial_tp_time": partial_tp_time,
        "partial_tp_price": partial_tp_price,
        "remaining_qty_exit": float(remaining_qty),
        "partial_exit_fee": float(partial_exit_fee),
    }


def _run_variant(frame: pd.DataFrame, variant: Variant, analysis_start: pd.Timestamp, fee_rate: float = 0.0004, position_notional: float = 100.0) -> dict[str, Any]:
    candidates = _build_candidates(frame, variant, analysis_start=analysis_start)
    trades: list[dict[str, Any]] = []
    current_exit_idx = -1
    for candidate in candidates:
        if int(candidate["entry_idx"]) <= current_exit_idx:
            continue
        trade = _simulate_trade(frame, candidate, variant, fee_rate=fee_rate, position_notional=position_notional)
        trades.append(trade)
        current_exit_idx = int(trade["exit_idx"])

    balance = 1000.0
    peak = balance
    max_dd_pct = 0.0
    for trade in trades:
        balance += float(trade["net_pnl"])
        peak = max(peak, balance)
        dd_pct = ((balance / peak) - 1.0) * 100.0 if peak > 0 else 0.0
        max_dd_pct = min(max_dd_pct, dd_pct)

    wins = [t for t in trades if float(t["net_pnl"]) > 0]
    gross_profit = sum(float(t["net_pnl"]) for t in wins)
    gross_loss = -sum(float(t["net_pnl"]) for t in trades if float(t["net_pnl"]) <= 0)
    summary = {
        "variant": variant.name,
        "trade_count": int(len(trades)),
        "signal_count": int(len(candidates)),
        "win_rate_pct": float((len(wins) / len(trades)) * 100.0) if trades else 0.0,
        "profit_factor": float(gross_profit / gross_loss) if gross_loss > 0 else (float("inf") if gross_profit > 0 else float("nan")),
        "ending_balance": float(balance),
        "return_pct": float(((balance / 1000.0) - 1.0) * 100.0),
        "avg_trade_pnl": float(np.mean([float(t["net_pnl"]) for t in trades])) if trades else 0.0,
        "max_drawdown_pct": float(max_dd_pct),
        "be_armed_count": int(sum(1 for t in trades if bool(t["be_armed"]))),
        "partial_tp_hit_count": int(sum(1 for t in trades if bool(t.get("partial_tp_hit")))),
        "vidya_break_exits": int(sum(1 for t in trades if str(t["exit_reason"]) == "vidya_break")),
        "take_profit_exits": int(sum(1 for t in trades if str(t["exit_reason"]) == "take_profit")),
        "time_stop_exits": int(sum(1 for t in trades if str(t["exit_reason"]) == "time_stop")),
        "stop_exits": int(sum(1 for t in trades if "stop" in str(t["exit_reason"]))),
    }
    return {
        "variant": asdict(variant),
        "summary": summary,
        "trades": trades,
    }


def _json_safe(value: Any) -> Any:
    if isinstance(value, np.generic):
        return value.item()
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


def _render_report(payload: dict[str, Any]) -> str:
    lines: list[str] = []
    lines.append("# VIDYA + RF Deep Pullback + MFI Break-Even Probe")
    lines.append("")
    lines.append(f"- symbol: `{payload['symbol']}`")
    lines.append(f"- warmup_start_utc: `{payload['warmup_start_utc']}`")
    lines.append(f"- analysis_start_utc: `{payload['analysis_start_utc']}`")
    lines.append(f"- analysis_end_utc: `{payload['analysis_end_utc']}`")
    lines.append("")
    lines.append("| Variant | Return % | Trades | Signals | Win Rate % | PF | Max DD % | BE Armed | Partial TP Hits | Vidya Break Exits |")
    lines.append("|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|")
    for row in payload["results"]:
        s = row["summary"]
        lines.append(
            f"| `{s['variant']}` | `{s['return_pct']:.2f}` | `{s['trade_count']}` | `{s['signal_count']}` | "
            f"`{s['win_rate_pct']:.2f}` | `{s['profit_factor']:.3f}` | `{s['max_drawdown_pct']:.2f}` | "
            f"`{s['be_armed_count']}` | `{s['partial_tp_hit_count']}` | `{s['vidya_break_exits']}` |"
        )
    if payload.get("best_variant"):
        best = payload["best_variant"]
        lines.append("")
        lines.append("## Best Variant")
        lines.append("")
        lines.append(f"- best_variant: `{best['summary']['variant']}`")
        lines.append(f"- return_pct: `{best['summary']['return_pct']:.2f}`")
        lines.append(f"- trades: `{best['summary']['trade_count']}`")
        lines.append(f"- win_rate_pct: `{best['summary']['win_rate_pct']:.2f}`")
        lines.append(f"- profit_factor: `{best['summary']['profit_factor']:.3f}`")
    return "\n".join(lines) + "\n"


def main() -> int:
    latest_file = sorted(LOCAL_DAY_STORE.glob("BTCUSDT_LAST_*.parquet"))[-1]
    latest_df = pd.read_parquet(latest_file, columns=["open_time"])
    latest_ts = pd.to_datetime(latest_df["open_time"], utc=True).max()
    analysis_end = pd.Timestamp(latest_ts)
    analysis_start = analysis_end - pd.Timedelta(days=14)
    warmup_start = analysis_start - pd.Timedelta(days=3)

    raw = _load_local_1m(warmup_start.floor("D"), analysis_end)
    frame = _prepare_frame(raw)

    variants = [
        Variant(name="core_both_vidya_break", side_mode="both", session_mode="all", min_reset_bars=2, max_reset_bars=16, min_neutral_bars=1, require_counter_weak=False, max_bars_since_regime=90, deep_touch_min_atr=-0.60, deep_touch_max_atr=0.25, max_entry_gap_atr=0.80, require_mfi_rebound=False, be_arm_r=0.80, be_mfi_long=65.0, be_mfi_short=35.0, take_profit_r=None, max_hold_bars=240),
        Variant(name="state_flush_both", side_mode="both", session_mode="all", min_reset_bars=3, max_reset_bars=18, min_neutral_bars=2, require_counter_weak=True, require_state_flush=True, max_bars_since_regime=90, deep_touch_min_atr=-0.70, deep_touch_max_atr=0.20, max_entry_gap_atr=0.60, require_mfi_rebound=False, be_arm_r=0.75, be_mfi_long=62.0, be_mfi_short=38.0, take_profit_r=None, max_hold_bars=240),
        Variant(name="mfi_rebound_both", side_mode="both", session_mode="all", min_reset_bars=3, max_reset_bars=18, min_neutral_bars=1, require_counter_weak=True, require_mfi_flush=True, max_bars_since_regime=90, deep_touch_min_atr=-0.70, deep_touch_max_atr=0.20, max_entry_gap_atr=0.60, require_mfi_rebound=True, reset_mfi_long_max=42.0, entry_mfi_long_min=48.0, reset_mfi_short_min=58.0, entry_mfi_short_max=52.0, be_arm_r=0.75, be_mfi_long=62.0, be_mfi_short=38.0, take_profit_r=None, max_hold_bars=240),
        Variant(name="mfi_rebound_tp2_both", side_mode="both", session_mode="all", min_reset_bars=3, max_reset_bars=18, min_neutral_bars=1, require_counter_weak=True, require_mfi_flush=True, max_bars_since_regime=75, deep_touch_min_atr=-0.70, deep_touch_max_atr=0.18, max_entry_gap_atr=0.55, require_mfi_rebound=True, reset_mfi_long_max=40.0, entry_mfi_long_min=50.0, reset_mfi_short_min=60.0, entry_mfi_short_max=50.0, be_arm_r=0.75, be_mfi_long=62.0, be_mfi_short=38.0, take_profit_r=2.0, max_hold_bars=180),
        Variant(name="overshoot_reclaim_short", side_mode="short_only", session_mode="all", min_reset_bars=3, max_reset_bars=20, min_neutral_bars=1, require_counter_weak=True, require_overshoot_reclaim=True, max_bars_since_regime=120, deep_touch_min_atr=-0.90, deep_touch_max_atr=0.10, max_entry_gap_atr=0.65, require_mfi_rebound=True, require_mfi_flush=True, reset_mfi_long_max=40.0, entry_mfi_long_min=50.0, reset_mfi_short_min=58.0, entry_mfi_short_max=52.0, be_arm_r=0.75, be_mfi_long=62.0, be_mfi_short=40.0, take_profit_r=2.0, max_hold_bars=240),
        Variant(name="state_flush_overlap_short", side_mode="short_only", session_mode="overlap", min_reset_bars=3, max_reset_bars=20, min_neutral_bars=2, require_counter_weak=True, require_state_flush=True, max_bars_since_regime=120, deep_touch_min_atr=-0.75, deep_touch_max_atr=0.20, max_entry_gap_atr=0.60, require_mfi_rebound=True, require_mfi_flush=True, reset_mfi_long_max=40.0, entry_mfi_long_min=50.0, reset_mfi_short_min=58.0, entry_mfi_short_max=52.0, be_arm_r=0.75, be_mfi_long=62.0, be_mfi_short=40.0, take_profit_r=2.0, max_hold_bars=240),
        Variant(name="mfi_rebound_tight_both", side_mode="both", session_mode="all", min_reset_bars=2, max_reset_bars=12, min_neutral_bars=2, require_counter_weak=True, require_state_flush=True, max_bars_since_regime=60, deep_touch_min_atr=-0.55, deep_touch_max_atr=0.15, max_entry_gap_atr=0.45, require_mfi_rebound=True, require_mfi_flush=True, reset_mfi_long_max=38.0, entry_mfi_long_min=50.0, reset_mfi_short_min=62.0, entry_mfi_short_max=50.0, be_arm_r=0.70, be_mfi_long=60.0, be_mfi_short=40.0, take_profit_r=1.8, max_hold_bars=180),
        Variant(name="ctx5_state_flush_short", side_mode="short_only", session_mode="all", require_ctx5_align=True, require_ctx5_rf_not_opposite=True, min_reset_bars=3, max_reset_bars=20, min_neutral_bars=2, require_counter_weak=True, require_state_flush=True, max_bars_since_regime=120, deep_touch_min_atr=-0.75, deep_touch_max_atr=0.20, max_entry_gap_atr=0.60, require_mfi_rebound=True, require_mfi_flush=True, reset_mfi_long_max=40.0, entry_mfi_long_min=50.0, reset_mfi_short_min=58.0, entry_mfi_short_max=52.0, be_arm_r=0.75, be_mfi_long=62.0, be_mfi_short=40.0, take_profit_r=2.0, max_hold_bars=240),
        Variant(name="ctx5_short_loose_tp2", side_mode="short_only", session_mode="all", require_ctx5_align=True, require_ctx5_rf_not_opposite=True, min_reset_bars=1, max_reset_bars=30, min_neutral_bars=0, require_counter_weak=False, max_bars_since_regime=240, deep_touch_min_atr=-1.20, deep_touch_max_atr=0.80, max_entry_gap_atr=2.00, require_mfi_rebound=False, be_arm_r=0.80, be_mfi_long=60.0, be_mfi_short=40.0, take_profit_r=2.0, max_hold_bars=240),
        Variant(name="ctx5_short_loose_no_tp", side_mode="short_only", session_mode="all", require_ctx5_align=True, require_ctx5_rf_not_opposite=True, min_reset_bars=1, max_reset_bars=30, min_neutral_bars=0, require_counter_weak=False, max_bars_since_regime=240, deep_touch_min_atr=-1.20, deep_touch_max_atr=0.80, max_entry_gap_atr=2.00, require_mfi_rebound=False, be_arm_r=0.80, be_mfi_long=60.0, be_mfi_short=40.0, take_profit_r=None, max_hold_bars=240),
        Variant(name="ctx5_short_hybrid_tp2_half", side_mode="short_only", session_mode="all", require_ctx5_align=True, require_ctx5_rf_not_opposite=True, min_reset_bars=1, max_reset_bars=30, min_neutral_bars=0, require_counter_weak=False, max_bars_since_regime=240, deep_touch_min_atr=-1.20, deep_touch_max_atr=0.80, max_entry_gap_atr=2.00, require_mfi_rebound=False, be_arm_r=0.80, be_mfi_long=60.0, be_mfi_short=40.0, take_profit_r=2.0, partial_tp_fraction=0.5, max_hold_bars=240),
        Variant(name="ctx5_short_hybrid_tp15_half", side_mode="short_only", session_mode="all", require_ctx5_align=True, require_ctx5_rf_not_opposite=True, min_reset_bars=1, max_reset_bars=30, min_neutral_bars=0, require_counter_weak=False, max_bars_since_regime=240, deep_touch_min_atr=-1.20, deep_touch_max_atr=0.80, max_entry_gap_atr=2.00, require_mfi_rebound=False, be_arm_r=0.80, be_mfi_long=60.0, be_mfi_short=40.0, take_profit_r=1.5, partial_tp_fraction=0.5, max_hold_bars=240),
        Variant(name="ctx5_short_hybrid_quality", side_mode="short_only", session_mode="all", require_ctx5_align=True, require_ctx5_rf_not_opposite=True, min_reset_bars=1, max_reset_bars=24, min_neutral_bars=0, require_counter_weak=False, max_bars_since_regime=240, deep_touch_min_atr=-1.20, deep_touch_max_atr=0.35, max_entry_gap_atr=1.30, require_mfi_rebound=False, be_arm_r=0.80, be_mfi_long=60.0, be_mfi_short=40.0, take_profit_r=2.0, partial_tp_fraction=0.5, max_hold_bars=240),
    ]

    results = [_run_variant(frame, variant, analysis_start=analysis_start) for variant in variants]
    results.sort(key=lambda item: float(item["summary"]["return_pct"]), reverse=True)
    best_variant = results[0] if results else None

    payload = {
        "symbol": "BTCUSDT",
        "warmup_start_utc": str(pd.Timestamp(warmup_start)),
        "analysis_start_utc": str(pd.Timestamp(analysis_start)),
        "analysis_end_utc": str(pd.Timestamp(analysis_end)),
        "results": results,
        "best_variant": best_variant,
    }

    stamp = pd.Timestamp.now(tz="UTC").strftime("%Y%m%d_%H%M%S")
    output_dir = DEFAULT_OUTPUT_ROOT / f"probe_vidya_rf_mfi_deep_pullback_{stamp}"
    output_dir.mkdir(parents=True, exist_ok=True)
    (output_dir / "summary.json").write_text(json.dumps(_json_safe(payload), indent=2, ensure_ascii=False), encoding="utf-8")
    (output_dir / "report.md").write_text(_render_report(payload), encoding="utf-8")
    for row in results:
        pd.DataFrame(row["trades"]).to_csv(output_dir / f"{row['summary']['variant']}_trades.csv", index=False)

    if LATEST_MIRROR.exists() or LATEST_MIRROR.is_symlink():
        if LATEST_MIRROR.is_symlink() or LATEST_MIRROR.is_file():
            LATEST_MIRROR.unlink()
        else:
            shutil.rmtree(LATEST_MIRROR)
    shutil.copytree(output_dir, LATEST_MIRROR)
    print(_render_report(payload))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
