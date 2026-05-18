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

from scripts.manual.probe_vidya_rf_mfi_deep_pullback import (
    DEFAULT_OUTPUT_ROOT,
    LOCAL_DAY_STORE,
    _json_safe,
    _load_local_1m,
    _prepare_frame,
    _simulate_trade,
)


LATEST_MIRROR = REPO_ROOT / "runs" / "perf" / "probe_vidya_price_action_pullbacks_latest"


@dataclass(frozen=True)
class Playbook:
    name: str
    family: str
    side_mode: str
    require_ctx5_align: bool = True
    require_ctx5_rf_not_opposite: bool = True
    max_bars_since_regime: int = 240
    min_impulse_atr: float = 1.5
    min_retrace_frac: float = 0.20
    max_retrace_frac: float = 0.70
    min_neutral_bars: int = 1
    max_neutral_bars: int = 8
    max_origin_break_atr: float = 0.10
    max_entry_gap_atr: float = 1.10
    max_signal_close_pos: float = 0.35
    max_signal_opposite_wick_pct: float = 0.35
    max_box_range_atr: float = 1.00
    min_sweep_excess_atr: float = 0.10
    stop_buffer_atr: float = 0.10
    be_arm_r: float = 0.80
    be_mfi_long: float = 60.0
    be_mfi_short: float = 40.0
    take_profit_r: float = 2.0
    partial_tp_fraction: float = 0.5
    max_hold_bars: int = 240


def _augment(frame: pd.DataFrame) -> pd.DataFrame:
    out = frame.copy()
    rng = (out["high"] - out["low"]).replace(0.0, np.nan)
    out["body_abs"] = (out["close"] - out["open"]).abs()
    out["body_pct_range"] = out["body_abs"] / rng
    out["close_pos_in_range"] = (out["close"] - out["low"]) / rng
    out["upper_wick"] = out["high"] - out[["open", "close"]].max(axis=1)
    out["lower_wick"] = out[["open", "close"]].min(axis=1) - out["low"]
    out["upper_wick_pct_range"] = out["upper_wick"] / rng
    out["lower_wick_pct_range"] = out["lower_wick"] / rng
    out["swing_high_20"] = out["high"].rolling(20, min_periods=5).max().shift(1)
    out["swing_low_20"] = out["low"].rolling(20, min_periods=5).min().shift(1)
    return out


def _neutral_run(frame: pd.DataFrame, i: int) -> Optional[dict[str, Any]]:
    if str(frame.at[i, "range_filter_state"] or "") != "NEUTRAL":
        return None
    start = i
    while start - 1 >= 0 and str(frame.at[start - 1, "range_filter_state"] or "") == "NEUTRAL":
        start -= 1
    end = i
    prev_state = str(frame.at[start - 1, "range_filter_state"] or "") if start - 1 >= 0 else ""
    return {"start": int(start), "end": int(end), "bars": int(end - start + 1), "prev_state": prev_state}


def _build_candidates(frame: pd.DataFrame, playbook: Playbook, analysis_start: pd.Timestamp) -> list[dict[str, Any]]:
    candidates: list[dict[str, Any]] = []
    for i in range(2, len(frame) - 1):
        ts = pd.Timestamp(frame.at[i, "ts"])
        if ts < analysis_start:
            continue

        vidya_state = str(frame.at[i, "vidya_state"] or "")
        side = "LONG" if vidya_state == "BUY" else ("SHORT" if vidya_state == "SELL" else None)
        if side is None or side.lower() + "_only" != playbook.side_mode:
            continue

        if playbook.require_ctx5_align:
            ctx5_vidya = str(frame.at[i, "ctx5_vidya_state"] or "")
            if side == "LONG" and ctx5_vidya != "BUY":
                continue
            if side == "SHORT" and ctx5_vidya != "SELL":
                continue
        if playbook.require_ctx5_rf_not_opposite:
            ctx5_rf = str(frame.at[i, "ctx5_range_state"] or "")
            if side == "LONG" and ctx5_rf == "SELL":
                continue
            if side == "SHORT" and ctx5_rf == "BUY":
                continue

        regime_start_idx = int(frame.at[i, "vidya_regime_start_idx"])
        bars_since_regime = int(i - regime_start_idx)
        if bars_since_regime <= 0 or bars_since_regime > int(playbook.max_bars_since_regime):
            continue

        neutral = _neutral_run(frame, i)
        if neutral is None:
            continue
        if neutral["bars"] < int(playbook.min_neutral_bars) or neutral["bars"] > int(playbook.max_neutral_bars):
            continue
        if side == "LONG" and neutral["prev_state"] != "BUY":
            continue
        if side == "SHORT" and neutral["prev_state"] != "SELL":
            continue

        pre_end = neutral["start"] - 1
        if pre_end <= regime_start_idx:
            continue

        impulse_slice = frame.iloc[regime_start_idx : pre_end + 1]
        reset_slice = frame.iloc[neutral["start"] : neutral["end"] + 1]
        signal = frame.iloc[i]
        entry_idx = i + 1
        entry = frame.iloc[entry_idx]

        atr_signal = float(signal["atr14"])
        atr_entry = float(entry["atr14"])
        if not math.isfinite(atr_signal) or not math.isfinite(atr_entry) or atr_signal <= 0.0 or atr_entry <= 0.0:
            continue

        if side == "SHORT":
            impulse_low_idx = int(impulse_slice["low"].astype(float).idxmin())
            origin_slice = frame.iloc[regime_start_idx : impulse_low_idx + 1]
            origin_high = float(origin_slice["high"].max())
            origin_high_idx = int(origin_slice["high"].astype(float).idxmax())
            impulse_low = float(frame.at[impulse_low_idx, "low"])
            impulse_size = origin_high - impulse_low
            if origin_high_idx >= impulse_low_idx or impulse_size <= 0.0:
                continue
            impulse_atr = impulse_size / atr_signal
            if impulse_atr < float(playbook.min_impulse_atr):
                continue

            reset_high = float(reset_slice["high"].max())
            retrace_frac = (reset_high - impulse_low) / impulse_size
            origin_break_atr = (reset_high - origin_high) / atr_signal
            entry_gap_atr = (float(entry["vidya_lower"]) - float(entry["open"])) / atr_entry
            close_pos = float(signal["close_pos_in_range"])
            opposite_wick = float(signal["upper_wick_pct_range"])

            if retrace_frac < float(playbook.min_retrace_frac) or retrace_frac > float(playbook.max_retrace_frac):
                continue
            if origin_break_atr > float(playbook.max_origin_break_atr):
                continue
            if entry_gap_atr < -0.15 or entry_gap_atr > float(playbook.max_entry_gap_atr):
                continue

            # Confirm candle: bearish acceptance back down, not just drift.
            if not (
                float(signal["close"]) < float(signal["open"])
                and float(signal["close"]) <= float(frame.at[i - 1, "low"])
                and float(signal["close"]) <= float(signal["vidya_lower"])
            ):
                continue
            if close_pos > float(playbook.max_signal_close_pos):
                continue
            if opposite_wick > float(playbook.max_signal_opposite_wick_pct):
                continue

            if playbook.family == "value_pullback":
                pass
            elif playbook.family == "sweep_reclaim":
                swing_level = float(reset_slice["swing_high_20"].dropna().max()) if reset_slice["swing_high_20"].notna().any() else -np.inf
                vidya_sweep_level = float(reset_slice["vidya_upper"].dropna().max()) if reset_slice["vidya_upper"].notna().any() else -np.inf
                sweep_excess_candidates = []
                if math.isfinite(swing_level):
                    sweep_excess_candidates.append((reset_high - swing_level) / atr_signal)
                if math.isfinite(vidya_sweep_level):
                    sweep_excess_candidates.append((reset_high - vidya_sweep_level) / atr_signal)
                if not sweep_excess_candidates:
                    continue
                if max(sweep_excess_candidates) < float(playbook.min_sweep_excess_atr):
                    continue
            elif playbook.family == "box_reset":
                box_low = float(reset_slice["low"].min())
                box_high = float(reset_slice["high"].max())
                box_range_atr = (box_high - box_low) / atr_signal
                if box_range_atr > float(playbook.max_box_range_atr):
                    continue
                box_accept_ceiling = box_low + 0.20 * (box_high - box_low)
                if float(signal["close"]) > box_accept_ceiling:
                    continue
            else:
                continue

            stop_anchor = max(reset_high, float(entry["vidya_upper"]))
            stop_price = stop_anchor + float(playbook.stop_buffer_atr) * atr_entry
        else:
            impulse_high_idx = int(impulse_slice["high"].astype(float).idxmax())
            origin_slice = frame.iloc[regime_start_idx : impulse_high_idx + 1]
            origin_low = float(origin_slice["low"].min())
            origin_low_idx = int(origin_slice["low"].astype(float).idxmin())
            impulse_high = float(frame.at[impulse_high_idx, "high"])
            impulse_size = impulse_high - origin_low
            if origin_low_idx >= impulse_high_idx or impulse_size <= 0.0:
                continue
            impulse_atr = impulse_size / atr_signal
            if impulse_atr < float(playbook.min_impulse_atr):
                continue

            reset_low = float(reset_slice["low"].min())
            retrace_frac = (impulse_high - reset_low) / impulse_size
            origin_break_atr = (origin_low - reset_low) / atr_signal
            entry_gap_atr = (float(entry["open"]) - float(entry["vidya_upper"])) / atr_entry
            close_pos = 1.0 - float(signal["close_pos_in_range"])
            opposite_wick = float(signal["lower_wick_pct_range"])

            if retrace_frac < float(playbook.min_retrace_frac) or retrace_frac > float(playbook.max_retrace_frac):
                continue
            if origin_break_atr > float(playbook.max_origin_break_atr):
                continue
            if entry_gap_atr < -0.15 or entry_gap_atr > float(playbook.max_entry_gap_atr):
                continue

            if not (
                float(signal["close"]) > float(signal["open"])
                and float(signal["close"]) >= float(frame.at[i - 1, "high"])
                and float(signal["close"]) >= float(signal["vidya_upper"])
            ):
                continue
            if close_pos > float(playbook.max_signal_close_pos):
                continue
            if opposite_wick > float(playbook.max_signal_opposite_wick_pct):
                continue

            if playbook.family == "value_pullback":
                pass
            elif playbook.family == "sweep_reclaim":
                swing_level = float(reset_slice["swing_low_20"].dropna().min()) if reset_slice["swing_low_20"].notna().any() else np.inf
                vidya_sweep_level = float(reset_slice["vidya_lower"].dropna().min()) if reset_slice["vidya_lower"].notna().any() else np.inf
                sweep_excess_candidates = []
                if math.isfinite(swing_level):
                    sweep_excess_candidates.append((swing_level - reset_low) / atr_signal)
                if math.isfinite(vidya_sweep_level):
                    sweep_excess_candidates.append((vidya_sweep_level - reset_low) / atr_signal)
                if not sweep_excess_candidates:
                    continue
                if max(sweep_excess_candidates) < float(playbook.min_sweep_excess_atr):
                    continue
            elif playbook.family == "box_reset":
                box_low = float(reset_slice["low"].min())
                box_high = float(reset_slice["high"].max())
                box_range_atr = (box_high - box_low) / atr_signal
                if box_range_atr > float(playbook.max_box_range_atr):
                    continue
                box_accept_floor = box_high - 0.20 * (box_high - box_low)
                if float(signal["close"]) < box_accept_floor:
                    continue
            else:
                continue

            stop_anchor = min(reset_low, float(entry["vidya_lower"]))
            stop_price = stop_anchor - float(playbook.stop_buffer_atr) * atr_entry

        entry_price = float(entry["open"])
        risk_abs = (entry_price - stop_price) if side == "LONG" else (stop_price - entry_price)
        if risk_abs <= 0.0:
            continue

        candidates.append(
            {
                "signal_idx": int(i),
                "entry_idx": int(entry_idx),
                "signal_time": str(ts),
                "entry_time": str(pd.Timestamp(entry["ts"])),
                "side": side,
                "family": playbook.family,
                "bars_since_regime": int(bars_since_regime),
                "neutral_bars": int(neutral["bars"]),
                "impulse_atr": float(impulse_atr),
                "retrace_frac": float(retrace_frac),
                "entry_gap_atr": float(entry_gap_atr),
                "entry_price": float(entry_price),
                "stop_price": float(stop_price),
                "risk_abs": float(risk_abs),
                "entry_mfi": float(signal["mfi14"]),
            }
        )
    return candidates


def _run_playbook(frame: pd.DataFrame, playbook: Playbook, analysis_start: pd.Timestamp) -> dict[str, Any]:
    candidates = _build_candidates(frame, playbook, analysis_start=analysis_start)
    trades: list[dict[str, Any]] = []
    current_exit_idx = -1
    for candidate in candidates:
        if int(candidate["entry_idx"]) <= current_exit_idx:
            continue
        trade = _simulate_trade(
            frame,
            candidate,
            playbook,
            fee_rate=0.0004,
            position_notional=100.0,
        )
        trades.append(trade)
        current_exit_idx = int(trade["exit_idx"])

    balance = 1000.0
    peak = balance
    max_dd_pct = 0.0
    wins = [t for t in trades if float(t["net_pnl"]) > 0]
    gross_profit = sum(float(t["net_pnl"]) for t in wins)
    gross_loss = -sum(float(t["net_pnl"]) for t in trades if float(t["net_pnl"]) <= 0)
    for trade in trades:
        balance += float(trade["net_pnl"])
        peak = max(peak, balance)
        dd_pct = ((balance / peak) - 1.0) * 100.0 if peak > 0 else 0.0
        max_dd_pct = min(max_dd_pct, dd_pct)

    summary = {
        "variant": playbook.name,
        "family": playbook.family,
        "side_mode": playbook.side_mode,
        "trade_count": int(len(trades)),
        "signal_count": int(len(candidates)),
        "win_rate_pct": float((len(wins) / len(trades)) * 100.0) if trades else 0.0,
        "profit_factor": float(gross_profit / gross_loss) if gross_loss > 0 else (float("inf") if gross_profit > 0 else float("nan")),
        "ending_balance": float(balance),
        "return_pct": float(((balance / 1000.0) - 1.0) * 100.0),
        "max_drawdown_pct": float(max_dd_pct),
        "vidya_break_exits": int(sum(1 for t in trades if str(t["exit_reason"]) == "vidya_break")),
        "take_profit_exits": int(sum(1 for t in trades if str(t["exit_reason"]) == "take_profit")),
        "stop_exits": int(sum(1 for t in trades if "stop" in str(t["exit_reason"]))),
    }
    return {"variant": asdict(playbook), "summary": summary, "trades": trades}


def _render_report(payload: dict[str, Any]) -> str:
    lines: list[str] = []
    lines.append("# VIDYA Price-Action Pullback Probe")
    lines.append("")
    for window in payload["windows"]:
        lines.append(f"## {window['label']}")
        lines.append("")
        lines.append(f"- start_utc: `{window['start_utc']}`")
        lines.append(f"- end_utc: `{window['end_utc']}`")
        lines.append("")
        lines.append("| Variant | Family | Side | Return % | Trades | Signals | Win Rate % | PF | Max DD % |")
        lines.append("|---|---|---|---:|---:|---:|---:|---:|---:|")
        for row in window["results"]:
            s = row["summary"]
            lines.append(
                f"| `{s['variant']}` | `{s['family']}` | `{s['side_mode']}` | `{s['return_pct']:.2f}` | "
                f"`{s['trade_count']}` | `{s['signal_count']}` | `{s['win_rate_pct']:.2f}` | `{s['profit_factor']:.3f}` | `{s['max_drawdown_pct']:.2f}` |"
            )
        lines.append("")
    return "\n".join(lines) + "\n"


def main() -> int:
    latest_file = sorted(LOCAL_DAY_STORE.glob("BTCUSDT_LAST_*.parquet"))[-1]
    latest_df = pd.read_parquet(latest_file, columns=["open_time"])
    latest_ts = pd.to_datetime(latest_df["open_time"], utc=True).max()
    recent_end = pd.Timestamp(latest_ts)
    recent_start = recent_end - pd.Timedelta(days=14)
    prev_end = recent_start - pd.Timedelta(minutes=1)
    prev_start = prev_end - pd.Timedelta(days=14) + pd.Timedelta(minutes=1)
    windows = [
        {"label": "recent14", "start": recent_start, "end": recent_end},
        {"label": "prev14", "start": prev_start, "end": prev_end},
    ]

    playbooks = [
        Playbook(name="short_value_pullback_v1", family="value_pullback", side_mode="short_only", min_impulse_atr=1.8, min_retrace_frac=0.25, max_retrace_frac=0.65, min_neutral_bars=1, max_neutral_bars=6, max_origin_break_atr=0.05, max_entry_gap_atr=1.00, max_signal_close_pos=0.35, max_signal_opposite_wick_pct=0.30),
        Playbook(name="short_sweep_reclaim_v1", family="sweep_reclaim", side_mode="short_only", min_impulse_atr=1.8, min_retrace_frac=0.35, max_retrace_frac=0.95, min_neutral_bars=1, max_neutral_bars=8, max_origin_break_atr=0.20, max_entry_gap_atr=1.10, max_signal_close_pos=0.35, max_signal_opposite_wick_pct=0.35, min_sweep_excess_atr=0.05),
        Playbook(name="short_box_reset_v1", family="box_reset", side_mode="short_only", min_impulse_atr=1.5, min_retrace_frac=0.15, max_retrace_frac=0.60, min_neutral_bars=3, max_neutral_bars=10, max_origin_break_atr=0.10, max_entry_gap_atr=1.00, max_signal_close_pos=0.40, max_signal_opposite_wick_pct=0.35, max_box_range_atr=1.00),
        Playbook(name="short_sweep_reclaim_v2", family="sweep_reclaim", side_mode="short_only", min_impulse_atr=1.5, min_retrace_frac=0.25, max_retrace_frac=1.05, min_neutral_bars=1, max_neutral_bars=10, max_origin_break_atr=0.25, max_entry_gap_atr=1.20, max_signal_close_pos=0.40, max_signal_opposite_wick_pct=0.45, min_sweep_excess_atr=0.00),
        Playbook(name="short_box_reset_v2", family="box_reset", side_mode="short_only", min_impulse_atr=1.2, min_retrace_frac=0.10, max_retrace_frac=0.75, min_neutral_bars=2, max_neutral_bars=12, max_origin_break_atr=0.15, max_entry_gap_atr=1.10, max_signal_close_pos=0.45, max_signal_opposite_wick_pct=0.45, max_box_range_atr=1.40),
        Playbook(name="long_value_pullback_v1", family="value_pullback", side_mode="long_only", min_impulse_atr=1.8, min_retrace_frac=0.25, max_retrace_frac=0.65, min_neutral_bars=1, max_neutral_bars=6, max_origin_break_atr=0.05, max_entry_gap_atr=1.00, max_signal_close_pos=0.35, max_signal_opposite_wick_pct=0.30),
        Playbook(name="long_sweep_reclaim_v1", family="sweep_reclaim", side_mode="long_only", min_impulse_atr=1.8, min_retrace_frac=0.35, max_retrace_frac=0.95, min_neutral_bars=1, max_neutral_bars=8, max_origin_break_atr=0.20, max_entry_gap_atr=1.10, max_signal_close_pos=0.35, max_signal_opposite_wick_pct=0.35, min_sweep_excess_atr=0.05),
        Playbook(name="long_box_reset_v1", family="box_reset", side_mode="long_only", min_impulse_atr=1.5, min_retrace_frac=0.15, max_retrace_frac=0.60, min_neutral_bars=3, max_neutral_bars=10, max_origin_break_atr=0.10, max_entry_gap_atr=1.00, max_signal_close_pos=0.40, max_signal_opposite_wick_pct=0.35, max_box_range_atr=1.00),
        Playbook(name="long_sweep_reclaim_v2", family="sweep_reclaim", side_mode="long_only", min_impulse_atr=1.5, min_retrace_frac=0.25, max_retrace_frac=1.05, min_neutral_bars=1, max_neutral_bars=10, max_origin_break_atr=0.25, max_entry_gap_atr=1.20, max_signal_close_pos=0.40, max_signal_opposite_wick_pct=0.45, min_sweep_excess_atr=0.00),
        Playbook(name="long_box_reset_v2", family="box_reset", side_mode="long_only", min_impulse_atr=1.2, min_retrace_frac=0.10, max_retrace_frac=0.75, min_neutral_bars=2, max_neutral_bars=12, max_origin_break_atr=0.15, max_entry_gap_atr=1.10, max_signal_close_pos=0.45, max_signal_opposite_wick_pct=0.45, max_box_range_atr=1.40),
    ]

    payload: dict[str, Any] = {"windows": []}
    for window in windows:
        raw = _load_local_1m((window["start"] - pd.Timedelta(days=3)).floor("D"), window["end"])
        frame = _augment(_prepare_frame(raw))
        results = [_run_playbook(frame, playbook, analysis_start=window["start"]) for playbook in playbooks]
        results.sort(key=lambda row: float(row["summary"]["return_pct"]), reverse=True)
        payload["windows"].append(
            {
                "label": window["label"],
                "start_utc": str(pd.Timestamp(window["start"])),
                "end_utc": str(pd.Timestamp(window["end"])),
                "results": results,
            }
        )

    if LATEST_MIRROR.exists():
        shutil.rmtree(LATEST_MIRROR)
    LATEST_MIRROR.mkdir(parents=True, exist_ok=True)
    (LATEST_MIRROR / "summary.json").write_text(json.dumps(_json_safe(payload), indent=2, ensure_ascii=False), encoding="utf-8")
    (LATEST_MIRROR / "report.md").write_text(_render_report(payload), encoding="utf-8")
    for window in payload["windows"]:
        label = window["label"]
        for row in window["results"]:
            pd.DataFrame(row["trades"]).to_csv(LATEST_MIRROR / f"{label}__{row['summary']['variant']}_trades.csv", index=False)

    print(_render_report(payload))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
