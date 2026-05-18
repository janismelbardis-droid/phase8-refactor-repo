from __future__ import annotations

import json
import math
import shutil
import sys
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd


REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from scripts.manual.probe_vidya_rf_mfi_deep_pullback import LOCAL_DAY_STORE, _json_safe, _load_local_1m, _prepare_frame


LATEST_MIRROR = REPO_ROOT / "runs" / "perf" / "probe_vidya_neutral_wick_pullback_latest"


@dataclass(frozen=True)
class Playbook:
    name: str
    side_mode: str
    reference_mode: str
    require_ctx5_align: bool = True
    require_ctx5_rf_not_opposite: bool = True
    max_bars_since_regime: int = 240
    max_reset_bars: int = 12
    min_impulse_atr: float = 1.5
    min_retrace_frac: float = 0.20
    max_retrace_frac: float = 0.85
    min_wick_penetration_atr: float = 0.00
    max_wick_penetration_atr: float = 0.80
    min_close_pos_in_range: float = 0.60
    max_close_pos_in_range: float = 0.40
    stop_buffer_atr: float = 0.05
    be_arm_r: float = 0.80
    take_profit_r: float = 2.0
    partial_tp_fraction: float = 0.5
    max_hold_bars: int = 180


def _augment(frame: pd.DataFrame) -> pd.DataFrame:
    out = frame.copy()
    rng = (out["high"] - out["low"]).replace(0.0, np.nan)
    out["close_pos_in_range"] = (out["close"] - out["low"]) / rng
    out["upper_wick"] = out["high"] - out[["open", "close"]].max(axis=1)
    out["lower_wick"] = out[["open", "close"]].min(axis=1) - out["low"]
    out["upper_wick_pct_range"] = out["upper_wick"] / rng
    out["lower_wick_pct_range"] = out["lower_wick"] / rng
    return out


def _reference_price(row: pd.Series, side: str, mode: str) -> float:
    if mode == "line":
        return float(row["vidya_line"])
    if mode == "band":
        return float(row["vidya_upper"] if side == "LONG" else row["vidya_lower"])
    raise ValueError(mode)


def _build_candidates(frame: pd.DataFrame, playbook: Playbook, analysis_start: pd.Timestamp) -> list[dict[str, Any]]:
    candidates: list[dict[str, Any]] = []
    for i in range(2, len(frame) - 1):
        ts = pd.Timestamp(frame.at[i, "ts"])
        if ts < analysis_start:
            continue
        if str(frame.at[i, "range_filter_state"] or "") != "NEUTRAL":
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

        reset_start = max(regime_start_idx, i - int(playbook.max_reset_bars) + 1)
        reset_slice = frame.iloc[reset_start : i + 1]

        atr = float(frame.at[i, "atr14"])
        if not math.isfinite(atr) or atr <= 0.0:
            continue

        if side == "LONG":
            impulse_high_idx = int(frame.iloc[regime_start_idx:i]["high"].astype(float).idxmax())
            origin_low = float(frame.iloc[regime_start_idx : impulse_high_idx + 1]["low"].min())
            impulse_high = float(frame.at[impulse_high_idx, "high"])
            impulse_size = impulse_high - origin_low
            if impulse_size <= 0.0 or (impulse_size / atr) < float(playbook.min_impulse_atr):
                continue
            reset_low = float(reset_slice["low"].min())
            retrace_frac = (impulse_high - reset_low) / impulse_size
            if retrace_frac < float(playbook.min_retrace_frac) or retrace_frac > float(playbook.max_retrace_frac):
                continue

            ref = _reference_price(frame.iloc[i], side, playbook.reference_mode)
            if not math.isfinite(ref):
                continue
            wick_penetration_atr = (ref - float(frame.at[i, "low"])) / atr
            if wick_penetration_atr < float(playbook.min_wick_penetration_atr) or wick_penetration_atr > float(playbook.max_wick_penetration_atr):
                continue
            if not (
                float(frame.at[i, "close"]) > float(frame.at[i, "open"])
                and float(frame.at[i, "close"]) >= ref
                and float(frame.at[i, "close_pos_in_range"]) >= float(playbook.min_close_pos_in_range)
            ):
                continue

            stop_price = float(frame.at[i, "low"]) - float(playbook.stop_buffer_atr) * atr
        else:
            impulse_low_idx = int(frame.iloc[regime_start_idx:i]["low"].astype(float).idxmin())
            origin_high = float(frame.iloc[regime_start_idx : impulse_low_idx + 1]["high"].max())
            impulse_low = float(frame.at[impulse_low_idx, "low"])
            impulse_size = origin_high - impulse_low
            if impulse_size <= 0.0 or (impulse_size / atr) < float(playbook.min_impulse_atr):
                continue
            reset_high = float(reset_slice["high"].max())
            retrace_frac = (reset_high - impulse_low) / impulse_size
            if retrace_frac < float(playbook.min_retrace_frac) or retrace_frac > float(playbook.max_retrace_frac):
                continue

            ref = _reference_price(frame.iloc[i], side, playbook.reference_mode)
            if not math.isfinite(ref):
                continue
            wick_penetration_atr = (float(frame.at[i, "high"]) - ref) / atr
            if wick_penetration_atr < float(playbook.min_wick_penetration_atr) or wick_penetration_atr > float(playbook.max_wick_penetration_atr):
                continue
            if not (
                float(frame.at[i, "close"]) < float(frame.at[i, "open"])
                and float(frame.at[i, "close"]) <= ref
                and float(frame.at[i, "close_pos_in_range"]) <= float(playbook.max_close_pos_in_range)
            ):
                continue

            stop_price = float(frame.at[i, "high"]) + float(playbook.stop_buffer_atr) * atr

        entry_idx = i + 1
        entry_price = float(frame.at[entry_idx, "open"])
        risk_abs = (entry_price - stop_price) if side == "LONG" else (stop_price - entry_price)
        if risk_abs <= 0.0:
            continue

        candidates.append(
            {
                "signal_idx": int(i),
                "entry_idx": int(entry_idx),
                "signal_time": str(ts),
                "entry_time": str(pd.Timestamp(frame.at[entry_idx, "ts"])),
                "side": side,
                "bars_since_regime": int(bars_since_regime),
                "retrace_frac": float(retrace_frac),
                "wick_penetration_atr": float(wick_penetration_atr),
                "entry_price": float(entry_price),
                "stop_price": float(stop_price),
                "risk_abs": float(risk_abs),
                "signal_close": float(frame.at[i, "close"]),
                "signal_extreme": float(frame.at[i, "low"] if side == "LONG" else frame.at[i, "high"]),
            }
        )
    return candidates


def _simulate(frame: pd.DataFrame, candidate: dict[str, Any], playbook: Playbook, *, fee_rate: float = 0.0004, position_notional: float = 100.0) -> dict[str, Any]:
    side = str(candidate["side"])
    signal_idx = int(candidate["signal_idx"])
    entry_idx = int(candidate["entry_idx"])
    entry_price = float(candidate["entry_price"])
    stop_price = float(candidate["stop_price"])
    risk_abs = float(candidate["risk_abs"])
    signal_close = float(candidate["signal_close"])
    qty = float(position_notional) / entry_price

    take_profit_price = entry_price + float(playbook.take_profit_r) * risk_abs if side == "LONG" else entry_price - float(playbook.take_profit_r) * risk_abs
    remaining_qty = float(qty)
    partial_realized_gross = 0.0
    partial_realized_net = 0.0
    partial_exit_fee = 0.0
    partial_tp_hit = False
    be_armed = False
    pending_stop = float(stop_price)
    exit_idx = len(frame) - 1
    exit_price = float(frame.iloc[exit_idx]["close"])
    exit_reason = "force_close_end"

    max_idx = min(len(frame) - 1, entry_idx + int(playbook.max_hold_bars))
    for i in range(entry_idx, max_idx + 1):
        row = frame.iloc[i]
        high = float(row["high"])
        low = float(row["low"])
        close = float(row["close"])

        if side == "LONG":
            hit_stop = low <= pending_stop
            hit_take = (not partial_tp_hit) and high >= take_profit_price
        else:
            hit_stop = high >= pending_stop
            hit_take = (not partial_tp_hit) and low <= take_profit_price

        if hit_stop and hit_take:
            hit_take = True
            hit_stop = False

        if hit_stop:
            exit_idx = i
            exit_price = float(pending_stop)
            exit_reason = "stop"
            break

        if hit_take:
            tp_qty = remaining_qty * float(playbook.partial_tp_fraction)
            if tp_qty > 0.0:
                partial_gross = ((take_profit_price - entry_price) * tp_qty) if side == "LONG" else ((entry_price - take_profit_price) * tp_qty)
                fee_tp = abs(take_profit_price * tp_qty) * float(fee_rate)
                partial_realized_gross += float(partial_gross)
                partial_realized_net += float(partial_gross - fee_tp)
                partial_exit_fee += float(fee_tp)
                remaining_qty -= tp_qty
                partial_tp_hit = True
                pending_stop = max(float(entry_price), pending_stop) if side == "LONG" else min(float(entry_price), pending_stop)
                be_armed = True
                if remaining_qty <= 1e-12:
                    exit_idx = i
                    exit_price = float(take_profit_price)
                    exit_reason = "take_profit"
                    break

        # Tight confirmation rule: the entry bar itself must not negate the wick idea.
        if i == entry_idx:
            if side == "LONG" and close < signal_close:
                exit_idx = i
                exit_price = close
                exit_reason = "next_close_fail"
                break
            if side == "SHORT" and close > signal_close:
                exit_idx = i
                exit_price = close
                exit_reason = "next_close_fail"
                break

        favorable_r = ((high - entry_price) / risk_abs) if side == "LONG" else ((entry_price - low) / risk_abs)
        if (not be_armed) and favorable_r >= float(playbook.be_arm_r):
            pending_stop = float(entry_price)
            be_armed = True

        vidya_line = float(row["vidya_line"])
        line_break = (close < vidya_line) if side == "LONG" else (close > vidya_line)
        if math.isfinite(vidya_line) and line_break:
            if i + 1 < len(frame):
                exit_idx = i + 1
                exit_price = float(frame.at[i + 1, "open"])
            else:
                exit_idx = i
                exit_price = close
            exit_reason = "vidya_break"
            break

        if i == max_idx:
            exit_idx = i
            exit_price = close
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
        "partial_tp_hit": bool(partial_tp_hit),
        "partial_exit_fee": float(partial_exit_fee),
    }


def _run(frame: pd.DataFrame, playbook: Playbook, analysis_start: pd.Timestamp) -> dict[str, Any]:
    candidates = _build_candidates(frame, playbook, analysis_start=analysis_start)
    trades: list[dict[str, Any]] = []
    current_exit_idx = -1
    for candidate in candidates:
        if int(candidate["entry_idx"]) <= current_exit_idx:
            continue
        trade = _simulate(frame, candidate, playbook)
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
        "side_mode": playbook.side_mode,
        "reference_mode": playbook.reference_mode,
        "trade_count": int(len(trades)),
        "signal_count": int(len(candidates)),
        "win_rate_pct": float((len(wins) / len(trades)) * 100.0) if trades else 0.0,
        "profit_factor": float(gross_profit / gross_loss) if gross_loss > 0 else (float("inf") if gross_profit > 0 else float("nan")),
        "ending_balance": float(balance),
        "return_pct": float(((balance / 1000.0) - 1.0) * 100.0),
        "max_drawdown_pct": float(max_dd_pct),
        "next_close_fail_exits": int(sum(1 for t in trades if str(t["exit_reason"]) == "next_close_fail")),
        "vidya_break_exits": int(sum(1 for t in trades if str(t["exit_reason"]) == "vidya_break")),
        "take_profit_exits": int(sum(1 for t in trades if str(t["exit_reason"]) == "take_profit")),
        "stop_exits": int(sum(1 for t in trades if str(t["exit_reason"]) == "stop")),
    }
    return {"variant": asdict(playbook), "summary": summary, "trades": trades}


def _render_report(payload: dict[str, Any]) -> str:
    lines: list[str] = []
    lines.append("# VIDYA Neutral Wick Pullback Probe")
    lines.append("")
    for window in payload["windows"]:
        lines.append(f"## {window['label']}")
        lines.append("")
        lines.append(f"- start_utc: `{window['start_utc']}`")
        lines.append(f"- end_utc: `{window['end_utc']}`")
        lines.append("")
        lines.append("| Variant | Side | Ref | Return % | Trades | Signals | Win Rate % | PF | Max DD % | Next-Close Fail |")
        lines.append("|---|---|---|---:|---:|---:|---:|---:|---:|---:|")
        for row in window["results"]:
            s = row["summary"]
            lines.append(
                f"| `{s['variant']}` | `{s['side_mode']}` | `{s['reference_mode']}` | `{s['return_pct']:.2f}` | "
                f"`{s['trade_count']}` | `{s['signal_count']}` | `{s['win_rate_pct']:.2f}` | `{s['profit_factor']:.3f}` | `{s['max_drawdown_pct']:.2f}` | `{s['next_close_fail_exits']}` |"
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
        Playbook(name="long_wick_band_v1", side_mode="long_only", reference_mode="band", min_impulse_atr=1.5, min_retrace_frac=0.20, max_retrace_frac=0.85, min_wick_penetration_atr=0.00, max_wick_penetration_atr=0.60, min_close_pos_in_range=0.60),
        Playbook(name="long_wick_line_v1", side_mode="long_only", reference_mode="line", min_impulse_atr=1.8, min_retrace_frac=0.25, max_retrace_frac=0.95, min_wick_penetration_atr=0.00, max_wick_penetration_atr=0.90, min_close_pos_in_range=0.60),
        Playbook(name="short_wick_band_v1", side_mode="short_only", reference_mode="band", min_impulse_atr=1.5, min_retrace_frac=0.20, max_retrace_frac=0.85, min_wick_penetration_atr=0.00, max_wick_penetration_atr=0.60, max_close_pos_in_range=0.40),
        Playbook(name="short_wick_line_v1", side_mode="short_only", reference_mode="line", min_impulse_atr=1.8, min_retrace_frac=0.25, max_retrace_frac=0.95, min_wick_penetration_atr=0.00, max_wick_penetration_atr=0.90, max_close_pos_in_range=0.40),
    ]

    payload: dict[str, Any] = {"windows": []}
    for window in windows:
        raw = _load_local_1m((window["start"] - pd.Timedelta(days=3)).floor("D"), window["end"])
        frame = _augment(_prepare_frame(raw))
        results = [_run(frame, playbook, analysis_start=window["start"]) for playbook in playbooks]
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
