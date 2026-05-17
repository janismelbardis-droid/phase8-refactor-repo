from __future__ import annotations

import argparse
import json
import shutil
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional

import numpy as np
import pandas as pd

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from app.indicators_streaming import compute_range_filter_series, stoch_rsi_series
from scripts.manual.probe_ms_trend_matrix_strategy import (
    DEFAULT_OUTPUT_ROOT,
    _compute_ms_trend_matrix_frame,
    _fetch_1m_klines,
    _server_now_utc,
)


LATEST_MIRROR = REPO_ROOT / "runs" / "perf" / "ms_rf_pullback_reentry_strategy_latest"


@dataclass(frozen=True)
class ReentrySpec:
    name: str
    max_bars_after_choch: int
    pullback_touch_atr: float
    stop_buffer_atr: float
    require_counter_event: bool
    require_neutral: bool
    use_target1: bool
    use_trailing_stop: bool
    require_stoch_extreme: bool = False
    stoch_oversold: float = 20.0
    stoch_overbought: float = 80.0
    require_stoch_confirm: bool = False
    stoch_entry_ceiling_long: float = 60.0
    stoch_entry_floor_short: float = 40.0
    require_micro_reclaim: bool = False
    micro_reclaim_min_bars: int = 2
    micro_reclaim_lookback: int = 3
    allowed_start_hour_utc: Optional[int] = None
    allowed_end_hour_utc: Optional[int] = None
    require_confirmation_bar: bool = False
    require_neutral_then_confirm: bool = False
    stoch_veto_only: bool = False
    stoch_veto_long_max: float = 80.0
    stoch_veto_short_min: float = 20.0
    break_even_r: Optional[float] = None


def _equity_metrics(equity: List[float]) -> Dict[str, Any]:
    if not equity:
        return {"max_drawdown_pct": float("nan")}
    peak = float(equity[0])
    max_dd_pct = 0.0
    for value in equity:
        peak = max(peak, float(value))
        if peak > 0:
            dd_pct = ((peak - float(value)) / peak) * 100.0
            max_dd_pct = max(max_dd_pct, dd_pct)
    return {"max_drawdown_pct": float(max_dd_pct)}


def _hour_allowed(ts: pd.Timestamp, spec: ReentrySpec) -> bool:
    start = spec.allowed_start_hour_utc
    end = spec.allowed_end_hour_utc
    if start is None or end is None:
        return True
    hour = int(pd.Timestamp(ts).tz_convert("UTC").hour if pd.Timestamp(ts).tzinfo else pd.Timestamp(ts, tz="UTC").hour)
    if start < end:
        return start <= hour < end
    return hour >= start or hour < end


def _prepare_frame(
    raw: pd.DataFrame,
    *,
    ms_len: int,
    atr_length: int,
    atr_mult: float,
    target_step_mult: float,
) -> pd.DataFrame:
    frame = _compute_ms_trend_matrix_frame(
        raw,
        ms_len=ms_len,
        atr_length=atr_length,
        atr_mult=atr_mult,
        target_step_mult=target_step_mult,
    ).copy()
    rf = compute_range_filter_series(frame["close"].to_numpy(dtype=float))
    for key, values in rf.items():
        frame[key] = values
    _rsi, stoch_k, stoch_d = stoch_rsi_series(frame["close"].to_numpy(dtype=float))
    frame["stoch_rsi_k"] = stoch_k
    frame["stoch_rsi_d"] = stoch_d
    frame["stoch_rsi_kd"] = np.where(
        np.isfinite(stoch_k) & np.isfinite(stoch_d),
        np.where(stoch_k > stoch_d, "GREEN", "RED"),
        None,
    )
    return frame


def _initial_context(side: str, row: pd.Series) -> Dict[str, Any]:
    return {
        "side": side,
        "signal_ts": str(pd.Timestamp(row["ts"])),
        "start_idx": int(row.name),
        "counter_seen": False,
        "neutral_seen": False,
        "touch_seen": False,
        "stoch_extreme_seen": False,
        "last_touch_idx": None,
        "entry_candidate_idx": None,
        "reclaim_ref": None,
        "neutral_idx": None,
        "confirm_ref_high": None,
        "confirm_ref_low": None,
        "break_even_armed": False,
    }


def _update_context(ctx: Dict[str, Any], row: pd.Series, spec: ReentrySpec) -> None:
    side = str(ctx["side"])
    atr = float(row["atr"]) if pd.notna(row["atr"]) else np.nan
    atr_ts = float(row["ms_atr_ts"]) if pd.notna(row["ms_atr_ts"]) else np.nan
    if not np.isfinite(atr) or not np.isfinite(atr_ts):
        return

    rf_state = str(row.get("range_filter_state") or "").upper()
    rf_buy = bool(row.get("range_filter_buy"))
    rf_sell = bool(row.get("range_filter_sell"))
    stoch_k = float(row.get("stoch_rsi_k")) if pd.notna(row.get("stoch_rsi_k")) else np.nan
    stoch_d = float(row.get("stoch_rsi_d")) if pd.notna(row.get("stoch_rsi_d")) else np.nan

    if side == "LONG":
        if rf_sell:
            ctx["counter_seen"] = True
        if rf_state == "NEUTRAL":
            ctx["neutral_seen"] = True
            ctx["neutral_idx"] = int(row.name)
        if np.isfinite(stoch_k) and stoch_k <= float(spec.stoch_oversold):
            ctx["stoch_extreme_seen"] = True
        if float(row["low"]) <= atr_ts + (spec.pullback_touch_atr * atr):
            ctx["touch_seen"] = True
            ctx["last_touch_idx"] = int(row.name)
            if ctx.get("reclaim_ref") is None:
                ctx["reclaim_ref"] = float(row["high"])
        ready = ctx["touch_seen"]
        if spec.require_counter_event:
            ready = ready and bool(ctx["counter_seen"])
        if spec.require_neutral:
            ready = ready and bool(ctx["neutral_seen"])
        if spec.require_stoch_extreme:
            ready = ready and bool(ctx["stoch_extreme_seen"])
        stoch_ok = True
        if spec.stoch_veto_only:
            stoch_ok = np.isfinite(stoch_k) and (stoch_k <= float(spec.stoch_veto_long_max))
        elif spec.require_stoch_confirm:
            stoch_ok = (
                np.isfinite(stoch_k)
                and np.isfinite(stoch_d)
                and (stoch_k > stoch_d)
                and (stoch_k <= float(spec.stoch_entry_ceiling_long))
            )
        reclaim_ok = True
        if spec.require_micro_reclaim:
            reclaim_ok = False
            touch_idx = ctx.get("last_touch_idx")
            if touch_idx is not None:
                bars_since_touch = int(row.name) - int(touch_idx)
                if bars_since_touch >= int(spec.micro_reclaim_min_bars):
                    reclaim_ok = float(row["close"]) > float(ctx["reclaim_ref"])
                ctx["reclaim_ref"] = max(float(ctx["reclaim_ref"]), float(row["high"]))
        neutral_order_ok = True
        if spec.require_neutral_then_confirm:
            neutral_order_ok = ctx["neutral_idx"] is not None and int(ctx["neutral_idx"]) < int(row.name)
        if ready and rf_buy and float(row["close"]) > atr_ts and stoch_ok and reclaim_ok and neutral_order_ok:
            if spec.require_confirmation_bar:
                if ctx["confirm_ref_high"] is None:
                    ctx["confirm_ref_high"] = float(row["high"])
            else:
                ctx["entry_candidate_idx"] = int(row.name)
        elif spec.require_confirmation_bar and ctx["confirm_ref_high"] is not None:
            if float(row["close"]) > float(ctx["confirm_ref_high"]) and float(row["close"]) > atr_ts and stoch_ok and reclaim_ok:
                ctx["entry_candidate_idx"] = int(row.name)
    else:
        if rf_buy:
            ctx["counter_seen"] = True
        if rf_state == "NEUTRAL":
            ctx["neutral_seen"] = True
            ctx["neutral_idx"] = int(row.name)
        if np.isfinite(stoch_k) and stoch_k >= float(spec.stoch_overbought):
            ctx["stoch_extreme_seen"] = True
        if float(row["high"]) >= atr_ts - (spec.pullback_touch_atr * atr):
            ctx["touch_seen"] = True
            ctx["last_touch_idx"] = int(row.name)
            if ctx.get("reclaim_ref") is None:
                ctx["reclaim_ref"] = float(row["low"])
        ready = ctx["touch_seen"]
        if spec.require_counter_event:
            ready = ready and bool(ctx["counter_seen"])
        if spec.require_neutral:
            ready = ready and bool(ctx["neutral_seen"])
        if spec.require_stoch_extreme:
            ready = ready and bool(ctx["stoch_extreme_seen"])
        stoch_ok = True
        if spec.stoch_veto_only:
            stoch_ok = np.isfinite(stoch_k) and (stoch_k >= float(spec.stoch_veto_short_min))
        elif spec.require_stoch_confirm:
            stoch_ok = (
                np.isfinite(stoch_k)
                and np.isfinite(stoch_d)
                and (stoch_k < stoch_d)
                and (stoch_k >= float(spec.stoch_entry_floor_short))
            )
        reclaim_ok = True
        if spec.require_micro_reclaim:
            reclaim_ok = False
            touch_idx = ctx.get("last_touch_idx")
            if touch_idx is not None:
                bars_since_touch = int(row.name) - int(touch_idx)
                if bars_since_touch >= int(spec.micro_reclaim_min_bars):
                    reclaim_ok = float(row["close"]) < float(ctx["reclaim_ref"])
                ctx["reclaim_ref"] = min(float(ctx["reclaim_ref"]), float(row["low"]))
        neutral_order_ok = True
        if spec.require_neutral_then_confirm:
            neutral_order_ok = ctx["neutral_idx"] is not None and int(ctx["neutral_idx"]) < int(row.name)
        if ready and rf_sell and float(row["close"]) < atr_ts and stoch_ok and reclaim_ok and neutral_order_ok:
            if spec.require_confirmation_bar:
                if ctx["confirm_ref_low"] is None:
                    ctx["confirm_ref_low"] = float(row["low"])
            else:
                ctx["entry_candidate_idx"] = int(row.name)
        elif spec.require_confirmation_bar and ctx["confirm_ref_low"] is not None:
            if float(row["close"]) < float(ctx["confirm_ref_low"]) and float(row["close"]) < atr_ts and stoch_ok and reclaim_ok:
                ctx["entry_candidate_idx"] = int(row.name)


def _run_strategy(frame: pd.DataFrame, spec: ReentrySpec) -> Dict[str, Any]:
    balance = 1000.0
    order_notional = 100.0
    fee_rate = 0.0004
    equity_curve = [balance]
    trades: List[Dict[str, Any]] = []
    signals: List[Dict[str, Any]] = []

    position: Optional[Dict[str, Any]] = None
    pending_entry: Optional[Dict[str, Any]] = None
    context: Optional[Dict[str, Any]] = None

    for i in range(len(frame)):
        row = frame.iloc[i]
        ts = pd.Timestamp(row["ts"], tz="UTC") if not isinstance(row["ts"], pd.Timestamp) else (
            pd.Timestamp(row["ts"]).tz_convert("UTC") if pd.Timestamp(row["ts"]).tzinfo else pd.Timestamp(row["ts"], tz="UTC")
        )

        if bool(row["ms_choch_up"]):
            context = _initial_context("LONG", row)
        elif bool(row["ms_choch_down"]):
            context = _initial_context("SHORT", row)

        if pending_entry is not None and i == int(pending_entry["entry_bar_idx"]):
            entry_price = float(row["open"])
            qty = order_notional / entry_price if entry_price > 0 else 0.0
            position = {
                "side": pending_entry["side"],
                "entry_ts": ts,
                "entry_price": float(entry_price),
                "qty": float(qty),
                "stop": float(pending_entry["stop"]),
                "initial_stop": float(pending_entry["stop"]),
                "target": float(pending_entry["target"]) if pending_entry["target"] is not None else np.nan,
                "signal_ts": pending_entry["signal_ts"],
                "signal_type": pending_entry["signal_type"],
                "break_even_armed": False,
            }
            pending_entry = None

        if position is not None:
            side = str(position["side"])
            stop_price = float(position["stop"])
            target_price = float(position["target"]) if np.isfinite(float(position["target"])) else np.nan
            exit_reason = None
            exit_price = np.nan

            if spec.break_even_r is not None:
                initial_stop = float(position["initial_stop"])
                entry_price = float(position["entry_price"])
                initial_r = abs(entry_price - initial_stop)
                if initial_r > 0 and not bool(position.get("break_even_armed", False)):
                    if side == "LONG":
                        be_trigger = entry_price + initial_r * float(spec.break_even_r)
                        if float(row["high"]) >= be_trigger:
                            position["stop"] = max(float(position["stop"]), entry_price)
                            position["break_even_armed"] = True
                    else:
                        be_trigger = entry_price - initial_r * float(spec.break_even_r)
                        if float(row["low"]) <= be_trigger:
                            position["stop"] = min(float(position["stop"]), entry_price)
                            position["break_even_armed"] = True
                    stop_price = float(position["stop"])

            if spec.use_trailing_stop and np.isfinite(float(row["ms_atr_ts"])):
                if side == "LONG":
                    position["stop"] = max(float(position["stop"]), float(row["ms_atr_ts"]) - spec.stop_buffer_atr * float(row["atr"]))
                else:
                    position["stop"] = min(float(position["stop"]), float(row["ms_atr_ts"]) + spec.stop_buffer_atr * float(row["atr"]))
                stop_price = float(position["stop"])

            if side == "LONG":
                hit_stop = float(row["low"]) <= stop_price
                hit_target = spec.use_target1 and np.isfinite(target_price) and float(row["high"]) >= target_price
                opposite = bool(row["ms_choch_down"])
                if hit_stop and hit_target:
                    exit_reason = "stop_loss"
                    exit_price = stop_price
                elif hit_stop:
                    exit_reason = "stop_loss"
                    exit_price = stop_price
                elif hit_target:
                    exit_reason = "target1"
                    exit_price = target_price
                elif opposite:
                    exit_reason = "opposite_choch"
                    exit_price = float(row["close"])
            else:
                hit_stop = float(row["high"]) >= stop_price
                hit_target = spec.use_target1 and np.isfinite(target_price) and float(row["low"]) <= target_price
                opposite = bool(row["ms_choch_up"])
                if hit_stop and hit_target:
                    exit_reason = "stop_loss"
                    exit_price = stop_price
                elif hit_stop:
                    exit_reason = "stop_loss"
                    exit_price = stop_price
                elif hit_target:
                    exit_reason = "target1"
                    exit_price = target_price
                elif opposite:
                    exit_reason = "opposite_choch"
                    exit_price = float(row["close"])

            if exit_reason is not None:
                qty = float(position["qty"])
                gross = ((float(exit_price) - float(position["entry_price"])) * qty) if side == "LONG" else ((float(position["entry_price"]) - float(exit_price)) * qty)
                fee_entry = order_notional * fee_rate
                fee_exit = abs(float(exit_price) * qty) * fee_rate
                net = gross - fee_entry - fee_exit
                balance += float(net)
                trades.append(
                    {
                        "side": side,
                        "entry_ts": str(position["entry_ts"]),
                        "exit_ts": str(ts),
                        "entry_price": float(position["entry_price"]),
                        "exit_price": float(exit_price),
                        "net_pnl": float(net),
                        "exit_reason": str(exit_reason),
                        "signal_type": str(position["signal_type"]),
                    }
                )
                equity_curve.append(float(balance))
                position = None

        if context is not None and position is None and pending_entry is None:
            age = int(i - int(context["start_idx"]))
            if age > int(spec.max_bars_after_choch) or age < 1:
                if age > int(spec.max_bars_after_choch):
                    context = None
            else:
                _update_context(context, row, spec)
                if context.get("entry_candidate_idx") == int(row.name) and _hour_allowed(ts, spec):
                    entry_bar_idx = i + 1
                    if entry_bar_idx < len(frame) and np.isfinite(float(row["ms_atr_ts"])) and np.isfinite(float(row["atr"])):
                        if str(context["side"]) == "LONG":
                            stop = float(row["ms_atr_ts"]) - spec.stop_buffer_atr * float(row["atr"])
                        else:
                            stop = float(row["ms_atr_ts"]) + spec.stop_buffer_atr * float(row["atr"])
                        pending_entry = {
                            "side": str(context["side"]),
                            "entry_bar_idx": entry_bar_idx,
                            "stop": float(stop),
                            "target": float(row["ms_current_target"]) if spec.use_target1 and np.isfinite(float(row["ms_current_target"])) else np.nan,
                            "signal_ts": str(ts),
                            "signal_type": f"ms_rf_pullback_{str(context['side']).lower()}",
                        }
                        signals.append(
                            {
                                "signal_ts": str(ts),
                                "side": str(context["side"]),
                                "entry_bar_idx": int(entry_bar_idx),
                                "bars_from_choch": age,
                                "signal_type": f"ms_rf_pullback_{str(context['side']).lower()}",
                                "counter_seen": bool(context["counter_seen"]),
                                "neutral_seen": bool(context["neutral_seen"]),
                                "touch_seen": bool(context["touch_seen"]),
                                "stoch_extreme_seen": bool(context["stoch_extreme_seen"]),
                            }
                        )
                    context = None

    if position is not None:
        last = frame.iloc[-1]
        ts = pd.Timestamp(last["ts"])
        exit_price = float(last["close"])
        qty = float(position["qty"])
        side = str(position["side"])
        gross = ((float(exit_price) - float(position["entry_price"])) * qty) if side == "LONG" else ((float(position["entry_price"]) - float(exit_price)) * qty)
        fee_entry = order_notional * fee_rate
        fee_exit = abs(float(exit_price) * qty) * fee_rate
        net = gross - fee_entry - fee_exit
        balance += float(net)
        trades.append(
            {
                "side": side,
                "entry_ts": str(position["entry_ts"]),
                "exit_ts": str(ts),
                "entry_price": float(position["entry_price"]),
                "exit_price": float(exit_price),
                "net_pnl": float(net),
                "exit_reason": "force_close_end",
                "signal_type": str(position["signal_type"]),
            }
        )
        equity_curve.append(float(balance))

    gross_profit = sum(max(0.0, float(t["net_pnl"])) for t in trades)
    gross_loss = sum(-min(0.0, float(t["net_pnl"])) for t in trades)
    win_count = sum(1 for t in trades if float(t["net_pnl"]) > 0)
    summary = {
        "strategy": spec.name,
        "ending_balance": float(balance),
        "return_pct": float(((balance / 1000.0) - 1.0) * 100.0),
        "num_trades": int(len(trades)),
        "num_signals": int(len(signals)),
        "win_rate_pct": float((win_count / len(trades)) * 100.0) if trades else 0.0,
        "profit_factor": float(gross_profit / gross_loss) if gross_loss > 0 else (float("inf") if gross_profit > 0 else float("nan")),
        "avg_trade_pnl": float(np.mean([float(t["net_pnl"]) for t in trades])) if trades else 0.0,
        **_equity_metrics(equity_curve),
    }
    return {"summary": summary, "signals": signals[:30], "trades": trades[:30]}


def _render_report(payload: Dict[str, Any]) -> str:
    lines: List[str] = []
    lines.append("# MS + RF Pullback Re-entry Strategy Probe")
    lines.append("")
    lines.append(f"- symbol: `{payload['symbol']}`")
    lines.append(f"- start_utc: `{payload['start_utc']}`")
    lines.append(f"- end_utc: `{payload['end_utc']}`")
    lines.append(f"- bars: `{payload['bars']}`")
    lines.append("")
    lines.append("| Strategy | Return % | End Balance | Trades | Signals | Win Rate % | Profit Factor | Max DD % |")
    lines.append("|---|---:|---:|---:|---:|---:|---:|---:|")
    for row in payload["results"]:
        s = row["summary"]
        lines.append(
            f"| `{s['strategy']}` | `{s['return_pct']:.2f}` | `{s['ending_balance']:.2f}` | `{s['num_trades']}` | `{s['num_signals']}` | "
            f"`{s['win_rate_pct']:.2f}` | `{s['profit_factor']:.3f}` | `{s['max_drawdown_pct']:.2f}` |"
        )
    return "\n".join(lines) + "\n"


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Probe standalone market-structure + range-filter pullback re-entry strategy on 1m BTCUSDT.")
    parser.add_argument("--symbol", default="BTCUSDT")
    parser.add_argument("--days-back", type=int, default=120)
    parser.add_argument("--ms-len", type=int, default=10)
    parser.add_argument("--atr-length", type=int, default=14)
    parser.add_argument("--atr-mult", type=float, default=4.0)
    parser.add_argument("--target-step-mult", type=float, default=2.0)
    parser.add_argument("--output-dir", default=None)
    return parser


def main() -> int:
    args = build_parser().parse_args()
    end = _server_now_utc().floor("min") - pd.Timedelta(minutes=1)
    start = end - pd.Timedelta(days=int(args.days_back))
    raw = _fetch_1m_klines(str(args.symbol).upper(), start, end)
    frame = _prepare_frame(
        raw,
        ms_len=int(args.ms_len),
        atr_length=int(args.atr_length),
        atr_mult=float(args.atr_mult),
        target_step_mult=float(args.target_step_mult),
    )

    specs = [
        ReentrySpec(
            name="ms_rf_reset_neutral_trail",
            max_bars_after_choch=60,
            pullback_touch_atr=0.20,
            stop_buffer_atr=0.12,
            require_counter_event=True,
            require_neutral=True,
            use_target1=False,
            use_trailing_stop=True,
        ),
        ReentrySpec(
            name="ms_rf_reset_neutral_stoch_confirm_trail",
            max_bars_after_choch=60,
            pullback_touch_atr=0.20,
            stop_buffer_atr=0.12,
            require_counter_event=True,
            require_neutral=True,
            use_target1=False,
            use_trailing_stop=True,
            require_stoch_confirm=True,
            stoch_entry_ceiling_long=60.0,
            stoch_entry_floor_short=40.0,
        ),
        ReentrySpec(
            name="ms_rf_stoch_confirm_london",
            max_bars_after_choch=60,
            pullback_touch_atr=0.20,
            stop_buffer_atr=0.12,
            require_counter_event=True,
            require_neutral=True,
            use_target1=False,
            use_trailing_stop=True,
            require_stoch_confirm=True,
            stoch_entry_ceiling_long=60.0,
            stoch_entry_floor_short=40.0,
            allowed_start_hour_utc=6,
            allowed_end_hour_utc=11,
        ),
        ReentrySpec(
            name="ms_rf_stoch_confirm_overlap",
            max_bars_after_choch=60,
            pullback_touch_atr=0.20,
            stop_buffer_atr=0.12,
            require_counter_event=True,
            require_neutral=True,
            use_target1=False,
            use_trailing_stop=True,
            require_stoch_confirm=True,
            stoch_entry_ceiling_long=60.0,
            stoch_entry_floor_short=40.0,
            allowed_start_hour_utc=11,
            allowed_end_hour_utc=16,
        ),
        ReentrySpec(
            name="ms_rf_stoch_confirm_overlap_early25",
            max_bars_after_choch=25,
            pullback_touch_atr=0.20,
            stop_buffer_atr=0.12,
            require_counter_event=True,
            require_neutral=True,
            use_target1=False,
            use_trailing_stop=True,
            require_stoch_confirm=True,
            stoch_entry_ceiling_long=60.0,
            stoch_entry_floor_short=40.0,
            allowed_start_hour_utc=11,
            allowed_end_hour_utc=16,
        ),
        ReentrySpec(
            name="ms_rf_veto_confirm_be_overlap_early25",
            max_bars_after_choch=25,
            pullback_touch_atr=0.20,
            stop_buffer_atr=0.12,
            require_counter_event=True,
            require_neutral=True,
            use_target1=False,
            use_trailing_stop=True,
            stoch_veto_only=True,
            stoch_veto_long_max=70.0,
            stoch_veto_short_min=30.0,
            require_confirmation_bar=True,
            require_neutral_then_confirm=True,
            break_even_r=1.0,
            allowed_start_hour_utc=11,
            allowed_end_hour_utc=16,
        ),
        ReentrySpec(
            name="ms_rf_stoch_confirm_asia",
            max_bars_after_choch=60,
            pullback_touch_atr=0.20,
            stop_buffer_atr=0.12,
            require_counter_event=True,
            require_neutral=True,
            use_target1=False,
            use_trailing_stop=True,
            require_stoch_confirm=True,
            stoch_entry_ceiling_long=60.0,
            stoch_entry_floor_short=40.0,
            allowed_start_hour_utc=0,
            allowed_end_hour_utc=6,
        ),
        ReentrySpec(
            name="ms_rf_reset_neutral_stoch_confirm_reclaim_trail",
            max_bars_after_choch=60,
            pullback_touch_atr=0.20,
            stop_buffer_atr=0.12,
            require_counter_event=True,
            require_neutral=True,
            use_target1=False,
            use_trailing_stop=True,
            require_stoch_confirm=True,
            stoch_entry_ceiling_long=60.0,
            stoch_entry_floor_short=40.0,
            require_micro_reclaim=True,
            micro_reclaim_min_bars=2,
            micro_reclaim_lookback=3,
        ),
        ReentrySpec(
            name="ms_rf_reset_neutral_stoch_extreme_trail",
            max_bars_after_choch=60,
            pullback_touch_atr=0.20,
            stop_buffer_atr=0.12,
            require_counter_event=True,
            require_neutral=True,
            use_target1=False,
            use_trailing_stop=True,
            require_stoch_extreme=True,
            require_stoch_confirm=True,
            stoch_oversold=20.0,
            stoch_overbought=80.0,
            stoch_entry_ceiling_long=65.0,
            stoch_entry_floor_short=35.0,
        ),
        ReentrySpec(
            name="ms_rf_reset_neutral_target1",
            max_bars_after_choch=60,
            pullback_touch_atr=0.20,
            stop_buffer_atr=0.12,
            require_counter_event=True,
            require_neutral=True,
            use_target1=True,
            use_trailing_stop=True,
        ),
        ReentrySpec(
            name="ms_rf_reset_simple_trail",
            max_bars_after_choch=60,
            pullback_touch_atr=0.20,
            stop_buffer_atr=0.12,
            require_counter_event=False,
            require_neutral=False,
            use_target1=False,
            use_trailing_stop=True,
        ),
    ]

    results = [_run_strategy(frame, spec) for spec in specs]
    payload = {
        "symbol": str(args.symbol).upper(),
        "start_utc": str(pd.Timestamp(start).tz_convert("UTC").isoformat() if pd.Timestamp(start).tzinfo else pd.Timestamp(start, tz="UTC").isoformat()),
        "end_utc": str(pd.Timestamp(end).tz_convert("UTC").isoformat() if pd.Timestamp(end).tzinfo else pd.Timestamp(end, tz="UTC").isoformat()),
        "bars": int(len(frame)),
        "params": {
            "ms_len": int(args.ms_len),
            "atr_length": int(args.atr_length),
            "atr_mult": float(args.atr_mult),
            "target_step_mult": float(args.target_step_mult),
        },
        "results": results,
    }

    output_dir = Path(args.output_dir).resolve() if args.output_dir else (
        DEFAULT_OUTPUT_ROOT / f"ms_rf_pullback_reentry_strategy_{pd.Timestamp.now('UTC').strftime('%Y%m%d_%H%M%S')}"
    )
    output_dir.mkdir(parents=True, exist_ok=True)
    json_path = output_dir / "summary.json"
    md_path = output_dir / "report.md"
    json_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    md_path.write_text(_render_report(payload), encoding="utf-8")

    if LATEST_MIRROR.exists():
        shutil.rmtree(LATEST_MIRROR)
    shutil.copytree(output_dir, LATEST_MIRROR)

    print(f"Output dir: {output_dir}")
    print(f"Latest copy: {LATEST_MIRROR}")
    for row in results:
        s = row["summary"]
        print(
            f"{s['strategy']}: return={s['return_pct']:.2f}% end={s['ending_balance']:.2f} "
            f"trades={s['num_trades']} signals={s['num_signals']} win={s['win_rate_pct']:.2f}% "
            f"pf={s['profit_factor']:.3f} dd={s['max_drawdown_pct']:.2f}%"
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
