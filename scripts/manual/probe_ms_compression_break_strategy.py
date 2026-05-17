from __future__ import annotations

import argparse
import json
import shutil
from dataclasses import dataclass
from pathlib import Path
import sys
from typing import Any, Dict, List, Optional

import numpy as np
import pandas as pd

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from scripts.manual.probe_ms_trend_matrix_strategy import (
    DEFAULT_OUTPUT_ROOT,
    _compute_ms_trend_matrix_frame,
    _fetch_1m_klines,
    _server_now_utc,
)


LATEST_MIRROR = REPO_ROOT / "runs" / "perf" / "ms_compression_break_strategy_latest"


@dataclass(frozen=True)
class CompressionSpec:
    name: str
    compression_lookback: int
    min_flips: int
    range_atr_mult: float
    drift_atr_mult: float
    break_buffer_atr: float
    hold_bars: int
    hold_tolerance_atr: float
    use_target1: bool
    use_trailing_stop: bool


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


def _prepare_frame(
    raw: pd.DataFrame,
    *,
    ms_len: int,
    atr_length: int,
    atr_mult: float,
    target_step_mult: float,
    spec: CompressionSpec,
) -> pd.DataFrame:
    frame = _compute_ms_trend_matrix_frame(
        raw,
        ms_len=ms_len,
        atr_length=atr_length,
        atr_mult=atr_mult,
        target_step_mult=target_step_mult,
    ).copy()
    flips = (frame["ms_choch_up"] | frame["ms_choch_down"]).astype(int)
    high_prev = frame["high"].shift(1).rolling(spec.compression_lookback, min_periods=spec.compression_lookback).max()
    low_prev = frame["low"].shift(1).rolling(spec.compression_lookback, min_periods=spec.compression_lookback).min()
    flip_prev = flips.shift(1).rolling(spec.compression_lookback, min_periods=spec.compression_lookback).sum()
    close_prev = frame["close"].shift(spec.compression_lookback)
    atr = frame["atr"]

    frame["cb_range_high_prev"] = high_prev
    frame["cb_range_low_prev"] = low_prev
    frame["cb_flip_count_prev"] = flip_prev
    frame["cb_range_width_atr"] = (high_prev - low_prev) / atr
    frame["cb_drift_atr"] = (frame["close"] - close_prev).abs() / atr
    frame["cb_compression"] = (
        (flip_prev >= spec.min_flips)
        & (frame["cb_range_width_atr"] <= spec.range_atr_mult)
        & (frame["cb_drift_atr"] <= spec.drift_atr_mult)
        & atr.notna()
    )
    frame["cb_long_candidate"] = (
        frame["cb_compression"]
        & frame["ms_choch_up"]
        & (frame["close"] > (high_prev + (atr * spec.break_buffer_atr)))
    )
    frame["cb_short_candidate"] = (
        frame["cb_compression"]
        & frame["ms_choch_down"]
        & (frame["close"] < (low_prev - (atr * spec.break_buffer_atr)))
    )
    return frame


def _run_strategy(frame: pd.DataFrame, spec: CompressionSpec) -> Dict[str, Any]:
    balance = 1000.0
    order_notional = 100.0
    fee_rate = 0.0004
    equity_curve = [balance]
    trades: List[Dict[str, Any]] = []
    signals: List[Dict[str, Any]] = []

    position: Optional[Dict[str, Any]] = None
    pending_entry: Optional[Dict[str, Any]] = None
    validation: Optional[Dict[str, Any]] = None

    for i in range(len(frame)):
        row = frame.iloc[i]
        ts = pd.Timestamp(row["ts"], tz="UTC") if not isinstance(row["ts"], pd.Timestamp) else (
            pd.Timestamp(row["ts"]).tz_convert("UTC") if pd.Timestamp(row["ts"]).tzinfo else pd.Timestamp(row["ts"], tz="UTC")
        )

        if pending_entry is not None and i == int(pending_entry["entry_bar_idx"]):
            entry_price = float(row["open"])
            qty = order_notional / entry_price if entry_price > 0 else 0.0
            position = {
                "side": pending_entry["side"],
                "entry_ts": ts,
                "entry_price": entry_price,
                "qty": float(qty),
                "stop": float(pending_entry["stop"]),
                "target": float(pending_entry["target"]) if pending_entry["target"] is not None else np.nan,
                "signal_ts": pending_entry["signal_ts"],
                "signal_type": pending_entry["signal_type"],
                "boundary": float(pending_entry["boundary"]),
            }
            pending_entry = None

        if position is not None:
            side = str(position["side"])
            stop_price = float(position["stop"])
            target_price = float(position["target"]) if np.isfinite(float(position["target"])) else np.nan
            exit_reason = None
            exit_price = np.nan

            if spec.use_trailing_stop and np.isfinite(float(row["ms_atr_ts"])):
                if side == "LONG":
                    position["stop"] = max(float(position["stop"]), float(row["ms_atr_ts"]))
                else:
                    position["stop"] = min(float(position["stop"]), float(row["ms_atr_ts"]))
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

        if validation is not None:
            side = str(validation["side"])
            boundary = float(validation["boundary"])
            tol = float(validation["tol"])
            invalid = False
            if side == "LONG":
                invalid = (float(row["low"]) < (boundary - tol)) or (float(row["close"]) < (boundary - tol))
            else:
                invalid = (float(row["high"]) > (boundary + tol)) or (float(row["close"]) > (boundary + tol))

            if invalid:
                validation = None
            else:
                validation["bars_ok"] = int(validation["bars_ok"]) + 1
                if int(validation["bars_ok"]) >= int(validation["hold_bars"]):
                    entry_bar_idx = i + 1
                    if entry_bar_idx < len(frame):
                        pending_entry = {
                            "side": side,
                            "entry_bar_idx": entry_bar_idx,
                            "stop": float(validation["stop"]),
                            "target": float(validation["target"]) if validation["target"] is not None else np.nan,
                            "signal_ts": validation["signal_ts"],
                            "signal_type": validation["signal_type"],
                            "boundary": boundary,
                        }
                        signals.append(
                            {
                                "signal_ts": validation["signal_ts"],
                                "side": side,
                                "boundary": boundary,
                                "entry_bar_idx": entry_bar_idx,
                                "signal_type": validation["signal_type"],
                            }
                        )
                    validation = None

        if position is None and pending_entry is None and validation is None and i + spec.hold_bars + 1 < len(frame):
            long_candidate = bool(row["cb_long_candidate"])
            short_candidate = bool(row["cb_short_candidate"])
            if long_candidate and np.isfinite(float(row["ms_atr_ts"])) and np.isfinite(float(row["cb_range_high_prev"])):
                validation = {
                    "side": "LONG",
                    "bars_ok": 0,
                    "hold_bars": int(spec.hold_bars),
                    "boundary": float(row["cb_range_high_prev"]),
                    "tol": float(row["atr"]) * float(spec.hold_tolerance_atr),
                    "stop": min(float(row["ms_atr_ts"]), float(row["low"])),
                    "target": float(row["ms_current_target"]) if spec.use_target1 and np.isfinite(float(row["ms_current_target"])) else np.nan,
                    "signal_ts": str(ts),
                    "signal_type": "compression_break_long",
                }
            elif short_candidate and np.isfinite(float(row["ms_atr_ts"])) and np.isfinite(float(row["cb_range_low_prev"])):
                validation = {
                    "side": "SHORT",
                    "bars_ok": 0,
                    "hold_bars": int(spec.hold_bars),
                    "boundary": float(row["cb_range_low_prev"]),
                    "tol": float(row["atr"]) * float(spec.hold_tolerance_atr),
                    "stop": max(float(row["ms_atr_ts"]), float(row["high"])),
                    "target": float(row["ms_current_target"]) if spec.use_target1 and np.isfinite(float(row["ms_current_target"])) else np.nan,
                    "signal_ts": str(ts),
                    "signal_type": "compression_break_short",
                }

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
    return {"summary": summary, "signals": signals[:20], "trades": trades[:20]}


def _render_report(payload: Dict[str, Any]) -> str:
    lines: List[str] = []
    lines.append("# MS Compression Break Strategy Probe")
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
    parser = argparse.ArgumentParser(description="Probe standalone compression-break market-structure hypothesis on 1m BTCUSDT.")
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

    specs = [
        CompressionSpec(
            name="compression_break_hold1_trail",
            compression_lookback=60,
            min_flips=3,
            range_atr_mult=6.0,
            drift_atr_mult=1.5,
            break_buffer_atr=0.25,
            hold_bars=1,
            hold_tolerance_atr=0.10,
            use_target1=False,
            use_trailing_stop=True,
        ),
        CompressionSpec(
            name="compression_break_hold1_target1",
            compression_lookback=60,
            min_flips=3,
            range_atr_mult=6.0,
            drift_atr_mult=1.5,
            break_buffer_atr=0.25,
            hold_bars=1,
            hold_tolerance_atr=0.10,
            use_target1=True,
            use_trailing_stop=True,
        ),
        CompressionSpec(
            name="compression_break_hold2_trail",
            compression_lookback=60,
            min_flips=3,
            range_atr_mult=6.0,
            drift_atr_mult=1.5,
            break_buffer_atr=0.25,
            hold_bars=2,
            hold_tolerance_atr=0.10,
            use_target1=False,
            use_trailing_stop=True,
        ),
    ]

    results = []
    for spec in specs:
        frame = _prepare_frame(
            raw,
            ms_len=int(args.ms_len),
            atr_length=int(args.atr_length),
            atr_mult=float(args.atr_mult),
            target_step_mult=float(args.target_step_mult),
            spec=spec,
        )
        results.append(_run_strategy(frame, spec))

    payload = {
        "symbol": str(args.symbol).upper(),
        "start_utc": str(pd.Timestamp(start).tz_convert("UTC").isoformat() if pd.Timestamp(start).tzinfo else pd.Timestamp(start, tz="UTC").isoformat()),
        "end_utc": str(pd.Timestamp(end).tz_convert("UTC").isoformat() if pd.Timestamp(end).tzinfo else pd.Timestamp(end, tz="UTC").isoformat()),
        "bars": int(len(raw)),
        "params": {
            "ms_len": int(args.ms_len),
            "atr_length": int(args.atr_length),
            "atr_mult": float(args.atr_mult),
            "target_step_mult": float(args.target_step_mult),
        },
        "results": results,
    }

    output_dir = Path(args.output_dir).resolve() if args.output_dir else (
        DEFAULT_OUTPUT_ROOT / f"ms_compression_break_strategy_{pd.Timestamp.now('UTC').strftime('%Y%m%d_%H%M%S')}"
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
