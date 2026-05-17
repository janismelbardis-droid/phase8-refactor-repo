from __future__ import annotations

import argparse
import json
import math
from dataclasses import asdict, dataclass
from pathlib import Path
import shutil
from typing import Any, Dict, List, Optional

import numpy as np
import pandas as pd
import requests


REPO_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_OUTPUT_ROOT = REPO_ROOT / "runs" / "perf"
LATEST_MIRROR = REPO_ROOT / "runs" / "perf" / "ms_trend_matrix_strategy_latest"
BINANCE_KLINES_URL = "https://fapi.binance.com/fapi/v1/klines"
BINANCE_TIME_URL = "https://fapi.binance.com/fapi/v1/time"


@dataclass(frozen=True)
class StrategySpec:
    name: str
    require_ema50_retest: bool
    retest_lookback_bars: int
    use_target1: bool
    use_trailing_stop: bool
    long_only: bool = False
    short_only: bool = False


def _server_now_utc() -> pd.Timestamp:
    payload = requests.get(BINANCE_TIME_URL, timeout=20).json()
    return pd.to_datetime(int(payload["serverTime"]), unit="ms", utc=True)


def _fetch_1m_klines(symbol: str, start: pd.Timestamp, end: pd.Timestamp) -> pd.DataFrame:
    rows: List[list[Any]] = []
    cursor = pd.to_datetime(start, utc=True)
    stop = pd.to_datetime(end, utc=True)
    while cursor <= stop:
        params = {
            "symbol": str(symbol).upper(),
            "interval": "1m",
            "startTime": int(cursor.timestamp() * 1000),
            "endTime": int(stop.timestamp() * 1000),
            "limit": 1500,
        }
        response = requests.get(BINANCE_KLINES_URL, params=params, timeout=30)
        response.raise_for_status()
        payload = response.json()
        if not payload:
            break
        rows.extend(payload)
        last_open = pd.to_datetime(int(payload[-1][0]), unit="ms", utc=True)
        next_cursor = last_open + pd.Timedelta(minutes=1)
        if next_cursor <= cursor:
            break
        cursor = next_cursor
        if len(payload) < 1500:
            break

    cols = [
        "open_time",
        "open",
        "high",
        "low",
        "close",
        "volume",
        "close_time",
        "quote_volume",
        "trade_count",
        "taker_buy_volume",
        "taker_buy_quote_volume",
        "ignore",
    ]
    frame = pd.DataFrame(rows, columns=cols)
    if frame.empty:
        raise RuntimeError("No klines returned from Binance.")
    frame["open_time"] = pd.to_datetime(frame["open_time"], unit="ms", utc=True)
    frame["close_time"] = pd.to_datetime(frame["close_time"], unit="ms", utc=True)
    for col in ("open", "high", "low", "close", "volume"):
        frame[col] = pd.to_numeric(frame[col], errors="coerce")
    frame = frame.drop_duplicates(subset=["open_time"], keep="last").sort_values("open_time").reset_index(drop=True)
    return frame


def _ema(values: np.ndarray, length: int) -> np.ndarray:
    out = np.full(len(values), np.nan, dtype=float)
    alpha = 2.0 / (float(length) + 1.0)
    prev = np.nan
    for i, value in enumerate(values):
        if not np.isfinite(value):
            continue
        prev = value if not np.isfinite(prev) else (alpha * value + (1.0 - alpha) * prev)
        out[i] = prev
    return out


def _rma(values: np.ndarray, length: int) -> np.ndarray:
    out = np.full(len(values), np.nan, dtype=float)
    acc = np.nan
    seed: List[float] = []
    alpha = 1.0 / float(length)
    for i, value in enumerate(values):
        if not np.isfinite(value):
            continue
        seed.append(float(value))
        if not np.isfinite(acc):
            if len(seed) < int(length):
                continue
            acc = float(np.mean(seed[-int(length):]))
            out[i] = acc
            continue
        acc = acc + alpha * (float(value) - acc)
        out[i] = acc
    return out


def _atr(high: np.ndarray, low: np.ndarray, close: np.ndarray, length: int) -> np.ndarray:
    prev_close = np.r_[np.nan, close[:-1]]
    tr = np.nanmax(
        np.vstack(
            [
                high - low,
                np.abs(high - prev_close),
                np.abs(low - prev_close),
            ]
        ),
        axis=0,
    )
    return _rma(tr, length)


def _compute_ms_trend_matrix_frame(
    frame: pd.DataFrame,
    *,
    ms_len: int,
    atr_length: int,
    atr_mult: float,
    target_step_mult: float,
) -> pd.DataFrame:
    n = len(frame)
    high = frame["high"].to_numpy(dtype=float)
    low = frame["low"].to_numpy(dtype=float)
    close = frame["close"].to_numpy(dtype=float)
    open_ = frame["open"].to_numpy(dtype=float)
    atr = _atr(high, low, close, atr_length)

    ph_confirm = np.full(n, np.nan, dtype=float)
    pl_confirm = np.full(n, np.nan, dtype=float)
    for pivot_idx in range(ms_len, n - ms_len):
        hh = high[pivot_idx - ms_len : pivot_idx + ms_len + 1]
        ll = low[pivot_idx - ms_len : pivot_idx + ms_len + 1]
        if np.isfinite(high[pivot_idx]) and high[pivot_idx] == np.max(hh):
            confirm_idx = pivot_idx + ms_len
            if confirm_idx < n:
                ph_confirm[confirm_idx] = high[pivot_idx]
        if np.isfinite(low[pivot_idx]) and low[pivot_idx] == np.min(ll):
            confirm_idx = pivot_idx + ms_len
            if confirm_idx < n:
                pl_confirm[confirm_idx] = low[pivot_idx]

    direction = False
    ph_val = np.nan
    pl_val = np.nan
    ph_idx: Optional[int] = None
    pl_idx: Optional[int] = None
    atr_ts = np.nan
    entry_price = np.nan
    current_target = np.nan
    trend_start_idx: Optional[int] = None

    out_rows: List[Dict[str, Any]] = []
    prev_close = np.nan
    prev_ph_val = np.nan
    prev_pl_val = np.nan
    prev_direction = direction

    for i in range(n):
        if np.isfinite(ph_confirm[i]):
            ph_val = high[i - ms_len]
            ph_idx = i - ms_len
        if np.isfinite(pl_confirm[i]):
            pl_val = low[i - ms_len]
            pl_idx = i - ms_len

        choch_up = bool(
            np.isfinite(prev_close)
            and np.isfinite(prev_ph_val)
            and np.isfinite(close[i])
            and np.isfinite(ph_val)
            and (prev_close <= prev_ph_val)
            and (close[i] > ph_val)
            and (not direction)
        )
        choch_down = bool(
            np.isfinite(prev_close)
            and np.isfinite(prev_pl_val)
            and np.isfinite(close[i])
            and np.isfinite(pl_val)
            and (prev_close >= prev_pl_val)
            and (close[i] < pl_val)
            and direction
        )

        if choch_up:
            direction = True
            atr_ts = close[i] - (atr[i] * atr_mult) if np.isfinite(atr[i]) else np.nan
            entry_price = ph_val
            current_target = entry_price + (atr[i] * target_step_mult) if np.isfinite(atr[i]) else np.nan
            trend_start_idx = i
        elif choch_down:
            direction = False
            atr_ts = close[i] + (atr[i] * atr_mult) if np.isfinite(atr[i]) else np.nan
            entry_price = pl_val
            current_target = entry_price - (atr[i] * target_step_mult) if np.isfinite(atr[i]) else np.nan
            trend_start_idx = i

        direction_change = direction != prev_direction
        if direction and np.isfinite(atr[i]):
            base = close[i] - (atr[i] * atr_mult)
            atr_ts = base if not np.isfinite(atr_ts) else max(float(atr_ts), float(base))
        elif (not direction) and np.isfinite(atr[i]):
            base = close[i] + (atr[i] * atr_mult)
            atr_ts = base if not np.isfinite(atr_ts) else min(float(atr_ts), float(base))

        target_hit = False
        if direction and np.isfinite(current_target) and high[i] >= current_target:
            target_hit = True
            current_target = current_target + (atr[i] * target_step_mult) if np.isfinite(atr[i]) else current_target
        elif (not direction) and np.isfinite(current_target) and low[i] <= current_target:
            target_hit = True
            current_target = current_target - (atr[i] * target_step_mult) if np.isfinite(atr[i]) else current_target

        out_rows.append(
            {
                "ts": frame.at[i, "open_time"],
                "open": float(open_[i]),
                "high": float(high[i]),
                "low": float(low[i]),
                "close": float(close[i]),
                "atr": float(atr[i]) if np.isfinite(atr[i]) else np.nan,
                "ms_pivot_high_value": float(ph_val) if np.isfinite(ph_val) else np.nan,
                "ms_pivot_low_value": float(pl_val) if np.isfinite(pl_val) else np.nan,
                "ms_pivot_high_time": frame.at[ph_idx, "open_time"] if ph_idx is not None else pd.NaT,
                "ms_pivot_low_time": frame.at[pl_idx, "open_time"] if pl_idx is not None else pd.NaT,
                "ms_direction_bull": bool(direction),
                "ms_direction_change": bool(direction_change),
                "ms_choch_up": bool(choch_up),
                "ms_choch_down": bool(choch_down),
                "ms_atr_ts": float(atr_ts) if np.isfinite(atr_ts) else np.nan,
                "ms_entry_price": float(entry_price) if np.isfinite(entry_price) else np.nan,
                "ms_current_target": float(current_target) if np.isfinite(current_target) else np.nan,
                "ms_target_hit": bool(target_hit),
                "ms_trend_start_time": frame.at[trend_start_idx, "open_time"] if trend_start_idx is not None else pd.NaT,
            }
        )

        prev_close = close[i]
        prev_ph_val = ph_val
        prev_pl_val = pl_val
        prev_direction = direction

    out = pd.DataFrame(out_rows)
    out["ema50"] = _ema(close, 50)
    out["ema200"] = _ema(close, 200)
    out["ema200_prev20"] = pd.Series(out["ema200"]).shift(20).to_numpy(dtype=float)
    out["bull_regime"] = (
        (out["close"] > out["ema200"])
        & (out["ema50"] > out["ema200"])
        & (out["ema200"] > out["ema200_prev20"])
    )
    out["bear_regime"] = (
        (out["close"] < out["ema200"])
        & (out["ema50"] < out["ema200"])
        & (out["ema200"] < out["ema200_prev20"])
    )
    return out


def _retest_ok(frame: pd.DataFrame, idx: int, *, side: str, lookback: int) -> bool:
    start = max(0, int(idx) - int(lookback))
    window = frame.iloc[start : int(idx) + 1]
    if window.empty:
        return False
    if side == "LONG":
        return bool((window["low"] <= window["ema50"]).any())
    return bool((window["high"] >= window["ema50"]).any())


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


def _run_strategy(frame: pd.DataFrame, spec: StrategySpec) -> Dict[str, Any]:
    balance = 1000.0
    order_notional = 100.0
    fee_rate = 0.0004
    equity_curve = [balance]
    trades: List[Dict[str, Any]] = []

    position: Optional[Dict[str, Any]] = None
    pending_entry: Optional[Dict[str, Any]] = None

    for i in range(len(frame)):
        row = frame.iloc[i]
        ts = pd.Timestamp(row["ts"], tz="UTC") if not isinstance(row["ts"], pd.Timestamp) else pd.Timestamp(row["ts"]).tz_convert("UTC") if pd.Timestamp(row["ts"]).tzinfo else pd.Timestamp(row["ts"], tz="UTC")

        if pending_entry is not None and i == int(pending_entry["entry_bar_idx"]):
            entry_price = float(row["open"])
            qty = order_notional / entry_price if entry_price > 0 else 0.0
            position = {
                "side": pending_entry["side"],
                "entry_ts": ts,
                "entry_price": float(entry_price),
                "qty": float(qty),
                "stop": float(pending_entry["stop"]),
                "target": float(pending_entry["target"]) if pending_entry["target"] is not None else np.nan,
                "signal_ts": pending_entry["signal_ts"],
                "signal_type": pending_entry["signal_type"],
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
                opposite = bool(row["ms_choch_down"]) and bool(row["bear_regime"])
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
                opposite = bool(row["ms_choch_up"]) and bool(row["bull_regime"])
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

        if position is None and pending_entry is None and i + 1 < len(frame):
            long_signal = bool(row["ms_choch_up"]) and bool(row["bull_regime"])
            short_signal = bool(row["ms_choch_down"]) and bool(row["bear_regime"])

            if spec.require_ema50_retest:
                if long_signal:
                    long_signal = _retest_ok(frame, i, side="LONG", lookback=spec.retest_lookback_bars)
                if short_signal:
                    short_signal = _retest_ok(frame, i, side="SHORT", lookback=spec.retest_lookback_bars)

            if spec.long_only:
                short_signal = False
            if spec.short_only:
                long_signal = False

            if long_signal and np.isfinite(float(row["ms_atr_ts"])):
                pending_entry = {
                    "side": "LONG",
                    "entry_bar_idx": i + 1,
                    "stop": float(row["ms_atr_ts"]),
                    "target": float(row["ms_current_target"]) if np.isfinite(float(row["ms_current_target"])) else np.nan,
                    "signal_ts": str(ts),
                    "signal_type": "ChoCh_up_bull_regime",
                }
            elif short_signal and np.isfinite(float(row["ms_atr_ts"])):
                pending_entry = {
                    "side": "SHORT",
                    "entry_bar_idx": i + 1,
                    "stop": float(row["ms_atr_ts"]),
                    "target": float(row["ms_current_target"]) if np.isfinite(float(row["ms_current_target"])) else np.nan,
                    "signal_ts": str(ts),
                    "signal_type": "ChoCh_down_bear_regime",
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
        "win_rate_pct": float((win_count / len(trades)) * 100.0) if trades else 0.0,
        "profit_factor": float(gross_profit / gross_loss) if gross_loss > 0 else (float("inf") if gross_profit > 0 else float("nan")),
        "avg_trade_pnl": float(np.mean([float(t["net_pnl"]) for t in trades])) if trades else 0.0,
        **_equity_metrics(equity_curve),
    }
    return {"summary": summary, "trades": trades[:20]}


def _render_report(payload: Dict[str, Any]) -> str:
    lines: List[str] = []
    lines.append("# MS Trend Matrix Strategy Probe")
    lines.append("")
    lines.append(f"- symbol: `{payload['symbol']}`")
    lines.append(f"- start_utc: `{payload['start_utc']}`")
    lines.append(f"- end_utc: `{payload['end_utc']}`")
    lines.append(f"- bars: `{payload['bars']}`")
    lines.append("")
    lines.append("| Strategy | Return % | End Balance | Trades | Win Rate % | Profit Factor | Max DD % |")
    lines.append("|---|---:|---:|---:|---:|---:|---:|")
    for row in payload["results"]:
        s = row["summary"]
        lines.append(
            f"| `{s['strategy']}` | `{s['return_pct']:.2f}` | `{s['ending_balance']:.2f}` | `{s['num_trades']}` | "
            f"`{s['win_rate_pct']:.2f}` | `{s['profit_factor']:.3f}` | `{s['max_drawdown_pct']:.2f}` |"
        )
    return "\n".join(lines) + "\n"


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Probe BigBeluga MS Trend Matrix + EMA50/200 strategy variants on 1m BTCUSDT.")
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
    frame = _compute_ms_trend_matrix_frame(
        raw,
        ms_len=int(args.ms_len),
        atr_length=int(args.atr_length),
        atr_mult=float(args.atr_mult),
        target_step_mult=float(args.target_step_mult),
    )

    specs = [
        StrategySpec(
            name="regime_choch_target1",
            require_ema50_retest=False,
            retest_lookback_bars=0,
            use_target1=True,
            use_trailing_stop=True,
        ),
        StrategySpec(
            name="regime_choch_ema50_retest_target1",
            require_ema50_retest=True,
            retest_lookback_bars=60,
            use_target1=True,
            use_trailing_stop=True,
        ),
        StrategySpec(
            name="regime_choch_ema50_retest_trail_only",
            require_ema50_retest=True,
            retest_lookback_bars=60,
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
        DEFAULT_OUTPUT_ROOT / f"ms_trend_matrix_strategy_{pd.Timestamp.now('UTC').strftime('%Y%m%d_%H%M%S')}"
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
            f"trades={s['num_trades']} win={s['win_rate_pct']:.2f}% pf={s['profit_factor']:.3f} dd={s['max_drawdown_pct']:.2f}%"
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
