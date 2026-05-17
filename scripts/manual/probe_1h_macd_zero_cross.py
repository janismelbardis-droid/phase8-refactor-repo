from __future__ import annotations

import argparse
import json
import math
import shutil
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd


REPO_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_OUTPUT_ROOT = REPO_ROOT / "runs" / "perf"
LATEST_MIRROR = REPO_ROOT / "runs" / "perf" / "probe_1h_macd_zero_cross_latest"
LOCAL_DAY_STORE = REPO_ROOT / "data_cache" / "ohlcv_store" / "BTCUSDT" / "LAST" / "1m"


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


def _resample_1h(df_1m: pd.DataFrame) -> pd.DataFrame:
    frame = df_1m.copy().set_index("open_time")
    out = (
        frame.resample("1h", label="left", closed="left")
        .agg({"open": "first", "high": "max", "low": "min", "close": "last", "volume": "sum"})
        .dropna()
        .reset_index()
    )
    out.rename(columns={"open_time": "ts"}, inplace=True)
    return out


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


def _compute_macd(frame: pd.DataFrame, fast: int = 12, slow: int = 26, signal: int = 9) -> pd.DataFrame:
    close = frame["close"].to_numpy(dtype=float)
    ema_fast = _ema(close, fast)
    ema_slow = _ema(close, slow)
    macd = ema_fast - ema_slow
    signal_line = _ema(macd, signal)
    hist = macd - signal_line
    out = frame.copy()
    out["ema_fast"] = ema_fast
    out["ema_slow"] = ema_slow
    out["macd"] = macd
    out["signal"] = signal_line
    out["hist"] = hist
    out["cross_up_zero"] = (pd.Series(macd).shift(1) <= 0) & (pd.Series(macd) > 0)
    out["cross_down_zero"] = (pd.Series(macd).shift(1) >= 0) & (pd.Series(macd) < 0)
    return out


def _max_drawdown_pct(equity_curve: list[float]) -> float:
    if not equity_curve:
        return float("nan")
    peak = float(equity_curve[0])
    max_dd = 0.0
    for value in equity_curve:
        peak = max(peak, float(value))
        if peak > 0:
            dd = ((float(value) / peak) - 1.0) * 100.0
            max_dd = min(max_dd, dd)
    return float(max_dd)


def _run_zero_cross_strategy(frame: pd.DataFrame, *, fee_rate: float, long_only: bool = False, short_only: bool = False) -> dict[str, Any]:
    balance = 1000.0
    order_notional = 100.0
    equity_curve = [balance]
    trades: list[dict[str, Any]] = []
    pending_entry: dict[str, Any] | None = None
    position: dict[str, Any] | None = None

    for i in range(len(frame)):
        row = frame.iloc[i]
        ts = pd.Timestamp(row["ts"])

        if pending_entry is not None and i == int(pending_entry["entry_idx"]):
            entry_price = float(row["open"])
            if entry_price > 0:
                qty = order_notional / entry_price
                position = {
                    "side": pending_entry["side"],
                    "entry_idx": i,
                    "entry_ts": ts,
                    "entry_price": entry_price,
                    "qty": qty,
                    "signal_ts": pending_entry["signal_ts"],
                }
            pending_entry = None

        if position is not None and i + 1 < len(frame):
            opposite = False
            if position["side"] == "LONG":
                opposite = bool(row["cross_down_zero"])
            else:
                opposite = bool(row["cross_up_zero"])
            if opposite:
                exit_idx = i + 1
                exit_row = frame.iloc[exit_idx]
                exit_price = float(exit_row["open"])
                qty = float(position["qty"])
                gross = ((exit_price - float(position["entry_price"])) * qty) if position["side"] == "LONG" else ((float(position["entry_price"]) - exit_price) * qty)
                fee_entry = order_notional * fee_rate
                fee_exit = abs(exit_price * qty) * fee_rate
                net = gross - fee_entry - fee_exit
                balance += net
                trades.append(
                    {
                        "side": position["side"],
                        "signal_ts": str(position["signal_ts"]),
                        "entry_ts": str(position["entry_ts"]),
                        "exit_ts": str(pd.Timestamp(exit_row["ts"])),
                        "entry_price": float(position["entry_price"]),
                        "exit_price": exit_price,
                        "net_pnl": float(net),
                        "exit_reason": "opposite_zero_cross",
                    }
                )
                equity_curve.append(float(balance))
                position = None

        if position is None and pending_entry is None and i + 1 < len(frame):
            long_signal = bool(row["cross_up_zero"])
            short_signal = bool(row["cross_down_zero"])
            if long_only:
                short_signal = False
            if short_only:
                long_signal = False
            if long_signal:
                pending_entry = {"side": "LONG", "entry_idx": i + 1, "signal_ts": ts}
            elif short_signal:
                pending_entry = {"side": "SHORT", "entry_idx": i + 1, "signal_ts": ts}

    if position is not None:
        last = frame.iloc[-1]
        exit_price = float(last["close"])
        qty = float(position["qty"])
        gross = ((exit_price - float(position["entry_price"])) * qty) if position["side"] == "LONG" else ((float(position["entry_price"]) - exit_price) * qty)
        fee_entry = order_notional * fee_rate
        fee_exit = abs(exit_price * qty) * fee_rate
        net = gross - fee_entry - fee_exit
        balance += net
        trades.append(
            {
                "side": position["side"],
                "signal_ts": str(position["signal_ts"]),
                "entry_ts": str(position["entry_ts"]),
                "exit_ts": str(pd.Timestamp(last["ts"])),
                "entry_price": float(position["entry_price"]),
                "exit_price": exit_price,
                "net_pnl": float(net),
                "exit_reason": "force_close_end",
            }
        )
        equity_curve.append(float(balance))

    gross_profit = sum(max(0.0, float(t["net_pnl"])) for t in trades)
    gross_loss = sum(-min(0.0, float(t["net_pnl"])) for t in trades)
    win_count = sum(1 for t in trades if float(t["net_pnl"]) > 0)
    summary = {
        "ending_balance": float(balance),
        "return_pct": float(((balance / 1000.0) - 1.0) * 100.0),
        "num_trades": int(len(trades)),
        "win_rate_pct": float((win_count / len(trades)) * 100.0) if trades else 0.0,
        "profit_factor": float(gross_profit / gross_loss) if gross_loss > 0 else (float("inf") if gross_profit > 0 else float("nan")),
        "avg_trade_pnl": float(np.mean([float(t["net_pnl"]) for t in trades])) if trades else 0.0,
        "max_drawdown_pct": _max_drawdown_pct(equity_curve),
    }
    return {"summary": summary, "trades": trades}


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


def _render_report(payload: dict[str, Any]) -> str:
    lines: list[str] = []
    lines.append("# 1H MACD Zero-Cross Probe")
    lines.append("")
    lines.append(f"- symbol: `{payload['symbol']}`")
    lines.append(f"- start_utc: `{payload['start_utc']}`")
    lines.append(f"- end_utc: `{payload['end_utc']}`")
    lines.append(f"- bars_1h: `{payload['bars_1h']}`")
    lines.append("")
    lines.append("| Variant | Return % | End Balance | Trades | Win Rate % | Profit Factor | Max DD % |")
    lines.append("|---|---:|---:|---:|---:|---:|---:|")
    for row in payload["results"]:
        s = row["summary"]
        lines.append(
            f"| `{row['name']}` | `{s['return_pct']:.2f}` | `{s['ending_balance']:.2f}` | `{s['num_trades']}` | "
            f"`{s['win_rate_pct']:.2f}` | `{s['profit_factor']:.3f}` | `{s['max_drawdown_pct']:.2f}` |"
        )
    return "\n".join(lines) + "\n"


def _console_summary(payload: dict[str, Any]) -> str:
    lines: list[str] = []
    lines.append("1H MACD Zero-Cross Probe")
    lines.append(f"symbol={payload['symbol']} start={payload['start_utc']} end={payload['end_utc']} bars_1h={payload['bars_1h']}")
    for row in payload["results"]:
        s = row["summary"]
        lines.append(
            f"{row['name']}: return={s['return_pct']:.2f}% trades={s['num_trades']} "
            f"win_rate={s['win_rate_pct']:.2f}% pf={s['profit_factor']:.3f} dd={s['max_drawdown_pct']:.2f}%"
        )
    return "\n".join(lines)


def main() -> int:
    parser = argparse.ArgumentParser(description="Probe a standalone 1H MACD zero-cross strategy on local BTCUSDT candles.")
    parser.add_argument("--start", default="2025-01-01T00:00:00Z")
    parser.add_argument("--end", default=None)
    parser.add_argument("--fee-rate", type=float, default=0.0004)
    parser.add_argument("--output-dir", default=None)
    args = parser.parse_args()

    latest_file = sorted(LOCAL_DAY_STORE.glob("BTCUSDT_LAST_*.parquet"))[-1]
    latest_df = pd.read_parquet(latest_file, columns=["open_time"])
    latest_ts = pd.to_datetime(latest_df["open_time"], utc=True).max()
    start = pd.to_datetime(args.start, utc=True)
    end = pd.to_datetime(args.end, utc=True) if args.end else pd.Timestamp(latest_ts)

    raw_1m = _load_local_1m(start.floor("D"), end)
    frame_1h = _resample_1h(raw_1m)
    frame_1h = frame_1h[(frame_1h["ts"] >= start) & (frame_1h["ts"] <= end)].reset_index(drop=True)
    frame_1h = _compute_macd(frame_1h)

    variants = [
        ("macd_zero_cross_both", {"long_only": False, "short_only": False}),
        ("macd_zero_cross_long_only", {"long_only": True, "short_only": False}),
        ("macd_zero_cross_short_only", {"long_only": False, "short_only": True}),
    ]
    results = []
    for name, cfg in variants:
        run = _run_zero_cross_strategy(frame_1h, fee_rate=float(args.fee_rate), **cfg)
        run["name"] = name
        results.append(run)

    payload = {
        "symbol": "BTCUSDT",
        "start_utc": str(pd.Timestamp(start)),
        "end_utc": str(pd.Timestamp(end)),
        "bars_1h": int(len(frame_1h)),
        "results": results,
    }

    stamp = pd.Timestamp.now(tz="UTC").strftime("%Y%m%d_%H%M%S")
    output_dir = Path(args.output_dir) if args.output_dir else DEFAULT_OUTPUT_ROOT / f"probe_1h_macd_zero_cross_{stamp}"
    output_dir.mkdir(parents=True, exist_ok=True)

    (output_dir / "summary.json").write_text(json.dumps(_json_safe(payload), indent=2, ensure_ascii=False), encoding="utf-8")
    (output_dir / "report.md").write_text(_render_report(payload), encoding="utf-8")
    for row in results:
        pd.DataFrame(row["trades"]).to_csv(output_dir / f"{row['name']}_trades.csv", index=False)

    if output_dir.resolve() != LATEST_MIRROR.resolve():
        if LATEST_MIRROR.exists() or LATEST_MIRROR.is_symlink():
            if LATEST_MIRROR.is_symlink() or LATEST_MIRROR.is_file():
                LATEST_MIRROR.unlink()
            else:
                shutil.rmtree(LATEST_MIRROR)
        shutil.copytree(output_dir, LATEST_MIRROR)
    print(_console_summary(payload))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
