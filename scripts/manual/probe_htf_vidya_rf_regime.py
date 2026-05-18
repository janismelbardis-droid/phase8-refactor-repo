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

from app.indicators_streaming import compute_range_filter_series, compute_vidya_series


DEFAULT_OUTPUT_ROOT = REPO_ROOT / "runs" / "perf"
LATEST_MIRROR = REPO_ROOT / "runs" / "perf" / "probe_htf_vidya_rf_regime_latest"
LOCAL_DAY_STORE = REPO_ROOT / "data_cache" / "ohlcv_store" / "BTCUSDT" / "LAST" / "1m"


@dataclass(frozen=True)
class Variant:
    name: str
    timeframe: str
    side_mode: str = "both"
    exit_mode: str = "either"


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


def _prepare_htf(raw_1m: pd.DataFrame, timeframe: str) -> pd.DataFrame:
    frame = (
        raw_1m.set_index("open_time")
        .resample(timeframe, label="left", closed="left")
        .agg({"open": "first", "high": "max", "low": "min", "close": "last", "volume": "sum"})
        .dropna()
        .reset_index()
    )
    vid = compute_vidya_series(
        frame["open"].to_numpy(dtype=float),
        frame["high"].to_numpy(dtype=float),
        frame["low"].to_numpy(dtype=float),
        frame["close"].to_numpy(dtype=float),
        frame["volume"].to_numpy(dtype=float),
    )
    rf = compute_range_filter_series(frame["close"].to_numpy(dtype=float), per=100, mult=3.0)
    frame["ts"] = pd.to_datetime(frame["open_time"], utc=True)
    for key in ("vidya_state", "vidya_line"):
        frame[key] = vid[key]
    for key in ("range_filter_state", "range_filter_phase", "range_filter_buy", "range_filter_sell"):
        frame[key] = rf[key]
    return frame


def _simulate(frame: pd.DataFrame, variant: Variant, *, initial_balance: float = 1000.0, position_notional: float = 100.0, fee_rate: float = 0.0004) -> dict[str, Any]:
    trades: list[dict[str, Any]] = []
    current_exit_idx = -1
    i = 1
    while i < len(frame) - 1:
        if i <= current_exit_idx:
            i += 1
            continue

        row = frame.iloc[i]
        side = None
        if bool(row["range_filter_buy"]) and str(row["vidya_state"]) == "BUY":
            side = "LONG"
        elif bool(row["range_filter_sell"]) and str(row["vidya_state"]) == "SELL":
            side = "SHORT"

        if side is None:
            i += 1
            continue
        if variant.side_mode == "long_only" and side != "LONG":
            i += 1
            continue
        if variant.side_mode == "short_only" and side != "SHORT":
            i += 1
            continue

        entry_idx = i + 1
        if entry_idx >= len(frame):
            break
        entry_price = float(frame.at[entry_idx, "open"])
        qty = float(position_notional) / entry_price

        exit_idx = len(frame) - 1
        exit_price = float(frame.iloc[exit_idx]["close"])
        exit_reason = "force_close_end"
        for j in range(entry_idx, len(frame) - 1):
            test = frame.iloc[j]
            exit_now = False
            if variant.exit_mode == "opposite_rf":
                if side == "LONG" and bool(test["range_filter_sell"]):
                    exit_now = True
                if side == "SHORT" and bool(test["range_filter_buy"]):
                    exit_now = True
            elif variant.exit_mode == "vidya_flip":
                if side == "LONG" and str(test["vidya_state"]) != "BUY":
                    exit_now = True
                if side == "SHORT" and str(test["vidya_state"]) != "SELL":
                    exit_now = True
            elif variant.exit_mode == "either":
                if side == "LONG" and (bool(test["range_filter_sell"]) or str(test["vidya_state"]) != "BUY"):
                    exit_now = True
                if side == "SHORT" and (bool(test["range_filter_buy"]) or str(test["vidya_state"]) != "SELL"):
                    exit_now = True
            if exit_now:
                exit_idx = j + 1
                exit_price = float(frame.at[exit_idx, "open"])
                exit_reason = variant.exit_mode
                break

        fee_entry = float(position_notional) * float(fee_rate)
        fee_exit = abs(exit_price * qty) * float(fee_rate)
        gross = ((exit_price - entry_price) * qty) if side == "LONG" else ((entry_price - exit_price) * qty)
        net = gross - fee_entry - fee_exit
        trades.append(
            {
                "timeframe": variant.timeframe,
                "side": side,
                "signal_time": str(pd.Timestamp(row["ts"])),
                "entry_time": str(pd.Timestamp(frame.at[entry_idx, "ts"])),
                "exit_time": str(pd.Timestamp(frame.at[exit_idx, "ts"])),
                "entry_price": float(entry_price),
                "exit_price": float(exit_price),
                "exit_reason": str(exit_reason),
                "gross_pnl": float(gross),
                "net_pnl": float(net),
                "return_pct_on_notional": float(net / float(position_notional) * 100.0),
            }
        )
        current_exit_idx = int(exit_idx)
        i = exit_idx + 1

    balance = float(initial_balance)
    peak = balance
    max_dd_pct = 0.0
    wins = 0
    gross_profit = 0.0
    gross_loss = 0.0
    for trade in trades:
        net = float(trade["net_pnl"])
        balance += net
        peak = max(peak, balance)
        dd_pct = ((balance / peak) - 1.0) * 100.0 if peak > 0 else 0.0
        max_dd_pct = min(max_dd_pct, dd_pct)
        if net > 0:
            wins += 1
            gross_profit += net
        else:
            gross_loss += -net

    trade_count = len(trades)
    return {
        "variant": asdict(variant),
        "summary": {
            "variant": variant.name,
            "timeframe": variant.timeframe,
            "trade_count": int(trade_count),
            "win_rate_pct": float((wins / trade_count) * 100.0) if trade_count else 0.0,
            "profit_factor": float(gross_profit / gross_loss) if gross_loss > 0 else (float("inf") if gross_profit > 0 else float("nan")),
            "ending_balance": float(balance),
            "return_pct": float(((balance / float(initial_balance)) - 1.0) * 100.0),
            "avg_trade_pnl": float(np.mean([float(t["net_pnl"]) for t in trades])) if trades else 0.0,
            "max_drawdown_pct": float(max_dd_pct),
        },
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
    lines.append("# HTF VIDYA + Range Filter Regime Probe")
    lines.append("")
    for window in payload["windows"]:
        lines.append(f"## {window['label']}")
        lines.append("")
        lines.append(f"- start_utc: `{window['start_utc']}`")
        lines.append(f"- end_utc: `{window['end_utc']}`")
        lines.append("")
        lines.append("| Variant | TF | Return % | Trades | Win Rate % | PF | Max DD % |")
        lines.append("|---|---|---:|---:|---:|---:|---:|")
        for row in window["results"]:
            s = row["summary"]
            lines.append(
                f"| `{s['variant']}` | `{s['timeframe']}` | `{s['return_pct']:.2f}` | `{s['trade_count']}` | "
                f"`{s['win_rate_pct']:.2f}` | `{s['profit_factor']:.3f}` | `{s['max_drawdown_pct']:.2f}` |"
            )
        lines.append("")
    return "\n".join(lines) + "\n"


def main() -> int:
    latest_file = sorted(LOCAL_DAY_STORE.glob("BTCUSDT_LAST_*.parquet"))[-1]
    latest_df = pd.read_parquet(latest_file, columns=["open_time"])
    latest_ts = pd.to_datetime(latest_df["open_time"], utc=True).max()

    windows = [
        {"label": "2025_to_now", "start": pd.Timestamp("2025-01-01 00:00:00+00:00"), "end": pd.Timestamp(latest_ts)},
        {"label": "2026_ytd", "start": pd.Timestamp("2026-01-01 00:00:00+00:00"), "end": pd.Timestamp(latest_ts)},
    ]
    variants = [
        Variant(name="both_opposite_rf", timeframe="1h", side_mode="both", exit_mode="opposite_rf"),
        Variant(name="both_vidya_flip", timeframe="1h", side_mode="both", exit_mode="vidya_flip"),
        Variant(name="both_either", timeframe="1h", side_mode="both", exit_mode="either"),
        Variant(name="short_either", timeframe="1h", side_mode="short_only", exit_mode="either"),
        Variant(name="long_either", timeframe="1h", side_mode="long_only", exit_mode="either"),
        Variant(name="both_opposite_rf", timeframe="4h", side_mode="both", exit_mode="opposite_rf"),
        Variant(name="both_vidya_flip", timeframe="4h", side_mode="both", exit_mode="vidya_flip"),
        Variant(name="both_either", timeframe="4h", side_mode="both", exit_mode="either"),
        Variant(name="short_either", timeframe="4h", side_mode="short_only", exit_mode="either"),
        Variant(name="long_either", timeframe="4h", side_mode="long_only", exit_mode="either"),
    ]

    payload: dict[str, Any] = {"windows": []}
    for window in windows:
        raw = _load_local_1m(window["start"], window["end"])
        cache: dict[str, pd.DataFrame] = {}
        results = []
        for variant in variants:
            if variant.timeframe not in cache:
                cache[variant.timeframe] = _prepare_htf(raw, variant.timeframe)
            results.append(_simulate(cache[variant.timeframe], variant))
        results.sort(key=lambda row: float(row["summary"]["return_pct"]), reverse=True)
        payload["windows"].append(
            {
                "label": window["label"],
                "start_utc": str(pd.Timestamp(window["start"])),
                "end_utc": str(pd.Timestamp(window["end"])),
                "results": results,
            }
        )

    output_dir = DEFAULT_OUTPUT_ROOT / "probe_htf_vidya_rf_regime_latest"
    if output_dir.exists():
        shutil.rmtree(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    (output_dir / "summary.json").write_text(json.dumps(_json_safe(payload), indent=2, ensure_ascii=False), encoding="utf-8")
    (output_dir / "report.md").write_text(_render_report(payload), encoding="utf-8")
    for window in payload["windows"]:
        label = window["label"]
        for row in window["results"]:
            pd.DataFrame(row["trades"]).to_csv(output_dir / f"{label}__{row['summary']['timeframe']}__{row['summary']['variant']}_trades.csv", index=False)

    print(_render_report(payload))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
