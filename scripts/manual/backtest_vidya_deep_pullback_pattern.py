from __future__ import annotations

import argparse
import json
import math
import sys
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any, Iterable, Optional

import pandas as pd


REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from scripts.manual.extract_trade_case_metrics import choose_meta, load_merged_dataset, load_meta, prepared_root


DEFAULT_META_ID = "prep_05287a6b9ecebfd6"
DEFAULT_CASES_JSON = (
    Path(__file__).resolve().parents[4]
    / "research_exports"
    / "trade_entry_casebook"
    / "preset_drafts"
    / "backtest_case_period_0001_0005.json"
)


@dataclass
class PatternParams:
    max_pullback_bars: int = 15
    reclaim_window: int = 3
    max_vidya_gap_pct: float = 0.25
    stop_abs: float = 160.0
    take_abs: float = 320.0
    position_notional: float = 100.0
    fee_rate: float = 0.0004
    initial_balance: float = 1000.0
    require_t0_rf_buy: bool = True
    require_vidya_buy_to_reclaim: bool = True
    allowed_pb_phases: tuple[str, ...] = ("BUY_WEAK", "NEUTRAL", "SELL_WEAK")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Backtest the deep pullback-after-VIDYA-flip pattern directly on prepared data.")
    parser.add_argument("--symbol", default="BTCUSDT")
    parser.add_argument("--start", required=True)
    parser.add_argument("--end", required=True)
    parser.add_argument("--output-json", type=Path, required=True)
    parser.add_argument("--cases-json", type=Path, default=DEFAULT_CASES_JSON)
    parser.add_argument("--meta-id", default=DEFAULT_META_ID)
    parser.add_argument("--max-pullback-bars", type=int, default=15)
    parser.add_argument("--reclaim-window", type=int, default=3)
    parser.add_argument("--max-vidya-gap-pct", type=float, default=0.25)
    parser.add_argument("--stop-abs", type=float, default=160.0)
    parser.add_argument("--take-abs", type=float, default=320.0)
    return parser


def _safe_float(value: Any) -> Optional[float]:
    try:
        if pd.isna(value):
            return None
    except Exception:
        pass
    try:
        return float(value)
    except Exception:
        return None


def _safe_bool(value: Any) -> bool:
    try:
        if pd.isna(value):
            return False
    except Exception:
        pass
    return bool(value)


def _load_cases(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    with path.open("r", encoding="utf-8") as handle:
        payload = json.load(handle)
    return list(payload.get("cases") or [])


def _case_hits(entry_ts: pd.Timestamp, cases: Iterable[dict[str, Any]]) -> list[str]:
    hits: list[str] = []
    for case in cases:
        case_id = str(case.get("case_id") or "").strip()
        t0 = pd.Timestamp(case.get("t0_utc"))
        window_end = pd.Timestamp(case.get("window_end_utc"))
        if t0 <= entry_ts <= window_end:
            hits.append(case_id)
    return hits


def _first_reclaim_index(df: pd.DataFrame, pullback_idx: int, t0_close: float, window: int) -> Optional[int]:
    start = pullback_idx + 1
    stop = min(len(df), pullback_idx + 1 + int(window))
    for idx in range(start, stop):
        close = float(df.at[idx, "close"])
        open_ = float(df.at[idx, "open"])
        prev_high = float(df.at[idx - 1, "high"]) if idx - 1 >= 0 else close
        vidya_line = _safe_float(df.at[idx, "vidya_line"])
        if close >= prev_high and close >= open_ and (vidya_line is None or close >= vidya_line):
            return idx
        if close >= float(t0_close) and close >= open_:
            return idx
    return None


def _pattern_signals(df: pd.DataFrame, params: PatternParams) -> list[dict[str, Any]]:
    anchors = df.index[df["vidya_trend_up"].fillna(False).astype(bool)].tolist()
    signals: dict[int, dict[str, Any]] = {}
    for anchor_idx in anchors:
        if anchor_idx + 2 >= len(df):
            continue
        t0 = df.iloc[anchor_idx]
        if str(t0.get("vidya_state") or "").upper() != "BUY":
            continue
        if params.require_t0_rf_buy and str(t0.get("range_filter_state") or "").upper() != "BUY":
            continue

        start = anchor_idx + 1
        stop = min(len(df), anchor_idx + 1 + int(params.max_pullback_bars))
        if start >= stop:
            continue
        pb_idx = int(df.iloc[start:stop]["low"].astype(float).idxmin())
        pb = df.iloc[pb_idx]
        t0_body_bottom = min(float(t0["open"]), float(t0["close"]))
        pb_low = float(pb["low"])
        if pb_low >= t0_body_bottom:
            continue
        phase = str(pb.get("range_filter_phase") or "").upper()
        if phase not in set(params.allowed_pb_phases):
            continue
        vidya_line = _safe_float(pb.get("vidya_line"))
        if vidya_line is None or pb_low < vidya_line:
            continue
        vidya_gap_pct = ((pb_low - vidya_line) / vidya_line) * 100.0 if vidya_line else math.inf
        if vidya_gap_pct > float(params.max_vidya_gap_pct):
            continue

        if params.require_vidya_buy_to_reclaim:
            vidya_slice = df.iloc[anchor_idx : pb_idx + 1]["vidya_state"].astype(str).str.upper()
            if not vidya_slice.eq("BUY").all():
                continue

        reclaim_idx = _first_reclaim_index(df, pb_idx, float(t0["close"]), int(params.reclaim_window))
        if reclaim_idx is None or reclaim_idx + 1 >= len(df):
            continue
        if params.require_vidya_buy_to_reclaim:
            reclaim_slice = df.iloc[anchor_idx : reclaim_idx + 1]["vidya_state"].astype(str).str.upper()
            if not reclaim_slice.eq("BUY").all():
                continue

        signal_idx = int(reclaim_idx)
        candidate = {
            "anchor_idx": int(anchor_idx),
            "pb_idx": int(pb_idx),
            "reclaim_idx": int(reclaim_idx),
            "entry_idx": int(reclaim_idx + 1),
            "t0_time": str(pd.Timestamp(t0["ts"])),
            "pb_time": str(pd.Timestamp(pb["ts"])),
            "reclaim_time": str(pd.Timestamp(df.at[reclaim_idx, "ts"])),
            "entry_time": str(pd.Timestamp(df.at[reclaim_idx + 1, "ts"])),
            "bars_t0_to_pb": int(pb_idx - anchor_idx),
            "bars_pb_to_reclaim": int(reclaim_idx - pb_idx),
            "pb_phase": phase,
            "pb_low_minus_t0_body_abs": float(pb_low - t0_body_bottom),
            "pb_low_to_vidya_pct": float(vidya_gap_pct),
        }
        existing = signals.get(signal_idx)
        if existing is None or int(candidate["anchor_idx"]) < int(existing["anchor_idx"]):
            signals[signal_idx] = candidate
    return [signals[idx] for idx in sorted(signals)]


def _simulate_exit(df: pd.DataFrame, entry_idx: int, params: PatternParams, end_ts: pd.Timestamp) -> dict[str, Any]:
    entry_row = df.iloc[entry_idx]
    entry_price = float(entry_row["open"])
    qty = float(params.position_notional) / entry_price
    stop_price = entry_price - float(params.stop_abs)
    take_price = entry_price + float(params.take_abs)

    exit_idx = len(df) - 1
    exit_reason = "Force Close (End)"
    exit_price = float(df.iloc[exit_idx]["close"])

    for idx in range(entry_idx, len(df)):
        row = df.iloc[idx]
        ts = pd.Timestamp(row["ts"])
        if ts > end_ts:
            exit_idx = max(entry_idx, idx - 1)
            exit_price = float(df.iloc[exit_idx]["close"])
            exit_reason = "Force Close (End)"
            break
        low = float(row["low"])
        high = float(row["high"])
        hit_stop = low <= stop_price
        hit_take = high >= take_price
        if hit_stop and hit_take:
            exit_idx = idx
            exit_price = stop_price
            exit_reason = "Stop Loss"
            break
        if hit_stop:
            exit_idx = idx
            exit_price = stop_price
            exit_reason = "Stop Loss"
            break
        if hit_take:
            exit_idx = idx
            exit_price = take_price
            exit_reason = "Take Profit"
            break
    else:
        exit_idx = len(df) - 1
        exit_price = float(df.iloc[exit_idx]["close"])
        exit_reason = "Force Close (End)"

    exit_row = df.iloc[exit_idx]
    gross_pnl = (exit_price - entry_price) * qty
    fee_entry = float(params.position_notional) * float(params.fee_rate)
    fee_exit = float(exit_price * qty) * float(params.fee_rate)
    fees = fee_entry + fee_exit
    net_pnl = gross_pnl - fees
    return {
        "entry_idx": int(entry_idx),
        "exit_idx": int(exit_idx),
        "entry_time": str(pd.Timestamp(entry_row["ts"])),
        "exit_time": str(pd.Timestamp(exit_row["ts"])),
        "entry_price": float(entry_price),
        "exit_price": float(exit_price),
        "qty": float(qty),
        "gross_pnl": float(gross_pnl),
        "fee_entry": float(fee_entry),
        "fee_exit": float(fee_exit),
        "fees": float(fees),
        "net_pnl": float(net_pnl),
        "exit_reason": exit_reason,
        "stop_loss_price": float(stop_price),
        "take_profit_price": float(take_price),
        "duration_min": float(max(0, exit_idx - entry_idx + 1)),
    }


def _backtest(df: pd.DataFrame, signals: list[dict[str, Any]], params: PatternParams, end_ts: pd.Timestamp, cases: list[dict[str, Any]]) -> list[dict[str, Any]]:
    trades: list[dict[str, Any]] = []
    current_exit_idx = -1
    trade_id = 1
    for signal in signals:
        entry_idx = int(signal["entry_idx"])
        if entry_idx >= len(df):
            continue
        if entry_idx <= current_exit_idx:
            continue
        trade = dict(signal)
        trade.update(_simulate_exit(df, entry_idx, params, end_ts=end_ts))
        current_exit_idx = int(trade["exit_idx"])
        entry_ts = pd.Timestamp(trade["entry_time"])
        hits = _case_hits(entry_ts, cases)
        trade["trade_id"] = str(trade_id)
        trade["case_hits"] = hits
        trade["nearest_case_id"] = hits[0] if hits else None
        trade["return_pct"] = float(trade["net_pnl"]) / float(params.position_notional) * 100.0
        trades.append(trade)
        trade_id += 1
    return trades


def _summarize_trades(trades: list[dict[str, Any]], initial_balance: float) -> dict[str, Any]:
    wins = [t for t in trades if float(t["net_pnl"]) > 0.0]
    losses = [t for t in trades if float(t["net_pnl"]) <= 0.0]
    gross_profit = sum(float(t["net_pnl"]) for t in wins)
    gross_loss = -sum(float(t["net_pnl"]) for t in losses)
    fees_total = sum(float(t["fees"]) for t in trades)
    gross_pnl_total = sum(float(t["gross_pnl"]) for t in trades)
    net_profit = sum(float(t["net_pnl"]) for t in trades)
    balance = float(initial_balance)
    peak = balance
    max_dd = 0.0
    max_dd_pct = 0.0
    for trade in trades:
        balance += float(trade["net_pnl"])
        if balance > peak:
            peak = balance
        dd = peak - balance
        dd_pct = (dd / peak * 100.0) if peak > 0 else 0.0
        max_dd = max(max_dd, dd)
        max_dd_pct = max(max_dd_pct, dd_pct)
    return {
        "trades": len(trades),
        "wins": len(wins),
        "losses": len(losses),
        "win_rate_pct": (len(wins) / len(trades) * 100.0) if trades else 0.0,
        "gross_profit": gross_profit,
        "gross_loss": gross_loss,
        "profit_factor": (gross_profit / gross_loss) if gross_loss > 0 else None,
        "net_profit": net_profit,
        "gross_pnl_total": gross_pnl_total,
        "fees_total": fees_total,
        "avg_trade": (net_profit / len(trades)) if trades else 0.0,
        "avg_win": (gross_profit / len(wins)) if wins else 0.0,
        "avg_loss": (-gross_loss / len(losses)) if losses else 0.0,
        "avg_duration_min": (sum(float(t["duration_min"]) for t in trades) / len(trades)) if trades else 0.0,
        "ending_balance": float(initial_balance) + net_profit,
        "return_pct": (net_profit / float(initial_balance) * 100.0) if initial_balance else 0.0,
        "max_drawdown": max_dd,
        "max_drawdown_pct": max_dd_pct,
        "force_close_count": sum(1 for t in trades if t["exit_reason"] == "Force Close (End)"),
        "stop_loss_count": sum(1 for t in trades if t["exit_reason"] == "Stop Loss"),
        "take_profit_count": sum(1 for t in trades if t["exit_reason"] == "Take Profit"),
    }


def _round_payload(value: Any) -> Any:
    if isinstance(value, dict):
        return {k: _round_payload(v) for k, v in value.items()}
    if isinstance(value, list):
        return [_round_payload(v) for v in value]
    if isinstance(value, float):
        return round(value, 6)
    return value


def main() -> None:
    args = build_parser().parse_args()
    params = PatternParams(
        max_pullback_bars=int(args.max_pullback_bars),
        reclaim_window=int(args.reclaim_window),
        max_vidya_gap_pct=float(args.max_vidya_gap_pct),
        stop_abs=float(args.stop_abs),
        take_abs=float(args.take_abs),
    )

    meta_dir = prepared_root(REPO_ROOT, args.symbol)
    meta_path = choose_meta(meta_dir, args.meta_id, require_full=True, required_timeframes=["1m"])
    meta = load_meta(meta_path)
    df = load_merged_dataset(meta_dir, meta)
    df["ts"] = pd.to_datetime(df["ts"], utc=True)

    start_ts = pd.Timestamp(args.start, tz="UTC")
    end_ts = pd.Timestamp(args.end, tz="UTC")
    df = df[(df["ts"] >= start_ts) & (df["ts"] <= end_ts)].copy().reset_index(drop=True)
    if df.empty:
        raise ValueError("No candles in requested window.")

    cases = _load_cases(Path(args.cases_json))
    signals = _pattern_signals(df, params)
    trades = _backtest(df, signals, params, end_ts=end_ts, cases=cases)
    summary = _summarize_trades(trades, initial_balance=float(params.initial_balance))

    result = {
        "pattern": {
            "name": "vidya_deep_pullback_reclaim_v1",
            "params": _round_payload(asdict(params)),
        },
        "period": {
            "start_utc": str(start_ts),
            "end_utc": str(end_ts),
            "symbol": str(args.symbol).upper(),
        },
        "prepared_meta": {
            "meta_id": Path(meta_path).name,
            "start_utc": meta.get("start_utc"),
            "end_utc": meta.get("end_utc"),
        },
        "signal_count": len(signals),
        "trade_count": len(trades),
        "summary": _round_payload(summary),
        "trades": _round_payload(trades),
    }

    args.output_json.parent.mkdir(parents=True, exist_ok=True)
    with args.output_json.open("w", encoding="utf-8") as handle:
        json.dump(result, handle, ensure_ascii=False, indent=2)
        handle.write("\n")

    print(json.dumps({"output_json": str(args.output_json), "signal_count": len(signals), "summary": result["summary"]}, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
