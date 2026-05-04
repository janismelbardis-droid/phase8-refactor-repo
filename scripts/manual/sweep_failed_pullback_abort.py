from __future__ import annotations

import argparse
import copy
import json
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pandas as pd

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from app.data_binance import fetch_klines_1m_futures


WORKSPACE_ROOT = Path(__file__).resolve().parents[4]
CASEBOOK_ROOT = WORKSPACE_ROOT / "research_exports" / "trade_entry_casebook"
BASE_DIR = CASEBOOK_ROOT / "ensemble_stage1_core_context_v1"
DEFAULT_PREV_MONTH_PORTFOLIO = BASE_DIR / "curated_two_prev_month.json"
DEFAULT_CASE_WINDOW_PORTFOLIO = BASE_DIR / "curated_two_case_window.json"
DEFAULT_OUTPUT_DIR = CASEBOOK_ROOT / "failed_pullback_abort_sweep_v1"
DEFAULT_CACHE_DIR = REPO_ROOT / "data_cache"


@dataclass(frozen=True)
class AbortVariant:
    label: str
    eval_minutes: int
    min_mfe_pct: float
    max_close_return_pct: float


def _load_json(path: Path) -> dict[str, Any]:
    with path.open("r", encoding="utf-8") as handle:
        payload = json.load(handle)
    if not isinstance(payload, dict):
        raise ValueError(f"Expected JSON object at {path}")
    return payload


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as handle:
        json.dump(payload, handle, indent=2, ensure_ascii=False)
        handle.write("\n")


def _coerce_ts(value: Any) -> pd.Timestamp:
    ts = pd.Timestamp(value)
    if ts.tzinfo is None:
        return ts.tz_localize("UTC")
    return ts.tz_convert("UTC")


def _coerce_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except Exception:
        return float(default)


def _portfolio_rows(payload: dict[str, Any]) -> list[dict[str, Any]]:
    rows = list(payload.get("portfolio_trades") or [])
    return [dict(row) for row in rows if isinstance(row, dict)]


def _portfolio_symbol(payload: dict[str, Any], rows: list[dict[str, Any]]) -> str:
    period = dict(payload.get("period") or {})
    symbol = str(period.get("symbol") or "").strip()
    if symbol:
        return symbol
    for row in rows:
        val = str(row.get("symbol") or "").strip()
        if val:
            return val
    raise ValueError("Unable to infer symbol from portfolio payload.")


def _prepare_portfolio_context(portfolio_path: Path, cache_dir: Path) -> dict[str, Any]:
    payload = _load_json(portfolio_path)
    rows = _portfolio_rows(payload)
    symbol = _portfolio_symbol(payload, rows)
    df_1m = _load_ohlcv(symbol, rows, cache_dir)
    return {
        "portfolio_path": str(portfolio_path),
        "rows": rows,
        "symbol": symbol,
        "df_1m": df_1m,
    }


def _candidate_variants() -> list[AbortVariant]:
    rows: list[AbortVariant] = [
        AbortVariant(label="baseline", eval_minutes=0, min_mfe_pct=0.0, max_close_return_pct=0.0),
    ]
    for eval_minutes in (3, 5, 10):
        for min_mfe_pct in (0.10, 0.15, 0.20):
            for max_close_return_pct in (-0.05, 0.00, 0.05):
                label = (
                    f"m{int(eval_minutes):02d}"
                    f"_mfe{int(round(min_mfe_pct * 100)):03d}"
                    f"_closele{int(round(max_close_return_pct * 100)):03d}"
                )
                rows.append(
                    AbortVariant(
                        label=label,
                        eval_minutes=int(eval_minutes),
                        min_mfe_pct=float(min_mfe_pct),
                        max_close_return_pct=float(max_close_return_pct),
                    )
                )
    return rows


def _load_ohlcv(symbol: str, trades: list[dict[str, Any]], cache_dir: Path) -> pd.DataFrame:
    if not trades:
        raise ValueError("Trades are required to load OHLCV.")
    start_utc = min(_coerce_ts(row["entry_time"]) for row in trades) - pd.Timedelta(minutes=2)
    end_utc = max(_coerce_ts(row["exit_time"]) for row in trades) + pd.Timedelta(minutes=15)
    df = fetch_klines_1m_futures(
        symbol=symbol,
        start_utc=start_utc,
        end_utc=end_utc,
        cache_dir=str(cache_dir),
        price_source="LAST",
    )
    if df.empty:
        raise ValueError(f"No 1m candles returned for {symbol} between {start_utc} and {end_utc}")
    return df.sort_values("open_time").reset_index(drop=True)


def _trade_window(df: pd.DataFrame, *, start_ts: pd.Timestamp, end_ts: pd.Timestamp) -> pd.DataFrame:
    mask = (df["open_time"] >= start_ts) & (df["open_time"] < end_ts)
    return df.loc[mask]


def _recompute_trade_metrics(row: dict[str, Any], *, exit_time: pd.Timestamp, exit_price: float, trade_slice: pd.DataFrame) -> dict[str, Any]:
    out = copy.deepcopy(row)
    entry_price = _coerce_float(row.get("entry_price"))
    qty = _coerce_float(row.get("qty"))
    entry_notional = _coerce_float(row.get("entry_notional"), qty * entry_price)
    fee_entry = _coerce_float(row.get("fee_entry"))
    fee_rate = (fee_entry / entry_notional) if entry_notional > 0 else 0.0004
    if fee_rate <= 0.0:
        fee_rate = 0.0004

    side = str(row.get("side") or "LONG").upper()
    exit_notional = qty * float(exit_price)
    if side == "SHORT":
        gross_pnl = (entry_price - float(exit_price)) * qty
        max_fav = max(0.0, entry_price - float(trade_slice["low"].min())) if not trade_slice.empty else 0.0
        max_adv = min(0.0, entry_price - float(trade_slice["high"].max())) if not trade_slice.empty else 0.0
    else:
        gross_pnl = (float(exit_price) - entry_price) * qty
        max_fav = max(0.0, float(trade_slice["high"].max()) - entry_price) if not trade_slice.empty else 0.0
        max_adv = min(0.0, float(trade_slice["low"].min()) - entry_price) if not trade_slice.empty else 0.0

    fee_exit = exit_notional * fee_rate
    fees = fee_entry + fee_exit
    net_pnl = gross_pnl - fees
    return_pct = (net_pnl / entry_notional * 100.0) if entry_notional > 0 else 0.0
    duration_min = int((exit_time - _coerce_ts(row["entry_time"])).total_seconds() // 60)
    mfe_pct = (max_fav / entry_price * 100.0) if entry_price > 0 else 0.0
    mae_pct = (max_adv / entry_price * 100.0) if entry_price > 0 else 0.0

    out.update(
        {
            "exit_time": str(exit_time),
            "exit_fill_time": str(exit_time),
            "exit_price": float(round(exit_price, 10)),
            "exit_notional": float(round(exit_notional, 10)),
            "gross_pnl": float(round(gross_pnl, 10)),
            "fee_exit": float(round(fee_exit, 10)),
            "fees": float(round(fees, 10)),
            "net_pnl": float(round(net_pnl, 10)),
            "return_pct": float(round(return_pct, 10)),
            "duration_min": int(duration_min),
            "mfe": float(round(max_fav, 10)),
            "mae": float(round(max_adv, 10)),
            "mfe_pct": float(round(mfe_pct, 10)),
            "mae_pct": float(round(mae_pct, 10)),
        }
    )
    return out


def _simulate_trade_abort(row: dict[str, Any], variant: AbortVariant, df_1m: pd.DataFrame) -> dict[str, Any]:
    if variant.label == "baseline" or variant.eval_minutes <= 0:
        return copy.deepcopy(row)

    entry_time = _coerce_ts(row["entry_time"])
    exit_time = _coerce_ts(row["exit_time"])
    if exit_time <= entry_time:
        return copy.deepcopy(row)

    abort_time = entry_time + pd.Timedelta(minutes=int(variant.eval_minutes))
    if abort_time >= exit_time:
        return copy.deepcopy(row)

    follow_window = _trade_window(df_1m, start_ts=entry_time, end_ts=abort_time)
    if follow_window.empty:
        return copy.deepcopy(row)

    entry_price = _coerce_float(row.get("entry_price"))
    side = str(row.get("side") or "LONG").upper()
    if side == "SHORT":
        max_fav_price = float(follow_window["low"].min())
        mfe_pct = ((entry_price - max_fav_price) / entry_price * 100.0) if entry_price > 0 else 0.0
        last_close = float(follow_window.iloc[-1]["close"])
        close_return_pct = ((entry_price - last_close) / entry_price * 100.0) if entry_price > 0 else 0.0
    else:
        max_fav_price = float(follow_window["high"].max())
        mfe_pct = ((max_fav_price - entry_price) / entry_price * 100.0) if entry_price > 0 else 0.0
        last_close = float(follow_window.iloc[-1]["close"])
        close_return_pct = ((last_close - entry_price) / entry_price * 100.0) if entry_price > 0 else 0.0

    if mfe_pct >= float(variant.min_mfe_pct):
        return copy.deepcopy(row)
    if close_return_pct > float(variant.max_close_return_pct):
        return copy.deepcopy(row)

    abort_bar = df_1m.loc[df_1m["open_time"] == abort_time]
    if abort_bar.empty:
        return copy.deepcopy(row)
    abort_exit_price = float(abort_bar.iloc[0]["open"])
    trade_slice = _trade_window(df_1m, start_ts=entry_time, end_ts=abort_time + pd.Timedelta(minutes=1))
    out = _recompute_trade_metrics(row, exit_time=abort_time, exit_price=abort_exit_price, trade_slice=trade_slice)
    out["exit_reason"] = f"Failed Pullback Abort ({variant.eval_minutes}m)"
    out["abort_eval_minutes"] = int(variant.eval_minutes)
    out["abort_mfe_pct_gate"] = float(variant.min_mfe_pct)
    out["abort_close_return_pct_gate"] = float(variant.max_close_return_pct)
    out["abort_trigger_mfe_pct"] = float(round(mfe_pct, 10))
    out["abort_trigger_close_return_pct"] = float(round(close_return_pct, 10))
    out["abort_triggered"] = True
    return out


def _summarize_trades(trades: list[dict[str, Any]], *, initial_balance: float = 1000.0) -> dict[str, Any]:
    wins = [row for row in trades if _coerce_float(row.get("net_pnl")) > 0.0]
    losses = [row for row in trades if _coerce_float(row.get("net_pnl")) <= 0.0]
    gross_profit = sum(_coerce_float(row.get("net_pnl")) for row in wins)
    gross_loss = sum(abs(_coerce_float(row.get("net_pnl"))) for row in losses)
    net_profit = gross_profit - gross_loss
    trades_count = len(trades)
    win_rate_pct = (len(wins) / trades_count * 100.0) if trades_count else 0.0
    avg_trade = (net_profit / trades_count) if trades_count else 0.0
    avg_win = (gross_profit / len(wins)) if wins else 0.0
    avg_loss = (-gross_loss / len(losses)) if losses else 0.0
    fees_total = sum(_coerce_float(row.get("fees")) for row in trades)
    avg_duration_min = (sum(_coerce_float(row.get("duration_min")) for row in trades) / trades_count) if trades_count else 0.0

    equity = float(initial_balance)
    high_water = float(initial_balance)
    max_drawdown = 0.0
    max_drawdown_pct = 0.0
    for row in sorted(trades, key=lambda item: _coerce_ts(item["exit_time"])):
        equity += _coerce_float(row.get("net_pnl"))
        if equity > high_water:
            high_water = equity
        dd = high_water - equity
        if dd > max_drawdown:
            max_drawdown = dd
            max_drawdown_pct = (dd / high_water * 100.0) if high_water > 0 else 0.0

    return {
        "trades": trades_count,
        "wins": len(wins),
        "losses": len(losses),
        "win_rate_pct": round(win_rate_pct, 6),
        "gross_profit": round(gross_profit, 6),
        "gross_loss": round(gross_loss, 6),
        "profit_factor": (round(gross_profit / gross_loss, 6) if gross_loss > 0 else None),
        "net_profit": round(net_profit, 6),
        "fees_total": round(fees_total, 6),
        "avg_trade": round(avg_trade, 6),
        "avg_win": round(avg_win, 6),
        "avg_loss": round(avg_loss, 6),
        "avg_duration_min": round(avg_duration_min, 6),
        "ending_balance": round(initial_balance + net_profit, 6),
        "return_pct": round((net_profit / initial_balance * 100.0) if initial_balance > 0 else 0.0, 6),
        "max_drawdown": round(max_drawdown, 6),
        "max_drawdown_pct": round(max_drawdown_pct, 6),
    }


def _case_hits(trades: list[dict[str, Any]]) -> list[str]:
    hits: set[str] = set()
    for row in trades:
        for case_id in list(row.get("case_hits") or []):
            hits.add(str(case_id))
    return sorted(hits)


def _portfolio_result(portfolio_ctx: dict[str, Any], variant: AbortVariant, output_dir: Path) -> dict[str, Any]:
    portfolio_path = Path(str(portfolio_ctx["portfolio_path"]))
    rows = list(portfolio_ctx.get("rows") or [])
    df_1m = portfolio_ctx["df_1m"]
    simulated = [_simulate_trade_abort(row, variant, df_1m) for row in rows]
    summary = _summarize_trades(simulated)
    abort_count = sum(1 for row in simulated if bool(row.get("abort_triggered")))
    result = {
        "variant": {
            "label": variant.label,
            "eval_minutes": int(variant.eval_minutes),
            "min_mfe_pct": float(variant.min_mfe_pct),
            "max_close_return_pct": float(variant.max_close_return_pct),
        },
        "source_portfolio": str(portfolio_path),
        "summary": summary,
        "abort_count": int(abort_count),
        "case_hits": _case_hits(simulated),
        "portfolio_trades": simulated,
    }
    out_path = output_dir / portfolio_path.stem / f"{variant.label}.json"
    _write_json(out_path, result)
    return result


def _row_sort_key(row: dict[str, Any]) -> tuple[Any, ...]:
    case = dict(row.get("case_window") or {})
    prev = dict(row.get("prev_month") or {})
    return (
        int(case.get("case_hit_count") or 0),
        float(prev.get("net_profit") or -9999.0),
        float(prev.get("profit_factor") or -9999.0),
        -float(prev.get("max_drawdown_pct") or 9999.0),
    )


def _summary_card(result: dict[str, Any]) -> dict[str, Any]:
    summary = dict(result.get("summary") or {})
    case_hits = list(result.get("case_hits") or [])
    return {
        "trades": int(summary.get("trades") or 0),
        "wins": int(summary.get("wins") or 0),
        "losses": int(summary.get("losses") or 0),
        "win_rate_pct": summary.get("win_rate_pct"),
        "net_profit": summary.get("net_profit"),
        "profit_factor": summary.get("profit_factor"),
        "max_drawdown_pct": summary.get("max_drawdown_pct"),
        "abort_count": int(result.get("abort_count") or 0),
        "case_hits": case_hits,
        "case_hit_count": len(case_hits),
    }


def _build_markdown(rows: list[dict[str, Any]], output_dir: Path) -> str:
    lines: list[str] = []
    lines.append("# Failed Pullback Abort Sweep")
    lines.append("")
    lines.append(f"- Output root: `{output_dir}`")
    lines.append(f"- Candidates tested: `{len(rows)}`")
    lines.append("")
    lines.append("| Rank | Variant | Case hits | Case net | Prev net | Prev PF | Prev DD % | Prev aborts |")
    lines.append("|---:|---|---:|---:|---:|---:|---:|---:|")
    for idx, row in enumerate(rows, start=1):
        case = dict(row.get("case_window") or {})
        prev = dict(row.get("prev_month") or {})
        lines.append(
            f"| {idx} | {row['label']} | {case.get('case_hit_count')} | {case.get('net_profit')} | "
            f"{prev.get('net_profit')} | {prev.get('profit_factor')} | {prev.get('max_drawdown_pct')} | {prev.get('abort_count')} |"
        )
    lines.append("")
    return "\n".join(lines)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Sweep early failed-pullback abort rules over saved portfolio trades.")
    parser.add_argument("--prev-month-portfolio", type=Path, default=DEFAULT_PREV_MONTH_PORTFOLIO)
    parser.add_argument("--case-window-portfolio", type=Path, default=DEFAULT_CASE_WINDOW_PORTFOLIO)
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    parser.add_argument("--cache-dir", type=Path, default=DEFAULT_CACHE_DIR)
    return parser


def main() -> int:
    args = build_parser().parse_args()
    prev_month_portfolio = Path(args.prev_month_portfolio).resolve()
    case_window_portfolio = Path(args.case_window_portfolio).resolve()
    output_dir = Path(args.output_dir).resolve()
    cache_dir = Path(args.cache_dir).resolve()
    output_dir.mkdir(parents=True, exist_ok=True)

    case_ctx = _prepare_portfolio_context(case_window_portfolio, cache_dir)
    prev_ctx = _prepare_portfolio_context(prev_month_portfolio, cache_dir)

    rows: list[dict[str, Any]] = []
    for idx, variant in enumerate(_candidate_variants(), start=1):
        print(f"[{idx}] {variant.label}", flush=True)
        case_result = _portfolio_result(case_ctx, variant, output_dir)
        prev_result = _portfolio_result(prev_ctx, variant, output_dir)
        row = {
            "label": variant.label,
            "variant": {
                "eval_minutes": int(variant.eval_minutes),
                "min_mfe_pct": float(variant.min_mfe_pct),
                "max_close_return_pct": float(variant.max_close_return_pct),
            },
            "case_window": _summary_card(case_result),
            "prev_month": _summary_card(prev_result),
        }
        rows.append(row)
        ranked = sorted(rows, key=_row_sort_key, reverse=True)
        summary_payload = {
            "rows": ranked,
            "best": ranked[0] if ranked else None,
        }
        _write_json(output_dir / "summary.json", summary_payload)
        (output_dir / "summary.md").write_text(_build_markdown(ranked, output_dir), encoding="utf-8")

    ranked = sorted(rows, key=_row_sort_key, reverse=True)
    print(json.dumps({"summary_json": str(output_dir / "summary.json"), "best": ranked[0] if ranked else None}, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
