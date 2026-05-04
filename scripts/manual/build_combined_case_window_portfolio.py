from __future__ import annotations

import argparse
import csv
import json
import sys
from pathlib import Path
from statistics import mean
from typing import Any

import pandas as pd

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from app.data_binance import fetch_klines_1m_futures
from app.services.ui_v2_runtime import BacktestRuntimeService, RunOverrides


PRESET_PRIORITY = {
    "901": 0,
    "903": 1,
    "902": 2,
}

NUMERIC_FIELDS = {
    "entry_price",
    "exit_price",
    "qty",
    "entry_notional",
    "exit_notional",
    "gross_pnl",
    "fees",
    "fee_entry",
    "fee_exit",
    "net_pnl",
    "return_pct",
    "duration_min",
    "signal_to_entry_sec",
    "order_to_entry_sec",
    "entry_to_exit_sec",
}

TIMESTAMP_FIELDS = {
    "entry_time",
    "exit_time",
    "signal_time",
    "order_created_time",
    "entry_fill_time",
    "exit_fill_time",
}


def default_workspace_root() -> Path:
    return Path(__file__).resolve().parents[4]


def default_repo_root() -> Path:
    return Path(__file__).resolve().parents[2]


def default_casebook_root() -> Path:
    return default_workspace_root() / "research_exports" / "trade_entry_casebook"


def default_report_json() -> Path:
    return default_casebook_root() / "preset_drafts" / "backtest_case_period_0001_0005.json"


def default_presets_path() -> Path:
    return default_casebook_root() / "preset_drafts" / "research_case_family_presets_v1.json"


def default_output_json() -> Path:
    return default_casebook_root() / "preset_drafts" / "backtest_case_period_0001_0005_combined.json"


def default_output_md(output_json: Path) -> Path:
    return output_json.with_suffix(".md")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Build a combined one-position portfolio report for the research case window."
    )
    parser.add_argument("--report-json", type=Path, default=default_report_json())
    parser.add_argument("--presets-path", type=Path, default=default_presets_path())
    parser.add_argument("--repo-root", type=Path, default=default_repo_root())
    parser.add_argument("--output-json", type=Path, default=default_output_json())
    parser.add_argument("--output-md", type=Path, default=None)
    return parser


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


def _parse_float(value: Any) -> float | None:
    if value in (None, "", "null"):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _parse_timestamp(value: Any) -> pd.Timestamp | None:
    if value in (None, "", "null"):
        return None
    return pd.Timestamp(value)


def _safe_round(value: Any, digits: int = 6) -> Any:
    if value is None:
        return None
    if isinstance(value, bool):
        return value
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        return round(float(value), digits)
    return value


def _resolve_cache_dir(repo_root: Path, cache_dir: str) -> Path:
    cache_path = Path(cache_dir)
    if cache_path.is_absolute():
        return cache_path
    return repo_root / cache_path


def _load_trade_rows(csv_path: Path) -> list[dict[str, Any]]:
    trades: list[dict[str, Any]] = []
    with csv_path.open("r", encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            trade: dict[str, Any] = {}
            for key, value in row.items():
                if key in NUMERIC_FIELDS:
                    trade[key] = _parse_float(value)
                elif key in TIMESTAMP_FIELDS:
                    trade[key] = _parse_timestamp(value)
                elif key == "trade_id":
                    trade[key] = str(value or "")
                else:
                    trade[key] = value
            trades.append(trade)
    return trades


def _build_close_lookup(
    *,
    symbol: str,
    start_utc: pd.Timestamp,
    end_utc: pd.Timestamp,
    cache_dir: Path,
    price_source: str,
) -> dict[pd.Timestamp, float]:
    df_1m = fetch_klines_1m_futures(
        symbol=symbol,
        start_utc=start_utc,
        end_utc=end_utc,
        cache_dir=str(cache_dir),
        price_source=price_source,
    )
    lookup: dict[pd.Timestamp, float] = {}
    for row in df_1m.itertuples(index=False):
        close_time = pd.Timestamp(getattr(row, "close_time"))
        lookup[close_time] = float(getattr(row, "close"))
    return lookup


def _repair_force_close_trade(
    trade: dict[str, Any],
    *,
    close_lookup: dict[pd.Timestamp, float],
) -> dict[str, Any]:
    fixed = dict(trade)
    exit_reason = str(fixed.get("exit_reason") or "")
    if exit_reason != "Force Close (End)" or fixed.get("net_pnl") is not None:
        fixed["repaired_force_close"] = False
        return fixed

    exit_time = fixed.get("exit_time")
    if not isinstance(exit_time, pd.Timestamp):
        raise ValueError(f"Force-close trade is missing exit_time: {trade}")
    if exit_time not in close_lookup:
        available = [ts for ts in close_lookup.keys() if ts <= exit_time]
        if not available:
            raise ValueError(f"No 1m close available to repair trade at {exit_time}")
        price_key = max(available)
    else:
        price_key = exit_time

    exit_price = float(close_lookup[price_key])
    entry_price = float(fixed.get("entry_price") or 0.0)
    qty = float(fixed.get("qty") or 0.0)
    side = str(fixed.get("side") or "LONG").upper()
    entry_notional = float(fixed.get("entry_notional") or (qty * entry_price))
    fee_entry = float(fixed.get("fee_entry") or 0.0)
    fee_rate = (fee_entry / entry_notional) if entry_notional else 0.0004
    if fee_rate <= 0.0:
        fee_rate = 0.0004

    exit_notional = qty * exit_price
    if side == "SHORT":
        gross_pnl = (entry_price - exit_price) * qty
    else:
        gross_pnl = (exit_price - entry_price) * qty
    fee_exit = exit_notional * fee_rate
    fees = fee_entry + fee_exit
    net_pnl = gross_pnl - fees
    return_pct = ((net_pnl / entry_notional) * 100.0) if entry_notional else None

    fixed.update(
        {
            "exit_price": exit_price,
            "exit_notional": exit_notional,
            "gross_pnl": gross_pnl,
            "fee_exit": fee_exit,
            "fees": fees,
            "net_pnl": net_pnl,
            "return_pct": return_pct,
            "repaired_force_close": True,
            "repair_source_close_time": price_key,
        }
    )
    return fixed


def _match_trade_to_cases(
    trade: dict[str, Any],
    *,
    cases: list[dict[str, Any]],
) -> dict[str, Any]:
    entry_time = trade.get("entry_time")
    if not isinstance(entry_time, pd.Timestamp):
        return {"case_hits": [], "nearest_case_id": None, "nearest_case_t0_diff_min": None}

    hits: list[str] = []
    nearest_case_id = None
    nearest_diff = None
    best_abs_diff = None
    for case in cases:
        start = pd.Timestamp(case["t_rf_utc"])
        end = pd.Timestamp(case["window_end_utc"])
        t0 = pd.Timestamp(case["t0_utc"])
        if start <= entry_time <= end:
            hits.append(str(case["case_id"]))
        diff_min = (entry_time - t0).total_seconds() / 60.0
        abs_diff = abs(diff_min)
        if best_abs_diff is None or abs_diff < best_abs_diff:
            best_abs_diff = abs_diff
            nearest_case_id = str(case["case_id"])
            nearest_diff = round(diff_min, 3)
    return {
        "case_hits": hits,
        "nearest_case_id": nearest_case_id,
        "nearest_case_t0_diff_min": nearest_diff,
    }


def _trade_to_serializable(trade: dict[str, Any]) -> dict[str, Any]:
    payload: dict[str, Any] = {}
    for key, value in trade.items():
        if isinstance(value, pd.Timestamp):
            payload[key] = str(value)
        elif isinstance(value, float):
            payload[key] = _safe_round(value, 6)
        else:
            payload[key] = value
    return payload


def _summarize_trades(trades: list[dict[str, Any]], *, initial_balance: float) -> dict[str, Any]:
    completed = [dict(trade) for trade in trades if trade.get("net_pnl") is not None]
    completed.sort(key=lambda trade: trade.get("exit_time") or trade.get("entry_time") or pd.Timestamp.min.tz_localize("UTC"))

    gross_profit = sum(float(trade["net_pnl"]) for trade in completed if float(trade["net_pnl"]) > 0.0)
    gross_loss = -sum(float(trade["net_pnl"]) for trade in completed if float(trade["net_pnl"]) < 0.0)
    wins = [trade for trade in completed if float(trade["net_pnl"]) > 0.0]
    losses = [trade for trade in completed if float(trade["net_pnl"]) < 0.0]
    net_profit = sum(float(trade["net_pnl"]) for trade in completed)
    gross_pnl_total = sum(float(trade.get("gross_pnl") or 0.0) for trade in completed)
    fees_total = sum(float(trade.get("fees") or 0.0) for trade in completed)

    equity = float(initial_balance)
    peak = equity
    max_drawdown = 0.0
    for trade in completed:
        equity += float(trade["net_pnl"])
        peak = max(peak, equity)
        max_drawdown = max(max_drawdown, peak - equity)
    max_drawdown_pct = (max_drawdown / peak * 100.0) if peak else 0.0

    avg_trade = mean(float(trade["net_pnl"]) for trade in completed) if completed else 0.0
    avg_win = mean(float(trade["net_pnl"]) for trade in wins) if wins else 0.0
    avg_loss = mean(float(trade["net_pnl"]) for trade in losses) if losses else 0.0
    avg_duration = mean(float(trade.get("duration_min") or 0.0) for trade in completed) if completed else 0.0
    ending_balance = initial_balance + net_profit
    return_pct = ((net_profit / initial_balance) * 100.0) if initial_balance else 0.0

    return {
        "trades": len(completed),
        "wins": len(wins),
        "losses": len(losses),
        "win_rate_pct": (len(wins) / len(completed) * 100.0) if completed else 0.0,
        "gross_profit": gross_profit,
        "gross_loss": gross_loss,
        "profit_factor": (gross_profit / gross_loss) if gross_loss > 0.0 else None,
        "net_profit": net_profit,
        "gross_pnl_total": gross_pnl_total,
        "fees_total": fees_total,
        "avg_trade": avg_trade,
        "avg_win": avg_win,
        "avg_loss": avg_loss,
        "avg_duration_min": avg_duration,
        "ending_balance": ending_balance,
        "return_pct": return_pct,
        "max_drawdown": max_drawdown,
        "max_drawdown_pct": max_drawdown_pct,
        "force_close_count": sum(1 for trade in completed if str(trade.get("exit_reason") or "") == "Force Close (End)"),
        "stop_loss_count": sum(1 for trade in completed if str(trade.get("exit_reason") or "") == "Stop Loss"),
        "take_profit_count": sum(1 for trade in completed if str(trade.get("exit_reason") or "") == "Take Profit"),
        "force_close_repairs": sum(1 for trade in completed if bool(trade.get("repaired_force_close"))),
    }


def _select_portfolio_trades(trades: list[dict[str, Any]]) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    sorted_trades = sorted(
        trades,
        key=lambda trade: (
            trade.get("entry_time") or pd.Timestamp.min.tz_localize("UTC"),
            PRESET_PRIORITY.get(str(trade.get("preset_name") or ""), 99),
            str(trade.get("preset_name") or ""),
        ),
    )
    selected: list[dict[str, Any]] = []
    skipped: list[dict[str, Any]] = []

    current_open_trade: dict[str, Any] | None = None
    for trade in sorted_trades:
        entry_time = trade.get("entry_time")
        exit_time = trade.get("exit_time")
        if not isinstance(entry_time, pd.Timestamp) or not isinstance(exit_time, pd.Timestamp):
            skipped.append(
                {
                    "preset_name": trade.get("preset_name"),
                    "entry_time": trade.get("entry_time"),
                    "skip_reason": "missing timestamps",
                }
            )
            continue

        if current_open_trade is None:
            selected.append(trade)
            current_open_trade = trade
            continue

        open_exit = current_open_trade.get("exit_time")
        if isinstance(open_exit, pd.Timestamp) and entry_time < open_exit:
            skipped.append(
                {
                    "preset_name": trade.get("preset_name"),
                    "preset_family": trade.get("preset_family"),
                    "entry_time": trade.get("entry_time"),
                    "exit_time": trade.get("exit_time"),
                    "case_hits": trade.get("case_hits"),
                    "skip_reason": "overlap_with_open_trade",
                    "blocked_by_preset": current_open_trade.get("preset_name"),
                    "blocked_by_entry_time": current_open_trade.get("entry_time"),
                    "blocked_by_exit_time": current_open_trade.get("exit_time"),
                }
            )
            continue

        selected.append(trade)
        current_open_trade = trade

    return selected, skipped


def _build_markdown(report: dict[str, Any]) -> str:
    lines: list[str] = []
    period = dict(report.get("period") or {})
    assumptions = dict(report.get("assumptions") or {})
    raw_summary = dict(report.get("raw_signal_summary") or {})
    portfolio_summary = dict(report.get("portfolio_summary") or {})

    lines.append("# Combined Portfolio Report For Case Window 0001-0005")
    lines.append("")
    lines.append(f"- Window start (UTC): `{period.get('start_utc', '')}`")
    lines.append(f"- Window end (UTC): `{period.get('end_utc', '')}`")
    lines.append(f"- Portfolio mode: `{assumptions.get('portfolio_mode', '')}`")
    lines.append(f"- Overlap rule: `{assumptions.get('overlap_rule', '')}`")
    lines.append(f"- Tie break priority: `{assumptions.get('tie_break_priority', '')}`")
    lines.append(f"- Force-close repairs from cache: `{assumptions.get('force_close_repairs')}`")
    lines.append("")
    lines.append("## Portfolio Summary")
    lines.append("")
    lines.append("| Metric | Value |")
    lines.append("|---|---:|")
    lines.append(f"| Selected trades | {portfolio_summary.get('trades')} |")
    lines.append(f"| Wins | {portfolio_summary.get('wins')} |")
    lines.append(f"| Losses | {portfolio_summary.get('losses')} |")
    lines.append(f"| Win rate % | {portfolio_summary.get('win_rate_pct')} |")
    lines.append(f"| Net profit | {portfolio_summary.get('net_profit')} |")
    lines.append(f"| Return % on balance | {portfolio_summary.get('return_pct')} |")
    lines.append(f"| Profit factor | {portfolio_summary.get('profit_factor')} |")
    lines.append(f"| Max drawdown | {portfolio_summary.get('max_drawdown')} |")
    lines.append(f"| Max drawdown % | {portfolio_summary.get('max_drawdown_pct')} |")
    lines.append("")
    lines.append("## Selected Trades")
    lines.append("")
    lines.append("| # | Preset | Family | Case hit | Entry UTC | Exit UTC | Net PnL | Return % | Exit reason |")
    lines.append("|---:|---:|---|---|---|---|---:|---:|---|")
    for index, trade in enumerate(list(report.get("portfolio_trades") or []), start=1):
        case_hits = ",".join(list(trade.get("case_hits") or [])) or "-"
        lines.append(
            f"| {index} | {trade.get('preset_name')} | {trade.get('preset_family')} | {case_hits} | "
            f"{trade.get('entry_time')} | {trade.get('exit_time')} | {trade.get('net_pnl')} | "
            f"{trade.get('return_pct')} | {trade.get('exit_reason')} |"
        )
    lines.append("")
    lines.append("## Skipped Overlaps")
    lines.append("")
    lines.append("| Preset | Family | Entry UTC | Blocked by | Open trade exit UTC |")
    lines.append("|---:|---|---|---:|---|")
    for trade in list(report.get("skipped_overlaps") or []):
        lines.append(
            f"| {trade.get('preset_name')} | {trade.get('preset_family', '')} | {trade.get('entry_time')} | "
            f"{trade.get('blocked_by_preset', '')} | {trade.get('blocked_by_exit_time', '')} |"
        )
    lines.append("")
    lines.append("## Adjusted Preset Summary")
    lines.append("")
    lines.append("| Preset | Family | Trades | Win rate % | Net profit | Profit factor | Max DD % | Verification bundle |")
    lines.append("|---:|---|---:|---:|---:|---:|---:|---|")
    for preset_name in sorted((report.get("per_preset") or {}).keys()):
        block = dict((report.get("per_preset") or {}).get(preset_name) or {})
        summary = dict(block.get("summary_adjusted") or {})
        lines.append(
            f"| {preset_name} | {block.get('preset_family', '')} | {summary.get('trades')} | "
            f"{summary.get('win_rate_pct')} | {summary.get('net_profit')} | "
            f"{summary.get('profit_factor')} | {summary.get('max_drawdown_pct')} | "
            f"{block.get('verification_bundle', '')} |"
        )
    lines.append("")
    lines.append("## Raw Signal Stack")
    lines.append("")
    lines.append("| Metric | Value |")
    lines.append("|---|---:|")
    lines.append(f"| Raw trades | {raw_summary.get('trades')} |")
    lines.append(f"| Raw net profit | {raw_summary.get('net_profit')} |")
    lines.append(f"| Raw profit factor | {raw_summary.get('profit_factor')} |")
    lines.append(f"| Raw max drawdown % | {raw_summary.get('max_drawdown_pct')} |")
    lines.append("")
    return "\n".join(lines).rstrip() + "\n"


def build_combined_case_window_portfolio(
    *,
    report_json: Path,
    presets_path: Path,
    repo_root: Path,
    output_json: Path,
    output_md: Path,
) -> dict[str, Any]:
    report = _load_json(report_json)
    preset_pack = _load_json(presets_path)
    period = dict(report.get("period") or {})
    start_utc = pd.Timestamp(period["start_utc"])
    end_utc = pd.Timestamp(period["end_utc"])
    cases = list(report.get("cases") or [])
    report_presets = dict(report.get("presets") or {})
    pack_presets = dict(preset_pack.get("presets") or {})

    if not report_presets:
        raise ValueError(f"No presets found in report {report_json}")

    first_preset_name = sorted(report_presets.keys())[0]
    first_settings = dict(dict(pack_presets.get(first_preset_name) or {}).get("settings") or {})
    symbol = str(first_settings.get("symbol") or "BTCUSDT")
    cache_dir = _resolve_cache_dir(repo_root, str(first_settings.get("cache_dir") or "data_cache"))
    price_source = str(first_settings.get("price_source") or "LAST")
    initial_balance = float(first_settings.get("initial_balance") or 1000.0)

    close_lookup = _build_close_lookup(
        symbol=symbol,
        start_utc=start_utc,
        end_utc=end_utc,
        cache_dir=cache_dir,
        price_source=price_source,
    )

    svc = BacktestRuntimeService(presets_path=presets_path, repo_root=repo_root)
    per_preset: dict[str, Any] = {}
    all_trades: list[dict[str, Any]] = []

    for preset_name in sorted(report_presets.keys()):
        preset_block = dict(report_presets[preset_name] or {})
        pack_block = dict(pack_presets.get(preset_name) or {})
        settings = dict(pack_block.get("settings") or {})
        run = svc.run_saved_preset(
            str(preset_name),
            RunOverrides(start=str(start_utc), end=str(end_utc)),
        )
        verification_bundle = Path(str(run.get("verification_bundle") or ""))
        trades_csv = verification_bundle / "trades.csv"
        trades = _load_trade_rows(trades_csv)
        fixed_trades: list[dict[str, Any]] = []
        for trade in trades:
            fixed = _repair_force_close_trade(trade, close_lookup=close_lookup)
            fixed["preset_name"] = str(preset_name)
            fixed["preset_family"] = str(preset_block.get("preset_family") or "")
            fixed["verification_bundle"] = str(verification_bundle)
            fixed["symbol"] = str(settings.get("symbol") or symbol)
            fixed.update(_match_trade_to_cases(fixed, cases=cases))
            fixed_trades.append(fixed)
            all_trades.append(fixed)

        summary_adjusted = _summarize_trades(
            fixed_trades,
            initial_balance=float(settings.get("initial_balance") or initial_balance),
        )
        per_preset[preset_name] = {
            "preset_family": str(preset_block.get("preset_family") or ""),
            "verification_bundle": str(verification_bundle),
            "summary_adjusted": {key: _safe_round(value, 6) for key, value in summary_adjusted.items()},
            "trade_count": len(fixed_trades),
            "force_close_repairs": sum(1 for trade in fixed_trades if bool(trade.get("repaired_force_close"))),
        }

    selected_trades, skipped_overlaps = _select_portfolio_trades(all_trades)
    raw_signal_summary = _summarize_trades(all_trades, initial_balance=initial_balance)
    portfolio_summary = _summarize_trades(selected_trades, initial_balance=initial_balance)

    force_close_repairs = sum(1 for trade in all_trades if bool(trade.get("repaired_force_close")))
    result = {
        "period": {
            "start_utc": str(start_utc),
            "end_utc": str(end_utc),
            "symbol": symbol,
        },
        "assumptions": {
            "portfolio_mode": "one_open_long_position_at_a_time",
            "overlap_rule": "take first chronological entry while flat and skip later overlapping entries",
            "tie_break_priority": "901 > 903 > 902",
            "force_close_repairs": force_close_repairs,
            "force_close_price_source": price_source,
            "force_close_cache_dir": str(cache_dir),
        },
        "per_preset": per_preset,
        "raw_signal_summary": {key: _safe_round(value, 6) for key, value in raw_signal_summary.items()},
        "portfolio_summary": {key: _safe_round(value, 6) for key, value in portfolio_summary.items()},
        "portfolio_trades": [_trade_to_serializable(trade) for trade in selected_trades],
        "skipped_overlaps": [_trade_to_serializable(trade) for trade in skipped_overlaps],
    }

    _write_json(output_json, result)
    output_md.write_text(_build_markdown(result), encoding="utf-8")
    return result


def main() -> int:
    args = build_parser().parse_args()
    report_json = args.report_json.resolve()
    presets_path = args.presets_path.resolve()
    repo_root = args.repo_root.resolve()
    output_json = args.output_json.resolve()
    output_md = args.output_md.resolve() if args.output_md else default_output_md(output_json)

    result = build_combined_case_window_portfolio(
        report_json=report_json,
        presets_path=presets_path,
        repo_root=repo_root,
        output_json=output_json,
        output_md=output_md,
    )

    compact = {
        "output_json": str(output_json),
        "output_md": str(output_md),
        "portfolio_summary": result.get("portfolio_summary"),
        "raw_signal_summary": result.get("raw_signal_summary"),
        "selected_trade_count": len(list(result.get("portfolio_trades") or [])),
        "skipped_overlap_count": len(list(result.get("skipped_overlaps") or [])),
    }
    print(json.dumps(compact, indent=2, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
