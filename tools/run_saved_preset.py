from __future__ import annotations

import argparse
import json
import math
from pathlib import Path
import sys
from typing import Any, Dict, List, Tuple

import pandas as pd

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from app.backtest import make_streams_htf_closed_only, run_backtest_tick
from app.backtest_reporting import _make_trade_df
from app.backtest_models import BacktestConfig
from app.fast_backtest import run_backtest_auto
from app.constants import (
    BACKTEST_MODE_BAR_1M_HTF_CLOSED_ONLY,
    BACKTEST_MODE_TICK,
)
from app.data_binance import binance_fapi_server_time_utc, fetch_klines_1m_futures
from app.indicators_streaming import simulate_multitf_indicators
from app.prepared_dataset import (
    build_prepared_dataset,
    find_covering_prepared_dataset_on_disk,
    save_prepared_dataset_to_disk,
    slice_df_1m_window,
    slice_streams_window,
)
from app.rules import EntryFilterConfig, Rule
from app.strategy_requirements import compile_stream_requirements
from app.utils_time import parse_utc, required_warmup_minutes


def _progress(msg: str, pct: Any = None) -> None:
    if pct is None:
        print(msg, flush=True)
        return
    print(f"[{pct}%] {msg}", flush=True)


def _load_presets(path: Path) -> Dict[str, Any]:
    with path.open("r", encoding="utf-8") as fh:
        payload = json.load(fh)
    if not isinstance(payload, dict):
        raise ValueError("Preset file must contain a JSON object.")
    return payload


def _pick_preset_name(payload: Dict[str, Any], requested_name: str | None) -> str:
    presets = payload.get("presets", {})
    if not isinstance(presets, dict) or not presets:
        raise ValueError("No presets found.")
    if requested_name:
        if requested_name not in presets:
            raise ValueError(f"Preset '{requested_name}' was not found.")
        return requested_name
    meta = payload.get("meta", {})
    if isinstance(meta, dict):
        last_name = str(meta.get("last_preset") or "").strip()
        if last_name in presets:
            return last_name
    return sorted(presets.keys())[0]


def _rule_from_dict(raw: Dict[str, Any]) -> Rule:
    return Rule(
        timeframe=str(raw.get("timeframe", "1m") or "1m"),
        mode=str(raw.get("mode", "event") or "event"),
        field=str(raw.get("field", "") or ""),
        op=str(raw.get("op", "EVENT") or "EVENT"),
        value=raw.get("value"),
        is_trigger=bool(raw.get("is_trigger", False)),
        bars_ago_mode=str(raw.get("bars_ago_mode", "OFF") or "OFF"),
        bars_ago_n=int(raw.get("bars_ago_n", 0) or 0),
        require_still_valid=bool(raw.get("require_still_valid", False)),
        is_final_gate=bool(raw.get("is_final_gate", False)),
        is_sequence_canceler=bool(raw.get("is_sequence_canceler", False)),
        window_bars=int(raw.get("window_bars", 1) or 1),
        compare_to=raw.get("compare_to"),
    )


def _materialize_preset(
    preset: Dict[str, Any],
) -> Tuple[Dict[str, Any], Dict[str, str], Dict[str, List[str]], Dict[str, EntryFilterConfig]]:
    tabs = preset.get("tabs", {})
    if not isinstance(tabs, dict):
        raise ValueError("Preset tabs payload is invalid.")

    rules_model: Dict[str, Any] = {}
    tab_group_join_mode: Dict[str, str] = {}
    group_rule_join_mode: Dict[str, List[str]] = {}

    for tab_name, tab_payload in tabs.items():
        if not isinstance(tab_payload, dict):
            continue
        tab_group_join_mode[str(tab_name)] = str(tab_payload.get("groups_join", "AND") or "AND")
        groups_payload = tab_payload.get("groups", [])
        groups: List[List[Rule]] = []
        group_joins: List[str] = []
        if isinstance(groups_payload, list):
            for group_payload in groups_payload:
                if not isinstance(group_payload, dict):
                    continue
                group_joins.append(str(group_payload.get("rules_join", "OR") or "OR"))
                raw_rules = group_payload.get("rules", [])
                rules = []
                if isinstance(raw_rules, list):
                    for raw_rule in raw_rules:
                        if isinstance(raw_rule, dict):
                            rules.append(_rule_from_dict(raw_rule))
                groups.append(rules)
        rules_model[str(tab_name)] = groups
        group_rule_join_mode[str(tab_name)] = group_joins

    raw_filters = preset.get("entry_filters", {})
    entry_filters: Dict[str, EntryFilterConfig] = {}
    if isinstance(raw_filters, dict):
        for tab_name in ("Long Entry", "Short Entry"):
            entry_filters[tab_name] = EntryFilterConfig.from_dict(raw_filters.get(tab_name, {}))
    else:
        entry_filters = {
            "Long Entry": EntryFilterConfig(),
            "Short Entry": EntryFilterConfig(),
        }

    return rules_model, tab_group_join_mode, group_rule_join_mode, entry_filters


def _resolve_cache_dir(repo_root: Path, preferred: str) -> Path:
    first = Path(preferred)
    if not first.is_absolute():
        first = (repo_root / first).resolve()
    if first.exists():
        return first

    fallback = Path.home() / "Downloads" / "data_cache"
    if fallback.exists():
        return fallback
    return first


def _safe_round(value: Any, digits: int = 4) -> Any:
    try:
        num = float(value)
    except Exception:
        return value
    if not math.isfinite(num):
        return str(num)
    return round(num, digits)


def _slug(value: str) -> str:
    cleaned = "".join(ch.lower() if ch.isalnum() else "_" for ch in str(value or "").strip())
    while "__" in cleaned:
        cleaned = cleaned.replace("__", "_")
    return cleaned.strip("_") or "run"


def _event_activity_summary(
    rules_model: Dict[str, Any],
    streams_full: Dict[str, pd.DataFrame],
    start: pd.Timestamp,
    end: pd.Timestamp,
) -> Dict[str, Dict[str, Any]]:
    out: Dict[str, Dict[str, Any]] = {}
    window_start = pd.to_datetime(start, utc=True)
    window_end = pd.to_datetime(end, utc=True)
    for tab_name, groups in (rules_model or {}).items():
        if not isinstance(groups, list):
            continue
        for group in groups:
            if not isinstance(group, list):
                continue
            for rule in group:
                try:
                    if str(getattr(rule, "mode", "") or "").lower() != "event":
                        continue
                    tf = str(getattr(rule, "timeframe", "") or "")
                    field = str(getattr(rule, "field", "") or "")
                    frame = streams_full.get(tf)
                    if frame is None or frame.empty or field not in frame.columns:
                        continue
                    idx = pd.to_datetime(frame.index, utc=True, errors="coerce")
                    mask = (idx >= window_start) & (idx <= window_end)
                    if not mask.any():
                        continue
                    series = frame.loc[mask, field]
                    count = int(pd.to_numeric(series, errors="coerce").fillna(0).astype(float).gt(0).sum())
                    last_hits = frame.loc[mask & pd.to_numeric(frame[field], errors="coerce").fillna(0).astype(float).gt(0), [field]].tail(5)
                    key = f"{tab_name} | {tf} | {field}"
                    out[key] = {
                        "count": count,
                        "sample_times": [str(ts) for ts in last_hits.index.tolist()],
                    }
                except Exception:
                    continue
    return out


def _event_signal_rows(
    rules_model: Dict[str, Any],
    streams_full: Dict[str, pd.DataFrame],
    start: pd.Timestamp,
    end: pd.Timestamp,
) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    window_start = pd.to_datetime(start, utc=True)
    window_end = pd.to_datetime(end, utc=True)
    for tab_name, groups in (rules_model or {}).items():
        if not isinstance(groups, list):
            continue
        for group_index, group in enumerate(groups, start=1):
            if not isinstance(group, list):
                continue
            for rule_index, rule in enumerate(group, start=1):
                try:
                    if str(getattr(rule, "mode", "") or "").lower() != "event":
                        continue
                    tf = str(getattr(rule, "timeframe", "") or "")
                    field = str(getattr(rule, "field", "") or "")
                    frame = streams_full.get(tf)
                    if frame is None or frame.empty or field not in frame.columns:
                        continue
                    idx = pd.to_datetime(frame.index, utc=True, errors="coerce")
                    mask = (idx >= window_start) & (idx <= window_end)
                    if not mask.any():
                        continue
                    values = frame.loc[mask, field]
                    truthy = pd.to_numeric(values, errors="coerce").fillna(0).astype(float).gt(0)
                    hits = values.loc[truthy]
                    for ts, val in hits.items():
                        rows.append(
                            {
                                "timestamp_utc": str(pd.to_datetime(ts, utc=True)),
                                "tab": str(tab_name),
                                "group_index": int(group_index),
                                "rule_index": int(rule_index),
                                "timeframe": str(tf),
                                "field": str(field),
                                "op": str(getattr(rule, "op", "") or ""),
                                "value": str(getattr(rule, "value", "") if getattr(rule, "value", None) is not None else ""),
                                "is_trigger": bool(getattr(rule, "is_trigger", False)),
                                "signal_value": _safe_round(val, 6),
                                "label": str(rule.label()),
                            }
                        )
                except Exception:
                    continue
    rows.sort(key=lambda row: (row["timestamp_utc"], row["tab"], row["group_index"], row["rule_index"]))
    return rows


def _write_verification_bundle(
    *,
    repo_root: Path,
    preset_name: str,
    symbol: str,
    start: pd.Timestamp,
    end: pd.Timestamp,
    bt_mode: str,
    cache_dir: Path,
    cfg: BacktestConfig,
    summary: Dict[str, Any],
    event_activity: Dict[str, Dict[str, Any]],
    signal_rows: List[Dict[str, Any]],
    res: Any,
) -> Path:
    runs_dir = repo_root / "runs"
    runs_dir.mkdir(parents=True, exist_ok=True)
    stamp = pd.Timestamp.now(tz="UTC").strftime("%Y%m%d_%H%M%S")
    bundle_dir = runs_dir / f"{_slug(preset_name)}_{_slug(symbol)}_{stamp}"
    bundle_dir.mkdir(parents=True, exist_ok=True)

    payload = {
        "preset_name": preset_name,
        "symbol": symbol,
        "start": str(start),
        "end": str(end),
        "mode": str(bt_mode),
        "cache_dir": str(cache_dir),
        "config": {
            "allow_reverse": bool(cfg.allow_reverse),
            "skip_if_both_entries": bool(cfg.skip_if_both_entries),
            "stop_loss_mode": str(cfg.stop_loss_mode),
            "stop_loss_value": float(cfg.stop_loss_value),
            "take_profit_mode": str(cfg.take_profit_mode),
            "take_profit_value": float(cfg.take_profit_value),
            "capture_trade_details": bool(cfg.capture_trade_details),
            "equity_curve_stride": int(cfg.equity_curve_stride),
            "step_timeframe": str(cfg.step_timeframe),
        },
        "summary": summary,
        "event_activity": event_activity,
    }
    (bundle_dir / "summary.json").write_text(json.dumps(payload, indent=2, ensure_ascii=False, default=str), encoding="utf-8")

    trade_df = _make_trade_df(list(res.trades or []))
    if trade_df is None or trade_df.empty:
        pd.DataFrame(
            columns=[
                "trade_id",
                "side",
                "entry_time",
                "exit_time",
                "entry_price",
                "exit_price",
                "net_pnl",
                "return_pct",
                "entry_reason",
                "exit_reason",
            ]
        ).to_csv(bundle_dir / "trades.csv", index=False)
    else:
        trade_df.to_csv(bundle_dir / "trades.csv", index=False)

    pd.DataFrame(signal_rows).to_csv(bundle_dir / "signal_events.csv", index=False)

    eq = res.equity_curve.copy() if getattr(res, "equity_curve", None) is not None else pd.DataFrame()
    if isinstance(eq, pd.DataFrame) and not eq.empty:
        eq_out = eq.copy()
        eq_out = eq_out.reset_index()
        first_col = str(eq_out.columns[0])
        eq_out = eq_out.rename(columns={first_col: "timestamp_utc"})
        eq_out.to_csv(bundle_dir / "equity_curve.csv", index=False)
    else:
        pd.DataFrame(columns=["timestamp_utc", "equity", "equity_mark"]).to_csv(bundle_dir / "equity_curve.csv", index=False)

    notes = (
        "Verification bundle\n"
        "1. Use the same symbol and window in TradingView.\n"
        "2. Compare signal markers against signal_events.csv first.\n"
        "3. Then compare trade-by-trade against trades.csv.\n"
        "4. Use summary.json only after signals and trades match.\n"
        "5. Do not fine-tune strategy settings until the signal timestamps match.\n"
    )
    (bundle_dir / "README.txt").write_text(notes, encoding="utf-8")
    return bundle_dir


def main() -> int:
    repo_root = REPO_ROOT
    default_presets = repo_root / "app" / "presets.json"

    parser = argparse.ArgumentParser(description="Run a saved project preset without the UI.")
    parser.add_argument("--preset", dest="preset_name", default=None, help="Preset name to run. Defaults to app/presets.json meta.last_preset.")
    parser.add_argument("--presets-file", dest="presets_file", default=str(default_presets), help="Path to presets.json.")
    parser.add_argument("--cache-dir", dest="cache_dir", default=None, help="Override candle/indicator cache directory.")
    parser.add_argument("--start", dest="start_override", default=None, help="Override preset start UTC, for example 2025-12-29 10:22:00")
    parser.add_argument("--end", dest="end_override", default=None, help="Override preset end UTC, for example 2026-03-29 10:22:00")
    parser.add_argument("--initial-balance", type=float, default=1000.0)
    parser.add_argument("--order-notional", type=float, default=100.0)
    parser.add_argument("--fee-pct", type=float, default=0.04, help="Percent per side, like the UI.")
    parser.add_argument("--slippage-bps", type=float, default=0.0)
    parser.add_argument("--stop-loss-mode", default="PERCENT")
    parser.add_argument("--stop-loss-value", type=float, default=0.0)
    parser.add_argument("--stop-loss-timeframe", default="1m")
    parser.add_argument("--stop-loss-field", default="")
    parser.add_argument("--stop-loss-offset-pct", type=float, default=0.0)
    parser.add_argument("--take-profit-mode", default="PERCENT")
    parser.add_argument("--take-profit-value", type=float, default=0.0)
    parser.add_argument("--take-profit-timeframe", default="1m")
    parser.add_argument("--take-profit-field", default="")
    parser.add_argument("--take-profit-offset-pct", type=float, default=0.0)
    parser.add_argument("--allow-reverse", dest="allow_reverse", action="store_true", default=True)
    parser.add_argument("--no-allow-reverse", dest="allow_reverse", action="store_false")
    parser.add_argument("--skip-both-entries", dest="skip_both_entries", action="store_true", default=True)
    parser.add_argument("--no-skip-both-entries", dest="skip_both_entries", action="store_false")
    parser.add_argument("--step-timeframe", default="1m")
    parser.add_argument("--capture-trade-details", action="store_true", default=False)
    parser.add_argument("--equity-curve-stride", type=int, default=15)
    parser.add_argument("--output-json", default=None, help="Optional path to save the backtest summary and a small trade sample.")
    args = parser.parse_args()

    presets_file = Path(args.presets_file).resolve()
    payload = _load_presets(presets_file)
    preset_name = _pick_preset_name(payload, args.preset_name)
    preset = payload["presets"][preset_name]

    settings = preset.get("settings", {})
    if not isinstance(settings, dict):
        raise ValueError("Preset settings payload is invalid.")

    symbol = str(settings.get("symbol", "BTCUSDT") or "BTCUSDT").strip().upper()
    start = parse_utc(str(args.start_override or settings.get("start", "") or ""))
    end = parse_utc(str(args.end_override or settings.get("end", "") or ""))
    if end <= start:
        raise ValueError("Preset end must be after start.")

    tfs = [
        tf
        for tf, enabled in (settings.get("timeframes", {}) or {}).items()
        if bool(enabled)
    ]
    if "1m" not in tfs:
        tfs = ["1m"] + tfs
    if args.step_timeframe not in tfs:
        tfs.append(args.step_timeframe)

    rules_model, tab_group_join_mode, group_rule_join_mode, entry_filters = _materialize_preset(preset)

    cfg = BacktestConfig(
        initial_balance=float(args.initial_balance),
        order_notional_usdt=float(args.order_notional),
        fee_rate=float(args.fee_pct) / 100.0,
        slippage_bps=float(args.slippage_bps),
        stop_loss_pct=(float(args.stop_loss_value) if str(args.stop_loss_mode).upper() == "PERCENT" else 0.0),
        take_profit_pct=(float(args.take_profit_value) if str(args.take_profit_mode).upper() == "PERCENT" else 0.0),
        stop_loss_mode=str(args.stop_loss_mode).upper(),
        stop_loss_value=float(args.stop_loss_value),
        stop_loss_timeframe=str(args.stop_loss_timeframe),
        stop_loss_field=str(args.stop_loss_field),
        stop_loss_offset_pct=float(args.stop_loss_offset_pct),
        take_profit_mode=str(args.take_profit_mode).upper(),
        take_profit_value=float(args.take_profit_value),
        take_profit_timeframe=str(args.take_profit_timeframe),
        take_profit_field=str(args.take_profit_field),
        take_profit_offset_pct=float(args.take_profit_offset_pct),
        allow_reverse=bool(args.allow_reverse),
        skip_if_both_entries=bool(args.skip_both_entries),
        step_timeframe=str(args.step_timeframe),
        capture_trade_details=bool(args.capture_trade_details),
        equity_curve_stride=max(1, int(args.equity_curve_stride)),
    )

    req_spec = compile_stream_requirements(
        rules_model,
        entry_filters=entry_filters,
        backtest_cfg=cfg,
        include_plot_defaults=True,
    )

    preset_warmup = int(settings.get("warmup_min", 0) or 0)
    needed_warmup = required_warmup_minutes(tfs, required_fields=req_spec)
    warmup = max(preset_warmup, int(needed_warmup or 0))
    warmup_start = start - pd.Timedelta(minutes=warmup)

    cache_dir = _resolve_cache_dir(repo_root, args.cache_dir or str(settings.get("cache_dir", "data_cache") or "data_cache"))
    cache_dir.mkdir(parents=True, exist_ok=True)

    bt_mode = str(settings.get("bt_mode", "") or "")
    price_source = str(settings.get("price_source", "LAST") or "LAST").upper()
    macd_impl = str(settings.get("macd_impl", "TRADINGVIEW") or "TRADINGVIEW").upper()
    adx_impl = str(settings.get("adx_impl", "TRADINGVIEW") or "TRADINGVIEW").upper()

    print(f"Preset: {preset_name}", flush=True)
    print(f"Symbol: {symbol}", flush=True)
    print(f"Window UTC: {start} -> {end}", flush=True)
    print(f"Warmup minutes: preset={preset_warmup}, required={needed_warmup}, used={warmup}", flush=True)
    print(f"Mode: {bt_mode}", flush=True)
    print(f"Cache dir: {cache_dir}", flush=True)
    print(
        "Backtest runtime settings: "
        f"reverse={cfg.allow_reverse}, "
        f"skip_both={cfg.skip_if_both_entries}, "
        f"stop={cfg.stop_loss_mode}:{cfg.stop_loss_value}, "
        f"take={cfg.take_profit_mode}:{cfg.take_profit_value}, "
        f"fast_details={not cfg.capture_trade_details}, "
        f"equity_stride={cfg.equity_curve_stride}",
        flush=True,
    )

    srv = binance_fapi_server_time_utc()
    if srv is not None:
        print(f"Binance server time (UTC): {srv}", flush=True)

    prepared_disk = find_covering_prepared_dataset_on_disk(
        str(cache_dir),
        symbol=symbol,
        start=warmup_start,
        end=end,
        timeframes=tfs,
        price_source=price_source,
        macd_impl=macd_impl,
        adx_impl=adx_impl,
        required_fields=req_spec,
        market_state_thresholds=None,
    )
    if prepared_disk is not None:
        _progress("Loaded prepared dataset from disk.", 100)
        df_1m = slice_df_1m_window(prepared_disk.df_1m_full, warmup_start, end)
        streams_full = slice_streams_window(prepared_disk.streams_full, warmup_start, end, timeframes=tfs)
    else:
        df_1m = fetch_klines_1m_futures(
            symbol,
            warmup_start,
            end,
            str(cache_dir),
            progress_cb=_progress,
            price_source=price_source,
        )
        if df_1m.empty:
            raise ValueError("No candles returned for the requested window.")

        streams_full = simulate_multitf_indicators(
            df_1m,
            tfs,
            _progress,
            macd_impl=macd_impl,
            adx_impl=adx_impl,
            cache_dir=str(cache_dir),
            symbol=symbol,
            start_utc=warmup_start,
            end_utc=end,
            price_source=price_source,
            required_fields=req_spec,
        )
        try:
            prepared_entry = build_prepared_dataset(
                symbol=symbol,
                start=warmup_start,
                end=end,
                timeframes=tfs,
                price_source=price_source,
                macd_impl=macd_impl,
                adx_impl=adx_impl,
                required_fields=req_spec,
                market_state_thresholds=None,
                df_1m_full=df_1m,
                streams_full=streams_full,
            )
            saved_prepared = save_prepared_dataset_to_disk(str(cache_dir), prepared_entry)
            if saved_prepared:
                _progress(f"Saved prepared dataset ({Path(saved_prepared).name}).", 100)
        except Exception:
            pass

    if bt_mode == BACKTEST_MODE_TICK:
        res = run_backtest_tick(
            symbol=symbol,
            df_1m_full=df_1m,
            start=start,
            end=end,
            tfs=tfs,
            rules_model=rules_model,
            tab_group_join_mode=tab_group_join_mode,
            group_rule_join_mode=group_rule_join_mode,
            entry_filters=entry_filters,
            cfg=cfg,
            cache_dir=str(cache_dir),
            progress_cb=_progress,
            streams_full=streams_full,
            macd_impl=macd_impl,
        )
    else:
        if bt_mode == BACKTEST_MODE_BAR_1M_HTF_CLOSED_ONLY:
            _progress("Planning backtest engine and preparing rule-specific streams...", 0)
        res = run_backtest_auto(
            symbol=symbol,
            streams_full=streams_full,
            start=start,
            end=end,
            rules_model=rules_model,
            tab_group_join_mode=tab_group_join_mode,
            group_rule_join_mode=group_rule_join_mode,
            cfg=cfg,
            df_1m_full=df_1m,
            entry_filters=entry_filters,
            apply_htf_closed_only=(bt_mode == BACKTEST_MODE_BAR_1M_HTF_CLOSED_ONLY),
        )

    summary = dict(res.summary or {})
    event_activity = _event_activity_summary(rules_model, streams_full, start, end)
    signal_rows = _event_signal_rows(rules_model, streams_full, start, end)
    bundle_dir = _write_verification_bundle(
        repo_root=repo_root,
        preset_name=preset_name,
        symbol=symbol,
        start=start,
        end=end,
        bt_mode=bt_mode,
        cache_dir=cache_dir,
        cfg=cfg,
        summary=summary,
        event_activity=event_activity,
        signal_rows=signal_rows,
        res=res,
    )
    print("", flush=True)
    print("Summary", flush=True)
    for key in [
        "strategy_plan_engine",
        "initial_balance",
        "ending_balance",
        "return_pct",
        "num_trades",
        "win_rate_pct",
        "profit_factor",
        "avg_trade_return_pct",
        "max_drawdown",
        "max_drawdown_pct",
        "stop_loss_count",
        "take_profit_count",
        "timing_prepare_streams_sec",
        "timing_compile_signals_sec",
        "timing_simulation_sec",
        "timing_total_sec",
    ]:
        if key in summary:
            print(f"{key}: {_safe_round(summary.get(key))}", flush=True)

    exit_reason_counts = summary.get("exit_reason_counts", {})
    if isinstance(exit_reason_counts, dict) and exit_reason_counts:
        print(f"exit_reason_counts: {exit_reason_counts}", flush=True)

    print(f"equity_points: {len(res.equity_curve)}", flush=True)
    print(f"trades_captured: {len(res.trades)}", flush=True)
    if event_activity:
        print("event_activity:", flush=True)
        for key, meta in event_activity.items():
            print(f"  {key}: count={meta.get('count')} sample_times={meta.get('sample_times')}", flush=True)
    print(f"verification_bundle: {bundle_dir}", flush=True)

    if args.output_json:
        out_path = Path(args.output_json).resolve()
        out_path.parent.mkdir(parents=True, exist_ok=True)
        sample_trades = []
        for trade in res.trades[:10]:
            sample_trades.append(
                {
                    "trade_id": int(trade.trade_id),
                    "side": str(trade.side),
                    "entry_time": str(trade.entry_time),
                    "exit_time": str(trade.exit_time),
                    "entry_price": _safe_round(trade.entry_price, 6),
                    "exit_price": _safe_round(trade.exit_price, 6),
                    "net_pnl": _safe_round(trade.net_pnl, 6),
                    "return_pct": _safe_round(trade.return_pct, 6),
                    "entry_reason": str(trade.entry_reason),
                    "exit_reason": str(trade.exit_reason),
                }
            )
        payload = {
            "preset_name": preset_name,
            "symbol": symbol,
            "start": str(start),
            "end": str(end),
            "warmup_minutes": warmup,
            "cache_dir": str(cache_dir),
            "config": {
                "allow_reverse": bool(cfg.allow_reverse),
                "skip_if_both_entries": bool(cfg.skip_if_both_entries),
                "stop_loss_mode": str(cfg.stop_loss_mode),
                "stop_loss_value": float(cfg.stop_loss_value),
                "take_profit_mode": str(cfg.take_profit_mode),
                "take_profit_value": float(cfg.take_profit_value),
                "capture_trade_details": bool(cfg.capture_trade_details),
                "equity_curve_stride": int(cfg.equity_curve_stride),
            },
            "summary": summary,
            "event_activity": event_activity,
            "sample_trades": sample_trades,
            "verification_bundle": str(bundle_dir),
        }
        out_path.write_text(json.dumps(payload, indent=2, ensure_ascii=False, default=str), encoding="utf-8")
        print(f"Saved report JSON: {out_path}", flush=True)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
