from __future__ import annotations

import argparse
import json
from pathlib import Path
import sys
from typing import Any, Dict, Iterable, List, Optional, Tuple

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from app.backtest_models import BacktestConfig
from app.services.ui_v2_downloader import DownloaderRuntimeService
from app.strategy_requirements import compile_stream_requirements
from app.utils_time import parse_utc
from tools.run_saved_preset import (  # noqa: E402
    _load_presets,
    _materialize_preset,
    _pick_preset_name,
    _resolve_cache_dir,
)


def _print_progress(message: str, pct: Any = None) -> None:
    if pct is None:
        print(message, flush=True)
        return
    try:
        value = float(pct)
    except Exception:
        print(message, flush=True)
        return
    print(f"[{value:6.2f}%] {message}", flush=True)


def _print_log(message: str) -> None:
    print(f"  {message}", flush=True)


def _selected_preset_names(payload: Dict[str, Any], requested_name: Optional[str], use_all: bool) -> List[str]:
    presets = payload.get("presets", {})
    if not isinstance(presets, dict) or not presets:
        raise ValueError("No presets found.")
    if use_all:
        return sorted(presets.keys())
    return [_pick_preset_name(payload, requested_name)]


def _coerce_bool(value: Any, default: bool) -> bool:
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    raw = str(value).strip().lower()
    if raw in {"1", "true", "yes", "on"}:
        return True
    if raw in {"0", "false", "no", "off"}:
        return False
    return default


def _build_backtest_cfg(settings: Dict[str, Any]) -> BacktestConfig:
    return BacktestConfig(
        initial_balance=float(settings.get("initial_balance", 1000.0) or 1000.0),
        order_notional_usdt=float(settings.get("order_notional_usdt", settings.get("order_notional", 100.0)) or 100.0),
        fee_rate=float(settings.get("fee_rate", 0.0004) or 0.0004),
        slippage_bps=float(settings.get("slippage_bps", 0.0) or 0.0),
        stop_loss_pct=float(settings.get("stop_loss_pct", 0.0) or 0.0),
        take_profit_pct=float(settings.get("take_profit_pct", 0.0) or 0.0),
        break_even_after_mfe_pct=float(settings.get("break_even_after_mfe_pct", 0.0) or 0.0),
        trailing_stop_pct=float(settings.get("trailing_stop_pct", 0.0) or 0.0),
        stop_loss_mode=str(settings.get("stop_loss_mode", "PERCENT") or "PERCENT").upper(),
        stop_loss_value=float(settings.get("stop_loss_value", 0.0) or 0.0),
        stop_loss_timeframe=str(settings.get("stop_loss_timeframe", "1m") or "1m"),
        stop_loss_field=str(settings.get("stop_loss_field", "") or ""),
        stop_loss_offset_pct=float(settings.get("stop_loss_offset_pct", 0.0) or 0.0),
        take_profit_mode=str(settings.get("take_profit_mode", "PERCENT") or "PERCENT").upper(),
        take_profit_value=float(settings.get("take_profit_value", 0.0) or 0.0),
        take_profit_timeframe=str(settings.get("take_profit_timeframe", "1m") or "1m"),
        take_profit_field=str(settings.get("take_profit_field", "") or ""),
        take_profit_offset_pct=float(settings.get("take_profit_offset_pct", 0.0) or 0.0),
        allow_reverse=_coerce_bool(settings.get("allow_reverse"), True),
        skip_if_both_entries=_coerce_bool(settings.get("skip_if_both_entries"), True),
        tick_auto_download=_coerce_bool(settings.get("tick_auto_download"), False),
        tick_cache_partition=str(settings.get("tick_cache_partition", "daily") or "daily"),
        step_timeframe=str(settings.get("step_timeframe", "1m") or "1m"),
        capture_trade_details=_coerce_bool(settings.get("capture_trade_details"), True),
        equity_curve_stride=max(1, int(settings.get("equity_curve_stride", 1) or 1)),
    )


def _enabled_timeframes(settings: Dict[str, Any]) -> List[str]:
    timeframes = [
        str(tf)
        for tf, enabled in (settings.get("timeframes", {}) or {}).items()
        if bool(enabled)
    ]
    ordered: List[str] = []
    for tf in ["1m", "5m", "15m", "30m", "1h"]:
        if tf in timeframes and tf not in ordered:
            ordered.append(tf)
    for tf in timeframes:
        if tf not in ordered:
            ordered.append(tf)
    if "1m" not in ordered:
        ordered.insert(0, "1m")
    return ordered


def _preset_scope(
    *,
    repo_root: Path,
    preset_name: str,
    preset: Dict[str, Any],
    cache_dir_override: Optional[str],
    start_override: Optional[str],
    end_override: Optional[str],
    save_exact_indicator_cache: bool,
) -> Dict[str, Any]:
    settings = preset.get("settings", {})
    if not isinstance(settings, dict):
        raise ValueError(f"Preset '{preset_name}' settings payload is invalid.")

    symbol = str(settings.get("symbol", "BTCUSDT") or "BTCUSDT").strip().upper()
    start = parse_utc(str(start_override or settings.get("start", "") or ""))
    end = parse_utc(str(end_override or settings.get("end", "") or ""))
    if start is None or end is None:
        raise ValueError(f"Preset '{preset_name}' must define start and end UTC.")
    if end <= start:
        raise ValueError(f"Preset '{preset_name}' has end <= start.")

    rules_model, _, _, entry_filters = _materialize_preset(preset)
    backtest_cfg = _build_backtest_cfg(settings)
    req_spec = compile_stream_requirements(
        rules_model,
        entry_filters=entry_filters,
        backtest_cfg=backtest_cfg,
        include_plot_defaults=True,
    )
    required_fields = sorted(req_spec.normalized_fields(include_defaults=False))
    cache_dir = _resolve_cache_dir(repo_root, cache_dir_override or str(settings.get("cache_dir", "data_cache") or "data_cache"))

    return {
        "preset_name": preset_name,
        "symbol": symbol,
        "start": str(start),
        "end": str(end),
        "cache_dir": str(cache_dir),
        "price_source": str(settings.get("price_source", "LAST") or "LAST").upper(),
        "timeframes": _enabled_timeframes(settings),
        "warmup_minutes": int(settings.get("warmup_min", 0) or 0),
        "indicator_fields": required_fields,
        "ema_settings": settings.get("ema_indicators"),
        "rsi_settings": settings.get("rsi_indicators"),
        "save_exact_indicator_cache": bool(save_exact_indicator_cache),
    }


def _dedupe_key(action: str, scope: Dict[str, Any]) -> Tuple[Any, ...]:
    if action == "candle_download_window":
        return (
            action,
            scope["symbol"],
            scope["cache_dir"],
            scope["price_source"],
            scope["start"],
            scope["end"],
            scope["warmup_minutes"],
        )
    return (
        action,
        scope["symbol"],
        scope["cache_dir"],
        scope["price_source"],
        tuple(scope["timeframes"]),
        scope["start"],
        scope["end"],
        scope["warmup_minutes"],
        tuple(scope["indicator_fields"]),
        bool(scope.get("save_exact_indicator_cache", False)),
        json.dumps(scope.get("ema_settings"), sort_keys=True, ensure_ascii=True),
        json.dumps(scope.get("rsi_settings"), sort_keys=True, ensure_ascii=True),
    )


def _iter_unique_scopes(action: str, scopes: Iterable[Dict[str, Any]]) -> List[Dict[str, Any]]:
    unique: List[Dict[str, Any]] = []
    seen = set()
    for scope in scopes:
        key = _dedupe_key(action, scope)
        if key in seen:
            continue
        seen.add(key)
        unique.append(scope)
    return unique


def _run_action(runtime: DownloaderRuntimeService, action: str, scope: Dict[str, Any]) -> Dict[str, Any]:
    preset_name = str(scope["preset_name"])
    print(f"\n=== {preset_name} :: {action} ===", flush=True)
    print(
        f"{scope['symbol']} {scope['price_source']} | "
        f"{scope['start']} -> {scope['end']} | "
        f"TFs={','.join(scope['timeframes'])}",
        flush=True,
    )
    if action == "indicator_build_window":
        print(
            f"Fields={len(scope['indicator_fields'])} | exact_cache={bool(scope.get('save_exact_indicator_cache', False))} "
            f"| warmup={scope['warmup_minutes']} min | cache={scope['cache_dir']}",
            flush=True,
        )
    else:
        print(f"Warmup={scope['warmup_minutes']} min | cache={scope['cache_dir']}", flush=True)

    return runtime.run_action(
        action,
        scope,
        progress_cb=_print_progress,
        log_cb=_print_log,
    )


def main() -> int:
    parser = argparse.ArgumentParser(description="Run the existing downloader service against one or more saved presets.")
    parser.add_argument(
        "--action",
        required=True,
        choices=["candle_download_window", "indicator_build_window"],
        help="Downloader action to execute.",
    )
    parser.add_argument("--preset", dest="preset_name", default=None, help="Preset name to run.")
    parser.add_argument("--all-presets", action="store_true", help="Run the action for every saved preset.")
    parser.add_argument("--presets-file", default=str(REPO_ROOT / "app" / "presets.json"), help="Path to presets.json.")
    parser.add_argument("--cache-dir", dest="cache_dir", default=None, help="Optional cache directory override.")
    parser.add_argument("--start", dest="start_override", default=None, help="Optional start UTC override.")
    parser.add_argument("--end", dest="end_override", default=None, help="Optional end UTC override.")
    parser.add_argument(
        "--save-exact-indicator-cache",
        action="store_true",
        help="Persist exact-window indicator_streams files. Disabled by default to keep preset builds slim.",
    )
    parser.add_argument("--json-out", default=None, help="Optional path for a JSON summary.")
    args = parser.parse_args()

    presets_file = Path(args.presets_file).resolve()
    payload = _load_presets(presets_file)
    preset_names = _selected_preset_names(payload, args.preset_name, bool(args.all_presets))
    scopes = [
        _preset_scope(
            repo_root=REPO_ROOT,
            preset_name=name,
            preset=payload["presets"][name],
            cache_dir_override=args.cache_dir,
            start_override=args.start_override,
            end_override=args.end_override,
            save_exact_indicator_cache=bool(args.save_exact_indicator_cache),
        )
        for name in preset_names
    ]
    unique_scopes = _iter_unique_scopes(args.action, scopes)

    runtime = DownloaderRuntimeService(REPO_ROOT)
    results: List[Dict[str, Any]] = []
    for scope in unique_scopes:
        result = _run_action(runtime, args.action, scope)
        result["preset_name"] = scope["preset_name"]
        results.append(result)

    summary = {
        "action": args.action,
        "requested_presets": preset_names,
        "executed_profiles": len(unique_scopes),
        "results": results,
    }
    print(
        f"\nCompleted {args.action} for {len(results)} unique preset profile(s).",
        flush=True,
    )
    if args.json_out:
        out_path = Path(args.json_out).resolve()
        out_path.parent.mkdir(parents=True, exist_ok=True)
        out_path.write_text(json.dumps(summary, indent=2, ensure_ascii=False), encoding="utf-8")
        print(f"Saved JSON summary to {out_path}", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
