from __future__ import annotations

import argparse
import json
import os
import sys
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Dict, List

import pandas as pd

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from app.data_binance import fetch_klines_1m_futures
from app.indicators_streaming import simulate_multitf_indicators
from app.prepared_dataset import build_prepared_dataset, save_prepared_dataset_to_disk
from app.services.ui_v2_downloader import DownloaderRuntimeService


DEFAULT_TIMEFRAMES = ["1m", "5m", "15m", "30m", "1h"]


def repo_root() -> Path:
    return REPO_ROOT


def utc_stamp() -> str:
    return datetime.now(UTC).strftime("%H:%M:%S")


def emit_progress(message: str, pct: float | None = None) -> None:
    if pct is None:
        print(f"[{utc_stamp()}] PROGRESS {message}", flush=True)
    else:
        print(f"[{utc_stamp()}] PROGRESS {pct:6.2f}% {message}", flush=True)


def emit_log(message: str) -> None:
    print(f"[{utc_stamp()}] {message}", flush=True)


def parse_timeframes(raw: str) -> List[str]:
    parts = [part.strip() for part in str(raw or "").split(",")]
    out: List[str] = []
    for tf in parts:
        if tf and tf not in out:
            out.append(tf)
    return out or list(DEFAULT_TIMEFRAMES)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Populate the shared data_cache for the research profile and optionally save a prepared dataset."
    )
    parser.add_argument("--symbol", default="BTCUSDT")
    parser.add_argument("--start", required=True, help="UTC start, e.g. 2025-01-01 00:00:00+00:00")
    parser.add_argument("--end", required=True, help="UTC end, e.g. 2026-04-17 16:53:00+00:00")
    parser.add_argument("--price-source", default="LAST")
    parser.add_argument("--cache-dir", default="data_cache")
    parser.add_argument("--timeframes", default="1m,5m,15m,30m,1h")
    parser.add_argument("--warmup-minutes", type=int, default=600)
    parser.add_argument("--disable-parallel", action="store_true")
    parser.add_argument("--skip-prepared", action="store_true")
    parser.add_argument("--output-json", type=Path, default=None)
    return parser


def build_prepared_entry(
    *,
    root: Path,
    symbol: str,
    start: str,
    end: str,
    price_source: str,
    cache_dir: str,
    timeframes: List[str],
) -> str | None:
    cache_path = root / cache_dir
    start_ts = pd.to_datetime(start, utc=True)
    end_ts = pd.to_datetime(end, utc=True)

    emit_log("Reloading source candles for prepared-dataset save.")
    df_1m = fetch_klines_1m_futures(
        symbol,
        start_ts,
        end_ts,
        str(cache_path),
        progress_cb=emit_progress,
        price_source=price_source,
    )
    if df_1m.empty:
        raise ValueError("No candles returned while building prepared dataset.")

    emit_log("Loading indicator streams through the shared indicator cache.")
    streams_full = simulate_multitf_indicators(
        df_1m,
        timeframes,
        progress_cb=emit_progress,
        macd_impl="TRADINGVIEW",
        adx_impl="TRADINGVIEW",
        cache_dir=str(cache_path),
        symbol=symbol,
        start_utc=start_ts,
        end_utc=end_ts,
        price_source=price_source,
        required_fields=None,
        ema_settings=None,
        rsi_settings=None,
    )

    emit_log("Saving prepared dataset to shared prepared_datasets store.")
    entry = build_prepared_dataset(
        symbol=symbol,
        start=start_ts,
        end=end_ts,
        timeframes=timeframes,
        price_source=price_source,
        macd_impl="TRADINGVIEW",
        adx_impl="TRADINGVIEW",
        required_fields=None,
        market_state_thresholds=None,
        df_1m_full=df_1m,
        streams_full=streams_full,
        ema_settings=None,
        rsi_settings=None,
    )
    return save_prepared_dataset_to_disk(str(cache_path), entry)


def main() -> int:
    args = build_parser().parse_args()

    if args.disable_parallel:
        os.environ["BT_INDICATOR_DISABLE_PARALLEL"] = "1"

    root = repo_root()
    runtime = DownloaderRuntimeService(root)
    timeframes = parse_timeframes(args.timeframes)
    output_json = args.output_json
    if output_json is None:
        safe_end = (
            str(args.end)
            .replace(":", "")
            .replace(" ", "_")
            .replace("+00:00", "Z")
            .replace("-", "")
        )
        output_json = root / "runs" / "downloader" / f"research_cache_profile_{args.symbol}_{safe_end}.json"
    output_json = output_json.resolve()
    output_json.parent.mkdir(parents=True, exist_ok=True)

    params: Dict[str, Any] = {
        "symbol": str(args.symbol).strip().upper(),
        "cache_dir": str(args.cache_dir),
        "start": str(args.start),
        "end": str(args.end),
        "price_source": str(args.price_source).strip().upper(),
        "timeframes": timeframes,
        "warmup_minutes": int(args.warmup_minutes),
    }

    emit_log(
        f"Starting shared indicator build for {params['symbol']} "
        f"{params['start']} -> {params['end']} on {', '.join(timeframes)}."
    )
    result = runtime.run_action(
        "indicator_build_window",
        params,
        progress_cb=emit_progress,
        log_cb=emit_log,
    )

    prepared_path = None
    if not args.skip_prepared:
        prepared_path = build_prepared_entry(
            root=root,
            symbol=params["symbol"],
            start=str(result.get("window_start") or params["start"]),
            end=str(result.get("window_end") or params["end"]),
            price_source=params["price_source"],
            cache_dir=params["cache_dir"],
            timeframes=timeframes,
        )

    payload = {
        "request": params,
        "indicator_build": result,
        "prepared_dataset_path": prepared_path,
        "parallel_disabled": bool(args.disable_parallel),
    }
    output_json.write_text(json.dumps(payload, indent=2, ensure_ascii=False, default=str), encoding="utf-8")
    emit_log(f"Saved run payload to {output_json}")
    if prepared_path:
        emit_log(f"Prepared dataset saved at {prepared_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
