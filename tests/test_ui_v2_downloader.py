from __future__ import annotations

import gzip
import json
import time
from pathlib import Path

import pandas as pd

from app.constants import INDICATOR_CACHE_VERSION
from app.indicators_streaming import (
    _indicator_cache_paths,
    _local_indicator_store_day_paths,
    simulate_multitf_indicators,
)
from app.services.ui_v2_downloader import (
    DownloaderJobStore,
    DownloaderRuntimeService,
    _cap_candle_window_end,
    _resolve_indicator_selection,
)
from app.data_binance import _local_1m_store_day_paths
from app import tick_manager as tm


def _write_tick_fixture(cache_dir: Path, symbol: str, day_utc: pd.Timestamp) -> None:
    path = Path(tm.day_path(str(cache_dir), symbol, day_utc))
    path.parent.mkdir(parents=True, exist_ok=True)
    tick = {"a": 1, "T": int(pd.Timestamp("2026-03-28 00:00:01", tz="UTC").value // 1_000_000), "p": "1.0", "q": "1.0", "m": False}
    with gzip.open(path, "wt", encoding="utf-8") as fh:
        fh.write(json.dumps(tick, separators=(",", ":")) + "\n")
    meta = {
        "symbol": symbol,
        "day_utc": day_utc.strftime("%Y-%m-%d"),
        "status": "FINAL",
        "rows": 1,
        "first_id": 1,
        "last_id": 1,
        "first_ts_ms": tick["T"],
        "last_ts_ms": tick["T"],
        "sha256": "abc123def456",
        "complete_start": True,
        "complete_end": True,
    }
    Path(tm.meta_path(str(path))).write_text(json.dumps(meta), encoding="utf-8")


def _write_truncated_tick_tmp_fixture(cache_dir: Path, symbol: str, day_utc: pd.Timestamp) -> str:
    path = tm.day_path(str(cache_dir), symbol, day_utc) + ".tmp"
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    rows = [
        {"a": 1, "T": int(pd.Timestamp("2026-03-28 00:00:01", tz="UTC").value // 1_000_000), "p": "1.0", "q": "1.0", "m": False},
        {"a": 2, "T": int(pd.Timestamp("2026-03-28 00:15:00", tz="UTC").value // 1_000_000), "p": "1.1", "q": "1.0", "m": False},
        {"a": 3, "T": int(pd.Timestamp("2026-03-28 12:00:00", tz="UTC").value // 1_000_000), "p": "1.2", "q": "1.0", "m": True},
    ]
    payload = "".join(json.dumps(item, separators=(",", ":")) + "\n" for item in rows).encode("utf-8")
    gz_bytes = gzip.compress(payload)
    Path(path).write_bytes(gz_bytes[:-8])
    return path


def _write_candle_fixture(cache_dir: Path, symbol: str, day_utc: pd.Timestamp) -> None:
    pq_path, csv_path = _local_1m_store_day_paths(str(cache_dir), symbol, "LAST", day_utc)
    frame = pd.DataFrame(
        {
            "open_time": [
                pd.Timestamp("2026-03-28 00:00:00", tz="UTC"),
                pd.Timestamp("2026-03-28 00:01:00", tz="UTC"),
            ],
            "close_time": [
                pd.Timestamp("2026-03-28 00:00:59", tz="UTC"),
                pd.Timestamp("2026-03-28 00:01:59", tz="UTC"),
            ],
            "open": [1.0, 2.0],
            "high": [1.5, 2.5],
            "low": [0.5, 1.5],
            "close": [1.2, 2.2],
            "volume": [10.0, 11.0],
        }
    )
    frame.to_csv(csv_path, index=False)
    assert not Path(pq_path).exists()


def _write_indicator_fixture(cache_dir: Path, symbol: str, start_utc: pd.Timestamp, end_utc: pd.Timestamp) -> None:
    _indicator_fields, _indicator_families, required_fields = _resolve_indicator_selection(
        None,
        ["core", "adx", "atr", "frama", "vidya", "range_filter", "market_aug"],
    )
    wide = pd.DataFrame(
        {
            "1m__price": [1.0, 2.0],
            "1m__macd": [0.1, 0.2],
            "1m__signal": [0.0, 0.1],
        },
        index=[
            pd.Timestamp("2026-03-28 00:00:00", tz="UTC"),
            pd.Timestamp("2026-03-28 00:01:00", tz="UTC"),
        ],
    )
    exact_pq, exact_meta = _indicator_cache_paths(
        str(cache_dir),
        symbol,
        start_utc,
        end_utc,
        ["1m"],
        "LAST",
        "TRADINGVIEW",
        "TRADINGVIEW",
        required_fields=required_fields,
    )
    Path(exact_pq).parent.mkdir(parents=True, exist_ok=True)
    wide.to_parquet(exact_pq)
    Path(exact_meta).write_text(
        json.dumps(
            {
                "report_version": INDICATOR_CACHE_VERSION,
                "created_at_utc": "2026-03-28T00:02:00Z",
                "start_utc": str(start_utc),
                "end_utc": str(end_utc),
                "timeframes": ["1m"],
                "required_families": ["core", "adx", "atr", "frama", "vidya", "range_filter", "market_aug"],
            }
        ),
        encoding="utf-8",
    )

    day_pq, day_meta = _local_indicator_store_day_paths(
        str(cache_dir),
        symbol,
        pd.Timestamp("2026-03-28", tz="UTC"),
        timeframes=["1m"],
        price_source="LAST",
        macd_impl="TRADINGVIEW",
        adx_impl="TRADINGVIEW",
        required_fields=required_fields,
        market_state_thresholds=None,
    )
    Path(day_pq).parent.mkdir(parents=True, exist_ok=True)
    wide.to_parquet(day_pq)
    Path(day_meta).write_text(
        json.dumps(
            {
                "report_version": INDICATOR_CACHE_VERSION,
                "created_at_utc": "2026-03-28T00:02:00Z",
                "required_families": ["core", "adx", "atr", "frama", "vidya", "range_filter", "market_aug"],
            }
        ),
        encoding="utf-8",
    )


def test_inspect_cache_reports_tick_candle_and_indicator_state(tmp_path: Path) -> None:
    cache_dir = tmp_path / "data_cache"
    cache_dir.mkdir(parents=True, exist_ok=True)
    symbol = "BTCUSDT"
    day_utc = pd.Timestamp("2026-03-28", tz="UTC")
    start_utc = pd.Timestamp("2026-03-28 00:00:00", tz="UTC")
    end_utc = pd.Timestamp("2026-03-28 00:01:00", tz="UTC")

    _write_tick_fixture(cache_dir, symbol, day_utc)
    _write_candle_fixture(cache_dir, symbol, day_utc)
    _write_indicator_fixture(cache_dir, symbol, start_utc, end_utc)

    service = DownloaderRuntimeService(repo_root=tmp_path)
    summary = service.inspect_cache(
        {
            "symbol": symbol,
            "cache_dir": str(cache_dir),
            "start": "2026-03-28 00:00:00",
            "end": "2026-03-28 00:01:00",
            "price_source": "LAST",
            "timeframes": ["1m"],
            "indicator_families": ["core", "adx", "atr", "frama", "vidya", "range_filter", "market_aug"],
        }
    )

    assert summary["tick"]["totals"]["days"] == 1
    assert summary["tick"]["rows"][0]["day"] == "2026-03-28"
    assert summary["tick"]["rows"][0]["sha256_prefix"] == "abc123def456"[:12]

    assert summary["candles"]["local_row_count"] == 2
    assert summary["candles"]["local_days"][0]["has_store"] is True
    assert summary["candles"]["missing_ranges"] == []

    assert summary["indicators"]["profile_hash"]
    assert summary["indicators"]["coverage_days"] == 1
    assert summary["indicators"]["exact_cache"]["exists"] is True


def test_inspect_cache_accepts_explicit_indicator_fields(tmp_path: Path) -> None:
    cache_dir = tmp_path / "data_cache"
    cache_dir.mkdir(parents=True, exist_ok=True)
    service = DownloaderRuntimeService(repo_root=tmp_path)

    summary = service.inspect_cache(
        {
            "symbol": "BTCUSDT",
            "cache_dir": str(cache_dir),
            "start": "2026-03-28 00:00:00",
            "end": "2026-03-28 00:05:00",
            "price_source": "LAST",
            "timeframes": ["1m"],
            "indicator_fields": ["angle"],
        }
    )

    assert summary["scope"]["indicator_fields"] == ["angle"]
    assert summary["scope"]["indicator_families"] == ["macd_lines", "macd_states"]
    assert summary["indicators"]["selected_fields"] == ["angle"]
    assert summary["indicators"]["required_families"] == ["macd_lines", "macd_states"]


def test_inspect_cache_defaults_without_market_aug(tmp_path: Path) -> None:
    cache_dir = tmp_path / "data_cache"
    cache_dir.mkdir(parents=True, exist_ok=True)
    service = DownloaderRuntimeService(repo_root=tmp_path)

    summary = service.inspect_cache(
        {
            "symbol": "BTCUSDT",
            "cache_dir": str(cache_dir),
            "start": "2026-03-28 00:00:00",
            "end": "2026-03-28 00:05:00",
            "price_source": "LAST",
            "timeframes": ["1m"],
        }
    )

    assert "market_aug" not in summary["scope"]["indicator_families"]
    assert "market_aug" not in summary["indicators"]["required_families"]


def test_inspect_cache_accepts_stoch_rsi_indicator_fields(tmp_path: Path) -> None:
    cache_dir = tmp_path / "data_cache"
    cache_dir.mkdir(parents=True, exist_ok=True)
    service = DownloaderRuntimeService(repo_root=tmp_path)

    summary = service.inspect_cache(
        {
            "symbol": "BTCUSDT",
            "cache_dir": str(cache_dir),
            "start": "2026-03-28 00:00:00",
            "end": "2026-03-28 00:05:00",
            "price_source": "LAST",
            "timeframes": ["1m"],
            "indicator_fields": ["stoch_rsi_kd"],
        }
    )

    assert summary["scope"]["indicator_fields"] == ["stoch_rsi_kd"]
    assert summary["scope"]["indicator_families"] == ["stoch_rsi"]
    assert summary["indicators"]["selected_fields"] == ["stoch_rsi_kd"]
    assert summary["indicators"]["required_families"] == ["stoch_rsi"]


def test_inspect_cache_accepts_json_encoded_ema_settings(tmp_path: Path) -> None:
    cache_dir = tmp_path / "data_cache"
    cache_dir.mkdir(parents=True, exist_ok=True)
    service = DownloaderRuntimeService(repo_root=tmp_path)

    summary = service.inspect_cache(
        {
            "symbol": "BTCUSDT",
            "cache_dir": str(cache_dir),
            "start": "2026-03-28 00:00:00",
            "end": "2026-03-28 00:05:00",
            "timeframes": ["1m"],
            "indicator_fields": ["ema_1", "ema_2", "ema_stack"],
            "ema_settings": json.dumps({"defaults": {"ema_1_length": 50, "ema_2_length": 200}}),
        }
    )

    assert summary["scope"]["indicator_fields"] == ["ema_1", "ema_2", "ema_stack"]
    assert summary["scope"]["indicator_families"] == ["ema", "ohlcv"]
    assert summary["scope"]["ema_settings"]["defaults"]["ema_1_length"] == 50
    assert summary["scope"]["ema_settings"]["defaults"]["ema_2_length"] == 200
    assert summary["indicators"]["required_families"] == ["ema", "ohlcv"]
    assert summary["indicators"]["ema_profile_signature"] == [["default", 50, 200], ["1m", 50, 200]]


def test_indicator_build_rejects_empty_explicit_indicator_fields(tmp_path: Path) -> None:
    service = DownloaderRuntimeService(repo_root=tmp_path)

    try:
        service._run_indicator_build(
            {
                "symbol": "BTCUSDT",
                "cache_dir": str(tmp_path / "data_cache"),
                "start": "2026-03-28 00:00:00",
                "end": "2026-03-28 00:05:00",
                "price_source": "LAST",
                "timeframes": ["1m"],
                "indicator_fields": [],
            },
            progress=lambda *_args, **_kwargs: None,
            log=lambda *_args, **_kwargs: None,
        )
        raise AssertionError("Expected empty explicit indicator selection to be rejected.")
    except ValueError as exc:
        assert "Select at least one indicator rule field" in str(exc)


def test_downloader_job_store_captures_logs_and_result() -> None:
    class _FakeRuntime:
        def run_action(self, action, params, *, progress_cb=None, log_cb=None, stop_evt=None):
            if log_cb is not None:
                log_cb(f"running {action}")
            if progress_cb is not None:
                progress_cb("Halfway", 50.0)
            return {"action": action, "params": params, "ok": True}

    store = DownloaderJobStore(_FakeRuntime())
    job = store.create_job("fake_action", {"hello": "world"})

    deadline = time.time() + 5.0
    current = store.get(job.job_id)
    while current is not None and current.state not in {"completed", "failed"} and time.time() < deadline:
        time.sleep(0.05)
        current = store.get(job.job_id)

    assert current is not None
    assert current.state == "completed"
    assert current.result == {"action": "fake_action", "params": {"hello": "world"}, "ok": True}
    assert current.logs == ["running fake_action"]
    assert current.progress_pct == 100.0


def test_salvage_variant_recovers_truncated_tmp_for_tail_resume(tmp_path: Path) -> None:
    cache_dir = tmp_path / "data_cache"
    cache_dir.mkdir(parents=True, exist_ok=True)
    symbol = "BTCUSDT"
    day_utc = pd.Timestamp("2026-03-28", tz="UTC")
    expected_path = tm.day_path(str(cache_dir), symbol, day_utc)
    meta_path = tm.meta_path(expected_path)
    tmp_path_variant = _write_truncated_tick_tmp_fixture(cache_dir, symbol, day_utc)

    meta = tm._salvage_variant_into_final(
        tmp_path_variant,
        expected_path,
        meta_path,
        symbol,
        day_utc,
        compute_hash=False,
    )

    assert meta is not None
    assert Path(expected_path).is_file()
    assert not Path(tmp_path_variant).exists()
    assert meta["status"] == "PARTIAL"
    assert meta["complete_start"] is True
    assert meta["complete_end"] is False
    assert int(meta["last_id"]) == 3

    report = tm._verify_day_file(expected_path, symbol, day_utc, compute_hash=False)
    assert report["status"] == "PARTIAL"
    assert report["rows"] == 3


def test_downloader_job_store_can_cancel_tick_job() -> None:
    class _CancellableRuntime:
        def run_action(self, action, params, *, progress_cb=None, log_cb=None, stop_evt=None):
            if progress_cb is not None:
                progress_cb("Running", 10.0)
            while stop_evt is not None and not stop_evt.is_set():
                time.sleep(0.02)
            return {"action": action, "cancelled": bool(stop_evt and stop_evt.is_set())}

    store = DownloaderJobStore(_CancellableRuntime())
    job = store.create_job("tick_download_range", {"symbol": "BTCUSDT"})
    time.sleep(0.05)
    current = store.cancel_job(job.job_id)

    deadline = time.time() + 5.0
    while current is not None and current.state not in {"cancelled", "failed", "completed"} and time.time() < deadline:
        time.sleep(0.05)
        current = store.get(job.job_id)

    assert current is not None
    assert current.stop_requested is True
    assert current.state == "cancelled"
    assert current.result == {"action": "tick_download_range", "cancelled": True}


def test_indicator_build_reserves_100_percent_for_final_summary(tmp_path: Path) -> None:
    import app.services.ui_v2_downloader as downloader_mod

    events = []
    source_df = pd.DataFrame(
        {
            "open_time": [pd.Timestamp("2026-03-28 00:00:00", tz="UTC")],
            "close_time": [pd.Timestamp("2026-03-28 00:00:59", tz="UTC")],
            "open": [1.0],
            "high": [1.2],
            "low": [0.9],
            "close": [1.1],
            "volume": [10.0],
        }
    )
    streams = {
        "1m": pd.DataFrame(
            {"1m__price": [1.1], "1m__macd": [0.1]},
            index=[pd.Timestamp("2026-03-28 00:00:00", tz="UTC")],
        )
    }

    original_fetch = downloader_mod.fetch_klines_1m_futures
    original_simulate = downloader_mod.simulate_multitf_indicators

    def _fake_fetch(symbol, start_utc, end_utc, cache_dir, progress_cb=None, price_source="LAST"):
        if progress_cb is not None:
            progress_cb("Loaded local candle store.", 100)
        return source_df

    def _fake_simulate(df_1m, timeframes, progress_cb=None, **kwargs):
        if progress_cb is not None:
            progress_cb("Calculating indicators (fast)...", 100)
        return streams

    class _TestService(DownloaderRuntimeService):
        def _summary_with_scope(self, scope, *, force_start=None, force_end=None):
            events.append(("summary", "called", None))
            return {"tick": {}, "candles": {}, "indicators": {}}

    try:
        downloader_mod.fetch_klines_1m_futures = _fake_fetch
        downloader_mod.simulate_multitf_indicators = _fake_simulate
        service = _TestService(repo_root=tmp_path)
        result = service._run_indicator_build(
            {
                "symbol": "BTCUSDT",
                "cache_dir": str(tmp_path / "data_cache"),
                "start": "2026-03-28 00:00:00",
                "end": "2026-03-28 00:01:00",
                "price_source": "LAST",
                "timeframes": ["1m"],
                "indicator_families": ["core", "adx"],
                "warmup_minutes": 0,
            },
            progress=lambda message, pct=None: events.append(("progress", message, pct)),
            log=lambda message: events.append(("log", message, None)),
        )
    finally:
        downloader_mod.fetch_klines_1m_futures = original_fetch
        downloader_mod.simulate_multitf_indicators = original_simulate

    summary_index = next(i for i, item in enumerate(events) if item[0] == "summary")
    progress_before_summary = [item for item in events[:summary_index] if item[0] == "progress"]

    assert progress_before_summary
    assert max(float(item[2] or 0.0) for item in progress_before_summary) < 100.0
    assert any(item[1] == "Refreshing indicator coverage summary..." for item in progress_before_summary)
    assert result["summary"] == {"tick": {}, "candles": {}, "indicators": {}}
    assert events[-1] == ("progress", "Indicator library build complete.", 100.0)


def test_cap_candle_window_end_clamps_future_requests() -> None:
    messages = []
    start_utc = pd.Timestamp("2026-03-31 15:00:00", tz="UTC")
    end_utc = pd.Timestamp("2026-03-31 20:00:00", tz="UTC")

    capped_end, clamped = _cap_candle_window_end(
        start_utc,
        end_utc,
        log=messages.append,
        now_utc=pd.Timestamp("2026-03-31 15:35:42", tz="UTC"),
    )

    assert clamped is True
    assert capped_end == pd.Timestamp("2026-03-31 15:34:00", tz="UTC")
    assert messages
    assert "last closed 1m candle" in messages[0]


def test_simulate_multitf_indicators_persists_exact_and_local_store(tmp_path: Path) -> None:
    cache_dir = tmp_path / "data_cache"
    cache_dir.mkdir(parents=True, exist_ok=True)
    symbol = "BTCUSDT"
    start_utc = pd.Timestamp("2026-03-28 00:00:00", tz="UTC")
    end_utc = pd.Timestamp("2026-03-28 00:02:00", tz="UTC")
    df = pd.DataFrame(
        {
            "open_time": [
                pd.Timestamp("2026-03-28 00:00:00", tz="UTC"),
                pd.Timestamp("2026-03-28 00:01:00", tz="UTC"),
                pd.Timestamp("2026-03-28 00:02:00", tz="UTC"),
            ],
            "close_time": [
                pd.Timestamp("2026-03-28 00:00:59", tz="UTC"),
                pd.Timestamp("2026-03-28 00:01:59", tz="UTC"),
                pd.Timestamp("2026-03-28 00:02:59", tz="UTC"),
            ],
            "open": [1.0, 1.1, 1.2],
            "high": [1.2, 1.3, 1.4],
            "low": [0.9, 1.0, 1.1],
            "close": [1.1, 1.2, 1.3],
            "volume": [10.0, 11.0, 12.0],
        }
    )

    streams = simulate_multitf_indicators(
        df,
        ["1m"],
        cache_dir=str(cache_dir),
        symbol=symbol,
        start_utc=start_utc,
        end_utc=end_utc,
        price_source="LAST",
        required_fields=None,
    )

    assert "1m" in streams
    exact_pq, exact_meta = _indicator_cache_paths(
        str(cache_dir),
        symbol,
        start_utc,
        end_utc,
        ["1m"],
        "LAST",
        "TRADINGVIEW",
        "TRADINGVIEW",
        required_fields=None,
    )
    day_pq, day_meta = _local_indicator_store_day_paths(
        str(cache_dir),
        symbol,
        pd.Timestamp("2026-03-28", tz="UTC"),
        timeframes=["1m"],
        price_source="LAST",
        macd_impl="TRADINGVIEW",
        adx_impl="TRADINGVIEW",
        required_fields=None,
        market_state_thresholds=None,
    )

    assert Path(exact_pq).is_file()
    assert Path(exact_meta).is_file()
    assert Path(day_pq).is_file()
    assert Path(day_meta).is_file()


def test_simulate_multitf_indicators_ema_profile_changes_cache_identity(tmp_path: Path) -> None:
    cache_dir = tmp_path / "data_cache"
    cache_dir.mkdir(parents=True, exist_ok=True)
    symbol = "BTCUSDT"
    start_utc = pd.Timestamp("2026-03-28 00:00:00", tz="UTC")
    end_utc = pd.Timestamp("2026-03-28 00:05:00", tz="UTC")
    df = pd.DataFrame(
        {
            "open_time": pd.date_range("2026-03-28 00:00:00", periods=6, freq="1min", tz="UTC"),
            "close_time": pd.date_range("2026-03-28 00:00:59", periods=6, freq="1min", tz="UTC"),
            "open": [1.0, 1.1, 1.2, 1.3, 1.4, 1.5],
            "high": [1.2, 1.3, 1.4, 1.5, 1.6, 1.7],
            "low": [0.9, 1.0, 1.1, 1.2, 1.3, 1.4],
            "close": [1.05, 1.15, 1.3, 1.4, 1.55, 1.7],
            "volume": [10.0, 11.0, 12.0, 13.0, 14.0, 15.0],
        }
    )
    req_fields = {"ema_1", "ema_2", "ema_stack", "ema_cross_up", "ema_cross_down"}
    ema_9_21 = {"defaults": {"ema_1_length": 9, "ema_2_length": 21}}
    ema_50_200 = {"defaults": {"ema_1_length": 50, "ema_2_length": 200}}

    exact_9_21, meta_9_21 = _indicator_cache_paths(
        str(cache_dir),
        symbol,
        start_utc,
        end_utc,
        ["1m"],
        "LAST",
        "TRADINGVIEW",
        "TRADINGVIEW",
        required_fields=req_fields,
        ema_settings=ema_9_21,
    )
    exact_50_200, _meta_50_200 = _indicator_cache_paths(
        str(cache_dir),
        symbol,
        start_utc,
        end_utc,
        ["1m"],
        "LAST",
        "TRADINGVIEW",
        "TRADINGVIEW",
        required_fields=req_fields,
        ema_settings=ema_50_200,
    )

    assert exact_9_21 != exact_50_200

    streams = simulate_multitf_indicators(
        df,
        ["1m"],
        cache_dir=str(cache_dir),
        symbol=symbol,
        start_utc=start_utc,
        end_utc=end_utc,
        price_source="LAST",
        required_fields=req_fields,
        ema_settings=ema_9_21,
    )

    assert "1m" in streams
    assert "ema_1" in streams["1m"].columns
    assert "ema_2" in streams["1m"].columns
    assert "ema_stack" in streams["1m"].columns
    assert "ema_cross_up" in streams["1m"].columns
    assert Path(exact_9_21).is_file()
    assert Path(meta_9_21).is_file()

    exact_meta = json.loads(Path(meta_9_21).read_text(encoding="utf-8"))
    assert exact_meta["ema_profile_signature"] == [["default", 9, 21], ["1m", 9, 21]]

    try:
        wide = pd.read_parquet(exact_9_21)
    except Exception:
        wide = pd.read_pickle(exact_9_21)
    assert "1m__ema_1" in wide.columns
    assert "1m__ema_2" in wide.columns
    assert "1m__ema_stack" in wide.columns
    assert "1m__ema_cross_up_i" in wide.columns

    day_pq, day_meta = _local_indicator_store_day_paths(
        str(cache_dir),
        symbol,
        pd.Timestamp("2026-03-28", tz="UTC"),
        timeframes=["1m"],
        price_source="LAST",
        macd_impl="TRADINGVIEW",
        adx_impl="TRADINGVIEW",
        required_fields=req_fields,
        ema_settings=ema_9_21,
        market_state_thresholds=None,
    )
    assert Path(day_pq).is_file()
    day_meta_payload = json.loads(Path(day_meta).read_text(encoding="utf-8"))
    assert day_meta_payload["ema_profile_signature"] == [["default", 9, 21], ["1m", 9, 21]]


def test_indicator_build_passes_ema_settings_and_auto_warmup(tmp_path: Path) -> None:
    import app.services.ui_v2_downloader as downloader_mod

    captured = {}
    source_df = pd.DataFrame(
        {
            "open_time": [pd.Timestamp("2026-03-28 00:00:00", tz="UTC")],
            "close_time": [pd.Timestamp("2026-03-28 00:00:59", tz="UTC")],
            "open": [1.0],
            "high": [1.2],
            "low": [0.9],
            "close": [1.1],
            "volume": [10.0],
        }
    )
    streams = {
        "1m": pd.DataFrame(
            {"price": [1.1], "ema_1": [1.1], "ema_2": [1.1], "ema_stack": ["NEUTRAL"]},
            index=[pd.Timestamp("2026-03-28 00:00:00", tz="UTC")],
        )
    }

    original_fetch = downloader_mod.fetch_klines_1m_futures
    original_simulate = downloader_mod.simulate_multitf_indicators

    def _fake_fetch(symbol, start_utc, end_utc, cache_dir, progress_cb=None, price_source="LAST"):
        captured["fetch_start_utc"] = start_utc
        return source_df

    def _fake_simulate(df_1m, timeframes, progress_cb=None, **kwargs):
        captured["simulate_kwargs"] = dict(kwargs)
        return streams

    class _TestService(DownloaderRuntimeService):
        def _summary_with_scope(self, scope, *, force_start=None, force_end=None):
            return {"tick": {}, "candles": {}, "indicators": {}}

    try:
        downloader_mod.fetch_klines_1m_futures = _fake_fetch
        downloader_mod.simulate_multitf_indicators = _fake_simulate
        service = _TestService(repo_root=tmp_path)
        result = service._run_indicator_build(
            {
                "symbol": "BTCUSDT",
                "cache_dir": str(tmp_path / "data_cache"),
                "start": "2026-03-28 00:00:00",
                "end": "2026-03-28 00:01:00",
                "price_source": "LAST",
                "timeframes": ["1m"],
                "indicator_fields": ["ema_1", "ema_2", "ema_stack"],
                "ema_settings": {"defaults": {"ema_1_length": 50, "ema_2_length": 200}},
                "warmup_minutes": 0,
            },
            progress=lambda *_args, **_kwargs: None,
            log=lambda *_args, **_kwargs: None,
        )
    finally:
        downloader_mod.fetch_klines_1m_futures = original_fetch
        downloader_mod.simulate_multitf_indicators = original_simulate

    assert result["requested_warmup_minutes"] == 0
    assert result["warmup_minutes"] >= 200
    assert captured["simulate_kwargs"]["ema_settings"]["defaults"]["ema_1_length"] == 50
    assert captured["simulate_kwargs"]["ema_settings"]["defaults"]["ema_2_length"] == 200
    assert result["ema_profile_signature"] == [["default", 50, 200], ["1m", 50, 200]]
