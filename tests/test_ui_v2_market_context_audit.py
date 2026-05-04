from __future__ import annotations

import json
from unittest.mock import patch

import pandas as pd

from app.research.market_context_audit import build_market_context_audit_window
from app.services.ui_v2_runtime import BacktestRuntimeService


def _sample_market_frame() -> pd.DataFrame:
    idx = pd.date_range("2026-01-01T00:00:00Z", periods=5, freq="5min", tz="UTC")
    return pd.DataFrame(
        {
            "open": [100.0, 100.2, 100.4, 100.5, 101.0],
            "high": [100.3, 100.6, 100.7, 101.1, 101.8],
            "low": [99.8, 100.0, 100.1, 100.3, 100.8],
            "close": [100.1, 100.5, 100.45, 101.0, 101.6],
            "volume": [10.0, 11.0, 9.0, 16.0, 18.0],
            "market_state": ["COMPRESSION", "COMPRESSION", "BULL_PULLBACK", "BULL_PULLBACK", "BULL_EXPANSION"],
            "market_bias": ["NEUTRAL", "NEUTRAL", "LONG", "LONG", "LONG"],
            "market_phase": ["COMPRESSION", "COMPRESSION", "PULLBACK", "PULLBACK", "EXPANSION"],
            "market_confidence": [44, 46, 58, 61, 74],
            "market_trend_state": ["FLAT", "FLAT", "UPTREND", "UPTREND", "UPTREND"],
            "market_structure_state": ["MIXED", "MIXED", "HH_HL", "HH_HL", "HH_HL"],
            "market_phase_state": ["BALANCE", "BALANCE", "PULLBACK", "PULLBACK", "EXPANSION"],
            "market_breakout_signal": ["NONE", "WATCH_LONG", "WATCH_LONG", "CONFIRM_LONG", "CONFIRM_LONG"],
            "market_volume_context": ["DRY", "WEAK", "WEAK", "SUPPORTIVE", "SUPPORTIVE"],
            "market_trend_age": [1, 2, 1, 2, 3],
            "market_phase_age": [1, 2, 1, 2, 1],
            "market_breakout_signal_age": [1, 1, 2, 1, 2],
            "market_protected_high": [100.6, 100.6, 100.7, 101.1, 101.8],
            "market_protected_low": [99.8, 100.0, 100.1, 100.3, 100.8],
            "market_structure_last_high": [100.3, 100.6, 100.7, 101.1, 101.8],
            "market_structure_last_low": [99.8, 100.0, 100.1, 100.3, 100.8],
            "market_structure_note": [
                "structure is mixed",
                "structure is mixed",
                "confirmed swing HH/HL structure is intact",
                "confirmed swing HH/HL structure is intact",
                "confirmed swing HH/HL structure is intact",
            ],
            "market_structure_basis": ["SWING", "SWING", "SWING", "SWING", "SWING"],
            "market_structure_pivot_bias": ["NEUTRAL", "NEUTRAL", "LONG", "LONG", "LONG"],
            "market_structure_pivot_count": [2, 3, 4, 5, 6],
            "market_structure_swing_hh": [False, False, True, True, True],
            "market_structure_swing_hl": [False, False, True, True, True],
            "market_structure_swing_lh": [False, False, False, False, False],
            "market_structure_swing_ll": [False, False, False, False, False],
            "market_boundary_high": [100.4, 100.6, 100.7, 101.1, 101.8],
            "market_boundary_low": [99.8, 100.0, 100.1, 100.3, 100.8],
            "market_context_note": [
                "range / balance context is active",
                "range / balance context is active",
                "counter-trend pause is holding",
                "counter-trend pause is holding",
                "price is extending with structure and follow-through",
            ],
            "market_context_reasons": [
                "no clean directional trend | structure is mixed",
                "no clean directional trend | structure is mixed | volume is dry versus recent bars",
                "confirmed HH/HL structure | counter-trend pause is holding",
                "confirmed HH/HL structure | counter-trend pause is holding | watch long",
                "confirmed HH/HL structure | price is extending with structure and follow-through | volume supports the current move",
            ],
            "market_pa_pattern": ["INSIDE", "INSIDE", "PULLBACK", "BREAKOUT", "IMPULSE"],
            "market_breakout_note": ["", "watching", "watching", "bull breakout is confirming", "bull breakout is confirming"],
            "range_filter_state": ["NEUTRAL", "NEUTRAL", "BUY", "BUY", "BUY"],
            "range_filter_phase": ["BALANCE", "BALANCE", "PULLBACK", "EXPANSION", "EXPANSION"],
            "range_filter_buy": [False, False, False, True, False],
            "range_filter_sell": [False, False, False, False, False],
            "frama_state": ["NEUTRAL", "NEUTRAL", "BUY", "BUY", "BUY"],
            "frama_break_up": [False, False, True, False, False],
            "frama_break_down": [False, False, False, False, False],
            "frama_mid_cross": [False, False, False, False, False],
            "vidya_state": ["NEUTRAL", "BUY", "BUY", "BUY", "BUY"],
            "ms": ["NEUTRAL", "GREEN", "GREEN", "GREEN", "GREEN"],
            "angle": ["FLAT", "GREEN", "GREEN", "GREEN", "GREEN"],
            "atr_angle": ["RED", "GREEN", "GREEN", "GREEN", "GREEN"],
        },
        index=idx,
    )


def test_build_market_context_audit_window_emits_rows_reasons_and_markers() -> None:
    payload = build_market_context_audit_window(_sample_market_frame(), timeframe="5m", max_bars=4)

    assert payload["timeframe"] == "5m"
    assert payload["bar_count"] == 4
    assert len(payload["rows"]) == 4
    assert payload["rows"][-1]["market_state"] == "BULL_EXPANSION"
    assert payload["rows"][-1]["market_context_reason_list"] == [
        "confirmed HH/HL structure",
        "price is extending with structure and follow-through",
        "volume supports the current move",
    ]
    assert payload["rows"][-1]["market_structure_pivot_count"] == 6
    assert any(marker["field"] == "market_phase_state" and marker["kind"] == "state_change" for marker in payload["markers"])
    assert not any(marker["field"] in {"range_filter_state", "frama_state", "vidya_state"} for marker in payload["markers"])
    assert not any(marker["field"] in {"range_filter_buy", "range_filter_sell", "frama_break_up", "frama_break_down", "frama_mid_cross"} for marker in payload["markers"])


def test_runtime_market_audit_returns_tail_window_and_timeframe_summaries(tmp_path) -> None:
    presets_path = tmp_path / "presets.json"
    presets_path.write_text(
        json.dumps(
            {
                "meta": {"last_preset": "demo"},
                "presets": {
                    "demo": {
                        "settings": {
                            "symbol": "BTCUSDT",
                            "timeframes": {"1m": False, "5m": True, "15m": False, "30m": False, "1h": True},
                            "end": "2026-01-01T01:00:00Z",
                        },
                        "tabs": {},
                    }
                },
            }
        ),
        encoding="utf-8",
    )

    service = BacktestRuntimeService(presets_path=presets_path, repo_root=tmp_path)
    frame_5m = _sample_market_frame()
    frame_1h = pd.DataFrame(
        {
            "open": [100.0, 101.0],
            "high": [101.0, 102.0],
            "low": [99.5, 100.4],
            "close": [100.8, 101.7],
            "volume": [40.0, 43.0],
            "market_trend_state": ["UPTREND", "UPTREND"],
            "market_structure_state": ["HH_HL", "HH_HL"],
            "market_phase_state": ["PULLBACK", "EXPANSION"],
            "market_breakout_signal": ["WATCH_LONG", "CONFIRM_LONG"],
            "market_volume_context": ["WEAK", "SUPPORTIVE"],
            "market_context_note": ["holding structure", "price is extending"],
        },
        index=pd.date_range("2026-01-01T00:00:00Z", periods=2, freq="1h", tz="UTC"),
    )

    captured: dict[str, object] = {}

    def _fake_load_stream_bundle(self, **kwargs):
        captured["required_fields"] = set(kwargs.get("required_fields") or [])
        captured["timeframes"] = list(kwargs.get("timeframes") or [])
        return pd.DataFrame(), {"5m": frame_5m.copy(), "1h": frame_1h.copy()}, True

    with patch.object(BacktestRuntimeService, "_load_stream_bundle", new=_fake_load_stream_bundle):
        payload = service.get_preset_market_audit("demo", timeframe="5m", bars=3)

    assert payload["result_kind"] == "market_context_audit"
    assert payload["preset_name"] == "demo"
    assert payload["audit_target_kind"] == "preset"
    assert payload["audit_target_name"] == "demo"
    assert payload["timeframe"] == "5m"
    assert payload["bar_count"] == 3
    assert len(payload["rows"]) == 3
    assert payload["audit_source_note"] == "prepared-cache"
    assert payload["analysis_lookback_minutes"] >= 180
    assert payload["latest_context_by_timeframe"]["1h"]["market_phase_state"] == "EXPANSION"
    assert "market_trend_state" in captured["required_fields"]
    assert "market_structure_last_high" in captured["required_fields"]
    assert captured["timeframes"] == ["5m", "1h"]


def test_runtime_live_market_audit_uses_settings_without_preset_binding(tmp_path) -> None:
    service = BacktestRuntimeService(presets_path=tmp_path / "presets.json", repo_root=tmp_path)
    frame_5m = _sample_market_frame()
    frame_15m = frame_5m.copy()
    frame_15m.index = pd.date_range("2026-01-01T00:00:00Z", periods=5, freq="15min", tz="UTC")

    captured: dict[str, object] = {}

    def _fake_load_stream_bundle(self, **kwargs):
        captured["required_fields"] = set(kwargs.get("required_fields") or [])
        captured["timeframes"] = list(kwargs.get("timeframes") or [])
        captured["ema_settings"] = dict(kwargs.get("ema_settings") or {})
        return pd.DataFrame(), {"5m": frame_5m.copy(), "15m": frame_15m.copy()}, False

    with patch.object(BacktestRuntimeService, "_load_stream_bundle", new=_fake_load_stream_bundle):
        payload = service.get_market_audit_from_settings(
            "live_market",
            {
                "symbol": "ETHUSDT",
                "timeframes": {"5m": True, "15m": True},
                "ema_indicators": {"defaults": {"ema_1_length": 9, "ema_2_length": 21}},
            },
            timeframe="5m",
            bars=4,
        )

    assert payload["result_kind"] == "market_context_audit"
    assert payload["preset_name"] is None
    assert payload["audit_target_kind"] == "live_context"
    assert payload["audit_target_name"] == "live_market"
    assert payload["symbol"] == "ETHUSDT"
    assert payload["bar_count"] == 4
    assert payload["audit_source_note"] == "fresh-streams"
    assert payload["analysis_warmup_minutes"] >= 0
    assert captured["timeframes"] == ["5m", "15m"]
    assert "market_boundary_high" in captured["required_fields"]


def test_runtime_market_audit_backfills_chart_ohlcv_from_raw_1m(tmp_path) -> None:
    service = BacktestRuntimeService(presets_path=tmp_path / "presets.json", repo_root=tmp_path)
    raw_idx = pd.date_range("2026-01-01T00:00:00Z", periods=10, freq="1min", tz="UTC")
    df_1m = pd.DataFrame(
        {
            "open_time": raw_idx,
            "close_time": raw_idx + pd.Timedelta(minutes=1) - pd.Timedelta(milliseconds=1),
            "open": [100.0, 100.2, 100.1, 100.4, 100.6, 100.7, 100.9, 101.2, 101.4, 101.5],
            "high": [100.3, 100.4, 100.5, 100.8, 100.9, 101.0, 101.3, 101.5, 101.7, 101.8],
            "low": [99.9, 100.0, 100.0, 100.2, 100.5, 100.6, 100.8, 101.0, 101.2, 101.4],
            "close": [100.2, 100.1, 100.4, 100.6, 100.7, 100.9, 101.2, 101.4, 101.5, 101.7],
            "volume": [10.0, 9.0, 11.0, 8.0, 12.0, 13.0, 15.0, 14.0, 16.0, 18.0],
        }
    )
    context_idx = pd.to_datetime(df_1m["close_time"].iloc[[4, 9]], utc=True)
    frame_5m = pd.DataFrame(
        {
            "market_state": ["BULL_PULLBACK", "BULL_EXPANSION"],
            "market_bias": ["LONG", "LONG"],
            "market_phase": ["PULLBACK", "EXPANSION"],
            "market_confidence": [61, 78],
            "market_trend_state": ["UPTREND", "UPTREND"],
            "market_structure_state": ["HH_HL", "HH_HL"],
            "market_phase_state": ["PULLBACK", "EXPANSION"],
            "market_breakout_signal": ["WATCH_LONG", "CONFIRM_LONG"],
            "market_volume_context": ["WEAK", "SUPPORTIVE"],
            "market_context_note": ["counter-trend pause is holding", "price is extending with structure and follow-through"],
            "market_context_reasons": [
                "confirmed HH/HL structure | counter-trend pause is holding",
                "confirmed HH/HL structure | price is extending with structure and follow-through | volume supports the current move",
            ],
        },
        index=context_idx,
    )

    def _fake_load_stream_bundle(self, **kwargs):
        return df_1m.copy(), {"5m": frame_5m.copy()}, False

    with patch.object(BacktestRuntimeService, "_load_stream_bundle", new=_fake_load_stream_bundle):
        payload = service.get_market_audit_from_settings(
            "live_market",
            {
                "symbol": "BTCUSDT",
                "timeframes": {"5m": True},
            },
            timeframe="5m",
            bars=20,
        )

    assert payload["bar_count"] == 2
    assert payload["rows"][0]["open"] == 100.0
    assert payload["rows"][0]["high"] == 100.9
    assert payload["rows"][0]["low"] == 99.9
    assert payload["rows"][0]["close"] == 100.7
    assert payload["rows"][0]["volume"] == 50.0
    assert payload["rows"][1]["open"] == 100.7
    assert payload["rows"][1]["high"] == 101.8
    assert payload["rows"][1]["low"] == 100.6
    assert payload["rows"][1]["close"] == 101.7
    assert payload["rows"][1]["volume"] == 76.0
    assert payload["rows"][1]["market_phase_state"] == "EXPANSION"
