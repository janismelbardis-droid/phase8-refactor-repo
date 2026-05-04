from __future__ import annotations

from collections import deque
import json
import queue
import tempfile
import unittest
from pathlib import Path

import pandas as pd

from app.indicators_streaming import _StochRsiState, compute_live_stoch_rsi
from app.live_engine import LiveEngine
from app.services.ui_v2_runtime import _augment_streams_with_ema
from app.services.ui_v2_live import LiveRuntimeService, LiveSessionConfig
from app.taker_bias import compute_taker_bias_payload


class UiV2LiveTests(unittest.TestCase):
    def _write_presets(self, payload: dict) -> Path:
        tmpdir = tempfile.TemporaryDirectory()
        self.addCleanup(tmpdir.cleanup)
        path = Path(tmpdir.name) / "presets.json"
        path.write_text(json.dumps(payload), encoding="utf-8")
        return path

    def test_build_config_uses_web_live_forming_alias_and_ema_lengths(self) -> None:
        presets_path = self._write_presets(
            {
                "meta": {"autoload_last": False, "last_preset": "Demo"},
                "presets": {
                    "Demo": {
                        "settings": {
                            "symbol": "ethusdt",
                            "timeframes": {"5m": True, "1m": True},
                            "price_source": "LAST",
                            "macd_impl": "TRADINGVIEW",
                            "adx_impl": "TRADINGVIEW",
                            "live_forming": False,
                            "ema_indicators": {
                                "defaults": {
                                    "ema_1_length": 34,
                                    "ema_2_length": 55,
                                },
                                "overrides": {
                                    "1m": {"ema_1_length": 9, "ema_2_length": 21},
                                    "5m": {"ema_1_length": 21, "ema_2_length": 50},
                                },
                            },
                        }
                    }
                },
            }
        )
        service = LiveRuntimeService(presets_path=presets_path)

        config = service._build_config("Demo")

        self.assertEqual(config.preset_name, "Demo")
        self.assertEqual(config.symbol, "ETHUSDT")
        self.assertEqual(config.timeframes, ("1m", "5m", "15m", "30m", "1h"))
        self.assertFalse(config.live_use_forming)
        self.assertEqual(config.ema_1_length, 34)
        self.assertEqual(config.ema_2_length, 55)
        self.assertEqual(config.rsi_length, 14)
        self.assertEqual(config.rsi_smoothing, 3)
        self.assertIn(("1m", 9, 21), config.ema_by_timeframe)
        self.assertIn(("5m", 21, 50), config.ema_by_timeframe)
        self.assertIn(("15m", 34, 55), config.ema_by_timeframe)
        self.assertIn(("30m", 34, 55), config.ema_by_timeframe)
        self.assertIn(("1h", 34, 55), config.ema_by_timeframe)

    def test_build_config_from_inline_preset_uses_given_name_and_enabled_timeframes(self) -> None:
        service = LiveRuntimeService(presets_path=self._write_presets({"meta": {}, "presets": {}}))

        config = service._build_config_from_preset(
            "builder_preview",
            {
                "settings": {
                    "symbol": "btcusdt",
                    "timeframes": {"15m": True, "1m": True, "5m": False},
                    "ema_indicators": {
                        "defaults": {"ema_1_length": 9, "ema_2_length": 21},
                        "overrides": {"15m": {"ema_1_length": 50, "ema_2_length": 200}},
                    },
                }
            },
        )

        self.assertEqual(config.preset_name, "builder_preview")
        self.assertEqual(config.symbol, "BTCUSDT")
        self.assertEqual(config.timeframes, ("1m", "5m", "15m", "30m", "1h"))
        self.assertEqual(
            config.ema_by_timeframe,
            (("1m", 9, 21), ("5m", 9, 21), ("15m", 50, 200), ("30m", 9, 21), ("1h", 9, 21)),
        )
        self.assertEqual(
            config.rsi_by_timeframe,
            (("1m", 14, 3), ("5m", 14, 3), ("15m", 14, 3), ("30m", 14, 3), ("1h", 14, 3)),
        )

    def test_build_config_from_live_settings_is_not_preset_bound(self) -> None:
        service = LiveRuntimeService(presets_path=self._write_presets({"meta": {}, "presets": {}}))

        config = service._build_config_from_settings(
            "live_market",
            {
                "symbol": "solusdt",
                "timeframes": {"1m": True, "5m": True, "15m": True, "30m": True, "1h": True},
                "ema_indicators": {
                    "defaults": {"ema_1_length": 12, "ema_2_length": 48},
                    "overrides": {"5m": {"ema_1_length": 21, "ema_2_length": 55}},
                },
            },
        )

        self.assertEqual(config.preset_name, "")
        self.assertEqual(config.session_name, "live_market")
        self.assertEqual(config.session_kind, "live_context")
        self.assertEqual(config.symbol, "SOLUSDT")
        self.assertEqual(config.timeframes, ("1m", "5m", "15m", "30m", "1h"))
        self.assertEqual(config.ema_1_length, 12)
        self.assertEqual(config.ema_2_length, 48)
        self.assertIn(("5m", 21, 55), config.ema_by_timeframe)

        service._config = config
        service._rows_by_tf = {tf: service._blank_row(tf) for tf in config.timeframes}
        payload = service._build_payload_locked()
        self.assertIsNone(payload["preset_name"])
        self.assertEqual(payload["session_name"], "live_market")
        self.assertEqual(payload["session_kind"], "live_context")

    def test_build_config_uses_rsi_defaults_and_overrides(self) -> None:
        service = LiveRuntimeService(presets_path=self._write_presets({"meta": {}, "presets": {}}))

        config = service._build_config_from_settings(
            "live_market",
            {
                "symbol": "xrpusdt",
                "timeframes": {"1m": True, "5m": True},
                "rsi_indicators": {
                    "defaults": {"length": 21, "smoothing": 5},
                    "overrides": {"5m": {"length": 34, "smoothing": 8}},
                },
            },
        )

        self.assertEqual(config.rsi_length, 21)
        self.assertEqual(config.rsi_smoothing, 5)
        self.assertIn(("1m", 21, 5), config.rsi_by_timeframe)
        self.assertIn(("5m", 34, 8), config.rsi_by_timeframe)
        self.assertIn(("15m", 21, 5), config.rsi_by_timeframe)

    def test_build_payload_includes_taker_bias_prev_closed_and_windows(self) -> None:
        service = LiveRuntimeService(presets_path=self._write_presets({"meta": {}, "presets": {}}))
        config = service._build_config_from_settings(
            "live_market",
            {
                "symbol": "btcusdt",
                "timeframes": {"1m": True},
            },
        )
        service._config = config
        service._rows_by_tf = {tf: service._blank_row(tf) for tf in config.timeframes}
        service._rows_by_tf["1m"] = {
            "timeframe": "1m",
            "timestamp_utc": "1970-01-01 00:04:00+00:00",
            "values": {
                "bar_close_time_ms": 240_000,
                "taker_bias_state": "BUY",
                "taker_trade_count": 11,
                "taker_delta_pct": 20.0,
                "taker_buy_share_pct": 60.0,
                "taker_buy_volume": 6.0,
                "taker_sell_volume": 4.0,
                "taker_delta_volume": 2.0,
            },
        }

        def closed_payload(close_ms: int, volume: float, taker_buy_volume: float, trades: int) -> dict:
            return {
                "bar_close_time_ms": close_ms,
                **compute_taker_bias_payload(volume, taker_buy_volume, trades),
            }

        service._record_closed_payload("1m", closed_payload(60_000, 10.0, 7.0, 5))
        service._record_closed_payload("1m", closed_payload(120_000, 12.0, 6.0, 7))
        service._record_closed_payload("1m", closed_payload(180_000, 8.0, 2.0, 4))

        payload = service._build_payload_locked()
        row_1m = next(row for row in payload["rows"] if row["timeframe"] == "1m")
        detail = row_1m["taker_bias_detail"]

        self.assertEqual(detail["prev_closed"]["taker_bias_state"], "SELL")
        self.assertEqual(detail["prev_closed"]["bars_used"], 1)
        self.assertEqual(detail["prev_closed"]["requested_bars"], 1)
        self.assertEqual(detail["prev_closed"]["taker_trade_count"], 4)
        self.assertEqual(detail["closed_count"], 3)
        self.assertEqual(detail["windows"]["1"]["taker_bias_state"], "SELL")
        self.assertEqual(detail["windows"]["1"]["bars_used"], 1)
        self.assertEqual(detail["windows"]["1"]["requested_bars"], 1)
        self.assertEqual(detail["windows"]["3"]["bars_used"], 3)
        self.assertEqual(detail["windows"]["3"]["requested_bars"], 3)
        self.assertEqual(detail["windows"]["3"]["taker_trade_count"], 16)
        self.assertEqual(detail["windows"]["3"]["taker_bias_state"], "NEUTRAL")
        self.assertAlmostEqual(detail["windows"]["3"]["taker_buy_volume"], 15.0)
        self.assertAlmostEqual(detail["windows"]["3"]["taker_sell_volume"], 15.0)
        self.assertAlmostEqual(detail["windows"]["3"]["taker_delta_pct"], 0.0)
        self.assertEqual(detail["windows"]["10"]["bars_used"], 3)
        self.assertEqual(detail["windows"]["10"]["requested_bars"], 10)

    def test_live_engine_builds_ema_overlay_from_lengths(self) -> None:
        engine = LiveEngine(
            queue.Queue(),
            symbol="BTCUSDT",
            timeframes=["1m"],
            ema_1_length=2,
            ema_2_length=4,
        )

        payload = engine._build_ema_payload(
            [100.0, 101.0, 102.0, 103.0, 104.0]
        )

        self.assertIsNotNone(payload["ema_1"])
        self.assertIsNotNone(payload["ema_2"])
        self.assertEqual(payload["ema_stack"], "BUY")
        self.assertEqual(payload["ema_1_angle_state"], "UP")
        self.assertEqual(payload["ema_2_angle_state"], "UP")
        self.assertGreater(payload["ema_spread"], 0)
        self.assertIn(payload["ema_spread_trend"], {"EXPANDING", "CONTRACTING", "FLAT", None})
        self.assertIn(payload["ema_stack_pressure"], {"BULL_ACCELERATING", "BULL_DECELERATING", "BEAR_ACCELERATING", "BEAR_DECELERATING", "NEUTRAL", None})
        self.assertIsInstance(payload["ema_cross_up"], (bool, type(None)))
        self.assertIsInstance(payload["ema_cross_down"], (bool, type(None)))

    def test_live_engine_ema_spread_is_absolute_for_bearish_stack(self) -> None:
        engine = LiveEngine(
            queue.Queue(),
            symbol="BTCUSDT",
            timeframes=["1m"],
            ema_1_length=2,
            ema_2_length=4,
        )

        payload = engine._build_ema_payload(
            [104.0, 103.0, 102.0, 101.0, 100.0]
        )

        self.assertEqual(payload["ema_stack"], "SELL")
        self.assertGreater(payload["ema_spread"], 0)

    def test_live_engine_prefers_forming_close_for_close_based_series(self) -> None:
        engine = LiveEngine(
            queue.Queue(),
            symbol="BTCUSDT",
            timeframes=["1m"],
            live_use_forming=True,
            ema_1_length=2,
            ema_2_length=4,
        )

        closes = [100.0, 101.0, 102.0, 103.0]
        series = engine._full_closes_for_tf("1m", closes, 120.0, (121.0, 99.0, 104.0))

        self.assertEqual(series, [100.0, 101.0, 102.0, 103.0, 104.0])

        payload_from_forming = engine._build_ema_payload(series, tf="1m")
        payload_from_live_price = engine._build_ema_payload(closes + [120.0], tf="1m")

        self.assertEqual(payload_from_forming["ema_1"], engine._build_ema_payload(closes + [104.0], tf="1m")["ema_1"])
        self.assertNotEqual(payload_from_forming["ema_1"], payload_from_live_price["ema_1"])
        self.assertNotEqual(payload_from_forming["ema_2"], payload_from_live_price["ema_2"])

    def test_live_engine_falls_back_to_live_price_when_forming_close_missing(self) -> None:
        engine = LiveEngine(
            queue.Queue(),
            symbol="BTCUSDT",
            timeframes=["1m"],
            live_use_forming=True,
        )

        closes = [100.0, 101.0, 102.0]
        series = engine._full_closes_for_tf("1m", closes, 103.5, (None, None, None))

        self.assertEqual(series, [100.0, 101.0, 102.0, 103.5])

    def test_live_engine_builds_stoch_rsi_overlay(self) -> None:
        engine = LiveEngine(
            queue.Queue(),
            symbol="BTCUSDT",
            timeframes=["1m"],
        )

        closes = [100.0 + ((i % 7) - 3) * 0.8 + i * 0.12 for i in range(64)]
        payload = engine._build_stoch_rsi_payload(closes)

        self.assertIn("stoch_rsi_k", payload)
        self.assertIn("stoch_rsi_d", payload)
        self.assertIn("stoch_rsi_kd", payload)
        self.assertIsNotNone(payload["stoch_rsi_k"])
        self.assertIsNotNone(payload["stoch_rsi_d"])
        self.assertGreaterEqual(float(payload["stoch_rsi_k"]), 0.0)
        self.assertLessEqual(float(payload["stoch_rsi_k"]), 100.0)
        self.assertGreaterEqual(float(payload["stoch_rsi_d"]), 0.0)
        self.assertLessEqual(float(payload["stoch_rsi_d"]), 100.0)
        self.assertIn(payload["stoch_rsi_kd"], {"GREEN", "RED"})

    def test_live_engine_keeps_stoch_rsi_k_when_d_not_ready_yet(self) -> None:
        engine = LiveEngine(
            queue.Queue(),
            symbol="BTCUSDT",
            timeframes=["1m"],
        )

        payload = engine._build_stoch_rsi_payload([100.0 + float(i) for i in range(30)])

        self.assertIsNotNone(payload["stoch_rsi_k"])
        self.assertIsNone(payload["stoch_rsi_d"])
        self.assertIsNone(payload["stoch_rsi_kd"])

    def test_stoch_rsi_helper_matches_stateful_preview(self) -> None:
        closes = [100.0 + ((i % 7) - 3) * 0.8 + i * 0.12 for i in range(64)]
        state = _StochRsiState()
        for close in closes[:-1]:
            state.commit(close)

        helper = compute_live_stoch_rsi(closes)
        preview = state.preview(closes[-1])

        self.assertAlmostEqual(helper[0], preview[0], places=10)
        self.assertAlmostEqual(helper[1], preview[1], places=10)
        self.assertEqual(helper[2], preview[2])

    def test_live_engine_builds_rsi_overlay(self) -> None:
        engine = LiveEngine(
            queue.Queue(),
            symbol="BTCUSDT",
            timeframes=["1m"],
            rsi_length=10,
            rsi_smoothing=4,
        )

        closes = [100.0 + ((i % 5) - 2) * 0.7 + i * 0.18 for i in range(48)]
        payload = engine._build_rsi_payload(closes, tf="1m")

        self.assertIn("rsi", payload)
        self.assertIn("rsi_smooth", payload)
        self.assertIn("rsi_state", payload)
        self.assertIsNotNone(payload["rsi"])
        self.assertIsNotNone(payload["rsi_smooth"])
        self.assertIn(payload["rsi_state"], {"GREEN", "RED"})

    def test_live_engine_keeps_rsi_value_when_smoothing_not_ready_yet(self) -> None:
        engine = LiveEngine(
            queue.Queue(),
            symbol="BTCUSDT",
            timeframes=["1m"],
            rsi_length=2,
            rsi_smoothing=4,
        )

        payload = engine._build_rsi_payload([100.0, 101.0, 102.0, 103.0], tf="1m")

        self.assertIsNotNone(payload["rsi"])
        self.assertIsNone(payload["rsi_smooth"])
        self.assertIsNone(payload["rsi_state"])

    def test_live_engine_bootstrap_prefill_bars_cover_live_table_warmup(self) -> None:
        engine = LiveEngine(
            queue.Queue(),
            symbol="BTCUSDT",
            timeframes=["1m"],
            ema_1_length=55,
            ema_2_length=233,
            rsi_length=144,
            rsi_smoothing=21,
        )

        bars = engine._bootstrap_prefill_bars_for_tf("1m", 7000)

        self.assertGreaterEqual(bars, 400)
        self.assertGreaterEqual(bars, 233 + 16)
        self.assertGreaterEqual(bars, 144 + 21 + 8)
        self.assertLessEqual(bars, 7000)

    def test_live_engine_uses_timeframe_specific_ema_lengths(self) -> None:
        engine = LiveEngine(
            queue.Queue(),
            symbol="BTCUSDT",
            timeframes=["1m", "5m"],
            ema_1_length=2,
            ema_2_length=4,
            ema_lengths_by_tf={
                "5m": {"ema_1_length": 3, "ema_2_length": 6},
            },
        )

        payload_1m = engine._build_ema_payload([100.0, 101.0, 102.0, 103.0, 104.0], tf="1m")
        payload_5m = engine._build_ema_payload([100.0, 101.0, 102.0, 103.0, 104.0], tf="5m")

        self.assertNotEqual(payload_1m["ema_1"], payload_5m["ema_1"])
        self.assertNotEqual(payload_1m["ema_2"], payload_5m["ema_2"])

    def test_live_engine_seed_history_uses_prefill_ohlcv_when_ring_is_empty(self) -> None:
        q = queue.Queue()
        engine = LiveEngine(
            q,
            symbol="BTCUSDT",
            timeframes=["1m"],
        )
        with engine.lock:
            engine.live_price = 103.0
            engine.last_ws_tick_ts = 1.0
            engine.storage["1m"].closes = [100.2, 100.7, 101.1, 101.6]
            engine._prefill_bar_times["1m"] = {
                "open_ms": [0, 60_000, 120_000, 180_000],
                "close_ms": [59_999, 119_999, 179_999, 239_999],
            }
            engine._prefill_ohlcv_series["1m"] = [
                {"open": 100.0, "high": 100.4, "low": 99.8, "close": 100.2, "volume": 10.0},
                {"open": 100.2, "high": 100.8, "low": 100.1, "close": 100.7, "volume": 12.0},
                {"open": 100.7, "high": 101.2, "low": 100.5, "close": 101.1, "volume": 11.0},
                {"open": 101.1, "high": 101.8, "low": 101.0, "close": 101.6, "volume": 15.0},
            ]
            engine._closed_ohlcv_ring["1m"].clear()

        engine._emit_seed_closed_history()

        kind, tf, _live_price, _age_sec, payloads = q.get_nowait()
        self.assertEqual(kind, "live_closed_seed")
        self.assertEqual(tf, "1m")
        self.assertEqual(len(payloads), 4)
        self.assertEqual(payloads[0]["open"], 100.0)
        self.assertEqual(payloads[-1]["close"], 101.6)
        self.assertTrue(all(payload.get("high") is not None for payload in payloads))
        self.assertTrue(all(payload.get("low") is not None for payload in payloads))
        self.assertTrue(all(payload.get("volume") is not None for payload in payloads))

    def test_runtime_snapshot_augment_adds_ema_pair_metrics(self) -> None:
        frame = pd.DataFrame({"close": [100.0, 101.0, 102.0, 103.0, 104.0]})

        augmented = _augment_streams_with_ema(
            {"1m": frame, "5m": frame},
            {
                "ema_indicators": {
                    "defaults": {"ema_1_length": 2, "ema_2_length": 4},
                    "overrides": {"5m": {"ema_1_length": 3, "ema_2_length": 6}},
                }
            },
        )

        row = augmented["1m"].iloc[-1]
        row_5m = augmented["5m"].iloc[-1]
        self.assertIn("ema_1_angle_state", augmented["1m"].columns)
        self.assertIn("ema_spread_pct", augmented["1m"].columns)
        self.assertIn("ema_spread_delta", augmented["1m"].columns)
        self.assertIn("ema_spread_trend", augmented["1m"].columns)
        self.assertEqual(row["ema_stack"], "BUY")
        self.assertEqual(row["ema_1_angle_state"], "UP")
        self.assertEqual(row["ema_2_angle_state"], "UP")
        self.assertAlmostEqual(float(row["ema_spread"]), abs(float(row["ema_1"]) - float(row["ema_2"])), places=8)
        self.assertIn(row["ema_spread_trend"], ("EXPANDING", "CONTRACTING", "FLAT"))
        self.assertIn(row["ema_stack_pressure"], ("BULL_ACCELERATING", "BULL_DECELERATING", "BEAR_ACCELERATING", "BEAR_DECELERATING", "NEUTRAL"))
        self.assertNotEqual(float(row["ema_1"]), float(row_5m["ema_1"]))

    def test_live_runtime_seeds_frama_and_range_state_ages_from_closed_history(self) -> None:
        service = LiveRuntimeService(presets_path=self._write_presets({"meta": {}, "presets": {}}))

        service._record_closed_payload("1m", {
            "bar_close_time_ms": 60_000,
            "frama_state": "BUY",
            "range_filter_state": "SELL",
        })
        service._record_closed_payload("1m", {
            "bar_close_time_ms": 120_000,
            "frama_state": "BUY",
            "range_filter_state": "SELL",
        })
        service._record_closed_payload("1m", {
            "bar_close_time_ms": 180_000,
            "frama_state": "BUY",
            "range_filter_state": "BUY",
        })

        same_state = service._normalize_live_row("1m", {
            "bar_close_time_ms": 240_000,
            "frama_state": "BUY",
            "range_filter_state": "BUY",
        })
        flipped_state = service._normalize_live_row("1m", {
            "bar_close_time_ms": 240_000,
            "frama_state": "SELL",
            "range_filter_state": "SELL",
        })

        self.assertEqual(same_state["values"]["frama_state_age"], 2)
        self.assertEqual(same_state["values"]["range_filter_state_age"], 0)
        self.assertEqual(flipped_state["values"]["frama_state_age"], 0)
        self.assertEqual(flipped_state["values"]["range_filter_state_age"], 0)

    def test_live_runtime_range_display_state_respects_explicit_neutral_over_backfilled_sell_regime(self) -> None:
        service = LiveRuntimeService(presets_path=self._write_presets({"meta": {}, "presets": {}}))

        current_payload = {
            "bar_open_time_ms": 240_000,
            "bar_close_time_ms": 300_000,
            "range_filter_state": "NEUTRAL",
            "range_filter_phase": "SELL_STRONG",
            "range_filter_cond_ini": -1,
            "range_filter_sell": False,
        }
        service._normalize_live_row("1m", current_payload)

        service._record_closed_payload("1m", {
            "bar_open_time_ms": 180_000,
            "bar_close_time_ms": 240_000,
            "range_filter_state": "SELL",
            "range_filter_phase": "SELL_STRONG",
            "range_filter_cond_ini": -1,
            "range_filter_sell": True,
        })

        row = service._normalize_live_row("1m", current_payload)

        self.assertEqual(row["values"]["range_filter_display_state"], "NEUTRAL")
        self.assertEqual(row["values"]["range_filter_trigger_side"], "SELL")
        self.assertEqual(row["values"]["range_filter_trigger_age"], 0)
        self.assertEqual(row["values"]["range_filter_display_age"], 0)
        self.assertEqual(row["values"]["range_filter_sell_age"], 0)

    def test_live_runtime_range_trigger_age_does_not_infer_event_from_regime_only(self) -> None:
        service = LiveRuntimeService(presets_path=self._write_presets({"meta": {}, "presets": {}}))

        service._record_closed_payload("1m", {
            "bar_open_time_ms": 180_000,
            "bar_close_time_ms": 240_000,
            "range_filter_state": "SELL",
            "range_filter_phase": "SELL_WEAK",
            "range_filter_cond_ini": -1,
            "range_filter_sell": False,
        })

        row = service._normalize_live_row("1m", {
            "bar_open_time_ms": 240_000,
            "bar_close_time_ms": 300_000,
            "range_filter_state": "SELL",
            "range_filter_phase": "SELL_WEAK",
            "range_filter_cond_ini": -1,
            "range_filter_sell": False,
        })

        self.assertEqual(row["values"]["range_filter_display_state"], "SELL")
        self.assertIsNone(row["values"]["range_filter_display_age"])
        self.assertIsNone(row["values"]["range_filter_trigger_side"])
        self.assertIsNone(row["values"]["range_filter_trigger_age"])
        self.assertIsNone(row["values"]["range_filter_sell_age"])

    def test_live_runtime_partial_indicator_payload_keeps_last_valid_numeric_families(self) -> None:
        service = LiveRuntimeService(presets_path=self._write_presets({"meta": {}, "presets": {}}))
        service._rows_by_tf["1m"] = {
            "timeframe": "1m",
            "timestamp_utc": "1970-01-01 00:04:00+00:00",
            "values": {
                "ema_1": 100.0,
                "ema_2": 101.0,
                "ema_stack": "DOWN",
                "ema_spread": -1.0,
                "rsi": 45.5,
                "rsi_smooth": 44.0,
                "rsi_state": "RED",
                "stoch_rsi_k": 12.0,
                "stoch_rsi_d": 15.0,
                "stoch_rsi_kd": "RED",
                "macd": -10.0,
                "signal": -12.0,
                "ms": -2.0,
                "angle": "RED",
                "sig_angle": "RED",
                "ppo": "RED",
                "flip_age": 8,
                "adx": 23.0,
                "atr": 17.0,
                "di_plus": 18.0,
                "di_minus": 22.0,
                "di_spread": -4.0,
                "di_cross": "RED",
                "adx_angle": "RED",
                "atr_angle": "GREEN",
            },
        }

        row = service._normalize_live_row("1m", {
            "bar_open_time_ms": 240_000,
            "bar_close_time_ms": 300_000,
            "ema_1": 105.0,
            "ema_2": 103.0,
            "ema_stack": "UP",
            "ema_spread": 2.0,
        })

        self.assertEqual(row["values"]["ema_1"], 105.0)
        self.assertEqual(row["values"]["ema_2"], 103.0)
        self.assertEqual(row["values"]["ema_stack"], "UP")
        self.assertEqual(row["values"]["ema_spread"], 2.0)
        self.assertEqual(row["values"]["rsi"], 45.5)
        self.assertEqual(row["values"]["rsi_smooth"], 44.0)
        self.assertEqual(row["values"]["stoch_rsi_k"], 12.0)
        self.assertEqual(row["values"]["macd"], -10.0)
        self.assertEqual(row["values"]["signal"], -12.0)
        self.assertEqual(row["values"]["adx"], 23.0)
        self.assertEqual(row["values"]["atr"], 17.0)
        self.assertEqual(row["values"]["di_minus"], 22.0)

    def test_consume_queue_burst_emits_one_atomic_payload_for_multiple_timeframes(self) -> None:
        service = LiveRuntimeService(presets_path=self._write_presets({"meta": {}, "presets": {}}))
        service._config = LiveSessionConfig(
            preset_name="",
            session_name="live_market",
            session_kind="live_context",
            symbol="BTCUSDT",
            timeframes=("1m", "5m"),
            price_source="LAST",
            macd_impl="TRADINGVIEW",
            adx_impl="TRADINGVIEW",
            live_use_forming=True,
            ema_1_length=9,
            ema_2_length=21,
            ema_by_timeframe=(("1m", 9, 21), ("5m", 9, 21)),
            rsi_length=14,
            rsi_smoothing=3,
            rsi_by_timeframe=(("1m", 14, 3), ("5m", 14, 3)),
        )
        service._rows_by_tf = {"1m": service._blank_row("1m"), "5m": service._blank_row("5m")}
        service._latest_closed_by_tf = {"1m": {}, "5m": {}}
        service._closed_history_by_tf = {"1m": deque(maxlen=4096), "5m": deque(maxlen=4096)}
        service._range_event_last_bar_ms = {"1m": {"buy": None, "sell": None}, "5m": {"buy": None, "sell": None}}
        service._range_event_meta = {
            "1m": {"last_seen_bar_ms": None, "current_bar_buy_seen": False, "current_bar_sell_seen": False},
            "5m": {"last_seen_bar_ms": None, "current_bar_buy_seen": False, "current_bar_sell_seen": False},
        }
        service._frama_event_last_bar_ms = {
            "1m": {"break_up": None, "break_down": None, "mid_cross": None},
            "5m": {"break_up": None, "break_down": None, "mid_cross": None},
        }
        service._state_age_by_tf = {"1m": {}, "5m": {}}
        service._prefill_done = True
        service._status = "live"
        service._status_message = "Live websocket feed is active."
        service._current_token = 1

        class DummyEngine:
            running = True
            prefill_done = True

        emitted = []

        def _capture_emit(payload, *, force):
            emitted.append((dict(payload), bool(force)))

        service._emit = _capture_emit  # type: ignore[method-assign]

        q_obj = queue.Queue()
        q_obj.put(("live_status", "5m", 101.0, 0.2, {
            "bar_open_time_ms": 300_000,
            "bar_close_time_ms": 600_000,
            "ema_1": 102.0,
            "ema_2": 101.0,
            "ema_stack": "UP",
        }))

        first_item = ("live_status", "1m", 100.0, 0.1, {
            "bar_open_time_ms": 240_000,
            "bar_close_time_ms": 300_000,
            "ema_1": 99.0,
            "ema_2": 100.0,
            "ema_stack": "DOWN",
        })
        service._consume_queue_burst(1, q_obj, DummyEngine(), first_item)

        self.assertEqual(len(emitted), 1)
        payload, forced = emitted[0]
        self.assertFalse(forced)
        rows = {row["timeframe"]: row for row in payload["rows"]}
        self.assertEqual(rows["1m"]["values"]["ema_1"], 99.0)
        self.assertEqual(rows["5m"]["values"]["ema_1"], 102.0)

    def test_live_runtime_closed_seed_batch_preserves_recent_frama_events(self) -> None:
        service = LiveRuntimeService(presets_path=self._write_presets({"meta": {}, "presets": {}}))

        payloads = []
        tf_ms = 900_000
        for idx in range(400):
            open_ms = idx * tf_ms
            close_ms = open_ms + tf_ms - 1
            payloads.append({
                "bar_open_time_ms": open_ms,
                "bar_close_time_ms": close_ms,
                "frama_state": "BUY" if idx >= 395 else "NEUTRAL",
                "frama_break_up": idx == 395,
                "frama_break_down": False,
                "frama_mid_cross": idx == 394,
                "range_filter_state": "NEUTRAL",
            })

        service._record_closed_seed_batch("15m", payloads)

        row = service._normalize_live_row("15m", {
            "bar_open_time_ms": 399 * tf_ms,
            "bar_close_time_ms": (400 * tf_ms) - 1,
            "frama_state": "BUY",
            "frama_break_up": False,
            "frama_break_down": False,
            "frama_mid_cross": False,
        })

        self.assertEqual(len(service._closed_history_by_tf["15m"]), 400)
        self.assertEqual(row["values"]["frama_break_up_age"], 4)
        self.assertEqual(row["values"]["frama_trigger_side"], "BUY")
        self.assertEqual(row["values"]["frama_display_age"], 4)

    def test_live_runtime_frama_trigger_age_uses_latest_closed_bar_when_forming_bar_has_no_new_event(self) -> None:
        service = LiveRuntimeService(presets_path=self._write_presets({"meta": {}, "presets": {}}))

        service._record_closed_payload("1m", {
            "bar_open_time_ms": 60_000,
            "bar_close_time_ms": 119_999,
            "frama_state": "BUY",
            "frama_break_up": True,
            "frama_break_down": False,
            "frama_mid_cross": False,
        })
        service._record_closed_payload("1m", {
            "bar_open_time_ms": 120_000,
            "bar_close_time_ms": 179_999,
            "frama_state": "SELL",
            "frama_break_up": False,
            "frama_break_down": True,
            "frama_mid_cross": False,
        })

        row = service._normalize_live_row("1m", {
            "bar_open_time_ms": 180_000,
            "bar_close_time_ms": 239_999,
            "frama_state": "SELL",
            "frama_break_up": False,
            "frama_break_down": False,
            "frama_mid_cross": False,
        })

        self.assertEqual(row["values"]["frama_state_age"], 0)
        self.assertEqual(row["values"]["frama_break_down_age"], 0)
        self.assertEqual(row["values"]["frama_trigger_side"], "SELL")
        self.assertEqual(row["values"]["frama_display_age"], 0)

    def test_live_runtime_frama_display_age_follows_last_directional_trigger_even_when_state_is_neutral(self) -> None:
        service = LiveRuntimeService(presets_path=self._write_presets({"meta": {}, "presets": {}}))

        service._record_closed_payload("1m", {
            "bar_close_time_ms": 60_000,
            "frama_state": "BUY",
            "frama_break_up": True,
            "frama_break_down": False,
            "frama_mid_cross": False,
        })
        service._record_closed_payload("1m", {
            "bar_close_time_ms": 120_000,
            "frama_state": "BUY",
            "frama_break_up": False,
            "frama_break_down": False,
            "frama_mid_cross": False,
        })
        service._record_closed_payload("1m", {
            "bar_close_time_ms": 180_000,
            "frama_state": "NEUTRAL",
            "frama_break_up": False,
            "frama_break_down": False,
            "frama_mid_cross": True,
        })

        neutral_row = service._normalize_live_row("1m", {
            "bar_close_time_ms": 240_000,
            "frama_state": "NEUTRAL",
            "frama_break_up": False,
            "frama_break_down": False,
            "frama_mid_cross": False,
        })
        buy_row = service._normalize_live_row("1m", {
            "bar_close_time_ms": 240_000,
            "frama_state": "BUY",
            "frama_break_up": False,
            "frama_break_down": False,
            "frama_mid_cross": False,
        })

        self.assertEqual(neutral_row["values"]["frama_mid_cross_age"], 0)
        self.assertEqual(neutral_row["values"]["frama_trigger_side"], "BUY")
        self.assertEqual(neutral_row["values"]["frama_display_age"], 2)
        self.assertEqual(buy_row["values"]["frama_break_up_age"], 2)
        self.assertEqual(buy_row["values"]["frama_trigger_side"], "BUY")
        self.assertEqual(buy_row["values"]["frama_display_age"], 2)

    def test_live_runtime_frama_trigger_age_does_not_shift_to_forming_bar_when_live_status_repeats_last_closed_event(self) -> None:
        service = LiveRuntimeService(presets_path=self._write_presets({"meta": {}, "presets": {}}))

        service._record_closed_payload("15m", {
            "bar_open_time_ms": 0,
            "bar_close_time_ms": 899_999,
            "frama_state": "SELL",
            "frama_break_up": False,
            "frama_break_down": True,
            "frama_mid_cross": False,
        })

        row_after_trigger = service._normalize_live_row("15m", {
            "bar_open_time_ms": 900_000,
            "bar_close_time_ms": 1_799_999,
            "frama_state": "SELL",
            "frama_break_up": False,
            "frama_break_down": True,
            "frama_mid_cross": False,
        })

        service._record_closed_payload("15m", {
            "bar_open_time_ms": 900_000,
            "bar_close_time_ms": 1_799_999,
            "frama_state": "SELL",
            "frama_break_up": False,
            "frama_break_down": False,
            "frama_mid_cross": False,
        })

        row_next_bar = service._normalize_live_row("15m", {
            "bar_open_time_ms": 1_800_000,
            "bar_close_time_ms": 2_699_999,
            "frama_state": "SELL",
            "frama_break_up": False,
            "frama_break_down": False,
            "frama_mid_cross": False,
        })

        self.assertEqual(row_after_trigger["values"]["frama_break_down_age"], 0)
        self.assertEqual(row_after_trigger["values"]["frama_trigger_side"], "SELL")
        self.assertEqual(row_after_trigger["values"]["frama_display_age"], 0)
        self.assertEqual(row_next_bar["values"]["frama_break_down_age"], 1)
        self.assertEqual(row_next_bar["values"]["frama_trigger_side"], "SELL")
        self.assertEqual(row_next_bar["values"]["frama_display_age"], 1)

    def test_live_runtime_frama_trigger_age_tracks_latest_directional_break_inside_same_sell_run(self) -> None:
        service = LiveRuntimeService(presets_path=self._write_presets({"meta": {}, "presets": {}}))

        service._record_closed_payload("1m", {
            "bar_close_time_ms": 60_000,
            "frama_state": "NEUTRAL",
            "frama_break_up": False,
            "frama_break_down": False,
            "frama_mid_cross": True,
        })
        service._record_closed_payload("1m", {
            "bar_close_time_ms": 120_000,
            "frama_state": "SELL",
            "frama_break_up": False,
            "frama_break_down": True,
            "frama_mid_cross": False,
        })
        service._record_closed_payload("1m", {
            "bar_close_time_ms": 180_000,
            "frama_state": "SELL",
            "frama_break_up": False,
            "frama_break_down": False,
            "frama_mid_cross": False,
        })
        service._record_closed_payload("1m", {
            "bar_close_time_ms": 240_000,
            "frama_state": "SELL",
            "frama_break_up": False,
            "frama_break_down": True,
            "frama_mid_cross": False,
        })

        row = service._normalize_live_row("1m", {
            "bar_close_time_ms": 300_000,
            "frama_state": "SELL",
            "frama_break_up": False,
            "frama_break_down": False,
            "frama_mid_cross": False,
        })

        self.assertEqual(row["values"]["frama_state_age"], 2)
        self.assertEqual(row["values"]["frama_break_down_age"], 0)
        self.assertEqual(row["values"]["frama_trigger_side"], "SELL")
        self.assertEqual(row["values"]["frama_display_age"], 0)

    def test_live_runtime_market_audit_uses_cached_closed_history(self) -> None:
        service = LiveRuntimeService(presets_path=self._write_presets({"meta": {}, "presets": {}}))
        service._config = service._build_config_from_settings(
            "live_market",
            {
                "symbol": "btcusdt",
                "timeframes": {"1m": True, "5m": True},
            },
        )
        service._prefill_done = True

        service._record_closed_payload("1m", {
            "bar_open_time_ms": 0,
            "bar_close_time_ms": 60_000,
            "open": 100.0,
            "high": 101.0,
            "low": 99.5,
            "close": 100.5,
            "volume": 10.0,
            "market_state": "COMPRESSION",
            "market_trend_state": "FLAT",
            "market_structure_state": "MIXED",
            "market_phase_state": "BALANCE",
            "market_breakout_signal": "NONE",
            "market_volume_context": "DRY",
            "market_context_note": "range / balance context is active",
        })
        service._record_closed_payload("1m", {
            "bar_open_time_ms": 60_000,
            "bar_close_time_ms": 120_000,
            "open": 100.5,
            "high": 101.5,
            "low": 100.2,
            "close": 101.2,
            "volume": 14.0,
            "market_state": "BULL_EXPANSION",
            "market_trend_state": "UPTREND",
            "market_structure_state": "HH_HL",
            "market_phase_state": "EXPANSION",
            "market_breakout_signal": "CONFIRM_LONG",
            "market_volume_context": "SUPPORTIVE",
            "market_context_note": "price is extending with structure and follow-through",
        })
        service._record_closed_payload("5m", {
            "bar_open_time_ms": 0,
            "bar_close_time_ms": 300_000,
            "open": 100.0,
            "high": 101.8,
            "low": 99.5,
            "close": 101.2,
            "volume": 48.0,
            "market_state": "BULL_EXPANSION",
            "market_trend_state": "UPTREND",
            "market_structure_state": "HH_HL",
            "market_phase_state": "EXPANSION",
            "market_breakout_signal": "CONFIRM_LONG",
            "market_volume_context": "SUPPORTIVE",
            "market_context_note": "price is extending with structure and follow-through",
        })

        payload = service.get_market_audit_from_cache(timeframe="1m", bars=2, session_name="live_market")

        self.assertEqual(payload["audit_target_kind"], "live_context")
        self.assertEqual(payload["audit_target_name"], "live_market")
        self.assertEqual(payload["audit_source_note"], "live-cache")
        self.assertEqual(payload["symbol"], "BTCUSDT")
        self.assertEqual(payload["bar_count"], 2)
        self.assertEqual(payload["rows"][-1]["open"], 100.5)
        self.assertEqual(payload["rows"][-1]["close"], 101.2)
        self.assertEqual(payload["rows"][-1]["volume"], 14.0)
        self.assertEqual(payload["latest_context_by_timeframe"]["1m"]["market_phase_state"], "EXPANSION")
        self.assertEqual(payload["latest_context_by_timeframe"]["5m"]["market_structure_state"], "HH_HL")

    def test_live_runtime_marks_session_live_while_prefill_continues_if_rows_exist(self) -> None:
        service = LiveRuntimeService(presets_path=self._write_presets({"meta": {}, "presets": {}}))
        service._config = service._build_config_from_settings(
            "live_market",
            {
                "symbol": "btcusdt",
                "timeframes": {"1m": True, "5m": True},
            },
        )
        service._current_token = 7
        service._status = "prefilling"
        service._status_message = "Preloading indicator history for live verification..."
        service._rows_by_tf = {
            tf: service._blank_row(tf) for tf in service._config.timeframes
        }
        service._rows_by_tf["1m"] = service._normalize_live_row(
            "1m",
            {
                "price": 101.25,
                "macd": 0.4,
                "signal": 0.2,
                "ms": "GREEN",
                "bar_close_time_ms": 60_000,
            },
        )

        engine = type("EngineStub", (), {"prefill_done": False})()
        service._sync_status_from_engine(7, engine, force_emit=False)

        self.assertEqual(service._status, "live")
        self.assertIn("background", service._status_message.lower())
        payload = service.get_state()
        self.assertEqual(payload["live_phase"], "live")

    def test_live_runtime_market_audit_can_include_forming_bar(self) -> None:
        service = LiveRuntimeService(presets_path=self._write_presets({"meta": {}, "presets": {}}))
        service._config = LiveSessionConfig(
            preset_name="",
            session_name="live_market",
            session_kind="live_context",
            symbol="BTCUSDT",
            timeframes=("1m",),
            price_source="LAST",
            macd_impl="TRADINGVIEW",
            adx_impl="TRADINGVIEW",
            live_use_forming=True,
            ema_1_length=9,
            ema_2_length=21,
            ema_by_timeframe=(("1m", 9, 21),),
        )
        service._prefill_done = True
        service._record_closed_payload("1m", {
            "bar_open_time_ms": 0,
            "bar_close_time_ms": 60_000,
            "open": 100.0,
            "high": 101.0,
            "low": 99.5,
            "close": 100.5,
            "volume": 10.0,
            "market_state": "COMPRESSION",
            "market_trend_state": "FLAT",
            "market_structure_state": "MIXED",
            "market_phase_state": "BALANCE",
            "market_breakout_signal": "NONE",
            "market_volume_context": "DRY",
        })
        service._record_closed_payload("1m", {
            "bar_open_time_ms": 60_000,
            "bar_close_time_ms": 120_000,
            "open": 100.5,
            "high": 101.5,
            "low": 100.0,
            "close": 101.2,
            "volume": 14.0,
            "market_state": "BULL_EXPANSION",
            "market_trend_state": "UPTREND",
            "market_structure_state": "HH_HL",
            "market_phase_state": "EXPANSION",
            "market_breakout_signal": "CONFIRM_LONG",
            "market_volume_context": "SUPPORTIVE",
        })
        service._rows_by_tf["1m"] = service._normalize_live_row("1m", {
            "bar_open_time_ms": 120_000,
            "bar_close_time_ms": 180_000,
            "open": 101.2,
            "high": 102.1,
            "low": 101.0,
            "close": 101.9,
            "volume": 8.0,
            "market_state": "BULL_EXPANSION",
            "market_trend_state": "UPTREND",
            "market_structure_state": "HH_HL",
            "market_phase_state": "EXPANSION",
            "market_breakout_signal": "NONE",
            "market_volume_context": "SUPPORTIVE",
        })

        payload = service.get_market_audit_from_cache(timeframe="1m", bars=3, session_name="live_market")

        self.assertTrue(payload["includes_forming_bar"])
        self.assertEqual(payload["audit_source_note"], "live-cache+forming")
        self.assertEqual(payload["bar_count"], 3)
        self.assertEqual(payload["rows"][-1]["timestamp_utc"], "1970-01-01 00:03:00+00:00")
        self.assertEqual(payload["rows"][-1]["close"], 101.9)
        self.assertEqual(payload["ohlc_valid_bars"], 3)
        self.assertEqual(payload["ohlc_invalid_bars"], 0)

    def test_has_active_session_checks_running_engine_and_name(self) -> None:
        service = LiveRuntimeService(presets_path=self._write_presets({"meta": {}, "presets": {}}))
        service._config = LiveSessionConfig(
            preset_name="",
            session_name="live_market",
            session_kind="live_context",
            symbol="BTCUSDT",
            timeframes=("1m",),
            price_source="LAST",
            macd_impl="TRADINGVIEW",
            adx_impl="TRADINGVIEW",
            live_use_forming=True,
            ema_1_length=9,
            ema_2_length=21,
            ema_by_timeframe=(("1m", 9, 21),),
        )

        class DummyEngine:
            running = True

        service._engine = DummyEngine()

        self.assertTrue(service.has_active_session())
        self.assertTrue(service.has_active_session("live_market"))
        self.assertFalse(service.has_active_session("other_session"))

    def test_ensure_session_config_hot_updates_only_ema_rsi_without_restarting_runtime(self) -> None:
        service = LiveRuntimeService(presets_path=self._write_presets({"meta": {}, "presets": {}}))
        base_config = LiveSessionConfig(
            preset_name="",
            session_name="live_market",
            session_kind="live_context",
            symbol="BTCUSDT",
            timeframes=("1m", "5m"),
            price_source="LAST",
            macd_impl="TRADINGVIEW",
            adx_impl="TRADINGVIEW",
            live_use_forming=True,
            ema_1_length=9,
            ema_2_length=21,
            ema_by_timeframe=(("1m", 9, 21), ("5m", 21, 50)),
            rsi_length=14,
            rsi_smoothing=3,
            rsi_by_timeframe=(("1m", 14, 3), ("5m", 21, 5)),
        )
        next_config = LiveSessionConfig(
            preset_name="",
            session_name="live_market",
            session_kind="live_context",
            symbol="BTCUSDT",
            timeframes=("1m", "5m"),
            price_source="LAST",
            macd_impl="TRADINGVIEW",
            adx_impl="TRADINGVIEW",
            live_use_forming=True,
            ema_1_length=20,
            ema_2_length=55,
            ema_by_timeframe=(("1m", 20, 55), ("5m", 34, 89)),
            rsi_length=10,
            rsi_smoothing=4,
            rsi_by_timeframe=(("1m", 10, 4), ("5m", 12, 6)),
        )

        class DummyEngine:
            def __init__(self) -> None:
                self.running = True
                self.calls = []

            def update_indicator_settings(self, **kwargs) -> None:
                self.calls.append(dict(kwargs))

        engine = DummyEngine()
        service._config = base_config
        service._engine = engine
        service._rows_by_tf = {
            "1m": service._normalize_live_row("1m", {
                "bar_open_time_ms": 240_000,
                "bar_close_time_ms": 300_000,
                "ema_fast": 100.0,
                "ema_slow": 101.0,
                "rsi": 49.5,
                "rsi_smooth": 50.5,
                "frama_state": "BUY",
                "range_filter_state": "SELL",
            }),
            "5m": service._blank_row("5m"),
        }
        service._latest_closed_by_tf = {
            "1m": {
                "bar_open_time_ms": 180_000,
                "bar_close_time_ms": 240_000,
                "frama_state": "BUY",
                "range_filter_state": "SELL",
            },
            "5m": {},
        }
        service._closed_history_by_tf = {
            "1m": deque(
                [
                    {
                        "bar_open_time_ms": 180_000,
                        "bar_close_time_ms": 240_000,
                        "frama_state": "BUY",
                        "range_filter_state": "SELL",
                    }
                ],
                maxlen=4096,
            ),
            "5m": deque(maxlen=4096),
        }
        service._frama_event_last_bar_ms = {
            "1m": {"break_up": 240_000, "break_down": None, "mid_cross": None},
            "5m": {"break_up": None, "break_down": None, "mid_cross": None},
        }
        service._range_event_last_bar_ms = {
            "1m": {"buy": None, "sell": 240_000},
            "5m": {"buy": None, "sell": None},
        }
        service._range_event_meta = {
            "1m": {"last_seen_bar_ms": 240_000, "current_bar_buy_seen": False, "current_bar_sell_seen": True},
            "5m": {"last_seen_bar_ms": None, "current_bar_buy_seen": False, "current_bar_sell_seen": False},
        }
        service._state_age_by_tf = {
            "1m": {
                "frama_state": {"state": "BUY", "last_change_bar_ms": 180_000},
                "range_filter_state": {"state": "SELL", "last_change_bar_ms": 240_000},
            },
            "5m": {},
        }
        service._prefill_done = True
        service._status = "live"
        service._status_message = "Live websocket feed is active."

        frama_memory_before = dict(service._frama_event_last_bar_ms["1m"])
        range_memory_before = dict(service._range_event_last_bar_ms["1m"])
        history_before = list(service._closed_history_by_tf["1m"])

        def _unexpected_start(_config: LiveSessionConfig) -> None:
            raise AssertionError("EMA/RSI-only changes must not restart the live session.")

        service._start_new_session = _unexpected_start  # type: ignore[method-assign]

        payload = service._ensure_session_config(next_config)

        self.assertEqual(service._config, next_config)
        self.assertEqual(len(engine.calls), 1)
        self.assertEqual(engine.calls[0]["ema_1_length"], 20)
        self.assertEqual(engine.calls[0]["ema_2_length"], 55)
        self.assertEqual(engine.calls[0]["rsi_length"], 10)
        self.assertEqual(engine.calls[0]["rsi_smoothing"], 4)
        self.assertTrue(service._prefill_done)
        self.assertEqual(service._frama_event_last_bar_ms["1m"], frama_memory_before)
        self.assertEqual(service._range_event_last_bar_ms["1m"], range_memory_before)
        self.assertEqual(list(service._closed_history_by_tf["1m"]), history_before)
        self.assertEqual(payload["live_phase"], "live")
        self.assertTrue(any(row["timeframe"] == "1m" for row in payload["rows"]))


if __name__ == "__main__":
    unittest.main()
