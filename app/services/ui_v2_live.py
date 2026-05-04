from __future__ import annotations

import json
import math
import queue
import threading
import time
from collections import deque
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Deque, Dict, List, Optional, Tuple

import pandas as pd

from app.constants import ALL_TIMEFRAMES
from app.live_engine import LiveEngine
from app.research.market_context_audit import build_market_context_audit_window, build_timeframe_context_summary
from app.taker_bias import compute_taker_bias_payload
from app.utils_time import interval_to_ms

from .ui_v2_runtime import PRESETS_PATH, _load_presets, _pick_preset_name, _resolve_ema_settings, _resolve_rsi_settings, _serialize_snapshot_value


LiveListener = Callable[[Dict[str, Any]], None]
VERIFICATION_TIMEFRAMES: Tuple[str, ...] = ("1m", "5m", "15m", "30m", "1h")
LIVE_MARKET_AUDIT_HISTORY_MAX = 4096
LIVE_ROW_FAMILY_CARRY_SPECS: Tuple[Dict[str, Tuple[str, ...]], ...] = (
    {
        "anchors": ("ema_1", "ema_2", "ema_stack"),
        "fields": (
            "ema_1", "ema_2", "ema_stack", "ema_cross_up", "ema_cross_down",
            "ema_1_slope", "ema_2_slope", "ema_1_angle_state", "ema_2_angle_state",
            "ema_spread", "ema_spread_pct", "ema_spread_delta", "ema_spread_pct_delta",
            "ema_spread_trend", "ema_stack_pressure",
        ),
    },
    {
        "anchors": ("rsi", "rsi_smooth", "rsi_state"),
        "fields": ("rsi", "rsi_smooth", "rsi_state"),
    },
    {
        "anchors": ("stoch_rsi_k", "stoch_rsi_d", "stoch_rsi_kd"),
        "fields": ("stoch_rsi_k", "stoch_rsi_d", "stoch_rsi_kd"),
    },
    {
        "anchors": ("macd", "signal", "ms"),
        "fields": ("macd", "signal", "ms", "angle", "sig_angle", "ppo", "flip_age"),
    },
    {
        "anchors": ("adx", "atr", "di_plus", "di_minus"),
        "fields": ("adx", "atr", "di_plus", "di_minus", "di_spread", "di_cross", "adx_angle", "atr_angle"),
    },
    {
        "anchors": ("taker_bias_state", "taker_delta_pct", "taker_trade_count"),
        "fields": (
            "taker_buy_volume",
            "taker_sell_volume",
            "taker_delta_volume",
            "taker_delta_pct",
            "taker_buy_share_pct",
            "taker_trade_count",
            "taker_bias_state",
        ),
    },
)


@dataclass(frozen=True)
class LiveSessionConfig:
    preset_name: str
    session_name: str
    session_kind: str
    symbol: str
    timeframes: Tuple[str, ...]
    price_source: str
    macd_impl: str
    adx_impl: str
    live_use_forming: bool
    ema_1_length: int
    ema_2_length: int
    ema_by_timeframe: Tuple[Tuple[str, int, int], ...] = ()
    rsi_length: int = 14
    rsi_smoothing: int = 3
    rsi_by_timeframe: Tuple[Tuple[str, int, int], ...] = ()


def _ordered_timeframes(enabled_map: Dict[str, Any]) -> Tuple[str, ...]:
    enabled = [tf for tf, value in enabled_map.items() if bool(value)]
    if not enabled:
        return ("1m",)
    return tuple(sorted(set(enabled), key=lambda tf: ALL_TIMEFRAMES.index(tf) if tf in ALL_TIMEFRAMES else 999))


def _verification_timeframes(enabled_map: Dict[str, Any]) -> Tuple[str, ...]:
    enabled = _ordered_timeframes(enabled_map)
    filtered = tuple(tf for tf in enabled if tf in VERIFICATION_TIMEFRAMES)
    return filtered or ("1m",)


def _iso_from_ms(value: Any) -> Optional[str]:
    try:
        if value is None:
            return None
        return str(pd.to_datetime(int(value), unit="ms", utc=True))
    except Exception:
        return None


def _coerce_range_cond_ini(value: Any) -> Optional[int]:
    try:
        if value is None:
            return None
        return int(float(value))
    except Exception:
        return None


def _range_display_state_from_payload(payload: Dict[str, Any]) -> Optional[str]:
    state_text = str(payload.get("range_filter_state") or "").strip().upper()
    phase_text = str(payload.get("range_filter_phase") or "").strip().upper()
    cond_ini_i = _coerce_range_cond_ini(payload.get("range_filter_cond_ini"))
    if state_text in ("BUY", "SELL", "NEUTRAL"):
        return state_text
    if cond_ini_i == 1:
        return "BUY"
    if cond_ini_i == -1:
        return "SELL"
    if phase_text.startswith("BUY"):
        return "BUY"
    if phase_text.startswith("SELL"):
        return "SELL"
    return None


def _runtime_core_signature(config: LiveSessionConfig) -> Tuple[Any, ...]:
    return (
        str(config.session_name or "").strip(),
        str(config.session_kind or "").strip(),
        str(config.symbol or "").strip().upper(),
        tuple(str(tf) for tf in (config.timeframes or ())),
        str(config.price_source or "").strip().upper(),
        str(config.macd_impl or "").strip().upper(),
        str(config.adx_impl or "").strip().upper(),
        bool(config.live_use_forming),
    )


def _indicator_settings_signature(config: LiveSessionConfig) -> Tuple[Any, ...]:
    return (
        int(config.ema_1_length),
        int(config.ema_2_length),
        tuple((str(tf), int(fast), int(slow)) for tf, fast, slow in (config.ema_by_timeframe or ())),
        int(config.rsi_length),
        int(config.rsi_smoothing),
        tuple((str(tf), int(length), int(smoothing)) for tf, length, smoothing in (config.rsi_by_timeframe or ())),
    )


def _payload_bar_time_ms(payload: Dict[str, Any], *, override: Any = None) -> Optional[int]:
    candidates = [override, payload.get("bar_open_time_ms"), payload.get("bar_close_time_ms"), payload.get("bar_ms"), payload.get("snapshot_bar_ms")]
    for candidate in candidates:
        try:
            if candidate is not None:
                return int(candidate)
        except Exception:
            continue
    return None


def _live_value_present(value: Any) -> bool:
    if value is None:
        return False
    if isinstance(value, str):
        return bool(value.strip())
    return True


def _coerce_float(value: Any) -> Optional[float]:
    try:
        if value is None:
            return None
        num = float(value)
        if not math.isfinite(num):
            return None
        return num
    except Exception:
        return None


def _coerce_int(value: Any) -> Optional[int]:
    try:
        if value is None:
            return None
        return int(float(value))
    except Exception:
        return None


class LiveRuntimeService:
    def __init__(self, presets_path: Path = PRESETS_PATH) -> None:
        self.presets_path = Path(presets_path)
        self._lock = threading.RLock()
        self._listeners: Dict[int, LiveListener] = {}
        self._listener_seq = 0
        self._current_token = 0
        self._config: Optional[LiveSessionConfig] = None
        self._engine: Optional[LiveEngine] = None
        self._engine_queue: Optional["queue.Queue[Any]"] = None
        self._consumer_thread: Optional[threading.Thread] = None
        self._rows_by_tf: Dict[str, Dict[str, Any]] = {}
        self._latest_closed_by_tf: Dict[str, Dict[str, Any]] = {}
        self._closed_history_by_tf: Dict[str, Deque[Dict[str, Any]]] = {}
        self._range_event_close_ms: Dict[str, Dict[str, Optional[int]]] = {}
        self._range_event_last_bar_ms: Dict[str, Dict[str, Optional[int]]] = {}
        self._range_event_meta: Dict[str, Dict[str, Any]] = {}
        self._frama_event_last_bar_ms: Dict[str, Dict[str, Optional[int]]] = {}
        self._state_age_by_tf: Dict[str, Dict[str, Dict[str, Any]]] = {}
        self._prefill_done = False
        self._status = "idle"
        self._status_message = "Live engine is idle."
        self._last_price: Optional[float] = None
        self._last_age_sec: Optional[float] = None
        self._last_update_monotonic: Optional[float] = None
        self._last_payload: Optional[Dict[str, Any]] = None
        self._last_push_monotonic = 0.0
        self._push_min_interval_sec = 0.5
        self._recent_logs: Deque[str] = deque(maxlen=24)
        self._last_error: Optional[str] = None

    def add_listener(self, listener: LiveListener) -> int:
        with self._lock:
            self._listener_seq += 1
            listener_id = self._listener_seq
            self._listeners[listener_id] = listener
            payload = dict(self._last_payload) if isinstance(self._last_payload, dict) else self._build_payload_locked()
        try:
            listener(payload)
        except Exception:
            pass
        return listener_id

    def remove_listener(self, listener_id: int) -> None:
        with self._lock:
            self._listeners.pop(listener_id, None)

    def stop(self) -> None:
        with self._lock:
            engine = self._engine
            self._current_token += 1
            self._engine = None
            self._engine_queue = None
            self._consumer_thread = None
            self._config = None
            self._rows_by_tf = {}
            self._latest_closed_by_tf = {}
            self._closed_history_by_tf = {}
            self._range_event_close_ms = {}
            self._range_event_last_bar_ms = {}
            self._range_event_meta = {}
            self._frama_event_last_bar_ms = {}
            self._state_age_by_tf = {}
            self._prefill_done = False
            self._status = "idle"
            self._status_message = "Live engine stopped."
            self._last_payload = self._build_payload_locked()
            listeners = list(self._listeners.values())
        try:
            if engine is not None:
                engine.stop()
        except Exception:
            pass
        for listener in listeners:
            try:
                listener(self._last_payload or {})
            except Exception:
                pass

    def set_unavailable(self, message: str) -> Dict[str, Any]:
        with self._lock:
            engine = self._engine
            self._current_token += 1
            self._engine = None
            self._engine_queue = None
            self._consumer_thread = None
            self._config = None
            self._rows_by_tf = {}
            self._latest_closed_by_tf = {}
            self._closed_history_by_tf = {}
            self._range_event_close_ms = {}
            self._range_event_last_bar_ms = {}
            self._range_event_meta = {}
            self._frama_event_last_bar_ms = {}
            self._state_age_by_tf = {}
            self._prefill_done = False
            self._status = "idle"
            self._status_message = str(message or "Live engine is unavailable.")
            self._last_price = None
            self._last_age_sec = None
            self._last_update_monotonic = None
            self._last_error = self._status_message
            self._last_payload = self._build_payload_locked()
            payload = dict(self._last_payload)
            listeners = list(self._listeners.values())
        try:
            if engine is not None:
                engine.stop()
        except Exception:
            pass
        for listener in listeners:
            try:
                listener(payload)
            except Exception:
                pass
        return payload

    def ensure_session(self, preset_name: str) -> Dict[str, Any]:
        config = self._build_config(preset_name)
        return self._ensure_session_config(config)

    def ensure_session_from_preset(self, preset_name: str, preset: Dict[str, Any]) -> Dict[str, Any]:
        config = self._build_config_from_preset(preset_name, preset)
        return self._ensure_session_config(config)

    def ensure_session_from_settings(self, session_name: str, settings: Dict[str, Any]) -> Dict[str, Any]:
        config = self._build_config_from_settings(session_name, settings)
        return self._ensure_session_config(config)

    def has_active_session(self, session_name: Optional[str] = None) -> bool:
        with self._lock:
            config = self._config
            engine = self._engine
            if config is None or engine is None:
                return False
            if session_name and str(config.session_name or "").strip() != str(session_name or "").strip():
                return False
            try:
                return bool(engine.running)
            except Exception:
                return True

    def _ensure_session_config(self, config: LiveSessionConfig) -> Dict[str, Any]:
        hot_update_engine: Optional[LiveEngine] = None
        with self._lock:
            current = self._config
            engine = self._engine
            engine_running = False
            if engine is not None:
                try:
                    engine_running = bool(engine.running)
                except Exception:
                    engine_running = True
            if current == config and engine is not None:
                if engine_running:
                    return self._build_payload_locked()
            if (
                current is not None
                and engine is not None
                and engine_running
                and _runtime_core_signature(current) == _runtime_core_signature(config)
                and _indicator_settings_signature(current) != _indicator_settings_signature(config)
            ):
                hot_update_engine = engine

        if hot_update_engine is not None:
            try:
                hot_update_engine.update_indicator_settings(
                    ema_1_length=config.ema_1_length,
                    ema_2_length=config.ema_2_length,
                    ema_lengths_by_tf={
                        tf: {"ema_1_length": fast, "ema_2_length": slow}
                        for tf, fast, slow in config.ema_by_timeframe
                    },
                    rsi_length=config.rsi_length,
                    rsi_smoothing=config.rsi_smoothing,
                    rsi_settings_by_tf={
                        tf: {"length": length, "smoothing": smoothing}
                        for tf, length, smoothing in config.rsi_by_timeframe
                    },
                )
            except Exception:
                hot_update_engine = None
            else:
                with self._lock:
                    if self._engine is hot_update_engine:
                        self._config = config
                        self._last_payload = self._build_payload_locked()
                        payload = dict(self._last_payload)
                    else:
                        payload = None
                if payload is not None:
                    self._emit(payload, force=True)
                    return payload

        self._start_new_session(config)
        with self._lock:
            return self._build_payload_locked()

    def get_state(self) -> Dict[str, Any]:
        with self._lock:
            return self._build_payload_locked()

    def _build_config(self, preset_name: str) -> LiveSessionConfig:
        payload = _load_presets(self.presets_path)
        chosen_preset_name = _pick_preset_name(payload, preset_name)
        preset = payload["presets"][chosen_preset_name]
        return self._build_config_from_preset(chosen_preset_name, preset)

    def _build_config_from_preset(self, preset_name: str, preset: Dict[str, Any]) -> LiveSessionConfig:
        settings = preset.get("settings", {})
        return self._build_config_from_settings(
            str(preset_name or "builder_preview").strip() or "builder_preview",
            settings,
            session_kind="preset",
            preset_name=str(preset_name or "builder_preview").strip() or "builder_preview",
        )

    def _build_config_from_settings(
        self,
        session_name: str,
        settings: Dict[str, Any],
        *,
        session_kind: str = "live_context",
        preset_name: str = "",
    ) -> LiveSessionConfig:
        if not isinstance(settings, dict):
            raise ValueError("Preset settings payload is invalid.")
        symbol = str(settings.get("symbol", "BTCUSDT") or "BTCUSDT").strip().upper()
        timeframes = _verification_timeframes(settings.get("timeframes", {}) or {})
        price_source = str(settings.get("price_source", "LAST") or "LAST").upper()
        macd_impl = str(settings.get("macd_impl", "TRADINGVIEW") or "TRADINGVIEW").upper()
        adx_impl = str(settings.get("adx_impl", "TRADINGVIEW") or "TRADINGVIEW").upper()
        ema_settings = _resolve_ema_settings(settings)
        rsi_settings = _resolve_rsi_settings(settings)
        ema_by_timeframe = tuple(
            (tf, int(_resolve_ema_settings(settings, tf)["ema_1_length"]), int(_resolve_ema_settings(settings, tf)["ema_2_length"]))
            for tf in timeframes
        )
        rsi_by_timeframe = tuple(
            (tf, int(_resolve_rsi_settings(settings, tf)["length"]), int(_resolve_rsi_settings(settings, tf)["smoothing"]))
            for tf in timeframes
        )
        raw_live_forming = settings.get("live_use_forming", settings.get("live_forming", True))
        live_use_forming = bool(raw_live_forming)
        return LiveSessionConfig(
            preset_name=str(preset_name or "").strip(),
            session_name=str(session_name or "live_market").strip() or "live_market",
            session_kind=str(session_kind or "live_context").strip() or "live_context",
            symbol=symbol,
            timeframes=timeframes,
            price_source=price_source,
            macd_impl=macd_impl,
            adx_impl=adx_impl,
            live_use_forming=live_use_forming,
            ema_1_length=int(ema_settings["ema_1_length"]),
            ema_2_length=int(ema_settings["ema_2_length"]),
            ema_by_timeframe=ema_by_timeframe,
            rsi_length=int(rsi_settings["length"]),
            rsi_smoothing=int(rsi_settings["smoothing"]),
            rsi_by_timeframe=rsi_by_timeframe,
        )

    def _start_new_session(self, config: LiveSessionConfig) -> None:
        old_engine: Optional[LiveEngine]
        q_obj: "queue.Queue[Any]" = queue.Queue()
        with self._lock:
            old_engine = self._engine
            self._current_token += 1
            token = self._current_token
            self._config = config
            self._engine_queue = q_obj
            self._engine = None
            self._consumer_thread = None
            self._rows_by_tf = {tf: self._blank_row(tf) for tf in config.timeframes}
            self._latest_closed_by_tf = {tf: {} for tf in config.timeframes}
            self._closed_history_by_tf = {tf: deque(maxlen=LIVE_MARKET_AUDIT_HISTORY_MAX) for tf in config.timeframes}
            self._range_event_close_ms = {tf: {"buy": None, "sell": None} for tf in config.timeframes}
            self._range_event_last_bar_ms = {tf: {"buy": None, "sell": None} for tf in config.timeframes}
            self._range_event_meta = {
                tf: {"last_seen_bar_ms": None, "current_bar_buy_seen": False, "current_bar_sell_seen": False}
                for tf in config.timeframes
            }
            self._frama_event_last_bar_ms = {
                tf: {"break_up": None, "break_down": None, "mid_cross": None}
                for tf in config.timeframes
            }
            self._state_age_by_tf = {tf: {} for tf in config.timeframes}
            self._prefill_done = False
            self._status = "starting"
            self._status_message = "Starting live engine and preloading indicator history..."
            self._last_price = None
            self._last_age_sec = None
            self._last_update_monotonic = None
            self._recent_logs.clear()
            self._last_error = None
            self._last_payload = self._build_payload_locked()
            listeners = list(self._listeners.values())

        try:
            if old_engine is not None:
                old_engine.stop()
        except Exception:
            pass

        try:
            engine = LiveEngine(
                q_obj,
                symbol=config.symbol,
                timeframes=list(config.timeframes),
                price_source=config.price_source,
                macd_impl=config.macd_impl,
                adx_impl=config.adx_impl,
                live_use_forming=config.live_use_forming,
                ema_1_length=config.ema_1_length,
                ema_2_length=config.ema_2_length,
                ema_lengths_by_tf={tf: {"ema_1_length": fast, "ema_2_length": slow} for tf, fast, slow in config.ema_by_timeframe},
                rsi_length=config.rsi_length,
                rsi_smoothing=config.rsi_smoothing,
                rsi_settings_by_tf={tf: {"length": length, "smoothing": smoothing} for tf, length, smoothing in config.rsi_by_timeframe},
            )
            engine.start()
        except Exception as exc:
            with self._lock:
                self._engine = None
                self._engine_queue = None
                self._status = "error"
                self._last_error = str(exc)
                self._status_message = f"Live start failed: {exc}"
                self._last_payload = self._build_payload_locked()
                error_payload = dict(self._last_payload)
                listeners = list(self._listeners.values())
            for listener in listeners:
                try:
                    listener(error_payload)
                except Exception:
                    pass
            return

        thread = threading.Thread(
            target=self._consume_loop,
            args=(token, q_obj, engine),
            name=f"ui-v2-live-{config.symbol.lower()}",
            daemon=True,
        )
        with self._lock:
            self._engine = engine
            self._consumer_thread = thread
        thread.start()

        for listener in listeners:
            try:
                listener(dict(self._last_payload or {}))
            except Exception:
                pass

    def _consume_loop(self, token: int, q_obj: "queue.Queue[Any]", engine: LiveEngine) -> None:
        while True:
            with self._lock:
                if token != self._current_token or self._engine_queue is not q_obj:
                    return
            try:
                item = q_obj.get(timeout=0.5)
            except queue.Empty:
                self._sync_status_from_engine(token, engine, force_emit=False)
                continue
            self._consume_queue_burst(token, q_obj, engine, item)

    def _consume_queue_burst(self, token: int, q_obj: "queue.Queue[Any]", engine: LiveEngine, first_item: Any) -> None:
        should_emit = False
        items = [first_item]
        while True:
            try:
                items.append(q_obj.get_nowait())
            except queue.Empty:
                break
        for item in items:
            should_emit = self._handle_queue_item(token, engine, item) or should_emit
        self._sync_status_from_engine(token, engine, force_emit=should_emit)

    def _handle_queue_item(self, token: int, engine: LiveEngine, item: Any) -> bool:
        try:
            kind = item[0]
        except Exception:
            return False

        if kind == "live_log":
            message = str(item[1] if len(item) > 1 else "").strip()
            if message:
                with self._lock:
                    if token != self._current_token:
                        return False
                    self._recent_logs.append(message)
                    if "prefill" in message.lower():
                        self._status_message = message
                        return True
            return False

        if kind == "live_closed_seed":
            if len(item) >= 5:
                _, tf, _, _, payloads = item
                if isinstance(payloads, list):
                    self._record_closed_seed_batch(tf, payloads)
            return False

        if kind == "live_closed":
            if len(item) >= 5:
                _, tf, _, _, payload = item
                self._record_closed_payload(tf, payload)
            return False

        if kind == "live_status":
            if len(item) < 5:
                return False
            _, tf, price, age_sec, payload = item
            with self._lock:
                if token != self._current_token:
                    return False
                try:
                    self._last_price = float(price)
                except Exception:
                    self._last_price = None
                try:
                    self._last_age_sec = float(age_sec)
                except Exception:
                    self._last_age_sec = None
                self._last_update_monotonic = time.monotonic()
                if payload is not None:
                    self._rows_by_tf[str(tf)] = self._normalize_live_row(str(tf), payload)
                    if self._prefill_done:
                        self._status = "live"
                        self._status_message = "Live websocket feed is active."
                    else:
                        self._status = "live"
                        self._status_message = "Live websocket feed is active. Preloading indicator history in the background..."
                else:
                    if not self._prefill_done:
                        self._status = "prefilling"
                        self._status_message = "Preloading indicator history for live verification..."
            return True

        return False

    def _sync_status_from_engine(self, token: int, engine: LiveEngine, *, force_emit: bool) -> None:
        with self._lock:
            if token != self._current_token:
                return
            try:
                engine_prefill_done = bool(engine.prefill_done)
            except Exception:
                engine_prefill_done = self._prefill_done

            status_changed = False
            if engine_prefill_done and not self._prefill_done:
                self._prefill_done = True
                status_changed = True

            if self._status == "error":
                payload = None
            elif self._prefill_done:
                age = None
                if self._last_update_monotonic is not None:
                    age = time.monotonic() - self._last_update_monotonic
                if age is not None and age > 10.0:
                    next_status = "stale"
                    next_message = "Live feed is stale. Waiting for the next update..."
                elif any(self._rows_by_tf.get(tf, {}).get("values") for tf in (self._config.timeframes if self._config else ())):
                    next_status = "live"
                    next_message = "Live websocket feed is active."
                else:
                    next_status = "warming"
                    next_message = "Prefill finished. Waiting for live indicator payloads..."
                if next_status != self._status or next_message != self._status_message:
                    self._status = next_status
                    self._status_message = next_message
                    status_changed = True
            else:
                has_live_rows = any(self._rows_by_tf.get(tf, {}).get("values") for tf in (self._config.timeframes if self._config else ()))
                if has_live_rows:
                    next_status = "live"
                    next_message = "Live websocket feed is active. Preloading indicator history in the background..."
                else:
                    next_status = "prefilling"
                    next_message = "Preloading indicator history for live verification..."
                if self._status != next_status or self._status_message != next_message:
                    self._status = next_status
                    self._status_message = next_message
                    status_changed = True

            if status_changed or force_emit:
                self._last_payload = self._build_payload_locked()
                payload = dict(self._last_payload)
            else:
                payload = None

        if payload is not None:
            self._emit(payload, force=status_changed)

    def _record_closed_payload(self, tf: Any, payload: Any) -> None:
        if not isinstance(payload, dict):
            return
        timeframe = str(tf)
        normalized_payload = {key: _serialize_snapshot_value(value) for key, value in payload.items()}
        has_price_fields = any(key in normalized_payload for key in ("open", "high", "low", "close"))
        if has_price_fields and not self._audit_row_has_valid_ohlc(normalized_payload):
            return
        close_ms_raw = normalized_payload.get("bar_close_time_ms")
        try:
            close_ms = int(close_ms_raw) if close_ms_raw is not None else None
        except Exception:
            close_ms = None
        with self._lock:
            if timeframe not in self._latest_closed_by_tf:
                self._latest_closed_by_tf[timeframe] = {}
            self._latest_closed_by_tf[timeframe] = dict(normalized_payload)
            self._record_closed_history_locked(timeframe, normalized_payload, close_ms=close_ms)
            self._update_frama_event_memory(timeframe, normalized_payload)
            self._update_range_event_memory(timeframe, normalized_payload)
            if close_ms is not None:
                self._update_state_age(timeframe, close_ms, "frama_state", normalized_payload.get("frama_state"))
                self._update_state_age(timeframe, close_ms, "range_filter_state", normalized_payload.get("range_filter_state"))

    def _record_closed_seed_batch(self, tf: Any, payloads: Any) -> None:
        if not isinstance(payloads, list):
            return
        timeframe = str(tf)
        rows: List[Tuple[Optional[int], Dict[str, Any]]] = []
        for payload in payloads:
            if not isinstance(payload, dict):
                continue
            normalized_payload = dict(payload)
            has_price_fields = any(key in normalized_payload for key in ("open", "high", "low", "close"))
            if has_price_fields and not self._audit_row_has_valid_ohlc(normalized_payload):
                continue
            close_ms_raw = normalized_payload.get("bar_close_time_ms")
            try:
                close_ms = int(close_ms_raw) if close_ms_raw is not None else None
            except Exception:
                close_ms = None
            if close_ms is not None:
                normalized_payload["bar_close_time_ms"] = int(close_ms)
                normalized_payload["timestamp_utc"] = _iso_from_ms(close_ms)
            rows.append((close_ms, normalized_payload))
        if not rows:
            return

        rows.sort(key=lambda item: (int(item[0]) if item[0] is not None else -1))

        with self._lock:
            if timeframe not in self._latest_closed_by_tf:
                self._latest_closed_by_tf[timeframe] = {}
            self._latest_closed_by_tf[timeframe] = dict(rows[-1][1])

            existing_rows = list(self._closed_history_by_tf.get(timeframe, ()))
            if not existing_rows:
                merged_rows = [dict(payload) for _, payload in rows]
            else:
                merged_by_close: Dict[Tuple[str, int], Dict[str, Any]] = {}
                for index, row in enumerate(existing_rows):
                    try:
                        row_close_ms = int(row.get("bar_close_time_ms")) if row.get("bar_close_time_ms") is not None else None
                    except Exception:
                        row_close_ms = None
                    if row_close_ms is None:
                        merged_by_close[("idx", index)] = dict(row)
                    else:
                        merged_by_close[("close", int(row_close_ms))] = dict(row)
                for index, (close_ms, payload) in enumerate(rows):
                    if close_ms is None:
                        merged_by_close[("seed", index)] = dict(payload)
                    else:
                        merged_by_close[("close", int(close_ms))] = dict(payload)
                merged_rows = list(merged_by_close.values())
                merged_rows.sort(
                    key=lambda item: (
                        int(item.get("bar_close_time_ms")) if item.get("bar_close_time_ms") is not None else -1
                    )
                )

            self._closed_history_by_tf[timeframe] = deque(
                merged_rows[-LIVE_MARKET_AUDIT_HISTORY_MAX:],
                maxlen=LIVE_MARKET_AUDIT_HISTORY_MAX,
            )

            for close_ms, normalized_payload in rows:
                self._update_frama_event_memory(timeframe, normalized_payload)
                self._update_range_event_memory(timeframe, normalized_payload)
                if close_ms is not None:
                    self._update_state_age(timeframe, close_ms, "frama_state", normalized_payload.get("frama_state"))
                    self._update_state_age(timeframe, close_ms, "range_filter_state", normalized_payload.get("range_filter_state"))

    def _record_closed_history_locked(self, timeframe: str, payload: Dict[str, Any], *, close_ms: Optional[int] = None) -> None:
        tf = str(timeframe)
        bucket = self._closed_history_by_tf.setdefault(tf, deque(maxlen=LIVE_MARKET_AUDIT_HISTORY_MAX))
        normalized = dict(payload or {})
        if close_ms is None:
            try:
                close_ms = int(normalized.get("bar_close_time_ms")) if normalized.get("bar_close_time_ms") is not None else None
            except Exception:
                close_ms = None
        if close_ms is not None:
            normalized["bar_close_time_ms"] = int(close_ms)
            normalized["timestamp_utc"] = _iso_from_ms(close_ms)

        if not bucket:
            bucket.append(normalized)
            return

        try:
            if close_ms is not None:
                last_close_ms = int(bucket[-1].get("bar_close_time_ms")) if bucket[-1].get("bar_close_time_ms") is not None else None
            else:
                last_close_ms = None
        except Exception:
            last_close_ms = None

        if close_ms is not None and (last_close_ms is None or int(close_ms) > last_close_ms):
            bucket.append(normalized)
            return

        rows = list(bucket)
        replaced = False
        if close_ms is not None:
            for idx, item in enumerate(rows):
                try:
                    item_close_ms = int(item.get("bar_close_time_ms")) if item.get("bar_close_time_ms") is not None else None
                except Exception:
                    item_close_ms = None
                if item_close_ms == int(close_ms):
                    rows[idx] = normalized
                    replaced = True
                    break
        if not replaced:
            rows.append(normalized)
        rows.sort(
            key=lambda item: (
                int(item.get("bar_close_time_ms")) if item.get("bar_close_time_ms") is not None else -1
            )
        )
        self._closed_history_by_tf[tf] = deque(rows[-LIVE_MARKET_AUDIT_HISTORY_MAX:], maxlen=LIVE_MARKET_AUDIT_HISTORY_MAX)

    def _update_frama_event_memory(self, timeframe: str, payload: Optional[Dict[str, Any]], *, bar_ms_override: Any = None) -> None:
        if not isinstance(payload, dict):
            return
        tfk = str(timeframe)
        bar_ms = _payload_bar_time_ms(payload, override=bar_ms_override)
        if bar_ms is None:
            return
        with self._lock:
            bucket = self._frama_event_last_bar_ms.setdefault(
                tfk,
                {"break_up": None, "break_down": None, "mid_cross": None},
            )
            for key, field in (
                ("break_up", "frama_break_up"),
                ("break_down", "frama_break_down"),
                ("mid_cross", "frama_mid_cross"),
            ):
                if not bool(payload.get(field, False)):
                    continue
                try:
                    prev_raw = bucket.get(key)
                    prev_bar_ms = int(prev_raw) if prev_raw is not None else None
                except Exception:
                    prev_bar_ms = None
                if prev_bar_ms is None or int(bar_ms) >= prev_bar_ms:
                    bucket[key] = int(bar_ms)

    def _update_range_event_memory(self, timeframe: str, payload: Optional[Dict[str, Any]], *, bar_ms_override: Any = None) -> None:
        if not isinstance(payload, dict):
            return
        tfk = str(timeframe)
        bar_ms = _payload_bar_time_ms(payload, override=bar_ms_override)
        if bar_ms is None:
            return
        with self._lock:
            bucket = self._range_event_last_bar_ms.setdefault(tfk, {"buy": None, "sell": None})
            meta = self._range_event_meta.setdefault(
                tfk,
                {"last_seen_bar_ms": None, "current_bar_buy_seen": False, "current_bar_sell_seen": False},
            )
            raw_buy_now = bool(payload.get("range_filter_buy", False))
            raw_sell_now = bool(payload.get("range_filter_sell", False))

            try:
                last_seen = meta.get("last_seen_bar_ms")
                last_seen_i = int(last_seen) if last_seen is not None else None
            except Exception:
                last_seen_i = None

            # Late CLOSED-bar history can arrive after the current live bar. Backfill only
            # real trigger timestamps from raw BUY/SELL events; do not infer them from the
            # current regime/state, or the UI ends up showing state age as trigger age.
            if last_seen_i is not None and int(bar_ms) < last_seen_i:
                if raw_buy_now:
                    try:
                        prev_buy = bucket.get("buy")
                        prev_buy_i = int(prev_buy) if prev_buy is not None else None
                    except Exception:
                        prev_buy_i = None
                    if prev_buy_i is None or int(bar_ms) > prev_buy_i:
                        bucket["buy"] = int(bar_ms)
                if raw_sell_now:
                    try:
                        prev_sell = bucket.get("sell")
                        prev_sell_i = int(prev_sell) if prev_sell is not None else None
                    except Exception:
                        prev_sell_i = None
                    if prev_sell_i is None or int(bar_ms) > prev_sell_i:
                        bucket["sell"] = int(bar_ms)
                return

            try:
                if last_seen_i is None or last_seen_i != int(bar_ms):
                    meta["last_seen_bar_ms"] = int(bar_ms)
                    meta["current_bar_buy_seen"] = False
                    meta["current_bar_sell_seen"] = False
            except Exception:
                meta["last_seen_bar_ms"] = int(bar_ms)
                meta["current_bar_buy_seen"] = False
                meta["current_bar_sell_seen"] = False

            if raw_buy_now and not bool(meta.get("current_bar_buy_seen")):
                bucket["buy"] = int(bar_ms)
                meta["current_bar_buy_seen"] = True
            if raw_sell_now and not bool(meta.get("current_bar_sell_seen")):
                bucket["sell"] = int(bar_ms)
                meta["current_bar_sell_seen"] = True

    def _update_state_age(self, timeframe: str, close_ms: int, field: str, raw_state: Any) -> None:
        state = str(raw_state or "").strip().upper()
        if not state:
            return
        tf_map = self._state_age_by_tf.setdefault(timeframe, {})
        prev = tf_map.get(field)
        if not isinstance(prev, dict):
            tf_map[field] = {"state": state, "close_ms": int(close_ms), "age": 0}
            return
        prev_state = str(prev.get("state") or "").strip().upper()
        prev_close_ms = prev.get("close_ms")
        prev_age = prev.get("age")
        try:
            prev_close_i = int(prev_close_ms) if prev_close_ms is not None else None
        except Exception:
            prev_close_i = None
        try:
            prev_age_i = int(prev_age) if prev_age is not None else 0
        except Exception:
            prev_age_i = 0
        if prev_close_i is None or int(close_ms) <= prev_close_i:
            tf_map[field] = {
                "state": state,
                "close_ms": int(close_ms),
                "age": prev_age_i if state == prev_state else 0,
            }
            return
        try:
            tf_ms = int(interval_to_ms(timeframe))
            step = max(1, int(round((int(close_ms) - prev_close_i) / tf_ms))) if tf_ms > 0 else 1
        except Exception:
            step = 1
        tf_map[field] = {
            "state": state,
            "close_ms": int(close_ms),
            "age": (prev_age_i + step) if state == prev_state else 0,
        }

    def _state_age_bars(self, timeframe: str, field: str, raw_state: Any) -> Optional[int]:
        state = str(raw_state or "").strip().upper()
        if not state:
            return None
        with self._lock:
            entry = dict((self._state_age_by_tf.get(timeframe, {}) or {}).get(field) or {})
        if not entry:
            return None
        entry_state = str(entry.get("state") or "").strip().upper()
        if entry_state != state:
            return 0
        try:
            return int(entry.get("age"))
        except Exception:
            return None

    def _blank_row(self, timeframe: str) -> Dict[str, Any]:
        return {
            "timeframe": timeframe,
            "timestamp_utc": None,
            "values": {},
        }

    def _taker_bias_snapshot_from_values(
        self,
        values: Dict[str, Any],
        *,
        label: Optional[str] = None,
        bars_used: Optional[int] = None,
        requested_bars: Optional[int] = None,
    ) -> Dict[str, Any]:
        if not isinstance(values, dict):
            return {}
        out = {
            "label": label,
            "timestamp_utc": values.get("timestamp_utc") or _iso_from_ms(values.get("bar_close_time_ms")),
            "bars_used": int(bars_used) if bars_used is not None else None,
            "requested_bars": int(requested_bars) if requested_bars is not None else (int(bars_used) if bars_used is not None else None),
            "taker_buy_volume": _coerce_float(values.get("taker_buy_volume")),
            "taker_sell_volume": _coerce_float(values.get("taker_sell_volume")),
            "taker_delta_volume": _coerce_float(values.get("taker_delta_volume")),
            "taker_delta_pct": _coerce_float(values.get("taker_delta_pct")),
            "taker_buy_share_pct": _coerce_float(values.get("taker_buy_share_pct")),
            "taker_trade_count": _coerce_int(values.get("taker_trade_count")),
            "taker_bias_state": str(values.get("taker_bias_state") or "").strip().upper() or None,
        }
        return out

    def _aggregate_taker_bias_window(self, rows: List[Dict[str, Any]], *, label: str, requested_bars: Optional[int] = None) -> Dict[str, Any]:
        buy_total = 0.0
        total_volume = 0.0
        trade_total = 0
        used = 0
        start_ts = None
        end_ts = None
        for row in rows:
            source = dict(row or {})
            values = dict(source.get("values") or source)
            buy_vol = _coerce_float(values.get("taker_buy_volume"))
            sell_vol = _coerce_float(values.get("taker_sell_volume"))
            trade_count = _coerce_int(values.get("taker_trade_count"))
            if buy_vol is None or sell_vol is None:
                continue
            candle_total = float(buy_vol) + float(sell_vol)
            if candle_total <= 0.0:
                continue
            buy_total += float(buy_vol)
            total_volume += float(candle_total)
            if trade_count is not None:
                trade_total += int(trade_count)
            used += 1
            if start_ts is None:
                start_ts = source.get("timestamp_utc") or values.get("timestamp_utc") or _iso_from_ms(values.get("bar_close_time_ms"))
            end_ts = source.get("timestamp_utc") or values.get("timestamp_utc") or _iso_from_ms(values.get("bar_close_time_ms"))

        payload = compute_taker_bias_payload(total_volume, buy_total, trade_total if used else None)
        payload.update(
            {
                "label": label,
                "bars_used": int(used),
                "requested_bars": int(requested_bars) if requested_bars is not None else int(used),
                "window_start_utc": start_ts,
                "window_end_utc": end_ts,
            }
        )
        return payload

    def _build_taker_bias_detail(self, timeframe: str) -> Dict[str, Any]:
        tf = str(timeframe)
        with self._lock:
            rows = [dict(item) for item in list(self._closed_history_by_tf.get(tf, ()))]
            latest_closed = dict(self._latest_closed_by_tf.get(tf, {}) or {})
        if not rows and latest_closed:
            rows = [{"timeframe": tf, "timestamp_utc": latest_closed.get("timestamp_utc"), "values": latest_closed}]
        if not rows:
            return {
                "available_windows": [1, 3, 5, 10],
                "closed_count": 0,
                "prev_closed": None,
                "windows": {},
            }

        prev_row = rows[-1]
        prev_values = dict(prev_row.get("values") or prev_row)
        prev_closed = self._taker_bias_snapshot_from_values(
            {
                **prev_values,
                "timestamp_utc": prev_row.get("timestamp_utc") or prev_values.get("timestamp_utc"),
            },
            label="Previous Closed",
            bars_used=1,
            requested_bars=1,
        )
        windows: Dict[str, Dict[str, Any]] = {}
        for size in (1, 3, 5, 10):
            sample = rows[-int(size):]
            windows[str(size)] = self._aggregate_taker_bias_window(sample, label=f"Last {int(size)} Closed", requested_bars=int(size))
        return {
            "available_windows": [1, 3, 5, 10],
            "closed_count": int(len(rows)),
            "prev_closed": prev_closed,
            "windows": windows,
        }

    def _merge_partial_live_indicator_families(self, timeframe: str, values: Dict[str, Any]) -> Dict[str, Any]:
        if not isinstance(values, dict) or not values:
            return values
        with self._lock:
            previous_values = dict(((self._rows_by_tf.get(str(timeframe), {}) or {}).get("values") or {}))
        if not previous_values:
            return values

        merged = dict(values)
        for spec in LIVE_ROW_FAMILY_CARRY_SPECS:
            anchors = tuple(spec.get("anchors") or ())
            fields = tuple(spec.get("fields") or ())
            if not fields:
                continue
            family_has_anchor = any(_live_value_present(merged.get(field)) for field in anchors)
            if family_has_anchor:
                for field in fields:
                    if not _live_value_present(merged.get(field)) and _live_value_present(previous_values.get(field)):
                        merged[field] = previous_values.get(field)
                continue
            for field in fields:
                if _live_value_present(previous_values.get(field)):
                    merged[field] = previous_values.get(field)
        return merged

    def _normalize_live_row(self, timeframe: str, payload: Dict[str, Any]) -> Dict[str, Any]:
        values = {key: _serialize_snapshot_value(value) for key, value in payload.items()}
        current_bar_ms = _payload_bar_time_ms(values)
        range_ref_bar_ms = self._latest_closed_reference_bar_ms(timeframe, current_bar_ms)
        frama_event_bar_ms = self._frama_live_event_bar_ms(timeframe, values, current_bar_ms)
        frama_ref_bar_ms = self._frama_age_reference_bar_ms(timeframe, values, current_bar_ms)
        close_ms = values.get("bar_close_time_ms")
        self._update_frama_event_memory(timeframe, values, bar_ms_override=frama_event_bar_ms)
        self._update_range_event_memory(timeframe, values, bar_ms_override=current_bar_ms)
        values["frama_break_up_age"] = self._frama_event_age_bars(timeframe, frama_ref_bar_ms, "break_up")
        values["frama_break_down_age"] = self._frama_event_age_bars(timeframe, frama_ref_bar_ms, "break_down")
        values["frama_mid_cross_age"] = self._frama_event_age_bars(timeframe, frama_ref_bar_ms, "mid_cross")
        values["range_filter_buy_age"] = self._event_age_bars(timeframe, range_ref_bar_ms, "buy")
        values["range_filter_sell_age"] = self._event_age_bars(timeframe, range_ref_bar_ms, "sell")
        values["frama_state_age"] = self._state_age_bars(timeframe, "frama_state", values.get("frama_state"))
        frama_trigger_side, frama_trigger_age = self._frama_latest_directional_trigger(
            timeframe,
            frama_ref_bar_ms,
            values,
        )
        values["frama_trigger_side"] = frama_trigger_side
        values["frama_trigger_age"] = frama_trigger_age
        values["frama_display_age"] = frama_trigger_age
        values["range_filter_state_age"] = self._state_age_bars(timeframe, "range_filter_state", values.get("range_filter_state"))
        values["range_filter_display_state"] = _range_display_state_from_payload(values)
        range_trigger_side, range_trigger_age = self._range_latest_directional_trigger(
            timeframe,
            range_ref_bar_ms,
            values,
        )
        values["range_filter_trigger_side"] = range_trigger_side
        values["range_filter_trigger_age"] = range_trigger_age
        values["range_filter_display_age"] = range_trigger_age
        values = self._merge_partial_live_indicator_families(timeframe, values)
        return {
            "timeframe": timeframe,
            "timestamp_utc": _iso_from_ms(close_ms),
            "values": values,
        }

    def _event_age_bars(self, timeframe: str, current_bar_ms: Any, side: str) -> Optional[int]:
        try:
            current_bar_i = int(current_bar_ms)
        except Exception:
            return None
        with self._lock:
            event_bar_ms = self._range_event_last_bar_ms.get(timeframe, {}).get(side)
        if event_bar_ms is None:
            return None
        try:
            tf_ms = int(interval_to_ms(timeframe))
            if tf_ms <= 0:
                return None
            diff_ms = max(0, int(current_bar_i) - int(event_bar_ms))
            return int(diff_ms // tf_ms)
        except Exception:
            return None

    def _frama_event_age_bars(self, timeframe: str, current_bar_ms: Any, event_key: str) -> Optional[int]:
        try:
            current_bar_i = int(current_bar_ms)
        except Exception:
            return None
        with self._lock:
            event_bar_ms = self._frama_event_last_bar_ms.get(timeframe, {}).get(event_key)
        if event_bar_ms is None:
            return None
        try:
            tf_ms = int(interval_to_ms(timeframe))
            if tf_ms <= 0:
                return None
            diff_ms = max(0, int(current_bar_i) - int(event_bar_ms))
            return int(diff_ms // tf_ms)
        except Exception:
            return None

    def _frama_latest_directional_trigger(
        self,
        timeframe: str,
        current_bar_ms: Any,
        payload: Optional[Dict[str, Any]] = None,
    ) -> Tuple[Optional[str], Optional[int]]:
        try:
            current_bar_i = int(current_bar_ms)
        except Exception:
            return None, None
        with self._lock:
            bucket = dict(self._frama_event_last_bar_ms.get(timeframe, {}) or {})
            latest_closed = dict((self._latest_closed_by_tf.get(str(timeframe), {}) or {}))

        def _as_int(value: Any) -> Optional[int]:
            try:
                return int(value) if value is not None else None
            except Exception:
                return None

        up_ms = _as_int(bucket.get("break_up"))
        down_ms = _as_int(bucket.get("break_down"))
        if up_ms is None and down_ms is None:
            return None, None
        if up_ms is not None and (down_ms is None or up_ms > down_ms):
            return "BUY", self._frama_event_age_bars(timeframe, current_bar_i, "break_up")
        if down_ms is not None and (up_ms is None or down_ms > up_ms):
            return "SELL", self._frama_event_age_bars(timeframe, current_bar_i, "break_down")

        probe = payload if isinstance(payload, dict) and payload else latest_closed
        if bool(probe.get("frama_break_up")) and not bool(probe.get("frama_break_down")):
            return "BUY", self._frama_event_age_bars(timeframe, current_bar_i, "break_up")
        if bool(probe.get("frama_break_down")) and not bool(probe.get("frama_break_up")):
            return "SELL", self._frama_event_age_bars(timeframe, current_bar_i, "break_down")
        return None, None

    def _range_latest_directional_trigger(
        self,
        timeframe: str,
        current_bar_ms: Any,
        payload: Optional[Dict[str, Any]] = None,
    ) -> Tuple[Optional[str], Optional[int]]:
        try:
            current_bar_i = int(current_bar_ms)
        except Exception:
            return None, None
        with self._lock:
            bucket = dict(self._range_event_last_bar_ms.get(timeframe, {}) or {})
            latest_closed = dict((self._latest_closed_by_tf.get(str(timeframe), {}) or {}))

        def _as_int(value: Any) -> Optional[int]:
            try:
                return int(value) if value is not None else None
            except Exception:
                return None

        buy_ms = _as_int(bucket.get("buy"))
        sell_ms = _as_int(bucket.get("sell"))
        if buy_ms is None and sell_ms is None:
            return None, None
        if buy_ms is not None and (sell_ms is None or buy_ms > sell_ms):
            return "BUY", self._event_age_bars(timeframe, current_bar_i, "buy")
        if sell_ms is not None and (buy_ms is None or sell_ms > buy_ms):
            return "SELL", self._event_age_bars(timeframe, current_bar_i, "sell")

        probe = payload if isinstance(payload, dict) and payload else latest_closed
        if bool(probe.get("range_filter_buy")) and not bool(probe.get("range_filter_sell")):
            return "BUY", self._event_age_bars(timeframe, current_bar_i, "buy")
        if bool(probe.get("range_filter_sell")) and not bool(probe.get("range_filter_buy")):
            return "SELL", self._event_age_bars(timeframe, current_bar_i, "sell")
        return None, None

    def _frama_state_entry_age_bars(self, timeframe: str, current_bar_ms: Any, target_state: str) -> Optional[int]:
        try:
            current_bar_i = int(current_bar_ms)
        except Exception:
            return None
        target = str(target_state or "").strip().upper()
        if target not in {"BUY", "SELL", "NEUTRAL"}:
            return None
        try:
            tf_ms = int(interval_to_ms(timeframe))
            if tf_ms <= 0:
                return None
        except Exception:
            return None
        with self._lock:
            rows = [dict(item) for item in list(self._closed_history_by_tf.get(str(timeframe), ()))]
        if not rows:
            return None
        last_entry_bar_ms: Optional[int] = None
        prev_state = ""
        for row in rows:
            row_bar_ms = _payload_bar_time_ms(row)
            try:
                row_bar_i = int(row_bar_ms) if row_bar_ms is not None else None
            except Exception:
                row_bar_i = None
            if row_bar_i is None or row_bar_i > int(current_bar_i):
                continue
            state = str(row.get("frama_state") or "").strip().upper()
            if not state:
                continue
            if state == target and prev_state != target:
                last_entry_bar_ms = int(row_bar_i)
            prev_state = state
        if last_entry_bar_ms is None:
            return None
        diff_ms = max(0, int(current_bar_i) - int(last_entry_bar_ms))
        return int(diff_ms // tf_ms)

    def _latest_closed_bar_ms(self, timeframe: str) -> Optional[int]:
        with self._lock:
            latest = dict((self._latest_closed_by_tf.get(str(timeframe), {}) or {}))
        if not latest:
            return None
        return _payload_bar_time_ms(latest)

    def _latest_closed_reference_bar_ms(self, timeframe: str, current_bar_ms: Any) -> Optional[int]:
        try:
            current_bar_i = int(current_bar_ms) if current_bar_ms is not None else None
        except Exception:
            current_bar_i = None
        if current_bar_i is None:
            return None
        latest_closed_ms = self._latest_closed_bar_ms(timeframe)
        try:
            latest_closed_i = int(latest_closed_ms) if latest_closed_ms is not None else None
        except Exception:
            latest_closed_i = None
        if latest_closed_i is not None and latest_closed_i < int(current_bar_i):
            return int(latest_closed_i)
        return int(current_bar_i)

    def _previous_bar_ms(self, timeframe: str, current_bar_ms: Any) -> Optional[int]:
        try:
            current_bar_i = int(current_bar_ms) if current_bar_ms is not None else None
        except Exception:
            current_bar_i = None
        if current_bar_i is None:
            return None
        try:
            tf_ms = int(interval_to_ms(timeframe))
            if tf_ms > 0:
                return int(current_bar_i - tf_ms)
        except Exception:
            pass
        return int(current_bar_i)

    def _frama_live_event_bar_ms(self, timeframe: str, payload: Dict[str, Any], current_bar_ms: Any) -> Optional[int]:
        if not (
            bool(payload.get("frama_break_up"))
            or bool(payload.get("frama_break_down"))
            or bool(payload.get("frama_mid_cross"))
        ):
            return None
        try:
            current_bar_i = int(current_bar_ms) if current_bar_ms is not None else None
        except Exception:
            current_bar_i = None
        if current_bar_i is None:
            return None
        latest_closed_ms = self._latest_closed_bar_ms(timeframe)
        try:
            latest_closed_i = int(latest_closed_ms) if latest_closed_ms is not None else None
        except Exception:
            latest_closed_i = None
        if latest_closed_i is not None and latest_closed_i < int(current_bar_i):
            return int(latest_closed_i)
        prev_bar_ms = self._previous_bar_ms(timeframe, current_bar_i)
        try:
            prev_bar_i = int(prev_bar_ms) if prev_bar_ms is not None else None
        except Exception:
            prev_bar_i = None
        if prev_bar_i is not None and prev_bar_i < int(current_bar_i):
            return int(prev_bar_i)
        return int(current_bar_i)

    def _frama_age_reference_bar_ms(self, timeframe: str, payload: Dict[str, Any], current_bar_ms: Any) -> Optional[int]:
        try:
            current_bar_i = int(current_bar_ms) if current_bar_ms is not None else None
        except Exception:
            current_bar_i = None
        if current_bar_i is None:
            return None
        latest_closed_ms = self._latest_closed_bar_ms(timeframe)
        try:
            latest_closed_i = int(latest_closed_ms) if latest_closed_ms is not None else None
        except Exception:
            latest_closed_i = None
        if latest_closed_i is not None and latest_closed_i < int(current_bar_i):
            return int(latest_closed_i)
        live_event_bar_ms = self._frama_live_event_bar_ms(timeframe, payload, current_bar_i)
        try:
            live_event_bar_i = int(live_event_bar_ms) if live_event_bar_ms is not None else None
        except Exception:
            live_event_bar_i = None
        if live_event_bar_i is not None:
            return int(live_event_bar_i)
        return int(current_bar_i)

    def _build_payload_locked(self) -> Dict[str, Any]:
        config = self._config
        rows: List[Dict[str, Any]] = []
        latest_ts: Optional[str] = None
        if config is not None:
            for tf in config.timeframes:
                row = self._rows_by_tf.get(tf) or self._blank_row(tf)
                normalized = {
                    "timeframe": row.get("timeframe", tf),
                    "timestamp_utc": row.get("timestamp_utc"),
                    "values": dict(row.get("values") or {}),
                }
                current_bar_ms = _payload_bar_time_ms(normalized["values"])
                range_ref_bar_ms = self._latest_closed_reference_bar_ms(tf, current_bar_ms)
                frama_ref_bar_ms = self._frama_age_reference_bar_ms(tf, normalized["values"], current_bar_ms)
                close_ms = normalized["values"].get("bar_close_time_ms")
                normalized["values"]["range_filter_buy_age"] = self._event_age_bars(tf, range_ref_bar_ms, "buy")
                normalized["values"]["range_filter_sell_age"] = self._event_age_bars(tf, range_ref_bar_ms, "sell")
                normalized["values"]["frama_break_up_age"] = self._frama_event_age_bars(tf, frama_ref_bar_ms, "break_up")
                normalized["values"]["frama_break_down_age"] = self._frama_event_age_bars(tf, frama_ref_bar_ms, "break_down")
                normalized["values"]["frama_mid_cross_age"] = self._frama_event_age_bars(tf, frama_ref_bar_ms, "mid_cross")
                normalized["values"]["frama_state_age"] = self._state_age_bars(tf, "frama_state", normalized["values"].get("frama_state"))
                frama_trigger_side, frama_trigger_age = self._frama_latest_directional_trigger(
                    tf,
                    frama_ref_bar_ms,
                    normalized["values"],
                )
                normalized["values"]["frama_trigger_side"] = frama_trigger_side
                normalized["values"]["frama_trigger_age"] = frama_trigger_age
                normalized["values"]["frama_display_age"] = frama_trigger_age
                normalized["values"]["range_filter_state_age"] = self._state_age_bars(tf, "range_filter_state", normalized["values"].get("range_filter_state"))
                normalized["values"]["range_filter_display_state"] = _range_display_state_from_payload(normalized["values"])
                range_trigger_side, range_trigger_age = self._range_latest_directional_trigger(
                    tf,
                    range_ref_bar_ms,
                    normalized["values"],
                )
                normalized["values"]["range_filter_trigger_side"] = range_trigger_side
                normalized["values"]["range_filter_trigger_age"] = range_trigger_age
                normalized["values"]["range_filter_display_age"] = range_trigger_age
                normalized["taker_bias_detail"] = self._build_taker_bias_detail(tf)
                rows.append(normalized)
                if normalized["timestamp_utc"]:
                    latest_ts = normalized["timestamp_utc"]

        return {
            "preset_name": config.preset_name if config and config.session_kind == "preset" else None,
            "session_name": config.session_name if config else None,
            "session_kind": config.session_kind if config else None,
            "symbol": config.symbol if config else None,
            "timeframes": list(config.timeframes) if config else [],
            "rows": rows,
            "snapshot_source": "live_engine",
            "snapshot_source_note": "websocket-preloaded" if self._prefill_done else "preloading-history",
            "snapshot_as_of_utc": latest_ts,
            "snapshot_cached": False,
            "snapshot_refresh_mode": "websocket",
            "snapshot_cache_age_sec": 0.0,
            "snapshot_is_stale_minute": False,
            "live_phase": self._status,
            "live_message": self._status_message,
            "prefill_done": bool(self._prefill_done),
            "live_price": self._last_price,
            "live_age_sec": self._last_age_sec,
            "live_log_tail": list(self._recent_logs),
            "live_error": self._last_error,
            "transport": "websocket",
        }

    def _closed_history_frame(self, timeframe: str) -> pd.DataFrame:
        with self._lock:
            rows = [dict(item) for item in list(self._closed_history_by_tf.get(str(timeframe), ()))]
        if not rows:
            latest = dict((self._latest_closed_by_tf.get(str(timeframe), {}) or {}))
            if latest:
                rows = [latest]
        if not rows:
            return pd.DataFrame()
        frame = pd.DataFrame(rows)
        if frame.empty:
            return pd.DataFrame()
        frame = frame.drop(columns=["timestamp_utc"], errors="ignore")
        close_ms = pd.to_numeric(frame.get("bar_close_time_ms"), errors="coerce")
        idx = pd.to_datetime(close_ms, unit="ms", utc=True, errors="coerce")
        frame.index = idx
        frame = frame.loc[~frame.index.isna()].sort_index()
        frame.index.name = "__prepared_index__"
        return frame

    def _forming_live_frame(self, timeframe: str) -> pd.DataFrame:
        with self._lock:
            row = dict(self._rows_by_tf.get(str(timeframe), {}) or {})
        values = dict(row.get("values") or {})
        if not values:
            return pd.DataFrame()
        try:
            close_ms = int(values.get("bar_close_time_ms")) if values.get("bar_close_time_ms") is not None else None
        except Exception:
            close_ms = None
        if close_ms is None:
            return pd.DataFrame()
        if not self._audit_row_has_valid_ohlc(values):
            return pd.DataFrame()
        frame = pd.DataFrame([values])
        frame.index = pd.to_datetime(pd.Series([close_ms]), unit="ms", utc=True, errors="coerce")
        frame = frame.loc[~frame.index.isna()]
        frame.index.name = "__prepared_index__"
        return frame

    def _merge_live_forming_frame(self, base: pd.DataFrame, forming: pd.DataFrame) -> pd.DataFrame:
        if not isinstance(forming, pd.DataFrame) or forming.empty:
            return base
        if not isinstance(base, pd.DataFrame) or base.empty:
            return forming.copy()
        # Drop columns that are completely empty on either side before concat so
        # pandas does not warn about future dtype changes for all-NA entries.
        base_nonempty = base.dropna(axis=1, how="all")
        forming_nonempty = forming.dropna(axis=1, how="all")
        merged = pd.concat([base_nonempty, forming_nonempty], axis=0, sort=False)
        merged = merged[~merged.index.duplicated(keep="last")].sort_index()
        return merged

    @staticmethod
    def _audit_row_has_valid_ohlc(row: Dict[str, Any]) -> bool:
        if not isinstance(row, dict):
            return False
        try:
            open_ = float(row.get("open"))
            high = float(row.get("high"))
            low = float(row.get("low"))
            close = float(row.get("close"))
        except Exception:
            return False
        if not all(math.isfinite(value) for value in (open_, high, low, close)):
            return False
        if high < low:
            return False
        if high < max(open_, close):
            return False
        if low > min(open_, close):
            return False
        return True

    def get_market_audit_from_cache(
        self,
        *,
        timeframe: str = "5m",
        bars: int = 300,
        end_utc: Optional[str] = None,
        session_name: Optional[str] = None,
        include_forming: bool = True,
    ) -> Dict[str, Any]:
        with self._lock:
            config = self._config
            symbol = str(config.symbol) if config is not None else None
            available_timeframes = list(config.timeframes) if config is not None else []
            audit_name = str(session_name or (config.session_name if config is not None else "live_market") or "live_market")
            prefill_done = bool(self._prefill_done)
            live_use_forming = bool(config.live_use_forming) if config is not None else False
        chosen_tf = str(timeframe or "").strip() or (available_timeframes[0] if available_timeframes else "1m")
        if available_timeframes and chosen_tf not in available_timeframes:
            chosen_tf = available_timeframes[0]
        requested_bars = max(1, min(1200, int(bars or 300)))

        forming_frames: Dict[str, pd.DataFrame] = {}
        frames = {tf: self._closed_history_frame(tf) for tf in available_timeframes}
        if include_forming and live_use_forming:
            for tf in list(available_timeframes):
                forming_frames[tf] = self._forming_live_frame(tf)
                frames[tf] = self._merge_live_forming_frame(frames.get(tf, pd.DataFrame()), forming_frames[tf])
        chosen_frame = frames.get(chosen_tf) if chosen_tf in frames else self._closed_history_frame(chosen_tf)
        if include_forming and live_use_forming:
            chosen_forming = forming_frames.get(chosen_tf)
            if chosen_forming is None:
                chosen_forming = self._forming_live_frame(chosen_tf)
                forming_frames[chosen_tf] = chosen_forming
            chosen_frame = self._merge_live_forming_frame(chosen_frame, chosen_forming)
        if chosen_tf not in frames and not chosen_frame.empty:
            frames[chosen_tf] = chosen_frame

        audit_payload = build_market_context_audit_window(
            chosen_frame,
            timeframe=chosen_tf,
            max_bars=requested_bars,
            end_utc=end_utc,
        )

        analysis_start = None
        analysis_end = None
        analysis_lookback_minutes = 0
        analysis_warmup_minutes = 0
        if isinstance(chosen_frame, pd.DataFrame) and not chosen_frame.empty:
            try:
                analysis_start = str(pd.to_datetime(chosen_frame.index[0], utc=True))
                analysis_end = str(pd.to_datetime(chosen_frame.index[-1], utc=True))
                delta = pd.to_datetime(chosen_frame.index[-1], utc=True) - pd.to_datetime(chosen_frame.index[0], utc=True)
                analysis_lookback_minutes = max(0, int(round(delta.total_seconds() / 60.0)))
            except Exception:
                analysis_start = analysis_end = None
                analysis_lookback_minutes = 0
            try:
                tf_minutes = max(1, int(interval_to_ms(chosen_tf) // 60_000))
            except Exception:
                tf_minutes = 1
            extra_bars = max(0, int(len(chosen_frame.index)) - int(requested_bars))
            analysis_warmup_minutes = max(0, int(extra_bars * tf_minutes))

        rows = list(audit_payload.get("rows") or [])
        while rows and not self._audit_row_has_valid_ohlc(rows[-1]):
            rows.pop()
        if rows:
            audit_payload["rows"] = rows
            audit_payload["bar_count"] = int(len(rows))
            audit_payload["window_start_utc"] = rows[0].get("timestamp_utc")
            audit_payload["window_end_utc"] = rows[-1].get("timestamp_utc")
            audit_payload["anchor_timestamp_utc"] = rows[-1].get("timestamp_utc")
            audit_payload["latest_context"] = dict(rows[-1])
            markers = list(audit_payload.get("markers") or [])
            audit_payload["markers"] = [
                marker for marker in markers
                if int(marker.get("bar_index", -1)) < len(rows)
            ]
        ohlc_valid_bars = sum(1 for row in rows if self._audit_row_has_valid_ohlc(row))
        forming_frame = forming_frames.get(chosen_tf)
        has_forming_row = bool(
            include_forming
            and live_use_forming
            and isinstance(forming_frame, pd.DataFrame)
            and not forming_frame.empty
            and str(audit_payload.get("anchor_timestamp_utc") or "") == str(pd.to_datetime(forming_frame.index[-1], utc=True))
        )

        return {
            "result_kind": "market_context_audit",
            "preset_name": None,
            "audit_target_name": audit_name,
            "audit_target_kind": "live_context",
            "symbol": symbol,
            "timeframe": chosen_tf,
            "requested_bars": int(requested_bars),
            "analysis_window_start_utc": analysis_start,
            "analysis_window_end_utc": analysis_end,
            "analysis_lookback_minutes": int(analysis_lookback_minutes),
            "analysis_warmup_minutes": int(analysis_warmup_minutes),
            "window_start_utc": audit_payload.get("window_start_utc"),
            "window_end_utc": audit_payload.get("window_end_utc"),
            "anchor_timestamp_utc": audit_payload.get("anchor_timestamp_utc"),
            "bar_count": int(audit_payload.get("bar_count") or 0),
            "rows": rows,
            "markers": list(audit_payload.get("markers") or []),
            "latest_context": dict(audit_payload.get("latest_context") or {}),
            "latest_context_by_timeframe": build_timeframe_context_summary(frames),
            "available_timeframes": list(available_timeframes or [chosen_tf]),
            "audit_source_note": (
                "live-cache+forming"
                if has_forming_row and prefill_done
                else ("live-cache-warming+forming" if has_forming_row else ("live-cache" if prefill_done else "live-cache-warming"))
            ),
            "request_end_utc": str(end_utc) if end_utc else analysis_end,
            "ohlc_valid_bars": int(ohlc_valid_bars),
            "ohlc_invalid_bars": max(0, int(len(rows) - ohlc_valid_bars)),
            "includes_forming_bar": bool(has_forming_row),
        }

    def _emit(self, payload: Dict[str, Any], *, force: bool) -> None:
        listeners: List[LiveListener]
        now = time.monotonic()
        with self._lock:
            self._last_payload = dict(payload)
            if not force and (now - self._last_push_monotonic) < self._push_min_interval_sec:
                return
            self._last_push_monotonic = now
            listeners = list(self._listeners.values())
        for listener in listeners:
            try:
                listener(dict(payload))
            except Exception:
                pass
