from __future__ import annotations

"""Live Binance engine: websocket + REST fallback + indicator streaming."""

import json
import time
import queue
import threading
import collections
from dataclasses import dataclass, field
from typing import Optional, Any, Dict, List, Tuple
import os

import numpy as np
import pandas as pd
import requests
import websocket  # websocket-client

from .constants import *
from .utils_time import interval_to_ms
from .data_binance import fetch_recent_closes_futures, fetch_recent_ohlc_futures
from .ema_pair import build_ema_pair_payload
from .indicators_streaming import (
    compute_live_indicators,
    ema_tradingview,
    macd_series,
    ppo_d_series,
    rsi_series,
    stoch_rsi_series,
    _WilderADXState,
    make_adx_state,
    make_atr_state,
    make_frama_state,
    make_vidya_state,
    make_range_filter_state,
    compute_market_state_snapshot,
)
from .orderflow_engine import make_orderflow_windows, LocalOrderBook, evaluate_boundary_aware_breakout
from .services.live.health_service import classify_orderflow_age, build_orderflow_quality_overlay
from .taker_bias import compute_taker_bias_payload

@dataclass
class LiveStorage:
    tf: str
    closes: List[float] = field(default_factory=list)
    # For LAST source, these are derived from the kline stream.
    # For MARK/INDEX, these are synthesized from UTC boundaries.
    last_open_time_ms: Optional[int] = None
    last_close_time_ms: Optional[int] = None
    forming_high: Optional[float] = None
    forming_low: Optional[float] = None
    forming_close: Optional[float] = None
    forming_volume: Optional[float] = None
    forming_trade_count: Optional[int] = None
    forming_taker_buy_volume: Optional[float] = None

class LiveEngine:
    """
    Live data engine:
      - Prefills closes for selected timeframes (LAST/MARK/INDEX).
      - Subscribes to websockets for live price.
      - For LAST source: uses kline close messages to append official candle closes.
      - For MARK/INDEX: synthesizes candle closes from the stream at UTC boundaries.
    """

    def __init__(
        self,
        q: "queue.Queue",
        symbol: str,
        timeframes: List[str],
        price_source: str = "LAST",
        macd_impl: str = "TRADINGVIEW",
        adx_impl: str = "TRADINGVIEW",
        live_use_forming: bool = True,
        ema_1_length: int = 9,
        ema_2_length: int = 21,
        ema_lengths_by_tf: Optional[Dict[str, Dict[str, int]]] = None,
        rsi_length: int = RSI_LENGTH,
        rsi_smoothing: int = RSI_SMOOTHING,
        rsi_settings_by_tf: Optional[Dict[str, Dict[str, int]]] = None,
        orderflow_mode: Optional[str] = None,
        orderflow_book_limit: int = 1000,
        orderflow_top_levels: int = 20,
        orderflow_integration_mode: Optional[str] = None,
    ):
        self.q = q
        self.symbol = symbol.upper()
        self.tfs = sorted(set(timeframes), key=lambda x: ALL_TIMEFRAMES.index(x) if x in ALL_TIMEFRAMES else 999)
        self.price_source = (price_source or "LAST").upper()
        if self.price_source not in PRICE_SOURCES:
            self.price_source = "LAST"
        self.macd_impl = (macd_impl or "TRADINGVIEW").upper()
        if self.macd_impl not in MACD_IMPLS:
            self.macd_impl = "TRADINGVIEW"
        self.adx_impl = (adx_impl or "TRADINGVIEW").upper()
        if self.adx_impl not in ADX_IMPLS:
            self.adx_impl = "TRADINGVIEW"
        self.live_use_forming = bool(live_use_forming)
        try:
            self.ema_1_length = max(1, int(ema_1_length or 9))
        except Exception:
            self.ema_1_length = 9
        try:
            self.ema_2_length = max(1, int(ema_2_length or 21))
        except Exception:
            self.ema_2_length = 21
        self.ema_lengths_by_tf: Dict[str, Dict[str, int]] = {}
        raw_ema_lengths = ema_lengths_by_tf if isinstance(ema_lengths_by_tf, dict) else {}
        for tf in self.tfs:
            raw_pair = raw_ema_lengths.get(tf, {})
            try:
                tf_fast = max(1, int(raw_pair.get("ema_1_length", self.ema_1_length) or self.ema_1_length))
            except Exception:
                tf_fast = self.ema_1_length
            try:
                tf_slow = max(1, int(raw_pair.get("ema_2_length", self.ema_2_length) or self.ema_2_length))
            except Exception:
                tf_slow = self.ema_2_length
            self.ema_lengths_by_tf[tf] = {
                "ema_1_length": tf_fast,
                "ema_2_length": tf_slow,
            }
        try:
            self.rsi_length = max(1, int(rsi_length or RSI_LENGTH))
        except Exception:
            self.rsi_length = RSI_LENGTH
        try:
            self.rsi_smoothing = max(1, int(rsi_smoothing or RSI_SMOOTHING))
        except Exception:
            self.rsi_smoothing = RSI_SMOOTHING
        self.rsi_settings_by_tf: Dict[str, Dict[str, int]] = {}
        raw_rsi_settings = rsi_settings_by_tf if isinstance(rsi_settings_by_tf, dict) else {}
        for tf in self.tfs:
            raw_cfg = raw_rsi_settings.get(tf, {})
            try:
                tf_length = max(1, int(raw_cfg.get("length", self.rsi_length) or self.rsi_length))
            except Exception:
                tf_length = self.rsi_length
            try:
                tf_smoothing = max(1, int(raw_cfg.get("smoothing", self.rsi_smoothing) or self.rsi_smoothing))
            except Exception:
                tf_smoothing = self.rsi_smoothing
            self.rsi_settings_by_tf[tf] = {
                "length": tf_length,
                "smoothing": tf_smoothing,
            }

        self.lock = threading.Lock()
        self.running = False

        self.storage: Dict[str, LiveStorage] = {tf: LiveStorage(tf=tf) for tf in self.tfs}


        # ADX / DI states on CLOSED bars (impl selectable)
        self.adx_state: Dict[str, Any] = {tf: make_adx_state(self.adx_impl, ADX_LEN) for tf in self.tfs}

        # ATR state on CLOSED bars (TradingView/Wilder)
        self.atr_state: Dict[str, Any] = {tf: make_atr_state(ATR_LEN) for tf in self.tfs}
        # FRAMA Channel state on CLOSED bars (exact TradingView script semantics)
        self.frama_state: Dict[str, Any] = {tf: make_frama_state(FRAMA_LEN, FRAMA_BANDS_DISTANCE) for tf in self.tfs}
        # Volumatic VIDYA state on CLOSED bars (exact TradingView script semantics for core signals)
        self.vidya_state: Dict[str, Any] = {tf: make_vidya_state(VIDYA_LENGTH, VIDYA_MOMENTUM, VIDYA_BAND_DISTANCE, VIDYA_PIVOT_LEFT, VIDYA_PIVOT_RIGHT) for tf in self.tfs}
        # Range Filter Buy and Sell state on CLOSED bars (exact TradingView script semantics)
        self.range_filter_state: Dict[str, Any] = {tf: make_range_filter_state(RANGE_FILTER_PER, RANGE_FILTER_MULT) for tf in self.tfs}

        self.live_price: Optional[float] = None
        self.last_ws_tick_ts: float = 0.0
        self.ws: Optional[websocket.WebSocketApp] = None

        # For MARK/INDEX synthetic candle closes (based on UTC boundaries)
        self._last_minute_open_ms: Optional[int] = None

        self.prefill_done: bool = False  # set True when prefill history is loaded

        # Track which CLOSED bar we've already emitted for each TF (prevents duplicates).
        self._last_emitted_close_ms: Dict[str, Optional[int]] = {tf: None for tf in self.tfs}

        # Prefill metadata for seeding CLOSED-bar history (so Bars-Ago works immediately)
        self._prefill_adx_series: Dict[str, List[tuple]] = {}
        self._prefill_atr_series: Dict[str, List[tuple]] = {}
        self._prefill_frama_series: Dict[str, List[dict]] = {}
        self._prefill_vidya_series: Dict[str, List[dict]] = {}
        self._prefill_range_filter_series: Dict[str, List[dict]] = {}
        self._prefill_ohlcv_series: Dict[str, List[dict]] = {}
        self._prefill_bar_times: Dict[str, Dict[str, List[int]]] = {}  # {tf: {'open_ms': [...], 'close_ms': [...]}}
        self._latest_closed_taker_metrics: Dict[str, Dict[str, Any]] = {tf: {} for tf in self.tfs}

        # While prefill runs in background, websocket may already deliver CLOSED klines.
        # Buffer their OHLC so we can replay onto a freshly-seeded ADX state after prefill (prevents drift).
        self._pending_adx_ohlc: Dict[str, List[tuple]] = {tf: [] for tf in self.tfs}
        self._pending_atr_ohlc: Dict[str, List[tuple]] = {tf: [] for tf in self.tfs}
        self._pending_frama_ohlc: Dict[str, List[tuple]] = {tf: [] for tf in self.tfs}
        self._pending_vidya_ohlcv: Dict[str, List[tuple]] = {tf: [] for tf in self.tfs}
        self._pending_range_filter_close: Dict[str, List[float]] = {tf: [] for tf in self.tfs}

        # Live ADX slope (TradingView-like): compare successive LIVE previews.
        # This makes ADXAngle match the visible line direction on TradingView.
        self._last_live_adx_preview: Dict[str, Optional[float]] = {tf: None for tf in self.tfs}
        self._last_live_adx_slope: Dict[str, Optional[str]] = {tf: None for tf in self.tfs}

        # Live ATR slope (TradingView-like): compare successive LIVE previews.
        self._last_live_atr_preview: Dict[str, Optional[float]] = {tf: None for tf in self.tfs}
        self._last_live_atr_slope: Dict[str, Optional[str]] = {tf: None for tf in self.tfs}
        self._market_state_runtime: Dict[str, Dict[str, Any]] = {tf: {"market_state": None, "market_state_age": 0, "bar_close_time_ms": None} for tf in self.tfs}


        # -------------------------
        # Closed-OHLC ring buffers (used to prevent long-run indicator drift)
        # -------------------------
        # We keep a reasonably large rolling window of CLOSED OHLC per timeframe.
        # This lets us periodically rebuild ADX/ATR state from source bars without a full engine restart.
        #
        # Why this exists:
        # - The websocket can occasionally drop a kline close.
        # - Our repair loop can patch missing CLOSES via REST.
        # - If we patch closes but don't also patch ADX/ATR with the missing OHLC, the state can drift.
        #
        # The ring is populated by:
        # - prefill OHLC history
        # - websocket closed klines
        # - repair-loop REST OHLC patches
        self._ohlc_ring_max = 5000
        self._closed_ohlc_ring: Dict[str, "collections.deque"] = {
            tf: collections.deque(maxlen=self._ohlc_ring_max) for tf in self.tfs
        }
        self._closed_ohlcv_ring: Dict[str, "collections.deque"] = {
            tf: collections.deque(maxlen=self._ohlc_ring_max) for tf in self.tfs
        }

        # Sanity / self-heal cadence (seconds). Every minute we rebuild state from the ring.
        # This is a lightweight equivalent of "toggle forming candle OFF/ON" but without restarting the UI.
        self._sanity_rebuild_sec: float = 60.0
        self._sanity_adx_threshold: float = 0.25
        self._sanity_atr_threshold: float = 1e-9

        # Live tape / order-flow windows.
        # This stays separate from indicator math and backtest logic.
        self._orderflow_windows = make_orderflow_windows(self.tfs)
        self._orderflow_last: Dict[str, Dict[str, Any]] = {tf: {} for tf in self.tfs}
        self._orderflow_dirty: Dict[str, bool] = {tf: True for tf in self.tfs}
        self._orderflow_boundary_runtime: Dict[str, Dict[str, Any]] = {tf: {} for tf in self.tfs}
        self._orderflow_event_log: Dict[str, collections.deque] = {tf: collections.deque(maxlen=400) for tf in self.tfs}
        self._orderflow_intrabar_runtime: Dict[str, Dict[str, Any]] = {tf: {} for tf in self.tfs}
        self._orderflow_last_event_signature: Dict[str, Optional[Tuple[Any, ...]]] = {tf: None for tf in self.tfs}
        self._orderflow_last_event_summary: Dict[str, str] = {tf: "" for tf in self.tfs}
        self._orderflow_event_seq: int = 0

        # Feature-flagged order-flow modes:
        #   OFF                 -> no tape / no DOM
        #   TAPE_ONLY           -> aggTrade only
        #   PARTIAL_DOM         -> aggTrade + partial depth snapshots
        #   LOCAL_L2            -> aggTrade + true local book (REST snapshot + diff-depth)
        #   AUTO                -> LOCAL_L2 with partial-depth fallback until sync succeeds
        raw_mode = str(orderflow_mode or os.getenv("PHASE19_ORDERFLOW_MODE", "AUTO")).upper().strip()
        allowed_modes = {"OFF", "TAPE_ONLY", "PARTIAL_DOM", "LOCAL_L2", "AUTO"}
        self.orderflow_mode = raw_mode if raw_mode in allowed_modes else "AUTO"
        self.orderflow_book_limit = int(max(100, min(1000, int(orderflow_book_limit))))
        self.orderflow_top_levels = int(max(10, min(50, int(orderflow_top_levels))))
        self._orderflow_tape_enabled = self.orderflow_mode not in ("OFF",)
        self._orderflow_partial_enabled = self.orderflow_mode in ("PARTIAL_DOM", "AUTO")
        self._orderflow_local_l2_enabled = self.orderflow_mode in ("LOCAL_L2", "AUTO")
        self._orderflow_local_book: Optional[LocalOrderBook] = (
            LocalOrderBook(symbol=self.symbol, snapshot_limit=self.orderflow_book_limit, keep_levels=max(self.orderflow_top_levels, 50))
            if self._orderflow_local_l2_enabled
            else None
        )
        self._orderflow_sync_lock = threading.Lock()
        self._orderflow_sync_inflight = False
        self._orderflow_partial_last_ms: Optional[int] = None
        self._orderflow_partial_updates: int = 0
        self._orderflow_dom_meta: Dict[str, Any] = {
            "of_feature_mode": self.orderflow_mode,
            "of_dom_source": ("NONE" if self.orderflow_mode in ("OFF", "TAPE_ONLY") else ("PARTIAL_DEPTH" if self._orderflow_partial_enabled and not self._orderflow_local_l2_enabled else "LOCAL_L2")),
            "of_dom_sync_state": ("DISABLED" if not self._orderflow_local_l2_enabled else "INIT"),
            "of_dom_resync_required": bool(self._orderflow_local_l2_enabled),
            "of_dom_sync_note": ("order flow disabled" if self.orderflow_mode == "OFF" else ("DOM disabled; tape-only mode" if self.orderflow_mode == "TAPE_ONLY" else "waiting for local book sync")),
            "of_dom_partial_fallback_enabled": bool(self._orderflow_partial_enabled),
            "of_dom_partial_fallback_active": bool(self._orderflow_partial_enabled and not self._orderflow_local_l2_enabled),
            "of_dom_partial_connected": False,
            "of_dom_partial_updates_total": 0,
            "of_dom_local_l2_enabled": bool(self._orderflow_local_l2_enabled),
            "of_dom_local_book_synced": False,
            "of_dom_local_snapshot_id": None,
            "of_dom_local_last_event_u": None,
            "of_dom_local_last_event_pu": None,
            "of_dom_local_buffered_events": 0,
            "of_dom_local_updates_applied": 0,
            "of_dom_book_levels_bid": None,
            "of_dom_book_levels_ask": None,
            "of_dom_depth_age_ms": None,
            "of_dom_snapshot_age_ms": None,
        }
        # Phase 21 / Delivery B: freshness, integrity, and confidence gating.
        # These thresholds are intentionally conservative because the order-flow layer
        # should never confirm a breakout with stale or damaged live data.
        self._orderflow_tape_good_ms: int = 1500
        self._orderflow_tape_degraded_ms: int = 5000
        self._orderflow_dom_good_ms: int = 1200
        self._orderflow_dom_degraded_ms: int = 3500
        self._orderflow_snapshot_stale_ms: int = 15000

        # Phase 24 / Delivery E: controlled live integration policy.
        # This affects only the LIVE breakout-trigger interpretation layer.
        # It does not touch indicator math and does not touch backtest.
        #
        #   ADVISORY -> order flow only annotates readiness; never blocks or enables
        #   SOFT     -> keep current hybrid behavior (confirm requires pass; watch can be helped)
        #   HARD     -> breakout trigger must have both structural readiness and OF pass
        raw_integration_mode = str(orderflow_integration_mode or os.getenv("PHASE24_OF_INTEGRATION_MODE", "SOFT")).upper().strip()
        allowed_integration_modes = {"ADVISORY", "SOFT", "HARD"}
        self.orderflow_integration_mode = raw_integration_mode if raw_integration_mode in allowed_integration_modes else "SOFT"

    def log(self, msg: str):
        self.q.put(("live_log", msg))

    @staticmethod
    def _coerce_positive_length(value: Any, fallback: int) -> int:
        try:
            return max(1, int(value or fallback))
        except Exception:
            return int(fallback)

    def update_indicator_settings(
        self,
        *,
        ema_1_length: int,
        ema_2_length: int,
        ema_lengths_by_tf: Optional[Dict[str, Dict[str, int]]] = None,
        rsi_length: int,
        rsi_smoothing: int,
        rsi_settings_by_tf: Optional[Dict[str, Dict[str, int]]] = None,
    ) -> None:
        with self.lock:
            self.ema_1_length = self._coerce_positive_length(ema_1_length, self.ema_1_length)
            self.ema_2_length = self._coerce_positive_length(ema_2_length, self.ema_2_length)

            next_ema_lengths_by_tf: Dict[str, Dict[str, int]] = {}
            raw_ema_lengths = ema_lengths_by_tf if isinstance(ema_lengths_by_tf, dict) else {}
            for tf in self.tfs:
                raw_pair = raw_ema_lengths.get(tf, {})
                next_ema_lengths_by_tf[tf] = {
                    "ema_1_length": self._coerce_positive_length(
                        raw_pair.get("ema_1_length", self.ema_1_length),
                        self.ema_1_length,
                    ),
                    "ema_2_length": self._coerce_positive_length(
                        raw_pair.get("ema_2_length", self.ema_2_length),
                        self.ema_2_length,
                    ),
                }
            self.ema_lengths_by_tf = next_ema_lengths_by_tf

            self.rsi_length = self._coerce_positive_length(rsi_length, self.rsi_length)
            self.rsi_smoothing = self._coerce_positive_length(rsi_smoothing, self.rsi_smoothing)

            next_rsi_settings_by_tf: Dict[str, Dict[str, int]] = {}
            raw_rsi_settings = rsi_settings_by_tf if isinstance(rsi_settings_by_tf, dict) else {}
            for tf in self.tfs:
                raw_cfg = raw_rsi_settings.get(tf, {})
                next_rsi_settings_by_tf[tf] = {
                    "length": self._coerce_positive_length(
                        raw_cfg.get("length", self.rsi_length),
                        self.rsi_length,
                    ),
                    "smoothing": self._coerce_positive_length(
                        raw_cfg.get("smoothing", self.rsi_smoothing),
                        self.rsi_smoothing,
                    ),
                }
            self.rsi_settings_by_tf = next_rsi_settings_by_tf

        self.log("[Live] Hot-updated EMA/RSI settings without restarting the live engine.")

    def _effective_forming_close(
        self,
        tf: str,
        live_price: Any,
        forming_snapshot: Optional[Tuple[Any, Any, Any]] = None,
    ) -> Optional[float]:
        try:
            live_price_f = float(live_price) if live_price is not None else None
            if live_price_f is not None and not np.isfinite(live_price_f):
                live_price_f = None
        except Exception:
            live_price_f = None

        if not self.live_use_forming:
            return live_price_f

        try:
            snapshot = forming_snapshot
            if snapshot is None:
                with self.lock:
                    st = self.storage.get(str(tf))
                    snapshot = (
                        getattr(st, "forming_high", None),
                        getattr(st, "forming_low", None),
                        getattr(st, "forming_close", None),
                    )
            _, _, forming_close = snapshot or (None, None, None)
            forming_close_f = float(forming_close) if forming_close is not None else None
            if forming_close_f is not None and np.isfinite(forming_close_f):
                return forming_close_f
        except Exception:
            pass

        return live_price_f

    def _full_closes_for_tf(
        self,
        tf: str,
        closes: List[float],
        live_price: Any,
        forming_snapshot: Optional[Tuple[Any, Any, Any]] = None,
    ) -> List[float]:
        series = list(closes or [])
        if not self.live_use_forming:
            return series
        effective_close = self._effective_forming_close(tf, live_price, forming_snapshot)
        if effective_close is None:
            return series
        return series + [float(effective_close)]

    def _forming_taker_snapshot(self, tf: str) -> Tuple[Optional[float], Optional[int], Optional[float]]:
        try:
            with self.lock:
                st = self.storage.get(str(tf))
                if st is None:
                    return None, None, None
                volume = getattr(st, "forming_volume", None)
                trades = getattr(st, "forming_trade_count", None)
                taker_buy = getattr(st, "forming_taker_buy_volume", None)
            volume_f = float(volume) if volume is not None else None
            trades_i = int(trades) if trades is not None else None
            taker_buy_f = float(taker_buy) if taker_buy is not None else None
            if volume_f is not None and not np.isfinite(volume_f):
                volume_f = None
            if taker_buy_f is not None and not np.isfinite(taker_buy_f):
                taker_buy_f = None
            return volume_f, trades_i, taker_buy_f
        except Exception:
            return None, None, None

    def _build_taker_bias_payload(
        self,
        tf: str,
        *,
        volume: Any = None,
        trade_count: Any = None,
        taker_buy_volume: Any = None,
    ) -> Dict[str, Any]:
        if volume is None and trade_count is None and taker_buy_volume is None and self.live_use_forming:
            volume, trade_count, taker_buy_volume = self._forming_taker_snapshot(tf)
        if volume is None and trade_count is None and taker_buy_volume is None:
            latest = dict(self._latest_closed_taker_metrics.get(str(tf), {}) or {})
            volume = latest.get("volume")
            trade_count = latest.get("trade_count")
            taker_buy_volume = latest.get("taker_buy_volume")
        return compute_taker_bias_payload(volume, taker_buy_volume, trade_count)

    def _mark_orderflow_dirty(self, tf: Optional[str] = None) -> None:
        if tf is None:
            for key in list((self._orderflow_windows or {}).keys()):
                self._orderflow_dirty[key] = True
            return
        self._orderflow_dirty[str(tf)] = True

    def _update_orderflow_trade(self, ts_ms: int, price: float, qty: float, is_buyer_maker: bool) -> None:
        if not self._orderflow_tape_enabled:
            return
        try:
            t = int(ts_ms)
            p = float(price)
            q = float(qty)
        except Exception:
            return
        for tf, win in (self._orderflow_windows or {}).items():
            try:
                win.add_trade(t, p, q, bool(is_buyer_maker))
                self._mark_orderflow_dirty(tf)
            except Exception:
                continue

    def _schedule_orderflow_local_book_sync(self, reason: str = "manual") -> None:
        if not self._orderflow_local_l2_enabled or self._orderflow_local_book is None:
            return
        with self._orderflow_sync_lock:
            if self._orderflow_sync_inflight:
                return
            self._orderflow_sync_inflight = True

        def _worker() -> None:
            try:
                self.log(f"[OrderFlow] Local L2 sync start ({reason})")
                url = f"{FAPI_BASE}/fapi/v1/depth"
                resp = requests.get(
                    url,
                    params={"symbol": self.symbol.upper(), "limit": int(self.orderflow_book_limit)},
                    timeout=5,
                )
                resp.raise_for_status()
                payload = resp.json() or {}
                last_update_id = payload.get("lastUpdateId")
                bids = payload.get("bids") or []
                asks = payload.get("asks") or []
                snap_ts_ms = int(time.time() * 1000)
                ok = self._orderflow_local_book.initialize_from_snapshot(
                    last_update_id=last_update_id,
                    bids=bids,
                    asks=asks,
                    snapshot_ts_ms=snap_ts_ms,
                )
                self._refresh_orderflow_dom_meta(now_ms=snap_ts_ms)
                if ok and self._orderflow_local_book.synced:
                    self._push_local_book_snapshot_into_windows(snap_ts_ms)
                    self.log("[OrderFlow] Local L2 sync complete")
                else:
                    self.log(f"[OrderFlow] Local L2 snapshot loaded but not synced yet: {self._orderflow_local_book.sync_note}")
            except Exception as e:
                if self._orderflow_local_book is not None:
                    self._orderflow_local_book.reset(note=f"snapshot fetch failed: {e}")
                    self._refresh_orderflow_dom_meta(now_ms=int(time.time() * 1000))
                self.log(f"[OrderFlow] Local L2 sync failed: {e}")
            finally:
                with self._orderflow_sync_lock:
                    self._orderflow_sync_inflight = False

        threading.Thread(target=_worker, daemon=True).start()

    def _refresh_orderflow_dom_meta(self, now_ms: Optional[int] = None) -> None:
        current_ms = int(now_ms) if now_ms is not None else int(time.time() * 1000)
        partial_connected = bool(self._orderflow_partial_last_ms is not None)
        partial_active = bool(self._orderflow_partial_enabled and (not self._orderflow_local_l2_enabled or not (self._orderflow_local_book and self._orderflow_local_book.synced)))
        meta: Dict[str, Any] = {
            "of_feature_mode": self.orderflow_mode,
            "of_dom_partial_fallback_enabled": bool(self._orderflow_partial_enabled),
            "of_dom_partial_fallback_active": bool(partial_active),
            "of_dom_partial_connected": bool(partial_connected),
            "of_dom_partial_updates_total": int(self._orderflow_partial_updates),
            "of_dom_local_l2_enabled": bool(self._orderflow_local_l2_enabled),
        }
        if self.orderflow_mode == "OFF":
            meta.update(
                {
                    "of_dom_source": "NONE",
                    "of_dom_sync_state": "DISABLED",
                    "of_dom_resync_required": False,
                    "of_dom_sync_note": "order flow disabled",
                    "of_dom_local_book_synced": False,
                }
            )
        elif self.orderflow_mode == "TAPE_ONLY":
            meta.update(
                {
                    "of_dom_source": "NONE",
                    "of_dom_sync_state": "DISABLED",
                    "of_dom_resync_required": False,
                    "of_dom_sync_note": "DOM disabled; tape-only mode",
                    "of_dom_local_book_synced": False,
                }
            )
        elif self._orderflow_local_l2_enabled and self._orderflow_local_book is not None:
            status = self._orderflow_local_book.status_dict(now_ms=current_ms)
            source = "LOCAL_L2" if status.get("synced") else ("PARTIAL_DEPTH" if partial_active else "LOCAL_L2")
            sync_note = str(status.get("sync_note") or "")
            if partial_active and not status.get("synced"):
                sync_note = (sync_note + "; using partial-depth fallback").strip("; ")
            meta.update(
                {
                    "of_dom_source": source,
                    "of_dom_sync_state": str(status.get("sync_state") or "INIT"),
                    "of_dom_resync_required": bool(status.get("resync_required")),
                    "of_dom_sync_note": sync_note,
                    "of_dom_local_book_synced": bool(status.get("synced")),
                    "of_dom_local_snapshot_id": status.get("last_snapshot_update_id"),
                    "of_dom_local_last_event_u": status.get("last_event_u"),
                    "of_dom_local_last_event_pu": status.get("last_event_pu"),
                    "of_dom_local_buffered_events": status.get("buffered_events"),
                    "of_dom_local_updates_applied": status.get("events_applied"),
                    "of_dom_book_levels_bid": status.get("book_levels_bid"),
                    "of_dom_book_levels_ask": status.get("book_levels_ask"),
                    "of_dom_depth_age_ms": status.get("depth_age_ms"),
                    "of_dom_snapshot_age_ms": status.get("snapshot_age_ms"),
                }
            )
        else:
            meta.update(
                {
                    "of_dom_source": "PARTIAL_DEPTH",
                    "of_dom_sync_state": ("PARTIAL_ONLY" if partial_connected else "WAITING_PARTIAL"),
                    "of_dom_resync_required": False,
                    "of_dom_sync_note": ("using partial-depth DOM mode" if partial_connected else "waiting for partial-depth updates"),
                    "of_dom_local_book_synced": False,
                }
            )
        self._orderflow_dom_meta = meta

    def _classify_orderflow_age(self, age_ms: Optional[int], *, good_ms: int, degraded_ms: int, disabled: bool = False) -> Tuple[str, int]:
        return classify_orderflow_age(age_ms, good_ms=good_ms, degraded_ms=degraded_ms, disabled=disabled)

    def _build_orderflow_quality_overlay(self, snap: Dict[str, Any], now_ms: int) -> Dict[str, Any]:
        return build_orderflow_quality_overlay(
            snap,
            now_ms=int(now_ms),
            mode=str(self.orderflow_mode or "AUTO").upper().strip() or "AUTO",
            dom_good_ms=int(self._orderflow_dom_good_ms),
            dom_degraded_ms=int(self._orderflow_dom_degraded_ms),
            tape_good_ms=int(self._orderflow_tape_good_ms),
            tape_degraded_ms=int(self._orderflow_tape_degraded_ms),
            snapshot_stale_ms=int(self._orderflow_snapshot_stale_ms),
            tape_enabled=bool(self._orderflow_tape_enabled),
        )

    def _push_local_book_snapshot_into_windows(self, ts_ms: int) -> None:
        if self._orderflow_local_book is None or not self._orderflow_local_book.synced:
            return
        bids, asks = self._orderflow_local_book.export_top_levels(self.orderflow_top_levels)
        if not bids or not asks:
            return
        for tf, win in (self._orderflow_windows or {}).items():
            try:
                win.add_depth_snapshot(int(ts_ms), bids, asks)
                self._mark_orderflow_dirty(tf)
            except Exception:
                continue
        self._refresh_orderflow_dom_meta(now_ms=int(ts_ms))

    def _update_orderflow_depth_partial(self, ts_ms: int, bids: List[List[Any]], asks: List[List[Any]]) -> None:
        if not self._orderflow_partial_enabled:
            return
        try:
            t = int(ts_ms)
        except Exception:
            t = int(time.time() * 1000)
        self._orderflow_partial_last_ms = int(t)
        self._orderflow_partial_updates += 1
        local_synced = bool(self._orderflow_local_book and self._orderflow_local_book.synced)
        if self._orderflow_local_l2_enabled and local_synced:
            self._refresh_orderflow_dom_meta(now_ms=t)
            return
        for tf, win in (self._orderflow_windows or {}).items():
            try:
                win.add_depth_snapshot(t, bids, asks)
                self._mark_orderflow_dirty(tf)
            except Exception:
                continue
        self._refresh_orderflow_dom_meta(now_ms=t)

    def _update_orderflow_depth_diff(self, data: Dict[str, Any]) -> None:
        if not self._orderflow_local_l2_enabled or self._orderflow_local_book is None:
            return
        try:
            event_ms = int(data.get("E") or data.get("T") or int(time.time() * 1000))
            first_update_id = int(data.get("U"))
            final_update_id = int(data.get("u"))
            prev_final_update_id = data.get("pu")
            bids = data.get("b", []) or []
            asks = data.get("a", []) or []
        except Exception:
            return
        result = self._orderflow_local_book.add_diff_event(
            ts_ms=event_ms,
            first_update_id=first_update_id,
            final_update_id=final_update_id,
            prev_final_update_id=prev_final_update_id,
            bids=bids,
            asks=asks,
        )
        self._refresh_orderflow_dom_meta(now_ms=event_ms)
        if result == "APPLIED":
            self._push_local_book_snapshot_into_windows(event_ms)
            return
        if result in ("BUFFERED", "RESYNC_REQUIRED") and self._orderflow_local_book.resync_required:
            self._schedule_orderflow_local_book_sync(reason=("depth_gap" if result == "RESYNC_REQUIRED" else "initial_buffer"))

    def _current_orderflow_snapshot(self, tf: str, ref_price: Optional[float] = None) -> Dict[str, Any]:
        snap: Dict[str, Any] = {}
        try:
            win = (self._orderflow_windows or {}).get(tf)
            if win is not None:
                snap = win.snapshot(ref_price=ref_price)
                self._orderflow_last[tf] = dict(snap)
                self._orderflow_dirty[tf] = False
            else:
                snap = dict((self._orderflow_last or {}).get(tf) or {})
        except Exception:
            snap = dict((self._orderflow_last or {}).get(tf) or {})
        now_ms = int(time.time() * 1000)
        self._refresh_orderflow_dom_meta(now_ms=now_ms)
        snap.update(dict(self._orderflow_dom_meta or {}))
        snap.update(self._build_orderflow_quality_overlay(snap, now_ms))
        dom_source = str(snap.get("of_dom_source") or "NONE")
        if dom_source == "NONE":
            snap["of_dom_connected"] = False
            snap["of_dom_note"] = str(snap.get("of_dom_sync_note") or snap.get("of_dom_note") or "DOM disabled")
        elif dom_source == "PARTIAL_DEPTH" and bool(snap.get("of_dom_partial_connected")):
            snap["of_dom_connected"] = True
            if bool(snap.get("of_dom_partial_fallback_active")):
                snap["of_dom_note"] = str(snap.get("of_dom_sync_note") or "using partial-depth fallback while local L2 sync is pending")
            else:
                snap["of_dom_note"] = str(snap.get("of_dom_sync_note") or "using partial-depth DOM mode")
        elif dom_source == "LOCAL_L2" and bool(snap.get("of_dom_local_book_synced")):
            snap["of_dom_connected"] = True
            snap["of_dom_note"] = str(snap.get("of_dom_sync_note") or "true local L2 book is synced")
        else:
            snap["of_dom_connected"] = bool(snap.get("of_dom_connected", False))
            snap["of_dom_note"] = str(snap.get("of_dom_sync_note") or snap.get("of_dom_note") or "waiting for DOM sync")
        return snap

    def _build_breakout_boundary_context(self, tf: str, payload: Dict[str, Any]) -> Dict[str, Any]:
        out: Dict[str, Any] = {}
        try:
            rows = list(self._closed_ohlc_ring.get(tf, []))
        except Exception:
            rows = []

        try:
            tf_sec = max(60, int(interval_to_ms(tf) // 1000))
        except Exception:
            tf_sec = 60

        if tf_sec <= 300:
            window_bars = 28
        elif tf_sec <= 900:
            window_bars = 24
        elif tf_sec <= 3600:
            window_bars = 20
        else:
            window_bars = 18

        try:
            compression_maturity = float(payload.get("market_compression_maturity") or 0.0)
        except Exception:
            compression_maturity = 0.0
        if compression_maturity >= 70.0:
            window_bars = min(40, window_bars + 6)
        touch_window = max(8, min(window_bars, 12))

        if len(rows) < max(12, window_bars // 2):
            out.update({
                "market_boundary_source": "RING_BOX",
                "market_boundary_valid": False,
                "market_boundary_window_bars": int(window_bars),
                "market_boundary_touch_window_bars": int(touch_window),
                "market_boundary_note": "not enough closed bars for live compression boundary",
            })
            return out

        box_rows = rows[-window_bars:]
        highs = [float(r[1]) for r in box_rows]
        lows = [float(r[2]) for r in box_rows]
        closes = [float(r[3]) for r in box_rows]
        boundary_high = max(highs) if highs else float("nan")
        boundary_low = min(lows) if lows else float("nan")
        if not np.isfinite(boundary_high) or not np.isfinite(boundary_low) or boundary_high <= boundary_low:
            out.update({
                "market_boundary_source": "RING_BOX",
                "market_boundary_valid": False,
                "market_boundary_window_bars": int(window_bars),
                "market_boundary_touch_window_bars": int(touch_window),
                "market_boundary_note": "boundary box could not be derived from ring data",
            })
            return out

        width_abs = float(boundary_high - boundary_low)
        try:
            atr_eff = float(payload.get("atr") if payload.get("atr") is not None else np.nan)
        except Exception:
            atr_eff = float("nan")
        if not np.isfinite(atr_eff) or atr_eff <= 0.0:
            try:
                atr_eff = float(np.nanmean([abs(float(h) - float(l)) for _ct, h, l, _c, _ot in box_rows]))
            except Exception:
                atr_eff = float("nan")
        if not np.isfinite(atr_eff) or atr_eff <= 0.0:
            atr_eff = max(width_abs / max(len(box_rows), 1), abs(boundary_high) * 0.001, 1e-9)

        current_price = None
        try:
            current_price = float(payload.get("price") if payload.get("price") is not None else payload.get("close"))
        except Exception:
            current_price = None
        if current_price is None or not np.isfinite(float(current_price)):
            current_price = float(closes[-1]) if closes else float((boundary_high + boundary_low) / 2.0)
        st = self.storage.get(tf)
        current_high = None
        current_low = None
        try:
            if st is not None and st.forming_high is not None and np.isfinite(float(st.forming_high)):
                current_high = float(st.forming_high)
        except Exception:
            current_high = None
        try:
            if st is not None and st.forming_low is not None and np.isfinite(float(st.forming_low)):
                current_low = float(st.forming_low)
        except Exception:
            current_low = None
        if current_high is None:
            current_high = float(current_price)
        if current_low is None:
            current_low = float(current_price)

        near_margin_abs = float(max(0.18 * atr_eff, 0.07 * width_abs, 1e-9))
        outside_margin_abs = float(max(0.10 * atr_eff, 0.035 * width_abs, 1e-9))

        touch_rows = box_rows[-touch_window:]
        touch_upper = 0
        touch_lower = 0
        upper_touch_threshold = boundary_high - (near_margin_abs * 0.75)
        lower_touch_threshold = boundary_low + (near_margin_abs * 0.75)
        for _ct, h, l, c, _ot in touch_rows:
            try:
                if float(h) >= upper_touch_threshold or float(c) >= boundary_high - near_margin_abs:
                    touch_upper += 1
            except Exception:
                pass
            try:
                if float(l) <= lower_touch_threshold or float(c) <= boundary_low + near_margin_abs:
                    touch_lower += 1
            except Exception:
                pass
        if current_high >= upper_touch_threshold:
            touch_upper += 1
        if current_low <= lower_touch_threshold:
            touch_lower += 1

        dist_upper_abs = float(boundary_high - float(current_price))
        dist_lower_abs = float(float(current_price) - boundary_low)
        dist_upper_atr = float(dist_upper_abs / max(atr_eff, 1e-9))
        dist_lower_atr = float(dist_lower_abs / max(atr_eff, 1e-9))
        width_atr = float(width_abs / max(atr_eff, 1e-9))

        near_upper = bool(float(current_price) >= (boundary_high - near_margin_abs) or current_high >= upper_touch_threshold)
        near_lower = bool(float(current_price) <= (boundary_low + near_margin_abs) or current_low <= lower_touch_threshold)
        outside_above = bool(float(current_price) > (boundary_high + outside_margin_abs) or (current_high > (boundary_high + outside_margin_abs * 0.60) and float(current_price) >= boundary_high))
        outside_below = bool(float(current_price) < (boundary_low - outside_margin_abs) or (current_low < (boundary_low - outside_margin_abs * 0.60) and float(current_price) <= boundary_low))

        if outside_above:
            touch_bias = "LONG"
        elif outside_below:
            touch_bias = "SHORT"
        elif touch_upper > touch_lower:
            touch_bias = "LONG"
        elif touch_lower > touch_upper:
            touch_bias = "SHORT"
        else:
            touch_bias = "NEUTRAL"

        note = f"{window_bars}-bar compression box"
        if near_upper:
            note += f" | upper edge active ({touch_upper} touches)"
        elif near_lower:
            note += f" | lower edge active ({touch_lower} touches)"
        else:
            note += " | price inside the middle of the box"
        if outside_above:
            note += " | price is trading above the upper boundary"
        elif outside_below:
            note += " | price is trading below the lower boundary"

        out.update({
            "market_boundary_source": "RING_BOX",
            "market_boundary_valid": True,
            "market_boundary_note": note,
            "market_boundary_window_bars": int(window_bars),
            "market_boundary_touch_window_bars": int(touch_window),
            "market_boundary_high": float(boundary_high),
            "market_boundary_low": float(boundary_low),
            "market_boundary_width_abs": float(width_abs),
            "market_boundary_width_atr": float(width_atr),
            "market_boundary_near_margin_abs": float(near_margin_abs),
            "market_boundary_outside_margin_abs": float(outside_margin_abs),
            "market_boundary_dist_to_upper_atr": float(dist_upper_atr),
            "market_boundary_dist_to_lower_atr": float(dist_lower_atr),
            "market_boundary_touch_count_upper": int(touch_upper),
            "market_boundary_touch_count_lower": int(touch_lower),
            "market_boundary_touch_bias": str(touch_bias),
            "market_boundary_near_upper": bool(near_upper),
            "market_boundary_near_lower": bool(near_lower),
            "market_boundary_outside_above": bool(outside_above),
            "market_boundary_outside_below": bool(outside_below),
            "market_boundary_current_price": float(current_price),
            "market_boundary_current_high": float(current_high),
            "market_boundary_current_low": float(current_low),
            "market_boundary_break_progress_atr": float(max(0.0, (float(current_price) - boundary_high) / max(atr_eff, 1e-9)) if outside_above else max(0.0, (boundary_low - float(current_price)) / max(atr_eff, 1e-9)) if outside_below else 0.0),
        })
        return out

    def _apply_orderflow_breakout_filter(self, tf: str, state_payload: Dict[str, Any], of_snap: Dict[str, Any]) -> Dict[str, Any]:
        out = dict(state_payload or {})
        snap = dict(of_snap or {})
        if snap:
            out.update(snap)

        boundary_overlay, boundary_runtime = evaluate_boundary_aware_breakout(out, prev_runtime=self._orderflow_boundary_runtime.get(str(tf), {}))
        out.update(boundary_overlay)
        self._orderflow_boundary_runtime[str(tf)] = dict(boundary_runtime or {})

        breakout_state = str(out.get("market_breakout_state") or "NONE").upper().strip() or "NONE"
        breakout_bias = str(out.get("market_breakout_bias") or "NEUTRAL").upper().strip() or "NEUTRAL"
        of_state = str(out.get("of_state") or "NEUTRAL").upper().strip() or "NEUTRAL"

        flow_watch_long = bool(out.get("of_breakout_watch_long"))
        flow_watch_short = bool(out.get("of_breakout_watch_short"))
        flow_confirm_long = bool(out.get("of_breakout_confirm_long"))
        flow_confirm_short = bool(out.get("of_breakout_confirm_short"))
        absorption_long = bool(out.get("of_absorption_long"))
        absorption_short = bool(out.get("of_absorption_short"))
        watch_allowed = bool(out.get("of_watch_allowed"))
        confirmation_allowed = bool(out.get("of_confirmation_allowed"))
        watch_lock_reason = str(out.get("of_watch_lock_reason") or "order-flow watch data is not ready")
        confirmation_lock_reason = str(out.get("of_confirmation_lock_reason") or "order-flow confirmation data is not ready")

        dom_connected = bool(out.get("of_dom_connected"))
        dom_watch_long = bool(out.get("of_dom_breakout_watch_long"))
        dom_watch_short = bool(out.get("of_dom_breakout_watch_short"))
        dom_confirm_long = bool(out.get("of_dom_breakout_confirm_long"))
        dom_confirm_short = bool(out.get("of_dom_breakout_confirm_short"))
        dom_bias = str(out.get("of_dom_bias") or "NEUTRAL").upper().strip() or "NEUTRAL"

        filter_gate = "NONE"
        filter_pass = False
        filter_note = "order flow neutral"

        long_watch_support = bool(flow_watch_long or flow_confirm_long or dom_watch_long or dom_confirm_long)
        short_watch_support = bool(flow_watch_short or flow_confirm_short or dom_watch_short or dom_confirm_short)
        long_confirm_support = bool((flow_confirm_long and (dom_watch_long or dom_confirm_long)) or (dom_confirm_long and (flow_watch_long or flow_confirm_long)))
        short_confirm_support = bool((flow_confirm_short and (dom_watch_short or dom_confirm_short)) or (dom_confirm_short and (flow_watch_short or flow_confirm_short)))

        boundary_valid = bool(out.get("of_boundary_valid"))
        boundary_state = str(out.get("of_boundary_state") or "BOUNDARY_NEUTRAL").upper().strip() or "BOUNDARY_NEUTRAL"
        boundary_note = str(out.get("of_boundary_note") or out.get("market_boundary_note") or "boundary not ready")
        boundary_watch_long = bool(out.get("of_boundary_watch_long"))
        boundary_watch_short = bool(out.get("of_boundary_watch_short"))
        boundary_confirm_long = bool(out.get("of_boundary_confirm_long"))
        boundary_confirm_short = bool(out.get("of_boundary_confirm_short"))
        boundary_failed_long = bool(out.get("of_boundary_failed_long"))
        boundary_failed_short = bool(out.get("of_boundary_failed_short"))
        boundary_acceptance_long = bool(out.get("of_boundary_acceptance_long"))
        boundary_acceptance_short = bool(out.get("of_boundary_acceptance_short"))

        if breakout_state in ("BREAKOUT_CONFIRM_LONG", "BREAKOUT_CONFIRM_SHORT", "BREAKOUT_WATCH_LONG", "BREAKOUT_WATCH_SHORT", "BREAKOUT_READY"):
            if breakout_bias == "LONG":
                if breakout_state == "BREAKOUT_CONFIRM_LONG":
                    filter_gate = "CONFIRM"
                    filter_pass = bool(boundary_confirm_long and long_confirm_support and confirmation_allowed)
                    if not confirmation_allowed:
                        filter_note = str(confirmation_lock_reason)
                    elif not boundary_valid:
                        filter_note = str(boundary_note)
                    elif boundary_failed_long:
                        filter_note = "long-side break slipped back inside the compression box"
                    elif absorption_long:
                        filter_note = "buy absorption blocks breakout confirmation at the upper boundary"
                    elif filter_pass:
                        filter_note = f"upper-boundary release is holding outside the box | {boundary_note}"
                    elif flow_confirm_long and not dom_connected and boundary_confirm_long:
                        filter_note = f"tape confirms long release at the boundary; DOM feed not connected | {boundary_note}"
                        filter_pass = True
                    elif not boundary_confirm_long:
                        filter_note = f"long flow exists, but price is not holding above the compression boundary yet | {boundary_note}"
                    elif flow_confirm_long:
                        filter_note = f"tape confirms, but DOM still does not support the long release | {boundary_note}"
                    elif dom_confirm_long:
                        filter_note = f"DOM confirms, but tape still does not support the long release | {boundary_note}"
                    else:
                        filter_note = f"order flow does not confirm the long release yet | {boundary_note}"
                else:
                    filter_gate = "WATCH"
                    filter_pass = bool(boundary_watch_long and long_watch_support and watch_allowed)
                    if not watch_allowed:
                        filter_note = str(watch_lock_reason)
                    elif not boundary_valid:
                        filter_note = str(boundary_note)
                    elif absorption_long:
                        filter_note = "buy absorption is present at the upper boundary"
                    elif filter_pass:
                        filter_note = f"upper compression edge is under pressure with supportive tape/DOM | {boundary_note}"
                    elif boundary_acceptance_long:
                        filter_note = f"price is leaning outside the upper edge, but the release is not mature enough yet | {boundary_note}"
                    elif flow_watch_long and dom_watch_long and not boundary_watch_long:
                        filter_note = f"flow is leaning long, but not at the correct boundary yet | {boundary_note}"
                    elif flow_watch_long:
                        filter_note = f"tape supports long pressure, but the upper boundary still needs more tests/hold | {boundary_note}"
                    elif dom_watch_long:
                        filter_note = f"DOM supports long pressure, but price is not yet at a strong upper-edge release | {boundary_note}"
                    elif dom_bias == "LONG":
                        filter_note = f"DOM leans long, but the breakout watch still needs stronger tape support at the upper boundary | {boundary_note}"
                    else:
                        filter_note = f"order flow stays neutral against the long watch | {boundary_note}"
            elif breakout_bias == "SHORT":
                if breakout_state == "BREAKOUT_CONFIRM_SHORT":
                    filter_gate = "CONFIRM"
                    filter_pass = bool(boundary_confirm_short and short_confirm_support and confirmation_allowed)
                    if not confirmation_allowed:
                        filter_note = str(confirmation_lock_reason)
                    elif not boundary_valid:
                        filter_note = str(boundary_note)
                    elif boundary_failed_short:
                        filter_note = "short-side break slipped back inside the compression box"
                    elif absorption_short:
                        filter_note = "sell absorption blocks breakout confirmation at the lower boundary"
                    elif filter_pass:
                        filter_note = f"lower-boundary release is holding outside the box | {boundary_note}"
                    elif flow_confirm_short and not dom_connected and boundary_confirm_short:
                        filter_note = f"tape confirms short release at the boundary; DOM feed not connected | {boundary_note}"
                        filter_pass = True
                    elif not boundary_confirm_short:
                        filter_note = f"short flow exists, but price is not holding below the compression boundary yet | {boundary_note}"
                    elif flow_confirm_short:
                        filter_note = f"tape confirms, but DOM still does not support the short release | {boundary_note}"
                    elif dom_confirm_short:
                        filter_note = f"DOM confirms, but tape still does not support the short release | {boundary_note}"
                    else:
                        filter_note = f"order flow does not confirm the short release yet | {boundary_note}"
                else:
                    filter_gate = "WATCH"
                    filter_pass = bool(boundary_watch_short and short_watch_support and watch_allowed)
                    if not watch_allowed:
                        filter_note = str(watch_lock_reason)
                    elif not boundary_valid:
                        filter_note = str(boundary_note)
                    elif absorption_short:
                        filter_note = "sell absorption is present at the lower boundary"
                    elif filter_pass:
                        filter_note = f"lower compression edge is under pressure with supportive tape/DOM | {boundary_note}"
                    elif boundary_acceptance_short:
                        filter_note = f"price is leaning outside the lower edge, but the release is not mature enough yet | {boundary_note}"
                    elif flow_watch_short and dom_watch_short and not boundary_watch_short:
                        filter_note = f"flow is leaning short, but not at the correct boundary yet | {boundary_note}"
                    elif flow_watch_short:
                        filter_note = f"tape supports short pressure, but the lower boundary still needs more tests/hold | {boundary_note}"
                    elif dom_watch_short:
                        filter_note = f"DOM supports short pressure, but price is not yet at a strong lower-edge release | {boundary_note}"
                    elif dom_bias == "SHORT":
                        filter_note = f"DOM leans short, but the breakout watch still needs stronger tape support at the lower boundary | {boundary_note}"
                    else:
                        filter_note = f"order flow stays neutral against the short watch | {boundary_note}"
            else:
                filter_gate = "WATCH"
                filter_pass = bool(((boundary_watch_long and long_watch_support) or (boundary_watch_short and short_watch_support)) and watch_allowed)
                if not watch_allowed:
                    filter_note = str(watch_lock_reason)
                elif filter_pass:
                    filter_note = f"order flow sees directional pressure at the compression edge | {boundary_note}"
                elif not boundary_valid:
                    filter_note = str(boundary_note)
                else:
                    filter_note = f"breakout watch has no directional order-flow support at the live boundary yet | {boundary_note}"

        out["market_breakout_filter_gate"] = filter_gate
        out["market_breakout_filter_pass"] = bool(filter_pass)
        out["market_breakout_filter_note"] = str(filter_note)

        existing_note = str(out.get("market_breakout_note") or "").strip()
        if filter_gate != "NONE":
            if existing_note:
                out["market_breakout_note"] = f"{existing_note} | {filter_note}"
            else:
                out["market_breakout_note"] = str(filter_note)

        base_trigger_ready = bool(out.get("market_breakout_trigger_ready"))
        out["market_breakout_trigger_ready_raw"] = bool(base_trigger_ready)
        integration_mode = str(getattr(self, "orderflow_integration_mode", "SOFT") or "SOFT").upper().strip() or "SOFT"
        integration_effect = "NONE"
        integration_note = "order-flow integration idle"
        integration_blocked = False
        integration_pass = bool(filter_pass)

        if integration_mode == "ADVISORY":
            effective_trigger_ready = bool(base_trigger_ready)
            if filter_gate != "NONE":
                integration_effect = ("ADVISORY_PASS" if filter_pass else "ADVISORY_NOTE")
                if filter_pass:
                    integration_note = f"advisory only: breakout stays structurally ready and order flow agrees | {filter_note}"
                else:
                    integration_note = f"advisory only: breakout stays structural; order flow does not confirm yet | {filter_note}"
            else:
                integration_note = "advisory only: no active order-flow gate"
        elif integration_mode == "HARD":
            if filter_gate != "NONE":
                effective_trigger_ready = bool(base_trigger_ready and filter_pass)
                integration_blocked = bool(base_trigger_ready and not filter_pass)
                integration_effect = ("HARD_PASS" if effective_trigger_ready else "HARD_BLOCK")
                if effective_trigger_ready:
                    integration_note = f"hard mode: structural breakout and order-flow gate both agree | {filter_note}"
                elif base_trigger_ready:
                    integration_note = f"hard mode blocks the live breakout trigger until order flow confirms | {filter_note}"
                else:
                    integration_note = f"hard mode stays idle because structural breakout is not ready yet | {filter_note}"
            else:
                effective_trigger_ready = bool(base_trigger_ready)
                integration_note = "hard mode: no active order-flow gate, leaving structural breakout state unchanged"
        else:
            if breakout_state in ("BREAKOUT_CONFIRM_LONG", "BREAKOUT_CONFIRM_SHORT"):
                effective_trigger_ready = bool(base_trigger_ready and filter_pass)
                integration_blocked = bool(base_trigger_ready and not filter_pass)
                integration_effect = ("SOFT_CONFIRM_PASS" if effective_trigger_ready else "SOFT_CONFIRM_BLOCK")
                if effective_trigger_ready:
                    integration_note = f"soft mode: confirmed breakout stays ready because order flow agrees | {filter_note}"
                elif base_trigger_ready:
                    integration_note = f"soft mode blocks confirmed breakout until order flow confirms | {filter_note}"
                else:
                    integration_note = f"soft mode: structural confirmed breakout is not ready yet | {filter_note}"
            elif breakout_state in ("BREAKOUT_WATCH_LONG", "BREAKOUT_WATCH_SHORT", "BREAKOUT_READY"):
                effective_trigger_ready = bool(base_trigger_ready or filter_pass)
                integration_effect = ("SOFT_WATCH_READY" if effective_trigger_ready else "SOFT_WATCH_WAIT")
                if filter_pass and not base_trigger_ready:
                    integration_note = f"soft mode elevates the breakout watch because order flow supports the boundary | {filter_note}"
                elif base_trigger_ready and not filter_pass:
                    integration_note = f"soft mode keeps the watch active on structure even without full order-flow pass | {filter_note}"
                elif effective_trigger_ready:
                    integration_note = f"soft mode keeps the breakout watch live with supportive order flow | {filter_note}"
                else:
                    integration_note = f"soft mode: breakout watch is not ready yet | {filter_note}"
            else:
                effective_trigger_ready = bool(base_trigger_ready)
                integration_note = "soft mode: no breakout watch/confirm gate is active"

        out["of_integration_mode"] = str(integration_mode)
        out["of_integration_effect"] = str(integration_effect)
        out["of_integration_note"] = str(integration_note)
        out["of_integration_blocked"] = bool(integration_blocked)
        out["of_integration_pass"] = bool(integration_pass)
        out["market_breakout_trigger_ready_filtered"] = bool(effective_trigger_ready)
        out["market_breakout_trigger_ready"] = bool(effective_trigger_ready)

        if filter_gate != "NONE":
            out["of_filter_pass"] = bool(filter_pass)
            out["of_filter_note"] = str(filter_note)
            if of_state in ("NEUTRAL", "NO_DATA") and breakout_bias in ("LONG", "SHORT"):
                out["of_state_pretty"] = f"OF {str(breakout_bias).title()} Not Confirmed"

        out["of_watch_lock_active"] = bool(filter_gate == "WATCH" and not filter_pass and not watch_allowed)
        out["of_confirmation_lock_active"] = bool(filter_gate == "CONFIRM" and not filter_pass and not confirmation_allowed)

        last_events = self._orderflow_event_log.get(str(tf)) or []
        if last_events:
            last_evt = last_events[-1]
            out["of_last_event_summary"] = str(last_evt.get("summary") or "")
            out["of_last_event_type"] = str(last_evt.get("event_type") or "")
            out["of_last_event_ts_ms"] = int(last_evt.get("ts_ms") or 0)
            out["of_event_count"] = int(len(last_events))
        else:
            out["of_last_event_summary"] = str(self._orderflow_last_event_summary.get(str(tf)) or "")
            out["of_last_event_type"] = str("")
            out["of_last_event_ts_ms"] = int(0)
            out["of_event_count"] = 0

        self._record_orderflow_event(tf, out)
        last_events = self._orderflow_event_log.get(str(tf)) or []
        if last_events:
            last_evt = last_events[-1]
            out["of_last_event_summary"] = str(last_evt.get("summary") or "")
            out["of_last_event_type"] = str(last_evt.get("event_type") or "")
            out["of_last_event_ts_ms"] = int(last_evt.get("ts_ms") or 0)
            out["of_event_count"] = int(len(last_events))
        return out


    def _apply_orderflow_execution_risk(self, tf: str, state_payload: Dict[str, Any]) -> Dict[str, Any]:
        """Apply a live execution-hazard layer from tape/DOM/order-flow.

        This is intentionally additive: it annotates the existing market-state
        snapshot with OF risk fields and lightly discounts confidence when the
        live microstructure is hostile. It does not rewrite indicator math,
        backtest logic, or the core market-state classifier.
        """
        out = dict(state_payload or {})
        tf = str(tf)

        def _s(key: str, default: str = "") -> str:
            try:
                val = out.get(key)
            except Exception:
                val = None
            txt = str(val if val is not None else default).upper().strip()
            return txt or str(default).upper().strip()

        def _f(key: str, default: float = 0.0) -> float:
            try:
                val = out.get(key)
                if val is None:
                    return float(default)
                num = float(val)
                return num if np.isfinite(num) else float(default)
            except Exception:
                return float(default)

        rt_prev = dict(self._orderflow_intrabar_runtime.get(tf) or {})
        bar_marker = out.get("bar_close_time_ms")
        if rt_prev.get("bar_close_time_ms") != bar_marker:
            rt: Dict[str, Any] = {
                "bar_close_time_ms": bar_marker,
                "hostile_long_hits": 0,
                "hostile_short_hits": 0,
                "absorption_long_hits": 0,
                "absorption_short_hits": 0,
                "flip_count": 0,
                "block_count": 0,
                "last_direction": "NEUTRAL",
                "prev_hostile_long": False,
                "prev_hostile_short": False,
                "prev_abs_long": False,
                "prev_abs_short": False,
                "prev_blocked": False,
            }
        else:
            rt = dict(rt_prev)

        orderflow_mode = _s("of_feature_mode", getattr(self, "orderflow_mode", "AUTO") or "AUTO")
        data_quality = _s("of_data_quality", "UNKNOWN")
        market_bias = _s("market_bias", "NEUTRAL")
        breakout_bias = _s("market_breakout_bias", "NEUTRAL")
        of_bias = _s("of_bias", "NEUTRAL")
        dom_bias = _s("of_dom_bias", "NEUTRAL")
        of_state = _s("of_state", "NEUTRAL")
        dom_state = _s("of_dom_state", "NO_DATA")
        integration_mode = _s("of_integration_mode", getattr(self, "orderflow_integration_mode", "SOFT") or "SOFT")

        absorption_long = bool(out.get("of_absorption_long"))
        absorption_short = bool(out.get("of_absorption_short"))
        filter_pass = bool(out.get("market_breakout_filter_pass") or out.get("of_filter_pass"))
        integration_blocked = bool(out.get("of_integration_blocked"))
        watch_allowed = bool(out.get("of_watch_allowed"))
        confirmation_allowed = bool(out.get("of_confirmation_allowed"))

        delta_pct = _f("of_delta_pct", 0.0)
        progress_bps = _f("of_progress_bps", 0.0)
        dom_pressure_pct = _f("of_dom_pressure_pct", 0.0)
        dom_pressure_accel = _f("of_dom_pressure_accel", 0.0)
        dom_bid_stack_ratio = _f("of_dom_bid_stack_ratio", 1.0)
        dom_ask_stack_ratio = _f("of_dom_ask_stack_ratio", 1.0)
        dom_bid_pull_ratio = _f("of_dom_bid_pull_ratio", 1.0)
        dom_ask_pull_ratio = _f("of_dom_ask_pull_ratio", 1.0)
        dom_spread_bps = _f("of_dom_spread_bps", 0.0)
        data_confidence = _f("of_data_confidence", 0.0)

        def _add(score: float, notes: List[str], amount: float, note: str) -> float:
            if note and note not in notes:
                notes.append(note)
            return float(score + amount)

        long_score = 0.0
        short_score = 0.0
        long_notes: List[str] = []
        short_notes: List[str] = []

        if orderflow_mode == "OFF":
            risk_flag = "OFF"
            risk_side = "NEUTRAL"
            risk_score = 0.0
            risk_note = "order flow is disabled"
            risk_pass = False
        else:
            if data_quality in ("NO_DATA", "STALE", "INVALID"):
                long_score = _add(long_score, long_notes, 18.0, f"live OF data {data_quality.lower()}")
                short_score = _add(short_score, short_notes, 18.0, f"live OF data {data_quality.lower()}")
            elif data_quality == "DEGRADED":
                long_score = _add(long_score, long_notes, 10.0, "live OF data degraded")
                short_score = _add(short_score, short_notes, 10.0, "live OF data degraded")

            if not watch_allowed:
                long_score = _add(long_score, long_notes, 8.0, "watch gate locked")
                short_score = _add(short_score, short_notes, 8.0, "watch gate locked")
            if not confirmation_allowed:
                long_score = _add(long_score, long_notes, 6.0, "confirmation gate locked")
                short_score = _add(short_score, short_notes, 6.0, "confirmation gate locked")

            # Long-side hazards.
            if absorption_long or of_state == "ABSORPTION_LONG":
                long_score = _add(long_score, long_notes, 34.0, "buy absorption")
            if delta_pct >= 8.0 and progress_bps <= 0.75:
                long_score = _add(long_score, long_notes, 18.0, "positive delta without upside progress")
            if dom_pressure_pct <= -18.0:
                long_score = _add(long_score, long_notes, 24.0, "ask-side DOM pressure")
            elif dom_pressure_pct <= -10.0:
                long_score = _add(long_score, long_notes, 14.0, "ask pressure leaning against longs")
            if dom_pressure_accel <= -4.0:
                long_score = _add(long_score, long_notes, 10.0, "ask pressure accelerating")
            if dom_ask_stack_ratio >= 1.06:
                long_score = _add(long_score, long_notes, 12.0, "ask wall stacked")
            if dom_bid_pull_ratio <= 0.97:
                long_score = _add(long_score, long_notes, 10.0, "bid support pulling")
            if dom_state in ("DOM_CONFIRM_SHORT", "DOM_WATCH_SHORT", "DOM_FLOW_SHORT"):
                long_score = _add(long_score, long_notes, 10.0, "DOM leans short")
            if of_bias == "SHORT":
                long_score = _add(long_score, long_notes, 8.0, "tape/flow leans short")
            if integration_blocked and breakout_bias == "LONG":
                long_score = _add(long_score, long_notes, 22.0, f"{integration_mode.lower()} mode blocks long trigger")
            elif breakout_bias == "LONG" and not filter_pass:
                long_score = _add(long_score, long_notes, 10.0, "long breakout filter not passed")

            # Short-side hazards.
            if absorption_short or of_state == "ABSORPTION_SHORT":
                short_score = _add(short_score, short_notes, 34.0, "sell absorption")
            if delta_pct <= -8.0 and progress_bps >= -0.75:
                short_score = _add(short_score, short_notes, 18.0, "negative delta without downside progress")
            if dom_pressure_pct >= 18.0:
                short_score = _add(short_score, short_notes, 24.0, "bid-side DOM pressure")
            elif dom_pressure_pct >= 10.0:
                short_score = _add(short_score, short_notes, 14.0, "bid pressure leaning against shorts")
            if dom_pressure_accel >= 4.0:
                short_score = _add(short_score, short_notes, 10.0, "bid pressure accelerating")
            if dom_bid_stack_ratio >= 1.06:
                short_score = _add(short_score, short_notes, 12.0, "bid wall stacked")
            if dom_ask_pull_ratio <= 0.97:
                short_score = _add(short_score, short_notes, 10.0, "asks pulling / lifting")
            if dom_state in ("DOM_CONFIRM_LONG", "DOM_WATCH_LONG", "DOM_FLOW_LONG"):
                short_score = _add(short_score, short_notes, 10.0, "DOM leans long")
            if of_bias == "LONG":
                short_score = _add(short_score, short_notes, 8.0, "tape/flow leans long")
            if integration_blocked and breakout_bias == "SHORT":
                short_score = _add(short_score, short_notes, 22.0, f"{integration_mode.lower()} mode blocks short trigger")
            elif breakout_bias == "SHORT" and not filter_pass:
                short_score = _add(short_score, short_notes, 10.0, "short breakout filter not passed")

            # Wide/unstable spread is a generic execution hazard.
            if dom_spread_bps >= 4.5:
                long_score = _add(long_score, long_notes, 6.0, "spread unstable")
                short_score = _add(short_score, short_notes, 6.0, "spread unstable")

            preferred_side = "NEUTRAL"
            for candidate in (breakout_bias, market_bias, of_bias, dom_bias):
                if candidate in ("LONG", "SHORT"):
                    preferred_side = candidate
                    break

            if preferred_side == "LONG":
                risk_side = "LONG"
                risk_score = float(long_score)
                risk_notes = list(long_notes)
            elif preferred_side == "SHORT":
                risk_side = "SHORT"
                risk_score = float(short_score)
                risk_notes = list(short_notes)
            else:
                if short_score > long_score:
                    risk_side = "SHORT"
                    risk_score = float(short_score)
                    risk_notes = list(short_notes)
                elif long_score > short_score:
                    risk_side = "LONG"
                    risk_score = float(long_score)
                    risk_notes = list(long_notes)
                else:
                    risk_side = "NEUTRAL"
                    risk_score = float(max(long_score, short_score))
                    risk_notes = list(long_notes if long_notes else short_notes)

            # Track intrabar hostile hits only when a condition turns on.
            hostile_long_now = bool(long_score >= 26.0)
            hostile_short_now = bool(short_score >= 26.0)
            if hostile_long_now and not bool(rt.get("prev_hostile_long")):
                rt["hostile_long_hits"] = int(rt.get("hostile_long_hits") or 0) + 1
            if hostile_short_now and not bool(rt.get("prev_hostile_short")):
                rt["hostile_short_hits"] = int(rt.get("hostile_short_hits") or 0) + 1
            if absorption_long and not bool(rt.get("prev_abs_long")):
                rt["absorption_long_hits"] = int(rt.get("absorption_long_hits") or 0) + 1
            if absorption_short and not bool(rt.get("prev_abs_short")):
                rt["absorption_short_hits"] = int(rt.get("absorption_short_hits") or 0) + 1
            if integration_blocked and not bool(rt.get("prev_blocked")):
                rt["block_count"] = int(rt.get("block_count") or 0) + 1
            current_dir = "NEUTRAL"
            if hostile_long_now and not hostile_short_now:
                current_dir = "LONG"
            elif hostile_short_now and not hostile_long_now:
                current_dir = "SHORT"
            last_dir = str(rt.get("last_direction") or "NEUTRAL")
            if current_dir in ("LONG", "SHORT") and last_dir in ("LONG", "SHORT") and current_dir != last_dir:
                rt["flip_count"] = int(rt.get("flip_count") or 0) + 1
            if current_dir in ("LONG", "SHORT"):
                rt["last_direction"] = current_dir
            rt["prev_hostile_long"] = bool(hostile_long_now)
            rt["prev_hostile_short"] = bool(hostile_short_now)
            rt["prev_abs_long"] = bool(absorption_long)
            rt["prev_abs_short"] = bool(absorption_short)
            rt["prev_blocked"] = bool(integration_blocked)

            # Add intrabar memory as an extra caution layer.
            if risk_side == "LONG":
                mem_hits = int(rt.get("hostile_long_hits") or 0) + int(rt.get("absorption_long_hits") or 0)
                if mem_hits >= 3:
                    risk_score += 10.0
                    if "repeated long-side OF hazards" not in risk_notes:
                        risk_notes.append("repeated long-side OF hazards")
            elif risk_side == "SHORT":
                mem_hits = int(rt.get("hostile_short_hits") or 0) + int(rt.get("absorption_short_hits") or 0)
                if mem_hits >= 3:
                    risk_score += 10.0
                    if "repeated short-side OF hazards" not in risk_notes:
                        risk_notes.append("repeated short-side OF hazards")
            if int(rt.get("flip_count") or 0) >= 2:
                risk_score += 6.0
                if "intrabar OF flipped repeatedly" not in risk_notes:
                    risk_notes.append("intrabar OF flipped repeatedly")
            if int(rt.get("block_count") or 0) >= 2:
                risk_score += 6.0
                if "repeated OF trigger blocks" not in risk_notes:
                    risk_notes.append("repeated OF trigger blocks")

            risk_score = float(max(0.0, min(100.0, risk_score)))
            if data_quality in ("NO_DATA", "STALE", "INVALID") and risk_score < 35.0:
                risk_score = 35.0
            if integration_blocked and risk_score < 55.0:
                risk_score = 55.0

            if risk_score >= 60.0:
                risk_flag = "BLOCK"
            elif risk_score >= 28.0:
                risk_flag = "CAUTION"
            else:
                risk_flag = "PASS"
            risk_pass = bool(risk_flag == "PASS")
            risk_note = "; ".join(risk_notes[:4]) if risk_notes else "live order flow is not warning against the current side"
            if data_confidence > 0 and data_quality not in ("NO_DATA", "STALE", "INVALID"):
                risk_note = f"{risk_note} | data {data_quality.lower()} {int(round(data_confidence))}%"

        out["market_of_risk_flag"] = str(risk_flag)
        out["market_of_risk_pass"] = bool(risk_pass)
        out["market_of_risk_score"] = float(risk_score)
        out["market_of_risk_note"] = str(risk_note)
        out["market_of_risk_side"] = str(risk_side)
        out["market_of_risk_long_score"] = float(long_score)
        out["market_of_risk_short_score"] = float(short_score)
        out["market_of_risk_intrabar_hostile_long_hits"] = int(rt.get("hostile_long_hits") or 0)
        out["market_of_risk_intrabar_hostile_short_hits"] = int(rt.get("hostile_short_hits") or 0)
        out["market_of_risk_intrabar_absorption_long_hits"] = int(rt.get("absorption_long_hits") or 0)
        out["market_of_risk_intrabar_absorption_short_hits"] = int(rt.get("absorption_short_hits") or 0)
        out["market_of_risk_intrabar_flip_count"] = int(rt.get("flip_count") or 0)
        out["market_of_risk_intrabar_block_count"] = int(rt.get("block_count") or 0)

        try:
            base_conf = float(out.get("market_confidence") if out.get("market_confidence") is not None else np.nan)
        except Exception:
            base_conf = np.nan
        if np.isfinite(base_conf):
            penalty = 0.0
            if risk_flag == "CAUTION":
                penalty += 8.0
            elif risk_flag == "BLOCK":
                penalty += 18.0
            if integration_blocked:
                penalty += 6.0
            if data_quality in ("NO_DATA", "STALE", "INVALID"):
                penalty += 4.0
            out["market_confidence"] = int(max(5.0, min(100.0, round(base_conf - penalty))))

        warn_note = str(out.get("market_warning_note") or "").strip()
        if risk_flag in ("CAUTION", "BLOCK"):
            if warn_note:
                if risk_note.lower() not in warn_note.lower():
                    out["market_warning_note"] = f"{warn_note} | OF {risk_flag.lower()}: {risk_note}"
            else:
                out["market_warning_note"] = f"OF {risk_flag.lower()}: {risk_note}"

        self._orderflow_intrabar_runtime[tf] = rt
        return out


    def _record_orderflow_event(self, tf: str, payload: Dict[str, Any]) -> None:
        tf = str(tf)
        out = dict(payload or {})
        try:
            event_ts_ms = int(out.get("ts_ms") or out.get("bar_close_time_ms") or time.time() * 1000)
        except Exception:
            event_ts_ms = int(time.time() * 1000)

        breakout_state = str(out.get("market_breakout_state") or "NONE").upper().strip() or "NONE"
        breakout_bias = str(out.get("market_breakout_bias") or "NEUTRAL").upper().strip() or "NEUTRAL"
        filter_gate = str(out.get("market_breakout_filter_gate") or "NONE").upper().strip() or "NONE"
        filter_pass = bool(out.get("market_breakout_filter_pass"))
        boundary_state = str(out.get("of_boundary_state") or "BOUNDARY_NEUTRAL").upper().strip() or "BOUNDARY_NEUTRAL"
        of_state = str(out.get("of_state") or "NEUTRAL").upper().strip() or "NEUTRAL"
        of_bias = str(out.get("of_bias") or "NEUTRAL").upper().strip() or "NEUTRAL"
        data_quality = str(out.get("of_data_quality") or "UNKNOWN").upper().strip() or "UNKNOWN"
        watch_lock = bool(out.get("of_watch_lock_active"))
        confirm_lock = bool(out.get("of_confirmation_lock_active"))
        boundary_failed_long = bool(out.get("of_boundary_failed_long"))
        boundary_failed_short = bool(out.get("of_boundary_failed_short"))
        boundary_confirm_long = bool(out.get("of_boundary_confirm_long"))
        boundary_confirm_short = bool(out.get("of_boundary_confirm_short"))
        boundary_watch_long = bool(out.get("of_boundary_watch_long"))
        boundary_watch_short = bool(out.get("of_boundary_watch_short"))
        acceptance_long = bool(out.get("of_boundary_acceptance_long"))
        acceptance_short = bool(out.get("of_boundary_acceptance_short"))

        integration_mode = str(out.get("of_integration_mode") or getattr(self, "orderflow_integration_mode", "SOFT") or "SOFT").upper().strip() or "SOFT"
        integration_effect = str(out.get("of_integration_effect") or "NONE").upper().strip() or "NONE"
        integration_blocked = bool(out.get("of_integration_blocked"))
        integration_trigger_ready = bool(out.get("market_breakout_trigger_ready"))

        signature = (
            breakout_state,
            breakout_bias,
            filter_gate,
            filter_pass,
            boundary_state,
            of_state,
            of_bias,
            data_quality,
            watch_lock,
            confirm_lock,
            boundary_failed_long,
            boundary_failed_short,
            boundary_confirm_long,
            boundary_confirm_short,
            boundary_watch_long,
            boundary_watch_short,
            acceptance_long,
            acceptance_short,
            integration_mode,
            integration_effect,
            integration_blocked,
            integration_trigger_ready,
        )
        if signature == self._orderflow_last_event_signature.get(tf):
            last_line = self._orderflow_event_log.get(tf)
            if last_line:
                last_evt = last_line[-1]
                last_evt["last_seen_ts_ms"] = int(event_ts_ms)
                last_evt["seen_count"] = int(last_evt.get("seen_count") or 1) + 1
            return

        note = str(out.get("market_breakout_filter_note") or out.get("of_filter_note") or out.get("of_note") or out.get("of_boundary_note") or "").strip()
        quality_note = str(out.get("of_quality_note") or "").strip()
        if quality_note and quality_note not in note:
            reason = f"{note} | {quality_note}" if note else quality_note
        else:
            reason = note

        event_type = "OF_STATE_CHANGE"
        if boundary_failed_long:
            event_type = "BOUNDARY_FAILED_LONG"
        elif boundary_failed_short:
            event_type = "BOUNDARY_FAILED_SHORT"
        elif filter_gate == "CONFIRM" and filter_pass:
            event_type = f"CONFIRM_PASS_{breakout_bias}"
        elif filter_gate == "CONFIRM" and not filter_pass:
            event_type = f"CONFIRM_BLOCK_{breakout_bias}"
        elif filter_gate == "WATCH" and filter_pass:
            event_type = f"WATCH_PASS_{breakout_bias}"
        elif filter_gate == "WATCH" and not filter_pass:
            event_type = f"WATCH_BLOCK_{breakout_bias}"
        elif boundary_confirm_long:
            event_type = "BOUNDARY_CONFIRM_LONG"
        elif boundary_confirm_short:
            event_type = "BOUNDARY_CONFIRM_SHORT"
        elif boundary_watch_long:
            event_type = "BOUNDARY_WATCH_LONG"
        elif boundary_watch_short:
            event_type = "BOUNDARY_WATCH_SHORT"
        elif acceptance_long:
            event_type = "BOUNDARY_ACCEPT_LONG"
        elif acceptance_short:
            event_type = "BOUNDARY_ACCEPT_SHORT"

        try:
            price = float(out.get("price") or out.get("close") or out.get("market_boundary_current_price") or np.nan)
        except Exception:
            price = np.nan
        try:
            delta_pct = float(out.get("of_delta_pct") or 0.0)
        except Exception:
            delta_pct = 0.0
        try:
            progress_bps = float(out.get("of_progress_bps") or 0.0)
        except Exception:
            progress_bps = 0.0
        try:
            dom_pressure = float(out.get("of_dom_pressure_pct") or 0.0)
        except Exception:
            dom_pressure = 0.0
        try:
            confidence = float(out.get("of_data_confidence") or 0.0)
        except Exception:
            confidence = 0.0
        try:
            boundary_high = float(out.get("market_boundary_high") or np.nan)
        except Exception:
            boundary_high = np.nan
        try:
            boundary_low = float(out.get("market_boundary_low") or np.nan)
        except Exception:
            boundary_low = np.nan

        status_part = f"{filter_gate}:{'PASS' if filter_pass else 'BLOCK'}" if filter_gate != "NONE" else boundary_state.replace("BOUNDARY_", "")
        bias_part = breakout_bias if breakout_bias in ("LONG", "SHORT") else of_bias
        price_part = f" @ {price:.4f}" if np.isfinite(price) else ""
        integration_part = f" | {integration_mode}:{integration_effect}" if integration_mode else ""
        if integration_blocked:
            integration_part += " [blocked]"
        summary = f"{event_type} | {status_part}{integration_part} | {bias_part}{price_part}"
        if reason:
            summary = f"{summary} | {reason}"

        self._orderflow_event_seq += 1
        event = {
            "seq": int(self._orderflow_event_seq),
            "tf": tf,
            "ts_ms": int(event_ts_ms),
            "last_seen_ts_ms": int(event_ts_ms),
            "seen_count": 1,
            "event_type": str(event_type),
            "summary": str(summary),
            "reason": str(reason),
            "price": (float(price) if np.isfinite(price) else None),
            "market_state": str(out.get("market_state") or ""),
            "market_breakout_state": str(breakout_state),
            "market_breakout_bias": str(breakout_bias),
            "of_state": str(of_state),
            "of_bias": str(of_bias),
            "of_data_quality": str(data_quality),
            "of_data_confidence": float(confidence),
            "filter_gate": str(filter_gate),
            "filter_pass": bool(filter_pass),
            "watch_lock_active": bool(watch_lock),
            "confirmation_lock_active": bool(confirm_lock),
            "of_integration_mode": str(integration_mode),
            "of_integration_effect": str(integration_effect),
            "of_integration_blocked": bool(integration_blocked),
            "market_breakout_trigger_ready": bool(integration_trigger_ready),
            "market_breakout_trigger_ready_raw": bool(out.get("market_breakout_trigger_ready_raw")),
            "of_integration_note": str(out.get("of_integration_note") or ""),
            "of_filter_note": str(out.get("market_breakout_filter_note") or out.get("of_filter_note") or ""),
            "of_boundary_state": str(boundary_state),
            "of_boundary_note": str(out.get("of_boundary_note") or ""),
            "delta_pct": float(delta_pct),
            "progress_bps": float(progress_bps),
            "dom_pressure_pct": float(dom_pressure),
            "boundary_high": (float(boundary_high) if np.isfinite(boundary_high) else None),
            "boundary_low": (float(boundary_low) if np.isfinite(boundary_low) else None),
            "touch_upper": int(out.get("market_boundary_touch_count_upper") or 0),
            "touch_lower": int(out.get("market_boundary_touch_count_lower") or 0),
            "near_upper": bool(out.get("market_boundary_near_upper")),
            "near_lower": bool(out.get("market_boundary_near_lower")),
            "outside_above": bool(out.get("market_boundary_outside_above")),
            "outside_below": bool(out.get("market_boundary_outside_below")),
            "boundary_hold_long": int(out.get("of_boundary_outside_hold_long") or 0),
            "boundary_hold_short": int(out.get("of_boundary_outside_hold_short") or 0),
        }
        self._orderflow_event_log.setdefault(tf, collections.deque(maxlen=400)).append(event)
        self._orderflow_last_event_signature[tf] = signature
        self._orderflow_last_event_summary[tf] = str(summary)

    def get_orderflow_event_history(self, tf: str, limit: int = 60) -> List[Dict[str, Any]]:
        tf = str(tf)
        try:
            lim = max(1, int(limit))
        except Exception:
            lim = 60
        rows = list(self._orderflow_event_log.get(tf) or [])
        if lim:
            rows = rows[-lim:]
        return [dict(r) for r in rows]

    def _attach_market_state(self, tf: str, payload: Dict[str, Any]) -> Dict[str, Any]:
        out = dict(payload or {})
        prev = dict(self._market_state_runtime.get(tf) or {})
        state = compute_market_state_snapshot(out, prev)
        state.update(self._build_breakout_boundary_context(tf, {**out, **state}))

        try:
            cur_bar_close = int(out.get("bar_close_time_ms")) if out.get("bar_close_time_ms") is not None else None
        except Exception:
            cur_bar_close = None
        try:
            prev_bar_close = int(prev.get("bar_close_time_ms")) if prev.get("bar_close_time_ms") is not None else None
        except Exception:
            prev_bar_close = None

        if cur_bar_close is not None and prev_bar_close is not None and cur_bar_close == prev_bar_close:
            if str(prev.get("market_state") or "") == str(state.get("market_state") or ""):
                try:
                    state["market_state_age"] = int(prev.get("market_state_age") or state.get("market_state_age") or 1)
                except Exception:
                    pass

        ref_price = out.get("price")
        try:
            if ref_price is not None:
                ref_price = float(ref_price)
        except Exception:
            ref_price = None
        of_snap = self._current_orderflow_snapshot(tf, ref_price=ref_price)
        state = self._apply_orderflow_breakout_filter(tf, state, of_snap)
        state = self._apply_orderflow_execution_risk(tf, state)

        out.update(state)
        remember = dict(state)
        remember["bar_close_time_ms"] = cur_bar_close
        self._market_state_runtime[tf] = remember
        return out

    def _ema_settings_for_tf(self, tf: Optional[str] = None) -> Dict[str, int]:
        key = str(tf or "").strip()
        if key and key in self.ema_lengths_by_tf:
            return dict(self.ema_lengths_by_tf[key])
        return {
            "ema_1_length": int(self.ema_1_length),
            "ema_2_length": int(self.ema_2_length),
        }

    def _build_ema_payload(self, closes: List[float], tf: Optional[str] = None) -> Dict[str, Any]:
        ema_settings = self._ema_settings_for_tf(tf)
        return build_ema_pair_payload(
            closes,
            ema_1_length=int(ema_settings["ema_1_length"]),
            ema_2_length=int(ema_settings["ema_2_length"]),
        )

    def _rsi_settings_for_tf(self, tf: Optional[str] = None) -> Dict[str, int]:
        key = str(tf or "").strip()
        if key and key in self.rsi_settings_by_tf:
            return dict(self.rsi_settings_by_tf[key])
        return {
            "length": int(self.rsi_length),
            "smoothing": int(self.rsi_smoothing),
        }

    def _build_rsi_payload(self, closes: List[float], tf: Optional[str] = None) -> Dict[str, Any]:
        rsi_settings = self._rsi_settings_for_tf(tf)
        try:
            rsi_values, smooth_values, _state_values = rsi_series(
                np.asarray(closes, dtype=float),
                length=int(rsi_settings["length"]),
                smoothing=int(rsi_settings["smoothing"]),
            )
            if len(rsi_values) < 1 or len(smooth_values) < 1:
                raise ValueError("Not enough bars")
            raw_rsi = float(rsi_values[-1])
            raw_smooth = float(smooth_values[-1])
            rsi_value = float(raw_rsi) if np.isfinite(raw_rsi) else None
            rsi_smooth = float(raw_smooth) if np.isfinite(raw_smooth) else None
            rsi_state = ("GREEN" if rsi_value > rsi_smooth else "RED") if (rsi_value is not None and rsi_smooth is not None) else None
            return {
                "rsi": rsi_value,
                "rsi_smooth": rsi_smooth,
                "rsi_state": rsi_state,
            }
        except Exception:
            return {
                "rsi": None,
                "rsi_smooth": None,
                "rsi_state": None,
            }

    def _build_stoch_rsi_payload(self, closes: List[float]) -> Dict[str, Any]:
        try:
            _rsi_values, k_values, d_values = stoch_rsi_series(np.asarray(closes, dtype=float))
            if len(k_values) < 1 or len(d_values) < 1:
                raise ValueError("Not enough bars")
            raw_k = float(k_values[-1])
            raw_d = float(d_values[-1])
            stoch_k = float(raw_k) if np.isfinite(raw_k) else None
            stoch_d = float(raw_d) if np.isfinite(raw_d) else None
            kd = ("GREEN" if stoch_k > stoch_d else "RED") if (stoch_k is not None and stoch_d is not None) else None
            return {
                "stoch_rsi_k": stoch_k,
                "stoch_rsi_d": stoch_d,
                "stoch_rsi_kd": kd,
            }
        except Exception:
            return {
                "stoch_rsi_k": None,
                "stoch_rsi_d": None,
                "stoch_rsi_kd": None,
            }

    def _bootstrap_prefill_bars_for_tf(self, tf: str, full_bars: int) -> int:
        ema_cfg = self._ema_settings_for_tf(tf)
        rsi_cfg = self._rsi_settings_for_tf(tf)
        try:
            range_need = max(400, int(RANGE_FILTER_PER) * 4)
        except Exception:
            range_need = 400
        needed = max(
            int(MIN_FULL_CLOSES),
            int(ema_cfg.get("ema_2_length", self.ema_2_length) or self.ema_2_length) + 16,
            int(rsi_cfg.get("length", self.rsi_length) or self.rsi_length)
            + int(rsi_cfg.get("smoothing", self.rsi_smoothing) or self.rsi_smoothing)
            + 8,
            int(STOCH_RSI_RSI_LENGTH + STOCH_RSI_STOCH_LENGTH + STOCH_RSI_SMOOTH_K + STOCH_RSI_SMOOTH_D + 8),
            int(ADX_LEN * 3),
            int(ATR_LEN * 3),
            int(max(250, 200 + int(FRAMA_LEN) + 8)),
            int(max(250, 200 + int(VIDYA_LENGTH) + int(VIDYA_MOMENTUM) + 8)),
            int(range_need),
        )
        return int(max(1, min(int(full_bars), int(needed))))

    def _prefill_timeframe(self, tf: str, bars: int) -> None:
        closes = fetch_recent_closes_futures(self.symbol, tf, bars, log_cb=self.log, price_source=self.price_source)

        with self.lock:
            self.storage[tf].closes = closes[-MAX_STORED_CLOSES:]

        try:
            ohlc_df = fetch_recent_ohlc_futures(self.symbol, tf, bars, log_cb=self.log, price_source=self.price_source)

            st_adx = self.adx_state.get(tf)

            open_ms = []
            close_ms = []
            fresh_adx = make_adx_state(self.adx_impl, ADX_LEN)
            fresh_atr = make_atr_state(ATR_LEN)
            fresh_frama = make_frama_state(FRAMA_LEN, FRAMA_BANDS_DISTANCE)
            fresh_vidya = make_vidya_state(VIDYA_LENGTH, VIDYA_MOMENTUM, VIDYA_BAND_DISTANCE, VIDYA_PIVOT_LEFT, VIDYA_PIVOT_RIGHT)
            fresh_range_filter = make_range_filter_state(RANGE_FILTER_PER, RANGE_FILTER_MULT)

            if ohlc_df is not None and not ohlc_df.empty:
                try:
                    open_ms = (ohlc_df["open_time"].astype("int64") // 1_000_000).astype(int).tolist()
                    close_ms = (ohlc_df["close_time"].astype("int64") // 1_000_000).astype(int).tolist()

                    if open_ms and close_ms and len(open_ms) == len(close_ms):
                        self._prefill_bar_times[tf] = {"open_ms": open_ms, "close_ms": close_ms}
                        try:
                            with self.lock:
                                st = self.storage.get(tf)
                                if st is not None:
                                    st.last_open_time_ms = int(open_ms[-1])
                                    st.last_close_time_ms = int(close_ms[-1])
                        except Exception:
                            pass

                except Exception:
                    pass

                if st_adx is not None:
                    adx_series = []
                    atr_series = []
                    frama_series = []
                    vidya_series = []
                    range_filter_series = []
                    ohlcv_series = []

                    for _, rr in ohlc_df.iterrows():
                        try:
                            fresh_adx.update(float(rr["high"]), float(rr["low"]), float(rr["close"]))

                            try:
                                fresh_atr.update(float(rr["high"]), float(rr["low"]), float(rr["close"]))
                            except Exception:
                                pass
                            try:
                                fresh_frama.update(float(rr["high"]), float(rr["low"]), float(rr["close"]))
                            except Exception:
                                pass
                            try:
                                fresh_vidya.update(float(rr["open"]), float(rr["high"]), float(rr["low"]), float(rr["close"]), float(rr.get("volume", 0.0)))
                            except Exception:
                                pass
                            try:
                                fresh_range_filter.update(float(rr["close"]))
                            except Exception:
                                pass

                            try:
                                ct_ms = int(int(rr["close_time"].value) // 1_000_000)
                            except Exception:
                                ct_ms = None
                            try:
                                ot_ms = int(int(rr["open_time"].value) // 1_000_000)
                            except Exception:
                                ot_ms = None
                            if ct_ms is not None:
                                try:
                                    with self.lock:
                                        self._ring_append_closed_ohlc(tf, int(ct_ms), float(rr["high"]), float(rr["low"]), float(rr["close"]), open_t_ms=ot_ms)
                                        self._ring_append_closed_ohlcv(
                                            tf,
                                            int(ct_ms),
                                            float(rr["open"]),
                                            float(rr["high"]),
                                            float(rr["low"]),
                                            float(rr["close"]),
                                            float(rr["volume"]),
                                            open_t_ms=ot_ms,
                                        )
                                except Exception:
                                    pass
                            try:
                                ohlcv_series.append(
                                    {
                                        "open_time_ms": int(ot_ms) if ot_ms is not None else None,
                                        "close_time_ms": int(ct_ms) if ct_ms is not None else None,
                                        "open": float(rr["open"]),
                                        "high": float(rr["high"]),
                                        "low": float(rr["low"]),
                                        "close": float(rr["close"]),
                                        "volume": float(rr["volume"]),
                                        "trade_count": float(rr.get("trade_count", 0.0)),
                                        "taker_buy_volume": float(rr.get("taker_buy_volume", 0.0)),
                                    }
                                )
                            except Exception:
                                ohlcv_series.append({})

                            try:
                                adx_series.append(fresh_adx.snapshot())
                            except Exception:
                                adx_series.append((None, None, None, None, None))

                            try:
                                atr_series.append(fresh_atr.snapshot())
                            except Exception:
                                atr_series.append((None, None))
                            try:
                                frama_series.append(fresh_frama.snapshot())
                            except Exception:
                                frama_series.append({})
                            try:
                                vidya_series.append(fresh_vidya.snapshot())
                            except Exception:
                                vidya_series.append({})
                            try:
                                range_filter_series.append(fresh_range_filter.snapshot())
                            except Exception:
                                range_filter_series.append({})

                        except Exception:
                            adx_series.append((None, None, None, None, None))
                            try:
                                atr_series.append((None, None))
                            except Exception:
                                pass
                            try:
                                frama_series.append({})
                            except Exception:
                                pass
                            try:
                                vidya_series.append({})
                            except Exception:
                                pass
                            try:
                                range_filter_series.append({})
                            except Exception:
                                pass

                    if adx_series:
                        self._prefill_adx_series[tf] = adx_series

                        try:
                            self._prefill_atr_series[tf] = atr_series
                        except Exception:
                            pass
                        try:
                            self._prefill_frama_series[tf] = frama_series
                        except Exception:
                            pass
                        try:
                            self._prefill_vidya_series[tf] = vidya_series
                            self._prefill_range_filter_series[tf] = range_filter_series
                        except Exception:
                            pass
                        try:
                            self._prefill_ohlcv_series[tf] = ohlcv_series
                        except Exception:
                            pass

                        try:
                            with self.lock:
                                self.adx_state[tf] = fresh_adx
                                try:
                                    self.atr_state[tf] = fresh_atr
                                except Exception:
                                    pass
                                try:
                                    self.frama_state[tf] = fresh_frama
                                except Exception:
                                    pass
                                try:
                                    self.vidya_state[tf] = fresh_vidya
                                    self.range_filter_state[tf] = fresh_range_filter
                                except Exception:
                                    pass
                                try:
                                    if close_ms and len(close_ms) > 0:
                                        self._last_emitted_close_ms[tf] = int(close_ms[-1])
                                except Exception:
                                    pass
                        except Exception:
                            pass

                        try:
                            last_rest_close = int(close_ms[-1]) if close_ms else None
                        except Exception:
                            last_rest_close = None
                        if last_rest_close is not None:
                            try:
                                pend = getattr(self, "_pending_adx_ohlc", {}).get(tf, [])
                                if pend:
                                    newer = [p for p in pend if int(p[0]) > int(last_rest_close)]
                                    newer.sort(key=lambda x: int(x[0]))
                                    if newer:
                                        try:
                                            tf_ms = int(interval_to_ms(tf))
                                        except Exception:
                                            tf_ms = 60_000
                                        for ct_p, h_p, l_p, c_p in newer:
                                            try:
                                                fresh_adx.update(float(h_p), float(l_p), float(c_p))
                                                adx_series.append(fresh_adx.snapshot())
                                                try:
                                                    fresh_atr.update(float(h_p), float(l_p), float(c_p))
                                                    atr_series.append(fresh_atr.snapshot())
                                                except Exception:
                                                    pass
                                                try:
                                                    fresh_frama.update(float(h_p), float(l_p), float(c_p))
                                                    frama_series.append(fresh_frama.snapshot())
                                                except Exception:
                                                    pass
                                                try:
                                                    pendv = (getattr(self, "_pending_vidya_ohlcv", {}).get(tf, []) or [])
                                                    pv = next((x for x in pendv if int(x[0]) == int(ct_p)), None)
                                                    if pv is not None:
                                                        _ctv, o_p, h2_p, l2_p, c2_p, vol_p, *extra_p = pv
                                                        trade_count_p = float(extra_p[0]) if len(extra_p) > 0 else 0.0
                                                        taker_buy_p = float(extra_p[1]) if len(extra_p) > 1 else 0.0
                                                        fresh_vidya.update(float(o_p), float(h2_p), float(l2_p), float(c2_p), float(vol_p))
                                                        ohlcv_series.append(
                                                            {
                                                                "open_time_ms": int(int(ct_p) - tf_ms + 1),
                                                                "close_time_ms": int(ct_p),
                                                                "open": float(o_p),
                                                                "high": float(h2_p),
                                                                "low": float(l2_p),
                                                                "close": float(c2_p),
                                                                "volume": float(vol_p),
                                                                "trade_count": float(trade_count_p),
                                                                "taker_buy_volume": float(taker_buy_p),
                                                            }
                                                        )
                                                    else:
                                                        fresh_vidya.update(float(c_p), float(h_p), float(l_p), float(c_p), 0.0)
                                                        ohlcv_series.append(
                                                            {
                                                                "open_time_ms": int(int(ct_p) - tf_ms + 1),
                                                                "close_time_ms": int(ct_p),
                                                                "open": float(c_p),
                                                                "high": float(h_p),
                                                                "low": float(l_p),
                                                                "close": float(c_p),
                                                                "volume": 0.0,
                                                                "trade_count": 0.0,
                                                                "taker_buy_volume": 0.0,
                                                            }
                                                        )
                                                    vidya_series.append(fresh_vidya.snapshot())
                                                except Exception:
                                                    pass
                                                try:
                                                    fresh_range_filter.update(float(c_p))
                                                    range_filter_series.append(fresh_range_filter.snapshot())
                                                except Exception:
                                                    pass
                                            except Exception:
                                                pass
                                        try:
                                            with self.lock:
                                                stx = self.storage.get(tf)
                                                if stx is not None:
                                                    for ct_p, _, _, c_p in newer:
                                                        try:
                                                            if stx.last_close_time_ms is None or int(ct_p) > int(stx.last_close_time_ms):
                                                                stx.closes.append(float(c_p))
                                                                stx.last_close_time_ms = int(ct_p)
                                                                try:
                                                                    stx.last_open_time_ms = int(int(ct_p) - int(interval_to_ms(tf)) + 1)
                                                                except Exception:
                                                                    pass
                                                        except Exception:
                                                            pass
                                                    if len(stx.closes) > MAX_STORED_CLOSES:
                                                        stx.closes = stx.closes[-MAX_STORED_CLOSES:]
                                                try:
                                                    self._last_emitted_close_ms[tf] = int(newer[-1][0])
                                                except Exception:
                                                    pass
                                        except Exception:
                                            pass
                                        try:
                                            self._prefill_adx_series[tf] = adx_series
                                            try:
                                                self._prefill_atr_series[tf] = atr_series
                                            except Exception:
                                                pass
                                            try:
                                                self._prefill_frama_series[tf] = frama_series
                                            except Exception:
                                                pass
                                            try:
                                                self._prefill_vidya_series[tf] = vidya_series
                                            except Exception:
                                                pass
                                            try:
                                                self._prefill_range_filter_series[tf] = range_filter_series
                                            except Exception:
                                                pass
                                            try:
                                                self._prefill_ohlcv_series[tf] = ohlcv_series
                                            except Exception:
                                                pass
                                        except Exception:
                                            pass
                                        try:
                                            tm_seed = dict(self._prefill_bar_times.get(tf) or {})
                                            open_seed = list(tm_seed.get("open_ms") or [])
                                            close_seed = list(tm_seed.get("close_ms") or [])
                                            for ct_p, _, _, _ in newer:
                                                close_i = int(ct_p)
                                                open_seed.append(int(close_i - tf_ms + 1))
                                                close_seed.append(close_i)
                                            if open_seed and close_seed and len(open_seed) == len(close_seed):
                                                self._prefill_bar_times[tf] = {"open_ms": open_seed, "close_ms": close_seed}
                                        except Exception:
                                            pass
                                        try:
                                            self._pending_adx_ohlc[tf] = [p for p in pend if int(p[0]) > int(newer[-1][0])]
                                            try:
                                                pend_a = getattr(self, "_pending_atr_ohlc", {}).get(tf, [])
                                                self._pending_atr_ohlc[tf] = [p for p in (pend_a or []) if int(p[0]) > int(newer[-1][0])]
                                            except Exception:
                                                pass
                                        except Exception:
                                            self._pending_adx_ohlc[tf] = []
                                            try:
                                                self._pending_atr_ohlc[tf] = []
                                            except Exception:
                                                pass
                            except Exception:
                                pass

        except Exception:
            pass


    # -------------------------
    # Closed-OHLC ring helpers
    # -------------------------
    def _ring_append_closed_ohlc(self, tf: str, close_t_ms: int, high: float, low: float, close: float, open_t_ms: Optional[int] = None):
        """Append a CLOSED bar OHLC into the rolling ring buffer.

        The ring is used to periodically rebuild ADX/ATR state to prevent long-run drift.
        We keep it monotonic by close time and ignore duplicates.
        """
        try:
            if tf not in self._closed_ohlc_ring:
                return
            ct = int(close_t_ms)
            h = float(high)
            l = float(low)
            c = float(close)
            ot = int(open_t_ms) if open_t_ms is not None else None
        except Exception:
            return

        if not np.isfinite(h) or not np.isfinite(l) or not np.isfinite(c):
            return

        ring = self._closed_ohlc_ring.get(tf)
        if ring is None:
            return

        try:
            if len(ring) > 0:
                last_ct = int(ring[-1][0])
                if ct <= last_ct:
                    # Duplicate or out-of-order. Ignore (keeps rebuild stable).
                    return
        except Exception:
            pass

        try:
            ring.append((ct, h, l, c, ot))
        except Exception:
            pass

    def _ring_append_closed_ohlcv(
        self,
        tf: str,
        close_t_ms: int,
        open_price: float,
        high: float,
        low: float,
        close: float,
        volume: float,
        open_t_ms: Optional[int] = None,
    ) -> None:
        """Append a CLOSED bar OHLCV row into the rolling audit ring."""
        try:
            if tf not in self._closed_ohlcv_ring:
                return
            ct = int(close_t_ms)
            op = float(open_price)
            h = float(high)
            l = float(low)
            c = float(close)
            vol = float(volume)
            ot = int(open_t_ms) if open_t_ms is not None else None
        except Exception:
            return

        if not (np.isfinite(op) and np.isfinite(h) and np.isfinite(l) and np.isfinite(c)):
            return
        if not np.isfinite(vol):
            vol = 0.0

        ring = self._closed_ohlcv_ring.get(tf)
        if ring is None:
            return

        try:
            if len(ring) > 0:
                last_ct = int(ring[-1][0])
                if ct <= last_ct:
                    return
        except Exception:
            pass

        try:
            ring.append((ct, op, h, l, c, vol, ot))
        except Exception:
            pass


    def _rebuild_state_from_ring(self, tf: str) -> Tuple[Optional[float], Optional[float]]:
        """Rebuild ADX and ATR state from the stored CLOSED OHLC ring.

        Returns (adx_value, atr_value) from the rebuilt states.
        """
        try:
            rows = list(self._closed_ohlc_ring.get(tf, []))
        except Exception:
            rows = []

        if not rows:
            return None, None

        # Rebuild using the selected ADX implementation (TRADINGVIEW / PINE_SMA_DX).
        fresh_adx = make_adx_state(self.adx_impl, ADX_LEN)
        fresh_atr = make_atr_state(ATR_LEN)
        fresh_frama = make_frama_state(FRAMA_LEN, FRAMA_BANDS_DISTANCE)
        for ct, h, l, c, _ot in rows:
            try:
                fresh_adx.update(float(h), float(l), float(c))
            except Exception:
                pass
            try:
                fresh_atr.update(float(h), float(l), float(c))
            except Exception:
                pass
            try:
                fresh_frama.update(float(h), float(l), float(c))
            except Exception:
                pass

        try:
            adx_val = fresh_adx.snapshot()[0]
        except Exception:
            adx_val = None
        try:
            atr_val = fresh_atr.snapshot()[0]
        except Exception:
            atr_val = None

        # Swap rebuilt state into the engine.
        try:
            with self.lock:
                self.adx_state[tf] = fresh_adx
                self.atr_state[tf] = fresh_atr
                self.frama_state[tf] = fresh_frama
                # Reset intrabar slope references so the next live preview doesn't compare to a stale value.
                self._last_live_adx_preview[tf] = None
                self._last_live_adx_slope[tf] = None
                self._last_live_atr_preview[tf] = None
                self._last_live_atr_slope[tf] = None
        except Exception:
            pass

        return (float(adx_val) if adx_val is not None and np.isfinite(float(adx_val)) else None,
                float(atr_val) if atr_val is not None and np.isfinite(float(atr_val)) else None)

    def start(self):
        if self.running:
            return
        self.running = True

        # IMPORTANT: Prefill can take time (thousands of candles). If we do it on the UI thread,
        # the app looks "stuck" and you won't see any live price.
        # Start websocket/REST first so price begins updating immediately, and prefill in background.
        threading.Thread(target=self._ws_loop, daemon=True).start()
        threading.Thread(target=self._fallback_rest_loop, daemon=True).start()
        threading.Thread(target=self._compute_loop, daemon=True).start()
        threading.Thread(target=self._prefill_all, daemon=True).start()
        threading.Thread(target=self._repair_closes_loop, daemon=True).start()
        threading.Thread(target=self._sanity_rebuild_loop, daemon=True).start()

    def stop(self):
        self.running = False
        try:
            if self.ws:
                self.ws.close()
        except Exception:
            pass


    def _prefill_all_legacy(self):

        self.log(f"[Live] Prefill started (EMA warmup)…  source={self.price_source}  macd={self.macd_impl}  adx={self.adx_impl}")

        for tf in self.tfs:

            try:

                bars = LIVE_PREFILL_BARS.get(tf, 200)

                closes = fetch_recent_closes_futures(self.symbol, tf, bars, log_cb=self.log, price_source=self.price_source)

                with self.lock:

                    self.storage[tf].closes = closes[-MAX_STORED_CLOSES:]


                # Seed ADX/DI + bar times from OHLC history (TradingView/Wilder)

                try:

                    ohlc_df = fetch_recent_ohlc_futures(self.symbol, tf, bars, log_cb=self.log, price_source=self.price_source)

                    st_adx = self.adx_state.get(tf)

                    open_ms = []
                    close_ms = []
                    fresh_adx = make_adx_state(self.adx_impl, ADX_LEN)
                    fresh_atr = make_atr_state(ATR_LEN)
                    fresh_frama = make_frama_state(FRAMA_LEN, FRAMA_BANDS_DISTANCE)
                    fresh_vidya = make_vidya_state(VIDYA_LENGTH, VIDYA_MOMENTUM, VIDYA_BAND_DISTANCE, VIDYA_PIVOT_LEFT, VIDYA_PIVOT_RIGHT)
                    fresh_range_filter = make_range_filter_state(RANGE_FILTER_PER, RANGE_FILTER_MULT)

                    if ohlc_df is not None and not ohlc_df.empty:

                        # Keep exact bar timing arrays so Bars-Ago uses exchange candle boundaries.

                        try:

                            open_ms = (ohlc_df["open_time"].astype("int64") // 1_000_000).astype(int).tolist()

                            close_ms = (ohlc_df["close_time"].astype("int64") // 1_000_000).astype(int).tolist()

                            if open_ms and close_ms and len(open_ms) == len(close_ms):

                                self._prefill_bar_times[tf] = {"open_ms": open_ms, "close_ms": close_ms}

                                # Also set storage's current bar timing to the latest prefilled closed candle.

                                try:

                                    with self.lock:

                                        st = self.storage.get(tf)

                                        if st is not None:

                                            st.last_open_time_ms = int(open_ms[-1])

                                            st.last_close_time_ms = int(close_ms[-1])

                                except Exception:

                                    pass

                        except Exception:

                            pass


                        # Build a full ADX/DI snapshot series aligned to OHLC history.

                        if st_adx is not None:

                            adx_series = []
                            atr_series = []
                            frama_series = []
                            vidya_series = []
                            range_filter_series = []
                            ohlcv_series = []

                            for _, rr in ohlc_df.iterrows():

                                try:

                                    fresh_adx.update(float(rr["high"]), float(rr["low"]), float(rr["close"]))

                                    try:

                                        fresh_atr.update(float(rr["high"]), float(rr["low"]), float(rr["close"]))

                                    except Exception:

                                        pass
                                    try:

                                        fresh_frama.update(float(rr["high"]), float(rr["low"]), float(rr["close"]))

                                    except Exception:

                                        pass
                                    try:
                                        fresh_vidya.update(float(rr["open"]), float(rr["high"]), float(rr["low"]), float(rr["close"]), float(rr.get("volume", 0.0)))
                                    except Exception:
                                        pass
                                    try:
                                        fresh_range_filter.update(float(rr["close"]))
                                    except Exception:
                                        pass


                                    # Populate CLOSED OHLC ring (for later sanity rebuilds)
                                    try:
                                        ct_ms = int(int(rr["close_time"].value) // 1_000_000)
                                    except Exception:
                                        ct_ms = None
                                    try:
                                        ot_ms = int(int(rr["open_time"].value) // 1_000_000)
                                    except Exception:
                                        ot_ms = None
                                    if ct_ms is not None:
                                        try:
                                            with self.lock:
                                                self._ring_append_closed_ohlc(tf, int(ct_ms), float(rr["high"]), float(rr["low"]), float(rr["close"]), open_t_ms=ot_ms)
                                                self._ring_append_closed_ohlcv(
                                                    tf,
                                                    int(ct_ms),
                                                    float(rr["open"]),
                                                    float(rr["high"]),
                                                    float(rr["low"]),
                                                    float(rr["close"]),
                                                    float(rr["volume"]),
                                                    open_t_ms=ot_ms,
                                                )
                                        except Exception:
                                            pass
                                    try:
                                        ohlcv_series.append(
                                            {
                                                "open_time_ms": int(ot_ms) if ot_ms is not None else None,
                                                "close_time_ms": int(ct_ms) if ct_ms is not None else None,
                                                "open": float(rr["open"]),
                                                "high": float(rr["high"]),
                                                "low": float(rr["low"]),
                                                "close": float(rr["close"]),
                                                "volume": float(rr["volume"]),
                                                "trade_count": float(rr.get("trade_count", 0.0)),
                                                "taker_buy_volume": float(rr.get("taker_buy_volume", 0.0)),
                                            }
                                        )
                                    except Exception:
                                        ohlcv_series.append({})

                                    try:

                                        adx_series.append(fresh_adx.snapshot())

                                    except Exception:

                                        adx_series.append((None, None, None, None, None))

                                    try:

                                        atr_series.append(fresh_atr.snapshot())

                                    except Exception:

                                        atr_series.append((None, None))
                                    try:

                                        frama_series.append(fresh_frama.snapshot())

                                    except Exception:

                                        frama_series.append({})
                                    try:
                                        vidya_series.append(fresh_vidya.snapshot())
                                    except Exception:
                                        vidya_series.append({})
                                    try:
                                        range_filter_series.append(fresh_range_filter.snapshot())
                                    except Exception:
                                        range_filter_series.append({})

                                except Exception:

                                    adx_series.append((None, None, None, None, None))
                                    try:
                                        atr_series.append((None, None))
                                    except Exception:
                                        pass
                                    try:
                                        frama_series.append({})
                                    except Exception:
                                        pass
                                    try:
                                        vidya_series.append({})
                                    except Exception:
                                        pass
                                    try:
                                        range_filter_series.append({})
                                    except Exception:
                                        pass

                            if adx_series:

                                self._prefill_adx_series[tf] = adx_series

                                try:
                                    self._prefill_atr_series[tf] = atr_series
                                except Exception:
                                    pass
                                try:
                                    self._prefill_frama_series[tf] = frama_series
                                except Exception:
                                    pass
                                try:
                                    self._prefill_vidya_series[tf] = vidya_series
                                    self._prefill_range_filter_series[tf] = range_filter_series
                                except Exception:
                                    pass
                                try:
                                    self._prefill_ohlcv_series[tf] = ohlcv_series
                                except Exception:
                                    pass
                                try:
                                    if ohlcv_series:
                                        self._latest_closed_taker_metrics[tf] = {
                                            "volume": ohlcv_series[-1].get("volume"),
                                            "trade_count": ohlcv_series[-1].get("trade_count"),
                                            "taker_buy_volume": ohlcv_series[-1].get("taker_buy_volume"),
                                        }
                                except Exception:
                                    pass

                                # Swap in freshly seeded ADX state (prevents drift when prefill runs after websocket starts).
                                try:
                                    with self.lock:
                                        self.adx_state[tf] = fresh_adx
                                        try:
                                            self.atr_state[tf] = fresh_atr
                                        except Exception:
                                            pass
                                        try:
                                            self.frama_state[tf] = fresh_frama
                                        except Exception:
                                            pass
                                        try:
                                            self.vidya_state[tf] = fresh_vidya
                                            self.range_filter_state[tf] = fresh_range_filter
                                        except Exception:
                                            pass
                                        try:
                                            if close_ms and len(close_ms) > 0:
                                                self._last_emitted_close_ms[tf] = int(close_ms[-1])
                                        except Exception:
                                            pass
                                except Exception:
                                    pass

                                # Replay any buffered CLOSED klines that arrived during prefill so ADX matches "now".
                                try:
                                    last_rest_close = int(close_ms[-1]) if close_ms else None
                                except Exception:
                                    last_rest_close = None
                                if last_rest_close is not None:
                                    try:
                                        pend = getattr(self, "_pending_adx_ohlc", {}).get(tf, [])
                                        if pend:
                                            newer = [p for p in pend if int(p[0]) > int(last_rest_close)]
                                            newer.sort(key=lambda x: int(x[0]))
                                            if newer:
                                                try:
                                                    tf_ms = int(interval_to_ms(tf))
                                                except Exception:
                                                    tf_ms = 60_000
                                                for ct_p, h_p, l_p, c_p in newer:
                                                    try:
                                                        fresh_adx.update(float(h_p), float(l_p), float(c_p))
                                                        adx_series.append(fresh_adx.snapshot())
                                                        try:
                                                            fresh_atr.update(float(h_p), float(l_p), float(c_p))
                                                            atr_series.append(fresh_atr.snapshot())
                                                        except Exception:
                                                            pass
                                                        try:
                                                            fresh_frama.update(float(h_p), float(l_p), float(c_p))
                                                            frama_series.append(fresh_frama.snapshot())
                                                        except Exception:
                                                            pass
                                                        try:
                                                            pendv = (getattr(self, "_pending_vidya_ohlcv", {}).get(tf, []) or [])
                                                            pv = next((x for x in pendv if int(x[0]) == int(ct_p)), None)
                                                            if pv is not None:
                                                                _ctv, o_p, h2_p, l2_p, c2_p, vol_p, *extra_p = pv
                                                                trade_count_p = float(extra_p[0]) if len(extra_p) > 0 else 0.0
                                                                taker_buy_p = float(extra_p[1]) if len(extra_p) > 1 else 0.0
                                                                fresh_vidya.update(float(o_p), float(h2_p), float(l2_p), float(c2_p), float(vol_p))
                                                                ohlcv_series.append(
                                                                    {
                                                                        "open_time_ms": int(int(ct_p) - tf_ms + 1),
                                                                        "close_time_ms": int(ct_p),
                                                                        "open": float(o_p),
                                                                        "high": float(h2_p),
                                                                        "low": float(l2_p),
                                                                        "close": float(c2_p),
                                                                        "volume": float(vol_p),
                                                                        "trade_count": float(trade_count_p),
                                                                        "taker_buy_volume": float(taker_buy_p),
                                                                    }
                                                                )
                                                            else:
                                                                fresh_vidya.update(float(c_p), float(h_p), float(l_p), float(c_p), 0.0)
                                                                ohlcv_series.append(
                                                                    {
                                                                        "open_time_ms": int(int(ct_p) - tf_ms + 1),
                                                                        "close_time_ms": int(ct_p),
                                                                        "open": float(c_p),
                                                                        "high": float(h_p),
                                                                        "low": float(l_p),
                                                                        "close": float(c_p),
                                                                        "volume": 0.0,
                                                                        "trade_count": 0.0,
                                                                        "taker_buy_volume": 0.0,
                                                                    }
                                                                )
                                                            vidya_series.append(fresh_vidya.snapshot())
                                                        except Exception:
                                                            pass
                                                        try:
                                                            fresh_range_filter.update(float(c_p))
                                                            range_filter_series.append(fresh_range_filter.snapshot())
                                                        except Exception:
                                                            pass
                                                    except Exception:
                                                        pass
                                                # Keep the storage closes aligned too (MACD/PPO etc.).
                                                try:
                                                    with self.lock:
                                                        stx = self.storage.get(tf)
                                                        if stx is not None:
                                                            for ct_p, _, _, c_p in newer:
                                                                try:
                                                                    if stx.last_close_time_ms is None or int(ct_p) > int(stx.last_close_time_ms):
                                                                        stx.closes.append(float(c_p))
                                                                        stx.last_close_time_ms = int(ct_p)
                                                                        try:
                                                                            stx.last_open_time_ms = int(int(ct_p) - int(interval_to_ms(tf)) + 1)
                                                                        except Exception:
                                                                            pass
                                                                except Exception:
                                                                    pass
                                                            if len(stx.closes) > MAX_STORED_CLOSES:
                                                                stx.closes = stx.closes[-MAX_STORED_CLOSES:]
                                                        try:
                                                            self._last_emitted_close_ms[tf] = int(newer[-1][0])
                                                        except Exception:
                                                            pass
                                                except Exception:
                                                    pass
                                                # Update seeded history so Bars-Ago works immediately.
                                                try:
                                                    self._prefill_adx_series[tf] = adx_series
                                                    try:
                                                        self._prefill_atr_series[tf] = atr_series
                                                    except Exception:
                                                        pass
                                                    try:
                                                        self._prefill_frama_series[tf] = frama_series
                                                    except Exception:
                                                        pass
                                                    try:
                                                        self._prefill_vidya_series[tf] = vidya_series
                                                    except Exception:
                                                        pass
                                                    try:
                                                        self._prefill_range_filter_series[tf] = range_filter_series
                                                    except Exception:
                                                        pass
                                                    try:
                                                        self._prefill_ohlcv_series[tf] = ohlcv_series
                                                    except Exception:
                                                        pass
                                                    try:
                                                        if ohlcv_series:
                                                            self._latest_closed_taker_metrics[tf] = {
                                                                "volume": ohlcv_series[-1].get("volume"),
                                                                "trade_count": ohlcv_series[-1].get("trade_count"),
                                                                "taker_buy_volume": ohlcv_series[-1].get("taker_buy_volume"),
                                                            }
                                                    except Exception:
                                                        pass
                                                except Exception:
                                                    pass
                                                try:
                                                    tm_seed = dict(self._prefill_bar_times.get(tf) or {})
                                                    open_seed = list(tm_seed.get("open_ms") or [])
                                                    close_seed = list(tm_seed.get("close_ms") or [])
                                                    for ct_p, _, _, _ in newer:
                                                        close_i = int(ct_p)
                                                        open_seed.append(int(close_i - tf_ms + 1))
                                                        close_seed.append(close_i)
                                                    if open_seed and close_seed and len(open_seed) == len(close_seed):
                                                        self._prefill_bar_times[tf] = {"open_ms": open_seed, "close_ms": close_seed}
                                                except Exception:
                                                    pass
                                                # Drop applied buffer items.
                                                try:
                                                    self._pending_adx_ohlc[tf] = [p for p in pend if int(p[0]) > int(newer[-1][0])]
                                                    try:
                                                        pend_a = getattr(self, "_pending_atr_ohlc", {}).get(tf, [])
                                                        self._pending_atr_ohlc[tf] = [p for p in (pend_a or []) if int(p[0]) > int(newer[-1][0])]
                                                    except Exception:
                                                        pass
                                                except Exception:
                                                    self._pending_adx_ohlc[tf] = []
                                                    try:
                                                        self._pending_atr_ohlc[tf] = []
                                                    except Exception:
                                                        pass
                                    except Exception:
                                        pass


                except Exception:

                    pass


                self.log(f"[Live] Prefilled {tf}: {len(self.storage[tf].closes)} closes")

            except Exception as e:

                self.log(f"[Live] Prefill error {tf}: {e}")


        with self.lock:

            # initialize live_price from last known close

            if self.storage.get("1m") and self.storage["1m"].closes:

                self.live_price = float(self.storage["1m"].closes[-1])

            elif self.tfs:

                any_tf = self.tfs[0]

                if self.storage.get(any_tf) and self.storage[any_tf].closes:

                    self.live_price = float(self.storage[any_tf].closes[-1])

            self.last_ws_tick_ts = time.time()


        # init boundary tracker for synthetic closes

        try:

            now_ms = int(pd.Timestamp.utcnow().timestamp() * 1000)

            self._last_minute_open_ms = now_ms - (now_ms % 60_000)

        except Exception:

            self._last_minute_open_ms = None


        self.prefill_done = True


        # Seed CLOSED-bar snapshots so STATE 'Bars Ago' works immediately (no need to wait for the next close).

        try:

            self._emit_seed_closed_history()

        except Exception:

            pass


        self.log("[Live] Prefill finished.")

        self.log("[Live] Click dots/cells to add rules. EVENT rules are pulses; ★ marks TRIGGER (sticky FIRED in monitoring).")





    def _prefill_all(self):

        self.log(f"[Live] Prefill started (EMA warmup)â€¦  source={self.price_source}  macd={self.macd_impl}  adx={self.adx_impl}")

        bootstrap_bars_by_tf: Dict[str, int] = {}

        for tf in self.tfs:
            try:
                bars = int(LIVE_PREFILL_BARS.get(tf, 200))
                bootstrap_bars = self._bootstrap_prefill_bars_for_tf(tf, bars)
                bootstrap_bars_by_tf[tf] = int(bootstrap_bars)
                self._prefill_timeframe(tf, bootstrap_bars)
                self.log(f"[Live] Bootstrap ready {tf}: {len(self.storage[tf].closes)} closes")
            except Exception as e:
                self.log(f"[Live] Bootstrap prefill error {tf}: {e}")

        for tf in self.tfs:
            try:
                bars = int(LIVE_PREFILL_BARS.get(tf, 200))
                bootstrap_bars = int(bootstrap_bars_by_tf.get(tf) or 0)
                if bars > bootstrap_bars:
                    self._prefill_timeframe(tf, bars)
                self.log(f"[Live] Prefilled {tf}: {len(self.storage[tf].closes)} closes")
            except Exception as e:
                self.log(f"[Live] Prefill error {tf}: {e}")

        with self.lock:

            # initialize live_price from last known close

            if self.storage.get("1m") and self.storage["1m"].closes:

                self.live_price = float(self.storage["1m"].closes[-1])

            elif self.tfs:

                any_tf = self.tfs[0]

                if self.storage.get(any_tf) and self.storage[any_tf].closes:

                    self.live_price = float(self.storage[any_tf].closes[-1])

            self.last_ws_tick_ts = time.time()


        # init boundary tracker for synthetic closes

        try:

            now_ms = int(pd.Timestamp.utcnow().timestamp() * 1000)

            self._last_minute_open_ms = now_ms - (now_ms % 60_000)

        except Exception:

            self._last_minute_open_ms = None


        self.prefill_done = True


        # Seed CLOSED-bar snapshots so STATE 'Bars Ago' works immediately (no need to wait for the next close).

        try:

            self._emit_seed_closed_history()

        except Exception:

            pass


        self.log("[Live] Prefill finished.")

        self.log("[Live] Click dots/cells to add rules. EVENT rules are pulses; â˜… marks TRIGGER (sticky FIRED in monitoring).")


    def _emit_seed_closed_history(self) -> None:




        """Emit CLOSED-bar snapshots from prefilled history.





        This seeds per-timeframe *closed* bar history so STATE rules with Bars-Ago (e.g. EXACT 7)




        work immediately in live mode (no need to wait N new candles to close).





        We emit a *batch* per timeframe to avoid flooding the UI queue.




        """




        try:




            with self.lock:




                live_price = float(self.live_price) if self.live_price is not None else None




                last_tick = float(self.last_ws_tick_ts) if self.last_ws_tick_ts else 0.0




                closes_snapshot = {tf: st.closes[:] for tf, st in self.storage.items()}




        except Exception:




            return





        if live_price is None:




            return





        age_sec = max(0.0, time.time() - last_tick) if last_tick else 0.0





        try:




            seed_n_default = int(LIVE_SEED_CLOSED_BARS)




        except Exception:




            seed_n_default = 1024





        for tf in self.tfs:




            closes = closes_snapshot.get(tf) or []




            n = len(closes)




            if n < 3:




                continue





            seed_n = min(n, max(32, seed_n_default))




            start_i = max(0, n - seed_n)





            c = np.asarray(closes, dtype=float)




            try:




                macd_line, sig_line, _ = macd_series(c, self.macd_impl)




            except Exception:




                continue




            try:




                d_ppo = ppo_d_series(c)




            except Exception:




                d_ppo = np.full_like(c, np.nan, dtype=float)




            rsi_cfg = self._rsi_settings_for_tf(tf)
            try:
                rsi_raw_series, rsi_smooth_series, _rsi_state_series = rsi_series(
                    c,
                    length=int(rsi_cfg["length"]),
                    smoothing=int(rsi_cfg["smoothing"]),
                )
            except Exception:
                rsi_raw_series = np.full_like(c, np.nan, dtype=float)
                rsi_smooth_series = np.full_like(c, np.nan, dtype=float)

            try:
                _seed_rsi, stoch_k_series, stoch_d_series = stoch_rsi_series(c)
            except Exception:
                stoch_k_series = np.full_like(c, np.nan, dtype=float)
                stoch_d_series = np.full_like(c, np.nan, dtype=float)





            ms_col = [None] * n




            ang_col = [None] * n



            sig_ang_col = [None] * n



            ppo_col = [None] * n




            flip_age = [None] * n





            prev_age = None




            for i in range(n):




                mval = float(macd_line[i]) if i < len(macd_line) else float('nan')




                sval = float(sig_line[i]) if i < len(sig_line) else float('nan')




                if np.isfinite(mval) and np.isfinite(sval):




                    ms_col[i] = 'GREEN' if (mval > sval) else 'RED'
                if i >= 1:
                    pm = float(macd_line[i-1]) if (i-1) < len(macd_line) else float('nan')
                    if np.isfinite(mval) and np.isfinite(pm):
                        ang_col[i] = 'GREEN' if (mval > pm) else 'RED'

                    ps = float(sig_line[i-1]) if (i-1) < len(sig_line) else float('nan')
                    if np.isfinite(sval) and np.isfinite(ps):
                        sig_ang_col[i] = 'GREEN' if (sval > ps) else 'RED'

                    a = float(d_ppo[i]) if i < len(d_ppo) else float('nan')
                    b = float(d_ppo[i-1]) if (i-1) < len(d_ppo) else float('nan')
                    if np.isfinite(a) and np.isfinite(b):
                        ppo_col[i] = 'GREEN' if (a > b) else 'RED'

                if ppo_col[i] is None:




                    flip_age[i] = None




                    prev_age = None




                else:




                    if i == 0 or ppo_col[i-1] is None or ppo_col[i-1] != ppo_col[i] or prev_age is None:




                        flip_age[i] = 0




                        prev_age = 0




                    else:




                        prev_age += 1




                        flip_age[i] = int(prev_age)





            # Bar timestamps from REST OHLC prefill (preferred) or synthetic fallback.




            open_ms_list = None




            close_ms_list = None




            try:




                tm = self._prefill_bar_times.get(tf) or {}




                open_ms_list = tm.get('open_ms')




                close_ms_list = tm.get('close_ms')




                if (not open_ms_list) or (not close_ms_list) or (len(open_ms_list) != n) or (len(close_ms_list) != n):




                    open_ms_list = None




                    close_ms_list = None




            except Exception:




                open_ms_list = None




                close_ms_list = None





            if open_ms_list is None:




                try:




                    tf_ms = int(interval_to_ms(tf))




                except Exception:




                    tf_ms = 60_000




                now_ms = int(time.time() * 1000)




                last_open_ms = now_ms - (now_ms % tf_ms)




                open_ms_list = [int(last_open_ms - (n - 1 - i) * tf_ms) for i in range(n)]




                close_ms_list = [int(om + tf_ms - 1) for om in open_ms_list]





            adx_series = self._prefill_adx_series.get(tf)
            atr_series = self._prefill_atr_series.get(tf)
            frama_series = self._prefill_frama_series.get(tf)
            vidya_series = self._prefill_vidya_series.get(tf)
            range_filter_series = self._prefill_range_filter_series.get(tf)
            ohlcv_series = self._prefill_ohlcv_series.get(tf)
            try:
                ohlcv_rows = list(self._closed_ohlcv_ring.get(tf, []))
            except Exception:
                ohlcv_rows = []
            ohlcv_by_close_ms = {}
            for row in ohlcv_rows:
                try:
                    ohlcv_by_close_ms[int(row[0])] = row
                except Exception:
                    continue




            if adx_series is not None and len(adx_series) != n:




                adx_series = None





            payloads = []




            for i in range(start_i, n):




                mval = float(macd_line[i]) if i < len(macd_line) else float('nan')




                sval = float(sig_line[i]) if i < len(sig_line) else float('nan')




                macd_v = float(mval) if np.isfinite(mval) else None




                sig_v = float(sval) if np.isfinite(sval) else None
                try:
                    rsi_raw = float(rsi_raw_series[i]) if i < len(rsi_raw_series) else float('nan')
                except Exception:
                    rsi_raw = float('nan')
                try:
                    rsi_smooth_raw = float(rsi_smooth_series[i]) if i < len(rsi_smooth_series) else float('nan')
                except Exception:
                    rsi_smooth_raw = float('nan')
                rsi_v = float(rsi_raw) if np.isfinite(rsi_raw) else None
                rsi_smooth_v = float(rsi_smooth_raw) if np.isfinite(rsi_smooth_raw) else None
                rsi_state_v = None
                if rsi_v is not None and rsi_smooth_v is not None:
                    rsi_state_v = 'GREEN' if rsi_v > rsi_smooth_v else 'RED'
                try:
                    stoch_k_raw = float(stoch_k_series[i]) if i < len(stoch_k_series) else float('nan')
                except Exception:
                    stoch_k_raw = float('nan')
                try:
                    stoch_d_raw = float(stoch_d_series[i]) if i < len(stoch_d_series) else float('nan')
                except Exception:
                    stoch_d_raw = float('nan')
                stoch_k_v = float(stoch_k_raw) if np.isfinite(stoch_k_raw) else None
                stoch_d_v = float(stoch_d_raw) if np.isfinite(stoch_d_raw) else None
                stoch_kd_v = None
                if stoch_k_v is not None and stoch_d_v is not None:
                    stoch_kd_v = 'GREEN' if stoch_k_v > stoch_d_v else 'RED'





                adx_v = di_p = di_m = di_s = None
                atr_v = atr_ang = None
                frama_state = None
                frama_break_up = False
                frama_break_down = False
                frama_mid_cross = False
                vidya_state = None
                vidya_trend_up = False
                vidya_trend_down = False
                vidya_delta_pct = None
                vidya_slope = None
                vidya_angle_deg = None
                vidya_angle_deg_norm = None
                vidya_angle_state = None
                vidya_angle_accel = None
                range_state = None
                range_phase = None
                range_buy = False
                range_sell = False
                range_line = None
                range_upper = None
                range_lower = None
                range_smooth = None
                range_upward_count = None
                range_downward_count = None




                if adx_series is not None:




                    try:




                        adx_v, di_p, di_m, di_s = adx_series[i]




                    except Exception:




                        pass
                if atr_series is not None:
                    try:
                        atr_v, atr_ang = atr_series[i]
                    except Exception:
                        pass
                if frama_series is not None:
                    try:
                        fr = frama_series[i] or {}
                        frama_state = fr.get("frama_state")
                        frama_break_up = bool(fr.get("frama_break_up", False))
                        frama_break_down = bool(fr.get("frama_break_down", False))
                        frama_mid_cross = bool(fr.get("frama_mid_cross", False))
                    except Exception:
                        pass
                if vidya_series is not None:
                    try:
                        vv = vidya_series[i] or {}
                        vidya_state = vv.get("vidya_state")
                        vidya_trend_up = bool(vv.get("vidya_trend_up", False))
                        vidya_trend_down = bool(vv.get("vidya_trend_down", False))
                        vidya_delta_pct = vv.get("vidya_delta_pct")
                        vidya_slope = vv.get("vidya_slope")
                        vidya_angle_deg = vv.get("vidya_angle_deg")
                        vidya_angle_deg_norm = vv.get("vidya_angle_deg_norm")
                        vidya_angle_state = vv.get("vidya_angle_state")
                        vidya_angle_accel = vv.get("vidya_angle_accel")
                    except Exception:
                        pass
                if range_filter_series is not None:
                    try:
                        rv = range_filter_series[i] or {}
                        range_state = rv.get("range_filter_state")
                        range_phase = rv.get("range_filter_phase")
                        range_buy = bool(rv.get("range_filter_buy", False))
                        range_sell = bool(rv.get("range_filter_sell", False))
                        range_line = rv.get("range_filter_line")
                        range_upper = rv.get("range_filter_upper")
                        range_lower = rv.get("range_filter_lower")
                        range_smooth = rv.get("range_filter_smooth_range")
                        range_upward_count = rv.get("range_filter_upward_count")
                        range_downward_count = rv.get("range_filter_downward_count")
                    except Exception:
                        pass

                bar_open = bar_high = bar_low = bar_close = bar_volume = None
                ohlcv_prefill = None
                if ohlcv_series is not None and i < len(ohlcv_series):
                    try:
                        ohlcv_prefill = ohlcv_series[i] or {}
                    except Exception:
                        ohlcv_prefill = None
                if isinstance(ohlcv_prefill, dict):
                    try:
                        bar_open = float(ohlcv_prefill.get("open")) if ohlcv_prefill.get("open") is not None else None
                        bar_high = float(ohlcv_prefill.get("high")) if ohlcv_prefill.get("high") is not None else None
                        bar_low = float(ohlcv_prefill.get("low")) if ohlcv_prefill.get("low") is not None else None
                        bar_close = float(ohlcv_prefill.get("close")) if ohlcv_prefill.get("close") is not None else None
                        bar_volume = float(ohlcv_prefill.get("volume")) if ohlcv_prefill.get("volume") is not None else None
                    except Exception:
                        bar_open = bar_high = bar_low = bar_close = bar_volume = None
                ohlcv_row = None
                if bar_open is None or bar_high is None or bar_low is None or bar_close is None:
                    try:
                        ohlcv_row = ohlcv_by_close_ms.get(int(close_ms_list[i]))
                    except Exception:
                        ohlcv_row = None
                    if ohlcv_row is None and len(ohlcv_rows) == n:
                        try:
                            ohlcv_row = ohlcv_rows[i]
                        except Exception:
                            ohlcv_row = None
                if ohlcv_row is not None and (bar_open is None or bar_high is None or bar_low is None or bar_close is None):
                    try:
                        _, bar_open, bar_high, bar_low, bar_close, bar_volume, _ = ohlcv_row
                    except Exception:
                        bar_open = bar_high = bar_low = bar_close = bar_volume = None
                taker_trade_count = None
                taker_buy_volume = None
                if isinstance(ohlcv_prefill, dict):
                    try:
                        taker_trade_count = ohlcv_prefill.get("trade_count")
                        taker_buy_volume = ohlcv_prefill.get("taker_buy_volume")
                    except Exception:
                        taker_trade_count = None
                        taker_buy_volume = None




                pl = {




                    'open': bar_open,
                    'high': bar_high,
                    'low': bar_low,
                    'close': bar_close,
                    'volume': bar_volume,
                    **compute_taker_bias_payload(bar_volume, taker_buy_volume, taker_trade_count),




                    'macd': macd_v,




                    'signal': sig_v,
                    'rsi': rsi_v,
                    'rsi_smooth': rsi_smooth_v,
                    'rsi_state': rsi_state_v,
                    'stoch_rsi_k': stoch_k_v,
                    'stoch_rsi_d': stoch_d_v,
                    'stoch_rsi_kd': stoch_kd_v,




                    'ms': ms_col[i],




                    'angle': ang_col[i],




                    'sig_angle': sig_ang_col[i],



                    'ppo': ppo_col[i],




                    'flip_age': flip_age[i],




                    'adx': adx_v,
                    'atr': atr_v,
                    'atr_angle': atr_ang,




                    'di_plus': di_p,




                    'di_minus': di_m,




                    'di_spread': di_s,
                    'frama_state': frama_state,
                    'frama_break_up': bool(frama_break_up),
                    'frama_break_down': bool(frama_break_down),
                    'frama_mid_cross': bool(frama_mid_cross),
                    'vidya_state': vidya_state,
                    'vidya_trend_up': bool(vidya_trend_up),
                    'vidya_trend_down': bool(vidya_trend_down),
                    'vidya_delta_pct': vidya_delta_pct,
                    'vidya_slope': vidya_slope,
                    'vidya_angle_deg': vidya_angle_deg,
                    'vidya_angle_deg_norm': vidya_angle_deg_norm,
                    'vidya_angle_state': vidya_angle_state,
                    'vidya_angle_accel': vidya_angle_accel,
                    'range_filter_state': range_state,
                    'range_filter_phase': range_phase,
                    'range_filter_buy': bool(range_buy),
                    'range_filter_sell': bool(range_sell),
                    'range_filter_line': range_line,
                    'range_filter_upper': range_upper,
                    'range_filter_lower': range_lower,
                    'range_filter_smooth_range': range_smooth,
                    'range_filter_upward_count': range_upward_count,
                    'range_filter_downward_count': range_downward_count,

                    'bar_open_time_ms': int(open_ms_list[i]),




                    'bar_close_time_ms': int(close_ms_list[i]),




                    'is_closed': True,




                    'seed': True,




                }
                pl = self._attach_market_state(tf, pl)
                payloads.append(pl)





            if not payloads:




                continue





            try:




                self._last_emitted_close_ms[tf] = int(payloads[-1].get('bar_close_time_ms'))




            except Exception:




                pass





            try:




                self.q.put(('live_closed_seed', tf, float(live_price), float(age_sec), payloads))




            except Exception:




                for pl in payloads:




                    self.q.put(('live_closed', tf, float(live_price), float(age_sec), pl))


    # -------------------------
    # Websockets
    # -------------------------
    def _ws_loop(self):
        streams: List[str] = []
        if self._orderflow_tape_enabled:
            streams.append(f"{self.symbol.lower()}@aggTrade")
        if self._orderflow_partial_enabled:
            streams.append(f"{self.symbol.lower()}@depth20@100ms")
        if self._orderflow_local_l2_enabled:
            streams.append(f"{self.symbol.lower()}@depth@100ms")
        if self.price_source == "LAST":
            streams += [f"{self.symbol.lower()}@kline_{tf}" for tf in self.tfs]
        else:
            # markPrice stream provides mark + index; keep market-data streams for microstructure filtering
            streams += [f"{self.symbol.lower()}@markPrice@1s"]

        ws_url = f"wss://fstream.binance.com/stream?streams={'/'.join(streams)}"

        def on_open(ws):
            self.log("[Live] Websocket connected")
            if self._orderflow_local_l2_enabled:
                self._schedule_orderflow_local_book_sync(reason="websocket_open")

        def on_message(ws, message):
            try:
                msg = json.loads(message)
                data = msg.get("data", {})
                stream = msg.get("stream", "")

                if "@depth20" in stream:
                    try:
                        bids = data.get("b", []) or []
                        asks = data.get("a", []) or []
                        event_ms = int(data.get("E") or data.get("T") or int(time.time() * 1000))
                        self._update_orderflow_depth_partial(event_ms, bids, asks)
                    except Exception:
                        pass
                    return

                if "@depth" in stream and "@depth20" not in stream:
                    try:
                        self._update_orderflow_depth_diff(data)
                    except Exception:
                        pass
                    return

                if stream.endswith("@aggTrade"):
                    p = float(data.get("p"))
                    try:
                        q = float(data.get("q"))
                    except Exception:
                        q = 0.0
                    try:
                        t_ms = int(data.get("T"))
                    except Exception:
                        t_ms = int(time.time() * 1000)
                    try:
                        is_buyer_maker = bool(data.get("m"))
                    except Exception:
                        is_buyer_maker = False
                    with self.lock:
                        self.live_price = p
                        self.last_ws_tick_ts = time.time()
                    try:
                        self._update_orderflow_trade(t_ms, p, q, is_buyer_maker)
                    except Exception:
                        pass
                    return

                if "@markPrice" in stream:
                    # futures mark price stream (1s). Payload sometimes uses short keys.
                    # Common keys: p (mark price), i (index price)
                    mp = data.get("p", data.get("markPrice", None))
                    ip = data.get("i", data.get("indexPrice", None))
                    if self.price_source == "MARK":
                        val = float(mp) if mp is not None else None
                    else:
                        val = float(ip) if ip is not None else None
                    if val is not None:
                        with self.lock:
                            self.live_price = val
                            self.last_ws_tick_ts = time.time()
                    return

                # kline stream
                k = data.get("k", {})
                tf = k.get("i")
                if tf not in self.storage:
                    return

                # Track bar open/close times so downstream can align "bars ago" to real candles.
                try:
                    open_t = int(k.get("t"))  # open time (ms)
                except Exception:
                    open_t = 0

                c = float(k.get("c"))
                try:
                    o = float(k.get("o"))
                except Exception:
                    o = c
                try:
                    vol = float(k.get("v"))
                except Exception:
                    vol = 0.0
                try:
                    trade_count = int(k.get("n"))
                except Exception:
                    trade_count = 0
                try:
                    taker_buy_volume = float(k.get("V"))
                except Exception:
                    taker_buy_volume = 0.0
                is_closed = bool(k.get("x"))

                with self.lock:
                    self.last_ws_tick_ts = time.time()

                if is_closed:
                    # Official kline close (LAST). Track close time to prevent duplicates.
                    try:
                        close_t = int(k.get("T"))  # close time (ms)
                    except Exception:
                        close_t = 0
                    with self.lock:
                        st = self.storage.get(tf)
                        if st is not None:
                            if open_t:
                                st.last_open_time_ms = open_t
                            if st.last_close_time_ms is None or (close_t and close_t > st.last_close_time_ms):
                                st.closes.append(c)
                                st.last_close_time_ms = close_t or st.last_close_time_ms
                                # Store last seen OHLC (helps ADX preview be correct immediately after a close)
                                try:
                                    st.forming_high = float(k.get("h"))
                                    st.forming_low = float(k.get("l"))
                                    st.forming_close = float(c)
                                    st.forming_volume = float(vol)
                                    st.forming_trade_count = int(trade_count)
                                    st.forming_taker_buy_volume = float(taker_buy_volume)
                                except Exception:
                                    pass
                                try:
                                    self._latest_closed_taker_metrics[tf] = {
                                        "volume": float(vol),
                                        "trade_count": int(trade_count),
                                        "taker_buy_volume": float(taker_buy_volume),
                                    }
                                except Exception:
                                    pass

                                # Append CLOSED OHLC to ring buffer for sanity rebuild / drift prevention.
                                try:
                                    self._ring_append_closed_ohlc(tf, int(close_t), float(k.get("h")), float(k.get("l")), float(c), open_t_ms=(int(open_t) if open_t else None))
                                    self._ring_append_closed_ohlcv(
                                        tf,
                                        int(close_t),
                                        float(o),
                                        float(k.get("h")),
                                        float(k.get("l")),
                                        float(c),
                                        float(vol),
                                        open_t_ms=(int(open_t) if open_t else None),
                                    )
                                except Exception:
                                    pass

                                # Update ADX/DI state on closed candle.
                                # Prefill is loaded in background; before it finishes we buffer OHLC so we can replay
                                # onto the freshly seeded state (avoids double-counting / drift).
                                try:
                                    h = float(k.get("h"))
                                    l = float(k.get("l"))
                                    if self.prefill_done:
                                        st_adx = self.adx_state.get(tf)
                                        if st_adx is not None:
                                            st_adx.update(h, l, c)
                                        try:
                                            st_atr = self.atr_state.get(tf)
                                            if st_atr is not None:
                                                st_atr.update(h, l, c)
                                        except Exception:
                                            pass
                                        try:
                                            st_fr = self.frama_state.get(tf)
                                            if st_fr is not None:
                                                st_fr.update(h, l, c)
                                        except Exception:
                                            pass
                                        try:
                                            st_vd = self.vidya_state.get(tf)
                                            if st_vd is not None:
                                                st_vd.update(o, h, l, c, vol)
                                        except Exception:
                                            pass
                                        try:
                                            st_rf = self.range_filter_state.get(tf)
                                            if st_rf is not None:
                                                st_rf.update(c)
                                        except Exception:
                                            pass
                                    else:
                                        try:
                                            pend = self._pending_adx_ohlc.get(tf)
                                            if pend is not None:
                                                pend.append((int(close_t), float(h), float(l), float(c)))
                                            try:
                                                pend2 = self._pending_atr_ohlc.get(tf)
                                                if pend2 is not None:
                                                    pend2.append((int(close_t), float(h), float(l), float(c)))
                                                    if len(pend2) > 500:
                                                        del pend2[:-200]
                                            except Exception:
                                                pass
                                            try:
                                                pend4 = self._pending_vidya_ohlcv.get(tf)
                                                if pend4 is not None:
                                                    pend4.append((int(close_t), float(o), float(h), float(l), float(c), float(vol), int(trade_count), float(taker_buy_volume)))
                                            except Exception:
                                                pass
                                            try:
                                                pend5 = self._pending_range_filter_close.get(tf)
                                                if pend5 is not None:
                                                    pend5.append(float(c))
                                                    if len(pend5) > 500:
                                                        del pend5[:-200]
                                                    if len(pend4) > 500:
                                                        del pend4[:-200]
                                            except Exception:
                                                pass
                                                if len(pend) > 500:
                                                    del pend[:-200]
                                        except Exception:
                                            pass
                                except Exception:
                                    pass

                                if len(st.closes) > MAX_STORED_CLOSES:
                                    st.closes.pop(0)
                else:
                    # Forming candle update: record current OHLC so indicators can preview (if enabled),
                    # and still record the bar open time so the UI aligns to exchange candle boundaries.
                    try:
                        fh = float(k.get("h"))
                    except Exception:
                        fh = float("nan")
                    try:
                        fl = float(k.get("l"))
                    except Exception:
                        fl = float("nan")
                    if open_t or np.isfinite(fh) or np.isfinite(fl):
                        with self.lock:
                            st = self.storage.get(tf)
                            if st is not None:
                                if open_t:
                                    st.last_open_time_ms = open_t
                                if np.isfinite(fh):
                                    st.forming_high = float(fh)
                                if np.isfinite(fl):
                                    st.forming_low = float(fl)
                                try:
                                    st.forming_close = float(c)
                                    st.forming_volume = float(vol)
                                    st.forming_trade_count = int(trade_count)
                                    st.forming_taker_buy_volume = float(taker_buy_volume)
                                except Exception:
                                    pass

            except Exception:
                pass

        def on_error(ws, error):
            self.log(f"[Live] Websocket error: {error}")

        def on_close(ws, code, reason):
            self.log(f"[Live] Websocket closed: {code} {reason}")

        while self.running:
            try:
                self.ws = websocket.WebSocketApp(
                    ws_url,
                    on_open=on_open,
                    on_message=on_message,
                    on_error=on_error,
                    on_close=on_close,
                )
                self.ws.run_forever(ping_interval=None)
            except Exception as e:
                self.log(f"[Live] run_forever exception: {e}")

            if self.running:
                self.log("[Live] Reconnecting in 2s…")
                time.sleep(2)

    # -------------------------
    # REST fallback
    # -------------------------
    def _fallback_rest_loop(self):
        sess = requests.Session()
        while self.running:
            time.sleep(LIVE_REST_FALLBACK_SEC)
            try:
                with self.lock:
                    last = self.last_ws_tick_ts
                if last and (time.time() - last) < LIVE_WS_STALE_SEC:
                    continue

                if self.price_source == "LAST":
                    r = sess.get(FAPI_BASE + FAPI_TICKER_PRICE, params={"symbol": self.symbol}, timeout=REQUEST_TIMEOUT)
                    r.raise_for_status()
                    p = float(r.json().get("price"))
                else:
                    # premiumIndex includes markPrice + indexPrice
                    r = sess.get(FAPI_BASE + FAPI_PREMIUM_INDEX, params={"symbol": self.symbol}, timeout=REQUEST_TIMEOUT)
                    r.raise_for_status()
                    j = r.json()
                    if self.price_source == "MARK":
                        p = float(j.get("markPrice"))
                    else:
                        p = float(j.get("indexPrice"))

                with self.lock:
                    self.live_price = p
                self.log("[Live] WS stale -> REST price poll used")
            except Exception:
                pass


    # -------------------------
    # Kline repair for LAST source (keeps indicator math aligned with TradingView)
    # -------------------------
    def _fetch_closed_klines_since(self, sess: "requests.Session", tf: str, start_ms: Optional[int]) -> List[Tuple[int, float]]:
        """
        Returns list of (close_time_ms, close_price) for CLOSED candles strictly after start_ms.
        Uses Binance Futures /fapi/v1/klines.

        We drop the still-forming candle (last row) if it is not yet closed.
        """
        interval = tf
        params: Dict[str, Any] = {"symbol": self.symbol, "interval": interval, "limit": 1000}
        if start_ms is not None:
            params["startTime"] = int(start_ms) + 1

        r = sess.get(FAPI_BASE + FAPI_KLINES, params=params, timeout=REQUEST_TIMEOUT)
        r.raise_for_status()
        rows = r.json() or []
        out: List[Tuple[int, float]] = []
        if not rows:
            return out

        now_ms = int(time.time() * 1000)

        # Each row: [open_time, open, high, low, close, volume, close_time, ...]
        for row in rows:
            try:
                close_t = int(row[6])
                close_px = float(row[4])
            except Exception:
                continue
            # closed if close_time <= now_ms (Binance close_time is end timestamp)
            if close_t <= now_ms:
                if start_ms is None or close_t > int(start_ms):
                    out.append((close_t, close_px))

        return out



    def _fetch_closed_ohlc_since(self, sess: "requests.Session", tf: str, start_ms: Optional[int]) -> List[Tuple[int, int, float, float, float, float, float, int, float]]:
        """Returns list of (open_time_ms, close_time_ms, open, high, low, close, volume, trade_count, taker_buy_volume) for CLOSED candles strictly after start_ms.

        Uses Binance Futures /fapi/v1/klines.
        We drop the still-forming candle (last row) if it is not yet closed.
        """
        interval = tf
        params: Dict[str, Any] = {"symbol": self.symbol, "interval": interval, "limit": 1000}
        if start_ms is not None:
            params["startTime"] = int(start_ms) + 1

        r = sess.get(FAPI_BASE + FAPI_KLINES, params=params, timeout=REQUEST_TIMEOUT)
        r.raise_for_status()
        rows = r.json() or []
        out: List[Tuple[int, int, float, float, float, float, float, int, float]] = []
        if not rows:
            return out

        now_ms = int(time.time() * 1000)
        for row in rows:
            try:
                open_t = int(row[0])
                open_px = float(row[1])
                high = float(row[2])
                low = float(row[3])
                close = float(row[4])
                volume = float(row[5]) if len(row) > 5 else 0.0
                close_t = int(row[6])
                trade_count = int(float(row[8])) if len(row) > 8 else 0
                taker_buy_volume = float(row[9]) if len(row) > 9 else 0.0
            except Exception:
                continue
            if close_t <= now_ms:
                if start_ms is None or close_t > int(start_ms):
                    out.append((open_t, close_t, open_px, high, low, close, volume, trade_count, taker_buy_volume))

        return out

    def _init_last_close_times(self):
        """
        After prefill (closes list) we try to initialize `last_close_time_ms` for each TF,
        so the repair loop can safely fill any gaps.
        """
        if self.price_source != "LAST":
            return
        sess = requests.Session()
        for tf in self.tfs:
            try:
                # Fetch the most recent CLOSED kline (full OHLC) and set last_close_time_ms.
                kl_ohlc = self._fetch_closed_ohlc_since(sess, tf, start_ms=None)
                if not kl_ohlc:
                    # Fallback to close-only list if needed.
                    kl = self._fetch_closed_klines_since(sess, tf, start_ms=None)
                    if not kl:
                        continue
                    close_t, close_px = kl[-1]
                    open_t = 0
                    open_px = float(close_px)
                    volume_px = 0.0
                    trade_count_px = 0
                    taker_buy_volume_px = 0.0
                    high = float("nan")
                    low = float("nan")
                else:
                    open_t, close_t, open_px, high, low, close_px, volume_px, trade_count_px, taker_buy_volume_px = kl_ohlc[-1]
                with self.lock:
                    st = self.storage.get(tf)
                    if not st:
                        continue
                    st.last_close_time_ms = close_t
                    if open_t:
                        st.last_open_time_ms = int(open_t)
                    # Use this as a baseline forming OHLC (helps preview immediately after init)
                    try:
                        if np.isfinite(float(high)):
                            st.forming_high = float(high)
                        if np.isfinite(float(low)):
                            st.forming_low = float(low)
                        st.forming_close = float(close_px)
                        st.forming_volume = float(volume_px)
                        st.forming_trade_count = int(trade_count_px)
                        st.forming_taker_buy_volume = float(taker_buy_volume_px)
                    except Exception:
                        pass
                    try:
                        self._latest_closed_taker_metrics[tf] = {
                            "volume": float(volume_px),
                            "trade_count": int(trade_count_px),
                            "taker_buy_volume": float(taker_buy_volume_px),
                        }
                    except Exception:
                        pass

                    # Add to ring buffer (if we have OHLC).
                    try:
                        if np.isfinite(float(high)) and np.isfinite(float(low)):
                            self._ring_append_closed_ohlc(tf, int(close_t), float(high), float(low), float(close_px), open_t_ms=(int(open_t) if open_t else None))
                            self._ring_append_closed_ohlcv(
                                tf,
                                int(close_t),
                                float(open_px),
                                float(high),
                                float(low),
                                float(close_px),
                                float(volume_px),
                                open_t_ms=(int(open_t) if open_t else None),
                            )
                    except Exception:
                        pass

                    # If closes are empty (shouldn't happen after prefill), seed with this.
                    did_append_close = False
                    if not st.closes:
                        st.closes.append(close_px)
                        did_append_close = True
                    else:
                        # Make sure latest CLOSED candle is present.
                        # Prefill/WS race can miss the very latest close; without this, MACD can drift.
                        try:
                            if abs(float(st.closes[-1]) - float(close_px)) > 1e-9:
                                st.closes.append(close_px)
                                did_append_close = True
                                if len(st.closes) > MAX_STORED_CLOSES:
                                    st.closes.pop(0)
                        except Exception:
                            pass

                    # Keep ADX/ATR/FRAMA/VIDYA state aligned with the latest closed candle if we have OHLC.
                    # IMPORTANT: only apply if we actually appended this close (otherwise we'd double-count).
                    try:
                        if np.isfinite(float(high)) and np.isfinite(float(low)):
                            if not did_append_close:
                                pass
                            elif self.prefill_done:
                                st_adx = self.adx_state.get(tf)
                                if st_adx is not None:
                                    st_adx.update(float(high), float(low), float(close_px))
                                st_atr = self.atr_state.get(tf)
                                if st_atr is not None:
                                    st_atr.update(float(high), float(low), float(close_px))
                                st_fr = self.frama_state.get(tf)
                                if st_fr is not None:
                                    st_fr.update(float(high), float(low), float(close_px))
                                st_vd = self.vidya_state.get(tf)
                                if st_vd is not None:
                                    st_vd.update(float(open_px), float(high), float(low), float(close_px), float(volume_px))
                                st_rf = self.range_filter_state.get(tf)
                                if st_rf is not None:
                                    st_rf.update(float(close_px))
                            else:
                                try:
                                    pend = self._pending_adx_ohlc.get(tf)
                                    if pend is not None:
                                        pend.append((int(close_t), float(high), float(low), float(close_px)))
                                except Exception:
                                    pass
                                try:
                                    pend2 = self._pending_atr_ohlc.get(tf)
                                    if pend2 is not None:
                                        pend2.append((int(close_t), float(high), float(low), float(close_px)))
                                except Exception:
                                    pass
                                try:
                                    pend3 = self._pending_frama_ohlc.get(tf)
                                    if pend3 is not None:
                                        pend3.append((int(close_t), float(high), float(low), float(close_px)))
                                except Exception:
                                    pass
                                try:
                                    pend5 = self._pending_range_filter_close.get(tf)
                                    if pend5 is not None:
                                        pend5.append(float(close_px))
                                        if len(pend5) > 500:
                                            del pend5[:-200]
                                except Exception:
                                    pass
                                try:
                                    pend4 = self._pending_vidya_ohlcv.get(tf)
                                    if pend4 is not None:
                                        pend4.append((int(close_t), float(open_px), float(high), float(low), float(close_px), float(volume_px), int(trade_count_px), float(taker_buy_volume_px)))
                                except Exception:
                                    pass
                    except Exception:
                        pass
            except Exception:
                continue

    def _repair_closes_loop(self):
        """
        Periodically checks for missed CLOSED klines and appends them.
        This is the #1 reason MACD can drift away from TradingView if websocket drops klines.
        """
        if self.price_source != "LAST":
            return

        sess = requests.Session()

        # Wait briefly for prefill to populate closes; then initialize last_close_time_ms.
        time.sleep(0.8)
        try:
            self._init_last_close_times()
        except Exception:
            pass

        tf_ms: Dict[str, int] = {}
        for tf in self.tfs:
            try:
                tf_ms[tf] = interval_to_ms(tf)
            except Exception:
                tf_ms[tf] = 60_000


        while self.running:
            try:
                time.sleep(1.0)
                now_ms = int(time.time() * 1000)

                for tf in self.tfs:
                    interval_ms = tf_ms.get(tf, 60_000)
                    with self.lock:
                        st = self.storage.get(tf)
                        last_ct = st.last_close_time_ms if st else None

                    # If we don't know last close time yet, skip; init will catch up soon.
                    if last_ct is None:
                        continue

                    # Only query when at least one new candle should have closed.
                    if (now_ms - last_ct) < interval_ms:
                        continue

                    kl_ohlc = self._fetch_closed_ohlc_since(sess, tf, start_ms=last_ct)
                    if not kl_ohlc:
                        continue

                    with self.lock:
                        st = self.storage.get(tf)
                        if not st:
                            continue
                        for ot, ct, op, h, l, c, vol, trade_count, taker_buy_volume in kl_ohlc:
                            if st.last_close_time_ms is None or int(ct) > int(st.last_close_time_ms):
                                try:
                                    st.closes.append(float(c))
                                except Exception:
                                    pass
                                st.last_close_time_ms = int(ct)
                                try:
                                    st.last_open_time_ms = int(ot)
                                except Exception:
                                    pass

                                # Store last seen OHLC for preview.
                                try:
                                    st.forming_high = float(h)
                                    st.forming_low = float(l)
                                    st.forming_close = float(c)
                                    st.forming_volume = float(vol)
                                    st.forming_trade_count = int(trade_count)
                                    st.forming_taker_buy_volume = float(taker_buy_volume)
                                except Exception:
                                    pass
                                try:
                                    self._latest_closed_taker_metrics[tf] = {
                                        "volume": float(vol),
                                        "trade_count": int(trade_count),
                                        "taker_buy_volume": float(taker_buy_volume),
                                    }
                                except Exception:
                                    pass

                                # Append CLOSED OHLC to ring.
                                try:
                                    self._ring_append_closed_ohlc(tf, int(ct), float(h), float(l), float(c), open_t_ms=int(ot))
                                    self._ring_append_closed_ohlcv(
                                        tf,
                                        int(ct),
                                        float(op),
                                        float(h),
                                        float(l),
                                        float(c),
                                        float(vol),
                                        open_t_ms=int(ot),
                                    )
                                except Exception:
                                    pass

                                # Keep ADX/ATR in sync with repaired candles.
                                try:
                                    if self.prefill_done:
                                        st_adx = self.adx_state.get(tf)
                                        if st_adx is not None:
                                            st_adx.update(float(h), float(l), float(c))
                                        st_atr = self.atr_state.get(tf)
                                        if st_atr is not None:
                                            st_atr.update(float(h), float(l), float(c))
                                        st_fr = self.frama_state.get(tf)
                                        if st_fr is not None:
                                            st_fr.update(float(h), float(l), float(c))
                                        st_vd = self.vidya_state.get(tf)
                                        if st_vd is not None:
                                            st_vd.update(float(op), float(h), float(l), float(c), float(vol))
                                        st_rf = self.range_filter_state.get(tf)
                                        if st_rf is not None:
                                            st_rf.update(float(c))
                                    else:
                                        try:
                                            pend = self._pending_adx_ohlc.get(tf)
                                            if pend is not None:
                                                pend.append((int(ct), float(h), float(l), float(c)))
                                                if len(pend) > 500:
                                                    del pend[:-200]
                                        except Exception:
                                            pass
                                        try:
                                            pend2 = self._pending_atr_ohlc.get(tf)
                                            if pend2 is not None:
                                                pend2.append((int(ct), float(h), float(l), float(c)))
                                                if len(pend2) > 500:
                                                    del pend2[:-200]
                                        except Exception:
                                            pass
                                        try:
                                            pend5 = self._pending_range_filter_close.get(tf)
                                            if pend5 is not None:
                                                pend5.append(float(c))
                                                if len(pend5) > 500:
                                                    del pend5[:-200]
                                        except Exception:
                                            pass
                                        try:
                                            pend3 = self._pending_frama_ohlc.get(tf)
                                            if pend3 is not None:
                                                pend3.append((int(ct), float(h), float(l), float(c)))
                                                if len(pend3) > 500:
                                                    del pend3[:-200]
                                        except Exception:
                                            pass
                                        try:
                                            pend4 = self._pending_vidya_ohlcv.get(tf)
                                            if pend4 is not None:
                                                pend4.append((int(ct), float(op), float(h), float(l), float(c), float(vol), int(trade_count), float(taker_buy_volume)))
                                                if len(pend4) > 500:
                                                    del pend4[:-200]
                                        except Exception:
                                            pass
                                except Exception:
                                    pass

                                if len(st.closes) > MAX_STORED_CLOSES:
                                    try:
                                        st.closes.pop(0)
                                    except Exception:
                                        pass
            except Exception:
                # Keep running; this is best-effort.
                continue



    # -------------------------
    # Periodic sanity rebuild (prevents ADX/ATR drift in long-running sessions)
    # -------------------------
    def _sanity_rebuild_loop(self):
        """Every N seconds, rebuild ADX/ATR state from the CLOSED OHLC ring.

        This is a lightweight alternative to restarting the live engine.
        It fixes the long-run drift scenario where:
          - websocket misses a closed kline,
          - repair loop patches closes,
          - but indicator state did not receive the missing OHLC.

        The rebuild uses only the bars stored in the ring (rolling window). With Wilder/RMA,
        a few hundred to a few thousand bars is enough for practical convergence.
        """
        # Give prefill a little time to populate the ring.
        time.sleep(2.0)

        while self.running:
            try:
                time.sleep(float(self._sanity_rebuild_sec))
            except Exception:
                time.sleep(60.0)

            # Only meaningful for LAST (official OHLC). MARK/INDEX are synthetic in this project.
            if self.price_source != "LAST":
                continue

            # Snapshot current values for comparison.
            try:
                with self.lock:
                    cur_snap = {
                        tf: (
                            (self.adx_state.get(tf).snapshot()[0] if self.adx_state.get(tf) is not None else None),
                            (self.atr_state.get(tf).snapshot()[0] if self.atr_state.get(tf) is not None else None),
                        )
                        for tf in self.tfs
                    }
            except Exception:
                cur_snap = {}

            for tf in self.tfs:
                try:
                    # Build a rebuilt state WITHOUT swapping yet, so we can compare.
                    rows = list(self._closed_ohlc_ring.get(tf, []))
                    if len(rows) < 20:
                        continue

                    fresh_adx = make_adx_state(self.adx_impl, ADX_LEN)
                    fresh_atr = make_atr_state(ATR_LEN)
                    for _ct, h, l, c, _ot in rows:
                        try:
                            fresh_adx.update(float(h), float(l), float(c))
                        except Exception:
                            pass
                        try:
                            fresh_atr.update(float(h), float(l), float(c))
                        except Exception:
                            pass

                    try:
                        new_adx = fresh_adx.snapshot()[0]
                    except Exception:
                        new_adx = None
                    try:
                        new_atr = fresh_atr.snapshot()[0]
                    except Exception:
                        new_atr = None

                    old_adx, old_atr = cur_snap.get(tf, (None, None))

                    def _finite(x: Any) -> Optional[float]:
                        try:
                            if x is None:
                                return None
                            xf = float(x)
                            return xf if np.isfinite(xf) else None
                        except Exception:
                            return None

                    o_adx = _finite(old_adx)
                    n_adx = _finite(new_adx)
                    o_atr = _finite(old_atr)
                    n_atr = _finite(new_atr)

                    adx_diff = None
                    atr_diff = None
                    if o_adx is not None and n_adx is not None:
                        adx_diff = abs(float(o_adx) - float(n_adx))
                    if o_atr is not None and n_atr is not None:
                        atr_diff = abs(float(o_atr) - float(n_atr))

                    need_swap = False

                    # If one is missing but the other exists, or diff exceeds threshold: swap.
                    if (o_adx is None and n_adx is not None) or (o_adx is not None and n_adx is None):
                        need_swap = True
                    if adx_diff is not None and adx_diff >= float(self._sanity_adx_threshold):
                        need_swap = True

                    if (o_atr is None and n_atr is not None) or (o_atr is not None and n_atr is None):
                        need_swap = True
                    if atr_diff is not None and atr_diff >= float(self._sanity_atr_threshold):
                        need_swap = True

                    if need_swap:
                        try:
                            with self.lock:
                                self.adx_state[tf] = fresh_adx
                                self.atr_state[tf] = fresh_atr
                                self._last_live_adx_preview[tf] = None
                                self._last_live_adx_slope[tf] = None
                                self._last_live_atr_preview[tf] = None
                                self._last_live_atr_slope[tf] = None
                        except Exception:
                            pass

                        try:
                            self.log(f"[Live] Sanity rebuild applied for {tf}: ADX {o_adx} → {n_adx} (Δ={adx_diff}), ATR {o_atr} → {n_atr} (Δ={atr_diff})")
                        except Exception:
                            pass

                except Exception:
                    # Keep loop resilient.
                    continue

    # -------------------------
    # Synthetic candle closes for MARK/INDEX sources
    # -------------------------
    def _append_synth_closes_if_needed(self, live_price: float):
        """
        For MARK/INDEX we don't get official kline closes, so we synthesize them from UTC boundaries.
        This is "good enough" for monitoring; for precise backtests use downloaded 1m candles.
        """
        if self.price_source == "LAST":
            return

        now_ms = int(pd.Timestamp.utcnow().timestamp() * 1000)
        cur_min_open_ms = now_ms - (now_ms % 60_000)

        with self.lock:
            if self._last_minute_open_ms is None:
                self._last_minute_open_ms = cur_min_open_ms
                return

            # Catch up minute-by-minute if the UI / thread stalled.
            while self._last_minute_open_ms < cur_min_open_ms:
                next_open_ms = self._last_minute_open_ms + 60_000  # new minute open
                close_price = float(live_price)

                # 1m always closes each minute
                if "1m" in self.storage:
                    st1 = self.storage["1m"]
                    st1.closes.append(close_price)
                    st1.last_open_time_ms = int(self._last_minute_open_ms)
                    st1.last_close_time_ms = int(next_open_ms - 1)
                    try:
                        self._ring_append_closed_ohlcv(
                            "1m",
                            int(next_open_ms - 1),
                            float(close_price),
                            float(close_price),
                            float(close_price),
                            float(close_price),
                            0.0,
                            open_t_ms=int(self._last_minute_open_ms),
                        )
                    except Exception:
                        pass
                    if len(self.storage["1m"].closes) > MAX_STORED_CLOSES:
                        self.storage["1m"].closes.pop(0)

                # other TFs close when their next bar opens on boundary
                for tf in self.tfs:
                    if tf == "1m":
                        continue
                    try:
                        tf_ms = interval_to_ms(tf)
                    except Exception:
                        continue
                    if next_open_ms % tf_ms == 0:
                        stt = self.storage[tf]
                        stt.closes.append(close_price)
                        stt.last_open_time_ms = int(next_open_ms - tf_ms)
                        stt.last_close_time_ms = int(next_open_ms - 1)
                        try:
                            self._ring_append_closed_ohlcv(
                                tf,
                                int(next_open_ms - 1),
                                float(close_price),
                                float(close_price),
                                float(close_price),
                                float(close_price),
                                0.0,
                                open_t_ms=int(next_open_ms - tf_ms),
                            )
                        except Exception:
                            pass
                        if len(self.storage[tf].closes) > MAX_STORED_CLOSES:
                            self.storage[tf].closes.pop(0)

                self._last_minute_open_ms = next_open_ms

    # -------------------------
    # Compute + emit statuses
    # -------------------------
    def _compute_loop(self):
        while self.running:
            time.sleep(LIVE_COMPUTE_INTERVAL_SEC)

            with self.lock:
                live_price = self.live_price
                last_tick = self.last_ws_tick_ts

            if live_price is None:
                continue

            # If MARK/INDEX, synthesize candle closes so multi-timeframe indicators move.
            self._append_synth_closes_if_needed(live_price)

            with self.lock:
                closes_snapshot = {tf: st.closes[:] for tf, st in self.storage.items()}
                open_snapshot = {tf: st.last_open_time_ms for tf, st in self.storage.items()}
                close_snapshot = {tf: st.last_close_time_ms for tf, st in self.storage.items()}
                forming_snapshot = {tf: (st.forming_high, st.forming_low, st.forming_close) for tf, st in self.storage.items()}
                adx_snapshot = {tf: (self.adx_state.get(tf).snapshot() if hasattr(self, "adx_state") and self.adx_state.get(tf) is not None else (None, None, None, None, None)) for tf in closes_snapshot.keys()}
                atr_snapshot = {tf: (self.atr_state.get(tf).snapshot() if hasattr(self, "atr_state") and self.atr_state.get(tf) is not None else (None, None)) for tf in closes_snapshot.keys()}
                frama_snapshot = {tf: (self.frama_state.get(tf).snapshot() if hasattr(self, "frama_state") and self.frama_state.get(tf) is not None else {}) for tf in closes_snapshot.keys()}
                vidya_snapshot = {tf: (self.vidya_state.get(tf).snapshot() if hasattr(self, "vidya_state") and self.vidya_state.get(tf) is not None else {}) for tf in closes_snapshot.keys()}
                range_filter_snapshot = {tf: (self.range_filter_state.get(tf).snapshot() if hasattr(self, "range_filter_state") and self.range_filter_state.get(tf) is not None else {}) for tf in closes_snapshot.keys()}

            age_sec = max(0.0, time.time() - last_tick) if last_tick else 9999.0

            for tf, closes in closes_snapshot.items():
                full_closes = self._full_closes_for_tf(
                    tf,
                    closes,
                    live_price,
                    forming_snapshot.get(tf),
                )
                if len(full_closes) < MIN_FULL_CLOSES:
                    self.q.put(("live_status", tf, float(live_price), age_sec, None))
                    continue

                try:
                    macd, sig, ms, ang, sig_ang, ppo, fa = compute_live_indicators(full_closes, self.macd_impl)
                    ema_live = self._build_ema_payload(full_closes, tf=tf)
                    rsi_live = self._build_rsi_payload(full_closes, tf=tf)
                    stoch_rsi_live = self._build_stoch_rsi_payload(full_closes)
                    # ADX/DI: by default use last CLOSED-bar snapshot; if live forming is enabled and we
                    # have a forming OHLC update from the kline stream, preview ADX intrabar (TradingView-like).
                    adx_live = adx_snapshot.get(tf, (None, None, None, None, None))
                    if self.live_use_forming:
                        try:
                            fh, fl, fc = forming_snapshot.get(tf, (None, None, None))
                            if fh is not None and fl is not None and fc is not None:
                                if np.isfinite(float(fh)) and np.isfinite(float(fl)) and np.isfinite(float(fc)):
                                    st_adx = self.adx_state.get(tf) if hasattr(self, "adx_state") else None
                                    if st_adx is not None:
                                        adx_live = st_adx.preview(float(fh), float(fl), float(fc))
                        except Exception:
                            pass
                    # ADX angle (TradingView-like slope):
                    # - When using forming candle, compare successive LIVE preview values (intrabar slope).
                    # - Otherwise keep the CLOSED-bar slope (compare current closed ADX vs previous closed ADX).
                    adx_val_live = None
                    try:
                        if adx_live is not None and len(adx_live) > 0 and adx_live[0] is not None:
                            adx_val_live = float(adx_live[0])
                            if not np.isfinite(adx_val_live):
                                adx_val_live = None
                    except Exception:
                        adx_val_live = None

                    adx_ang_live = None
                    try:
                        adx_ang_live = adx_live[4] if (adx_live is not None and len(adx_live) > 4) else None
                    except Exception:
                        adx_ang_live = None

                    if self.live_use_forming and adx_val_live is not None:
                        prev_live = self._last_live_adx_preview.get(tf)
                        try:
                            if prev_live is not None:
                                prev_live_f = float(prev_live)
                                if np.isfinite(prev_live_f):
                                    if float(adx_val_live) > float(prev_live_f):
                                        adx_ang_live = "GREEN"
                                    elif float(adx_val_live) < float(prev_live_f):
                                        adx_ang_live = "RED"
                                    else:
                                        # flat: keep previous slope if known
                                        adx_ang_live = self._last_live_adx_slope.get(tf) or adx_ang_live
                        except Exception:
                            pass
                        # persist last live preview for next tick
                        try:
                            self._last_live_adx_preview[tf] = float(adx_val_live)
                            self._last_live_adx_slope[tf] = adx_ang_live
                        except Exception:
                            pass



                    # ATR: by default use last CLOSED-bar snapshot; if live forming is enabled and we
                    # have a forming OHLC update from the kline stream, preview ATR intrabar (TradingView-like).
                    atr_live = atr_snapshot.get(tf, (None, None))
                    if self.live_use_forming:
                        try:
                            fh, fl, fc = forming_snapshot.get(tf, (None, None, None))
                            if fh is not None and fl is not None and fc is not None:
                                if np.isfinite(float(fh)) and np.isfinite(float(fl)) and np.isfinite(float(fc)):
                                    st_atr = self.atr_state.get(tf) if hasattr(self, "atr_state") else None
                                    if st_atr is not None:
                                        atr_live = st_atr.preview(float(fh), float(fl), float(fc))
                        except Exception:
                            pass

                    atr_val_live = None
                    try:
                        if atr_live is not None and len(atr_live) > 0 and atr_live[0] is not None:
                            atr_val_live = float(atr_live[0])
                            if not np.isfinite(atr_val_live):
                                atr_val_live = None
                    except Exception:
                        atr_val_live = None

                    atr_ang_live = None
                    try:
                        atr_ang_live = atr_live[1] if (atr_live is not None and len(atr_live) > 1) else None
                    except Exception:
                        atr_ang_live = None

                    # Keep the angle returned by st_atr.preview():
                    # current preview ATR vs previous CLOSED ATR.

                    live_bar_open = live_bar_high = live_bar_low = live_bar_close = live_bar_volume = None
                    live_bar_open_ms = open_snapshot.get(tf)
                    live_bar_close_ms = close_snapshot.get(tf)
                    try:
                        if self.live_use_forming:
                            fh, fl, fc = forming_snapshot.get(tf, (None, None, None))
                            if fh is not None and fl is not None and fc is not None:
                                fh_f = float(fh)
                                fl_f = float(fl)
                                fc_f = float(fc)
                                if np.isfinite(fh_f) and np.isfinite(fl_f) and np.isfinite(fc_f):
                                    try:
                                        live_bar_open = float(closes[-1]) if closes else float(fc_f)
                                    except Exception:
                                        live_bar_open = float(fc_f)
                                    live_bar_high = float(fh_f)
                                    live_bar_low = float(fl_f)
                                    live_bar_close = float(fc_f)
                                    live_bar_volume = 0.0
                                    try:
                                        if live_bar_open_ms is not None:
                                            live_bar_close_ms = int(int(live_bar_open_ms) + int(interval_to_ms(tf)) - 1)
                                    except Exception:
                                        live_bar_close_ms = close_snapshot.get(tf)
                    except Exception:
                        live_bar_open = live_bar_high = live_bar_low = live_bar_close = live_bar_volume = None

                    payload = {
                        "open": live_bar_open,
                        "high": live_bar_high,
                        "low": live_bar_low,
                        "close": live_bar_close,
                        "volume": live_bar_volume,
                        "price": float(live_price),
                        **ema_live,
                        **rsi_live,
                        **stoch_rsi_live,
                        **self._build_taker_bias_payload(tf),
                        "macd": float(macd),
                        "signal": float(sig),
                        "ms": ms,         # GREEN/RED
                        "angle": ang,     # GREEN/RED
                        "sig_angle": sig_ang,  # GREEN/RED (Signal slope)
                        "ppo": ppo,       # GREEN/RED
                        "flip_age": fa,   # int or None
                        "adx": (adx_val_live if adx_val_live is not None else (adx_live[0] if adx_live is not None else None)),
                        "adx_angle": adx_ang_live,
                        "atr": (atr_val_live if atr_val_live is not None else (atr_live[0] if atr_live is not None else None)),
                        "atr_angle": atr_ang_live,
                        "di_plus": adx_live[1],
                        "di_minus": adx_live[2],
                        "di_spread": adx_live[3],
                        "frama": frama_snapshot.get(tf, {}).get("frama"),
                        "frama_upper": frama_snapshot.get(tf, {}).get("frama_upper"),
                        "frama_lower": frama_snapshot.get(tf, {}).get("frama_lower"),
                        "frama_alpha": frama_snapshot.get(tf, {}).get("frama_alpha"),
                        "frama_state": frama_snapshot.get(tf, {}).get("frama_state"),
                        "frama_break_up": bool(frama_snapshot.get(tf, {}).get("frama_break_up", False)),
                        "frama_break_down": bool(frama_snapshot.get(tf, {}).get("frama_break_down", False)),
                        "frama_mid_cross": bool(frama_snapshot.get(tf, {}).get("frama_mid_cross", False)),
                        "vidya_state": vidya_snapshot.get(tf, {}).get("vidya_state"),
                        "vidya_trend_up": bool(vidya_snapshot.get(tf, {}).get("vidya_trend_up", False)),
                        "vidya_trend_down": bool(vidya_snapshot.get(tf, {}).get("vidya_trend_down", False)),
                        "vidya_delta_pct": vidya_snapshot.get(tf, {}).get("vidya_delta_pct"),
                        "vidya_slope": vidya_snapshot.get(tf, {}).get("vidya_slope"),
                        "vidya_angle_deg": vidya_snapshot.get(tf, {}).get("vidya_angle_deg"),
                        "vidya_angle_deg_norm": vidya_snapshot.get(tf, {}).get("vidya_angle_deg_norm"),
                        "vidya_angle_state": vidya_snapshot.get(tf, {}).get("vidya_angle_state"),
                        "vidya_angle_accel": vidya_snapshot.get(tf, {}).get("vidya_angle_accel"),
                        "vidya_up_volume": vidya_snapshot.get(tf, {}).get("vidya_up_volume"),
                        "vidya_down_volume": vidya_snapshot.get(tf, {}).get("vidya_down_volume"),
                        "vidya_active_liq_low_count": vidya_snapshot.get(tf, {}).get("vidya_active_liq_low_count"),
                        "vidya_active_liq_high_count": vidya_snapshot.get(tf, {}).get("vidya_active_liq_high_count"),
                        "vidya_nearest_liq_low": vidya_snapshot.get(tf, {}).get("vidya_nearest_liq_low"),
                        "vidya_nearest_liq_high": vidya_snapshot.get(tf, {}).get("vidya_nearest_liq_high"),
                        "vidya_dist_to_liq_low_pct": vidya_snapshot.get(tf, {}).get("vidya_dist_to_liq_low_pct"),
                        "vidya_dist_to_liq_high_pct": vidya_snapshot.get(tf, {}).get("vidya_dist_to_liq_high_pct"),
                        "vidya_new_liq_low": bool(vidya_snapshot.get(tf, {}).get("vidya_new_liq_low", False)),
                        "vidya_new_liq_high": bool(vidya_snapshot.get(tf, {}).get("vidya_new_liq_high", False)),
                        "vidya_liq_low_taken": bool(vidya_snapshot.get(tf, {}).get("vidya_liq_low_taken", False)),
                        "vidya_liq_high_taken": bool(vidya_snapshot.get(tf, {}).get("vidya_liq_high_taken", False)),
                        "vidya_last_liq_low_taken_price": vidya_snapshot.get(tf, {}).get("vidya_last_liq_low_taken_price"),
                        "vidya_last_liq_high_taken_price": vidya_snapshot.get(tf, {}).get("vidya_last_liq_high_taken_price"),
                        "range_filter_state": range_filter_snapshot.get(tf, {}).get("range_filter_state"),
                        "range_filter_phase": range_filter_snapshot.get(tf, {}).get("range_filter_phase"),
                        "range_filter_buy": bool(range_filter_snapshot.get(tf, {}).get("range_filter_buy", False)),
                        "range_filter_sell": bool(range_filter_snapshot.get(tf, {}).get("range_filter_sell", False)),
                        "range_filter_long_cond": bool(range_filter_snapshot.get(tf, {}).get("range_filter_long_cond", False)),
                        "range_filter_short_cond": bool(range_filter_snapshot.get(tf, {}).get("range_filter_short_cond", False)),
                        "range_filter_cond_ini": range_filter_snapshot.get(tf, {}).get("range_filter_cond_ini"),
                        "range_filter_line": range_filter_snapshot.get(tf, {}).get("range_filter_line"),
                        "range_filter_upper": range_filter_snapshot.get(tf, {}).get("range_filter_upper"),
                        "range_filter_lower": range_filter_snapshot.get(tf, {}).get("range_filter_lower"),
                        "range_filter_smooth_range": range_filter_snapshot.get(tf, {}).get("range_filter_smooth_range"),
                        "range_filter_upward_count": range_filter_snapshot.get(tf, {}).get("range_filter_upward_count"),
                        "range_filter_downward_count": range_filter_snapshot.get(tf, {}).get("range_filter_downward_count"),
                        # Candle timing metadata (ms since epoch)
                        "bar_open_time_ms": live_bar_open_ms,
                        "bar_close_time_ms": live_bar_close_ms,
                    }
                    payload = self._attach_market_state(tf, payload)
                    self.q.put(("live_status", tf, float(live_price), age_sec, payload))

                    # Emit a stable CLOSED-bar snapshot exactly once per new candle close.
                    try:
                        closed_closes = closes[:]  # closed bars only
                        try:
                            closed_rows = list(self._closed_ohlcv_ring.get(tf, []))
                        except Exception:
                            closed_rows = []
                        if closed_rows and len(closed_closes) >= MIN_FULL_CLOSES:
                            latest_closed_row = closed_rows[-1]
                            try:
                                close_ms_i = int(latest_closed_row[0])
                            except Exception:
                                close_ms_i = None
                            if close_ms_i is not None:
                                prev_emitted = self._last_emitted_close_ms.get(tf)
                                if prev_emitted is None or int(prev_emitted) != close_ms_i:
                                    self._last_emitted_close_ms[tf] = close_ms_i
                                    ema_closed = self._build_ema_payload(closed_closes, tf=tf)
                                    rsi_closed = self._build_rsi_payload(closed_closes, tf=tf)
                                    stoch_rsi_closed = self._build_stoch_rsi_payload(closed_closes)
                                    macd_c, sig_c, ms_c, ang_c, sig_ang_c, ppo_c, fa_c = compute_live_indicators(closed_closes, self.macd_impl)
                                    adx_v, di_p, di_m, di_s, adx_ang = adx_snapshot.get(tf, (None, None, None, None, None))
                                    atr_v, atr_ang = atr_snapshot.get(tf, (None, None))

                                    try:
                                        tf_ms_i = int(interval_to_ms(tf))
                                    except Exception:
                                        tf_ms_i = 60_000

                                    try:
                                        _, closed_bar_open, closed_bar_high, closed_bar_low, closed_bar_close, closed_bar_volume, open_ms = latest_closed_row
                                    except Exception:
                                        closed_bar_open = closed_bar_high = closed_bar_low = closed_bar_close = closed_bar_volume = open_ms = None
                                    try:
                                        open_ms_i = int(open_ms) if open_ms is not None else None
                                    except Exception:
                                        open_ms_i = None
                                    if open_ms_i is None:
                                        try:
                                            open_ms_i = int(close_ms_i - tf_ms_i + 1)
                                        except Exception:
                                            open_ms_i = None

                                    payload_closed = {
                                        "open": closed_bar_open,
                                        "high": closed_bar_high,
                                        "low": closed_bar_low,
                                        "close": closed_bar_close,
                                        "volume": closed_bar_volume,
                                        "price": float(live_price),
                                        **ema_closed,
                                        **rsi_closed,
                                        **stoch_rsi_closed,
                                        **self._build_taker_bias_payload(
                                            tf,
                                            volume=closed_bar_volume,
                                            trade_count=(self._latest_closed_taker_metrics.get(tf, {}) or {}).get("trade_count"),
                                            taker_buy_volume=(self._latest_closed_taker_metrics.get(tf, {}) or {}).get("taker_buy_volume"),
                                        ),
                                        "macd": float(macd_c),
                                        "signal": float(sig_c),
                                        "ms": ms_c,
                                        "angle": ang_c,
                                        "sig_angle": sig_ang_c,
                                        "ppo": ppo_c,
                                        "flip_age": fa_c,
                                        "adx": adx_v,
                                        "adx_angle": adx_ang,
                                        "atr": atr_v,
                                        "atr_angle": atr_ang,
                                        "frama": frama_snapshot.get(tf, {}).get("frama"),
                                        "frama_upper": frama_snapshot.get(tf, {}).get("frama_upper"),
                                        "frama_lower": frama_snapshot.get(tf, {}).get("frama_lower"),
                                        "frama_alpha": frama_snapshot.get(tf, {}).get("frama_alpha"),
                                        "frama_state": frama_snapshot.get(tf, {}).get("frama_state"),
                                        "frama_break_up": bool(frama_snapshot.get(tf, {}).get("frama_break_up", False)),
                                        "frama_break_down": bool(frama_snapshot.get(tf, {}).get("frama_break_down", False)),
                                        "frama_mid_cross": bool(frama_snapshot.get(tf, {}).get("frama_mid_cross", False)),
                                        "vidya_state": vidya_snapshot.get(tf, {}).get("vidya_state"),
                                        "vidya_trend_up": bool(vidya_snapshot.get(tf, {}).get("vidya_trend_up", False)),
                                        "vidya_trend_down": bool(vidya_snapshot.get(tf, {}).get("vidya_trend_down", False)),
                                        "vidya_delta_pct": vidya_snapshot.get(tf, {}).get("vidya_delta_pct"),
                                        "vidya_slope": vidya_snapshot.get(tf, {}).get("vidya_slope"),
                                        "vidya_angle_deg": vidya_snapshot.get(tf, {}).get("vidya_angle_deg"),
                                        "vidya_angle_deg_norm": vidya_snapshot.get(tf, {}).get("vidya_angle_deg_norm"),
                                        "vidya_angle_state": vidya_snapshot.get(tf, {}).get("vidya_angle_state"),
                                        "vidya_angle_accel": vidya_snapshot.get(tf, {}).get("vidya_angle_accel"),
                                        "vidya_up_volume": vidya_snapshot.get(tf, {}).get("vidya_up_volume"),
                                        "vidya_down_volume": vidya_snapshot.get(tf, {}).get("vidya_down_volume"),
                                        "vidya_active_liq_low_count": vidya_snapshot.get(tf, {}).get("vidya_active_liq_low_count"),
                                        "vidya_active_liq_high_count": vidya_snapshot.get(tf, {}).get("vidya_active_liq_high_count"),
                                        "vidya_nearest_liq_low": vidya_snapshot.get(tf, {}).get("vidya_nearest_liq_low"),
                                        "vidya_nearest_liq_high": vidya_snapshot.get(tf, {}).get("vidya_nearest_liq_high"),
                                        "vidya_dist_to_liq_low_pct": vidya_snapshot.get(tf, {}).get("vidya_dist_to_liq_low_pct"),
                                        "vidya_dist_to_liq_high_pct": vidya_snapshot.get(tf, {}).get("vidya_dist_to_liq_high_pct"),
                                        "vidya_new_liq_low": bool(vidya_snapshot.get(tf, {}).get("vidya_new_liq_low", False)),
                                        "vidya_new_liq_high": bool(vidya_snapshot.get(tf, {}).get("vidya_new_liq_high", False)),
                                        "vidya_liq_low_taken": bool(vidya_snapshot.get(tf, {}).get("vidya_liq_low_taken", False)),
                                        "vidya_liq_high_taken": bool(vidya_snapshot.get(tf, {}).get("vidya_liq_high_taken", False)),
                                        "vidya_last_liq_low_taken_price": vidya_snapshot.get(tf, {}).get("vidya_last_liq_low_taken_price"),
                                        "vidya_last_liq_high_taken_price": vidya_snapshot.get(tf, {}).get("vidya_last_liq_high_taken_price"),
                                        "range_filter_state": range_filter_snapshot.get(tf, {}).get("range_filter_state"),
                                        "range_filter_phase": range_filter_snapshot.get(tf, {}).get("range_filter_phase"),
                                        "range_filter_buy": bool(range_filter_snapshot.get(tf, {}).get("range_filter_buy", False)),
                                        "range_filter_sell": bool(range_filter_snapshot.get(tf, {}).get("range_filter_sell", False)),
                                        "range_filter_long_cond": bool(range_filter_snapshot.get(tf, {}).get("range_filter_long_cond", False)),
                                        "range_filter_short_cond": bool(range_filter_snapshot.get(tf, {}).get("range_filter_short_cond", False)),
                                        "range_filter_cond_ini": range_filter_snapshot.get(tf, {}).get("range_filter_cond_ini"),
                                        "range_filter_line": range_filter_snapshot.get(tf, {}).get("range_filter_line"),
                                        "range_filter_upper": range_filter_snapshot.get(tf, {}).get("range_filter_upper"),
                                        "range_filter_lower": range_filter_snapshot.get(tf, {}).get("range_filter_lower"),
                                        "range_filter_smooth_range": range_filter_snapshot.get(tf, {}).get("range_filter_smooth_range"),
                                        "range_filter_upward_count": range_filter_snapshot.get(tf, {}).get("range_filter_upward_count"),
                                        "range_filter_downward_count": range_filter_snapshot.get(tf, {}).get("range_filter_downward_count"),
                                        "di_plus": di_p,
                                        "di_minus": di_m,
                                        "di_spread": di_s,
                                        "bar_open_time_ms": open_ms_i,
                                        "bar_close_time_ms": close_ms_i,
                                        "is_closed": True,
                                    }
                                    payload_closed = self._attach_market_state(tf, payload_closed)
                                    self.q.put(("live_closed", tf, float(live_price), age_sec, payload_closed))
                    except Exception:
                        pass
                except Exception:
                    self.q.put(("live_status", tf, float(live_price), age_sec, None))
