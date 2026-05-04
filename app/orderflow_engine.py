from __future__ import annotations

"""Live tape + DOM/L2 order-flow helpers for breakout filtering.

This module intentionally stays separate from indicator math.

What it now supports:
- aggTrade-based tape pressure
- legacy partial-depth snapshots (safe fallback)
- true local L2 book maintenance from REST snapshot + diff-depth events

The local-book workflow follows Binance USDⓈ-M futures guidance:
- buffer diff-depth events
- fetch REST snapshot (GET /fapi/v1/depth)
- drop events with u < lastUpdateId
- first processed event must satisfy U <= lastUpdateId <= u
- each subsequent event should have pu == previous u, otherwise resync
"""

from dataclasses import dataclass, field
from collections import deque
from typing import Any, Deque, Dict, Iterable, List, Optional, Sequence, Tuple

import numpy as np

from .utils_time import interval_to_ms


TradeTuple = Tuple[int, float, float, int]
BookPoint = Tuple[int, float, float, float, float, float, float, float, float, float, float, float, float]
# (ts_ms, best_bid, best_ask, mid, spread_bps,
#  pressure_pct, imbalance_top5_pct, imbalance_top10_pct,
#  bid_qty_5, ask_qty_5, bid_qty_10, ask_qty_10, weighted_near_qty_total)


@dataclass
class DiffDepthEvent:
    ts_ms: int
    first_update_id: int
    final_update_id: int
    prev_final_update_id: Optional[int]
    bids: List[Tuple[float, float]]
    asks: List[Tuple[float, float]]


def _clip01(v: float) -> float:
    try:
        fv = float(v)
    except Exception:
        return 0.0
    if not np.isfinite(fv):
        return 0.0
    return float(max(0.0, min(1.0, fv)))


def _safe_float(v: Any, default: float = 0.0) -> float:
    try:
        fv = float(v)
        if np.isfinite(fv):
            return float(fv)
    except Exception:
        pass
    return float(default)


def _safe_int(v: Any, default: int = 0) -> int:
    try:
        return int(v)
    except Exception:
        return int(default)


def _weighted_qty(levels: Sequence[Tuple[float, float]], n: int) -> float:
    total = 0.0
    for idx, (_p, q) in enumerate(list(levels)[: max(0, int(n))]):
        w = 1.0 / float(idx + 1)
        total += w * max(0.0, _safe_float(q, 0.0))
    return float(total)


def _sum_qty(levels: Sequence[Tuple[float, float]], n: int) -> float:
    total = 0.0
    for _p, q in list(levels)[: max(0, int(n))]:
        total += max(0.0, _safe_float(q, 0.0))
    return float(total)


def _imbalance_pct(bid_qty: float, ask_qty: float) -> float:
    b = max(0.0, _safe_float(bid_qty, 0.0))
    a = max(0.0, _safe_float(ask_qty, 0.0))
    den = b + a
    if den <= 0.0:
        return 0.0
    return float(200.0 * (b - a) / den)


def _parse_price_qty_rows(rows: Sequence[Sequence[Any]]) -> List[Tuple[float, float]]:
    out: List[Tuple[float, float]] = []
    for row in rows or []:
        try:
            if row is None or len(row) < 2:
                continue
            p = float(row[0])
            q = float(row[1])
        except Exception:
            continue
        if np.isfinite(p) and np.isfinite(q) and p > 0.0 and q >= 0.0:
            out.append((float(p), float(q)))
    return out


@dataclass
class LocalOrderBook:
    symbol: str
    snapshot_limit: int = 1000
    keep_levels: int = 50
    bids: Dict[float, float] = field(default_factory=dict)
    asks: Dict[float, float] = field(default_factory=dict)
    buffered_events: Deque[DiffDepthEvent] = field(default_factory=lambda: deque(maxlen=20000))
    synced: bool = False
    sync_state: str = "INIT"
    resync_required: bool = True
    last_snapshot_update_id: Optional[int] = None
    last_event_u: Optional[int] = None
    last_event_pu: Optional[int] = None
    last_snapshot_ts_ms: Optional[int] = None
    last_depth_event_ts_ms: Optional[int] = None
    sync_note: str = "awaiting snapshot"
    total_events_buffered: int = 0
    total_events_applied: int = 0
    total_resyncs: int = 0

    def reset(self, note: str = "reset") -> None:
        self.bids.clear()
        self.asks.clear()
        self.synced = False
        self.sync_state = "WAITING_SNAPSHOT"
        self.resync_required = True
        self.last_snapshot_update_id = None
        self.last_event_u = None
        self.last_event_pu = None
        self.last_snapshot_ts_ms = None
        self.sync_note = str(note)
        self.total_resyncs += 1

    def _trim(self) -> None:
        keep_n = max(20, int(self.keep_levels))
        if len(self.bids) > keep_n * 3:
            top_bids = sorted(self.bids.items(), key=lambda x: x[0], reverse=True)[:keep_n]
            self.bids = {float(p): float(q) for p, q in top_bids if q > 0.0}
        if len(self.asks) > keep_n * 3:
            top_asks = sorted(self.asks.items(), key=lambda x: x[0])[:keep_n]
            self.asks = {float(p): float(q) for p, q in top_asks if q > 0.0}

    def _apply_side_updates(self, side: Dict[float, float], updates: Sequence[Tuple[float, float]]) -> None:
        for price, qty in updates:
            p = float(price)
            q = float(qty)
            if not np.isfinite(p) or p <= 0.0 or not np.isfinite(q):
                continue
            if q <= 0.0:
                side.pop(p, None)
            else:
                side[p] = q

    def _apply_event(self, event: DiffDepthEvent) -> None:
        self._apply_side_updates(self.bids, event.bids)
        self._apply_side_updates(self.asks, event.asks)
        self._trim()
        self.last_event_u = int(event.final_update_id)
        self.last_event_pu = int(event.prev_final_update_id) if event.prev_final_update_id is not None else None
        self.last_snapshot_update_id = int(event.final_update_id)
        self.last_depth_event_ts_ms = int(event.ts_ms)
        self.total_events_applied += 1

    def add_diff_event(
        self,
        ts_ms: int,
        first_update_id: int,
        final_update_id: int,
        prev_final_update_id: Optional[int],
        bids: Sequence[Sequence[Any]],
        asks: Sequence[Sequence[Any]],
    ) -> str:
        event = DiffDepthEvent(
            ts_ms=int(ts_ms),
            first_update_id=int(first_update_id),
            final_update_id=int(final_update_id),
            prev_final_update_id=(int(prev_final_update_id) if prev_final_update_id is not None else None),
            bids=_parse_price_qty_rows(bids),
            asks=_parse_price_qty_rows(asks),
        )
        self.last_depth_event_ts_ms = int(event.ts_ms)
        self.total_events_buffered += 1

        if not self.synced:
            self.buffered_events.append(event)
            self.sync_state = "BUFFERING"
            self.sync_note = "buffering diff-depth events until snapshot sync completes"
            self.resync_required = True if self.last_snapshot_update_id is None else self.resync_required
            return "BUFFERED"

        if event.final_update_id < int(self.last_snapshot_update_id or 0):
            return "STALE"

        if self.last_event_u is not None and event.prev_final_update_id is not None and int(event.prev_final_update_id) != int(self.last_event_u):
            self.synced = False
            self.sync_state = "RESYNC_REQUIRED"
            self.resync_required = True
            self.sync_note = f"depth diff gap detected (pu={event.prev_final_update_id}, expected={self.last_event_u})"
            self.buffered_events.clear()
            self.buffered_events.append(event)
            return "RESYNC_REQUIRED"

        self._apply_event(event)
        self.resync_required = False
        self.synced = True
        self.sync_state = "SYNCED"
        self.sync_note = "local L2 book synced from diff-depth stream"
        return "APPLIED"

    def initialize_from_snapshot(
        self,
        last_update_id: int,
        bids: Sequence[Sequence[Any]],
        asks: Sequence[Sequence[Any]],
        snapshot_ts_ms: Optional[int] = None,
    ) -> bool:
        try:
            snap_u = int(last_update_id)
        except Exception:
            self.synced = False
            self.sync_state = "SNAPSHOT_ERROR"
            self.resync_required = True
            self.sync_note = "snapshot lastUpdateId invalid"
            return False

        bid_rows = _parse_price_qty_rows(bids)
        ask_rows = _parse_price_qty_rows(asks)
        if not bid_rows or not ask_rows:
            self.synced = False
            self.sync_state = "SNAPSHOT_ERROR"
            self.resync_required = True
            self.sync_note = "snapshot returned empty bids/asks"
            return False

        self.bids = {float(p): float(q) for p, q in bid_rows if q > 0.0}
        self.asks = {float(p): float(q) for p, q in ask_rows if q > 0.0}
        self._trim()
        self.last_snapshot_update_id = int(snap_u)
        self.last_snapshot_ts_ms = int(snapshot_ts_ms) if snapshot_ts_ms is not None else None
        self.last_event_u = None
        self.last_event_pu = None
        self.synced = False
        self.sync_state = "SNAPSHOT_LOADED"
        self.resync_required = True
        self.sync_note = "snapshot loaded; aligning buffered diff-depth events"

        buffered: List[DiffDepthEvent] = list(self.buffered_events)
        if buffered:
            buffered = [ev for ev in buffered if int(ev.final_update_id) >= int(snap_u)]
        if not buffered:
            self.sync_state = "WAITING_POST_SNAPSHOT_EVENT"
            self.resync_required = False
            self.sync_note = "snapshot loaded; waiting for first diff-depth event"
            return False

        start_idx: Optional[int] = None
        for idx, ev in enumerate(buffered):
            if int(ev.first_update_id) <= int(snap_u) <= int(ev.final_update_id):
                start_idx = idx
                break
        if start_idx is None:
            self.synced = False
            self.sync_state = "SNAPSHOT_MISALIGNED"
            self.resync_required = True
            self.sync_note = "could not align buffered diff-depth events to snapshot; requesting resync"
            self.buffered_events = deque(buffered[-self.buffered_events.maxlen :], maxlen=self.buffered_events.maxlen)
            return False

        ok = True
        prev_u: Optional[int] = None
        for idx, ev in enumerate(buffered[start_idx:]):
            if idx > 0 and prev_u is not None and ev.prev_final_update_id is not None and int(ev.prev_final_update_id) != int(prev_u):
                ok = False
                self.sync_note = f"buffered diff-depth chain gap detected during sync (pu={ev.prev_final_update_id}, expected={prev_u})"
                break
            self._apply_event(ev)
            prev_u = int(ev.final_update_id)

        self.buffered_events.clear()
        if not ok:
            self.synced = False
            self.sync_state = "RESYNC_REQUIRED"
            self.resync_required = True
            return False

        self.synced = True
        self.sync_state = "SYNCED"
        self.resync_required = False
        self.sync_note = "snapshot + diff-depth alignment completed"
        return True

    def export_top_levels(self, n: int = 20) -> Tuple[List[List[float]], List[List[float]]]:
        if not self.bids or not self.asks:
            return [], []
        bids = sorted(self.bids.items(), key=lambda x: x[0], reverse=True)[: max(1, int(n))]
        asks = sorted(self.asks.items(), key=lambda x: x[0])[: max(1, int(n))]
        return [[float(p), float(q)] for p, q in bids], [[float(p), float(q)] for p, q in asks]

    def status_dict(self, now_ms: Optional[int] = None) -> Dict[str, Any]:
        current_ms = int(now_ms) if now_ms is not None else None
        depth_age_ms = None
        snapshot_age_ms = None
        if current_ms is not None and self.last_depth_event_ts_ms is not None:
            depth_age_ms = int(max(0, current_ms - int(self.last_depth_event_ts_ms)))
        if current_ms is not None and self.last_snapshot_ts_ms is not None:
            snapshot_age_ms = int(max(0, current_ms - int(self.last_snapshot_ts_ms)))
        return {
            "source": "LOCAL_L2",
            "synced": bool(self.synced),
            "sync_state": str(self.sync_state),
            "resync_required": bool(self.resync_required),
            "sync_note": str(self.sync_note),
            "last_snapshot_update_id": (int(self.last_snapshot_update_id) if self.last_snapshot_update_id is not None else None),
            "last_event_u": (int(self.last_event_u) if self.last_event_u is not None else None),
            "last_event_pu": (int(self.last_event_pu) if self.last_event_pu is not None else None),
            "buffered_events": int(len(self.buffered_events)),
            "events_applied": int(self.total_events_applied),
            "resync_count": int(self.total_resyncs),
            "depth_age_ms": depth_age_ms,
            "snapshot_age_ms": snapshot_age_ms,
            "book_levels_bid": int(len(self.bids)),
            "book_levels_ask": int(len(self.asks)),
        }


@dataclass
class OrderFlowWindow:
    tf: str
    tf_sec: int = field(init=False)
    short_sec: int = field(init=False)
    long_sec: int = field(init=False)
    history_sec: int = field(init=False)
    trades: Deque[TradeTuple] = field(default_factory=deque)
    book_points: Deque[BookPoint] = field(default_factory=deque)

    def __post_init__(self) -> None:
        try:
            self.tf_sec = max(60, int(interval_to_ms(self.tf) // 1000))
        except Exception:
            self.tf_sec = 60
        self.short_sec = int(max(20, min(180, round(self.tf_sec / 10.0))))
        self.long_sec = int(max(90, min(1200, round(self.tf_sec / 3.0))))
        if self.long_sec <= self.short_sec:
            self.long_sec = int(max(self.short_sec + 30, self.short_sec * 3))
        self.history_sec = int(max(self.long_sec * 3, self.short_sec * 8, 240))

    def add_trade(self, ts_ms: int, price: float, qty: float, is_buyer_maker: bool) -> None:
        try:
            t = int(ts_ms)
            p = float(price)
            q = float(qty)
        except Exception:
            return
        if not np.isfinite(p) or not np.isfinite(q) or q <= 0.0:
            return
        side = -1 if bool(is_buyer_maker) else 1
        self.trades.append((t, p, q, side))
        self._prune(t)

    def add_depth_snapshot(self, ts_ms: int, bids: Sequence[Sequence[Any]], asks: Sequence[Sequence[Any]]) -> None:
        try:
            t = int(ts_ms)
        except Exception:
            return
        bid_levels = _parse_price_qty_rows(bids)
        ask_levels = _parse_price_qty_rows(asks)
        if not bid_levels or not ask_levels:
            return

        bid_levels.sort(key=lambda x: x[0], reverse=True)
        ask_levels.sort(key=lambda x: x[0])
        best_bid = float(bid_levels[0][0])
        best_ask = float(ask_levels[0][0])
        if not np.isfinite(best_bid) or not np.isfinite(best_ask) or best_bid <= 0.0 or best_ask <= 0.0 or best_ask < best_bid:
            return
        mid = float((best_bid + best_ask) / 2.0)
        spread_bps = float(10000.0 * (best_ask - best_bid) / max(mid, 1e-12))

        bid_qty_5 = _sum_qty(bid_levels, 5)
        ask_qty_5 = _sum_qty(ask_levels, 5)
        bid_qty_10 = _sum_qty(bid_levels, 10)
        ask_qty_10 = _sum_qty(ask_levels, 10)
        weighted_bid = _weighted_qty(bid_levels, 10)
        weighted_ask = _weighted_qty(ask_levels, 10)
        pressure_pct = _imbalance_pct(weighted_bid, weighted_ask)
        imbalance_top5_pct = _imbalance_pct(bid_qty_5, ask_qty_5)
        imbalance_top10_pct = _imbalance_pct(bid_qty_10, ask_qty_10)
        weighted_total = float(weighted_bid + weighted_ask)

        self.book_points.append(
            (
                int(t),
                float(best_bid),
                float(best_ask),
                float(mid),
                float(spread_bps),
                float(pressure_pct),
                float(imbalance_top5_pct),
                float(imbalance_top10_pct),
                float(bid_qty_5),
                float(ask_qty_5),
                float(bid_qty_10),
                float(ask_qty_10),
                float(weighted_total),
            )
        )
        self._prune(t)

    def _prune(self, latest_ts_ms: Optional[int] = None) -> None:
        latest = None
        try:
            if latest_ts_ms is not None:
                latest = int(latest_ts_ms)
            elif self.trades:
                latest = int(self.trades[-1][0])
            elif self.book_points:
                latest = int(self.book_points[-1][0])
        except Exception:
            latest = None
        if latest is None:
            return
        cutoff = latest - int(self.history_sec * 1000)
        while self.trades and int(self.trades[0][0]) < cutoff:
            self.trades.popleft()
        while self.book_points and int(self.book_points[0][0]) < cutoff:
            self.book_points.popleft()

    def _aggregate_trades(self, window_sec: int, ref_now_ms: Optional[int] = None) -> Dict[str, float]:
        if not self.trades:
            return {
                "buy_qty": 0.0,
                "sell_qty": 0.0,
                "buy_notional": 0.0,
                "sell_notional": 0.0,
                "count": 0.0,
                "first_price": float("nan"),
                "last_price": float("nan"),
            }
        try:
            now_ms = int(ref_now_ms if ref_now_ms is not None else self.trades[-1][0])
        except Exception:
            now_ms = int(self.trades[-1][0])
        cutoff = now_ms - int(max(1, window_sec) * 1000)
        buy_qty = 0.0
        sell_qty = 0.0
        buy_notional = 0.0
        sell_notional = 0.0
        count = 0
        first_price = float("nan")
        last_price = float("nan")

        for ts_ms, price, qty, side in reversed(self.trades):
            if int(ts_ms) < cutoff:
                break
            if count == 0:
                last_price = float(price)
            first_price = float(price)
            count += 1
            if side > 0:
                buy_qty += float(qty)
                buy_notional += float(price) * float(qty)
            else:
                sell_qty += float(qty)
                sell_notional += float(price) * float(qty)
        return {
            "buy_qty": float(buy_qty),
            "sell_qty": float(sell_qty),
            "buy_notional": float(buy_notional),
            "sell_notional": float(sell_notional),
            "count": float(count),
            "first_price": float(first_price),
            "last_price": float(last_price),
        }

    def _aggregate_book(self, window_sec: int, ref_now_ms: Optional[int] = None) -> Dict[str, float]:
        if not self.book_points:
            return {
                "count": 0.0,
                "pressure_pct_mean": 0.0,
                "top5_pct_mean": 0.0,
                "top10_pct_mean": 0.0,
                "spread_bps_mean": 0.0,
                "bid_qty_5_mean": 0.0,
                "ask_qty_5_mean": 0.0,
                "bid_qty_10_mean": 0.0,
                "ask_qty_10_mean": 0.0,
                "weighted_total_mean": 0.0,
            }
        try:
            now_ms = int(ref_now_ms if ref_now_ms is not None else self.book_points[-1][0])
        except Exception:
            now_ms = int(self.book_points[-1][0])
        cutoff = now_ms - int(max(1, window_sec) * 1000)
        count = 0
        pressure_vals = []
        top5_vals = []
        top10_vals = []
        spread_vals = []
        bid5_vals = []
        ask5_vals = []
        bid10_vals = []
        ask10_vals = []
        total_vals = []
        for row in reversed(self.book_points):
            if int(row[0]) < cutoff:
                break
            count += 1
            pressure_vals.append(float(row[5]))
            top5_vals.append(float(row[6]))
            top10_vals.append(float(row[7]))
            bid5_vals.append(float(row[8]))
            ask5_vals.append(float(row[9]))
            bid10_vals.append(float(row[10]))
            ask10_vals.append(float(row[11]))
            spread_vals.append(float(row[4]))
            total_vals.append(float(row[12]))
        if count <= 0:
            return {
                "count": 0.0,
                "pressure_pct_mean": 0.0,
                "top5_pct_mean": 0.0,
                "top10_pct_mean": 0.0,
                "spread_bps_mean": 0.0,
                "bid_qty_5_mean": 0.0,
                "ask_qty_5_mean": 0.0,
                "bid_qty_10_mean": 0.0,
                "ask_qty_10_mean": 0.0,
                "weighted_total_mean": 0.0,
            }
        return {
            "count": float(count),
            "pressure_pct_mean": float(np.mean(pressure_vals) if pressure_vals else 0.0),
            "top5_pct_mean": float(np.mean(top5_vals) if top5_vals else 0.0),
            "top10_pct_mean": float(np.mean(top10_vals) if top10_vals else 0.0),
            "spread_bps_mean": float(np.mean(spread_vals) if spread_vals else 0.0),
            "bid_qty_5_mean": float(np.mean(bid5_vals) if bid5_vals else 0.0),
            "ask_qty_5_mean": float(np.mean(ask5_vals) if ask5_vals else 0.0),
            "bid_qty_10_mean": float(np.mean(bid10_vals) if bid10_vals else 0.0),
            "ask_qty_10_mean": float(np.mean(ask10_vals) if ask10_vals else 0.0),
            "weighted_total_mean": float(np.mean(total_vals) if total_vals else 0.0),
        }

    def snapshot(self, ref_price: Optional[float] = None) -> Dict[str, Any]:
        last_trade_ts_ms = int(self.trades[-1][0]) if self.trades else None
        last_book_ts_ms = int(self.book_points[-1][0]) if self.book_points else None
        latest_ts_ms = None
        if last_trade_ts_ms is not None:
            latest_ts_ms = int(last_trade_ts_ms)
        if last_book_ts_ms is not None:
            try:
                latest_ts_ms = max(int(latest_ts_ms) if latest_ts_ms is not None else int(last_book_ts_ms), int(last_book_ts_ms))
            except Exception:
                latest_ts_ms = latest_ts_ms if latest_ts_ms is not None else int(last_book_ts_ms)
        if latest_ts_ms is None:
            return {
                "of_state": "NO_DATA",
                "of_state_pretty": "No Tape/DOM Data",
                "of_bias": "NEUTRAL",
                "of_note": "waiting for aggTrade and depth flow",
                "of_delta_pct": 0.0,
                "of_volume_burst": 0.0,
                "of_trade_burst": 0.0,
                "of_progress_bps": 0.0,
                "of_absorption_long": False,
                "of_absorption_short": False,
                "of_breakout_watch_long": False,
                "of_breakout_watch_short": False,
                "of_breakout_confirm_long": False,
                "of_breakout_confirm_short": False,
                "of_filter_score": 0,
                "of_filter_pass": False,
                "of_window_short_sec": int(self.short_sec),
                "of_window_long_sec": int(self.long_sec),
                "of_dom_connected": False,
                "of_dom_state": "NO_DATA",
                "of_dom_state_pretty": "No DOM Data",
                "of_dom_bias": "NEUTRAL",
                "of_dom_note": "DOM not connected yet",
                "of_last_trade_ts_ms": None,
                "of_last_book_ts_ms": None,
                "of_last_event_ts_ms": None,
            }

        self._prune(int(latest_ts_ms))
        short = self._aggregate_trades(self.short_sec, ref_now_ms=latest_ts_ms)
        long = self._aggregate_trades(self.long_sec, ref_now_ms=latest_ts_ms)
        book_short = self._aggregate_book(self.short_sec, ref_now_ms=latest_ts_ms)
        book_long = self._aggregate_book(self.long_sec, ref_now_ms=latest_ts_ms)

        short_total = float(short["buy_qty"] + short["sell_qty"])
        long_total = float(long["buy_qty"] + long["sell_qty"])
        if short_total > 0.0:
            delta_pct = 200.0 * (float(short["buy_qty"]) - float(short["sell_qty"])) / short_total
        else:
            delta_pct = 0.0

        short_vps = short_total / float(max(1, self.short_sec))
        long_vps = long_total / float(max(1, self.long_sec))
        vol_burst_ratio = short_vps / max(long_vps, 1e-12) if long_vps > 0.0 else 1.0
        volume_burst = _clip01((vol_burst_ratio - 0.90) / 0.80)

        short_tps = float(short["count"]) / float(max(1, self.short_sec))
        long_tps = float(long["count"]) / float(max(1, self.long_sec))
        trade_burst_ratio = short_tps / max(long_tps, 1e-12) if long_tps > 0.0 else 1.0
        trade_burst = _clip01((trade_burst_ratio - 0.90) / 0.90)

        try:
            ref_px = float(ref_price) if ref_price is not None and np.isfinite(float(ref_price)) else float(short["last_price"])
        except Exception:
            ref_px = float(short["last_price"])
        try:
            first_px = float(short["first_price"])
            last_px = float(short["last_price"])
            progress_bps = 10000.0 * (last_px - first_px) / max(abs(ref_px), 1e-12)
        except Exception:
            progress_bps = 0.0

        delta_strength = _clip01(abs(delta_pct) / 30.0)
        progress_strength = _clip01(abs(progress_bps) / 6.0)
        flow_sync_long = bool(delta_pct >= 8.0 and progress_bps >= 0.35)
        flow_sync_short = bool(delta_pct <= -8.0 and progress_bps <= -0.35)
        absorption_long = bool(delta_pct >= 12.0 and progress_bps < 0.60)
        absorption_short = bool(delta_pct <= -12.0 and progress_bps > -0.60)

        tape_watch_long = bool(delta_pct >= 10.0 and vol_burst_ratio >= 1.02 and trade_burst_ratio >= 0.95 and not absorption_long and progress_bps >= 0.35)
        tape_watch_short = bool(delta_pct <= -10.0 and vol_burst_ratio >= 1.02 and trade_burst_ratio >= 0.95 and not absorption_short and progress_bps <= -0.35)
        tape_confirm_long = bool(delta_pct >= 18.0 and vol_burst_ratio >= 1.08 and trade_burst_ratio >= 1.00 and progress_bps >= 1.25 and not absorption_long)
        tape_confirm_short = bool(delta_pct <= -18.0 and vol_burst_ratio >= 1.08 and trade_burst_ratio >= 1.00 and progress_bps <= -1.25 and not absorption_short)

        dom_connected = bool(self.book_points)
        dom_state = "NO_DATA"
        dom_state_pretty = "No DOM Data"
        dom_bias = "NEUTRAL"
        dom_note = "DOM not connected yet"
        dom_watch_long = False
        dom_watch_short = False
        dom_confirm_long = False
        dom_confirm_short = False
        dom_filter_score = 0
        dom_pressure_pct = 0.0
        dom_pressure_accel = 0.0
        dom_top5_imbalance_pct = 0.0
        dom_top10_imbalance_pct = 0.0
        dom_spread_bps = 0.0
        dom_bid_stack_ratio = 1.0
        dom_ask_stack_ratio = 1.0
        dom_bid_pull_ratio = 1.0
        dom_ask_pull_ratio = 1.0
        dom_short_updates = int(book_short.get("count") or 0)
        dom_long_updates = int(book_long.get("count") or 0)
        dom_best_bid = None
        dom_best_ask = None
        dom_mid = None

        if dom_connected:
            current = self.book_points[-1]
            dom_best_bid = float(current[1])
            dom_best_ask = float(current[2])
            dom_mid = float(current[3])
            dom_spread_bps = float(current[4])
            dom_pressure_pct = float(current[5])
            dom_top5_imbalance_pct = float(current[6])
            dom_top10_imbalance_pct = float(current[7])
            bid_qty_5_now = float(current[8])
            ask_qty_5_now = float(current[9])
            bid_qty_10_now = float(current[10])
            ask_qty_10_now = float(current[11])

            bid_qty_5_mean = max(float(book_long.get("bid_qty_5_mean") or 0.0), 1e-12)
            ask_qty_5_mean = max(float(book_long.get("ask_qty_5_mean") or 0.0), 1e-12)
            bid_qty_10_mean = max(float(book_long.get("bid_qty_10_mean") or 0.0), 1e-12)
            ask_qty_10_mean = max(float(book_long.get("ask_qty_10_mean") or 0.0), 1e-12)
            dom_bid_stack_ratio = float(bid_qty_5_now / bid_qty_5_mean)
            dom_ask_stack_ratio = float(ask_qty_5_now / ask_qty_5_mean)
            dom_bid_pull_ratio = float(bid_qty_10_now / bid_qty_10_mean)
            dom_ask_pull_ratio = float(ask_qty_10_now / ask_qty_10_mean)
            dom_pressure_accel = float(book_short.get("pressure_pct_mean") or 0.0) - float(book_long.get("pressure_pct_mean") or 0.0)

            bid_stack_strength = _clip01((dom_bid_stack_ratio - 0.96) / 0.40)
            ask_stack_strength = _clip01((dom_ask_stack_ratio - 0.96) / 0.40)
            bid_pull_strength = _clip01((1.06 - dom_bid_pull_ratio) / 0.25)
            ask_pull_strength = _clip01((1.06 - dom_ask_pull_ratio) / 0.25)
            pressure_strength = _clip01(abs(dom_pressure_pct) / 26.0)
            accel_strength = _clip01(abs(dom_pressure_accel) / 10.0)
            spread_quality = _clip01((3.00 - dom_spread_bps) / 2.50)

            dom_watch_long = bool(
                dom_pressure_pct >= 10.0
                and dom_pressure_accel >= 1.5
                and dom_spread_bps <= 3.2
                and (dom_bid_stack_ratio >= 1.02 or dom_ask_pull_ratio <= 0.98)
            )
            dom_watch_short = bool(
                dom_pressure_pct <= -10.0
                and dom_pressure_accel <= -1.5
                and dom_spread_bps <= 3.2
                and (dom_ask_stack_ratio >= 1.02 or dom_bid_pull_ratio <= 0.98)
            )
            dom_confirm_long = bool(
                dom_pressure_pct >= 18.0
                and dom_pressure_accel >= 4.0
                and dom_spread_bps <= 2.2
                and (dom_bid_stack_ratio >= 1.06 or dom_ask_pull_ratio <= 0.95)
            )
            dom_confirm_short = bool(
                dom_pressure_pct <= -18.0
                and dom_pressure_accel <= -4.0
                and dom_spread_bps <= 2.2
                and (dom_ask_stack_ratio >= 1.06 or dom_bid_pull_ratio <= 0.95)
            )

            if dom_confirm_long:
                dom_state = "DOM_CONFIRM_LONG"
                dom_state_pretty = "DOM Confirm Long"
                dom_bias = "LONG"
                dom_note = "bid-side book pressure is dominant and the ask side is lightening"
            elif dom_confirm_short:
                dom_state = "DOM_CONFIRM_SHORT"
                dom_state_pretty = "DOM Confirm Short"
                dom_bias = "SHORT"
                dom_note = "ask-side book pressure is dominant and the bid side is lightening"
            elif dom_watch_long:
                dom_state = "DOM_WATCH_LONG"
                dom_state_pretty = "DOM Watch Long"
                dom_bias = "LONG"
                dom_note = "book pressure is leaning long, but the release is not fully committed yet"
            elif dom_watch_short:
                dom_state = "DOM_WATCH_SHORT"
                dom_state_pretty = "DOM Watch Short"
                dom_bias = "SHORT"
                dom_note = "book pressure is leaning short, but the release is not fully committed yet"
            elif dom_pressure_pct >= 6.0:
                dom_state = "DOM_FLOW_LONG"
                dom_state_pretty = "DOM Flow Long"
                dom_bias = "LONG"
                dom_note = "top-of-book pressure is leaning long, but not enough for breakout confirmation"
            elif dom_pressure_pct <= -6.0:
                dom_state = "DOM_FLOW_SHORT"
                dom_state_pretty = "DOM Flow Short"
                dom_bias = "SHORT"
                dom_note = "top-of-book pressure is leaning short, but not enough for breakout confirmation"
            else:
                dom_state = "DOM_NEUTRAL"
                dom_state_pretty = "DOM Neutral"
                dom_bias = "NEUTRAL"
                dom_note = "the order book is balanced or too mixed to support a breakout"

            dom_filter_score = int(
                np.clip(
                    round(
                        100.0
                        * (
                            0.42 * pressure_strength
                            + 0.18 * accel_strength
                            + 0.16 * bid_stack_strength
                            + 0.16 * ask_stack_strength
                            + 0.08 * spread_quality
                        )
                    ),
                    0,
                    100,
                )
            )

        combined_watch_long = bool((tape_watch_long and (dom_watch_long or dom_confirm_long)) or (dom_watch_long and delta_pct >= 6.0))
        combined_watch_short = bool((tape_watch_short and (dom_watch_short or dom_confirm_short)) or (dom_watch_short and delta_pct <= -6.0))
        combined_confirm_long = bool((tape_confirm_long and (dom_watch_long or dom_confirm_long)) or (dom_confirm_long and tape_watch_long))
        combined_confirm_short = bool((tape_confirm_short and (dom_watch_short or dom_confirm_short)) or (dom_confirm_short and tape_watch_short))

        if combined_confirm_long:
            state = "BREAKOUT_CONFIRM_LONG"
            bias = "LONG"
            note = "tape and DOM both support a long-side breakout release"
        elif combined_confirm_short:
            state = "BREAKOUT_CONFIRM_SHORT"
            bias = "SHORT"
            note = "tape and DOM both support a short-side breakout release"
        elif combined_watch_long:
            state = "BREAKOUT_WATCH_LONG"
            bias = "LONG"
            note = "compression release pressure is building long with tape/book agreement"
        elif combined_watch_short:
            state = "BREAKOUT_WATCH_SHORT"
            bias = "SHORT"
            note = "compression release pressure is building short with tape/book agreement"
        elif absorption_long:
            state = "ABSORPTION_LONG"
            bias = "LONG"
            note = "buy aggression is present, but price is not progressing enough"
        elif absorption_short:
            state = "ABSORPTION_SHORT"
            bias = "SHORT"
            note = "sell aggression is present, but price is not progressing enough"
        elif flow_sync_long or dom_bias == "LONG":
            state = "FLOW_LONG"
            bias = "LONG"
            note = "microstructure is leaning long, but the move is not confirmed yet"
        elif flow_sync_short or dom_bias == "SHORT":
            state = "FLOW_SHORT"
            bias = "SHORT"
            note = "microstructure is leaning short, but the move is not confirmed yet"
        else:
            state = "NEUTRAL"
            bias = "NEUTRAL"
            note = "microstructure is balanced or noisy"

        pretty_map = {
            "BREAKOUT_CONFIRM_LONG": "OF Confirm Long",
            "BREAKOUT_CONFIRM_SHORT": "OF Confirm Short",
            "BREAKOUT_WATCH_LONG": "OF Watch Long",
            "BREAKOUT_WATCH_SHORT": "OF Watch Short",
            "ABSORPTION_LONG": "OF Absorption Long",
            "ABSORPTION_SHORT": "OF Absorption Short",
            "FLOW_LONG": "OF Flow Long",
            "FLOW_SHORT": "OF Flow Short",
            "NEUTRAL": "OF Neutral",
            "NO_DATA": "No Tape/DOM Data",
        }

        tape_filter_score = int(np.clip(round(100.0 * (0.42 * delta_strength + 0.22 * volume_burst + 0.16 * trade_burst + 0.20 * progress_strength)), 0, 100))
        if dom_connected:
            filter_score = int(np.clip(round(0.60 * tape_filter_score + 0.40 * dom_filter_score), 0, 100))
        else:
            filter_score = int(tape_filter_score)
        filter_pass = bool(combined_confirm_long or combined_confirm_short)

        return {
            "of_state": state,
            "of_state_pretty": pretty_map.get(state, state.replace("_", " ").title()),
            "of_bias": bias,
            "of_note": str(note),
            "of_delta_pct": float(delta_pct),
            "of_volume_burst": float(volume_burst),
            "of_volume_burst_ratio": float(vol_burst_ratio),
            "of_trade_burst": float(trade_burst),
            "of_trade_burst_ratio": float(trade_burst_ratio),
            "of_progress_bps": float(progress_bps),
            "of_trade_rate": float(short_tps),
            "of_trade_count_short": int(round(float(short["count"]))),
            "of_trade_count_long": int(round(float(long["count"]))),
            "of_buy_qty_short": float(short["buy_qty"]),
            "of_sell_qty_short": float(short["sell_qty"]),
            "of_buy_qty_long": float(long["buy_qty"]),
            "of_sell_qty_long": float(long["sell_qty"]),
            "of_absorption_long": bool(absorption_long),
            "of_absorption_short": bool(absorption_short),
            "of_breakout_watch_long": bool(combined_watch_long),
            "of_breakout_watch_short": bool(combined_watch_short),
            "of_breakout_confirm_long": bool(combined_confirm_long),
            "of_breakout_confirm_short": bool(combined_confirm_short),
            "of_tape_watch_long": bool(tape_watch_long),
            "of_tape_watch_short": bool(tape_watch_short),
            "of_tape_confirm_long": bool(tape_confirm_long),
            "of_tape_confirm_short": bool(tape_confirm_short),
            "of_filter_score": int(filter_score),
            "of_tape_filter_score": int(tape_filter_score),
            "of_dom_filter_score": int(dom_filter_score),
            "of_filter_pass": bool(filter_pass),
            "of_window_short_sec": int(self.short_sec),
            "of_window_long_sec": int(self.long_sec),
            "of_dom_connected": bool(dom_connected),
            "of_dom_state": str(dom_state),
            "of_dom_state_pretty": str(dom_state_pretty),
            "of_dom_bias": str(dom_bias),
            "of_dom_note": str(dom_note),
            "of_dom_pressure_pct": float(dom_pressure_pct),
            "of_dom_pressure_accel": float(dom_pressure_accel),
            "of_dom_top5_imbalance_pct": float(dom_top5_imbalance_pct),
            "of_dom_top10_imbalance_pct": float(dom_top10_imbalance_pct),
            "of_dom_spread_bps": float(dom_spread_bps),
            "of_dom_bid_stack_ratio": float(dom_bid_stack_ratio),
            "of_dom_ask_stack_ratio": float(dom_ask_stack_ratio),
            "of_dom_bid_pull_ratio": float(dom_bid_pull_ratio),
            "of_dom_ask_pull_ratio": float(dom_ask_pull_ratio),
            "of_dom_breakout_watch_long": bool(dom_watch_long),
            "of_dom_breakout_watch_short": bool(dom_watch_short),
            "of_dom_breakout_confirm_long": bool(dom_confirm_long),
            "of_dom_breakout_confirm_short": bool(dom_confirm_short),
            "of_dom_updates_short": int(dom_short_updates),
            "of_dom_updates_long": int(dom_long_updates),
            "of_dom_best_bid": (float(dom_best_bid) if dom_best_bid is not None and np.isfinite(float(dom_best_bid)) else None),
            "of_dom_best_ask": (float(dom_best_ask) if dom_best_ask is not None and np.isfinite(float(dom_best_ask)) else None),
            "of_dom_mid": (float(dom_mid) if dom_mid is not None and np.isfinite(float(dom_mid)) else None),
            "of_last_trade_ts_ms": (int(last_trade_ts_ms) if last_trade_ts_ms is not None else None),
            "of_last_book_ts_ms": (int(last_book_ts_ms) if last_book_ts_ms is not None else None),
            "of_last_event_ts_ms": (int(latest_ts_ms) if latest_ts_ms is not None else None),
        }


def evaluate_boundary_aware_breakout(state_payload: Dict[str, Any], prev_runtime: Optional[Dict[str, Any]] = None) -> Tuple[Dict[str, Any], Dict[str, Any]]:
    """Evaluate order-flow only when price is interacting with a live compression boundary.

    This keeps microstructure confirmation tied to the actual squeeze/range edge instead of
    acting like a general tape-pressure meter. The function is pure and only consumes
    already-derived live fields plus a tiny runtime dictionary used to count consecutive
    outside-hold evaluations.
    """
    out = dict(state_payload or {})
    prev = dict(prev_runtime or {})

    current_price = _safe_float(out.get("price", out.get("close", out.get("of_dom_mid", np.nan))), np.nan)
    boundary_high = _safe_float(out.get("market_boundary_high"), np.nan)
    boundary_low = _safe_float(out.get("market_boundary_low"), np.nan)
    boundary_width_abs = _safe_float(out.get("market_boundary_width_abs"), np.nan)
    boundary_width_atr = _safe_float(out.get("market_boundary_width_atr"), np.nan)
    near_margin_abs = max(0.0, _safe_float(out.get("market_boundary_near_margin_abs"), 0.0))
    outside_margin_abs = max(0.0, _safe_float(out.get("market_boundary_outside_margin_abs"), 0.0))
    touch_upper = max(0, _safe_int(out.get("market_boundary_touch_count_upper"), 0))
    touch_lower = max(0, _safe_int(out.get("market_boundary_touch_count_lower"), 0))
    near_upper = bool(out.get("market_boundary_near_upper"))
    near_lower = bool(out.get("market_boundary_near_lower"))
    outside_above = bool(out.get("market_boundary_outside_above"))
    outside_below = bool(out.get("market_boundary_outside_below"))
    window_bars = max(0, _safe_int(out.get("market_boundary_window_bars"), 0))
    touch_window_bars = max(0, _safe_int(out.get("market_boundary_touch_window_bars"), 0))

    compression_maturity = _safe_float(out.get("market_compression_maturity"), 0.0)
    market_state = str(out.get("market_state") or "").upper().strip()
    breakout_bias = str(out.get("market_breakout_bias") or "NEUTRAL").upper().strip() or "NEUTRAL"
    pressure_bias = str(out.get("market_pressure_bias") or "NEUTRAL").upper().strip() or "NEUTRAL"
    data_quality = str(out.get("of_data_quality") or "UNKNOWN").upper().strip() or "UNKNOWN"

    watch_allowed = bool(out.get("of_watch_allowed"))
    confirmation_allowed = bool(out.get("of_confirmation_allowed"))
    absorption_long = bool(out.get("of_absorption_long"))
    absorption_short = bool(out.get("of_absorption_short"))
    dom_connected = bool(out.get("of_dom_connected"))

    flow_watch_long = bool(out.get("of_breakout_watch_long"))
    flow_watch_short = bool(out.get("of_breakout_watch_short"))
    flow_confirm_long = bool(out.get("of_breakout_confirm_long"))
    flow_confirm_short = bool(out.get("of_breakout_confirm_short"))
    dom_watch_long = bool(out.get("of_dom_breakout_watch_long"))
    dom_watch_short = bool(out.get("of_dom_breakout_watch_short"))
    dom_confirm_long = bool(out.get("of_dom_breakout_confirm_long"))
    dom_confirm_short = bool(out.get("of_dom_breakout_confirm_short"))
    progress_bps = _safe_float(out.get("of_progress_bps"), 0.0)
    delta_pct = _safe_float(out.get("of_delta_pct"), 0.0)

    valid_box = bool(
        np.isfinite(boundary_high)
        and np.isfinite(boundary_low)
        and np.isfinite(current_price)
        and boundary_high > boundary_low
        and np.isfinite(boundary_width_abs)
        and boundary_width_abs > 0.0
    )

    same_box = False
    if valid_box:
        prev_high = _safe_float(prev.get("boundary_high"), np.nan)
        prev_low = _safe_float(prev.get("boundary_low"), np.nan)
        tol = max(outside_margin_abs * 2.0, near_margin_abs, max(boundary_width_abs, 1e-12) * 0.08, 1e-9)
        if np.isfinite(prev_high) and np.isfinite(prev_low):
            same_box = abs(boundary_high - prev_high) <= tol and abs(boundary_low - prev_low) <= tol

    prev_hold_long = max(0, _safe_int(prev.get("outside_hold_long"), 0)) if same_box else 0
    prev_hold_short = max(0, _safe_int(prev.get("outside_hold_short"), 0)) if same_box else 0
    outside_hold_long = (prev_hold_long + 1) if outside_above else 0
    outside_hold_short = (prev_hold_short + 1) if outside_below else 0

    failed_reentry_long = bool(prev_hold_long >= 1 and (not outside_above) and np.isfinite(current_price) and current_price <= (boundary_high - max(near_margin_abs * 0.35, outside_margin_abs * 0.25, 1e-9)))
    failed_reentry_short = bool(prev_hold_short >= 1 and (not outside_below) and np.isfinite(current_price) and current_price >= (boundary_low + max(near_margin_abs * 0.35, outside_margin_abs * 0.25, 1e-9)))

    touch_threshold = max(3, int(round(max(10, window_bars) * 0.22))) if window_bars > 0 else 3
    mature_compression = bool((market_state == "COMPRESSION" or compression_maturity >= 58.0) and window_bars >= 12)

    long_pressure_ok = breakout_bias == "LONG" or pressure_bias == "LONG" or delta_pct >= 6.0
    short_pressure_ok = breakout_bias == "SHORT" or pressure_bias == "SHORT" or delta_pct <= -6.0

    long_watch_support = bool((flow_watch_long or flow_confirm_long) and (dom_watch_long or dom_confirm_long or not dom_connected))
    short_watch_support = bool((flow_watch_short or flow_confirm_short) and (dom_watch_short or dom_confirm_short or not dom_connected))
    long_confirm_support = bool((flow_confirm_long and (dom_watch_long or dom_confirm_long or not dom_connected)) or (dom_confirm_long and (flow_watch_long or flow_confirm_long)) or (flow_confirm_long and not dom_connected))
    short_confirm_support = bool((flow_confirm_short and (dom_watch_short or dom_confirm_short or not dom_connected)) or (dom_confirm_short and (flow_watch_short or flow_confirm_short)) or (flow_confirm_short and not dom_connected))

    boundary_watch_long = bool(valid_box and mature_compression and long_pressure_ok and watch_allowed and not absorption_long and (near_upper or outside_above) and touch_upper >= touch_threshold and long_watch_support)
    boundary_watch_short = bool(valid_box and mature_compression and short_pressure_ok and watch_allowed and not absorption_short and (near_lower or outside_below) and touch_lower >= touch_threshold and short_watch_support)
    boundary_confirm_long = bool(valid_box and mature_compression and long_pressure_ok and confirmation_allowed and not absorption_long and outside_above and outside_hold_long >= 2 and progress_bps >= 0.40 and long_confirm_support)
    boundary_confirm_short = bool(valid_box and mature_compression and short_pressure_ok and confirmation_allowed and not absorption_short and outside_below and outside_hold_short >= 2 and progress_bps <= -0.40 and short_confirm_support)

    acceptance_long = bool(valid_box and outside_above and outside_hold_long >= 2 and progress_bps >= 0.25 and (flow_watch_long or flow_confirm_long or dom_watch_long or dom_confirm_long))
    acceptance_short = bool(valid_box and outside_below and outside_hold_short >= 2 and progress_bps <= -0.25 and (flow_watch_short or flow_confirm_short or dom_watch_short or dom_confirm_short))

    boundary_state = "BOUNDARY_NEUTRAL"
    boundary_bias = "NEUTRAL"
    boundary_note_parts: List[str] = []

    if not valid_box:
        boundary_note_parts.append("compression boundary is not ready yet")
    else:
        boundary_note_parts.append(f"box {boundary_low:.4f} to {boundary_high:.4f} over {window_bars} bars")
        if near_upper:
            boundary_note_parts.append(f"upper edge under pressure ({touch_upper} touches)")
        elif near_lower:
            boundary_note_parts.append(f"lower edge under pressure ({touch_lower} touches)")
        else:
            boundary_note_parts.append("price is inside the box, away from the edge")

        if boundary_confirm_long:
            boundary_state = "BOUNDARY_CONFIRM_LONG"
            boundary_bias = "LONG"
            boundary_note_parts.append("price is holding above the box with supportive tape/DOM")
        elif boundary_confirm_short:
            boundary_state = "BOUNDARY_CONFIRM_SHORT"
            boundary_bias = "SHORT"
            boundary_note_parts.append("price is holding below the box with supportive tape/DOM")
        elif failed_reentry_long:
            boundary_state = "BOUNDARY_FAILED_LONG"
            boundary_bias = "LONG"
            boundary_note_parts.append("long-side break fell back inside the box")
        elif failed_reentry_short:
            boundary_state = "BOUNDARY_FAILED_SHORT"
            boundary_bias = "SHORT"
            boundary_note_parts.append("short-side break fell back inside the box")
        elif boundary_watch_long:
            boundary_state = "BOUNDARY_WATCH_LONG"
            boundary_bias = "LONG"
            boundary_note_parts.append("watch long: pressure is building right at the upper boundary")
        elif boundary_watch_short:
            boundary_state = "BOUNDARY_WATCH_SHORT"
            boundary_bias = "SHORT"
            boundary_note_parts.append("watch short: pressure is building right at the lower boundary")
        elif acceptance_long:
            boundary_state = "BOUNDARY_ACCEPT_LONG"
            boundary_bias = "LONG"
            boundary_note_parts.append("long-side acceptance is starting outside the box")
        elif acceptance_short:
            boundary_state = "BOUNDARY_ACCEPT_SHORT"
            boundary_bias = "SHORT"
            boundary_note_parts.append("short-side acceptance is starting outside the box")
        elif mature_compression:
            boundary_state = "BOUNDARY_COMPRESSION"
            boundary_bias = "LONG" if touch_upper > touch_lower else ("SHORT" if touch_lower > touch_upper else "NEUTRAL")
            boundary_note_parts.append("mature compression remains intact; wait for true edge release")
        else:
            boundary_note_parts.append("structure is not compressed enough for a boundary breakout read")

    pretty_map = {
        "BOUNDARY_CONFIRM_LONG": "Boundary Confirm Long",
        "BOUNDARY_CONFIRM_SHORT": "Boundary Confirm Short",
        "BOUNDARY_WATCH_LONG": "Boundary Watch Long",
        "BOUNDARY_WATCH_SHORT": "Boundary Watch Short",
        "BOUNDARY_ACCEPT_LONG": "Boundary Acceptance Long",
        "BOUNDARY_ACCEPT_SHORT": "Boundary Acceptance Short",
        "BOUNDARY_FAILED_LONG": "Boundary Failed Long",
        "BOUNDARY_FAILED_SHORT": "Boundary Failed Short",
        "BOUNDARY_COMPRESSION": "Boundary Compression",
        "BOUNDARY_NEUTRAL": "Boundary Neutral",
    }

    overlay = {
        "of_boundary_valid": bool(valid_box),
        "of_boundary_state": boundary_state,
        "of_boundary_state_pretty": pretty_map.get(boundary_state, boundary_state.replace("_", " ").title()),
        "of_boundary_bias": boundary_bias,
        "of_boundary_note": " | ".join(boundary_note_parts),
        "of_boundary_touch_threshold": int(touch_threshold),
        "of_boundary_watch_long": bool(boundary_watch_long),
        "of_boundary_watch_short": bool(boundary_watch_short),
        "of_boundary_confirm_long": bool(boundary_confirm_long),
        "of_boundary_confirm_short": bool(boundary_confirm_short),
        "of_boundary_acceptance_long": bool(acceptance_long),
        "of_boundary_acceptance_short": bool(acceptance_short),
        "of_boundary_failed_long": bool(failed_reentry_long),
        "of_boundary_failed_short": bool(failed_reentry_short),
        "of_boundary_outside_hold_long": int(outside_hold_long),
        "of_boundary_outside_hold_short": int(outside_hold_short),
    }

    runtime = {
        "boundary_high": (float(boundary_high) if np.isfinite(boundary_high) else None),
        "boundary_low": (float(boundary_low) if np.isfinite(boundary_low) else None),
        "outside_hold_long": int(outside_hold_long),
        "outside_hold_short": int(outside_hold_short),
        "boundary_state": str(boundary_state),
    }
    return overlay, runtime


def make_orderflow_windows(timeframes: Iterable[str]) -> Dict[str, OrderFlowWindow]:
    out: Dict[str, OrderFlowWindow] = {}
    for tf in timeframes:
        key = str(tf)
        if key and key not in out:
            out[key] = OrderFlowWindow(tf=key)
    return out
