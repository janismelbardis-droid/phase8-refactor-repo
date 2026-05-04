from __future__ import annotations

"""Live order-flow quality helpers extracted from the live engine shell."""

from dataclasses import dataclass, field
from typing import Any, Dict, Optional, Tuple

from ...runtime_safety import annotate_payload, safe_execute


@dataclass(frozen=True)
class OrderflowAssessment:
    tape_age_ms: Optional[int] = None
    tape_quality: str = "NO_DATA"
    tape_confidence: int = 0
    tape_fresh: bool = False
    dom_quality: str = "NO_DATA"
    dom_confidence: int = 0
    dom_fresh: bool = False
    data_quality: str = "NO_DATA"
    data_confidence: int = 0
    watch_allowed: bool = False
    confirmation_allowed: bool = False
    watch_lock_reason: str = ""
    confirmation_lock_reason: str = ""
    quality_note: str = ""
    dom_sync_state_pretty: str = "-"
    status: str = "ok"
    status_reason: str = ""
    error_type: str = ""
    degraded_feature: str = ""
    extras: Dict[str, Any] = field(default_factory=dict)

    def as_dict(self) -> Dict[str, Any]:
        out = dict(self.extras)
        out.update({
            "of_tape_age_ms": self.tape_age_ms,
            "of_tape_quality": self.tape_quality,
            "of_tape_confidence": int(self.tape_confidence),
            "of_tape_fresh": bool(self.tape_fresh),
            "of_dom_quality": self.dom_quality,
            "of_dom_confidence": int(self.dom_confidence),
            "of_dom_fresh": bool(self.dom_fresh),
            "of_data_quality": self.data_quality,
            "of_data_confidence": int(self.data_confidence),
            "of_watch_allowed": bool(self.watch_allowed),
            "of_confirmation_allowed": bool(self.confirmation_allowed),
            "of_watch_lock_reason": self.watch_lock_reason,
            "of_confirmation_lock_reason": self.confirmation_lock_reason,
            "of_quality_note": self.quality_note,
            "of_dom_sync_state_pretty": self.dom_sync_state_pretty,
            "orderflow_status": self.status,
            "orderflow_status_reason": self.status_reason,
            "orderflow_error_type": self.error_type,
            "orderflow_degraded_feature": self.degraded_feature,
        })
        return out


def classify_orderflow_age(age_ms: Optional[int], *, good_ms: int, degraded_ms: int, disabled: bool = False) -> Tuple[str, int]:
    if disabled:
        return "DISABLED", 0
    if age_ms is None:
        return "NO_DATA", 0
    try:
        age = int(age_ms)
    except Exception:
        return "INVALID", 0
    if age < 0:
        return "INVALID", 0
    if age <= int(good_ms):
        return "GOOD", 100
    if age <= int(degraded_ms):
        span = max(1, int(degraded_ms) - int(good_ms))
        decay = max(0.0, min(1.0, float(age - int(good_ms)) / float(span)))
        return "DEGRADED", int(round(100.0 - 35.0 * decay))
    return "STALE", 0


def build_orderflow_quality_overlay(
    snap: Dict[str, Any],
    *,
    now_ms: int,
    mode: str,
    dom_good_ms: int,
    dom_degraded_ms: int,
    tape_good_ms: int,
    tape_degraded_ms: int,
    snapshot_stale_ms: int,
    tape_enabled: bool,
) -> Dict[str, Any]:
    def _build() -> Dict[str, Any]:
        overlay: Dict[str, Any] = {}
        mode_local = str(mode or "AUTO").upper().strip() or "AUTO"
        dom_source = str(snap.get("of_dom_source") or "NONE").upper().strip() or "NONE"

        last_trade_ts = snap.get("of_last_trade_ts_ms")
        tape_age_ms = None
        if last_trade_ts is not None:
            try:
                tape_age_ms = int(max(0, int(now_ms) - int(last_trade_ts)))
            except Exception:
                tape_age_ms = None
        tape_quality, tape_confidence = classify_orderflow_age(
            tape_age_ms,
            good_ms=tape_good_ms,
            degraded_ms=tape_degraded_ms,
            disabled=(not tape_enabled),
        )
        overlay["of_tape_age_ms"] = tape_age_ms
        overlay["of_tape_quality"] = tape_quality
        overlay["of_tape_confidence"] = int(tape_confidence)
        overlay["of_tape_fresh"] = bool(tape_quality in ("GOOD", "DEGRADED"))

        dom_age_ms = snap.get("of_dom_depth_age_ms")
        dom_snapshot_age_ms = snap.get("of_dom_snapshot_age_ms")
        dom_resync_required = bool(snap.get("of_dom_resync_required"))
        dom_local_synced = bool(snap.get("of_dom_local_book_synced"))
        dom_partial_connected = bool(snap.get("of_dom_partial_connected"))
        dom_sync_state = str(snap.get("of_dom_sync_state") or "-")

        if dom_source == "NONE":
            dom_quality = "DISABLED"
            dom_confidence = 0
        else:
            dom_quality, dom_confidence = classify_orderflow_age(
                dom_age_ms,
                good_ms=dom_good_ms,
                degraded_ms=dom_degraded_ms,
                disabled=False,
            )
            if dom_snapshot_age_ms is not None:
                try:
                    if int(dom_snapshot_age_ms) > int(snapshot_stale_ms):
                        dom_quality = "STALE"
                        dom_confidence = 0
                except Exception:
                    pass
            if dom_source == "LOCAL_L2":
                if dom_resync_required:
                    dom_quality = "INVALID"
                    dom_confidence = 0
                elif not dom_local_synced:
                    dom_quality = "NO_DATA"
                    dom_confidence = 0
            elif dom_source == "PARTIAL_DEPTH":
                if not dom_partial_connected:
                    dom_quality = "NO_DATA"
                    dom_confidence = 0
                elif dom_quality == "GOOD":
                    dom_quality = "DEGRADED"
                    dom_confidence = min(int(dom_confidence), 72)

        overlay["of_dom_quality"] = dom_quality
        overlay["of_dom_confidence"] = int(dom_confidence)
        overlay["of_dom_fresh"] = bool(dom_quality in ("GOOD", "DEGRADED"))

        watch_allowed = False
        confirmation_allowed = False
        confirmation_lock_reason = ""
        watch_lock_reason = ""

        if mode_local == "OFF":
            watch_lock_reason = "order flow disabled"
            confirmation_lock_reason = "order flow disabled"
        elif mode_local == "TAPE_ONLY":
            watch_allowed = bool(tape_quality in ("GOOD", "DEGRADED"))
            confirmation_allowed = bool(tape_quality in ("GOOD", "DEGRADED"))
            if not watch_allowed:
                watch_lock_reason = "tape feed is stale or missing"
                confirmation_lock_reason = "tape feed is stale or missing"
        elif mode_local == "PARTIAL_DOM":
            watch_allowed = bool(tape_quality in ("GOOD", "DEGRADED") or dom_quality in ("GOOD", "DEGRADED"))
            confirmation_allowed = bool(tape_quality in ("GOOD", "DEGRADED") and dom_quality in ("GOOD", "DEGRADED"))
            if not watch_allowed:
                watch_lock_reason = "tape and DOM are both stale or missing"
            if not confirmation_allowed:
                if tape_quality not in ("GOOD", "DEGRADED"):
                    confirmation_lock_reason = "tape feed is stale or missing"
                elif dom_quality not in ("GOOD", "DEGRADED"):
                    confirmation_lock_reason = "partial DOM feed is stale or missing"
        else:
            watch_allowed = bool(tape_quality in ("GOOD", "DEGRADED") or dom_quality in ("GOOD", "DEGRADED"))
            dom_ready_for_confirm = bool(
                dom_source == "LOCAL_L2"
                and dom_quality in ("GOOD", "DEGRADED")
                and dom_local_synced
                and not dom_resync_required
            )
            confirmation_allowed = bool(tape_quality in ("GOOD", "DEGRADED") and dom_ready_for_confirm)
            if not watch_allowed:
                watch_lock_reason = "tape and DOM are both stale or missing"
            if not confirmation_allowed:
                if tape_quality not in ("GOOD", "DEGRADED"):
                    confirmation_lock_reason = "tape feed is stale or missing"
                elif dom_resync_required:
                    confirmation_lock_reason = "local L2 book requires resync"
                elif dom_source != "LOCAL_L2" or not dom_local_synced:
                    confirmation_lock_reason = "local L2 book is not synced yet"
                elif dom_quality not in ("GOOD", "DEGRADED"):
                    confirmation_lock_reason = "local L2 DOM feed is stale"
                else:
                    confirmation_lock_reason = "order-flow confirmation data is not ready"

        overall_quality = "NO_DATA"
        if mode_local == "OFF":
            overall_quality = "DISABLED"
        else:
            quality_rank = {"INVALID": 0, "STALE": 1, "NO_DATA": 2, "DEGRADED": 3, "GOOD": 4, "DISABLED": 5}
            comps = []
            if tape_quality != "DISABLED":
                comps.append(tape_quality)
            if dom_quality != "DISABLED" and mode_local != "TAPE_ONLY":
                comps.append(dom_quality)
            if comps:
                overall_quality = min(comps, key=lambda q: quality_rank.get(q, -1))
            elif tape_quality == "DISABLED" and dom_quality == "DISABLED":
                overall_quality = "DISABLED"
        if mode_local in ("AUTO", "LOCAL_L2") and dom_source == "PARTIAL_DEPTH" and overall_quality == "GOOD":
            overall_quality = "DEGRADED"

        overall_confidence = int(max(0, min(100, round((int(tape_confidence) + int(dom_confidence if mode_local != "TAPE_ONLY" else tape_confidence)) / (2 if mode_local != "TAPE_ONLY" else 1)))))
        quality_note_parts = [f"Tape {tape_quality}"]
        if mode_local != "TAPE_ONLY":
            quality_note_parts.append(f"DOM {dom_quality}")
        if confirmation_allowed:
            quality_note_parts.append("confirmation unlocked")
        elif confirmation_lock_reason:
            quality_note_parts.append(f"confirm lock: {confirmation_lock_reason}")

        assessment = OrderflowAssessment(
            tape_age_ms=tape_age_ms,
            tape_quality=tape_quality,
            tape_confidence=int(tape_confidence),
            tape_fresh=bool(tape_quality in ("GOOD", "DEGRADED")),
            dom_quality=dom_quality,
            dom_confidence=int(dom_confidence),
            dom_fresh=bool(dom_quality in ("GOOD", "DEGRADED")),
            data_quality=overall_quality,
            data_confidence=int(overall_confidence),
            watch_allowed=bool(watch_allowed),
            confirmation_allowed=bool(confirmation_allowed),
            watch_lock_reason=str(watch_lock_reason),
            confirmation_lock_reason=str(confirmation_lock_reason),
            quality_note=" | ".join(quality_note_parts),
            dom_sync_state_pretty=dom_sync_state,
            extras=overlay,
        )
        return assessment.as_dict()

    payload, report = safe_execute(
        _build,
        default=OrderflowAssessment(
            data_quality='NO_DATA',
            data_confidence=0,
            quality_note='order-flow overlay degraded / fallback active',
            status='failed',
            status_reason='order-flow overlay degraded / fallback active',
            degraded_feature='quality_overlay',
        ).as_dict(),
        module='live_orderflow',
        function='build_orderflow_quality_overlay',
        degraded_feature='quality_overlay',
    )
    annotate_payload(payload, 'orderflow', report)
    return payload
