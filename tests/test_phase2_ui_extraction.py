from __future__ import annotations

from types import SimpleNamespace

from app.ui.inspectors import (
    build_market_state_detail_text,
    build_orderflow_detail_text,
    build_orderflow_trail_text,
    derive_classic_ta_snapshot,
    popup_yes_no,
)
from app.ui.dialogs import bind_popup_text_scroll, set_popup_text_content


class _FakeEngine:
    def get_orderflow_event_history(self, tf: str, limit: int = 80):
        assert tf == "1m"
        assert limit == 80
        return [
            {
                "ts_ms": 1710000000000,
                "seq": 12,
                "event_type": "WATCH_LONG",
                "summary": "watch fired",
                "seen_count": 2,
                "delta_pct": 1.25,
                "progress_bps": 4.5,
                "dom_pressure_pct": 12.0,
                "filter_gate": "PASS",
                "filter_pass": True,
                "of_data_quality": "GOOD",
                "reason": "delta + DOM aligned",
            }
        ]


class _FakeApp:
    def __init__(self):
        self.latest = {
            "1m": {
                "market_state": "BULL_EXPANSION",
                "market_bias": "LONG",
                "market_phase": "EXPANSION",
                "market_confidence": 91,
                "market_state_age": 2,
                "market_texture": "trend",
                "market_summary_short": "Bull pressure intact",
                "market_structure_flag": "INTACT",
                "market_structure_pass": True,
                "market_structure_swing_hh": True,
                "market_structure_swing_hl": True,
                "market_structure_last_high": 102.5,
                "market_structure_last_low": 100.2,
                "market_boundary_high": 103.0,
                "market_boundary_low": 99.8,
                "market_boundary_width_atr": 1.2,
                "market_pa_vol_flag": "PASS",
                "market_pa_vol_pass": True,
                "market_pa_pattern": "Impulse",
                "market_pa_strict_flag": "PASS",
                "market_pa_strict_pass": True,
                "market_breakout_state": "BREAKOUT_CONFIRM",
                "market_breakout_pretty": "Breakout Confirm",
                "market_breakout_bias": "LONG",
                "market_breakout_score": 88,
                "market_breakout_style": "SEQUENCE_LONG",
                "market_breakout_trigger_ready_raw": True,
                "market_breakout_trigger_ready": True,
                "market_breakout_filter_gate": "PASS_GATE",
                "market_breakout_filter_pass": True,
                "market_breakout_filter_note": "green-light",
                "market_of_risk_flag": "PASS",
                "market_of_risk_pass": True,
                "market_of_risk_score": 82,
                "market_of_risk_note": "clean tape",
                "market_volatility_regime": "NORMAL",
                "market_trend_regime": "UPTREND",
                "market_trend_quality": "CONFIRMED",
                "market_market_type": "TREND",
                "market_value_context": "above VWAP",
                "market_vwap_bias": "LONG",
                "market_summary_long": "Broad trend is healthy.",
                "vidya_state": "BUY",
                "vidya_slope": 1.0,
                "vidya_angle_deg_norm": 8.0,
                "vidya_angle_state": "UP",
                "vidya_angle_accel": "BUILDING",
                "vidya_delta_pct": 14.2,
                "frama_state": "BUY",
                "range_filter_state": "BUY",
                "range_filter_phase": "EXPANSION",
                "ms": "BULLISH",
                "angle": "UP",
                "atr": 1.1,
                "atr_angle": "UP",
                "macd": 1.5,
                "signal": 1.0,
                "of_state": "ALIGNED",
                "of_state_pretty": "Aligned",
                "of_bias": "LONG",
                "of_filter_score": 77,
                "of_integration_mode": "AUTO",
                "of_integration_effect": "ALLOW",
                "of_integration_blocked": False,
                "of_filter_pass": True,
                "of_filter_note": "tape confirms",
                "of_note": "microstructure agrees",
                "of_intrabar_hostile_long_hits": 0,
                "of_intrabar_hostile_short_hits": 0,
                "of_intrabar_absorption_long_hits": 1,
                "of_intrabar_absorption_short_hits": 0,
                "of_intrabar_flip_count": 0,
                "of_intrabar_filter_block_hits": 0,
                "of_data_quality": "GOOD",
                "of_data_confidence": 95,
                "of_event_count": 4,
                "of_last_event_type": "WATCH_LONG",
                "of_last_event_summary": "watch fired",
                "of_watch_allowed": True,
                "of_confirmation_allowed": True,
                "of_window_short_sec": 5,
                "of_window_long_sec": 30,
                "of_delta_pct": 1.25,
                "of_progress_bps": 4.5,
                "of_tape_age_ms": 120,
                "of_tape_quality": "GOOD",
                "of_volume_burst": 1.4,
                "of_volume_burst_ratio": 1.2,
                "of_trade_burst_ratio": 1.1,
                "of_trade_rate": 9.0,
                "of_trade_count_short": 15,
                "of_trade_count_long": 25,
                "of_buy_qty_short": 7.5,
                "of_sell_qty_short": 5.0,
                "of_tape_watch_long": True,
                "of_tape_watch_short": False,
                "of_tape_confirm_long": True,
                "of_tape_confirm_short": False,
                "of_absorption_long": True,
                "of_absorption_short": False,
                "of_tape_filter_score": 80,
                "of_dom_filter_score": 73,
                "of_boundary_state": "ACCEPTED_ABOVE",
                "of_boundary_state_pretty": "Accepted Above",
                "of_boundary_bias": "LONG",
                "market_boundary_valid": True,
                "market_boundary_source": "swing",
                "market_boundary_dist_to_upper_atr": 0.2,
                "market_boundary_dist_to_lower_atr": 1.1,
                "market_boundary_touch_count_upper": 3,
                "market_boundary_touch_count_lower": 1,
                "of_boundary_touch_threshold": 2,
                "market_boundary_near_upper": True,
                "market_boundary_near_lower": False,
                "market_boundary_outside_above": True,
                "market_boundary_outside_below": False,
                "of_boundary_watch_long": True,
                "of_boundary_watch_short": False,
                "of_boundary_confirm_long": True,
                "of_boundary_confirm_short": False,
                "of_boundary_outside_hold_long": 2,
                "of_boundary_outside_hold_short": 0,
                "of_boundary_failed_long": False,
                "of_boundary_failed_short": False,
                "of_boundary_note": "accepted above range",
                "of_watch_lock_active": False,
                "of_confirmation_lock_active": False,
                "of_watch_lock_reason": "-",
                "of_confirmation_lock_reason": "-",
                "of_dom_connected": True,
                "of_dom_source": "LOCAL_L2",
                "of_dom_sync_state": "SYNCED",
                "of_dom_resync_required": False,
                "of_dom_quality": "GOOD",
                "of_dom_confidence": 90,
                "of_dom_state": "STACKED_BIDS",
                "of_dom_state_pretty": "Stacked Bids",
                "of_dom_bias": "LONG",
                "of_dom_pressure_pct": 12.0,
                "of_dom_pressure_accel": 1.4,
                "of_dom_spread_bps": 0.4,
                "of_dom_local_snapshot_id": 10,
                "of_dom_local_last_event_u": 11,
                "of_dom_local_last_event_pu": 10,
                "of_dom_local_buffered_events": 0,
                "of_dom_local_updates_applied": 25,
                "of_dom_book_levels_bid": 20,
                "of_dom_book_levels_ask": 20,
                "of_dom_partial_fallback_enabled": True,
                "of_dom_partial_fallback_active": False,
                "of_dom_partial_connected": True,
                "of_dom_partial_updates_total": 100,
                "of_dom_depth_age_ms": 50,
                "of_dom_snapshot_age_ms": 70,
                "of_dom_top5_imbalance_pct": 8.5,
                "of_dom_top10_imbalance_pct": 6.2,
                "of_dom_bid_stack_ratio": 1.6,
                "of_dom_ask_stack_ratio": 0.8,
                "of_dom_bid_pull_ratio": 0.7,
                "of_dom_ask_pull_ratio": 1.3,
                "of_dom_breakout_watch_long": True,
                "of_dom_breakout_watch_short": False,
                "of_dom_breakout_confirm_long": True,
                "of_dom_breakout_confirm_short": False,
                "of_dom_updates_short": 15,
                "of_dom_updates_long": 20,
                "of_dom_best_bid": 101.2,
                "of_dom_best_ask": 101.3,
                "of_dom_mid": 101.25,
                "of_dom_note": "book supportive",
            }
        }
        self._breakout_alert_last_by_tf = {
            "1m": {"event_time": "2026-03-28 12:00:00", "direction": "LONG", "price": 101.2}
        }
        self.live_engine = _FakeEngine()

    def _market_state_thresholds_obj(self):
        return None


class _FakeText:
    def __init__(self):
        self.text = ""
        self._x = 0.4
        self._y = 0.6
        self.bound = {}
        self.history = []

    def bind(self, event, func):
        self.bound[event] = func

    def xview(self):
        return (self._x, 1.0)

    def yview(self):
        return (self._y, 1.0)

    def config(self, **kwargs):
        self.history.append(("config", kwargs))

    def delete(self, start, end):
        self.history.append(("delete", start, end))
        self.text = ""

    def insert(self, start, content):
        self.history.append(("insert", start, content))
        self.text = content

    def xview_moveto(self, value):
        self._x = value

    def yview_moveto(self, value):
        self._y = value



def test_popup_yes_no_and_snapshot_mapping():
    latest = {
        "market_structure_flag": "INTACT",
        "market_structure_swing_hh": True,
        "market_structure_swing_hl": True,
        "market_pa_phase3_context": "LONG_SEQUENCE",
        "market_breakout_state": "BREAKOUT_CONFIRM",
        "market_breakout_bias": "LONG",
        "market_pa_strict_flag": "PASS",
        "market_pa_vol_flag": "PASS",
        "market_of_risk_flag": "PASS",
        "market_market_type": "TREND",
        "market_trend_regime": "UPTREND",
        "market_trend_quality": "CONFIRMED",
        "market_volatility_regime": "NORMAL",
        "market_vwap_bias": "LONG",
    }
    classic = derive_classic_ta_snapshot(latest)
    assert popup_yes_no(True) == "YES"
    assert popup_yes_no(False) == "NO"
    assert classic["swing_map"] == "HH + HL intact"
    assert classic["local_sequence"] == "confirming"
    assert classic["actionability"] == "Actionable breakout"



def test_market_state_detail_text_keeps_core_sections():
    app = _FakeApp()
    text = build_market_state_detail_text(app, "1m")
    assert "=== MARKET SUMMARY ===" in text
    assert "State: Bull Expansion   Bias: LONG   Phase: EXPANSION" in text
    assert "=== SWING STRUCTURE ===" in text
    assert "=== BREAKOUT / TRIGGER STATUS ===" in text
    assert "=== QUICK RULE IDEAS ===" in text
    assert "market_state = BULL_EXPANSION" in text
    assert "market_confidence >= 91" in text



def test_orderflow_detail_and_trail_text_keep_dom_and_event_data():
    app = _FakeApp()
    detail = build_orderflow_detail_text(app, "1m")
    trail = build_orderflow_trail_text(app, "1m")
    assert "DOM Connected: YES" in detail
    assert "Boundary State: Accepted Above   Bias: LONG" in detail
    assert "book supportive" in detail
    assert "Trail Size: 1" in trail
    assert "WATCH_LONG" in trail
    assert "delta + DOM aligned" in trail



def test_popup_text_helpers_preserve_view_and_bindings():
    txt = _FakeText()
    bind_popup_text_scroll(txt)
    assert "<MouseWheel>" in txt.bound
    assert "<Shift-MouseWheel>" in txt.bound

    set_popup_text_content(txt, "hello world", preserve_view=True)
    assert txt.text == "hello world"
    assert txt.xview()[0] == 0.4
    assert txt.yview()[0] == 0.6

    set_popup_text_content(txt, "", preserve_view=False)
    assert "No detail text available yet." in txt.text
    assert txt.xview()[0] == 0.0
    assert txt.yview()[0] == 0.0
