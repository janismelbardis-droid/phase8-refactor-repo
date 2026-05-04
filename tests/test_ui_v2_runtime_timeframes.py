import pandas as pd

from app.services.ui_v2_runtime import (
    _latest_directional_event_side_age,
    _latest_event_age_bars,
    _latest_state_age_bars,
    _range_display_state,
    _resolve_preset_timeframes,
    _sync_preset_timeframes,
)


def test_resolve_preset_timeframes_prefers_rules_and_ema_overrides_over_legacy_flags() -> None:
    preset = {
        "settings": {
            "timeframes": {"1m": True, "5m": False, "15m": False, "30m": True, "1h": False},
            "ema_indicators": {
                "defaults": {"ema_1_length": 9, "ema_2_length": 21},
                "overrides": {"1h": {"ema_1_length": 50, "ema_2_length": 200}},
            },
        },
        "tabs": {
            "Long Entry": {
                "groups": [
                    {
                        "rules": [
                            {"timeframe": "5m", "mode": "state", "field": "ms", "op": "=", "value": "GREEN"},
                        ]
                    }
                ]
            }
        },
    }

    assert _resolve_preset_timeframes(preset) == ["5m", "1h"]

    synced = _sync_preset_timeframes(preset)
    assert synced == ["5m", "1h"]
    assert preset["settings"]["timeframes"] == {
        "1m": False,
        "5m": True,
        "15m": False,
        "30m": False,
        "1h": True,
    }


def test_resolve_preset_timeframes_falls_back_to_legacy_flags_then_1m() -> None:
    legacy_only = {
        "settings": {
            "timeframes": {"1m": False, "5m": False, "15m": True, "30m": False, "1h": True},
        },
        "tabs": {},
    }
    empty = {"settings": {"timeframes": {}}, "tabs": {}}

    assert _resolve_preset_timeframes(legacy_only) == ["15m", "1h"]
    assert _resolve_preset_timeframes(empty) == ["1m"]


def test_latest_state_age_bars_counts_current_state_run_from_history() -> None:
    frame = pd.DataFrame({
        "frama_state": ["BUY", "BUY", "SELL", "SELL", "SELL"],
        "range_filter_state": ["NEUTRAL", "BUY", "BUY", "BUY", "BUY"],
    })

    assert _latest_state_age_bars(frame, "frama_state") == 2
    assert _latest_state_age_bars(frame, "range_filter_state") == 3


def test_latest_event_age_bars_counts_distance_from_last_trigger() -> None:
    frame = pd.DataFrame({
        "frama_break_up": [False, True, False, False, False],
        "frama_break_down": [False, False, False, True, False],
        "frama_mid_cross": [False, False, True, False, False],
    })

    assert _latest_event_age_bars(frame, "frama_break_up") == 3
    assert _latest_event_age_bars(frame, "frama_break_down") == 1
    assert _latest_event_age_bars(frame, "frama_mid_cross") == 2


def test_latest_directional_event_side_age_tracks_last_buy_sell_trigger_independent_of_state() -> None:
    frame = pd.DataFrame({
        "frama_state": ["BUY", "BUY", "NEUTRAL", "NEUTRAL", "NEUTRAL"],
        "frama_break_up": [False, True, False, False, False],
        "frama_break_down": [False, False, False, False, False],
    })

    assert _latest_directional_event_side_age(frame, "frama_break_up", "frama_break_down") == ("BUY", 3)


def test_range_display_state_prefers_explicit_neutral_over_stale_phase_memory() -> None:
    assert _range_display_state("NEUTRAL", "SELL_WEAK", -1) == "NEUTRAL"
