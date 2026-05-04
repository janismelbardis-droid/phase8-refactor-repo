from app.backtest import _normalize_entry_filter_map
from app.services.ui_v2_runtime import _materialize_preset
from app.strategy_requirements import compile_stream_requirements


def test_materialize_preset_preserves_rule_compare_to_targets() -> None:
    preset = {
        "tabs": {
            "Long Entry": {
                "groups_join": "AND",
                "groups": [
                    {
                        "rules_join": "AND",
                        "rules": [
                            {
                                "timeframe": "1m",
                                "mode": "state",
                                "field": "close",
                                "op": ">",
                                "value": None,
                                "compare_to": {"field": "ema_2", "bars_ago": 20},
                            }
                        ],
                    }
                ],
            }
        }
    }

    rules_model, _, _, _ = _materialize_preset(preset)
    rule = rules_model["Long Entry"][0][0]

    assert rule.compare_to == {"field": "ema_2", "bars_ago": 20}


def test_materialize_preset_preserves_active_entry_filter_defaults_and_remaps_group_keys() -> None:
    preset = {
        "tabs": {
            "Long Entry": {
                "groups_join": "AND",
                "groups": [
                    {"group_key": "A", "rules_join": "OR", "rules": []},
                    {"group_key": "B", "rules_join": "OR", "rules": []},
                ],
            },
            "Long Exit": {"groups_join": "AND", "groups": []},
            "Short Entry": {"groups_join": "AND", "groups": []},
            "Short Exit": {"groups_join": "AND", "groups": []},
        },
        "entry_filters": {
            "Long Entry": {
                "enabled": True,
                "mode": "RETRACE_ALWAYS",
                "hard_skip_atr_mult": 0.0,
                "confirm_bars": 6,
                "reference_range": "FULL_CANDLE",
                "touch_type": "WICK",
                "min_retrace_pct": 35.0,
                "max_retrace_pct": 100.0,
                "groups": {
                    "B": {
                        "enabled": True,
                        "mode": "ANTI_CHASE",
                        "expansion_atr_mult": 2.5,
                        "hard_skip_atr_mult": 3.0,
                        "confirm_bars": 2,
                    }
                },
            },
            "Short Entry": {"enabled": False, "mode": "OFF", "groups": {}},
        },
    }

    _, _, _, entry_filters = _materialize_preset(preset)

    assert entry_filters["Long Entry"]["enabled"] is True
    assert entry_filters["Long Entry"]["mode"] == "RETRACE_ALWAYS"
    assert entry_filters["Long Entry"]["groups"]["1"]["enabled"] is True
    assert entry_filters["Long Entry"]["groups"]["1"]["mode"] == "ANTI_CHASE"

    normalized = _normalize_entry_filter_map(entry_filters)
    assert normalized["Long Entry"]["default"].is_active() is True
    assert normalized["Long Entry"]["default"].normalized_mode() == "RETRACE_ALWAYS"
    assert normalized["Long Entry"]["groups"]["1"].is_active() is True
    assert normalized["Long Entry"]["groups"]["1"].normalized_mode() == "ANTI_CHASE"

    spec = compile_stream_requirements({}, entry_filters=entry_filters, include_plot_defaults=False)
    assert "atr" in spec.required_fields
