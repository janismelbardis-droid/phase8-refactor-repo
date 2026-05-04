from __future__ import annotations

import copy
import json
from pathlib import Path
from typing import Any


WORKSPACE_ROOT = Path(__file__).resolve().parents[4]
CASEBOOK_ROOT = WORKSPACE_ROOT / "research_exports" / "trade_entry_casebook"
SOURCE_PACK = CASEBOOK_ROOT / "dual_window_candidate_v1" / "abs_take_320_stop_160_dual_window_candidate_v1.json"
OUTPUT_DIR = CASEBOOK_ROOT / "user_pullback_hypothesis_v1"
OUTPUT_PACK = OUTPUT_DIR / "user_pullback_hypothesis_v1.json"
CASE_REPORT = OUTPUT_DIR / "case_window_report.json"
MONTH_REPORT = OUTPUT_DIR / "prev_month_report.json"


def _load_json(path: Path) -> dict[str, Any]:
    with path.open("r", encoding="utf-8") as handle:
        payload = json.load(handle)
    if not isinstance(payload, dict):
        raise ValueError(f"Expected JSON object at {path}")
    return payload


def _sequence_rule(
    *,
    field: str,
    mode: str,
    op: str,
    value: Any,
    is_trigger: bool = False,
    is_final_gate: bool = False,
    bars_ago_mode: str = "OFF",
    bars_ago_n: int = 0,
    require_still_valid: bool = False,
    is_sequence_canceler: bool = False,
) -> dict[str, Any]:
    return {
        "timeframe": "1m",
        "mode": mode,
        "field": field,
        "op": op,
        "value": value,
        "is_trigger": is_trigger,
        "is_final_gate": is_final_gate,
        "bars_ago_mode": bars_ago_mode,
        "bars_ago_n": int(bars_ago_n),
        "window_bars": 1,
        "require_still_valid": require_still_valid,
        "is_sequence_canceler": is_sequence_canceler,
    }


def _build_preset(base_settings: dict[str, Any], phase_value: str) -> dict[str, Any]:
    settings = copy.deepcopy(base_settings)
    settings["stop_loss_mode"] = "ABSOLUTE"
    settings["stop_loss_value"] = 160.0
    settings["take_profit_mode"] = "ABSOLUTE"
    settings["take_profit_value"] = 320.0
    settings["stop_loss_pct"] = 0.0
    settings["take_profit_pct"] = 0.0
    settings["break_even_after_mfe_pct"] = 0.0
    settings["trailing_stop_pct"] = 0.0

    sequence_group = {
        "group_key": "A",
        "rules_join": "SEQUENCE",
        "rules": [
            _sequence_rule(
                field="vidya_trend_up",
                mode="event",
                op="EVENT",
                value=None,
                is_trigger=True,
            ),
            _sequence_rule(
                field="stoch_rsi_k",
                mode="state",
                op="<=",
                value=20.0,
            ),
            _sequence_rule(
                field="stoch_rsi_kd",
                mode="state",
                op="=",
                value="GREEN",
            ),
            _sequence_rule(
                field="vidya_state",
                mode="state",
                op="=",
                value="BUY",
                is_final_gate=True,
            ),
            _sequence_rule(
                field="range_filter_phase",
                mode="state",
                op="=",
                value=phase_value,
                is_final_gate=True,
                bars_ago_mode="WITHIN",
                bars_ago_n=5,
            ),
            _sequence_rule(
                field="vidya_trend_down",
                mode="event",
                op="EVENT",
                value=None,
                is_sequence_canceler=True,
            ),
        ],
    }

    return {
        "version": 1,
        "settings": settings,
        "tabs": {
            "Long Entry": {
                "groups_join": "AND",
                "groups": [sequence_group],
                "advanced_logic": {"enabled": False, "routes": []},
            },
            "Long Exit": {
                "groups_join": "AND",
                "groups": [],
                "advanced_logic": {"enabled": False, "routes": []},
            },
            "Short Entry": {
                "groups_join": "AND",
                "groups": [],
                "advanced_logic": {"enabled": False, "routes": []},
            },
            "Short Exit": {
                "groups_join": "AND",
                "groups": [],
                "advanced_logic": {"enabled": False, "routes": []},
            },
        },
        "entry_filters": {
            "Long Entry": {
                "enabled": False,
                "mode": "OFF",
                "expansion_atr_mult": 2.0,
                "hard_skip_atr_mult": 0.0,
                "confirm_bars": 1,
                "reference_range": "FULL_CANDLE",
                "touch_type": "WICK",
                "min_retrace_pct": 50.0,
                "max_retrace_pct": 100.0,
                "require_next_close_beyond_mid": True,
                "require_setup_still_valid": False,
                "apply_to_reversals": True,
                "groups": {},
            },
            "Short Entry": {
                "enabled": False,
                "mode": "OFF",
                "expansion_atr_mult": 2.0,
                "hard_skip_atr_mult": 3.0,
                "confirm_bars": 1,
                "reference_range": "FULL_CANDLE",
                "touch_type": "WICK",
                "min_retrace_pct": 50.0,
                "max_retrace_pct": 100.0,
                "require_next_close_beyond_mid": True,
                "require_setup_still_valid": True,
                "apply_to_reversals": True,
                "groups": {},
            },
        },
    }


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as handle:
        json.dump(payload, handle, ensure_ascii=False, indent=2)
        handle.write("\n")


def main() -> None:
    source_pack = _load_json(SOURCE_PACK)
    source_presets = dict(source_pack.get("presets") or {})
    base_settings = copy.deepcopy(dict(source_presets["901"]["settings"]))

    pack = {
        "meta": {
            "autoload_last": False,
            "last_preset": "911",
            "source": "user_pullback_hypothesis_v1",
            "version_note": (
                "Test pack built from user description: after 1m VIDYA flip up, wait for Stoch K <= 20, "
                "then Stoch K/D back to GREEN, with a recent Range Filter weak/neutral phase."
            ),
            "take_experiment": source_pack.get("meta", {}).get("take_experiment", {}),
            "user_hypothesis": {
                "goal": "Test a post-flip deep-pullback reclaim family anchored by oversold Stoch RSI and recent Range Filter weak/neutral phase.",
                "notes": [
                    "Preset language cannot directly compare entry price to the original EnterMACD candle body, so that part is approximated by the pullback sequence.",
                    "911 looks for recent RF NEUTRAL.",
                    "912 looks for recent RF BUY_WEAK.",
                    "913 looks for recent RF SELL_WEAK.",
                ],
            },
        },
        "presets": {
            "911": _build_preset(base_settings, "NEUTRAL"),
            "912": _build_preset(base_settings, "BUY_WEAK"),
            "913": _build_preset(base_settings, "SELL_WEAK"),
        },
    }

    case_period = {
        "period": {
            "start_utc": "2026-04-15 21:42:59.999000+00:00",
            "end_utc": "2026-04-17 11:47:59.999000+00:00",
        },
        "cases": [],
        "presets": {
            "911": {"preset_family": "U_NEUTRAL"},
            "912": {"preset_family": "U_BUY_WEAK"},
            "913": {"preset_family": "U_SELL_WEAK"},
        },
    }

    month_period = {
        "period": {
            "start_utc": "2026-03-15 21:42:59.999000+00:00",
            "end_utc": "2026-04-15 21:41:59.999000+00:00",
        },
        "cases": [],
        "presets": {
            "911": {"preset_family": "U_NEUTRAL"},
            "912": {"preset_family": "U_BUY_WEAK"},
            "913": {"preset_family": "U_SELL_WEAK"},
        },
    }

    _write_json(OUTPUT_PACK, pack)
    _write_json(CASE_REPORT, case_period)
    _write_json(MONTH_REPORT, month_period)

    print(str(OUTPUT_PACK))
    print(str(CASE_REPORT))
    print(str(MONTH_REPORT))


if __name__ == "__main__":
    main()
