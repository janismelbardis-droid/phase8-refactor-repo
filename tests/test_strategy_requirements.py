from __future__ import annotations

from app.rules import Rule
from app.strategy_requirements import (
    FAMILY_ADX,
    FAMILY_ATR,
    FAMILY_EMA,
    FAMILY_FRAMA,
    FAMILY_MACD_LINES,
    FAMILY_MACD_STATES,
    FAMILY_MARKET_AUG,
    FAMILY_OHLCV,
    FAMILY_RANGE_FILTER,
    FAMILY_RSI,
    FAMILY_STOCH_RSI,
    FAMILY_TAKER_BIAS,
    FAMILY_VIDYA,
    canonicalize_family_selection,
    compile_stream_requirements,
    normalize_required_fields,
    normalize_required_families,
)


def test_legacy_core_alias_expands_to_granular_families() -> None:
    assert canonicalize_family_selection(["core"]) == [
        FAMILY_MACD_LINES,
        FAMILY_MACD_STATES,
        FAMILY_OHLCV,
    ]


def test_angle_rule_pulls_only_macd_state_family_and_line_dependency() -> None:
    rules = {
        "Long Entry": [[Rule(timeframe="1m", mode="state", field="angle", op="IS", value="GREEN")]],
        "Long Exit": [],
        "Short Entry": [],
        "Short Exit": [],
    }

    spec = compile_stream_requirements(rules, include_plot_defaults=False)
    fams = spec.normalized_families(include_defaults=True)
    fields = spec.normalized_fields(include_defaults=True)

    assert fams == {FAMILY_MACD_LINES, FAMILY_MACD_STATES}
    assert {"macd", "signal", "angle", "ms", "sig_angle", "ppo", "flip_age"}.issubset(fields)
    assert FAMILY_OHLCV not in fams
    assert FAMILY_ADX not in fams


def test_market_family_expands_all_required_dependencies() -> None:
    fams = normalize_required_families([FAMILY_MARKET_AUG], include_defaults=False)
    fields = normalize_required_fields([FAMILY_MARKET_AUG], include_defaults=False)

    assert fams == {
        FAMILY_MARKET_AUG,
        FAMILY_OHLCV,
        FAMILY_MACD_LINES,
        FAMILY_ATR,
        FAMILY_FRAMA,
        FAMILY_VIDYA,
        FAMILY_RANGE_FILTER,
    }
    assert {"market_state", "price", "macd", "atr", "frama", "vidya_line", "range_filter_line"}.issubset(fields)


def test_default_none_requirements_exclude_market_aug() -> None:
    fams = normalize_required_families(None, include_defaults=True)
    fields = normalize_required_fields(None, include_defaults=True)

    assert FAMILY_MARKET_AUG not in fams
    assert "market_state" not in fields
    assert {FAMILY_OHLCV, FAMILY_MACD_LINES, FAMILY_MACD_STATES, FAMILY_ADX, FAMILY_RANGE_FILTER}.issubset(fams)


def test_ema_rule_expands_ema_family_and_pair_metrics() -> None:
    rules = {
        "Long Entry": [[Rule(timeframe="1m", mode="state", field="ema_1_angle_state", op="=", value="UP")]],
        "Long Exit": [],
        "Short Entry": [],
        "Short Exit": [],
    }

    spec = compile_stream_requirements(rules, include_plot_defaults=False)
    fams = spec.normalized_families(include_defaults=True)
    fields = spec.normalized_fields(include_defaults=True)

    assert FAMILY_EMA in fams
    assert {"ema_1_angle_state", "ema_1", "ema_2", "ema_stack", "ema_spread_trend", "ema_stack_pressure", "price"}.issubset(fields)


def test_field_compare_rule_registers_left_and_right_families() -> None:
    rules = {
        "Long Entry": [[
            Rule(timeframe="1m", mode="state", field="close", op=">", value=None, compare_to={"field": "ema_2", "bars_ago": 20}),
            Rule(timeframe="1m", mode="state", field="close", op=">", value=None, compare_to="high[1]"),
        ]],
        "Long Exit": [],
        "Short Entry": [],
        "Short Exit": [],
    }

    spec = compile_stream_requirements(rules, include_plot_defaults=False)
    fams = spec.normalized_families(include_defaults=True)
    fields = spec.normalized_fields(include_defaults=True)

    assert FAMILY_OHLCV in fams
    assert FAMILY_EMA in fams
    assert {"close", "price", "high", "ema_1", "ema_2"}.issubset(fields)


def test_stoch_rsi_rule_expands_stoch_family_and_fields() -> None:
    rules = {
        "Long Entry": [[Rule(timeframe="1m", mode="state", field="stoch_rsi_kd", op="=", value="GREEN")]],
        "Long Exit": [],
        "Short Entry": [],
        "Short Exit": [],
    }

    spec = compile_stream_requirements(rules, include_plot_defaults=False)
    fams = spec.normalized_families(include_defaults=True)
    fields = spec.normalized_fields(include_defaults=True)

    assert FAMILY_STOCH_RSI in fams
    assert {"stoch_rsi_k", "stoch_rsi_d", "stoch_rsi_kd"}.issubset(fields)


def test_rsi_rule_expands_rsi_family_and_fields() -> None:
    rules = {
        "Long Entry": [[Rule(timeframe="1m", mode="state", field="rsi_state", op="=", value="GREEN")]],
        "Long Exit": [],
        "Short Entry": [],
        "Short Exit": [],
    }

    spec = compile_stream_requirements(rules, include_plot_defaults=False)
    fams = spec.normalized_families(include_defaults=True)
    fields = spec.normalized_fields(include_defaults=True)

    assert FAMILY_RSI in fams
    assert {"rsi", "rsi_smooth", "rsi_state"}.issubset(fields)


def test_taker_bias_rule_expands_family_and_fields() -> None:
    rules = {
        "Long Entry": [[Rule(timeframe="1m", mode="state", field="taker_bias_state", op="=", value="BUY")]],
        "Long Exit": [],
        "Short Entry": [],
        "Short Exit": [],
    }

    spec = compile_stream_requirements(rules, include_plot_defaults=False)
    fams = spec.normalized_families(include_defaults=True)
    fields = spec.normalized_fields(include_defaults=True)

    assert FAMILY_TAKER_BIAS in fams
    assert {"taker_buy_volume", "taker_sell_volume", "taker_delta_volume", "taker_delta_pct", "taker_buy_share_pct", "taker_trade_count", "taker_bias_state"}.issubset(fields)
