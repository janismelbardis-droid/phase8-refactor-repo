from __future__ import annotations

from app.rules import Rule, RuleEvalContext, eval_rule_generic, snapshot_from_stream_row


def test_eval_rule_generic_supports_ema_pair_states() -> None:
    cur = {
        "1m": {
            "ema_stack": "BUY",
            "ema_1_angle_state": "UP",
            "ema_2_angle_state": "DOWN",
            "ema_spread": 2.5,
            "ema_spread_pct": 1.25,
            "ema_spread_delta": 0.4,
            "ema_spread_pct_delta": 0.2,
            "ema_spread_trend": "EXPANDING",
            "ema_stack_pressure": "BULL_ACCELERATING",
        }
    }
    prev = {"1m": {}}

    assert eval_rule_generic(Rule(timeframe="1m", mode="state", field="ema_stack", op="=", value="BUY"), cur, prev) is True
    assert eval_rule_generic(Rule(timeframe="1m", mode="state", field="ema_1_angle_state", op="=", value="UP"), cur, prev) is True
    assert eval_rule_generic(Rule(timeframe="1m", mode="state", field="ema_2_angle_state", op="=", value="DOWN"), cur, prev) is True
    assert eval_rule_generic(Rule(timeframe="1m", mode="state", field="ema_spread", op=">", value=0), cur, prev) is True
    assert eval_rule_generic(Rule(timeframe="1m", mode="state", field="ema_spread_pct", op=">", value=1.0), cur, prev) is True
    assert eval_rule_generic(Rule(timeframe="1m", mode="state", field="ema_spread_delta", op=">", value=0), cur, prev) is True
    assert eval_rule_generic(Rule(timeframe="1m", mode="state", field="ema_spread_pct_delta", op=">", value=0), cur, prev) is True
    assert eval_rule_generic(Rule(timeframe="1m", mode="state", field="ema_spread_trend", op="=", value="EXPANDING"), cur, prev) is True
    assert eval_rule_generic(Rule(timeframe="1m", mode="state", field="ema_stack_pressure", op="=", value="BULL_ACCELERATING"), cur, prev) is True


def test_eval_rule_generic_supports_ema_cross_events() -> None:
    cur = {"1m": {"ema_cross_up": True, "ema_cross_down": False, "ema_stack": "BUY"}}
    prev = {"1m": {"ema_cross_up": False, "ema_cross_down": False, "ema_stack": "SELL"}}

    assert eval_rule_generic(Rule(timeframe="1m", mode="event", field="ema_cross_up", op="EVENT", value=None), cur, prev) is True
    assert eval_rule_generic(Rule(timeframe="1m", mode="event", field="ema_cross_down", op="EVENT", value=None), cur, prev) is False


def test_eval_rule_generic_supports_numeric_field_to_field_comparisons() -> None:
    cur = {
        "1m": {
            "close": 105.0,
            "price": 105.0,
            "high": 106.0,
            "low": 99.0,
            "ema_1": 100.0,
            "ema_2": 95.0,
        }
    }
    prev = {"1m": {"high": 104.0, "low": 98.0, "close": 102.0, "price": 102.0}}

    assert eval_rule_generic(Rule("1m", "state", "close", ">", None, compare_to={"field": "ema_2"}), cur, prev) is True
    assert eval_rule_generic(Rule("1m", "state", "ema_1", ">", None, compare_to={"field": "ema_2"}), cur, prev) is True
    assert eval_rule_generic(Rule("1m", "state", "low", "<=", None, compare_to={"field": "ema_1"}), cur, prev) is True
    assert eval_rule_generic(Rule("1m", "state", "close", ">", None, compare_to={"field": "high", "bars_ago": 1}), cur, prev) is True
    assert eval_rule_generic(Rule("1m", "state", "close", ">", None, compare_to={"field": "high[1]"}), cur, prev) is True
    assert eval_rule_generic(Rule("1m", "state", "close", "<", None, compare_to={"field": "low", "bars_ago": 1}), cur, prev) is False


def test_eval_rule_generic_supports_compare_to_bars_ago_from_context() -> None:
    ctx = RuleEvalContext()
    cur = {}
    for i in range(21):
        ctx.step_index = i
        cur = {
            "1m": {
                "ema_2": 100.0 + i,
                "bar_ms": i * 60_000,
                "is_closed": True,
            }
        }
        ctx.observe(cur)

    rule = Rule("1m", "state", "ema_2", ">", None, compare_to={"field": "ema_2", "bars_ago": 20})
    assert eval_rule_generic(rule, cur, {"1m": {}}, ctx=ctx) is True


def test_snapshot_from_stream_row_exposes_ohlcv_and_price_close_aliases() -> None:
    row = {
        "price": 101.5,
        "open": 100.0,
        "high": 103.0,
        "low": 99.0,
        "volume": 42.0,
    }

    snap = snapshot_from_stream_row(row, required_fields={"price", "close", "open", "high", "low", "volume"})

    assert snap["price"] == 101.5
    assert snap["close"] == 101.5
    assert snap["open"] == 100.0
    assert snap["high"] == 103.0
    assert snap["low"] == 99.0
    assert snap["volume"] == 42.0
