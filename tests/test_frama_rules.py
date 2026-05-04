import pandas as pd

from app.rules import Rule, RuleEvalContext, eval_rule_generic


def _frama_snap(state: str, bar_ms: int, *, up: bool = False, down: bool = False, mid: bool = False):
    return {
        "1m": {
            "frama_state": state,
            "frama_break_up": up,
            "frama_break_down": down,
            "frama_mid_cross": mid,
            "bar_ms": int(bar_ms),
            "time": pd.Timestamp(bar_ms, unit="ms", tz="UTC"),
            "is_closed": True,
        }
    }


def test_frama_break_up_still_valid_tracks_last_directional_trigger_not_current_state() -> None:
    ctx = RuleEvalContext()
    rule = Rule(
        timeframe="1m",
        mode="event",
        field="frama_break_up",
        op="EVENT",
        value=None,
        bars_ago_mode="WITHIN",
        bars_ago_n=3,
        require_still_valid=True,
    )

    prev = {}

    ctx.step_index = 1
    cur = _frama_snap("SELL", 60_000)
    ctx.observe(cur)
    assert eval_rule_generic(rule, cur, prev, ctx=ctx) is False
    prev = cur

    ctx.step_index = 2
    cur = _frama_snap("BUY", 120_000, up=True)
    ctx.observe(cur)
    assert eval_rule_generic(rule, cur, prev, ctx=ctx) is True
    prev = cur

    ctx.step_index = 3
    cur = _frama_snap("NEUTRAL", 180_000, mid=True)
    ctx.observe(cur)
    assert eval_rule_generic(rule, cur, prev, ctx=ctx) is True
    prev = cur

    ctx.step_index = 4
    cur = _frama_snap("NEUTRAL", 240_000)
    ctx.observe(cur)
    assert eval_rule_generic(rule, cur, prev, ctx=ctx) is True
    prev = cur

    ctx.step_index = 5
    cur = _frama_snap("SELL", 300_000, down=True)
    ctx.observe(cur)
    assert eval_rule_generic(rule, cur, prev, ctx=ctx) is False
