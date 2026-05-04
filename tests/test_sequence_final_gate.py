import pandas as pd

from app.rules import Rule, RuleEvalContext, eval_group_generic, eval_group_generic_trace


def _snap(ms, angle, ppo, bar_ms):
    return {
        "1m": {
            "ms": ms,
            "angle": angle,
            "ppo": ppo,
            "bar_ms": int(bar_ms),
            "time": pd.Timestamp(bar_ms, unit="ms", tz="UTC"),
        }
    }


def test_sequence_final_gate_checks_only_on_completion_bar_and_resets_when_gate_fails():
    ctx = RuleEvalContext()
    rules = [
        Rule("1m", "state", "ms", "=", "GREEN"),
        Rule("1m", "state", "angle", "=", "GREEN"),
        Rule("1m", "state", "ppo", "=", "GREEN", is_final_gate=True),
    ]

    ctx.step_index = 1
    out1 = eval_group_generic(rules, "SEQUENCE", _snap("GREEN", "RED", "RED", 60_000), {}, ctx=ctx, group_key=("Long Entry", 0))
    assert out1 is False
    assert ctx.sequence_progress_by_group[("Long Entry", 0)] == 1

    ctx.step_index = 2
    out2 = eval_group_generic(rules, "SEQUENCE", _snap("GREEN", "GREEN", "RED", 120_000), {}, ctx=ctx, group_key=("Long Entry", 0))
    assert out2 is False
    assert ctx.sequence_progress_by_group[("Long Entry", 0)] == 0

    ctx.step_index = 3
    out3 = eval_group_generic(rules, "SEQUENCE", _snap("GREEN", "GREEN", "GREEN", 180_000), {}, ctx=ctx, group_key=("Long Entry", 0))
    assert out3 is False
    assert ctx.sequence_progress_by_group[("Long Entry", 0)] == 1


def test_sequence_trace_reports_only_real_steps_and_marks_final_gate():
    ctx = RuleEvalContext()
    rules = [
        Rule("1m", "state", "ms", "=", "GREEN"),
        Rule("1m", "state", "angle", "=", "GREEN"),
        Rule("1m", "state", "ppo", "=", "GREEN", is_final_gate=True),
    ]
    ctx.step_index = 1
    trace = eval_group_generic_trace(rules, "SEQUENCE", _snap("GREEN", "RED", "RED", 60_000), {}, ctx=ctx, group_key=("Long Entry", 0))
    assert trace["sequence"]["total_steps"] == 2
    assert trace["sequence_gate_count"] == 1
    assert any(bool(r.get("is_final_gate")) for r in trace["rules"])
