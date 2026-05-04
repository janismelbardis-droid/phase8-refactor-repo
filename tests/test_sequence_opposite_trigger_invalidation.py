import pandas as pd

from app.rules import Rule, RuleEvalContext, eval_group_generic, format_tab_trace


def _snap(ms, angle, bar_ms, **extra):
    row = {
        "ms": ms,
        "angle": angle,
        "bar_ms": int(bar_ms),
        "time": pd.Timestamp(bar_ms, unit="ms", tz="UTC"),
    }
    row.update(extra)
    return {"1m": row}


def test_long_sequence_is_invalidated_when_newer_short_trigger_arrives() -> None:
    ctx = RuleEvalContext()
    long_rules = [
        Rule("1m", "event", "ms", "TURNED", "GREEN"),
        Rule("1m", "state", "angle", "=", "GREEN"),
    ]
    short_rules = [
        Rule("1m", "event", "ms", "TURNED", "RED"),
        Rule("1m", "state", "angle", "=", "RED"),
    ]

    long_group = ("Long Entry", 0)
    short_group = ("Short Entry", 0)

    ctx.step_index = 1
    out1 = eval_group_generic(
        long_rules,
        "SEQUENCE",
        _snap("GREEN", "RED", 60_000),
        _snap("RED", "RED", 0),
        ctx=ctx,
        group_key=long_group,
    )
    assert out1 is False
    assert ctx.sequence_progress_by_group[long_group] == 1
    assert ctx.latest_directional_trigger_bar_ms_by_side["LONG"] == 60_000

    ctx.step_index = 2
    out2 = eval_group_generic(
        short_rules,
        "SEQUENCE",
        _snap("RED", "RED", 120_000),
        _snap("GREEN", "RED", 60_000),
        ctx=ctx,
        group_key=short_group,
    )
    assert out2 is False
    assert ctx.sequence_progress_by_group[short_group] == 1
    assert ctx.latest_directional_trigger_bar_ms_by_side["SHORT"] == 120_000

    ctx.step_index = 3
    out3 = eval_group_generic(
        long_rules,
        "SEQUENCE",
        _snap("RED", "GREEN", 180_000),
        _snap("RED", "RED", 120_000),
        ctx=ctx,
        group_key=long_group,
    )
    assert out3 is False
    assert ctx.sequence_progress_by_group.get(long_group, 0) == 0
    assert long_group not in ctx.sequence_start_bar_ms_by_group
    seq = ctx.sequence_snapshot(long_group, total_steps=2)
    assert isinstance(seq.get("last_reset_meta"), dict)
    assert seq["last_reset_meta"]["reason"] == "opposite_trigger"
    assert seq["last_reset_meta"]["opposite_side"] == "SHORT"


def test_trace_mentions_opposite_trigger_sequence_reset() -> None:
    trace_text = format_tab_trace(
        {
            "tab": "Long Entry",
            "result": False,
            "groups_join": "AND",
            "tf_snapshots": {},
            "groups": [
                {
                    "join_mode": "SEQUENCE",
                    "result": False,
                    "sequence_gate_count": 0,
                    "sequence": {
                        "completed_steps": 0,
                        "total_steps": 2,
                        "next_step_index": 0,
                        "last_reset_meta": {
                            "reason": "opposite_trigger",
                            "entry_side": "LONG",
                            "opposite_side": "SHORT",
                        },
                    },
                    "rules": [],
                }
            ],
        }
    )
    assert "Sequence reset: invalidated by opposite SHORT trigger while building LONG setup" in trace_text


def test_sequence_canceler_rule_resets_sequence_and_wins_over_pending_step() -> None:
    ctx = RuleEvalContext()
    rules = [
        Rule("1m", "event", "ms", "TURNED", "GREEN"),
        Rule("1m", "state", "angle", "=", "GREEN"),
        Rule("1m", "state", "ppo", "=", "RED", is_sequence_canceler=True),
    ]
    group_key = ("Long Entry", 0)

    ctx.step_index = 1
    out1 = eval_group_generic(
        rules,
        "SEQUENCE",
        _snap("GREEN", "RED", 60_000, ppo="GREEN"),
        _snap("RED", "RED", 0, ppo="GREEN"),
        ctx=ctx,
        group_key=group_key,
    )
    assert out1 is False
    assert ctx.sequence_progress_by_group[group_key] == 1

    ctx.step_index = 2
    out2 = eval_group_generic(
        rules,
        "SEQUENCE",
        _snap("GREEN", "GREEN", 120_000, ppo="RED"),
        _snap("GREEN", "RED", 60_000, ppo="GREEN"),
        ctx=ctx,
        group_key=group_key,
    )
    assert out2 is False
    assert ctx.sequence_progress_by_group.get(group_key, 0) == 0
    seq = ctx.sequence_snapshot(group_key, total_steps=2)
    assert isinstance(seq.get("last_reset_meta"), dict)
    assert seq["last_reset_meta"]["reason"] == "rule_canceler"
    assert "SEQ CANCEL" in str(seq["last_reset_meta"]["rule_label"])


def test_long_sequence_still_completes_when_no_newer_short_trigger_exists() -> None:
    ctx = RuleEvalContext()
    long_rules = [
        Rule("1m", "event", "ms", "TURNED", "GREEN"),
        Rule("1m", "state", "angle", "=", "GREEN"),
    ]
    long_group = ("Long Entry", 0)

    ctx.step_index = 1
    out1 = eval_group_generic(
        long_rules,
        "SEQUENCE",
        _snap("GREEN", "RED", 60_000),
        _snap("RED", "RED", 0),
        ctx=ctx,
        group_key=long_group,
    )
    assert out1 is False
    assert ctx.sequence_progress_by_group[long_group] == 1

    ctx.step_index = 2
    out2 = eval_group_generic(
        long_rules,
        "SEQUENCE",
        _snap("GREEN", "GREEN", 120_000),
        _snap("GREEN", "RED", 60_000),
        ctx=ctx,
        group_key=long_group,
    )
    assert out2 is True
    assert ctx.sequence_progress_by_group.get(long_group, 0) == 0
