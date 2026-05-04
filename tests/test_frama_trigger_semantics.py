from app.indicators_streaming import make_frama_state


def test_frama_directional_trigger_matches_pine_first_label_after_opposite_side() -> None:
    state = make_frama_state()

    assert state._gate_directional_triggers(False, False) == (False, False)
    assert state._gate_directional_triggers(True, False) == (True, False)
    assert state._gate_directional_triggers(True, False) == (False, False)
    assert state._gate_directional_triggers(False, False) == (False, False)
    assert state._gate_directional_triggers(False, True) == (False, True)
    assert state._gate_directional_triggers(False, True) == (False, False)
    assert state._gate_directional_triggers(True, False) == (True, False)
