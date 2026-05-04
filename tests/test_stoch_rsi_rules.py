from __future__ import annotations

import numpy as np
import pandas as pd

from app.indicators_streaming import simulate_multitf_indicators_fast
from app.rules import Rule, eval_rule_generic, snapshot_from_stream_row


def _sample_1m(periods: int = 128) -> pd.DataFrame:
    idx = pd.date_range("2026-01-01T00:00:00Z", periods=periods, freq="1min", tz="UTC")
    base = 100.0 + np.linspace(0.0, 8.0, periods) + np.sin(np.linspace(0.0, 10.0 * np.pi, periods)) * 2.5
    return pd.DataFrame(
        {
            "open_time": [ts - pd.Timedelta(minutes=1) + pd.Timedelta(milliseconds=1) for ts in idx],
            "close_time": list(idx),
            "open": base - 0.15,
            "high": base + 0.55,
            "low": base - 0.65,
            "close": base + 0.1,
            "volume": np.linspace(10.0, 25.0, periods),
        }
    )


def test_snapshot_from_stream_row_supports_stoch_rsi_fields() -> None:
    row = {
        "stoch_rsi_k": 82.5,
        "stoch_rsi_d": 76.25,
        "stoch_rsi_kd": "GREEN",
    }

    snap = snapshot_from_stream_row(row, required_fields={"stoch_rsi_k", "stoch_rsi_d", "stoch_rsi_kd"})

    assert snap["stoch_rsi_k"] == 82.5
    assert snap["stoch_rsi_d"] == 76.25
    assert snap["stoch_rsi_kd"] == "GREEN"


def test_snapshot_from_stream_row_supports_rsi_fields() -> None:
    row = {
        "rsi": 61.5,
        "rsi_smooth": 58.0,
        "rsi_state": "GREEN",
    }

    snap = snapshot_from_stream_row(row, required_fields={"rsi", "rsi_smooth", "rsi_state"})

    assert snap["rsi"] == 61.5
    assert snap["rsi_smooth"] == 58.0
    assert snap["rsi_state"] == "GREEN"


def test_eval_rule_generic_supports_stoch_rsi_numeric_and_dot_logic() -> None:
    cur = {"1m": {"stoch_rsi_k": 84.0, "stoch_rsi_d": 77.0, "stoch_rsi_kd": "GREEN"}}
    prev = {"1m": {"stoch_rsi_k": 72.0, "stoch_rsi_d": 78.0, "stoch_rsi_kd": "RED"}}

    assert eval_rule_generic(Rule(timeframe="1m", mode="state", field="stoch_rsi_k", op=">=", value=80), cur, prev) is True
    assert eval_rule_generic(Rule(timeframe="1m", mode="state", field="stoch_rsi_d", op=">=", value=70), cur, prev) is True
    assert eval_rule_generic(Rule(timeframe="1m", mode="state", field="stoch_rsi_kd", op="=", value="GREEN"), cur, prev) is True
    assert eval_rule_generic(Rule(timeframe="1m", mode="event", field="stoch_rsi_kd", op="TURNED", value="GREEN"), cur, prev) is True
    assert eval_rule_generic(Rule(timeframe="1m", mode="event", field="stoch_rsi_kd", op="FLIPPED", value=None), cur, prev) is True


def test_eval_rule_generic_supports_rsi_numeric_and_dot_logic() -> None:
    cur = {"1m": {"rsi": 64.0, "rsi_smooth": 58.0, "rsi_state": "GREEN"}}
    prev = {"1m": {"rsi": 54.0, "rsi_smooth": 59.0, "rsi_state": "RED"}}

    assert eval_rule_generic(Rule(timeframe="1m", mode="state", field="rsi", op=">=", value=60), cur, prev) is True
    assert eval_rule_generic(Rule(timeframe="1m", mode="state", field="rsi_smooth", op=">=", value=55), cur, prev) is True
    assert eval_rule_generic(Rule(timeframe="1m", mode="state", field="rsi_state", op="=", value="GREEN"), cur, prev) is True
    assert eval_rule_generic(Rule(timeframe="1m", mode="event", field="rsi_state", op="TURNED", value="GREEN"), cur, prev) is True
    assert eval_rule_generic(Rule(timeframe="1m", mode="event", field="rsi_state", op="FLIPPED", value=None), cur, prev) is True


def test_simulate_multitf_indicators_fast_materializes_stoch_rsi_fields() -> None:
    df_1m = _sample_1m()

    streams = simulate_multitf_indicators_fast(
        df_1m,
        ["1m", "5m"],
        required_fields={"stoch_rsi_k", "stoch_rsi_d", "stoch_rsi_kd"},
    )

    frame = streams["1m"]
    assert "stoch_rsi_k" in frame.columns
    assert "stoch_rsi_d" in frame.columns
    assert "stoch_rsi_kd" in frame.columns
    assert frame["stoch_rsi_k"].notna().any()
    assert frame["stoch_rsi_d"].notna().any()
    assert set(frame["stoch_rsi_kd"].dropna().unique()).issubset({"GREEN", "RED"})


def test_simulate_multitf_indicators_fast_materializes_rsi_fields() -> None:
    df_1m = _sample_1m()

    streams = simulate_multitf_indicators_fast(
        df_1m,
        ["1m", "5m"],
        required_fields={"rsi", "rsi_smooth", "rsi_state"},
    )

    frame = streams["1m"]
    assert "rsi" in frame.columns
    assert "rsi_smooth" in frame.columns
    assert "rsi_state" in frame.columns
    assert frame["rsi"].notna().any()
    assert frame["rsi_smooth"].notna().any()
    assert set(frame["rsi_state"].dropna().unique()).issubset({"GREEN", "RED"})
