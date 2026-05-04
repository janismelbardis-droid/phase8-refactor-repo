from __future__ import annotations

import queue
from collections import deque

from app.live_engine import LiveEngine
from app.rules import Rule, RuleEvalContext, eval_rule_generic
from app.taker_bias import compute_taker_bias_payload


def test_compute_taker_bias_payload_buy_bias() -> None:
    payload = compute_taker_bias_payload(100.0, 70.0, 12)

    assert payload["taker_buy_volume"] == 70.0
    assert payload["taker_sell_volume"] == 30.0
    assert payload["taker_delta_volume"] == 40.0
    assert payload["taker_delta_pct"] == 40.0
    assert payload["taker_buy_share_pct"] == 70.0
    assert payload["taker_trade_count"] == 12
    assert payload["taker_bias_state"] == "BUY"


def test_compute_taker_bias_payload_neutral_bias() -> None:
    payload = compute_taker_bias_payload(100.0, 52.0, 9)

    assert payload["taker_delta_pct"] == 4.0
    assert payload["taker_bias_state"] == "NEUTRAL"


def test_live_engine_build_taker_bias_payload_prefers_forming_snapshot() -> None:
    engine = LiveEngine(
        queue.Queue(),
        symbol="BTCUSDT",
        timeframes=["1m"],
        live_use_forming=True,
    )

    with engine.lock:
        st = engine.storage["1m"]
        st.forming_volume = 120.0
        st.forming_trade_count = 18
        st.forming_taker_buy_volume = 78.0

    payload = engine._build_taker_bias_payload("1m")

    assert payload["taker_trade_count"] == 18
    assert payload["taker_buy_volume"] == 78.0
    assert payload["taker_sell_volume"] == 42.0
    assert payload["taker_bias_state"] == "BUY"


def test_live_engine_build_taker_bias_payload_falls_back_to_closed_metrics() -> None:
    engine = LiveEngine(
        queue.Queue(),
        symbol="BTCUSDT",
        timeframes=["1m"],
        live_use_forming=False,
    )
    engine._latest_closed_taker_metrics["1m"] = {
        "volume": 50.0,
        "trade_count": 7,
        "taker_buy_volume": 15.0,
    }

    payload = engine._build_taker_bias_payload("1m")

    assert payload["taker_trade_count"] == 7
    assert payload["taker_sell_volume"] == 35.0
    assert payload["taker_delta_pct"] == -40.0
    assert payload["taker_bias_state"] == "SELL"


def test_taker_bias_state_rule_aggregates_last_n_closed_bars() -> None:
    ctx = RuleEvalContext()
    ctx.bar_history_by_tf["1m"] = deque(
        [
            {"bar_ms": 60_000, "taker_buy_volume": 70.0, "taker_sell_volume": 30.0, "taker_trade_count": 11},
            {"bar_ms": 120_000, "taker_buy_volume": 65.0, "taker_sell_volume": 35.0, "taker_trade_count": 10},
            {"bar_ms": 180_000, "taker_buy_volume": 55.0, "taker_sell_volume": 45.0, "taker_trade_count": 9},
        ],
        maxlen=ctx.max_history,
    )
    cur = {"1m": {"bar_ms": 240_000, "is_closed": False, "taker_bias_state": "SELL"}}
    prev = {"1m": {}}

    state_rule = Rule(timeframe="1m", mode="state", field="taker_bias_state", op="=", value="BUY", window_bars=3)
    delta_rule = Rule(timeframe="1m", mode="state", field="taker_delta_pct", op=">", value=20.0, window_bars=3)

    assert eval_rule_generic(state_rule, cur, prev, ctx=ctx) is True
    assert eval_rule_generic(delta_rule, cur, prev, ctx=ctx) is True


def test_taker_bias_rule_waits_when_requested_window_is_not_available() -> None:
    ctx = RuleEvalContext()
    ctx.bar_history_by_tf["1m"] = deque(
        [
            {"bar_ms": 60_000, "taker_buy_volume": 52.0, "taker_sell_volume": 48.0, "taker_trade_count": 7},
            {"bar_ms": 120_000, "taker_buy_volume": 51.0, "taker_sell_volume": 49.0, "taker_trade_count": 6},
            {"bar_ms": 180_000, "taker_buy_volume": 55.0, "taker_sell_volume": 45.0, "taker_trade_count": 8},
        ],
        maxlen=ctx.max_history,
    )
    cur = {"1m": {"bar_ms": 240_000, "is_closed": False}}
    prev = {"1m": {}}

    rule = Rule(timeframe="1m", mode="state", field="taker_delta_pct", op=">", value=5.0, window_bars=5)

    assert eval_rule_generic(rule, cur, prev, ctx=ctx) is None
