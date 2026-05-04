from __future__ import annotations

from app.ema_settings import (
    coerce_ema_pair,
    ema_required_warmup_bars,
    ema_settings_signature,
    normalize_ema_settings,
    resolve_ema_settings,
)
from app.rsi_settings import (
    coerce_rsi_config,
    normalize_rsi_settings,
    resolve_rsi_settings,
    rsi_required_warmup_bars,
    rsi_settings_signature,
)


def test_ema_settings_normalize_nested_and_plain_shapes_equally() -> None:
    plain = {
        "defaults": {"ema_1_length": 12, "ema_2_length": 48},
        "overrides": {"15m": {"ema_1_length": 21, "ema_2_length": 55}},
    }
    nested = {"ema_indicators": plain}

    assert normalize_ema_settings(plain) == normalize_ema_settings(nested)
    assert resolve_ema_settings(nested, "15m") == {"ema_1_length": 21, "ema_2_length": 55}
    assert resolve_ema_settings(nested, "5m") == {"ema_1_length": 12, "ema_2_length": 48}


def test_rsi_settings_normalize_nested_and_plain_shapes_equally() -> None:
    plain = {
        "defaults": {"length": 21, "smoothing": 5},
        "overrides": {"1h": {"length": 34, "smoothing": 8}},
    }
    nested = {"rsi_indicators": plain}

    assert normalize_rsi_settings(plain) == normalize_rsi_settings(nested)
    assert resolve_rsi_settings(nested, "1h") == {"length": 34, "smoothing": 8}
    assert resolve_rsi_settings(nested, "5m") == {"length": 21, "smoothing": 5}


def test_ema_and_rsi_signatures_follow_same_timeframe_ordering_contract() -> None:
    ema_raw = {
        "defaults": {"ema_1_length": 9, "ema_2_length": 21},
        "overrides": {
            "1h": {"ema_1_length": 55, "ema_2_length": 144},
            "5m": {"ema_1_length": 12, "ema_2_length": 48},
            "15m": {"ema_1_length": 21, "ema_2_length": 55},
        },
    }
    rsi_raw = {
        "defaults": {"length": 14, "smoothing": 3},
        "overrides": {
            "1h": {"length": 34, "smoothing": 8},
            "5m": {"length": 21, "smoothing": 5},
            "15m": {"length": 28, "smoothing": 7},
        },
    }

    ema_sig = ema_settings_signature(ema_raw)
    rsi_sig = rsi_settings_signature(rsi_raw)

    assert [entry[0] for entry in ema_sig] == ["default", "5m", "15m", "1h"]
    assert [entry[0] for entry in rsi_sig] == ["default", "5m", "15m", "1h"]


def test_ema_and_rsi_coercion_and_warmup_stay_stable() -> None:
    assert coerce_ema_pair({"ema_1_length": "0", "ema_2_length": "abc"}) == {"ema_1_length": 1, "ema_2_length": 21}
    assert coerce_rsi_config({"length": "0", "smoothing": "abc"}) == {"length": 1, "smoothing": 3}

    assert ema_required_warmup_bars({"defaults": {"ema_1_length": 9, "ema_2_length": 144}}) == 144
    assert rsi_required_warmup_bars({"defaults": {"length": 21, "smoothing": 5}}) == 30
