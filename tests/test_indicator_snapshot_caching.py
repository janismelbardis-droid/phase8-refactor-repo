from __future__ import annotations

from unittest.mock import patch

import numpy as np
import pandas as pd

from app.indicators_streaming import simulate_multitf_indicators_fast


def _sample_1m(periods: int = 10) -> pd.DataFrame:
    open_times = pd.date_range("2026-01-01 00:00:00", periods=periods, freq="1min", tz="UTC")
    close_times = open_times + pd.Timedelta(seconds=59)
    base = np.linspace(100.0, 110.0, periods)
    return pd.DataFrame(
        {
            "open_time": open_times,
            "close_time": close_times,
            "open": base,
            "high": base + 0.5,
            "low": base - 0.5,
            "close": base + 0.2,
            "volume": np.linspace(10.0, 20.0, periods),
        }
    )


def test_adx_snapshots_are_reused_between_commit_boundaries() -> None:
    df = _sample_1m(periods=10)
    snapshot_calls = {"count": 0}

    import app.indicators_streaming as indicators_mod

    original_factory = indicators_mod.make_adx_state

    class _CountingAdxState:
        def __init__(self, inner):
            self._inner = inner

        def update(self, *args, **kwargs):
            return self._inner.update(*args, **kwargs)

        def preview(self, *args, **kwargs):
            return self._inner.preview(*args, **kwargs)

        def snapshot(self):
            snapshot_calls["count"] += 1
            return self._inner.snapshot()

    def _factory(*args, **kwargs):
        return _CountingAdxState(original_factory(*args, **kwargs))

    with patch("app.indicators_streaming.make_adx_state", side_effect=_factory):
        streams = simulate_multitf_indicators_fast(
            df,
            ["1m", "5m"],
            required_fields={"adx", "di_plus", "di_minus", "di_spread", "adx_angle"},
        )

    assert snapshot_calls["count"] == 14
    assert set(streams.keys()) == {"1m", "5m"}


def test_range_filter_snapshots_are_reused_between_commit_boundaries() -> None:
    df = _sample_1m(periods=10)
    snapshot_calls = {"count": 0}

    import app.indicators_streaming as indicators_mod

    original_factory = indicators_mod.make_range_filter_state

    class _CountingRangeFilterState:
        def __init__(self, inner):
            self._inner = inner

        def update(self, *args, **kwargs):
            return self._inner.update(*args, **kwargs)

        def snapshot(self):
            snapshot_calls["count"] += 1
            return self._inner.snapshot()

    def _factory(*args, **kwargs):
        return _CountingRangeFilterState(original_factory(*args, **kwargs))

    with patch("app.indicators_streaming.make_range_filter_state", side_effect=_factory):
        streams = simulate_multitf_indicators_fast(
            df,
            ["1m", "5m"],
            required_fields={
                "range_filter_line",
                "range_filter_upper",
                "range_filter_lower",
                "range_filter_smooth_range",
                "range_filter_state",
                "range_filter_phase",
                "range_filter_buy",
                "range_filter_sell",
                "range_filter_long_cond",
                "range_filter_short_cond",
                "range_filter_cond_ini",
                "range_filter_upward_count",
                "range_filter_downward_count",
            },
        )

    assert snapshot_calls["count"] == 14
    assert set(streams.keys()) == {"1m", "5m"}
