import pandas as pd

from app.prepared_dataset import slice_streams_window


def _frame(start="2024-01-01 00:00:00+00:00", periods=3, freq="1min"):
    idx = pd.date_range(start=start, periods=periods, freq=freq, tz="UTC")
    return pd.DataFrame({"x": range(periods)}, index=idx)


def test_slice_streams_window_keeps_1m_when_requested_timeframes_omit_it():
    streams = {"1m": _frame(freq="1min"), "5m": _frame(freq="5min")}
    out = slice_streams_window(streams, pd.Timestamp("2024-01-01", tz="UTC"), pd.Timestamp("2024-01-01 00:10:00", tz="UTC"), timeframes=("5m",))
    assert "1m" in out
    assert "5m" in out
    assert not out["1m"].empty
