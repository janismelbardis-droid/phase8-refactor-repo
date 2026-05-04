from __future__ import annotations

import py_compile
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

TARGETS = [
    ROOT / "app" / "backtest.py",
    ROOT / "app" / "backtest_planning.py",
    ROOT / "app" / "backtest_frames.py",
    ROOT / "tests" / "test_phase3_backtest_split.py",
]

for path in TARGETS:
    py_compile.compile(str(path), doraise=True)
    print(f"OK  {path.relative_to(ROOT)}")

from app import backtest as bt
from app.rules import Rule
import pandas as pd

idx = pd.date_range("2026-01-01T00:00:00Z", periods=3, freq="1min", tz="UTC")
stream = pd.DataFrame({"price": [100.0, 101.0, 102.0], "ms": ["RED", "GREEN", "GREEN"]}, index=idx)
prev_idx = {"1m": bt._prev_source_row_index_for_df(stream, "1m")}
arr = bt._compile_tab_signal_array(
    "Long Entry",
    {"Long Entry": [[Rule(timeframe="1m", mode="state", field="ms", op="==", value="GREEN")]], "Long Exit": [], "Short Entry": [], "Short Exit": []},
    {"Long Entry": "AND", "Long Exit": "AND", "Short Entry": "AND", "Short Exit": "AND"},
    {"Long Entry": ["AND"], "Long Exit": [], "Short Entry": [], "Short Exit": []},
    {"1m": stream},
    prev_idx,
    len(stream),
)
assert arr.tolist() == [False, True, True]
assert bt._compile_tab_signal_array.__module__ == "app.backtest_planning"
assert bt.make_streams_htf_closed_only.__module__ == "app.backtest_frames"
print("Phase 3 backtest split smoke check passed")
