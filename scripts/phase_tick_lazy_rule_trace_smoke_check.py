from __future__ import annotations

import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]

TESTS = [
    "tests/test_perf_tick_lazy_rule_traces.py",
    "tests/test_perf_tick_equity_sampling.py",
    "tests/test_perf_tick_lazy_signal_snapshots.py",
    "tests/test_perf_tick_signal_prep.py",
    "tests/test_backtest_replay.py",
]


def main() -> int:
    cmd = [sys.executable, "-m", "pytest", "-q", *TESTS]
    return subprocess.call(cmd, cwd=ROOT)


if __name__ == "__main__":
    raise SystemExit(main())
