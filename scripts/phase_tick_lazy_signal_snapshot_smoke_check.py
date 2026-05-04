from __future__ import annotations

import subprocess
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
TESTS = [
    "tests/test_perf_tick_signal_prep.py",
    "tests/test_perf_tick_lazy_signal_snapshots.py",
    "tests/test_phase0_freeze.py",
]


def main() -> int:
    cmd = [sys.executable, "-m", "pytest", "-q", *TESTS]
    proc = subprocess.run(cmd, cwd=str(ROOT))
    return int(proc.returncode)


if __name__ == "__main__":
    raise SystemExit(main())
