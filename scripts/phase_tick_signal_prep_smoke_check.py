from __future__ import annotations

import py_compile
import sys
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

TARGETS = [
    ROOT / "app" / "backtest.py",
    ROOT / "tests" / "test_perf_tick_signal_prep.py",
]

for path in TARGETS:
    py_compile.compile(str(path), doraise=True)
    print(f"OK  {path.relative_to(ROOT)}")

suite = unittest.defaultTestLoader.discover(str(ROOT / "tests"), pattern="test_perf_tick_signal_prep.py")
result = unittest.TextTestRunner(verbosity=2).run(suite)
if not result.wasSuccessful():
    raise SystemExit(1)
print("Tick signal prep runtime checks passed")
