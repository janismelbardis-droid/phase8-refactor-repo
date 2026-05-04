from __future__ import annotations

import importlib
import py_compile
import sys
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

TARGETS = [
    ROOT / "app" / "research" / "__init__.py",
    ROOT / "app" / "research" / "replay.py",
    ROOT / "tests" / "common.py",
    ROOT / "tests" / "test_phase5_unit.py",
    ROOT / "tests" / "test_phase5_fixtures.py",
    ROOT / "tests" / "test_phase5_replay.py",
    ROOT / "tests" / "test_phase5_regression.py",
    ROOT / "scripts" / "generate_phase5_goldens.py",
]

for path in TARGETS:
    py_compile.compile(str(path), doraise=True)
    print(f"OK  {path.relative_to(ROOT)}")

importlib.import_module("tests.common")

suite = unittest.defaultTestLoader.discover(str(ROOT / "tests"), pattern="test_phase5*.py")
result = unittest.TextTestRunner(verbosity=2).run(suite)
if not result.wasSuccessful():
    raise SystemExit(1)
print("Phase 5 runtime checks passed")
