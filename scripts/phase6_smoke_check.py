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
    ROOT / "app" / "research" / "validation.py",
    ROOT / "tests" / "test_phase6_validation.py",
]

for path in TARGETS:
    py_compile.compile(str(path), doraise=True)
    print(f"OK  {path.relative_to(ROOT)}")

importlib.import_module("tests.common")

suite = unittest.defaultTestLoader.discover(str(ROOT / "tests"), pattern="test_phase6*.py")
result = unittest.TextTestRunner(verbosity=2).run(suite)
if not result.wasSuccessful():
    raise SystemExit(1)
print("Phase 6 runtime checks passed")
