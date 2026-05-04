from __future__ import annotations

import importlib
import json
import sys
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

sys.dont_write_bytecode = True

TARGETS = [
    ROOT / 'app' / 'release_manifest.py',
    ROOT / 'scripts' / 'clean_repo.py',
    ROOT / 'scripts' / 'generate_phase0_goldens.py',
    ROOT / 'tests' / 'test_phase0_freeze.py',
    ROOT / 'tests' / 'test_phase1_repo_hygiene.py',
]

for path in TARGETS:
    source = path.read_text(encoding='utf-8')
    compile(source, str(path), 'exec')
    print(f'OK  {path.relative_to(ROOT)}')

importlib.import_module('tests.common')

suite = unittest.defaultTestLoader.loadTestsFromNames([
    'tests.test_phase0_freeze',
    'tests.test_phase1_repo_hygiene',
])
result = unittest.TextTestRunner(verbosity=2).run(suite)
if not result.wasSuccessful():
    raise SystemExit(1)

from scripts.clean_repo import build_cleanup_plan

plan = build_cleanup_plan(ROOT)
print(json.dumps(plan, indent=2))
print('Phase 0 / Phase 1 runtime checks passed')
