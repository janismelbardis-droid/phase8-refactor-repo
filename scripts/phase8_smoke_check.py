from __future__ import annotations

import importlib
import json
import py_compile
import shutil
import sys
import tempfile
import unittest
from pathlib import Path
from zipfile import ZipFile

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

TARGETS = [
    ROOT / 'app' / 'release_manifest.py',
    ROOT / 'scripts' / 'build_release_bundle.py',
    ROOT / 'tests' / 'test_phase8_release.py',
]

for path in TARGETS:
    py_compile.compile(str(path), doraise=True)
    print(f'OK  {path.relative_to(ROOT)}')

importlib.import_module('tests.common')

suite = unittest.defaultTestLoader.discover(str(ROOT / 'tests'), pattern='test_phase8*.py')
result = unittest.TextTestRunner(verbosity=2).run(suite)
if not result.wasSuccessful():
    raise SystemExit(1)

from scripts.build_release_bundle import DEFAULT_NAME, build_release_bundle

with tempfile.TemporaryDirectory() as tmpdir:
    output_dir = Path(tmpdir)
    bundle = build_release_bundle(output_dir)
    zip_path = Path(bundle['zip_path'])
    assert zip_path.exists(), zip_path
    with ZipFile(zip_path) as zf:
        members = zf.namelist()
    blocked = [m for m in members if m.endswith('.pyc') or m.endswith('.bak') or m.endswith('.orig') or '__pycache__/' in m or '/archive/' in m]
    if blocked:
        raise SystemExit(f'Blocked artifacts found in release bundle: {blocked}')
    manifest_path = Path(bundle['manifest_path'])
    manifest = json.loads(manifest_path.read_text(encoding='utf-8'))
    print(json.dumps({'included_count': manifest['included_count'], 'excluded_count': manifest['excluded_count'], 'bundle_name': DEFAULT_NAME}, indent=2))

print('Phase 8 runtime checks passed')
