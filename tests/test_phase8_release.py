from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path
from zipfile import ZipFile

from app.release_manifest import decide_release_file, should_exclude
from scripts.build_release_bundle import DEFAULT_NAME, build_release_bundle


class ReleaseManifestTests(unittest.TestCase):
    def test_excludes_compiled_and_backup_artifacts(self) -> None:
        self.assertTrue(should_exclude('__pycache__/x.pyc'))
        self.assertTrue(should_exclude('app/ui_app.py.orig'))
        self.assertTrue(should_exclude('app/backtest.py.bak'))
        self.assertTrue(should_exclude('archive/legacy/legacy_monolith.py'))

    def test_keeps_runtime_sources(self) -> None:
        decision = decide_release_file('app/ui_app.py')
        self.assertTrue(decision.included)
        self.assertEqual(decision.reason, 'included in clean release')


class ReleaseBundleBuilderTests(unittest.TestCase):
    def test_build_release_bundle_creates_clean_zip(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            result = build_release_bundle(Path(tmpdir))
            zip_path = Path(result['zip_path'])
            self.assertTrue(zip_path.exists())
            with ZipFile(zip_path) as zf:
                members = zf.namelist()
            self.assertTrue(any(name.endswith('run_app.py') for name in members))
            self.assertFalse(any(name.endswith('.pyc') for name in members))
            self.assertFalse(any(name.endswith('.bak') for name in members))
            self.assertFalse(any(name.endswith('.orig') for name in members))
            self.assertFalse(any('/archive/' in name for name in members))
            self.assertTrue(any(name.endswith('docs/release_manifest.json') for name in members))

    def test_manifest_json_is_written(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            result = build_release_bundle(Path(tmpdir))
            manifest_path = Path(result['manifest_path'])
            data = json.loads(manifest_path.read_text(encoding='utf-8'))
            self.assertEqual(data['bundle_name'], DEFAULT_NAME)
            self.assertGreater(data['included_count'], 0)
            self.assertGreater(data['excluded_count'], 0)


if __name__ == '__main__':
    unittest.main()
