from __future__ import annotations

import shutil
import tempfile
import unittest
from pathlib import Path

from app.release_manifest import should_exclude
from scripts.clean_repo import apply_cleanup_plan, build_cleanup_plan


class Phase1RepoHygieneTests(unittest.TestCase):
    def test_release_filter_excludes_generated_build_and_scratch_artifacts(self) -> None:
        self.assertTrue(should_exclude('__pycache__/x.pyc'))
        self.assertTrue(should_exclude('.pytest_cache/README.md'))
        self.assertTrue(should_exclude('dist/phase8_trade_inspector_release.zip'))
        self.assertTrue(should_exclude('archive/backups/backtest.py.bak'))
        self.assertTrue(should_exclude('_smoke_popup.py'))

    def test_cleanup_plan_reports_no_remaining_repo_mess(self) -> None:
        plan = build_cleanup_plan()
        self.assertEqual(plan['move_notes_to_docs_phases'], [])
        self.assertEqual(plan['move_scratch_to_scripts_manual'], [])
        self.assertFalse(any(item.startswith('dist') for item in plan['remove']))
        self.assertFalse(any(item.startswith('archive/backups') for item in plan['remove']))
        self.assertTrue(all(item.endswith('__pycache__') or item == '.pytest_cache' for item in plan['remove']))

    def test_cleanup_plan_can_fix_sample_repo_tree(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            (root / 'docs').mkdir()
            (root / 'scripts').mkdir()
            (root / 'archive' / 'backups').mkdir(parents=True)
            (root / 'dist').mkdir()
            (root / 'app' / '__pycache__').mkdir(parents=True)
            (root / '.pytest_cache').mkdir()
            (root / 'PHASE99_NOTES.md').write_text('notes', encoding='utf-8')
            (root / '_smoke_popup.py').write_text('print(1)', encoding='utf-8')
            (root / 'archive' / 'backups' / 'backtest.py.bak').write_text('bak', encoding='utf-8')
            (root / 'dist' / 'phase.zip').write_text('zip', encoding='utf-8')
            (root / 'app' / '__pycache__' / 'x.pyc').write_bytes(b'pyc')
            (root / '.pytest_cache' / 'README.md').write_text('cache', encoding='utf-8')

            planned = build_cleanup_plan(root)
            self.assertIn('PHASE99_NOTES.md', planned['move_notes_to_docs_phases'])
            self.assertIn('_smoke_popup.py', planned['move_scratch_to_scripts_manual'])
            self.assertIn('archive/backups', planned['remove'])
            self.assertIn('dist', planned['remove'])

            after = apply_cleanup_plan(root, dry_run=False)
            self.assertEqual(after['remove'], [])
            self.assertEqual(after['move_notes_to_docs_phases'], [])
            self.assertEqual(after['move_scratch_to_scripts_manual'], [])
            self.assertTrue((root / 'docs' / 'phases' / 'PHASE99_NOTES.md').exists())
            self.assertTrue((root / 'scripts' / 'manual' / 'smoke_popup.py').exists())
            self.assertFalse((root / 'dist').exists())
            self.assertFalse((root / 'archive' / 'backups').exists())
            self.assertFalse((root / '.pytest_cache').exists())
            self.assertFalse((root / 'app' / '__pycache__').exists())


if __name__ == '__main__':
    unittest.main()
