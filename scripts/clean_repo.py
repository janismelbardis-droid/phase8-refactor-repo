from __future__ import annotations

import argparse
import json
import shutil
import sys
from pathlib import Path
from typing import Dict, List

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

REMOVE_GLOBS = (
    '**/__pycache__',
    '.pytest_cache',
    'dist',
    'archive/backups',
)
MOVE_NOTE_GLOBS = (
    '*_NOTES.md',
    '*_PATCH_NOTES.md',
    'AUDIT_FIXES.md',
    'MARKET_EVAL_UPGRADE_NOTES.md',
)
SCRATCH_FILES = (
    '_smoke_popup.py',
)


def _unique_paths(paths: List[Path]) -> List[Path]:
    seen = set()
    ordered: List[Path] = []
    for path in sorted(paths):
        key = path.resolve() if path.exists() else path
        if key in seen:
            continue
        seen.add(key)
        ordered.append(path)
    return ordered


def build_cleanup_plan(root: Path = ROOT) -> Dict[str, List[str]]:
    phase_notes_dir = root / 'docs' / 'phases'
    manual_dir = root / 'scripts' / 'manual'

    remove_targets: List[Path] = []
    for pattern in REMOVE_GLOBS:
        remove_targets.extend(path for path in root.glob(pattern) if path.exists())

    move_targets: List[Path] = []
    for pattern in MOVE_NOTE_GLOBS:
        move_targets.extend(
            path for path in root.glob(pattern)
            if path.is_file() and path.parent == root
        )

    scratch_targets = [root / name for name in SCRATCH_FILES if (root / name).exists()]

    return {
        'remove': [path.relative_to(root).as_posix() for path in _unique_paths(remove_targets)],
        'move_notes_to_docs_phases': [path.relative_to(root).as_posix() for path in _unique_paths(move_targets)],
        'move_scratch_to_scripts_manual': [path.relative_to(root).as_posix() for path in _unique_paths(scratch_targets)],
        'docs_phases_dir': [phase_notes_dir.relative_to(root).as_posix()],
        'scripts_manual_dir': [manual_dir.relative_to(root).as_posix()],
    }


def apply_cleanup_plan(root: Path = ROOT, *, dry_run: bool = True) -> Dict[str, List[str]]:
    plan = build_cleanup_plan(root)
    if dry_run:
        return plan

    phase_notes_dir = root / 'docs' / 'phases'
    manual_dir = root / 'scripts' / 'manual'
    phase_notes_dir.mkdir(parents=True, exist_ok=True)
    manual_dir.mkdir(parents=True, exist_ok=True)

    for rel in plan['move_notes_to_docs_phases']:
        source = root / rel
        target = phase_notes_dir / source.name
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.move(str(source), str(target))

    for rel in plan['move_scratch_to_scripts_manual']:
        source = root / rel
        target = manual_dir / source.name.lstrip('_')
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.move(str(source), str(target))

    for rel in plan['remove']:
        target = root / rel
        if target.is_dir():
            shutil.rmtree(target, ignore_errors=True)
        elif target.exists():
            target.unlink()

    return build_cleanup_plan(root)


def main() -> int:
    parser = argparse.ArgumentParser(description='Phase 1 repo hygiene helper.')
    parser.add_argument('--apply', action='store_true', help='Apply the cleanup plan in place.')
    args = parser.parse_args()
    report = apply_cleanup_plan(dry_run=not args.apply)
    print(json.dumps(report, indent=2))
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
