from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, Sequence

EXCLUDED_NAMES: Sequence[str] = (
    '__pycache__',
    '.pytest_cache',
    '.mypy_cache',
    '.ruff_cache',
    'dist',
)

EXCLUDED_SUFFIXES: Sequence[str] = (
    '.pyc',
    '.pyo',
    '.orig',
    '.bak',
    '.zip',
)

EXCLUDED_TOP_LEVEL: Sequence[str] = (
    'archive',
)

EXCLUDED_FILES: Sequence[str] = (
    '_smoke_popup.py',
)

INCLUDED_TOP_LEVEL: Sequence[str] = (
    'app',
    'scripts',
    'tests',
    'docs',
    'run_app.py',
    'README.md',
    '.gitignore',
    'MANIFEST.txt',
)


@dataclass(frozen=True)
class ReleaseFileDecision:
    relative_path: str
    included: bool
    reason: str


def should_exclude(relative_path: str) -> bool:
    path = Path(relative_path)
    parts = path.parts
    if not parts:
        return False
    if parts[0] in EXCLUDED_TOP_LEVEL:
        return True
    if path.name in EXCLUDED_FILES:
        return True
    if any(part in EXCLUDED_NAMES for part in parts):
        return True
    if path.suffix in EXCLUDED_SUFFIXES:
        return True
    return False


def decide_release_file(relative_path: str) -> ReleaseFileDecision:
    excluded = should_exclude(relative_path)
    if excluded:
        return ReleaseFileDecision(relative_path=relative_path, included=False, reason='excluded by release filter')
    return ReleaseFileDecision(relative_path=relative_path, included=True, reason='included in clean release')


def iter_release_candidates(root: Path) -> Iterable[Path]:
    for entry in root.iterdir():
        if entry.name.startswith('.') and entry.name not in INCLUDED_TOP_LEVEL:
            continue
        if entry.name not in INCLUDED_TOP_LEVEL and entry.name not in EXCLUDED_TOP_LEVEL:
            continue
        yield entry


def iter_release_files(root: Path) -> Iterable[Path]:
    for candidate in iter_release_candidates(root):
        if candidate.is_file():
            if not should_exclude(candidate.relative_to(root).as_posix()):
                yield candidate
            continue
        for path in sorted(candidate.rglob('*')):
            if not path.is_file():
                continue
            rel = path.relative_to(root).as_posix()
            if should_exclude(rel):
                continue
            yield path
