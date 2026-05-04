from __future__ import annotations

import json
import shutil
import sys
from pathlib import Path
from zipfile import ZIP_DEFLATED, ZipFile

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from app.release_manifest import decide_release_file, iter_release_candidates

DEFAULT_NAME = 'phase8_trade_inspector_release'


def _iter_files(root: Path):
    for top in iter_release_candidates(root):
        if top.is_file():
            yield top
            continue
        for path in top.rglob('*'):
            if path.is_file():
                yield path


def build_release_bundle(output_dir: Path) -> dict:
    output_dir.mkdir(parents=True, exist_ok=True)
    staging_dir = output_dir / DEFAULT_NAME
    if staging_dir.exists():
        shutil.rmtree(staging_dir)
    staging_dir.mkdir(parents=True)

    included = []
    excluded = []

    for path in _iter_files(ROOT):
        rel = path.relative_to(ROOT)
        decision = decide_release_file(rel.as_posix())
        if not decision.included:
            excluded.append(decision.relative_path)
            continue
        target = staging_dir / rel
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(path, target)
        included.append(decision.relative_path)

    manifest = {
        'bundle_name': DEFAULT_NAME,
        'included_count': len(included),
        'excluded_count': len(excluded),
        'included_files': sorted(included),
        'excluded_files': sorted(excluded),
    }
    (staging_dir / 'docs' / 'release_manifest.json').parent.mkdir(parents=True, exist_ok=True)
    (staging_dir / 'docs' / 'release_manifest.json').write_text(json.dumps(manifest, indent=2), encoding='utf-8')

    zip_path = output_dir / f'{DEFAULT_NAME}.zip'
    if zip_path.exists():
        zip_path.unlink()
    with ZipFile(zip_path, 'w', compression=ZIP_DEFLATED) as zf:
        for file_path in sorted(staging_dir.rglob('*')):
            if file_path.is_file():
                zf.write(file_path, file_path.relative_to(output_dir).as_posix())

    return {
        'staging_dir': str(staging_dir),
        'zip_path': str(zip_path),
        'manifest_path': str(staging_dir / 'docs' / 'release_manifest.json'),
        'included_count': len(included),
        'excluded_count': len(excluded),
    }


def main() -> int:
    output_dir = ROOT / 'dist'
    result = build_release_bundle(output_dir)
    print(json.dumps(result, indent=2))
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
