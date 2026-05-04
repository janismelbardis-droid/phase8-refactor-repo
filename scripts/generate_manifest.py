from __future__ import annotations

import hashlib
import sys
from pathlib import Path
from typing import List

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from app.release_manifest import iter_release_files


def _manifest_lines(root: Path) -> List[str]:
    files = [path.relative_to(root).as_posix() for path in iter_release_files(root) if path.name != "MANIFEST.txt"]
    lines = [
        "# Clean release manifest",
        f"files={len(files) + 1}",
        "# path\tsize_bytes\tsha256_12",
    ]
    for rel in files:
        path = root / rel
        digest = hashlib.sha256(path.read_bytes()).hexdigest()[:12]
        lines.append(f"{rel}\t{path.stat().st_size}\t{digest}")
    return lines


def write_manifest(root: Path | None = None) -> Path:
    repo_root = Path(root or ROOT).resolve()
    manifest_path = repo_root / "MANIFEST.txt"
    lines = _manifest_lines(repo_root)
    manifest_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    digest = hashlib.sha256(manifest_path.read_bytes()).hexdigest()[:12]
    lines.append(f"MANIFEST.txt\t{manifest_path.stat().st_size}\t{digest}")
    manifest_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return manifest_path


if __name__ == "__main__":
    path = write_manifest()
    print(path)
