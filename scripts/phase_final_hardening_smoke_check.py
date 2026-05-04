from __future__ import annotations

import subprocess
import sys
from pathlib import Path

from scripts.generate_manifest import write_manifest


def main() -> int:
    root = Path(__file__).resolve().parents[1]
    manifest = write_manifest(root)
    if not manifest.exists():
        raise SystemExit("manifest generation failed")
    cmd = [
        sys.executable,
        "-m",
        "pytest",
        "-q",
        "tests/test_phase_final_hardening.py",
        "tests/test_perf_tick_lazy_signal_snapshots.py",
    ]
    return subprocess.call(cmd, cwd=str(root))


if __name__ == "__main__":
    raise SystemExit(main())
