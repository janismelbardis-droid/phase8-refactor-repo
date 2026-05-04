from __future__ import annotations

import pathlib
import subprocess
import sys


def main() -> int:
    repo_root = pathlib.Path(__file__).resolve().parents[1]
    cmd = [
        sys.executable,
        '-m',
        'pytest',
        '-q',
        'tests/test_ui_phase6_live_top_lazy_wakeup.py',
        'tests/test_ui_phase5_live_workspace_lazy_load.py',
    ]
    proc = subprocess.run(cmd, cwd=repo_root)
    return int(proc.returncode)


if __name__ == '__main__':
    raise SystemExit(main())
