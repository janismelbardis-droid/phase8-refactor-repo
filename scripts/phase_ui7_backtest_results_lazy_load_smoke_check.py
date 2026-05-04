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
        'tests/test_ui_phase7_backtest_results_lazy_load.py',
        'tests/test_ui_phase4_chart_wakeup_cleanup.py',
    ]
    proc = subprocess.run(cmd, cwd=repo_root)
    return int(proc.returncode)


if __name__ == '__main__':
    raise SystemExit(main())
