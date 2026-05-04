"""Remove extracted line ranges from ui_app.py (1-based inclusive). Run from repo root."""

from __future__ import annotations

from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
UI_APP = ROOT / "app" / "ui_app.py"

# 1-based inclusive; must delete from bottom of file upward or indices shift incorrectly.
RANGES = [
    (5386, 8814),  # backtest
    (2375, 3103),  # presets
    (588, 1003),   # live shell
]


def main() -> None:
    lines = UI_APP.read_text(encoding="utf-8").splitlines(keepends=True)
    for lo, hi in sorted(RANGES, key=lambda t: t[0], reverse=True):
        del lines[lo - 1 : hi]
    UI_APP.write_text("".join(lines), encoding="utf-8")
    print("Updated ui_app.py")


if __name__ == "__main__":
    main()
