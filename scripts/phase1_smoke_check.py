from __future__ import annotations

import py_compile
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
TARGETS = [
    ROOT / "run_app.py",
    ROOT / "app" / "ui_app.py",
    ROOT / "app" / "live_engine.py",
    ROOT / "app" / "indicators_streaming.py",
    ROOT / "app" / "ui" / "presenters.py",
    ROOT / "app" / "domain" / "market_state" / "thresholds.py",
    ROOT / "app" / "services" / "live" / "health_service.py",
]

for path in TARGETS:
    py_compile.compile(str(path), doraise=True)
    print(f"OK  {path.relative_to(ROOT)}")
