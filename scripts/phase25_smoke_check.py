from __future__ import annotations

import py_compile
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

TARGETS = [
    ROOT / "app" / "ui_app.py",
    ROOT / "app" / "ui" / "inspectors.py",
    ROOT / "app" / "ui" / "dialogs.py",
]

for path in TARGETS:
    py_compile.compile(str(path), doraise=True)
    print(f"OK  {path.relative_to(ROOT)}")

from app.ui.inspectors import build_market_state_detail_text, build_orderflow_trail_text


class _Engine:
    def get_orderflow_event_history(self, tf: str, limit: int = 80):
        return [{"ts_ms": 1710000000000, "seq": 1, "event_type": "WATCH_LONG", "summary": "ok"}]


class _App:
    latest = {"1m": {"market_state": "BULL_EXPANSION", "market_bias": "LONG", "market_phase": "EXPANSION"}}
    _breakout_alert_last_by_tf = {"1m": {}}
    live_engine = _Engine()

    def _market_state_thresholds_obj(self):
        return None


app = _App()
assert "MARKET SUMMARY" in build_market_state_detail_text(app, "1m")
assert "Trail Size" in build_orderflow_trail_text(app, "1m")
print("Phase 25 UI extraction checks passed")
