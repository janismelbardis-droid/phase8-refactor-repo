from __future__ import annotations

import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from app.indicators_streaming import compute_market_state_snapshot
from app.research.contracts import apply_contract_overlay, build_contract_matrix, build_replay_capture_batch
from app.services.live.health_service import build_orderflow_quality_overlay
from tests.common import bull_dynamic_sample, compression_basic_sample, orderflow_sample

live_row = compute_market_state_snapshot(bull_dynamic_sample(), None)
live_row.update(orderflow_sample())
live_row.update(
    build_orderflow_quality_overlay(
        live_row,
        now_ms=1200,
        mode="AUTO",
        dom_good_ms=200,
        dom_degraded_ms=500,
        tape_good_ms=200,
        tape_degraded_ms=500,
        snapshot_stale_ms=800,
        tape_enabled=True,
    )
)
live_row.update({"runtime": "live", "symbol": "BTCUSDT", "timeframe": "5m", "ts_ms": 1200})
live_row = apply_contract_overlay(live_row)

backtest_row = compute_market_state_snapshot(compression_basic_sample(), None)
backtest_row.update({"runtime": "backtest", "symbol": "BTCUSDT", "timeframe": "5m", "ts_ms": 1300})
backtest_row = apply_contract_overlay(backtest_row)

matrix = build_contract_matrix([live_row, backtest_row]).as_dict()
captures = build_replay_capture_batch([live_row])

(ROOT / "tests" / "golden_samples" / "phase7_contract_report.json").write_text(
    json.dumps(matrix, indent=2, sort_keys=True), encoding="utf-8"
)
(ROOT / "tests" / "golden_samples" / "phase7_replay_capture.json").write_text(
    json.dumps(captures, indent=2, sort_keys=True), encoding="utf-8"
)
print("tests/golden_samples/phase7_contract_report.json")
print("tests/golden_samples/phase7_replay_capture.json")
