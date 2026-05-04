from __future__ import annotations

import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from app.indicators_streaming import compute_market_state_snapshot
from app.research.validation import build_validation_report
from tests.common import bear_dynamic_sample, bull_dynamic_sample, compression_basic_sample

rows = [
    compression_basic_sample(),
    bull_dynamic_sample(),
    bull_dynamic_sample(),
    bear_dynamic_sample(),
    bear_dynamic_sample(),
    compression_basic_sample(),
]
for index, row in enumerate(rows):
    row["close"] = row.get("close", row.get("price", 100.0)) + (0.5 * index)
    row["price"] = row["close"]
    row["symbol"] = "BTCUSDT"
    row["timeframe"] = "5m"

report = build_validation_report(
    rows,
    compute_fn=compute_market_state_snapshot,
    threshold_sets={
        "default": {},
        "strict": {"direction_long_min": 6, "direction_short_max": -6},
    },
)

out_path = ROOT / "tests" / "golden_samples" / "phase6_validation_report.json"
out_path.write_text(json.dumps(report.as_dict(), indent=2, sort_keys=True), encoding="utf-8")
print(out_path.relative_to(ROOT))
