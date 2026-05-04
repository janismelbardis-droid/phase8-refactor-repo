from __future__ import annotations

import unittest

from tests.common import load_json, orderflow_sample

from app.indicators_streaming import compute_market_state_snapshot
from app.research.replay import extract_state_signature
from app.services.live.health_service import build_orderflow_quality_overlay


class Phase5RegressionTests(unittest.TestCase):
    def test_orderflow_overlay_regression_signature(self) -> None:
        golden = load_json("orderflow_overlay.json")
        actual = build_orderflow_quality_overlay(
            orderflow_sample(),
            now_ms=golden["params"]["now_ms"],
            mode=golden["params"]["mode"],
            dom_good_ms=golden["params"]["dom_good_ms"],
            dom_degraded_ms=golden["params"]["dom_degraded_ms"],
            tape_good_ms=golden["params"]["tape_good_ms"],
            tape_degraded_ms=golden["params"]["tape_degraded_ms"],
            snapshot_stale_ms=golden["params"]["snapshot_stale_ms"],
            tape_enabled=golden["params"]["tape_enabled"],
        )
        for key, expected in golden["signature"].items():
            self.assertEqual(actual.get(key), expected, msg=f"mismatch for {key}")

    def test_public_state_signature_roundtrip(self) -> None:
        golden = load_json("market_state_replay.json")
        first_row = golden["rows"][0]
        state = compute_market_state_snapshot(first_row)
        self.assertEqual(extract_state_signature(state), golden["expected"][0]["signature"])


if __name__ == "__main__":
    unittest.main()
