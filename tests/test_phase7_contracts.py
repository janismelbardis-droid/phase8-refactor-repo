from __future__ import annotations

import unittest

from tests.common import bull_dynamic_sample, compression_basic_sample, orderflow_sample

from app.indicators_streaming import compute_market_state_snapshot
from app.research.contracts import (
    apply_contract_overlay,
    build_contract_matrix,
    build_replay_capture_batch,
    build_support_contract,
)
from app.services.live.health_service import build_orderflow_quality_overlay
from app.services.live.orderflow_bridge import (
    attach_live_contract_overlay,
    build_live_replay_capture,
    merge_live_advisory_payload,
)


class Phase7ContractTests(unittest.TestCase):
    def _live_row(self):
        row = bull_dynamic_sample()
        row["symbol"] = "BTCUSDT"
        row["timeframe"] = "5m"
        row["ts_ms"] = 2_000
        state = compute_market_state_snapshot(row, None)
        state.update({"symbol": row["symbol"], "timeframe": row["timeframe"], "ts_ms": row["ts_ms"]})
        state.update(build_orderflow_quality_overlay({**state, **orderflow_sample()}, now_ms=2_200, mode="AUTO", dom_good_ms=200, dom_degraded_ms=500, tape_good_ms=200, tape_degraded_ms=500, snapshot_stale_ms=800, tape_enabled=True))
        state["runtime"] = "live"
        return state

    def test_live_contract_marks_orderflow_as_advisory(self) -> None:
        contract = build_support_contract(self._live_row())
        self.assertEqual(contract.runtime, "live")
        self.assertEqual(contract.support_mode, "validated_plus_advisory")
        self.assertIn("market_state", contract.validated_features)
        self.assertIn("of_data_quality", contract.advisory_features)
        self.assertTrue(contract.replay_capture_ready)
        self.assertFalse(contract.research_ready)

    def test_backtest_contract_stays_validated_only(self) -> None:
        row = compute_market_state_snapshot(compression_basic_sample(), None)
        row["runtime"] = "backtest"
        payload = apply_contract_overlay(row)
        self.assertEqual(payload["support_mode"], "validated_only")
        self.assertEqual(payload["support_advisory_features"], [])
        self.assertTrue(payload["support_research_ready"])

    def test_contract_matrix_and_replay_capture(self) -> None:
        live_row = attach_live_contract_overlay(self._live_row())
        backtest_row = apply_contract_overlay({**compute_market_state_snapshot(compression_basic_sample(), None), "runtime": "backtest"})
        matrix = build_contract_matrix([live_row, backtest_row]).as_dict()
        self.assertEqual(matrix["row_count"], 2)
        self.assertEqual(matrix["advisory_rows"], 1)
        self.assertIn("live", matrix["by_runtime"])
        self.assertIn("backtest", matrix["by_runtime"])

        batch = build_replay_capture_batch([live_row])
        self.assertEqual(len(batch), 1)
        self.assertIn("orderflow", batch[0])
        self.assertIn("market", batch[0])
        self.assertEqual(batch[0]["symbol"], "BTCUSDT")

    def test_orderflow_bridge_helpers_attach_overlay_and_capture(self) -> None:
        live_row = self._live_row()
        merged = merge_live_advisory_payload(live_row)
        self.assertEqual(merged["support_mode"], "validated_plus_advisory")
        self.assertEqual(merged["support_bridge_version"], "phase7")
        capture = build_live_replay_capture(merged)
        self.assertEqual(capture["bridge_version"], "phase7")
        self.assertEqual(capture["runtime"], "live")


if __name__ == "__main__":
    unittest.main()
