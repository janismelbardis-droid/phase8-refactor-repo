from __future__ import annotations

import unittest

from tests.common import bear_dynamic_sample, bull_dynamic_sample, compression_basic_sample

from app.domain.market_state.pipeline import run_basic_market_state_pipeline
from app.domain.market_state.thresholds import DEFAULT_MARKET_STATE_THRESHOLDS
from app.indicators_streaming import compute_market_state_snapshot


class Phase5FixtureTests(unittest.TestCase):
    def test_basic_compression_fixture_keeps_compression_label(self) -> None:
        state = run_basic_market_state_pipeline(
            compression_basic_sample(),
            None,
            DEFAULT_MARKET_STATE_THRESHOLDS,
            price_action_overlay_builder=lambda *_args, **_kwargs: {},
            pretty_label_builder=lambda label: label,
        )
        self.assertEqual(state["market_state"], "COMPRESSION")
        self.assertEqual(state["market_phase"], "COMPRESSION")
        self.assertEqual(state["market_state_status"], "ok")

    def test_public_dynamic_bull_fixture_keeps_bull_expansion(self) -> None:
        state = compute_market_state_snapshot(bull_dynamic_sample())
        self.assertEqual(state["market_state"], "BULL_EXPANSION")
        self.assertEqual(state["market_bias"], "LONG")
        self.assertEqual(state["market_phase"], "EXPANSION")
        self.assertIn("market_summary_short", state)

    def test_public_dynamic_bear_fixture_keeps_bear_expansion(self) -> None:
        state = compute_market_state_snapshot(bear_dynamic_sample())
        self.assertEqual(state["market_state"], "BEAR_EXPANSION")
        self.assertEqual(state["market_bias"], "SHORT")
        self.assertEqual(state["market_phase"], "EXPANSION")
        self.assertIn("market_texture", state)


if __name__ == "__main__":
    unittest.main()
