from __future__ import annotations

import unittest
import warnings
from unittest.mock import patch

import numpy as np
import pandas as pd

from app.domain.market_state.pipeline import finalize_market_state_payload
from app.indicators_streaming import _augment_stream_with_market_state


class MarketStateContractFixTests(unittest.TestCase):
    def test_finalize_market_state_payload_preserves_nan_for_macd_gap_norm(self) -> None:
        payload = finalize_market_state_payload({"market_macd_gap_norm": np.nan})
        self.assertTrue(pd.isna(payload["market_macd_gap_norm"]))

    def test_augment_stream_tolerates_none_numeric_market_state_values(self) -> None:
        df = pd.DataFrame(
            [
                {
                    "open": 100.0,
                    "high": 101.0,
                    "low": 99.0,
                    "close": 100.5,
                    "price": 100.5,
                    "volume": 10.0,
                    "atr": 1.0,
                    "vidya_line": 100.2,
                    "vidya_raw": 100.2,
                    "frama": 100.1,
                    "frama_upper": 101.0,
                    "frama_lower": 99.0,
                    "vidya_upper": 101.0,
                    "vidya_lower": 99.0,
                    "range_filter_line": 100.0,
                    "range_filter_upper": 101.0,
                    "range_filter_lower": 99.0,
                }
            ]
        )
        mocked_state = {
            "market_state": "TRANSITION",
            "market_bias": "NEUTRAL",
            "market_phase": "TRANSITION",
            "market_confidence": 0,
            "market_state_age": 1,
            "market_state_pretty": "Transition",
            "market_direction_score": 0,
            "market_energy_score": 0,
            "market_alignment_bonus": 0,
            "market_macd_gap_norm": None,
            "market_texture": "TEST",
            "market_summary_short": "test",
            "market_summary_long": "test",
            "market_breakout_state": "NONE",
            "market_breakout_bias": "NEUTRAL",
            "market_breakout_pretty": "-",
            "market_breakout_score": 0,
            "market_breakout_trigger_ready": False,
            "market_breakout_note": "test",
        }
        with patch("app.indicators_streaming.compute_market_state_snapshot", return_value=mocked_state):
            out = _augment_stream_with_market_state(df)
        self.assertEqual(len(out), 1)
        self.assertTrue(pd.isna(out.iloc[0]["market_macd_gap_norm"]))

    def test_augment_stream_batched_column_attach_avoids_performance_warning(self) -> None:
        rows = []
        for i in range(4):
            base = 100.0 + i
            rows.append(
                {
                    "open": base,
                    "high": base + 1.0,
                    "low": base - 1.0,
                    "close": base + 0.25,
                    "price": base + 0.25,
                    "volume": 10.0 + i,
                    "atr": 1.0,
                    "vidya_line": base + 0.1,
                    "vidya_raw": base + 0.1,
                    "frama": base,
                    "frama_upper": base + 1.0,
                    "frama_lower": base - 1.0,
                    "vidya_upper": base + 1.0,
                    "vidya_lower": base - 1.0,
                    "range_filter_line": base,
                    "range_filter_upper": base + 1.0,
                    "range_filter_lower": base - 1.0,
                }
            )
        df = pd.DataFrame(rows)
        mocked_state = {
            "market_state": "TRANSITION",
            "market_bias": "NEUTRAL",
            "market_phase": "TRANSITION",
            "market_confidence": 0.0,
            "market_state_age": 1.0,
            "market_state_pretty": "Transition",
            "market_direction_score": 0.0,
            "market_energy_score": 0.0,
            "market_alignment_bonus": 0.0,
            "market_macd_gap_norm": np.nan,
            "market_texture": "TEST",
            "market_summary_short": "test",
            "market_summary_long": "test",
            "market_breakout_state": "NONE",
            "market_breakout_bias": "NEUTRAL",
            "market_breakout_pretty": "-",
            "market_breakout_score": 0.0,
            "market_breakout_trigger_ready": False,
            "market_breakout_note": "test",
        }
        with patch("app.indicators_streaming.compute_market_state_snapshot", return_value=mocked_state):
            with warnings.catch_warnings(record=True) as caught:
                warnings.simplefilter("always", pd.errors.PerformanceWarning)
                out = _augment_stream_with_market_state(df)
        perf_warnings = [w for w in caught if issubclass(w.category, pd.errors.PerformanceWarning)]
        self.assertEqual(perf_warnings, [])
        self.assertEqual(len(out), len(df))

    def test_augment_stream_passes_only_market_state_input_subset(self) -> None:
        df = pd.DataFrame(
            [
                {
                    "open": 100.0,
                    "high": 101.0,
                    "low": 99.0,
                    "close": 100.5,
                    "price": 100.5,
                    "volume": 10.0,
                    "atr": 1.0,
                    "macd": 0.2,
                    "signal": 0.1,
                    "ms": "GREEN",
                    "angle": "GREEN",
                    "atr_angle": "GREEN",
                    "vidya_state": "BUY",
                    "frama_state": "NEUTRAL",
                    "range_filter_state": "BUY",
                    "range_filter_phase": "BUY",
                    "vidya_delta_pct": 11.0,
                    "vidya_angle_deg": 15.0,
                    "vidya_angle_deg_norm": 9.0,
                    "vidya_line": 100.2,
                    "vidya_raw": 100.2,
                    "frama": 100.1,
                    "frama_upper": 101.0,
                    "frama_lower": 99.0,
                    "vidya_upper": 101.0,
                    "vidya_lower": 99.0,
                    "range_filter_line": 100.0,
                    "range_filter_upper": 101.0,
                    "range_filter_lower": 99.0,
                    "totally_irrelevant_debug_blob": "should_not_be_forwarded",
                }
            ]
        )
        captured = []

        def _fake_snapshot(snapshot, prev_state=None, thresholds=None):
            captured.append(dict(snapshot))
            return {
                "market_state": "TRANSITION",
                "market_bias": "NEUTRAL",
                "market_phase": "TRANSITION",
                "market_confidence": 0.0,
                "market_state_age": 1.0,
                "market_state_pretty": "Transition",
                "market_direction_score": 0.0,
                "market_energy_score": 0.0,
                "market_alignment_bonus": 0.0,
                "market_macd_gap_norm": np.nan,
                "market_texture": "TEST",
                "market_summary_short": "test",
                "market_summary_long": "test",
                "market_breakout_state": "NONE",
                "market_breakout_bias": "NEUTRAL",
                "market_breakout_pretty": "-",
                "market_breakout_score": 0.0,
                "market_breakout_trigger_ready": False,
                "market_breakout_note": "test",
            }

        with patch("app.indicators_streaming.compute_market_state_snapshot", side_effect=_fake_snapshot):
            out = _augment_stream_with_market_state(df)

        self.assertEqual(len(out), 1)
        self.assertEqual(len(captured), 1)
        snapshot = captured[0]
        self.assertNotIn("totally_irrelevant_debug_blob", snapshot)
        self.assertIn("close", snapshot)
        self.assertIn("market_compression_score", snapshot)
        self.assertIn("market_breakout_confirmation", snapshot)



if __name__ == "__main__":
    unittest.main()
