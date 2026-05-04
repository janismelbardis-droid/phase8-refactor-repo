from __future__ import annotations

import unittest

from tests.common import ROOT

from app.domain.market_state.classification import normalize_market_bias, normalize_market_label, normalize_market_phase
from app.domain.market_state.features import build_market_state_inputs
from app.domain.market_state.thresholds import coerce_market_state_thresholds
from app.runtime_safety import annotate_payload, safe_execute


class Phase5UnitTests(unittest.TestCase):
    def test_threshold_coercion_clamps_and_defaults(self) -> None:
        thresholds = coerce_market_state_thresholds(
            {
                "vidya_delta_support_pct": -10,
                "vidya_delta_weak_pct": 999,
                "compression_direction_abs_max": -5,
                "confidence_base": -1,
                "expansion_confirmation_min": 1.5,
                "expansion_pullback_floor": 999,
                "expansion_exhaustion_cap": -50,
                "expansion_max_weakness_score": 999,
            }
        )
        self.assertEqual(thresholds.vidya_delta_support_pct, 0.0)
        self.assertEqual(thresholds.vidya_delta_weak_pct, 0.0)
        self.assertEqual(thresholds.compression_direction_abs_max, 0)
        self.assertEqual(thresholds.confidence_base, 0)
        self.assertEqual(thresholds.expansion_confirmation_min, 0.9)
        self.assertEqual(thresholds.expansion_pullback_floor, 80.0)
        self.assertEqual(thresholds.expansion_exhaustion_cap, 20.0)
        self.assertEqual(thresholds.expansion_max_weakness_score, 4.0)

    def test_normalizers_preserve_known_values_and_fallback_unknown(self) -> None:
        self.assertEqual(normalize_market_label("bull_expansion"), "BULL_EXPANSION")
        self.assertEqual(normalize_market_label("bad"), "TRANSITION")
        self.assertEqual(normalize_market_bias("short"), "SHORT")
        self.assertEqual(normalize_market_bias("sideways"), "NEUTRAL")
        self.assertEqual(normalize_market_phase("compression"), "COMPRESSION")
        self.assertEqual(normalize_market_phase(None), "TRANSITION")

    def test_typed_inputs_keep_raw_and_previous_payloads(self) -> None:
        inputs = build_market_state_inputs({"close": "101.5", "market_trend_score": "63"}, prev_state={"market_state": "BULL_EXPANSION"})
        self.assertEqual(inputs.bar.close, 101.5)
        self.assertEqual(inputs.features.trend_score, 63.0)
        self.assertEqual(inputs.previous_state["market_state"], "BULL_EXPANSION")

    def test_safe_execute_and_annotation_expose_degraded_status(self) -> None:
        payload, report = safe_execute(
            lambda: (_ for _ in ()).throw(ValueError("bad overlay")),
            default={"fallback": True},
            module="tests",
            function="unit",
            degraded_feature="overlay",
            log_exceptions=False,
        )
        self.assertEqual(payload, {"fallback": True})
        self.assertEqual(report.status, "degraded")
        annotated = annotate_payload({}, "market_state", report)
        self.assertEqual(annotated["market_state_status"], "partial")
        self.assertEqual(annotated["market_state_degraded_feature"], "overlay")


if __name__ == "__main__":
    unittest.main()
