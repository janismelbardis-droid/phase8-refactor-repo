from __future__ import annotations

import unittest

from tests.common import bear_dynamic_sample, bull_dynamic_sample, compression_basic_sample

from app.domain.market_state.thresholds import coerce_market_state_thresholds
from app.indicators_streaming import compute_market_state_snapshot
from app.research.validation import (
    build_asset_timeframe_breakdown,
    build_baseline_comparison,
    build_persistence_analysis,
    build_state_statistics,
    build_threshold_sensitivity_report,
    build_transition_matrix,
    build_validation_report,
    compute_forward_return_series,
)


class Phase6ValidationTests(unittest.TestCase):
    def _prepared_rows(self):
        rows = []
        samples = [
            (compression_basic_sample(), 100.0),
            (bull_dynamic_sample(), 101.0),
            (bull_dynamic_sample(), 102.0),
            (bear_dynamic_sample(), 100.0),
            (bear_dynamic_sample(), 99.0),
            (compression_basic_sample(), 99.5),
        ]
        prev = None
        for index, (sample, close_value) in enumerate(samples):
            row = dict(sample)
            row["close"] = close_value
            row["price"] = close_value
            row["symbol"] = "BTCUSDT" if index < 4 else "ETHUSDT"
            row["timeframe"] = "5m" if index % 2 == 0 else "15m"
            state = compute_market_state_snapshot(row, prev)
            state.update({"close": close_value, "price": close_value, "symbol": row["symbol"], "timeframe": row["timeframe"]})
            rows.append(state)
            prev = state
        return rows

    def test_forward_return_series_adds_horizon_payloads(self) -> None:
        rows = self._prepared_rows()
        prepared = compute_forward_return_series(rows, horizons=(1, 2))
        self.assertIn("forward_returns", prepared[0])
        self.assertEqual(set(prepared[0]["forward_returns"].keys()), {"1", "2"})
        self.assertIsNone(prepared[-1]["forward_returns"]["1"])

    def test_state_statistics_and_transition_matrix_are_populated(self) -> None:
        rows = self._prepared_rows()
        stats = build_state_statistics(rows, horizons=(1, 3))
        self.assertIn("BULL_EXPANSION", stats)
        self.assertIn("COMPRESSION", stats)
        self.assertIn("1", stats["BULL_EXPANSION"]["horizons"])

        transitions = build_transition_matrix(rows)
        self.assertIn("COMPRESSION", transitions)
        self.assertGreaterEqual(transitions["COMPRESSION"]["count"], 1)

    def test_persistence_and_asset_timeframe_breakdown(self) -> None:
        rows = self._prepared_rows()
        persistence = build_persistence_analysis(rows)
        self.assertIn("BULL_EXPANSION", persistence)
        self.assertGreaterEqual(persistence["BULL_EXPANSION"]["max_run_length"], 1)

        breakdown = build_asset_timeframe_breakdown(rows)
        self.assertIn("BTCUSDT|5m", breakdown)
        self.assertIn("ETHUSDT|15m", breakdown)

    def test_baseline_comparison_includes_market_and_baseline_models(self) -> None:
        rows = self._prepared_rows()
        comparison = build_baseline_comparison(rows, horizons=(1,))
        self.assertIn("1", comparison)
        self.assertIn("market_model", comparison["1"])
        self.assertIn("baseline_model", comparison["1"])

    def test_threshold_sensitivity_runs_with_compute_function(self) -> None:
        rows = []
        for evidence in (1.0, 2.0, 3.0, -1.0, -3.5):
            row = bull_dynamic_sample()
            row["market_direction_evidence"] = evidence
            row["close"] = 100.0 + evidence
            row["price"] = row["close"]
            rows.append(row)

        report = build_threshold_sensitivity_report(
            rows,
            compute_fn=compute_market_state_snapshot,
            threshold_sets={
                "default": {},
                "strict": {"direction_long_min": 6, "direction_short_max": -6},
            },
        )
        self.assertIn("default", report)
        self.assertIn("strict", report)
        self.assertIn("state_distribution", report["default"])

    def test_validation_report_assembles_all_sections(self) -> None:
        rows = self._prepared_rows()
        report = build_validation_report(rows, horizons=(1, 3))
        payload = report.as_dict()
        self.assertEqual(payload["row_count"], len(rows))
        self.assertIn("state_statistics", payload)
        self.assertIn("transition_matrix", payload)
        self.assertIn("baseline_comparison", payload)


if __name__ == "__main__":
    unittest.main()
