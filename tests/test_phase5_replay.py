from __future__ import annotations

import unittest

from tests.common import bull_dynamic_sample, load_json

from app.indicators_streaming import compute_market_state_snapshot
from app.research.replay import compare_state_signatures, extract_state_signature, replay_market_state_sequence, replay_matches_expected


class Phase5ReplayTests(unittest.TestCase):
    def test_replay_sequence_matches_saved_golden_signatures(self) -> None:
        golden = load_json("market_state_replay.json")
        ok, mismatches, actual = replay_matches_expected(
            golden["rows"],
            [row["signature"] for row in golden["expected"]],
            compute_fn=compute_market_state_snapshot,
        )
        self.assertTrue(ok, msg="; ".join(f"#{m.index}:{m.key} expected={m.expected!r} actual={m.actual!r}" for m in mismatches))
        self.assertEqual(len(actual), len(golden["expected"]))

    def test_replay_helper_matches_manual_iteration(self) -> None:
        rows = [bull_dynamic_sample(), bull_dynamic_sample()]
        replayed = replay_market_state_sequence(rows, compute_fn=compute_market_state_snapshot)
        manual = []
        prev = None
        for row in rows:
            current = compute_market_state_snapshot(row, prev)
            manual.append(current)
            prev = current
        self.assertEqual(
            [extract_state_signature(row) for row in replayed],
            [extract_state_signature(row) for row in manual],
        )

    def test_compare_state_signatures_reports_key_differences(self) -> None:
        actual = [{"market_state": "BULL_EXPANSION", "market_bias": "LONG"}]
        expected = [{"market_state": "TRANSITION", "market_bias": "LONG"}]
        mismatches = compare_state_signatures(actual, expected, keys=("market_state", "market_bias"))
        self.assertEqual(len(mismatches), 1)
        self.assertEqual(mismatches[0].key, "market_state")
        self.assertEqual(mismatches[0].expected, "TRANSITION")
        self.assertEqual(mismatches[0].actual, "BULL_EXPANSION")


if __name__ == "__main__":
    unittest.main()
