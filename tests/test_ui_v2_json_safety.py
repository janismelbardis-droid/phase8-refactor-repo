from __future__ import annotations

import json
import math
import unittest

from app.services.ui_v2_runtime import _json_safe_value, _safe_round
from app.ui_v2.server import _json_dumps


class UiV2JsonSafetyTests(unittest.TestCase):
    def test_server_json_dumps_replaces_non_finite_numbers(self) -> None:
        payload = {
            "balance": math.nan,
            "net_profit": math.inf,
            "nested": {"max_drawdown_pct": -math.inf},
            "curve": [1.0, math.nan, 2.0],
        }

        decoded = json.loads(_json_dumps(payload))

        self.assertIsNone(decoded["balance"])
        self.assertIsNone(decoded["net_profit"])
        self.assertIsNone(decoded["nested"]["max_drawdown_pct"])
        self.assertEqual(decoded["curve"], [1.0, None, 2.0])

    def test_runtime_json_safe_value_replaces_non_finite_numbers(self) -> None:
        payload = {
            "summary": {
                "ending_balance": math.nan,
                "profit_factor": math.inf,
            },
            "equity_curve_preview": [{"equity": math.nan, "equity_mark": 1000.0}],
        }

        safe = _json_safe_value(payload)

        self.assertIsNone(safe["summary"]["ending_balance"])
        self.assertIsNone(safe["summary"]["profit_factor"])
        self.assertIsNone(safe["equity_curve_preview"][0]["equity"])
        self.assertEqual(safe["equity_curve_preview"][0]["equity_mark"], 1000.0)

    def test_safe_round_returns_none_for_non_finite_numbers(self) -> None:
        self.assertIsNone(_safe_round(math.nan, 6))
        self.assertIsNone(_safe_round(math.inf, 6))
        self.assertEqual(_safe_round(12.345678, 3), 12.346)


if __name__ == "__main__":
    unittest.main()
