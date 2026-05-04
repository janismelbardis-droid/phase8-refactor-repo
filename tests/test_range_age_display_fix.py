import unittest

from app.ui.presenters import format_range_event_age_cell
from app.ui_app import VisualApp


class RangeAgeDisplayFixTests(unittest.TestCase):
    def _make_app_stub(self):
        app = VisualApp.__new__(VisualApp)
        app._range_event_last_bar_ms = {}
        app._range_event_meta = {}
        return app

    def test_fired_now_forces_zero_even_if_memory_is_stale(self):
        txt, _fg = format_range_event_age_cell(
            {"1m": {"buy": 60_000, "sell": 120_000}},
            "1m",
            300_000,
            "buy",
            True,
        )
        self.assertEqual(txt, "0")

    def test_out_of_order_seed_bars_backfill_history_without_rewinding_live_bar(self):
        app = self._make_app_stub()

        # Live status for the current BUY bar is seen first.
        app._update_range_event_memory("1m", {
            "bar_open_time_ms": 240_000,
            "range_filter_state": "BUY",
            "range_filter_cond_ini": 1,
            "range_filter_buy": True,
        })

        # Delayed seeded CLOSED-bar history arrives afterward.
        app._update_range_event_memory("1m", {
            "bar_open_time_ms": 120_000,
            "range_filter_state": "BUY",
            "range_filter_cond_ini": 1,
            "range_filter_buy": True,
        })
        app._update_range_event_memory("1m", {
            "bar_open_time_ms": 180_000,
            "range_filter_state": "SELL",
            "range_filter_cond_ini": -1,
            "range_filter_sell": True,
        })

        # The current BUY bar must remain the newest BUY event, while late seed bars are still
        # allowed to backfill the most recent historical SELL event for display bootstrap.
        self.assertEqual(app._range_event_last_bar_ms["1m"]["buy"], 240_000)
        self.assertEqual(app._range_event_last_bar_ms["1m"]["sell"], 180_000)

        buy_txt, _ = app._format_range_event_age_cell("1m", 240_000, "buy", True)
        sell_txt, _ = app._format_range_event_age_cell("1m", 240_000, "sell", False)
        self.assertEqual(buy_txt, "0")
        self.assertEqual(sell_txt, "1")

    def test_late_seed_buy_backfills_startup_display_when_current_bar_has_no_new_pulse(self):
        app = self._make_app_stub()

        # Current live status says BUY, but no fresh BUY pulse is active on this bar.
        app._update_range_event_memory("1m", {
            "bar_open_time_ms": 300_000,
            "range_filter_state": "BUY",
            "range_filter_cond_ini": None,
            "range_filter_buy": False,
        })

        # Historical seed bars arrive later and should populate the last BUY event.
        app._update_range_event_memory("1m", {
            "bar_open_time_ms": 180_000,
            "range_filter_state": "BUY",
            "range_filter_cond_ini": 1,
            "range_filter_buy": True,
        })

        self.assertEqual(app._range_event_last_bar_ms["1m"]["buy"], 180_000)
        buy_txt, _ = app._format_range_event_age_cell("1m", 300_000, "buy", False)
        self.assertEqual(buy_txt, "2")

    def test_ordered_regime_inference_still_works_without_raw_pulse(self):
        app = self._make_app_stub()

        app._update_range_event_memory("1m", {
            "bar_open_time_ms": 60_000,
            "range_filter_state": "SELL",
            "range_filter_cond_ini": -1,
        })
        app._update_range_event_memory("1m", {
            "bar_open_time_ms": 120_000,
            "range_filter_state": "BUY",
            "range_filter_cond_ini": 1,
        })

        self.assertEqual(app._range_event_last_bar_ms["1m"]["buy"], 120_000)


if __name__ == "__main__":
    unittest.main()
