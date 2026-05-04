Phase 19 - Live DOM/L2 order-book filter

Added:
- Binance futures partial depth stream (`@depth20@100ms`) alongside aggTrade.
- Live L2 analytics in `app/orderflow_engine.py`.
- DOM-aware breakout gating in `app/live_engine.py`.
- Expanded order-flow detail popup in `app/ui_app.py`.

Kept intact:
- indicator math for Range Filter / FRAMA / VIDYA
- existing market-state and breakout-state logic
- all earlier functionality outside the live microstructure layer

New live order-flow fields include:
- `of_dom_state`, `of_dom_state_pretty`, `of_dom_bias`
- `of_dom_pressure_pct`, `of_dom_pressure_accel`
- `of_dom_top5_imbalance_pct`, `of_dom_top10_imbalance_pct`
- `of_dom_bid_stack_ratio`, `of_dom_ask_stack_ratio`
- `of_dom_bid_pull_ratio`, `of_dom_ask_pull_ratio`
- `of_dom_breakout_watch_long/short`
- `of_dom_breakout_confirm_long/short`
- `of_dom_updates_short/long`
- `of_dom_best_bid`, `of_dom_best_ask`, `of_dom_mid`

Live breakout filter behavior:
- watch states accept tape OR DOM directional support
- confirm states prefer tape + DOM agreement
- if DOM is unavailable, tape-only confirmation is still allowed
