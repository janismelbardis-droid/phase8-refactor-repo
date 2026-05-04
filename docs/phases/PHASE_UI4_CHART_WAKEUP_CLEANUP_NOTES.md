# Phase UI-4 — Chart wake-up cleanup

This phase keeps all backtest, indicator, and rule logic unchanged.

What changed:
- Main Backtest charts are no longer fully materialized the moment the Backtest tab workspace is built.
- The **Price + Trades + MACD** chart surface is built on first chart draw or first chart-tab access.
- The **Equity Curve** chart surface is built on first redraw or first chart-tab access.
- Chart placeholders keep the workspace responsive while the user has not asked for those surfaces yet.
- Initial chart wake-up now uses `draw_idle()` instead of eager `draw()` for the main embedded charts.

What stayed the same:
- backtest math
- indicator math
- bars-ago behavior
- filters
- sequences
- replay flow
- tick tester availability

Why this is safe:
- only Tk/Matplotlib widget materialization timing changed
- no trading logic or indicator formulas changed
- charts still render through the same draw functions once requested
