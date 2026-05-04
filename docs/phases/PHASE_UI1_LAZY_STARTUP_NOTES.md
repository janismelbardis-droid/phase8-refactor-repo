# Phase UI-1 — Lazy startup for heavier GUI surfaces

Scope of this phase:
- Keep existing trading logic, indicator math, rule math, bars-ago logic, filters, and sequences unchanged.
- Reduce startup weight by deferring heavy GUI construction until first use.

Changes in this phase:
- Backtest tab is no longer built during initial window construction.
- A lightweight placeholder is shown in the Backtest tab until first open.
- The Backtest workspace is built on first Backtest tab selection or right before a backtest/report action needs it.
- Market State Board popup is no longer created during Live tab build.
- The popup is created lazily on first open.

Behavioral intent:
- No trading or indicator semantics changed.
- UI functionality stays available; it is just instantiated later.
