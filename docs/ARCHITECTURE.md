# Architecture

## Runtime surfaces
- `run_app.py`: application entry point
- `app/ui_app.py`: compatibility-facing UI shell
- `app/live_engine.py`: compatibility-facing live runtime shell
- `app/backtest.py`: historical execution surface
- `app/indicators_streaming.py`: indicator and market-state compatibility layer

## Internal packages
- `app/ui/`: extracted UI presentation and tab helpers
- `app/ui/visual_app/`: `VisualApp` mixins — prepared-dataset cache, scroll/screen layout, lazy backtest results table, **live tab shell** (lazy workspace/top builds, main notebook tab), **presets** (JSON + entry filters + diagnostics dialogs), **backtest charts/reports/inspector** (large Matplotlib + trade UI path); `app/ui_app.py` composes them with `tk.Tk` and keeps the public `VisualApp` entrypoint
- `app/domain/market_state/`: typed market-state schema, scoring, classification, summaries, pipeline
- `app/services/live/`: live service helpers and advisory order-flow bridge
- `app/research/`: replay, validation, and support-contract reporting

## Compatibility rule
Public runtime shells remain in place while extracted modules carry increasing internal ownership.

## Release rule
Shipping bundles exclude compiled caches, backup files, original-file snapshots, and archive material.
