# Phase 25 — UI inspector extraction, parity-preserving

Scope of this pass:
- Extract market-state detail rendering helpers from `app/ui_app.py` into `app/ui/inspectors.py`.
- Extract popup text/panel helpers from `app/ui_app.py` into `app/ui/dialogs.py`.
- Keep the public `VisualApp` methods in place as compatibility wrappers.

Guardrails:
- No backtest math changes.
- No indicator math changes.
- No rule-evaluation changes.
- Bars-ago, filters, sequence logic, and tick backtest path remain untouched.

Validation added:
- `tests/test_phase2_ui_extraction.py`
  - inspector text rendering
  - order-flow trail formatting
  - popup text widget helpers
