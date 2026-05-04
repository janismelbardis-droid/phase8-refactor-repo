# Diagnostics Guide

## Market-state failures
Phase 4 introduced guarded execution and fallback payloads through `app/runtime_safety.py` and the market-state pipeline.

Check:
- `market_state_status`
- `market_state_status_reason`
- order-flow status fields when advisory overlays are present

## Release diagnostics
Use the generated `docs/release_manifest.json` inside the clean release bundle to confirm what was shipped and what was excluded.

## Replay and validation diagnostics
- Replay coverage: `tests/test_phase5_replay.py`
- Validation coverage: `tests/test_phase6_validation.py`
- Contract coverage: `tests/test_phase7_contracts.py`
- Release coverage: `tests/test_phase8_release.py`
