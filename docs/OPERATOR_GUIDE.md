# Operator Guide

## Start the app
```bash
python run_app.py
```

## Smoke checks
```bash
python scripts/phase8_smoke_check.py
```

## Build a clean release bundle
```bash
python scripts/build_release_bundle.py
```
Generated output goes to `dist/phase8_trade_inspector_release.zip`.

## Research helpers
- Phase 6 validation report: `python scripts/generate_phase6_validation_report.py`
- Phase 7 contract report: `python scripts/generate_phase7_contract_report.py`

## Archive material
Historical backups and legacy reference code live under `archive/` and are intentionally excluded from clean release bundles.
