# Release Checklist

- Run `python scripts/phase8_smoke_check.py`
- Build `python scripts/build_release_bundle.py`
- Confirm bundle exists in `dist/`
- Confirm bundle excludes `__pycache__`, `.pyc`, `.bak`, `.orig`, and `archive/`
- Confirm `docs/release_manifest.json` is present in the bundle
- Confirm `run_app.py` and `app/` sources are present
- Confirm research scripts and tests are present as intended
