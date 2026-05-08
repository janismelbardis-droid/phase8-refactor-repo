# Trade Inspector

Behavior-preserving refactor of the Trade Inspector / phase 8 backtest workspace.

This repository now carries:
- runtime code
- backtest and research tools
- smoke checks and tests
- cloud/remote execution helpers

This repository does **not** carry the full local market cache. Large candle, indicator, and prepared-dataset stores stay outside git.

## What is here

- `app/` - runtime modules, backtest engine, indicator pipeline, UI
- `tools/` - headless research and backtest entrypoints
- `scripts/` - smoke checks, maintenance, release helpers
- `docs/` - architecture, diagnostics, phased notes, cloud-run notes
- `tests/` - regression and smoke coverage

## Local quick start

Install runtime dependencies:

```bash
pip install -r requirements.txt
```

Install test and tooling dependencies:

```bash
pip install -r requirements-dev.txt
```

On Windows you can bootstrap the local virtualenv with:

```powershell
.\bootstrap_dev.ps1
```

## Main entrypoints

Desktop UI:

```bash
python run_app.py
```

Headless smoke:

```bash
python tools/remote_job.py smoke
```

Headless saved-preset backtest:

```bash
python tools/remote_job.py saved-preset --preset "My Preset" --start "2026-03-01 00:00:00" --end "2026-04-01 00:00:00"
```

Build a slim backtest cache profile with only the indicator families you actually need:

```bash
python tools/remote_job.py slim-profile --start "2026-04-20 00:00:00+00:00" --end "2026-04-20 04:00:00+00:00"
```

Build a compact AI research pack for remote analysis:

```bash
python tools/remote_job.py build-pack --output-dir dist/ai_pack
```

Submit that compact pack to OpenAI Code Interpreter:

```bash
python tools/remote_job.py submit-pack --zip dist/ai_pack/ai_research_pack.zip
```

## Remote and cloud use

The repo is prepared for two remote modes:

1. GitHub Actions for smoke checks and lightweight remote automation.
2. Compact AI research packs for OpenAI-hosted analysis instead of pushing a 17+ GB local cache into git.

The main doc for this is:

[docs/CLOUD_EXECUTION.md](docs/CLOUD_EXECUTION.md)

## Cache and data policy

Large runtime data stays outside git:

- `data_cache/`
- `runs/`
- `.venv/`

Use `TRADE_INSPECTOR_CACHE_DIR` when you want the code to work against an external cache mount or a copied cloud cache.

Example:

```bash
export TRADE_INSPECTOR_CACHE_DIR=/mnt/trade-cache
python tools/remote_job.py saved-preset --preset "My Preset"
```

Windows PowerShell:

```powershell
$env:TRADE_INSPECTOR_CACHE_DIR = "D:\trade-cache"
python .\tools\remote_job.py saved-preset --preset "My Preset"
```

## Useful commands

Run default pytest suite:

```bash
python -m pytest
```

Run the full technical backtest certification ladder:

```bash
python tools/real_backtest_system_certification_suite.py
```

Run the prefix-causality invariance check only:

```bash
python tools/real_backtest_causality_suite.py
```

Run the hardened smoke path:

```bash
python scripts/phase0_phase1_smoke_check.py
python scripts/phase_final_hardening_smoke_check.py
```

Build a clean release bundle:

```bash
python scripts/build_release_bundle.py
```

Audit and organize the local cache library:

```bash
python scripts/manual/organize_cache_library.py --cache-dir data_cache
```

## Backtest status

The backtest engine now has a separate technical certification ladder. This is different from strategy or casebook exactness.

Technical certification currently covers:
- independent `Range Filter` oracle parity
- backtest parity on fixed control windows
- trade-by-trade parity on fixed control windows
- prefix-causality invariance
- replay-context safety
- runtime regression
- live/downloader contract tests

If `python tools/real_backtest_system_certification_suite.py` passes, the engine itself is considered technically healthy.

Casebook or preset exactness is a higher semantic layer. If a strategy family still does not hit a research case exactly, that is treated as a strategy-semantics problem unless the technical certification ladder fails.

More detail:

[docs/BACKTEST_CERTIFICATION.md](docs/BACKTEST_CERTIFICATION.md)
