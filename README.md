# Refactored Trade Inspector

This folder is a behavior-preserving refactor with phased hardening through Phase 8, plus a Phase 0/1 safety pass that freezes current backtest behavior and cleans the repo layout without changing indicator math or execution logic.

## Install dependencies

```bash
pip install -r requirements.txt
```

For tests and development:

```bash
pip install -r requirements-dev.txt
```

## Windows developer setup

From the repo root, bootstrap the local virtual environment once:

```powershell
.\bootstrap_dev.ps1
```

Open a ready-to-use PowerShell session for the repo:

```powershell
. .\dev_shell.ps1
```

Run the default test suite:

```powershell
.\run_tests.ps1
```

Run tests faster across CPU cores:

```powershell
.\run_tests.ps1 -Fast
```

Run tests with coverage:

```powershell
.\run_tests.ps1 -Cov
```

Run the default tests plus the UI syntax check:

```powershell
.\run_checks.ps1
```

The helper scripts always use `.venv\Scripts\python.exe`, so they do not depend on whichever global `python` happens to be first on `PATH`.

## Run

```bash
python run_app.py
```

## Freeze current behavior

```bash
python scripts/generate_phase0_goldens.py
python scripts/phase0_phase1_smoke_check.py
```

The Phase 0 harness locks the current bar-close, replay, and tick-based tester behavior against a deterministic synthetic fixture.

## Clean release bundle

```bash
python scripts/build_release_bundle.py
```

The clean release artifact is written to `dist/phase8_trade_inspector_release.zip` when you build it.

## Repo hygiene helper

```bash
python scripts/clean_repo.py        # dry-run
python scripts/clean_repo.py --apply
```

## Structure

- `run_app.py` — entry point
- `app/` — runtime source modules
- `app/ui/` — extracted UI helpers
- `app/ui/visual_app/` — `VisualApp` mixins (presets, live shell, backtest charts, lazy surfaces; split from `ui_app.py` without changing the public class)
- `app/domain/market_state/` — market-state pipeline modules
- `app/services/live/` — live helper services
- `app/research/` — replay, validation, and support contracts
- `docs/` — architecture, operator, diagnostics, and phased notes
- `scripts/` — release, hygiene, and regression helpers
- `archive/legacy/` — legacy material kept out of clean release bundles

## Notes

- Tick-based backtesting remains available; the Phase 0 harness now freezes that path too.
- Generated caches, backups, release artifacts, and scratch files are intentionally kept out of clean bundles.
- `ripgrep` (`rg`) is recommended for fast search; `dev_shell.ps1` refreshes `PATH` so the normal installed binary is picked up in new shells.
