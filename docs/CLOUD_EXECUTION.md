# Cloud Execution

This repository is now structured so we can run it in a cleaner remote workflow instead of depending only on the local desktop.

## The practical architecture

Use these layers:

1. GitHub repository for code, presets, scripts, docs, and compact artifacts.
2. External cache storage for large candle and indicator data.
3. GitHub Actions for smoke checks and lightweight remote jobs.
4. OpenAI-hosted analysis for compact research packs, not for the full raw 17+ GB cache.

## What should stay out of git

Do not push these into the repository:

- `data_cache/`
- `runs/`
- `.venv/`
- temporary exports

The full local cache is too large for normal git workflow and not a good fit for GitHub storage.

## Environment variables

Recommended environment variables:

- `TRADE_INSPECTOR_CACHE_DIR`
- `TRADE_INSPECTOR_OUTPUT_DIR`
- `OPENAI_API_KEY`
- `OPENAI_MODEL`
- `OPENAI_MEMORY_LIMIT`

Example Linux/macOS:

```bash
export TRADE_INSPECTOR_CACHE_DIR=/mnt/trade-cache
export TRADE_INSPECTOR_OUTPUT_DIR=$PWD/dist/remote_jobs
export OPENAI_API_KEY=...
export OPENAI_MODEL=gpt-5.5
export OPENAI_MEMORY_LIMIT=16g
```

Example PowerShell:

```powershell
$env:TRADE_INSPECTOR_CACHE_DIR = "D:\trade-cache"
$env:TRADE_INSPECTOR_OUTPUT_DIR = "$PWD\dist\remote_jobs"
$env:OPENAI_API_KEY = "..."
$env:OPENAI_MODEL = "gpt-5.5"
$env:OPENAI_MEMORY_LIMIT = "16g"
```

## One stable headless entrypoint

Use:

```bash
python tools/remote_job.py --help
```

Supported flows:

```bash
python tools/remote_job.py smoke
python tools/remote_job.py saved-preset --preset "My Preset" --start "2026-03-01 00:00:00" --end "2026-04-01 00:00:00"
python tools/remote_job.py build-pack --output-dir dist/ai_pack
python tools/remote_job.py submit-pack --zip dist/ai_pack/ai_research_pack.zip
```

## GitHub Actions

Two workflows are included:

1. `ci.yml`
   Runs the hardened smoke path on push, pull request, and manual dispatch.

2. `build-research-pack.yml`
   Manually builds a compact AI research pack as a downloadable artifact.

This gives us a remote baseline that does not burn the local machine for every check.

## OpenAI-hosted workflow

Recommended sequence:

1. Build a compact research pack:

```bash
python tools/remote_job.py build-pack --output-dir dist/ai_pack
```

2. Submit it for hosted analysis:

```bash
python tools/remote_job.py submit-pack --zip dist/ai_pack/ai_research_pack.zip
```

This route is good for:

- casebook analysis
- compact backtest reports
- rule-search review
- summary and diagnostic work

This route is not ideal for:

- shipping the full raw local cache
- huge brute-force sweeps on every minute file
- long-running heavyweight numeric jobs that need persistent 10+ GB local state

For those heavier jobs, use a cloud VM or mounted storage plus the same headless entrypoints.

## Recommended next step for heavier compute

If we want larger remote backtests later:

1. Move the cache to external storage.
2. Mount or sync that cache into a cloud VM.
3. Point `TRADE_INSPECTOR_CACHE_DIR` there.
4. Run the same headless tools from this repo.

That way the repo stays clean, and the execution target can change without changing the scripts.
