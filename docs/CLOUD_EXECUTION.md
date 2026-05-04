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
python tools/remote_job.py slim-profile --start "2026-04-20 00:00:00+00:00" --end "2026-04-20 04:00:00+00:00"
python tools/remote_job.py saved-preset --preset "My Preset" --start "2026-03-01 00:00:00" --end "2026-04-01 00:00:00"
python tools/remote_job.py build-pack --output-dir dist/ai_pack
python tools/remote_job.py submit-pack --zip dist/ai_pack/ai_research_pack.zip
```

## Recommended Codex cloud environment for this repo

Use the public repository:

- `janismelbardis-droid/phase8-refactor-repo`

Recommended environment settings:

1. Setup script:

```bash
bash scripts/cloud_bootstrap.sh
```

2. Internet access:

- enable it when the task may need to install dependencies or fetch missing candles
- disable it for analysis-only tasks that work entirely from an already prepared cache

3. Cache path:

- default cloud bootstrap path is `$PWD/.cache/trade_inspector`
- override with `TRADE_INSPECTOR_CACHE_DIR` if you mount or sync a different storage location

## Slim backtest workflow

The default cloud workflow should now stay slim:

- keep canonical candles in `ohlcv_store`
- keep only the indicator families actually used by the backtest
- do not persist `indicator_streams` unless explicitly requested
- do not persist `prepared_datasets` unless explicitly requested
- avoid `market_aug` unless a strategy genuinely needs it

Example slim profile build:

```bash
python tools/remote_job.py slim-profile \
  --symbol BTCUSDT \
  --start "2026-04-20 00:00:00+00:00" \
  --end "2026-04-20 04:00:00+00:00" \
  --indicator-families "ohlcv,vidya,range_filter,stoch_rsi,taker_bias"
```

If you want exact-window indicator cache files anyway:

```bash
python tools/remote_job.py slim-profile \
  --symbol BTCUSDT \
  --start "2026-04-20 00:00:00+00:00" \
  --end "2026-04-20 04:00:00+00:00" \
  --indicator-families "ohlcv,vidya,range_filter,stoch_rsi,taker_bias" \
  --save-exact-indicator-cache
```

If a workflow truly needs `prepared_datasets`:

```bash
python tools/remote_job.py slim-profile \
  --symbol BTCUSDT \
  --start "2026-04-20 00:00:00+00:00" \
  --end "2026-04-20 04:00:00+00:00" \
  --indicator-families "ohlcv,vidya,range_filter,stoch_rsi,taker_bias" \
  --save-prepared
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
