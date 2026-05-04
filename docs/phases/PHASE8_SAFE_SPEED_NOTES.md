# Phase 8 — Safe speed: canonical local 1m candle store

This phase keeps indicator math and trade logic intact.

## What changed

- added a persistent **daily chunked 1m candle store** under `cache_dir/ohlcv_store/...`
- `fetch_klines_1m_futures(...)` now tries, in order:
  1. exact-window cache
  2. covering exact-window cache
  3. canonical local candle store
  4. Binance only for **missing minute ranges**
- newly downloaded segments are merged back into the local daily store
- exact-window caches are still written for compatibility with the existing workflow

## Why this helps

Repeated backtests on overlapping windows no longer need to redownload the whole range.
The fetch path can now satisfy requests from local chunks and only gap-fill the missing parts.

## Safety

- no indicator formulas changed
- no backtest execution rules changed
- no report schema changed
- exact-window cache behavior is preserved
