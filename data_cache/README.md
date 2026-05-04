# Data Cache Layout

This cache is organized around one canonical rule:

- daily canonical stores live in dedicated subdirectories
- exact-window caches do not belong in the `data_cache` root

## Main folders

- `ohlcv_store/`
  - canonical daily 1m candle store
  - path pattern: `ohlcv_store/<SYMBOL>/<PRICE_SOURCE>/1m/<SYMBOL>_<PRICE_SOURCE>_YYYYMMDD.parquet`

- `ohlcv_exact/`
  - exact requested 1m candle windows
  - path pattern: `ohlcv_exact/<SYMBOL>/<PRICE_SOURCE>/1m/FUTURES_<SYMBOL>_1m_<PRICE_SOURCE>_<START>_<END>.parquet`
  - these files are reproducible slices and may be rebuilt from `ohlcv_store`

- `indicator_store/`
  - canonical daily materialized indicator day-store
  - one profile-hash directory per indicator/timeframe/settings signature

- `indicator_streams/`
  - exact-window indicator cache pairs for previously requested windows
  - this layer is now optional/opt-in and should stay empty in the default slim backtest workflow

- `prepared_datasets/`
  - reusable prepared backtest datasets with metadata plus stream payload files
  - this layer is derivative and should only be kept when a workflow explicitly needs saved prepared bundles

- `_inventory/`
  - generated audit reports

- `_quarantine/`
  - files kept aside when a migration finds conflicts

## Root hygiene

The `data_cache` root should stay almost empty.

Allowed root-level files are only exceptional/manual files such as this `README.md`.
Loose `FUTURES_*` 1m window caches should be migrated into `ohlcv_exact/`.

## Slim backtest mode

For normal research and repeated preset sweeps the preferred cache shape is:

- keep `ohlcv_store/`
- keep selected `indicator_store/` profiles that match the indicators you actually backtest
- do not persist `indicator_streams/` unless exact-window reuse is explicitly needed
- do not persist `prepared_datasets/` unless a workflow explicitly reuses saved prepared bundles
