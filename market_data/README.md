# market_data

Compact, git-tracked candle history. Unlike `data_cache/` (the large local
runtime cache that stays out of git), these files are small enough to live in
the repo so they are available everywhere — including the GitHub web/mobile UI.

## Layout

```
market_data/<SYMBOL>/<PRICE_SOURCE>/1m/<SYMBOL>_<PRICE_SOURCE>_1m_<YEAR>.parquet
```

Current contents: **BTCUSDT 1m (LAST price), 2020-01-01 → present**, one parquet
file per year, zstd-compressed (~13 MB/year, ~86 MB total). Verified gap-free
(every UTC minute present).

Compact schema (per row): `open_time, open, high, low, close, volume`.
`close_time` and the extra Binance volume columns (`quote_volume`,
`trade_count`, `taker_buy_volume`, `taker_buy_quote_volume`) are reconstructed /
zero-filled on restore, matching how the app already treats those extras.

## Source

Downloaded from Binance's public historical archive
`data.binance.vision` (NOT the live REST API, which is geo-blocked in the cloud
environment — see `CLAUDE.md`).

## Regenerate / update / use

```bash
# Download or refresh the history (incremental: re-uses cached zips):
python tools/fetch_candle_history.py

# Same, but also commit + push the result:
python tools/fetch_candle_history.py --push

# Expand into data_cache/ohlcv_store so the desktop app / backtests can read it:
python tools/fetch_candle_history.py restore
```
