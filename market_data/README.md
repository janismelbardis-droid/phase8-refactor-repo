# market_data

Compact, git-tracked candle history. Unlike `data_cache/` (the large local
runtime cache that stays out of git), these files are small enough to live in
the repo so they are available everywhere — including the GitHub web/mobile UI.

## Layout

```
market_data/<SYMBOL>/<PRICE_SOURCE>/<TIMEFRAME>/<SYMBOL>_<PRICE_SOURCE>_<TIMEFRAME>_<YEAR>.parquet
```

Current contents: **BTCUSDT (LAST price), 2020-01-01 → present**, one parquet
file per year per timeframe, zstd-compressed. Verified gap-free.

| Timeframe | Candles   | Size    |
|-----------|-----------|---------|
| 1m        | 3,412,800 | ~86 MB  |
| 5m        |   682,560 | ~25 MB  |
| 15m       |   227,520 | ~8 MB   |
| 1h        |    56,880 | ~2 MB   |
| 4h        |    14,220 | ~0.5 MB |

Every timeframe is downloaded directly from the Binance archive.

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
# Download or refresh history (incremental: re-uses cached zips). --timeframes
# selects which to pull; --push also commits + pushes; --symbol for other markets:
python tools/fetch_candle_history.py --timeframes 1m,5m,15m,1h,4h --push

# Expand the 1m set into data_cache/ohlcv_store so the desktop app / backtests can read it:
python tools/fetch_candle_history.py restore
```
