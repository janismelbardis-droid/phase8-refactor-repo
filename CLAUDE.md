# Project memory

## Market data / candle downloads — IMPORTANT environment fact

- This repo runs in a **cloud/remote execution environment where the live Binance
  REST API (`https://fapi.binance.com`) is geo-blocked → HTTP 451**. Do NOT try to
  download candles by calling the Binance REST API from the cloud session, and do
  not keep re-reporting that it is blocked — it is known and permanent here.
- The correct pattern: candles are downloaded **on the user's own machine** by a
  committed script, then the resulting files are pushed to git. The cloud session's
  job is to write/maintain that script, not to run the download itself.
- `data.binance.vision` (Binance's public historical archive, served via CloudFront)
  IS reachable from the cloud and from the user's machine, returns HTTP 200, and is
  the preferred bulk-history source (one zip per month/day, far cheaper than the REST
  API). The script uses this source so it works in both places.
- Candle download script: `tools/fetch_candle_history.py`
  (downloads BTCUSDT 1m history → compact yearly parquet under `market_data/`,
  optional `--push` to commit + push to git).
- 1m OHLCV candles for one symbol over ~6 years are small (tens of MB compressed),
  NOT the 17 GB local cache. The 17 GB figure is the tick/aggTrades + indicator
  cache, a different thing. Do not conflate them.
