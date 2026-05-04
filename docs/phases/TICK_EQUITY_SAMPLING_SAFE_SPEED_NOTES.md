# Tick Equity Sampling Safe Speed Notes

This phase keeps the tick tester behavior intact while trimming equity/reporting overhead on the preferred prepared-streams tick path.

## What changed

- The prepared-streams tick engine now honors `BacktestConfig.equity_curve_stride`.
- Equity rows are sampled with integer millisecond staging and converted to timestamps at the end.
- Max drawdown is tracked live during signal processing so summary metrics remain exact even when the returned equity curve is sampled more sparsely.

## What did not change

- rule math
- indicator math
- bars-ago behavior
- filters
- sequence handling
- tick fill mechanics
- replay/trade output shape

## Intent

This is a safe speed pass for reporting/equity collection only. Default behavior stays the same because the default stride is still `1`.
