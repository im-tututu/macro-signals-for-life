# Architecture

## High-level flow

```text
ChinaBond yield curve  ─┐
                        ├─> yc_curve -> rate_dashboard -> curve_history -> curve_slope -> etf_signal
Money market (DR001…) ─┤
Bond futures (T0/TF0) ─┘
                                   └─> macro_dashboard
```

## Main responsibilities

- `runEnhancedSystem()` orchestrates the daily pipeline.
- `runDailyWide_()` fetches and writes curve data.
- `fetchPledgedRepoRates_()` writes money market data.
- `fetchBondFutures_()` writes futures data.
- `buildMacroDashboard_()` renders the one-page monitoring sheet.
