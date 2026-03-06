# Sheet schema

## yc_curve
- `date`
- `curve`
- `Y_0 ... Y_50`

## rate_dashboard
- `date`
- `gov_1y`, `gov_3y`, `gov_5y`, `gov_10y`
- `cdb_10y`
- `aaa_5y`
- `policy_spread`
- `credit_spread`
- `term_spread`

## curve_history
- `date`, `gov_1y`, `gov_3y`, `gov_5y`, `gov_10y`

## curve_slope
- `date`, `10Y-1Y`, `10Y-3Y`, `5Y-1Y`

## etf_signal
- `date`, `10Y-1Y`, `signal`
