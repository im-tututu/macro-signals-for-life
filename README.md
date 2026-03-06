# Bond Market Terminal

A Google Apps Script based bond-market monitoring project for:

- ChinaBond yield-curve scraping
- DR001 / DR007 money-market monitoring
- bond-futures snapshot tracking
- curve slope and spread analytics
- bond allocation signal generation
- Google Sheets dashboard generation
- historical backfill with retry / jitter / resume support

## Repository structure

```text
bond-market-terminal-complete-repo
├─ src/
│  ├─ 00_main.gs
│  ├─ 01_config.gs
│  ├─ 02_utils.gs
│  ├─ 10_chinabond_curve.gs
│  ├─ 20_rate_dashboard.gs
│  ├─ 21_curve_history_slope_signal.gs
│  ├─ 22_money_market.gs
│  ├─ 23_futures.gs
│  ├─ 24_bond_allocation_signal.gs
│  ├─ 30_macro_dashboard.gs
│  └─ 40_backfill.gs
├─ docs/
├─ scripts/
├─ examples/
├─ .github/workflows/
├─ appsscript.json
├─ .clasp.json.example
└─ README.md
```

## Quick start

1. Create a new standalone Google Apps Script project.
2. Copy files under `src/` into the project, or use `clasp` to sync this repo.
3. In Apps Script, run:

```javascript
runEnhancedSystem()
```

## Manual entry points

- `runEnhancedSystem()`
- `backfillLast120Days()`
- `testBackfillSafe()`
- `rebuildAll_()`
- `buildBondAllocationSignal_()`

## Sheets generated

- `yc_curve`
- `rate_dashboard`
- `curve_history`
- `curve_slope`
- `etf_signal`
- `money_market`
- `futures`
- `bond_allocation_signal`
- `macro_dashboard`

## Notes

- `23_futures.gs` still uses an empirical parser for Sina futures text fields. If T0 / TF0 looks wrong, check that module first.
- `24_bond_allocation_signal.gs` is a low-frequency allocation model for duration preference, not a high-frequency trading strategy.
