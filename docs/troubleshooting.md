# Troubleshooting

## Common issues

### No curve data on a date
Likely weekend, holiday, or upstream source returned no data.

### Dashboard chart empty
Check whether upstream sheets contain at least one valid row.

### Backfill stops midway
Use `testBackfillSafe()` repeatedly; it resumes from stored cursor.

### Futures value looks odd
The Sina text-format parser is heuristic-based. Verify raw response first.
