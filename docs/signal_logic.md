# Signal logic

Current simplified signal uses `10Y-1Y`:

- `< 0.20` -> `长债机会`
- `0.20 ~ 1.00` -> `中性`
- `> 1.00` -> `短债优先`

Future extensions can add:

- 120-day moving average of 10Y yield
- 250-day percentile of 10Y yield
- DR007 trend filter
- credit-spread regime filter
