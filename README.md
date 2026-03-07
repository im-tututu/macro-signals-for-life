# Bond Market Terminal

一个基于 Google Apps Script + Google Sheets 的债券市场监控项目。当前代码库已经收敛为：

- 原始数据层：收益率曲线、资金面、国债期货、债券指数
- 指标层：统一利率指标宽表
- 信号层：统一利率信号表
- 支持历史回补、断点续跑与日常重建

## 代码结构

```text
bond-rate-terminal-main
├─ src/
│  ├─ 00_main.js
│  ├─ 01_config.js
│  ├─ 02_utils.js
│  ├─ 10_raw_curve.js
│  ├─ 11_raw_money_market.js
│  ├─ 12_raw_futures.js
│  ├─ 13_raw_bond_index.js
│  ├─ 14_raw_backfill.js
│  ├─ 20_metrics.js
│  └─ 21_signal.js
├─ CHANGELOG.md
├─ README.md
└─ TODO.md
```

## 分层结构

### 0x 基础层
- `00_main.js`
- `01_config.js`
- `02_utils.js`

### 1x 原始数据层
- `10_raw_curve.js`
- `11_raw_money_market.js`
- `12_raw_futures.js`
- `13_raw_bond_index.js`
- `14_raw_backfill.js`

### 2x 指标与信号层
- `20_metrics.js`
- `21_signal.js`

## 当前 Sheet 结构

### 原始数据表
- `原始_收益率曲线`
- `原始_资金面`
- `原始_国债期货`

### 指标表
- `指标_利率`

### 信号表
- `信号_利率`

## 数据流

```text
原始_收益率曲线 ─┐
                  ├─> 指标_利率 ──> 信号_利率
原始_资金面   ────┘

原始_国债期货  ──────────────────> 市场观察辅助数据
```

## 主入口

执行完整日更：

```javascript
runEnhancedSystem()
```

当前顺序：

1. `runDailyWide_(today)`
2. `fetchPledgedRepoRates_()`
3. `fetchBondFutures_()`
4. `rebuildAll_()`
   - `buildMetrics_()`
   - `buildSignal_()`

## 常用手工入口

- `runEnhancedSystem()`：完整日更
- `rebuildAll_()`：只重建指标与信号
- `buildMetrics_()`：只重建统一指标表
- `buildSignal_()`：只重建统一信号表
- `testBackfillSafe()`：从最近 30 天安全回补
- `resumeBackfillSafe()`：按回补游标继续补数

## 模块职责

### `10_raw_curve.js`
抓取中债收益率曲线原始数据，写入 `原始_收益率曲线`。

### `11_raw_money_market.js`
抓取 Chinamoney 资金面数据（如 DR007），写入 `原始_资金面`。

### `12_raw_futures.js`
抓取国债期货行情快照，写入 `原始_国债期货`。

### `13_raw_bond_index.js`
提供债券指数相关的抓取/查询辅助函数。

### `14_raw_backfill.js`
负责历史回补、断点续跑与批量补数调度。

### `20_metrics.js`
从原始收益率曲线构建统一指标宽表 `指标_利率`。

### `21_signal.js`
从统一指标表与原始资金面表构建统一信号表 `信号_利率`。

## 表结构说明

### 1. 原始_收益率曲线

用途：存放中债收益率曲线原始抓取结果。

主要字段：
- `date`
- `curve`
- `Y_0 ... Y_50`

说明：
- 每个交易日通常会有多行，对应不同曲线：国债、国开债、AAA 信用、AA+ 信用。
- `20_metrics.js` 会从该表抽取关键期限并计算利差/斜率。

### 2. 原始_资金面

用途：存放资金面抓取结果。

主要字段（实际列可能更多）：
- `date`
- `DR001_weightedRate`
- `DR007_weightedRate`
- 其他回购期限相关字段

说明：
- `21_signal.js` 当前主要读取 `DR007_weightedRate`。

### 3. 原始_国债期货

用途：存放国债期货快照。

常见字段：
- `date`
- `T0_last`
- `TF0_last`
- `source`
- `fetched_at`

说明：
- 当前统一信号表暂未直接依赖该表，但它属于原始市场观察数据的一部分。

### 4. 指标_利率

用途：统一利率指标宽表，是当前项目唯一的指标主表。

字段：
- `date`
- `gov_1y`
- `gov_3y`
- `gov_5y`
- `gov_10y`
- `cdb_10y`
- `aaa_5y`
- `slope_10_1`
- `slope_10_3`
- `slope_5_1`
- `policy_spread`
- `credit_spread`
- `term_spread`

字段说明：
- `gov_*`：国债关键期限收益率
- `cdb_10y`：国开债 10Y
- `aaa_5y`：AAA 信用债 5Y
- `slope_10_1`：`gov_10y - gov_1y`
- `slope_10_3`：`gov_10y - gov_3y`
- `slope_5_1`：`gov_5y - gov_1y`
- `policy_spread`：`cdb_10y - gov_10y`
- `credit_spread`：`aaa_5y - gov_5y`
- `term_spread`：当前实现中等同于 `slope_10_1`

### 5. 信号_利率

用途：统一利率信号表，是当前项目唯一的信号主表。

字段：
- `date`
- `etf_slope_10_1`
- `etf_signal`
- `10Y`
- `MA120`
- `pct250`
- `slope10_1`
- `dr007`
- `credit_spread`
- `funding_view`
- `credit_view`
- `regime`
- `long_bond`
- `mid_bond`
- `short_bond`
- `cash`
- `comment`

说明：
- `etf_signal` 与 `bond_allocation_signal` 已并入本表。
- 旧函数入口仍保留兼容包装，但底层输出已统一到本表。

## 信号逻辑

### 1. ETF 观察信号

使用字段：
- `slope_10_1`

阈值来自 `SIGNAL_THRESHOLDS`：
- `< steep_low` → `长债机会`
- `> steep_high` → `短债优先`
- 其余 → `中性`

默认配置：
- `steep_low = 0.20`
- `steep_high = 1.00`

### 2. 债券配置建议

使用字段：
- `10Y`
- `MA120`
- `pct250`
- `slope10_1`
- `dr007`
- `credit_spread`

核心思路：
- 用 `MA120` 判断 10Y 相对均线位置
- 用 `pct250` 判断 10Y 在历史中的相对高低
- 用 `slope10_1` 判断曲线是否偏平
- 用 `dr007` 做资金面过滤
- 用 `credit_spread` 做信用环境提示

当前 `regime` 输出：
- `VERY_DEFENSIVE`
- `DEFENSIVE`
- `NEUTRAL`
- `BUY_LONG_BOND`
- `STRONG_BUY_LONG_BOND`

历史样本不足时：
- 若 `10Y / MA120 / pct250` 任一不足，则输出 `NEUTRAL`
- `comment = 历史数据不足（MA120/PCT250未就绪），中性配置`

### 3. 资金面与信用面补充说明

`funding_view`：
- `dr007 >= 1.90` → `资金偏紧`
- `dr007 <= 1.60` → `资金偏松`
- 其余 → `资金中性`

`credit_view`：
- `credit_spread >= 0.45` → `信用利差偏宽，信用债性价比改善`
- `credit_spread <= 0.36` → `信用利差偏窄，信用保护垫较薄`
- 其余 → `信用中性`

## 结构调整说明

当前版本已完成两层收敛：

### 指标层收敛
以下旧表逻辑已并入统一指标表：
- 曲线历史
- 曲线斜率
- 利率总览 / 利差快照

### 信号层收敛
以下旧表逻辑已并入统一信号表：
- ETF 观察信号
- 债券配置建议

旧入口函数仍保留为兼容包装，因此已有调用通常不需要全部重写。

## 兼容入口

虽然表结构已收敛，但以下旧入口函数仍保留：
- `buildRateMetrics_()`
- `buildCurveHistory_()`
- `buildCurveSlope_()`
- `buildETFSignal_()`
- `buildBondAllocationSignal_()`

它们最终都会落到统一表：
- `指标_利率`
- `信号_利率`

## 当前设计取舍

- 当前代码后缀统一保留为 `.js`，便于继续沿用现有 Apps Script / clasp 工作流。
- 当前已移除宏观看板模块；如果后续确实需要，再从统一指标表/统一信号表派生新的展示页会更自然。
- 国债期货抓取模块仍建议单独核查字段位次与合约代码，尤其是 `T0 / TF0` 连续代码。
