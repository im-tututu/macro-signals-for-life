# Bond Market Terminal

一个基于 Google Apps Script + Google Sheets 的债券市场监控项目。当前结构已经收敛为：

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
├─ README.md
└─ appsscript.json
```

## 数据流

```text
原始_收益率曲线 ─┐
                  ├─> 指标_利率 ──> 信号_利率
原始_资金面   ────┘
```

## 主入口

完整日更：

```javascript
runEnhancedSystem()
```

常用手工入口：

- `rebuildAll_()`：只重建指标与信号
- `buildMetrics_()`：只重建统一指标表
- `buildSignal_()`：只重建统一信号表
- `testBackfillSafe()`：最近 30 天安全回补
- `resumeBackfillSafe()`：按游标继续回补

## 当前 Sheet 结构

### 原始数据表
- `原始_收益率曲线`
- `原始_资金面`
- `原始_国债期货`

### 指标表
- `指标_利率`

### 信号表
- `信号_利率`

## 指标表设计说明

### 设计原则

- 先放关键点位
- 再放期限结构与相对利差
- 最后补 rolling 均线和分位数
- 表按日期逆序输出，最新在最上面

### 指标口径说明

有些曲线并不覆盖 3Y / 5Y：

- `AA+信用` 当前主要覆盖到 `1Y`
- `AAA+中票` 当前主要覆盖到 `1Y`
- `AAA城投` 当前主要覆盖到 `1Y`
- `AAA存单` 本来就主要使用 `1Y`

因此指标表顺着源数据选代表期限，不强行把所有曲线都做成 `3Y / 5Y`。

### 指标_利率 主要字段含义

#### 1) 利率基准点位
- `gov_1y / gov_3y / gov_5y / gov_10y / gov_30y`：国债关键期限收益率
- `cdb_3y / cdb_5y / cdb_10y`：国开债关键期限收益率

用途：看利率中枢、久期位置和超长端弹性。

#### 2) 高等级信用与子类点位
- `aaa_credit_1y / 3y / 5y`：AAA 信用收益率
- `aa_plus_credit_1y`：AA+ 信用短端收益率
- `aaa_plus_mtn_1y`：AAA+ 中票短端收益率
- `aaa_mtn_1y / 3y / 5y`：AAA 中票收益率
- `aaa_bank_bond_1y / 3y / 5y`：AAA 银行债收益率
- `aaa_lgfv_1y`：AAA 城投短端收益率
- `aaa_ncd_1y`：AAA 存单 1Y

用途：观察高等级信用、信用下沉、银行负债成本与短端信用传导。

#### 3) 期限结构
- `gov_slope_10_1 = gov_10y - gov_1y`
- `gov_slope_10_3 = gov_10y - gov_3y`
- `gov_slope_30_10 = gov_30y - gov_10y`
- `cdb_slope_10_3 = cdb_10y - cdb_3y`

用途：判断曲线平坦/陡峭、长端与超长端性价比。

#### 4) 政金 / 地方利差
- `spread_cdb_gov_3y / 5y / 10y`：国开相对国债利差
- `spread_local_gov_gov_5y / 10y`：地方债相对国债利差

用途：判断国开债、地方债相对国债是偏贵还是偏便宜。

#### 5) 高等级信用利差
- `spread_aaa_credit_gov_1y / 3y / 5y`
- `spread_aa_plus_vs_aaa_credit_1y`
- `spread_aaa_plus_mtn_gov_1y`
- `spread_aaa_mtn_vs_aaa_plus_mtn_1y`
- `spread_aaa_credit_ncd_1y`
- `spread_aaa_bank_vs_aaa_credit_1y / 3y / 5y`
- `spread_aaa_lgfv_vs_aaa_credit_1y`

用途：判断高等级信用性价比、信用下沉环境、中票分层、存单与信用的相对价值。

#### 6) Rolling 指标
- `gov_10y_ma20 / ma60 / ma120`
- `gov_10y_pct250`
- `spread_cdb_gov_10y_ma20 / pct250`
- `spread_aaa_credit_gov_5y_ma20 / pct250`
- `spread_aa_plus_vs_aaa_credit_1y_ma20 / pct250`
- `aaa_ncd_1y_ma20 / pct250`

用途：把“当前值”放到滚动历史里判断高低分位，而不是只看绝对数值。

## 信号表设计说明

### 信号_利率 主要字段含义

#### 1) 久期信号
- `signal_duration`：总久期判断
- `signal_ultra_long`：30Y 是否占优
- `signal_curve`：曲线平/陡/中性

#### 2) 利率债相对价值
- `signal_policy_bank`：国开 vs 国债
- `signal_local_gov`：地方债 vs 国债

#### 3) 信用信号
- `signal_high_grade_credit`：高等级信用是否值得增配
- `signal_credit_sink`：是否适合信用下沉
- `signal_mtn_tier`：AAA 中票 vs AAA+ 中票
- `signal_ncd_vs_credit`：短端信用 vs 存单

#### 4) 环境判断
- `view_funding`：资金偏松 / 中性 / 偏紧
- `view_credit`：信用环境判断
- `view_regime`：综合环境（防守 / 中性 / 债牛偏进攻）

#### 5) 配置建议
- `weight_long_duration`
- `weight_mid_duration`
- `weight_short_duration`
- `weight_high_grade_credit`
- `weight_credit_sink`
- `weight_cash`

说明：这是日常看盘用的建议权重，不是回测最优权重。

## 模块职责

### `10_raw_curve.js`
抓取中债收益率曲线原始数据，写入 `原始_收益率曲线`。

### `11_raw_money_market.js`
抓取 Chinamoney 资金面数据（如 DR007），写入 `原始_资金面`。

### `12_raw_futures.js`
抓取国债期货快照，写入 `原始_国债期货`。

### `13_raw_bond_index.js`
提供债券指数相关的抓取/查询辅助函数。

### `14_raw_backfill.js`
负责历史回补、断点续跑与批量补数调度。

### `20_metrics.js`
从 `原始_收益率曲线` 构建统一指标宽表 `指标_利率`。

### `21_signal.js`
从 `指标_利率` 与 `原始_资金面` 构建统一信号表 `信号_利率`。
