# Macro Signals for Life

基于 Google Apps Script + Google Sheets 的宏观观察项目。

这版代码库按“来源 / 原始表 / 指标 / 信号 / 任务入口”重新整理，目标是：

- 保留现有表结构与主流程可用性
- 让不同来源的抓取逻辑更容易定位
- 让 GAS 触发器只绑定少量稳定入口函数
- 给后续新增国家统计局 / SGE / 中国银行 / 余额宝等来源留出清晰位置

## 目录结构

```text
src/
  00_main.js
  01_config.js
  02_utils.js

  10_source_chinabond.js
  11_source_chinamoney.js
  12_source_pbc.js
  13_source_fred.js
  14_source_stats_gov.js
  15_source_external_misc.js

  20_raw_curve.js
  21_raw_policy_rate.js
  22_raw_money_market.js
  23_raw_futures.js
  24_raw_macro.js

  30_metrics.js
  31_signal.js

  40_formula_chinabond_index.js

  50_jobs.js
  51_trigger_admin.js
  52_notify_log.js
```

## 分层说明

### 10–15：来源层
只处理“怎么抓、怎么解析”。

### 20–24：原始表层
只处理“写哪张表、怎么 upsert / 排序 / 回补”。

### 30–31：派生层
- `30_metrics.js`：统一指标宽表
- `31_signal.js`：统一信号主表、明细表

### 40：自定义公式
表格单元格直接调用的函数，不参与定时任务。

### 50+：运行层
- 定时触发入口
- 触发器重建
- 运行日志
- 复盘表构建

## 兼容性说明

- 宏观原始表仍沿用旧表名 `原始_海外宏观`，避免打断既有公式与下游链路
- `runEnhancedSystem()`、`backfillBatch_()`、`syncRawPolicyRateLatest()` 等常用入口仍保留
- `fetchLifeAsset_()` 当前提供“可安全跳过”的最小实现，不会阻塞主流程


## TODO

当前未完成事项已单独整理到 `TODO.md`，按 **P0 / P1 / P2 / P3** 分优先级维护，便于后续按现结构继续推进。

## Secrets

Script Properties 可配置：

- `FRED_API_KEY`
- `ALPHA_VANTAGE_API_KEY`
- `ALERT_EMAIL`（可选，运行告警邮件）

## 触发器

推荐只绑定 `50_jobs.js` 里的入口函数。
也可以执行：

- `rebuildProjectTriggers()`

来按默认注册表重建 time-driven triggers。
