# `chinabond_curve` 核心数据集说明

`chinabond_curve` 对当前仓库属于核心原始数据集，不建议在早期把它当作普通来源做激进通用化。

## 基本定义

- `dataset_id`: `chinabond_curve`
- `source_name`: `chinabond`
- `target_table`: `raw_bond_curve`
- `key_fields`: `date + curve`
- `date_field`: `date`
- `update_mode`: `upsert`
- `trading_day_sensitive`: `true`
- `prefer_trading_day_window`: `true`

## latest 落表

- 支持 `latest`
- 模式不是“今天”，而是 `latest_available_with_lookback`
- 业务含义：
  - 先尝试目标日期
  - 若当天无可用曲线，则按回看窗口寻找最近可用交易日
- 当前实现入口：
  - `py/src/jobs/latest.py`
  - `fetch_latest_chinabond_curve`

## historical 落表

- 支持 `backfill`
- 模式为 `trading_day_range`
- 业务含义：
  - 应按交易日序列展开
  - 不宜按自然日无差别盲抓
- 当前实现入口：
  - `py/src/jobs/backfill.py`
  - `backfill_chinabond_curve_window`
  - `py/src/jobs/ingest.py`
  - `fetch_bond_curve_for_date`

## 为什么单独视为核心数据集

- 数据量大，曲线种类多
- latest 与 backfill 逻辑都重要
- 强依赖交易日
- `metric_daily` 中大量基础指标直接来自 `raw_bond_curve`
- 一旦 schema / 口径变化，影响面大

## 当前改造原则

- 先做“元数据显式化”，不急于 generic store
- 先把 latest / backfill 语义明确拆开
- 普通来源先做通用化试点，`bond_curve` 优先稳住
