# TODO

本清单基于当前 `py/` 目录现状整理，只保留 **Python 底座里仍未完成**、且与原 GAS TODO 对应的事项。

当前已完成的基础能力不再重复列入：

- 原始层 `source -> store -> job` 主体结构已统一
- `money_market / bond_curve / overseas_macro / policy_rate / futures / etf / bond_index` 已有可运行入口
- `bond_curve backfill` 已接入交易日文件与 pre-check
- 项目内 [trading_days.csv](/Users/zy/Documents/git/macro-signals-for-life-1/py/data/reference/trading_days.csv) 已建立并支持更新到 `2026-12-31`

---

## P0｜优先补齐的底座缺口

### 1. 原始层回填能力继续补全

- [ ] `policy_rate` 从“近期事件补采”升级为更明确的历史回填策略
  - [ ] 支持按日期范围回填，而不只靠 `--limit`
  - [ ] 支持已存在业务键的 pre-check，减少重复请求
- [ ] `bond_index` 从“单 index/手动传 id”升级为批量 backfill
  - [ ] 从 `cfg_bond_index_list` 读取 Python 可用的指数配置
  - [ ] 定义哪些 index_name / index_code / provider 组合是真正的债券指数
  - [ ] 支持批量抓取与分批失败重试
- [ ] 明确哪些 raw 表支持 backfill，哪些只做 latest sync
  - [ ] `bond_curve`
  - [ ] `policy_rate`
  - [ ] `bond_index`
  - [ ] `money_market`
  - [ ] `overseas_macro`

### 2. 真实数据与占位数据边界收口

- [ ] `life_asset` 仍是 placeholder，需要改成真实抓取链路
  - [ ] 明确首批真实字段
  - [ ] 明确各字段对应来源
  - [ ] 去掉“占位成功写入”造成的假稳定
- [ ] `stats_gov` 仍未正式落地
  - [ ] 明确首批指标
  - [ ] 明确发布日期与观察期字段口径
  - [ ] 接入 raw 表真实写入
- [ ] `external_misc` 中 SGE / BOC / 货币基金仍是占位能力
  - [ ] 明确真实来源
  - [ ] 明确抓取频率
  - [ ] 明确 source note / source url 约定

### 3. 原始表数据质量前置检查

- [ ] 把“抓后 dedupe”继续升级为“抓前 pre-check”
  - [ ] `policy_rate`
  - [ ] `bond_index`
  - [ ] `money_market`
- [ ] 增加原始表关键字段完整性判断
  - [ ] `raw_bond_curve` 至少若干关键 tenor 非空
  - [ ] `raw_policy_rate` 的 `date/type/term/rate`
  - [ ] `raw_overseas_macro` 的关键指标不全为空
- [ ] 增加异常值初筛规则
  - [ ] 利率量级异常
  - [ ] 日期字段非标准格式
  - [ ] 单日重复快照异常激增

---

## P1｜来源层与 raw 层待完成事项

### 4. 国家统计局来源正式实现

- [ ] 在 Python 侧实现 `stats_gov` 正式 source
- [ ] 优先接入这些月频指标
  - [ ] CPI
  - [ ] PPI
  - [ ] 社零
  - [ ] 固投
  - [ ] 工业增加值
- [ ] 明确这些指标应该落到哪张 raw 表
  - [ ] 继续并入 `raw_life_asset`
  - [ ] 或拆出新的 Python raw macro 表

### 5. 海外宏观与生活/资产价格边界收口

- [ ] `raw_overseas_macro` 与 `raw_life_asset` 的字段边界需要进一步固定
- [ ] 明确这几个时间字段的统一口径
  - [ ] `date`
  - [ ] `source_date`
  - [ ] `fetched_at`
- [ ] 明确覆盖策略
  - [ ] 缺失值是否覆盖旧值
  - [ ] 最新值是否允许回写历史日期

### 6. ETF 抓取与历史快照策略

- [ ] 当前 `etf` 已去重，但仍只是“当日快照抓取”
- [ ] 明确 Python 侧是否要支持 ETF 历史快照累积
- [ ] 若支持，需要明确：
  - [ ] 快照频率
  - [ ] 是否按交易日抓
  - [ ] 是否需要 pre-check 跳过当日已抓

### 7. `bond_index` 映射层缺失

- [ ] GAS 中 `26_mapping_bond_index_name.js` 对应能力尚未迁到 Python
- [ ] 需要在 Python 侧补
  - [ ] 名称映射
  - [ ] provider 区分
  - [ ] 候选 index 清洗
  - [ ] 配置表校验

---

## P1｜任务与调度层待完成事项

### 8. Python 定时 job 体系还不完整

- [ ] `run_daily_job.py` 只是手动入口，尚未形成完整调度方案
- [ ] 需要补一版 Python 侧 job registry
  - [ ] job 名
  - [ ] 默认参数
  - [ ] 推荐执行时间
  - [ ] 是否允许 dry-run
- [ ] 为各来源定义建议调度节奏
  - [ ] Chinabond 曲线：收盘后
  - [ ] ChinaMoney：盘中/日终
  - [ ] PBC：轮询
  - [ ] FRED / Alpha：夜间
  - [ ] 交易日文件：定期更新

### 9. 失败处理与重试策略继续统一

- [ ] `run_backfill.py` 目前已能汇总失败，但不同 source 的重试策略还不完全一致
- [ ] 需要统一：
  - [ ] 最大重试次数
  - [ ] 退避时间
  - [ ] 可重试错误类型
  - [ ] 失败后的日志格式

### 10. 通知分级仍需完善

- [ ] 成功通知目前仍较粗
- [ ] 需要补：
  - [ ] 每日汇总通知
  - [ ] 失败分级
  - [ ] API key 缺失仅提醒一次
  - [ ] backfill 批量失败摘要

---

## P2｜metrics / signal 层仍基本未实现

### 11. metrics 计算层

- [ ] [metrics.py](/Users/zy/Documents/git/macro-signals-for-life-1/py/src/analytics/metrics.py) 目前仍是空壳
- [ ] 需要从 GAS `30_metrics.js` 提取 Python 版实现
  - [ ] 利率/曲线指标
  - [ ] 资金面指标
  - [ ] 政策利率指标
  - [ ] 海外宏观指标
- [ ] 明确每个 metric 的：
  - [ ] 输入表
  - [ ] 输入列
  - [ ] 计算口径
  - [ ] 缺失值保护

### 12. signal 生成层

- [ ] [signals.py](/Users/zy/Documents/git/macro-signals-for-life-1/py/src/analytics/signals.py) 目前仍是空壳
- [ ] 需要从 GAS `31_signal.js` 提取 Python 版实现
  - [ ] signal_main
  - [ ] signal_detail
  - [ ] signal_review
- [ ] 明确：
  - [ ] 信号分层
  - [ ] 分数口径
  - [ ] note 汇总规则
  - [ ] 空数据时不误判的保护

### 13. metrics / signal 执行入口

- [ ] 目前没有 Python 侧统一入口生成 `metrics` / `signal`
- [ ] 需要新增脚本或 job
  - [ ] `run_metrics.py`
  - [ ] `run_signals.py`
  - [ ] 或并入统一 `run_pipeline.py`

---

## P2｜质量检查与可观测性

### 14. 原始表检查脚本

- [ ] 补一个图形界面之外的快速检查脚本
  - [ ] 行数
  - [ ] 日期范围
  - [ ] 重复 key
  - [ ] 空 key
  - [ ] 关键列空值率
- [ ] 支持单表和全部 raw 表

### 15. source 对账与抽样校验

- [ ] 目前更多依赖人工抽样
- [ ] 需要补最小自动校验
  - [ ] `bond_curve` 抽查关键 tenor
  - [ ] `policy_rate` 抽查最新公告
  - [ ] `money_market` 抽查关键字段
  - [ ] `bond_index` 抽查 duration / ytm

### 16. smoke test 体系继续补

- [ ] `smoke_sources.py` / `smoke_stores.py` 还偏轻
- [ ] 需要补：
  - [ ] 单日抓取 smoke
  - [ ] 单段 backfill smoke
  - [ ] 原始表 review smoke
  - [ ] metrics / signal smoke

---

## P3｜文档与入口整理

### 17. Python CLI 入口继续收口

- [ ] 当前入口分散在：
  - [ ] [main.py](/Users/zy/Documents/git/macro-signals-for-life-1/py/main.py)
  - [ ] [run_daily_job.py](/Users/zy/Documents/git/macro-signals-for-life-1/py/scripts/run_daily_job.py)
  - [ ] [run_backfill.py](/Users/zy/Documents/git/macro-signals-for-life-1/py/scripts/run_backfill.py)
  - [ ] [update_trading_calendar.py](/Users/zy/Documents/git/macro-signals-for-life-1/py/scripts/update_trading_calendar.py)
- [ ] 后续需要考虑是否统一成一个更清晰的 CLI

### 18. schema / 表头文档

- [ ] Python 侧仍缺一份正式 schema 文档
- [ ] 至少需要覆盖：
  - [ ] raw 表主键
  - [ ] 更新频率
  - [ ] 来源
  - [ ] 允许为空字段
  - [ ] 业务日期字段定义

### 19. README 与 Python 文档同步

- [ ] 当 raw 表、job、backfill 能力继续变化时，同步更新：
  - [ ] README
  - [ ] [python_使用说明.md](/Users/zy/Documents/git/macro-signals-for-life-1/docs/python_使用说明.md)
  - [ ] 本文件

---

## 暂不做

- [ ] 不为了“更优雅”继续大拆目录结构
- [ ] 不在 raw 主链路未稳定前引入复杂测试框架
- [ ] 不在真实来源未确认前硬补 SGE / BOC / 货币基金完整实现
- [ ] 不急着把所有入口强行统一成一个超大 CLI
