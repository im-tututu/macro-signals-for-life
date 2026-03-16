/********************
 * 50_jobs.js
 * 触发器入口函数。
 *
 * 原则：
 * - Apps Script 触发器只绑定这里的无参入口
 * - 具体抓取 / 写表逻辑继续留在 20-24 原始层
 * - 手工执行时也优先从这里选，便于区分“可直接运行的 job”和内部函数
 ********************/

/**
 * 收盘后抓取中债收益率曲线。
 *
 * 用法：
 * - 适合绑定工作日傍晚触发器
 * - 也可手工执行，用今天日期跑一轮曲线抓取
 *
 * 影响范围：
 * - 写入/更新：原始_收益率曲线
 */
function jobCurveClose() {
  runDailyWide_(today_());
}

/**
 * 日内抓取银行间质押式回购快照。
 *
 * 用法：
 * - 适合绑定每 30 分钟一次的触发器
 * - 接口若返回上一交易日，会自动按接口业务日期落表
 *
 * 影响范围：
 * - 写入/更新：原始_资金面
 */
function jobMoneyMarketIntraday() {
  fetchPledgedRepoRates_();
}

/**
 * 收盘后抓取国债期货近似收盘价。
 *
 * 用法：
 * - 适合绑定工作日傍晚触发器
 * - 当前抓取 T0 / TF0 连续合约价格快照
 *
 * 影响范围：
 * - 写入：原始_国债期货
 */
function jobFuturesClose() {
  fetchBondFutures_();
}

/**
 * 轮询央行政策利率公告并同步最新事件。
 *
 * 用法：
 * - 适合绑定 1~2 小时一次的触发器
 * - 也可在你刚修复 PBC 解析逻辑后手工跑一次验证
 *
 * 影响范围：
 * - 写入/更新：原始_政策利率
 */
function jobPolicyWindowPoll() {
  syncRawPolicyRateLatest();
}

/**
 * 夜间抓取海外宏观与民生/资产价格原始表。
 *
 * 用法：
 * - 适合绑定夜间触发器
 * - overseas macro 依赖 Script Properties 中的 API key
 * - life asset 当前为占位行，后续可继续扩展来源
 *
 * 影响范围：
 * - 写入/更新：原始_海外宏观、原始_民生与资产价格
 */
function jobMacroNightly() {
  fetchOverseasMacro_();
  fetchLifeAsset_();
}

/**
 * 完整日更主任务。
 *
 * 用法：
 * - 适合绑定每日收盘后的总任务触发器
 * - 也适合你手工点一次，跑完整抓取 + 派生重建
 *
 * 流程：
 * 1) 曲线
 * 2) 资金面
 * 3) 国债期货
 * 4) 政策利率
 * 5) 海外宏观 / 民生资产
 * 6) 指标、信号、复盘重建
 */
function jobDailyClose() {
  runEnhancedSystem();
}

/**
 * 只重建派生层，不抓取任何外部源。
 *
 * 用法：
 * - 你改了 metrics / signal 规则后直接执行
 * - 原始表已齐全时，用它最快
 *
 * 影响范围：
 * - 重建：指标、信号、信号-复盘
 */
function jobRebuildOnly() {
  rebuildAll_();
}
