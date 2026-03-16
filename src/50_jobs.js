/********************
 * 50_jobs.js
 * 触发器入口函数。
 *
 * 原则：
 * - 触发器只绑定这里的无参入口
 * - 具体抓取 / 写表逻辑继续留在 20-24 段
 ********************/

function jobCurveClose() {
  runDailyWide_(today_());
}

function jobMoneyMarketIntraday() {
  fetchPledgedRepoRates_();
}

function jobFuturesClose() {
  fetchBondFutures_();
}

function jobPolicyWindowPoll() {
  syncRawPolicyRateLatest();
}

function jobMacroNightly() {
  fetchOverseasMacro_();
  fetchLifeAsset_();
}

function jobDailyClose() {
  runEnhancedSystem();
}

function jobRebuildOnly() {
  rebuildAll_();
}
