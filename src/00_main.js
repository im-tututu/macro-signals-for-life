/********************
 * 00_main.js
 * 主入口与人工测试入口。
 *
 * 说明：
 * - 真正的定时触发入口统一放在 50_jobs.js
 * - 这里保留 runEnhancedSystem / backfill 等高频手工入口
 * - 需要绑触发器时，优先绑定 50_jobs.js 中的 job* 函数
 ********************/

/**
 * 手工测试入口。
 */
function test() {
  runEnhancedSystem();
}

/**
 * 一次性执行完整日更流程：
 * 1) 中债曲线
 * 2) 资金面
 * 3) 国债期货
 * 4) 政策利率
 * 5) 海外宏观 / 民生资产
 * 6) 指标、信号、复盘重建
 */
function runEnhancedSystem() {
  var today = formatDate_(new Date());

  runDailyWide_(today);
  fetchPledgedRepoRates_();
  fetchBondFutures_();
  syncRawPolicyRateLatest();
  fetchOverseasMacro_();
  fetchLifeAsset_();

  rebuildAll_();
}

/**
 * 仅重建派生层，不抓取原始数据。
 */
function rebuildAll_() {
  buildMetrics_();
  buildSignal_();
  SpreadsheetApp.flush();
  Utilities.sleep(500);
  buildSignalReview_();
}

function rebuildSignalAndReview_() {
  buildSignal_();
  SpreadsheetApp.flush();
  Utilities.sleep(500);
  buildSignalReview_();
}

/**
 * 从最近 30 天起点重新安全回补，每次最多处理 8 个非周末日期。
 */
function testBackfillSafe() {
  var end = new Date();
  var start = new Date(end);
  start.setDate(end.getDate() - 30);
  backfillBatch_(formatDate_(start), formatDate_(end), 8, true);
}

/**
 * 从回补游标继续补最近 120 天数据，每次最多处理 8 个非周末日期。
 */
function resumeBackfillSafe() {
  var end = new Date();
  var start = new Date(end);
  start.setDate(end.getDate() - 120);
  backfillBatch_(formatDate_(start), formatDate_(end), 8, false);
}

function showBackfillCursor() {
  Logger.log('BACKFILL_CURSOR=' + getBackfillCursor_());
}

function resetBackfillCursor() {
  clearBackfillCursor_();
  Logger.log('BACKFILL_CURSOR cleared');
}

function testLifeAssetEntry() {
  return forceFetchLifeAsset_();
}
