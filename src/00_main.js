/********************
 * 00_main.js
 * 项目入口与任务编排。
 ********************/

/**
 * 手工测试入口：执行完整日更流程。
 */
function test() {
  rebuildSignalAndReview_();
  //runEnhancedSystem();
  
  // buildMetrics_();
  // buildSignal_();
  // buildSignalReview_();
  //buildSignalRecent_(7);
}

/**
 * 主入口：抓取当日原始数据，并重建统一指标表与统一信号表。
 */
function runEnhancedSystem() {
  //var today = formatDate_("2026-03-09");
  var today = formatDate_(new Date());

  runDailyWide_(today);
  fetchPledgedRepoRates_();
  fetchBondFutures_();

  /**
   * 海外宏观原始表：
   * - 已配置 FRED / ALPHA_VANTAGE secrets 时自动抓取
   * - 未配置时仅打印提示并跳过，不阻塞现有国内数据流程
   */
  fetchOverseasMacro_();

  rebuildAll_();
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

/**
 * 重建所有中间指标与信号表。
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
 * 输出当前回补游标。
 */
function showBackfillCursor() {
  Logger.log('BACKFILL_CURSOR=' + getBackfillCursor_());
}

/**
 * 清空当前回补游标。
 */
function resetBackfillCursor() {
  clearBackfillCursor_();
  Logger.log('BACKFILL_CURSOR cleared');
}
