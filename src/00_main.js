/********************
 * 00_main.gs
 * 项目入口函数与运行编排。
 ********************/

function test() {
  runEnhancedSystem();
  //testBackfillSafe();
  //resumeBackfillSafe();
  //backfillLast120Days();
  //buildBondAllocationSignal_();
  //testMoneyMarketBackfill120();
  //backfill('2025-01-01','2025-1-31');
  
}

/**
 * 主入口：抓当日数据 + 重建派生表
 */
function runEnhancedSystem() {
  var today = formatDate_(new Date());

  // 原始数据
  runDailyWide_(today);
  fetchPledgedRepoRates_();
  fetchBondFutures_();

  // 派生表/面板
  rebuildAll_();
}

/**
 * 每次从“最近120天”起点重新开始，最多补 8 个非周末日期
 * 适合手工多次点击执行做安全回补
 */
function testBackfillSafe() {
  var end = new Date();
  var start = new Date(end);
  start.setDate(end.getDate() - 30);

  backfillBatch_(
    formatDate_(start),
    formatDate_(end),
    8,
    true   // 强制从头开始
  );
}

/**
 * 从上次 cursor 继续回补最近120天，每次最多补 8 个非周末日期
 */
function resumeBackfillSafe() {
  var end = new Date();
  var start = new Date(end);
  start.setDate(end.getDate() - 120);

  backfillBatch_(
    formatDate_(start),
    formatDate_(end),
    8,
    false  // 断点续跑
  );
}

/**
 * 只重建派生表和总览
 */
function rebuildAll_() {
  updateDashboard_();
  buildCurveHistory_();
  buildCurveSlope_();
  buildETFSignal_();
  buildBondAllocationSignal_();
  buildMacroDashboard_();
}

/**
 * 查看当前回补游标
 */
function showBackfillCursor() {
  Logger.log("BACKFILL_CURSOR=" + getBackfillCursor_());
}

/**
 * 清空当前回补游标
 */
function resetBackfillCursor() {
  clearBackfillCursor_();
  Logger.log("BACKFILL_CURSOR cleared");
}



function backfillMoneyMarketLast120Days() {
  var end = new Date();
  var start = new Date(end);
  start.setDate(end.getDate() - 120);
  backfillMoneyMarket(formatDate_(start), formatDate_(end));
}