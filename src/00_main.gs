/********************
* 00_main.gs
* 项目入口函数与运行编排。
********************/

function test() {
  runEnhancedSystem();
}

// 安全回补：每次补 8 个交易日，反复运行直到完成
function testBackfillSafe() {
  backfillResumeable_("2025-11-06", "2026-03-06", 8);
}

// 只重建派生表和面板
function rebuildAll_() {
  updateDashboard_();
  buildCurveHistory_();
  buildCurveSlope_();
  buildETFSignal_();
  buildBondAllocationSignal_();
  buildMacroDashboard_();
}


/********************
* 最终增强版：利率终端（Google Apps Script）
********************/

/********** 配置区 **********/
