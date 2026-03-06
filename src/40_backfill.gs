/********************
* 40_backfill.gs
* 历史回补、断点续跑与派生表重建。
********************/

function backfill(startDate, endDate) {
  var start = parseYMD_(startDate);
  var end = parseYMD_(endDate);

  if (!start || !end) {
    throw new Error("日期格式错误，示例：backfill('2025-11-01','2026-03-06')");
  }
  if (start > end) {
    throw new Error("startDate 不能大于 endDate");
  }

  Logger.log("========== backfill 开始 ==========");
  Logger.log("startDate=" + formatDate_(start));
  Logger.log("endDate=" + formatDate_(end));

  var insertedDays = 0;
  var skippedDays = 0;
  var failedDays = 0;

  var d = new Date(start.getTime());
  while (d <= end) {
    var ds = formatDate_(d);

    if (isWeekend_(d)) {
      Logger.log("⏭ 周末跳过: " + ds);
      d.setDate(d.getDate() + 1);
      continue;
    }

    try {
      Logger.log("----- 回补 " + ds + " -----");

      var before = countCurveRows_();
      runDailyWide_(ds);
      var after = countCurveRows_();

      if (after > before) insertedDays++;
      else skippedDays++;
    } catch (e) {
      Logger.log("❌ backfill 失败: " + ds + " err=" + e);
      failedDays++;
    }

    Utilities.sleep(1500 + Math.floor(Math.random() * 1500));
    d.setDate(d.getDate() + 1);
  }

  Logger.log("========== 开始重建派生表 ==========");
  updateDashboard_();
  buildCurveHistory_();
  buildCurveSlope_();
  buildETFSignal_();
  buildBondAllocationSignal_();

  Logger.log("========== backfill 结束 ==========");
  Logger.log("新增天数=" + insertedDays + " 跳过/无数据=" + skippedDays + " 失败=" + failedDays);
}

function backfillRecentDays_(days) {
  days = days || 120;

  var end = new Date();
  var start = new Date();
  start.setDate(end.getDate() - days);

  backfill(formatDate_(start), formatDate_(end));
}

function backfillLast120Days() {
  backfillRecentDays_(120);
}

// 断点续跑版
function backfillResumeable_(startDate, endDate, maxDaysPerRun) {
  maxDaysPerRun = maxDaysPerRun || 20;

  var start = parseYMD_(startDate);
  var end = parseYMD_(endDate);
  if (!start || !end) throw new Error("日期格式错误");

  var cursor = getBackfillCursor_();
  var d = cursor ? parseYMD_(cursor) : new Date(start.getTime());

  if (!d || d < start || d > end) {
    d = new Date(start.getTime());
    setBackfillCursor_(formatDate_(d));
  }

  var processed = 0;

  while (d <= end && processed < maxDaysPerRun) {
    var ds = formatDate_(d);

    if (isWeekend_(d)) {
      Logger.log("⏭ 周末跳过: " + ds);
      d.setDate(d.getDate() + 1);
      setBackfillCursor_(formatDate_(d));
      continue;
    }

    try {
      Logger.log("----- 回补 " + ds + " -----");
      runDailyWide_(ds);
    } catch (e) {
      Logger.log("❌ 回补失败: " + ds + " err=" + e);
    }

    processed++;
    d.setDate(d.getDate() + 1);
    setBackfillCursor_(formatDate_(d));

    Utilities.sleep(1800 + Math.floor(Math.random() * 1800));
  }

  if (d > end) {
    Logger.log("✅ backfillResumeable_ 已完成");
    clearBackfillCursor_();
    updateDashboard_();
    buildCurveHistory_();
    buildCurveSlope_();
    buildETFSignal_();
    buildBondAllocationSignal_();
  } else {
    Logger.log("⏸ 本轮完成，cursor=" + getBackfillCursor_());
  }
}

function countCurveRows_() {
  var ss = SpreadsheetApp.getActive();
  var sh = ss.getSheetByName(SHEET_CURVE);
  if (!sh) return 0;
  return sh.getLastRow();
}

function getBackfillCursor_() {
  return PropertiesService.getScriptProperties().getProperty("BACKFILL_CURSOR") || "";
}

function setBackfillCursor_(dateStr) {
  PropertiesService.getScriptProperties().setProperty("BACKFILL_CURSOR", dateStr);
}

function clearBackfillCursor_() {
  PropertiesService.getScriptProperties().deleteProperty("BACKFILL_CURSOR");
}


/********************
* 工具函数
********************/
