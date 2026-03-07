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
  rebuildAll_();

  Logger.log("========== backfill 结束 ==========");
  Logger.log("新增天数=" + insertedDays + " 跳过/无数据=" + skippedDays + " 失败=" + failedDays);
}

function backfillRecentDays_(days) {
  days = days || 120;
  var end = new Date();
  var start = new Date(end);
  start.setDate(end.getDate() - days);
  backfill(formatDate_(start), formatDate_(end));
}

function backfillLast120Days() {
  backfillRecentDays_(120);
}

/**
 * 新版分批回补：
 * - resetCursor=true  => 忽略旧 cursor，从 startDate 重新开始
 * - resetCursor=false => 从 BACKFILL_CURSOR 继续
 * - maxDaysPerRun 表示最多处理多少个“非周末日期”
 */
function backfillBatch_(startDate, endDate, maxDaysPerRun, resetCursor) {
  maxDaysPerRun = maxDaysPerRun || 8;

  var start = parseYMD_(startDate);
  var end = parseYMD_(endDate);

  if (!start || !end) throw new Error("日期格式错误");
  if (start > end) throw new Error("startDate 不能大于 endDate");

  if (resetCursor) {
    clearBackfillCursor_();
  }

  var cursor = getBackfillCursor_();
  var d = cursor ? parseYMD_(cursor) : new Date(start.getTime());

  // cursor 非法 / 越界时，强制拉回起点
  if (!d || d < start || d > end) {
    d = new Date(start.getTime());
    setBackfillCursor_(formatDate_(d));
  }

  Logger.log("========== backfillBatch_ 开始 ==========");
  Logger.log("startDate=" + formatDate_(start));
  Logger.log("endDate=" + formatDate_(end));
  Logger.log("cursor=" + formatDate_(d));
  Logger.log("maxDaysPerRun=" + maxDaysPerRun);
  Logger.log("resetCursor=" + !!resetCursor);

  var processed = 0;
  var failedDays = 0;

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
      failedDays++;
    }

    processed++;
    d.setDate(d.getDate() + 1);
    setBackfillCursor_(formatDate_(d));
    Utilities.sleep(1800 + Math.floor(Math.random() * 1800));
  }

  if (d > end) {
    Logger.log("✅ backfillBatch_ 已完成");
    clearBackfillCursor_();
    rebuildAll_();
  } else {
    Logger.log("⏸ 本轮完成，processed=" + processed + ", failed=" + failedDays);
    Logger.log("⏸ 下次从 cursor=" + getBackfillCursor_() + " 继续");
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