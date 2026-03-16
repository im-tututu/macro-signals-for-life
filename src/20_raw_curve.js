/********************
 * 20_raw_curve.js
 * 中债收益率曲线原始表。
 *
 * 职责：
 * 1) 调用 10_source_chinabond.js 抓取曲线
 * 2) 维护“原始_收益率曲线”表头与固定期限落表
 * 3) 提供历史回补、断点续跑入口
 ********************/


function runDailyWide_(date) {
  var ss = SpreadsheetApp.getActive();
  var sheet = ss.getSheetByName(SHEET_CURVE_RAW) || ss.insertSheet(SHEET_CURVE_RAW);

  ensureCurveHeader_(sheet);

  var index = buildCurveIndex_(sheet);
  var batchCurves = CURVES.filter(function(c) {
    return !c.fetch_separately;
  });
  var singleCurves = CURVES.filter(function(c) {
    return !!c.fetch_separately;
  });

  Logger.log('曲线数: total=' + CURVES.length + ' batch=' + batchCurves.length + ' single=' + singleCurves.length);

  var batchBlocks = [];
  var usedBlockIndex = {};
  if (batchCurves.length) {
    var batchIds = batchCurves.map(function(c) {
      return c.id;
    });
    var html = fetchChinaBondCurves_(date, batchIds);
    batchBlocks = parseChinaBondCurveBlocks_(html);
  }

  var inserted = 0;
  var skipped = 0;
  var failed = 0;

  for (var i = 0; i < CURVES.length; i++) {
    var curve = CURVES[i];
    var key = date + '|' + curve.name;

    if (index.has(key)) {
      Logger.log('⏭ 跳过(已存在): ' + key);
      skipped++;
      continue;
    }

    var matched = null;
    if (curve.fetch_separately) {
      matched = fetchChinaBondCurveSeparately_(date, curve);
    } else {
      matched = resolveCurveBlock_(curve, batchBlocks, usedBlockIndex, findCurveRequestIndex_(batchCurves, curve.name));
    }

    var map = matched ? matched.map : null;
    if (!map || map.size === 0) {
      Logger.log('❌ 无数据/未解析到: ' + curve.name + ' id=' + curve.id + ' mode=' + (curve.fetch_separately ? 'single' : 'batch'));
      failed++;
      continue;
    }

    try {
      appendCurveRowFixed_(sheet, date, curve.name, map);
      Logger.log('✅ 插入: ' + key + ' 节点=' + map.size + ' sourceTitle=' + matched.title + ' mode=' + (curve.fetch_separately ? 'single' : 'batch'));
      inserted++;
    } catch (e) {
      Logger.log('❌ 插入失败: ' + key + ' err=' + e);
      failed++;
    }
  }

  Logger.log('yc_curve 新增=' + inserted + ' 跳过=' + skipped + ' 失败=' + failed);
}

/**
 * 抓取中债收益率曲线原始 HTML，并使用脚本缓存减少重复请求。
 */


function ensureCurveHeader_(sheet) {
  if (sheet.getLastRow() > 0) return;

  var header = ['date', 'curve'];
  for (var i = 0; i < TERMS.length; i++) {
    header.push('Y_' + TERMS[i]);
  }
  sheet.appendRow(header);
}

/**
 * 按 TERMS 固定列顺序追加一行曲线数据。
 */


function appendCurveRowFixed_(sheet, date, curveName, map) {
  var row = [date, curveName];
  for (var i = 0; i < TERMS.length; i++) {
    var term = TERMS[i];
    row.push(map.has(term) ? map.get(term) : '');
  }
  sheet.appendRow(row);
}

/**
 * 构建 date|curve 唯一键索引，用于避免重复写入。
 */


function buildCurveIndex_(sheet) {
  var last = sheet.getLastRow();
  var set = new Set();
  if (last < 2) return set;

  var values = sheet.getRange(2, 1, last - 1, 2).getValues();
  for (var i = 0; i < values.length; i++) {
    var dateValue = values[i][0];
    var curveName = values[i][1];
    if (!dateValue || !curveName) continue;
    set.add(normYMD_(dateValue) + '|' + curveName);
  }
  return set;
}



function backfill(startDate, endDate) {
  var start = parseYMD_(startDate);
  var end = parseYMD_(endDate);

  if (!start || !end) {
    throw new Error("日期格式错误，示例：backfill('2025-11-01','2026-03-06')");
  }
  if (start > end) {
    throw new Error('startDate 不能大于 endDate');
  }

  Logger.log('========== backfill 开始 ==========' );
  Logger.log('startDate=' + formatDate_(start));
  Logger.log('endDate=' + formatDate_(end));

  var insertedDays = 0;
  var skippedDays = 0;
  var failedDays = 0;
  var current = new Date(start.getTime());

  while (current <= end) {
    var dateStr = formatDate_(current);

    if (isWeekend_(current)) {
      Logger.log('⏭ 周末跳过: ' + dateStr);
      current.setDate(current.getDate() + 1);
      continue;
    }

    try {
      Logger.log('----- 回补 ' + dateStr + ' -----');
      var before = countCurveRows_();
      runDailyWide_(dateStr);
      var after = countCurveRows_();

      if (after > before) {
        insertedDays++;
      } else {
        skippedDays++;
      }
    } catch (e) {
      Logger.log('❌ backfill 失败: ' + dateStr + ' err=' + e);
      failedDays++;
    }

    Utilities.sleep(1500 + Math.floor(Math.random() * 1500));
    current.setDate(current.getDate() + 1);
  }

  Logger.log('========== 开始重建派生表 ==========' );
  rebuildAll_();

  Logger.log('========== backfill 结束 ==========' );
  Logger.log('新增天数=' + insertedDays + ' 跳过/无数据=' + skippedDays + ' 失败=' + failedDays);
}

/**
 * 回补最近若干天的数据。
 */


function backfillRecentDays_(days) {
  days = days || 120;
  var end = new Date();
  var start = new Date(end);
  start.setDate(end.getDate() - days);
  backfill(formatDate_(start), formatDate_(end));
}

/**
 * 回补最近 120 天的数据。
 */


function backfillLast120Days() {
  backfillRecentDays_(120);
}

/**
 * 按游标分批回补，适合手工多次执行。
 * - resetCursor=true  时从 startDate 重新开始
 * - resetCursor=false 时从 BACKFILL_CURSOR 继续
 * - maxDaysPerRun 表示最多处理多少个非周末日期
 */


function backfillBatch_(startDate, endDate, maxDaysPerRun, resetCursor) {
  maxDaysPerRun = maxDaysPerRun || 8;

  var start = parseYMD_(startDate);
  var end = parseYMD_(endDate);
  if (!start || !end) throw new Error('日期格式错误');
  if (start > end) throw new Error('startDate 不能大于 endDate');

  if (resetCursor) {
    clearBackfillCursor_();
  }

  var cursor = getBackfillCursor_();
  var current = cursor ? parseYMD_(cursor) : new Date(start.getTime());
  if (!current || current < start || current > end) {
    current = new Date(start.getTime());
    setBackfillCursor_(formatDate_(current));
  }

  Logger.log('========== backfillBatch_ 开始 ==========' );
  Logger.log('startDate=' + formatDate_(start));
  Logger.log('endDate=' + formatDate_(end));
  Logger.log('cursor=' + formatDate_(current));
  Logger.log('maxDaysPerRun=' + maxDaysPerRun);
  Logger.log('resetCursor=' + !!resetCursor);

  var processed = 0;
  var failedDays = 0;

  while (current <= end && processed < maxDaysPerRun) {
    var dateStr = formatDate_(current);

    if (isWeekend_(current)) {
      Logger.log('⏭ 周末跳过: ' + dateStr);
      current.setDate(current.getDate() + 1);
      setBackfillCursor_(formatDate_(current));
      continue;
    }

    try {
      Logger.log('----- 回补 ' + dateStr + ' -----');
      runDailyWide_(dateStr);
    } catch (e) {
      Logger.log('❌ 回补失败: ' + dateStr + ' err=' + e);
      failedDays++;
    }

    processed++;
    current.setDate(current.getDate() + 1);
    setBackfillCursor_(formatDate_(current));
    Utilities.sleep(1800 + Math.floor(Math.random() * 1800));
  }

  if (current > end) {
    Logger.log('✅ backfillBatch_ 已完成');
    clearBackfillCursor_();
    rebuildAll_();
  } else {
    Logger.log('⏸ 本轮完成，processed=' + processed + ', failed=' + failedDays);
    Logger.log('⏸ 下次从 cursor=' + getBackfillCursor_() + ' 继续');
  }
}

/**
 * 统计 yc_curve 当前行数。
 */


function countCurveRows_() {
  var ss = SpreadsheetApp.getActive();
  var sh = ss.getSheetByName(SHEET_CURVE_RAW);
  if (!sh) return 0;
  return sh.getLastRow();
}

/**
 * 读取回补游标。
 */


function getBackfillCursor_() {
  return PropertiesService.getScriptProperties().getProperty('BACKFILL_CURSOR') || '';
}

/**
 * 写入回补游标。
 */


function setBackfillCursor_(dateStr) {
  PropertiesService.getScriptProperties().setProperty('BACKFILL_CURSOR', dateStr);
}

/**
 * 清空回补游标。
 */


function clearBackfillCursor_() {
  PropertiesService.getScriptProperties().deleteProperty('BACKFILL_CURSOR');
}

