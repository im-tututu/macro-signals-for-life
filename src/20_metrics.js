/********************
 * 20_metrics.js
 * 利率指标宽表：统一生成关键期限、曲线斜率与利差指标。
 *
 * 输出 Sheet：SHEET_METRICS（指标_利率）
 * 主要字段：
 *   - date
 *   - gov_1y / gov_3y / gov_5y / gov_10y
 *   - cdb_10y / aaa_5y
 *   - slope_10_1 / slope_10_3 / slope_5_1
 *   - policy_spread / credit_spread / term_spread
 ********************/

/**
 * 构建统一的利率指标宽表。
 */
function buildMetrics_() {
  var ss = SpreadsheetApp.getActive();
  var src = ss.getSheetByName(SHEET_CURVE_RAW);
  if (!src) return;

  var dst = ss.getSheetByName(SHEET_METRICS) || ss.insertSheet(SHEET_METRICS);
  var data = src.getDataRange().getValues();
  if (data.length < 2) {
    ensureMetricsHeader_(dst);
    return;
  }

  var header = data[0];
  var idxY1 = header.indexOf('Y_1');
  var idxY3 = header.indexOf('Y_3');
  var idxY5 = header.indexOf('Y_5');
  var idxY10 = header.indexOf('Y_10');

  if (idxY1 < 0 || idxY3 < 0 || idxY5 < 0 || idxY10 < 0) {
    Logger.log('❌ 原始收益率曲线缺少 Y_1/Y_3/Y_5/Y_10 列');
    return;
  }

  var byDate = {};
  for (var i = 1; i < data.length; i++) {
    var row = data[i];
    var dateValue = row[0];
    var curveName = row[1];
    if (!dateValue || !curveName) continue;

    var key = normYMD_(dateValue);
    if (!byDate[key]) byDate[key] = {};
    byDate[key][curveName] = row;
  }

  var dates = Object.keys(byDate).sort();
  var out = [buildMetricsHeader_()];

  for (var j = 0; j < dates.length; j++) {
    var dateKey = dates[j];
    var bucket = byDate[dateKey];
    var gov = bucket['国债'];
    if (!gov) continue;

    var cdb = bucket['国开债'];
    var aaa = bucket['AAA信用'];

    var gov1 = toNumberOrBlank_(gov[idxY1]);
    var gov3 = toNumberOrBlank_(gov[idxY3]);
    var gov5 = toNumberOrBlank_(gov[idxY5]);
    var gov10 = toNumberOrBlank_(gov[idxY10]);
    var cdb10 = cdb ? toNumberOrBlank_(cdb[idxY10]) : '';
    var aaa5 = aaa ? toNumberOrBlank_(aaa[idxY5]) : '';

    var slope10_1 = safeSubOrBlank_(gov10, gov1);
    var slope10_3 = safeSubOrBlank_(gov10, gov3);
    var slope5_1 = safeSubOrBlank_(gov5, gov1);
    var policySpread = safeSubOrBlank_(cdb10, gov10);
    var creditSpread = safeSubOrBlank_(aaa5, gov5);
    var termSpread = slope10_1;

    out.push([
      dateKey,
      gov1,
      gov3,
      gov5,
      gov10,
      cdb10,
      aaa5,
      slope10_1,
      slope10_3,
      slope5_1,
      policySpread,
      creditSpread,
      termSpread
    ]);
  }

  dst.clearContents();
  dst.clearFormats();
  dst.getRange(1, 1, out.length, out[0].length).setValues(out);

  if (out.length > 1) {
    dst.getRange(2, 1, out.length - 1, 1).setNumberFormat('yyyy-mm-dd');
    dst.getRange(2, 2, out.length - 1, out[0].length - 1).setNumberFormat('0.0000');
  }

  dst.setFrozenRows(1);
  dst.getRange(1, 1, 1, out[0].length).setFontWeight('bold').setBackground('#d9eaf7');
  dst.autoResizeColumns(1, out[0].length);

  Logger.log(SHEET_METRICS + ' 已重建，共 ' + Math.max(0, out.length - 1) + ' 条');
}

function buildMetricsHeader_() {
  return [
    'date',
    'gov_1y',
    'gov_3y',
    'gov_5y',
    'gov_10y',
    'cdb_10y',
    'aaa_5y',
    'slope_10_1',
    'slope_10_3',
    'slope_5_1',
    'policy_spread',
    'credit_spread',
    'term_spread'
  ];
}

function ensureMetricsHeader_(sheet) {
  if (sheet.getLastRow() > 0) return;
  sheet.appendRow(buildMetricsHeader_());
}

function toNumberOrBlank_(value) {
  var n = toNumberOrNull_(value);
  return isFiniteNumber_(n) ? n : '';
}

function safeSubOrBlank_(a, b) {
  if (!isFiniteNumber_(a) || !isFiniteNumber_(b)) return '';
  return a - b;
}

/**
 * 兼容旧入口：历史上各指标表已并入统一宽表。
 */
function buildRateMetrics_() {
  buildMetrics_();
}

function updateDashboard_() {
  buildMetrics_();
}

function buildCurveHistory_() {
  buildMetrics_();
}

function buildCurveSlope_() {
  buildMetrics_();
}

function rebuildCurveHistory_() {
  buildMetrics_();
}

function appendCurveHistoryRows_(rows) {
  if (!rows || !rows.length) return;
  buildMetrics_();
}
