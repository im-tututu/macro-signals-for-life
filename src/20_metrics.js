/********************
 * 20_metrics.js
 * 统一生成“指标_利率”宽表。
 *
 * 设计原则：
 * 1) 先放关键点位
 * 2) 再放期限结构 / 利差结构
 * 3) 最后补滚动均线和分位数
 *
 * 注意：部分曲线（AA+信用 / AAA+中票 / AAA城投）当前原始表主要只有 1Y 以内期限，
 * 因此指标口径顺着源数据走，不强行生成 3Y/5Y 代表点。
 ********************/

function buildMetrics_() {
  Logger.log('buildMetrics_ v3 running');

  var ss = SpreadsheetApp.getActive();
  var src = ss.getSheetByName(SHEET_CURVE_RAW);
  if (!src) throw new Error('找不到工作表: ' + SHEET_CURVE_RAW);

  var dst = ss.getSheetByName(SHEET_METRICS) || ss.insertSheet(SHEET_METRICS);
  var values = src.getDataRange().getValues();
  var header = buildMetricsHeader_();

  if (values.length < 2) {
    writeMetricsOutput_(dst, [header]);
    return;
  }

  var rawHeader = values[0];
  var termIndex = buildTermColumnIndex_(rawHeader);

  var byDate = {};
  for (var i = 1; i < values.length; i++) {
    var row = values[i];
    var dateKey = normYMD_(row[0]);
    var curveName = normalizeCurveName_(row[1]);
    if (!dateKey || !curveName) continue;

    if (!byDate[dateKey]) byDate[dateKey] = {};
    byDate[dateKey][curveName] = row;
  }

  var dates = Object.keys(byDate).sort(); // 先升序计算 rolling
  var rows = [];
  for (var j = 0; j < dates.length; j++) {
    rows.push(buildMetricsBaseRow_(dates[j], byDate[dates[j]], termIndex));
  }

  applyRollingMetrics_(rows);
  rows.reverse(); // 输出逆序：最新在最上面

  var out = [header];
  for (var k = 0; k < rows.length; k++) {
    out.push(metricsRowToArray_(rows[k], header));
  }

  writeMetricsOutput_(dst, out);
  Logger.log(SHEET_METRICS + ' 已重建，共 ' + Math.max(0, out.length - 1) + ' 条');
}

function buildMetricsHeader_() {
  return [
    'date',

    'gov_1y', 'gov_3y', 'gov_5y', 'gov_10y', 'gov_30y',
    'cdb_3y', 'cdb_5y', 'cdb_10y',

    'aaa_credit_1y', 'aaa_credit_3y', 'aaa_credit_5y',
    'aa_plus_credit_1y',

    'aaa_plus_mtn_1y',
    'aaa_mtn_1y', 'aaa_mtn_3y', 'aaa_mtn_5y',

    'aaa_ncd_1y',

    'aaa_bank_bond_1y', 'aaa_bank_bond_3y', 'aaa_bank_bond_5y',
    'aaa_lgfv_1y',

    'local_gov_5y', 'local_gov_10y',

    'gov_slope_10_1',
    'gov_slope_10_3',
    'gov_slope_30_10',
    'cdb_slope_10_3',

    'spread_cdb_gov_3y',
    'spread_cdb_gov_5y',
    'spread_cdb_gov_10y',

    'spread_local_gov_gov_5y',
    'spread_local_gov_gov_10y',

    'spread_aaa_credit_gov_1y',
    'spread_aaa_credit_gov_3y',
    'spread_aaa_credit_gov_5y',

    'spread_aa_plus_vs_aaa_credit_1y',
    'spread_aaa_plus_mtn_gov_1y',

    'spread_aaa_mtn_vs_aaa_plus_mtn_1y',
    'spread_aaa_credit_ncd_1y',

    'spread_aaa_bank_vs_aaa_credit_1y',
    'spread_aaa_bank_vs_aaa_credit_3y',
    'spread_aaa_bank_vs_aaa_credit_5y',

    'spread_aaa_lgfv_vs_aaa_credit_1y',

    'gov_10y_ma20',
    'gov_10y_ma60',
    'gov_10y_ma120',
    'gov_10y_pct250',

    'spread_cdb_gov_10y_ma20',
    'spread_cdb_gov_10y_pct250',

    'spread_aaa_credit_gov_5y_ma20',
    'spread_aaa_credit_gov_5y_pct250',

    'spread_aa_plus_vs_aaa_credit_1y_ma20',
    'spread_aa_plus_vs_aaa_credit_1y_pct250',

    'aaa_ncd_1y_ma20',
    'aaa_ncd_1y_pct250'
  ];
}

function buildTermColumnIndex_(rawHeader) {
  var index = {};
  for (var i = 0; i < rawHeader.length; i++) {
    index[String(rawHeader[i]).trim()] = i;
  }
  return index;
}

function buildMetricsBaseRow_(dateKey, bucket, termIndex) {
  var row = { date: dateKey };

  row.gov_1y = getCurvePoint_(bucket, '国债', 'Y_1', termIndex);
  row.gov_3y = getCurvePoint_(bucket, '国债', 'Y_3', termIndex);
  row.gov_5y = getCurvePoint_(bucket, '国债', 'Y_5', termIndex);
  row.gov_10y = getCurvePoint_(bucket, '国债', 'Y_10', termIndex);
  row.gov_30y = getCurvePoint_(bucket, '国债', 'Y_30', termIndex);

  row.cdb_3y = getCurvePoint_(bucket, '国开债', 'Y_3', termIndex);
  row.cdb_5y = getCurvePoint_(bucket, '国开债', 'Y_5', termIndex);
  row.cdb_10y = getCurvePoint_(bucket, '国开债', 'Y_10', termIndex);

  row.aaa_credit_1y = getCurvePoint_(bucket, 'AAA信用', 'Y_1', termIndex);
  row.aaa_credit_3y = getCurvePoint_(bucket, 'AAA信用', 'Y_3', termIndex);
  row.aaa_credit_5y = getCurvePoint_(bucket, 'AAA信用', 'Y_5', termIndex);

  row.aa_plus_credit_1y = getCurvePoint_(bucket, 'AA+信用', 'Y_1', termIndex);

  row.aaa_plus_mtn_1y = getCurvePoint_(bucket, 'AAA+中票', 'Y_1', termIndex);

  row.aaa_mtn_1y = getCurvePoint_(bucket, 'AAA中票', 'Y_1', termIndex);
  row.aaa_mtn_3y = getCurvePoint_(bucket, 'AAA中票', 'Y_3', termIndex);
  row.aaa_mtn_5y = getCurvePoint_(bucket, 'AAA中票', 'Y_5', termIndex);

  row.aaa_ncd_1y = getCurvePoint_(bucket, 'AAA存单', 'Y_1', termIndex);

  row.aaa_bank_bond_1y = getCurvePoint_(bucket, 'AAA银行债', 'Y_1', termIndex);
  row.aaa_bank_bond_3y = getCurvePoint_(bucket, 'AAA银行债', 'Y_3', termIndex);
  row.aaa_bank_bond_5y = getCurvePoint_(bucket, 'AAA银行债', 'Y_5', termIndex);

  row.aaa_lgfv_1y = getCurvePoint_(bucket, 'AAA城投', 'Y_1', termIndex);

  row.local_gov_5y = getCurvePoint_(bucket, '地方债', 'Y_5', termIndex);
  row.local_gov_10y = getCurvePoint_(bucket, '地方债', 'Y_10', termIndex);

  row.gov_slope_10_1 = safeSubOrBlank_(row.gov_10y, row.gov_1y);
  row.gov_slope_10_3 = safeSubOrBlank_(row.gov_10y, row.gov_3y);
  row.gov_slope_30_10 = safeSubOrBlank_(row.gov_30y, row.gov_10y);
  row.cdb_slope_10_3 = safeSubOrBlank_(row.cdb_10y, row.cdb_3y);

  row.spread_cdb_gov_3y = safeSubOrBlank_(row.cdb_3y, row.gov_3y);
  row.spread_cdb_gov_5y = safeSubOrBlank_(row.cdb_5y, row.gov_5y);
  row.spread_cdb_gov_10y = safeSubOrBlank_(row.cdb_10y, row.gov_10y);

  row.spread_local_gov_gov_5y = safeSubOrBlank_(row.local_gov_5y, row.gov_5y);
  row.spread_local_gov_gov_10y = safeSubOrBlank_(row.local_gov_10y, row.gov_10y);

  row.spread_aaa_credit_gov_1y = safeSubOrBlank_(row.aaa_credit_1y, row.gov_1y);
  row.spread_aaa_credit_gov_3y = safeSubOrBlank_(row.aaa_credit_3y, row.gov_3y);
  row.spread_aaa_credit_gov_5y = safeSubOrBlank_(row.aaa_credit_5y, row.gov_5y);

  row.spread_aa_plus_vs_aaa_credit_1y = safeSubOrBlank_(row.aa_plus_credit_1y, row.aaa_credit_1y);
  row.spread_aaa_plus_mtn_gov_1y = safeSubOrBlank_(row.aaa_plus_mtn_1y, row.gov_1y);

  row.spread_aaa_mtn_vs_aaa_plus_mtn_1y = safeSubOrBlank_(row.aaa_mtn_1y, row.aaa_plus_mtn_1y);
  row.spread_aaa_credit_ncd_1y = safeSubOrBlank_(row.aaa_credit_1y, row.aaa_ncd_1y);

  row.spread_aaa_bank_vs_aaa_credit_1y = safeSubOrBlank_(row.aaa_bank_bond_1y, row.aaa_credit_1y);
  row.spread_aaa_bank_vs_aaa_credit_3y = safeSubOrBlank_(row.aaa_bank_bond_3y, row.aaa_credit_3y);
  row.spread_aaa_bank_vs_aaa_credit_5y = safeSubOrBlank_(row.aaa_bank_bond_5y, row.aaa_credit_5y);

  row.spread_aaa_lgfv_vs_aaa_credit_1y = safeSubOrBlank_(row.aaa_lgfv_1y, row.aaa_credit_1y);

  row.gov_10y_ma20 = '';
  row.gov_10y_ma60 = '';
  row.gov_10y_ma120 = '';
  row.gov_10y_pct250 = '';

  row.spread_cdb_gov_10y_ma20 = '';
  row.spread_cdb_gov_10y_pct250 = '';

  row.spread_aaa_credit_gov_5y_ma20 = '';
  row.spread_aaa_credit_gov_5y_pct250 = '';

  row.spread_aa_plus_vs_aaa_credit_1y_ma20 = '';
  row.spread_aa_plus_vs_aaa_credit_1y_pct250 = '';

  row.aaa_ncd_1y_ma20 = '';
  row.aaa_ncd_1y_pct250 = '';

  return row;
}

function applyRollingMetrics_(rows) {
  var gov10Arr = [];
  var cdbGov10Arr = [];
  var aaaCreditGov5Arr = [];
  var sink1Arr = [];
  var ncd1Arr = [];

  for (var i = 0; i < rows.length; i++) {
    var r = rows[i];

    gov10Arr.push(toNumberOrNull_(r.gov_10y));
    cdbGov10Arr.push(toNumberOrNull_(r.spread_cdb_gov_10y));
    aaaCreditGov5Arr.push(toNumberOrNull_(r.spread_aaa_credit_gov_5y));
    sink1Arr.push(toNumberOrNull_(r.spread_aa_plus_vs_aaa_credit_1y));
    ncd1Arr.push(toNumberOrNull_(r.aaa_ncd_1y));

    r.gov_10y_ma20 = rollingMeanAllowBlank_(gov10Arr, 20);
    r.gov_10y_ma60 = rollingMeanAllowBlank_(gov10Arr, 60);
    r.gov_10y_ma120 = rollingMeanAllowBlank_(gov10Arr, 120);
    r.gov_10y_pct250 = rollingPercentileRankAllowBlank_(gov10Arr, 250);

    r.spread_cdb_gov_10y_ma20 = rollingMeanAllowBlank_(cdbGov10Arr, 20);
    r.spread_cdb_gov_10y_pct250 = rollingPercentileRankAllowBlank_(cdbGov10Arr, 250);

    r.spread_aaa_credit_gov_5y_ma20 = rollingMeanAllowBlank_(aaaCreditGov5Arr, 20);
    r.spread_aaa_credit_gov_5y_pct250 = rollingPercentileRankAllowBlank_(aaaCreditGov5Arr, 250);

    r.spread_aa_plus_vs_aaa_credit_1y_ma20 = rollingMeanAllowBlank_(sink1Arr, 20);
    r.spread_aa_plus_vs_aaa_credit_1y_pct250 = rollingPercentileRankAllowBlank_(sink1Arr, 250);

    r.aaa_ncd_1y_ma20 = rollingMeanAllowBlank_(ncd1Arr, 20);
    r.aaa_ncd_1y_pct250 = rollingPercentileRankAllowBlank_(ncd1Arr, 250);
  }
}

function normalizeCurveName_(name) {
  return String(name == null ? '' : name)
    .replace(/＋/g, '+')
    .replace(/\u3000/g, ' ')
    .replace(/\s+/g, '')
    .trim();
}

function getCurvePoint_(bucket, curveName, colName, termIndex) {
  var key = normalizeCurveName_(curveName);
  var row = bucket[key];
  if (!row) return '';

  var idx = termIndex[colName];
  if (idx == null) return '';

  var n = toNumberOrNull_(row[idx]);
  return isFiniteNumber_(n) ? n : '';
}

function metricsRowToArray_(rowObj, header) {
  var out = [];
  for (var i = 0; i < header.length; i++) {
    var key = header[i];
    out.push(rowObj.hasOwnProperty(key) ? rowObj[key] : '');
  }
  return out;
}

function writeMetricsOutput_(sheet, out) {
  sheet.clearContents();
  sheet.clearFormats();
  sheet.getRange(1, 1, out.length, out[0].length).setValues(out);

  if (out.length > 1) {
    sheet.getRange(2, 1, out.length - 1, 1).setNumberFormat('yyyy-mm-dd');
    sheet.getRange(2, 2, out.length - 1, out[0].length - 1).setNumberFormat('0.0000');
  }

  sheet.setFrozenRows(1);
  sheet.getRange(1, 1, 1, out[0].length)
    .setFontWeight('bold')
    .setBackground('#d9eaf7');
  sheet.autoResizeColumns(1, out[0].length);
}

function safeSubOrBlank_(a, b) {
  if (!isFiniteNumber_(a) || !isFiniteNumber_(b)) return '';
  return a - b;
}

function rollingMeanAllowBlank_(arr, windowSize) {
  if (!arr || arr.length < windowSize || windowSize <= 0) return '';
  var start = arr.length - windowSize;
  var sum = 0;
  for (var i = start; i < arr.length; i++) {
    var v = arr[i];
    if (!isFiniteNumber_(v)) return '';
    sum += v;
  }
  return sum / windowSize;
}

function rollingPercentileRankAllowBlank_(arr, windowSize) {
  if (!arr || arr.length < windowSize || windowSize <= 0) return '';
  var slice = arr.slice(arr.length - windowSize);
  var currentValue = slice[slice.length - 1];
  if (!isFiniteNumber_(currentValue)) return '';

  var n = 0;
  var leCount = 0;
  for (var i = 0; i < slice.length; i++) {
    var v = slice[i];
    if (!isFiniteNumber_(v)) return '';
    n++;
    if (v <= currentValue) leCount++;
  }
  if (n < windowSize) return '';
  return leCount / n;
}

function buildRateMetrics_() { buildMetrics_(); }
function updateDashboard_() { buildMetrics_(); }
function buildCurveHistory_() { buildMetrics_(); }
function buildCurveSlope_() { buildMetrics_(); }
function rebuildCurveHistory_() { buildMetrics_(); }
function appendCurveHistoryRows_(rows) {
  if (!rows || !rows.length) return;
  buildMetrics_();
}
