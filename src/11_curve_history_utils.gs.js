/***************************************
 * 11_curve_history_utils.gs
 ***************************************/

function rebuildCurveHistory_() {
  var ss = SpreadsheetApp.getActiveSpreadsheet();
  var sheet = mustGetSheet_(ss, 'curve_history');

  var values = sheet.getDataRange().getValues();
  if (values.length < 2) {
    Logger.log('curve_history 无数据');
    return;
  }

  var header = values[0];
  var idx = buildHeaderIndex_(header);

  var dateCol   = idx['date'];
  var gov1yCol  = idx['gov_1y'];
  var gov3yCol  = idx['gov_3y'];
  var gov5yCol  = idx['gov_5y'];
  var gov10yCol = idx['gov_10y'];

  requireColumn_(dateCol, 'curve_history.date');
  requireColumn_(gov10yCol, 'curve_history.gov_10y');

  var map = {};

  for (var i = 1; i < values.length; i++) {
    var r = values[i];
    var dateObj = normalizeSheetDate_(r[dateCol]);
    var gov10y = toNumberOrNull_(r[gov10yCol]);

    if (!dateObj || !isFiniteNumber_(gov10y)) continue;

    var key = formatDateKey_(dateObj);
    map[key] = [
      dateObj,
      gov1yCol == null ? null : toNumberOrNull_(r[gov1yCol]),
      gov3yCol == null ? null : toNumberOrNull_(r[gov3yCol]),
      gov5yCol == null ? null : toNumberOrNull_(r[gov5yCol]),
      gov10y
    ];
  }

  var rows = Object.keys(map).map(function (k) { return map[k]; });

  rows.sort(function (a, b) {
    return a[0].getTime() - b[0].getTime();
  });

  sheet.clearContents();
  sheet.clearFormats();

  var outHeader = [['date', 'gov_1y', 'gov_3y', 'gov_5y', 'gov_10y']];
  sheet.getRange(1, 1, 1, 5).setValues(outHeader);

  if (rows.length) {
    sheet.getRange(2, 1, rows.length, 5).setValues(rows);
    sheet.getRange(2, 1, rows.length, 1).setNumberFormat('yyyy-mm-dd');
    sheet.getRange(2, 2, rows.length, 4).setNumberFormat('0.0000');
  }

  sheet.setFrozenRows(1);
  sheet.getRange(1, 1, 1, 5).setFontWeight('bold').setBackground('#d9eaf7');
  sheet.autoResizeColumns(1, 5);

  Logger.log('curve_history 已重建，共 ' + rows.length + ' 条');
}

/**
 * 如需手工追加历史数据，可用这个函数
 * rows 格式：
 * [
 *   [dateObjOrString, gov_1y, gov_3y, gov_5y, gov_10y],
 *   ...
 * ]
 */
function appendCurveHistoryRows_(rows) {
  var ss = SpreadsheetApp.getActiveSpreadsheet();
  var sheet = mustGetSheet_(ss, 'curve_history');

  if (!rows || !rows.length) return;

  var startRow = sheet.getLastRow() + 1;
  sheet.getRange(startRow, 1, rows.length, 5).setValues(rows);

  rebuildCurveHistory_();
}

/***************************************
 * Helpers
 ***************************************/

function mustGetSheet_(ss, name) {
  var sheet = ss.getSheetByName(name);
  if (!sheet) throw new Error('找不到工作表: ' + name);
  return sheet;
}

function buildHeaderIndex_(headerRow) {
  var idx = {};
  for (var i = 0; i < headerRow.length; i++) {
    var k = normalizeHeader_(headerRow[i]);
    if (k) idx[k] = i;
  }
  return idx;
}

function normalizeHeader_(v) {
  if (v == null) return '';
  return String(v).trim().toLowerCase();
}

function requireColumn_(colIndex, label) {
  if (colIndex == null || colIndex < 0) {
    throw new Error('缺少列: ' + label);
  }
}

function toNumberOrNull_(v) {
  if (v == null || v === '') return null;
  var n = Number(v);
  return isNaN(n) ? null : n;
}

function isFiniteNumber_(v) {
  return typeof v === 'number' && isFinite(v);
}

function normalizeSheetDate_(v) {
  if (Object.prototype.toString.call(v) === '[object Date]' && !isNaN(v.getTime())) {
    return new Date(v.getFullYear(), v.getMonth(), v.getDate());
  }
  return normalizeLooseDate_(v);
}

function normalizeLooseDate_(v) {
  if (v == null || v === '') return null;

  if (Object.prototype.toString.call(v) === '[object Date]' && !isNaN(v.getTime())) {
    return new Date(v.getFullYear(), v.getMonth(), v.getDate());
  }

  var s = String(v).trim();
  if (!s) return null;

  var m = s.match(/^(\d{4})[-\/](\d{1,2})[-\/](\d{1,2})$/);
  if (m) {
    return new Date(Number(m[1]), Number(m[2]) - 1, Number(m[3]));
  }

  var d = new Date(s);
  if (!isNaN(d.getTime())) {
    return new Date(d.getFullYear(), d.getMonth(), d.getDate());
  }

  return null;
}

function formatDateKey_(dateObj) {
  return Utilities.formatDate(dateObj, Session.getScriptTimeZone(), 'yyyy-MM-dd');
}
