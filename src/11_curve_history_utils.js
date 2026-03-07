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

