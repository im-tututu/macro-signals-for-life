/********************
* 21_curve_history_slope_signal.gs
* 生成关键期限历史、曲线斜率与 ETF 信号。
********************/

function buildCurveHistory_() {
  var ss = SpreadsheetApp.getActive();
  var src = ss.getSheetByName(SHEET_CURVE);
  if (!src) return;

  var dst = ss.getSheetByName(SHEET_HIST) || ss.insertSheet(SHEET_HIST);

  var values = src.getDataRange().getValues();
  if (values.length < 2) return;

  var header = values[0];

  var idxY1 = header.indexOf("Y_1");
  var idxY3 = header.indexOf("Y_3");
  var idxY5 = header.indexOf("Y_5");
  var idxY10 = header.indexOf("Y_10");

  var out = [];
  out.push(["date", "gov_1y", "gov_3y", "gov_5y", "gov_10y"]);

  for (var i = 1; i < values.length; i++) {
    var r = values[i];
    if (r[1] !== "国债") continue;

    out.push([
      normYMD_(r[0]),
      idxY1 >= 0 ? r[idxY1] : "",
      idxY3 >= 0 ? r[idxY3] : "",
      idxY5 >= 0 ? r[idxY5] : "",
      idxY10 >= 0 ? r[idxY10] : ""
    ]);
  }

  dst.clearContents();
  dst.getRange(1, 1, out.length, out[0].length).setValues(out);
}


/********************
* 4) curve_slope：斜率/利差
********************/
function buildCurveSlope_() {
  var ss = SpreadsheetApp.getActive();
  var src = ss.getSheetByName(SHEET_HIST);
  if (!src) return;

  var dst = ss.getSheetByName(SHEET_SLOPE) || ss.insertSheet(SHEET_SLOPE);

  var values = src.getDataRange().getValues();
  if (values.length < 2) return;

  var out = [];
  out.push(["date", "10Y-1Y", "10Y-3Y", "5Y-1Y"]);

  for (var i = 1; i < values.length; i++) {
    var r = values[i];

    var d = r[0];
    var y1 = r[1];
    var y3 = r[2];
    var y5 = r[3];
    var y10 = r[4];

    if (y1 === "" || y10 === "") continue;

    out.push([
      normYMD_(d),
      (y10 - y1),
      (y10 - y3),
      (y5 - y1)
    ]);
  }

  dst.clearContents();
  dst.getRange(1, 1, out.length, out[0].length).setValues(out);
}


/********************
* 5) etf_signal：基于 10Y-1Y 简单提示
********************/
function buildETFSignal_() {
  var ss = SpreadsheetApp.getActive();
  var src = ss.getSheetByName(SHEET_SLOPE);
  if (!src) return;

  var dst = ss.getSheetByName(SHEET_SIGNAL) || ss.insertSheet(SHEET_SIGNAL);

  var values = src.getDataRange().getValues();
  if (values.length < 2) return;

  var out = [];
  out.push(["date", "10Y-1Y", "signal"]);

  for (var i = 1; i < values.length; i++) {
    var r = values[i];

    var steep = r[1];
    if (steep === "" || steep === null) continue;

    var sig = "中性";
    if (steep < SIGNAL_THRESHOLDS.steep_low) sig = "长债机会";
    else if (steep > SIGNAL_THRESHOLDS.steep_high) sig = "短债优先";

    out.push([normYMD_(r[0]), steep, sig]);
  }

  dst.clearContents();
  dst.getRange(1, 1, out.length, out[0].length).setValues(out);
}


/********************
* 6) DR001/DR007 等资金利率
********************/
