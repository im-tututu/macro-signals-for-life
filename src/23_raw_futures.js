/********************
 * 23_raw_futures.js
 * 国债期货原始行情抓取。
 *
 * 当前使用新浪接口抓取 T0 / TF0 连续合约近似价格，
 * 结果写入原始期货表，供后续检查与扩展使用。
 ********************/

function fetchBondFutures_() {
  var ss = SpreadsheetApp.getActive();
  var sheet = ss.getSheetByName(SHEET_FUTURES_RAW) || ss.insertSheet(SHEET_FUTURES_RAW);

  ensureFuturesHeader_(sheet);

  var idx = buildDateIndex_(sheet, 0);
  var today = today_();
  if (idx.has(today)) {
    Logger.log("⏭ futures 已有今日(" + today + ")，跳过");
    return;
  }

  var t0 = fetchSinaFuturePrice_("T0");
  var tf0 = fetchSinaFuturePrice_("TF0");

  sheet.appendRow([today, t0, tf0, "hq.sinajs.cn", new Date()]);
  Logger.log("FUT T0=" + t0 + " TF0=" + tf0);
}

function ensureFuturesHeader_(sheet) {
  if (sheet.getLastRow() > 0) return;
  sheet.appendRow(["date", "T0_last", "TF0_last", "source", "fetched_at"]);
}
