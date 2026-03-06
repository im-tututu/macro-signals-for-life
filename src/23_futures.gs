/********************
* 23_futures.gs
* 国债期货模块：抓取 T0 / TF0 连续合约近似价格。
********************/

function fetchBondFutures_() {
  var ss = SpreadsheetApp.getActive();
  var sheet = ss.getSheetByName(SHEET_FUT) || ss.insertSheet(SHEET_FUT);

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

function fetchSinaFuturePrice_(symbol) {
  var url = "https://hq.sinajs.cn/list=" + encodeURIComponent(symbol);
  var res = safeFetch_(url, {
    method: "get",
    headers: {
      "User-Agent": "Mozilla/5.0",
      "Referer": "https://finance.sina.com.cn/"
    }
  }, 4);

  var txt = res.getContentText();
  var m = txt.match(/="([^"]*)"/);
  if (!m) return "";

  var arr = m[1].split(",");

  if (arr.length > 3) {
    var p = parseFloat(arr[3]);
    if (!isNaN(p) && p > 0) return p;
  }

  for (var j = 0; j < arr.length; j++) {
    var v2 = parseFloat(arr[j]);
    if (!isNaN(v2) && v2 > 0) return v2;
  }
  return "";
}


/********************
* 8) macro_dashboard：一屏终端
********************/
