/********************
* 22_money_market.gs
* 资金市场模块：抓取 DR001/DR007/DR014/DR021/DR1M。
********************/

function fetchPledgedRepoRates_() {
  var ss = SpreadsheetApp.getActive();
  var sheet = ss.getSheetByName(SHEET_MM) || ss.insertSheet(SHEET_MM);

  ensureMoneyMarketHeader_(sheet);
  var idx = buildDateIndex_(sheet, 0);

  var url = "https://www.chinamoney.com.cn/r/cms/www/chinamoney/data/currency/prr-md.json?t=" + Date.now();
  var res = safeFetch_(url, {
    method: "get",
    headers: { "User-Agent": "Mozilla/5.0" }
  }, 4);

  if (res.getResponseCode() !== 200) {
    Logger.log("❌ prr-md.json HTTP=" + res.getResponseCode());
    return;
  }

  var json = JSON.parse(res.getContentText());
  var showDateCN = (json.data && json.data.showDateCN) ? json.data.showDateCN : "";
  var dateKey = showDateCN ? normYMD_(showDateCN) : today_();

  if (idx.has(dateKey)) {
    Logger.log("⏭ money_market 已有日期(" + dateKey + ")，跳过");
    return;
  }

  var byCode = {};
  (json.records || []).forEach(function (r) { byCode[r.productCode] = r; });

  var codes = ["DR001", "DR007", "DR014", "DR021", "DR1M"];
  var row = [dateKey, showDateCN, url];

  codes.forEach(function (c) {
    var r = byCode[c];
    row.push(r ? parseFloat(r.weightedRate) : "");
    row.push(r ? parseFloat(r.latestRate) : "");
    row.push(r ? parseFloat(r.avgPrd) : "");
  });

  row.push(new Date());
  sheet.appendRow(row);

  Logger.log("✅ money_market 写入 " + dateKey + " DR007=" + (byCode["DR007"] ? byCode["DR007"].weightedRate : "NA"));
}

function ensureMoneyMarketHeader_(sheet) {
  if (sheet.getLastRow() > 0) return;

  var codes = ["DR001", "DR007", "DR014", "DR021", "DR1M"];
  var header = ["date", "showDateCN", "source_url"];

  codes.forEach(function (c) {
    header.push(c + "_weightedRate");
    header.push(c + "_latestRate");
    header.push(c + "_avgPrd");
  });

  header.push("fetched_at");
  sheet.appendRow(header);
}

function resetMoneyMarketSheet_() {
  var ss = SpreadsheetApp.getActive();
  var sheet = ss.getSheetByName(SHEET_MM) || ss.insertSheet(SHEET_MM);
  sheet.clear();
  ensureMoneyMarketHeader_(sheet);

  var lastCol = sheet.getLastColumn();
  if (sheet.getMaxRows() > 1) {
    sheet.getRange(2, lastCol, sheet.getMaxRows() - 1, 1).setNumberFormat("yyyy-mm-dd hh:mm:ss");
  }
}


/********************
* 7) 国债期货连续：T0 / TF0
********************/
