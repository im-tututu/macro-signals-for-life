/********************
 * 22_money_market.gs
 * 银行间质押式回购利率：
 * - 当天：prr-md.json（实时更新）
 * - 历史：prr-chrt.csv（历史回补）
 ********************/

var SHEET_MM = "money_market";

var MM_HEADERS = [
  "date",
  "showDateCN",
  "source_url",

  "DR001_weightedRate", "DR001_latestRate", "DR001_avgPrd",
  "DR007_weightedRate", "DR007_latestRate", "DR007_avgPrd",
  "DR014_weightedRate", "DR014_latestRate", "DR014_avgPrd",
  "DR021_weightedRate", "DR021_latestRate", "DR021_avgPrd",
  "DR1M_weightedRate", "DR1M_latestRate", "DR1M_avgPrd",

  "fetched_at"
];

/**
 * 当天更新
 * 用 prr-md.json 覆盖更新当天这一行
 */
function fetchPledgedRepoRates_() {
  var sheet = getOrCreateMoneyMarketSheet_();

  var url =
    "https://www.chinamoney.com.cn/r/cms/www/chinamoney/data/currency/prr-md.json?t=" +
    Date.now();

  var res = UrlFetchApp.fetch(url, {
    method: "get",
    muteHttpExceptions: true
  });

  var code = res.getResponseCode();
  if (code !== 200) {
    throw new Error("prr-md.json HTTP=" + code + " body=" + res.getContentText().slice(0, 300));
  }

  var json = JSON.parse(res.getContentText());
  var data = json.data || {};
  var records = json.records || [];
  if (!records.length) {
    throw new Error("prr-md.json records 为空");
  }

  // 当天实时快照，主键直接用今天
  var ds = formatDate_(new Date());

  var map = {};
  records.forEach(function(r) {
    var code = toStr_(r.productCode);
    if (code) map[code] = r;
  });

  var dr001 = map.DR001 || {};
  var dr007 = map.DR007 || {};
  var dr014 = map.DR014 || {};
  var dr021 = map.DR021 || {};
  var dr1m  = map.DR1M  || {};

  var dr001Weighted = toNum_(dr001.weightedRate);
  var dr007Weighted = toNum_(dr007.weightedRate);
  var dr014Weighted = toNum_(dr014.weightedRate);

  if (dr001Weighted === "" && dr007Weighted === "" && dr014Weighted === "") {
    throw new Error("prr-md.json 关键字段为空，停止写入，避免生成空行");
  }

  upsertMoneyMarketRow_(sheet, ds, {
    showDateCN: toStr_(data.showDateCN),
    source_url: url,

    DR001_weightedRate: dr001Weighted,
    DR001_latestRate:   toNum_(dr001.latestRate),
    DR001_avgPrd:       toNum_(dr001.avgPrd),

    DR007_weightedRate: dr007Weighted,
    DR007_latestRate:   toNum_(dr007.latestRate),
    DR007_avgPrd:       toNum_(dr007.avgPrd),

    DR014_weightedRate: dr014Weighted,
    DR014_latestRate:   toNum_(dr014.latestRate),
    DR014_avgPrd:       toNum_(dr014.avgPrd),

    DR021_weightedRate: toNum_(dr021.weightedRate),
    DR021_latestRate:   toNum_(dr021.latestRate),
    DR021_avgPrd:       toNum_(dr021.avgPrd),

    DR1M_weightedRate:  toNum_(dr1m.weightedRate),
    DR1M_latestRate:    toNum_(dr1m.latestRate),
    DR1M_avgPrd:        toNum_(dr1m.avgPrd)
  }, {
    overwriteBlankOnly: false
  });

  sortMoneyMarketByDate_(sheet);
  Logger.log("✅ money_market 当天更新完成: " + ds);
}

/**
 * 回补最近120天历史（到昨天）
 */
function backfillMoneyMarketLast120Days() {
  var end = new Date();
  end.setDate(end.getDate() - 1);

  var start = new Date(end);
  start.setDate(end.getDate() - 119);

  backfillMoneyMarket(formatDate_(start), formatDate_(end));
}

/**
 * 自定义区间历史回补
 * 用 prr-chrt.csv，只补 yesterday 及更早
 */
function backfillMoneyMarket(startDate, endDate) {
  var start = parseYMD_(startDate);
  var end = parseYMD_(endDate);

  if (!start || !end) throw new Error("日期格式错误");
  if (start > end) throw new Error("startDate 不能大于 endDate");

  var sheet = getOrCreateMoneyMarketSheet_();

  var url =
    "https://www.chinamoney.com.cn/r/cms/www/chinamoney/data/currency/prr-chrt.csv?t=" +
    Date.now();

  var res = UrlFetchApp.fetch(url, {
    method: "get",
    muteHttpExceptions: true
  });

  var code = res.getResponseCode();
  if (code !== 200) {
    throw new Error("prr-chrt.csv HTTP=" + code + " body=" + res.getContentText().slice(0, 300));
  }

  var text = res.getContentText();
  if (!text || !text.trim()) {
    throw new Error("prr-chrt.csv 返回空内容");
  }

  var todayStr = formatDate_(new Date());
  var lines = text.trim().split(/\r?\n/);

  var inserted = 0;
  var updated = 0;
  var skipped = 0;

  for (var i = 0; i < lines.length; i++) {
    var line = lines[i];
    if (!line || !line.trim()) continue;

    var parts = line.split(",");
    if (parts.length < 9) {
      skipped++;
      continue;
    }

    // 当前按你抓到的结构：
    // [0] date
    // [6] DR001
    // [7] DR007
    // [8] DR014
    var ds = normalizeDateKey_(parts[0]);
    if (!ds) {
      skipped++;
      continue;
    }

    var d = parseYMD_(ds);
    if (!d) {
      skipped++;
      continue;
    }

    if (d < start || d > end) continue;
    if (ds === todayStr) continue;

    var dr001 = toNum_(parts[6]);
    var dr007 = toNum_(parts[7]);
    var dr014 = toNum_(parts[8]);

    if (dr001 === "" && dr007 === "" && dr014 === "") {
      skipped++;
      continue;
    }

    var existed = findMoneyMarketRowNumByDate_(sheet, ds) > 0;

    upsertMoneyMarketRow_(sheet, ds, {
      showDateCN: "",
      source_url: url,

      DR001_weightedRate: dr001,
      DR007_weightedRate: dr007,
      DR014_weightedRate: dr014
    }, {
      overwriteBlankOnly: true
    });

    if (existed) updated++;
    else inserted++;
  }

  sortMoneyMarketByDate_(sheet);

  Logger.log(
    "✅ money_market 历史回补完成: " +
    startDate + " ~ " + endDate +
    " inserted=" + inserted +
    " updated=" + updated +
    " skipped=" + skipped
  );
}

/**
 * 测试：刷新当天
 */
function testMoneyMarketToday() {
  fetchPledgedRepoRates_();
}

/**
 * 测试：回补最近120天
 */
function testMoneyMarketBackfill120() {
  backfillMoneyMarketLast120Days();
}

/**
 * 获取/创建 sheet
 */
function getOrCreateMoneyMarketSheet_() {
  var ss = SpreadsheetApp.getActive();
  var sheet = ss.getSheetByName(SHEET_MM);
  if (!sheet) {
    sheet = ss.insertSheet(SHEET_MM);
  }
  ensureMoneyMarketHeader_(sheet);
  return sheet;
}

/**
 * 确保表头
 */
function ensureMoneyMarketHeader_(sheet) {
  var needRewrite = false;
  var current = [];

  if (sheet.getLastRow() >= 1) {
    current = sheet.getRange(1, 1, 1, MM_HEADERS.length).getValues()[0];
  }

  for (var i = 0; i < MM_HEADERS.length; i++) {
    if (String(current[i] || "") !== MM_HEADERS[i]) {
      needRewrite = true;
      break;
    }
  }

  if (needRewrite || sheet.getLastRow() === 0) {
    sheet.getRange(1, 1, 1, MM_HEADERS.length).setValues([MM_HEADERS]);
    sheet.setFrozenRows(1);
  }

  sheet.getRange("A:A").setNumberFormat("@");
}

/**
 * 按日期 upsert
 */
function upsertMoneyMarketRow_(sheet, dateStr, rowObj, opts) {
  opts = opts || {};
  var overwriteBlankOnly = !!opts.overwriteBlankOnly;

  var rowNum = findMoneyMarketRowNumByDate_(sheet, dateStr);

  if (rowNum > 0) {
    updateMoneyMarketRowByObj_(sheet, rowNum, rowObj, overwriteBlankOnly);
  } else {
    appendMoneyMarketRow_(sheet, dateStr, rowObj);
  }
}

/**
 * 找到某日期所在行
 * 只用 sheet，不用 values
 */
function findMoneyMarketRowNumByDate_(sheet, dateStr) {
  var target = String(dateStr).trim();
  var lastRow = sheet.getLastRow();
  if (lastRow <= 1) return -1;

  var values = sheet.getRange(2, 1, lastRow - 1, 1).getValues();

  for (var i = 0; i < values.length; i++) {
    var cell = values[i][0];
    var key = (cell instanceof Date) ? formatDate_(cell) : normalizeDateKey_(cell);

    if (key === target) {
      return i + 2;
    }
  }

  return -1;
}

/**
 * 追加新行
 */
function appendMoneyMarketRow_(sheet, dateStr, rowObj) {
  var row = buildEmptyMoneyMarketRow_();

  row[0] = String(dateStr).trim();
  row[1] = toStr_(rowObj.showDateCN);
  row[2] = toStr_(rowObj.source_url);

  applyRowObjToArray_(row, rowObj, false);
  row[18] = new Date();

  var rowNum = sheet.getLastRow() + 1;
  sheet.getRange(rowNum, 1).setNumberFormat("@");
  sheet.getRange(rowNum, 1, 1, MM_HEADERS.length).setValues([row]);
}

/**
 * 更新已有行
 */
function updateMoneyMarketRowByObj_(sheet, rowNum, rowObj, overwriteBlankOnly) {
  var row = sheet.getRange(rowNum, 1, 1, MM_HEADERS.length).getValues()[0];

  if (!overwriteBlankOnly || isBlank_(row[1])) {
    if ("showDateCN" in rowObj) row[1] = toStr_(rowObj.showDateCN);
  }

  if (!overwriteBlankOnly || isBlank_(row[2])) {
    if ("source_url" in rowObj) row[2] = toStr_(rowObj.source_url);
  }

  applyRowObjToArray_(row, rowObj, overwriteBlankOnly);

  row[18] = new Date();
  sheet.getRange(rowNum, 1, 1, MM_HEADERS.length).setValues([row]);
}

/**
 * 应用字段到一整行
 */
function applyRowObjToArray_(row, rowObj, overwriteBlankOnly) {
  var map = getMoneyMarketColumnMap_();

  Object.keys(map).forEach(function(key) {
    if (!(key in rowObj)) return;

    var idx = map[key];
    var newVal = rowObj[key];

    if (newVal === undefined) return;

    if (overwriteBlankOnly) {
      if (isBlank_(row[idx])) row[idx] = newVal;
    } else {
      row[idx] = newVal;
    }
  });
}

function buildEmptyMoneyMarketRow_() {
  var row = [];
  for (var i = 0; i < MM_HEADERS.length; i++) row.push("");
  return row;
}

function getMoneyMarketColumnMap_() {
  return {
    DR001_weightedRate: 3,
    DR001_latestRate: 4,
    DR001_avgPrd: 5,

    DR007_weightedRate: 6,
    DR007_latestRate: 7,
    DR007_avgPrd: 8,

    DR014_weightedRate: 9,
    DR014_latestRate: 10,
    DR014_avgPrd: 11,

    DR021_weightedRate: 12,
    DR021_latestRate: 13,
    DR021_avgPrd: 14,

    DR1M_weightedRate: 15,
    DR1M_latestRate: 16,
    DR1M_avgPrd: 17
  };
}

function sortMoneyMarketByDate_(sheet) {
  var lastRow = sheet.getLastRow();
  if (lastRow <= 2) return;

  sheet.getRange(2, 1, lastRow - 1, MM_HEADERS.length)
    .sort([{ column: 1, ascending: true }]);
}

/**
 * 调试：看今天定位到哪一行
 */
function debugTodayRow() {
  var sheet = getOrCreateMoneyMarketSheet_();
  var today = formatDate_(new Date());
  var row = findMoneyMarketRowNumByDate_(sheet, today);

  Logger.log("today=" + today);
  Logger.log("row=" + row);
}

/**
 * 调试：重复日期
 */
function debugMoneyMarketDuplicateDates() {
  var sheet = getOrCreateMoneyMarketSheet_();
  var lastRow = sheet.getLastRow();
  if (lastRow <= 1) return;

  var vals = sheet.getRange(2, 1, lastRow - 1, 1).getValues();
  var seen = {};

  for (var i = 0; i < vals.length; i++) {
    var k = vals[i][0] instanceof Date ? formatDate_(vals[i][0]) : normalizeDateKey_(vals[i][0]);
    if (!k) continue;
    if (!seen[k]) seen[k] = [];
    seen[k].push(i + 2);
  }

  Object.keys(seen).forEach(function(k) {
    if (seen[k].length > 1) {
      Logger.log(k + " duplicated rows: " + seen[k].join(", "));
    }
  });
}

/**
 * 标准化日期文本
 */
function normalizeDateKey_(v) {
  if (v === null || v === undefined || v === "") return "";

  if (v instanceof Date && !isNaN(v.getTime())) {
    return formatDate_(v);
  }

  var s = String(v).trim();
  if (!s) return "";

  var m4 = s.match(/^(\d{4})[-\/.](\d{1,2})[-\/.](\d{1,2})$/);
  if (m4) {
    return m4[1] + "-" + ("0" + m4[2]).slice(-2) + "-" + ("0" + m4[3]).slice(-2);
  }

  var m2 = s.match(/^(\d{2})[-\/.](\d{1,2})[-\/.](\d{1,2})$/);
  if (m2) {
    return "20" + m2[1] + "-" + ("0" + m2[2]).slice(-2) + "-" + ("0" + m2[3]).slice(-2);
  }

  return s;
}

function toNum_(v) {
  if (v === null || v === undefined || v === "") return "";
  var n = Number(v);
  return isFinite(n) ? n : "";
}

function toStr_(v) {
  return v === null || v === undefined ? "" : String(v).trim();
}

function isBlank_(v) {
  return v === "" || v === null || v === undefined;
}