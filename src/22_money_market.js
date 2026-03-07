/********************
 * 22_money_market.js
 *
 * 银行间质押式回购利率：
 * - 当天：prr-md.json（实时更新）
 * - 历史：prr-chrt.csv（历史回补）
 *
 * 本版原则：
 * 1) 保留原有整体结构，尽量少动其他文件
 * 2) 历史回补优化为：一次读入、建索引、内存更新、整块写回
 * 3) 当天更新优先根据接口日期落表，避免周末/节假日重复造行
 * 4) 日志明确说明：到底是新增、更新，还是无变化
 * 5) 兼容 safeFetch_，没有则回退 UrlFetchApp.fetch
 ********************/

/**
 * 避免与其他文件重复定义时直接覆盖
 */
if (typeof SHEET_MM === "undefined") {
  var SHEET_MM = "money_market";
}

/**
 * money_market 表头
 */
var MM_HEADERS = [
  "date",
  "showDateCN",
  "source_url",
  "DR001_weightedRate",
  "DR001_latestRate",
  "DR001_avgPrd",
  "DR007_weightedRate",
  "DR007_latestRate",
  "DR007_avgPrd",
  "DR014_weightedRate",
  "DR014_latestRate",
  "DR014_avgPrd",
  "DR021_weightedRate",
  "DR021_latestRate",
  "DR021_avgPrd",
  "DR1M_weightedRate",
  "DR1M_latestRate",
  "DR1M_avgPrd",
  "fetched_at"
];

/**
 * 列索引
 */
var MM_COL = {
  date: 0,
  showDateCN: 1,
  source_url: 2,
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
  DR1M_avgPrd: 17,
  fetched_at: 18
};

/**
 * 抓取当天 / 最近交易日数据并写入表
 *
 * 重要：
 * - 周末/节假日运行时，接口可能返回的是上一交易日数据
 * - 因此主键 date 不能简单使用 new Date()
 * - 这里优先从接口字段中提取真实交易日
 */
function fetchPledgedRepoRates_() {
  var sheet = getOrCreateMoneyMarketSheet_();

  var url =
    "https://www.chinamoney.com.cn/r/cms/www/chinamoney/data/currency/prr-md.json?t=" +
    Date.now();

  var res = fetchWithFallback_(url, {
    method: "get",
    muteHttpExceptions: true
  });

  var code = res.getResponseCode();
  if (code !== 200) {
    throw new Error(
      "prr-md.json HTTP=" + code + " body=" + safeSlice_(res.getContentText(), 300)
    );
  }

  var json = JSON.parse(res.getContentText());
  var data = json.data || {};
  var records = json.records || [];

  if (!records.length) {
    throw new Error("prr-md.json records 为空");
  }

  /**
   * 优先根据接口文本推导真实业务日期，避免周末重复造行
   */
  var ds = deriveMoneyMarketBizDate_(data);

  var map = {};
  records.forEach(function (r) {
    var codeKey = toStr_(r.productCode);
    if (codeKey) map[codeKey] = r;
  });

  var dr001 = map.DR001 || {};
  var dr007 = map.DR007 || {};
  var dr014 = map.DR014 || {};
  var dr021 = map.DR021 || {};
  var dr1m = map.DR1M || {};

  var dr001Weighted = toNum_(dr001.weightedRate);
  var dr007Weighted = toNum_(dr007.weightedRate);
  var dr014Weighted = toNum_(dr014.weightedRate);

  if (dr001Weighted === "" && dr007Weighted === "" && dr014Weighted === "") {
    throw new Error("prr-md.json 关键字段为空，停止写入，避免生成空行");
  }

  var rowObj = {
    showDateCN: toStr_(data.showDateCN),
    source_url: url,

    DR001_weightedRate: dr001Weighted,
    DR001_latestRate: toNum_(dr001.latestRate),
    DR001_avgPrd: toNum_(dr001.avgPrd),

    DR007_weightedRate: dr007Weighted,
    DR007_latestRate: toNum_(dr007.latestRate),
    DR007_avgPrd: toNum_(dr007.avgPrd),

    DR014_weightedRate: dr014Weighted,
    DR014_latestRate: toNum_(dr014.latestRate),
    DR014_avgPrd: toNum_(dr014.avgPrd),

    DR021_weightedRate: toNum_(dr021.weightedRate),
    DR021_latestRate: toNum_(dr021.latestRate),
    DR021_avgPrd: toNum_(dr021.avgPrd),

    DR1M_weightedRate: toNum_(dr1m.weightedRate),
    DR1M_latestRate: toNum_(dr1m.latestRate),
    DR1M_avgPrd: toNum_(dr1m.avgPrd)
  };

  var action = upsertMoneyMarketSingleRow_(sheet, ds, rowObj, {
    overwriteBlankOnly: false
  });

  sortMoneyMarketByDate_(sheet);

  Logger.log(
    "money_market update"
      + " | date=" + ds
      + " | showDateCN=" + toStr_(data.showDateCN)
      + " | action=" + action
      + " | DR001=" + dr001Weighted
      + " | DR007=" + dr007Weighted
      + " | DR014=" + dr014Weighted
  );
}

/**
 * 回补最近 120 天（到昨天）
 */
function backfillMoneyMarketLast120Days() {
  var end = new Date();
  end.setDate(end.getDate() - 1);

  var start = new Date(end);
  start.setDate(end.getDate() - 119);

  backfillMoneyMarket(formatDate_(start), formatDate_(end));
}

/**
 * 历史回补
 *
 * 优化点：
 * - 一次读取现有数据
 * - 建 date -> rowIndex 索引
 * - 内存中更新
 * - 最后整块写回
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

  var res = fetchWithFallback_(url, {
    method: "get",
    muteHttpExceptions: true
  });

  var code = res.getResponseCode();
  if (code !== 200) {
    throw new Error(
      "prr-chrt.csv HTTP=" + code + " body=" + safeSlice_(res.getContentText(), 300)
    );
  }

  /**
   * 去掉 BOM，避免第一行日期解析失败
   */
  var text = String(res.getContentText() || "").replace(/^\uFEFF/, "");
  if (!text || !String(text).trim()) {
    throw new Error("prr-chrt.csv 返回空内容");
  }

  var todayStr = formatDate_(new Date());

  var data = readMoneyMarketBodyValues_(sheet);
  var rowIndexMap = buildMoneyMarketDateRowIndexFromValues_(data);

  var lines = String(text).trim().split(/\r?\n/);

  var inserted = 0;
  var updated = 0;
  var skipped = 0;

  for (var i = 0; i < lines.length; i++) {
    var line = lines[i];
    if (!line || !String(line).trim()) {
      skipped++;
      continue;
    }

    var parts = line.split(",");
    if (parts.length < 9) {
      skipped++;
      continue;
    }

    /**
     * 第一行如果是表头则跳过
     */
    if (i === 0 && /date/i.test(String(parts[0] || ""))) {
      skipped++;
      continue;
    }

    /**
     * 当前按你已有接口结构：
     * [0] date
     * [6] DR001
     * [7] DR007
     * [8] DR014
     */
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

    /**
     * 历史 CSV 不处理今天，避免和 md.json 冲突
     */
    if (ds === todayStr) continue;

    var dr001 = toNum_(parts[6]);
    var dr007 = toNum_(parts[7]);
    var dr014 = toNum_(parts[8]);

    if (dr001 === "" && dr007 === "" && dr014 === "") {
      skipped++;
      continue;
    }

    var rowObj = {
      showDateCN: "",
      source_url: url,
      DR001_weightedRate: dr001,
      DR007_weightedRate: dr007,
      DR014_weightedRate: dr014
    };

    if (rowIndexMap.hasOwnProperty(ds)) {
      var existedIdx = rowIndexMap[ds];
      var before = JSON.stringify(data[existedIdx]);

      /**
       * 历史回补只补空值，不覆盖已有值
       */
      applyMoneyMarketRowObjToExistingRow_(data[existedIdx], rowObj, true);

      if (isBlank_(data[existedIdx][MM_COL.showDateCN])) {
        data[existedIdx][MM_COL.showDateCN] = "";
      }
      if (isBlank_(data[existedIdx][MM_COL.source_url])) {
        data[existedIdx][MM_COL.source_url] = url;
      }

      data[existedIdx][MM_COL.fetched_at] = new Date();

      var after = JSON.stringify(data[existedIdx]);
      if (before !== after) {
        updated++;
      } else {
        skipped++;
      }
    } else {
      var newRow = buildEmptyMoneyMarketRow_();
      newRow[MM_COL.date] = ds;
      newRow[MM_COL.showDateCN] = "";
      newRow[MM_COL.source_url] = url;
      applyMoneyMarketRowObjToExistingRow_(newRow, rowObj, false);
      newRow[MM_COL.fetched_at] = new Date();

      data.push(newRow);
      rowIndexMap[ds] = data.length - 1;
      inserted++;
    }
  }

  /**
   * 内存中按日期排序后再写回
   */
  data.sort(function (a, b) {
    var da = normalizeDateKey_(a[MM_COL.date]);
    var db = normalizeDateKey_(b[MM_COL.date]);
    if (da < db) return -1;
    if (da > db) return 1;
    return 0;
  });

  writeMoneyMarketBodyValues_(sheet, data);

  Logger.log(
    "money_market backfill"
      + " | start=" + startDate
      + " | end=" + endDate
      + " | inserted=" + inserted
      + " | updated=" + updated
      + " | skipped=" + skipped
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
 * 获取或创建 sheet
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
 * 确保表头存在且一致
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

  /**
   * date 列固定为文本格式
   */
  sheet.getRange("A:A").setNumberFormat("@");
}

/**
 * 单行 upsert
 *
 * 返回值：
 * - inserted
 * - updated
 * - unchanged
 *
 * overwriteBlankOnly:
 * - true  只补空值
 * - false 直接覆盖
 */
function upsertMoneyMarketSingleRow_(sheet, dateStr, rowObj, opts) {
  opts = opts || {};
  var overwriteBlankOnly = !!opts.overwriteBlankOnly;
  var result = "unchanged";

  var data = readMoneyMarketBodyValues_(sheet);
  var rowIndexMap = buildMoneyMarketDateRowIndexFromValues_(data);

  if (rowIndexMap.hasOwnProperty(dateStr)) {
    var idx = rowIndexMap[dateStr];
    var before = JSON.stringify(data[idx]);

    if (!overwriteBlankOnly || isBlank_(data[idx][MM_COL.showDateCN])) {
      if ("showDateCN" in rowObj) data[idx][MM_COL.showDateCN] = toStr_(rowObj.showDateCN);
    }
    if (!overwriteBlankOnly || isBlank_(data[idx][MM_COL.source_url])) {
      if ("source_url" in rowObj) data[idx][MM_COL.source_url] = toStr_(rowObj.source_url);
    }

    applyMoneyMarketRowObjToExistingRow_(data[idx], rowObj, overwriteBlankOnly);
    data[idx][MM_COL.fetched_at] = new Date();

    var after = JSON.stringify(data[idx]);
    if (before !== after) {
      result = "updated";
    }
  } else {
    var row = buildEmptyMoneyMarketRow_();
    row[MM_COL.date] = String(dateStr).trim();
    row[MM_COL.showDateCN] = toStr_(rowObj.showDateCN);
    row[MM_COL.source_url] = toStr_(rowObj.source_url);
    applyMoneyMarketRowObjToExistingRow_(row, rowObj, false);
    row[MM_COL.fetched_at] = new Date();
    data.push(row);
    result = "inserted";
  }

  writeMoneyMarketBodyValues_(sheet, data);
  return result;
}

/**
 * 一次读取正文区（不含表头）
 */
function readMoneyMarketBodyValues_(sheet) {
  var lastRow = sheet.getLastRow();
  if (lastRow <= 1) return [];
  return sheet.getRange(2, 1, lastRow - 1, MM_HEADERS.length).getValues();
}

/**
 * 一次写回正文区（不含表头）
 *
 * 这里仍保留“清空再整块写回”的稳妥方式。
 */
function writeMoneyMarketBodyValues_(sheet, values) {
  var lastRow = sheet.getLastRow();
  if (lastRow > 1) {
    sheet.getRange(2, 1, lastRow - 1, MM_HEADERS.length).clearContent();
  }

  if (!values || !values.length) return;

  sheet.getRange(2, 1, values.length, MM_HEADERS.length).setValues(values);
  sheet.getRange(2, 1, values.length, 1).setNumberFormat("@");
}

/**
 * 构建 date -> bodyRowIndex 映射
 *
 * bodyRowIndex 从 0 开始，对应 sheet 第 2 行开始的数据区。
 */
function buildMoneyMarketDateRowIndexFromValues_(values) {
  var map = {};
  for (var i = 0; i < values.length; i++) {
    var ds = normalizeDateKey_(values[i][MM_COL.date]);
    if (ds) {
      map[ds] = i;
    }
  }
  return map;
}

/**
 * 把 rowObj 中数据写入某一行数组
 *
 * 注意：
 * - date / showDateCN / source_url / fetched_at 不在这里处理
 * - 它们由调用方单独控制
 */
function applyMoneyMarketRowObjToExistingRow_(row, rowObj, overwriteBlankOnly) {
  if (!rowObj) return;

  Object.keys(MM_COL).forEach(function (key) {
    if (key === "date") return;
    if (key === "showDateCN") return;
    if (key === "source_url") return;
    if (key === "fetched_at") return;
    if (!(key in rowObj)) return;

    var idx = MM_COL[key];
    var newVal = rowObj[key];

    if (newVal === undefined) return;

    if (overwriteBlankOnly) {
      if (isBlank_(row[idx])) row[idx] = newVal;
    } else {
      row[idx] = newVal;
    }
  });
}

/**
 * 兼容旧接口：按日期找行号
 *
 * 返回实际 sheet 行号；找不到返回 -1
 */
function findMoneyMarketRowNumByDate_(sheet, dateStr) {
  var data = readMoneyMarketBodyValues_(sheet);
  var rowIndexMap = buildMoneyMarketDateRowIndexFromValues_(data);
  if (rowIndexMap.hasOwnProperty(dateStr)) {
    return rowIndexMap[dateStr] + 2;
  }
  return -1;
}

/**
 * 兼容旧接口：追加行
 */
function appendMoneyMarketRow_(sheet, dateStr, rowObj) {
  return upsertMoneyMarketSingleRow_(sheet, dateStr, rowObj, {
    overwriteBlankOnly: false
  });
}

/**
 * 兼容旧接口：按行号更新
 */
function updateMoneyMarketRowByObj_(sheet, rowNum, rowObj, overwriteBlankOnly) {
  var data = readMoneyMarketBodyValues_(sheet);
  var idx = rowNum - 2;
  if (idx < 0 || idx >= data.length) {
    throw new Error("updateMoneyMarketRowByObj_ rowNum 超出范围: " + rowNum);
  }

  var before = JSON.stringify(data[idx]);

  if (!overwriteBlankOnly || isBlank_(data[idx][MM_COL.showDateCN])) {
    if ("showDateCN" in rowObj) data[idx][MM_COL.showDateCN] = toStr_(rowObj.showDateCN);
  }
  if (!overwriteBlankOnly || isBlank_(data[idx][MM_COL.source_url])) {
    if ("source_url" in rowObj) data[idx][MM_COL.source_url] = toStr_(rowObj.source_url);
  }

  applyMoneyMarketRowObjToExistingRow_(data[idx], rowObj, overwriteBlankOnly);
  data[idx][MM_COL.fetched_at] = new Date();

  var after = JSON.stringify(data[idx]);
  writeMoneyMarketBodyValues_(sheet, data);

  return before !== after ? "updated" : "unchanged";
}

/**
 * 构建空白数据行
 */
function buildEmptyMoneyMarketRow_() {
  var row = [];
  for (var i = 0; i < MM_HEADERS.length; i++) row.push("");
  return row;
}

/**
 * 按 date 升序排序
 */
function sortMoneyMarketByDate_(sheet) {
  var lastRow = sheet.getLastRow();
  if (lastRow <= 2) return;
  sheet
    .getRange(2, 1, lastRow - 1, MM_HEADERS.length)
    .sort([{ column: 1, ascending: true }]);
}

/**
 * 调试：查看今天对应哪一行
 */
function debugTodayRow() {
  var sheet = getOrCreateMoneyMarketSheet_();
  var today = formatDate_(new Date());
  var row = findMoneyMarketRowNumByDate_(sheet, today);
  Logger.log("today=" + today);
  Logger.log("row=" + row);
}

/**
 * 调试：找出重复 date
 */
function debugMoneyMarketDuplicateDates() {
  var sheet = getOrCreateMoneyMarketSheet_();
  var vals = readMoneyMarketBodyValues_(sheet);
  var seen = {};

  for (var i = 0; i < vals.length; i++) {
    var k = normalizeDateKey_(vals[i][MM_COL.date]);
    if (!k) continue;
    if (!seen[k]) seen[k] = [];
    seen[k].push(i + 2);
  }

  Object.keys(seen).forEach(function (k) {
    if (seen[k].length > 1) {
      Logger.log(k + " duplicated rows: " + seen[k].join(", "));
    }
  });
}

/**
 * 调试：找出重复 showDateCN
 */
function debugMoneyMarketDuplicateShowDateCN() {
  var sheet = getOrCreateMoneyMarketSheet_();
  var vals = readMoneyMarketBodyValues_(sheet);
  var seen = {};

  for (var i = 0; i < vals.length; i++) {
    var k = toStr_(vals[i][MM_COL.showDateCN]);
    if (!k) continue;
    if (!seen[k]) seen[k] = [];
    seen[k].push(i + 2);
  }

  Object.keys(seen).forEach(function (k) {
    if (seen[k].length > 1) {
      Logger.log("showDateCN duplicated: " + k + " rows=" + seen[k].join(", "));
    }
  });
}

/**
 * 从 md.json 的 data 中推导业务日期
 *
 * 规则：
 * 1) 优先从 showDateCN / date / showDate / tradeDate 提取日期
 * 2) 提取不到时，退回运行日
 *
 * 这样可避免周末/节假日运行时，
 * 把“上一交易日数据”写成“今天日期”
 */
function deriveMoneyMarketBizDate_(data) {
  data = data || {};

  var candidates = [
    data.showDateCN,
    data.date,
    data.showDate,
    data.tradeDate
  ];

  for (var i = 0; i < candidates.length; i++) {
    var ds = extractDateFromAnyText_(candidates[i]);
    if (ds) return ds;
  }

  return formatDate_(new Date());
}

/**
 * 从任意文本中提取日期，返回 yyyy-MM-dd
 *
 * 支持：
 * - 2026-03-06
 * - 2026/03/06
 * - 2026.03.06
 * - 2026年3月6日
 * - 含时间文本，如 2026-03-06 14:30
 */
function extractDateFromAnyText_(v) {
  if (v === null || v === undefined || v === "") return "";

  if (v instanceof Date && !isNaN(v.getTime())) {
    return formatDate_(v);
  }

  var s = String(v).trim();
  if (!s) return "";

  var m;

  m = s.match(/(\d{4})[-\/.](\d{1,2})[-\/.](\d{1,2})/);
  if (m) {
    return (
      m[1] +
      "-" +
      ("0" + m[2]).slice(-2) +
      "-" +
      ("0" + m[3]).slice(-2)
    );
  }

  m = s.match(/(\d{4})年(\d{1,2})月(\d{1,2})日/);
  if (m) {
    return (
      m[1] +
      "-" +
      ("0" + m[2]).slice(-2) +
      "-" +
      ("0" + m[3]).slice(-2)
    );
  }

  return "";
}

/**
 * 统一日期字符串
 *
 * 支持：
 * - Date
 * - yyyy-M-d / yyyy/M/d / yyyy.M.d
 * - yy-M-d / yy/M/d / yy.M.d
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
    return (
      m4[1] +
      "-" +
      ("0" + m4[2]).slice(-2) +
      "-" +
      ("0" + m4[3]).slice(-2)
    );
  }

  var m2 = s.match(/^(\d{2})[-\/.](\d{1,2})[-\/.](\d{1,2})$/);
  if (m2) {
    return (
      "20" +
      m2[1] +
      "-" +
      ("0" + m2[2]).slice(-2) +
      "-" +
      ("0" + m2[3]).slice(-2)
    );
  }

  return s;
}

/**
 * 安全转数字
 *
 * - 空 => ""
 * - 可转数字 => number
 * - 不能转 => ""
 */
function toNum_(v) {
  if (v === null || v === undefined || v === "") return "";
  var n = Number(v);
  return isFinite(n) ? n : "";
}

/**
 * 安全转字符串
 */
function toStr_(v) {
  return v === null || v === undefined ? "" : String(v).trim();
}

/**
 * 判空
 */
function isBlank_(v) {
  return v === "" || v === null || v === undefined;
}

/**
 * 日志安全截断
 */
function safeSlice_(s, len) {
  s = s == null ? "" : String(s);
  return s.length <= len ? s : s.slice(0, len);
}

/**
 * 请求兼容层：
 * - 有 safeFetch_ 就优先用
 * - 没有则回退 UrlFetchApp.fetch
 */
function fetchWithFallback_(url, options) {
  if (typeof safeFetch_ === "function") {
    return safeFetch_(url, options);
  }
  return UrlFetchApp.fetch(url, options || {});
}