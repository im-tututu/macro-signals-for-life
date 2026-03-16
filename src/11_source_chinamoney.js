/********************
 * 11_source_chinamoney.js
 * ChinaMoney 来源适配层。
 *
 * 当前覆盖：
 * - 质押式回购日内快照 prr-md.json
 * - 历史回补 CSV prr-chrt.csv
 ********************/


function fetchChinaMoneyRepoSnapshot_() {
  var url = 'https://www.chinamoney.com.cn/r/cms/www/chinamoney/data/currency/prr-md.json?t=' + Date.now();
  var res = safeFetch_(url, {method: 'get', muteHttpExceptions: true}, 3);
  var code = res.getResponseCode();
  if (code !== 200) {
    throw new Error('prr-md.json HTTP=' + code + ' body=' + safeSlice_(res.getContentText(), 300));
  }
  return { url: url, json: JSON.parse(res.getContentText()) };
}

function fetchChinaMoneyRepoHistoryCsv_() {
  var url = 'https://www.chinamoney.com.cn/r/cms/www/chinamoney/data/currency/prr-chrt.csv?t=' + Date.now();
  var res = safeFetch_(url, {method: 'get', muteHttpExceptions: true}, 3);
  var code = res.getResponseCode();
  if (code !== 200) {
    throw new Error('prr-chrt.csv HTTP=' + code + ' body=' + safeSlice_(res.getContentText(), 300));
  }
  return { url: url, csv: res.getContentText('UTF-8') };
}

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

