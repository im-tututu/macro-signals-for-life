/********************
 * 02_utils.gs
 * 公共工具函数
 ********************/

/**
 * Date -> yyyy-MM-dd
 */
function formatDate_(d) {
  if (!(d instanceof Date)) d = new Date(d);
  return Utilities.formatDate(d, Session.getScriptTimeZone(), "yyyy-MM-dd");
}

/**
 * 解析 yyyy-MM-dd / yy-MM-dd
 */
function parseYMD_(s) {
  if (!s) return null;
  s = String(s).trim();

  var m4 = s.match(/^(\d{4})-(\d{1,2})-(\d{1,2})$/);
  if (m4) {
    return new Date(Number(m4[1]), Number(m4[2]) - 1, Number(m4[3]));
  }

  var m2 = s.match(/^(\d{2})-(\d{1,2})-(\d{1,2})$/);
  if (m2) {
    return new Date(2000 + Number(m2[1]), Number(m2[2]) - 1, Number(m2[3]));
  }

  return null;
}

/**
 * 统一标准化为 yyyy-MM-dd
 */
function normYMD_(v) {
  if (v === null || v === undefined || v === "") return "";

  if (v instanceof Date && !isNaN(v.getTime())) {
    return formatDate_(v);
  }

  var s = String(v).trim();
  if (!s) return "";

  var d = parseYMD_(s);
  if (d) return formatDate_(d);

  // 兼容 2026/3/6、2026.3.6、26/3/6、26.3.6
  var m4 = s.match(/^(\d{4})[\/.](\d{1,2})[\/.](\d{1,2})$/);
  if (m4) {
    return m4[1] + "-" + ("0" + m4[2]).slice(-2) + "-" + ("0" + m4[3]).slice(-2);
  }

  var m2 = s.match(/^(\d{2})[\/.](\d{1,2})[\/.](\d{1,2})$/);
  if (m2) {
    return "20" + m2[1] + "-" + ("0" + m2[2]).slice(-2) + "-" + ("0" + m2[3]).slice(-2);
  }

  var dt = new Date(s);
  if (!isNaN(dt.getTime())) {
    return formatDate_(dt);
  }

  return s;
}

/**
 * 今天 yyyy-MM-dd
 */
function today_() {
  return formatDate_(new Date());
}

/**
 * 是否周末
 */
function isWeekend_(d) {
  if (!(d instanceof Date)) d = parseYMD_(d);
  if (!d) return false;
  var day = d.getDay();
  return day === 0 || day === 6;
}

/**
 * 构建某列的日期索引 Set
 * colIndex: 0-based
 */
function buildDateIndex_(sheet, colIndex) {
  var set = new Set();
  var lastRow = sheet.getLastRow();
  if (lastRow <= 1) return set;

  var vals = sheet.getRange(2, colIndex + 1, lastRow - 1, 1).getDisplayValues();
  for (var i = 0; i < vals.length; i++) {
    var k = normYMD_(vals[i][0]);
    if (k) set.add(k);
  }
  return set;
}

/**
 * 安全 fetch
 */
function safeFetch_(url, options, retryTimes) {
  retryTimes = retryTimes || 3;
  var lastErr = null;

  for (var i = 0; i < retryTimes; i++) {
    try {
      return UrlFetchApp.fetch(url, options || {});
    } catch (e) {
      lastErr = e;
      if (i < retryTimes - 1) {
        Utilities.sleep(1200 + Math.floor(Math.random() * 1200));
      }
    }
  }

  throw lastErr || new Error("safeFetch_ failed");
}




function stripTags_(html) {
  if (html == null || html === '') return '';
  return String(html)
    .replace(/<script\b[^<]*(?:(?!<\/script>)<[^<]*)*<\/script>/gi, '')
    .replace(/<style\b[^<]*(?:(?!<\/style>)<[^<]*)*<\/style>/gi, '')
    .replace(/<[^>]+>/g, '')
    .replace(/&nbsp;/gi, ' ')
    .replace(/&amp;/gi, '&')
    .replace(/&lt;/gi, '<')
    .replace(/&gt;/gi, '>')
    .replace(/&quot;/gi, '"')
    .replace(/&#39;/gi, "'")
    .replace(/\s+/g, ' ')
    .trim();
}


function mustGetSheet_(name) {
  var ss = SpreadsheetApp.getActiveSpreadsheet();
  var sh = ss.getSheetByName(name);
  if (!sh) throw new Error("sheet not found: " + name);
  return sh;
}

function normalizeHeader_(h) {
  return String(h).trim().toLowerCase();
}

function buildHeaderIndex_(headers) {
  var map = {};
  headers.forEach(function (h, i) {
    map[normalizeHeader_(h)] = i;
  });
  return map;
}

function requireColumn_(index, name) {
  var key = normalizeHeader_(name);
  if (!(key in index)) {
    throw new Error("missing column: " + name);
  }
  return index[key];
}

function toNumberOrNull_(v) {
  if (v === "" || v === null) return null;
  var n = Number(v);
  return isNaN(n) ? null : n;
}

function isFiniteNumber_(v) {
  return typeof v === "number" && isFinite(v);
}

function normalizeSheetDate_(v) {
  if (v instanceof Date) return v;
  return new Date(v);
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


