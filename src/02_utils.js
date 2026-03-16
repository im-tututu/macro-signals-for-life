/********************
 * 02_utils.js
 * 公共工具函数。
 ********************/

/**
 * 将日期值标准化为 yyyy-MM-dd。
 */
function formatDate_(d) {
  if (!(d instanceof Date)) d = new Date(d);
  return Utilities.formatDate(d, Session.getScriptTimeZone(), 'yyyy-MM-dd');
}

/**
 * 解析 yyyy-MM-dd / yy-MM-dd 为 Date。
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
 * 将常见日期输入统一为 yyyy-MM-dd。
 */
function normYMD_(v) {
  if (v === null || v === undefined || v === '') return '';

  if (v instanceof Date && !isNaN(v.getTime())) {
    return formatDate_(v);
  }

  var s = String(v).trim();
  if (!s) return '';

  var d = parseYMD_(s);
  if (d) return formatDate_(d);

  var m4 = s.match(/^(\d{4})[\/.](\d{1,2})[\/.](\d{1,2})$/);
  if (m4) {
    return m4[1] + '-' + ('0' + m4[2]).slice(-2) + '-' + ('0' + m4[3]).slice(-2);
  }

  var m2 = s.match(/^(\d{2})[\/.](\d{1,2})[\/.](\d{1,2})$/);
  if (m2) {
    return '20' + m2[1] + '-' + ('0' + m2[2]).slice(-2) + '-' + ('0' + m2[3]).slice(-2);
  }

  var dt = new Date(s);
  if (!isNaN(dt.getTime())) {
    return formatDate_(dt);
  }

  return s;
}

/**
 * 返回今天的 yyyy-MM-dd。
 */

/**
 * 从任意文本中提取日期，返回 yyyy-MM-dd。
 */
function extractDateFromAnyText_(v) {
  if (v === null || v === undefined || v === '') return '';

  if (v instanceof Date && !isNaN(v.getTime())) {
    return formatDate_(v);
  }

  var s = String(v).trim();
  if (!s) return '';

  var m = s.match(/(\d{4})[-\/.](\d{1,2})[-\/.](\d{1,2})/);
  if (m) {
    return m[1] + '-' + ('0' + m[2]).slice(-2) + '-' + ('0' + m[3]).slice(-2);
  }

  m = s.match(/(\d{4})年(\d{1,2})月(\d{1,2})日/);
  if (m) {
    return m[1] + '-' + ('0' + m[2]).slice(-2) + '-' + ('0' + m[3]).slice(-2);
  }

  return '';
}

/**
 * 统一日期字符串。
 */
function normalizeDateKey_(v) {
  if (v === null || v === undefined || v === '') return '';

  if (v instanceof Date && !isNaN(v.getTime())) {
    return formatDate_(v);
  }

  var s = String(v).trim();
  if (!s) return '';

  var m4 = s.match(/^(\d{4})[-\/.](\d{1,2})[-\/.](\d{1,2})$/);
  if (m4) {
    return m4[1] + '-' + ('0' + m4[2]).slice(-2) + '-' + ('0' + m4[3]).slice(-2);
  }

  var m2 = s.match(/^(\d{2})[-\/.](\d{1,2})[-\/.](\d{1,2})$/);
  if (m2) {
    return '20' + m2[1] + '-' + ('0' + m2[2]).slice(-2) + '-' + ('0' + m2[3]).slice(-2);
  }

  return s;
}

function today_() {
  return formatDate_(new Date());
}

/**
 * 判断是否为周末。
 */
function isWeekend_(d) {
  if (!(d instanceof Date)) d = parseYMD_(d);
  if (!d) return false;
  var day = d.getDay();
  return day === 0 || day === 6;
}

/**
 * 按指定列构建日期索引集合。
 * colIndex 为 0-based。
 */
function buildDateIndex_(sheet, colIndex) {
  var set = new Set();
  var lastRow = sheet.getLastRow();
  if (lastRow <= 1) return set;

  var vals = sheet.getRange(2, colIndex + 1, lastRow - 1, 1).getDisplayValues();
  for (var i = 0; i < vals.length; i++) {
    var key = normYMD_(vals[i][0]);
    if (key) set.add(key);
  }
  return set;
}

/**
 * 带重试的安全抓取。
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

  throw lastErr || new Error('safeFetch_ failed');
}

/**
 * 去除 HTML 标签并还原常见实体字符。
 */
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

/**
 * 获取指定工作表；兼容 mustGetSheet_(name) 与 mustGetSheet_(ss, name) 两种调用方式。
 */
function mustGetSheet_(spreadsheetOrName, maybeName) {
  var ss = SpreadsheetApp.getActiveSpreadsheet();
  var name = spreadsheetOrName;

  if (typeof spreadsheetOrName !== 'string') {
    ss = spreadsheetOrName || ss;
    name = maybeName;
  }

  var sh = ss.getSheetByName(name);
  if (!sh) throw new Error('sheet not found: ' + name);
  return sh;
}

/**
 * 统一表头键名。
 */
function normalizeHeader_(h) {
  return String(h).trim().toLowerCase();
}

/**
 * 基于表头数组构建列索引。
 */
function buildHeaderIndex_(headers) {
  var map = {};
  headers.forEach(function (h, i) {
    map[normalizeHeader_(h)] = i;
  });
  return map;
}

/**
 * 校验指定列存在并返回列索引。
 */
function requireColumn_(index, name) {
  var key = normalizeHeader_(name);
  if (!(key in index)) {
    throw new Error('missing column: ' + name);
  }
  return index[key];
}

/**
 * 将值转换为数字；空值返回 null。
 */
function toNumberOrNull_(v) {
  if (v === '' || v === null) return null;
  var n = Number(v);
  return isNaN(n) ? null : n;
}

/**
 * 判断值是否为有限数字。
 */
function isFiniteNumber_(v) {
  return typeof v === 'number' && isFinite(v);
}

/**
 * 将表格中的日期值转为 Date。
 */
function normalizeSheetDate_(v) {
  if (v instanceof Date) return v;
  return new Date(v);
}

/**
 * 宽松解析日期输入，只保留年月日。
 */
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

/**
 * Date 转 yyyy-MM-dd 键值。
 */
function formatDateKey_(dateObj) {
  return Utilities.formatDate(dateObj, Session.getScriptTimeZone(), 'yyyy-MM-dd');
}


/**
 * 统一获取或创建工作表。
 * 兼容旧项目里散落的 getOrCreateSheetByName_ 调用。
 */
function ensureSheet_(spreadsheetOrName, maybeName) {
  var ss = SpreadsheetApp.getActiveSpreadsheet();
  var name = spreadsheetOrName;

  if (typeof spreadsheetOrName !== 'string') {
    ss = spreadsheetOrName || ss;
    name = maybeName;
  }

  var sh = ss.getSheetByName(name);
  if (!sh) {
    sh = ss.insertSheet(name);
  }
  return sh;
}

function getOrCreateSheetByName_(spreadsheetOrName, maybeName) {
  return ensureSheet_(spreadsheetOrName, maybeName);
}

/**
 * 统一生成较短的缓存 key，避免 CacheService key 过长。
 */
function buildShortCacheKey_(prefix, parts) {
  parts = parts || [];
  var raw = prefix + '|' + parts.join('|');
  var digest = Utilities.computeDigest(
    Utilities.DigestAlgorithm.MD5,
    raw,
    Utilities.Charset.UTF_8
  );
  var hex = digest.map(function(b) {
    var v = (b < 0 ? b + 256 : b).toString(16);
    return v.length === 1 ? '0' + v : v;
  }).join('');
  return prefix + '_' + hex;
}

/**
 * 统一文本请求，返回字符串。
 */
function safeFetchText_(url, options, retryTimes) {
  return safeFetch_(url, options, retryTimes).getContentText('UTF-8');
}

/**
 * 统一 JSON 请求，返回已解析对象。
 */
function safeFetchJson_(url, options, retryTimes) {
  var text = safeFetchText_(url, options, retryTimes);
  return JSON.parse(text);
}

/**
 * HTML entity 解码。与 stripTags_ 搭配使用。
 */
function decodeHtmlText_(s) {
  if (s == null || s === '') return '';
  return String(s)
    .replace(/&nbsp;/gi, ' ')
    .replace(/&amp;/gi, '&')
    .replace(/&lt;/gi, '<')
    .replace(/&gt;/gi, '>')
    .replace(/&quot;/gi, '"')
    .replace(/&#39;/gi, "'")
    .replace(/&#10;/gi, '\n')
    .replace(/&#13;/gi, '\r');
}

/**
 * 宽松字符串转数字；空值返回空字符串，便于直接写表。
 */
function toNum_(v) {
  if (v === '' || v === null || v === undefined) return '';
  var n = Number(String(v).replace(/,/g, ''));
  return isNaN(n) ? '' : n;
}

/**
 * 宽松字符串清洗。
 */
function toStr_(v) {
  return v == null ? '' : String(v).trim();
}

function isBlank_(v) {
  return v === '' || v === null || v === undefined;
}

function safeSlice_(s, len) {
  s = String(s == null ? '' : s);
  return s.length <= len ? s : s.slice(0, len);
}

/**
 * 统一 yyyy-MM-dd HH:mm:ss。
 */
function formatDateTime_(d) {
  if (!(d instanceof Date)) d = new Date(d);
  return Utilities.formatDate(d, Session.getScriptTimeZone(), 'yyyy-MM-dd HH:mm:ss');
}

/**
 * 统一按首列 date 查找 / 覆盖 / 追加。
 */
function upsertRowByDate_(sheet, dateStr, rowValues) {
  var lastRow = sheet.getLastRow();
  if (lastRow > 1) {
    var values = sheet.getRange(2, 1, lastRow - 1, 1).getDisplayValues();
    for (var i = 0; i < values.length; i++) {
      if (normYMD_(values[i][0]) === dateStr) {
        sheet.getRange(i + 2, 1, 1, rowValues.length).setValues([rowValues]);
        return 'updated';
      }
    }
  }
  sheet.appendRow(rowValues);
  return 'inserted';
}
