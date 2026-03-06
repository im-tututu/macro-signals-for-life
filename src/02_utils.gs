/********************
* 02_utils.gs
* 通用工具函数：安全请求、日期规范化、字符串清洗等。
********************/

function safeFetch_(url, options, maxRetries) {
  maxRetries = maxRetries || 4;
  options = options || {};

  if (!options.headers) options.headers = {};
  if (!options.method) options.method = "get";
  options.muteHttpExceptions = true;

  options.headers["User-Agent"] = options.headers["User-Agent"] || "Mozilla/5.0";
  options.headers["Accept-Language"] = options.headers["Accept-Language"] || "zh-CN,zh;q=0.9,en;q=0.8";
  options.headers["Cache-Control"] = "no-cache";

  for (var i = 0; i <= maxRetries; i++) {
    try {
      var jitter = 800 + Math.floor(Math.random() * 1200);
      Utilities.sleep(jitter);

      var res = UrlFetchApp.fetch(url, options);
      var code = res.getResponseCode();
      var text = res.getContentText();

      if (code >= 200 && code < 300) return res;

      if (code === 403 || code === 404 || code === 429 || code >= 500) {
        var wait = Math.pow(2, i) * 1500 + Math.floor(Math.random() * 1000);
        Logger.log("safeFetch_ retry code=" + code + " try=" + i + " wait=" + wait);
        Utilities.sleep(wait);
        continue;
      }

      throw new Error("HTTP " + code + " body=" + text.slice(0, 300));
    } catch (e) {
      if (i === maxRetries) throw e;
      var wait2 = Math.pow(2, i) * 2000 + Math.floor(Math.random() * 1000);
      Logger.log("safeFetch_ exception retry try=" + i + " wait=" + wait2 + " err=" + e);
      Utilities.sleep(wait2);
    }
  }

  throw new Error("safeFetch_ failed after retries: " + url);
}

function today_() {
  return Utilities.formatDate(new Date(), Session.getScriptTimeZone(), "yyyy-MM-dd");
}

function stripTags_(s) {
  return String(s).replace(/<[^>]+>/g, "").replace(/&nbsp;/g, " ").trim();
}

function normYMD_(v) {
  if (v instanceof Date) {
    return Utilities.formatDate(v, Session.getScriptTimeZone(), "yyyy-MM-dd");
  }
  var s = String(v || "").trim();
  var m = s.match(/^(\d{4})-(\d{1,2})-(\d{1,2})/);
  if (!m) return s.slice(0, 10);
  var y = m[1], mo = ("0" + m[2]).slice(-2), d = ("0" + m[3]).slice(-2);
  return y + "-" + mo + "-" + d;
}

function parseYMD_(s) {
  var m = String(s || "").trim().match(/^(\d{4})-(\d{1,2})-(\d{1,2})$/);
  if (!m) return null;
  return new Date(Number(m[1]), Number(m[2]) - 1, Number(m[3]));
}

function formatDate_(d) {
  return Utilities.formatDate(d, Session.getScriptTimeZone(), "yyyy-MM-dd");
}

function isWeekend_(d) {
  var day = d.getDay();
  return day === 0 || day === 6;
}
