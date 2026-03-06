/********************
* 10_chinabond_curve.gs
* 中债收益率曲线抓取、解析与落表。
********************/

function runDailyWide_(date) {
  var ss = SpreadsheetApp.getActive();
  var sheet = ss.getSheetByName(SHEET_CURVE) || ss.insertSheet(SHEET_CURVE);

  ensureCurveHeader_(sheet);

  var index = buildCurveIndex_(sheet);

  var ids = CURVES.map(function (c) { return c.id; });
  Logger.log("曲线数: " + ids.length);

  var html = fetchChinaBondCurves_(date, ids);
  var parsed = parseChinaBondCurvesPairwise_(html);

  var inserted = 0, skipped = 0, failed = 0;

  for (var i = 0; i < CURVES.length; i++) {
    var c = CURVES[i];
    var key = date + "|" + c.name;

    if (index.has(key)) {
      Logger.log("⏭ 跳过(已存在): " + key);
      skipped++;
      continue;
    }

    var map = parsed[c.name];
    if (!map || map.size === 0) {
      Logger.log("❌ 无数据/未解析到: " + c.name);
      failed++;
      continue;
    }

    try {
      appendCurveRowFixed_(sheet, date, c.name, map);
      Logger.log("✅ 插入: " + key + " 节点=" + map.size);
      inserted++;
    } catch (e) {
      Logger.log("❌ 插入失败: " + key + " err=" + e);
      failed++;
    }
  }

  Logger.log("yc_curve 新增=" + inserted + " 跳过=" + skipped + " 失败=" + failed);
}

function fetchChinaBondCurves_(date, ids) {
  var cache = CacheService.getScriptCache();
  var cacheKey = "chinabond_" + date + "_" + ids.join("_");
  var cached = cache.get(cacheKey);
  if (cached) {
    Logger.log("命中缓存: " + cacheKey);
    return cached;
  }

  var url = "https://yield.chinabond.com.cn/cbweb-mn/yc/ycDetail?ycDefIds=" + ids.join(",");

  var payload = {
    ycDefIds: ids.join(","),
    zblx: "txy",
    workTime: date,
    dxbj: "0",
    qxlx: "0",
    yqqxN: "N",
    yqqxK: "K",
    wrjxCBFlag: "0",
    locale: "zh_CN"
  };

  var options = {
    method: "post",
    payload: payload,
    headers: {
      "User-Agent": "Mozilla/5.0",
      "Referer": "https://yield.chinabond.com.cn/",
      "Origin": "https://yield.chinabond.com.cn"
    }
  };

  var res = safeFetch_(url, options, 4);
  var text = res.getContentText();
  cache.put(cacheKey, text, 21600);

  Logger.log("ChinaBond HTTP=" + res.getResponseCode() + " len=" + text.length);
  return text;
}

function parseChinaBondCurvesPairwise_(html) {
  var result = {};
  var pairRe = /<table[^>]*class="t1"[\s\S]*?<span>\s*([^<]+?)\s*<\/span>[\s\S]*?<\/table>\s*<table[^>]*class="tablelist"[\s\S]*?<\/table>/gi;

  var m;
  while ((m = pairRe.exec(html)) !== null) {
    var title = m[1];
    var block = m[0];

    var tableMatch = block.match(/<table[^>]*class="tablelist"[\s\S]*?<\/table>/i);
    if (!tableMatch) continue;

    var table = tableMatch[0];
    var map = parseTableListToMap_(table);

    var name = normalizeCurveName_(title);
    if (name) {
      result[name] = map;
      Logger.log("解析: " + name + " title=" + title + " nodes=" + map.size);
    } else {
      Logger.log("⚠️ 未映射曲线标题: " + title + " nodes=" + map.size);
    }
  }

  return result;
}

function parseTableListToMap_(tableHtml) {
  var map = new Map();
  var rowRe = /<tr[\s\S]*?<\/tr>/gi;
  var tdRe = /<td[^>]*>([\s\S]*?)<\/td>/gi;

  var rows = tableHtml.match(rowRe) || [];
  for (var i = 0; i < rows.length; i++) {
    var tds = [];
    var mm;
    tdRe.lastIndex = 0;
    while ((mm = tdRe.exec(rows[i])) !== null) {
      tds.push(stripTags_(mm[1]));
    }
    if (tds.length < 2) continue;

    var term = parseFloat(String(tds[0]).replace("y", ""));
    var y = parseFloat(String(tds[1]));

    if (!isNaN(term) && !isNaN(y)) map.set(term, y);
  }
  return map;
}

function normalizeCurveName_(title) {
  if (title.indexOf("国债收益率曲线") >= 0) return "国债";
  if (title.indexOf("国开债收益率曲线") >= 0) return "国开债";
  if (title.indexOf("企业债收益率曲线") >= 0 && title.indexOf("(AAA)") >= 0) return "AAA信用";
  if (title.indexOf("企业债收益率曲线") >= 0 && (title.indexOf("(AA+)") >= 0 || title.indexOf("(AA＋)") >= 0)) return "AA+信用";
  return "";
}

function ensureCurveHeader_(sheet) {
  if (sheet.getLastRow() > 0) return;

  var header = ["date", "curve"];
  for (var i = 0; i < TERMS.length; i++) header.push("Y_" + TERMS[i]);
  sheet.appendRow(header);
}

function appendCurveRowFixed_(sheet, date, curveName, map) {
  var row = [date, curveName];
  for (var i = 0; i < TERMS.length; i++) {
    var t = TERMS[i];
    row.push(map.has(t) ? map.get(t) : "");
  }
  sheet.appendRow(row);
}

function buildCurveIndex_(sheet) {
  var last = sheet.getLastRow();
  var set = new Set();
  if (last < 2) return set;

  var values = sheet.getRange(2, 1, last - 1, 2).getValues();
  for (var i = 0; i < values.length; i++) {
    var d = values[i][0];
    var c = values[i][1];
    if (!d || !c) continue;
    set.add(normYMD_(d) + "|" + c);
  }
  return set;
}


/********************
* 2) rate_dashboard：政策/信用/期限利差
********************/
