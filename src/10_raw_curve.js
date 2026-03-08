/********************
 * 10_raw_curve.js
 * 中债收益率曲线抓取、解析与落表。
 ********************/

/**
 * 抓取指定日期的多条收益率曲线并按固定期限落表。
 */
function runDailyWide_(date) {
  var ss = SpreadsheetApp.getActive();
  var sheet = ss.getSheetByName(SHEET_CURVE_RAW) || ss.insertSheet(SHEET_CURVE_RAW);

  ensureCurveHeader_(sheet);

  var index = buildCurveIndex_(sheet);
  var ids = CURVES.map(function (c) {
    return c.id;
  });
  Logger.log('曲线数: ' + ids.length);

  var html = fetchChinaBondCurves_(date, ids);
  var parsed = parseChinaBondCurvesPairwise_(html);

  var inserted = 0;
  var skipped = 0;
  var failed = 0;

  for (var i = 0; i < CURVES.length; i++) {
    var curve = CURVES[i];
    var key = date + '|' + curve.name;

    if (index.has(key)) {
      Logger.log('⏭ 跳过(已存在): ' + key);
      skipped++;
      continue;
    }

    var map = parsed[curve.name];
    if (!map || map.size === 0) {
      Logger.log('❌ 无数据/未解析到: ' + curve.name);
      failed++;
      continue;
    }

    try {
      appendCurveRowFixed_(sheet, date, curve.name, map);
      Logger.log('✅ 插入: ' + key + ' 节点=' + map.size);
      inserted++;
    } catch (e) {
      Logger.log('❌ 插入失败: ' + key + ' err=' + e);
      failed++;
    }
  }

  Logger.log('yc_curve 新增=' + inserted + ' 跳过=' + skipped + ' 失败=' + failed);
}

/**
 * 抓取中债收益率曲线原始 HTML，并使用脚本缓存减少重复请求。
 */
function buildShortCacheKey_(prefix, dateStr, ids) {
  var raw = prefix + '|' + dateStr + '|' + ids.join(',');
  var digest = Utilities.computeDigest(
    Utilities.DigestAlgorithm.MD5,
    raw,
    Utilities.Charset.UTF_8
  );

  var hex = digest.map(function(b) {
    var v = (b < 0 ? b + 256 : b).toString(16);
    return v.length === 1 ? '0' + v : v;
  }).join('');

  return prefix + '_' + dateStr.replace(/-/g, '') + '_' + hex;
}

function fetchChinaBondCurves_(workTime, ycDefIds) {
  var url = 'https://yield.chinabond.com.cn/cbweb-mn/yc/ycDetail';

  var cache = CacheService.getScriptCache();
  var cacheKey = buildShortCacheKey_('yc_detail', workTime, ycDefIds);
  var cached = cache.get(cacheKey);
  if (cached) return cached;

  var payload = {
    ycDefIds: ycDefIds.join(','),
    zblx: 'txy',
    workTime: workTime,
    dxbj: '0',
    qxlx: '0',
    yqqxN: 'N',
    yqqxK: 'K',
    wrjxCBFlag: '0',
    locale: 'zh_CN'
  };

  var resp = UrlFetchApp.fetch(url, {
    method: 'post',
    payload: payload,
    muteHttpExceptions: true,
    headers: {
      'User-Agent': 'Mozilla/5.0'
    }
  });

  var code = resp.getResponseCode();
  var text = resp.getContentText('UTF-8');

  if (code !== 200) {
    throw new Error('ycDetail 请求失败 code=' + code + ' body=' + text.slice(0, 500));
  }

  cache.put(cacheKey, text, 21600);
  return text;
}

/**
 * 将页面中的标题表和数据表配对解析为曲线映射。
 */
function parseChinaBondCurvesPairwise_(html) {
  var result = {};
  var pairRe = /<table[^>]*class="t1"[\s\S]*?<span>\s*([^<]+?)\s*<\/span>[\s\S]*?<\/table>\s*<table[^>]*class="tablelist"[\s\S]*?<\/table>/gi;

  var match;
  while ((match = pairRe.exec(html)) !== null) {
    var title = match[1];
    var block = match[0];
    var tableMatch = block.match(/<table[^>]*class="tablelist"[\s\S]*?<\/table>/i);
    if (!tableMatch) continue;

    var map = parseTableListToMap_(tableMatch[0]);
    var name = normalizeCurveName_(title);
    if (name) {
      result[name] = map;
      Logger.log('解析: ' + name + ' title=' + title + ' nodes=' + map.size);
    } else {
      Logger.log('⚠️ 未映射曲线标题: ' + title + ' nodes=' + map.size);
    }
  }

  return result;
}

/**
 * 把 tablelist 表格解析为 term -> yield 的 Map。
 */
function parseTableListToMap_(tableHtml) {
  var map = new Map();
  var rowRe = /<tr[\s\S]*?<\/tr>/gi;
  var tdRe = /<td[^>]*>([\s\S]*?)<\/td>/gi;
  var rows = tableHtml.match(rowRe) || [];

  for (var i = 0; i < rows.length; i++) {
    var tds = [];
    var cellMatch;
    tdRe.lastIndex = 0;

    while ((cellMatch = tdRe.exec(rows[i])) !== null) {
      tds.push(stripTags_(cellMatch[1]));
    }
    if (tds.length < 2) continue;

    var term = parseFloat(String(tds[0]).replace('y', ''));
    var yieldValue = parseFloat(String(tds[1]));
    if (!isNaN(term) && !isNaN(yieldValue)) {
      map.set(term, yieldValue);
    }
  }

  return map;
}

/**
 * 将页面标题统一映射为项目内使用的曲线名。
 */
function normalizeCurveName_(title) {
  if (title.indexOf('国债收益率曲线') >= 0) return '国债';
  if (title.indexOf('国开债收益率曲线') >= 0) return '国开债';
  if (title.indexOf('企业债收益率曲线') >= 0 && title.indexOf('(AAA)') >= 0) return 'AAA信用';
  if (title.indexOf('企业债收益率曲线') >= 0 && (title.indexOf('(AA+)') >= 0 || title.indexOf('(AA＋)') >= 0)) return 'AA+信用';
  return '';
}

/**
 * 确保 yc_curve 表头存在。
 */
function ensureCurveHeader_(sheet) {
  if (sheet.getLastRow() > 0) return;

  var header = ['date', 'curve'];
  for (var i = 0; i < TERMS.length; i++) {
    header.push('Y_' + TERMS[i]);
  }
  sheet.appendRow(header);
}

/**
 * 按 TERMS 固定列顺序追加一行曲线数据。
 */
function appendCurveRowFixed_(sheet, date, curveName, map) {
  var row = [date, curveName];
  for (var i = 0; i < TERMS.length; i++) {
    var term = TERMS[i];
    row.push(map.has(term) ? map.get(term) : '');
  }
  sheet.appendRow(row);
}

/**
 * 构建 date|curve 唯一键索引，用于避免重复写入。
 */
function buildCurveIndex_(sheet) {
  var last = sheet.getLastRow();
  var set = new Set();
  if (last < 2) return set;

  var values = sheet.getRange(2, 1, last - 1, 2).getValues();
  for (var i = 0; i < values.length; i++) {
    var dateValue = values[i][0];
    var curveName = values[i][1];
    if (!dateValue || !curveName) continue;
    set.add(normYMD_(dateValue) + '|' + curveName);
  }
  return set;
}
