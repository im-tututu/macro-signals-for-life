/********************
 * 16_raw_life_assets.js
 *
 * 原始_民生与资产价格
 * -------------------
 * P3 第一版：先把“能稳定自动抓到”的字段落表。
 *
 * 当前实现：
 * 1) mortgage_rate_est      -> 人民银行“商业性个人住房贷款加权平均利率”最新官方披露值
 * 2) house_price_*          -> 国家统计局 70 城新建商品住宅销售价格指数（环比）简单平均
 * 3) gold_cny              -> 上海黄金交易所首页“上海金早盘价（元/克）”
 * 4) money_fund_7d         -> 天弘余额宝货币七日年化代理值
 * 5) deposit_1y            -> 中国银行最新人民币存款挂牌利率（一年期整存整取）
 *
 * 这张表是“快照表”，date 统一写脚本执行日；各字段自己的 observation date 写进 source。
 ********************/

function testLifeAsset() {
  return fetchLifeAsset_(true);
}

function forceFetchLifeAsset_() {
  return fetchLifeAsset_(true);
}

function fetchLifeAsset_(forceRefresh) {
  var force = forceRefresh === true;
  var sheet = getOrCreateLifeAssetSheet_();

  if (!force && hasFetchedLifeAssetToday_(sheet)) {
    Logger.log('life_asset skip | reason=already_fetched_today');
    return { skipped: true, reason: 'already_fetched_today' };
  }

  var snapshotDate = formatDate_(new Date());
  var fetchedAt = formatDateTimeLifeAsset_(new Date());
  var sourceNotes = [];

  var mortgage = executeLifeAssetFetcher_('mortgage_rate_est', fetchMortgageRatePbc_);
  var housePrice = executeLifeAssetFetcher_('house_price_bundle', fetchHousePriceBundleFromNbs_);
  var gold = executeLifeAssetFetcher_('gold_cny', fetchGoldCnyFromSge_);
  var moneyFund = executeLifeAssetFetcher_('money_fund_7d', fetchMoneyFund7dProxy_);
  var deposit1y = executeLifeAssetFetcher_('deposit_1y', fetchDeposit1YFromBoc_);

  pushLifeAssetSourceNote_(sourceNotes, 'mortgage_rate_est', mortgage);
  pushLifeAssetSourceNote_(sourceNotes, 'house_price_nbs_70city', housePrice);
  pushLifeAssetSourceNote_(sourceNotes, 'gold_cny', gold);
  pushLifeAssetSourceNote_(sourceNotes, 'money_fund_7d', moneyFund);
  pushLifeAssetSourceNote_(sourceNotes, 'deposit_1y', deposit1y);

  var row = [
    snapshotDate,
    mortgage.value,
    housePrice.tier1,
    housePrice.tier2,
    housePrice.all70,
    gold.value,
    moneyFund.value,
    deposit1y.value,
    sourceNotes.join(' | '),
    fetchedAt
  ];

  upsertLifeAssetRowByDate_(sheet, snapshotDate, row);

  Logger.log(
    'life_asset update'
      + ' | date=' + snapshotDate
      + ' | mortgage=' + safeLogNum_(mortgage.value)
      + ' | house70=' + safeLogNum_(housePrice.all70)
      + ' | gold_cny=' + safeLogNum_(gold.value)
      + ' | money_fund_7d=' + safeLogNum_(moneyFund.value)
      + ' | deposit_1y=' + safeLogNum_(deposit1y.value)
  );

  return {
    skipped: false,
    date: snapshotDate,
    fetched_at: fetchedAt,
    row: row
  };
}

/* =========================
 * Fetchers
 * ========================= */

function fetchMortgageRatePbc_() {
  var listHtml = fetchLifeAssetText_(LIFE_ASSET_SOURCE.pbc_mortgage_list);
  var item = extractFirstLinkedItem_(listHtml, /全国新发放商业性个人住房贷款加权平均利率/);
  if (!item) {
    throw new Error('PBC mortgage list item not found');
  }

  var articleUrl = toAbsoluteUrl_(LIFE_ASSET_SOURCE.pbc_mortgage_list, item.url);
  var articleText = htmlToReadableText_(fetchLifeAssetText_(articleUrl));
  var m = articleText.match(/加权平均利率为\s*([0-9.]+)%/);
  if (!m) {
    throw new Error('PBC mortgage value not found');
  }

  return {
    value: Number(m[1]),
    obsDate: inferQuarterEndFromText_(item.title || articleText),
    source: 'PBC:' + articleUrl
  };
}

function fetchHousePriceBundleFromNbs_() {
  var listHtml = fetchLifeAssetText_(LIFE_ASSET_SOURCE.nbs_release_list);
  var item = extractFirstLinkedItem_(listHtml, /70个大中城市商品住宅销售价格变动情况/);
  if (!item) {
    throw new Error('NBS 70-city release item not found');
  }

  var articleUrl = toAbsoluteUrl_(LIFE_ASSET_SOURCE.nbs_release_list, item.url);
  var articleText = htmlToReadableText_(fetchLifeAssetText_(articleUrl));
  var parsed = parseNbs70CityMomTable_(articleText);
  var series = parsed.momByCity;

  return {
    value: parsed.all70,
    all70: parsed.all70,
    tier1: averageByCities_(series, LIFE_ASSET_TIER1_CITIES),
    tier2: averageByCities_(series, LIFE_ASSET_TIER2_CITIES),
    obsDate: inferMonthFromText_(item.title || articleText),
    source: 'NBS:' + articleUrl
  };
}


function fetchGoldCnyFromSge_() {
  var text = htmlToReadableText_(fetchLifeAssetText_(LIFE_ASSET_SOURCE.sge_home));
  var normalized = text.replace(/\s+/g, ' ');
  var m = normalized.match(/上海金早盘价（元\/克）\s*([0-9.]+)/);
  if (!m) {
    m = normalized.match(/上海金基准价[^0-9]{0,20}([0-9.]+)/);
  }
  if (!m) {
    throw new Error('SGE gold price not found');
  }

  var d = normalized.match(/行情日期[:：]?\s*(\d{4}-\d{2}-\d{2}|\d{8})/);
  return {
    value: Number(m[1]),
    obsDate: d ? normalizeCompactYmd_(d[1]) : formatDate_(new Date()),
    source: 'SGE:' + LIFE_ASSET_SOURCE.sge_home
  };
}

function fetchMoneyFund7dProxy_() {
  var text = htmlToReadableText_(fetchLifeAssetText_(LIFE_ASSET_SOURCE.money_fund_proxy_url));
  var normalized = text.replace(/\s+/g, ' ');
  var m = normalized.match(/7日年化收益率[（(]?([0-9\-]*)[）)]?[：:]?\s*([0-9.]+)%/);
  if (!m) {
    throw new Error('money fund 7d proxy not found');
  }

  return {
    value: Number(m[2]),
    obsDate: inferObsDateFromMonthDay_(m[1]),
    source: 'EASTMONEY:' + LIFE_ASSET_SOURCE.money_fund_proxy_url
  };
}


function fetchDeposit1YFromBoc_() {
  var listHtml = fetchLifeAssetText_(LIFE_ASSET_SOURCE.boc_deposit_list);
  var item = extractFirstLinkedItem_(listHtml, /人民币存款利率表\d{4}-\d{2}-\d{2}/);
  if (!item) {
    throw new Error('BOC deposit list item not found');
  }

  var articleUrl = toAbsoluteUrl_(LIFE_ASSET_SOURCE.boc_deposit_list, item.url);
  var text = htmlToReadableText_(fetchLifeAssetText_(articleUrl));
  var blockMatch = text.match(/整存整取([\s\S]{0,500}?)(二年|零存整取|定活两便)/);
  var block = blockMatch ? blockMatch[1] : text;
  var m = block.match(/一年\s*([0-9.]+)/);
  if (!m) {
    throw new Error('BOC deposit 1Y not found');
  }

  return {
    value: Number(m[1]),
    obsDate: inferDateFromTitleText_(item.title),
    source: 'BOC:' + articleUrl
  };
}

/* =========================
 * Sheet helpers
 * ========================= */

function getOrCreateLifeAssetSheet_() {
  var ss = SpreadsheetApp.getActiveSpreadsheet();
  var sh = ss.getSheetByName(SHEET_LIFE_ASSET_RAW);
  if (!sh) {
    sh = ss.insertSheet(SHEET_LIFE_ASSET_RAW);
  }

  var range = sh.getRange(1, 1, 1, LIFE_ASSET_HEADERS.length);
  var existing = range.getValues()[0];
  var same = true;
  for (var i = 0; i < LIFE_ASSET_HEADERS.length; i++) {
    if (String(existing[i] || '') !== LIFE_ASSET_HEADERS[i]) {
      same = false;
      break;
    }
  }
  if (!same) {
    range.setValues([LIFE_ASSET_HEADERS]);
  }
  return sh;
}

function hasFetchedLifeAssetToday_(sheet) {
  var lastRow = sheet.getLastRow();
  if (lastRow < 2) return false;

  var vals = sheet.getRange(2, LIFE_ASSET_COL.fetched_at + 1, lastRow - 1, 1).getValues();
  var todayStr = formatDate_(new Date());
  for (var i = vals.length - 1; i >= 0; i--) {
    if (extractDatePartLifeAsset_(vals[i][0]) === todayStr) {
      return true;
    }
  }
  return false;
}

function upsertLifeAssetRowByDate_(sheet, dateStr, row) {
  if (!dateStr) throw new Error('upsertLifeAssetRowByDate_ missing dateStr');

  var lastRow = sheet.getLastRow();
  if (lastRow < 2) {
    sheet.getRange(2, 1, 1, row.length).setValues([row]);
    return;
  }

  var dateVals = sheet.getRange(2, LIFE_ASSET_COL.date + 1, lastRow - 1, 1).getDisplayValues();
  for (var i = 0; i < dateVals.length; i++) {
    if (normYMD_(dateVals[i][0]) === dateStr) {
      sheet.getRange(i + 2, 1, 1, row.length).setValues([row]);
      return;
    }
  }

  sheet.getRange(lastRow + 1, 1, 1, row.length).setValues([row]);
}

/* =========================
 * Parsing / transforms
 * ========================= */

function parseNbs70CityMomTable_(text) {
  var normalized = String(text || '').replace(/\s+/g, '');
  var start = normalized.indexOf('北京');
  var end = normalized.indexOf('表2');
  if (start < 0 || end < 0 || end <= start) {
    throw new Error('NBS table 1 boundary not found');
  }

  var body = normalized.slice(start, end);
  var re = /([^\d]{2,5}?)(\d+\.\d)(\d+\.\d)(\d+\.\d)/g;
  var match;
  var momByCity = {};
  while ((match = re.exec(body)) !== null) {
    var city = normalizeNbsCityName_(match[1]);
    if (!city || momByCity.hasOwnProperty(city)) continue;
    momByCity[city] = Number(match[2]);
  }

  var cityNames = Object.keys(momByCity);
  if (cityNames.length < 65) {
    throw new Error('NBS 70-city parsed count too small: ' + cityNames.length);
  }

  return {
    momByCity: momByCity,
    all70: averageValues_(cityNames.map(function (city) { return momByCity[city]; }))
  };
}

function normalizeNbsCityName_(name) {
  return String(name || '').replace(/[^\u4e00-\u9fa5]/g, '');
}

function averageByCities_(map, cities) {
  var arr = [];
  for (var i = 0; i < cities.length; i++) {
    var v = map[cities[i]];
    if (isFiniteNumber_(v)) arr.push(v);
  }
  return averageValues_(arr);
}

function averageValues_(arr) {
  if (!arr || !arr.length) return '';
  var sum = 0;
  var n = 0;
  for (var i = 0; i < arr.length; i++) {
    var v = Number(arr[i]);
    if (!isFinite(v)) continue;
    sum += v;
    n++;
  }
  return n ? roundTo_(sum / n, 4) : '';
}

function htmlToReadableText_(html) {
  if (html == null) return '';
  return String(html)
    .replace(/<script[\s\S]*?<\/script>/gi, ' ')
    .replace(/<style[\s\S]*?<\/style>/gi, ' ')
    .replace(/<\/?(br|p|div|li|tr|td|th|h\d|table|tbody|thead)[^>]*>/gi, ' ')
    .replace(/&nbsp;/gi, ' ')
    .replace(/&#12288;/g, ' ')
    .replace(/&amp;/gi, '&')
    .replace(/&lt;/gi, '<')
    .replace(/&gt;/gi, '>')
    .replace(/<[^>]+>/g, ' ')
    .replace(/\s+/g, ' ')
    .trim();
}

function extractFirstLinkedItem_(html, titlePattern) {
  if (!html) return null;
  var re = /<a[^>]*href=["']([^"']+)["'][^>]*>([\s\S]*?)<\/a>/gi;
  var m;
  while ((m = re.exec(html)) !== null) {
    var title = htmlToReadableText_(m[2]);
    if (titlePattern.test(title)) {
      return { url: m[1], title: title };
    }
  }
  return null;
}

/* =========================
 * Small helpers
 * ========================= */

function executeLifeAssetFetcher_(label, fn) {
  try {
    var out = fn();
    if (!out) out = {};
    if (out.value === null || out.value === undefined) out.value = '';
    if (!out.obsDate) out.obsDate = '';
    if (!out.source) out.source = label;
    return out;
  } catch (e) {
    Logger.log('life_asset field failed | field=' + label + ' | error=' + e.message);
    return {
      value: '',
      obsDate: '',
      source: 'ERROR:' + label + ':' + safeSliceLifeAsset_(e.message, 120)
    };
  }
}

function pushLifeAssetSourceNote_(arr, field, obj) {
  arr.push(
    field
      + '=' + (obj && obj.source ? obj.source : 'unknown')
      + (obj && obj.obsDate ? '@' + obj.obsDate : '')
  );
}

function fetchLifeAssetText_(url, options) {
  var res = fetchLifeAssetUrl_(url, options || {
    method: 'get',
    muteHttpExceptions: true,
    headers: {
      'User-Agent': 'Mozilla/5.0'
    }
  });

  if (res.getResponseCode() >= 400) {
    throw new Error('HTTP=' + res.getResponseCode() + ' | url=' + url);
  }
  return res.getContentText('UTF-8');
}

function fetchLifeAssetUrl_(url, options) {
  if (typeof fetchWithFallback_ === 'function') {
    return fetchWithFallback_(url, options);
  }
  if (typeof safeFetch_ === 'function') {
    return safeFetch_(url, options);
  }
  return UrlFetchApp.fetch(url, options || {});
}

function toAbsoluteUrl_(baseUrl, maybeRelative) {
  if (!maybeRelative) return baseUrl;
  if (/^https?:\/\//i.test(maybeRelative)) return maybeRelative;
  if (maybeRelative.indexOf('//') === 0) return 'https:' + maybeRelative;
  if (maybeRelative.charAt(0) === '/') {
    var m = String(baseUrl).match(/^(https?:\/\/[^\/]+)/i);
    return (m ? m[1] : '') + maybeRelative;
  }
  return String(baseUrl).replace(/[^\/]+$/, '') + maybeRelative;
}

function inferQuarterEndFromText_(s) {
  var m = String(s || '').match(/(\d{4})年第?([一二三四1-4])季度/);
  if (!m) return '';
  var year = Number(m[1]);
  var qMap = { '一': 1, '二': 2, '三': 3, '四': 4, '1': 1, '2': 2, '3': 3, '4': 4 };
  var q = qMap[m[2]];
  if (!q) return '';
  return quarterEndYmd_(year, q);
}

function inferMonthFromText_(s) {
  var m = String(s || '').match(/(\d{4})年(\d{1,2})月份/);
  if (!m) return '';
  return monthEndYmd_(Number(m[1]), Number(m[2]));
}

function inferDateFromTitleText_(s) {
  var m = String(s || '').match(/(\d{4})-(\d{2})-(\d{2})/);
  if (!m) return '';
  return m[1] + '-' + m[2] + '-' + m[3];
}

function inferObsDateFromMonthDay_(s) {
  s = String(s || '').trim();
  if (!s) return formatDate_(new Date());
  var m = s.match(/(\d{2})-(\d{2})/);
  if (!m) return formatDate_(new Date());
  var now = new Date();
  return now.getFullYear() + '-' + m[1] + '-' + m[2];
}

function normalizeCompactYmd_(s) {
  s = String(s || '');
  if (/^\d{8}$/.test(s)) {
    return s.slice(0, 4) + '-' + s.slice(4, 6) + '-' + s.slice(6, 8);
  }
  return normYMD_(s);
}

function normalizeChineseDate_(s) {
  s = String(s || '').trim()
    .replace(/年/g, '-')
    .replace(/月/g, '-')
    .replace(/日/g, '');
  return normYMD_(s);
}

function monthEndYmd_(year, month) {
  var d = new Date(year, month, 0);
  return formatDate_(d);
}

function quarterEndYmd_(year, quarter) {
  var month = quarter * 3;
  return monthEndYmd_(year, month);
}

function extractDatePartLifeAsset_(v) {
  return normYMD_(v);
}

function formatDateTimeLifeAsset_(d) {
  if (!(d instanceof Date)) d = new Date(d);
  return Utilities.formatDate(d, Session.getScriptTimeZone(), 'yyyy-MM-dd HH:mm:ss');
}

function safeSliceLifeAsset_(s, len) {
  s = s == null ? '' : String(s);
  return s.length <= len ? s : s.slice(0, len);
}

function safeLogNum_(v) {
  return v === '' || v == null ? '' : String(v);
}

function roundTo_(num, digits) {
  if (!isFiniteNumber_(num)) return '';
  var p = Math.pow(10, digits || 0);
  return Math.round(num * p) / p;
}
