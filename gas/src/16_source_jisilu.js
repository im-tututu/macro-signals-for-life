/********************
 * 16_source_jisilu.js
 * 集思录指数 ETF 数据源适配层。
 *
 * 设计原则：
 * - 不把登录 Cookie 写进代码仓库，统一走 Script Properties
 * - 抓取时优先使用当前 Cookie
 * - 命中“游客仅显示前 20 条”时，可自动调用外部 refresh webhook 刷新 Cookie 后重试一次
 * - 只做“源适配”，不负责写表
 ********************/

/** 集思录 ETF 列表接口 */
var JISILU_ETF_LIST_URL = 'https://www.jisilu.cn/data/etf/etf_list/';
var JISILU_ETF_REFERER_URL = 'https://www.jisilu.cn/data/etf/';

/** Script Properties keys */
var PROP_JISILU_COOKIE = 'JISILU_COOKIE';
var PROP_JISILU_REFRESH_URL = 'JISILU_REFRESH_URL';
var PROP_JISILU_REFRESH_TOKEN = 'JISILU_REFRESH_TOKEN';
var PROP_JISILU_COOKIE_UPDATED_AT = 'JISILU_COOKIE_UPDATED_AT';

/** 调试开关：true 时会记录更多日志 */
var JISILU_DEBUG = false;

/**
 * 手动初始化 / 更新 Cookie。
 *
 * 用法：
 * 1) 浏览器登录集思录
 * 2) F12 -> Network -> etf_list 请求
 * 3) 复制 Request Headers 里的整段 Cookie 值（不要包含“Cookie:”前缀）
 * 4) 把下面字符串替换后执行一次
 */
function initJisiluCookieManual_() {
  var cookie = 'Hm_lvt_164fe01b1433a19b507595a43bf58262=1745384500; kbz_newcookie=1; kbzw__Session=0eb0f9uatdiaufpusf769qgc36; kbzw__user_login=7Obd08_P1ebax9aX69fq1uvs5-2Cq47k2ujc8NvpxtS-otiksMKmk6erqc-rz6uV2MKs2KqwkaTEqN-mzbKdqceugrKk6OHFzr6fqq6craSog7GOuMvR1YyhmKutpqCvpKmbl5ykzLTWvpuu4_Pe1eXNppekkZefwNnE2c_o6OjRtIzA0OrG45fA2cSQsMeZzYmqnNaTq8CuoJO50eDN2dDay8TV65GrlK6lpq6BmKy8zcK1pYzjy-HGl77Y28zfipS83dvo2dyRp5WtpaOmkZ6RlMzWz9re4JGrlK6lpq6BtcXbqKadr5qnkKaPpw..';
  setJisiluCookie_(cookie);
}

/** 保存 Cookie 到 Script Properties。 */
function setJisiluCookie_(cookieHeader) {
  cookieHeader = toStr_(cookieHeader);
  if (!cookieHeader) {
    throw new Error('empty JISILU_COOKIE');
  }
  var props = PropertiesService.getScriptProperties();
  props.setProperty(PROP_JISILU_COOKIE, cookieHeader);
  props.setProperty(PROP_JISILU_COOKIE_UPDATED_AT, formatDateTime_(new Date()));
  return cookieHeader;
}

/** 删除 Cookie。 */
function clearJisiluCookie_() {
  var props = PropertiesService.getScriptProperties();
  props.deleteProperty(PROP_JISILU_COOKIE);
  props.deleteProperty(PROP_JISILU_COOKIE_UPDATED_AT);
}

/** 获取当前 Cookie。 */
function getJisiluCookie_() {
  return PropertiesService.getScriptProperties().getProperty(PROP_JISILU_COOKIE) || '';
}

/** 获取当前 Cookie 更新时间。 */
function getJisiluCookieUpdatedAt_() {
  return PropertiesService.getScriptProperties().getProperty(PROP_JISILU_COOKIE_UPDATED_AT) || '';
}

/** 获取 refresh webhook URL。 */
function getJisiluRefreshUrl_() {
  return PropertiesService.getScriptProperties().getProperty(PROP_JISILU_REFRESH_URL) || '';
}

/** 获取 refresh webhook Bearer Token。 */
function getJisiluRefreshToken_() {
  return PropertiesService.getScriptProperties().getProperty(PROP_JISILU_REFRESH_TOKEN) || '';
}

/** 给集思录请求统一拼 headers。 */
function buildJisiluHeaders_() {
  var headers = {
    'User-Agent': 'Mozilla/5.0',
    'Referer': JISILU_ETF_REFERER_URL,
    'Accept': 'application/json, text/javascript, */*; q=0.01',
    'X-Requested-With': 'XMLHttpRequest'
  };

  var cookie = getJisiluCookie_();
  if (cookie) {
    headers.Cookie = cookie;
  }
  return headers;
}

/** 构造 ETF 列表 URL。 */
function buildJisiluEtfListUrl_(options) {
  options = options || {};

  var params = [
    '___jsl=LST___t=' + new Date().getTime(),
    'rp=' + encodeURIComponent(String(options.rowsPerPage || 500)),
    'page=' + encodeURIComponent(String(options.page || 1))
  ];

  // 成交额过滤，单位：万元。空字符串表示不限。
  if (options.minVolumeWan !== undefined && options.minVolumeWan !== null) {
    params.push('volume=' + encodeURIComponent(String(options.minVolumeWan)));
  }

  // 规模过滤，单位：亿元。2 表示“规模 >= 2 亿元”。
  if (options.minUnitTotalYi !== undefined && options.minUnitTotalYi !== null) {
    params.push('unit_total=' + encodeURIComponent(String(options.minUnitTotalYi)));
  }

  // 兼容可能的其他透传参数
  if (!isBlank_(options.extraQueryString)) {
    params.push(String(options.extraQueryString).replace(/^[?&]+/, ''));
  }

  return JISILU_ETF_LIST_URL + '?' + params.join('&');
}

/**
 * 检测响应是否命中游客限制。
 * 典型提示：
 * “共有 1212 条记录，游客仅显示前 20 条，请登录查看完整列表数据”
 */
function detectJisiluTouristLimit_(rawText, json) {
  var msgParts = [];
  if (!isBlank_(rawText)) msgParts.push(String(rawText));
  if (json && typeof json === 'object') {
    if (!isBlank_(json.err_msg)) msgParts.push(String(json.err_msg));
    if (!isBlank_(json.message)) msgParts.push(String(json.message));
    if (!isBlank_(json.msg)) msgParts.push(String(json.msg));
    if (!isBlank_(json.notice)) msgParts.push(String(json.notice));
  }
  var s = msgParts.join(' | ');
  return /游客仅显示前\s*20\s*条/.test(s) || /请登录查看完整列表数据/.test(s);
}

/** 判断结果是否疑似游客态。 */
function looksLikeJisiluTouristRows_(rows) {
  rows = rows || [];
  return rows.length > 0 && rows.length <= 20;
}

/** 生成页面签名，用于检测重复页。 */
function buildJisiluPageSignature_(rows) {
  rows = rows || [];
  var keys = [];
  for (var i = 0; i < rows.length && i < 30; i++) {
    var cell = rows[i] && rows[i].cell ? rows[i].cell : rows[i];
    keys.push(
      toStr_(cell.fund_id) || toStr_(rows[i].id) || ('idx_' + i)
    );
  }
  return keys.join('|') + '::' + rows.length;
}

/** 发起单页请求。 */
function fetchJisiluEtfIndexPageRaw_(options) {
  options = options || {};
  var url = buildJisiluEtfListUrl_(options);

  var resp = safeFetch_(url, {
    method: 'get',
    muteHttpExceptions: true,
    headers: buildJisiluHeaders_()
  }, 3);

  var code = resp.getResponseCode();
  var rawText = resp.getContentText('UTF-8');

  if (code < 200 || code >= 300) {
    throw new Error('Jisilu ETF HTTP=' + code + ' | body=' + safeSlice_(rawText, 500));
  }

  var json;
  try {
    json = JSON.parse(rawText);
  } catch (e) {
    throw new Error('Jisilu ETF 非 JSON 返回 | body=' + safeSlice_(rawText, 500));
  }

  var rows = json && json.rows ? json.rows : [];
  var records = toNum_(json && (json.records || json.total || json.total_rows || ''));

  if (JISILU_DEBUG) {
    Logger.log(
      'Jisilu page raw'
      + ' | page=' + (options.page || 1)
      + ' | rows=' + rows.length
      + ' | records=' + records
    );
  }

  return {
    ok: true,
    url: url,
    code: code,
    rawText: rawText,
    json: json,
    rows: rows,
    records: records
  };
}

/**
 * 拉取全量指数 ETF。
 *
 * 终止条件：
 * - 空页
 * - 重复页
 * - 本页没有新增 fund_id
 * - 到达 maxPages
 */
function fetchJisiluEtfIndexAll_(options) {
  options = options || {};

  var rowsPerPage = Number(options.rowsPerPage || 500);
  var maxPages = Number(options.maxPages || 20);
  var allRows = [];
  var seenPageSignatures = {};
  var seenFundIds = {};
  var lastResult = null;
  var totalRecords = '';

  for (var page = 1; page <= maxPages; page++) {
    var one = fetchJisiluEtfIndexPageRaw_({
      page: page,
      rowsPerPage: rowsPerPage,
      minVolumeWan: options.minVolumeWan,
      minUnitTotalYi: options.minUnitTotalYi,
      extraQueryString: options.extraQueryString
    });

    lastResult = one;
    if (one.records !== '') totalRecords = one.records;

    if (detectJisiluTouristLimit_(one.rawText, one.json)) {
      var suffix = totalRecords !== '' ? (' | total=' + totalRecords) : '';
      throw new Error('Jisilu tourist limited' + suffix);
    }

    var rows = one.rows || [];
    if (!rows.length) {
      Logger.log('Jisilu ETF empty page | page=' + page);
      break;
    }

    var pageSignature = buildJisiluPageSignature_(rows);
    if (seenPageSignatures[pageSignature]) {
      Logger.log('Jisilu ETF repeated page | page=' + page + ' | rows=' + rows.length);
      break;
    }
    seenPageSignatures[pageSignature] = true;

    var added = 0;
    for (var i = 0; i < rows.length; i++) {
      var row = rows[i];
      var cell = row && row.cell ? row.cell : row;
      var fundId = toStr_(cell && cell.fund_id) || toStr_(row && row.id);

      if (!fundId) {
        // fund_id 缺失时仍保留一份，但尽量生成稳定 key
        fundId = 'row_' + page + '_' + i + '_' + buildShortCacheKey_('jisilu_etf', [JSON.stringify(row)]);
      }

      if (seenFundIds[fundId]) continue;
      seenFundIds[fundId] = true;
      allRows.push(row);
      added++;
    }

    Logger.log(
      'Jisilu ETF fetched'
      + ' | page=' + page
      + ' | page_rows=' + rows.length
      + ' | added=' + added
      + ' | total=' + allRows.length
      + (totalRecords !== '' ? (' | records=' + totalRecords) : '')
    );

    if (added === 0) {
      Logger.log('Jisilu ETF no new rows added | stop at page=' + page);
      break;
    }
  }

  return {
    rows: allRows,
    records: totalRecords,
    last_url: lastResult ? lastResult.url : '',
    raw: lastResult ? lastResult.json : null,
    rawText: lastResult ? lastResult.rawText : '',
    filter: {
      rowsPerPage: rowsPerPage,
      minVolumeWan: options.minVolumeWan,
      minUnitTotalYi: options.minUnitTotalYi
    }
  };
}

/** 调外部 refresh webhook，获取新的 Cookie。 */
function refreshJisiluCookieFromWebhook_() {
  var refreshUrl = getJisiluRefreshUrl_();
  var refreshToken = getJisiluRefreshToken_();

  if (!refreshUrl) {
    throw new Error('missing ' + PROP_JISILU_REFRESH_URL);
  }

  var resp = safeFetch_(refreshUrl, {
    method: 'post',
    contentType: 'application/json',
    muteHttpExceptions: true,
    headers: refreshToken ? { Authorization: 'Bearer ' + refreshToken } : {},
    payload: JSON.stringify({
      source: 'gas',
      action: 'refresh_cookie',
      site: 'jisilu'
    })
  }, 2);

  var code = resp.getResponseCode();
  var text = resp.getContentText('UTF-8');

  if (code < 200 || code >= 300) {
    throw new Error('refresh webhook failed | code=' + code + ' | body=' + safeSlice_(text, 500));
  }

  var json;
  try {
    json = JSON.parse(text);
  } catch (e) {
    throw new Error('refresh webhook non-json | body=' + safeSlice_(text, 500));
  }

  var cookie = toStr_(json && json.cookie);
  if (!cookie) {
    throw new Error('refresh webhook returned empty cookie');
  }

  setJisiluCookie_(cookie);
  Logger.log('Jisilu cookie refreshed | updated_at=' + getJisiluCookieUpdatedAt_());
  return cookie;
}

/**
 * 先用当前 Cookie 抓；若命中游客限制，则刷新 Cookie 后重试一次。
 */
function fetchJisiluEtfIndexAllWithAutoRefresh_(options) {
  options = options || {};
  try {
    return fetchJisiluEtfIndexAll_(options);
  } catch (e) {
    var msg = String(e && e.message || e);
    var needRefresh =
      /Jisilu tourist limited/i.test(msg) ||
      /游客仅显示前\s*20\s*条/.test(msg) ||
      /请登录查看完整列表数据/.test(msg);

    if (!needRefresh) throw e;

    Logger.log('Jisilu cookie may be expired, trying refresh webhook once...');
    refreshJisiluCookieFromWebhook_();
    return fetchJisiluEtfIndexAll(options);
  }
}

/** 调试用：查看当前 Cookie 状态。 */
function debugJisiluCookieState_() {
  return {
    has_cookie: !!getJisiluCookie_(),
    cookie_updated_at: getJisiluCookieUpdatedAt_(),
    has_refresh_url: !!getJisiluRefreshUrl_(),
    has_refresh_token: !!getJisiluRefreshToken_()
  };
}

/** 调试用：仅抓第一页。 */
function testFetchJisiluEtfPage_() {
  return fetchJisiluEtfIndexPageRaw_({
    page: 1,
    rowsPerPage: 500,
    minUnitTotalYi: 2,
    minVolumeWan: ''
  });
}
