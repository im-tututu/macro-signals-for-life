/********************
 * 16_source_bond_index.js
 * 债券指数特征抓取来源层。
 *
 * 职责：
 * - 根据配置表中的发行方 / 代码 / URL，统一抓取债券指数特征
 * - 输出统一字段：data_date / dm / y / cons_number / d / v
 *
 * 说明：
 * - 表格写入逻辑统一放在 25_raw_bond_index.js
 * - 不替代 40_formula_chinabond_index.js，后者仍可保留给单元格公式/人工核对使用
 ********************/

/**
 * 统一抓取一行配置对应的指数特征。
 *
 * 返回：
 * {
 *   ok: true/false,
 *   provider: '中证'/'国证'/'中债',
 *   data_date: 'yyyy-MM-dd',
 *   dm: number|'',
 *   y: number|'',
 *   cons_number: number|'',
 *   d: number|'',
 *   v: number|'',
 *   raw_json: '...',
 *   error: '...'
 * }
 */
function fetchBondIndexFeatureByRow_(rowObj) {
  rowObj = rowObj || {};

  var provider = biwToStr_(
    rowObj.provider || rowObj['指数发行公司'] || rowObj['provider_guess']
  );
  var code = biwToStr_(
    rowObj.index_code || rowObj['指数代码'] || rowObj['代码'] || rowObj['Column 19']
  );
  var sourceUrl = biwToStr_(
    rowObj.source_url || rowObj['source_url'] || rowObj['链接'] || rowObj['详情链接'] || rowObj['Column 15']
  );

  if (!provider) {
    provider = inferBondIndexProvider_(rowObj);
  }

  if (!code) {
    return {
      ok: false,
      provider: provider,
      error: 'missing index code',
      data_date: '',
      dm: '',
      y: '',
      cons_number: '',
      d: '',
      v: '',
      raw_json: ''
    };
  }

  try {
    if (provider === '中证') return fetchCsiBondIndexFeature_(code);
    if (provider === '国证') return fetchCniBondIndexFeature_(code);
    if (provider === '中债') return fetchChinabondIndexFeature_(code);

    // 兜底：按 URL 或代码形态推断。
    if (/csindex\.com\.cn/i.test(sourceUrl)) return fetchCsiBondIndexFeature_(code);
    if (/cnindex\.com\.cn/i.test(sourceUrl)) return fetchCniBondIndexFeature_(code);
    if (/^8a8b/i.test(code)) return fetchChinabondIndexFeature_(code);
    if (/^[A-Z]\d+/i.test(code)) return fetchCsiBondIndexFeature_(code);
    if (/^\d{6}$/.test(code)) return fetchCsiBondIndexFeature_(code);

    return {
      ok: false,
      provider: provider,
      error: 'unsupported provider: ' + provider,
      data_date: '',
      dm: '',
      y: '',
      cons_number: '',
      d: '',
      v: '',
      raw_json: ''
    };
  } catch (e) {
    return {
      ok: false,
      provider: provider,
      error: String(e),
      data_date: '',
      dm: '',
      y: '',
      cons_number: '',
      d: '',
      v: '',
      raw_json: ''
    };
  }
}

/**
 * 中证债券指数特征。
 * 现有 40_formula_chinabond_index.js 就是走这个接口。
 */
function fetchCsiBondIndexFeature_(code) {
  var url = 'https://www.csindex.com.cn/csindex-home/perf/get-bond-index-feature/' + encodeURIComponent(code);
  var json = safeFetchJson_(url, {
    method: 'get',
    headers: {
      accept: 'application/json, text/plain, */*'
    },
    muteHttpExceptions: true
  }, 3);
  var d = json && json.data ? json.data : {};

  return {
    ok: true,
    provider: '中证',
    data_date: biwNormalizeDate_(d.date || d.tradeDate || d.trade_date || ''),
    dm: biwToNum_(d.dm),
    y: biwToNum_(d.y),
    cons_number: biwToNum_(d.consNumber),
    d: biwToNum_(d.d),
    v: biwToNum_(d.v),
    raw_json: JSON.stringify(d)
  };
}

/**
 * 国证债券指数特征。
 * 与现有 40_formula_chinabond_index.js 保持相同接口口径。
 */
function fetchCniBondIndexFeature_(code) {
  var url = 'https://www.cnindex.com.cn/module/index-detail.html?act_menu=1&indexCode=' + encodeURIComponent(code);
  var json = safeFetchJson_(url, {
    method: 'get',
    headers: {
      accept: 'application/json, text/plain, */*'
    },
    muteHttpExceptions: true
  }, 3);
  var d = json && json.data ? json.data : {};

  return {
    ok: true,
    provider: '国证',
    data_date: biwNormalizeDate_(d.date || d.tradeDate || d.trade_date || ''),
    dm: biwToNum_(d.dm),
    y: biwToNum_(d.y),
    cons_number: biwToNum_(d.consNumber),
    d: biwToNum_(d.d),
    v: biwToNum_(d.v),
    raw_json: JSON.stringify(d)
  };
}

/**
 * 中债债券指数特征。
 * 复用 10_source_chinabond.js 中已有的 fetchChinabondIndexSeries_ / getLatestPoint_。
 *
 * 当前中债这一路，仓库现有 helper 只能稳定拿到：
 * - 修正久期 dm（PJSZFJQ）
 * - 到期收益率 y（PJSZFDQSYL）
 * - 凸性 v（PJSZFTX）
 *
 * cons_number / d 暂留空。
 */
function fetchChinabondIndexFeature_(indexid) {
  var json = fetchChinabondIndexSeries_(indexid);

  var durationPoint = getLatestPoint_(json.PJSZFJQ_00);
  var ytmPoint = getLatestPoint_(json.PJSZFDQSYL_00);
  var convexityPoint = getLatestPoint_(json.PJSZFTX_00);

  if (!durationPoint) {
    throw new Error('中债未获取到久期数据: ' + indexid);
  }

  return {
    ok: true,
    provider: '中债',
    data_date: biwNormalizeDate_(durationPoint.date),
    dm: biwToNum_(durationPoint.value),
    y: ytmPoint ? biwToNum_(ytmPoint.value) : '',
    cons_number: '',
    d: '',
    v: convexityPoint ? biwToNum_(convexityPoint.value) : '',
    raw_json: JSON.stringify({
      duration: durationPoint,
      ytm: ytmPoint || null,
      convexity: convexityPoint || null
    })
  };
}

function inferBondIndexProvider_(rowObj) {
  rowObj = rowObj || {};
  var name = biwToStr_(rowObj.index_name || rowObj['指数'] || rowObj['指数名称']);
  var code = biwToStr_(rowObj.index_code || rowObj['指数代码'] || rowObj['代码'] || rowObj['Column 19']);
  var sourceUrl = biwToStr_(rowObj.source_url || rowObj['source_url'] || rowObj['Column 15']);

  if (/^中债/.test(name) || /^8a8b/i.test(code)) return '中债';
  if (/^(国证|深)/.test(name) || /cnindex\.com\.cn/i.test(sourceUrl)) return '国证';
  if (/^(中证|上证|沪)/.test(name) || /csindex\.com\.cn/i.test(sourceUrl)) return '中证';
  return '';
}

function biwNormalizeDate_(v) {
  if (typeof normYMD_ === 'function') return normYMD_(v);
  return biwToStr_(v);
}

function biwToNum_(v) {
  if (typeof toNum_ === 'function') return toNum_(v);
  if (v === '' || v === null || v === undefined || v === 'N/A') return '';
  var n = Number(String(v).replace(/,/g, ''));
  return isNaN(n) ? '' : n;
}

function biwToStr_(v) {
  if (typeof toStr_ === 'function') return toStr_(v);
  return v == null ? '' : String(v).trim();
}
