/********************
 * 13_source_fred.js
 * FRED 来源适配层。
 *
 * 负责：
 * - 逐条序列读取最新 observation
 * - 返回统一 { value, date, source } 结构
 ********************/


function fetchOverseasMacroFromFred_() {
  var apiKey = getRequiredSecret_('FRED_API_KEY');
  var out = {};

  Object.keys(OVERSEAS_MACRO_FRED_SERIES).forEach(function (field) {
    out[field] = fetchFredLatestObservation_(
      OVERSEAS_MACRO_FRED_SERIES[field],
      apiKey
    );
  });

  return out;
}

/**
 * 获取单个 FRED 序列最近可用的 observation。
 *
 * 关键处理：
 * - FRED 对部分序列会返回 '.' 作为空值
 * - 因此这里不是直接取第一条，而是向下寻找最近的有效数字
 */


function fetchFredLatestObservation_(seriesId, apiKey) {
  var url =
    'https://api.stlouisfed.org/fred/series/observations'
    + '?series_id=' + encodeURIComponent(seriesId)
    + '&api_key=' + encodeURIComponent(apiKey)
    + '&file_type=json'
    + '&sort_order=desc'
    + '&limit=10';

  var res = fetchOverseasMacroUrl_(url, {
    method: 'get',
    muteHttpExceptions: true
  });

  if (res.getResponseCode() !== 200) {
    throw new Error(
      'FRED HTTP=' + res.getResponseCode()
      + ' | seriesId=' + seriesId
      + ' | body=' + safeSliceOverseas_(res.getContentText(), 300)
    );
  }

  var json = JSON.parse(res.getContentText());
  var observations = json && json.observations ? json.observations : [];

  if (!observations.length) {
    throw new Error('FRED observations empty: ' + seriesId);
  }

  for (var i = 0; i < observations.length; i++) {
    var obs = observations[i];
    if (!obs || !obs.date) continue;
    if (obs.value === '.' || obs.value === '' || obs.value === null || obs.value === undefined) continue;

    var value = toNumberOrNull_(obs.value);
    if (!isFiniteNumber_(value)) continue;

    return {
      date: normYMD_(obs.date),
      value: value,
      source: 'FRED:' + seriesId
    };
  }

  throw new Error('FRED no valid observation: ' + seriesId);
}

/* =========================
 * Source 2: Alpha Vantage
 * ========================= */

/**
 * 从 Alpha Vantage 批量抓取商品字段。
 *
 * 说明：
 * - gold / wti / brent 为日频
 * - copper 当前固定 monthly
 * - 返回结构与 FRED 保持一致，便于后续统一拼表
 */
/**
 * 从 Alpha Vantage 批量抓取商品相关字段。
 *
 * 重要：
 * - Alpha Vantage 免费层有较严格的短时请求频率限制
 * - 因此这里不要连续无间隔请求
 * - 每次请求之间主动 sleep，避免触发 "1 request per second" 限制
 */

