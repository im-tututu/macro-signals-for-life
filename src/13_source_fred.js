/********************
 * 13_source_fred.js
 * FRED 来源适配层。
 *
 * 负责：
 * - 集中维护 FRED 序列映射与接口 URL
 * - 逐条序列读取最新 observation
 * - 返回统一 { value, date, source } 结构
 ********************/

var FRED_OBSERVATIONS_URL = 'https://api.stlouisfed.org/fred/series/observations';
var OVERSEAS_MACRO_FRED_SERIES = {
  fed_upper: 'DFEDTARU',
  fed_lower: 'DFEDTARL',
  sofr: 'SOFR',
  ust_2y: 'DGS2',
  ust_10y: 'DGS10',
  us_real_10y: 'DFII10',
  usd_broad: 'DTWEXBGS',
  usd_cny: 'DEXCHUS',
  vix: 'VIXCLS',
  spx: 'SP500',
  nasdaq_100: 'NASDAQ100'
};

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
  var url = FRED_OBSERVATIONS_URL
    + '?series_id=' + encodeURIComponent(seriesId)
    + '&api_key=' + encodeURIComponent(apiKey)
    + '&file_type=json'
    + '&sort_order=desc'
    + '&limit=10';

  var res = safeFetch_(url, {
    method: 'get',
    muteHttpExceptions: true
  }, 3);

  if (res.getResponseCode() !== 200) {
    throw new Error(
      'FRED HTTP=' + res.getResponseCode()
      + ' | seriesId=' + seriesId
      + ' | body=' + safeSlice_(res.getContentText(), 300)
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
