/********************
 * 15_source_external_misc.js
 * 其余外部来源适配层。
 *
 * 当前已落地：
 * - Alpha Vantage 商品数据
 * - 新浪国债期货报价
 *
 * 预留：
 * - 上海黄金交易所（SGE）
 * - 中国银行存款利率 / 汇率牌价
 * - 余额宝 / 货基代理值
 ********************/


function fetchOverseasMacroFromAlphaVantage_() {
  var apiKey = getRequiredSecret_('ALPHA_VANTAGE_API_KEY');
  var out = {};
  var fields = Object.keys(OVERSEAS_MACRO_ALPHA_SERIES);

  for (var i = 0; i < fields.length; i++) {
    var field = fields[i];
    var spec = OVERSEAS_MACRO_ALPHA_SERIES[field];

    try {
      out[field] = fetchAlphaVantageLatestObservation_(spec, apiKey);
    } catch (e) {
      Logger.log(
        'alpha field failed'
        + ' | field=' + field
        + ' | fn=' + spec.fn
        + ' | error=' + e.message
      );
      out[field] = null;
    }

    /**
     * 免费层限频保护：
     * - 官方返回已明确提示 1 request per second
     * - 这里留 1.2 秒缓冲，尽量避免边界抖动
     */
    if (i < fields.length - 1) {
      Utilities.sleep(1200);
    }
  }

  return out;
}

/**
 * 获取单个 Alpha Vantage 商品序列最近 observation。
 *
 * 重要：
 * - Alpha Vantage 免费层超限时，常常不是 HTTP 4xx，而是在 JSON 里返回 Note / Information
 * - 所以这里必须同时检查 HTTP 状态码和 JSON 错误字段
 */

/**
 * 获取单个 Alpha Vantage 商品序列最近 observation。
 *
 * 重要说明：
 * - Alpha Vantage 的商品接口虽然都放在 Commodities 分类下，
 *   但不同 function 的返回结构不一定完全一致。
 * - 例如：
 *   - WTI / BRENT / COPPER 常见是 data[i].value
 *   - GOLD_SILVER_HISTORY 可能不是 value，而是 price / close 等其他字段
 *
 * 因此这里不能只写死 obs.value，而要做兼容提取。
 */


function fetchAlphaVantageLatestObservation_(spec, apiKey) {
  var url = buildAlphaVantageUrl_(spec, apiKey);

  var res = fetchOverseasMacroUrl_(url, {
    method: 'get',
    muteHttpExceptions: true
  });

  if (res.getResponseCode() !== 200) {
    throw new Error(
      'Alpha Vantage HTTP=' + res.getResponseCode()
      + ' | fn=' + spec.fn
      + ' | body=' + safeSliceOverseas_(res.getContentText(), 300)
    );
  }

  var body = res.getContentText();
  var json = JSON.parse(body);

  if (!json) {
    throw new Error('Alpha Vantage empty JSON: ' + spec.fn);
  }
  if (json['Error Message']) {
    throw new Error('Alpha Vantage error: ' + spec.fn + ' | ' + json['Error Message']);
  }
  if (json['Information']) {
    throw new Error('Alpha Vantage information: ' + spec.fn + ' | ' + json['Information']);
  }
  if (json['Note']) {
    throw new Error('Alpha Vantage note: ' + spec.fn + ' | ' + json['Note']);
  }

  var data = json.data || [];
  if (!data.length) {
    throw new Error(
      'Alpha Vantage data empty: ' + spec.fn
      + ' | body=' + safeSliceOverseas_(body, 500)
    );
  }

  for (var i = 0; i < data.length; i++) {
    var obs = data[i];
    if (!obs || !obs.date) continue;

    var value = extractAlphaVantageNumericValue_(obs);
    if (!isFiniteNumber_(value)) continue;

    return {
      date: normYMD_(obs.date),
      value: value,
      source: 'ALPHA_VANTAGE:' + spec.fn + ':' + spec.interval
    };
  }

  /**
   * 如果走到这里，说明 data 有内容，但没有找到可识别的数值字段。
   * 把首条记录的 key 打出来，方便快速修正解析逻辑。
   */
  var sample = data[0] || {};
  var sampleKeys = Object.keys(sample).join(',');

  throw new Error(
    'Alpha Vantage no valid observation: ' + spec.fn
    + ' | sample_keys=' + sampleKeys
    + ' | sample=' + safeSliceOverseas_(JSON.stringify(sample), 500)
  );
}





/**
 * 从 Alpha Vantage 单条 observation 中提取数值。
 *
 * 为什么要单独封装：
 * - 不同商品 function 的 JSON 字段名不完全一致
 * - 不能把所有接口都假定成 data[i].value
 *
 * 当前兼容顺序：
 * 1) value   - WTI / BRENT / COPPER 常见
 * 2) price   - GOLD / SILVER 类接口常见候选
 * 3) close   - 兜底兼容
 * 4) 其他常见命名再向后补
 */


function extractAlphaVantageNumericValue_(obs) {
  if (!obs) return null;

  var candidates = [
    obs.value,
    obs.price,
    obs.close,
    obs.gold,
    obs.silver
  ];

  for (var i = 0; i < candidates.length; i++) {
    var n = toNumberOrNull_(candidates[i]);
    if (isFiniteNumber_(n)) {
      return n;
    }
  }

  return null;
}




/**
 * 构造 Alpha Vantage 商品 URL。
 *
 * 目前支持：
 * - GOLD_SILVER_HISTORY
 * - WTI
 * - BRENT
 * - COPPER
 */


function buildAlphaVantageUrl_(spec, apiKey) {
  var url =
    'https://www.alphavantage.co/query'
    + '?function=' + encodeURIComponent(spec.fn)
    + '&apikey=' + encodeURIComponent(apiKey);

  if (spec.symbol) {
    url += '&symbol=' + encodeURIComponent(spec.symbol);
  }
  if (spec.interval) {
    url += '&interval=' + encodeURIComponent(spec.interval);
  }
  return url;
}

/* =========================
 * Sheet helpers
 * ========================= */

/**
 * 获取或创建 原始_海外宏观 工作表，并校验表头。
 */


function fetchSinaFuturePrice_(symbol) {
  var url = "https://hq.sinajs.cn/list=" + encodeURIComponent(symbol);
  var res = safeFetch_(url, {
    method: "get",
    headers: {
      "User-Agent": "Mozilla/5.0",
      "Referer": "https://finance.sina.com.cn/"
    }
  }, 4);

  var txt = res.getContentText();
  var m = txt.match(/="([^"]*)"/);
  if (!m) return "";

  var arr = m[1].split(",");

  if (arr.length > 3) {
    var p = parseFloat(arr[3]);
    if (!isNaN(p) && p > 0) return p;
  }

  for (var j = 0; j < arr.length; j++) {
    var v2 = parseFloat(arr[j]);
    if (!isNaN(v2) && v2 > 0) return v2;
  }
  return "";
}


function fetchSgeGoldSnapshot_() {
  return { date: '', gold_cny: '', source: LIFE_ASSET_SOURCE.sge_home || '' };
}

function fetchBocDeposit1ySnapshot_() {
  return { date: '', deposit_1y: '', source: LIFE_ASSET_SOURCE.boc_deposit_list || '' };
}

function fetchMoneyFund7dSnapshot_() {
  return { date: '', money_fund_7d: '', source: LIFE_ASSET_SOURCE.money_fund_proxy_url || '' };
}
