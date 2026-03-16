/********************
 * 24_raw_macro.js
 * 宏观补充原始表。
 *
 * 当前主表仍沿用旧表名“原始_海外宏观”，避免打断既有公式与指标链路。
 *
 * 当前接入：
 * - FRED（独立来源文件）
 * - Alpha Vantage（放在 external misc 中）
 * - 国家统计局 / SGE / BOC / 余额宝（先预留来源接口与民生资产表抓取入口）
 ********************/

/********************
 * 15_raw_overseas_macro.js
 *
 * 原始_海外宏观
 * ------------------
 * 这张表的职责是沉淀“海外宏观背景”的原始输入，不在这里直接做解释性判断。
 *
 * 当前字段分成两组来源：
 * 1) FRED
 *    - 联邦基金目标区间、SOFR、2Y / 10Y 美债、10Y 实际利率
 *    - 广义美元指数、美元兑人民币、VIX、SPX、NASDAQ100
 * 2) Alpha Vantage
 *    - 黄金、WTI、Brent、铜
 *
 * 设计原则：
 * 1) 抓取前先检查今天是否已经抓过；今天抓过就直接跳过
 * 2) 判重看 fetched_at 的日期，而不是 date
 *    - 原因：海外数据存在时区差，今天抓到的数据 observation date 可能仍是美国上一交易日
 * 3) 两个数据源分开函数抓，方便排错与后续替换
 * 4) API key 不写在代码里
 *    - 运行时统一从 Script Properties 读取
 *    - GitHub Actions 可通过 setApiKeysFromParams() 写入 Script Properties
 * 5) 未配置 secrets 时不抛错，不阻塞现有国内数据日更流程
 ********************/

/**
 * 手工测试入口：默认遵循“今天已抓过则跳过”。
 */


function testOverseasMacro() {
  return forceFetchOverseasMacro_();
}

/**
 * 手工测试入口：强制刷新。
 *
 * 适用场景：
 * - 你刚改了字段或 source 说明，想重新覆盖同一个 date 的行
 * - 你怀疑上一次抓取结果有空值，想立即重抓
 */


function forceFetchOverseasMacro_() {
  return fetchOverseasMacro_(false);
}

/**
 * 海外宏观主入口。
 *
 * @param {boolean=} forceRefresh
 *   true  => 忽略“今天已抓取”判重，直接重抓
 *   false => 今天抓过则跳过
 *
 * @return {Object}
 *   统一返回状态对象，便于在日志或未来 CI 中定位结果。
 */


function fetchOverseasMacro_(forceRefresh) {
  var force = forceRefresh === true;
  var sheet = getOrCreateOverseasMacroSheet_();

  /**
   * 先检查 secrets，未配置时只记录提示并跳过。
   *
   * 为什么不直接 throw：
   * - runEnhancedSystem() 已经承担国内主流程
   * - 海外宏观模块属于新增能力，不应该因为尚未配置 key 就阻塞主流程
   */
  var secretStatus = getApiKeyStatus_();
  if (!secretStatus.fred || !secretStatus.alphaVantage) {
    logOverseasMacroSecretsHint_(secretStatus);
    return {
      skipped: true,
      reason: 'missing_secrets',
      secretStatus: secretStatus
    };
  }

  /**
   * 默认模式：今天已抓取则直接跳过，不再消耗外部 API 次数。
   *
   * 判重依据：
   * - 看 fetched_at 的日期是否等于今天（脚本时区）
   * - 不看 date；因为 date 是海外 observation date，不一定等于今天
   */
  if (!force && hasFetchedOverseasMacroToday_(sheet)) {
    Logger.log('overseas_macro skip | reason=already_fetched_today');
    return {
      skipped: true,
      reason: 'already_fetched_today'
    };
  }

  /**
   * 分别抓取两个来源。
   * 保持函数边界清晰，出现问题时更容易快速定位到 FRED 还是 Alpha Vantage。
   */
  var fredData = fetchOverseasMacroFromFred_();
  var alphaData = fetchOverseasMacroFromAlphaVantage_();

  /**
   * 选取本行 date。
   *
   * 这是“本次快照的主日期”，不是 fetched_at。
   * 选择顺序：
   * 1) 风险资产日期（spx / nasdaq_100）
   * 2) 长端利率日期（ust_10y）
   * 3) 短端利率日期（sofr）
   * 4) 最后退到脚本当前日期
   */
  var rowDate =
    pickObsDate_(fredData.spx) ||
    pickObsDate_(fredData.nasdaq_100) ||
    pickObsDate_(fredData.ust_10y) ||
    pickObsDate_(fredData.sofr) ||
    formatDate_(new Date());

  var fetchedAt = formatDateTimeOverseas_(new Date());

  /**
   * 拼成最终一行。
   * 这里不做任何派生计算；所有利差、比值、分位、信号都留给 metrics / signal。
   */
  var row = [
    rowDate,
    pickObsValue_(fredData.fed_upper),
    pickObsValue_(fredData.fed_lower),
    pickObsValue_(fredData.sofr),
    pickObsValue_(fredData.ust_2y),
    pickObsValue_(fredData.ust_10y),
    pickObsValue_(fredData.us_real_10y),
    pickObsValue_(fredData.usd_broad),
    pickObsValue_(fredData.usd_cny),
    pickObsValue_(alphaData.gold),
    pickObsValue_(alphaData.wti),
    pickObsValue_(alphaData.brent),
    pickObsValue_(alphaData.copper),
    pickObsValue_(fredData.vix),
    pickObsValue_(fredData.spx),
    pickObsValue_(fredData.nasdaq_100),
    buildOverseasMacroSourceNote_(fredData, alphaData),
    fetchedAt
  ];

  /**
   * 用 date 做 upsert：
   * - 同一 date 覆盖
   * - 新 date 追加
   *
   * 这样即使你 force refresh，也不会生成重复 date 的脏数据。
   */
  upsertOverseasMacroRowByDate_(sheet, rowDate, row);

  Logger.log(
    'overseas_macro update'
      + ' | date=' + rowDate
      + ' | fetched_at=' + fetchedAt
      + ' | fed_upper=' + row[OVERSEAS_MACRO_COL.fed_upper]
      + ' | sofr=' + row[OVERSEAS_MACRO_COL.sofr]
      + ' | ust_10y=' + row[OVERSEAS_MACRO_COL.ust_10y]
      + ' | usd_broad=' + row[OVERSEAS_MACRO_COL.usd_broad]
      + ' | gold=' + row[OVERSEAS_MACRO_COL.gold]
      + ' | wti=' + row[OVERSEAS_MACRO_COL.wti]
      + ' | brent=' + row[OVERSEAS_MACRO_COL.brent]
      + ' | copper=' + row[OVERSEAS_MACRO_COL.copper]
  );

  return {
    skipped: false,
    date: rowDate,
    fetched_at: fetchedAt
  };
}

/* =========================
 * Secrets / GitHub Actions
 * ========================= */

/**
 * 将 GitHub Secrets 同步到 Apps Script 的 Script Properties。
 *
 * 推荐搭配：
 * - GitHub Actions
 * - clasp run-function setApiKeysFromParams --params '["FRED_KEY","ALPHA_KEY"]'
 *
 * 说明：
 * - 这里只写入，不在日志中回显真实 key
 * - 写入的是 Script Properties，因此对整个脚本可见
 */


function setApiKeysFromParams(fredApiKey, alphaVantageApiKey) {
  if (!fredApiKey) {
    throw new Error('缺少 fredApiKey');
  }
  if (!alphaVantageApiKey) {
    throw new Error('缺少 alphaVantageApiKey');
  }

  PropertiesService.getScriptProperties().setProperties({
    FRED_API_KEY: String(fredApiKey),
    ALPHA_VANTAGE_API_KEY: String(alphaVantageApiKey)
  }, false);

  Logger.log('api secrets synced to Script Properties');
  return {
    ok: true,
    keys: ['FRED_API_KEY', 'ALPHA_VANTAGE_API_KEY']
  };
}

/**
 * 返回当前 key 是否已配置。
 *
 * 注意：
 * - 只返回布尔状态
 * - 不返回真实值，避免在日志或 API 返回中泄露密钥
 */


function getApiKeyStatus_() {
  var props = PropertiesService.getScriptProperties();
  return {
    fred: !!props.getProperty('FRED_API_KEY'),
    alphaVantage: !!props.getProperty('ALPHA_VANTAGE_API_KEY')
  };
}

/**
 * 当 secrets 缺失时，打印足够明确的提示。
 *
 * 提示内容分成两层：
 * 1) GAS 侧需要配置的 Script Properties 名称
 * 2) GitHub 侧建议使用的 Secrets 名称
 *
 * 这样你不管是手工在 GAS 中配置，还是走 GitHub Actions 自动写入，都能直接照着做。
 */


function logOverseasMacroSecretsHint_(status) {
  var missing = [];
  if (!status.fred) missing.push('FRED_API_KEY');
  if (!status.alphaVantage) missing.push('ALPHA_VANTAGE_API_KEY');

  Logger.log(
    'overseas_macro skip | reason=missing_secrets'
      + ' | missing=' + missing.join(',')
      + ' | GAS Script Properties: FRED_API_KEY / ALPHA_VANTAGE_API_KEY'
      + ' | GitHub Secrets 建议同名: FRED_API_KEY / ALPHA_VANTAGE_API_KEY'
      + ' | 同步函数: setApiKeysFromParams(fredApiKey, alphaVantageApiKey)'
  );
}

/**
 * 统一读取必须存在的 Script Property。
 */


function getRequiredSecret_(key) {
  var value = PropertiesService.getScriptProperties().getProperty(key);
  if (!value) {
    throw new Error('missing script property: ' + key);
  }
  return String(value);
}

/* =========================
 * Source 1: FRED
 * ========================= */

/**
 * 从 FRED 批量抓取海外宏观字段。
 *
 * 返回结构示例：
 * {
 *   fed_upper: { date: '2026-03-11', value: 4.5, source: 'FRED:DFEDTARU' },
 *   ...
 * }
 */


function getOrCreateOverseasMacroSheet_() {
  var ss = SpreadsheetApp.getActiveSpreadsheet();
  var sh = ss.getSheetByName(SHEET_OVERSEAS_MACRO_RAW);

  if (!sh) {
    sh = ss.insertSheet(SHEET_OVERSEAS_MACRO_RAW);
  }

  var range = sh.getRange(1, 1, 1, OVERSEAS_MACRO_HEADERS.length);
  var existing = range.getValues()[0];
  var same = true;

  for (var i = 0; i < OVERSEAS_MACRO_HEADERS.length; i++) {
    if (String(existing[i] || '') !== OVERSEAS_MACRO_HEADERS[i]) {
      same = false;
      break;
    }
  }

  if (!same) {
    range.setValues([OVERSEAS_MACRO_HEADERS]);
  }

  return sh;
}

/**
 * 判断今天是否已经抓取过海外宏观数据。
 *
 * 判定逻辑：
 * - 读取 fetched_at 列
 * - 只要存在某一行的 fetched_at 日期 == 今天，就视为“今天已经抓过”
 */


function hasFetchedOverseasMacroToday_(sheet) {
  var lastRow = sheet.getLastRow();
  if (lastRow < 2) return false;

  var todayStr = formatDate_(new Date());
  var fetchedCol = OVERSEAS_MACRO_COL.fetched_at + 1;
  var vals = sheet.getRange(2, fetchedCol, lastRow - 1, 1).getValues();

  for (var i = vals.length - 1; i >= 0; i--) {
    var ymd = extractDatePart_(vals[i][0]);
    if (ymd === todayStr) {
      return true;
    }
  }

  return false;
}

/**
 * 按 date 进行 upsert。
 */


function upsertOverseasMacroRowByDate_(sheet, dateStr, row) {
  if (!dateStr) {
    throw new Error('upsertOverseasMacroRowByDate_ missing dateStr');
  }

  var lastRow = sheet.getLastRow();
  if (lastRow < 2) {
    sheet.getRange(2, 1, 1, row.length).setValues([row]);
    return;
  }

  var dateVals = sheet.getRange(2, OVERSEAS_MACRO_COL.date + 1, lastRow - 1, 1).getDisplayValues();
  for (var i = 0; i < dateVals.length; i++) {
    if (normYMD_(dateVals[i][0]) === dateStr) {
      sheet.getRange(i + 2, 1, 1, row.length).setValues([row]);
      return;
    }
  }

  sheet.getRange(lastRow + 1, 1, 1, row.length).setValues([row]);
}

/* =========================
 * Small helpers
 * ========================= */

/**
 * 抓取兼容层：
 * - 有 fetchWithFallback_ 就优先用
 * - 否则有 safeFetch_ 就用 safeFetch_
 * - 再否则回退 UrlFetchApp.fetch
 *
 * 这样能最大程度兼容你当前仓库已有的网络请求封装。
 */


function formatDateTimeOverseas_(d) {
  if (!(d instanceof Date)) d = new Date(d);
  return Utilities.formatDate(d, Session.getScriptTimeZone(), 'yyyy-MM-dd HH:mm:ss');
}

/**
 * 从 observation 对象中安全取 value。
 */


function pickObsValue_(obj) {
  return obj && obj.value != null ? obj.value : '';
}

/**
 * 从 observation 对象中安全取 date。
 */


function pickObsDate_(obj) {
  return obj && obj.date ? obj.date : '';
}

/**
 * 从日期时间值中提取 yyyy-MM-dd。
 *
 * 用途：
 * - 给 fetched_at 判重
 * - 支持 Date / 字符串两种输入
 */


function extractDatePart_(v) {
  return normYMD_(v);
}

/**
 * 构造 source 字段。
 *
 * 为什么不只写 “FRED | ALPHA_VANTAGE”：
 * - 当你发现某个字段明显滞后时，需要知道它自己的 observation date
 * - 尤其 copper 是低频序列，如果不把 observation date 一起记下来，后续容易误判
 */


function buildOverseasMacroSourceNote_(fredData, alphaData) {
  return [
    'FRED',
    'ALPHA_VANTAGE',
    'spx_obs_date=' + pickObsDate_(fredData.spx),
    'nasdaq_obs_date=' + pickObsDate_(fredData.nasdaq_100),
    'gold_obs_date=' + pickObsDate_(alphaData.gold),
    'wti_obs_date=' + pickObsDate_(alphaData.wti),
    'brent_obs_date=' + pickObsDate_(alphaData.brent),
    'copper_obs_date=' + pickObsDate_(alphaData.copper)
  ].join(' | ');
}


/* =========================
 * 民生 / 资产价格表（P3 预留）
 * ========================= */

/**
 * 当前仓库已在 config 中保留 LIFE_ASSET_* 常量，但原始实现尚未提交。
 * 为了让主流程稳定，这里先提供“可安全跳过”的最小实现：
 * - 会初始化表头
 * - 不会因未实现具体抓取而中断主流程
 * - 后续若补齐国家统计局 / SGE / BOC / 余额宝抓取，可直接在此扩展
 */
function fetchLifeAsset_() {
  var sheet = getOrCreateLifeAssetSheet_();
  if (hasFetchedLifeAssetToday_(sheet)) {
    Logger.log('life_asset skip | reason=already_fetched_today');
    return { skipped: true, reason: 'already_fetched_today' };
  }

  var today = formatDate_(new Date());
  var row = [
    today, '', '', '', '', '', '', '',
    'placeholder: stats_gov|sge|boc|money_fund',
    formatDateTime_(new Date())
  ];

  upsertRowByDate_(sheet, today, row);
  Logger.log('life_asset placeholder row upserted | date=' + today);
  return { skipped: false, date: today, placeholder: true };
}

function forceFetchLifeAsset_() {
  var sheet = getOrCreateLifeAssetSheet_();
  var today = formatDate_(new Date());
  var row = [
    today, '', '', '', '', '', '', '',
    'placeholder: stats_gov|sge|boc|money_fund',
    formatDateTime_(new Date())
  ];
  upsertRowByDate_(sheet, today, row);
  return { skipped: false, date: today, placeholder: true };
}

function testLifeAsset() {
  return fetchLifeAsset_();
}

function getOrCreateLifeAssetSheet_() {
  var sheet = ensureSheet_(SHEET_LIFE_ASSET_RAW);
  if (sheet.getLastRow() === 0) {
    sheet.appendRow(LIFE_ASSET_HEADERS);
  }
  return sheet;
}

function hasFetchedLifeAssetToday_(sheet) {
  var lastRow = sheet.getLastRow();
  if (lastRow <= 1) return false;
  var values = sheet.getRange(2, LIFE_ASSET_COL.fetched_at + 1, lastRow - 1, 1).getDisplayValues();
  var today = formatDate_(new Date());
  for (var i = values.length - 1; i >= 0; i--) {
    if (extractDatePart_(values[i][0]) === today) return true;
  }
  return false;
}
