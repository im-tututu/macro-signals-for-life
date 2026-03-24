/********************
 * 30_metrics.js
 * 统一生成“指标”宽表。
 *
 * 当前覆盖：
 * 1) 利率曲线关键点位与期限结构
 * 2) 信用 / 地方债 / 银行债 / 存单相对价值
 * 3) P4 政策—市场联动指标
 * 4) P5 海外宏观一期指标
 * 5) P6 房地产融资环境一期指标
 * 6) 滚动均线 / 历史分位
 *
 * 口径说明：
 * - 政策利率原始表是事件表，这里按“截至当日最近一次已知值”承接
 * - 海外宏观原始表保留了真实缺口；在 metrics 层同样按“截至当日最近一次已知值”承接，
 *   再计算 MA20 / pct250，避免被零星空值打断
 * - 资金面（DR007）按交易日精确匹配，不做向前填充
 ********************/

function buildMetrics_() {
  return buildMetricsFast_();
}

function buildMetricsFast_() {
  return buildMetricsCore_({
    buildDictionary: false,
    fullRebuild: false,
    recentWindowSize: 320,
    logTag: 'buildMetricsFast_'
  });
}

function buildMetricsFull_() {
  return buildMetricsCore_({
    buildDictionary: true,
    fullRebuild: true,
    recentWindowSize: 320,
    logTag: 'buildMetricsFull_'
  });
}

function buildMetricDictionary_() {
  var ss = SpreadsheetApp.getActive();
  var metricSheet = ss.getSheetByName(SHEET_METRICS);
  if (!metricSheet) throw new Error('找不到工作表: ' + SHEET_METRICS);

  Logger.log('▶️ 指标说明重建 | start');
  var started = Date.now();
  var lastRow = metricSheet.getLastRow();
  var lastCol = metricSheet.getLastColumn();
  if (lastRow < 1 || lastCol < 1) throw new Error('指标表为空，无法生成指标说明');

  var t0 = Date.now();
  var out = metricSheet.getRange(1, 1, lastRow, lastCol).getValues();
  Logger.log('buildMetricDictionary_ | metrics_loaded_ms=' + (Date.now() - t0) + ' | rows=' + Math.max(0, lastRow - 1) + ' | cols=' + lastCol);
  if (!out || !out.length) throw new Error('指标表为空，无法生成指标说明');

  t0 = Date.now();
  buildMetricDictionarySheet_(ss, out[0], out);
  Logger.log('buildMetricDictionary_ | dictionary_written_ms=' + (Date.now() - t0));
  Logger.log('✅ 指标说明重建 | done | elapsed_ms=' + (Date.now() - started) + ' | rows=' + Math.max(0, out.length - 1));
}

function buildMetricsCore_(options) {
  options = options || {};
  var buildDictionary = !!options.buildDictionary;
  var fullRebuild = !!options.fullRebuild;
  var recentWindowSize = Math.max(260, Number(options.recentWindowSize || 320));
  var logTag = options.logTag || 'buildMetricsCore_';
  var started = Date.now();

  Logger.log('▶️ ' + logTag + ' | start | dictionary=' + (buildDictionary ? 'yes' : 'no') + ' | mode=' + (fullRebuild ? 'full' : 'recent'));

  var ss = SpreadsheetApp.getActive();
  var curveSheet = ss.getSheetByName(SHEET_CURVE_RAW);
  if (!curveSheet) throw new Error('找不到工作表: ' + SHEET_CURVE_RAW);

  var dst = ss.getSheetByName(SHEET_METRICS) || ss.insertSheet(SHEET_METRICS);
  var t0 = Date.now();
  var curveValues = curveSheet.getDataRange().getValues();
  var header = buildMetricsHeader_();
  Logger.log(logTag + ' | curve_loaded_ms=' + (Date.now() - t0) + ' | curve_rows=' + Math.max(0, curveValues.length - 1));

  if (curveValues.length < 2) {
    writeMetricsOutput_(dst, [header]);
    if (buildDictionary) buildMetricDictionarySheet_(ss, header, [header]);
    Logger.log('✅ ' + logTag + ' | done | elapsed_ms=' + (Date.now() - started) + ' | rows=0');
    return {
      rowCount: 0,
      dictionaryBuilt: buildDictionary,
      fullRebuild: fullRebuild
    };
  }

  var rawHeader = curveValues[0];
  var termIndex = buildTermColumnIndex_(rawHeader);
  var curveByDate = buildCurveBucketByDate_(curveValues);

  t0 = Date.now();
  var moneySheet = ss.getSheetByName(SHEET_MONEY_MARKET_RAW);
  var moneyMap = moneySheet ? readMoneyMarketMetricsMap_(moneySheet) : {};

  var policySheet = ss.getSheetByName(SHEET_POLICY_RATE_RAW);
  var policyTimeline = policySheet ? readPolicyRateTimeline_(policySheet) : [];

  var overseasSheet = ss.getSheetByName(SHEET_OVERSEAS_MACRO_RAW);
  var overseasTimeline = overseasSheet ? readOverseasMacroTimeline_(overseasSheet) : [];
  Logger.log(logTag + ' | aux_loaded_ms=' + (Date.now() - t0) + ' | money_dates=' + Object.keys(moneyMap).length + ' | policy_points=' + policyTimeline.length + ' | overseas_points=' + overseasTimeline.length);

  var allDates = Object.keys(curveByDate).sort();
  var dates = allDates;
  var cutoffDate = '';
  var preservedRows = [];
  var usedExistingTail = false;

  if (!fullRebuild && allDates.length > recentWindowSize) {
    cutoffDate = allDates[allDates.length - recentWindowSize];
    dates = allDates.slice(allDates.length - recentWindowSize);

    t0 = Date.now();
    var existing = readExistingMetricsForFastMerge_(dst, header, cutoffDate);
    preservedRows = existing.rows;
    usedExistingTail = existing.usedExistingTail;
    Logger.log(logTag + ' | existing_tail_loaded_ms=' + (Date.now() - t0) + ' | cutoff=' + cutoffDate + ' | preserved_rows=' + preservedRows.length + ' | used=' + (usedExistingTail ? 'yes' : 'no'));
  }

  var rows = [];

  var policyState = {
    omo_7d: '',
    mlf_1y: '',
    lpr_1y: '',
    lpr_5y: ''
  };
  var overseasState = {
    fed_upper: '',
    fed_lower: '',
    sofr: '',
    ust_2y: '',
    ust_10y: '',
    us_real_10y: '',
    usd_broad: '',
    usd_cny: '',
    gold: '',
    wti: '',
    brent: '',
    copper: '',
    vix: '',
    spx: '',
    nasdaq_100: ''
  };
  var policyPtr = 0;
  var overseasPtr = 0;

  t0 = Date.now();
  for (var i = 0; i < dates.length; i++) {
    var dateKey = dates[i];

    while (policyPtr < policyTimeline.length && policyTimeline[policyPtr].date <= dateKey) {
      policyState[policyTimeline[policyPtr].field] = policyTimeline[policyPtr].rate;
      policyPtr++;
    }

    while (overseasPtr < overseasTimeline.length && overseasTimeline[overseasPtr].date <= dateKey) {
      applyOverseasSnapshot_(overseasState, overseasTimeline[overseasPtr].values);
      overseasPtr++;
    }

    rows.push(
      buildMetricsBaseRow_(
        dateKey,
        curveByDate[dateKey],
        termIndex,
        moneyMap[dateKey] || {},
        policyState,
        overseasState
      )
    );
  }
  Logger.log(logTag + ' | base_rows_built_ms=' + (Date.now() - t0) + ' | rows=' + rows.length + ' | source_dates=' + dates.length);

  t0 = Date.now();
  applyRollingMetrics_(rows);
  rows.reverse();
  Logger.log(logTag + ' | rolling_done_ms=' + (Date.now() - t0));

  t0 = Date.now();
  var out = [header];
  for (var j = 0; j < rows.length; j++) {
    out.push(metricsRowToArray_(rows[j], header));
  }
  if (!fullRebuild && usedExistingTail && preservedRows.length) {
    for (var p = 0; p < preservedRows.length; p++) {
      out.push(preservedRows[p]);
    }
  }
  Logger.log(logTag + ' | output_assembled_ms=' + (Date.now() - t0) + ' | final_rows=' + Math.max(0, out.length - 1));

  t0 = Date.now();
  writeMetricsOutput_(dst, out);
  Logger.log(logTag + ' | metrics_written_ms=' + (Date.now() - t0));

  if (buildDictionary) {
    t0 = Date.now();
    buildMetricDictionarySheet_(ss, header, out);
    Logger.log(logTag + ' | dictionary_written_ms=' + (Date.now() - t0));
  }

  Logger.log('✅ ' + logTag + ' | done | elapsed_ms=' + (Date.now() - started) + ' | rows=' + Math.max(0, out.length - 1) + ' | dictionary=' + (buildDictionary ? 'yes' : 'no') + ' | mode=' + (fullRebuild ? 'full' : 'recent'));
  return {
    rowCount: Math.max(0, out.length - 1),
    dictionaryBuilt: buildDictionary,
    fullRebuild: fullRebuild
  };
}

function readExistingMetricsForFastMerge_(sheet, header, cutoffDate) {
  var result = {
    usedExistingTail: false,
    rows: []
  };
  if (!sheet || !cutoffDate) return result;

  var lastRow = sheet.getLastRow();
  var lastCol = sheet.getLastColumn();
  if (lastRow < 2 || lastCol !== header.length) return result;

  var existing = sheet.getRange(1, 1, lastRow, lastCol).getValues();
  if (!existing || existing.length < 2) return result;
  if (!sameMetricHeader_(existing[0], header)) return result;

  for (var i = 1; i < existing.length; i++) {
    var row = existing[i];
    var dateKey = normYMD_(row[0]);
    if (!dateKey) continue;
    if (dateKey < cutoffDate) {
      result.rows.push(row);
    }
  }
  result.usedExistingTail = true;
  return result;
}

function sameMetricHeader_(a, b) {
  if (!a || !b || a.length !== b.length) return false;
  for (var i = 0; i < a.length; i++) {
    if (String(a[i] || '') !== String(b[i] || '')) return false;
  }
  return true;
}

function buildMetricsHeader_() {
  var codes = getMetricCatalogCodes_();
  return ['date'].concat(codes);
}

function buildCurveBucketByDate_(curveValues) {
  var byDate = {};

  for (var i = 1; i < curveValues.length; i++) {
    var row = curveValues[i];
    var dateKey = normYMD_(row[0]);
    var curveName = normalizeCurveName_(row[1]);
    if (!dateKey || !curveName) continue;

    if (!byDate[dateKey]) byDate[dateKey] = {};
    byDate[dateKey][curveName] = row;
  }

  return byDate;
}

function buildTermColumnIndex_(rawHeader) {
  var index = {};
  for (var i = 0; i < rawHeader.length; i++) {
    index[String(rawHeader[i]).trim()] = i;
  }
  return index;
}

function buildMetricsBaseRow_(dateKey, bucket, termIndex, moneyRow, policyState, overseasState) {
  var row = { date: dateKey };

  row.gov_1y = getCurvePoint_(bucket, '国债', 'Y_1', termIndex);
  row.gov_2y = getCurvePoint_(bucket, '国债', 'Y_2', termIndex);
  row.gov_3y = getCurvePoint_(bucket, '国债', 'Y_3', termIndex);
  row.gov_5y = getCurvePoint_(bucket, '国债', 'Y_5', termIndex);
  row.gov_10y = getCurvePoint_(bucket, '国债', 'Y_10', termIndex);
  row.gov_30y = getCurvePoint_(bucket, '国债', 'Y_30', termIndex);

  row.cdb_3y = getCurvePoint_(bucket, '国开债', 'Y_3', termIndex);
  row.cdb_5y = getCurvePoint_(bucket, '国开债', 'Y_5', termIndex);
  row.cdb_10y = getCurvePoint_(bucket, '国开债', 'Y_10', termIndex);

  row.aaa_credit_1y = getCurvePoint_(bucket, 'AAA信用', 'Y_1', termIndex);
  row.aaa_credit_3y = getCurvePoint_(bucket, 'AAA信用', 'Y_3', termIndex);
  row.aaa_credit_5y = getCurvePoint_(bucket, 'AAA信用', 'Y_5', termIndex);

  row.aa_plus_credit_1y = getCurvePoint_(bucket, 'AA+信用', 'Y_1', termIndex);

  row.aaa_plus_mtn_1y = getCurvePoint_(bucket, 'AAA+中票', 'Y_1', termIndex);

  row.aaa_mtn_1y = getCurvePoint_(bucket, 'AAA中票', 'Y_1', termIndex);
  row.aaa_mtn_3y = getCurvePoint_(bucket, 'AAA中票', 'Y_3', termIndex);
  row.aaa_mtn_5y = getCurvePoint_(bucket, 'AAA中票', 'Y_5', termIndex);

  row.aaa_ncd_1y = getCurvePoint_(bucket, 'AAA存单', 'Y_1', termIndex);

  row.aaa_bank_bond_1y = getCurvePoint_(bucket, 'AAA银行债', 'Y_1', termIndex);
  row.aaa_bank_bond_3y = getCurvePoint_(bucket, 'AAA银行债', 'Y_3', termIndex);
  row.aaa_bank_bond_5y = getCurvePoint_(bucket, 'AAA银行债', 'Y_5', termIndex);

  row.aaa_lgfv_1y = getCurvePoint_(bucket, 'AAA城投', 'Y_1', termIndex);

  row.local_gov_1y = getCurvePoint_(bucket, '地方债', 'Y_1', termIndex);
  row.local_gov_5y = getCurvePoint_(bucket, '地方债', 'Y_5', termIndex);
  row.local_gov_10y = getCurvePoint_(bucket, '地方债', 'Y_10', termIndex);

  row.dr007_weighted_rate = pctValueOrBlank_(moneyRow.dr007_weighted_rate);
  row.omo_7d = pctValueOrBlank_(policyState.omo_7d);
  row.mlf_1y = pctValueOrBlank_(policyState.mlf_1y);
  row.lpr_1y = pctValueOrBlank_(policyState.lpr_1y);
  row.lpr_5y = pctValueOrBlank_(policyState.lpr_5y);

  row.ust_2y = pctValueOrBlank_(overseasState.ust_2y);
  row.ust_10y = pctValueOrBlank_(overseasState.ust_10y);
  row.usd_broad = pickMetricOrBlank_(overseasState.usd_broad);
  row.usd_cny = pickMetricOrBlank_(overseasState.usd_cny);
  row.gold = pickMetricOrBlank_(overseasState.gold);
  row.wti = pickMetricOrBlank_(overseasState.wti);
  row.brent = pickMetricOrBlank_(overseasState.brent);
  row.copper = pickMetricOrBlank_(overseasState.copper);
  row.vix = pickMetricOrBlank_(overseasState.vix);
  row.spx = pickMetricOrBlank_(overseasState.spx);
  row.nasdaq_100 = pickMetricOrBlank_(overseasState.nasdaq_100);

  row.slope_gov_10y_1y_bp = safeSpreadBpOrBlank_(row.gov_10y, row.gov_1y);
  row.slope_gov_10y_3y_bp = safeSpreadBpOrBlank_(row.gov_10y, row.gov_3y);
  row.slope_gov_30y_10y_bp = safeSpreadBpOrBlank_(row.gov_30y, row.gov_10y);
  row.slope_cdb_10y_3y_bp = safeSpreadBpOrBlank_(row.cdb_10y, row.cdb_3y);
  row.slope_ust_10y_2y_bp = safeSpreadBpOrBlank_(row.ust_10y, row.ust_2y);

  row.bfly_gov_1y_5y_10y_bp = safeButterflyBpOrBlank_(row.gov_5y, row.gov_1y, row.gov_10y);
  row.bfly_gov_3y_5y_10y_bp = safeButterflyBpOrBlank_(row.gov_5y, row.gov_3y, row.gov_10y);
  row.bfly_gov_5y_10y_30y_bp = safeButterflyBpOrBlank_(row.gov_10y, row.gov_5y, row.gov_30y);
  row.bfly_cdb_3y_5y_10y_bp = safeButterflyBpOrBlank_(row.cdb_5y, row.cdb_3y, row.cdb_10y);

  row.spread_cdb_gov_3y_bp = safeSpreadBpOrBlank_(row.cdb_3y, row.gov_3y);
  row.spread_cdb_gov_5y_bp = safeSpreadBpOrBlank_(row.cdb_5y, row.gov_5y);
  row.spread_cdb_gov_10y_bp = safeSpreadBpOrBlank_(row.cdb_10y, row.gov_10y);

  row.spread_local_gov_gov_5y_bp = safeSpreadBpOrBlank_(row.local_gov_5y, row.gov_5y);
  row.spread_local_gov_gov_10y_bp = safeSpreadBpOrBlank_(row.local_gov_10y, row.gov_10y);
  row.spread_local_gov_cdb_5y_bp = safeSpreadBpOrBlank_(row.local_gov_5y, row.cdb_5y);
  row.spread_local_gov_cdb_10y_bp = safeSpreadBpOrBlank_(row.local_gov_10y, row.cdb_10y);

  row.spread_aaa_credit_gov_1y_bp = safeSpreadBpOrBlank_(row.aaa_credit_1y, row.gov_1y);
  row.spread_aaa_credit_gov_3y_bp = safeSpreadBpOrBlank_(row.aaa_credit_3y, row.gov_3y);
  row.spread_aaa_credit_gov_5y_bp = safeSpreadBpOrBlank_(row.aaa_credit_5y, row.gov_5y);
  row.spread_aaa_credit_cdb_3y_bp = safeSpreadBpOrBlank_(row.aaa_credit_3y, row.cdb_3y);
  row.spread_aaa_credit_cdb_5y_bp = safeSpreadBpOrBlank_(row.aaa_credit_5y, row.cdb_5y);

  row.spread_aa_plus_credit_aaa_credit_1y_bp = safeSpreadBpOrBlank_(row.aa_plus_credit_1y, row.aaa_credit_1y);
  row.spread_aaa_plus_mtn_gov_1y_bp = safeSpreadBpOrBlank_(row.aaa_plus_mtn_1y, row.gov_1y);

  row.spread_aaa_mtn_aaa_plus_mtn_1y_bp = safeSpreadBpOrBlank_(row.aaa_mtn_1y, row.aaa_plus_mtn_1y);
  row.spread_aaa_credit_ncd_1y_bp = safeSpreadBpOrBlank_(row.aaa_credit_1y, row.aaa_ncd_1y);

  row.spread_aaa_bank_bond_aaa_credit_1y_bp = safeSpreadBpOrBlank_(row.aaa_bank_bond_1y, row.aaa_credit_1y);
  row.spread_aaa_bank_bond_aaa_credit_3y_bp = safeSpreadBpOrBlank_(row.aaa_bank_bond_3y, row.aaa_credit_3y);
  row.spread_aaa_bank_bond_aaa_credit_5y_bp = safeSpreadBpOrBlank_(row.aaa_bank_bond_5y, row.aaa_credit_5y);

  row.spread_aaa_lgfv_aaa_credit_1y_bp = safeSpreadBpOrBlank_(row.aaa_lgfv_1y, row.aaa_credit_1y);
  row.spread_aaa_lgfv_gov_1y_bp = safeSpreadBpOrBlank_(row.aaa_lgfv_1y, row.gov_1y);
  row.spread_aaa_lgfv_local_gov_1y_bp = safeSpreadBpOrBlank_(row.aaa_lgfv_1y, row.local_gov_1y);

  // 政策—市场联动
  row.spread_dr007_omo_7d_bp = safeSpreadBpOrBlank_(row.dr007_weighted_rate, row.omo_7d);
  row.spread_ncd_mlf_1y_bp = safeSpreadBpOrBlank_(row.aaa_ncd_1y, row.mlf_1y);
  row.spread_gov_mlf_1y_bp = safeSpreadBpOrBlank_(row.gov_1y, row.mlf_1y);
  row.spread_lpr_mlf_1y_bp = safeSpreadBpOrBlank_(row.lpr_1y, row.mlf_1y);
  row.spread_aaa_ncd_dr007_bp = safeSpreadBpOrBlank_(row.aaa_ncd_1y, row.dr007_weighted_rate);
  row.spread_gov_1y_dr007_bp = safeSpreadBpOrBlank_(row.gov_1y, row.dr007_weighted_rate);
  row.spread_aaa_credit_dr007_bp = safeSpreadBpOrBlank_(row.aaa_credit_1y, row.dr007_weighted_rate);
  row.spread_lpr1y_gov1y_bp = safeSpreadBpOrBlank_(row.lpr_1y, row.gov_1y);
  row.spread_lpr_gov_5y_bp = safeSpreadBpOrBlank_(row.lpr_5y, row.gov_5y);
  row.spread_lpr5y_ncd1y_bp = safeSpreadBpOrBlank_(row.lpr_5y, row.aaa_ncd_1y);
  row.spread_lpr5y_lpr1y_bp = safeSpreadBpOrBlank_(row.lpr_5y, row.lpr_1y);

  // 海外宏观
  row.spread_gov_ust_10y_bp = safeSpreadBpOrBlank_(row.gov_10y, row.ust_10y);
  row.spread_gov_ust_2y_bp = safeSpreadBpOrBlank_(row.gov_2y, row.ust_2y);
  row.spread_cn_us_curve_slope_bp = safeCurveSlopeGapBpOrBlank_(row.gov_10y, row.gov_2y, row.ust_10y, row.ust_2y);

  row.ratio_copper_gold = safeRatioOrBlank_(row.copper, row.gold);
  row.ratio_spx_gold = safeRatioOrBlank_(row.spx, row.gold);
  row.ratio_spx_vix = safeRatioOrBlank_(row.spx, row.vix);
  row.ratio_brent_gold = safeRatioOrBlank_(row.brent, row.gold);

  row.count_rates_top_decile = '';
  row.count_rates_bottom_decile = '';
  row.count_credit_spreads_top_decile = '';
  row.count_credit_spreads_bottom_decile = '';
  row.count_curve_metrics_top_decile = '';
  row.count_curve_metrics_bottom_decile = '';

  row.usd_broad_ma20 = '';
  row.usd_cny_ma20 = '';
  row.gold_ma20 = '';
  row.ust_10y_prank250 = '';
  row.usd_cny_prank250 = '';

  row.gov_10y_ma20 = '';
  row.gov_10y_ma60 = '';
  row.gov_10y_ma120 = '';
  row.gov_10y_prank250 = '';

  row.spread_cdb_gov_10y_bp_ma20 = '';
  row.spread_cdb_gov_10y_bp_prank250 = '';

  row.spread_aaa_credit_gov_5y_bp_ma20 = '';
  row.spread_aaa_credit_gov_5y_bp_prank250 = '';

  row.spread_aa_plus_credit_aaa_credit_1y_bp_ma20 = '';
  row.spread_aa_plus_credit_aaa_credit_1y_bp_prank250 = '';

  row.aaa_ncd_1y_ma20 = '';
  row.aaa_ncd_1y_prank250 = '';

  return row;
}

function applyRollingMetrics_(rows) {
  var gov1Arr = [];
  var gov2Arr = [];
  var gov3Arr = [];
  var gov5Arr = [];
  var gov10Arr = [];
  var gov30Arr = [];
  var cdb3Arr = [];
  var cdb5Arr = [];
  var cdb10Arr = [];
  var local5Arr = [];
  var local10Arr = [];

  var cdbGov10Arr = [];
  var aaaCreditGov1Arr = [];
  var aaaCreditGov3Arr = [];
  var aaaCreditGov5Arr = [];
  var aaPlusAaaArr = [];
  var bank1Arr = [];
  var bank3Arr = [];
  var bank5Arr = [];
  var lgfvAaaArr = [];

  var slope10_1Arr = [];
  var slope10_3Arr = [];
  var slope30_10Arr = [];
  var slopeCdb10_3Arr = [];
  var bflyGov1_5_10Arr = [];
  var bflyGov3_5_10Arr = [];
  var bflyGov5_10_30Arr = [];
  var bflyCdb3_5_10Arr = [];

  var ncd1Arr = [];
  var usdBroadArr = [];
  var usdCnyArr = [];
  var goldArr = [];
  var ust10Arr = [];

  for (var i = 0; i < rows.length; i++) {
    var r = rows[i];

    gov1Arr.push(toNumberOrNull_(r.gov_1y));
    gov2Arr.push(toNumberOrNull_(r.gov_2y));
    gov3Arr.push(toNumberOrNull_(r.gov_3y));
    gov5Arr.push(toNumberOrNull_(r.gov_5y));
    gov10Arr.push(toNumberOrNull_(r.gov_10y));
    gov30Arr.push(toNumberOrNull_(r.gov_30y));
    cdb3Arr.push(toNumberOrNull_(r.cdb_3y));
    cdb5Arr.push(toNumberOrNull_(r.cdb_5y));
    cdb10Arr.push(toNumberOrNull_(r.cdb_10y));
    local5Arr.push(toNumberOrNull_(r.local_gov_5y));
    local10Arr.push(toNumberOrNull_(r.local_gov_10y));

    cdbGov10Arr.push(toNumberOrNull_(r.spread_cdb_gov_10y_bp));
    aaaCreditGov1Arr.push(toNumberOrNull_(r.spread_aaa_credit_gov_1y_bp));
    aaaCreditGov3Arr.push(toNumberOrNull_(r.spread_aaa_credit_gov_3y_bp));
    aaaCreditGov5Arr.push(toNumberOrNull_(r.spread_aaa_credit_gov_5y_bp));
    aaPlusAaaArr.push(toNumberOrNull_(r.spread_aa_plus_credit_aaa_credit_1y_bp));
    bank1Arr.push(toNumberOrNull_(r.spread_aaa_bank_bond_aaa_credit_1y_bp));
    bank3Arr.push(toNumberOrNull_(r.spread_aaa_bank_bond_aaa_credit_3y_bp));
    bank5Arr.push(toNumberOrNull_(r.spread_aaa_bank_bond_aaa_credit_5y_bp));
    lgfvAaaArr.push(toNumberOrNull_(r.spread_aaa_lgfv_aaa_credit_1y_bp));

    slope10_1Arr.push(toNumberOrNull_(r.slope_gov_10y_1y_bp));
    slope10_3Arr.push(toNumberOrNull_(r.slope_gov_10y_3y_bp));
    slope30_10Arr.push(toNumberOrNull_(r.slope_gov_30y_10y_bp));
    slopeCdb10_3Arr.push(toNumberOrNull_(r.slope_cdb_10y_3y_bp));
    bflyGov1_5_10Arr.push(toNumberOrNull_(r.bfly_gov_1y_5y_10y_bp));
    bflyGov3_5_10Arr.push(toNumberOrNull_(r.bfly_gov_3y_5y_10y_bp));
    bflyGov5_10_30Arr.push(toNumberOrNull_(r.bfly_gov_5y_10y_30y_bp));
    bflyCdb3_5_10Arr.push(toNumberOrNull_(r.bfly_cdb_3y_5y_10y_bp));

    ncd1Arr.push(toNumberOrNull_(r.aaa_ncd_1y));
    usdBroadArr.push(toNumberOrNull_(r.usd_broad));
    usdCnyArr.push(toNumberOrNull_(r.usd_cny));
    goldArr.push(toNumberOrNull_(r.gold));
    ust10Arr.push(toNumberOrNull_(r.ust_10y));

    r.usd_broad_ma20 = rollingMeanAllowBlank_(usdBroadArr, 20);
    r.usd_cny_ma20 = rollingMeanAllowBlank_(usdCnyArr, 20);
    r.gold_ma20 = rollingMeanAllowBlank_(goldArr, 20);
    r.ust_10y_prank250 = rollingPercentileRankAllowBlank_(ust10Arr, 250);
    r.usd_cny_prank250 = rollingPercentileRankAllowBlank_(usdCnyArr, 250);

    r.gov_10y_ma20 = rollingMeanAllowBlank_(gov10Arr, 20);
    r.gov_10y_ma60 = rollingMeanAllowBlank_(gov10Arr, 60);
    r.gov_10y_ma120 = rollingMeanAllowBlank_(gov10Arr, 120);
    r.gov_10y_prank250 = rollingPercentileRankAllowBlank_(gov10Arr, 250);

    r.spread_cdb_gov_10y_bp_ma20 = rollingMeanAllowBlank_(cdbGov10Arr, 20);
    r.spread_cdb_gov_10y_bp_prank250 = rollingPercentileRankAllowBlank_(cdbGov10Arr, 250);

    r.spread_aaa_credit_gov_5y_bp_ma20 = rollingMeanAllowBlank_(aaaCreditGov5Arr, 20);
    r.spread_aaa_credit_gov_5y_bp_prank250 = rollingPercentileRankAllowBlank_(aaaCreditGov5Arr, 250);

    r.spread_aa_plus_credit_aaa_credit_1y_bp_ma20 = rollingMeanAllowBlank_(aaPlusAaaArr, 20);
    r.spread_aa_plus_credit_aaa_credit_1y_bp_prank250 = rollingPercentileRankAllowBlank_(aaPlusAaaArr, 250);

    r.aaa_ncd_1y_ma20 = rollingMeanAllowBlank_(ncd1Arr, 20);
    r.aaa_ncd_1y_prank250 = rollingPercentileRankAllowBlank_(ncd1Arr, 250);

    r.count_rates_top_decile = countExtremePercentilesOrBlank_([
      gov1Arr, gov2Arr, gov3Arr, gov5Arr, gov10Arr, gov30Arr,
      cdb3Arr, cdb5Arr, cdb10Arr, local5Arr, local10Arr
    ], 250, 0.9, 'top');

    r.count_rates_bottom_decile = countExtremePercentilesOrBlank_([
      gov1Arr, gov2Arr, gov3Arr, gov5Arr, gov10Arr, gov30Arr,
      cdb3Arr, cdb5Arr, cdb10Arr, local5Arr, local10Arr
    ], 250, 0.1, 'bottom');

    r.count_credit_spreads_top_decile = countExtremePercentilesOrBlank_([
      aaaCreditGov1Arr, aaaCreditGov3Arr, aaaCreditGov5Arr,
      aaPlusAaaArr, bank1Arr, bank3Arr, bank5Arr, lgfvAaaArr
    ], 250, 0.9, 'top');

    r.count_credit_spreads_bottom_decile = countExtremePercentilesOrBlank_([
      aaaCreditGov1Arr, aaaCreditGov3Arr, aaaCreditGov5Arr,
      aaPlusAaaArr, bank1Arr, bank3Arr, bank5Arr, lgfvAaaArr
    ], 250, 0.1, 'bottom');

    r.count_curve_metrics_top_decile = countExtremePercentilesOrBlank_([
      slope10_1Arr, slope10_3Arr, slope30_10Arr, slopeCdb10_3Arr,
      bflyGov1_5_10Arr, bflyGov3_5_10Arr, bflyGov5_10_30Arr, bflyCdb3_5_10Arr
    ], 250, 0.9, 'top');

    r.count_curve_metrics_bottom_decile = countExtremePercentilesOrBlank_([
      slope10_1Arr, slope10_3Arr, slope30_10Arr, slopeCdb10_3Arr,
      bflyGov1_5_10Arr, bflyGov3_5_10Arr, bflyGov5_10_30Arr, bflyCdb3_5_10Arr
    ], 250, 0.1, 'bottom');
  }
}

function readMoneyMarketMetricsMap_(sheet) {
  var values = sheet.getDataRange().getValues();
  if (values.length < 2) return {};

  var idx = buildHeaderIndex_(values[0]);
  var dateCol = pickFirstExistingColumn_(idx, ['date']);
  var dr007Col = pickFirstExistingColumn_(idx, ['dr007_weightedrate', 'dr007_weighted_rate']);

  if (dateCol == null) return {};

  var map = {};
  for (var i = 1; i < values.length; i++) {
    var row = values[i];
    var dateKey = normYMD_(row[dateCol]);
    if (!dateKey) continue;

    map[dateKey] = {
      dr007_weighted_rate: dr007Col == null ? '' : pickMetricOrBlank_(row[dr007Col])
    };
  }

  return map;
}

function readPolicyRateTimeline_(sheet) {
  var values = sheet.getDataRange().getValues();
  if (values.length < 2) return [];

  var idx = buildHeaderIndex_(values[0]);
  var dateCol = pickFirstExistingColumn_(idx, ['date']);
  var typeCol = pickFirstExistingColumn_(idx, ['type']);
  var termCol = pickFirstExistingColumn_(idx, ['term']);
  var rateCol = pickFirstExistingColumn_(idx, ['rate']);

  if (dateCol == null || typeCol == null || termCol == null || rateCol == null) {
    return [];
  }

  var out = [];
  for (var i = 1; i < values.length; i++) {
    var row = values[i];
    var dateKey = normYMD_(row[dateCol]);
    var field = mapPolicyRateField_(row[typeCol], row[termCol]);
    var rate = toNumberOrNull_(row[rateCol]);

    if (!dateKey || !field || !isFiniteNumber_(rate)) continue;

    out.push({
      date: dateKey,
      field: field,
      rate: rate
    });
  }

  out.sort(function(a, b) {
    if (a.date !== b.date) return a.date < b.date ? -1 : 1;
    if (a.field !== b.field) return a.field < b.field ? -1 : 1;
    return 0;
  });

  return out;
}

function readOverseasMacroTimeline_(sheet) {
  var values = sheet.getDataRange().getValues();
  if (values.length < 2) return [];

  var idx = buildHeaderIndex_(values[0]);
  var dateCol = pickFirstExistingColumn_(idx, ['date']);
  if (dateCol == null) return [];

  var fieldNames = [
    'fed_upper', 'fed_lower', 'sofr',
    'ust_2y', 'ust_10y', 'us_real_10y',
    'usd_broad', 'usd_cny', 'gold',
    'wti', 'brent', 'copper',
    'vix', 'spx', 'nasdaq_100'
  ];

  var colMap = {};
  for (var j = 0; j < fieldNames.length; j++) {
    var name = fieldNames[j];
    colMap[name] = pickFirstExistingColumn_(idx, [name]);
  }

  var out = [];
  for (var i = 1; i < values.length; i++) {
    var row = values[i];
    var dateKey = normYMD_(row[dateCol]);
    if (!dateKey) continue;

    var snapshot = {};
    for (var k = 0; k < fieldNames.length; k++) {
      var field = fieldNames[k];
      var col = colMap[field];
      if (col == null) continue;
      var n = toNumberOrNull_(row[col]);
      if (isFiniteNumber_(n)) snapshot[field] = n;
    }

    if (Object.keys(snapshot).length === 0) continue;
    out.push({
      date: dateKey,
      values: snapshot
    });
  }

  out.sort(function(a, b) {
    return a.date < b.date ? -1 : (a.date > b.date ? 1 : 0);
  });

  return out;
}

function applyOverseasSnapshot_(state, snapshot) {
  if (!snapshot) return;
  for (var key in snapshot) {
    if (!snapshot.hasOwnProperty(key)) continue;
    state[key] = snapshot[key];
  }
}

function mapPolicyRateField_(type, term) {
  var t = normalizePolicyType_(type);
  var k = normalizePolicyTerm_(term);

  if (t === 'OMO' && k === '7D') return 'omo_7d';
  if (t === 'MLF' && k === '1Y') return 'mlf_1y';
  if (t === 'LPR' && k === '1Y') return 'lpr_1y';
  if (t === 'LPR' && k === '5Y+') return 'lpr_5y';
  return '';
}

function normalizePolicyType_(v) {
  var s = String(v == null ? '' : v)
    .replace(/\u3000/g, ' ')
    .replace(/\s+/g, '')
    .toUpperCase();

  if (s === 'OMO') return 'OMO';
  if (s === 'MLF') return 'MLF';
  if (s === 'LPR') return 'LPR';

  return s;
}

function normalizePolicyTerm_(v) {
  var s = String(v == null ? '' : v)
    .replace(/＋/g, '+')
    .replace(/\u3000/g, ' ')
    .replace(/\s+/g, '')
    .toUpperCase();

  if (s === '7D' || s === '7天') return '7D';
  if (s === '1Y' || s === '1年') return '1Y';
  if (s === '5Y+' || s === '5Y以上' || s === '5年以上' || s === '5年期以上') return '5Y+';

  return s;
}

function pickFirstExistingColumn_(idx, names) {
  for (var i = 0; i < names.length; i++) {
    var key = normalizeHeader_(names[i]);
    if (key in idx) return idx[key];
  }
  return null;
}

function normalizeCurveName_(name) {
  return String(name == null ? '' : name)
    .replace(/＋/g, '+')
    .replace(/\u3000/g, ' ')
    .replace(/\s+/g, '')
    .trim();
}

function getCurvePoint_(bucket, curveName, colName, termIndex) {
  var key = normalizeCurveName_(curveName);
  var row = bucket[key];
  if (!row) return '';

  var idx = termIndex[colName];
  if (idx == null) return '';

  var n = toNumberOrNull_(row[idx]);
  return isFiniteNumber_(n) ? n / 100 : '';
}

function pickMetricOrBlank_(v) {
  var n = toNumberOrNull_(v);
  return isFiniteNumber_(n) ? n : '';
}

function pctValueOrBlank_(v) {
  var n = toNumberOrNull_(v);
  return isFiniteNumber_(n) ? n / 100 : '';
}

function metricsRowToArray_(rowObj, header) {
  var out = [];
  for (var i = 0; i < header.length; i++) {
    var key = header[i];
    out.push(rowObj.hasOwnProperty(key) ? rowObj[key] : '');
  }
  return out;
}

function writeMetricsOutput_(sheet, out) {
  sheet.clearContents();
  sheet.getRange(1, 1, out.length, out[0].length).setValues(out);

  if (out.length > 1) {
    var rowCount = out.length - 1;
    var colCount = out[0].length;
    var formatRow = [];
    formatRow.push('yyyy-mm-dd');
    for (var c = 2; c <= colCount; c++) {
      formatRow.push(getMetricNumberFormat_(out[0][c - 1]));
    }

    var numberFormats = [];
    for (var r = 0; r < rowCount; r++) {
      numberFormats.push(formatRow.slice());
    }
    sheet.getRange(2, 1, rowCount, colCount).setNumberFormats(numberFormats);
  }

  sheet.setFrozenRows(1);
  sheet.getRange(1, 1, 1, out[0].length)
    .setFontWeight('bold')
    .setBackground('#d9eaf7');
}

function getMetricNumberFormat_(headerName) {
  var name = String(headerName || '');
  var item = getMetricCatalogItem_(name);
  if (item) {
    if (item.display_unit === 'percent') {
      return item.storage_unit === 'ratio' ? '0.0%' : '0.00%';
    }
    if (item.display_unit === 'bp') return '0.0 "bp"';
    if (item.display_unit === 'count') return '0';
    if (item.display_unit === 'ratio') return '0.000';
  }
  if (name === 'usd_cny' || name === 'usd_cny_ma20') return '0.0000';
  if (name === 'usd_broad' || name === 'usd_broad_ma20' || name === 'gold' || name === 'gold_ma20' || name === 'wti' || name === 'brent' || name === 'copper' || name === 'vix' || name === 'spx' || name === 'nasdaq_100') return '0.00';
  if (/_prank\d+$/.test(name)) return '0.0%';
  if (/_bp$/.test(name) || /_bp_ma\d+$/.test(name)) return '0.0 "bp"';
  return '0.00%';
}

function getDictionaryLatestValueNumberFormat_(code) {
  var name = String(code || '');
  var item = getMetricCatalogItem_(name);
  if (item) {
    if (item.display_unit === 'percent') return '0.00%';
    if (item.display_unit === 'bp') return '0 "bp"';
    if (item.display_unit === 'count') return '0';
    if (item.display_unit === 'ratio') return '0.000';
  }
  if (/_prank\d+$/.test(name)) return '0.00%';
  if (/_bp$/.test(name) || /_bp_ma\d+$/.test(name)) return '0 "bp"';
  return '0.00';
}

function getDictionaryDeltaNumberFormat_(code) {
  if (isPercentileMetric_(code)) return '0.0%';
  if (isBpMetric_(code) || isPercentMetric_(code)) return '0 "bp"';
  return stripNumberFormatUnitSuffix_(getDictionaryLatestValueNumberFormat_(code));
}

function stripNumberFormatUnitSuffix_(fmt) {
  var s = String(fmt || '').trim();
  if (!s) return '0.00';
  s = s.replace(/\s+"[^"]*"/g, '');
  return s || '0.00';
}

function getDictionaryPercentileNumberFormat_() {
  return '0.0%';
}

function isBpMetric_(code) {
  var name = String(code || '');
  var item = getMetricCatalogItem_(name);
  if (item && item.display_unit === 'bp') return true;
  return /_bp$/.test(name) || /_bp_ma\d+$/.test(name);
}

function isPercentileMetric_(code) {
  var name = String(code || '');
  return /_prank\d+$/.test(name);
}

function isPercentMetric_(code) {
  var name = String(code || '');
  var item = getMetricCatalogItem_(name);
  if (isPercentileMetric_(name)) return false;
  return !!(item && item.display_unit === 'percent');
}

function metricChangeToDisplayUnitOrBlank_(code, latest, base) {
  if (!isFiniteNumber_(latest) || !isFiniteNumber_(base)) return '';

  if (isBpMetric_(code)) {
    return latest - base;
  }

  if (isPercentileMetric_(code)) {
    return (latest - base) * 100;
  }

  if (isPercentMetric_(code)) {
    return (latest - base) * 10000;
  }

  return latest - base;
}

function normalizeMeaningText_(text) {
  var s = String(text || '').trim();
  if (!s) return '';
  s = s
    .replace(/^通常意味着/, '')
    .replace(/^常对应/, '')
    .replace(/^表示/, '')
    .replace(/^意味着/, '')
    .replace(/[。；;，,\s]+$/, '');
  return s;
}

function safeSpreadBpOrBlank_(a, b) {
  if (!isFiniteNumber_(a) || !isFiniteNumber_(b)) return '';
  return (a - b) * 10000;
}

function safeButterflyBpOrBlank_(mid, shortEnd, longEnd) {
  if (!isFiniteNumber_(mid) || !isFiniteNumber_(shortEnd) || !isFiniteNumber_(longEnd)) return '';
  return (2 * mid - shortEnd - longEnd) * 10000;
}

function safeCurveSlopeGapBpOrBlank_(cnLong, cnShort, usLong, usShort) {
  if (!isFiniteNumber_(cnLong) || !isFiniteNumber_(cnShort) || !isFiniteNumber_(usLong) || !isFiniteNumber_(usShort)) return '';
  return ((cnLong - cnShort) - (usLong - usShort)) * 10000;
}

function safeRatioOrBlank_(a, b) {
  if (!isFiniteNumber_(a) || !isFiniteNumber_(b) || b === 0) return '';
  return a / b;
}

function countExtremePercentilesOrBlank_(seriesList, windowSize, threshold, mode) {
  if (!seriesList || !seriesList.length) return '';
  var count = 0;
  var available = 0;

  for (var i = 0; i < seriesList.length; i++) {
    var p = rollingPercentileRankAllowBlank_(seriesList[i], windowSize);
    if (!isFiniteNumber_(p)) continue;
    available++;
    if (mode === 'bottom') {
      if (p <= threshold) count++;
    } else {
      if (p >= threshold) count++;
    }
  }

  return available ? count : '';
}

function rollingMeanAllowBlank_(arr, windowSize) {
  if (!arr || arr.length < windowSize || windowSize <= 0) return '';
  var start = arr.length - windowSize;
  var sum = 0;
  for (var i = start; i < arr.length; i++) {
    var v = arr[i];
    if (!isFiniteNumber_(v)) return '';
    sum += v;
  }
  return sum / windowSize;
}

function rollingPercentileRankAllowBlank_(arr, windowSize) {
  if (!arr || arr.length < windowSize || windowSize <= 0) return '';
  var slice = arr.slice(arr.length - windowSize);
  var currentValue = slice[slice.length - 1];
  if (!isFiniteNumber_(currentValue)) return '';

  var n = 0;
  var leCount = 0;
  for (var i = 0; i < slice.length; i++) {
    var v = slice[i];
    if (!isFiniteNumber_(v)) return '';
    n++;
    if (v <= currentValue) leCount++;
  }
  if (n < windowSize) return '';
  return leCount / n;
}

function metricLagChangeDisplayUnitOrBlank_(code, descValues, lag) {
  if (!descValues || descValues.length <= lag || lag <= 0) return '';

  var latest = descValues[0];
  var base = descValues[lag];
  return metricChangeToDisplayUnitOrBlank_(code, latest, base);
}

function metricDiffVsMaDisplayUnitOrBlank_(code, descValues, windowSize) {
  if (!descValues || descValues.length < windowSize || windowSize <= 0) return '';

  var latest = descValues[0];
  if (!isFiniteNumber_(latest)) return '';

  var ascWindow = descValues.slice(0, windowSize).reverse();
  var ma = rollingMeanAllowBlank_(ascWindow, windowSize);
  if (!isFiniteNumber_(ma)) return '';

  return metricChangeToDisplayUnitOrBlank_(code, latest, ma);
}

function metricPercentileRankOrBlank_(descValues, windowSize) {
  if (!descValues || descValues.length < windowSize || windowSize <= 0) return '';

  var ascWindow = descValues.slice(0, windowSize).reverse();
  return rollingPercentileRankAllowBlank_(ascWindow, windowSize);
}


function metricPercentileRankFullSampleOrBlank_(descValues) {
  if (!descValues || !descValues.length) return '';

  var latest = descValues[0];
  if (!isFiniteNumber_(latest)) return '';

  var n = 0;
  var leCount = 0;
  for (var i = 0; i < descValues.length; i++) {
    var v = descValues[i];
    if (!isFiniteNumber_(v)) continue;
    n++;
    if (v <= latest) leCount++;
  }
  if (!n) return '';
  return leCount / n;
}

function meanOfFiniteValuesOrBlank_(values) {
  if (!values || !values.length) return '';
  var n = 0;
  var sum = 0;
  for (var i = 0; i < values.length; i++) {
    var v = values[i];
    if (!isFiniteNumber_(v)) continue;
    n++;
    sum += v;
  }
  if (!n) return '';
  return sum / n;
}

function stdDevPopulationFiniteValuesOrBlank_(values, mean) {
  if (!values || !values.length || !isFiniteNumber_(mean)) return '';
  var n = 0;
  var sumSq = 0;
  for (var i = 0; i < values.length; i++) {
    var v = values[i];
    if (!isFiniteNumber_(v)) continue;
    n++;
    var d = v - mean;
    sumSq += d * d;
  }
  if (!n) return '';
  return Math.sqrt(sumSq / n);
}

function medianOfFiniteValuesOrBlank_(values) {
  if (!values || !values.length) return '';
  var arr = [];
  for (var i = 0; i < values.length; i++) {
    var v = values[i];
    if (isFiniteNumber_(v)) arr.push(v);
  }
  if (!arr.length) return '';
  arr.sort(function(a, b) { return a - b; });
  var mid = Math.floor(arr.length / 2);
  if (arr.length % 2) return arr[mid];
  return (arr[mid - 1] + arr[mid]) / 2;
}

function metricZScoreOrBlank_(descValues, windowSize) {
  if (!descValues || descValues.length < windowSize || windowSize <= 1) return '';

  var slice = descValues.slice(0, windowSize);
  var mean = meanOfFiniteValuesOrBlank_(slice);
  if (!isFiniteNumber_(mean)) return '';

  var sd = stdDevPopulationFiniteValuesOrBlank_(slice, mean);
  if (!isFiniteNumber_(sd) || sd === 0) return '';

  return (slice[0] - mean) / sd;
}

function metricChangeToBpForRateLikeOrBlank_(code, latest, base) {
  if (!isFiniteNumber_(latest) || !isFiniteNumber_(base)) return '';

  if (isBpMetric_(code)) return latest - base;
  if (isPercentMetric_(code)) return (latest - base) * 10000;
  return '';
}

function metricDistanceToMedianBpOrBlank_(code, descValues, windowSize) {
  if (!descValues || descValues.length < windowSize || windowSize <= 0) return '';

  var latest = descValues[0];
  if (!isFiniteNumber_(latest)) return '';

  var slice = descValues.slice(0, windowSize);
  var median = medianOfFiniteValuesOrBlank_(slice);
  if (!isFiniteNumber_(median)) return '';

  return metricChangeToBpForRateLikeOrBlank_(code, latest, median);
}

function isLongHistoryCompareMetric_(code, meta) {
  if (isPercentileMetric_(code)) return false;
  var m = meta || getMetricMeta_(code);
  var category = String((m && m.category) || '');
  var isRateLikeCategory =
    category.indexOf('利率') >= 0 ||
    category.indexOf('资金') >= 0 ||
    category.indexOf('相对价值') >= 0 ||
    category.indexOf('曲线') >= 0;

  if (!isRateLikeCategory) return false;
  return isBpMetric_(code) || isPercentMetric_(code);
}


function finalizeMetricStatsComparisons_(code, stats) {
  var descValues = stats.recentValuesDesc || [];
  var longHistoryDesc = stats.longHistoryDesc || descValues;
  var meta = getMetricMeta_(code);
  var isLongHistoryMetric = isLongHistoryCompareMetric_(code, meta);

  stats.prevValue = descValues.length > 1 ? descValues[1] : '';
  stats.chg1d = metricLagChangeDisplayUnitOrBlank_(code, descValues, 1);
  stats.chg5d = metricLagChangeDisplayUnitOrBlank_(code, descValues, 5);
  stats.chg20d = metricLagChangeDisplayUnitOrBlank_(code, descValues, 20);
  stats.diffMa20 = metricDiffVsMaDisplayUnitOrBlank_(code, descValues, 20);
  stats.pctl250 = isPercentileMetric_(code) ? '' : metricPercentileRankOrBlank_(descValues, 250);

  stats.pctl750 = isLongHistoryMetric ? metricPercentileRankOrBlank_(longHistoryDesc, 750) : '';
  stats.pctlAll = isLongHistoryMetric ? metricPercentileRankFullSampleOrBlank_(longHistoryDesc) : '';
  stats.zscore250 = isLongHistoryMetric ? metricZScoreOrBlank_(longHistoryDesc, 250) : '';
  stats.diffMedian250Bp = isLongHistoryMetric ? metricDistanceToMedianBpOrBlank_(code, longHistoryDesc, 250) : '';

  delete stats.recentValuesDesc;
  delete stats.longHistoryDesc;
  delete stats.keepLongHistory;
  return stats;
}

function buildRateMetrics_() { buildMetrics_(); }
function updateDashboard_() { buildMetrics_(); }
function buildCurveHistory_() { buildMetrics_(); }
function buildCurveSlope_() { buildMetrics_(); }
function rebuildCurveHistory_() { buildMetrics_(); }
function appendCurveHistoryRows_(rows) {
  if (!rows || !rows.length) return;
  buildMetrics_();
}


function buildMetricDictionarySheet_(ss, header, out) {
  var sheetName = '指标说明';
  var sheet = ss.getSheetByName(sheetName) || ss.insertSheet(sheetName);
  var t0 = Date.now();
  var statsMap = summarizeMetricColumnsOnce_(header, out);
  Logger.log('buildMetricDictionarySheet_ | stats_built_ms=' + (Date.now() - t0) + ' | metric_count=' + Math.max(0, header.length - 1));

  var rows = [];
  rows.push([
    'code',
    '中文名称',
    'English common name',
    '分类',
    '单位',
    '重要性',
    '历史起始',
    '历史截止',
    '可用样本数',
    '缺失数',
    '最新日期',
    '最新值',
    '较前值',
    '5日变化',
    '20日变化',
    '偏离20日均',
    '250日分位',
    '3年分位',
    '全样本分位',
    '1年Z-score',
    '距1年中位数(bp)',
    '偏高/上行解读',
    '偏低/下行解读',
    '主要用途',
    '备注'
  ]);

  var latestValueFormats = [];
  var deltaFormats = [];
  var percentileFormats = [];
  var zScoreFormats = [];
  var medianBpFormats = [];

  for (var c = 1; c < header.length; c++) {
    var code = header[c];
    var meta = getMetricMeta_(code);
    var stats = statsMap[code] || emptyMetricStats_();

    rows.push([
      code,
      meta.zh,
      meta.en,
      meta.category,
      meta.unit,
      meta.importance,
      stats.startDate,
      stats.endDate,
      stats.count,
      stats.missing,
      stats.latestDate,
      stats.latestValue,
      stats.chg1d,
      stats.chg5d,
      stats.chg20d,
      stats.diffMa20,
      stats.pctl250,
      stats.pctl750,
      stats.pctlAll,
      stats.zscore250,
      stats.diffMedian250Bp,
      meta.rise,
      meta.fall,
      meta.use,
      meta.note
    ]);

    var deltaFmt = getDictionaryDeltaNumberFormat_(code);
    latestValueFormats.push([getDictionaryLatestValueNumberFormat_(code)]);
    deltaFormats.push([deltaFmt, deltaFmt, deltaFmt, deltaFmt]);
    percentileFormats.push([getDictionaryPercentileNumberFormat_(), getDictionaryPercentileNumberFormat_(), getDictionaryPercentileNumberFormat_()]);
    zScoreFormats.push(['0.00']);
    medianBpFormats.push(['0 "bp"']);
  }

  t0 = Date.now();
  sheet.clearContents();
  sheet.getRange(1, 1, rows.length, rows[0].length).setValues(rows);
  sheet.setFrozenRows(1);
  if (rows.length > 1) {
    var rowCount = rows.length - 1;
    sheet.getRange(2, 7, rowCount, 1).setNumberFormat('yyyy-mm-dd');
    sheet.getRange(2, 8, rowCount, 1).setNumberFormat('yyyy-mm-dd');
    sheet.getRange(2, 11, rowCount, 1).setNumberFormat('yyyy-mm-dd');
    sheet.getRange(2, 12, rowCount, 1).setNumberFormats(latestValueFormats);
    sheet.getRange(2, 13, rowCount, 4).setNumberFormats(deltaFormats);
    sheet.getRange(2, 17, rowCount, 3).setNumberFormats(percentileFormats);
    sheet.getRange(2, 20, rowCount, 1).setNumberFormats(zScoreFormats);
    sheet.getRange(2, 21, rowCount, 1).setNumberFormats(medianBpFormats);
    applyMetricDictionaryConditionalFormatting_(sheet, rowCount);
  }
  Logger.log('buildMetricDictionarySheet_ | sheet_written_ms=' + (Date.now() - t0));
  Logger.log(sheetName + ' 已生成，共 ' + Math.max(0, rows.length - 1) + ' 条');
}

function rangesIntersect_(a, b) {
  if (!a || !b) return false;
  if (a.getSheet().getSheetId() !== b.getSheet().getSheetId()) return false;

  var aRowStart = a.getRow();
  var aRowEnd = a.getLastRow();
  var aColStart = a.getColumn();
  var aColEnd = a.getLastColumn();
  var bRowStart = b.getRow();
  var bRowEnd = b.getLastRow();
  var bColStart = b.getColumn();
  var bColEnd = b.getLastColumn();

  var rowOverlap = aRowStart <= bRowEnd && bRowStart <= aRowEnd;
  var colOverlap = aColStart <= bColEnd && bColStart <= aColEnd;
  return rowOverlap && colOverlap;
}

function ruleTouchesAnyRange_(rule, targetRanges) {
  if (!rule || !targetRanges || !targetRanges.length) return false;

  var ruleRanges = rule.getRanges();
  for (var i = 0; i < ruleRanges.length; i++) {
    for (var j = 0; j < targetRanges.length; j++) {
      if (rangesIntersect_(ruleRanges[i], targetRanges[j])) {
        return true;
      }
    }
  }
  return false;
}

function replaceConditionalFormatRulesForSheetRanges_(sheet, targetRanges, newRules) {
  var existingRules = sheet.getConditionalFormatRules() || [];
  var kept = [];

  for (var i = 0; i < existingRules.length; i++) {
    var rule = existingRules[i];
    if (!ruleTouchesAnyRange_(rule, targetRanges)) {
      kept.push(rule);
    }
  }

  sheet.setConditionalFormatRules(kept.concat(newRules || []));
}

function applyMetricDictionaryConditionalFormatting_(sheet, rowCount) {
  if (!sheet || rowCount <= 0) return;

  var deltaRange = sheet.getRange(2, 13, rowCount, 4);
  var percentileRange = sheet.getRange(2, 17, rowCount, 3);
  var zScoreRange = sheet.getRange(2, 20, rowCount, 1);
  var medianBpRange = sheet.getRange(2, 21, rowCount, 1);
  var rules = [];

  rules.push(
    SpreadsheetApp.newConditionalFormatRule()
      .whenNumberGreaterThan(0.5)
      .setFontColor('#9C0006')
      .setBackground('#FCE4D6')
      .setRanges([deltaRange, medianBpRange])
      .build()
  );

  rules.push(
    SpreadsheetApp.newConditionalFormatRule()
      .whenNumberLessThan(-0.5)
      .setFontColor('#006100')
      .setBackground('#E2F0D9')
      .setRanges([deltaRange, medianBpRange])
      .build()
  );

  rules.push(
    SpreadsheetApp.newConditionalFormatRule()
      .whenNumberBetween(-0.5, 0.5)
      .setFontColor('#666666')
      .setBackground('#F2F2F2')
      .setRanges([deltaRange, medianBpRange])
      .build()
  );

  rules.push(
    SpreadsheetApp.newConditionalFormatRule()
      .setGradientMinpointWithValue('#E2F0D9', SpreadsheetApp.InterpolationType.NUMBER, '0')
      .setGradientMidpointWithValue('#F2F2F2', SpreadsheetApp.InterpolationType.NUMBER, '0.5')
      .setGradientMaxpointWithValue('#FCE4D6', SpreadsheetApp.InterpolationType.NUMBER, '1')
      .setRanges([percentileRange])
      .build()
  );

  rules.push(
    SpreadsheetApp.newConditionalFormatRule()
      .whenNumberGreaterThan(1)
      .setFontColor('#9C0006')
      .setBackground('#FCE4D6')
      .setRanges([zScoreRange])
      .build()
  );

  rules.push(
    SpreadsheetApp.newConditionalFormatRule()
      .whenNumberLessThan(-1)
      .setFontColor('#006100')
      .setBackground('#E2F0D9')
      .setRanges([zScoreRange])
      .build()
  );

  rules.push(
    SpreadsheetApp.newConditionalFormatRule()
      .whenNumberBetween(-0.5, 0.5)
      .setFontColor('#666666')
      .setBackground('#F2F2F2')
      .setRanges([zScoreRange])
      .build()
  );

  replaceConditionalFormatRulesForSheetRanges_(sheet, [deltaRange, percentileRange, zScoreRange, medianBpRange], rules);
}

function summarizeMetricColumnsOnce_(header, out) {
  var stats = {};
  for (var c = 1; c < header.length; c++) {
    var code0 = header[c];
    var meta0 = getMetricMeta_(code0);
    var s0 = emptyMetricStats_();
    s0.keepLongHistory = isLongHistoryCompareMetric_(code0, meta0);
    stats[code0] = s0;
  }

  for (var r = 1; r < out.length; r++) {
    var row = out[r];
    var dateValue = normYMD_(row[0]);
    if (!dateValue) continue;

    for (var c2 = 1; c2 < header.length; c2++) {
      var code = header[c2];
      var s = stats[code];
      var value = row[c2];
      var hasValue = value !== '' && value !== null && typeof value !== 'undefined';

      if (hasValue) {
        s.count++;
        if (!s.startDate || dateValue < s.startDate) s.startDate = dateValue;
        if (!s.endDate || dateValue > s.endDate) s.endDate = dateValue;
        if (!s.latestDate || dateValue > s.latestDate) {
          s.latestDate = dateValue;
          s.latestValue = value;
        }

        var n = toNumberOrNull_(value);
        if (isFiniteNumber_(n)) {
          if (s.recentValuesDesc.length < 251) {
            s.recentValuesDesc.push(n);
          }
          if (s.keepLongHistory) {
            s.longHistoryDesc.push(n);
          }
        }
      } else {
        s.missing++;
      }
    }
  }

  for (var c3 = 1; c3 < header.length; c3++) {
    finalizeMetricStatsComparisons_(header[c3], stats[header[c3]]);
  }

  return stats;
}

function emptyMetricStats_() {
  return {
    startDate: '',
    endDate: '',
    count: 0,
    missing: 0,
    latestDate: '',
    latestValue: '',
    prevValue: '',
    chg1d: '',
    chg5d: '',
    chg20d: '',
    diffMa20: '',
    pctl250: '',
    pctl750: '',
    pctlAll: '',
    zscore250: '',
    diffMedian250Bp: '',
    keepLongHistory: false,
    recentValuesDesc: [],
    longHistoryDesc: []
  };
}

function getExtraMetricCatalogItems_() {
  return [
    {
      code: 'bfly_gov_1y_5y_10y_bp',
      zh: '国债1-5-10年蝶式',
      en: 'CGB 1Y-5Y-10Y butterfly',
      category: '曲线斜率',
      display_unit: 'bp',
      importance: '观察',
      rise: '5年腹部相对两端偏便宜，曲线中段抬升',
      fall: '5年腹部相对两端偏贵，曲线中段下沉',
      usage: '观察国债曲线腹部相对两端的扭曲程度。',
      note: '2*5Y - 1Y - 10Y。'
    },
    {
      code: 'bfly_gov_3y_5y_10y_bp',
      zh: '国债3-5-10年蝶式',
      en: 'CGB 3Y-5Y-10Y butterfly',
      category: '曲线斜率',
      display_unit: 'bp',
      importance: '重要',
      rise: '5年腹部相对3年和10年偏便宜',
      fall: '5年腹部相对3年和10年偏贵',
      usage: '观察国债中段相对两端的配置价值。',
      note: '2*5Y - 3Y - 10Y。'
    },
    {
      code: 'bfly_gov_5y_10y_30y_bp',
      zh: '国债5-10-30年蝶式',
      en: 'CGB 5Y-10Y-30Y butterfly',
      category: '曲线斜率',
      display_unit: 'bp',
      importance: '重要',
      rise: '10年腹部相对5年和30年偏便宜',
      fall: '10年腹部相对5年和30年偏贵',
      usage: '观察10年段相对短端与超长端的中枢位置。',
      note: '2*10Y - 5Y - 30Y。'
    },
    {
      code: 'bfly_cdb_3y_5y_10y_bp',
      zh: '国开债3-5-10年蝶式',
      en: 'CDB 3Y-5Y-10Y butterfly',
      category: '曲线斜率',
      display_unit: 'bp',
      importance: '观察',
      rise: '国开5年腹部相对两端偏便宜',
      fall: '国开5年腹部相对两端偏贵',
      usage: '观察国开债曲线腹部相对两端的扭曲。',
      note: '2*5Y - 3Y - 10Y。'
    },
    {
      code: 'spread_local_gov_cdb_5y_bp',
      zh: '地方债-国开债5年利差',
      en: 'Local Gov - CDB 5Y spread',
      category: '相对价值',
      display_unit: 'bp',
      importance: '重要',
      rise: '地方债相对国开债补偿上升',
      fall: '地方债相对国开债补偿压缩',
      usage: '比较地方债与国开债在5年端的横向配置价值。',
      note: '地方债5Y - 国开债5Y。'
    },
    {
      code: 'spread_local_gov_cdb_10y_bp',
      zh: '地方债-国开债10年利差',
      en: 'Local Gov - CDB 10Y spread',
      category: '相对价值',
      display_unit: 'bp',
      importance: '核心',
      rise: '地方债相对国开债补偿上升',
      fall: '地方债相对国开债补偿压缩',
      usage: '比较地方债与国开债在10年端的横向配置价值。',
      note: '地方债10Y - 国开债10Y。'
    },
    {
      code: 'spread_aaa_credit_cdb_3y_bp',
      zh: 'AAA信用-国开债3年利差',
      en: 'AAA credit - CDB 3Y spread',
      category: '相对价值',
      display_unit: 'bp',
      importance: '重要',
      rise: '高等级信用相对国开债补偿上升',
      fall: '高等级信用相对国开债补偿压缩',
      usage: '比较高等级信用与政策性金融债在3年端的配置价值。',
      note: 'AAA信用3Y - 国开债3Y。'
    },
    {
      code: 'spread_aaa_credit_cdb_5y_bp',
      zh: 'AAA信用-国开债5年利差',
      en: 'AAA credit - CDB 5Y spread',
      category: '相对价值',
      display_unit: 'bp',
      importance: '核心',
      rise: '高等级信用相对国开债补偿上升',
      fall: '高等级信用相对国开债补偿压缩',
      usage: '比较高等级信用与政策性金融债在5年端的配置价值。',
      note: 'AAA信用5Y - 国开债5Y。'
    },
    {
      code: 'spread_aaa_lgfv_gov_1y_bp',
      zh: 'AAA城投-国债1年利差',
      en: 'AAA LGFV - CGB 1Y spread',
      category: '相对价值',
      display_unit: 'bp',
      importance: '观察',
      rise: '城投相对国债补偿上升',
      fall: '城投相对国债补偿压缩',
      usage: '观察短端城投相对利率债的补偿水平。',
      note: 'AAA城投1Y - 国债1Y。'
    },
    {
      code: 'spread_aaa_lgfv_local_gov_1y_bp',
      zh: 'AAA城投-地方债1年利差',
      en: 'AAA LGFV - Local Gov 1Y spread',
      category: '相对价值',
      display_unit: 'bp',
      importance: '观察',
      rise: '城投相对地方债补偿上升',
      fall: '城投相对地方债补偿压缩',
      usage: '比较短端城投与地方债的横向价值。',
      note: '若原始表缺少地方债1Y点位，则该指标留空。'
    },
    {
      code: 'spread_aaa_ncd_dr007_bp',
      zh: 'AAA存单-DR007利差',
      en: 'AAA NCD - DR007 spread',
      category: '政策联动',
      display_unit: 'bp',
      importance: '重要',
      rise: '银行1年负债相对资金利率补偿走阔',
      fall: '银行1年负债相对资金利率补偿收窄',
      usage: '观察资金利率向银行同业负债端的传导。',
      note: 'AAA存单1Y - DR007。'
    },
    {
      code: 'spread_gov_1y_dr007_bp',
      zh: '国债1年-DR007利差',
      en: 'CGB 1Y - DR007 spread',
      category: '政策联动',
      display_unit: 'bp',
      importance: '重要',
      rise: '1年利率债相对资金利率补偿走阔',
      fall: '1年利率债相对资金利率补偿收窄',
      usage: '观察资金利率向短端利率债的传导。',
      note: '国债1Y - DR007。'
    },
    {
      code: 'spread_aaa_credit_dr007_bp',
      zh: 'AAA信用-DR007利差',
      en: 'AAA credit - DR007 spread',
      category: '政策联动',
      display_unit: 'bp',
      importance: '观察',
      rise: '高等级信用相对资金利率补偿走阔',
      fall: '高等级信用相对资金利率补偿收窄',
      usage: '观察资金利率向高等级信用债的传导。',
      note: 'AAA信用1Y - DR007。'
    },
    {
      code: 'spread_lpr1y_gov1y_bp',
      zh: 'LPR1Y-国债1年利差',
      en: 'LPR 1Y - CGB 1Y spread',
      category: '政策联动',
      display_unit: 'bp',
      importance: '观察',
      rise: '贷款定价相对短端国债更高',
      fall: '贷款定价相对短端国债更低',
      usage: '观察贷款利率与利率债之间的传导和定价缝隙。',
      note: 'LPR1Y - 国债1Y。'
    },
    {
      code: 'spread_lpr5y_lpr1y_bp',
      zh: 'LPR5Y-LPR1Y期限利差',
      en: 'LPR 5Y-1Y slope',
      category: '政策联动',
      display_unit: 'bp',
      importance: '观察',
      rise: '中长期贷款定价相对一般贷款更高',
      fall: '中长期贷款定价相对一般贷款更低',
      usage: '观察贷款利率曲线及按揭定价相对一般贷款的结构变化。',
      note: 'LPR5Y - LPR1Y。'
    },
    {
      code: 'slope_ust_10y_2y_bp',
      zh: '美债10年-2年期限利差',
      en: 'UST 10Y-2Y curve slope',
      category: '海外',
      display_unit: 'bp',
      importance: '重要',
      rise: '美国曲线更陡，增长或期限溢价定价抬升',
      fall: '美国曲线更平或更倒挂，衰退/降息预期更强',
      usage: '观察美国利率曲线结构及其对国内长端的外部约束。',
      note: 'UST10Y - UST2Y。'
    },
    {
      code: 'spread_cn_us_curve_slope_bp',
      zh: '中美曲线斜率差',
      en: 'China-US curve slope gap',
      category: '海外',
      display_unit: 'bp',
      importance: '重要',
      rise: '中国曲线相对美国更陡',
      fall: '中国曲线相对美国更平',
      usage: '比较中美利率曲线结构差异，判断国内陡峭化是否具有相对独立性。',
      note: '(国债10Y-2Y) - (美债10Y-2Y)。'
    },
    {
      code: 'ratio_copper_gold',
      zh: '铜/金比值',
      en: 'Copper/Gold ratio',
      category: '海外',
      display_unit: 'ratio',
      importance: '重要',
      rise: '增长偏好相对避险偏好增强',
      fall: '避险偏好相对增长偏好增强',
      usage: '观察全球增长预期与避险需求的相对强弱。',
      note: '铜价 / 金价。'
    },
    {
      code: 'ratio_spx_gold',
      zh: '标普/黄金比值',
      en: 'SPX/Gold ratio',
      category: '海外',
      display_unit: 'ratio',
      importance: '观察',
      rise: '风险资产相对避险资产更强',
      fall: '避险资产相对风险资产更强',
      usage: '观察全球风险偏好与避险偏好的相对变化。',
      note: 'SPX / Gold。'
    },
    {
      code: 'ratio_spx_vix',
      zh: '标普/VIX比值',
      en: 'SPX/VIX ratio',
      category: '海外',
      display_unit: 'ratio',
      importance: '观察',
      rise: '风险偏好改善，恐慌溢价回落',
      fall: '恐慌抬升或风险偏好走弱',
      usage: '将股指水平与波动率压缩为单一风险偏好代理。',
      note: 'SPX / VIX。'
    },
    {
      code: 'ratio_brent_gold',
      zh: '布油/黄金比值',
      en: 'Brent/Gold ratio',
      category: '海外',
      display_unit: 'ratio',
      importance: '观察',
      rise: '增长/通胀交易相对避险更强',
      fall: '避险偏好相对通胀交易更强',
      usage: '观察通胀交易与避险交易的相对强弱。',
      note: 'Brent / Gold。'
    },
    {
      code: 'count_rates_top_decile',
      zh: '利率指标高分位个数',
      en: 'Rate metrics in top decile',
      category: '状态统计',
      display_unit: 'count',
      importance: '观察',
      rise: '更多利率水平处于近一年高位',
      fall: '高分位利率数量减少',
      usage: '快速判断利率板块高位扩散程度。',
      note: '统计核心利率水平指标中250日分位≥90%的个数。'
    },
    {
      code: 'count_rates_bottom_decile',
      zh: '利率指标低分位个数',
      en: 'Rate metrics in bottom decile',
      category: '状态统计',
      display_unit: 'count',
      importance: '观察',
      rise: '更多利率水平处于近一年低位',
      fall: '低分位利率数量减少',
      usage: '快速判断利率板块低位拥挤程度。',
      note: '统计核心利率水平指标中250日分位≤10%的个数。'
    },
    {
      code: 'count_credit_spreads_top_decile',
      zh: '信用利差高分位个数',
      en: 'Credit spreads in top decile',
      category: '状态统计',
      display_unit: 'count',
      importance: '观察',
      rise: '更多信用利差处于近一年高位',
      fall: '高分位信用利差数量减少',
      usage: '快速判断信用补偿是否普遍走阔。',
      note: '统计核心信用利差指标中250日分位≥90%的个数。'
    },
    {
      code: 'count_credit_spreads_bottom_decile',
      zh: '信用利差低分位个数',
      en: 'Credit spreads in bottom decile',
      category: '状态统计',
      display_unit: 'count',
      importance: '观察',
      rise: '更多信用利差处于近一年低位',
      fall: '低分位信用利差数量减少',
      usage: '快速判断信用补偿是否普遍压缩。',
      note: '统计核心信用利差指标中250日分位≤10%的个数。'
    },
    {
      code: 'count_curve_metrics_top_decile',
      zh: '曲线指标高分位个数',
      en: 'Curve metrics in top decile',
      category: '状态统计',
      display_unit: 'count',
      importance: '观察',
      rise: '更多斜率/蝶式指标处于近一年高位',
      fall: '高分位曲线指标数量减少',
      usage: '快速判断陡峭化或腹部偏弱是否呈现扩散。',
      note: '统计核心曲线指标中250日分位≥90%的个数。'
    },
    {
      code: 'count_curve_metrics_bottom_decile',
      zh: '曲线指标低分位个数',
      en: 'Curve metrics in bottom decile',
      category: '状态统计',
      display_unit: 'count',
      importance: '观察',
      rise: '更多斜率/蝶式指标处于近一年低位',
      fall: '低分位曲线指标数量减少',
      usage: '快速判断平坦化或腹部偏贵是否呈现扩散。',
      note: '统计核心曲线指标中250日分位≤10%的个数。'
    }
  ];
}

function getMetricCatalogCodes_() {
  var base = (typeof METRIC_CATALOG_ !== 'undefined' && METRIC_CATALOG_) ? METRIC_CATALOG_ : [];
  if (!base.length) {
    throw new Error('METRIC_CATALOG_ 未定义，请先加载 30_metric_catalog.js');
  }
  var codes = [];
  var seen = {};
  var all = base.concat(getExtraMetricCatalogItems_());
  for (var i = 0; i < all.length; i++) {
    var code = String(all[i].code || '');
    if (!code || seen[code]) continue;
    seen[code] = true;
    codes.push(code);
  }
  return codes;
}

function getMetricCatalogMap_() {
  if (!getMetricCatalogMap_.cache_) {
    var map = {};
    if (typeof METRIC_CATALOG_ !== 'undefined' && METRIC_CATALOG_) {
      for (var i = 0; i < METRIC_CATALOG_.length; i++) {
        map[METRIC_CATALOG_[i].code] = METRIC_CATALOG_[i];
      }
    }
    var extras = getExtraMetricCatalogItems_();
    for (var j = 0; j < extras.length; j++) {
      map[extras[j].code] = extras[j];
    }
    getMetricCatalogMap_.cache_ = map;
  }
  return getMetricCatalogMap_.cache_;
}

function getMetricCatalogItem_(code) {
  return getMetricCatalogMap_()[String(code || '')] || null;
}

function getMetricMeta_(code) {
  var exact = getMetricCatalogItem_(code);
  if (exact) {
    return {
      zh: exact.zh || code,
      en: exact.en || code,
      category: exact.category || '其他',
      unit: metricUnitLabelFromCatalogItem_(exact),
      importance: exact.importance || '观察',
      rise: normalizeMeaningText_(exact.rise || ''),
      fall: normalizeMeaningText_(exact.fall || ''),
      use: exact.usage || '',
      note: exact.note || ''
    };
  }

  var maMatch = code.match(/^(.*)_ma(\d+)$/);
  if (maMatch) {
    var baseMeta = getMetricMeta_(maMatch[1]);
    return {
      zh: baseMeta.zh + maMatch[2] + '日均线',
      en: baseMeta.en + ' ' + maMatch[2] + '-day moving average',
      category: baseMeta.category,
      unit: baseMeta.unit,
      importance: '观察',
      rise: '趋势中枢上移',
      fall: '趋势中枢下移',
      use: '平滑短期噪音，观察趋势方向与拐点。',
      note: '派生指标；基于基础序列滚动计算。'
    };
  }

  var prankMatch = code.match(/^(.*)_prank(\d+)$/);
  if (prankMatch) {
    var baseMeta2 = getMetricMeta_(prankMatch[1]);
    return {
      zh: baseMeta2.zh + prankMatch[2] + '日历史分位',
      en: baseMeta2.en + ' ' + prankMatch[2] + '-day percentile rank',
      category: baseMeta2.category,
      unit: '0-1（显示为%）',
      importance: '观察',
      rise: '处于近' + prankMatch[2] + '日相对高位',
      fall: '处于近' + prankMatch[2] + '日相对低位',
      use: '判断当前水平在历史滚动窗口中的相对位置。',
      note: '派生指标；percentile rank，不是涨跌幅。'
    };
  }

  return {
    zh: code,
    en: code,
    category: '其他',
    unit: inferUnitLabel_(code),
    importance: '观察',
    rise: '',
    fall: '',
    use: '',
    note: '未单独维护元数据，请按字段名补充。'
  };
}

function metricUnitLabelFromCatalogItem_(item) {
  if (!item) return '';
  if (item.display_unit === 'bp') return 'bp';
  if (item.display_unit === 'percent') return '0-1（显示为%）';
  if (item.display_unit === 'count') return '个数';
  if (item.display_unit === 'ratio') return '倍数';
  return '原值';
}

function inferUnitLabel_(code) {
  if (/_prank\d+$/.test(code)) return '0-1（显示为%）';
  if (/_bp$/.test(code) || /_bp_ma\d+$/.test(code)) return 'bp';
  if (code === 'usd_broad' || code === 'usd_cny' || code === 'gold' || code === 'wti' || code === 'brent' || code === 'copper' || code === 'vix' || code === 'spx' || code === 'nasdaq_100' || /^(usd_broad|usd_cny|gold|wti|brent|copper|vix|spx|nasdaq_100)_ma\d+$/.test(code)) return '原值';
  return '0-1（显示为%）';
}
