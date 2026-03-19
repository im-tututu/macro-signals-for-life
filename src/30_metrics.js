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

  row.spread_cdb_gov_3y_bp = safeSpreadBpOrBlank_(row.cdb_3y, row.gov_3y);
  row.spread_cdb_gov_5y_bp = safeSpreadBpOrBlank_(row.cdb_5y, row.gov_5y);
  row.spread_cdb_gov_10y_bp = safeSpreadBpOrBlank_(row.cdb_10y, row.gov_10y);

  row.spread_local_gov_gov_5y_bp = safeSpreadBpOrBlank_(row.local_gov_5y, row.gov_5y);
  row.spread_local_gov_gov_10y_bp = safeSpreadBpOrBlank_(row.local_gov_10y, row.gov_10y);

  row.spread_aaa_credit_gov_1y_bp = safeSpreadBpOrBlank_(row.aaa_credit_1y, row.gov_1y);
  row.spread_aaa_credit_gov_3y_bp = safeSpreadBpOrBlank_(row.aaa_credit_3y, row.gov_3y);
  row.spread_aaa_credit_gov_5y_bp = safeSpreadBpOrBlank_(row.aaa_credit_5y, row.gov_5y);

  row.spread_aa_plus_credit_aaa_credit_1y_bp = safeSpreadBpOrBlank_(row.aa_plus_credit_1y, row.aaa_credit_1y);
  row.spread_aaa_plus_mtn_gov_1y_bp = safeSpreadBpOrBlank_(row.aaa_plus_mtn_1y, row.gov_1y);

  row.spread_aaa_mtn_aaa_plus_mtn_1y_bp = safeSpreadBpOrBlank_(row.aaa_mtn_1y, row.aaa_plus_mtn_1y);
  row.spread_aaa_credit_ncd_1y_bp = safeSpreadBpOrBlank_(row.aaa_credit_1y, row.aaa_ncd_1y);

  row.spread_aaa_bank_bond_aaa_credit_1y_bp = safeSpreadBpOrBlank_(row.aaa_bank_bond_1y, row.aaa_credit_1y);
  row.spread_aaa_bank_bond_aaa_credit_3y_bp = safeSpreadBpOrBlank_(row.aaa_bank_bond_3y, row.aaa_credit_3y);
  row.spread_aaa_bank_bond_aaa_credit_5y_bp = safeSpreadBpOrBlank_(row.aaa_bank_bond_5y, row.aaa_credit_5y);

  row.spread_aaa_lgfv_aaa_credit_1y_bp = safeSpreadBpOrBlank_(row.aaa_lgfv_1y, row.aaa_credit_1y);

  // 政策—市场联动
  row.spread_dr007_omo_7d_bp = safeSpreadBpOrBlank_(row.dr007_weighted_rate, row.omo_7d);
  row.spread_ncd_mlf_1y_bp = safeSpreadBpOrBlank_(row.aaa_ncd_1y, row.mlf_1y);
  row.spread_gov_mlf_1y_bp = safeSpreadBpOrBlank_(row.gov_1y, row.mlf_1y);
  row.spread_lpr_mlf_1y_bp = safeSpreadBpOrBlank_(row.lpr_1y, row.mlf_1y);
  row.spread_lpr_gov_5y_bp = safeSpreadBpOrBlank_(row.lpr_5y, row.gov_5y);
  row.spread_lpr5y_ncd1y_bp = safeSpreadBpOrBlank_(row.lpr_5y, row.aaa_ncd_1y);

  // 海外宏观
  row.spread_gov_ust_10y_bp = safeSpreadBpOrBlank_(row.gov_10y, row.ust_10y);
  row.spread_gov_ust_2y_bp = safeSpreadBpOrBlank_(row.gov_2y, row.ust_2y);
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
  var gov10Arr = [];
  var cdbGov10Arr = [];
  var aaaCreditGov5Arr = [];
  var sink1Arr = [];
  var ncd1Arr = [];
  var usdBroadArr = [];
  var usdCnyArr = [];
  var goldArr = [];
  var ust10Arr = [];

  for (var i = 0; i < rows.length; i++) {
    var r = rows[i];

    gov10Arr.push(toNumberOrNull_(r.gov_10y));
    cdbGov10Arr.push(toNumberOrNull_(r.spread_cdb_gov_10y_bp));
    aaaCreditGov5Arr.push(toNumberOrNull_(r.spread_aaa_credit_gov_5y_bp));
    sink1Arr.push(toNumberOrNull_(r.spread_aa_plus_credit_aaa_credit_1y_bp));
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

    r.spread_aa_plus_credit_aaa_credit_1y_bp_ma20 = rollingMeanAllowBlank_(sink1Arr, 20);
    r.spread_aa_plus_credit_aaa_credit_1y_bp_prank250 = rollingPercentileRankAllowBlank_(sink1Arr, 250);

    r.aaa_ncd_1y_ma20 = rollingMeanAllowBlank_(ncd1Arr, 20);
    r.aaa_ncd_1y_prank250 = rollingPercentileRankAllowBlank_(ncd1Arr, 250);
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
  }
  if (/_prank\d+$/.test(name)) return '0.00%';
  if (/_bp$/.test(name) || /_bp_ma\d+$/.test(name)) return '0 "bp"';
  return '0.00';
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
    '偏高/上行解读',
    '偏低/下行解读',
    '主要用途',
    '备注'
  ]);

  var latestValueFormats = [];
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
      meta.rise,
      meta.fall,
      meta.use,
      meta.note
    ]);
    latestValueFormats.push([getDictionaryLatestValueNumberFormat_(code)]);
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
  }
  Logger.log('buildMetricDictionarySheet_ | sheet_written_ms=' + (Date.now() - t0));
  Logger.log(sheetName + ' 已生成，共 ' + Math.max(0, rows.length - 1) + ' 条');
}

function summarizeMetricColumnsOnce_(header, out) {
  var stats = {};
  for (var c = 1; c < header.length; c++) {
    stats[header[c]] = emptyMetricStats_();
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
      } else {
        s.missing++;
      }
    }
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
    latestValue: ''
  };
}

function getMetricCatalogCodes_() {
  if (typeof METRIC_CATALOG_ === 'undefined' || !METRIC_CATALOG_ || !METRIC_CATALOG_.length) {
    throw new Error('METRIC_CATALOG_ 未定义，请先加载 30_metric_catalog.js');
  }
  var codes = [];
  for (var i = 0; i < METRIC_CATALOG_.length; i++) {
    codes.push(METRIC_CATALOG_[i].code);
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
  return '原值';
}

function inferUnitLabel_(code) {
  if (/_prank\d+$/.test(code)) return '0-1（显示为%）';
  if (/_bp$/.test(code) || /_bp_ma\d+$/.test(code)) return 'bp';
  if (code === 'usd_broad' || code === 'usd_cny' || code === 'gold' || code === 'wti' || code === 'brent' || code === 'copper' || code === 'vix' || code === 'spx' || code === 'nasdaq_100' || /^(usd_broad|usd_cny|gold|wti|brent|copper|vix|spx|nasdaq_100)_ma\d+$/.test(code)) return '原值';
  return '0-1（显示为%）';
}
