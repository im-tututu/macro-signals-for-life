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
  Logger.log('buildMetrics_ v5 running');

  var ss = SpreadsheetApp.getActive();
  var curveSheet = ss.getSheetByName(SHEET_CURVE_RAW);
  if (!curveSheet) throw new Error('找不到工作表: ' + SHEET_CURVE_RAW);

  var dst = ss.getSheetByName(SHEET_METRICS) || ss.insertSheet(SHEET_METRICS);
  var curveValues = curveSheet.getDataRange().getValues();
  var header = buildMetricsHeader_();

  if (curveValues.length < 2) {
    writeMetricsOutput_(dst, [header]);
    return;
  }

  var rawHeader = curveValues[0];
  var termIndex = buildTermColumnIndex_(rawHeader);
  var curveByDate = buildCurveBucketByDate_(curveValues);

  var moneySheet = ss.getSheetByName(SHEET_MONEY_MARKET_RAW);
  var moneyMap = moneySheet ? readMoneyMarketMetricsMap_(moneySheet) : {};

  var policySheet = ss.getSheetByName(SHEET_POLICY_RATE_RAW);
  var policyTimeline = policySheet ? readPolicyRateTimeline_(policySheet) : [];

  var overseasSheet = ss.getSheetByName(SHEET_OVERSEAS_MACRO_RAW);
  var overseasTimeline = overseasSheet ? readOverseasMacroTimeline_(overseasSheet) : [];

  var dates = Object.keys(curveByDate).sort();
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

  applyRollingMetrics_(rows);
  rows.reverse();

  var out = [header];
  for (var j = 0; j < rows.length; j++) {
    out.push(metricsRowToArray_(rows[j], header));
  }

  writeMetricsOutput_(dst, out);
  Logger.log(SHEET_METRICS + ' 已重建，共 ' + Math.max(0, out.length - 1) + ' 条');
}

function buildMetricsHeader_() {
  return [
    'date',

    'gov_1y', 'gov_2y', 'gov_3y', 'gov_5y', 'gov_10y', 'gov_30y',
    'cdb_3y', 'cdb_5y', 'cdb_10y',

    'aaa_credit_1y', 'aaa_credit_3y', 'aaa_credit_5y',
    'aa_plus_credit_1y',

    'aaa_plus_mtn_1y',
    'aaa_mtn_1y', 'aaa_mtn_3y', 'aaa_mtn_5y',

    'aaa_ncd_1y',

    'aaa_bank_bond_1y', 'aaa_bank_bond_3y', 'aaa_bank_bond_5y',
    'aaa_lgfv_1y',

    'local_gov_5y', 'local_gov_10y',

    'dr007_weighted_rate',
    'omo_7d',
    'mlf_1y',
    'lpr_1y',
    'lpr_5y',

    'ust_2y',
    'ust_10y',
    'usd_broad',
    'usd_cny',
    'gold',
    'wti',
    'brent',
    'copper',
    'vix',
    'spx',
    'nasdaq_100',

    'gov_slope_10_1',
    'gov_slope_10_3',
    'gov_slope_30_10',
    'cdb_slope_10_3',

    'spread_cdb_gov_3y',
    'spread_cdb_gov_5y',
    'spread_cdb_gov_10y',

    'spread_local_gov_gov_5y',
    'spread_local_gov_gov_10y',

    'spread_aaa_credit_gov_1y',
    'spread_aaa_credit_gov_3y',
    'spread_aaa_credit_gov_5y',

    'spread_aa_plus_vs_aaa_credit_1y',
    'spread_aaa_plus_mtn_gov_1y',

    'spread_aaa_mtn_vs_aaa_plus_mtn_1y',
    'spread_aaa_credit_ncd_1y',

    'spread_aaa_bank_vs_aaa_credit_1y',
    'spread_aaa_bank_vs_aaa_credit_3y',
    'spread_aaa_bank_vs_aaa_credit_5y',

    'spread_aaa_lgfv_vs_aaa_credit_1y',

    'spread_dr007_omo_7d',
    'spread_ncd_1y_mlf_1y',
    'spread_gov_1y_mlf_1y',
    'spread_lpr_1y_mlf_1y',
    'spread_lpr_5y_gov_5y',
    'spread_lpr_5y_ncd_1y',

    'spread_lgfv_vs_high_grade_credit_1y',
    'spread_bank_bond_vs_high_grade_credit_1y',
    'spread_bank_bond_vs_high_grade_credit_3y',
    'spread_bank_bond_vs_high_grade_credit_5y',
    'spread_ncd_mlf_1y',
    'spread_lpr5y_gov5y',

    'cn_us_10y_spread',
    'cn_us_2y_spread',
    'usd_broad_ma20',
    'usd_cny_ma20',
    'gold_ma20',
    'ust_10y_pct250',
    'usd_cny_pct250',

    'gov_10y_ma20',
    'gov_10y_ma60',
    'gov_10y_ma120',
    'gov_10y_pct250',

    'spread_cdb_gov_10y_ma20',
    'spread_cdb_gov_10y_pct250',

    'spread_aaa_credit_gov_5y_ma20',
    'spread_aaa_credit_gov_5y_pct250',

    'spread_aa_plus_vs_aaa_credit_1y_ma20',
    'spread_aa_plus_vs_aaa_credit_1y_pct250',

    'aaa_ncd_1y_ma20',
    'aaa_ncd_1y_pct250'
  ];
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

  row.dr007_weighted_rate = pickMetricOrBlank_(moneyRow.dr007_weighted_rate);
  row.omo_7d = pickMetricOrBlank_(policyState.omo_7d);
  row.mlf_1y = pickMetricOrBlank_(policyState.mlf_1y);
  row.lpr_1y = pickMetricOrBlank_(policyState.lpr_1y);
  row.lpr_5y = pickMetricOrBlank_(policyState.lpr_5y);

  row.ust_2y = pickMetricOrBlank_(overseasState.ust_2y);
  row.ust_10y = pickMetricOrBlank_(overseasState.ust_10y);
  row.usd_broad = pickMetricOrBlank_(overseasState.usd_broad);
  row.usd_cny = pickMetricOrBlank_(overseasState.usd_cny);
  row.gold = pickMetricOrBlank_(overseasState.gold);
  row.wti = pickMetricOrBlank_(overseasState.wti);
  row.brent = pickMetricOrBlank_(overseasState.brent);
  row.copper = pickMetricOrBlank_(overseasState.copper);
  row.vix = pickMetricOrBlank_(overseasState.vix);
  row.spx = pickMetricOrBlank_(overseasState.spx);
  row.nasdaq_100 = pickMetricOrBlank_(overseasState.nasdaq_100);

  row.gov_slope_10_1 = safeSubOrBlank_(row.gov_10y, row.gov_1y);
  row.gov_slope_10_3 = safeSubOrBlank_(row.gov_10y, row.gov_3y);
  row.gov_slope_30_10 = safeSubOrBlank_(row.gov_30y, row.gov_10y);
  row.cdb_slope_10_3 = safeSubOrBlank_(row.cdb_10y, row.cdb_3y);

  row.spread_cdb_gov_3y = safeSubOrBlank_(row.cdb_3y, row.gov_3y);
  row.spread_cdb_gov_5y = safeSubOrBlank_(row.cdb_5y, row.gov_5y);
  row.spread_cdb_gov_10y = safeSubOrBlank_(row.cdb_10y, row.gov_10y);

  row.spread_local_gov_gov_5y = safeSubOrBlank_(row.local_gov_5y, row.gov_5y);
  row.spread_local_gov_gov_10y = safeSubOrBlank_(row.local_gov_10y, row.gov_10y);

  row.spread_aaa_credit_gov_1y = safeSubOrBlank_(row.aaa_credit_1y, row.gov_1y);
  row.spread_aaa_credit_gov_3y = safeSubOrBlank_(row.aaa_credit_3y, row.gov_3y);
  row.spread_aaa_credit_gov_5y = safeSubOrBlank_(row.aaa_credit_5y, row.gov_5y);

  row.spread_aa_plus_vs_aaa_credit_1y = safeSubOrBlank_(row.aa_plus_credit_1y, row.aaa_credit_1y);
  row.spread_aaa_plus_mtn_gov_1y = safeSubOrBlank_(row.aaa_plus_mtn_1y, row.gov_1y);

  row.spread_aaa_mtn_vs_aaa_plus_mtn_1y = safeSubOrBlank_(row.aaa_mtn_1y, row.aaa_plus_mtn_1y);
  row.spread_aaa_credit_ncd_1y = safeSubOrBlank_(row.aaa_credit_1y, row.aaa_ncd_1y);

  row.spread_aaa_bank_vs_aaa_credit_1y = safeSubOrBlank_(row.aaa_bank_bond_1y, row.aaa_credit_1y);
  row.spread_aaa_bank_vs_aaa_credit_3y = safeSubOrBlank_(row.aaa_bank_bond_3y, row.aaa_credit_3y);
  row.spread_aaa_bank_vs_aaa_credit_5y = safeSubOrBlank_(row.aaa_bank_bond_5y, row.aaa_credit_5y);

  row.spread_aaa_lgfv_vs_aaa_credit_1y = safeSubOrBlank_(row.aaa_lgfv_1y, row.aaa_credit_1y);

  // P4：政策—市场联动指标
  row.spread_dr007_omo_7d = safeSubOrBlank_(row.dr007_weighted_rate, row.omo_7d);
  row.spread_ncd_1y_mlf_1y = safeSubOrBlank_(row.aaa_ncd_1y, row.mlf_1y);
  row.spread_gov_1y_mlf_1y = safeSubOrBlank_(row.gov_1y, row.mlf_1y);
  row.spread_lpr_1y_mlf_1y = safeSubOrBlank_(row.lpr_1y, row.mlf_1y);
  row.spread_lpr_5y_gov_5y = safeSubOrBlank_(row.lpr_5y, row.gov_5y);
  row.spread_lpr_5y_ncd_1y = safeSubOrBlank_(row.lpr_5y, row.aaa_ncd_1y);

  // P6：房地产融资环境一期版
  row.spread_lgfv_vs_high_grade_credit_1y = row.spread_aaa_lgfv_vs_aaa_credit_1y;
  row.spread_bank_bond_vs_high_grade_credit_1y = row.spread_aaa_bank_vs_aaa_credit_1y;
  row.spread_bank_bond_vs_high_grade_credit_3y = row.spread_aaa_bank_vs_aaa_credit_3y;
  row.spread_bank_bond_vs_high_grade_credit_5y = row.spread_aaa_bank_vs_aaa_credit_5y;
  row.spread_ncd_mlf_1y = row.spread_ncd_1y_mlf_1y;
  row.spread_lpr5y_gov5y = row.spread_lpr_5y_gov_5y;

  // P5：海外宏观一期版
  row.cn_us_10y_spread = safeSubOrBlank_(row.gov_10y, row.ust_10y);
  row.cn_us_2y_spread = safeSubOrBlank_(row.gov_2y, row.ust_2y);
  row.usd_broad_ma20 = '';
  row.usd_cny_ma20 = '';
  row.gold_ma20 = '';
  row.ust_10y_pct250 = '';
  row.usd_cny_pct250 = '';

  row.gov_10y_ma20 = '';
  row.gov_10y_ma60 = '';
  row.gov_10y_ma120 = '';
  row.gov_10y_pct250 = '';

  row.spread_cdb_gov_10y_ma20 = '';
  row.spread_cdb_gov_10y_pct250 = '';

  row.spread_aaa_credit_gov_5y_ma20 = '';
  row.spread_aaa_credit_gov_5y_pct250 = '';

  row.spread_aa_plus_vs_aaa_credit_1y_ma20 = '';
  row.spread_aa_plus_vs_aaa_credit_1y_pct250 = '';

  row.aaa_ncd_1y_ma20 = '';
  row.aaa_ncd_1y_pct250 = '';

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
    cdbGov10Arr.push(toNumberOrNull_(r.spread_cdb_gov_10y));
    aaaCreditGov5Arr.push(toNumberOrNull_(r.spread_aaa_credit_gov_5y));
    sink1Arr.push(toNumberOrNull_(r.spread_aa_plus_vs_aaa_credit_1y));
    ncd1Arr.push(toNumberOrNull_(r.aaa_ncd_1y));
    usdBroadArr.push(toNumberOrNull_(r.usd_broad));
    usdCnyArr.push(toNumberOrNull_(r.usd_cny));
    goldArr.push(toNumberOrNull_(r.gold));
    ust10Arr.push(toNumberOrNull_(r.ust_10y));

    r.usd_broad_ma20 = rollingMeanAllowBlank_(usdBroadArr, 20);
    r.usd_cny_ma20 = rollingMeanAllowBlank_(usdCnyArr, 20);
    r.gold_ma20 = rollingMeanAllowBlank_(goldArr, 20);
    r.ust_10y_pct250 = rollingPercentileRankAllowBlank_(ust10Arr, 250);
    r.usd_cny_pct250 = rollingPercentileRankAllowBlank_(usdCnyArr, 250);

    r.gov_10y_ma20 = rollingMeanAllowBlank_(gov10Arr, 20);
    r.gov_10y_ma60 = rollingMeanAllowBlank_(gov10Arr, 60);
    r.gov_10y_ma120 = rollingMeanAllowBlank_(gov10Arr, 120);
    r.gov_10y_pct250 = rollingPercentileRankAllowBlank_(gov10Arr, 250);

    r.spread_cdb_gov_10y_ma20 = rollingMeanAllowBlank_(cdbGov10Arr, 20);
    r.spread_cdb_gov_10y_pct250 = rollingPercentileRankAllowBlank_(cdbGov10Arr, 250);

    r.spread_aaa_credit_gov_5y_ma20 = rollingMeanAllowBlank_(aaaCreditGov5Arr, 20);
    r.spread_aaa_credit_gov_5y_pct250 = rollingPercentileRankAllowBlank_(aaaCreditGov5Arr, 250);

    r.spread_aa_plus_vs_aaa_credit_1y_ma20 = rollingMeanAllowBlank_(sink1Arr, 20);
    r.spread_aa_plus_vs_aaa_credit_1y_pct250 = rollingPercentileRankAllowBlank_(sink1Arr, 250);

    r.aaa_ncd_1y_ma20 = rollingMeanAllowBlank_(ncd1Arr, 20);
    r.aaa_ncd_1y_pct250 = rollingPercentileRankAllowBlank_(ncd1Arr, 250);
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
  return isFiniteNumber_(n) ? n : '';
}

function pickMetricOrBlank_(v) {
  var n = toNumberOrNull_(v);
  return isFiniteNumber_(n) ? n : '';
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
  sheet.clearFormats();
  sheet.getRange(1, 1, out.length, out[0].length).setValues(out);

  if (out.length > 1) {
    sheet.getRange(2, 1, out.length - 1, 1).setNumberFormat('yyyy-mm-dd');
    sheet.getRange(2, 2, out.length - 1, out[0].length - 1).setNumberFormat('0.0000');
  }

  sheet.setFrozenRows(1);
  sheet.getRange(1, 1, 1, out[0].length)
    .setFontWeight('bold')
    .setBackground('#d9eaf7');
  sheet.autoResizeColumns(1, out[0].length);
}

function safeSubOrBlank_(a, b) {
  if (!isFiniteNumber_(a) || !isFiniteNumber_(b)) return '';
  return a - b;
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
