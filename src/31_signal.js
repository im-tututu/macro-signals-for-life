/********************
 * 31_signal.js
 * 基于“指标”生成两个信号表：
 * 1) 信号-主要：一天一行，聚合主要信号与配置建议
 * 2) 信号-明细：一天多行，一个信号一行，便于后续扩展
 *
 * 命名约定：
 * - 一级分类：liquidity / rates / credit
 * - 当前 theme：资产配置
 *
 * 原则：
 * 1) 指标表负责客观数值
 * 2) 主要表负责日常查看
 * 3) 明细表负责长表扩展
 * 4) 尽量少改动现有项目结构，只在本文件内完成拆表与重命名
 ********************/

var SIGNAL_THEME_ASSET_ALLOC = '资产配置';
var SIGNAL_THEME_LIFE_INVEST = '生活投资';


function buildSignal_() {
  var ss = SpreadsheetApp.getActiveSpreadsheet();
  var metricsSheet = mustGetSheet_(ss, SHEET_METRICS);
  var mainSheet = getOrCreateSheetByName_(ss, SHEET_SIGNAL_MAIN || '信号-主要');
  var detailSheet = getOrCreateSheetByName_(ss, SHEET_SIGNAL_DETAIL || '信号-明细');

  var metricRows = readMetricsRowsV2_(metricsSheet);
  if (!metricRows.length) {
    throw new Error(SHEET_METRICS + ' 无有效数据。');
  }

  metricRows = sortAndDedupeByDate_(metricRows, function(row) {
    return row.dateKey;
  });

  var moneyMarketSheet = ss.getSheetByName(SHEET_MONEY_MARKET_RAW);
  var dr007Map = moneyMarketSheet ? readMoneyMarketDr007Map_(moneyMarketSheet) : {};

  var mainRowsAsc = [];
  var detailRows = [];

  for (var i = 0; i < metricRows.length; i++) {
    var row = metricRows[i];
    var dr007 = toNumberOrNull_(dr007Map[row.dateKey]);

    var liquidity = classifyLiquidityRegime_(dr007);
    var ratesDuration = classifyDurationSignal_(row);
    var ratesUltraLong = classifyUltraLongSignal_(row);
    var ratesStrategy = classifyRatesStrategyTilt_(ratesDuration, ratesUltraLong);
    var ratesCurve = classifyCurveSignal_(row);
    var ratesPolicyBank = classifyPolicyBankSignal_(row);
    var ratesLocalGov = classifyLocalGovSignal_(row);
    var ratesRvRanking = classifyRatesRvRanking_(ratesPolicyBank, ratesLocalGov);
    var creditQuality = classifyHighGradeCreditSignal_(row);
    var creditSink = classifyCreditSinkSignal_(row);
    var creditStrategy = classifyCreditStrategyTilt_(creditQuality, creditSink);
    var creditMtnTier = classifyMtnTierSignal_(row);
    var creditShortEnd = classifyNcdSignal_(row, dr007);
    var mortgageBackground = classifyMortgageBackgroundSignal_(row);
    var housingBackground = classifyHousingBackgroundSignal_(row);
    var fxBackground = classifyFxBackgroundSignal_(row);
    var usdAllocationBackground = classifyUsdAllocationBackgroundSignal_(row);
    var goldBackground = classifyGoldBackgroundSignal_(row);
    var commodityBackground = classifyCommodityBackgroundSignal_(row);

    var alloc = buildAllocationFromSignals_(
      ratesDuration,
      ratesUltraLong,
      creditQuality,
      creditSink,
      creditShortEnd
    );

    var householdBuckets = buildHouseholdBuckets_(
      liquidity,
      ratesStrategy,
      creditStrategy,
      mortgageBackground,
      housingBackground,
      fxBackground,
      usdAllocationBackground,
      goldBackground,
      commodityBackground,
      alloc
    );

    var householdAllocation = classifyHouseholdAllocationSignal_(
      ratesStrategy,
      creditStrategy,
      mortgageBackground,
      housingBackground,
      fxBackground,
      usdAllocationBackground,
      goldBackground,
      commodityBackground,
      householdBuckets
    );

    var householdComment = buildHouseholdComment_(
      liquidity,
      ratesStrategy,
      creditStrategy,
      mortgageBackground,
      housingBackground,
      fxBackground,
      usdAllocationBackground,
      goldBackground,
      commodityBackground,
      alloc,
      householdAllocation,
      householdBuckets
    );

    mainRowsAsc.push([
      row.dateObj,
      liquidity.label,
      ratesStrategy.label,
      ratesRvRanking.label,
      creditStrategy.label,
      mortgageBackground.label,
      housingBackground.label,
      fxBackground.label,
      usdAllocationBackground.label,
      goldBackground.label,
      commodityBackground.label,
      householdAllocation.label,
      householdBuckets.bucket_cash.signal.label,
      householdBuckets.bucket_stable_fixed_income.signal.label,
      householdBuckets.bucket_active_fixed_income.signal.label,
      householdBuckets.bucket_hedge.signal.label,
      householdBuckets.bucket_risk.signal.label,
      householdBuckets.bucket_cash.target_pct,
      householdBuckets.bucket_stable_fixed_income.target_pct,
      householdBuckets.bucket_active_fixed_income.target_pct,
      householdBuckets.bucket_hedge.target_pct,
      householdBuckets.bucket_risk.target_pct,
      householdComment,
      alloc.alloc_rates_long,
      alloc.alloc_rates_mid,
      alloc.alloc_rates_short,
      alloc.alloc_credit_high_grade,
      alloc.alloc_credit_sink,
      alloc.alloc_cash
    ]);

    pushSignalDetailRows_(detailRows, row.dateObj, SIGNAL_THEME_ASSET_ALLOC, [
      makeSignalDetail_(liquidity, 'liquidity_regime', 'liquidity', '资金与流动性环境', 'dr007_weighted_rate', 'daily', 10),
      makeSignalDetail_(ratesStrategy, 'rates_strategy_tilt', 'rates', '利率债久期/超长端策略', 'gov_10y_pct250|gov_slope_10_1|gov_slope_30_10', 'weekly', 15),
      makeSignalDetail_(ratesDuration, 'rates_duration_tilt', 'rates', '利率债久期倾向', 'gov_10y_pct250|gov_slope_10_1', 'weekly', 20),
      makeSignalDetail_(ratesUltraLong, 'rates_ultra_long_tilt', 'rates', '利率债超长端倾向', 'gov_slope_30_10', 'weekly', 30),
      makeSignalDetail_(ratesCurve, 'rates_curve_shape', 'rates', '利率债曲线形态', 'gov_slope_10_1', 'weekly', 40),
      makeSignalDetail_(ratesPolicyBank, 'rates_rv_policy_bank_vs_gov', 'rates', '利率债相对价值：国开债 vs 国债', 'spread_cdb_gov_10y|spread_cdb_gov_10y_pct250', 'weekly', 50),
      makeSignalDetail_(ratesLocalGov, 'rates_rv_local_gov_vs_gov', 'rates', '利率债相对价值：地方债 vs 国债', 'spread_local_gov_gov_10y', 'weekly', 60),
      makeSignalDetail_(ratesRvRanking, 'rates_rv_ranking', 'rates', '利率债相对价值排序', 'spread_cdb_gov_10y_pct250|spread_local_gov_gov_10y', 'weekly', 70),
      makeSignalDetail_(creditStrategy, 'credit_strategy_tilt', 'credit', '信用债资质/下沉策略', 'spread_aaa_credit_gov_5y_pct250|spread_aa_plus_vs_aaa_credit_1y_pct250', 'weekly', 80),
      makeSignalDetail_(creditQuality, 'credit_quality_tilt', 'credit', '信用债资质倾向', 'spread_aaa_credit_gov_5y|spread_aaa_credit_gov_5y_pct250', 'weekly', 90),
      makeSignalDetail_(creditSink, 'credit_sink_tilt', 'credit', '信用债下沉倾向', 'spread_aa_plus_vs_aaa_credit_1y|spread_aa_plus_vs_aaa_credit_1y_pct250', 'weekly', 100),
      makeSignalDetail_(creditMtnTier, 'credit_rv_mtn_tier', 'credit', '信用债相对价值：AAA中票 vs AAA+中票', 'spread_aaa_mtn_vs_aaa_plus_mtn_1y', 'weekly', 110),
      makeSignalDetail_(creditShortEnd, 'credit_rv_short_end_vs_ncd', 'credit', '短端票息资产：高等级信用 vs 存单', 'spread_aaa_credit_ncd_1y|aaa_ncd_1y_pct250|dr007_weighted_rate', 'weekly', 120)
    ]);

    pushSignalDetailRows_(detailRows, row.dateObj, SIGNAL_THEME_LIFE_INVEST, [
      makeSignalDetail_(mortgageBackground, 'view_mortgage_background', 'housing', '房贷背景', 'spread_lpr_5y_gov_5y|spread_lpr_5y_ncd_1y|spread_lpr_1y_mlf_1y|spread_ncd_mlf_1y|spread_dr007_omo_7d', 'weekly', 210),
      makeSignalDetail_(housingBackground, 'view_housing_background', 'housing', '住房背景', 'spread_local_gov_gov_5y|spread_local_gov_gov_10y|spread_lgfv_vs_high_grade_credit_1y|spread_bank_bond_vs_high_grade_credit_1y|spread_bank_bond_vs_high_grade_credit_3y|spread_bank_bond_vs_high_grade_credit_5y|spread_ncd_mlf_1y|spread_lpr5y_gov5y', 'weekly', 220),
      makeSignalDetail_(fxBackground, 'view_fx_background', 'fx', '汇率背景', 'cn_us_10y_spread|cn_us_2y_spread|usd_cny|usd_cny_ma20|usd_cny_pct250', 'daily', 230),
      makeSignalDetail_(usdAllocationBackground, 'view_usd_allocation_background', 'fx', '美元配置背景', 'cn_us_10y_spread|cn_us_2y_spread|usd_broad|usd_broad_ma20|usd_cny_pct250', 'weekly', 240),
      makeSignalDetail_(goldBackground, 'view_gold_background', 'hedge', '黄金背景', 'gold|gold_ma20|usd_cny_pct250|cn_us_10y_spread|cn_us_2y_spread|vix', 'weekly', 250),
      makeSignalDetail_(commodityBackground, 'view_commodity_background', 'macro', '顺周期商品背景', 'wti|brent|copper|vix|spx|nasdaq_100', 'weekly', 260),
      makeSignalDetail_(householdAllocation, 'view_household_allocation', 'household', '家庭资产桶建议', 'rates_strategy_tilt|credit_strategy_tilt|view_fx_background|view_gold_background|view_commodity_background', 'weekly', 270),
      makeSignalDetail_(householdBuckets.bucket_cash.signal, 'bucket_cash_tilt', 'household', '活钱桶倾向', 'liquidity_regime|view_fx_background|view_household_allocation', 'weekly', 271),
      makeSignalDetail_(householdBuckets.bucket_stable_fixed_income.signal, 'bucket_stable_fixed_income_tilt', 'household', '稳健固收桶倾向', 'rates_strategy_tilt|credit_strategy_tilt|view_household_allocation', 'weekly', 272),
      makeSignalDetail_(householdBuckets.bucket_active_fixed_income.signal, 'bucket_active_fixed_income_tilt', 'household', '进取固收桶倾向', 'rates_strategy_tilt|credit_strategy_tilt|view_household_allocation', 'weekly', 273),
      makeSignalDetail_(householdBuckets.bucket_hedge.signal, 'bucket_hedge_tilt', 'household', '对冲保值桶倾向', 'view_fx_background|view_usd_allocation_background|view_gold_background', 'weekly', 274),
      makeSignalDetail_(householdBuckets.bucket_risk.signal, 'bucket_risk_tilt', 'household', '风险资产桶倾向', 'view_housing_background|view_commodity_background|view_fx_background', 'weekly', 275),
      makeSignalDetail_(makeSignalResult_(householdComment, 'note', 0, 'neutral', 'low', householdComment), 'comment_household', 'household', '家庭配置说明', 'multiple', 'weekly', 280)
    ]);
  }

  var mainRowsDesc = mainRowsAsc.slice().reverse();
  var detailRowsDesc = sortDetailRowsDesc_(detailRows);

  writeSignalMainSheet_(mainSheet, mainRowsDesc);
  writeSignalDetailSheet_(detailSheet, detailRowsDesc);
}


function buildETFSignal_() { buildSignal_(); }
function runBondAllocationSignal_() { buildSignal_(); }
function buildBondAllocationSignal_() { buildSignal_(); }


function writeSignalMainSheet_(sheet, rows) {
  var header = [[
    'date',
    'liquidity_regime',
    'rates_strategy_tilt',
    'rates_rv_ranking',
    'credit_strategy_tilt',
    'view_mortgage_background',
    'view_housing_background',
    'view_fx_background',
    'view_usd_allocation_background',
    'view_gold_background',
    'view_commodity_background',
    'view_household_allocation',
    'bucket_cash_tilt',
    'bucket_stable_fixed_income_tilt',
    'bucket_active_fixed_income_tilt',
    'bucket_hedge_tilt',
    'bucket_risk_tilt',
    'bucket_cash',
    'bucket_stable_fixed_income',
    'bucket_active_fixed_income',
    'bucket_hedge',
    'bucket_risk',
    'comment_household',
    'alloc_rates_long',
    'alloc_rates_mid',
    'alloc_rates_short',
    'alloc_credit_high_grade',
    'alloc_credit_sink',
    'alloc_cash'
  ]];

  sheet.clearContents();
  sheet.clearFormats();
  sheet.getRange(1, 1, 1, header[0].length).setValues(header);

  if (rows.length > 0) {
    sheet.getRange(2, 1, rows.length, rows[0].length).setValues(rows);
    sheet.getRange(2, 1, rows.length, 1).setNumberFormat('yyyy-mm-dd');
    sheet.getRange(2, 18, rows.length, 5).setNumberFormat('0');
    sheet.getRange(2, 24, rows.length, 6).setNumberFormat('0');
  }

  formatSignalSheet_(sheet);
}



function writeSignalDetailSheet_(sheet, rows) {
  var header = [[
    'date',
    'theme',
    'level1_bucket',
    'signal_code',
    'signal_name',
    'signal_value',
    'signal_score',
    'signal_direction',
    'signal_strength',
    'signal_text',
    'action_hint',
    'avoid_hint',
    'source_metric',
    'cadence',
    'sort_order'
  ]];

  sheet.clearContents();
  sheet.clearFormats();
  sheet.getRange(1, 1, 1, header[0].length).setValues(header);

  if (rows.length > 0) {
    sheet.getRange(2, 1, rows.length, rows[0].length).setValues(rows);
    sheet.getRange(2, 1, rows.length, 1).setNumberFormat('yyyy-mm-dd');
    sheet.getRange(2, 7, rows.length, 1).setNumberFormat('0');
    sheet.getRange(2, 15, rows.length, 1).setNumberFormat('0');
  }

  formatSignalSheet_(sheet);
}


function readMetricsRowsV2_(sheet) {
  var values = sheet.getDataRange().getValues();
  if (values.length < 2) return [];

  var header = values[0];
  var idx = buildHeaderIndex_(header);

  var rows = [];
  for (var i = 1; i < values.length; i++) {
    var r = values[i];
    var dateObj = normalizeSheetDate_(r[requireColumn_(idx, 'date')]);
    if (!dateObj || isNaN(dateObj.getTime())) continue;

    rows.push({
      dateObj: dateObj,
      dateKey: formatDateKey_(dateObj),

      gov_10y: readMetricNum_(r, idx, 'gov_10y'),
      gov_slope_10_1: readMetricNum_(r, idx, 'gov_slope_10_1'),
      gov_slope_30_10: readMetricNum_(r, idx, 'gov_slope_30_10'),

      spread_cdb_gov_10y: readMetricNum_(r, idx, 'spread_cdb_gov_10y'),
      spread_cdb_gov_10y_pct250: readMetricNum_(r, idx, 'spread_cdb_gov_10y_pct250'),

      spread_local_gov_gov_5y: readMetricNum_(r, idx, 'spread_local_gov_gov_5y'),
      spread_local_gov_gov_10y: readMetricNum_(r, idx, 'spread_local_gov_gov_10y'),

      spread_dr007_omo_7d: readMetricNum_(r, idx, 'spread_dr007_omo_7d'),
      spread_lpr_1y_mlf_1y: readMetricNum_(r, idx, 'spread_lpr_1y_mlf_1y'),
      spread_lpr_5y_gov_5y: readMetricNum_(r, idx, 'spread_lpr_5y_gov_5y'),
      spread_lpr_5y_ncd_1y: readMetricNum_(r, idx, 'spread_lpr_5y_ncd_1y'),
      spread_lpr5y_gov5y: readMetricNum_(r, idx, 'spread_lpr5y_gov5y'),
      spread_ncd_1y_mlf_1y: readMetricNum_(r, idx, 'spread_ncd_1y_mlf_1y'),
      spread_ncd_mlf_1y: readMetricNum_(r, idx, 'spread_ncd_mlf_1y'),
      spread_lgfv_vs_high_grade_credit_1y: readMetricNum_(r, idx, 'spread_lgfv_vs_high_grade_credit_1y'),
      spread_bank_bond_vs_high_grade_credit_1y: readMetricNum_(r, idx, 'spread_bank_bond_vs_high_grade_credit_1y'),
      spread_bank_bond_vs_high_grade_credit_3y: readMetricNum_(r, idx, 'spread_bank_bond_vs_high_grade_credit_3y'),
      spread_bank_bond_vs_high_grade_credit_5y: readMetricNum_(r, idx, 'spread_bank_bond_vs_high_grade_credit_5y'),

      ust_2y: readMetricNum_(r, idx, 'ust_2y'),
      ust_10y: readMetricNum_(r, idx, 'ust_10y'),
      usd_broad: readMetricNum_(r, idx, 'usd_broad'),
      usd_cny: readMetricNum_(r, idx, 'usd_cny'),
      gold: readMetricNum_(r, idx, 'gold'),
      wti: readMetricNum_(r, idx, 'wti'),
      brent: readMetricNum_(r, idx, 'brent'),
      copper: readMetricNum_(r, idx, 'copper'),
      vix: readMetricNum_(r, idx, 'vix'),
      spx: readMetricNum_(r, idx, 'spx'),
      nasdaq_100: readMetricNum_(r, idx, 'nasdaq_100'),

      cn_us_10y_spread: readMetricNum_(r, idx, 'cn_us_10y_spread'),
      cn_us_2y_spread: readMetricNum_(r, idx, 'cn_us_2y_spread'),
      usd_broad_ma20: readMetricNum_(r, idx, 'usd_broad_ma20'),
      usd_cny_ma20: readMetricNum_(r, idx, 'usd_cny_ma20'),
      gold_ma20: readMetricNum_(r, idx, 'gold_ma20'),
      ust_10y_pct250: readMetricNum_(r, idx, 'ust_10y_pct250'),
      usd_cny_pct250: readMetricNum_(r, idx, 'usd_cny_pct250'),

      spread_aaa_credit_gov_5y: readMetricNum_(r, idx, 'spread_aaa_credit_gov_5y'),
      spread_aaa_credit_gov_5y_pct250: readMetricNum_(r, idx, 'spread_aaa_credit_gov_5y_pct250'),

      spread_aa_plus_vs_aaa_credit_1y: readMetricNum_(r, idx, 'spread_aa_plus_vs_aaa_credit_1y'),
      spread_aa_plus_vs_aaa_credit_1y_pct250: readMetricNum_(r, idx, 'spread_aa_plus_vs_aaa_credit_1y_pct250'),

      spread_aaa_mtn_vs_aaa_plus_mtn_1y: readMetricNum_(r, idx, 'spread_aaa_mtn_vs_aaa_plus_mtn_1y'),

      spread_aaa_credit_ncd_1y: readMetricNum_(r, idx, 'spread_aaa_credit_ncd_1y'),
      aaa_ncd_1y: readMetricNum_(r, idx, 'aaa_ncd_1y'),
      aaa_ncd_1y_pct250: readMetricNum_(r, idx, 'aaa_ncd_1y_pct250'),

      gov_10y_pct250: readMetricNum_(r, idx, 'gov_10y_pct250')
    });
  }

  return rows;
}

function readMetricNum_(row, idx, colName) {
  var key = normalizeHeader_(colName);
  if (!(key in idx)) return null;
  return toNumberOrNull_(row[idx[key]]);
}

function readMoneyMarketDr007Map_(sheet) {
  var values = sheet.getDataRange().getValues();
  if (values.length < 2) return {};

  var header = values[0];
  var idx = buildHeaderIndex_(header);
  var dateCol = idx['date'];
  var dr007Col = idx['dr007_weightedrate'];

  if (dateCol == null || dr007Col == null) return {};

  var map = {};
  for (var i = 1; i < values.length; i++) {
    var r = values[i];
    var dateObj = normalizeLooseDate_(r[dateCol]);
    var dr007 = toNumberOrNull_(r[dr007Col]);
    if (!dateObj || !isFiniteNumber_(dr007)) continue;
    map[formatDateKey_(dateObj)] = dr007;
  }
  return map;
}

function classifyLiquidityRegime_(dr007) {
  if (!isFiniteNumber_(dr007)) {
    return makeSignalResult_('资金与流动性：未知', 'unknown', 0, 'unknown', 'low', 'DR007 缺失，暂无法判断资金与流动性环境');
  }
  if (dr007 >= SIGNAL_THRESHOLDS.funding_tight) {
    return makeSignalResult_('资金与流动性：偏紧', 'tight', -1, 'tight', 'medium', 'DR007 偏高，资金与流动性环境对杠杆和短端负债不利');
  }
  if (dr007 <= SIGNAL_THRESHOLDS.funding_loose) {
    return makeSignalResult_('资金与流动性：偏松', 'loose', 1, 'loose', 'medium', 'DR007 偏低，资金与流动性环境对久期和票息策略更友好');
  }
  return makeSignalResult_('资金与流动性：中性', 'neutral', 0, 'neutral', 'low', 'DR007 处于中性区间');
}

function classifyDurationSignal_(row) {
  var pct = row.gov_10y_pct250;
  var slope = row.gov_slope_10_1;

  if (!isFiniteNumber_(pct)) {
    return makeSignalResult_('利率债久期：中性', 'neutral', 0, 'neutral', 'low', '10Y 分位不足，利率债久期保持中性');
  }

  if (pct >= SIGNAL_THRESHOLDS.duration_pct_high) {
    if (isFiniteNumber_(slope) && slope <= SIGNAL_THRESHOLDS.curve_10_1_flat) {
      return makeSignalResult_('利率债久期：偏长', 'long', 2, 'long', 'high', '10Y 利率高分位且曲线偏平，利率债长久期性价比更高');
    }
    return makeSignalResult_('利率债久期：中性偏长', 'long_mild', 1, 'long', 'medium', '10Y 利率偏高，可适度拉长利率债久期');
  }

  if (pct <= SIGNAL_THRESHOLDS.duration_pct_low) {
    return makeSignalResult_('利率债久期：缩短', 'shorter', -2, 'shorter', 'high', '10Y 利率低分位，利率债久期宜缩短');
  }

  return makeSignalResult_('利率债久期：中性', 'neutral', 0, 'neutral', 'low', '10Y 利率处于中间区域，利率债久期维持中性');
}

function classifyUltraLongSignal_(row) {
  var slope = row.gov_slope_30_10;
  if (!isFiniteNumber_(slope)) {
    return makeSignalResult_('利率债超长端：中性', 'neutral', 0, 'neutral', 'low', '30Y-10Y 缺失，利率债超长端保持中性');
  }

  if (slope >= SIGNAL_THRESHOLDS.ultra_long_slope_high) {
    return makeSignalResult_('利率债超长端：超配', 'overweight', 1, 'ultra_long', 'medium', '30Y-10Y 偏陡，利率债超长端弹性更好');
  }
  if (slope <= SIGNAL_THRESHOLDS.ultra_long_slope_low) {
    return makeSignalResult_('利率债超长端：低配', 'underweight', -1, 'avoid_ultra_long', 'medium', '30Y-10Y 偏平，利率债超长端赔率下降');
  }
  return makeSignalResult_('利率债超长端：中性', 'neutral', 0, 'neutral', 'low', '30Y-10Y 处于中性区间，利率债超长端无明显优势');
}


function classifyRatesStrategyTilt_(ratesDuration, ratesUltraLong) {
  if (ratesDuration.value === 'long' && ratesUltraLong.value === 'overweight') {
    return makeSignalResult_('利率债策略：拉长久期，超长端可超配', 'long_with_ultra_long', 2, 'long_ultra_long', 'high', '整体利率债可拉长久期，且超长端相对更有弹性');
  }
  if (ratesDuration.value === 'long' && ratesUltraLong.value === 'underweight') {
    return makeSignalResult_('利率债策略：拉长久期，但不做超长端', 'long_without_ultra_long', 1, 'long_no_ultra_long', 'medium', '整体仍偏长久期，但不建议把久期主要放在超长端');
  }
  if (ratesDuration.value === 'long_mild' && ratesUltraLong.value === 'overweight') {
    return makeSignalResult_('利率债策略：适度拉长，超长端可超配', 'mild_long_with_ultra_long', 1, 'mild_long_ultra_long', 'medium', '利率债可适度拉长，结构上可向超长端倾斜');
  }
  if (ratesDuration.value === 'long_mild' && ratesUltraLong.value === 'underweight') {
    return makeSignalResult_('利率债策略：适度拉长，但不做超长端', 'mild_long_without_ultra_long', 1, 'mild_long_no_ultra_long', 'medium', '利率债久期可适度拉长，但以中长端替代超长端更稳妥');
  }
  if (ratesDuration.value === 'shorter' && ratesUltraLong.value === 'overweight') {
    return makeSignalResult_('利率债策略：整体缩短，保留少量超长端弹性', 'shorter_with_tail', -1, 'shorter_with_tail', 'medium', '组合整体宜缩短久期，如需保留弹性可少量配置超长端');
  }
  if (ratesDuration.value === 'shorter' && ratesUltraLong.value === 'underweight') {
    return makeSignalResult_('利率债策略：缩短久期，超长端低配', 'shorter_without_ultra_long', -2, 'defensive', 'high', '整体久期宜缩短，且不建议在超长端承担过多波动');
  }
  if (ratesDuration.value === 'shorter') {
    return makeSignalResult_('利率债策略：缩短久期', 'shorter', -1, 'shorter', 'medium', '利率债整体以缩短久期为主');
  }
  if (ratesUltraLong.value === 'overweight') {
    return makeSignalResult_('利率债策略：久期中性，超长端可超配', 'neutral_with_ultra_long', 1, 'neutral_ultra_long', 'medium', '整体久期中性，但超长端相对更有吸引力');
  }
  if (ratesUltraLong.value === 'underweight') {
    return makeSignalResult_('利率债策略：久期中性，超长端低配', 'neutral_without_ultra_long', -1, 'neutral_no_ultra_long', 'medium', '整体久期中性，但不建议在超长端承担过多仓位');
  }
  return makeSignalResult_('利率债策略：中性', 'neutral', 0, 'neutral', 'low', '久期与超长端均未给出明确方向');
}

function classifyCurveSignal_(row) {
  var slope = row.gov_slope_10_1;
  if (!isFiniteNumber_(slope)) {
    return makeSignalResult_('利率债曲线：未知', 'unknown', 0, 'unknown', 'low', '10Y-1Y 缺失，无法判断利率债曲线形态');
  }
  if (slope <= SIGNAL_THRESHOLDS.curve_10_1_flat) {
    return makeSignalResult_('利率债曲线：偏平', 'flat', 1, 'long_end', 'medium', '10Y-1Y 偏低，利率债长端相对更受益');
  }
  if (slope >= SIGNAL_THRESHOLDS.curve_10_1_steep) {
    return makeSignalResult_('利率债曲线：偏陡', 'steep', -1, 'front_mid_end', 'medium', '10Y-1Y 偏高，利率债短中端相对更稳');
  }
  return makeSignalResult_('利率债曲线：中性', 'neutral', 0, 'neutral', 'low', '10Y-1Y 处于中性区间');
}

function classifyPolicyBankSignal_(row) {
  var pct = row.spread_cdb_gov_10y_pct250;
  var val = row.spread_cdb_gov_10y;
  if (!isFiniteNumber_(val) || !isFiniteNumber_(pct)) {
    return makeSignalResult_('利率债相对价值：国开债 vs 国债：中性', 'neutral', 0, 'neutral', 'low', '国开-国债利差数据不足');
  }

  if (pct >= SIGNAL_THRESHOLDS.policy_spread_high) {
    return makeSignalResult_('利率债相对价值：国开债占优', 'policy_bank_over_gov', 1, 'policy_bank', 'medium', '国开-国债利差高位，国开债相对更便宜');
  }
  if (pct <= SIGNAL_THRESHOLDS.policy_spread_low) {
    return makeSignalResult_('利率债相对价值：国债占优', 'gov_over_policy_bank', -1, 'gov', 'medium', '国开-国债利差低位，国债相对更稳妥');
  }
  return makeSignalResult_('利率债相对价值：国开债 vs 国债：中性', 'neutral', 0, 'neutral', 'low', '国开-国债利差处于中性区间');
}

function classifyLocalGovSignal_(row) {
  var v = row.spread_local_gov_gov_10y;
  if (!isFiniteNumber_(v)) {
    return makeSignalResult_('利率债相对价值：地方债 vs 国债：中性', 'neutral', 0, 'neutral', 'low', '地方债数据不足或历史较短');
  }
  if (v >= 0.20) {
    return makeSignalResult_('利率债相对价值：地方债占优', 'local_gov_cheap', 1, 'local_gov', 'medium', '地方债-国债利差偏高，地方债配置价值改善');
  }
  if (v <= 0.08) {
    return makeSignalResult_('利率债相对价值：地方债偏贵', 'local_gov_rich', -1, 'gov', 'medium', '地方债-国债利差偏低，地方债性价比一般');
  }
  return makeSignalResult_('利率债相对价值：地方债 vs 国债：中性', 'neutral', 0, 'neutral', 'low', '地方债-国债利差处于中性区间');
}

function classifyRatesRvRanking_(ratesPolicyBank, ratesLocalGov) {
  var items = [
    { name: '国开债', score: ratesPolicyBank && isFiniteNumber_(ratesPolicyBank.score) ? ratesPolicyBank.score : 0 },
    { name: '国债', score: 0 },
    { name: '地方债', score: ratesLocalGov && isFiniteNumber_(ratesLocalGov.score) ? ratesLocalGov.score : 0 }
  ];

  items.sort(function(a, b) {
    if (b.score !== a.score) return b.score - a.score;
    return tieBreakRateRvOrder_(a.name) - tieBreakRateRvOrder_(b.name);
  });

  var groups = [];
  for (var i = 0; i < items.length; i++) {
    if (!groups.length || groups[groups.length - 1].score !== items[i].score) {
      groups.push({ score: items[i].score, names: [items[i].name] });
    } else {
      groups[groups.length - 1].names.push(items[i].name);
    }
  }

  var ranking = [];
  for (var j = 0; j < groups.length; j++) {
    ranking.push(groups[j].names.join(' ≈ '));
  }

  var rankingText = ranking.join(' > ');
  var comment = [
    '基于“国开债 vs 国债”和“地方债 vs 国债”两条原子信号汇总得到的利率债相对价值粗排序',
    ratesPolicyBank ? ratesPolicyBank.comment : '',
    ratesLocalGov ? ratesLocalGov.comment : ''
  ].filter(function(x) { return !!x; }).join('；');

  return makeSignalResult_(rankingText, 'ranking', 0, 'ranking', 'medium', comment);
}

function tieBreakRateRvOrder_(name) {
  if (name === '国开债') return 1;
  if (name === '国债') return 2;
  if (name === '地方债') return 3;
  return 99;
}

function classifyHighGradeCreditSignal_(row) {
  var pct = row.spread_aaa_credit_gov_5y_pct250;
  var v = row.spread_aaa_credit_gov_5y;
  if (!isFiniteNumber_(v) || !isFiniteNumber_(pct)) {
    return makeSignalResult_('信用债资质：中性', 'neutral', 0, 'neutral', 'low', '高等级信用利差数据不足');
  }

  if (pct >= SIGNAL_THRESHOLDS.credit_spread_high) {
    return makeSignalResult_('信用债资质：高等级占优', 'high_grade_favored', 1, 'high_grade', 'medium', 'AAA 信用利差高位，高等级信用性价比改善');
  }
  if (pct <= SIGNAL_THRESHOLDS.credit_spread_low) {
    return makeSignalResult_('信用债资质：低等级相对占优', 'high_grade_rich', -1, 'avoid_high_grade_chasing', 'medium', 'AAA 信用利差低位，继续追高等级的赔率一般');
  }
  return makeSignalResult_('信用债资质：中性', 'neutral', 0, 'neutral', 'low', 'AAA 信用利差处于中性区间');
}

function classifyCreditSinkSignal_(row) {
  var pct = row.spread_aa_plus_vs_aaa_credit_1y_pct250;
  var v = row.spread_aa_plus_vs_aaa_credit_1y;
  if (!isFiniteNumber_(v) || !isFiniteNumber_(pct)) {
    return makeSignalResult_('信用债下沉：中性', 'neutral', 0, 'neutral', 'low', '信用下沉利差数据不足');
  }

  if (pct >= SIGNAL_THRESHOLDS.sink_spread_high) {
    return makeSignalResult_('信用债下沉：不宜下沉', 'avoid_sink', -1, 'avoid_sink', 'medium', 'AA+-AAA(1Y) 利差高位，信用下沉风险偏高');
  }
  if (pct <= SIGNAL_THRESHOLDS.sink_spread_low) {
    return makeSignalResult_('信用债下沉：可适度下沉', 'can_sink', 1, 'sink', 'medium', 'AA+-AAA(1Y) 利差低位，信用下沉环境改善');
  }
  return makeSignalResult_('信用债下沉：中性', 'neutral', 0, 'neutral', 'low', 'AA+-AAA(1Y) 利差处于中性区间');
}

function classifyMtnTierSignal_(row) {
  var v = row.spread_aaa_mtn_vs_aaa_plus_mtn_1y;
  if (!isFiniteNumber_(v)) {
    return makeSignalResult_('信用债相对价值：AAA中票 vs AAA+中票：未知', 'unknown', 0, 'unknown', 'low', 'AAA/AAA+ 中票 1Y 利差缺失');
  }
  if (v >= 0.08) {
    return makeSignalResult_('信用债相对价值：AAA中票占优', 'aaa_mtn_cheaper', 1, 'aaa_mtn', 'medium', 'AAA 中票相对 AAA+ 中票利差偏高');
  }
  if (v <= 0.03) {
    return makeSignalResult_('信用债相对价值：AAA+中票占优', 'aaa_plus_mtn_steadier', -1, 'aaa_plus_mtn', 'medium', 'AAA/AAA+ 中票利差偏低，更偏基准化配置');
  }
  return makeSignalResult_('信用债相对价值：AAA中票 vs AAA+中票：中性', 'neutral', 0, 'neutral', 'low', 'AAA/AAA+ 中票利差处于中性区间');
}

function classifyNcdSignal_(row, dr007) {
  var ncdPct = row.aaa_ncd_1y_pct250;
  var creditNcdSpread = row.spread_aaa_credit_ncd_1y;
  if (!isFiniteNumber_(ncdPct) || !isFiniteNumber_(creditNcdSpread)) {
    return makeSignalResult_('短端票息资产：中性', 'neutral', 0, 'neutral', 'low', '存单/信用利差数据不足');
  }

  if (ncdPct >= SIGNAL_THRESHOLDS.ncd_pct_high && creditNcdSpread >= 0.20) {
    return makeSignalResult_('短端票息资产：高等级信用占优', 'short_credit_over_ncd', 1, 'short_credit', 'medium', '存单高位且信用-存单利差较高，短端高等级信用性价比提升');
  }
  if (ncdPct <= SIGNAL_THRESHOLDS.ncd_pct_low && creditNcdSpread <= 0.12) {
    return makeSignalResult_('短端票息资产：赔率偏低', 'low_edge', -1, 'cash_like', 'medium', '存单低位且信用-存单利差偏窄，短端赔率一般');
  }
  if (isFiniteNumber_(dr007) && dr007 >= SIGNAL_THRESHOLDS.funding_tight) {
    return makeSignalResult_('短端票息资产：关注资金扰动', 'funding_watch', -1, 'watch_funding', 'medium', '资金偏紧，短端信用需关注负债端压力');
  }
  return makeSignalResult_('短端票息资产：中性', 'neutral', 0, 'neutral', 'low', '存单与短端信用关系中性');
}

function classifyCreditStrategyTilt_(creditQuality, creditSink) {
  if (creditQuality.value === 'high_grade_favored' && creditSink.value === 'avoid_sink') {
    return makeSignalResult_('信用债策略：高等级优先，不宜下沉', 'high_grade_no_sink', -1, 'high_grade_defensive', 'high', '高等级更占优，同时不建议把仓位明显下沉到更低等级');
  }
  if (creditQuality.value === 'high_grade_favored' && creditSink.value === 'can_sink') {
    return makeSignalResult_('信用债策略：高等级优先，可适度下沉', 'high_grade_small_sink', 1, 'high_grade_with_small_sink', 'medium', '整体仍以高等级为主，但可少量下沉增强票息');
  }
  if (creditQuality.value === 'high_grade_favored') {
    return makeSignalResult_('信用债策略：高等级优先', 'high_grade_first', 1, 'high_grade', 'medium', '高等级信用利差更有吸引力，信用债以高等级配置为主');
  }
  if (creditSink.value === 'avoid_sink') {
    return makeSignalResult_('信用债策略：中高等级为主，不宜下沉', 'mid_high_no_sink', -1, 'defensive', 'medium', '下沉补偿不足或风险偏高，信用债以中高等级为主');
  }
  if (creditSink.value === 'can_sink') {
    return makeSignalResult_('信用债策略：中性，可适度下沉', 'neutral_small_sink', 1, 'constructive', 'medium', '整体信用环境中性，但可适度向下沉要票息');
  }
  return makeSignalResult_('信用债策略：中性', 'neutral', 0, 'neutral', 'low', '资质与下沉信号均未给出明确偏向');
}

function classifyMortgageBackgroundSignal_(row) {
  var lpr5Gov5 = firstFiniteNumber_([row.spread_lpr5y_gov5y, row.spread_lpr_5y_gov_5y]);
  var lpr5Ncd1 = row.spread_lpr_5y_ncd_1y;
  var lpr1Mlf1 = row.spread_lpr_1y_mlf_1y;
  var ncdMlf1 = firstFiniteNumber_([row.spread_ncd_mlf_1y, row.spread_ncd_1y_mlf_1y]);
  var dr007Omo = row.spread_dr007_omo_7d;

  if (!hasEnoughFiniteNumbers_([lpr5Gov5, lpr5Ncd1, lpr1Mlf1], 2)) {
    return makeSignalResult_('房贷背景：未知', 'unknown', 0, 'unknown', 'low', '房贷相关利差数据不足，暂无法判断房贷背景');
  }

  var fundingTight = (isFiniteNumber_(ncdMlf1) && ncdMlf1 >= 0.35) || (isFiniteNumber_(dr007Omo) && dr007Omo >= 0.20);
  var fundingLoose = (!isFiniteNumber_(ncdMlf1) || ncdMlf1 <= 0.12) && (!isFiniteNumber_(dr007Omo) || dr007Omo <= 0.08);

  var roomStrong = isFiniteNumber_(lpr5Gov5) && lpr5Gov5 >= 1.70 && isFiniteNumber_(lpr5Ncd1) && lpr5Ncd1 >= 1.80;
  var roomOkay = isFiniteNumber_(lpr5Gov5) && lpr5Gov5 >= 1.40 && isFiniteNumber_(lpr5Ncd1) && lpr5Ncd1 >= 1.50;
  var roomLimited = (isFiniteNumber_(lpr5Gov5) && lpr5Gov5 <= 1.10) ||
    (isFiniteNumber_(lpr5Ncd1) && lpr5Ncd1 <= 1.20) ||
    (isFiniteNumber_(lpr1Mlf1) && lpr1Mlf1 <= 0.35);

  if (roomStrong && !fundingTight) {
    return makeSignalResult_('房贷背景：下行背景增强', 'easing_bias', 2, 'easing', 'high', '5Y LPR 与国债/存单利差仍较宽，且银行负债端未明显偏紧，后续房贷利率下行背景增强；可关注重定价窗口，但不等同于房价立刻上涨');
  }
  if ((roomOkay && fundingLoose) || (roomOkay && !fundingTight && isFiniteNumber_(lpr1Mlf1) && lpr1Mlf1 >= 0.55)) {
    return makeSignalResult_('房贷背景：偏友好，可关注重定价窗口', 'watch_reset_window', 1, 'mild_easing', 'medium', '贷款报价与市场利率之间仍有一定缓冲，且银行负债端压力不大，可关注后续重定价或按揭利率优化窗口');
  }
  if (fundingTight && !roomStrong) {
    return makeSignalResult_('房贷背景：银行负债端偏紧', 'funding_tight', -1, 'tight', 'medium', '存单或资金利率相对政策利率偏高，银行负债端偏紧，房贷利率继续明显下行的约束更大');
  }
  if (roomLimited) {
    return makeSignalResult_('房贷背景：下调空间有限', 'limited_room', 0, 'neutral', 'medium', 'LPR 与国债/存单/MLF 的利差已不算宽，房贷利率进一步下调空间相对有限');
  }
  return makeSignalResult_('房贷背景：中性', 'neutral', 0, 'neutral', 'low', '房贷相关利差处于中间区域，可继续观察政策利率、银行负债成本与重定价窗口');
}

function classifyHousingBackgroundSignal_(row) {
  var localAvg = meanFiniteNumbers_([row.spread_local_gov_gov_5y, row.spread_local_gov_gov_10y]);
  var lgfvSpread = row.spread_lgfv_vs_high_grade_credit_1y;
  var bankAvg = meanFiniteNumbers_([
    row.spread_bank_bond_vs_high_grade_credit_1y,
    row.spread_bank_bond_vs_high_grade_credit_3y,
    row.spread_bank_bond_vs_high_grade_credit_5y
  ]);
  var ncdMlf1 = firstFiniteNumber_([row.spread_ncd_mlf_1y, row.spread_ncd_1y_mlf_1y]);
  var lpr5Gov5 = firstFiniteNumber_([row.spread_lpr5y_gov5y, row.spread_lpr_5y_gov_5y]);

  if (!hasEnoughFiniteNumbers_([localAvg, lgfvSpread, bankAvg, ncdMlf1, lpr5Gov5], 3)) {
    return makeSignalResult_('住房背景：未知', 'unknown', 0, 'unknown', 'low', '住房相关融资利差数据不足，暂无法判断住房背景');
  }

  var warmScore = 0;
  var notes = [];

  if (isFiniteNumber_(localAvg)) {
    if (localAvg <= 0.10) {
      warmScore += 1;
      notes.push('地方债-国债利差偏低，地方财政融资压力相对可控');
    } else if (localAvg >= 0.18) {
      warmScore -= 1;
      notes.push('地方债-国债利差偏高，地方财政与项目端压力仍在');
    }
  }

  if (isFiniteNumber_(lgfvSpread)) {
    if (lgfvSpread <= 0.15) {
      warmScore += 1;
      notes.push('城投相对高等级信用利差不高，地产链相关融资环境边际更稳');
    } else if (lgfvSpread >= 0.30) {
      warmScore -= 1;
      notes.push('城投相对高等级信用利差偏高，地产链相关融资环境仍偏谨慎');
    }
  }

  if (isFiniteNumber_(bankAvg)) {
    if (bankAvg <= 0.02) {
      warmScore += 1;
      notes.push('银行债相对高等级信用利差较低，银行体系融资环境较顺');
    } else if (bankAvg >= 0.12) {
      warmScore -= 1;
      notes.push('银行债相对高等级信用利差偏高，银行信用投放环境仍偏保守');
    }
  }

  if (isFiniteNumber_(ncdMlf1)) {
    if (ncdMlf1 <= 0.10) {
      warmScore += 1;
      notes.push('存单相对 MLF 利差不高，银行负债端压力较轻');
    } else if (ncdMlf1 >= 0.30) {
      warmScore -= 1;
      notes.push('存单相对 MLF 利差偏高，银行负债端仍有掣肘');
    }
  }

  if (isFiniteNumber_(lpr5Gov5)) {
    if (lpr5Gov5 <= 1.40) {
      warmScore += 1;
      notes.push('5Y LPR 相对 5Y 国债利差已不高，居民按揭利率背景相对友好');
    } else if (lpr5Gov5 >= 1.90) {
      warmScore -= 1;
      notes.push('5Y LPR 相对 5Y 国债利差仍高，住房融资成本背景仍偏冷');
    }
  }

  var tail = '仅反映住房融资与利率背景，不等同于房价会马上上涨或下跌';
  if (warmScore >= 3) {
    return makeSignalResult_('住房背景：偏暖', 'warm', 2, 'warm', 'high', notes.concat([tail]).join('；'));
  }
  if (warmScore >= 1) {
    return makeSignalResult_('住房背景：中性偏暖', 'mild_warm', 1, 'mild_warm', 'medium', notes.concat([tail]).join('；'));
  }
  if (warmScore <= -3) {
    return makeSignalResult_('住房背景：偏冷', 'cold', -2, 'cold', 'high', notes.concat([tail]).join('；'));
  }
  if (warmScore <= -1) {
    return makeSignalResult_('住房背景：中性偏冷', 'mild_cold', -1, 'mild_cold', 'medium', notes.concat([tail]).join('；'));
  }
  return makeSignalResult_('住房背景：中性', 'neutral', 0, 'neutral', 'low', (notes.length ? notes.join('；') + '；' : '') + tail);
}


function classifyFxBackgroundSignal_(row) {
  var spread10 = row.cn_us_10y_spread;
  var spread2 = row.cn_us_2y_spread;
  var fx = row.usd_cny;
  var fxMa20 = row.usd_cny_ma20;
  var fxPct = row.usd_cny_pct250;

  if (!hasEnoughFiniteNumbers_([spread10, spread2, fx, fxMa20, fxPct], 3)) {
    return makeSignalResult_('汇率背景：未知', 'unknown', 0, 'unknown', 'low', '中美利差或人民币汇率数据不足，暂无法判断人民币背景');
  }

  var score = 0;
  var notes = [];

  if (isFiniteNumber_(spread10)) {
    if (spread10 <= -1.60) { score += 1; notes.push('中美10Y利差偏负，人民币外部利差压力偏大'); }
    else if (spread10 >= -0.60) { score -= 1; notes.push('中美10Y利差压力相对收敛'); }
  }

  if (isFiniteNumber_(spread2)) {
    if (spread2 <= -2.00) { score += 1; notes.push('中美2Y利差偏负，短端利差对人民币仍有约束'); }
    else if (spread2 >= -1.00) { score -= 1; notes.push('中美2Y利差压力相对缓和'); }
  }

  if (isFiniteNumber_(fx) && isFiniteNumber_(fxMa20)) {
    if (fx > fxMa20 * 1.005) { score += 1; notes.push('USD/CNY 高于20日均值，人民币短期偏弱'); }
    else if (fx < fxMa20 * 0.995) { score -= 1; notes.push('USD/CNY 低于20日均值，人民币短期偏稳'); }
  }

  if (isFiniteNumber_(fxPct)) {
    if (fxPct >= 0.70) { score += 1; notes.push('USD/CNY 处在偏高分位，人民币偏弱背景增强'); }
    else if (fxPct <= 0.35) { score -= 1; notes.push('USD/CNY 处在偏低分位，人民币偏稳背景增强'); }
  }

  if (score >= 2) {
    return makeSignalResult_('汇率背景：人民币偏弱', 'rmb_weak', -1, 'rmb_weak', 'medium', notes.join('；'));
  }
  if (score <= -2) {
    return makeSignalResult_('汇率背景：人民币偏稳', 'rmb_stable', 1, 'rmb_stable', 'medium', notes.join('；'));
  }
  return makeSignalResult_('汇率背景：中性', 'neutral', 0, 'neutral', 'low', notes.length ? notes.join('；') : '人民币汇率与中美利差处于中间区域');
}

function classifyUsdAllocationBackgroundSignal_(row) {
  var usd = row.usd_broad;
  var usdMa20 = row.usd_broad_ma20;
  var spread10 = row.cn_us_10y_spread;
  var spread2 = row.cn_us_2y_spread;
  var fxPct = row.usd_cny_pct250;

  if (!hasEnoughFiniteNumbers_([usd, usdMa20, spread10, spread2, fxPct], 3)) {
    return makeSignalResult_('美元配置背景：未知', 'unknown', 0, 'unknown', 'low', '美元指数或中美利差数据不足，暂无法判断美元配置背景');
  }

  var score = 0;
  var notes = [];

  if (isFiniteNumber_(usd) && isFiniteNumber_(usdMa20)) {
    if (usd >= usdMa20 * 1.01) { score += 1; notes.push('美元 broad 指数高于20日均值'); }
    else if (usd <= usdMa20 * 0.99) { score -= 1; notes.push('美元 broad 指数低于20日均值'); }
  }

  if ((isFiniteNumber_(spread10) && spread10 <= -1.60) || (isFiniteNumber_(spread2) && spread2 <= -2.00)) {
    score += 1;
    notes.push('中美利差对美元相对有利');
  } else if ((isFiniteNumber_(spread10) && spread10 >= -0.60) && (isFiniteNumber_(spread2) && spread2 >= -1.00)) {
    score -= 1;
    notes.push('中美利差压力阶段性缓和');
  }

  if (isFiniteNumber_(fxPct)) {
    if (fxPct >= 0.70) { score += 1; notes.push('人民币偏弱背景下，美元配置的对冲价值更高'); }
    else if (fxPct <= 0.35) { score -= 1; notes.push('人民币偏稳时，美元配置的必要性下降'); }
  }

  if (score >= 2) {
    return makeSignalResult_('美元配置背景：偏强，可保留一定对冲', 'usd_keep_hedge', 1, 'keep_usd_hedge', 'medium', notes.join('；'));
  }
  if (score <= -2) {
    return makeSignalResult_('美元配置背景：偏弱，无需明显增加', 'usd_not_urgent', -1, 'no_need_add_usd', 'medium', notes.join('；'));
  }
  return makeSignalResult_('美元配置背景：中性', 'neutral', 0, 'neutral', 'low', notes.length ? notes.join('；') : '美元配置环境处于中间区域');
}

function classifyGoldBackgroundSignal_(row) {
  var gold = row.gold;
  var goldMa20 = row.gold_ma20;
  var fxPct = row.usd_cny_pct250;
  var spread10 = row.cn_us_10y_spread;
  var spread2 = row.cn_us_2y_spread;
  var vix = row.vix;

  if (!hasEnoughFiniteNumbers_([gold, goldMa20, fxPct, spread10, spread2], 3)) {
    return makeSignalResult_('黄金背景：未知', 'unknown', 0, 'unknown', 'low', '黄金、汇率或中美利差数据不足，暂无法判断黄金背景');
  }

  var score = 0;
  var notes = [];

  if (isFiniteNumber_(gold) && isFiniteNumber_(goldMa20)) {
    if (gold >= goldMa20 * 1.01) { score += 1; notes.push('黄金高于20日均值'); }
    else if (gold <= goldMa20 * 0.99) { score -= 1; notes.push('黄金低于20日均值'); }
  }

  if (isFiniteNumber_(fxPct)) {
    if (fxPct >= 0.70) { score += 1; notes.push('人民币偏弱时，黄金的本币对冲属性更强'); }
    else if (fxPct <= 0.35) { score -= 1; notes.push('人民币偏稳时，黄金的本币对冲需求下降'); }
  }

  if ((isFiniteNumber_(spread10) && spread10 <= -1.60) || (isFiniteNumber_(spread2) && spread2 <= -2.00)) {
    score += 1;
    notes.push('中美利差偏负，黄金背景更受支撑');
  } else if ((isFiniteNumber_(spread10) && spread10 >= -0.60) && (isFiniteNumber_(spread2) && spread2 >= -1.00)) {
    score -= 1;
    notes.push('中美利差压力缓和，黄金的对冲吸引力下降');
  }

  if (isFiniteNumber_(vix)) {
    if (vix >= 22) { score += 1; notes.push('VIX 偏高，避险需求对黄金更友好'); }
    else if (vix <= 16) { score -= 1; notes.push('VIX 偏低，避险需求不强'); }
  }

  if (score >= 2) {
    return makeSignalResult_('黄金背景：偏强', 'gold_strong', 1, 'keep_gold_hedge', 'medium', notes.join('；'));
  }
  if (score <= -2) {
    return makeSignalResult_('黄金背景：偏弱', 'gold_soft', -1, 'no_need_add_gold', 'medium', notes.join('；'));
  }
  return makeSignalResult_('黄金背景：中性', 'neutral', 0, 'neutral', 'low', notes.length ? notes.join('；') : '黄金环境处于中间区域');
}

function classifyCommodityBackgroundSignal_(row) {
  var wti = row.wti;
  var brent = row.brent;
  var copper = row.copper;
  var vix = row.vix;
  var spx = row.spx;
  var ndx = row.nasdaq_100;

  if (!hasEnoughFiniteNumbers_([wti, brent, copper, vix], 2)) {
    return makeSignalResult_('顺周期商品背景：未知', 'unknown', 0, 'unknown', 'low', '油价、铜价或波动率数据不足，暂无法判断顺周期商品背景');
  }

  var score = 0;
  var notes = [];

  if (isFiniteNumber_(vix)) {
    if (vix <= 18) { score += 1; notes.push('VIX 偏低，全球风险偏好相对平稳'); }
    else if (vix >= 25) { score -= 1; notes.push('VIX 偏高，顺周期商品背景承压'); }
  }

  if (isFiniteNumber_(wti) && isFiniteNumber_(brent)) {
    if (wti >= 65 && brent >= 68) { score += 1; notes.push('油价维持在中高区间，商品需求预期不弱'); }
    else if (wti <= 55 || brent <= 58) { score -= 1; notes.push('油价偏弱，顺周期商品背景一般'); }
  }

  if (isFiniteNumber_(copper)) {
    if (copper >= 9000) { score += 1; notes.push('铜价处于相对较高水平，工业需求背景偏强'); }
    else if (copper <= 8000) { score -= 1; notes.push('铜价偏弱，工业景气背景一般'); }
  }

  if (isFiniteNumber_(spx) && isFiniteNumber_(ndx)) {
    notes.push('同时参考美股风险偏好，但该信号更偏背景观察，不等同于具体商品一定上涨');
  }

  if (score >= 2) {
    return makeSignalResult_('顺周期商品背景：偏强', 'pro_cyclical_strong', 1, 'pro_cyclical', 'medium', notes.join('；'));
  }
  if (score <= -2) {
    return makeSignalResult_('顺周期商品背景：偏弱', 'pro_cyclical_soft', -1, 'defensive', 'medium', notes.join('；'));
  }
  return makeSignalResult_('顺周期商品背景：中性', 'neutral', 0, 'neutral', 'low', notes.length ? notes.join('；') : '商品背景处于中间区域');
}


function classifyHouseholdAllocationSignal_(ratesStrategy, creditStrategy, mortgageBackground, housingBackground, fxBackground, usdAllocationBackground, goldBackground, commodityBackground, householdBuckets) {
  var stableScore = 0;
  var hedgeScore = 0;
  var riskScore = 0;

  if (ratesStrategy && (ratesStrategy.score >= 1)) stableScore += 1;
  if (creditStrategy && creditStrategy.value.indexOf('high_grade') >= 0) stableScore += 1;
  if (mortgageBackground && mortgageBackground.value === 'funding_tight') stableScore += 1;
  if (housingBackground && housingBackground.score <= -1) stableScore += 1;

  if (fxBackground && fxBackground.value === 'rmb_weak') hedgeScore += 1;
  if (usdAllocationBackground && usdAllocationBackground.score >= 1) hedgeScore += 1;
  if (goldBackground && goldBackground.score >= 1) hedgeScore += 1;

  if (commodityBackground && commodityBackground.score >= 1) riskScore += 1;
  if (housingBackground && housingBackground.score >= 1) riskScore += 1;
  if (fxBackground && fxBackground.value === 'rmb_stable') riskScore += 0.5;

  var bucketSummary = householdBuckets ? buildHouseholdBucketSummary_(householdBuckets) : '';

  if (stableScore >= 2 && hedgeScore >= 2) {
    return makeSignalResult_('家庭配置：稳健固收为主，同时保留对冲', 'stable_plus_hedge', 1, 'stable_plus_hedge', 'medium', bucketSummary || '稳健固收桶可略高于中性，同时保留一定美元/黄金对冲');
  }
  if (stableScore >= 2 && riskScore <= 0.5) {
    return makeSignalResult_('家庭配置：先把底仓放在稳健固收', 'stable_income_first', 1, 'stable_income', 'medium', bucketSummary || '当前更适合把配置重心放在稳健固收桶');
  }
  if (riskScore >= 2 && hedgeScore <= 1) {
    return makeSignalResult_('家庭配置：可均衡配置，风险桶中性偏高', 'balanced_with_risk', 1, 'balanced_risk', 'medium', bucketSummary || '风险资产桶可中性偏高，但仍应保留基本稳健固收底仓');
  }
  if (hedgeScore >= 2 && stableScore <= 1) {
    return makeSignalResult_('家庭配置：先稳住活钱与对冲', 'cash_and_hedge', 0, 'cash_hedge', 'medium', bucketSummary || '活钱桶与对冲保值桶可略高于中性，先降低组合脆弱性');
  }
  return makeSignalResult_('家庭配置：中性均衡', 'balanced', 0, 'balanced', 'low', bucketSummary || '五类资产桶以中性均衡配置为主，根据个人负债与现金流微调');
}

function buildHouseholdComment_(liquidity, ratesStrategy, creditStrategy, mortgageBackground, housingBackground, fxBackground, usdAllocationBackground, goldBackground, commodityBackground, alloc, householdAllocation, householdBuckets) {
  var parts = [];
  parts.push(householdAllocation.label);

  if (liquidity) parts.push(liquidity.label);
  if (ratesStrategy) parts.push(ratesStrategy.label);
  if (creditStrategy) parts.push(creditStrategy.label);
  if (mortgageBackground) parts.push(mortgageBackground.label);
  if (housingBackground) parts.push(housingBackground.label);
  if (fxBackground) parts.push(fxBackground.label);
  if (usdAllocationBackground) parts.push(usdAllocationBackground.label);
  if (goldBackground) parts.push(goldBackground.label);
  if (commodityBackground) parts.push(commodityBackground.label);

  if (householdBuckets) {
    parts.push(buildHouseholdBucketSummary_(householdBuckets));
  }

  parts.push(
    '内部固收引擎参考：长久期利率债 ' + alloc.alloc_rates_long + '%；中久期利率债 ' + alloc.alloc_rates_mid + '%；短久期利率债 ' + alloc.alloc_rates_short + '%；高等级信用债 ' + alloc.alloc_credit_high_grade + '%；票息增强/适度下沉 ' + alloc.alloc_credit_sink + '%；现金/类现金 ' + alloc.alloc_cash + '%'
  );
  parts.push('以上更偏背景判断，不是交易指令，仍需结合你的负债、现金流和风险承受力调整');
  return parts.join('；');
}

function buildHouseholdBuckets_(liquidity, ratesStrategy, creditStrategy, mortgageBackground, housingBackground, fxBackground, usdAllocationBackground, goldBackground, commodityBackground, alloc) {
  var base = HOUSEHOLD_BUCKET_BASELINE || {
    cash: 15,
    stable_fixed_income: 35,
    active_fixed_income: 20,
    hedge: 10,
    risk: 20
  };

  var plan = {
    bucket_cash: {
      title: '活钱桶',
      base_pct: base.cash,
      target_pct: base.cash,
      notes: ['用于应急、短期开销与随时可用资金']
    },
    bucket_stable_fixed_income: {
      title: '稳健固收桶',
      base_pct: base.stable_fixed_income,
      target_pct: base.stable_fixed_income,
      notes: ['承担组合底仓与波动缓冲']
    },
    bucket_active_fixed_income: {
      title: '进取固收桶',
      base_pct: base.active_fixed_income,
      target_pct: base.active_fixed_income,
      notes: ['用于拉久期、信用增强或波段弹性']
    },
    bucket_hedge: {
      title: '对冲保值桶',
      base_pct: base.hedge,
      target_pct: base.hedge,
      notes: ['用于汇率与黄金等防御配置']
    },
    bucket_risk: {
      title: '风险资产桶',
      base_pct: base.risk,
      target_pct: base.risk,
      notes: ['用于权益、商品或更高波动资产']
    }
  };

  if (liquidity && liquidity.value === 'tight') {
    applyBucketAdjust_(plan.bucket_cash, 6, '资金偏紧，活钱要更充足');
    applyBucketAdjust_(plan.bucket_active_fixed_income, -3, '资金偏紧时不宜把太多仓位放在更依赖负债和波动承受的方向');
    applyBucketAdjust_(plan.bucket_risk, -3, '资金偏紧时风险桶宜收一点');
  } else if (liquidity && liquidity.value === 'loose') {
    applyBucketAdjust_(plan.bucket_cash, -4, '资金偏松时可以少留一点闲置现金');
    applyBucketAdjust_(plan.bucket_stable_fixed_income, 2, '资金偏松对稳健固收更友好');
    applyBucketAdjust_(plan.bucket_active_fixed_income, 2, '资金偏松时固收增强策略更容易展开');
  }

  if (ratesStrategy && ratesStrategy.score >= 1) {
    applyBucketAdjust_(plan.bucket_stable_fixed_income, 4, '利率债环境偏友好，稳健固收桶可略高于中性');
    applyBucketAdjust_(plan.bucket_active_fixed_income, 4, '久期环境改善时，进取固收桶也可略增');
    applyBucketAdjust_(plan.bucket_cash, -3, '固收环境改善时不必留过多空闲现金');
    applyBucketAdjust_(plan.bucket_risk, -2, '可以把部分风险预算挪回固收');
  } else if (ratesStrategy && ratesStrategy.value.indexOf('shorter') >= 0) {
    applyBucketAdjust_(plan.bucket_cash, 3, '久期宜偏短时，活钱和短久期仓位可略高');
    applyBucketAdjust_(plan.bucket_stable_fixed_income, -1, '久期环境一般时，稳健固收也不必过度拉长');
    applyBucketAdjust_(plan.bucket_active_fixed_income, -4, '久期不利时，进取固收桶宜收一点');
    applyBucketAdjust_(plan.bucket_hedge, 1, '部分防御预算可转向对冲桶');
    applyBucketAdjust_(plan.bucket_risk, 1, '避免把全部调整都压回现金');
  }

  if (creditStrategy && creditStrategy.value.indexOf('high_grade') >= 0) {
    applyBucketAdjust_(plan.bucket_stable_fixed_income, 3, '高等级信用更有性价比时，稳健固收桶更适合作为承接');
    applyBucketAdjust_(plan.bucket_active_fixed_income, -1, '不必为了更高收益明显下沉');
  }
  if (creditStrategy && (creditStrategy.value === 'high_grade_small_sink' || creditStrategy.value === 'neutral_small_sink')) {
    applyBucketAdjust_(plan.bucket_active_fixed_income, 4, '信用环境允许时，进取固收桶可适度增加');
    applyBucketAdjust_(plan.bucket_cash, -2, '票息增强可替代一部分空置现金');
    applyBucketAdjust_(plan.bucket_stable_fixed_income, -2, '部分仓位可从稳健固收挪向进取固收');
  } else if (creditStrategy && creditStrategy.value.indexOf('avoid') >= 0) {
    applyBucketAdjust_(plan.bucket_stable_fixed_income, 2, '下沉性价比一般时，更适合回到高等级');
    applyBucketAdjust_(plan.bucket_active_fixed_income, -3, '进取固收桶不宜重配');
    applyBucketAdjust_(plan.bucket_cash, 1, '腾出的仓位先回到更稳的方向');
  }

  if (mortgageBackground && mortgageBackground.value === 'funding_tight') {
    applyBucketAdjust_(plan.bucket_cash, 2, '房贷与负债端偏紧时，家庭现金流缓冲更重要');
    applyBucketAdjust_(plan.bucket_risk, -2, '先降低高波动暴露');
  }
  if (housingBackground && housingBackground.score <= -1) {
    applyBucketAdjust_(plan.bucket_stable_fixed_income, 2, '住房背景偏冷时，整体配置宜偏稳');
    applyBucketAdjust_(plan.bucket_risk, -2, '风险桶不必激进');
  } else if (housingBackground && housingBackground.score >= 1) {
    applyBucketAdjust_(plan.bucket_risk, 2, '住房与信用背景回暖时，风险偏好可略抬升');
  }

  if (fxBackground && fxBackground.value === 'rmb_weak') {
    applyBucketAdjust_(plan.bucket_hedge, 4, '人民币偏弱背景增强时，对冲保值桶可略高于中性');
    applyBucketAdjust_(plan.bucket_risk, -2, '先降低组合对单一人民币资产的脆弱性');
  } else if (fxBackground && fxBackground.value === 'rmb_stable') {
    applyBucketAdjust_(plan.bucket_risk, 1, '汇率压力较轻时，风险桶可更从容');
  }

  if (usdAllocationBackground && usdAllocationBackground.score >= 1) {
    applyBucketAdjust_(plan.bucket_hedge, 2, '美元配置背景不差时，可给对冲桶更多空间');
  } else if (usdAllocationBackground && usdAllocationBackground.score <= -1) {
    applyBucketAdjust_(plan.bucket_hedge, -1, '美元配置背景一般时，不必把对冲桶推得太高');
  }

  if (goldBackground && goldBackground.score >= 1) {
    applyBucketAdjust_(plan.bucket_hedge, 3, '黄金背景偏强时，对冲桶可再加一点');
    applyBucketAdjust_(plan.bucket_cash, -1, '部分防御预算可从现金转向黄金/外汇对冲');
  } else if (goldBackground && goldBackground.score <= -1) {
    applyBucketAdjust_(plan.bucket_hedge, -1, '黄金背景一般时，对冲桶维持基础配置即可');
  }

  if (commodityBackground && commodityBackground.score >= 1) {
    applyBucketAdjust_(plan.bucket_risk, 4, '顺周期商品背景偏强时，风险资产桶可中性偏高');
    applyBucketAdjust_(plan.bucket_cash, -2, '部分活钱可转为风险预算');
  } else if (commodityBackground && commodityBackground.score <= -1) {
    applyBucketAdjust_(plan.bucket_risk, -4, '顺周期商品背景偏弱时，风险桶宜收一点');
    applyBucketAdjust_(plan.bucket_cash, 2, '腾出的仓位先回到活钱或稳健固收');
    applyBucketAdjust_(plan.bucket_stable_fixed_income, 2, '稳健固收可承接部分预算');
  }

  if (alloc) {
    var stableAnchor = toNumberOrNull_(alloc.alloc_rates_mid) + toNumberOrNull_(alloc.alloc_credit_high_grade);
    var activeAnchor = toNumberOrNull_(alloc.alloc_rates_long) + toNumberOrNull_(alloc.alloc_credit_sink);
    if (isFiniteNumber_(stableAnchor) && stableAnchor >= 45) {
      applyBucketAdjust_(plan.bucket_stable_fixed_income, 1, '内部固收引擎也偏向稳健底仓');
    }
    if (isFiniteNumber_(activeAnchor) && activeAnchor >= 28) {
      applyBucketAdjust_(plan.bucket_active_fixed_income, 1, '内部固收引擎给出的进取固收权重不低');
    }
    if (isFiniteNumber_(alloc.alloc_cash) && alloc.alloc_cash >= 15) {
      applyBucketAdjust_(plan.bucket_cash, 1, '内部固收引擎也建议多留一些现金/类现金');
    }
  }

  normalizeHouseholdBucketPlan_(plan);

  var keys = [
    'bucket_cash',
    'bucket_stable_fixed_income',
    'bucket_active_fixed_income',
    'bucket_hedge',
    'bucket_risk'
  ];

  for (var i = 0; i < keys.length; i++) {
    var item = plan[keys[i]];
    item.signal = makeBucketTiltSignal_(item.title, item.target_pct, item.base_pct, item.notes);
  }

  return plan;
}

function applyBucketAdjust_(bucket, delta, note) {
  bucket.target_pct += delta;
  if (note) bucket.notes.push(note);
}

function normalizeHouseholdBucketPlan_(plan) {
  var keys = [
    'bucket_cash',
    'bucket_stable_fixed_income',
    'bucket_active_fixed_income',
    'bucket_hedge',
    'bucket_risk'
  ];
  var sum = 0;
  for (var i = 0; i < keys.length; i++) {
    if (plan[keys[i]].target_pct < 0) plan[keys[i]].target_pct = 0;
    sum += plan[keys[i]].target_pct;
  }
  if (sum <= 0) return;

  var running = 0;
  for (var j = 0; j < keys.length; j++) {
    var item = plan[keys[j]];
    if (j < keys.length - 1) {
      item.target_pct = Math.round(item.target_pct * 100 / sum);
      running += item.target_pct;
    } else {
      item.target_pct = 100 - running;
    }
  }
}

function makeBucketTiltSignal_(title, targetPct, basePct, notes) {
  var delta = targetPct - basePct;
  var strength = Math.abs(delta) >= 8 ? 'high' : (Math.abs(delta) >= 5 ? 'medium' : 'low');
  var comment = title + '目标约 ' + targetPct + '%，中性基准约 ' + basePct + '%';
  if (notes && notes.length) {
    comment += '；' + dedupeTextParts_(notes).join('；');
  }

  if (delta >= 5) {
    return makeSignalResult_(title + '：高于中性', 'increase', 1, 'increase', strength, comment);
  }
  if (delta <= -5) {
    return makeSignalResult_(title + '：低于中性', 'decrease', -1, 'decrease', strength, comment);
  }
  return makeSignalResult_(title + '：中性', 'neutral', 0, 'neutral', 'low', comment);
}

function buildHouseholdBucketSummary_(plan) {
  if (!plan) return '';
  return [
    plan.bucket_cash.signal.label + '（约 ' + plan.bucket_cash.target_pct + '%）',
    plan.bucket_stable_fixed_income.signal.label + '（约 ' + plan.bucket_stable_fixed_income.target_pct + '%）',
    plan.bucket_active_fixed_income.signal.label + '（约 ' + plan.bucket_active_fixed_income.target_pct + '%）',
    plan.bucket_hedge.signal.label + '（约 ' + plan.bucket_hedge.target_pct + '%）',
    plan.bucket_risk.signal.label + '（约 ' + plan.bucket_risk.target_pct + '%）'
  ].join('；');
}

function dedupeTextParts_(parts) {
  var seen = {};
  var out = [];
  for (var i = 0; i < parts.length; i++) {
    var key = String(parts[i] || '');
    if (!key || seen[key]) continue;
    seen[key] = true;
    out.push(key);
  }
  return out;
}


function firstFiniteNumber_(values) {
  for (var i = 0; i < values.length; i++) {
    if (isFiniteNumber_(values[i])) return values[i];
  }
  return null;
}

function meanFiniteNumbers_(values) {
  var sum = 0;
  var count = 0;
  for (var i = 0; i < values.length; i++) {
    if (!isFiniteNumber_(values[i])) continue;
    sum += values[i];
    count++;
  }
  return count ? sum / count : null;
}

function hasEnoughFiniteNumbers_(values, minCount) {
  var count = 0;
  for (var i = 0; i < values.length; i++) {
    if (isFiniteNumber_(values[i])) count++;
  }
  return count >= minCount;
}

function buildAllocationFromSignals_(ratesDuration, ratesUltraLong, creditQuality, creditSink, creditShortEnd) {
  var alloc = {
    alloc_rates_long: 20,
    alloc_rates_mid: 25,
    alloc_rates_short: 20,
    alloc_credit_high_grade: 20,
    alloc_credit_sink: 5,
    alloc_cash: 10,
    comment: '基础中性配置'
  };

  if (ratesDuration.value === 'long') {
    alloc.alloc_rates_long += 20;
    alloc.alloc_rates_short -= 10;
    alloc.alloc_cash -= 5;
  } else if (ratesDuration.value === 'long_mild') {
    alloc.alloc_rates_long += 10;
    alloc.alloc_rates_short -= 5;
  } else if (ratesDuration.value === 'shorter') {
    alloc.alloc_rates_long -= 10;
    alloc.alloc_rates_short += 10;
    alloc.alloc_cash += 5;
  }

  if (ratesUltraLong.value === 'overweight') {
    alloc.alloc_rates_long += 5;
    alloc.alloc_rates_mid -= 5;
  } else if (ratesUltraLong.value === 'underweight') {
    alloc.alloc_rates_long -= 5;
    alloc.alloc_rates_mid += 5;
  }

  if (creditQuality.value === 'high_grade_favored') {
    alloc.alloc_credit_high_grade += 10;
    alloc.alloc_cash -= 5;
    alloc.alloc_rates_mid -= 5;
  } else if (creditQuality.value === 'high_grade_rich') {
    alloc.alloc_credit_high_grade -= 10;
    alloc.alloc_cash += 5;
    alloc.alloc_rates_mid += 5;
  }

  if (creditSink.value === 'can_sink') {
    alloc.alloc_credit_sink += 10;
    alloc.alloc_credit_high_grade -= 5;
    alloc.alloc_cash -= 5;
  } else if (creditSink.value === 'avoid_sink') {
    alloc.alloc_credit_sink = 0;
    alloc.alloc_credit_high_grade += 5;
    alloc.alloc_cash += 5;
  }

  if (creditShortEnd.value === 'short_credit_over_ncd') {
    alloc.alloc_rates_short += 5;
    alloc.alloc_cash -= 5;
  } else if (creditShortEnd.value === 'low_edge' || creditShortEnd.value === 'funding_watch') {
    alloc.alloc_rates_short -= 5;
    alloc.alloc_cash += 5;
  }

  normalizeAllocation_(alloc);

  alloc.comment = buildAllocationComment_(alloc);

  return alloc;
}

function buildAllocationComment_(alloc) {
  return '配置：长久期利率债 ' + alloc.alloc_rates_long + '%；中久期利率债 ' + alloc.alloc_rates_mid + '%；短久期利率债 ' + alloc.alloc_rates_short + '%；高等级信用债 ' + alloc.alloc_credit_high_grade + '%；信用下沉 ' + alloc.alloc_credit_sink + '%；现金/类现金 ' + alloc.alloc_cash + '%';
}


function normalizeAllocation_(alloc) {
  var keys = [
    'alloc_rates_long',
    'alloc_rates_mid',
    'alloc_rates_short',
    'alloc_credit_high_grade',
    'alloc_credit_sink',
    'alloc_cash'
  ];

  for (var i = 0; i < keys.length; i++) {
    if (alloc[keys[i]] < 0) alloc[keys[i]] = 0;
  }

  var sum = 0;
  for (var j = 0; j < keys.length; j++) sum += alloc[keys[j]];
  if (sum <= 0) return;

  var running = 0;
  for (var k = 0; k < keys.length; k++) {
    if (k < keys.length - 1) {
      alloc[keys[k]] = Math.round(alloc[keys[k]] * 100 / sum);
      running += alloc[keys[k]];
    } else {
      alloc[keys[k]] = 100 - running;
    }
  }
}


function makeSignalResult_(label, value, score, direction, strength, comment, actionHint, avoidHint) {
  return {
    label: toLifeFriendlyCopy_(label),
    value: value,
    score: score,
    direction: direction,
    strength: strength,
    comment: toLifeFriendlyCopy_(comment || ''),
    action_hint: actionHint ? toLifeFriendlyCopy_(actionHint) : '',
    avoid_hint: avoidHint ? toLifeFriendlyCopy_(avoidHint) : ''
  };
}

function makeSignalDetail_(signal, code, bucket, name, sourceMetric, cadence, sortOrder) {
  var actionHint = signal.action_hint || inferSignalActionHint_(code, signal);
  var avoidHint = signal.avoid_hint || inferSignalAvoidHint_(code, signal);

  return {
    level1_bucket: bucket,
    signal_code: code,
    signal_name: name,
    signal_value: signal.value,
    signal_score: signal.score,
    signal_direction: signal.direction,
    signal_strength: signal.strength,
    signal_text: buildSignalText_(signal),
    action_hint: actionHint,
    avoid_hint: avoidHint,
    source_metric: sourceMetric,
    cadence: cadence || 'weekly',
    sort_order: sortOrder
  };
}

function buildSignalText_(signal) {
  if (!signal) return '';
  if (!signal.comment) return signal.label;
  if (signal.comment === signal.label) return signal.label;
  return signal.label + '；' + signal.comment;
}

function pushSignalDetailRows_(rows, dateObj, theme, signalDefs) {
  for (var i = 0; i < signalDefs.length; i++) {
    var s = signalDefs[i];
    rows.push([
      dateObj,
      theme,
      s.level1_bucket,
      s.signal_code,
      s.signal_name,
      s.signal_value,
      s.signal_score,
      s.signal_direction,
      s.signal_strength,
      s.signal_text,
      s.action_hint,
      s.avoid_hint,
      s.source_metric,
      s.cadence,
      s.sort_order
    ]);
  }
}

function sortDetailRowsDesc_(rows) {
  rows.sort(function(a, b) {
    var ta = a[0] instanceof Date ? a[0].getTime() : new Date(a[0]).getTime();
    var tb = b[0] instanceof Date ? b[0].getTime() : new Date(b[0]).getTime();
    if (tb !== ta) return tb - ta;
    return a[14] - b[14];
  });
  return rows;
}

function toLifeFriendlyCopy_(text) {
  if (!text) return '';
  var out = String(text);

  var exactMap = {
    '利率债超长端：超配': '利率债超长端：更值得关注',
    '利率债超长端：低配': '利率债超长端：不宜重配',
    '利率债策略：拉长久期，超长端可超配': '利率债策略：久期可偏长，超长端更值得关注',
    '利率债策略：拉长久期，但不做超长端': '利率债策略：久期可偏长，但先不重超长端',
    '利率债策略：适度拉长，超长端可超配': '利率债策略：久期可略长，超长端更值得关注',
    '利率债策略：适度拉长，但不做超长端': '利率债策略：久期可略长，但先不重超长端',
    '利率债策略：缩短久期，超长端低配': '利率债策略：久期宜偏短，超长端不宜重配',
    '利率债策略：缩短久期': '利率债策略：久期宜偏短',
    '利率债策略：久期中性，超长端可超配': '利率债策略：久期中性，超长端更值得关注',
    '利率债策略：久期中性，超长端低配': '利率债策略：久期中性，超长端不宜重配',
    '信用债策略：中性，可适度下沉': '信用债策略：中性，可少量增加更高票息信用',
    '信用债策略：高等级占优': '信用债策略：高等级信用更占优',
    '信用债下沉：不宜下沉': '信用债下沉：不宜为了更高收益明显下沉信用风险'
  };

  if (exactMap[out]) return exactMap[out];

  out = out
    .replace(/超配/g, '更值得关注')
    .replace(/低配/g, '不宜重配')
    .replace(/拉长久期/g, '久期可偏长')
    .replace(/缩短久期/g, '久期宜偏短')
    .replace(/下沉要票息/g, '增加更高票息信用')
    .replace(/下沉/g, '增加更高票息信用');

  return out;
}

function inferSignalActionHint_(code, signal) {
  if (!signal) return '';
  var value = signal.value || '';
  var score = toNumberOrNull_(signal.score) || 0;

  if (code === 'liquidity_regime') {
    if (value === 'tight') return '减少对短期加杠杆、过高久期和负债端脆弱资产的依赖';
    if (value === 'loose') return '现金用途明确时，可把一部分闲置资金转向稳健固收或票息资产';
    return '按计划持有即可，等待资金环境更明确';
  }

  if (code === 'rates_strategy_tilt' || code === 'rates_duration_tilt' || code === 'rates_ultra_long_tilt') {
    if (value.indexOf('long') >= 0 || score >= 1) return '稳健固收桶和进取固收桶可向中长久期倾斜';
    if (value.indexOf('short') >= 0 || value.indexOf('underweight') >= 0 || score <= -1) return '久期先收一点，更多放在短久期和活钱桶';
    return '利率债久期先维持中性';
  }

  if (code === 'rates_curve_shape') {
    if (value === 'flat') return '更关注长端和高质量久期资产的配置价值';
    if (value === 'steep') return '中短端和滚动票息策略可更从容一些';
    return '曲线形态暂不提供强动作';
  }

  if (code === 'rates_rv_policy_bank_vs_gov' || code === 'rates_rv_local_gov_vs_gov' || code === 'rates_rv_ranking') {
    if (score >= 1) return '同类久期里优先看相对更便宜的一侧';
    if (score <= -1) return '别只因为票息略高就明显加仓';
    return '相对价值暂按中性处理';
  }

  if (code === 'credit_strategy_tilt' || code === 'credit_quality_tilt') {
    if (value.indexOf('high_grade') >= 0 || score >= 1) return '稳健固收桶优先放在高等级信用和高流动性品种';
    if (score <= -1) return '信用敞口先收一点，回到更透明的资产';
    return '信用仓位以中性为主';
  }

  if (code === 'credit_sink_tilt') {
    if (value === 'can_sink' || score >= 1) return '进取固收桶可少量增加更高票息信用';
    if (value === 'avoid_sink' || score <= -1) return '先别为了更高收益明显下沉信用风险';
    return '信用下沉先维持中性';
  }

  if (code === 'credit_rv_short_end_vs_ncd') {
    if (value === 'short_credit_over_ncd') return '短端票息资金可优先看高等级短久期信用';
    if (value === 'funding_watch' || value === 'low_edge') return '短端票息仓位先保守一些，保留更多活钱';
    return '短端票息方向暂时中性';
  }

  if (code === 'view_mortgage_background') {
    if (score >= 1) return '有房贷的可留意重定价、提前还贷和留贷比较';
    if (score <= -1) return '家庭现金流安排上预留更多缓冲';
    return '房贷相关决策可先维持原计划';
  }

  if (code === 'view_housing_background') {
    if (score >= 1) return '住房相关大额决策可结合现金流再评估，但仍以自住与长期需求为主';
    if (score <= -1) return '住房相关决策更适合看现金流安全边际，不必急于提高风险暴露';
    return '住房背景先按中性理解';
  }

  if (code === 'view_fx_background' || code === 'view_usd_allocation_background') {
    if (score >= 1 || value === 'rmb_weak') return '对冲保值桶可略高于中性，留意外币资产或美元敞口';
    if (score <= -1) return '对冲桶维持基础配置即可';
    return '汇率相关仓位先中性';
  }

  if (code === 'view_gold_background') {
    if (score >= 1) return '对冲保值桶可小幅增加黄金相关配置';
    if (score <= -1) return '黄金更多当作基础对冲，不必追高';
    return '黄金配置维持基础仓位即可';
  }

  if (code === 'view_commodity_background') {
    if (score >= 1) return '风险资产桶可中性偏高，但仍需保留底仓和现金缓冲';
    if (score <= -1) return '风险桶先收一点，把预算挪回活钱或稳健固收';
    return '顺周期方向先中性';
  }

  if (code === 'view_household_allocation') {
    return '按五类资产桶来理解整体配置，而不是盯着单一品种涨跌';
  }

  if (code.indexOf('bucket_') === 0) {
    if (value === 'increase') return '这一桶可以略高于你的中性配置';
    if (value === 'decrease') return '这一桶先低于中性配置';
    return '这一桶暂时维持中性';
  }

  return '';
}

function inferSignalAvoidHint_(code, signal) {
  if (!signal) return '';
  var value = signal.value || '';
  var score = toNumberOrNull_(signal.score) || 0;

  if (code === 'liquidity_regime') {
    if (value === 'tight') return '不要在资金偏紧时把组合做得过满';
    if (value === 'loose') return '资金偏松也不等于任何久期和信用都可以无脑加';
    return '避免把中性环境误读成趋势确认';
  }

  if (code === 'rates_strategy_tilt' || code === 'rates_duration_tilt' || code === 'rates_ultra_long_tilt') {
    if (score >= 1) return '不要把全部久期都压在超长端';
    if (score <= -1) return '不要因为票息诱惑忽视久期回撤';
    return '避免频繁来回切换久期';
  }

  if (code === 'credit_strategy_tilt' || code === 'credit_quality_tilt' || code === 'credit_sink_tilt' || code === 'credit_rv_short_end_vs_ncd') {
    if (value == 'can_sink' || score >= 1) return '即便环境允许，也别明显越过自己的信用风险边界';
    if (value === 'avoid_sink' || score <= -1) return '不要为了多一点票息去承担不透明信用风险';
    return '不要把中性信用环境理解成稳赚机会';
  }

  if (code === 'view_mortgage_background' || code === 'view_housing_background') {
    return '这些信号只反映融资与利率背景，不等于房价会立刻上涨或下跌';
  }

  if (code === 'view_fx_background' || code === 'view_usd_allocation_background') {
    return '不要情绪化追美元，也不要把汇率信号当成短线交易指令';
  }

  if (code === 'view_gold_background') {
    return '不要把黄金当成永远只涨不跌的单向资产';
  }

  if (code === 'view_commodity_background') {
    return '不要把商品背景信号直接等同于短期价格必然上涨';
  }

  if (code === 'view_household_allocation' || code === 'comment_household') {
    return '最终仍要结合收入稳定性、负债和流动性需求';
  }

  if (code.indexOf('bucket_') === 0) {
    return '资产桶倾向是相对中性基准的提醒，不是必须一次到位调仓';
  }

  return '';
}


function sortAndDedupeByDate_(rows, keyFn) {
  var map = {};
  for (var i = 0; i < rows.length; i++) map[keyFn(rows[i])] = rows[i];

  var deduped = Object.keys(map).map(function(k) { return map[k]; });
  deduped.sort(function(a, b) { return a.dateObj.getTime() - b.dateObj.getTime(); });
  return deduped;
}

function withSpreadsheetRetry_(fn, label) {
  var lastErr = null;
  for (var attempt = 1; attempt <= 4; attempt++) {
    try {
      return fn();
    } catch (err) {
      lastErr = err;
      var msg = String(err && err.message || err || '');
      var retryable = msg.indexOf('Service Spreadsheets timed out') >= 0 ||
        msg.indexOf('Service invoked too many times') >= 0 ||
        msg.indexOf('Exceeded maximum execution time') >= 0;
      if (!retryable || attempt === 4) break;
      SpreadsheetApp.flush();
      Utilities.sleep(400 * attempt);
    }
  }
  throw new Error((label || 'Spreadsheet operation') + ' failed: ' + (lastErr && lastErr.message ? lastErr.message : lastErr));
}
function formatSignalSheet_(sheet, options) {
  options = options || {};
  var lastRow = withSpreadsheetRetry_(function() { return sheet.getLastRow(); }, 'sheet.getLastRow');
  var lastCol = withSpreadsheetRetry_(function() { return sheet.getLastColumn(); }, 'sheet.getLastColumn');
  if (lastRow < 1 || lastCol < 1) return;

  withSpreadsheetRetry_(function() {
    sheet.setFrozenRows(1);
    sheet.getRange(1, 1, 1, lastCol)
      .setFontWeight('bold')
      .setBackground('#f3f6d8');
    return true;
  }, 'formatSignalSheet_ header');

  if (lastRow >= 2) {
    withSpreadsheetRetry_(function() {
      sheet.getRange(2, 1, lastRow - 1, lastCol).setFontSize(10);
      return true;
    }, 'formatSignalSheet_ body');
  }

  if (!options.skipAutoResize && lastRow <= 2000) {
    withSpreadsheetRetry_(function() {
      sheet.autoResizeColumns(1, lastCol);
      return true;
    }, 'formatSignalSheet_ resize');
  }
}
