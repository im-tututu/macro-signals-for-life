/********************
 * 21_signal.js
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

function buildSignal_() {
  var ss = SpreadsheetApp.getActiveSpreadsheet();
  var metricsSheet = mustGetSheet_(ss, SHEET_METRICS);
  var mainSheet = getOrCreateSheetByName_(ss, '信号-主要');
  var detailSheet = getOrCreateSheetByName_(ss, '信号-明细');

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

    var alloc = buildAllocationFromSignals_(
      ratesDuration,
      ratesUltraLong,
      creditQuality,
      creditSink,
      creditShortEnd
    );

    mainRowsAsc.push([
      row.dateObj,
      liquidity.label,
      ratesStrategy.label,
      ratesRvRanking.label,
      creditStrategy.label,
      alloc.alloc_rates_long,
      alloc.alloc_rates_mid,
      alloc.alloc_rates_short,
      alloc.alloc_credit_high_grade,
      alloc.alloc_credit_sink,
      alloc.alloc_cash
    ]);

    pushSignalDetailRows_(detailRows, row.dateObj, SIGNAL_THEME_ASSET_ALLOC, [
      makeSignalDetail_(liquidity, 'liquidity_regime', 'liquidity', '资金与流动性环境', 'dr007_weighted_rate', 10),
      makeSignalDetail_(ratesStrategy, 'rates_strategy_tilt', 'rates', '利率债久期/超长端策略', 'gov_10y_pct250|gov_slope_10_1|gov_slope_30_10', 15),
      makeSignalDetail_(ratesDuration, 'rates_duration_tilt', 'rates', '利率债久期倾向', 'gov_10y_pct250|gov_slope_10_1', 20),
      makeSignalDetail_(ratesUltraLong, 'rates_ultra_long_tilt', 'rates', '利率债超长端倾向', 'gov_slope_30_10', 30),
      makeSignalDetail_(ratesCurve, 'rates_curve_shape', 'rates', '利率债曲线形态', 'gov_slope_10_1', 40),
      makeSignalDetail_(ratesPolicyBank, 'rates_rv_policy_bank_vs_gov', 'rates', '利率债相对价值：国开债 vs 国债', 'spread_cdb_gov_10y|spread_cdb_gov_10y_pct250', 50),
      makeSignalDetail_(ratesLocalGov, 'rates_rv_local_gov_vs_gov', 'rates', '利率债相对价值：地方债 vs 国债', 'spread_local_gov_gov_10y', 60),
      makeSignalDetail_(ratesRvRanking, 'rates_rv_ranking', 'rates', '利率债相对价值排序', 'spread_cdb_gov_10y_pct250|spread_local_gov_gov_10y', 70),
      makeSignalDetail_(creditStrategy, 'credit_strategy_tilt', 'credit', '信用债资质/下沉策略', 'spread_aaa_credit_gov_5y_pct250|spread_aa_plus_vs_aaa_credit_1y_pct250', 80),
      makeSignalDetail_(creditQuality, 'credit_quality_tilt', 'credit', '信用债资质倾向', 'spread_aaa_credit_gov_5y|spread_aaa_credit_gov_5y_pct250', 90),
      makeSignalDetail_(creditSink, 'credit_sink_tilt', 'credit', '信用债下沉倾向', 'spread_aa_plus_vs_aaa_credit_1y|spread_aa_plus_vs_aaa_credit_1y_pct250', 100),
      makeSignalDetail_(creditMtnTier, 'credit_rv_mtn_tier', 'credit', '信用债相对价值：AAA中票 vs AAA+中票', 'spread_aaa_mtn_vs_aaa_plus_mtn_1y', 110),
      makeSignalDetail_(creditShortEnd, 'credit_rv_short_end_vs_ncd', 'credit', '短端票息资产：高等级信用 vs 存单', 'spread_aaa_credit_ncd_1y|aaa_ncd_1y_pct250|dr007_weighted_rate', 120)
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
    sheet.getRange(2, 6, rows.length, 6).setNumberFormat('0');
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
    'source_metric',
    'sort_order'
  ]];

  sheet.clearContents();
  sheet.clearFormats();
  sheet.getRange(1, 1, 1, header[0].length).setValues(header);

  if (rows.length > 0) {
    sheet.getRange(2, 1, rows.length, rows[0].length).setValues(rows);
    sheet.getRange(2, 1, rows.length, 1).setNumberFormat('yyyy-mm-dd');
    sheet.getRange(2, 7, rows.length, 1).setNumberFormat('0');
    sheet.getRange(2, 12, rows.length, 1).setNumberFormat('0');
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

      spread_local_gov_gov_10y: readMetricNum_(r, idx, 'spread_local_gov_gov_10y'),

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

function makeSignalResult_(label, value, score, direction, strength, comment) {
  return {
    label: label,
    value: value,
    score: score,
    direction: direction,
    strength: strength,
    comment: comment
  };
}

function makeSignalDetail_(signal, code, bucket, name, sourceMetric, sortOrder) {
  return {
    level1_bucket: bucket,
    signal_code: code,
    signal_name: name,
    signal_value: signal.value,
    signal_score: signal.score,
    signal_direction: signal.direction,
    signal_strength: signal.strength,
    signal_text: signal.label + '；' + signal.comment,
    source_metric: sourceMetric,
    sort_order: sortOrder
  };
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
      s.source_metric,
      s.sort_order
    ]);
  }
}

function sortDetailRowsDesc_(rows) {
  rows.sort(function(a, b) {
    var ta = a[0] instanceof Date ? a[0].getTime() : new Date(a[0]).getTime();
    var tb = b[0] instanceof Date ? b[0].getTime() : new Date(b[0]).getTime();
    if (tb !== ta) return tb - ta;
    return a[11] - b[11];
  });
  return rows;
}

function sortAndDedupeByDate_(rows, keyFn) {
  var map = {};
  for (var i = 0; i < rows.length; i++) map[keyFn(rows[i])] = rows[i];

  var deduped = Object.keys(map).map(function(k) { return map[k]; });
  deduped.sort(function(a, b) { return a.dateObj.getTime() - b.dateObj.getTime(); });
  return deduped;
}

function getOrCreateSheetByName_(ss, name) {
  var sh = ss.getSheetByName(name);
  return sh || ss.insertSheet(name);
}

function formatSignalSheet_(sheet) {
  var lastRow = sheet.getLastRow();
  var lastCol = sheet.getLastColumn();
  if (lastRow < 1 || lastCol < 1) return;

  sheet.setFrozenRows(1);
  sheet.getRange(1, 1, 1, lastCol)
    .setFontWeight('bold')
    .setBackground('#f3f6d8');

  if (lastRow >= 2) {
    sheet.getRange(2, 1, lastRow - 1, lastCol).setFontSize(10);
  }
  sheet.autoResizeColumns(1, lastCol);
}
