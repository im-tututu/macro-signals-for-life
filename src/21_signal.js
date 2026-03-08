/********************
 * 21_signal.js
 * 基于“指标_利率”生成“信号_利率”。
 *
 * 原则：
 * 1) 指标表负责客观数值
 * 2) 信号表负责结论与配置建议
 * 3) 缺失扩展曲线时，不阻塞主信号生成
 ********************/

function buildSignal_() {
  var ss = SpreadsheetApp.getActiveSpreadsheet();
  var metricsSheet = mustGetSheet_(ss, SHEET_METRICS);
  var outSheet = mustGetSheet_(ss, SHEET_SIGNAL);

  var metricRows = readMetricsRowsV2_(metricsSheet);
  if (!metricRows.length) {
    throw new Error(SHEET_METRICS + ' 无有效数据。');
  }

  metricRows = sortAndDedupeByDate_(metricRows, function(row) {
    return row.dateKey;
  });

  var moneyMarketSheet = ss.getSheetByName(SHEET_MONEY_MARKET_RAW);
  var dr007Map = moneyMarketSheet ? readMoneyMarketDr007Map_(moneyMarketSheet) : {};

  var resultsAsc = [];
  for (var i = 0; i < metricRows.length; i++) {
    var row = metricRows[i];
    var dr007 = toNumberOrNull_(dr007Map[row.dateKey]);

    var duration = classifyDurationSignal_(row);
    var ultraLong = classifyUltraLongSignal_(row);
    var curve = classifyCurveSignal_(row);
    var policyBank = classifyPolicyBankSignal_(row);
    var localGov = classifyLocalGovSignal_(row);
    var highGradeCredit = classifyHighGradeCreditSignal_(row);
    var creditSink = classifyCreditSinkSignal_(row);
    var mtnTier = classifyMtnTierSignal_(row);
    var ncdVsCredit = classifyNcdSignal_(row, dr007);

    var fundingView = classifyFundingViewV2_(dr007);
    var creditView = classifyCreditViewV2_(row);
    var regimeView = classifyRegimeView_(duration, highGradeCredit, creditSink);

    var alloc = buildAllocationFromSignals_(
      duration,
      ultraLong,
      highGradeCredit,
      creditSink,
      ncdVsCredit
    );

    resultsAsc.push([
      row.dateObj,

      duration.label,
      ultraLong.label,
      curve.label,

      policyBank.label,
      localGov.label,

      highGradeCredit.label,
      creditSink.label,
      mtnTier.label,
      ncdVsCredit.label,

      fundingView,
      creditView,
      regimeView,

      alloc.weight_long_duration,
      alloc.weight_mid_duration,
      alloc.weight_short_duration,
      alloc.weight_high_grade_credit,
      alloc.weight_credit_sink,
      alloc.weight_cash,

      duration.comment,
      mergeSignalComments_([highGradeCredit.comment, creditSink.comment, mtnTier.comment, ncdVsCredit.comment]),
      alloc.comment
    ]);
  }

  var resultsDesc = resultsAsc.slice().reverse();
  var header = [[
    'date',

    'signal_duration',
    'signal_ultra_long',
    'signal_curve',

    'signal_policy_bank',
    'signal_local_gov',

    'signal_high_grade_credit',
    'signal_credit_sink',
    'signal_mtn_tier',
    'signal_ncd_vs_credit',

    'view_funding',
    'view_credit',
    'view_regime',

    'weight_long_duration',
    'weight_mid_duration',
    'weight_short_duration',
    'weight_high_grade_credit',
    'weight_credit_sink',
    'weight_cash',

    'comment_duration',
    'comment_credit',
    'comment_allocation'
  ]];

  outSheet.clearContents();
  outSheet.clearFormats();
  outSheet.getRange(1, 1, 1, header[0].length).setValues(header);

  if (resultsDesc.length > 0) {
    outSheet.getRange(2, 1, resultsDesc.length, resultsDesc[0].length).setValues(resultsDesc);
    outSheet.getRange(2, 1, resultsDesc.length, 1).setNumberFormat('yyyy-mm-dd');
    outSheet.getRange(2, 14, resultsDesc.length, 6).setNumberFormat('0');
  }

  formatSignalSheet_(outSheet);
}

function buildETFSignal_() { buildSignal_(); }
function runBondAllocationSignal_() { buildSignal_(); }
function buildBondAllocationSignal_() { buildSignal_(); }

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

function classifyDurationSignal_(row) {
  var pct = row.gov_10y_pct250;
  var slope = row.gov_slope_10_1;

  if (!isFiniteNumber_(pct)) return { label: '中性', comment: '10Y 分位不足，久期中性' };

  if (pct >= SIGNAL_THRESHOLDS.duration_pct_high) {
    if (isFiniteNumber_(slope) && slope <= SIGNAL_THRESHOLDS.curve_10_1_flat) {
      return { label: '偏多长久期', comment: '10Y 利率高分位且曲线偏平，长久期性价比高' };
    }
    return { label: '适度拉长久期', comment: '10Y 利率偏高，可适度拉长久期' };
  }

  if (pct <= SIGNAL_THRESHOLDS.duration_pct_low) {
    return { label: '偏防守久期', comment: '10Y 利率低分位，控制久期更稳妥' };
  }

  return { label: '久期中性', comment: '10Y 利率处于中间区域，久期保持中性' };
}

function classifyUltraLongSignal_(row) {
  var slope = row.gov_slope_30_10;
  if (!isFiniteNumber_(slope)) return { label: '中性', comment: '30Y-10Y 缺失，超长端中性' };

  if (slope >= SIGNAL_THRESHOLDS.ultra_long_slope_high) {
    return { label: '超长债占优', comment: '30Y-10Y 偏陡，超长端弹性更好' };
  }
  if (slope <= SIGNAL_THRESHOLDS.ultra_long_slope_low) {
    return { label: '超长债偏拥挤', comment: '30Y-10Y 偏平，超长端赔率下降' };
  }
  return { label: '超长债中性', comment: '30Y-10Y 处于中性区间' };
}

function classifyCurveSignal_(row) {
  var slope = row.gov_slope_10_1;
  if (!isFiniteNumber_(slope)) return { label: '曲线未知', comment: '10Y-1Y 缺失' };
  if (slope <= SIGNAL_THRESHOLDS.curve_10_1_flat) return { label: '曲线偏平', comment: '10Y-1Y 偏低，长端更受益' };
  if (slope >= SIGNAL_THRESHOLDS.curve_10_1_steep) return { label: '曲线偏陡', comment: '10Y-1Y 偏高，短中端相对更稳' };
  return { label: '曲线中性', comment: '10Y-1Y 中性' };
}

function classifyPolicyBankSignal_(row) {
  var pct = row.spread_cdb_gov_10y_pct250;
  var val = row.spread_cdb_gov_10y;
  if (!isFiniteNumber_(val) || !isFiniteNumber_(pct)) return { label: '中性', comment: '政金利差数据不足' };

  if (pct >= SIGNAL_THRESHOLDS.policy_spread_high) {
    return { label: '增配国开', comment: '国开-国债利差高位，国开相对更便宜' };
  }
  if (pct <= SIGNAL_THRESHOLDS.policy_spread_low) {
    return { label: '偏向国债', comment: '国开-国债利差低位，国债更稳妥' };
  }
  return { label: '政金中性', comment: '国开-国债利差中性' };
}

function classifyLocalGovSignal_(row) {
  var v = row.spread_local_gov_gov_10y;
  if (!isFiniteNumber_(v)) return { label: '中性', comment: '地方债数据不足或历史较短' };
  if (v >= 0.20) return { label: '地方债偏便宜', comment: '地方债-国债利差偏高，可关注配置价值' };
  if (v <= 0.08) return { label: '地方债偏贵', comment: '地方债-国债利差偏低，性价比一般' };
  return { label: '地方债中性', comment: '地方债-国债利差中性' };
}

function classifyHighGradeCreditSignal_(row) {
  var pct = row.spread_aaa_credit_gov_5y_pct250;
  var v = row.spread_aaa_credit_gov_5y;
  if (!isFiniteNumber_(v) || !isFiniteNumber_(pct)) return { label: '中性', comment: '高等级信用利差数据不足' };

  if (pct >= SIGNAL_THRESHOLDS.credit_spread_high) {
    return { label: '增配高等级信用', comment: 'AAA 信用利差高位，高等级信用性价比改善' };
  }
  if (pct <= SIGNAL_THRESHOLDS.credit_spread_low) {
    return { label: '高等级信用偏贵', comment: 'AAA 信用利差低位，信用保护垫偏薄' };
  }
  return { label: '高等级信用中性', comment: 'AAA 信用利差中性' };
}

function classifyCreditSinkSignal_(row) {
  var pct = row.spread_aa_plus_vs_aaa_credit_1y_pct250;
  var v = row.spread_aa_plus_vs_aaa_credit_1y;
  if (!isFiniteNumber_(v) || !isFiniteNumber_(pct)) return { label: '中性', comment: '信用下沉利差数据不足' };

  if (pct >= SIGNAL_THRESHOLDS.sink_spread_high) {
    return { label: '不宜下沉', comment: 'AA+-AAA(1Y) 利差高位，风险偏好偏弱' };
  }
  if (pct <= SIGNAL_THRESHOLDS.sink_spread_low) {
    return { label: '可适度下沉', comment: 'AA+-AAA(1Y) 利差低位，风险偏好改善' };
  }
  return { label: '下沉中性', comment: 'AA+-AAA(1Y) 利差中性' };
}

function classifyMtnTierSignal_(row) {
  var v = row.spread_aaa_mtn_vs_aaa_plus_mtn_1y;
  if (!isFiniteNumber_(v)) return { label: '中票分层未知', comment: 'AAA/AAA+ 中票 1Y 利差缺失' };
  if (v >= 0.08) return { label: 'AAA中票更便宜', comment: 'AAA 中票相对 AAA+ 中票利差偏高' };
  if (v <= 0.03) return { label: 'AAA+中票更稳', comment: 'AAA/AAA+ 中票利差偏低，更偏基准化配置' };
  return { label: '中票分层中性', comment: 'AAA/AAA+ 中票利差中性' };
}

function classifyNcdSignal_(row, dr007) {
  var ncdPct = row.aaa_ncd_1y_pct250;
  var creditNcdSpread = row.spread_aaa_credit_ncd_1y;
  if (!isFiniteNumber_(ncdPct) || !isFiniteNumber_(creditNcdSpread)) {
    return { label: '中性', comment: '存单/信用利差数据不足' };
  }

  if (ncdPct >= SIGNAL_THRESHOLDS.ncd_pct_high && creditNcdSpread >= 0.20) {
    return { label: '短端信用占优', comment: '存单高位且信用-存单利差较高，短端高等级信用性价比提升' };
  }
  if (ncdPct <= SIGNAL_THRESHOLDS.ncd_pct_low && creditNcdSpread <= 0.12) {
    return { label: '短端赔率偏低', comment: '存单低位且信用-存单利差偏窄，短端赔率一般' };
  }
  if (isFiniteNumber_(dr007) && dr007 >= SIGNAL_THRESHOLDS.funding_tight) {
    return { label: '关注资金扰动', comment: '资金偏紧，短端信用需关注负债端压力' };
  }
  return { label: '短端中性', comment: '存单与短端信用关系中性' };
}

function classifyFundingViewV2_(dr007) {
  if (!isFiniteNumber_(dr007)) return '资金未知';
  if (dr007 >= SIGNAL_THRESHOLDS.funding_tight) return '资金偏紧';
  if (dr007 <= SIGNAL_THRESHOLDS.funding_loose) return '资金偏松';
  return '资金中性';
}

function classifyCreditViewV2_(row) {
  var highGrade = row.spread_aaa_credit_gov_5y_pct250;
  var sink = row.spread_aa_plus_vs_aaa_credit_1y_pct250;

  if (isFiniteNumber_(highGrade) && highGrade >= SIGNAL_THRESHOLDS.credit_spread_high) {
    return '高等级信用偏便宜';
  }
  if (isFiniteNumber_(sink) && sink >= SIGNAL_THRESHOLDS.sink_spread_high) {
    return '下沉风险偏高';
  }
  if (isFiniteNumber_(sink) && sink <= SIGNAL_THRESHOLDS.sink_spread_low) {
    return '信用下沉环境改善';
  }
  return '信用中性';
}

function classifyRegimeView_(duration, highGradeCredit, creditSink) {
  if (duration.label === '偏多长久期' && highGradeCredit.label === '增配高等级信用') {
    return '债牛偏进攻';
  }
  if (duration.label === '偏防守久期' && creditSink.label === '不宜下沉') {
    return '防守环境';
  }
  return '中性环境';
}

function buildAllocationFromSignals_(duration, ultraLong, highGradeCredit, creditSink, ncdVsCredit) {
  var alloc = {
    weight_long_duration: 20,
    weight_mid_duration: 25,
    weight_short_duration: 20,
    weight_high_grade_credit: 20,
    weight_credit_sink: 5,
    weight_cash: 10,
    comment: '基础中性配置'
  };

  if (duration.label === '偏多长久期') {
    alloc.weight_long_duration += 20;
    alloc.weight_short_duration -= 10;
    alloc.weight_cash -= 5;
  } else if (duration.label === '适度拉长久期') {
    alloc.weight_long_duration += 10;
    alloc.weight_short_duration -= 5;
  } else if (duration.label === '偏防守久期') {
    alloc.weight_long_duration -= 10;
    alloc.weight_short_duration += 10;
    alloc.weight_cash += 5;
  }

  if (ultraLong.label === '超长债占优') {
    alloc.weight_long_duration += 5;
    alloc.weight_mid_duration -= 5;
  } else if (ultraLong.label === '超长债偏拥挤') {
    alloc.weight_long_duration -= 5;
    alloc.weight_mid_duration += 5;
  }

  if (highGradeCredit.label === '增配高等级信用') {
    alloc.weight_high_grade_credit += 10;
    alloc.weight_cash -= 5;
    alloc.weight_mid_duration -= 5;
  } else if (highGradeCredit.label === '高等级信用偏贵') {
    alloc.weight_high_grade_credit -= 10;
    alloc.weight_cash += 5;
    alloc.weight_mid_duration += 5;
  }

  if (creditSink.label === '可适度下沉') {
    alloc.weight_credit_sink += 10;
    alloc.weight_high_grade_credit -= 5;
    alloc.weight_cash -= 5;
  } else if (creditSink.label === '不宜下沉') {
    alloc.weight_credit_sink = 0;
    alloc.weight_high_grade_credit += 5;
    alloc.weight_cash += 5;
  }

  if (ncdVsCredit.label === '短端信用占优') {
    alloc.weight_short_duration += 5;
    alloc.weight_cash -= 5;
  } else if (ncdVsCredit.label === '短端赔率偏低') {
    alloc.weight_short_duration -= 5;
    alloc.weight_cash += 5;
  }

  normalizeAllocation_(alloc);

  alloc.comment = [
    duration.label,
    highGradeCredit.label,
    creditSink.label,
    ncdVsCredit.label
  ].join('；');

  return alloc;
}

function normalizeAllocation_(alloc) {
  var keys = [
    'weight_long_duration',
    'weight_mid_duration',
    'weight_short_duration',
    'weight_high_grade_credit',
    'weight_credit_sink',
    'weight_cash'
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

function mergeSignalComments_(arr) {
  var parts = [];
  for (var i = 0; i < arr.length; i++) if (arr[i]) parts.push(arr[i]);
  return parts.join('；');
}

function sortAndDedupeByDate_(rows, keyFn) {
  var map = {};
  for (var i = 0; i < rows.length; i++) map[keyFn(rows[i])] = rows[i];

  var deduped = Object.keys(map).map(function(k) { return map[k]; });
  deduped.sort(function(a, b) { return a.dateObj.getTime() - b.dateObj.getTime(); });
  return deduped;
}

function formatSignalSheet_(sheet) {
  var lastRow = sheet.getLastRow();
  var lastCol = sheet.getLastColumn();
  if (lastRow < 1 || lastCol < 1) return;

  sheet.getRange(1, 1, 1, lastCol)
    .setFontWeight('bold')
    .setBackground('#f3f6d8');

  if (lastRow >= 2) {
    sheet.getRange(2, 1, lastRow - 1, lastCol).setFontSize(10);
  }
  sheet.autoResizeColumns(1, lastCol);
}
