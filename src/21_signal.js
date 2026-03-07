/********************
 * 21_signal.js
 * 利率统一信号表：合并 ETF 观察信号与债券配置建议。
 *
 * 输出 Sheet：SHEET_SIGNAL（信号_利率）
 * 主要字段：
 *   - date / etf_slope_10_1 / etf_signal
 *   - 10Y / MA120 / pct250 / slope10_1 / dr007 / credit_spread
 *   - funding_view / credit_view / regime
 *   - long_bond / mid_bond / short_bond / cash / comment
 ********************/

/**
 * 统一构建利率信号表。
 */
function buildSignal_() {
  var ss = SpreadsheetApp.getActiveSpreadsheet();
  var metricsSheet = mustGetSheet_(ss, SHEET_METRICS);
  var moneyMarketSheet = mustGetSheet_(ss, SHEET_MONEY_MARKET_RAW);
  var outSheet = mustGetSheet_(ss, SHEET_SIGNAL);

  var metricRows = readMetricsRows_(metricsSheet);
  if (!metricRows.length) {
    throw new Error(SHEET_METRICS + ' 无有效数据。');
  }

  metricRows = sortAndDedupeByDate_(metricRows, function(row) {
    return row.dateKey;
  });

  var dr007Map = readMoneyMarketDr007Map_(moneyMarketSheet);
  var resultsAsc = [];
  var tenYHistory = [];

  for (var i = 0; i < metricRows.length; i++) {
    var row = metricRows[i];
    var tenY = toNumberOrNull_(row.gov10y);

    if (!isFiniteNumber_(tenY)) {
      continue;
    }

    tenYHistory.push(tenY);

    var ma120 = rollingMean_(tenYHistory, 120);
    var pct250 = rollingPercentileRank_(tenYHistory, 250, tenY);
    var slope10_1 = toNumberOrNull_(row.slope10_1);
    var dr007 = toNumberOrNull_(dr007Map[row.dateKey]);
    var creditSpread = toNumberOrNull_(row.credit_spread);

    var etfSignal = classifyETFSignal_(slope10_1);
    var fundingView = classifyFundingView_(dr007);
    var creditView = classifyCreditView_(creditSpread);

    var regimeObj = classifyBondRegime_({
      tenY: tenY,
      ma120: ma120,
      pct250: pct250,
      slope10_1: slope10_1,
      dr007: dr007
    });

    resultsAsc.push([
      row.dateObj,
      isFiniteNumber_(slope10_1) ? slope10_1 : '',
      etfSignal,
      tenY,
      isFiniteNumber_(ma120) ? ma120 : '',
      isFiniteNumber_(pct250) ? pct250 : '',
      isFiniteNumber_(slope10_1) ? slope10_1 : '',
      isFiniteNumber_(dr007) ? dr007 : '',
      isFiniteNumber_(creditSpread) ? creditSpread : '',
      fundingView,
      creditView,
      regimeObj.regime,
      regimeObj.long_bond,
      regimeObj.mid_bond,
      regimeObj.short_bond,
      regimeObj.cash,
      mergeComment_(regimeObj.comment, fundingView, creditView)
    ]);
  }

  var resultsDesc = resultsAsc.slice().reverse();
  var header = [[
    'date',
    'etf_slope_10_1',
    'etf_signal',
    '10Y',
    'MA120',
    'pct250',
    'slope10_1',
    'dr007',
    'credit_spread',
    'funding_view',
    'credit_view',
    'regime',
    'long_bond',
    'mid_bond',
    'short_bond',
    'cash',
    'comment'
  ]];

  outSheet.clearContents();
  outSheet.clearFormats();
  outSheet.getRange(1, 1, 1, header[0].length).setValues(header);

  if (resultsDesc.length > 0) {
    outSheet.getRange(2, 1, resultsDesc.length, resultsDesc[0].length).setValues(resultsDesc);
    outSheet.getRange(2, 1, resultsDesc.length, 1).setNumberFormat('yyyy-mm-dd');
  }

  formatSignalSheet_(outSheet);
}

/**
 * 兼容旧入口：ETF 信号已并入统一信号表。
 */
function buildETFSignal_() {
  buildSignal_();
}

/**
 * 兼容旧入口：债券配置建议已并入统一信号表。
 */
function runBondAllocationSignal_() {
  buildSignal_();
}

function buildBondAllocationSignal_() {
  buildSignal_();
}

function classifyETFSignal_(slope10_1) {
  if (!isFiniteNumber_(slope10_1)) return '中性';
  if (slope10_1 < SIGNAL_THRESHOLDS.steep_low) return '长债机会';
  if (slope10_1 > SIGNAL_THRESHOLDS.steep_high) return '短债优先';
  return '中性';
}

function readMetricsRows_(sheet) {
  var values = sheet.getDataRange().getValues();
  if (values.length < 2) return [];

  var header = values[0];
  var idx = buildHeaderIndex_(header);

  var dateCol = idx['date'];
  var gov1yCol = idx['gov_1y'];
  var gov3yCol = idx['gov_3y'];
  var gov5yCol = idx['gov_5y'];
  var gov10yCol = idx['gov_10y'];
  var slope10_1Col = idx['slope_10_1'];
  var creditSpreadCol = idx['credit_spread'];

  var rows = [];
  for (var i = 1; i < values.length; i++) {
    var r = values[i];
    var dateObj = normalizeSheetDate_(r[dateCol]);
    var gov10y = toNumberOrNull_(r[gov10yCol]);
    if (!dateObj || !isFiniteNumber_(gov10y)) continue;

    rows.push({
      dateObj: dateObj,
      dateKey: formatDateKey_(dateObj),
      gov1y: gov1yCol == null ? null : toNumberOrNull_(r[gov1yCol]),
      gov3y: gov3yCol == null ? null : toNumberOrNull_(r[gov3yCol]),
      gov5y: gov5yCol == null ? null : toNumberOrNull_(r[gov5yCol]),
      gov10y: gov10y,
      slope10_1: slope10_1Col == null ? null : toNumberOrNull_(r[slope10_1Col]),
      credit_spread: creditSpreadCol == null ? null : toNumberOrNull_(r[creditSpreadCol])
    });
  }

  return rows;
}

function readMoneyMarketDr007Map_(sheet) {
  var values = sheet.getDataRange().getValues();
  if (values.length < 2) return {};

  var header = values[0];
  var idx = buildHeaderIndex_(header);
  var dateCol = idx['date'];
  var dr007Col = idx['dr007_weightedrate'];

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

function classifyBondRegime_(x) {
  var tenY = x.tenY;
  var ma120 = x.ma120;
  var pct250 = x.pct250;
  var slope10_1 = x.slope10_1;
  var dr007 = x.dr007;

  if (!isFiniteNumber_(tenY) || !isFiniteNumber_(ma120) || !isFiniteNumber_(pct250)) {
    return {
      regime: 'NEUTRAL',
      long_bond: 25,
      mid_bond: 35,
      short_bond: 30,
      cash: 10,
      comment: '历史数据不足（MA120/PCT250未就绪），中性配置'
    };
  }

  var fundingTight = isFiniteNumber_(dr007) && dr007 >= 1.80;
  var curveFlat = isFiniteNumber_(slope10_1) && slope10_1 <= 0.50;

  if (pct250 <= 0.10 && tenY < ma120) {
    return {
      regime: 'VERY_DEFENSIVE',
      long_bond: 0,
      mid_bond: 20,
      short_bond: 50,
      cash: 30,
      comment: '利率低位且弱于均线，久期偏贵'
    };
  }

  if (pct250 >= 0.80 && curveFlat && !fundingTight) {
    return {
      regime: 'STRONG_BUY_LONG_BOND',
      long_bond: 70,
      mid_bond: 20,
      short_bond: 10,
      cash: 0,
      comment: '利率高位且曲线偏平，长债性价比高'
    };
  }

  if (pct250 >= 0.65 && !fundingTight) {
    return {
      regime: 'BUY_LONG_BOND',
      long_bond: 50,
      mid_bond: 25,
      short_bond: 20,
      cash: 5,
      comment: '利率偏高，可适度拉长久期'
    };
  }

  if (pct250 <= 0.20) {
    return {
      regime: 'DEFENSIVE',
      long_bond: 10,
      mid_bond: 25,
      short_bond: 45,
      cash: 20,
      comment: '利率偏低，控制久期'
    };
  }

  return {
    regime: 'NEUTRAL',
    long_bond: 25,
    mid_bond: 35,
    short_bond: 30,
    cash: 10,
    comment: '中性配置'
  };
}

function rollingMean_(arr, windowSize) {
  if (!arr || !arr.length || windowSize <= 0) return null;

  var slice = arr.slice(Math.max(0, arr.length - windowSize));
  var sum = 0;
  var n = 0;

  for (var i = 0; i < slice.length; i++) {
    if (isFiniteNumber_(slice[i])) {
      sum += slice[i];
      n++;
    }
  }

  if (n < windowSize) return null;
  return sum / n;
}

function rollingPercentileRank_(arr, windowSize, currentValue) {
  if (!arr || !arr.length || windowSize <= 0 || !isFiniteNumber_(currentValue)) {
    return null;
  }

  var slice = arr.slice(Math.max(0, arr.length - windowSize));
  var n = 0;
  var leCount = 0;

  for (var i = 0; i < slice.length; i++) {
    var v = slice[i];
    if (!isFiniteNumber_(v)) continue;

    n++;
    if (v <= currentValue) leCount++;
  }

  if (n < windowSize) return null;
  return leCount / n;
}

function sortAndDedupeByDate_(rows, keyFn) {
  var map = {};
  for (var i = 0; i < rows.length; i++) {
    map[keyFn(rows[i])] = rows[i];
  }

  var deduped = Object.keys(map).map(function(k) {
    return map[k];
  });

  deduped.sort(function(a, b) {
    return a.dateObj.getTime() - b.dateObj.getTime();
  });

  return deduped;
}

function formatSignalSheet_(sheet) {
  var lastRow = sheet.getLastRow();
  var lastCol = sheet.getLastColumn();
  if (lastRow < 1 || lastCol < 1) return;

  sheet.getRange(1, 1, 1, lastCol).setFontWeight('bold').setBackground('#f3f6d8');
  if (lastRow >= 2) {
    sheet.getRange(2, 1, lastRow - 1, lastCol).setFontSize(10);
  }
  sheet.autoResizeColumns(1, lastCol);
}

function testSignalLast10_() {
  var ss = SpreadsheetApp.getActiveSpreadsheet();
  var sheet = mustGetSheet_(ss, SHEET_SIGNAL);
  buildSignal_();

  var values = sheet.getDataRange().getValues();
  Logger.log(values.slice(0, Math.min(values.length, 11)));
}

function classifyFundingView_(dr007) {
  if (!isFiniteNumber_(dr007)) return '资金未知';
  if (dr007 >= 1.90) return '资金偏紧';
  if (dr007 <= 1.60) return '资金偏松';
  return '资金中性';
}

function classifyCreditView_(creditSpread) {
  if (!isFiniteNumber_(creditSpread)) return '信用中性';
  if (creditSpread >= 0.45) return '信用利差偏宽，信用债性价比改善';
  if (creditSpread <= 0.36) return '信用利差偏窄，信用保护垫较薄';
  return '信用中性';
}

function mergeComment_(baseComment, fundingView, creditView) {
  var parts = [];
  if (baseComment) parts.push(baseComment);
  if (fundingView) parts.push(fundingView);
  if (creditView) parts.push(creditView);
  return parts.join('；');
}
