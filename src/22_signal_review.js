
/********************
 * 22_signal_review.js
 * 对关键信号做最小可用的方向有效性复盘。
 *
 * 设计原则：
 * 1) 先做原子信号，不做复杂组合回测
 * 2) 先看方向命中率和平均未来变化
 * 3) 复盘结果是观察工具，不是参数自动优化器
 ********************/

function buildSignalReview_() {
  SpreadsheetApp.flush();
  Utilities.sleep(300);

  var ss = withSpreadsheetRetry_(function() {
    return SpreadsheetApp.getActiveSpreadsheet();
  }, 'SpreadsheetApp.getActiveSpreadsheet');

  var metricsSheet = mustGetSheet_(ss, SHEET_METRICS);
  var detailSheet = mustGetSheet_(ss, SHEET_SIGNAL_DETAIL || '信号-明细');
  var reviewSheet = getOrCreateSheetByName_(ss, SHEET_SIGNAL_REVIEW || '信号-复盘');

  var metricsState = readMetricsRowsForReview_(metricsSheet);
  var detailRows = readSignalDetailRowsForReview_(detailSheet);
  var specs = buildSignalReviewSpecMap_();
  var horizons = (typeof SIGNAL_REVIEW_HORIZONS !== 'undefined' && SIGNAL_REVIEW_HORIZONS && SIGNAL_REVIEW_HORIZONS.length)
    ? SIGNAL_REVIEW_HORIZONS.slice()
    : [20, 60, 120];

  var aggregates = {};

  for (var i = 0; i < detailRows.length; i++) {
    var detail = detailRows[i];
    var spec = specs[detail.signal_code];
    if (!spec) continue;
    if (detail.signal_value === 'unknown' || detail.signal_value === 'note') continue;

    var idx = metricsState.indexByDate[detail.dateKey];
    if (idx == null) continue;

    for (var h = 0; h < horizons.length; h++) {
      var horizon = horizons[h];
      var futureIdx = idx + horizon;
      if (futureIdx >= metricsState.rows.length) continue;

      var current = metricsState.rows[idx];
      var future = metricsState.rows[futureIdx];
      var expected = spec.expectedDirection(detail.signal_value);

      if (expected == null) continue;

      var forwardChange = spec.computeForwardChange(current, future);
      if (!isFiniteNumber_(forwardChange)) continue;

      var hit = spec.isHit(expected, forwardChange, detail.signal_value);
      var key = detail.signal_code + '|' + detail.signal_value + '|' + horizon;
      if (!aggregates[key]) {
        aggregates[key] = {
          signal_code: detail.signal_code,
          signal_name: spec.signal_name,
          signal_value: detail.signal_value,
          horizon_days: horizon,
          target_metric: spec.target_metric,
          value_unit: spec.value_unit,
          hit_definition: spec.hit_definition,
          changes: [],
          hit_count: 0,
          sample_count: 0
        };
      }

      aggregates[key].changes.push(forwardChange);
      aggregates[key].sample_count += 1;
      if (hit) aggregates[key].hit_count += 1;
    }
  }

  var header = [[
    'signal_code',
    'signal_name',
    'signal_value',
    'horizon_days',
    'sample_count',
    'avg_forward_change',
    'median_forward_change',
    'win_rate',
    'target_metric',
    'value_unit',
    'hit_definition',
    'last_updated'
  ]];

  var rows = [];
  var keys = Object.keys(aggregates);
  keys.sort();

  var now = new Date();
  for (var j = 0; j < keys.length; j++) {
    var item = aggregates[keys[j]];
    if (!item.sample_count) continue;

    rows.push([
      item.signal_code,
      item.signal_name,
      item.signal_value,
      item.horizon_days,
      item.sample_count,
      meanNumberArray_(item.changes),
      medianNumberArray_(item.changes),
      item.hit_count / item.sample_count,
      item.target_metric,
      item.value_unit,
      item.hit_definition,
      now
    ]);
  }

  withSpreadsheetRetry_(function() {
    reviewSheet.clearContents();
    reviewSheet.getRange(1, 1, 1, header[0].length).setValues(header);
    return true;
  }, 'buildSignalReview_ header write');

  if (rows.length) {
    withSpreadsheetRetry_(function() {
      reviewSheet.getRange(2, 1, rows.length, rows[0].length).setValues(rows);
      reviewSheet.getRange(2, 4, rows.length, 1).setNumberFormat('0');
      reviewSheet.getRange(2, 5, rows.length, 1).setNumberFormat('0');
      reviewSheet.getRange(2, 6, rows.length, 2).setNumberFormat('0.00');
      reviewSheet.getRange(2, 8, rows.length, 1).setNumberFormat('0.0%');
      reviewSheet.getRange(2, 12, rows.length, 1).setNumberFormat('yyyy-mm-dd hh:mm');
      return true;
    }, 'buildSignalReview_ body write');
  }

  formatSignalSheet_(reviewSheet, { skipAutoResize: true });
  Logger.log((SHEET_SIGNAL_REVIEW || '信号-复盘') + ' 已重建，共 ' + rows.length + ' 行');
}

function readMetricsRowsForReview_(sheet) {
  var values = sheet.getDataRange().getValues();
  if (values.length < 2) {
    return { rows: [], indexByDate: {} };
  }

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
      gov_10y: readReviewMetricNum_(r, idx, 'gov_10y'),
      usd_cny: readReviewMetricNum_(r, idx, 'usd_cny'),
      usd_broad: readReviewMetricNum_(r, idx, 'usd_broad'),
      gold: readReviewMetricNum_(r, idx, 'gold'),
      wti: readReviewMetricNum_(r, idx, 'wti'),
      brent: readReviewMetricNum_(r, idx, 'brent'),
      copper: readReviewMetricNum_(r, idx, 'copper')
    });
  }

  rows.sort(function(a, b) {
    return a.dateObj.getTime() - b.dateObj.getTime();
  });

  var indexByDate = {};
  for (var j = 0; j < rows.length; j++) {
    indexByDate[rows[j].dateKey] = j;
  }

  return {
    rows: rows,
    indexByDate: indexByDate
  };
}

function readReviewMetricNum_(row, idx, colName) {
  var key = normalizeHeader_(colName);
  if (!(key in idx)) return null;
  return toNumberOrNull_(row[idx[key]]);
}

function readSignalDetailRowsForReview_(sheet) {
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
      signal_code: String(r[requireColumn_(idx, 'signal_code')] || ''),
      signal_value: String(r[requireColumn_(idx, 'signal_value')] || '')
    });
  }

  return rows;
}

function buildSignalReviewSpecMap_() {
  return {
    rates_strategy_tilt: {
      signal_name: '利率债策略',
      target_metric: '10Y国债收益率',
      value_unit: 'bp',
      hit_definition: '偏长信号看未来收益率是否下行；偏短信号看未来收益率是否上行；中性信号看未来是否维持窄幅波动',
      expectedDirection: function(signalValue) {
        if (!signalValue) return null;
        if (signalValue.indexOf('long') >= 0) return -1;
        if (signalValue.indexOf('shorter') >= 0 || signalValue.indexOf('short') >= 0) return 1;
        if (signalValue.indexOf('neutral') >= 0) return 0;
        return null;
      },
      computeForwardChange: function(current, future) {
        if (!isFiniteNumber_(current.gov_10y) || !isFiniteNumber_(future.gov_10y)) return null;
        return (future.gov_10y - current.gov_10y) * 100;
      },
      isHit: function(expected, deltaBp) {
        if (!isFiniteNumber_(deltaBp)) return false;
        if (expected === -1) return deltaBp <= -3;
        if (expected === 1) return deltaBp >= 3;
        return Math.abs(deltaBp) <= 5;
      }
    },

    view_fx_background: {
      signal_name: '汇率背景',
      target_metric: 'USD/CNY',
      value_unit: '%',
      hit_definition: '人民币偏弱信号看未来 USD/CNY 是否上行；人民币偏稳信号看未来 USD/CNY 是否回落或保持偏稳',
      expectedDirection: function(signalValue) {
        if (signalValue === 'rmb_weak') return 1;
        if (signalValue === 'rmb_stable') return -1;
        if (signalValue === 'neutral') return 0;
        return null;
      },
      computeForwardChange: function(current, future) {
        return computePctChange_(current.usd_cny, future.usd_cny);
      },
      isHit: function(expected, pct) {
        if (!isFiniteNumber_(pct)) return false;
        if (expected === 1) return pct >= 0.5;
        if (expected === -1) return pct <= -0.5;
        return Math.abs(pct) <= 1.0;
      }
    },

    view_usd_allocation_background: {
      signal_name: '美元配置背景',
      target_metric: '美元 broad 指数',
      value_unit: '%',
      hit_definition: '偏强信号看未来美元 broad 是否走强；偏弱信号看未来美元 broad 是否回落',
      expectedDirection: function(signalValue) {
        if (signalValue === 'usd_keep_hedge') return 1;
        if (signalValue === 'usd_not_urgent') return -1;
        if (signalValue === 'neutral') return 0;
        return null;
      },
      computeForwardChange: function(current, future) {
        return computePctChange_(current.usd_broad, future.usd_broad);
      },
      isHit: function(expected, pct) {
        if (!isFiniteNumber_(pct)) return false;
        if (expected === 1) return pct >= 0.5;
        if (expected === -1) return pct <= -0.5;
        return Math.abs(pct) <= 1.0;
      }
    },

    view_gold_background: {
      signal_name: '黄金背景',
      target_metric: '黄金',
      value_unit: '%',
      hit_definition: '偏强信号看未来黄金是否走强；偏弱信号看未来黄金是否回落；中性信号看未来是否维持窄幅波动',
      expectedDirection: function(signalValue) {
        if (signalValue === 'gold_strong') return 1;
        if (signalValue === 'gold_soft') return -1;
        if (signalValue === 'neutral') return 0;
        return null;
      },
      computeForwardChange: function(current, future) {
        return computePctChange_(current.gold, future.gold);
      },
      isHit: function(expected, pct) {
        if (!isFiniteNumber_(pct)) return false;
        if (expected === 1) return pct >= 1.5;
        if (expected === -1) return pct <= -1.5;
        return Math.abs(pct) <= 3.0;
      }
    },

    view_commodity_background: {
      signal_name: '顺周期商品背景',
      target_metric: '商品篮子（WTI/Brent/铜）',
      value_unit: '%',
      hit_definition: '偏强信号看未来商品篮子是否走强；偏弱信号看未来商品篮子是否回落；中性信号看未来是否维持窄幅波动',
      expectedDirection: function(signalValue) {
        if (signalValue === 'pro_cyclical_strong') return 1;
        if (signalValue === 'pro_cyclical_soft') return -1;
        if (signalValue === 'neutral') return 0;
        return null;
      },
      computeForwardChange: function(current, future) {
        var changes = [];
        pushIfFinite_(changes, computePctChange_(current.wti, future.wti));
        pushIfFinite_(changes, computePctChange_(current.brent, future.brent));
        pushIfFinite_(changes, computePctChange_(current.copper, future.copper));
        return changes.length ? meanNumberArray_(changes) : null;
      },
      isHit: function(expected, pct) {
        if (!isFiniteNumber_(pct)) return false;
        if (expected === 1) return pct >= 1.0;
        if (expected === -1) return pct <= -1.0;
        return Math.abs(pct) <= 2.5;
      }
    }
  };
}

function pushIfFinite_(arr, value) {
  if (isFiniteNumber_(value)) arr.push(value);
}

function computePctChange_(current, future) {
  if (!isFiniteNumber_(current) || !isFiniteNumber_(future) || current === 0) return null;
  return (future / current - 1) * 100;
}

function meanNumberArray_(values) {
  if (!values || !values.length) return null;
  var sum = 0;
  var count = 0;
  for (var i = 0; i < values.length; i++) {
    if (!isFiniteNumber_(values[i])) continue;
    sum += values[i];
    count += 1;
  }
  return count ? sum / count : null;
}

function medianNumberArray_(values) {
  if (!values || !values.length) return null;
  var arr = [];
  for (var i = 0; i < values.length; i++) {
    if (isFiniteNumber_(values[i])) arr.push(values[i]);
  }
  if (!arr.length) return null;
  arr.sort(function(a, b) { return a - b; });
  var mid = Math.floor(arr.length / 2);
  if (arr.length % 2) return arr[mid];
  return (arr[mid - 1] + arr[mid]) / 2;
}
