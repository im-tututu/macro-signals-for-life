/********************
* 24_bond_allocation_signal.gs
* 基于 10Y、120日均线、250日分位、曲线斜率、DR007 生成债券配置建议。
*
* 输出 Sheet：bond_allocation_signal
* 字段：
*   - date        日期
*   - 10Y         国债10Y收益率
*   - MA120       10Y最近120个有效样本均值
*   - pct250      10Y在最近250个有效样本中的分位（0~1）
*   - slope10_1   10Y-1Y 曲线斜率
*   - dr007       DR007 加权利率
*   - regime      配置状态
*   - long_bond   长债建议比例
*   - mid_bond    中债建议比例
*   - short_bond  短债建议比例
*   - cash        现金建议比例
*   - comment     解释说明
********************/



/***************************************
 * 30_bond_allocation_signal.gs
 * 修正版：先排序，再按历史窗口计算 MA120 / pct250
 ***************************************/

function runBondAllocationSignal_() {
  buildBondAllocationSignal_();
}

/**
 * 主函数：重建 bond_allocation_signal
 */
function buildBondAllocationSignal_() {
  var ss = SpreadsheetApp.getActiveSpreadsheet();

  var curveHistorySheet = mustGetSheet_(ss, 'curve_history');
  var curveSlopeSheet   = mustGetSheet_(ss, 'curve_slope');
  var moneyMarketSheet  = mustGetSheet_(ss, 'money_market');
  var outSheet          = mustGetSheet_(ss, 'bond_allocation_signal');

  // ===== 1) 读取并整理 curve_history（最关键） =====
  var curveRows = readCurveHistoryRows_(curveHistorySheet);

  if (!curveRows.length) {
    throw new Error('curve_history 无有效数据。');
  }

  // 按日期升序 + 重复日期去重（保留最后一个）
  curveRows = sortAndDedupeByDate_(curveRows, function (r) { return r.dateKey; });

  // ===== 2) 读取辅助表，做 date -> value 映射 =====
  var slopeMap = readCurveSlopeMap_(curveSlopeSheet);     // dateKey -> slope10_1
  var dr007Map = readMoneyMarketDr007Map_(moneyMarketSheet); // dateKey -> dr007

  // ===== 3) 逐日按“截至当日历史窗口”计算 =====
  var resultsAsc = [];
  var tenYHistory = [];

  for (var i = 0; i < curveRows.length; i++) {
    var row = curveRows[i];
    var tenY = row.gov10y;

    if (!isFiniteNumber_(tenY)) {
      continue;
    }

    // 截至当日的历史序列（先把今天放进去）
    tenYHistory.push(tenY);

    var ma120 = rollingMean_(tenYHistory, 120);

    // pct250 定义：
    // 当前值在“最近250个历史值”中的百分位，值越低 -> 分位越低
    // 例如当前是最低，则接近 1/N；当前是最高，则 = 1
    var pct250 = rollingPercentileRank_(tenYHistory, 250, tenY);

    var slope10_1 = slopeMap[row.dateKey];
    var dr007 = dr007Map[row.dateKey];

    var regimeObj = classifyBondRegime_({
      tenY: tenY,
      ma120: ma120,
      pct250: pct250,
      slope10_1: slope10_1,
      dr007: dr007
    });

    resultsAsc.push([
      row.dateObj,                  // date
      tenY,                         // 10Y
      ma120,                        // MA120
      pct250,                       // pct250
      slope10_1,                    // slope10_1
      dr007,                        // dr007
      regimeObj.regime,             // regime
      regimeObj.long_bond,          // long_bond
      regimeObj.mid_bond,           // mid_bond
      regimeObj.short_bond,         // short_bond
      regimeObj.cash,               // cash
      regimeObj.comment             // comment
    ]);
  }

  // ===== 4) 输出前改为倒序（最新在上） =====
  var resultsDesc = resultsAsc.slice().reverse();

  // ===== 5) 写回表 =====
  var header = [[
    'date',
    '10Y',
    'MA120',
    'pct250',
    'slope10_1',
    'dr007',
    'regime',
    'long_bond',
    'mid_bond',
    'short_bond',
    'cash',
    'comment'
  ]];

  outSheet.clearContents();
  //outSheet.clearFormats();

  outSheet.getRange(1, 1, 1, header[0].length).setValues(header);

  if (resultsDesc.length > 0) {
    outSheet.getRange(2, 1, resultsDesc.length, resultsDesc[0].length).setValues(resultsDesc);
  }

  formatBondAllocationSignalSheet_(outSheet, resultsDesc.length + 1);
}

/**
 * 读取 curve_history
 * 预期表头：
 * date | gov_1y | gov_3y | gov_5y | gov_10y
 */
function readCurveHistoryRows_(sheet) {
  var values = sheet.getDataRange().getValues();
  if (values.length < 2) return [];

  var header = values[0];
  var idx = buildHeaderIndex_(header);

  var dateCol  = idx['date'];
  var gov1yCol = idx['gov_1y'];
  var gov3yCol = idx['gov_3y'];
  var gov5yCol = idx['gov_5y'];
  var gov10yCol = idx['gov_10y'];

  requireColumn_(dateCol,  'curve_history.date');
  requireColumn_(gov10yCol, 'curve_history.gov_10y');

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
      gov10y: gov10y
    });
  }

  return rows;
}

/**
 * 读取 curve_slope 映射
 * 预期表头：
 * date | 10Y-1Y | 10Y-3Y | 5Y-1Y
 */
function readCurveSlopeMap_(sheet) {
  var values = sheet.getDataRange().getValues();
  if (values.length < 2) return {};

  var header = values[0];
  var idx = buildHeaderIndex_(header);

  var dateCol = idx['date'];
  var slopeCol = idx['10y-1y'];

  requireColumn_(dateCol, 'curve_slope.date');
  requireColumn_(slopeCol, 'curve_slope.10Y-1Y');

  var map = {};

  for (var i = 1; i < values.length; i++) {
    var r = values[i];
    var dateObj = normalizeSheetDate_(r[dateCol]);
    var slope = toNumberOrNull_(r[slopeCol]);
    if (!dateObj || !isFiniteNumber_(slope)) continue;

    map[formatDateKey_(dateObj)] = slope;
  }

  return map;
}

/**
 * 读取 money_market 的 DR007_weightedRate
 * 预期表头至少包含：
 * date | DR007_weightedRate
 */
function readMoneyMarketDr007Map_(sheet) {
  var values = sheet.getDataRange().getValues();
  if (values.length < 2) return {};

  var header = values[0];
  var idx = buildHeaderIndex_(header);

  var dateCol = idx['date'];
  var dr007Col = idx['dr007_weightedrate'];

  requireColumn_(dateCol, 'money_market.date');
  requireColumn_(dr007Col, 'money_market.DR007_weightedRate');

  var map = {};

  for (var i = 1; i < values.length; i++) {
    var r = values[i];
    var dateObj = normalizeLooseDate_(r[dateCol]); // money_market 里可能是 yyyy-mm-dd 字符串
    var dr007 = toNumberOrNull_(r[dr007Col]);
    if (!dateObj || !isFiniteNumber_(dr007)) continue;

    map[formatDateKey_(dateObj)] = dr007;
  }

  return map;
}

/**
 * regime 分类逻辑
 * 可按你的偏好再调阈值
 */
function classifyBondRegime_(x) {
  var tenY = x.tenY;
  var ma120 = x.ma120;
  var pct250 = x.pct250;
  var slope10_1 = x.slope10_1;
  var dr007 = x.dr007;

  // 容错：如果辅助变量缺失，退化为中性
  if (!isFiniteNumber_(tenY) || !isFiniteNumber_(ma120) || !isFiniteNumber_(pct250)) {
    return {
      regime: 'NEUTRAL',
      long_bond: 25,
      mid_bond: 35,
      short_bond: 30,
      cash: 10,
      comment: '数据不足，中性配置'
    };
  }

  // 资金面是否偏紧
  var fundingTight = isFiniteNumber_(dr007) && dr007 >= 1.80;

  // 曲线是否偏平：10Y-1Y 越小，曲线越平，长债相对更有性价比
  var curveFlat = isFiniteNumber_(slope10_1) && slope10_1 <= 0.50;

  // 1) 利率很低、且低于均线 -> 防御
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

  // 2) 利率高位、曲线偏平、资金面不紧 -> 强配长债
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

  // 3) 利率高位但条件没那么强 -> 偏长久期
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

  // 4) 利率较低但还没到最极端 -> 偏防御
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

  // 5) 默认中性
  return {
    regime: 'NEUTRAL',
    long_bond: 25,
    mid_bond: 35,
    short_bond: 30,
    cash: 10,
    comment: '中性配置'
  };
}

/**
 * 滚动均值：取最近 window 个值，不足则取全部可用值
 */
function rollingMean_(arr, windowSize) {
  var slice = arr.slice(Math.max(0, arr.length - windowSize));
  if (!slice.length) return null;

  var sum = 0;
  var n = 0;
  for (var i = 0; i < slice.length; i++) {
    if (isFiniteNumber_(slice[i])) {
      sum += slice[i];
      n++;
    }
  }
  return n ? sum / n : null;
}

/**
 * 滚动百分位排名
 * 返回区间 (0, 1]：
 * - 当前值是窗口最低值 => 1/N
 * - 当前值是窗口最高值 => 1
 *
 * 定义：窗口内 <= current 的个数 / 窗口样本数
 */
function rollingPercentileRank_(arr, windowSize, currentValue) {
  var slice = arr.slice(Math.max(0, arr.length - windowSize));
  if (!slice.length || !isFiniteNumber_(currentValue)) return null;

  var n = 0;
  var leCount = 0;

  for (var i = 0; i < slice.length; i++) {
    var v = slice[i];
    if (!isFiniteNumber_(v)) continue;
    n++;
    if (v <= currentValue) leCount++;
  }

  if (!n) return null;
  return leCount / n;
}

/**
 * 按日期排序并对重复日期去重
 * 保留同一 dateKey 的最后一个元素
 */
function sortAndDedupeByDate_(rows, keyFn) {
  var map = {};
  for (var i = 0; i < rows.length; i++) {
    map[keyFn(rows[i])] = rows[i];
  }

  var deduped = Object.keys(map).map(function (k) { return map[k]; });

  deduped.sort(function (a, b) {
    return a.dateObj.getTime() - b.dateObj.getTime();
  });

  return deduped;
}

/**
 * 输出格式
 */
function formatBondAllocationSignalSheet_(sheet, lastRow) {
  if (lastRow < 1) lastRow = 1;

  sheet.setFrozenRows(1);

  sheet.getRange(1, 1, 1, 12)
    .setFontWeight('bold')
    .setBackground('#d9eaf7');

  sheet.getRange(2, 1, Math.max(0, lastRow - 1), 1).setNumberFormat('yyyy-mm-dd');
  sheet.getRange(2, 2, Math.max(0, lastRow - 1), 5).setNumberFormat('0.0000');
  sheet.getRange(2, 8, Math.max(0, lastRow - 1), 4).setNumberFormat('0');

  //sheet.autoResizeColumns(1, 12);
}

/**
 * 调试：打印最近若干天
 */
function testBondAllocationSignalLast10_() {
  var ss = SpreadsheetApp.getActiveSpreadsheet();
  var sheet = mustGetSheet_(ss, 'bond_allocation_signal');
  buildBondAllocationSignal_();

  var values = sheet.getDataRange().getValues();
  Logger.log(values.slice(0, Math.min(values.length, 11)));
}

