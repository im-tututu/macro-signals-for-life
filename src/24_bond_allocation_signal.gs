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

function buildBondAllocationSignal_() {
  var ss = SpreadsheetApp.getActive();
  var hist = ss.getSheetByName(SHEET_HIST);
  var slopeSh = ss.getSheetByName(SHEET_SLOPE);
  var mm = ss.getSheetByName(SHEET_MM);
  var out = ss.getSheetByName(SHEET_ALLOC) || ss.insertSheet(SHEET_ALLOC);

  out.clearContents();
  out.appendRow([
    "date", "10Y", "MA120", "pct250", "slope10_1", "dr007",
    "regime", "long_bond", "mid_bond", "short_bond", "cash", "comment"
  ]);

  if (!hist || !slopeSh) return;

  var histData = hist.getDataRange().getValues();
  var slopeData = slopeSh.getDataRange().getValues();
  if (histData.length < 2 || slopeData.length < 2) return;

  // curve_history: A=date, E=10Y
  var rows = [];
  for (var i = 1; i < histData.length; i++) {
    var d = histData[i][0];
    var y10 = histData[i][4];
    if (d && y10 !== "") {
      rows.push([normYMD_(d), Number(y10)]);
    }
  }
  if (rows.length < 30) return;

  // curve_slope: A=date, B=10Y-1Y
  var slopeMap = {};
  for (var j = 1; j < slopeData.length; j++) {
    var ds = normYMD_(slopeData[j][0]);
    slopeMap[ds] = slopeData[j][1];
  }

  // money_market: G列 = DR007_weightedRate
  var dr007Map = {};
  if (mm && mm.getLastRow() >= 2) {
    var mmData = mm.getDataRange().getValues();
    for (var k = 1; k < mmData.length; k++) {
      var md = normYMD_(mmData[k][0]);
      dr007Map[md] = mmData[k][6];
    }
  }

  for (var t = 0; t < rows.length; t++) {
    var date = rows[t][0];
    var y10Now = rows[t][1];

    // 最近120个有效样本均值
    var start120 = Math.max(0, t - 119);
    var arr120 = [];
    for (var a = start120; a <= t; a++) arr120.push(rows[a][1]);
    var ma120 = average_(arr120);

    // 最近250个有效样本分位
    var start250 = Math.max(0, t - 249);
    var arr250 = [];
    for (var b = start250; b <= t; b++) arr250.push(rows[b][1]);
    var pct250 = percentileRank_(arr250, y10Now);

    var slope = slopeMap[date] !== undefined ? Number(slopeMap[date]) : "";
    var dr007 = dr007Map[date] !== undefined ? Number(dr007Map[date]) : "";

    var alloc = computeBondAllocation_(y10Now, ma120, pct250, slope, dr007);

    out.appendRow([
      date,
      y10Now,
      ma120,
      pct250,
      slope,
      dr007,
      alloc.regime,
      alloc.long_bond,
      alloc.mid_bond,
      alloc.short_bond,
      alloc.cash,
      alloc.comment
    ]);
  }
}

function computeBondAllocation_(y10, ma120, pct250, slope, dr007) {
  var regime = "NEUTRAL";
  var longBond = 25, midBond = 35, shortBond = 30, cash = 10;
  var comment = "中性配置";

  if (pct250 <= 0.2 && y10 < ma120) {
    regime = "VERY_DEFENSIVE";
    longBond = 0; midBond = 20; shortBond = 50; cash = 30;
    comment = "利率低位且弱于均线，久期偏贵";
  } else if (pct250 <= 0.3) {
    regime = "REDUCE_DURATION";
    longBond = 10; midBond = 30; shortBond = 40; cash = 20;
    comment = "利率偏低，降低久期";
  } else if (pct250 >= 0.8 && slope !== "" && slope <= 0.5) {
    regime = "STRONG_BUY_LONG_BOND";
    longBond = 70; midBond = 20; shortBond = 10; cash = 0;
    comment = "利率高位且曲线偏平，长债性价比高";
  } else if (pct250 >= 0.7) {
    regime = "BUY_LONG_BOND";
    longBond = 50; midBond = 30; shortBond = 20; cash = 0;
    comment = "利率偏高，可偏长久期";
  }

  // 资金面偏紧时，小幅降低长债比例
  if (dr007 !== "" && dr007 > 2.2 && longBond >= 50) {
    longBond -= 10;
    shortBond += 10;
    comment += "；资金面偏紧，小幅降久期";
  }

  return {
    regime: regime,
    long_bond: longBond,
    mid_bond: midBond,
    short_bond: shortBond,
    cash: cash,
    comment: comment
  };
}

function average_(arr) {
  if (!arr || arr.length === 0) return "";
  var s = 0;
  for (var i = 0; i < arr.length; i++) s += Number(arr[i]);
  return s / arr.length;
}

function percentileRank_(arr, value) {
  if (!arr || arr.length === 0) return "";
  var lessOrEqual = 0;
  for (var i = 0; i < arr.length; i++) {
    if (Number(arr[i]) <= Number(value)) lessOrEqual++;
  }
  return lessOrEqual / arr.length;
}
