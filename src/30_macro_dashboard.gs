/********************
* 30_macro_dashboard.gs
* 一屏 Dashboard：KPI、曲线图、斜率图、DR007 vs 10Y。
********************/

function buildMacroDashboard_() {
  var ss = SpreadsheetApp.getActive();
  var sh = ss.getSheetByName(SHEET_MACRO) || ss.insertSheet(SHEET_MACRO);

  sh.clear();
  sh.setHiddenGridlines(true);
  sh.setFrozenRows(1);

  var widths = {
    1: 140, 2: 150, 3: 120, 4: 150, 5: 140, 6: 160, 7: 120, 8: 140,
    11: 95, 12: 95, 13: 95, 14: 95, 15: 95, 16: 95, 17: 95, 18: 95,
    19: 95, 20: 95, 21: 95, 22: 95, 23: 95, 24: 95, 25: 95, 26: 95,
    27: 95, 28: 95, 29: 95, 30: 95, 31: 95
  };
  Object.keys(widths).forEach(function (c) { sh.setColumnWidth(Number(c), widths[c]); });

  sh.getRange("A1:H1").merge()
    .setValue("利率终端（Macro Dashboard）")
    .setFontSize(16).setFontWeight("bold")
    .setHorizontalAlignment("left").setVerticalAlignment("middle");

  writeKPIBlock_(sh, "A3", ["日期", "DR007(加权)", "10Y国债", "1Y国债"], [
    '=MAX(curve_history!A:A)',
    '=IFERROR(INDEX(money_market!G:G, MATCH(MAX(money_market!A:A), money_market!A:A, 0)), "")',
    '=IFERROR(INDEX(curve_history!E:E, MATCH(MAX(curve_history!A:A), curve_history!A:A, 0)), "")',
    '=IFERROR(INDEX(curve_history!B:B, MATCH(MAX(curve_history!A:A), curve_history!A:A, 0)), "")'
  ]);

  writeKPIBlock_(sh, "E3", ["10Y-1Y", "政策利差", "信用利差", "T0 / TF0"], [
    '=IFERROR(INDEX(curve_slope!B:B, MATCH(MAX(curve_slope!A:A), curve_slope!A:A, 0)), "")',
    '=IFERROR(INDEX(rate_dashboard!H:H, MATCH(MAX(rate_dashboard!A:A), rate_dashboard!A:A, 0)), "")',
    '=IFERROR(INDEX(rate_dashboard!I:I, MATCH(MAX(rate_dashboard!A:A), rate_dashboard!A:A, 0)), "")',
    '=IFERROR(TEXT(INDEX(futures!B:B, MATCH(MAX(futures!A:A), futures!A:A, 0)),"0.00")&" / "&TEXT(INDEX(futures!C:C, MATCH(MAX(futures!A:A), futures!A:A, 0)),"0.00"), "")'
  ]);

  sh.getRange("B4:B6").setNumberFormat("0.0000");
  sh.getRange("F3:F5").setNumberFormat("0.0000");
  sh.getRange("B3").setNumberFormat("yyyy-mm-dd");

  sh.getRange("H3").setValue("ETF信号").setFontWeight("bold");
  sh.getRange("H4").setFormula('=IFERROR(INDEX(etf_signal!C:C, MATCH(MAX(etf_signal!A:A), etf_signal!A:A, 0)), "")')
    .setFontSize(14).setFontWeight("bold");

  sh.getRange("H5").setValue("配置状态").setFontWeight("bold");
  sh.getRange("H6").setFormula('=IFERROR(INDEX(bond_allocation_signal!G:G, MATCH(MAX(bond_allocation_signal!A:A), bond_allocation_signal!A:A, 0)), "")')
    .setFontSize(12).setFontWeight("bold");

  sh.getRange("I3").setValue("长债").setFontWeight("bold");
  sh.getRange("I4").setFormula('=IFERROR(INDEX(bond_allocation_signal!H:H, MATCH(MAX(bond_allocation_signal!A:A), bond_allocation_signal!A:A, 0)), "")');
  sh.getRange("I5").setValue("中债").setFontWeight("bold");
  sh.getRange("I6").setFormula('=IFERROR(INDEX(bond_allocation_signal!I:I, MATCH(MAX(bond_allocation_signal!A:A), bond_allocation_signal!A:A, 0)), "")');
  sh.getRange("J3").setValue("短债").setFontWeight("bold");
  sh.getRange("J4").setFormula('=IFERROR(INDEX(bond_allocation_signal!J:J, MATCH(MAX(bond_allocation_signal!A:A), bond_allocation_signal!A:A, 0)), "")');
  sh.getRange("J5").setValue("现金").setFontWeight("bold");
  sh.getRange("J6").setFormula('=IFERROR(INDEX(bond_allocation_signal!K:K, MATCH(MAX(bond_allocation_signal!A:A), bond_allocation_signal!A:A, 0)), "")');
  sh.getRange("I4:J6").setNumberFormat('0"%"');

  box_(sh, "A3:B6");
  box_(sh, "E3:F6");
  box_(sh, "H3:J6");

  setMacroConditionalFormats_(sh);

  // helper: 完整曲线
  sh.getRange("K1").setValue("helper_curve").setFontColor("#999999");
  sh.getRange("K2:AE2").setValues([[
    "Y_0","Y_0.08","Y_0.17","Y_0.25","Y_0.5","Y_0.75",
    "Y_1","Y_2","Y_3","Y_4","Y_5","Y_6","Y_7","Y_8","Y_9","Y_10",
    "Y_15","Y_20","Y_30","Y_40","Y_50"
  ]]);
  sh.getRange("K3").setFormula('=IFERROR(FILTER(yc_curve!C:W, yc_curve!A:A=MAX(yc_curve!A:A), yc_curve!B:B="国债"), )');

  insertOrReplaceChart_(sh, "国债收益率曲线（最新）", Charts.ChartType.LINE, sh.getRange("K2:AE3"), 1, 8);

  // helper: slope
  sh.getRange("K5").setValue("helper_slope").setFontColor("#999999");
  sh.getRange("K6:L6").setValues([["date", "10Y-1Y"]]).setFontWeight("bold");
  sh.getRange("K7").setFormula('=SORT(QUERY(curve_slope!A:B,"select A,B where A is not null order by A desc limit 180",0),1,TRUE)');

  insertOrReplaceChart_(sh, "10Y-1Y 斜率（近180日）", Charts.ChartType.LINE, sh.getRange("K6:L187"), 1, 23);

  // helper: DR007 vs 10Y
  sh.getRange("K24").setValue("helper_mm_vs_10y").setFontColor("#999999");
  sh.getRange("K25:M25").setValues([["date", "DR007", "10Y"]]).setFontWeight("bold");
  sh.getRange("K26").setFormula('=QUERY(curve_history!A:E,"select A where A is not null order by A desc limit 180",0)');
  sh.getRange("L26").setFormula('=ARRAYFORMULA(IF(K26:K="",,IFERROR(VLOOKUP(K26:K, {money_market!A:A, money_market!G:G}, 2, FALSE), )))');
  sh.getRange("M26").setFormula('=ARRAYFORMULA(IF(K26:K="",,IFERROR(VLOOKUP(K26:K, {curve_history!A:A, curve_history!E:E}, 2, FALSE), )))');

  insertOrReplaceChart_(sh, "DR007 vs 10Y（近180日）", Charts.ChartType.LINE, sh.getRange("K25:M206"), 9, 23);

  sh.getRange("A36").setValue("最近10天概览").setFontWeight("bold");
  sh.getRange("A37").setFormula('=QUERY({curve_history!A:A, curve_history!E:E, curve_slope!B:B},"select Col1,Col2,Col3 where Col1 is not null order by Col1 desc limit 10",0)');
  sh.getRange("E37").setFormula('=QUERY({money_market!A:A, money_market!G:G},"select Col1,Col2 where Col1 is not null order by Col1 desc limit 10",0)');
  sh.getRange("G37").setFormula('=QUERY({futures!A:A, futures!B:B, futures!C:C},"select Col1,Col2,Col3 where Col1 is not null order by Col1 desc limit 10",0)');
}

function writeKPIBlock_(sh, topLeftA1, labels, formulas) {
  var r = sh.getRange(topLeftA1);
  var row = r.getRow();
  var col = r.getColumn();

  for (var i = 0; i < labels.length; i++) {
    sh.getRange(row + i, col).setValue(labels[i]).setFontWeight("bold");
    sh.getRange(row + i, col + 1).setFormula(formulas[i]).setFontSize(14);
  }
  sh.getRange(row, col, labels.length, 2).setHorizontalAlignment("left");
}

function box_(sh, a1) {
  sh.getRange(a1).setBorder(true, true, true, true, true, true);
}

function insertOrReplaceChart_(sh, title, chartType, dataRange, col, row) {
  var builder = sh.newChart()
    .setChartType(chartType)
    .addRange(dataRange)
    .setOption("title", title)
    .setPosition(row, col, 0, 0);

  sh.insertChart(builder.build());
}

function setMacroConditionalFormats_(sh) {
  var rules = [];

  var steepCell = sh.getRange("F3");
  rules.push(
    SpreadsheetApp.newConditionalFormatRule()
      .whenNumberLessThan(0.20)
      .setBackground("#fce8e6")
      .setFontColor("#a50e0e")
      .setRanges([steepCell])
      .build()
  );
  rules.push(
    SpreadsheetApp.newConditionalFormatRule()
      .whenNumberGreaterThan(1.00)
      .setBackground("#e8f0fe")
      .setFontColor("#174ea6")
      .setRanges([steepCell])
      .build()
  );

  var sigCell = sh.getRange("H4");
  rules.push(
    SpreadsheetApp.newConditionalFormatRule()
      .whenTextContains("长债机会")
      .setBackground("#e6f4ea")
      .setFontColor("#137333")
      .setRanges([sigCell])
      .build()
  );
  rules.push(
    SpreadsheetApp.newConditionalFormatRule()
      .whenTextContains("短债优先")
      .setBackground("#fef7e0")
      .setFontColor("#b06000")
      .setRanges([sigCell])
      .build()
  );

  var regimeCell = sh.getRange("H6");
  rules.push(
    SpreadsheetApp.newConditionalFormatRule()
      .whenTextContains("STRONG_BUY_LONG_BOND")
      .setBackground("#d7f8e5")
      .setFontColor("#0b8043")
      .setRanges([regimeCell])
      .build()
  );
  rules.push(
    SpreadsheetApp.newConditionalFormatRule()
      .whenTextContains("BUY_LONG_BOND")
      .setBackground("#e6f4ea")
      .setFontColor("#137333")
      .setRanges([regimeCell])
      .build()
  );
  rules.push(
    SpreadsheetApp.newConditionalFormatRule()
      .whenTextContains("REDUCE_DURATION")
      .setBackground("#fef7e0")
      .setFontColor("#b06000")
      .setRanges([regimeCell])
      .build()
  );
  rules.push(
    SpreadsheetApp.newConditionalFormatRule()
      .whenTextContains("VERY_DEFENSIVE")
      .setBackground("#fce8e6")
      .setFontColor("#a50e0e")
      .setRanges([regimeCell])
      .build()
  );

  sh.setConditionalFormatRules(rules);
}


/********************
* 9) 历史回补
********************/
