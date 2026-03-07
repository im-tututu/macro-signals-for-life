/********************
* 20_rate_dashboard.gs
* 从 yc_curve 生成利差面板 rate_dashboard。
********************/

function updateDashboard_() {
  var ss = SpreadsheetApp.getActive();
  var src = ss.getSheetByName(SHEET_CURVE);
  if (!src) return;

  var dst = ss.getSheetByName(SHEET_DASH) || ss.insertSheet(SHEET_DASH);

  ensureDashboardHeader_(dst);
  var dashIndex = buildDateIndex_(dst, 0);

  var data = src.getDataRange().getValues();
  if (data.length < 2) return;

  var header = data[0];
  var idxY1 = header.indexOf("Y_1");
  var idxY3 = header.indexOf("Y_3");
  var idxY5 = header.indexOf("Y_5");
  var idxY10 = header.indexOf("Y_10");

  if (idxY1 < 0 || idxY5 < 0 || idxY10 < 0) {
    Logger.log("❌ yc_curve 缺少 Y_1/Y_5/Y_10 列");
    return;
  }

  var byDate = {};
  for (var i = 1; i < data.length; i++) {
    var r = data[i];
    var d = r[0];
    var c = r[1];
    if (!d || !c) continue;

    var key = normYMD_(d);
    if (!byDate[key]) byDate[key] = {};
    byDate[key][c] = r;
  }

  var inserted = 0, skipped = 0;

  Object.keys(byDate).forEach(function (d) {
    if (dashIndex.has(d)) {
      skipped++;
      return;
    }

    var g = byDate[d]["国债"];
    var cdb = byDate[d]["国开债"];
    var aaa = byDate[d]["AAA信用"];

    if (!g || !cdb || !aaa) return;

    var gov1 = g[idxY1];
    var gov3 = idxY3 >= 0 ? g[idxY3] : "";
    var gov5 = g[idxY5];
    var gov10 = g[idxY10];
    var cdb10 = cdb[idxY10];
    var aaa5 = aaa[idxY5];

    if (gov1 === "" || gov5 === "" || gov10 === "" || cdb10 === "" || aaa5 === "") return;

    var policy = cdb10 - gov10;
    var credit = aaa5 - gov5;
    var term = gov10 - gov1;

    dst.appendRow([
      d,
      gov1, gov3, gov5, gov10,
      cdb10, aaa5,
      policy, credit, term
    ]);

    inserted++;
  });

  Logger.log("rate_dashboard 新增=" + inserted + " 跳过=" + skipped);
}

function ensureDashboardHeader_(sheet) {
  if (sheet.getLastRow() > 0) return;
  sheet.appendRow([
    "date",
    "gov_1y",
    "gov_3y",
    "gov_5y",
    "gov_10y",
    "cdb_10y",
    "aaa_5y",
    "policy_spread",
    "credit_spread",
    "term_spread"
  ]);
}

function buildDateIndex_(sheet, dateCol0Based) {
  var last = sheet.getLastRow();
  var set = new Set();
  if (last < 2) return set;

  var values = sheet.getRange(2, 1 + dateCol0Based, last - 1, 1).getValues();
  for (var i = 0; i < values.length; i++) {
    var d = values[i][0];
    if (!d) continue;
    set.add(normYMD_(d));
  }
  return set;
}


/********************
* 3) curve_history：国债 1/3/5/10Y 历史
********************/
