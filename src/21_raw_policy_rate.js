/********************
 * 21_raw_policy_rate.js
 * 政策利率原始事件表。
 *
 * 职责：
 * 1) 调用 12_source_pbc.js 抓取 PBC 公告
 * 2) 维护事件表落表、去重、排序
 * 3) 暴露最新同步与历史回补入口
 ********************/

var RAW_POLICY_RATE_HEADERS = [
  "date",
  "type",
  "term",
  "rate",
  "amount",
  "source",
  "fetched_at",
  "note"
];

/* =========================
 * 对外入口
 * ========================= */


function initRawPolicyRateSheet() {
  prEnsureRawPolicyRateSheet_();
  Logger.log("已初始化表：" + SHEET_POLICY_RATE_RAW);
}


function testFetchLatestPolicyRates() {
  var events = prFetchLatestPolicyRateEvents_();
  Logger.log(JSON.stringify(events, null, 2));
}


function syncRawPolicyRateLatest() {
  var sheet = prEnsureRawPolicyRateSheet_();
  var events = prFetchLatestPolicyRateEvents_();
  var inserted = 0;
  var updated = 0;

  for (var i = 0; i < events.length; i++) {
    var e = events[i];
    if (!e.date || !e.type || !e.term) continue;

    var existed = prFindPolicyRateEventRow_(sheet, e.date, e.type, e.term);
    prUpsertPolicyRateEvent_(sheet, e);
    if (existed) updated++;
    else inserted++;
  }

  prSortRawPolicyRateSheet_(sheet);
  Logger.log("政策利率最新事件同步完成 inserted=" + inserted + ", updated=" + updated);

  return {
    message: 'policy rate sync done',
    stats: {
      inserted_rows: inserted,
      updated_rows: updated,
      skipped_rows: 0,
      failed_rows: 0,
      changed_points: inserted + updated
    },
    detail: {
      event_count: events.length
    }
  };  
}


function backfillPolicyRateRecent(omoLimit, mlfLimit, lprLimit) {
  omoLimit = Number(omoLimit || 30);
  mlfLimit = Number(mlfLimit || 200);
  lprLimit = Number(lprLimit || 240);

  var sheet = prEnsureRawPolicyRateSheet_();
  var events = []
    .concat(prFetchRecentOmoEvents_(omoLimit))
    .concat(prFetchRecentMlfEvents_(mlfLimit))
    .concat(prFetchRecentLprEvents_(lprLimit));

  var inserted = 0;
  var updated = 0;

  for (var i = 0; i < events.length; i++) {
    var e = events[i];
    if (!e.date || !e.type || !e.term) continue;

    var existed = prFindPolicyRateEventRow_(sheet, e.date, e.type, e.term);
    prUpsertPolicyRateEvent_(sheet, e);
    if (existed) updated++;
    else inserted++;
  }

  prSortRawPolicyRateSheet_(sheet);
  Logger.log("政策利率近期事件回补完成 inserted=" + inserted + ", updated=" + updated + ", total=" + events.length);
}


function readRawPolicyRateEvents_() {
  var sheet = prEnsureRawPolicyRateSheet_();
  return prReadPolicyRateRows_(sheet);
}

/* =========================
 * 最新抓取
 * ========================= */


function prEnsureRawPolicyRateSheet_() {
  var ss = SpreadsheetApp.getActiveSpreadsheet();
  var sheet = ss.getSheetByName(SHEET_POLICY_RATE_RAW);
  if (!sheet) sheet = ss.insertSheet(SHEET_POLICY_RATE_RAW);

  var needInit = false;

  if (sheet.getLastRow() < 1) {
    needInit = true;
  } else {
    var existing = sheet.getRange(1, 1, 1, RAW_POLICY_RATE_HEADERS.length).getValues()[0];
    for (var i = 0; i < RAW_POLICY_RATE_HEADERS.length; i++) {
      if (String(existing[i] || "") !== RAW_POLICY_RATE_HEADERS[i]) {
        needInit = true;
        break;
      }
    }
  }

  if (needInit) {
    sheet.clear();
    sheet.getRange(1, 1, 1, RAW_POLICY_RATE_HEADERS.length).setValues([RAW_POLICY_RATE_HEADERS]);
    sheet.setFrozenRows(1);
  }

  return sheet;
}


function prReadPolicyRateRows_(sheet) {
  var lastRow = sheet.getLastRow();
  if (lastRow < 2) return [];

  var vals = sheet.getRange(2, 1, lastRow - 1, RAW_POLICY_RATE_HEADERS.length).getValues();
  var rows = [];

  for (var i = 0; i < vals.length; i++) {
    var v = vals[i];
    if (!v[0]) continue;

    rows.push({
      _rowNum: i + 2,
      date: String(v[0] || ""),
      type: String(v[1] || ""),
      term: String(v[2] || ""),
      rate: v[3],
      amount: v[4],
      source: String(v[5] || ""),
      fetched_at: v[6],
      note: String(v[7] || "")
    });
  }

  rows.sort(function(a, b) {
    if (a.date !== b.date) return a.date < b.date ? -1 : 1;
    if (a.type !== b.type) return a.type < b.type ? -1 : 1;
    return a.term < b.term ? -1 : a.term > b.term ? 1 : 0;
  });

  return rows;
}


function prFindPolicyRateEventRow_(sheet, date, type, term) {
  var rows = prReadPolicyRateRows_(sheet);
  for (var i = 0; i < rows.length; i++) {
    if (rows[i].date === date && rows[i].type === type && rows[i].term === term) {
      return rows[i]._rowNum;
    }
  }
  return null;
}


function prUpsertPolicyRateEvent_(sheet, rowObj) {
  if (!rowObj || !rowObj.date || !rowObj.type || !rowObj.term) return;

  var rowNum = prFindPolicyRateEventRow_(sheet, rowObj.date, rowObj.type, rowObj.term);

  var arr = [[
    rowObj.date,
    rowObj.type,
    rowObj.term,
    rowObj.rate,
    rowObj.amount,
    rowObj.source,
    rowObj.fetched_at,
    rowObj.note
  ]];

  if (rowNum) {
    sheet.getRange(rowNum, 1, 1, RAW_POLICY_RATE_HEADERS.length).setValues(arr);
  } else {
    sheet.getRange(sheet.getLastRow() + 1, 1, 1, RAW_POLICY_RATE_HEADERS.length).setValues(arr);
  }
}


function prSortRawPolicyRateSheet_(sheet) {
  var lastRow = sheet.getLastRow();
  if (lastRow <= 2) return;

  sheet.getRange(2, 1, lastRow - 1, RAW_POLICY_RATE_HEADERS.length)
    .sort([
      { column: 1, ascending: true },
      { column: 2, ascending: true },
      { column: 3, ascending: true }
    ]);
}

/* =========================
 * 工具函数
 * ========================= */


function prDedupePolicyRateEvents_(events) {
  var out = [];
  var seen = {};

  for (var i = 0; i < events.length; i++) {
    var e = events[i];
    var key = e.date + "||" + e.type + "||" + e.term;
    if (seen[key]) continue;
    seen[key] = true;
    out.push(e);
  }

  return out;
}

