/********************
 * 52_notify_log.js
 * 运行日志、通知与信号复盘。
 ********************/


/**
 * 统一记录任务结果。
 */
function logJobResult_(jobName, status, message, detail) {
  var sheet = ensureSheet_(SHEET_RUN_LOG);
  if (sheet.getLastRow() === 0) {
    sheet.appendRow(['timestamp', 'job_name', 'status', 'message', 'detail']);
  }
  sheet.appendRow([
    formatDateTime_(new Date()),
    jobName,
    status,
    message || '',
    detail ? JSON.stringify(detail) : ''
  ]);
}

/**
 * 可选邮件通知：
 * - 若 Script Properties 里配置 ALERT_EMAIL，则发送
 * - 未配置时仅写 Logger，不报错
 */
function notifyText_(subject, body) {
  var email = PropertiesService.getScriptProperties().getProperty('ALERT_EMAIL');
  if (!email) {
    Logger.log(subject + ' | ' + body);
    return;
  }
  MailApp.sendEmail(email, subject, body);
}

/**
 * 构建简版复盘表。
 * 当前直接从“信号-主要”抽取最近数据，保留核心观察字段。
 */
function buildSignalReview_() {
  var ss = SpreadsheetApp.getActive();
  var mainSheet = ss.getSheetByName(SHEET_SIGNAL_MAIN);
  var reviewSheet = ensureSheet_(ss, SHEET_SIGNAL_REVIEW);

  var header = [
    'date',
    'liquidity_regime',
    'rates_strategy_tilt',
    'rates_rv_ranking',
    'credit_strategy_tilt',
    'view_fx_background',
    'view_gold_background',
    'view_commodity_background',
    'view_household_allocation',
    'comment_household'
  ];

  if (!mainSheet || mainSheet.getLastRow() <= 1) {
    reviewSheet.clearContents();
    reviewSheet.getRange(1, 1, 1, header.length).setValues([header]);
    return;
  }

  var values = mainSheet.getDataRange().getValues();
  var srcHeader = buildHeaderIndex_(values[0]);

  var picked = [];
  for (var i = 1; i < values.length; i++) {
    var row = values[i];
    picked.push([
      row[requireColumn_(srcHeader, 'date')],
      row[requireColumn_(srcHeader, 'liquidity_regime')],
      row[requireColumn_(srcHeader, 'rates_strategy_tilt')],
      row[requireColumn_(srcHeader, 'rates_rv_ranking')],
      row[requireColumn_(srcHeader, 'credit_strategy_tilt')],
      row[requireColumn_(srcHeader, 'view_fx_background')],
      row[requireColumn_(srcHeader, 'view_gold_background')],
      row[requireColumn_(srcHeader, 'view_commodity_background')],
      row[requireColumn_(srcHeader, 'view_household_allocation')],
      row[requireColumn_(srcHeader, 'comment_household')]
    ]);
  }

  reviewSheet.clearContents();
  reviewSheet.getRange(1, 1, 1, header.length).setValues([header]);
  if (picked.length) {
    reviewSheet.getRange(2, 1, picked.length, header.length).setValues(picked);
  }
  formatSignalSheet_(reviewSheet, { frozenRows: 1 });
}

/**
 * 最近 N 天复盘的简便入口。
 */
function buildSignalRecent_(days) {
  days = Number(days || 7);
  buildSignalReview_();

  var sheet = mustGetSheet_(SHEET_SIGNAL_REVIEW);
  if (sheet.getLastRow() <= 1) return;

  var values = sheet.getDataRange().getValues();
  var out = [values[0]];
  var cutoff = new Date();
  cutoff.setDate(cutoff.getDate() - days);

  for (var i = 1; i < values.length; i++) {
    var d = normalizeLooseDate_(values[i][0]);
    if (d && d >= cutoff) out.push(values[i]);
  }

  sheet.clearContents();
  sheet.getRange(1, 1, out.length, out[0].length).setValues(out);
  formatSignalSheet_(sheet, { frozenRows: 1 });
}
