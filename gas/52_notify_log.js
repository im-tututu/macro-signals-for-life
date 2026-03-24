/********************
 * 52_notify_log.gs
 * 运行日志、通知与信号复盘。
 ********************/

/**
 * 统一记录任务结果。
 *
 * 日志字段说明：
 * - inserted_rows: 新增行数
 * - updated_rows: 更新行数
 * - skipped_rows: 跳过行数
 * - failed_rows: 失败条数
 * - changed_points: 更新的数据点/节点数（可选）
 * - source_date: 本次写入对应的业务日期（可选）
 */
function logJobResult_(jobName, status, message, detail, stats) {
  var sheet = ensureSheet_(SHEET_RUN_LOG);

  if (sheet.getLastRow() === 0) {
    sheet.appendRow([
      'timestamp',
      'job_name',
      'status',
      'message',
      'inserted_rows',
      'updated_rows',
      'skipped_rows',
      'failed_rows',
      'changed_points',
      'source_date',
      'detail'
    ]);
  }

  stats = stats || {};

  sheet.appendRow([
    formatDateTime_(new Date()),
    jobName,
    status,
    message || '',
    Number(stats.inserted_rows || 0),
    Number(stats.updated_rows || 0),
    Number(stats.skipped_rows || 0),
    Number(stats.failed_rows || 0),
    Number(stats.changed_points || 0),
    stats.source_date || '',
    detail ? JSON.stringify(detail) : ''
  ]);
}

/**
 * 一次性写入 Bark 配置。
 *
 * 用法：
 * - 首次接入 Bark 后手工执行一次
 * - 建议后续把 key 改为你自己在 Script Properties 中维护
 */
function setBarkNotifyConfig_() {
  PropertiesService.getScriptProperties().setProperties({
    NOTIFY_PROVIDER: 'bark',
    BARK_SERVER: 'https://api.day.app',
    BARK_DEVICE_KEY: 'AvNnUJ93Kf4XT4DwUmia7B',
    BARK_GROUP: 'macro-signals',
    BARK_LEVEL: 'timeSensitive',

    // success 通知策略：
    // - changed: 只有有新增/更新时才通知
    // - always: 每次成功都通知
    // - never: 成功不通知
    NOTIFY_SUCCESS_MODE: 'changed'
  }, true);

  Logger.log('Bark notify config saved.');
}

/**
 * 统一通知入口：
 * - provider=bark 时走 Bark
 * - 否则若配置 ALERT_EMAIL，则发邮件
 * - 都未配置时仅写 Logger
 */
function notifyText_(subject, body, options) {
  options = options || {};

  var props = PropertiesService.getScriptProperties();
  var provider = String(props.getProperty('NOTIFY_PROVIDER') || '').toLowerCase();

  if (provider === 'bark') {
    return notifyByBark_(subject, body, options);
  }

  var email = props.getProperty('ALERT_EMAIL');
  if (!email) {
    Logger.log(subject + ' | ' + body);
    return;
  }

  MailApp.sendEmail(email, subject, body);
}

/**
 * Bark 通知实现。
 */
function notifyByBark_(title, body, options) {
  options = options || {};

  var props = PropertiesService.getScriptProperties();
  var server = props.getProperty('BARK_SERVER') || 'https://api.day.app';
  var deviceKey = props.getProperty('BARK_DEVICE_KEY');

  if (!deviceKey) {
    Logger.log('Bark device key missing. ' + title + ' | ' + body);
    return;
  }

  var payload = {
    title: title || 'Macro Signals',
    body: body || '',
    group: options.group || props.getProperty('BARK_GROUP') || 'macro-signals',
    level: options.level || props.getProperty('BARK_LEVEL') || 'active'
  };

  if (options.sound) payload.sound = options.sound;
  if (options.url) payload.url = options.url;
  if (options.call) payload.call = '1';

  var endpoint = server.replace(/\/+$/, '') + '/' + encodeURIComponent(deviceKey);

  var resp = UrlFetchApp.fetch(endpoint, {
    method: 'post',
    contentType: 'application/json',
    payload: JSON.stringify(payload),
    muteHttpExceptions: true
  });

  var code = resp.getResponseCode();
  var text = resp.getContentText();

  if (code < 200 || code >= 300) {
    throw new Error('Bark notify failed: HTTP ' + code + ' | ' + text);
  }

  Logger.log('Bark notified: ' + title);
}

/**
 * Bark 测试入口。
 */
function testNotifyBark_() {
  notifyText_(
    'Macro Signals 测试',
    '这是一条 Bark 测试通知\n' + formatDateTime_(new Date()),
    {
      group: 'test',
      level: 'active'
    }
  );
}

/**
 * 合并多个 stats。
 *
 * 约定字段：
 * - inserted_rows
 * - updated_rows
 * - skipped_rows
 * - failed_rows
 * - changed_points
 * - source_date
 */
function mergeJobStats_(statsList) {
  var out = {
    inserted_rows: 0,
    updated_rows: 0,
    skipped_rows: 0,
    failed_rows: 0,
    changed_points: 0,
    source_date: ''
  };

  (statsList || []).forEach(function (s) {
    s = s || {};
    out.inserted_rows += Number(s.inserted_rows || 0);
    out.updated_rows += Number(s.updated_rows || 0);
    out.skipped_rows += Number(s.skipped_rows || 0);
    out.failed_rows += Number(s.failed_rows || 0);
    out.changed_points += Number(s.changed_points || 0);

    if (!out.source_date && s.source_date) {
      out.source_date = s.source_date;
    }
  });

  return out;
}

/**
 * 从源函数返回值中提取 stats。
 *
 * 兼容两种返回风格：
 * 1) { stats: {...}, ... }
 * 2) 直接返回 stats 对象
 */
function extractStatsFromResult_(result) {
  if (!result) return {};
  if (result.stats) return result.stats;
  return result;
}

/**
 * 根据统计结果构造简短摘要。
 */
function buildJobStatsText_(stats) {
  stats = stats || {};

  return [
    'inserted=' + Number(stats.inserted_rows || 0),
    'updated=' + Number(stats.updated_rows || 0),
    'skipped=' + Number(stats.skipped_rows || 0),
    'failed=' + Number(stats.failed_rows || 0),
    'points=' + Number(stats.changed_points || 0),
    'source_date=' + (stats.source_date || '-')
  ].join(' | ');
}

/**
 * 是否发送成功通知。
 */
function shouldNotifySuccess_(stats, modeOverride) {
  var mode = String(
    modeOverride || PropertiesService.getScriptProperties().getProperty('NOTIFY_SUCCESS_MODE') || 'changed'
  ).toLowerCase();

  if (mode === 'never') return false;
  if (mode === 'always') return true;

  // 默认 changed：有新增/更新/变化点时才通知
  stats = stats || {};
  return Number(stats.inserted_rows || 0) > 0
    || Number(stats.updated_rows || 0) > 0
    || Number(stats.changed_points || 0) > 0;
}

/**
 * 统一包装任务执行：
 * - 成功：写运行日志；按策略决定是否成功通知
 * - 失败：写运行日志 + Bark/邮件告警
 *
 * 约定：
 * - fn() 最好返回 { message, detail, stats, action, source_date }
 * - 若未返回，则按空统计处理
 *
 * options:
 * - successNotifyMode: always / changed / never
 */
function runJobWithNotify_(jobName, fn, defaultMessage, options) {
  options = options || {};

  try {
    var result = fn() || {};
    var detail = result.detail || {};
    var stats = result.stats || {};
    var message = result.message || defaultMessage || 'ok';
    var action = String(result.action || detail.action || '').toLowerCase();
    var sourceDate = result.source_date || stats.source_date || '';

    logJobResult_(jobName, 'SUCCESS', message, detail, stats);

    if (shouldNotifySuccess_(stats, options.successNotifyMode)) {
      notifyText_(
        '任务成功：' + jobName,
        [
          message,
          'action=' + (action || '-'),
          buildJobStatsText_(stats),
          'source_date=' + (sourceDate || '-')
        ].join('\n'),
        {
          group: 'job-success',
          level: 'active'
        }
      );
    }

    return result;
  } catch (err) {
    var message = err && err.message ? err.message : String(err);
    var stack = err && err.stack ? err.stack : '';

    logJobResult_(
      jobName,
      'ERROR',
      message,
      { stack: stack },
      { failed_rows: 1 }
    );

    notifyText_(
      '任务失败：' + jobName,
      message + (stack ? '\n\n' + stack : ''),
      {
        group: 'job-error',
        level: 'timeSensitive',
        call: true
      }
    );

    throw err;
  }
}

/**
 * 构建简版复盘表。
 * 当前直接从“信号-主要”抽取最近数据，保留核心观察字段。
 */
function buildSignalReview_() {
  var mainSheet = mustGetSheet_(SHEET_SIGNAL_MAIN);
  var reviewSheet = ensureSheet_(SHEET_SIGNAL_REVIEW);

  var header = [
    'date',
    'liquidity_regime',
    'rates_strategy_tilt',
    'rates_rv_ranking',
    'credit_strategy_tilt',
    'view_mortgage_background',
    'view_housing_background',
    'view_fx_background',
    'view_gold_background',
    'view_commodity_background'
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
      row[requireColumn_(srcHeader, 'view_mortgage_background')],
      row[requireColumn_(srcHeader, 'view_housing_background')],
      row[requireColumn_(srcHeader, 'view_fx_background')],
      row[requireColumn_(srcHeader, 'view_gold_background')],
      row[requireColumn_(srcHeader, 'view_commodity_background')]
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