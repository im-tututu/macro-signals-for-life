/********************
 * 00_main.js
 * 主入口与人工测试入口。
 *
 * 说明：
 * - 真正的定时触发入口统一放在 50_jobs.js
 * - 这里保留 runEnhancedSystem / backfill 等高频手工入口
 * - 需要绑触发器时，优先绑定 50_jobs.js 中的 job* 函数
 ********************/

/**
 * 手工测试入口。
 */
function test() {
  buildMetricDictionary_();
}

/**
 * 统一输出作业步骤日志。
 */
function runJobStepWithLog_(stepName, fn) {
  var startedAt = new Date();
  Logger.log('▶️ ' + stepName + ' | start');
  try {
    var result = fn();
    var elapsedMs = new Date().getTime() - startedAt.getTime();
    Logger.log(
      '✅ ' + stepName +
      ' | done' +
      ' | elapsed_ms=' + elapsedMs +
      ' | ' + buildJobStatsText_(extractStatsFromResult_(result))
    );
    return result;
  } catch (err) {
    var elapsed = new Date().getTime() - startedAt.getTime();
    Logger.log('❌ ' + stepName + ' | failed | elapsed_ms=' + elapsed + ' | err=' + err);
    throw err;
  }
}

/**
 * 一次性执行完整日更流程：
 * 1) 中债曲线
 * 2) 资金面
 * 3) 国债期货
 * 4) 政策利率
 * 5) 海外宏观 / 民生资产
 * 6) 指标重建
 * 7) 信号重建（默认最近一周）
 */
function runEnhancedSystem() {
  var today = formatDate_(new Date());
  Logger.log('runEnhancedSystem | date=' + today + ' | signal_mode=recent_' + DEFAULT_SIGNAL_REBUILD_DAYS + 'd');

  var r1 = runJobStepWithLog_('中债曲线', function() {
    return runDailyWide_(today);
  });
  var r2 = runJobStepWithLog_('资金面', function() {
    return fetchPledgedRepoRates_();
  });
  var r3 = runJobStepWithLog_('国债期货', function() {
    return fetchBondFutures_();
  });
  var r4 = runJobStepWithLog_('政策利率', function() {
    return syncRawPolicyRateLatest();
  });
  var r5 = runJobStepWithLog_('海外宏观', function() {
    return fetchOverseasMacro_();
  });
  var r6 = runJobStepWithLog_('民生与资产价格', function() {
    return fetchLifeAsset_();
  });
  var r7 = runJobStepWithLog_('派生层重建', function() {
    return rebuildAll_();
  });

  return {
    message: 'daily close done',
    detail: {
      curve: r1 || null,
      money_market: r2 || null,
      futures: r3 || null,
      policy_rate: r4 || null,
      overseas_macro: r5 || null,
      life_asset: r6 || null,
      rebuild: r7 || null
    },
    stats: mergeJobStats_([
      extractStatsFromResult_(r1),
      extractStatsFromResult_(r2),
      extractStatsFromResult_(r3),
      extractStatsFromResult_(r4),
      extractStatsFromResult_(r5),
      extractStatsFromResult_(r6),
      extractStatsFromResult_(r7)
    ])
  };
}

/**
 * 仅重建派生层，不抓取原始数据。
 *
 * 默认：
 * - 指标全量重建
 * - 信号仅重建最近一周
 */
function rebuildAll_() {
  Logger.log('rebuildAll_ | signal_mode=recent_' + DEFAULT_SIGNAL_REBUILD_DAYS + 'd');
  var r1 = runJobStepWithLog_('重建指标', function() {
    buildMetrics_();
    return {
      message: 'metrics rebuild done',
      stats: { updated_rows: 1 }
    };
  });
  var r2 = runJobStepWithLog_('重建信号(最近一周)', function() {
    return buildSignalRecentDays_(DEFAULT_SIGNAL_REBUILD_DAYS);
  });
  SpreadsheetApp.flush();
  Utilities.sleep(500);
  return {
    message: 'derived rebuild done',
    detail: {
      metrics: r1 || null,
      signal: r2 || null,
      signal_mode: 'recent',
      signal_days: DEFAULT_SIGNAL_REBUILD_DAYS
    },
    stats: mergeJobStats_([
      extractStatsFromResult_(r1),
      extractStatsFromResult_(r2)
    ])
  };
}

/**
 * 全量重建派生层。
 *
 * 用法：
 * - 改了信号口径，且需要把历史整段重刷时执行
 */
function rebuildAllFull_() {
  Logger.log('rebuildAllFull_ | signal_mode=all');
  var r1 = runJobStepWithLog_('重建指标', function() {
    buildMetrics_();
    return {
      message: 'metrics rebuild done',
      stats: { updated_rows: 1 }
    };
  });
  var r2 = runJobStepWithLog_('重建信号(全量)', function() {
    return buildSignalAll_();
  });
  SpreadsheetApp.flush();
  Utilities.sleep(500);
  return {
    message: 'derived full rebuild done',
    detail: {
      metrics: r1 || null,
      signal: r2 || null,
      signal_mode: 'all'
    },
    stats: mergeJobStats_([
      extractStatsFromResult_(r1),
      extractStatsFromResult_(r2)
    ])
  };
}

function rebuildSignalAndReview_() {
  Logger.log('rebuildSignalAndReview_ | signal_mode=recent_' + DEFAULT_SIGNAL_REBUILD_DAYS + 'd');
  var result = runJobStepWithLog_('重建信号(最近一周)', function() {
    return buildSignalRecentDays_(DEFAULT_SIGNAL_REBUILD_DAYS);
  });
  SpreadsheetApp.flush();
  Utilities.sleep(500);
  return result;
}

function rebuildSignalAndReviewFull_() {
  Logger.log('rebuildSignalAndReviewFull_ | signal_mode=all');
  var result = runJobStepWithLog_('重建信号(全量)', function() {
    return buildSignalAll_();
  });
  SpreadsheetApp.flush();
  Utilities.sleep(500);
  return result;
}

/**
 * 从最近 30 天起点重新安全回补，每次最多处理 8 个非周末日期。
 */
function testBackfillSafe() {
  var end = new Date();
  var start = new Date(end);
  start.setDate(end.getDate() - 30);
  backfillBatch_(formatDate_(start), formatDate_(end), 8, true);
}

/**
 * 从回补游标继续补最近 120 天数据，每次最多处理 8 个非周末日期。
 */
function resumeBackfillSafe() {
  var end = new Date();
  var start = new Date(end);
  start.setDate(end.getDate() - 120);
  backfillBatch_(formatDate_(start), formatDate_(end), 8, false);
}

function showBackfillCursor() {
  Logger.log('BACKFILL_CURSOR=' + getBackfillCursor_());
}

function resetBackfillCursor() {
  clearBackfillCursor_();
  Logger.log('BACKFILL_CURSOR cleared');
}

function testLifeAssetEntry() {
  return forceFetchLifeAsset_();
}
