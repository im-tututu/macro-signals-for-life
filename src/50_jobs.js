/********************
 * 50_jobs.js
 * 触发器入口函数。
 *
 * 原则：
 * - Apps Script 触发器只绑定这里的无参入口
 * - job 命名优先体现调度时段与范围，而不是底层实现细节
 * - 具体抓取 / 写表逻辑继续留在 20-24 原始层
 * - 手工执行时也优先从这里选，便于区分“可直接运行的 job”和内部函数
 ********************/

/**
 * 夜间更新国内数据。
 *
 * 用法：
 * - 适合绑定每日夜间触发器
 * - 统一处理国内日频数据抓取，并在最后重建派生层
 *
 * 流程：
 * 1) 中债收益率曲线
 * 2) 银行间资金面
 * 3) 国债期货
 * 4) 政策利率
 * 5) 民生 / 资产价格
 * 6) 指标重建
 * 7) 信号重建（默认最近一周）
 *
 * 影响范围：
 * - 写入/更新：原始_收益率曲线、原始_资金面、原始_国债期货、原始_政策利率、原始_民生与资产价格
 * - 重建：指标、信号、信号-复盘
 */
function jobNightlyCn() {
  return runJobWithNotify_(
    'jobNightlyCn',
    function () {
      var today = today_();
      Logger.log('jobNightlyCn | date=' + today + ' | signal_mode=recent_' + DEFAULT_SIGNAL_REBUILD_DAYS + 'd');

      var r1 = runJobStepWithLog_('中债曲线', function () {
        return runDailyWide_(today);
      });
      var r2 = runJobStepWithLog_('资金面', function () {
        return fetchPledgedRepoRates_();
      });
      var r3 = runJobStepWithLog_('国债期货', function () {
        return fetchBondFutures_();
      });
      var r4 = runJobStepWithLog_('政策利率', function () {
        return syncRawPolicyRateLatest();
      });
      var r5 = runJobStepWithLog_('民生与资产价格', function () {
        return fetchLifeAsset_ ? fetchLifeAsset_() : null;
      });
      var r6 = runJobStepWithLog_('派生层重建', function () {
        return rebuildAll_();
      });

      return {
        message: 'nightly cn done',
        detail: {
          curve: r1 || null,
          money_market: r2 || null,
          futures: r3 || null,
          policy_rate: r4 || null,
          life_asset: r5 || null,
          rebuild: r6 || null
        },
        stats: mergeJobStats_([
          extractStatsFromResult_(r1),
          extractStatsFromResult_(r2),
          extractStatsFromResult_(r3),
          extractStatsFromResult_(r4),
          extractStatsFromResult_(r5),
          extractStatsFromResult_(r6)
        ])
      };
    },
    'nightly cn done',
    {
      successNotifyMode: 'changed'
    }
  );
}

/**
 * 早晨更新美国 / 海外数据。
 *
 * 用法：
 * - 适合绑定每日早晨触发器
 * - 主要用于补齐隔夜更新的美元、利率、黄金等海外宏观数据
 *
 * 流程：
 * 1) 海外宏观
 * 2) 指标重建
 * 3) 信号重建（默认最近一周）
 *
 * 影响范围：
 * - 写入/更新：原始_海外宏观
 * - 重建：指标、信号、信号-复盘
 */
function jobMorningUs() {
  return runJobWithNotify_(
    'jobMorningUs',
    function () {
      Logger.log('jobMorningUs | signal_mode=recent_' + DEFAULT_SIGNAL_REBUILD_DAYS + 'd');

      var r1 = runJobStepWithLog_('海外宏观', function () {
        return fetchOverseasMacro_();
      });
      var r2 = runJobStepWithLog_('派生层重建', function () {
        return rebuildAll_();
      });

      return {
        message: 'morning us done',
        detail: {
          overseas_macro: r1 || null,
          rebuild: r2 || null
        },
        stats: mergeJobStats_([
          extractStatsFromResult_(r1),
          extractStatsFromResult_(r2)
        ])
      };
    },
    'morning us done',
    {
      successNotifyMode: 'changed'
    }
  );
}



/**
 * 夜间更新指数 ETF 快照。
 *
 * 用法：
 * - 推荐单独绑定在收盘后 / 晚间触发
 * - 依赖：
 *   1) 16_source_jisilu.js
 *   2) 25_raw_etf_index.js
 *   3) Script Properties 中已有 JISILU_COOKIE
 *   4) 若要自动续 Cookie，还需 JISILU_REFRESH_URL / JISILU_REFRESH_TOKEN
 */
function jobNightlyEtfIndex() {
  return runJobWithNotify_(
    'jobNightlyEtfIndex',
    function () {
      var r1 = runJobStepWithLog_('指数ETF快照', function () {
        return syncRawEtfIndexLatest({
          minUnitTotalYi: 2,
          minVolumeWan: '',
          rowsPerPage: 500,
          maxPages: 20
        });
      });

      return {
        message: 'nightly etf index done',
        detail: {
          etf_index: r1 || null
        },
        stats: mergeJobStats_([
          extractStatsFromResult_(r1)
        ])
      };
    },
    'nightly etf index done',
    { successNotifyMode: 'changed' }
  );
}






function jobBondIndexOnly() {
  return runJobWithNotify_(
    'jobBondIndexOnly',
    function () {
      var r1 = runJobStepWithLog_('债券指数特征', function () {
        return syncRawBondIndexFeatures_();
      });
      return {
        message: 'bond index only done',
        detail: { bond_index: r1 || null },
        stats: mergeJobStats_([extractStatsFromResult_(r1)])
      };
    },
    'bond index only done',
    { successNotifyMode: 'changed' }
  );
}





/**
 * 只重建派生层，不抓取任何外部源。
 *
 * 用法：
 * - 你改了 metrics / signal 规则后直接执行
 * - 原始表已齐全时，用它最快
 *
 * 默认：
 * - 指标全量
 * - 信号最近一周
 */
function jobManualRebuild() {
  return rebuildAll_();
}

/**
 * 全量重建派生层。
 *
 * 用法：
 * - 改了信号规则并需要把历史整段重刷时执行
 */
function jobManualRebuildFull() {
  return rebuildAllFull_();
}
