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
 * 6) 指标、信号、复盘重建
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

      var r1 = runDailyWide_(today);
      var r2 = fetchPledgedRepoRates_();
      var r3 = fetchBondFutures_();
      var r4 = syncRawPolicyRateLatest();
      var r5 = fetchLifeAsset_ ? fetchLifeAsset_() : null;

      rebuildAll_();

      return {
        message: 'nightly cn done',
        detail: {
          curve: r1 || null,
          money_market: r2 || null,
          futures: r3 || null,
          policy_rate: r4 || null,
          life_asset: r5 || null,
          rebuild: 'done'
        },
        stats: mergeJobStats_([
          extractStatsFromResult_(r1),
          extractStatsFromResult_(r2),
          extractStatsFromResult_(r3),
          extractStatsFromResult_(r4),
          extractStatsFromResult_(r5),
          { updated_rows: 3 }
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
 * 2) 指标、信号、复盘重建
 *
 * 影响范围：
 * - 写入/更新：原始_海外宏观
 * - 重建：指标、信号、信号-复盘
 */
function jobMorningUs() {
  return runJobWithNotify_(
    'jobMorningUs',
    function () {
      var r1 = fetchOverseasMacro_();

      rebuildAll_();

      return {
        message: 'morning us done',
        detail: {
          overseas_macro: r1 || null,
          rebuild: 'done'
        },
        stats: mergeJobStats_([
          extractStatsFromResult_(r1),
          { updated_rows: 3 }
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
 * 只重建派生层，不抓取任何外部源。
 *
 * 用法：
 * - 你改了 metrics / signal 规则后直接执行
 * - 原始表已齐全时，用它最快
 *
 * 影响范围：
 * - 重建：指标、信号、信号-复盘
 */
function jobManualRebuild() {
  rebuildAll_();
}