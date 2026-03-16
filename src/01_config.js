/********************
 * 01_config.js
 * 项目级公共配置。
 *
 * 约定：
 * - 这里只放跨模块共享的公共契约
 * - 各数据源自己的 URL、字段映射、序列 ID、别名等，统一收回对应 source 文件
 ********************/

/** 原始数据表 */
var SHEET_CURVE_RAW = '原始_收益率曲线';
var SHEET_MONEY_MARKET_RAW = '原始_资金面';
var SHEET_FUTURES_RAW = '原始_国债期货';
var SHEET_POLICY_RATE_RAW = '原始_政策利率';
var SHEET_MACRO_RAW = '原始_海外宏观';
var SHEET_OVERSEAS_MACRO_RAW = SHEET_MACRO_RAW;
var SHEET_LIFE_ASSET_RAW = '原始_民生与资产价格';

/** 汇总指标与信号表 */
var SHEET_METRICS = '指标';
var SHEET_SIGNAL = '信号';
var SHEET_SIGNAL_MAIN = '信号-主要';
var SHEET_SIGNAL_DETAIL = '信号-明细';
var SHEET_SIGNAL_REVIEW = '信号-复盘';

/** 复盘 / 运维表 */
var SHEET_RUN_LOG = '运行日志';

/** 固定期限列，单位为年。 */
var TERMS = [
  0, 0.08, 0.17, 0.25, 0.5, 0.75,
  1, 2, 3, 4, 5, 6, 7, 8, 9, 10,
  15, 20, 30, 40, 50
];

/** 家庭配置基线。 */
var HOUSEHOLD_BUCKET_BASELINE = {
  cash: 15,
  stable_fixed_income: 35,
  active_fixed_income: 20,
  hedge: 10,
  risk: 20
};

/** 信号复盘默认观察窗口。 */
var SIGNAL_REVIEW_HORIZONS = [20, 60, 120];

/**
 * 信号阈值。
 * 分位列取值范围为 0~1。
 */
var SIGNAL_THRESHOLDS = {
  duration_pct_high: 0.80,
  duration_pct_low: 0.20,

  policy_spread_high: 0.80,
  policy_spread_low: 0.20,

  credit_spread_high: 0.80,
  credit_spread_low: 0.20,

  sink_spread_high: 0.80,
  sink_spread_low: 0.20,

  ncd_pct_high: 0.80,
  ncd_pct_low: 0.20,

  ultra_long_slope_high: 0.55,
  ultra_long_slope_low: 0.25,

  curve_10_1_flat: 0.35,
  curve_10_1_steep: 0.90,

  funding_tight: 1.90,
  funding_loose: 1.60
};
