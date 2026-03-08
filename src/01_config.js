/********************
 * 01_config.js
 * 集中定义 Sheet 常量、期限列表、曲线 ID 与信号阈值。
 ********************/

/** 原始数据表 */
var SHEET_CURVE_RAW = '原始_收益率曲线';
var SHEET_MONEY_MARKET_RAW = '原始_资金面';
var SHEET_FUTURES_RAW = '原始_国债期货';

/** 汇总指标与信号表 */
var SHEET_METRICS = '指标_利率';
var SHEET_SIGNAL = '信号_利率';

/** 兼容旧常量名：指标层已并表到 SHEET_METRICS。 */
var SHEET_CURVE_HISTORY = SHEET_METRICS;
var SHEET_CURVE_SLOPE = SHEET_METRICS;
var SHEET_RATE_METRICS = SHEET_METRICS;

/** 兼容旧常量名：信号层已并表到 SHEET_SIGNAL。 */
var SHEET_ETF_SIGNAL = SHEET_SIGNAL;
var SHEET_BOND_ALLOC_SIGNAL = SHEET_SIGNAL;

/** 固定期限列，单位为年。 */
var TERMS = [
  0, 0.08, 0.17, 0.25, 0.5, 0.75,
  1, 2, 3, 4, 5, 6, 7, 8, 9, 10,
  15, 20, 30, 40, 50
];

/**
 * 中债曲线配置。
 * 说明：地方债历史较短，当前仍作为扩展曲线保留。
 */
var CURVES = [
  { name: '国债', id: '2c9081e50a2f9606010a3068cae70001' },
  { name: '国开债', id: '8a8b2ca037a7ca910137bfaa94fa5057' },
  { name: 'AAA信用', id: '2c9081e50a2f9606010a309f4af50111' },
  { name: 'AA+信用', id: '2c908188138b62cd01139a2ee6b51e25' },
  { name: 'AAA+中票', id: '2c9081e9257ddf2a012590efdded1d35' },
  { name: 'AAA中票', id: '2c9081880fa9d507010fb8505b393fe7' },
  { name: 'AAA城投', id: '2c9081e91b55cc84011be3c53b710598' },
  { name: 'AAA银行债', id: '2c9081e9259b766a0125be8b5115149f' },
  { name: 'AAA存单', id: '8308218D1D030E0DE0540010E03EE6DA' },
  { name: '地方债', id: '998183ff8c00f640018c32d4721a0d16' }
];

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
