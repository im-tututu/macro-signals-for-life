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

/**
 * 兼容旧常量名：指标层已并表到 SHEET_METRICS。
 */
var SHEET_CURVE_HISTORY = SHEET_METRICS;
var SHEET_CURVE_SLOPE = SHEET_METRICS;
var SHEET_RATE_METRICS = SHEET_METRICS;

/**
 * 兼容旧常量名：信号层已并表到 SHEET_SIGNAL。
 */
var SHEET_ETF_SIGNAL = SHEET_SIGNAL;
var SHEET_BOND_ALLOC_SIGNAL = SHEET_SIGNAL;

/**
 * 固定期限列，单位为年。
 */
var TERMS = [
  0, 0.08, 0.17, 0.25, 0.5, 0.75,
  1, 2, 3, 4, 5, 6, 7, 8, 9, 10,
  15, 20, 30, 40, 50
];

/**
 * 中债曲线配置。
 */
var CURVES = [
  { name: '国债', id: '2c9081e50a2f9606010a3068cae70001' },
  { name: '国开债', id: '8a8b2ca037a7ca910137bfaa94fa5057' },
  { name: 'AAA信用', id: '2c9081e50a2f9606010a309f4af50111' },
  { name: 'AA+信用', id: '2c908188138b62cd01139a2ee6b51e25' }
];

/**
 * ETF 信号阈值。
 */
var SIGNAL_THRESHOLDS = {
  steep_low: 0.20,
  steep_high: 1.00
};
