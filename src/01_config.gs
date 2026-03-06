/********************
* 01_config.gs
* 集中定义 Sheet 名称、期限列表、曲线 ID、信号阈值。
********************/

var SHEET_CURVE = "yc_curve";
var SHEET_DASH = "rate_dashboard";
var SHEET_HIST = "curve_history";
var SHEET_SLOPE = "curve_slope";
var SHEET_SIGNAL = "etf_signal";
var SHEET_MM = "money_market";
var SHEET_FUT = "futures";
var SHEET_MACRO = "macro_dashboard";
var SHEET_ALLOC = "bond_allocation_signal";

// 固定期限列
var TERMS = [
  0, 0.08, 0.17, 0.25, 0.5, 0.75,
  1, 2, 3, 4, 5, 6, 7, 8, 9, 10,
  15, 20, 30, 40, 50
];

// 曲线配置
var CURVES = [
  { name: "国债", id: "2c9081e50a2f9606010a3068cae70001" },
  { name: "国开债", id: "8a8b2ca037a7ca910137bfaa94fa5057" },
  { name: "AAA信用", id: "2c9081e50a2f9606010a309f4af50111" },
  { name: "AA+信用", id: "2c908188138b62cd01139a2ee6b51e25" }
];

// ETF信号阈值
var SIGNAL_THRESHOLDS = {
  steep_low: 0.20,
  steep_high: 1.00
};


/********** 一键运行：建议每天收盘后触发 **********/
