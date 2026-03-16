/********************
 * 14_source_stats_gov.js
 * 国家统计局来源适配层（预留）。
 *
 * 当前仓库仅保留来源配置与占位返回，
 * 方便后续把 70 城房价月度抓取独立补齐。
 ********************/

var STATS_GOV_RELEASE_LIST_URL = 'https://www.stats.gov.cn/sj/zxfbhjd/';

/**
 * 返回国家统计局月频快照。
 * 当前为占位实现，不阻断主流程。
 */
function fetchStatsGovSnapshot_() {
  return {
    date: '',
    house_price_tier1: '',
    house_price_tier2: '',
    house_price_nbs_70city: '',
    source: STATS_GOV_RELEASE_LIST_URL
  };
}
