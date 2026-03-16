/********************
 * 14_source_stats_gov.js
 * 国家统计局来源适配层（预留）。
 *
 * 当前仓库已在 config 中预留 70 城房价等字段与来源 URL，
 * 但尚未提交稳定抓取实现。此文件先独立占位，方便后续扩展，
 * 并让整体目录结构与来源分层保持一致。
 ********************/

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
    source: LIFE_ASSET_SOURCE.nbs_release_list || ''
  };
}
