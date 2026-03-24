/********************
 * 11_source_chinamoney.js
 * ChinaMoney 来源适配层。
 *
 * 当前覆盖：
 * - 质押式回购日内快照 prr-md.json
 * - 历史回补 CSV prr-chrt.csv
 ********************/

var CHINAMONEY_REPO_SNAPSHOT_URL = 'https://www.chinamoney.com.cn/r/cms/www/chinamoney/data/currency/prr-md.json';
var CHINAMONEY_REPO_HISTORY_CSV_URL = 'https://www.chinamoney.com.cn/r/cms/www/chinamoney/data/currency/prr-chrt.csv';

function buildChinaMoneyTimestampedUrl_(baseUrl) {
  return baseUrl + '?t=' + Date.now();
}

function fetchChinaMoneyRepoSnapshot_() {
  var url = buildChinaMoneyTimestampedUrl_(CHINAMONEY_REPO_SNAPSHOT_URL);
  var res = safeFetch_(url, { method: 'get', muteHttpExceptions: true }, 3);
  var code = res.getResponseCode();
  if (code !== 200) {
    throw new Error('prr-md.json HTTP=' + code + ' body=' + safeSlice_(res.getContentText(), 300));
  }
  return { url: url, json: JSON.parse(res.getContentText()) };
}

function fetchChinaMoneyRepoHistoryCsv_() {
  var url = buildChinaMoneyTimestampedUrl_(CHINAMONEY_REPO_HISTORY_CSV_URL);
  var res = safeFetch_(url, { method: 'get', muteHttpExceptions: true }, 3);
  var code = res.getResponseCode();
  if (code !== 200) {
    throw new Error('prr-chrt.csv HTTP=' + code + ' body=' + safeSlice_(res.getContentText(), 300));
  }
  return { url: url, csv: res.getContentText('UTF-8') };
}

function deriveMoneyMarketBizDate_(data) {
  data = data || {};

  var candidates = [
    data.showDateCN,
    data.date,
    data.showDate,
    data.tradeDate
  ];

  for (var i = 0; i < candidates.length; i++) {
    var ds = extractDateFromAnyText_(candidates[i]);
    if (ds) return ds;
  }

  return formatDate_(new Date());
}
