/********************
 * 25_raw_etf_index.js
 * 原始_指数ETF 表写入逻辑。
 *
 * 默认抓取口径：
 * - 数据源：集思录 ETF 列表
 * - 分组：指数 ETF
 * - 筛选：规模 >= 2 亿元；成交额不限
 * - 存储：每日一个快照；同一天重跑时先删旧快照再写新快照
 ********************/

if (typeof SHEET_ETF_INDEX_RAW === 'undefined') {
  var SHEET_ETF_INDEX_RAW = '原始_指数ETF';
}

/** 原始_指数ETF 表头 */
var ETF_INDEX_RAW_HEADERS = [
  'snapshot_date',
  'fetched_at',
  'fund_id',
  'fund_nm',
  'index_nm',
  'issuer_nm',
  'price',
  'increase_rt',
  'volume_wan',
  'amount_yi',
  'unit_total_yi',
  'discount_rt',
  'fund_nav',
  'nav_dt',
  'estimate_value',
  'creation_unit',
  'pe',
  'pb',
  'last_time',
  'last_est_time',
  'is_qdii',
  'is_t0',
  'apply_fee',
  'redeem_fee',
  'records_total',
  'source_url'
];

/**
 * 把单条集思录 row 转成表格行。
 * 说明：
 * - Jisilu 常见是 {id, cell:{...}}
 * - 这里优先读 cell，缺失时回退到 row 自身
 */
function buildEtfIndexRawRow_(snapshotDate, fetchedAt, row, recordsTotal, sourceUrl) {
  var cell = row && row.cell ? row.cell : (row || {});

  return [
    snapshotDate,
    fetchedAt,
    toStr_(cell.fund_id || row.id),
    toStr_(cell.fund_nm),
    toStr_(cell.index_nm),
    toStr_(cell.issuer_nm),
    toNum_(cell.price),
    toNum_(cell.increase_rt),
    toNum_(cell.volume),
    toNum_(cell.amount),
    toNum_(cell.unit_total),
    toNum_(cell.discount_rt),
    toNum_(cell.fund_nav),
    toStr_(cell.nav_dt),
    toNum_(cell.estimate_value),
    toNum_(cell.creation_unit),
    toNum_(cell.pe),
    toNum_(cell.pb),
    toStr_(cell.last_time),
    toStr_(cell.last_est_time),
    toStr_(cell.is_qdii),
    toStr_(cell.is_t0),
    toNum_(cell.apply_fee),
    toNum_(cell.redeem_fee),
    toNum_(recordsTotal),
    toStr_(sourceUrl)
  ];
}

/** 确保表头存在且正确。 */
function ensureEtfIndexRawSheet_(sheet) {
  sheet = sheet || ensureSheet_(SHEET_ETF_INDEX_RAW);
  var needHeader = sheet.getLastRow() === 0;
  if (!needHeader) {
    var current = sheet.getRange(1, 1, 1, Math.max(sheet.getLastColumn(), ETF_INDEX_RAW_HEADERS.length)).getDisplayValues()[0];
    needHeader = current.join('\t') !== ETF_INDEX_RAW_HEADERS.join('\t');
  }
  if (needHeader) {
    sheet.clearContents();
    sheet.getRange(1, 1, 1, ETF_INDEX_RAW_HEADERS.length).setValues([ETF_INDEX_RAW_HEADERS]);
  }
  return sheet;
}

/**
 * 删除首列 snapshot_date = 指定日期的旧快照。
 * 为减少 deleteRows 次数，按连续区块自底向上删除。
 */
function deleteSnapshotRowsByDate_(sheet, snapshotDate) {
  var lastRow = sheet.getLastRow();
  if (lastRow <= 1) return 0;

  var vals = sheet.getRange(2, 1, lastRow - 1, 1).getDisplayValues();
  var rowsToDelete = [];
  for (var i = 0; i < vals.length; i++) {
    if (normYMD_(vals[i][0]) === snapshotDate) {
      rowsToDelete.push(i + 2);
    }
  }
  if (!rowsToDelete.length) return 0;

  var blocks = [];
  var start = rowsToDelete[0];
  var prev = rowsToDelete[0];
  for (var j = 1; j < rowsToDelete.length; j++) {
    var cur = rowsToDelete[j];
    if (cur === prev + 1) {
      prev = cur;
      continue;
    }
    blocks.push([start, prev]);
    start = cur;
    prev = cur;
  }
  blocks.push([start, prev]);

  var deleted = 0;
  for (var k = blocks.length - 1; k >= 0; k--) {
    var b = blocks[k];
    var rowStart = b[0];
    var rowEnd = b[1];
    var cnt = rowEnd - rowStart + 1;
    sheet.deleteRows(rowStart, cnt);
    deleted += cnt;
  }
  return deleted;
}

/** 批量追加快照。 */
function appendEtfIndexRows_(sheet, rows) {
  if (!rows || !rows.length) return 0;
  var startRow = sheet.getLastRow() + 1;
  sheet.getRange(startRow, 1, rows.length, ETF_INDEX_RAW_HEADERS.length).setValues(rows);
  return rows.length;
}

/**
 * 抓取并写入最新指数 ETF 快照。
 *
 * options:
 * - snapshotDate: 默认 today_()
 * - minUnitTotalYi: 默认 2
 * - minVolumeWan: 默认 ''
 * - rowsPerPage: 默认 500
 * - maxPages: 默认 20
 */
function syncRawEtfIndexLatest(options) {
  options = options || {};
  var snapshotDate = normYMD_(options.snapshotDate || today_());
  var fetchedAt = formatDateTime_(new Date());

  var result = fetchJisiluEtfIndexAllWithAutoRefresh_({
    rowsPerPage: options.rowsPerPage || 500,
    maxPages: options.maxPages || 20,
    minUnitTotalYi: options.minUnitTotalYi !== undefined ? options.minUnitTotalYi : 2,
    minVolumeWan: options.minVolumeWan !== undefined ? options.minVolumeWan : '',
    extraQueryString: options.extraQueryString || ''
  });

  var rows = result.rows || [];
  if (!rows.length) {
    throw new Error('Jisilu ETF rows empty');
  }

  var sheet = ensureEtfIndexRawSheet_();
  var deleted = deleteSnapshotRowsByDate_(sheet, snapshotDate);

  var writeRows = [];
  for (var i = 0; i < rows.length; i++) {
    writeRows.push(
      buildEtfIndexRawRow_(snapshotDate, fetchedAt, rows[i], result.records, result.last_url)
    );
  }
  var inserted = appendEtfIndexRows_(sheet, writeRows);

  Logger.log(
    'syncRawEtfIndexLatest done'
    + ' | date=' + snapshotDate
    + ' | deleted=' + deleted
    + ' | inserted=' + inserted
    + (result.records !== '' ? (' | records=' + result.records) : '')
  );

  return {
    message: 'etf index snapshot done',
    detail: {
      sheet: SHEET_ETF_INDEX_RAW,
      snapshot_date: snapshotDate,
      fetched_at: fetchedAt,
      inserted_rows: inserted,
      deleted_rows: deleted,
      total_records: result.records || '',
      source_url: result.last_url || ''
    },
    stats: {
      inserted: inserted,
      updated: 0,
      deleted: deleted,
      changed: inserted + deleted
    }
  };
}

/** 手工测试入口。 */
function testEtfIndexSnapshot() {
  return syncRawEtfIndexLatest({
    minUnitTotalYi: 2,
    minVolumeWan: '',
    rowsPerPage: 500,
    maxPages: 20
  });
}
