/********************
 * 25_raw_bond_index.js
 * 债券指数特征原始表写入。
 *
 * 流程：
 * 1) 读取「配置_债券指数清单」
 * 2) 批量抓取中证 / 国证 / 中债指数特征
 * 3) 写入 / 更新「原始_债券指数特征」
 ********************/

function syncRawBondIndexFeatures_() {
  var cfgSheet = mustGetSheet_(SHEET_BOND_INDEX_LIST);
  var rawSheet = getOrCreateSheetByName_(SHEET_BOND_INDEX_RAW);

  var cfg = readBondIndexConfigRows_();
  if (!cfg.length) {
    return {
      message: 'no bond index config rows',
      detail: { config_rows: 0 },
      stats: { inserted: 0, updated: 0, deleted: 0, changed: 0 }
    };
  }

  ensureBondIndexRawHeader_(rawSheet);

  var tradeDate = today_();
  var existingMeta = readBondIndexRawExistingMap_(rawSheet);
  var appendRows = [];
  var updateActions = [];
  var inserted = 0;
  var updated = 0;
  var failed = 0;
  var skippedBlank = 0;

  for (var i = 0; i < cfg.length; i++) {
    var item = cfg[i];

    var indexName = birToStr_(item.index_name || item['指数'] || item['指数名称']);
    var indexCode = birToStr_(item.index_code || item['指数代码'] || item['代码'] || item['Column 19']);
    var sourceUrl = birToStr_(item.source_url || item['source_url'] || item['链接'] || item['详情链接'] || item['Column 15']);
    var provider = birToStr_(item.provider || item['指数发行公司'] || item['provider_guess']);
    var type1 = birToStr_(item.type_lv1 || item['类型']);
    var type2 = birToStr_(item.type_lv2 || item['类型-二级'] || item['类型二级']);
    var type3 = birToStr_(item.type_lv3 || item['类型-三级'] || item['类型三级']);

    if (!indexName && !indexCode) {
      skippedBlank++;
      continue;
    }

    var res = fetchBondIndexFeatureByRow_(item);
    var rowValues = [
      tradeDate,
      indexName,
      indexCode,
      provider || (res && res.provider) || '',
      type1,
      type2,
      type3,
      sourceUrl,
      res && res.data_date ? res.data_date : '',
      res && res.dm !== undefined ? res.dm : '',
      res && res.y !== undefined ? res.y : '',
      res && res.cons_number !== undefined ? res.cons_number : '',
      res && res.d !== undefined ? res.d : '',
      res && res.v !== undefined ? res.v : '',
      res && res.ok ? 'OK' : 'ERROR',
      res && res.raw_json ? res.raw_json : '',
      birNowText_(),
      res && res.ok ? '' : (res && res.error ? res.error : 'unknown error')
    ];

    var key = tradeDate + '|' + indexCode;
    if (existingMeta.rowMap[key]) {
      updateActions.push({ rowNumber: existingMeta.rowMap[key], values: rowValues });
      updated++;
    } else {
      appendRows.push(rowValues);
      inserted++;
    }

    if (!res || !res.ok) failed++;
  }

  for (var u = 0; u < updateActions.length; u++) {
    var act = updateActions[u];
    rawSheet.getRange(act.rowNumber, 1, 1, act.values.length).setValues([act.values]);
  }

  if (appendRows.length) {
    rawSheet.getRange(rawSheet.getLastRow() + 1, 1, appendRows.length, appendRows[0].length)
      .setValues(appendRows);
  }

  return {
    message: 'sync raw bond index features done',
    detail: {
      config_rows: cfg.length,
      appended_rows: appendRows.length,
      updated_rows: updateActions.length,
      skipped_blank_rows: skippedBlank,
      failed_rows: failed
    },
    stats: {
      inserted: inserted,
      updated: updated,
      deleted: 0,
      changed: inserted + updated
    }
  };
}

function readBondIndexConfigRows_() {
  var sh = mustGetSheet_(SHEET_BOND_INDEX_LIST);
  var lastRow = sh.getLastRow();
  var lastCol = sh.getLastColumn();
  if (lastRow <= 0 || lastCol <= 0) return [];

  var headerMeta = detectBondIndexConfigHeaderRow_(sh);
  var headerRow = headerMeta.headerRow;
  var headers = headerMeta.headers;

  if (lastRow <= headerRow) return [];

  var values = sh.getRange(headerRow + 1, 1, lastRow - headerRow, lastCol).getDisplayValues();
  var rows = [];

  for (var r = 0; r < values.length; r++) {
    var row = values[r];
    var obj = {};
    var nonEmpty = false;
    for (var c = 0; c < headers.length; c++) {
      obj[headers[c]] = row[c];
      if (row[c] !== '') nonEmpty = true;
    }
    if (nonEmpty) rows.push(obj);
  }
  return rows;
}

function detectBondIndexConfigHeaderRow_(sheet) {
  var lastRow = sheet.getLastRow();
  var lastCol = Math.max(sheet.getLastColumn(), 1);
  var scanRows = Math.min(Math.max(lastRow, 1), 8);
  var values = sheet.getRange(1, 1, scanRows, lastCol).getDisplayValues();

  var best = null;
  for (var r = 0; r < values.length; r++) {
    var headers = values[r];
    var idx = buildLooseHeaderIndexForBondIndex_(headers);

    var score = 0;
    if (findColumnByCandidatesLooseForBondIndex_(idx, ['指数', '指数名称', 'index_name']) >= 0) score += 10;
    if (findColumnByCandidatesLooseForBondIndex_(idx, ['指数代码', '代码', 'index_code', 'column 19']) >= 0) score += 8;
    if (findColumnByCandidatesLooseForBondIndex_(idx, ['类型', 'type_lv1']) >= 0) score += 2;
    if (findColumnByCandidatesLooseForBondIndex_(idx, ['类型-二级', '类型二级', 'type_lv2']) >= 0) score += 2;
    if (findColumnByCandidatesLooseForBondIndex_(idx, ['指数发行公司', 'provider', 'provider_guess']) >= 0) score += 2;

    if (!best || score > best.score) {
      best = {
        score: score,
        headerRow: r + 1,
        headers: headers,
        headerIndex: idx
      };
    }
  }

  if (!best || best.score < 10) {
    var preview = values.map(function(row, i) {
      return 'row ' + (i + 1) + ': ' + row.join(' | ');
    }).join('\n');
    throw new Error('未识别到「配置_债券指数清单」表头行，请检查前 8 行。\n' + preview);
  }
  return best;
}

function ensureBondIndexRawHeader_(sheet) {
  var headers = [[
    'trade_date',
    'index_name',
    'index_code',
    'provider',
    'type_lv1',
    'type_lv2',
    'type_lv3',
    'source_url',
    'data_date',
    'dm',
    'y',
    'cons_number',
    'd',
    'v',
    'fetch_status',
    'raw_json',
    'fetched_at',
    'error'
  ]];

  if (sheet.getLastRow() === 0) {
    sheet.getRange(1, 1, 1, headers[0].length).setValues(headers);
    sheet.setFrozenRows(1);
    return;
  }

  var current = sheet.getRange(1, 1, 1, sheet.getLastColumn()).getDisplayValues()[0];
  if (current.join('|') !== headers[0].join('|')) {
    sheet.getRange(1, 1, 1, headers[0].length).setValues(headers);
    sheet.setFrozenRows(1);
  }
}

function readBondIndexRawExistingMap_(sheet) {
  var map = {};
  var lastRow = sheet.getLastRow();
  var lastCol = sheet.getLastColumn();
  if (lastRow <= 1 || lastCol <= 0) return { rowMap: map };

  var headers = sheet.getRange(1, 1, 1, lastCol).getDisplayValues()[0];
  var idx = buildHeaderIndex_(headers);
  var colTradeDate = requireColumn_(idx, 'trade_date');
  var colIndexCode = requireColumn_(idx, 'index_code');
  var values = sheet.getRange(2, 1, lastRow - 1, lastCol).getDisplayValues();

  for (var i = 0; i < values.length; i++) {
    var key = birToStr_(values[i][colTradeDate]) + '|' + birToStr_(values[i][colIndexCode]);
    if (key !== '|') map[key] = i + 2;
  }
  return { rowMap: map };
}

function normalizeLooseHeaderForBondIndex_(h) {
  return String(h == null ? '' : h)
    .replace(/[\r\n\t]/g, ' ')
    .replace(/\u3000/g, ' ')
    .replace(/\s+/g, ' ')
    .trim()
    .toLowerCase();
}

function buildLooseHeaderIndexForBondIndex_(headers) {
  var map = {};
  for (var i = 0; i < headers.length; i++) {
    var key = normalizeLooseHeaderForBondIndex_(headers[i]);
    if (key) map[key] = i;
  }
  return map;
}

function findColumnByCandidatesLooseForBondIndex_(headerIndex, candidates) {
  for (var i = 0; i < candidates.length; i++) {
    var key = normalizeLooseHeaderForBondIndex_(candidates[i]);
    if (key in headerIndex) return headerIndex[key];
  }
  return -1;
}

function birToStr_(v) {
  if (typeof toStr_ === 'function') return toStr_(v);
  return v == null ? '' : String(v).trim();
}

function birNowText_() {
  if (typeof formatDateTime_ === 'function') return formatDateTime_(new Date());
  return Utilities.formatDate(new Date(), Session.getScriptTimeZone(), 'yyyy-MM-dd HH:mm:ss');
}

function testSyncRawBondIndexFeatures() {
  return syncRawBondIndexFeatures_();
}
