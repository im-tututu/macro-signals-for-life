/********************
 * 26_mapping_bond_index_name.js
 * 从「原始_指数ETF」提取债券指数候选名称，
 * 写入「映射_Jisilu债券指数候选」，
 * 再将配置表「配置_债券指数清单」里尚不存在的名称自动追加进去。
 ********************/

/** ========= sheet name fallback ========= */

function jbicSheetBondIndexListName_() {
  return (typeof SHEET_BOND_INDEX_LIST !== 'undefined' && SHEET_BOND_INDEX_LIST)
    ? SHEET_BOND_INDEX_LIST
    : '配置_债券指数清单';
}

function jbicSheetEtfIndexRawName_() {
  return (typeof SHEET_ETF_INDEX_RAW !== 'undefined' && SHEET_ETF_INDEX_RAW)
    ? SHEET_ETF_INDEX_RAW
    : '原始_指数ETF';
}

function jbicSheetCandidateName_() {
  return (typeof SHEET_JISILU_BOND_INDEX_CANDIDATE !== 'undefined' && SHEET_JISILU_BOND_INDEX_CANDIDATE)
    ? SHEET_JISILU_BOND_INDEX_CANDIDATE
    : '映射_Jisilu债券指数候选';
}

/** ========= public ========= */

function syncAndAppendBondIndexNamesFromJisilu() {
  var refreshRes = refreshJisiluBondIndexCandidates_();
  var appendRes = appendMissingBondIndexNamesFromJisiluCandidates();

  var result = {
    ok: true,
    candidate: refreshRes,
    append: appendRes
  };

  Logger.log(JSON.stringify(result));
  return result;
}

function refreshJisiluBondIndexCandidates_() {
  var rawSheet = jbicMustGetSheet_(jbicSheetEtfIndexRawName_());
  var candidateSheet = jbicGetOrCreateSheet_(jbicSheetCandidateName_());

  var lastRow = rawSheet.getLastRow();
  var lastCol = rawSheet.getLastColumn();
  if (lastRow < 2 || lastCol < 1) {
    throw new Error('原始ETF表为空: ' + jbicSheetEtfIndexRawName_());
  }

  var headers = rawSheet.getRange(1, 1, 1, lastCol).getDisplayValues()[0];
  var headerIndex = jbicBuildLooseHeaderIndex_(headers);

  var colIndexName = jbicFindColumnByCandidatesLoose_(headerIndex, [
    'index_nm', 'index name', 'index_name',
    '跟踪指数', '跟踪指数名称', '指数名称', '指数'
  ]);
  if (colIndexName < 0) {
    throw new Error(
      '原始ETF表未找到指数名称列。headers=' + headers.join(' | ')
    );
  }

  var colFundCode = jbicFindColumnByCandidatesLoose_(headerIndex, [
    'fund_id', 'fund code', 'fund_code', '代码', '基金代码', 'etf代码'
  ]);
  var colFundName = jbicFindColumnByCandidatesLoose_(headerIndex, [
    'fund_nm', 'fund name', 'fund_name', '名称', '基金名称', 'etf名称'
  ]);
  var colDataDate = jbicFindColumnByCandidatesLoose_(headerIndex, [
    'data_date', 'trade_date', 'snapshot_date', 'snapshot_day', 'nav_dt',
    '数据日期', '日期'
  ]);

  var values = rawSheet.getRange(2, 1, lastRow - 1, lastCol).getDisplayValues();
  var map = {};

  for (var i = 0; i < values.length; i++) {
    var row = values[i];
    var indexName = jbicToStr_(row[colIndexName]);
    if (!indexName) continue;
    if (!jbicIsBondIndexName_(indexName)) continue;

    var key = jbicNormalizeBondIndexNameKey_(indexName);
    if (!key) continue;
    if (map[key]) continue;

    var typeGuess = jbicGuessBondType_(indexName);
    map[key] = {
      index_name: indexName,
      match_key: key,
      provider_guess: jbicGuessProvider_(indexName),
      type_lv1: '债券',
      type_lv2: typeGuess.type2,
      type_lv3: typeGuess.type3,
      sample_fund_code: colFundCode >= 0 ? jbicToStr_(row[colFundCode]) : '',
      sample_fund_name: colFundName >= 0 ? jbicToStr_(row[colFundName]) : '',
      source_data_date: colDataDate >= 0 ? jbicToStr_(row[colDataDate]) : '',
      source_sheet: jbicSheetEtfIndexRawName_(),
      note: '由 Jisilu 指数ETF 原始表自动提取'
    };
  }

  var list = [];
  for (var k in map) {
    if (map.hasOwnProperty(k)) list.push(map[k]);
  }

  list.sort(function(a, b) {
    return a.index_name > b.index_name ? 1 : (a.index_name < b.index_name ? -1 : 0);
  });

  var outHeaders = [
    'index_name',
    'match_key',
    'provider_guess',
    'type_lv1',
    'type_lv2',
    'type_lv3',
    'sample_fund_code',
    'sample_fund_name',
    'source_data_date',
    'source_sheet',
    'note',
    'updated_at'
  ];

  var out = [outHeaders];
  var nowText = jbicNowText_();
  for (var j = 0; j < list.length; j++) {
    var item = list[j];
    out.push([
      item.index_name,
      item.match_key,
      item.provider_guess,
      item.type_lv1,
      item.type_lv2,
      item.type_lv3,
      item.sample_fund_code,
      item.sample_fund_name,
      item.source_data_date,
      item.source_sheet,
      item.note,
      nowText
    ]);
  }

  candidateSheet.clearContents();
  candidateSheet.getRange(1, 1, out.length, out[0].length).setValues(out);
  candidateSheet.setFrozenRows(1);

  Logger.log(
    'refreshJisiluBondIndexCandidates done'
    + ' | raw_rows=' + values.length
    + ' | candidate_rows=' + list.length
  );

  return {
    message: 'refresh jisilu bond index candidates done',
    detail: {
      raw_sheet: jbicSheetEtfIndexRawName_(),
      candidate_sheet: jbicSheetCandidateName_(),
      raw_rows: values.length,
      candidate_rows: list.length
    }
  };
}

function appendMissingBondIndexNamesFromJisiluCandidates() {
  var configSheet = jbicMustGetSheet_(jbicSheetBondIndexListName_());
  var lastRow = configSheet.getLastRow();
  var lastCol = Math.max(configSheet.getLastColumn(), 1);
  if (lastRow <= 0) throw new Error('配置表为空: ' + jbicSheetBondIndexListName_());

  var headerMeta = detectBondIndexListHeaderRow_(configSheet);
  var headerRow = headerMeta.headerRow;
  var headers = headerMeta.headers;
  var headerIndex = headerMeta.headerIndex;

  var colName = jbicFindColumnByCandidatesLoose_(headerIndex, ['指数', '指数名称', 'index_name']);
  if (colName < 0) {
    throw new Error(
      '未找到指数名称列。识别到的表头行=' + headerRow + ' | headers=' + headers.join(' | ')
    );
  }

  var colCode = jbicFindColumnByCandidatesLoose_(headerIndex, [
    '指数代码', '代码', 'index_code', 'index id', 'indexid', 'column 19'
  ]);
  var colUrl = jbicFindColumnByCandidatesLoose_(headerIndex, [
    'source_url', 'url', '链接', '详情链接', 'column 15'
  ]);
  var colType1 = jbicFindColumnByCandidatesLoose_(headerIndex, ['类型', 'type_lv1']);
  var colType2 = jbicFindColumnByCandidatesLoose_(headerIndex, ['类型-二级', '类型二级', 'type_lv2']);
  var colType3 = jbicFindColumnByCandidatesLoose_(headerIndex, ['类型-三级', '类型三级', 'type_lv3']);
  var colProvider = jbicFindColumnByCandidatesLoose_(headerIndex, ['指数发行公司', 'provider', 'provider_guess']);
  var colNote = jbicFindColumnByCandidatesLoose_(headerIndex, ['备注', 'note']);

  var existingSet = {};
  if (lastRow > headerRow) {
    var currentValues = configSheet.getRange(headerRow + 1, 1, lastRow - headerRow, lastCol).getDisplayValues();
    for (var i = 0; i < currentValues.length; i++) {
      var currentName = jbicToStr_(currentValues[i][colName]);
      var currentKey = jbicNormalizeBondIndexNameKey_(currentName);
      if (currentKey) existingSet[currentKey] = true;
    }
  }

  var candidateSheet = jbicMustGetSheet_(jbicSheetCandidateName_());
  var candidateLastRow = candidateSheet.getLastRow();
  var candidateLastCol = candidateSheet.getLastColumn();
  if (candidateLastRow <= 1) {
    return {
      message: 'no candidate rows to append',
      detail: { appended_rows: 0 },
      stats: { inserted: 0, updated: 0, deleted: 0, changed: 0 }
    };
  }

  var cHeaders = candidateSheet.getRange(1, 1, 1, candidateLastCol).getDisplayValues()[0];
  var cIndex = jbicBuildLooseHeaderIndex_(cHeaders);

  var cColName = jbicRequireColumnLoose_(cIndex, ['index_name']);
  var cColKey = jbicRequireColumnLoose_(cIndex, ['match_key']);
  var cColProvider = jbicRequireColumnLoose_(cIndex, ['provider_guess']);
  var cColType1 = jbicRequireColumnLoose_(cIndex, ['type_lv1']);
  var cColType2 = jbicRequireColumnLoose_(cIndex, ['type_lv2']);
  var cColType3 = jbicRequireColumnLoose_(cIndex, ['type_lv3']);
  var cColNote = jbicRequireColumnLoose_(cIndex, ['note']);

  var candidates = candidateSheet.getRange(2, 1, candidateLastRow - 1, candidateLastCol).getDisplayValues();
  var appendRows = [];

  for (var j = 0; j < candidates.length; j++) {
    var cRow = candidates[j];
    var cName = jbicToStr_(cRow[cColName]);
    var cKey = jbicToStr_(cRow[cColKey]);
    if (!cName || !cKey) continue;
    if (existingSet[cKey]) continue;

    var newRow = [];
    for (var x = 0; x < lastCol; x++) newRow.push('');

    newRow[colName] = cName;
    if (colCode >= 0) newRow[colCode] = '';
    if (colUrl >= 0) newRow[colUrl] = '';
    if (colType1 >= 0) newRow[colType1] = jbicToStr_(cRow[cColType1]) || '债券';
    if (colType2 >= 0) newRow[colType2] = jbicToStr_(cRow[cColType2]);
    if (colType3 >= 0) newRow[colType3] = jbicToStr_(cRow[cColType3]);
    if (colProvider >= 0) newRow[colProvider] = jbicToStr_(cRow[cColProvider]);
    if (colNote >= 0) newRow[colNote] = '名称由 Jisilu 指数ETF 候选自动追加';

    appendRows.push(newRow);
    existingSet[cKey] = true;
  }

  if (appendRows.length) {
    configSheet.getRange(configSheet.getLastRow() + 1, 1, appendRows.length, lastCol).setValues(appendRows);
  }

  Logger.log(
    'appendMissingBondIndexNamesFromJisiluCandidates done'
    + ' | header_row=' + headerRow
    + ' | appended=' + appendRows.length
  );

  return {
    message: 'append missing bond index names from jisilu candidates done',
    detail: {
      config_sheet: jbicSheetBondIndexListName_(),
      header_row: headerRow,
      appended_rows: appendRows.length
    },
    stats: {
      inserted: appendRows.length,
      updated: 0,
      deleted: 0,
      changed: appendRows.length
    }
  };
}

/** ========= debug ========= */

function testDetectBondIndexHeader() {
  var sh = jbicMustGetSheet_(jbicSheetBondIndexListName_());
  var meta = detectBondIndexListHeaderRow_(sh);
  Logger.log(JSON.stringify({
    headerRow: meta.headerRow,
    headers: meta.headers
  }));
  return meta;
}

function testRefreshJisiluBondIndexCandidates() {
  return refreshJisiluBondIndexCandidates_();
}

function testAppendMissingBondIndexNamesFromJisiluCandidates() {
  return appendMissingBondIndexNamesFromJisiluCandidates();
}

/** ========= header detect ========= */

function detectBondIndexListHeaderRow_(sheet) {
  var lastRow = sheet.getLastRow();
  var lastCol = Math.max(sheet.getLastColumn(), 1);
  var scanRows = Math.min(Math.max(lastRow, 1), 8);

  var values = sheet.getRange(1, 1, scanRows, lastCol).getDisplayValues();

  var best = null;
  for (var r = 0; r < values.length; r++) {
    var headers = values[r];
    var idx = jbicBuildLooseHeaderIndex_(headers);

    var score = 0;
    if (jbicFindColumnByCandidatesLoose_(idx, ['指数', '指数名称', 'index_name']) >= 0) score += 10;
    if (jbicFindColumnByCandidatesLoose_(idx, ['类型', 'type_lv1']) >= 0) score += 2;
    if (jbicFindColumnByCandidatesLoose_(idx, ['类型-二级', '类型二级', 'type_lv2']) >= 0) score += 2;
    if (jbicFindColumnByCandidatesLoose_(idx, ['类型-三级', '类型三级', 'type_lv3']) >= 0) score += 2;
    if (jbicFindColumnByCandidatesLoose_(idx, ['指数发行公司', 'provider', 'provider_guess']) >= 0) score += 2;
    if (jbicFindColumnByCandidatesLoose_(idx, ['备注', 'note']) >= 0) score += 1;

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
    throw new Error(
      '未识别到配置表表头行，请检查前 8 行。候选预览：\n' + preview
    );
  }

  return best;
}

/** ========= classify ========= */

function jbicIsBondIndexName_(name) {
  var s = jbicToStr_(name);
  if (!s) return false;

  var positive = /(国债|地债|地方债|地方政府债|政策性金融债|政金债|国开|农发|进出口|信用债|公司债|城投债|短融|中票|票据|可转债|转债|金融债|债券|存单)/;
  var negative = /(黄金|货币|现金|纳斯达克|纳指|标普|红利|科技|创业板|科创|沪深300|中证500|中证1000|上证50|港股|恒生|医药|消费|半导体|人工智能|证券公司|银行股|煤炭|有色|军工|游戏|新能源|光伏|芯片|通信|家电|白酒|酒|房地产|REIT|REITS|商品)/i;

  return positive.test(s) && !negative.test(s);
}

function jbicGuessProvider_(name) {
  var s = jbicToStr_(name);
  if (!s) return '';
  if (/^中债/.test(s)) return '中债';
  if (/^(国证|深)/.test(s)) return '国证';
  if (/^(中证|上证|沪)/.test(s)) return '中证';
  return '';
}

function jbicGuessBondType_(name) {
  var s = jbicToStr_(name);
  var type2 = '';
  var type3 = '';

  if (/可转债|转债/.test(s)) {
    type2 = '可转债';
    type3 = '';
  } else if (/地方政府债|地方债|地债/.test(s)) {
    type2 = '利率债';
    type3 = '地方政府债';
  } else if (/政策性金融债|政金债|国开|农发|进出口/.test(s)) {
    type2 = '利率债';
    type3 = '政策性金融债';
  } else if (/国债/.test(s)) {
    type2 = '利率债';
    type3 = '国债';
  } else if (/短融|短期融资券/.test(s)) {
    type2 = '信用债';
    type3 = '短期融资券';
  } else if (/城投债/.test(s)) {
    type2 = '信用债';
    type3 = '城投债';
  } else if (/公司债|信用债|中票|票据|企业债/.test(s)) {
    type2 = '信用债';
    type3 = '';
  } else {
    type2 = '债券';
    type3 = '';
  }

  return { type2: type2, type3: type3 };
}

/** ========= normalize ========= */

function jbicNormalizeLooseHeader_(h) {
  return String(h == null ? '' : h)
    .replace(/[\r\n\t]/g, ' ')
    .replace(/\u3000/g, ' ')
    .replace(/\s+/g, ' ')
    .trim()
    .toLowerCase();
}

function jbicBuildLooseHeaderIndex_(headers) {
  var map = {};
  for (var i = 0; i < headers.length; i++) {
    var key = jbicNormalizeLooseHeader_(headers[i]);
    if (key) map[key] = i;
  }
  return map;
}

function jbicFindColumnByCandidatesLoose_(headerIndex, candidates) {
  for (var i = 0; i < candidates.length; i++) {
    var key = jbicNormalizeLooseHeader_(candidates[i]);
    if (key in headerIndex) return headerIndex[key];
  }
  return -1;
}

function jbicRequireColumnLoose_(headerIndex, candidates) {
  var idx = jbicFindColumnByCandidatesLoose_(headerIndex, candidates);
  if (idx < 0) {
    throw new Error('未找到列: ' + candidates.join(' / '));
  }
  return idx;
}

function jbicNormalizeBondIndexNameKey_(name) {
  var s = jbicToStr_(name);
  if (!s) return '';

  return s
    .replace(/[\r\n\t]/g, '')
    .replace(/\u3000/g, ' ')
    .replace(/\s+/g, '')
    .replace(/[()（）\[\]【】\-—_]/g, '')
    .replace(/指数/g, '')
    .replace(/地方政府债/g, '地债')
    .toLowerCase();
}

/** ========= utils ========= */

function jbicMustGetSheet_(name) {
  var sh = SpreadsheetApp.getActive().getSheetByName(name);
  if (!sh) throw new Error('未找到工作表: ' + name);
  return sh;
}

function jbicGetOrCreateSheet_(name) {
  var ss = SpreadsheetApp.getActive();
  var sh = ss.getSheetByName(name);
  if (!sh) sh = ss.insertSheet(name);
  return sh;
}

function jbicToStr_(v) {
  return String(v == null ? '' : v).trim();
}

function jbicNowText_() {
  return Utilities.formatDate(new Date(), Session.getScriptTimeZone(), 'yyyy-MM-dd HH:mm:ss');
}