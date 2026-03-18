/********************
 * 10_source_chinabond.js
 * Chinabond / 中债来源适配层。
 *
 * 职责：
 * 1) 集中维护中债相关来源配置（URL、曲线定义）
 * 2) 请求中债收益率曲线原始页面
 * 3) 解析 tablelist block
 * 4) 处理曲线标题归一化与 block 匹配
 *
 * 说明：
 * - 表格写入逻辑统一放在 20_raw_curve.js
 * - 债券指数自定义公式统一放在 40_formula_chinabond_index.js
 ********************/

var CHINABOND_YC_DETAIL_URL = 'https://yield.chinabond.com.cn/cbweb-mn/yc/ycDetail';
var CHINABOND_INDEX_SINGLE_QUERY_URL = 'https://yield.chinabond.com.cn/cbweb-mn/indices/singleIndexQueryResult';

/**
 * 中债曲线配置。
 * 约定：
 * - tier 仅用于固化分层，不影响现有表结构与指标/信号口径
 * - fetch_separately=true 的曲线需要单独请求，不能与其他曲线合并抓取
 */
var CURVES = [
  { name: '国债', id: '2c9081e50a2f9606010a3068cae70001', tier: 'main', aliases: ['国债收益率曲线', 'Government Bond'] },
  { name: '国开债', id: '8a8b2ca037a7ca910137bfaa94fa5057', tier: 'main', aliases: ['国开债收益率曲线', '政策性金融债', 'Policy Bank'] },
  { name: 'AAA信用', id: '2c9081e50a2f9606010a309f4af50111', tier: 'main', aliases: ['企业债收益率曲线(AAA)', '企业债AAA', 'Enterprise Bond AAA'] },
  { name: 'AA+信用', id: '2c908188138b62cd01139a2ee6b51e25', tier: 'main', aliases: ['企业债收益率曲线(AA+)', '企业债收益率曲线(AA＋)', '企业债AA+', 'Enterprise Bond AA+'] },
  { name: 'AAA+中票', id: '2c9081e9257ddf2a012590efdded1d35', tier: 'main', aliases: ['中短期票据收益率曲线(AAA+)', '中票AAA+', 'CP&Note AAA+'] },
  { name: 'AAA中票', id: '2c9081880fa9d507010fb8505b393fe7', tier: 'main', aliases: ['中短期票据收益率曲线(AAA)', '中票AAA', 'CP&Note AAA'] },
  { name: 'AAA存单', id: '8308218D1D030E0DE0540010E03EE6DA', tier: 'main', aliases: ['同业存单收益率曲线(AAA)', '存单AAA', 'NCD AAA', 'Negotiable CD AAA'] },
  { name: 'AAA城投', id: '2c9081e91b55cc84011be3c53b710598', tier: 'extended', aliases: ['城投债收益率曲线(AAA)', '城投AAA', 'LGFV AAA'] },
  { name: 'AAA银行债', id: '2c9081e9259b766a0125be8b5115149f', tier: 'extended', aliases: ['商业银行普通债收益率曲线(AAA)', '商业银行债收益率曲线(AAA)', '银行债AAA', 'Financial Bond of Commercial Bank AAA'] },
  { name: '地方债', id: '998183ff8c00f640018c32d4721a0d16', tier: 'short_history', fetch_separately: true, aliases: ['地方政府债收益率曲线', '地方政府债', 'Local Government'] }
];

function findCurveRequestIndex_(curves, curveName) {
  for (var i = 0; i < curves.length; i++) {
    if (curves[i] && curves[i].name === curveName) return i;
  }
  return -1;
}

function fetchChinaBondCurveSeparately_(date, curve) {
  var html = fetchChinaBondCurves_(date, [curve.id]);
  var blocks = parseChinaBondCurveBlocks_(html);
  if (!blocks.length) return null;
  if (blocks.length === 1) return blocks[0];

  var matched = resolveCurveBlock_(curve, blocks, {}, 0);
  if (matched) return matched;

  Logger.log('⚠️ 单独抓取返回多个 block，兜底取第一个: ' + curve.name + ' count=' + blocks.length);
  return blocks[0];
}

function fetchChinaBondCurves_(workTime, ycDefIds) {
  var cache = CacheService.getScriptCache();
  var cacheKey = buildShortCacheKey_('yc_detail', [workTime].concat(ycDefIds));
  var cached = cache.get(cacheKey);
  if (cached) return cached;

  var payload = {
    ycDefIds: ycDefIds.join(','),
    zblx: 'txy',
    workTime: workTime,
    dxbj: '0',
    qxlx: '0',
    yqqxN: 'N',
    yqqxK: 'K',
    wrjxCBFlag: '0',
    locale: 'zh_CN'
  };

  var resp = UrlFetchApp.fetch(CHINABOND_YC_DETAIL_URL, {
    method: 'post',
    payload: payload,
    muteHttpExceptions: true,
    headers: {
      'User-Agent': 'Mozilla/5.0'
    }
  });

  var code = resp.getResponseCode();
  var text = resp.getContentText('UTF-8');

  if (code !== 200) {
    throw new Error('ycDetail 请求失败 code=' + code + ' body=' + text.slice(0, 500));
  }

  cache.put(cacheKey, text, 21600);
  return text;
}

/**
 * 将页面中的标题表和数据表按顺序解析为 blocks。
 */
function parseChinaBondCurveBlocks_(html) {
  var blocks = [];
  var pairRe = /<table[^>]*class="t1"[\s\S]*?<span>\s*([^<]+?)\s*<\/span>[\s\S]*?<\/table>\s*<table[^>]*class="tablelist"[\s\S]*?<\/table>/gi;

  var match;
  while ((match = pairRe.exec(html)) !== null) {
    var title = stripTags_(match[1]);
    var block = match[0];
    var tableMatch = block.match(/<table[^>]*class="tablelist"[\s\S]*?<\/table>/i);
    if (!tableMatch) continue;

    var map = parseTableListToMap_(tableMatch[0]);
    var titleKey = buildCurveTitleKey_(title);
    blocks.push({
      title: title,
      titleKey: titleKey,
      map: map
    });
    Logger.log('解析block[' + (blocks.length - 1) + ']: title=' + title + ' key=' + titleKey + ' nodes=' + map.size);
  }

  return blocks;
}

/**
 * 将页面标题与内部曲线名统一压缩成便于匹配的 key。
 */
function buildCurveTitleKey_(text) {
  return String(text || '')
    .replace(/<[^>]+>/g, '')
    .replace(/\s+/g, '')
    .replace(/[（(]/g, '')
    .replace(/[）)]/g, '')
    .replace(/＋/g, '+')
    .replace(/&amp;/gi, '&')
    .replace(/中债/gi, '')
    .replace(/收益率曲线/gi, '')
    .replace(/yield\s*curve/gi, '')
    .replace(/curve/gi, '')
    .replace(/chinabond/gi, '')
    .replace(/governmentbond/gi, '国债')
    .replace(/policybank/gi, '国开债')
    .replace(/enterprisebond/gi, '企业债')
    .replace(/cp&note/gi, '中票')
    .replace(/commercialbank/gi, '银行')
    .replace(/financialbondof/gi, '')
    .replace(/financialbond/gi, '银行债')
    .replace(/ordinarybond/gi, '普通债')
    .replace(/negotiablecd/gi, '存单')
    .replace(/ncd/gi, '存单')
    .replace(/localgovernment/gi, '地方债')
    .replace(/地方政府债/g, '地方债')
    .replace(/中短期票据/g, '中票')
    .replace(/中期票据/g, '中票')
    .replace(/商业银行普通债/g, '银行债')
    .replace(/商业银行债/g, '银行债')
    .replace(/同业存单/g, '存单')
    .replace(/城投债/g, '城投')
    .replace(/lgfv/gi, '城投')
    .replace(/\.|\-|_/g, '')
    .toLowerCase();
}

/**
 * 根据 CURVES 配置的 name / aliases 与解析出的标题做匹配。
 * 先按别名匹配，匹配不到再按请求顺序兜底。
 */
function resolveCurveBlock_(curve, blocks, usedBlockIndex, requestIndex) {
  var aliases = [curve.name].concat(curve.aliases || []);
  var aliasKeys = aliases.map(function(alias) {
    return buildCurveTitleKey_(alias);
  });

  for (var i = 0; i < blocks.length; i++) {
    if (usedBlockIndex[i]) continue;

    var block = blocks[i];
    for (var j = 0; j < aliasKeys.length; j++) {
      var aliasKey = aliasKeys[j];
      if (!aliasKey) continue;
      if (block.titleKey === aliasKey || block.titleKey.indexOf(aliasKey) >= 0 || aliasKey.indexOf(block.titleKey) >= 0) {
        usedBlockIndex[i] = true;
        Logger.log('匹配曲线: ' + curve.name + ' <= ' + block.title + ' via alias=' + aliases[j]);
        return block;
      }
    }
  }

  if (requestIndex < blocks.length && !usedBlockIndex[requestIndex]) {
    usedBlockIndex[requestIndex] = true;
    Logger.log('⚠️ 按顺序兜底匹配: ' + curve.name + ' <= ' + blocks[requestIndex].title);
    return blocks[requestIndex];
  }

  Logger.log('⚠️ 未找到匹配 block: ' + curve.name + ' aliases=' + aliases.join(' | '));
  return null;
}

/**
 * 把 tablelist 表格解析为 term -> yield 的 Map。
 */
function parseTableListToMap_(tableHtml) {
  var map = new Map();
  var rowRe = /<tr[\s\S]*?<\/tr>/gi;
  var tdRe = /<td[^>]*>([\s\S]*?)<\/td>/gi;
  var rows = tableHtml.match(rowRe) || [];

  for (var i = 0; i < rows.length; i++) {
    var tds = [];
    var cellMatch;
    tdRe.lastIndex = 0;

    while ((cellMatch = tdRe.exec(rows[i])) !== null) {
      tds.push(stripTags_(cellMatch[1]));
    }
    if (tds.length < 2) continue;

    var term = parseFloat(String(tds[0]).replace('y', ''));
    var yieldValue = parseFloat(String(tds[1]));
    if (!isNaN(term) && !isNaN(yieldValue)) {
      map.set(term, yieldValue);
    }
  }

  return map;
}

/**
 * 获取中债单一指数查询结果 JSON。
 * 供 40_formula_chinabond_index.js 中的自定义公式复用。
 */
function fetchChinabondIndexSeries_(indexid) {
  var url = CHINABOND_INDEX_SINGLE_QUERY_URL
    + '?indexid=' + encodeURIComponent(indexid)
    + '&&qxlxt=00&&ltcslx=00&&zslxt=PJSZFJQ,PJSZFDQSYL,PJSZFTX'
    + '&&zslxt1=PJSZFJQ,PJSZFDQSYL,PJSZFTX&&lx=1&&locale=';
  return safeFetchJson_(url, {
    method: 'post',
    headers: {
      accept: 'application/json, text/javascript, */*; q=0.01',
      'x-requested-with': 'XMLHttpRequest'
    }
  }, 4);
}

/**
 * 取得时间序列中的最新一个点。
 */
function getLatestPoint_(series) {
  if (!series || typeof series !== 'object') return null;
  var keys = Object.keys(series);
  if (!keys.length) return null;
  var latestTs = Math.max.apply(null, keys.map(Number));
  return {
    ts: latestTs,
    date: Utilities.formatDate(new Date(latestTs), 'Asia/Shanghai', 'yyyy-MM-dd'),
    value: series[String(latestTs)]
  };
}
