/********************
 * 13_raw_bond_index.js
 * 若干债券指数数据抓取辅助函数。
 ********************/

/**
 * 获取中证公司债指数特征信息。
 * @param {string} code 债券指数代码。
 * @return {Array<Array<*>>} 指数名称、收益率、成分券数量、久期、估值等信息。
 * @customfunction
 */
function GetCSIBondIndexData(code) {
  var url = 'https://www.csindex.com.cn/csindex-home/perf/get-bond-index-feature/' + code;
  var options = {
    method: 'get',
    headers: {
      accept: 'application/json, text/plain, */*'
    }
  };

  try {
    var response = safeFetch_(url, options);
    var json = JSON.parse(response.getContentText());
    var data = json.data || {};

    Logger.log(json);
    return [[data.dm || 'N/A', data.y, data.consNumber, data.d, data.v]];
  } catch (error) {
    Logger.log('获取数据失败: ' + error.toString());
    return [['错误', error.toString()]];
  }
}

/**
 * 获取国证公司债指数特征信息。
 * @param {string} code 债券指数代码。
 * @return {Array<Array<*>>} 指数名称、收益率、成分券数量、久期、估值等信息。
 * @customfunction
 */
function GetCNIBondIndexData(code) {
  var url = 'https://www.cnindex.com.cn/module/index-detail.html?act_menu=1&indexCode=' + code;
  var options = {
    method: 'get',
    headers: {
      accept: 'application/json, text/plain, */*'
    }
  };

  try {
    var response = safeFetch_(url, options);
    var json = JSON.parse(response.getContentText());
    var data = json.data || {};

    Logger.log(json);
    return [[data.dm || 'N/A', data.y, data.consNumber, data.d, data.v]];
  } catch (error) {
    Logger.log('获取数据失败: ' + error.toString());
    return [['错误', error.toString()]];
  }
}

/**
 * 获取当天 00:00:00 的毫秒级时间戳。
 */
function getMidnightUnixTimestamp() {
  var now = new Date();
  now.setHours(0, 0, 0, 0);
  return now.getTime();
}

/**
 * 取得时间序列中的最新一个点。
 */
function getLatestPoint(series) {
  if (!series || typeof series !== 'object') return null;

  var keys = Object.keys(series);
  if (keys.length === 0) return null;

  var latestTs = Math.max.apply(null, keys.map(Number));
  return {
    ts: latestTs,
    date: Utilities.formatDate(new Date(latestTs), 'Asia/Shanghai', 'yyyy-MM-dd'),
    value: series[String(latestTs)]
  };
}

/**
 * 获取中国债券信息网债券指数当前日期的久期（平均市值法）。
 * @param {string=} id 债券指数 ID。
 * @return {Array<Array<*>>} 日期、久期、到期收益率、占位字段、凸性。
 * @customfunction
 */
function GetChinabondIndexDuration(id) {
  var indexid = id || '8a8b2ca0332abed20134ea76d8885831';
  var url = 'https://yield.chinabond.com.cn/cbweb-mn/indices/singleIndexQueryResult?indexid=' + indexid + '&&qxlxt=00&&ltcslx=00&&zslxt=PJSZFJQ,PJSZFDQSYL,PJSZFTX&&zslxt1=PJSZFJQ,PJSZFDQSYL,PJSZFTX&&lx=1&&locale=';
  var options = {
    method: 'post',
    headers: {
      accept: 'application/json, text/javascript, */*; q=0.01',
      'x-requested-with': 'XMLHttpRequest'
    }
  };

  try {
    var response = safeFetch_(url, options);
    var json = JSON.parse(response.getContentText());

    var durationPoint = getLatestPoint(json.PJSZFJQ_00);
    var ytmPoint = getLatestPoint(json.PJSZFDQSYL_00);
    var convexityPoint = getLatestPoint(json.PJSZFTX_00);
    if (!durationPoint) {
      throw new Error('未获取到久期数据');
    }

    Logger.log(durationPoint.date);
    Logger.log(durationPoint.value);

    return [[
      durationPoint.date,
      durationPoint.value,
      ytmPoint ? ytmPoint.value : 'N/A',
      'N/A',
      'N/A',
      convexityPoint ? convexityPoint.value : 'N/A'
    ]];
  } catch (error) {
    return [['错误', error.toString()]];
  }
}
