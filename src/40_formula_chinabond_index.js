/********************
 * 40_formula_chinabond_index.js
 * 表格自定义公式：债券指数 / 中债指数查询。
 *
 * 说明：
 * - 这些函数主要供 Google Sheets 单元格直接调用
 * - 不参与日更触发器与原始表写入链路
 ********************/


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
    var json = fetchChinabondIndexSeries_(indexid);

    var durationPoint = getLatestPoint_(json.PJSZFJQ_00);
    var ytmPoint = getLatestPoint_(json.PJSZFDQSYL_00);
    var convexityPoint = getLatestPoint_(json.PJSZFTX_00);
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

