
/**
 * 获取中证公司债券指数特征信息.
 * @param {code} 债券指数代码.
 * @return 债券指数特征信息数组
 * @customfunction
*/

function GetCSIBondIndexData(code) {
  var url = "https://www.csindex.com.cn/csindex-home/perf/get-bond-index-feature/" + code;

  var options = {
    "method": "get",
    "headers": {
      "accept": "application/json, text/plain, */*"
    }
  };

  try {
    var response = UrlFetchApp.fetch(url, options);
    var json = JSON.parse(response.getContentText());

    // 假设返回的数据结构如下：
    // { "duration": 7.12, "effectiveDuration": 7.05, "ytm": 2.85 }

    // var sheet = SpreadsheetApp.getActiveSpreadsheet().getActiveSheet();

    // // 写入数据到 Google Sheets
    // sheet.getRange("A1").setValue("指标名称");
    // sheet.getRange("B1").setValue("数值");

    // sheet.getRange("A2").setValue("修正久期（Modified Duration）");
    // sheet.getRange("B2").setValue(json.duration || "N/A");

    // sheet.getRange("A3").setValue("有效久期（Effective Duration）");
    // sheet.getRange("B3").setValue(json.effectiveDuration || "N/A");

    // sheet.getRange("A4").setValue("到期收益率（YTM）");
    // sheet.getRange("B4").setValue(json.ytm || "N/A");

    Logger.log(json);
    var data = json.data;

    return( [ [data.dm || "N/A", data.y, data.consNumber, data.d, data.v] ] );
  } catch (error) {
    Logger.log("获取数据失败: " + error.toString());
  }
}





/**
 * 获取国证公司债券指数特征信息.
 * @param {code} 债券指数代码.
 * @return 债券指数特征信息数组
 * @customfunction
*/

// https://www.cnindex.com.cn/module/index-detail.html?act_menu=1&indexCode=921128
function GetCNIBondIndexData(code) {
  var url = "www.cnindex.com.cn/module/index-detail.html?act_menu=1&indexCode=" + code;

  var options = {
    "method": "get",
    "headers": {
      "accept": "application/json, text/plain, */*"
    }
  };

  try {
    var response = UrlFetchApp.fetch(url, options);
    var json = JSON.parse(response.getContentText());
    Logger.log(json);
    var data = json.data;

    return( [ [data.dm || "N/A", data.y, data.consNumber, data.d, data.v] ] );
  } catch (error) {
    Logger.log("获取数据失败: " + error.toString());
  }

}











function getMidnightUnixTimestamp() {
  var now = new Date();
  now.setHours(0, 0, 0, 0);  // 设置为当天00:00:00

  //var timestampSeconds = Math.floor(now.getTime() / 1000); // 秒级（10位）
  var timestampMillis = now.getTime(); // 毫秒级（13位）

  return timestampMillis; 
}




function getLatestPoint(series) {
  if (!series || typeof series !== "object") return null;

  var keys = Object.keys(series);
  if (keys.length === 0) return null;

  var latestTs = Math.max(...keys.map(Number));
  return {
    ts: latestTs,
    date: Utilities.formatDate(
      new Date(latestTs),
      "Asia/Shanghai",
      "yyyy-MM-dd"
    ),
    value: series[String(latestTs)]
  };
}



// https://yield.chinabond.com.cn/cbweb-mn/indices/singleIndexQueryResult?indexid=8a8b2c836393f4480163950643d92795&&qxlxt=00&&ltcslx=00&&zslxt=CFZS,XQJSL,XQJSL&&zslxt1=CFZS,XQJSL,XQJSL&&lx=1&&locale=

/**
 * 获取中国债券信息网债券指数当前日期的久期（平均市值法）。
 * @param {code} 债券指数代码.
 * @return 债券指数久期（平均市值法）
 * @customfunction
*/
function GetChinabondIndexDuration( id ) {
  var timestamp = Math.floor(Date.now() / 1000);  // 以秒为单位  
  
  // var indexid = id ? id : "0de96406d19a7ab66c7869bbbda8d549"; // 1-5年国开行债券指数ID
  // 8a8b2ca0332abed20134ea76d8885831

  var indexid = id ? id : "8a8b2ca0332abed20134ea76d8885831"; // 


  var url = "https://yield.chinabond.com.cn/cbweb-mn/indices/singleIndexQueryResult?indexid=" + indexid + "&&qxlxt=00&&ltcslx=00&&zslxt=PJSZFJQ,PJSZFDQSYL,PJSZFTX&&zslxt1=PJSZFJQ,PJSZFDQSYL,PJSZFTX&&lx=1&&locale=";
  // var params = {
  //   "indexid": "0de96406d19a7ab66c7869bbbda8d549",  // 1-5年国开行债券指数ID
  //   "qxlxt": "00",
  //   "ltcslx": "00",
  //   "zslxt": "PJSZFJQ",
  //   "zslxt1": "PJSZFJQ",
  //   "lx": "1",
  //   "locale": ""
  // };

  var options = {
    'method': 'post',
    //'contentType': 'multipart/form-data; boundary=--------------------------783291642804236047081515',
    "headers": {
      "accept": "application/json, text/javascript, */*; q=0.01",
      "x-requested-with": "XMLHttpRequest"
    },
    //"payload": params
  };

  try {
    var response = UrlFetchApp.fetch(url, options);
    var json = JSON.parse(response.getContentText());
    
    // 平均市值法久期、到期收益率、凸性
    var PJSZFJQ = json.PJSZFJQ_00;
    var PJSZFDQSYL = json.PJSZFDQSYL_00;
    var PJSZFTX = json.PJSZFTX_00;

    var timestampMillis = getMidnightUnixTimestamp();



    var latest = getLatestPoint(PJSZFJQ);
    Logger.log(latest.date);   // 最新交易日
    Logger.log(latest.value);  // 最新久期值


    // Logger.log(PJSZFJQ[timestampMillis]);

    //return [ [PJSZFJQ[timestampMillis], PJPXL[timestampMillis],"N/A", "N/A", PJSZFTX[timestampMillis]] ];
    
    return [ [getLatestPoint(PJSZFJQ).date, getLatestPoint(PJSZFJQ).value, getLatestPoint(PJSZFDQSYL).value,"N/A", "N/A", getLatestPoint(PJSZFTX) ] ]
  } catch (error) {
    return [["错误", error.toString()]];
  }

}
