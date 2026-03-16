/********************
 * 51_trigger_admin.js
 * 安装 / 重建 Apps Script time-driven triggers。
 ********************/

var JOB_TRIGGER_REGISTRY = [
  { fn: 'jobMoneyMarketIntraday', everyMinutes: 30 },
  { fn: 'jobPolicyWindowPoll', everyHours: 2 },
  { fn: 'jobCurveClose', atHour: 18 },
  { fn: 'jobFuturesClose', atHour: 18 },
  { fn: 'jobMacroNightly', atHour: 22 },
  { fn: 'jobDailyClose', atHour: 23 }
];

function rebuildProjectTriggers() {
  clearProjectTriggers_();

  JOB_TRIGGER_REGISTRY.forEach(function(job) {
    var builder = ScriptApp.newTrigger(job.fn).timeBased();
    if (job.everyMinutes) {
      builder.everyMinutes(job.everyMinutes);
    } else if (job.everyHours) {
      builder.everyHours(job.everyHours);
    } else if (job.atHour !== undefined) {
      builder.atHour(job.atHour).everyDays(1);
    } else {
      throw new Error('unknown trigger config: ' + JSON.stringify(job));
    }
    builder.create();
  });

  Logger.log('project triggers rebuilt: ' + JOB_TRIGGER_REGISTRY.length);
}

function clearProjectTriggers_() {
  var triggers = ScriptApp.getProjectTriggers();
  for (var i = 0; i < triggers.length; i++) {
    ScriptApp.deleteTrigger(triggers[i]);
  }
}
