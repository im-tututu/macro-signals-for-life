/********************
 * 51_trigger_admin.js
 * 安装 / 重建 Apps Script time-driven triggers。
 *
 * 用法：
 * - 手工执行 rebuildProjectTriggers() 可按当前注册表重建全部定时器
 * - 修改 job 时间后，重新执行一次即可同步
 ********************/

var JOB_TRIGGER_REGISTRY = [
  { fn: 'jobNightlyCn', atHour: 22 },
  { fn: 'jobMorningUs', atHour: 7 }
];

/**
 * 清空当前项目 time-driven triggers，并按注册表重建。
 */
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

/**
 * 删除当前项目的全部 time-driven triggers。
 */
function clearProjectTriggers_() {
  var triggers = ScriptApp.getProjectTriggers();
  for (var i = 0; i < triggers.length; i++) {
    ScriptApp.deleteTrigger(triggers[i]);
  }
}