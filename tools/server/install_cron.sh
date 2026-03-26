#!/usr/bin/env bash
set -euo pipefail

# 用法：
#   bash tools/server/install_cron.sh
#
# 说明：
# - 默认按北京时间(Asia/Shanghai)写入两条任务：
#   22:10 跑 cn_night
#   07:10 跑 us_morning
# - 运行用户：当前用户（建议 ubuntu）

APP_DIR="${APP_DIR:-/home/ubuntu/apps/macro-signals-for-life-1}"
LOG_DIR="${LOG_DIR:-$APP_DIR/py/data/logs}"
PY_BIN="$APP_DIR/.venv/bin/python"

mkdir -p "$LOG_DIR"

CRON_FILE="$(mktemp)"
{
  echo "SHELL=/bin/bash"
  echo "PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin"
  echo "TZ=Asia/Shanghai"
  echo ""
  echo "10 22 * * 1-5 cd $APP_DIR && $PY_BIN py/scripts/run_job_group.py cn_night >> $LOG_DIR/cron_cn_night.log 2>&1"
  echo "20 22 * * 1-5 cd $APP_DIR && $PY_BIN py/scripts/run_metrics_snapshot.py --print-sample 0 >> $LOG_DIR/cron_metrics.log 2>&1"
  echo ""
  echo "10 7 * * 2-6 cd $APP_DIR && $PY_BIN py/scripts/run_job_group.py us_morning >> $LOG_DIR/cron_us_morning.log 2>&1"
  echo "20 7 * * 2-6 cd $APP_DIR && $PY_BIN py/scripts/run_metrics_snapshot.py --print-sample 0 >> $LOG_DIR/cron_metrics.log 2>&1"
} >"$CRON_FILE"

crontab "$CRON_FILE"
rm -f "$CRON_FILE"

echo "[OK] crontab installed for $(whoami)"
crontab -l
