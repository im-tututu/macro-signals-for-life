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

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
ENV_FILE="${ENV_FILE:-$REPO_ROOT/.env}"

load_env_file() {
  local file="$1"
  [[ -f "$file" ]] || return 0
  while IFS= read -r raw || [[ -n "$raw" ]]; do
    local line="${raw#"${raw%%[![:space:]]*}"}"
    line="${line%"${line##*[![:space:]]}"}"
    [[ -z "$line" || "${line:0:1}" == "#" ]] && continue
    [[ "$line" == *"="* ]] || continue
    local key="${line%%=*}"
    local value="${line#*=}"
    key="${key%"${key##*[![:space:]]}"}"
    key="${key#"${key%%[![:space:]]*}"}"
    value="${value#"${value%%[![:space:]]*}"}"
    value="${value%"${value##*[![:space:]]}"}"
    if [[ "${#value}" -ge 2 ]]; then
      local first="${value:0:1}"
      local last="${value: -1}"
      if [[ "$first" == "$last" && ( "$first" == "\"" || "$first" == "'" ) ]]; then
        value="${value:1:${#value}-2}"
      fi
    fi
    if [[ -z "${!key+x}" ]]; then
      export "$key=$value"
    fi
  done <"$file"
}

load_env_file "$ENV_FILE"

APP_DIR="${APP_DIR:-${SERVER_APP_DIR:-$HOME/apps/macro-signals-for-life}}"
CRON_TZ="${TZ:-Asia/Shanghai}"
LOG_DIR="${LOG_DIR:-$APP_DIR/runtime/logs}"
PY_BIN="$APP_DIR/.venv/bin/python"

mkdir -p "$LOG_DIR"

CRON_FILE="$(mktemp)"
{
  echo "SHELL=/bin/bash"
  echo "PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin"
  echo "TZ=$CRON_TZ"
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
echo "[INFO] APP_DIR=$APP_DIR"
echo "[INFO] TZ=$CRON_TZ"
echo "[INFO] LOG_DIR=$LOG_DIR"
crontab -l
