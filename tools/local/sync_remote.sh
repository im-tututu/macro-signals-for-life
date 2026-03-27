#!/usr/bin/env bash
set -euo pipefail

# Sync local runtime files to remote server.
#
# Default behavior:
# - sync .env only
#
# Usage:
#   bash tools/local/sync_remote.sh
#   bash tools/local/sync_remote.sh --dry-run
#   bash tools/local/sync_remote.sh --env-only
#   bash tools/local/sync_remote.sh --db-push
#   bash tools/local/sync_remote.sh --db-pull
#   bash tools/local/sync_remote.sh --db-pull --no-env
#
# Optional env overrides:
#   ENV_FILE=.env
#   SERVER_HOST=your-host.example.com
#   SERVER_PORT=22
#   SERVER_USER=ubuntu
#   SERVER_APP_DIR=/home/ubuntu/apps/macro-signals-for-life
#   SSH_KEY_PATH=~/.ssh/id_ed25519
#   LOCAL_ENV_PATH=.env
#   DB_PATH=py/data/db.sqlite
#   LOCAL_DB_PATH=py/data/db.sqlite
#   REMOTE_DB_PATH=/home/ubuntu/apps/macro-signals-for-life/py/data/db.sqlite
#
# This script auto-loads ENV_FILE (default .env) first.

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

REMOTE_HOST="${REMOTE_HOST:-${SERVER_HOST:-}}"
REMOTE_PORT="${REMOTE_PORT:-${SERVER_PORT:-22}}"
REMOTE_USER="${REMOTE_USER:-${SERVER_USER:-}}"
REMOTE_APP_DIR="${REMOTE_APP_DIR:-${SERVER_APP_DIR:-}}"

LOCAL_ENV_PATH="${LOCAL_ENV_PATH:-$ENV_FILE}"
DB_PATH_VALUE="${DB_PATH:-py/data/db.sqlite}"
LOCAL_DB_PATH="${LOCAL_DB_PATH:-$DB_PATH_VALUE}"

if [[ "$DB_PATH_VALUE" = /* ]]; then
  DEFAULT_REMOTE_DB_PATH="$DB_PATH_VALUE"
else
  DEFAULT_REMOTE_DB_PATH="${REMOTE_APP_DIR:+$REMOTE_APP_DIR/}$DB_PATH_VALUE"
fi
REMOTE_DB_PATH="${REMOTE_DB_PATH:-$DEFAULT_REMOTE_DB_PATH}"

SSH_KEY_PATH="${SSH_KEY_PATH:-}"
DRY_RUN=0
SYNC_ENV=1
DB_MODE="none" # none | push | pull

while [[ $# -gt 0 ]]; do
  case "$1" in
    --dry-run)
      DRY_RUN=1
      shift
      ;;
    --env-only)
      SYNC_ENV=1
      DB_MODE="none"
      shift
      ;;
    --no-env)
      SYNC_ENV=0
      shift
      ;;
    --db-push)
      DB_MODE="push"
      shift
      ;;
    --db-pull)
      DB_MODE="pull"
      shift
      ;;
    --db-only)
      echo "[WARN] --db-only 已废弃，请改用 --db-push 或 --db-pull"
      DB_MODE="push"
      shift
      ;;
    -h|--help)
      sed -n '1,40p' "$0"
      exit 0
      ;;
    *)
      echo "[ERR] Unknown arg: $1" >&2
      exit 2
      ;;
  esac
done

if [[ "$SYNC_ENV" -eq 0 && "$DB_MODE" == "none" ]]; then
  echo "[ERR] Nothing to sync. Use default / --env-only / --db-push / --db-pull." >&2
  exit 2
fi

if [[ -z "$REMOTE_HOST" || -z "$REMOTE_USER" || -z "$REMOTE_APP_DIR" ]]; then
  echo "[ERR] Missing remote target settings. Please set SERVER_HOST/SERVER_USER/SERVER_APP_DIR (or REMOTE_*)." >&2
  exit 2
fi

if [[ -z "$REMOTE_DB_PATH" ]]; then
  echo "[ERR] Missing remote DB target. Please set DB_PATH or REMOTE_DB_PATH." >&2
  exit 2
fi

SSH_OPTS=(-p "$REMOTE_PORT")
SCP_OPTS=(-P "$REMOTE_PORT")
if [[ -n "$SSH_KEY_PATH" ]]; then
  SSH_OPTS+=(-i "$SSH_KEY_PATH")
  SCP_OPTS+=(-i "$SSH_KEY_PATH")
fi

SSH_CMD=(ssh "${SSH_OPTS[@]}" "${REMOTE_USER}@${REMOTE_HOST}")
SCP_CMD=(scp "${SCP_OPTS[@]}")
RSYNC_RSH="ssh -p ${REMOTE_PORT}"
if [[ -n "$SSH_KEY_PATH" ]]; then
  RSYNC_RSH+=" -i ${SSH_KEY_PATH}"
fi

echo "[INFO] remote=${REMOTE_USER}@${REMOTE_HOST}:${REMOTE_APP_DIR}"
echo "[INFO] env_file=${ENV_FILE}"
echo "[INFO] local_env_path=${LOCAL_ENV_PATH}"
echo "[INFO] local_db_path=${LOCAL_DB_PATH}"
echo "[INFO] remote_db_path=${REMOTE_DB_PATH}"
echo "[INFO] dry_run=${DRY_RUN} sync_env=${SYNC_ENV} db_mode=${DB_MODE}"

if [[ "$DRY_RUN" -eq 0 ]]; then
  "${SSH_CMD[@]}" "mkdir -p ${REMOTE_APP_DIR} && mkdir -p \$(dirname ${REMOTE_DB_PATH}) && mkdir -p ${REMOTE_APP_DIR}/py/data/logs"
else
  echo "[DRY] ssh ${REMOTE_USER}@${REMOTE_HOST} 'mkdir -p ${REMOTE_APP_DIR} \$(dirname ${REMOTE_DB_PATH}) ${REMOTE_APP_DIR}/py/data/logs'"
fi

if [[ "$SYNC_ENV" -eq 1 ]]; then
  if [[ ! -f "$LOCAL_ENV_PATH" ]]; then
    echo "[ERR] Missing local env file: $LOCAL_ENV_PATH" >&2
    exit 1
  fi
  if [[ "$DRY_RUN" -eq 0 ]]; then
    "${SCP_CMD[@]}" "$LOCAL_ENV_PATH" "${REMOTE_USER}@${REMOTE_HOST}:${REMOTE_APP_DIR}/.env"
    "${SSH_CMD[@]}" "chmod 600 ${REMOTE_APP_DIR}/.env"
  else
    echo "[DRY] scp $LOCAL_ENV_PATH ${REMOTE_USER}@${REMOTE_HOST}:${REMOTE_APP_DIR}/.env"
    echo "[DRY] ssh ${REMOTE_USER}@${REMOTE_HOST} 'chmod 600 ${REMOTE_APP_DIR}/.env'"
  fi
  echo "[OK] .env synced"
fi

if [[ "$DB_MODE" != "none" ]]; then
  if [[ ! -f "$LOCAL_DB_PATH" ]]; then
    if [[ "$DB_MODE" == "push" ]]; then
      echo "[ERR] Missing local db file for push: $LOCAL_DB_PATH" >&2
      exit 1
    fi
  fi
  if [[ "$DB_MODE" == "push" ]]; then
    if [[ "$DRY_RUN" -eq 0 ]]; then
      rsync -avz --partial --inplace -e "$RSYNC_RSH" \
        "$LOCAL_DB_PATH" \
        "${REMOTE_USER}@${REMOTE_HOST}:${REMOTE_DB_PATH}"
    else
      echo "[DRY] rsync -avz -e \"$RSYNC_RSH\" $LOCAL_DB_PATH ${REMOTE_USER}@${REMOTE_HOST}:${REMOTE_DB_PATH}"
    fi
    echo "[OK] db.sqlite pushed to remote"
  else
    if [[ "$DRY_RUN" -eq 0 ]]; then
      rsync -avz --partial --inplace -e "$RSYNC_RSH" \
        "${REMOTE_USER}@${REMOTE_HOST}:${REMOTE_DB_PATH}" \
        "$LOCAL_DB_PATH"
    else
      echo "[DRY] rsync -avz -e \"$RSYNC_RSH\" ${REMOTE_USER}@${REMOTE_HOST}:${REMOTE_DB_PATH} $LOCAL_DB_PATH"
    fi
    echo "[OK] db.sqlite pulled to local"
  fi
fi

echo "[DONE] sync completed"
