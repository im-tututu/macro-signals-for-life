#!/usr/bin/env bash
set -euo pipefail

# Sync selected GitHub Actions secrets from local .env.
#
# Usage:
#   bash tools/set_github_secrets_from_env.sh
#   ENV_FILE=.env bash tools/set_github_secrets_from_env.sh
#
# Required:
#   - gh CLI installed
#   - gh auth login completed
#   - local .env includes:
#       SERVER_HOST
#       SERVER_PORT
#       SERVER_USER
#       SERVER_APP_DIR
#
# Optional:
#   GH_REPO=owner/repo   # defaults to current repo

ENV_FILE="${ENV_FILE:-.env}"
GH_REPO="${GH_REPO:-}"

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
    export "$key=$value"
  done <"$file"
}

load_env_file "$ENV_FILE"

if ! command -v gh >/dev/null 2>&1; then
  echo "[ERR] gh command not found. Install GitHub CLI first." >&2
  exit 1
fi

: "${SERVER_HOST:?Missing SERVER_HOST in $ENV_FILE}"
: "${SERVER_PORT:?Missing SERVER_PORT in $ENV_FILE}"
: "${SERVER_USER:?Missing SERVER_USER in $ENV_FILE}"
: "${SERVER_APP_DIR:?Missing SERVER_APP_DIR in $ENV_FILE}"

gh_secret_set() {
  local name="$1"
  if [[ -n "$GH_REPO" ]]; then
    gh secret set "$name" --repo "$GH_REPO"
  else
    gh secret set "$name"
  fi
}

echo "[INFO] syncing secrets from $ENV_FILE"
echo -n "$SERVER_HOST" | gh_secret_set SERVER_HOST
echo -n "$SERVER_PORT" | gh_secret_set SERVER_PORT
echo -n "$SERVER_USER" | gh_secret_set SERVER_USER
echo -n "$SERVER_APP_DIR" | gh_secret_set SERVER_APP_DIR

echo "[OK] secrets updated: SERVER_HOST SERVER_PORT SERVER_USER SERVER_APP_DIR"
echo "[INFO] SERVER_SSH_KEY is NOT updated by this script. Set it manually in GitHub Secrets."
