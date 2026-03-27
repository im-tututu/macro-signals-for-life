#!/usr/bin/env bash
set -euo pipefail

# Sync selected GitHub Actions secrets from local .env.
#
# Usage:
#   bash tools/local/set_github_secrets_from_env.sh
#   ENV_FILE=.env bash tools/local/set_github_secrets_from_env.sh
#
# Required:
#   - gh CLI installed
#   - gh auth login completed
#   - local .env includes:
#       SERVER_HOST
#       SERVER_PORT
#       SERVER_USER
#       SERVER_APP_DIR
#       GAS_SCRIPT_ID
#
# Optional:
#   GH_REPO=owner/repo   # defaults to current repo
#   CLASPRC_JSON         # if present, also sync to GitHub Secrets

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
ENV_FILE="${ENV_FILE:-$REPO_ROOT/.env}"
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
: "${GAS_SCRIPT_ID:?Missing GAS_SCRIPT_ID in $ENV_FILE}"

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
echo -n "$GAS_SCRIPT_ID" | gh_secret_set GAS_SCRIPT_ID

updated_names=(SERVER_HOST SERVER_PORT SERVER_USER SERVER_APP_DIR GAS_SCRIPT_ID)

if [[ -n "${CLASPRC_JSON:-}" ]]; then
  echo -n "$CLASPRC_JSON" | gh_secret_set CLASPRC_JSON
  updated_names+=(CLASPRC_JSON)
fi

echo "[OK] secrets updated: ${updated_names[*]}"
echo "[INFO] SERVER_SSH_KEY is NOT updated by this script. Set it manually in GitHub Secrets."
