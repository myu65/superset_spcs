#!/usr/bin/env bash
set -euo pipefail

log() {
  echo "[bootstrap] $*"
}

require_env() {
  local name=$1
  local value="${!name:-}"
  if [[ -z "$value" ]]; then
    log "Missing required env var: ${name}"
    exit 1
  fi
}

bootstrap_admins() {
  local users_string="${SUPERSET_BOOTSTRAP_ADMINS:-}"
  if [[ -z "$users_string" ]]; then
    log "No SUPERSET_BOOTSTRAP_ADMINS provided; skipping admin creation."
    return
  fi
  IFS=',' read -ra admins <<< "$users_string"
  for raw in "${admins[@]}"; do
    local username
    username="$(echo "$raw" | xargs)"
    [[ -z "$username" ]] && continue
    log "Ensuring admin user ${username}"
    superset fab create-admin \
      --username "$username" \
      --firstname "$username" \
      --lastname Admin \
      --email "${username}@example.invalid" \
      --password "${SUPERSET_BOOTSTRAP_ADMIN_PASSWORD:-bootstrap}" \
      || log "create-admin exited non-zero (possibly already exists) for ${username}"
  done
}

require_env SUPERSET_DB_URI
require_env SUPERSET_SECRET_KEY

log "Running superset db upgrade"
superset db upgrade

log "Running superset init"
superset init

bootstrap_admins

log "Bootstrap complete."
