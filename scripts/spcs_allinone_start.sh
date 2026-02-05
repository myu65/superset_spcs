#!/usr/bin/env bash
set -Eeuo pipefail

log() {
  echo "[allinone] $*"
}

on_err() {
  local exit_code=$1
  local line_no=$2
  local cmd=$3
  log "ERROR exit=${exit_code} line=${line_no} cmd=${cmd}"
}

trap 'on_err $? $LINENO "$BASH_COMMAND"' ERR

require_env() {
  local name=$1
  local value="${!name:-}"
  if [[ -z "$value" ]]; then
    log "Missing required env var: ${name}"
    exit 1
  fi
}

log "Entrypoint starting"
log "User: $(id -u):$(id -g) ($(whoami 2>/dev/null || true))"
log "Python: $(python3 --version 2>/dev/null || true)"
log "Superset: $(superset --version 2>/dev/null || true)"
log "Scripts in /app/scripts:"
ls -la /app/scripts 2>/dev/null || true

token_path="${SNOWFLAKE_SERVICE_TOKEN_PATH:-/snowflake/session/token}"
if [[ -f "$token_path" ]]; then
  token_len="$(wc -c <"$token_path" 2>/dev/null || true)"
  log "SPCS token file: ${token_path} (bytes=${token_len:-?})"
else
  log "SPCS token file missing: ${token_path}"
fi
log "SNOWFLAKE_HOST=${SNOWFLAKE_HOST-<unset>}"
log "SNOWFLAKE_ACCOUNT=${SNOWFLAKE_ACCOUNT-<unset>}"

parse_host_port_from_uri() {
  local uri=$1
  local rest hostport host port

  # Drop scheme and credentials.
  rest="${uri#*://}"
  rest="${rest#*@}"
  hostport="${rest%%/*}"

  host="${hostport%%:*}"
  port="${hostport##*:}"

  if [[ -z "$host" || "$host" == "$hostport" ]]; then
    host="$hostport"
  fi
  if [[ -z "$port" || "$port" == "$hostport" ]]; then
    port="5432"
  fi

  printf '%s %s' "$host" "$port"
}

redact_db_uri() {
  local uri=$1
  echo "$uri" | sed -E 's#(://[^:/@]+):[^@]+@#\\1:***@#'
}

wait_for_port() {
  local host=$1
  local port=$2
  local attempts=${3:-60}
  local sleep_s=${4:-2}

  for ((i = 1; i <= attempts; i++)); do
    if python3 - "$host" "$port" <<'PY' >/dev/null 2>&1
import socket
import sys
host = sys.argv[1]
port = int(sys.argv[2])
try:
    s = socket.create_connection((host, port), timeout=1.5)
    s.close()
    sys.exit(0)
except Exception:
    sys.exit(1)
PY
    then
      return 0
    fi
    log "Waiting for ${host}:${port} (${i}/${attempts})..."
    sleep "$sleep_s"
  done

  log "Timed out waiting for ${host}:${port}"
  return 1
}

bootstrap_with_retries() {
  local attempts=${1:-20}
  local sleep_s=${2:-3}

  for ((i = 1; i <= attempts; i++)); do
    log "Bootstrap attempt ${i}/${attempts}"
    if /app/scripts/spcs_bootstrap.sh; then
      return 0
    fi
    log "Bootstrap failed; retrying in ${sleep_s}s..."
    sleep "$sleep_s"
  done
  return 1
}

require_env SUPERSET_DB_URI
secret_key="${SUPERSET_SECRET_KEY-}"
if [[ -z "$secret_key" ]]; then
  log "SUPERSET_SECRET_KEY is empty (Snowflake secret value is empty, or secret injection is not configured)"
  exit 1
fi
log "SUPERSET_SECRET_KEY len=${#secret_key}"
log "SUPERSET_FERNET_KEY len=${#SUPERSET_FERNET_KEY}"
log "SUPERSET_BOOTSTRAP_ADMINS=${SUPERSET_BOOTSTRAP_ADMINS-}"
log "SUPERSET_DB_URI=$(redact_db_uri "$SUPERSET_DB_URI")"

db_host_port="$(parse_host_port_from_uri "$SUPERSET_DB_URI")"
if [[ -z "$db_host_port" ]]; then
  log "Failed to parse host/port from SUPERSET_DB_URI"
  exit 1
fi
db_host="${db_host_port% *}"
db_port="${db_host_port#* }"
if [[ -z "$db_host" || -z "$db_port" || "$db_port" == "$db_host_port" ]]; then
  log "Failed to parse host/port from SUPERSET_DB_URI (got: ${db_host_port})"
  exit 1
fi
log "Parsed Postgres host=${db_host} port=${db_port}"
log "Waiting for Postgres at ${db_host}:${db_port}"
wait_for_port "$db_host" "$db_port" 90 2

bootstrap_with_retries 30 3

log "Starting Superset server"
exec superset run -h 0.0.0.0 -p 8088
