#!/usr/bin/env bash
set -euo pipefail

log() {
  echo "[allinone] $*"
}

require_env() {
  local name=$1
  local value="${!name:-}"
  if [[ -z "$value" ]]; then
    log "Missing required env var: ${name}"
    exit 1
  fi
}

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

wait_for_port() {
  local host=$1
  local port=$2
  local attempts=${3:-60}
  local sleep_s=${4:-2}

  for ((i = 1; i <= attempts; i++)); do
    if python3 - <<PY >/dev/null 2>&1
import socket
import sys
host = ${host@Q}
port = int(${port@Q})
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
require_env SUPERSET_SECRET_KEY

read -r db_host db_port < <(parse_host_port_from_uri "$SUPERSET_DB_URI")
wait_for_port "$db_host" "$db_port" 90 2

bootstrap_with_retries 30 3

log "Starting Superset server"
exec superset run -h 0.0.0.0 -p 8088

