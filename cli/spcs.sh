#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
IMAGE_NAME=${IMAGE_NAME:-superset-spcs:latest}
IMAGE_PUSH_TARGET=${IMAGE_PUSH_TARGET:-}
CONTAINER_NAME=${CONTAINER_NAME:-superset-spcs-dev}
SUPERSET_PORT=${SUPERSET_PORT:-8088}
POSTGRES_URI=${SUPERSET_DB_URI:-postgresql://superset:superset@localhost:5432/superset}
CONFIG_STAGE=${CONFIG_STAGE:-@app_config.superset}
SPCS_PROFILE=${SPCS_PROFILE:-managed}
SERVICE_SPEC=${SERVICE_SPEC:-}
BOOTSTRAP_SPEC=${BOOTSTRAP_SPEC:-$ROOT_DIR/infra/spcs/jobs/bootstrap_admin.yaml}
SNOW_CONNECTION=${SNOW_CONNECTION:-}

load_repo_env() {
  # Source a repo-local env file for naming, stages, and connection selection.
  # This makes `./cli/spcs.sh deploy` reproducible without exporting many vars.
  local env_file="${SPCS_ENV_FILE:-$ROOT_DIR/config/spcs.env}"
  if [[ -f "$env_file" ]]; then
    # shellcheck disable=SC1090
    set -a
    source "$env_file"
    set +a
  fi
}

pick_service_spec() {
  if [[ -n "$SERVICE_SPEC" ]]; then
    echo "$SERVICE_SPEC"
    return
  fi

  case "${SPCS_PROFILE}" in
    managed) echo "$ROOT_DIR/infra/spcs/service-managed.yaml" ;;
    allinone) echo "$ROOT_DIR/infra/spcs/service-allinone.yaml" ;;
    *) echo "$ROOT_DIR/infra/spcs/service-managed.yaml" ;;
  esac
}

log() {
  echo "[spcs] $*"
}

maybe_fix_repo_local_snowflake_perms() {
  # Snowflake CLI rejects overly-permissive config file permissions.
  # For the repo-local config we can safely fix perms automatically.
  local snowflake_home=$1
  local cfg="$snowflake_home/config.toml"

  if [[ "$snowflake_home" != "$ROOT_DIR/.snowflake" ]]; then
    return 0
  fi
  if [[ ! -f "$cfg" ]]; then
    return 0
  fi

  # Best-effort: if this fails, Snowflake CLI will raise a clear error.
  chmod 0700 "$snowflake_home" 2>/dev/null || true
  chmod 0600 "$cfg" 2>/dev/null || true
}

require_cmd() {
  if ! command -v "$1" >/dev/null 2>&1; then
    echo "Missing required command: $1" >&2
    exit 1
  fi
}

snow_exec() {
  # Prefer a locally installed `snow`. If missing, fall back to uv/uvx.
  local snowflake_home="${SNOWFLAKE_HOME:-}"
  if [[ -z "$snowflake_home" ]] && [[ -f "$ROOT_DIR/.snowflake/config.toml" ]]; then
    snowflake_home="$ROOT_DIR/.snowflake"
  fi

  local -a env_prefix=()
  if [[ -n "$snowflake_home" ]]; then
    maybe_fix_repo_local_snowflake_perms "$snowflake_home"
    env_prefix=(env "SNOWFLAKE_HOME=$snowflake_home")
  fi

  if command -v snow >/dev/null 2>&1; then
    "${env_prefix[@]}" snow "$@"
    return
  fi

  if command -v uvx >/dev/null 2>&1; then
    "${env_prefix[@]}" uvx --from snowflake-cli snow "$@"
    return
  fi

  if command -v uv >/dev/null 2>&1; then
    "${env_prefix[@]}" uv tool run --from snowflake-cli snow "$@"
    return
  fi

  echo "Missing required command: snow" >&2
  echo "Install Snowflake CLI (recommended): uv tool install snowflake-cli" >&2
  echo "Or run without installing (requires uv): uvx --from snowflake-cli snow --help" >&2
  exit 1
}


require_env() {
  for var in "$@"; do
    if [[ -z "${!var:-}" ]]; then
      echo "Environment variable $var must be set for this command." >&2
      exit 1
    fi
  done
}

render_to_tmp() {
  local file=$1
  require_cmd envsubst
  local tmp
  tmp=$(mktemp)
  envsubst < "$file" > "$tmp"
  echo "$tmp"
}

build_image() {
  log "Building Docker image $IMAGE_NAME"
  docker build -t "$IMAGE_NAME" -f "$ROOT_DIR/docker/Dockerfile" "$ROOT_DIR"
}

maybe_push_image() {
  if [[ -n "$IMAGE_PUSH_TARGET" ]]; then
    log "Pushing image to $IMAGE_PUSH_TARGET"
    docker tag "$IMAGE_NAME" "$IMAGE_PUSH_TARGET"
    docker push "$IMAGE_PUSH_TARGET"
  fi
}

sync_config_stage() {
  local -a conn=()
  if [[ -n "$SNOW_CONNECTION" ]]; then
    conn+=(--connection "$SNOW_CONNECTION")
  fi
  # Snowflake CLI v2 uses `snow stage copy` (not `put`) for upload/download.
  # Use no-auto-compress to keep filenames intact so SPCS stage mounts can import .py files directly.
  local dest="${CONFIG_STAGE%/}/"
  log "Uploading superset/config/ to stage $dest"
  snow_exec stage create "${conn[@]}" "${CONFIG_STAGE}" >/dev/null 2>&1 || true
  # `snow stage copy` does not support recursive uploads; copy files one by one.
  shopt -s nullglob
  local file
  for file in "$ROOT_DIR"/superset/config/*; do
    [[ -f "$file" ]] || continue
    snow_exec stage copy "${conn[@]}" --overwrite --no-auto-compress "$file" "$dest"
  done
  shopt -u nullglob
}

apply_service_spec() {
  require_env COMPUTE_POOL SERVICE_NAME
  local spec_file
  spec_file="$(pick_service_spec)"
  local rendered
  rendered=$(render_to_tmp "$spec_file")
  log "Applying service spec to $SERVICE_NAME on pool $COMPUTE_POOL"
  local -a conn=()
  if [[ -n "$SNOW_CONNECTION" ]]; then
    conn+=(--connection "$SNOW_CONNECTION")
  fi
  snow_exec spcs service create "${conn[@]}" \
    --name "$SERVICE_NAME" \
    --compute-pool "$COMPUTE_POOL" \
    --spec-file "$rendered" \
    --if-exists replace
  rm -f "$rendered"
}

run_job_spec() {
  local spec=$1
  local job_name=${2:-${JOB_NAME:-superset-job}}
  require_env COMPUTE_POOL
  local rendered
  rendered=$(render_to_tmp "$spec")
  log "Running job $job_name on pool $COMPUTE_POOL"
  local -a conn=()
  if [[ -n "$SNOW_CONNECTION" ]]; then
    conn+=(--connection "$SNOW_CONNECTION")
  fi
  snow_exec spcs service execute-job "${conn[@]}" \
    --name "$job_name" \
    --compute-pool "$COMPUTE_POOL" \
    --spec-file "$rendered"
  rm -f "$rendered"
}

deploy_all() {
  if [[ "${AUTO_BOOTSTRAP_SNOWFLAKE:-1}" == "1" ]]; then
    bootstrap_snowflake
  fi
  build_image
  if [[ "${PUSH_IMAGE:-0}" == "1" ]]; then
    push_images
  else
    maybe_push_image
  fi
  sync_config_stage
  if [[ "${RUN_BOOTSTRAP:-0}" == "1" ]]; then
    run_job_spec "$BOOTSTRAP_SPEC" "${BOOTSTRAP_JOB_NAME:-superset-bootstrap}"
  fi
  apply_service_spec
  log "Deployment complete."
}

bootstrap_snowflake() {
  require_env APP_DB APP_SCHEMA CONFIG_STAGE ARTIFACT_STAGE IMAGE_REPO
  require_env SECRET_SUPERSET_DB_URI SECRET_SUPERSET_SECRET_KEY SECRET_SUPERSET_FERNET_KEY SECRET_SUPERSET_ADMINS

  local -a conn=()
  if [[ -n "$SNOW_CONNECTION" ]]; then
    conn+=(--connection "$SNOW_CONNECTION")
  fi

  log "Ensuring database/schema exist: ${APP_DB}.${APP_SCHEMA}"
  snow_exec sql "${conn[@]}" -q "CREATE DATABASE IF NOT EXISTS ${APP_DB};"
  snow_exec sql "${conn[@]}" -q "CREATE SCHEMA IF NOT EXISTS ${APP_DB}.${APP_SCHEMA};"

  log "Ensuring stages exist: ${CONFIG_STAGE}, ${ARTIFACT_STAGE}"
  snow_exec stage create "${conn[@]}" "${CONFIG_STAGE}"
  snow_exec stage create "${conn[@]}" "${ARTIFACT_STAGE}"

  # `snow spcs image-repository create` creates in the current schema; pass db/schema explicitly.
  local repo_name="${IMAGE_REPO##*.}"
  log "Ensuring image repository exists: ${APP_DB}.${APP_SCHEMA}.${repo_name}"
  snow_exec spcs image-repository create "${conn[@]}" --if-not-exists --database "${APP_DB}" --schema "${APP_SCHEMA}" "${repo_name}"

  cat <<EOF
[spcs] Bootstrap done.

Remaining prerequisites (usually manual):
- Create secrets:
  - ${SECRET_SUPERSET_DB_URI} (GENERIC_STRING: full SQLAlchemy DB URI)
  - ${SECRET_SUPERSET_SECRET_KEY} (GENERIC_STRING)
  - ${SECRET_SUPERSET_FERNET_KEY} (GENERIC_STRING)
  - ${SECRET_SUPERSET_ADMINS} (GENERIC_STRING: comma-separated usernames)
- Create/confirm compute pool: ${COMPUTE_POOL}
- (managed profile) Create EAI/network rules if your managed Postgres hostname requires egress: ${PG_EAI_NAME}
EOF
}

push_images() {
  require_env SPCS_IMAGE
  local -a conn=()
  if [[ -n "$SNOW_CONNECTION" ]]; then
    conn+=(--connection "$SNOW_CONNECTION")
  fi

  log "Logging in to Snowflake image registry via Docker"
  snow_exec spcs image-registry login "${conn[@]}"

  local registry_url
  registry_url="$(snow_exec spcs image-registry url "${conn[@]}" --format JSON | python3 -c 'import json,sys\n\ndef find_str(o):\n  if isinstance(o,str):\n    return o\n  if isinstance(o,dict):\n    for v in o.values():\n      s=find_str(v)\n      if s:\n        return s\n  if isinstance(o,list):\n    for v in o:\n      s=find_str(v)\n      if s:\n        return s\n  return None\n\ntry:\n  data=json.load(sys.stdin)\nexcept Exception:\n  sys.exit(1)\n\ns=find_str(data)\nif s:\n  sys.stdout.write(s)')"

  if [[ -z "$registry_url" ]]; then
    echo "Failed to detect Snowflake image registry URL." >&2
    exit 1
  fi

  push_one() {
    local spcs_path=$1
    local source_image=${2:-$IMAGE_NAME}
    local target="${registry_url}/${spcs_path#/}"
    log "Pushing $source_image -> $target"
    docker tag "$source_image" "$target"
    docker push "$target"
  }

  # Push this repo's Superset image (built locally).
  push_one "$SPCS_IMAGE" "$IMAGE_NAME"

  # Optionally push dependency images to Snowflake image repository.
  if [[ -n "${SPCS_REDIS_IMAGE:-}" ]]; then
    docker pull redis:7-alpine
    push_one "$SPCS_REDIS_IMAGE" "redis:7-alpine"
  fi
  if [[ -n "${SPCS_POSTGRES_IMAGE:-}" ]]; then
    docker pull postgres:16-alpine
    push_one "$SPCS_POSTGRES_IMAGE" "postgres:16-alpine"
  fi
}

usage() {
  cat <<USAGE
Usage: $(basename "$0") <command>
Commands:
  build-image             Build the Superset container image
  run-local               Run the container locally (expects Postgres & Redis reachable)
  stop-local              Stop the local container
  exec <cmd>              Run an arbitrary command inside the container
  render-spec <file>      Print an SPCS YAML spec with env substitution
  bootstrap-snowflake     Create DB/SCHEMA/stages/image repo if missing (best-effort)
  push-images             Login to Snowflake image registry and push images (Superset + optional deps)
  sync-config-stage       Upload superset/config/* to the configured Snowflake stage
  apply-service           Render + apply infra/spcs/service.yaml via snow CLI
  run-job <spec> [name]   Render + run an SPCS job spec via snow CLI
  deploy                  Build image, upload config, (optional) bootstrap, and apply service

Snowflake CLI config:
  - Put Snowflake CLI connection settings in $SNOWFLAKE_HOME/config.toml.
  - This repo supports a repo-local config at .snowflake/config.toml (auto-detected).
  - Use SNOW_CONNECTION=<name> to select a non-default connection.
USAGE
}

run_local_container() {
  local fake_user="${SF_FAKE_REMOTE_USER:-admin}"
  docker run --rm -d \
    --name "$CONTAINER_NAME" \
    -p "$SUPERSET_PORT:8088" \
    -v "$ROOT_DIR/superset/config:/config:ro" \
    -e SUPERSET_DB_URI="$POSTGRES_URI" \
    -e SUPERSET_CONFIG_PATH="/config/superset_config.py" \
    -e SUPERSET_SECRET_KEY="$(openssl rand -hex 16)" \
    -e SUPERSET_FERNET_KEY="$(openssl rand -base64 32)" \
    -e SF_FAKE_REMOTE_USER="$fake_user" \
    "$IMAGE_NAME" "$@"
}

cmd=${1:-help}
load_repo_env
case "$cmd" in
  build-image)
    build_image
    ;;
  run-local)
    shift || true
    run_local_container "$@"
    echo "Superset running at http://localhost:$SUPERSET_PORT"
    ;;
  stop-local)
    docker rm -f "$CONTAINER_NAME" >/dev/null 2>&1 || true
    ;;
  exec)
    shift || true
    docker exec -it "$CONTAINER_NAME" "$@"
    ;;
  render-spec)
    shift || true
    file=${1:-}
    if [[ -z "$file" ]]; then
      echo "spec file required" >&2
      exit 1
    fi
    envsubst < "$file"
    ;;
  bootstrap-snowflake)
    bootstrap_snowflake
    ;;
  push-images)
    push_images
    ;;
  sync-config-stage)
    sync_config_stage
    ;;
  apply-service)
    apply_service_spec
    ;;
  run-job)
    shift || true
    spec=${1:-}
    if [[ -z "$spec" ]]; then
      echo "spec file required" >&2
      exit 1
    fi
    job_name=${2:-}
    run_job_spec "$spec" "$job_name"
    ;;
  deploy)
    deploy_all
    ;;
  *)
    usage
    ;;
 esac
