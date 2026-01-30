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
BOOTSTRAP_SPEC=${BOOTSTRAP_SPEC:-}
SNOW_CONNECTION=${SNOW_CONNECTION:-}

# Enable Snowflake CLI debug output when `--debug` is present anywhere in args.
for arg in "$@"; do
  if [[ "$arg" == "--debug" ]]; then
    export SNOW_DEBUG=1
    break
  fi
done

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
  normalize_stage_env
  normalize_image_env
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

pick_bootstrap_spec() {
  if [[ -n "${BOOTSTRAP_SPEC:-}" ]]; then
    echo "$BOOTSTRAP_SPEC"
    return
  fi

  case "${SPCS_PROFILE}" in
    managed) echo "$ROOT_DIR/infra/spcs/jobs/bootstrap_admin.yaml" ;;
    *) echo "$ROOT_DIR/infra/spcs/jobs/bootstrap_admin.yaml" ;;
  esac
}

log() {
  echo "[spcs] $*"
}

validate_identifier() {
  local name=$1
  local value=$2
  if [[ -z "$value" ]]; then
    echo "$name must be set." >&2
    exit 1
  fi
  if [[ ! "$value" =~ ^[A-Za-z0-9_]+$ ]]; then
    echo "$name must match ^[A-Za-z0-9_]+$ (got: $value)" >&2
    exit 1
  fi
}

stage_ref() {
  # Ensure a stage reference is prefixed with '@' (used for copy/mount paths).
  local s=${1:-}
  if [[ -z "$s" ]]; then
    echo ""
    return 0
  fi
  if [[ "$s" == @* ]]; then
    echo "$s"
    return 0
  fi
  echo "@$s"
}

stage_object_name() {
  # Convert a stage reference like "@DB.SCHEMA.STAGE/path" to "DB.SCHEMA.STAGE"
  # (used for CREATE STAGE / snow stage create).
  local s=${1:-}
  s="${s#@}"
  s="${s%%/*}"
  echo "$s"
}

normalize_stage_env() {
  # Allow CONFIG_STAGE/ARTIFACT_STAGE to be specified with or without '@'.
  # Specs/mounts generally want '@', but stage creation rejects it.
  if [[ -n "${CONFIG_STAGE:-}" ]]; then
    CONFIG_STAGE="$(stage_ref "$CONFIG_STAGE")"
  fi
  if [[ -n "${ARTIFACT_STAGE:-}" ]]; then
    ARTIFACT_STAGE="$(stage_ref "$ARTIFACT_STAGE")"
  fi
}

to_lower() {
  tr '[:upper:]' '[:lower:]'
}

normalize_image_env() {
  # Docker image references must be lowercase; Snowflake object names are case-insensitive unless quoted.
  if [[ -n "${SPCS_IMAGE:-}" ]]; then
    SPCS_IMAGE="$(printf '%s' "$SPCS_IMAGE" | to_lower)"
  fi
  if [[ -n "${SPCS_REDIS_IMAGE:-}" ]]; then
    SPCS_REDIS_IMAGE="$(printf '%s' "$SPCS_REDIS_IMAGE" | to_lower)"
  fi
  if [[ -n "${SPCS_POSTGRES_IMAGE:-}" ]]; then
    SPCS_POSTGRES_IMAGE="$(printf '%s' "$SPCS_POSTGRES_IMAGE" | to_lower)"
  fi
}

ensure_compute_pool() {
  # Compute pool creation is account-level and may require elevated privileges.
  # Default behavior: fail-fast with an actionable message.
  require_env COMPUTE_POOL
  validate_identifier "COMPUTE_POOL" "$COMPUTE_POOL"

  local -a conn=()
  if [[ -n "$SNOW_CONNECTION" ]]; then
    conn+=(--connection "$SNOW_CONNECTION")
  fi

  # `DESCRIBE COMPUTE POOL` fails if the pool doesn't exist or isn't authorized.
  if snow_exec sql "${conn[@]}" -q "DESCRIBE COMPUTE POOL ${COMPUTE_POOL};" >/dev/null 2>&1; then
    return 0
  fi

  if [[ "${AUTO_CREATE_COMPUTE_POOL:-0}" != "1" ]]; then
    echo "[spcs] Compute pool '${COMPUTE_POOL}' does not exist or is not authorized." >&2
    echo "[spcs] Create it (or grant access) then retry. Example SQL:" >&2
    echo "CREATE COMPUTE POOL ${COMPUTE_POOL} MIN_NODES = 1 MAX_NODES = 1 INSTANCE_FAMILY = '<FILL_ME>'; " >&2
    echo "[spcs] Or set AUTO_CREATE_COMPUTE_POOL=1 and configure COMPUTE_POOL_INSTANCE_FAMILY / MIN/MAX." >&2
    exit 1
  fi

  require_env COMPUTE_POOL_INSTANCE_FAMILY COMPUTE_POOL_MIN_NODES COMPUTE_POOL_MAX_NODES
  validate_identifier "COMPUTE_POOL_MIN_NODES" "$COMPUTE_POOL_MIN_NODES"
  validate_identifier "COMPUTE_POOL_MAX_NODES" "$COMPUTE_POOL_MAX_NODES"

  log "Creating compute pool ${COMPUTE_POOL} (MIN_NODES=${COMPUTE_POOL_MIN_NODES}, MAX_NODES=${COMPUTE_POOL_MAX_NODES}, INSTANCE_FAMILY=${COMPUTE_POOL_INSTANCE_FAMILY})"
  snow_exec sql "${conn[@]}" -q "CREATE COMPUTE POOL IF NOT EXISTS ${COMPUTE_POOL} MIN_NODES = ${COMPUTE_POOL_MIN_NODES} MAX_NODES = ${COMPUTE_POOL_MAX_NODES} INSTANCE_FAMILY = '${COMPUTE_POOL_INSTANCE_FAMILY}';"
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

  local -a snow_args=("$@")
  if [[ "${SNOW_DEBUG:-0}" == "1" ]]; then
    # Snowflake CLI v3 does not accept `--debug` at the root command, but most leaf
    # commands accept it after the command path (e.g. `snow sql --debug ...`,
    # `snow stage copy --debug ...`, `snow spcs service create --debug ...`).
    if [[ "${#snow_args[@]}" -ge 1 ]]; then
      case "${snow_args[0]}" in
        sql)
          snow_args=(sql --debug "${snow_args[@]:1}")
          ;;
        stage)
          if [[ "${#snow_args[@]}" -ge 2 ]]; then
            snow_args=(stage "${snow_args[1]}" --debug "${snow_args[@]:2}")
          else
            snow_args=(stage --debug)
          fi
          ;;
        spcs)
          if [[ "${#snow_args[@]}" -ge 3 ]]; then
            snow_args=(spcs "${snow_args[1]}" "${snow_args[2]}" --debug "${snow_args[@]:3}")
          else
            snow_args=(spcs --debug "${snow_args[@]:1}")
          fi
          ;;
        *)
          # Best-effort: append to the first command token.
          snow_args=("${snow_args[0]}" --debug "${snow_args[@]:1}")
          ;;
      esac
    fi
  fi

  local -a env_prefix=()
  if [[ -n "$snowflake_home" ]]; then
    maybe_fix_repo_local_snowflake_perms "$snowflake_home"
    env_prefix=(env "SNOWFLAKE_HOME=$snowflake_home")
  fi

  # Make uv/uvx usable in restricted environments by keeping caches inside the repo.
  local uv_cache_dir="${UV_CACHE_DIR:-$ROOT_DIR/.uv-cache}"
  local xdg_data_home="${XDG_DATA_HOME:-$ROOT_DIR/.xdg-data}"
  local xdg_cache_home="${XDG_CACHE_HOME:-$ROOT_DIR/.xdg-cache}"
  env_prefix+=(env "UV_CACHE_DIR=$uv_cache_dir" "XDG_DATA_HOME=$xdg_data_home" "XDG_CACHE_HOME=$xdg_cache_home")

  # Prefer a repo-local venv snow (this also ensures we use the venv's Python, e.g. 3.11+).
  if [[ -x "$ROOT_DIR/.venv/bin/snow" ]]; then
    "${env_prefix[@]}" "$ROOT_DIR/.venv/bin/snow" "${snow_args[@]}"
    return
  fi

  if command -v snow >/dev/null 2>&1; then
    "${env_prefix[@]}" snow "${snow_args[@]}"
    return
  fi

  # If a repo venv exists, use its interpreter for uvx so we can resolve newer snowflake-cli.
  if [[ -x "$ROOT_DIR/.venv/bin/python" ]] && command -v uvx >/dev/null 2>&1; then
    "${env_prefix[@]}" uvx -p "$ROOT_DIR/.venv/bin/python" --from snowflake-cli snow "${snow_args[@]}"
    return
  fi

  if command -v uvx >/dev/null 2>&1; then
    "${env_prefix[@]}" uvx --from snowflake-cli snow "${snow_args[@]}"
    return
  fi

  if command -v uv >/dev/null 2>&1; then
    "${env_prefix[@]}" uv tool run --from snowflake-cli snow "${snow_args[@]}"
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

sql_quote() {
  # Print a safely single-quoted SQL string literal for stdin.
  python3 - <<'PY'
import sys

s = sys.stdin.read()
s = s.replace("'", "''")
sys.stdout.write("'" + s + "'")
PY
}

dotenv_get() {
  # Read a KEY=VALUE entry from repo-local .env (used as a convenience fallback).
  # This is intentionally minimal and only supports simple single-line assignments.
  local key=$1
  local env_file="${ROOT_DIR}/.env"
  if [[ ! -f "$env_file" ]]; then
    return 1
  fi

  awk -v k="$key" '
    BEGIN { found=0 }
    /^[[:space:]]*#/ { next }
    /^[[:space:]]*$/ { next }
    {
      line=$0
      sub(/^[[:space:]]*export[[:space:]]+/, "", line)
      if (index(line, k "=") == 1) {
        val=substr(line, length(k)+2)
        sub(/[[:space:]]+$/, "", val)
        print val
        found=1
        exit
      }
    }
    END { if (!found) exit 1 }
  ' "$env_file"
}

snow_connection_user() {
  # Best-effort: read the "user" field for the selected connection from a TOML file.
  # Supports:
  #   - repo-local: $ROOT_DIR/.snowflake/config.toml
  #   - global: ~/.snowflake/connections.toml
  local conn_name="${SNOW_CONNECTION:-default}"
  local cfg=""
  if [[ -f "$ROOT_DIR/.snowflake/config.toml" ]]; then
    cfg="$ROOT_DIR/.snowflake/config.toml"
  elif [[ -f "$HOME/.snowflake/connections.toml" ]]; then
    cfg="$HOME/.snowflake/connections.toml"
  else
    return 1
  fi

  awk -v section="connections.${conn_name}" '
    $0 ~ "^\\[" section "\\]$" { in=1; next }
    $0 ~ "^\\[" && in { exit }
    in {
      match($0, /user[[:space:]]*=[[:space:]]*\"([^\"]+)\"/, m)
      if (m[1] != "") { print m[1]; exit }
    }
  ' "$cfg"
}

create_secret() {
  local secret_name=$1
  local secret_value=$2

  if [[ -z "$secret_name" ]]; then
    echo "Secret name must be set." >&2
    exit 1
  fi
  if [[ -z "$secret_value" ]]; then
    echo "Secret value for $secret_name must be set." >&2
    exit 1
  fi

  local -a conn=()
  if [[ -n "$SNOW_CONNECTION" ]]; then
    conn+=(--connection "$SNOW_CONNECTION")
  fi

  log "Creating secret ${secret_name} (len=${#secret_value})"
  local quoted
  quoted="$(printf '%s' "$secret_value" | sql_quote)"
  snow_exec sql "${conn[@]}" -q "CREATE OR REPLACE SECRET ${secret_name} TYPE=GENERIC_STRING SECRET_STRING=${quoted};"
}

create_secrets() {
  require_env SECRET_SUPERSET_DB_URI SECRET_SUPERSET_SECRET_KEY SECRET_SUPERSET_FERNET_KEY SECRET_SUPERSET_ADMINS

  local db_uri="${SUPERSET_DB_URI_VALUE:-${SUPERSET_DB_URI:-}}"
  if [[ -z "$db_uri" ]]; then
    db_uri="$(dotenv_get PG_CON 2>/dev/null || true)"
  fi
  if [[ "$db_uri" == postgres://* ]]; then
    db_uri="postgresql+psycopg2://${db_uri#postgres://}"
  fi
  if [[ -z "$db_uri" ]] && [[ "${SPCS_PROFILE}" == "allinone" ]]; then
    db_uri="postgresql+psycopg2://superset:superset@postgres:5432/superset"
  fi
  if [[ -z "$db_uri" ]]; then
    echo "SUPERSET_DB_URI_VALUE (or SUPERSET_DB_URI) must be set to create ${SECRET_SUPERSET_DB_URI}." >&2
    exit 1
  fi

  local secret_key="${SUPERSET_SECRET_KEY_VALUE:-${SUPERSET_SECRET_KEY:-}}"
  if [[ -z "$secret_key" ]]; then
    secret_key="$(dotenv_get SECRET_KEY 2>/dev/null || true)"
  fi
  if [[ -z "$secret_key" ]]; then
    secret_key="$(python3 -c 'import secrets; print(secrets.token_urlsafe(64))')"
  fi

  local fernet_key="${SUPERSET_FERNET_KEY_VALUE:-${SUPERSET_FERNET_KEY:-}}"
  if [[ -z "$fernet_key" ]]; then
    fernet_key="$(dotenv_get FERNET_KEY 2>/dev/null || true)"
  fi
  if [[ -z "$fernet_key" ]]; then
    fernet_key="$(python3 -c 'import os,base64; print(base64.urlsafe_b64encode(os.urandom(32)).decode())')"
  fi

  local admins="${SUPERSET_ADMINS_VALUE:-${SUPERSET_BOOTSTRAP_ADMINS:-${SUPERSET_ADMINS:-}}}"
  if [[ -z "$admins" ]]; then
    admins="$(snow_connection_user 2>/dev/null || true)"
  fi
  if [[ -z "$admins" ]]; then
    admins="admin"
  fi

  log "Creating/updating Superset secrets in Snowflake (values are not printed)"
  create_secret "$SECRET_SUPERSET_DB_URI" "$db_uri"
  create_secret "$SECRET_SUPERSET_SECRET_KEY" "$secret_key"
  create_secret "$SECRET_SUPERSET_FERNET_KEY" "$fernet_key"
  create_secret "$SECRET_SUPERSET_ADMINS" "$admins"
  log "Secrets created/updated."
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
  local dest
  dest="$(stage_ref "$CONFIG_STAGE")"
  dest="${dest%/}/"
  log "Uploading superset/config/ to stage $dest"
  snow_exec stage create "${conn[@]}" "$(stage_object_name "$CONFIG_STAGE")" >/dev/null 2>&1 || true
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
  require_env COMPUTE_POOL SERVICE_NAME SPCS_IMAGE CONFIG_STAGE
  if [[ "${SPCS_PROFILE}" == "allinone" ]]; then
    require_env SUPERSET_SECRET_KEY_VALUE SUPERSET_FERNET_KEY_VALUE SUPERSET_ADMINS_VALUE
  fi
  local spec_file
  spec_file="$(pick_service_spec)"
  local rendered
  rendered=$(render_to_tmp "$spec_file")
  log "Applying service spec to $SERVICE_NAME on pool $COMPUTE_POOL"
  local -a conn=()
  if [[ -n "$SNOW_CONNECTION" ]]; then
    conn+=(--connection "$SNOW_CONNECTION")
  fi
  # Snowflake CLI v3 uses positional <name> and `--spec-path`.
  # Create if missing, then upgrade to apply the rendered spec.
  snow_exec spcs service create "${conn[@]}" \
    "$SERVICE_NAME" \
    --compute-pool "$COMPUTE_POOL" \
    --spec-path "$rendered" \
    --if-not-exists
  snow_exec spcs service upgrade "${conn[@]}" \
    "$SERVICE_NAME" \
    --spec-path "$rendered"
  rm -f "$rendered"
}

run_job_spec() {
  local spec=$1
  local job_name=${2:-${JOB_NAME:-superset-job}}
  # Snowflake CLI validates names; hyphens are rejected. Keep this forgiving.
  job_name="${job_name//-/_}"
  if [[ "${JOB_NAME_UNIQUE:-0}" == "1" ]]; then
    job_name="${job_name}_$(date -u +%Y%m%d_%H%M%S)"
  fi
  require_env SPCS_IMAGE CONFIG_STAGE
  require_env COMPUTE_POOL
  local rendered
  rendered=$(render_to_tmp "$spec")
  log "Running job $job_name on pool $COMPUTE_POOL"
  local -a conn=()
  if [[ -n "$SNOW_CONNECTION" ]]; then
    conn+=(--connection "$SNOW_CONNECTION")
  fi
  # Snowflake CLI v3 uses positional <name> and `--spec-path`.
  local out
  if ! out="$(
    snow_exec spcs service execute-job "${conn[@]}" \
      "$job_name" \
      --compute-pool "$COMPUTE_POOL" \
      --spec-path "$rendered" 2>&1
  )"; then
    # `execute-job` fails if a service with the same name exists (often from a prior failed run).
    # Retry after dropping the job service via SQL (more stable than guessing CLI subcommands).
    shopt -s nocasematch
    if [[ "$out" == *"already exists"* ]]; then
      log "Job service $job_name already exists; dropping and retrying"
      snow_exec sql "${conn[@]}" -q "DROP SERVICE IF EXISTS ${job_name};"
      snow_exec spcs service execute-job "${conn[@]}" \
        "$job_name" \
        --compute-pool "$COMPUTE_POOL" \
        --spec-path "$rendered"
    else
      shopt -u nocasematch
      echo "$out" >&2
      rm -f "$rendered"
      exit 1
    fi
    shopt -u nocasematch
  else
    echo "$out"
  fi
  rm -f "$rendered"
}

deploy_all() {
  if [[ "${AUTO_BOOTSTRAP_SNOWFLAKE:-1}" == "1" ]]; then
    bootstrap_snowflake
  fi
  ensure_compute_pool
  build_image
  if [[ "${PUSH_IMAGE:-0}" == "1" ]]; then
    push_images
  else
    if [[ -n "${IMAGE_PUSH_TARGET:-}" ]]; then
      maybe_push_image
    else
      log "Skipping image push (set PUSH_IMAGE=1 to push to Snowflake image registry)"
    fi
  fi
  sync_config_stage
  if [[ "${RUN_BOOTSTRAP:-0}" == "1" ]]; then
    if [[ "${SPCS_PROFILE}" == "allinone" ]]; then
      log "Skipping bootstrap job for allinone: Postgres/Redis are inside the service; migrations run in the Superset container on startup."
    else
      run_job_spec "$(pick_bootstrap_spec)" "${BOOTSTRAP_JOB_NAME:-superset_bootstrap}"
    fi
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
  snow_exec stage create "${conn[@]}" "$(stage_object_name "$CONFIG_STAGE")"
  snow_exec stage create "${conn[@]}" "$(stage_object_name "$ARTIFACT_STAGE")"

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
  - Tip: `./cli/spcs.sh create-secrets` can create these (generates keys if missing).
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
  log "Detecting Snowflake image registry URL"

  local registry_json
  if ! registry_json="$(snow_exec spcs image-registry url "${conn[@]}" --format json 2>/dev/null || snow_exec spcs image-registry url "${conn[@]}")"; then
    echo "Failed to fetch Snowflake image registry URL (snow spcs image-registry url)." >&2
    exit 1
  fi

  if ! registry_url="$(
    REGISTRY_JSON="$registry_json" python3 - <<'PY'
import json
import os
import re
import sys

raw = os.environ.get("REGISTRY_JSON", "")


def collect_strings(obj, out):
    if isinstance(obj, str):
        out.append(obj)
    elif isinstance(obj, dict):
        for v in obj.values():
            collect_strings(v, out)
    elif isinstance(obj, list):
        for v in obj:
            collect_strings(v, out)


def pick_registry(strings):
    # Prefer the canonical Snowflake registry hostname if present.
    for s in strings:
        if isinstance(s, str) and "registry.snowflakecomputing.com" in s:
            return s.strip().rstrip("/")

    # Otherwise, pick the "most registry-like" string.
    candidates = []
    for s in strings:
        if not isinstance(s, str):
            continue
        s = s.strip()
        if not s:
            continue
        if " " in s:
            continue
        if "." not in s:
            continue
        lower = s.lower()
        score = 0
        if "registry" in lower:
            score += 10
        if "snowflake" in lower:
            score += 5
        if lower.startswith("http://") or lower.startswith("https://"):
            score += 2
        candidates.append((score, len(s), s))
    if not candidates:
        return None
    candidates.sort(reverse=True)
    return candidates[0][2].rstrip("/")


def fallback_from_text(text):
    patterns = [
        r"[A-Za-z0-9][A-Za-z0-9.-]*registry\\.snowflakecomputing\\.com",
        r"https?://[^\\s\"']+",
    ]
    for pat in patterns:
        m = re.search(pat, text)
        if m:
            return m.group(0).strip().rstrip("/")
    return None


strings = []
try:
    data = json.loads(raw)
except Exception:
    data = None

url = None
if data is not None:
    collect_strings(data, strings)
    url = pick_registry(strings)
if not url:
    url = fallback_from_text(raw)
if not url:
    sys.exit(3)

sys.stdout.write(url)
PY
  )"; then
    echo "Failed to detect Snowflake image registry URL from output." >&2
    echo "Raw output:" >&2
    echo "$registry_json" >&2
    exit 1
  fi

  # Sanitize for docker (strip scheme, whitespace, CRLF, trailing slashes).
  registry_url="$(printf '%s' "$registry_url" | tr -d '\r\n' | sed -e 's#^https\\?://##' -e 's#^[[:space:]]*##' -e 's#[[:space:]]*$##' -e 's#/*$##')"
  if [[ -z "$registry_url" ]] || [[ "$registry_url" == /* ]]; then
    echo "Failed to detect Snowflake image registry URL." >&2
    echo "Raw output:" >&2
    echo "$registry_json" >&2
    exit 1
  fi

  push_one() {
    local spcs_path=$1
    local source_image=${2:-$IMAGE_NAME}
    # Docker enforces lowercase repository names. Snowflake object identifiers are case-insensitive,
    # so push using a lowercase path.
    local repo_path="${spcs_path#/}"
    repo_path="${repo_path,,}"
    local target="${registry_url}/${repo_path}"
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
  create-secrets          Create/replace required Superset secrets in Snowflake
  endpoints               Show public endpoint URL(s) for SERVICE_NAME
  debug-service [id]      Print describe + logs for SERVICE_NAME (default instance id: 0)
  drop-service            Drop the SPCS service (use --force to delete volumes)
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

service_endpoints() {
  require_env SERVICE_NAME

  local -a conn=()
  if [[ -n "$SNOW_CONNECTION" ]]; then
    conn+=(--connection "$SNOW_CONNECTION")
  fi

  log "Service endpoints: ${SERVICE_NAME}"
  snow_exec spcs service list-endpoints "${conn[@]}" "${SERVICE_NAME}"
}

drop_service() {
  require_env SERVICE_NAME
  local force=${1:-0}

  local -a conn=()
  if [[ -n "$SNOW_CONNECTION" ]]; then
    conn+=(--connection "$SNOW_CONNECTION")
  fi

  if [[ "$force" == "1" ]]; then
    log "Dropping service ${SERVICE_NAME} (FORCE)"
    snow_exec sql "${conn[@]}" -q "DROP SERVICE IF EXISTS ${SERVICE_NAME} FORCE;"
  else
    log "Dropping service ${SERVICE_NAME}"
    snow_exec spcs service drop "${conn[@]}" "${SERVICE_NAME}"
  fi
}

debug_service() {
  require_env SERVICE_NAME
  local instance_id=${1:-0}

  local -a conn=()
  if [[ -n "$SNOW_CONNECTION" ]]; then
    conn+=(--connection "$SNOW_CONNECTION")
  fi

  log "Service describe: ${SERVICE_NAME}"
  snow_exec spcs service describe "${conn[@]}" "${SERVICE_NAME}" || true

  log "Service instances: ${SERVICE_NAME}"
  snow_exec spcs service list-instances "${conn[@]}" "${SERVICE_NAME}" || true

  log "Service containers: ${SERVICE_NAME}"
  snow_exec spcs service list-containers "${conn[@]}" "${SERVICE_NAME}" || true

  local c
  for c in superset postgres redis; do
    log "Logs (${c}) instance=${instance_id} previous"
    snow_exec spcs service logs "${conn[@]}" "${SERVICE_NAME}" \
      --container-name "$c" \
      --instance-id "$instance_id" \
      --previous-logs \
      --num-lines 400 || true
    log "Logs (${c}) instance=${instance_id} current"
    snow_exec spcs service logs "${conn[@]}" "${SERVICE_NAME}" \
      --container-name "$c" \
      --instance-id "$instance_id" \
      --num-lines 200 || true
  done
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
  create-secrets)
    create_secrets
    ;;
  endpoints)
    service_endpoints
    ;;
  debug-service)
    shift || true
    debug_service "${1:-0}"
    ;;
  drop-service)
    shift || true
    if [[ "${1:-}" == "--force" ]]; then
      drop_service 1
    else
      drop_service 0
    fi
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
