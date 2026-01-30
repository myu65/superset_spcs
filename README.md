# Superset on SPCS Toolkit

This repo captures everything needed to build, ship, and operate a Snowflake Superset deployment fully inside Snowpark Container Services.

このリポジトリは、Snowflake の Snowpark Container Services (SPCS) 上で Apache Superset を構築・ビルド・運用するための設定一式をまとめたものです。ローカル検証 → SPCS デプロイまでを `./cli/spcs.sh` で揃え、名前・配置は `config/spcs.env` に集約します。

## Layout

- `cli/` — helper entrypoints (`spcs.sh`) for local builds, tests, and templating SPCS specs.
- `docker/` — container build context (Dockerfile + Python deps).
- `superset/config/` — Superset runtime config (`superset_config.py` + middleware + security manager). In SPCS this directory is uploaded to an internal stage and mounted at `/config`. Locally it is bind-mounted at `/config`.
- `infra/spcs/` — declarative specs for the long-running service plus one-off job specs (bootstrap admin, pg_restore, etc.).
- `config/` — repo-local deployment configuration (DB/SCHEMA, stages, secrets, image repo, service name, etc.).
- `scripts/` — drop auxiliary automation here (none yet).

## Prereqs

- Docker CLI for local builds/runs.
- `envsubst` for spec rendering (part of GNU gettext). 
- (Optional) `uv` for local unit tests.

## CLI usage

```bash
# Build the image (Superset config is mounted at runtime via /config)
./cli/spcs.sh build-image

# Run unit tests (local via uv)
uv python install 3.11
uv venv .venv
source .venv/bin/activate
uv pip install -e .[dev]
uv run pytest

# Local stack via Docker Compose
cp .env.local.example .env.local   # edit secrets/ports if needed
docker compose up --build
# Then open http://localhost:${SUPERSET_PORT:-8088}
docker compose down -v             # tear down when done

# Override the fake REMOTE_USER for local runs
echo "SF_FAKE_REMOTE_USER=alice" >> .env.local
docker compose up --build          # Superset now logs in as 'alice'
```

## SPCS Deployment Overview

SPCS requires that **all container images referenced in a service spec** are available in your Snowflake account image registry / image repository.

This repo supports two deployment profiles (set `SPCS_PROFILE` in `config/spcs.env`):

- `managed` — Superset + Redis run in SPCS. Metadata DB is external (e.g. Snowflake managed Postgres) and is passed via a secret.
- `allinone` — Superset + Redis + Postgres run in one SPCS service. Postgres data persists on a block volume.

Note: For `allinone`, don't run a separate bootstrap job (jobs must exit, but Postgres/Redis are long-running). The Superset container runs `superset db upgrade/init` on startup after Postgres is reachable. The `allinone` service spec injects `SUPERSET_SECRET_KEY`/`SUPERSET_FERNET_KEY`/`SUPERSET_BOOTSTRAP_ADMINS` from `config/spcs.env` (`*_VALUE` variables).

## Automated deployment

Run `./cli/spcs.sh deploy` to execute the whole pipeline in one go (it will source `config/spcs.env` automatically):

1. `bootstrap-snowflake` (best-effort; enabled by `AUTO_BOOTSTRAP_SNOWFLAKE=1`)
2. Build the Docker image
3. (Optional) push images to Snowflake image registry (`PUSH_IMAGE=1`)
4. Upload `superset/config/*` to `CONFIG_STAGE`
5. (Optional) run bootstrap job (`RUN_BOOTSTRAP=1`) to run `superset db upgrade/init` and create admin users
6. Create/replace the SPCS service with the selected profile spec

Configuration is expected to live in `config/spcs.env` (copy from `config/spcs.env.example`).

Note: `bootstrap-snowflake` does not create secrets (they must contain values), compute pools, or EAI/network rules.
Tip: `./cli/spcs.sh create-secrets` can create/replace the required Superset secrets (and can auto-generate `SECRET_KEY`/`FERNET_KEY`). Configure these in `config/spcs.env` (or pass them inline):

- `SUPERSET_DB_URI_VALUE` — SQLAlchemy DB URI (required for `managed`; optional for `allinone`)
- `SUPERSET_SECRET_KEY_VALUE`
- `SUPERSET_FERNET_KEY_VALUE`
- `SUPERSET_ADMINS_VALUE` — comma-separated usernames
For convenience, `create-secrets` will also fall back to repo-local `.env` keys (`PG_CON`, `SECRET_KEY`, `FERNET_KEY`) if present.
If you hit `Service <JOB_NAME> already exists` when re-running jobs, set `JOB_NAME_UNIQUE=1` to suffix job names with a timestamp.

### Commands

The CLI includes:

- `./cli/spcs.sh bootstrap-snowflake` — create DB/SCHEMA, stages, and the image repository if missing
- `./cli/spcs.sh create-secrets` — create/replace required Superset secrets in Snowflake
- `./cli/spcs.sh push-images` — login to Snowflake image registry and push images (Superset + optional deps)
- `./cli/spcs.sh sync-config-stage`
- `./cli/spcs.sh apply-service`
- `./cli/spcs.sh run-job infra/spcs/jobs/pg_restore.yaml superset-restore`

All SPCS commands require Snowflake CLI (`snow`). If `snow` isn't installed, the CLI will try to run it via uv (`uvx --from snowflake-cli snow ...`).

Note: Snowflake CLI v2 uses `snow stage copy` to upload/download files; there is no `snow stage put`.

### Snowflake CLI connection config

You can keep Snowflake CLI config repo-local:

1. Copy `.snowflake/config.toml.example` to `.snowflake/config.toml` and fill in values.
2. Run any `./cli/spcs.sh ...` command; the CLI will auto-set `SNOWFLAKE_HOME` to `./.snowflake` if that file exists.
3. If you use multiple connections, set `SNOW_CONNECTION=<name>` to select one.

For repo-local `.snowflake/config.toml`, the CLI will also auto-fix permissions (dir `0700`, file `0600`) to satisfy Snowflake CLI security checks.

### Repo-local naming config (recommended)

Copy `config/spcs.env.example` to `config/spcs.env` and edit. `./cli/spcs.sh` will auto-source it on every run.
This is where you define:

- DB/schema names
- stage names for config/assets
- secret names
- (optional) secret values used by `create-secrets`: `SUPERSET_DB_URI_VALUE`, `SUPERSET_SECRET_KEY_VALUE`, `SUPERSET_FERNET_KEY_VALUE`, `SUPERSET_ADMINS_VALUE`
- image repository + image reference
- compute pool and service name

Also pick which SPCS spec you want via `SPCS_PROFILE=managed` or `SPCS_PROFILE=allinone`.

By default, `deploy` runs `bootstrap-snowflake` first (controlled by `AUTO_BOOTSTRAP_SNOWFLAKE=1`), which creates DB/SCHEMA, stages, and the image repository if missing.

## 日本語での使い方まとめ

1. `config/spcs.env.example` を `config/spcs.env` にコピーして編集（DB/SCHEMA、ステージ名、Secret名、Image Repo、compute pool、service 名、`SPCS_PROFILE` などを集約）。
2. `.snowflake/config.toml.example` を `.snowflake/config.toml` にコピーして編集（Snowflake CLI 接続設定。必要なら `SNOW_CONNECTION` で別名接続を選択）。
3. `./cli/spcs.sh deploy`（必要なものの best-effort 作成 → イメージbuild → (任意) push → configアップロード → (任意) bootstrap job → service適用）。
4. ローカル検証は `docker compose up --build` が簡単です（Superset + Postgres + Redis をまとめて起動）。

※ `bootstrap-snowflake` は DB/SCHEMA/ステージ/image repository までの best-effort 作成です。Secret（値が必要）、compute pool、EAI/network rule は別途用意が必要です。Secret は `./cli/spcs.sh create-secrets` でまとめて作れます（`config/spcs.env.example` に例あり）。

### デプロイをワンコマンドで実行する

`./cli/spcs.sh deploy` を実行すると、下記が一括で進みます。

1. 必要な Snowflake オブジェクト作成（`AUTO_BOOTSTRAP_SNOWFLAKE=1` の場合。DB/SCHEMA、ステージ、image repository）
2. Docker イメージのビルド
3. `PUSH_IMAGE=1` の場合は Snowflake image registry に push
4. `superset/config/*` の Snowflake ステージへのアップロード
5. `RUN_BOOTSTRAP=1` の場合は `bootstrap_admin.yaml` ジョブで `superset db upgrade/init` と管理ユーザー作成
6. 選択したプロファイル (`SPCS_PROFILE`) の service spec を適用

主要な設定（`config/spcs.env`）:

- `APP_DB` / `APP_SCHEMA`
- `CONFIG_STAGE` / `ARTIFACT_STAGE`
- `SECRET_SUPERSET_*`（DB URI / SECRET_KEY / FERNET_KEY / 管理者リスト）
- （任意）Secret に入れる値（`create-secrets` 用）: `SUPERSET_DB_URI_VALUE`, `SUPERSET_SECRET_KEY_VALUE`, `SUPERSET_FERNET_KEY_VALUE`, `SUPERSET_ADMINS_VALUE`
- `IMAGE_REPO` / `SPCS_IMAGE`（必要なら `SPCS_REDIS_IMAGE` / `SPCS_POSTGRES_IMAGE`）
- `COMPUTE_POOL` / `SERVICE_NAME`
- `SPCS_PROFILE`（`managed` or `allinone`）
- `AUTO_BOOTSTRAP_SNOWFLAKE` / `PUSH_IMAGE` / `RUN_BOOTSTRAP`

補助コマンド:

- `./cli/spcs.sh create-secrets` — Superset が必要とする Snowflake Secret をまとめて作成/更新（値は表示しません）
- `./cli/spcs.sh sync-config-stage` — 設定ファイルだけアップロード
- `./cli/spcs.sh apply-service` — サービス定義だけ適用
- `./cli/spcs.sh run-job <spec> [job名]` — どの Job spec でも Snowflake CLI 経由で実行

### Docker Compose でローカル一式を起動する

1. `.env.local.example` を `.env.local` にコピーし、`SUPERSET_SECRET_KEY` やポート番号を必要に応じて変更します。
2. `docker compose up --build` を実行すると、Superset（このリポジトリのDockerfile）、Postgres、Redis がまとめて起動します。
3. ブラウザで `http://localhost:${SUPERSET_PORT:-8088}` を開いて動作確認します。
4. 終了する際は `docker compose down -v` でコンテナ＋ボリュームを掃除できます。

Compose や `run-local` では SPCS ingress ヘッダーが無いため、`.env.local` の `SF_FAKE_REMOTE_USER` でダミーの REMOTE_USER を注入しています。任意のユーザー名を指定すれば、そのユーザーで直接ログインできます（例: `echo "SF_FAKE_REMOTE_USER=alice" >> .env.local`）。

※ Compose の変数展開 (`${VAR}`) は `.env` やシェル環境のみを見るため、`env_file` (`.env.local`) に定義した値ではデフォルト式が再評価されません。このリポジトリの `docker-compose.yml` は `SF_FAKE_REMOTE_USER` 自体をコンテナへ渡すようにしているので、`.env.local` に設定しておけば確実に上書きできます。`./cli/spcs.sh run-local` も同じ変数を読んで Superset に渡します。

Snowflake CLI (`snow`) は、手元に `snow` バイナリが無い場合でも `uvx --from snowflake-cli snow ...` で実行できます（初回はダウンロードが走ります）。

### Snowflake CLI の接続設定 (日本語)

`snow` の接続情報は Snowflake CLI の設定ファイルに書きます。このリポではリポジトリ内に置けるようにしています。

1. `.snowflake/config.toml.example` を `.snowflake/config.toml` にコピーして値を埋める（このファイルは `.gitignore` 済み）。
2. その状態で `./cli/spcs.sh deploy` などを実行すると、このリポの CLI が自動で `SNOWFLAKE_HOME=./.snowflake` を使って `snow` を呼びます。
3. 接続を複数持つ場合は `SNOW_CONNECTION=<接続名>` を指定して切り替えできます（例: `SNOW_CONNECTION=spcs-dev ./cli/spcs.sh deploy`）。

### まとめて設定する場所 (日本語)

このリポでは「名前や配置」を `config/spcs.env` に集約します（`config/spcs.env.example` をコピーして編集）。
ここに書くもの:

- Snowflake側の DB/SCHEMA 名 (`APP_DB`, `APP_SCHEMA`)
- ステージ名 (`CONFIG_STAGE`, `ARTIFACT_STAGE`)
- Secret名（`SECRET_SUPERSET_DB_URI`, `SECRET_SUPERSET_SECRET_KEY`, `SECRET_SUPERSET_FERNET_KEY`, `SECRET_SUPERSET_ADMINS`）
- （任意）Secret に入れる値（`create-secrets` 用）: `SUPERSET_DB_URI_VALUE`, `SUPERSET_SECRET_KEY_VALUE`, `SUPERSET_FERNET_KEY_VALUE`, `SUPERSET_ADMINS_VALUE`
- Image Repository 名と、SPCS spec で参照するイメージパス (`IMAGE_REPO`, `SPCS_IMAGE`)
- compute pool / service 名 (`COMPUTE_POOL`, `SERVICE_NAME`)
- どの構成を適用するか (`SPCS_PROFILE=managed` or `allinone`)

これを埋めたら基本は `./cli/spcs.sh deploy` で進められます。
