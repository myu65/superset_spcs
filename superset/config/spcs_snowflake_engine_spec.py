import logging
import os
from typing import Any

import snowflake.connector
from flask import has_request_context, request
from sqlalchemy import create_engine
from sqlalchemy.engine.url import make_url
from sqlalchemy.pool import NullPool
from superset.db_engine_specs.snowflake import SnowflakeEngineSpec

_TOKEN_PATH = os.getenv("SNOWFLAKE_SERVICE_TOKEN_PATH", "/snowflake/session/token")

_logger = logging.getLogger(__name__)

_ACCOUNT_ENV_CANDIDATES = (
    "SNOWFLAKE_ACCOUNT",
    "SNOWFLAKE_ACCOUNT_NAME",
    "SNOWFLAKE_ACCOUNT_IDENTIFIER",
    "SNOWFLAKE_ACCOUNT_LOCATOR",
)


def _read_service_token() -> str | None:
    try:
        with open(_TOKEN_PATH, "r", encoding="utf-8") as handle:
            token = handle.read().strip()
        if not token:
            raise RuntimeError(f"SPCS OAuth token file is empty (path={_TOKEN_PATH})")
        return token
    except FileNotFoundError:
        return None
    except PermissionError as ex:
        raise RuntimeError(
            f"SPCS OAuth token exists but is not readable (path={_TOKEN_PATH}). "
            "Check container user/permissions, or set SNOWFLAKE_SERVICE_TOKEN_PATH."
        ) from ex
    except Exception as ex:
        raise RuntimeError(f"Failed to read SPCS OAuth token (path={_TOKEN_PATH}): {type(ex).__name__}") from ex


def _env_first(keys: tuple[str, ...]) -> str | None:
    for key in keys:
        value = os.getenv(key)
        if value:
            return value
    return None


def _infer_host(account: str) -> str:
    # If this already looks like a hostname, keep it.
    if "." in account and account.endswith(".snowflakecomputing.com"):
        return account
    return f"{account}.snowflakecomputing.com"


def _normalize_host(host: str) -> str:
    host = host.strip()
    if host.startswith("https://"):
        host = host.removeprefix("https://")
    if host.startswith("http://"):
        host = host.removeprefix("http://")
    # Drop any port.
    host = host.split("/", 1)[0]
    host = host.split(":", 1)[0]
    return host


def _infer_account_from_host(host: str) -> str | None:
    host = _normalize_host(host)
    for suffix in (".snowflakecomputing.com", ".privatelink.snowflakecomputing.com"):
        if host.endswith(suffix):
            return host[: -len(suffix)] or None
    return None


def _split_database_schema(database: str | None) -> tuple[str | None, str | None]:
    if not database:
        return None, None
    if "/" in database:
        db, schema = database.split("/", 1)
        return db or None, schema or None
    return database, None


class SPCSSnowflakeEngineSpec(SnowflakeEngineSpec):
    """
    Snowflake in SPCS (trial-friendly): connect using the injected OAuth token.

    - Service user: token from /snowflake/session/token
    - Caller’s rights (optional): <service_token>.<Sf-Context-Current-User-Token>
      (Only works inside an HTTP request context; no background jobs.)
    """

    engine = "snowflake"
    engine_name = "Snowflake (SPCS)"

    @classmethod
    def _get_connect_kwargs(cls, uri: str, connect_args: dict[str, Any] | None = None) -> dict[str, Any]:
        connect_args = dict(connect_args or {})
        for reserved_key in ("account", "host", "authenticator", "token", "user", "username", "password"):
            connect_args.pop(reserved_key, None)

        url = make_url(uri)
        q = dict(url.query)

        db_from_path, schema_from_path = _split_database_schema(url.database)
        database = db_from_path or q.get("database") or os.getenv("SNOWFLAKE_DATABASE")
        schema = schema_from_path or q.get("schema") or os.getenv("SNOWFLAKE_SCHEMA")
        warehouse = q.get("warehouse") or os.getenv("SNOWFLAKE_WAREHOUSE")
        role = q.get("role") or os.getenv("SNOWFLAKE_ROLE")

        account = _env_first(_ACCOUNT_ENV_CANDIDATES) or _infer_account_from_host(os.getenv("SNOWFLAKE_HOST", ""))
        if not account:
            raise RuntimeError(
                "Missing Snowflake account env var (expected one of: "
                + ", ".join(_ACCOUNT_ENV_CANDIDATES)
                + ")"
            )

        # Prefer Snowflake-provided host, but fall back to <account>.snowflakecomputing.com.
        host = _normalize_host(os.getenv("SNOWFLAKE_HOST", "")) or _infer_host(account)

        connect_kwargs: dict[str, Any] = {"host": host, "account": account, "authenticator": "oauth"}

        spcs_auth = (q.get("spcs_auth") or os.getenv("SPCS_SNOWFLAKE_AUTH", "service")).lower()
        if spcs_auth == "caller":
            if not has_request_context():
                raise RuntimeError("spcs_auth=caller requires an HTTP request context")
            if not request.headers.get("Sf-Context-Current-User-Token"):
                raise RuntimeError("Missing Sf-Context-Current-User-Token header (execute-as-caller not enabled?)")

        if warehouse:
            connect_kwargs["warehouse"] = warehouse
        if role:
            connect_kwargs["role"] = role
        if database:
            connect_kwargs["database"] = database
        if schema:
            connect_kwargs["schema"] = schema

        connect_kwargs.update(connect_args)
        return connect_kwargs

    @classmethod
    def get_sqlalchemy_engine(  # type: ignore[override]
        cls,
        uri: str,
        connect_args: dict[str, Any] | None = None,
        **kwargs: Any,
    ):
        # Only fall back to the stock Snowflake engine outside SPCS.
        token = _read_service_token()
        if not token:
            return create_engine(uri, connect_args=dict(connect_args or {}), **kwargs)

        if os.getenv("SPCS_SNOWFLAKE_DEBUG") == "1":
            _logger.info("SPCS Snowflake EngineSpec enabled (token_len=%s, token_path=%s)", len(token), _TOKEN_PATH)

        connect_kwargs = cls._get_connect_kwargs(uri, connect_args=connect_args)

        url = make_url(uri)
        q = dict(url.query)
        spcs_auth = (q.get("spcs_auth") or os.getenv("SPCS_SNOWFLAKE_AUTH", "service")).lower()

        if os.getenv("SPCS_SNOWFLAKE_DEBUG") == "1":
            _logger.info(
                "SPCS Snowflake connect config host=%s account=%s warehouse=%s role=%s database=%s schema=%s spcs_auth=%s",
                connect_kwargs.get("host"),
                connect_kwargs.get("account"),
                connect_kwargs.get("warehouse"),
                connect_kwargs.get("role"),
                connect_kwargs.get("database"),
                connect_kwargs.get("schema"),
                spcs_auth,
            )

        def creator():
            # Read token per connection to handle rotation.
            service_token = _read_service_token()
            if not service_token:
                raise RuntimeError(f"SPCS OAuth token is not available (path={_TOKEN_PATH})")

            token = service_token
            if spcs_auth == "caller":
                caller_token = request.headers.get("Sf-Context-Current-User-Token")
                if not caller_token:
                    raise RuntimeError("Missing Sf-Context-Current-User-Token header (execute-as-caller not enabled?)")
                token = f"{service_token}.{caller_token}"

            return snowflake.connector.connect(**(connect_kwargs | {"token": token}))

        engine_kwargs: dict[str, Any] = dict(kwargs)
        engine_kwargs.setdefault("pool_pre_ping", True)
        engine_kwargs.setdefault("pool_recycle", int(os.getenv("SPCS_SNOWFLAKE_POOL_RECYCLE", "3300")))
        if spcs_auth == "caller":
            engine_kwargs["poolclass"] = NullPool

        # Use the Snowflake dialect, but override connection creation with our creator().
        return create_engine("snowflake://", creator=creator, **engine_kwargs)

    @classmethod
    def get_sqla_engine(cls, uri: str, connect_args: dict[str, Any] | None = None, **kwargs: Any):  # type: ignore[override]
        return cls.get_sqlalchemy_engine(uri, connect_args=connect_args, **kwargs)
