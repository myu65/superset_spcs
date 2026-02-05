import logging
import os

from flask_appbuilder.security.manager import AUTH_REMOTE_USER

from sf_security import SnowflakeSyncedSecurityManager
from snowflake_remote_user_middleware import SnowflakeRemoteUserMiddleware

logger = logging.getLogger(__name__)


AUTH_TYPE = AUTH_REMOTE_USER
AUTH_REMOTE_USER_ENV_VAR = "REMOTE_USER"
AUTH_USER_REGISTRATION = True
AUTH_USER_REGISTRATION_ROLE = os.getenv("SUPERSET_DEFAULT_ROLE", "Gamma")
CUSTOM_SECURITY_MANAGER = SnowflakeSyncedSecurityManager
ADDITIONAL_MIDDLEWARE = [SnowflakeRemoteUserMiddleware]

SQLALCHEMY_DATABASE_URI = os.environ["SUPERSET_DB_URI"]
SQLALCHEMY_TRACK_MODIFICATIONS = False

FAB_ADD_SECURITY_API = True

# Override the Snowflake engine spec so Superset connections can use the SPCS-injected
# OAuth token (/snowflake/session/token) instead of reaching the public endpoint.
CUSTOM_ENGINE_SPECS = {"snowflake": "spcs_snowflake_engine_spec.SPCSSnowflakeEngineSpec"}

# Some Superset builds ignore CUSTOM_ENGINE_SPECS overrides; also monkey-patch the
# stock SnowflakeEngineSpec. The patch is safe outside SPCS because our EngineSpec
# falls back to the stock implementation when the token file is missing.
try:
    from spcs_snowflake_engine_spec import SPCSSnowflakeEngineSpec
    from superset.db_engine_specs.snowflake import SnowflakeEngineSpec as _BaseSnowflakeEngineSpec

    _BaseSnowflakeEngineSpec.get_sqla_engine = classmethod(  # type: ignore[method-assign]
        lambda cls, uri, connect_args=None, **kwargs: SPCSSnowflakeEngineSpec.get_sqla_engine(
            uri,
            connect_args=connect_args,
            **kwargs,
        )
    )
except Exception:
    logger.exception("Failed to patch SnowflakeEngineSpec for SPCS token auth")


def _spcs_read_service_token() -> str | None:
    token_path = os.getenv("SNOWFLAKE_SERVICE_TOKEN_PATH", "/snowflake/session/token")
    try:
        with open(token_path, "r", encoding="utf-8") as handle:
            token = handle.read().strip()
        return token or None
    except FileNotFoundError:
        return None


def _spcs_env_account() -> str | None:
    for key in (
        "SNOWFLAKE_ACCOUNT",
        "SNOWFLAKE_ACCOUNT_NAME",
        "SNOWFLAKE_ACCOUNT_IDENTIFIER",
        "SNOWFLAKE_ACCOUNT_LOCATOR",
    ):
        value = os.getenv(key)
        if value:
            return value
    host = (os.getenv("SNOWFLAKE_HOST") or "").strip()
    if host.startswith("https://"):
        host = host.removeprefix("https://")
    if host.startswith("http://"):
        host = host.removeprefix("http://")
    host = host.split("/", 1)[0].split(":", 1)[0]
    for suffix in (".snowflakecomputing.com", ".privatelink.snowflakecomputing.com"):
        if host.endswith(suffix):
            return host[: -len(suffix)] or None
    return None


# As a last line of defense, patch the Snowflake connector to use the SPCS-injected
# OAuth token when available. This ensures placeholder hosts like `@ignored` never
# get used inside SPCS even if an engine spec override is bypassed.
try:
    import snowflake.connector as _spcs_sf_connector

    _spcs_orig_connect = _spcs_sf_connector.connect
    _spcs_orig_Connect = getattr(_spcs_sf_connector, "Connect", None)

    def _spcs_connect(*args, **kwargs):
        token = _spcs_read_service_token()
        account = _spcs_env_account()
        host = (os.getenv("SNOWFLAKE_HOST") or "").strip()

        # Outside SPCS (or when env/token are missing), keep stock behavior.
        if not token or not account:
            return _spcs_orig_connect(*args, **kwargs)

        # Allow explicit oauth/token calls to pass through unchanged.
        if kwargs.get("authenticator") == "oauth" and kwargs.get("token"):
            return _spcs_orig_connect(*args, **kwargs)

        # Optional: caller token (only present when execute-as-caller is enabled).
        if os.getenv("SPCS_SNOWFLAKE_AUTH", "service").lower() == "caller":
            try:
                from flask import has_request_context, request

                if has_request_context():
                    caller = request.headers.get("Sf-Context-Current-User-Token")
                    if caller:
                        token = f"{token}.{caller}"
            except Exception:
                pass

        if host.startswith("https://"):
            host = host.removeprefix("https://")
        if host.startswith("http://"):
            host = host.removeprefix("http://")
        host = host.split("/", 1)[0].split(":", 1)[0]
        if not host:
            host = f"{account}.snowflakecomputing.com"

        # Remove unsupported/unsafe auth fields from the upstream call.
        for key in ("user", "username", "password", "account", "host", "authenticator", "token"):
            kwargs.pop(key, None)

        kwargs.update({"host": host, "account": account, "authenticator": "oauth", "token": token})
        return _spcs_orig_connect(*args, **kwargs)

    _spcs_sf_connector.connect = _spcs_connect  # type: ignore[assignment]
    if _spcs_orig_Connect is not None:
        _spcs_sf_connector.Connect = _spcs_connect  # type: ignore[assignment]
except Exception:
    logger.exception("Failed to patch snowflake.connector.connect for SPCS token auth")

SECRET_KEY = os.environ["SUPERSET_SECRET_KEY"]
FERNET_KEY = os.environ.get("SUPERSET_FERNET_KEY")

SESSION_COOKIE_SECURE = True
SESSION_COOKIE_HTTPONLY = True

# Optional Redis integration for cache + rate limiting (recommended outside dev).
_redis_host = os.getenv("SUPERSET_REDIS_HOST")
_redis_port = os.getenv("SUPERSET_REDIS_PORT", "6379")
if _redis_host:
    _redis_uri = f"redis://{_redis_host}:{_redis_port}/0"
    RATELIMIT_STORAGE_URI = _redis_uri
    CACHE_CONFIG = {
        "CACHE_TYPE": "RedisCache",
        "CACHE_REDIS_URL": _redis_uri,
    }
