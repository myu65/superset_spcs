import os

from flask_appbuilder.security.manager import AUTH_REMOTE_USER

from sf_security import SnowflakeSyncedSecurityManager
from snowflake_remote_user_middleware import SnowflakeRemoteUserMiddleware


AUTH_TYPE = AUTH_REMOTE_USER
AUTH_REMOTE_USER_ENV_VAR = "REMOTE_USER"
AUTH_USER_REGISTRATION = True
AUTH_USER_REGISTRATION_ROLE = os.getenv("SUPERSET_DEFAULT_ROLE", "Gamma")
CUSTOM_SECURITY_MANAGER = SnowflakeSyncedSecurityManager
ADDITIONAL_MIDDLEWARE = [SnowflakeRemoteUserMiddleware]

SQLALCHEMY_DATABASE_URI = os.environ["SUPERSET_DB_URI"]
SQLALCHEMY_TRACK_MODIFICATIONS = False

FAB_ADD_SECURITY_API = True

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
