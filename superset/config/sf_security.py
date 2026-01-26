import os
import time
from typing import Iterable

import snowflake.connector
from flask import has_request_context, request
from superset.extensions import db
from superset.security import SupersetSecurityManager

TOKEN_PATH = os.getenv("SNOWFLAKE_SERVICE_TOKEN_PATH", "/snowflake/session/token")
ROLE_PREFIX = os.getenv("SF_SYNC_ROLE_PREFIX", "BI_")
SUPERSET_ROLE_PREFIX = os.getenv("SF_SUPERSET_ROLE_PREFIX", "SF_")
SYNC_TTL_SECONDS = int(os.getenv("SF_ROLE_SYNC_TTL", "300"))


def _read_service_token() -> str:
    with open(TOKEN_PATH, "r", encoding="utf-8") as handle:
        return handle.read().strip()


def _snowflake_connect_as_caller():
    if not has_request_context():
        raise RuntimeError("Role sync must run inside request context")
    caller_token = request.headers.get("Sf-Context-Current-User-Token")
    if not caller_token:
        raise RuntimeError("Missing Sf-Context-Current-User-Token header")
    token = f"{_read_service_token()}.{caller_token}"
    return snowflake.connector.connect(
        host=os.environ["SNOWFLAKE_HOST"],
        account=os.environ["SNOWFLAKE_ACCOUNT"],
        authenticator="oauth",
        token=token,
    )


def _fetch_roles() -> set[str]:
    conn = _snowflake_connect_as_caller()
    try:
        cur = conn.cursor()
        cur.execute("SHOW GRANTS TO USER CURRENT_USER()")
        role_names: set[str] = set()
        for row in cur.fetchall():
            role_names.add(row[1])
        return role_names
    finally:
        conn.close()


class SnowflakeSyncedSecurityManager(SupersetSecurityManager):
    def auth_user_remote_user(self, username):
        user = super().auth_user_remote_user(username)
        if not user:
            return None
        cached_at = getattr(user, "last_login", None)
        now = int(time.time())
        if cached_at and (now - int(cached_at.timestamp())) < SYNC_TTL_SECONDS:
            return user
        try:
            sf_roles = _fetch_roles()
        except Exception:
            return user
        desired = {
            f"{SUPERSET_ROLE_PREFIX}{role}"
            for role in sf_roles
            if role.startswith(ROLE_PREFIX)
        }
        current = {role.name for role in user.roles if role.name.startswith(SUPERSET_ROLE_PREFIX)}
        for role_name in desired - current:
            role = self.find_role(role_name) or self.add_role(role_name)
            user.roles.append(role)
        if current - desired:
            user.roles = [role for role in user.roles if role.name not in (current - desired)]
        db.session.commit()
        return user
