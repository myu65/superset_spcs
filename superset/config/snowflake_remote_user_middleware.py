import json
import os
from typing import Any, Callable


class SnowflakeRemoteUserMiddleware:
    """Copy Sf-Context-Current-User into REMOTE_USER with optional mapping."""

    def __init__(self, app: Callable[[dict[str, Any], Callable], Any]):
        self.app = app
        self.map = json.loads(os.getenv("SF_SUPERSET_USER_MAP", "{}"))
        self.default_policy = os.getenv("SF_USER_UNMAPPED_POLICY", "create")
        self.fallback_user = os.getenv("SF_FAKE_REMOTE_USER")

    def __call__(self, environ: dict[str, Any], start_response: Callable):
        sf_user = environ.get("HTTP_SF_CONTEXT_CURRENT_USER") or self.fallback_user
        if sf_user:
            mapped = self._resolve_user(sf_user)
            if mapped is None:
                start_response("403 Forbidden", [("Content-Type", "text/plain")])
                return [b"Superset access denied: user mapping missing"]
            environ["REMOTE_USER"] = mapped
        return self.app(environ, start_response)

    def _resolve_user(self, sf_user: str) -> str | None:
        key = sf_user.upper()
        if key in self.map:
            return self.map[key]
        policy = self.default_policy.lower()
        if policy == "deny":
            return None
        normalize = os.getenv("SF_USERNAME_NORMALIZER", "lower")
        if normalize == "identity":
            return sf_user
        return sf_user.lower()
