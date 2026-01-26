import importlib
import os

import pytest


MODULE_PATH = "superset.config.snowflake_remote_user_middleware"


def build_middleware(**env_vars):
    for key, value in env_vars.items():
        os.environ[key] = value
    module = importlib.reload(importlib.import_module(MODULE_PATH))
    return module.SnowflakeRemoteUserMiddleware(lambda environ, start: environ)


def run_middleware(middleware, environ):
    captured = {}

    def start_response(status, headers):
        captured["status"] = status
        captured["headers"] = headers

    body = middleware(environ, start_response)
    return captured, body


def test_static_mapping_sets_remote_user(monkeypatch):
    middleware = build_middleware(SF_SUPERSET_USER_MAP='{"TARO":"taro@example.com"}')
    environ = {"HTTP_SF_CONTEXT_CURRENT_USER": "TARO"}
    captured, _ = run_middleware(middleware, environ)
    assert environ["REMOTE_USER"] == "taro@example.com"
    assert captured.get("status") is None  # downstream app handles response


def test_default_policy_lowercases(monkeypatch):
    middleware = build_middleware(SF_SUPERSET_USER_MAP="{}", SF_USER_UNMAPPED_POLICY="create")
    environ = {"HTTP_SF_CONTEXT_CURRENT_USER": "ALICE"}
    run_middleware(middleware, environ)
    assert environ["REMOTE_USER"] == "alice"


def test_policy_deny_blocks_request(monkeypatch):
    middleware = build_middleware(SF_SUPERSET_USER_MAP="{}", SF_USER_UNMAPPED_POLICY="deny")
    environ = {"HTTP_SF_CONTEXT_CURRENT_USER": "UNKNOWN"}
    captured, body = run_middleware(middleware, environ)
    assert captured["status"].startswith("403")
    assert b"access denied" in body[0].lower()
    assert "REMOTE_USER" not in environ


def test_fallback_user_used_when_header_missing(monkeypatch):
    middleware = build_middleware(SF_FAKE_REMOTE_USER="localuser")
    environ = {}
    run_middleware(middleware, environ)
    assert environ["REMOTE_USER"] == "localuser"
