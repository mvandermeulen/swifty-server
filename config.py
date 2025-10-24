"""Application configuration utilities."""
from __future__ import annotations

import json
import logging
import os
import secrets
from dataclasses import dataclass, field
from functools import lru_cache
from pathlib import Path

logger = logging.getLogger(__name__)


def _parse_bool(value: str | None, default: bool) -> bool:
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}


def _parse_int(value: str | None, default: int) -> int:
    if value is None:
        return default
    try:
        return int(value)
    except ValueError:
        logger.warning("Invalid integer for configuration value '%s'; using default %s", value, default)
        return default


def _parse_list(value: str | None, default: list[str]) -> list[str]:
    if not value:
        return list(default)
    items = [item.strip() for item in value.split(",") if item.strip()]
    return items if items else list(default)


@dataclass
class Settings:
    """Runtime settings loaded from environment variables."""

    jwt_secret_key: str
    jwt_algorithm: str = "HS256"
    access_token_expire_minutes: int = 15
    refresh_token_expire_minutes: int = 60 * 24
    token_rotation_enabled: bool = True
    audit_log_name: str = "auth.audit"
    default_client_roles: list[str] = field(default_factory=lambda: ["user"])
    admin_client_ids: list[str] = field(default_factory=list)
    redis_token_prefix: str = "token"


def _load_secret_from_file(path: str | None) -> str | None:
    if not path:
        return None
    secret_path = Path(path)
    if not secret_path.exists():
        logger.warning("JWT secret file '%s' does not exist", secret_path)
        return None
    try:
        return secret_path.read_text(encoding="utf-8").strip()
    except OSError as exc:
        logger.error("Failed to read JWT secret file '%s': %s", secret_path, exc)
        return None


def load_settings() -> Settings:
    """Load settings from the environment and optional secret files."""

    secret_key = os.getenv("JWT_SECRET_KEY") or _load_secret_from_file(os.getenv("JWT_SECRET_FILE"))
    if not secret_key:
        secret_key = secrets.token_urlsafe(64)
        logger.warning("JWT_SECRET_KEY not provided; generated ephemeral key for runtime use.")

    jwt_algorithm = os.getenv("JWT_ALGORITHM", "HS256")
    access_expire_minutes = _parse_int(os.getenv("ACCESS_TOKEN_EXPIRE_MINUTES"), 15)
    refresh_expire_minutes = _parse_int(os.getenv("REFRESH_TOKEN_EXPIRE_MINUTES"), 60 * 24)
    token_rotation_enabled = _parse_bool(os.getenv("TOKEN_ROTATION_ENABLED"), True)
    audit_log_name = os.getenv("AUTH_AUDIT_LOGGER", "auth.audit")
    default_roles = _parse_list(os.getenv("DEFAULT_CLIENT_ROLES"), ["user"])

    admin_ids_env = os.getenv("ADMIN_CLIENT_IDS")
    admin_ids: list[str]
    if admin_ids_env and admin_ids_env.strip().startswith("["):
        try:
            parsed = json.loads(admin_ids_env)
            if isinstance(parsed, list):
                admin_ids = [str(item) for item in parsed]
            else:
                admin_ids = _parse_list(admin_ids_env, [])
        except json.JSONDecodeError:
            admin_ids = _parse_list(admin_ids_env, [])
    else:
        admin_ids = _parse_list(admin_ids_env, [])

    redis_token_prefix = os.getenv("REDIS_TOKEN_PREFIX", "token")

    return Settings(
        jwt_secret_key=secret_key,
        jwt_algorithm=jwt_algorithm,
        access_token_expire_minutes=access_expire_minutes,
        refresh_token_expire_minutes=refresh_expire_minutes,
        token_rotation_enabled=token_rotation_enabled,
        audit_log_name=audit_log_name,
        default_client_roles=default_roles,
        admin_client_ids=admin_ids,
        redis_token_prefix=redis_token_prefix,
    )


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    """Return cached application settings."""

    return load_settings()
