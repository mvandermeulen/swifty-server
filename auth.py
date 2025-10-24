"""JWT authentication utilities with refresh token support."""
from __future__ import annotations

import logging
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from uuid import UUID, uuid4

from jose import JWTError, jwt

from config import get_settings
from redis_store import RedisOperationError, RedisUnavailableError, redis_store

logger = logging.getLogger(__name__)
settings = get_settings()
audit_logger = logging.getLogger(settings.audit_log_name)


@dataclass
class TokenDetails:
    """Container describing an issued token."""

    token: str
    expires_at: datetime
    jti: str
    token_type: str

    @property
    def expires_in(self) -> int:
        """Return remaining lifetime in seconds."""

        return max(int((self.expires_at - datetime.now(timezone.utc)).total_seconds()), 0)


def _normalise_roles(client_uuid: UUID, roles: Sequence[str] | None) -> list[str]:
    """Resolve token roles by combining defaults and administrator overrides."""

    resolved_roles = list(roles) if roles else list(settings.default_client_roles)
    client_id = str(client_uuid)
    if client_id in settings.admin_client_ids and "admin" not in resolved_roles:
        resolved_roles.append("admin")
    return resolved_roles


def _issue_token(
    client_uuid: UUID,
    client_name: str,
    roles: Sequence[str],
    token_type: str,
    lifetime: timedelta,
) -> TokenDetails:
    """Create, sign, and persist a token."""

    now = datetime.now(timezone.utc)
    expires_at = now + lifetime
    jti = str(uuid4())
    payload = {
        "sub": str(client_uuid),
        "name": client_name,
        "roles": list(roles),
        "type": token_type,
        "jti": jti,
        "iat": now,
        "nbf": now,
        "exp": expires_at,
    }

    encoded_jwt = jwt.encode(payload, settings.jwt_secret_key, algorithm=settings.jwt_algorithm)
    ttl_seconds = max(int(lifetime.total_seconds()), 1)

    try:
        stored = redis_store.store_token(
            jti=jti,
            token=encoded_jwt,
            client_uuid=str(client_uuid),
            token_type=token_type,
            ttl_seconds=ttl_seconds,
            expires_at=expires_at,
        )
        if not stored:
            logger.warning(
                "Unable to persist %s token metadata for client %s (%s)",
                token_type,
                client_name,
                client_uuid,
            )
    except RedisUnavailableError as exc:
        logger.warning("Redis unavailable while storing %s token metadata: %s", token_type, exc)
    except RedisOperationError as exc:
        logger.error("Redis error storing %s token metadata: %s", token_type, exc)

    audit_logger.info(
        "Issued %s token",
        token_type,
        extra={
            "client_uuid": str(client_uuid),
            "client_name": client_name,
            "jti": jti,
            "expires_at": expires_at.isoformat(),
        },
    )

    return TokenDetails(token=encoded_jwt, expires_at=expires_at, jti=jti, token_type=token_type)


def create_access_token(
    client_uuid: UUID,
    client_name: str,
    roles: Sequence[str] | None = None,
    expires_delta: timedelta | None = None,
) -> TokenDetails:
    """Create a signed access token for a client."""

    resolved_roles = _normalise_roles(client_uuid, roles)
    lifetime = expires_delta or timedelta(minutes=settings.access_token_expire_minutes)
    return _issue_token(client_uuid, client_name, resolved_roles, "access", lifetime)


def create_refresh_token(
    client_uuid: UUID,
    client_name: str,
    roles: Sequence[str] | None = None,
    expires_delta: timedelta | None = None,
) -> TokenDetails:
    """Create a signed refresh token for a client."""

    resolved_roles = _normalise_roles(client_uuid, roles)
    lifetime = expires_delta or timedelta(minutes=settings.refresh_token_expire_minutes)
    return _issue_token(client_uuid, client_name, resolved_roles, "refresh", lifetime)


def issue_token_pair(
    client_uuid: UUID,
    client_name: str,
    roles: Sequence[str] | None = None,
) -> dict:
    """Issue a fresh access and refresh token pair."""

    resolved_roles = _normalise_roles(client_uuid, roles)
    access_token = create_access_token(client_uuid, client_name, roles=resolved_roles)
    refresh_token = create_refresh_token(client_uuid, client_name, roles=resolved_roles)

    try:
        redis_store.register_client(str(client_uuid), client_name, access_token.token)
    except (RedisUnavailableError, RedisOperationError) as exc:
        logger.warning(
            "Unable to record client %s (%s) in Redis during issuance: %s",
            client_name,
            client_uuid,
            exc,
        )

    audit_logger.info(
        "Issued token pair",
        extra={
            "client_uuid": str(client_uuid),
            "client_name": client_name,
            "access_jti": access_token.jti,
            "refresh_jti": refresh_token.jti,
        },
    )

    return {
        "access_token": access_token.token,
        "access_token_expires_at": access_token.expires_at.isoformat(),
        "access_token_expires_in": access_token.expires_in,
        "refresh_token": refresh_token.token,
        "refresh_token_expires_at": refresh_token.expires_at.isoformat(),
        "refresh_token_expires_in": refresh_token.expires_in,
        "token_type": "bearer",
        "roles": resolved_roles,
    }


def verify_token(token: str, expected_type: str = "access") -> dict | None:
    """Verify and decode a JWT token."""

    try:
        payload = jwt.decode(token, settings.jwt_secret_key, algorithms=[settings.jwt_algorithm])
    except JWTError as exc:
        audit_logger.warning("Token decode failed: %s", exc)
        return None

    if expected_type and payload.get("type") != expected_type:
        audit_logger.warning(
            "Token type mismatch: expected %s but received %s",
            expected_type,
            payload.get("type"),
        )
        return None

    jti = payload.get("jti")
    if not jti:
        audit_logger.warning("Token missing jti claim")
        return None

    try:
        active = redis_store.is_token_active(jti, token)
    except RedisUnavailableError as exc:
        active = bool(exc.fallback_result)
        logger.warning("Redis unavailable during token verification: %s", exc)
    except RedisOperationError as exc:
        logger.error("Redis error verifying token %s: %s", jti, exc)
        return None

    if not active:
        audit_logger.warning("Inactive or revoked token used", extra={"jti": jti})
        return None

    return payload


def rotate_refresh_token(refresh_token: str) -> dict | None:
    """Rotate a refresh token and issue a new token pair."""

    payload = verify_token(refresh_token, expected_type="refresh")
    if not payload:
        return None

    try:
        client_uuid = UUID(payload["sub"])
    except (KeyError, ValueError):
        audit_logger.warning("Refresh token missing valid subject")
        return None

    client_name = payload.get("name", "")
    roles_claim = payload.get("roles") or settings.default_client_roles
    roles = list(roles_claim)

    previous_jti = payload.get("jti")
    if settings.token_rotation_enabled and previous_jti:
        try:
            redis_store.revoke_token(previous_jti)
            audit_logger.info(
                "Revoked previous refresh token",
                extra={"jti": previous_jti, "client_uuid": str(client_uuid)},
            )
        except RedisUnavailableError as exc:
            logger.warning("Redis unavailable while revoking refresh token %s: %s", previous_jti, exc)
        except RedisOperationError as exc:
            logger.error("Redis error revoking refresh token %s: %s", previous_jti, exc)

    resolved_roles = _normalise_roles(client_uuid, roles)

    access_token = create_access_token(client_uuid, client_name, roles=resolved_roles)
    new_refresh_token = create_refresh_token(client_uuid, client_name, roles=resolved_roles)

    try:
        redis_store.register_client(str(client_uuid), client_name, access_token.token)
    except (RedisUnavailableError, RedisOperationError) as exc:
        logger.warning(
            "Unable to record client %s (%s) during refresh rotation: %s",
            client_name,
            client_uuid,
            exc,
        )

    audit_logger.info(
        "Rotated refresh token",
        extra={"client_uuid": str(client_uuid), "new_jti": new_refresh_token.jti},
    )

    return {
        "access_token": access_token.token,
        "access_token_expires_at": access_token.expires_at.isoformat(),
        "access_token_expires_in": access_token.expires_in,
        "refresh_token": new_refresh_token.token,
        "refresh_token_expires_at": new_refresh_token.expires_at.isoformat(),
        "refresh_token_expires_in": new_refresh_token.expires_in,
        "token_type": "bearer",
        "roles": resolved_roles,
    }


def revoke_token(token: str | None = None, jti: str | None = None) -> bool:
    """Revoke a token using its encoded value or identifier."""

    resolved_jti = jti
    if not resolved_jti and token:
        try:
            claims = jwt.get_unverified_claims(token)
        except JWTError:
            logger.warning("Unable to inspect token for revocation")
            return False
        resolved_jti = claims.get("jti")

    if not resolved_jti:
        return False

    try:
        revoked = redis_store.revoke_token(resolved_jti)
    except RedisUnavailableError as exc:
        revoked = bool(exc.fallback_result)
        logger.warning("Redis unavailable while revoking token %s: %s", resolved_jti, exc)
    except RedisOperationError as exc:
        logger.error("Redis error revoking token %s: %s", resolved_jti, exc)
        return False

    if revoked:
        audit_logger.info("Token revoked", extra={"jti": resolved_jti})
    return revoked


def revoke_client_tokens(client_uuid: UUID, token_type: str | None = None) -> int:
    """Revoke all tokens for a client, optionally filtering by token type."""

    try:
        revoked_count = redis_store.revoke_client_tokens(str(client_uuid), token_type)
    except RedisUnavailableError as exc:
        revoked_count = int(exc.fallback_result or 0)
        logger.warning("Redis unavailable while revoking tokens for %s: %s", client_uuid, exc)
    except RedisOperationError as exc:
        logger.error("Redis error revoking tokens for %s: %s", client_uuid, exc)
        return 0

    if revoked_count:
        audit_logger.info(
            "Revoked %s token(s) for client",
            revoked_count,
            extra={"client_uuid": str(client_uuid), "token_type": token_type},
        )
    return revoked_count
