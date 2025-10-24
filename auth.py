"""JWT authentication utilities with refresh token support."""
from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Optional, Sequence
from uuid import UUID, uuid4

from jose import JWTError, jwt

from redis_store import RedisOperationError, RedisUnavailableError, redis_store
from config import get_settings

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

# Secret key for JWT - in production, use environment variable
SECRET_KEY = os.getenv("JWT_SECRET_KEY", "your-secret-key-change-in-production")
ALGORITHM = "HS256"
ACCESS_TOKEN_EXPIRE_MINUTES = 60


def create_access_token(
    client_uuid: UUID, client_name: str, expires_delta: timedelta | None = None
) -> str:
    """
    Create a JWT access token for a client.
    
    Args:
        client_uuid: Client's UUID
        client_name: Client's name
        expires_delta: Optional expiration time delta
        
    Returns:
        JWT token string
    """
    to_encode = {
        return max(int((self.expires_at - datetime.now(timezone.utc)).total_seconds()), 0)


def _normalise_roles(client_uuid: UUID, roles: Optional[Sequence[str]]) -> list[str]:
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
    expires_delta: timedelta,
) -> TokenDetails:
    """Create, sign, and persist a token."""

    now = datetime.now(timezone.utc)
    expires_at = now + expires_delta
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
    
    if expires_delta:
        expire = datetime.now(timezone.utc) + expires_delta
    else:
        expire = datetime.now(timezone.utc) + timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES)
    
    to_encode.update({"exp": expire})
    encoded_jwt = jwt.encode(to_encode, SECRET_KEY, algorithm=ALGORITHM)
    
    # Store token in Redis
    try:
        redis_store.register_client(str(client_uuid), client_name, encoded_jwt)
    except RedisUnavailableError as exc:
        logger.warning("Redis unavailable during client registration: %s", exc)
    
    return encoded_jwt


def verify_token(token: str) -> dict | None:
    """
    Verify and decode a JWT token.
    
    Args:
        token: JWT token string
        
    Returns:
        Decoded token payload or None if invalid
    """
    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        
        # Verify token exists in Redis (if available)
        try:
            client_uuid = redis_store.get_client_by_token(token)
        except RedisUnavailableError as exc:
            client_uuid = exc.fallback_result
        except RedisOperationError as exc:
            logger.error("Redis error during token verification: %s", exc)
            return None
        if client_uuid and client_uuid != payload.get("sub"):
            logger.warning(f"Token UUID mismatch: {client_uuid} vs {payload.get('sub')}")
            return None
        
        return payload
    except JWTError:

    encoded_jwt = jwt.encode(payload, settings.jwt_secret_key, algorithm=settings.jwt_algorithm)
    ttl_seconds = max(int(expires_delta.total_seconds()), 1)
    if not redis_store.store_token(jti, encoded_jwt, str(client_uuid), token_type, ttl_seconds):
        logger.warning("Unable to persist token metadata for client %s (%s)", client_name, client_uuid)

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
    roles: Optional[Sequence[str]] = None,
    expires_delta: Optional[timedelta] = None,
) -> TokenDetails:
    """Create a signed access token for a client."""

    resolved_roles = _normalise_roles(client_uuid, roles)
    lifetime = expires_delta or timedelta(minutes=settings.access_token_expire_minutes)
    return _issue_token(client_uuid, client_name, resolved_roles, "access", lifetime)


def create_refresh_token(
    client_uuid: UUID,
    client_name: str,
    roles: Optional[Sequence[str]] = None,
    expires_delta: Optional[timedelta] = None,
) -> TokenDetails:
    """Create a signed refresh token for a client."""

    resolved_roles = _normalise_roles(client_uuid, roles)
    lifetime = expires_delta or timedelta(minutes=settings.refresh_token_expire_minutes)
    return _issue_token(client_uuid, client_name, resolved_roles, "refresh", lifetime)


def issue_token_pair(
    client_uuid: UUID,
    client_name: str,
    roles: Optional[Sequence[str]] = None,
) -> dict:
    """Issue a fresh access and refresh token pair."""

    resolved_roles = _normalise_roles(client_uuid, roles)
    access_token = create_access_token(client_uuid, client_name, roles=resolved_roles)
    refresh_token = create_refresh_token(client_uuid, client_name, roles=resolved_roles)

    redis_store.register_client(str(client_uuid), client_name, last_token_jti=access_token.jti)

    response = {
        "access_token": access_token.token,
        "access_token_expires_at": access_token.expires_at.isoformat(),
        "access_token_expires_in": access_token.expires_in,
        "refresh_token": refresh_token.token,
        "refresh_token_expires_at": refresh_token.expires_at.isoformat(),
        "refresh_token_expires_in": refresh_token.expires_in,
        "token_type": "bearer",
        "roles": resolved_roles,
    }

    audit_logger.info(
        "Issued token pair",
        extra={"client_uuid": str(client_uuid), "client_name": client_name, "access_jti": access_token.jti},
    )
    return response


def verify_token(token: str, expected_type: str = "access") -> Optional[dict]:
    """Verify and decode a JWT token."""

    try:
        payload = jwt.decode(token, settings.jwt_secret_key, algorithms=[settings.jwt_algorithm])
    except JWTError as exc:
        audit_logger.warning("Token decode failed: %s", exc)
        return None

    if expected_type and payload.get("type") != expected_type:
        audit_logger.warning(
            "Token type mismatch: expected %s but received %s", expected_type, payload.get("type")
        )
        return None

    jti = payload.get("jti")
    if not jti:
        audit_logger.warning("Token missing jti claim")
        return None

    if not redis_store.is_token_active(jti, token):
        audit_logger.warning("Inactive or revoked token used", extra={"jti": jti})
        return None

    return payload


def rotate_refresh_token(refresh_token: str) -> Optional[dict]:
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
        redis_store.revoke_token(previous_jti)
        audit_logger.info("Revoked previous refresh token", extra={"jti": previous_jti, "client_uuid": str(client_uuid)})

    resolved_roles = _normalise_roles(client_uuid, roles)

    access_token = create_access_token(client_uuid, client_name, roles=resolved_roles)
    new_refresh_token = create_refresh_token(client_uuid, client_name, roles=resolved_roles)
    redis_store.register_client(str(client_uuid), client_name, last_token_jti=access_token.jti)

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


def revoke_token(token: Optional[str] = None, jti: Optional[str] = None) -> bool:
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

    revoked = redis_store.revoke_token(resolved_jti)
    if revoked:
        audit_logger.info("Token revoked", extra={"jti": resolved_jti})
    return revoked


def revoke_client_tokens(client_uuid: UUID, token_type: Optional[str] = None) -> int:
    """Revoke all tokens for a client, optionally filtering by token type."""

    revoked_count = redis_store.revoke_client_tokens(str(client_uuid), token_type)
    if revoked_count:
        audit_logger.info(
            "Revoked %s token(s) for client", revoked_count, extra={"client_uuid": str(client_uuid)}
        )
    return revoked_count
