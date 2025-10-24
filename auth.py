"""
JWT authentication utilities.
"""
from datetime import datetime, timedelta, timezone
from typing import Optional
from jose import JWTError, jwt
from uuid import UUID
import os
import logging

from redis_store import RedisOperationError, RedisUnavailableError, redis_store

logger = logging.getLogger(__name__)

# Secret key for JWT - in production, use environment variable
SECRET_KEY = os.getenv("JWT_SECRET_KEY", "your-secret-key-change-in-production")
ALGORITHM = "HS256"
ACCESS_TOKEN_EXPIRE_MINUTES = 60


def create_access_token(client_uuid: UUID, client_name: str, expires_delta: Optional[timedelta] = None) -> str:
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
        "sub": str(client_uuid),
        "name": client_name,
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


def verify_token(token: str) -> Optional[dict]:
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
        return None
