"""Redis repository for WebSocket connections, client data, and topics. With fallback storage and health monitoring."""

from __future__ import annotations

import json
import logging
import os
import time
from collections.abc import Callable
from copy import deepcopy
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, cast

import redis

from config import get_settings

logger = logging.getLogger(__name__)

# Redis configuration
REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))
REDIS_DB = int(os.getenv("REDIS_DB", "0"))
REDIS_PASSWORD = os.getenv("REDIS_PASSWORD")


def _json_default(value: Any) -> Any:
    if isinstance(value, datetime):
        return value.isoformat()
    raise TypeError(f"Object of type {type(value).__name__} is not JSON serializable")


class RedisStoreError(Exception):
    """Base class for Redis repository errors."""


class RedisOperationError(RedisStoreError):
    """Raised when a Redis command fails despite Redis being available."""


class RedisUnavailableError(RedisStoreError):
    """Raised when Redis is unavailable and the fallback store was used."""

    def __init__(self, message: str, fallback_result: Any = None):
        super().__init__(message)
        self.fallback_result = fallback_result


@dataclass
class TopicData:
    """Container for topic metadata used by the fallback store."""

    id: str
    creator: str
    metadata: dict[str, Any]



class InMemoryFallbackStore:
    """Simple in-memory data store that mirrors Redis operations."""

    def __init__(self) -> None:
        self.clients: dict[str, dict[str, Any]] = {}
        self.tokens: dict[str, str] = {}
        self.client_metadata: dict[str, dict[str, Any]] = {}
        self.connected_clients: dict[str, str] = {}
        self.topics: dict[str, TopicData] = {}
        self.topic_subscribers: dict[str, set[str]] = {}
        self.client_topics: dict[str, set[str]] = {}
        self.token_metadata: dict[str, dict[str, Any]] = {}
        self.client_token_index: dict[str, set[str]] = {}
        self.revoked_tokens: set[str] = set()
        self.offline_messages: dict[str, list[dict[str, Any]]] = {}
        self.delivery_status: dict[str, dict[str, Any]] = {}

    # Client management -------------------------------------------------
    def register_client(self, client_uuid: str, client_name: str, token: str) -> bool:
        self.clients[client_uuid] = {
            "uuid": client_uuid,
            "name": client_name,
            "token": token,
        }
        if token:
            self.tokens[token] = client_uuid
        return True

    def get_client_by_uuid(self, client_uuid: str) -> dict[str, Any] | None:
        data = self.clients.get(client_uuid)
        return dict(data) if data else None

    def get_client_by_token(self, token: str) -> str | None:
        return self.tokens.get(token)

    def update_client_metadata(self, client_uuid: str, metadata: dict[str, Any]) -> bool:
        self.client_metadata[client_uuid] = dict(metadata)
        return True

    def get_client_metadata(self, client_uuid: str) -> dict[str, Any] | None:
        metadata = self.client_metadata.get(client_uuid)
        return dict(metadata) if metadata is not None else None

    # Token metadata ----------------------------------------------------
    def store_token(
        self,
        jti: str,
        token: str,
        client_uuid: str,
        token_type: str,
        expires_at: datetime,
    ) -> bool:
        self.token_metadata[jti] = {
            "token": token,
            "client_uuid": client_uuid,
            "type": token_type,
            "expires_at": expires_at,
            "revoked": False,
        }
        self.client_token_index.setdefault(client_uuid, set()).add(jti)
        self.revoked_tokens.discard(jti)
        if token:
            self.tokens[token] = client_uuid
        return True

    def is_token_active(self, jti: str, token: str) -> bool:
        data = self.token_metadata.get(jti)
        if not data or data.get("token") != token:
            return False
        if data.get("revoked"):
            return False
        expires_at = data.get("expires_at")
        if isinstance(expires_at, datetime) and expires_at <= datetime.now(timezone.utc):
            return False
        return True

    def revoke_token(self, jti: str) -> bool:
        data = self.token_metadata.get(jti)
        if not data:
            return False
        data["revoked"] = True
        self.revoked_tokens.add(jti)
        client_uuid = data.get("client_uuid")
        if client_uuid in self.client_token_index:
            self.client_token_index[client_uuid].discard(jti)
            if not self.client_token_index[client_uuid]:
                self.client_token_index.pop(client_uuid)
        return True

    def revoke_client_tokens(self, client_uuid: str, token_type: str | None = None) -> int:
        jtis = list(self.client_token_index.get(client_uuid, set()))
        count = 0
        for jti in jtis:
            data = self.token_metadata.get(jti)
            if token_type and data and data.get("type") != token_type:
                continue
            if self.revoke_token(jti):
                count += 1
        return count

    # Connection state --------------------------------------------------
    def set_client_connected(self, client_uuid: str, client_name: str) -> bool:
        self.connected_clients[client_uuid] = client_name
        return True

    def set_client_disconnected(self, client_uuid: str) -> bool:
        self.connected_clients.pop(client_uuid, None)
        return True

    def get_connected_clients(self) -> dict[str, str]:
        return dict(self.connected_clients)

    def is_client_connected(self, client_uuid: str) -> bool:
        return client_uuid in self.connected_clients

    # Delivery tracking -------------------------------------------------
    def record_delivery_attempt(self, msgid: str, metadata: dict[str, Any]) -> bool:
        entry = self.delivery_status.setdefault(msgid, {})
        entry.setdefault("created_at", time.time())
        entry.setdefault("attempts", 0)
        entry["attempts"] = int(entry.get("attempts", 0)) + 1
        entry["status"] = "attempted"
        entry["last_attempt"] = time.time()
        payload = metadata.get("payload")
        entry.update({k: v for k, v in metadata.items() if k != "payload"})
        if isinstance(payload, dict):
            entry["payload"] = deepcopy(payload)
        elif payload is not None:
            entry["payload"] = payload
        return True

    def update_delivery_status(self, msgid: str, status: str, **extra: Any) -> bool:
        entry = self.delivery_status.setdefault(msgid, {})
        entry.setdefault("created_at", time.time())
        entry.setdefault("attempts", 0)
        entry["status"] = status
        if "last_attempt" not in extra:
            entry.setdefault("last_attempt", time.time())
        entry.update(extra)
        return True

    def enqueue_offline_message(self, recipient_uuid: str, message: dict[str, Any]) -> bool:
        queue = self.offline_messages.setdefault(recipient_uuid, [])
        queue.append(deepcopy(message))
        return True

    def pop_offline_messages(self, recipient_uuid: str) -> list[dict[str, Any]]:
        messages = self.offline_messages.pop(recipient_uuid, [])
        return [deepcopy(message) for message in messages]

    def get_delivery_status(self, msgid: str) -> dict[str, Any] | None:
        entry = self.delivery_status.get(msgid)
        return deepcopy(entry) if entry is not None else None

    def list_undelivered_messages(self, client_uuid: str) -> list[dict[str, Any]]:
        messages = self.offline_messages.get(client_uuid, [])
        return [deepcopy(message) for message in messages]

    def get_delivery_metrics(self) -> dict[str, Any]:
        status_counts: dict[str, int] = {}
        for data in self.delivery_status.values():
            status = str(data.get("status", "unknown"))
            status_counts[status] = status_counts.get(status, 0) + 1

        offline_sizes = {
            client_uuid: len(messages)
            for client_uuid, messages in self.offline_messages.items()
        }

        return {
            "tracked_messages": len(self.delivery_status),
            "status_counts": status_counts,
            "offline_queue_sizes": offline_sizes,
            "total_offline": sum(offline_sizes.values()),
        }

    # Topic management --------------------------------------------------
    def create_topic(
        self, topic_id: str, creator_uuid: str, metadata: dict[str, Any] | None = None
    ) -> bool:
        if topic_id in self.topics:
            return False
        self.topics[topic_id] = TopicData(
            id=topic_id,
            creator=creator_uuid,
            metadata=dict(metadata or {}),
        )
        self.topic_subscribers.setdefault(topic_id, set())
        return True

    def get_topic(self, topic_id: str) -> dict[str, Any] | None:
        topic = self.topics.get(topic_id)
        if not topic:
            return None
        return {
            "id": topic.id,
            "creator": topic.creator,
            "metadata": dict(topic.metadata),
        }

    def list_topics(self) -> list[str]:
        return list(self.topics.keys())

    def delete_topic(self, topic_id: str) -> bool:
        if topic_id not in self.topics:
            return False
        del self.topics[topic_id]
        self.topic_subscribers.pop(topic_id, None)
        for topics in self.client_topics.values():
            topics.discard(topic_id)
        return True

    def subscribe_to_topic(self, topic_id: str, client_uuid: str) -> bool:
        if topic_id not in self.topics:
            return False
        subscribers = self.topic_subscribers.setdefault(topic_id, set())
        subscribers.add(client_uuid)
        self.client_topics.setdefault(client_uuid, set()).add(topic_id)
        return True

    def unsubscribe_from_topic(self, topic_id: str, client_uuid: str) -> bool:
        subscribers = self.topic_subscribers.get(topic_id)
        if not subscribers:
            return False
        subscribers.discard(client_uuid)
        if client_uuid in self.client_topics:
            self.client_topics[client_uuid].discard(topic_id)
        return True

    def get_topic_subscribers(self, topic_id: str) -> set[str]:
        return set(self.topic_subscribers.get(topic_id, set()))

    def get_client_topics(self, client_uuid: str) -> set[str]:
        return set(self.client_topics.get(client_uuid, set()))

    # Cleanup -----------------------------------------------------------
    def cleanup_client(self, client_uuid: str) -> bool:
        self.revoke_client_tokens(client_uuid)
        topics = list(self.client_topics.get(client_uuid, set()))
        for topic_id in topics:
            self.unsubscribe_from_topic(topic_id, client_uuid)
        self.set_client_disconnected(client_uuid)
        self.client_metadata.pop(client_uuid, None)
        self.offline_messages.pop(client_uuid, None)
        return True

    # Snapshot ----------------------------------------------------------
    def snapshot(self) -> dict[str, Any]:
        return {
            "clients": {uuid: dict(data) for uuid, data in self.clients.items()},
            "tokens": dict(self.tokens),
            "client_metadata": {uuid: dict(meta) for uuid, meta in self.client_metadata.items()},
            "connected_clients": dict(self.connected_clients),
            "topics": {
                topic_id: {
                    "id": topic.id,
                    "creator": topic.creator,
                    "metadata": dict(topic.metadata),
                }
                for topic_id, topic in self.topics.items()
            },
            "topic_subscribers": {
                topic_id: list(subscribers)
                for topic_id, subscribers in self.topic_subscribers.items()
            },
            "client_topics": {
                client_uuid: list(topics)
                for client_uuid, topics in self.client_topics.items()
            },
            "token_metadata": {
                jti: {
                    "token": data.get("token"),
                    "client_uuid": data.get("client_uuid"),
                    "type": data.get("type"),
                    "expires_at": (
                        expires_at.isoformat()
                        if isinstance((expires_at := data.get("expires_at")), datetime)
                        else None
                    ),
                    "revoked": data.get("revoked", False),
                }
                for jti, data in self.token_metadata.items()
            },
            "client_token_index": {
                client_uuid: list(tokens)
                for client_uuid, tokens in self.client_token_index.items()
            },
            "revoked_tokens": list(self.revoked_tokens),
            "offline_messages": {
                client_uuid: [deepcopy(message) for message in messages]
                for client_uuid, messages in self.offline_messages.items()
            },
            "delivery_status": {
                msgid: deepcopy(data) for msgid, data in self.delivery_status.items()
            },
        }
_settings = get_settings()
TOKEN_KEY_PREFIX = f"{_settings.redis_token_prefix}:"
TOKEN_META_PREFIX = f"{_settings.redis_token_prefix}_meta"
CLIENT_TOKEN_SET_PREFIX = f"{_settings.redis_token_prefix}_by_client"
REVOKED_SET_KEY = f"{_settings.redis_token_prefix}_revoked"
DELIVERY_STATUS_PREFIX = "delivery:status"
DELIVERY_STATUS_INDEX_KEY = "delivery:ids"
OFFLINE_QUEUE_PREFIX = "offline:queue"
OFFLINE_QUEUE_INDEX_KEY = "offline:recipients"


class RedisStore:
    """Redis repository with retry logic, circuit breaking, and fallback support."""

    def __init__(self) -> None:
        self._client: redis.Redis | None = None
        self._max_retries = int(os.getenv("REDIS_MAX_RETRIES", "3"))
        self._retry_backoff = float(os.getenv("REDIS_RETRY_BACKOFF", "0.2"))
        self._health_check_interval = float(os.getenv("REDIS_HEALTH_INTERVAL", "5"))
        self._last_health_check = 0.0
        self._circuit_open = False
        self._backfill_needed = False
        self.fallback = InMemoryFallbackStore()

        self._connect(initial=True)

    @property
    def client(self) -> redis.Redis | None:
        """Compatibility accessor for the underlying Redis client."""

        return self._client

    # Connection management --------------------------------------------
    def _build_client(self) -> redis.Redis:
        return redis.Redis(
            host=REDIS_HOST,
            port=REDIS_PORT,
            db=REDIS_DB,
            password=REDIS_PASSWORD,
            decode_responses=True,
        )

    def _connect(self, *, initial: bool = False) -> None:
        try:
            client = self._build_client()
            client.ping()
        except redis.exceptions.RedisError as exc:
            if initial:
                logger.warning(
                    "Redis connection failed on startup: %s. Entering fallback mode.",
                    exc,
                )
            else:
                logger.debug("Redis connection attempt failed: %s", exc)
            self._client = None
            self._circuit_open = True
            self._last_health_check = time.monotonic()
            return

        self._client = client
        self._circuit_open = False
        logger.info("Redis connected: %s:%s", REDIS_HOST, REDIS_PORT)
        if self._backfill_needed:
            self._backfill_from_fallback()

    def _ensure_connection(self) -> bool:
        if self._client and not self._circuit_open:
            return True

        now = time.monotonic()
        if now - self._last_health_check < self._health_check_interval:
            return False

        self._last_health_check = now
        self._connect()
        return bool(self._client)

    def check_health(self) -> dict[str, Any]:
        """Return health information and attempt to close the circuit when possible."""
        available = self._ensure_connection()
        status = "available" if available else "degraded"
        return {
            "status": status,
            "redis": bool(self._client),
            "fallback_in_use": not available,
            "backfill_pending": self._backfill_needed,
        }

    def _execute(self, operation: Callable[[], Any], *, description: str) -> Any:
        if not self._ensure_connection():
            raise RedisUnavailableError(
                f"Redis unavailable during {description}; using fallback store instead."
            )

        last_error: BaseException | None = None
        for attempt in range(1, self._max_retries + 1):
            try:
                return operation()
            except (redis.exceptions.ConnectionError, redis.exceptions.TimeoutError) as exc:
                last_error = exc
                logger.warning(
                    "Redis %s failed on attempt %s/%s: %s", description, attempt, self._max_retries, exc
                )
                time.sleep(self._retry_backoff * attempt)
                self._client = None
                self._circuit_open = True
                self._last_health_check = time.monotonic()
                self._ensure_connection()
            except redis.exceptions.RedisError as exc:
                logger.error("Redis %s failed with error: %s", description, exc)
                raise RedisOperationError(f"Redis {description} failed: {exc}") from exc

        raise RedisUnavailableError(
            f"Redis unavailable during {description}; using fallback store instead."
        ) from last_error

    def _require_client(self) -> redis.Redis:
        client = self._client
        if client is None:
            raise RedisUnavailableError("Redis client unavailable")
        return client

    def _backfill_from_fallback(self) -> None:
        client = self._client
        if client is None or not self._backfill_needed:
            return

        snapshot = self.fallback.snapshot()
        pipe = client.pipeline()

        # Clients
        for client_uuid, client_data in snapshot["clients"].items():
            pipe.hset(f"client:{client_uuid}", mapping=client_data)

        # Tokens
        for token, client_uuid in snapshot["tokens"].items():
            pipe.setex(f"token:{token}", 3600, client_uuid)

        now = datetime.now(timezone.utc)
        for jti, metadata in snapshot.get("token_metadata", {}).items():
            token_value = metadata.get("token") or ""
            expires_at_str = metadata.get("expires_at")
            ttl = 3600
            if expires_at_str:
                try:
                    expires_at = datetime.fromisoformat(expires_at_str)
                    ttl = max(int((expires_at - now).total_seconds()), 1)
                except ValueError:
                    ttl = 3600
            token_key = f"{TOKEN_KEY_PREFIX}{jti}"
            meta_key = f"{TOKEN_META_PREFIX}:{jti}"
            client_uuid = metadata.get("client_uuid") or ""
            token_type = metadata.get("type") or ""
            pipe.setex(token_key, ttl, token_value)
            pipe.hset(meta_key, mapping={"client_uuid": client_uuid, "type": token_type})
            pipe.expire(meta_key, ttl)
            if client_uuid:
                client_key = f"{CLIENT_TOKEN_SET_PREFIX}:{client_uuid}"
                pipe.sadd(client_key, jti)
                pipe.expire(client_key, max(ttl, 3600))

        for client_uuid, jtis in snapshot.get("client_token_index", {}).items():
            if jtis:
                client_key = f"{CLIENT_TOKEN_SET_PREFIX}:{client_uuid}"
                pipe.delete(client_key)
                pipe.sadd(client_key, *jtis)

        revoked_tokens = snapshot.get("revoked_tokens") or []
        if revoked_tokens:
            pipe.delete(REVOKED_SET_KEY)
            pipe.sadd(REVOKED_SET_KEY, *revoked_tokens)

        # Metadata
        for client_uuid, metadata in snapshot["client_metadata"].items():
            pipe.set(f"client:{client_uuid}:metadata", json.dumps(metadata))

        # Connected clients
        if snapshot["connected_clients"]:
            pipe.delete("connected_clients")
            pipe.hset("connected_clients", mapping=snapshot["connected_clients"])

        # Topics
        if snapshot["topics"]:
            pipe.delete("topics")
            pipe.sadd("topics", *snapshot["topics"].keys())

        for topic_id, topic_data in snapshot["topics"].items():
            pipe.hset(f"topic:{topic_id}", mapping={
                "id": topic_data["id"],
                "creator": topic_data["creator"],
                "metadata": json.dumps(topic_data.get("metadata", {})),
            })

        for topic_id, subscribers in snapshot["topic_subscribers"].items():
            if subscribers:
                pipe.delete(f"topic:{topic_id}:subscribers")
                pipe.sadd(f"topic:{topic_id}:subscribers", *subscribers)

        for client_uuid, topics in snapshot["client_topics"].items():
            if topics:
                pipe.delete(f"client:{client_uuid}:topics")
                pipe.sadd(f"client:{client_uuid}:topics", *topics)

        for recipient_uuid, messages in snapshot.get("offline_messages", {}).items():
            queue_key = f"{OFFLINE_QUEUE_PREFIX}:{recipient_uuid}"
            pipe.delete(queue_key)
            if messages:
                pipe.rpush(queue_key, *[json.dumps(message) for message in messages])
                pipe.sadd(OFFLINE_QUEUE_INDEX_KEY, recipient_uuid)
            else:
                pipe.srem(OFFLINE_QUEUE_INDEX_KEY, recipient_uuid)

        delivery_snapshot = snapshot.get("delivery_status", {})
        if delivery_snapshot:
            pipe.delete(DELIVERY_STATUS_INDEX_KEY)
            pipe.sadd(DELIVERY_STATUS_INDEX_KEY, *delivery_snapshot.keys())
        for msgid, data in delivery_snapshot.items():
            pipe.set(f"{DELIVERY_STATUS_PREFIX}:{msgid}", json.dumps(data))

        pipe.execute()
        self._backfill_needed = False
        logger.info("Redis backfilled from fallback store")

    # Repository methods -----------------------------------------------
    def register_client(self, client_uuid: str, client_name: str, token: str) -> bool:
        self.fallback.register_client(client_uuid, client_name, token)

        try:
            def op() -> bool:
                client = self._require_client()
                pipe = client.pipeline()
                pipe.hset(
                    f"client:{client_uuid}",
                    mapping={"uuid": client_uuid, "name": client_name, "token": token},
                )
                pipe.setex(f"token:{token}", 3600, client_uuid)
                pipe.execute()
                return True

            self._execute(op, description="register_client")
            return True
        except RedisUnavailableError as exc:
            self._backfill_needed = True
            raise RedisUnavailableError(exc.args[0], fallback_result=True) from exc

    def get_client_by_uuid(self, client_uuid: str) -> dict[str, Any] | None:
        fallback_value = self.fallback.get_client_by_uuid(client_uuid)
        try:
            def op() -> dict[str, Any] | None:
                client = self._require_client()
                data = cast(dict[str, Any], client.hgetall(f"client:{client_uuid}"))
                return data or None

            result = cast(
                dict[str, Any] | None,
                self._execute(op, description="get_client_by_uuid"),
            )
            if result:
                token = result.get("token", "")
                self.fallback.register_client(client_uuid, result.get("name", ""), token)
            return result
        except RedisUnavailableError as exc:
            raise RedisUnavailableError(exc.args[0], fallback_result=fallback_value) from exc

    def get_client_by_token(self, token: str) -> str | None:
        fallback_value = self.fallback.get_client_by_token(token)
        try:
            def op() -> str | None:
                client = self._require_client()
                return cast(str | None, client.get(f"token:{token}"))

            result = cast(
                str | None,
                self._execute(op, description="get_client_by_token"),
            )
            if result:
                client = self.get_client_by_uuid(result)
                if client:
                    self.fallback.register_client(result, client.get("name", ""), token)
            return result
        except RedisUnavailableError as exc:
            raise RedisUnavailableError(exc.args[0], fallback_result=fallback_value) from exc

    def update_client_metadata(self, client_uuid: str, metadata: dict[str, Any]) -> bool:
        self.fallback.update_client_metadata(client_uuid, metadata)
        try:
            def op() -> bool:
                client = self._require_client()
                client.set(f"client:{client_uuid}:metadata", json.dumps(metadata))
                return True

            self._execute(op, description="update_client_metadata")
            return True
        except RedisUnavailableError as exc:
            self._backfill_needed = True
            raise RedisUnavailableError(exc.args[0], fallback_result=True) from exc
    
    def get_client_metadata(self, client_uuid: str) -> dict[str, Any] | None:
        fallback_value = self.fallback.get_client_metadata(client_uuid)

        try:
            def op() -> dict[str, Any] | None:
                client = self._require_client()
                data = cast(str | None, client.get(f"client:{client_uuid}:metadata"))
                return json.loads(data) if data else None

            result = cast(
                dict[str, Any] | None,
                self._execute(op, description="get_client_metadata"),
            )
            if result is not None:
                self.fallback.update_client_metadata(client_uuid, result)
            return result
        except RedisUnavailableError as exc:
            raise RedisUnavailableError(exc.args[0], fallback_result=fallback_value) from exc


    # Token metadata ----------------------------------------------------
    def store_token(
        self,
        *,
        jti: str,
        token: str,
        client_uuid: str,
        token_type: str,
        ttl_seconds: int,
        expires_at: datetime,
    ) -> bool:
        self.fallback.store_token(jti, token, client_uuid, token_type, expires_at)

        try:
            def op() -> bool:
                client = self._require_client()
                pipe = client.pipeline()
                token_key = f"{TOKEN_KEY_PREFIX}{jti}"
                meta_key = f"{TOKEN_META_PREFIX}:{jti}"
                client_key = f"{CLIENT_TOKEN_SET_PREFIX}:{client_uuid}"
                pipe.setex(token_key, ttl_seconds, token)
                pipe.hset(meta_key, mapping={"client_uuid": client_uuid, "type": token_type})
                pipe.expire(meta_key, ttl_seconds)
                pipe.sadd(client_key, jti)
                pipe.expire(client_key, max(ttl_seconds, 3600))
                pipe.srem(REVOKED_SET_KEY, jti)
                pipe.execute()
                return True

            self._execute(op, description="store_token")
            return True
        except RedisUnavailableError as exc:
            self._backfill_needed = True
            raise RedisUnavailableError(exc.args[0], fallback_result=True) from exc

    def is_token_active(
        self,
        jti: str,
        token: str,
    ) -> bool:
        fallback_value = self.fallback.is_token_active(jti, token)
        try:
            def op() -> bool:
                client = self._require_client()
                if client.sismember(REVOKED_SET_KEY, jti):
                    return False
                stored_token = cast(str | None, client.get(f"{TOKEN_KEY_PREFIX}{jti}"))
                return stored_token == token if stored_token else False

            return cast(bool, self._execute(op, description="is_token_active"))
        except RedisUnavailableError as exc:
            raise RedisUnavailableError(exc.args[0], fallback_result=fallback_value) from exc

    def revoke_token(self, jti: str) -> bool:
        revoked = self.fallback.revoke_token(jti)
        if not revoked:
            return False

        try:
            def op() -> bool:
                client = self._require_client()
                client_uuid = client.hget(f"{TOKEN_META_PREFIX}:{jti}", "client_uuid")
                pipe = client.pipeline()
                if client_uuid:
                    pipe.srem(f"{CLIENT_TOKEN_SET_PREFIX}:{client_uuid}", jti)
                pipe.delete(f"{TOKEN_KEY_PREFIX}{jti}")
                pipe.delete(f"{TOKEN_META_PREFIX}:{jti}")
                pipe.sadd(REVOKED_SET_KEY, jti)
                pipe.execute()
                return True

            self._execute(op, description="revoke_token")
            return True
        except RedisUnavailableError as exc:
            self._backfill_needed = True
            raise RedisUnavailableError(exc.args[0], fallback_result=revoked) from exc

    def revoke_client_tokens(
        self,
        client_uuid: str,
        token_type: str | None = None,
    ) -> int:
        revoked_count = self.fallback.revoke_client_tokens(client_uuid, token_type)

        try:
            def op() -> int:
                client_key = f"{CLIENT_TOKEN_SET_PREFIX}:{client_uuid}"
                client = self._require_client()
                jtis = cast(set[str], client.smembers(client_key) or set())
                count = 0
                for jti in list(jtis):
                    stored_type = client.hget(f"{TOKEN_META_PREFIX}:{jti}", "type")
                    if token_type and stored_type != token_type:
                        continue
                    pipe = client.pipeline()
                    pipe.srem(client_key, jti)
                    pipe.delete(f"{TOKEN_KEY_PREFIX}{jti}")
                    pipe.delete(f"{TOKEN_META_PREFIX}:{jti}")
                    pipe.sadd(REVOKED_SET_KEY, jti)
                    pipe.execute()
                    count += 1
                return count

            redis_count = cast(int, self._execute(op, description="revoke_client_tokens"))
        except RedisUnavailableError as exc:
            self._backfill_needed = True
            raise RedisUnavailableError(exc.args[0], fallback_result=revoked_count) from exc

        return max(revoked_count, redis_count)

    def set_client_connected(self, client_uuid: str, client_name: str) -> bool:
        self.fallback.set_client_connected(client_uuid, client_name)
        try:
            def op() -> bool:
                client = self._require_client()
                client.hset("connected_clients", client_uuid, client_name)
                return True

            self._execute(op, description="set_client_connected")
            return True
        except RedisUnavailableError as exc:
            self._backfill_needed = True
            raise RedisUnavailableError(exc.args[0], fallback_result=True) from exc

    def set_client_disconnected(self, client_uuid: str) -> bool:
        self.fallback.set_client_disconnected(client_uuid)
        try:
            def op() -> bool:
                client = self._require_client()
                client.hdel("connected_clients", client_uuid)
                return True

            self._execute(op, description="set_client_disconnected")
            return True
        except RedisUnavailableError as exc:
            self._backfill_needed = True
            raise RedisUnavailableError(exc.args[0], fallback_result=True) from exc

    def get_connected_clients(self) -> dict[str, str]:
        fallback_value = self.fallback.get_connected_clients()
        try:
            def op() -> dict[str, str]:
                client = self._require_client()
                return cast(dict[str, str], client.hgetall("connected_clients"))

            return cast(
                dict[str, str],
                self._execute(op, description="get_connected_clients"),
            )
        except RedisUnavailableError as exc:
            raise RedisUnavailableError(exc.args[0], fallback_result=fallback_value) from exc

    def is_client_connected(self, client_uuid: str) -> bool:
        fallback_value = self.fallback.is_client_connected(client_uuid)
        try:
            def op() -> bool:
                client = self._require_client()
                return bool(client.hexists("connected_clients", client_uuid))

            return bool(self._execute(op, description="is_client_connected"))
        except RedisUnavailableError as exc:
            raise RedisUnavailableError(exc.args[0], fallback_result=fallback_value) from exc

    def record_delivery_attempt(self, msgid: str, metadata: dict[str, Any]) -> bool:
        self.fallback.record_delivery_attempt(msgid, metadata)
        entry = self.fallback.get_delivery_status(msgid) or {}

        try:
            def op() -> bool:
                client = self._require_client()
                payload = json.dumps(entry, default=_json_default)
                pipe = client.pipeline()
                pipe.set(f"{DELIVERY_STATUS_PREFIX}:{msgid}", payload)
                pipe.sadd(DELIVERY_STATUS_INDEX_KEY, msgid)
                pipe.execute()
                return True

            self._execute(op, description="record_delivery_attempt")
            return True
        except RedisUnavailableError as exc:
            self._backfill_needed = True
            raise RedisUnavailableError(exc.args[0], fallback_result=True) from exc

    def update_delivery_status(self, msgid: str, status: str, **extra: Any) -> bool:
        self.fallback.update_delivery_status(msgid, status, **extra)
        entry = self.fallback.get_delivery_status(msgid) or {"status": status}

        try:
            def op() -> bool:
                client = self._require_client()
                payload = json.dumps(entry, default=_json_default)
                pipe = client.pipeline()
                pipe.set(f"{DELIVERY_STATUS_PREFIX}:{msgid}", payload)
                pipe.sadd(DELIVERY_STATUS_INDEX_KEY, msgid)
                pipe.execute()
                return True

            self._execute(op, description="update_delivery_status")
            return True
        except RedisUnavailableError as exc:
            self._backfill_needed = True
            raise RedisUnavailableError(exc.args[0], fallback_result=True) from exc

    def enqueue_offline_message(self, recipient_uuid: str, message: dict[str, Any]) -> bool:
        queued = self.fallback.enqueue_offline_message(recipient_uuid, message)
        try:
            def op() -> bool:
                client = self._require_client()
                payload = json.dumps(message, default=_json_default)
                pipe = client.pipeline()
                pipe.rpush(f"{OFFLINE_QUEUE_PREFIX}:{recipient_uuid}", payload)
                pipe.sadd(OFFLINE_QUEUE_INDEX_KEY, recipient_uuid)
                pipe.execute()
                return True

            self._execute(op, description="enqueue_offline_message")
            return queued
        except RedisUnavailableError as exc:
            self._backfill_needed = True
            raise RedisUnavailableError(exc.args[0], fallback_result=queued) from exc

    def pop_offline_messages(self, recipient_uuid: str) -> list[dict[str, Any]]:
        messages = self.fallback.pop_offline_messages(recipient_uuid)
        try:
            def op() -> list[dict[str, Any]]:
                client = self._require_client()
                queue_key = f"{OFFLINE_QUEUE_PREFIX}:{recipient_uuid}"
                raw_messages = cast(list[str], client.lrange(queue_key, 0, -1))
                pipe = client.pipeline()
                pipe.delete(queue_key)
                pipe.srem(OFFLINE_QUEUE_INDEX_KEY, recipient_uuid)
                pipe.execute()
                return [json.loads(item) for item in raw_messages]

            redis_messages = cast(
                list[dict[str, Any]],
                self._execute(op, description="pop_offline_messages"),
            )
            return redis_messages or messages
        except RedisUnavailableError as exc:
            self._backfill_needed = True
            raise RedisUnavailableError(exc.args[0], fallback_result=messages) from exc

    def get_delivery_status(self, msgid: str) -> dict[str, Any] | None:
        fallback_value = self.fallback.get_delivery_status(msgid)
        try:
            def op() -> dict[str, Any] | None:
                client = self._require_client()
                raw = cast(str | None, client.get(f"{DELIVERY_STATUS_PREFIX}:{msgid}"))
                return json.loads(raw) if raw else None

            result = cast(
                dict[str, Any] | None,
                self._execute(op, description="get_delivery_status"),
            )
            if result is not None:
                self.fallback.delivery_status[msgid] = deepcopy(result)
            return result
        except RedisUnavailableError as exc:
            raise RedisUnavailableError(exc.args[0], fallback_result=fallback_value) from exc

    def list_undelivered_messages(self, client_uuid: str) -> list[dict[str, Any]]:
        fallback_value = self.fallback.list_undelivered_messages(client_uuid)
        try:
            def op() -> list[dict[str, Any]]:
                client = self._require_client()
                queue_key = f"{OFFLINE_QUEUE_PREFIX}:{client_uuid}"
                raw_messages = cast(list[str], client.lrange(queue_key, 0, -1))
                return [json.loads(item) for item in raw_messages]

            return cast(
                list[dict[str, Any]],
                self._execute(op, description="list_undelivered_messages"),
            )
        except RedisUnavailableError as exc:
            raise RedisUnavailableError(exc.args[0], fallback_result=fallback_value) from exc

    def get_delivery_metrics(self) -> dict[str, Any]:
        fallback_value = self.fallback.get_delivery_metrics()
        try:
            def op() -> dict[str, Any]:
                client = self._require_client()
                status_ids = cast(set[str], client.smembers(DELIVERY_STATUS_INDEX_KEY) or set())
                status_counts: dict[str, int] = {}
                for msgid in status_ids:
                    raw = cast(str | None, client.get(f"{DELIVERY_STATUS_PREFIX}:{msgid}"))
                    if not raw:
                        continue
                    data = json.loads(raw)
                    status = str(data.get("status", "unknown"))
                    status_counts[status] = status_counts.get(status, 0) + 1

                offline_queue_sizes: dict[str, int] = {}
                recipients = cast(set[str], client.smembers(OFFLINE_QUEUE_INDEX_KEY) or set())
                for recipient in recipients:
                    length = cast(int, client.llen(f"{OFFLINE_QUEUE_PREFIX}:{recipient}"))
                    offline_queue_sizes[str(recipient)] = int(length)

                return {
                    "tracked_messages": len(status_ids),
                    "status_counts": status_counts,
                    "offline_queue_sizes": offline_queue_sizes,
                    "total_offline": sum(offline_queue_sizes.values()),
                }

            return cast(
                dict[str, Any],
                self._execute(op, description="get_delivery_metrics"),
            )
        except RedisUnavailableError as exc:
            raise RedisUnavailableError(exc.args[0], fallback_result=fallback_value) from exc

    def create_topic(
        self, topic_id: str, creator_uuid: str, metadata: dict[str, Any] | None = None
    ) -> bool:
        created = self.fallback.create_topic(topic_id, creator_uuid, metadata)
        if not created:
            return False
        try:
            def op() -> bool:
                topic_key = f"topic:{topic_id}"
                client = self._require_client()
                pipe = client.pipeline()
                pipe.hset(
                    topic_key,
                    mapping={
                        "id": topic_id,
                        "creator": creator_uuid,
                        "metadata": json.dumps(metadata or {}),
                    },
                )
                pipe.sadd("topics", topic_id)
                pipe.execute()
                return True

            self._execute(op, description="create_topic")
            return True
        except RedisUnavailableError as exc:
            self._backfill_needed = True
            raise RedisUnavailableError(exc.args[0], fallback_result=True) from exc

    def get_topic(self, topic_id: str) -> dict[str, Any] | None:
        fallback_value = self.fallback.get_topic(topic_id)
        try:
            def op() -> dict[str, Any] | None:
                client = self._require_client()
                topic_key = f"topic:{topic_id}"
                data = cast(dict[str, Any], client.hgetall(topic_key))
                if data and "metadata" in data:
                    data["metadata"] = json.loads(data["metadata"])
                return data or None

            return cast(dict[str, Any] | None, self._execute(op, description="get_topic"))
        except RedisUnavailableError as exc:
            raise RedisUnavailableError(exc.args[0], fallback_result=fallback_value) from exc

    def list_topics(self) -> list[str]:
        fallback_value = self.fallback.list_topics()
        try:
            def op() -> list[str]:
                client = self._require_client()
                topics = cast(set[str], client.smembers("topics"))
                return list(topics) if topics else []

            return cast(list[str], self._execute(op, description="list_topics"))
        except RedisUnavailableError as exc:
            raise RedisUnavailableError(exc.args[0], fallback_result=fallback_value) from exc

    def delete_topic(self, topic_id: str) -> bool:
        deleted = self.fallback.delete_topic(topic_id)
        if not deleted:
            return False
        try:
            def op() -> bool:
                client = self._require_client()
                pipe = client.pipeline()
                pipe.delete(f"topic:{topic_id}")
                pipe.delete(f"topic:{topic_id}:subscribers")
                pipe.srem("topics", topic_id)
                pipe.execute()
                return deleted

            return self._execute(op, description="delete_topic")
        except RedisUnavailableError as exc:
            self._backfill_needed = True
            raise RedisUnavailableError(exc.args[0], fallback_result=deleted) from exc

    def subscribe_to_topic(self, topic_id: str, client_uuid: str) -> bool:
        subscribed = self.fallback.subscribe_to_topic(topic_id, client_uuid)
        if not subscribed:
            return False
        try:
            def op() -> bool:
                client = self._require_client()
                pipe = client.pipeline()
                pipe.sadd(f"topic:{topic_id}:subscribers", client_uuid)
                pipe.sadd(f"client:{client_uuid}:topics", topic_id)
                pipe.execute()
                return subscribed

            return self._execute(op, description="subscribe_to_topic")
        except RedisUnavailableError as exc:
            self._backfill_needed = True
            raise RedisUnavailableError(exc.args[0], fallback_result=subscribed) from exc

    def unsubscribe_from_topic(self, topic_id: str, client_uuid: str) -> bool:
        unsubscribed = self.fallback.unsubscribe_from_topic(topic_id, client_uuid)
        try:
            def op() -> bool:
                client = self._require_client()
                pipe = client.pipeline()
                pipe.srem(f"topic:{topic_id}:subscribers", client_uuid)
                pipe.srem(f"client:{client_uuid}:topics", topic_id)
                pipe.execute()
                return unsubscribed

            return self._execute(op, description="unsubscribe_from_topic")
        except RedisUnavailableError as exc:
            self._backfill_needed = True
            raise RedisUnavailableError(exc.args[0], fallback_result=unsubscribed) from exc

    def get_topic_subscribers(self, topic_id: str) -> set[str]:
        fallback_value = self.fallback.get_topic_subscribers(topic_id)
        try:
            def op() -> set[str]:
                client = self._require_client()
                subscribers = cast(set[str], client.smembers(f"topic:{topic_id}:subscribers"))
                return set(subscribers) if subscribers else set()

            return cast(set[str], self._execute(op, description="get_topic_subscribers"))
        except RedisUnavailableError as exc:
            raise RedisUnavailableError(exc.args[0], fallback_result=fallback_value) from exc

    def get_client_topics(self, client_uuid: str) -> set[str]:
        fallback_value = self.fallback.get_client_topics(client_uuid)
        try:
            def op() -> set[str]:
                client = self._require_client()
                topics = cast(set[str], client.smembers(f"client:{client_uuid}:topics"))
                return set(topics) if topics else set()

            return cast(set[str], self._execute(op, description="get_client_topics"))
        except RedisUnavailableError as exc:
            raise RedisUnavailableError(exc.args[0], fallback_result=fallback_value) from exc

    def cleanup_client(self, client_uuid: str) -> bool:
        topics_for_cleanup = self.fallback.get_client_topics(client_uuid)
        self.fallback.cleanup_client(client_uuid)
        try:
            def op() -> bool:
                client = self._require_client()
                pipe = client.pipeline()
                for topic_id in topics_for_cleanup:
                    pipe.srem(f"topic:{topic_id}:subscribers", client_uuid)
                pipe.hdel("connected_clients", client_uuid)
                pipe.delete(f"client:{client_uuid}:topics")
                pipe.execute()
                return True

            self._execute(op, description="cleanup_client")
            return True
        except RedisUnavailableError as exc:
            self._backfill_needed = True
            raise RedisUnavailableError(exc.args[0], fallback_result=True) from exc


# Global Redis store instance
redis_store = RedisStore()

