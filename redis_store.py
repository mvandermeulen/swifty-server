"""Redis repository for WebSocket connections, client data, and topics. With fallback storage and health monitoring."""

from __future__ import annotations

import json
import logging
import os
import time
from dataclasses import dataclass
from typing import Any, Callable

import redis

from collections import defaultdict
from uuid import UUID

import redis

from config import get_settings

logger = logging.getLogger(__name__)

# Redis configuration
REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))
REDIS_DB = int(os.getenv("REDIS_DB", "0"))
REDIS_PASSWORD = os.getenv("REDIS_PASSWORD")


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
        topics = list(self.client_topics.get(client_uuid, set()))
        for topic_id in topics:
            self.unsubscribe_from_topic(topic_id, client_uuid)
        self.set_client_disconnected(client_uuid)
        self.client_metadata.pop(client_uuid, None)
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
        }

_settings = get_settings()
TOKEN_META_PREFIX = f"{_settings.redis_token_prefix}_meta"
CLIENT_TOKEN_SET_PREFIX = "client_tokens"


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

    def _backfill_from_fallback(self) -> None:
        if not self._client or not self._backfill_needed:
            return

        snapshot = self.fallback.snapshot()
        pipe = self._client.pipeline()

        # Clients
        for client_uuid, client_data in snapshot["clients"].items():
            pipe.hset(f"client:{client_uuid}", mapping=client_data)

        # Tokens
        for token, client_uuid in snapshot["tokens"].items():
            pipe.setex(f"token:{token}", 3600, client_uuid)

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

        pipe.execute()
        self._backfill_needed = False
        logger.info("Redis backfilled from fallback store")

    # Repository methods -----------------------------------------------
    def register_client(self, client_uuid: str, client_name: str, token: str) -> bool:
        self.fallback.register_client(client_uuid, client_name, token)

        try:
            def op() -> bool:
                pipe = self._client.pipeline()
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
                data = self._client.hgetall(f"client:{client_uuid}")
                return data or None

            result = self._execute(op, description="get_client_by_uuid")
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
                return self._client.get(f"token:{token}")

            result = self._execute(op, description="get_client_by_token")
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
                self._client.set(f"client:{client_uuid}:metadata", json.dumps(metadata))
                return True

            self._execute(op, description="update_client_metadata")
            return True
        except RedisUnavailableError as exc:
            self._backfill_needed = True
            raise RedisUnavailableError(exc.args[0], fallback_result=True) from exc
    
    def get_client_metadata(self, client_uuid: str) -> Optional[Dict]:
        """
        Get client metadata.
        
        Args:
            client_uuid: Client's UUID
            
        Returns:
            Metadata dictionary or None
        """
        if not self.client:
            metadata = self.fallback_clients.get(client_uuid, {}).get("metadata")
            return metadata

        try:
            def op() -> dict[str, Any] | None:
                data = self._client.get(f"client:{client_uuid}:metadata")
                return json.loads(data) if data else None

            return self._execute(op, description="get_client_metadata")
        except RedisUnavailableError as exc:
            raise RedisUnavailableError(exc.args[0], fallback_result=fallback_value) from exc

    def set_client_connected(self, client_uuid: str, client_name: str) -> bool:
        self.fallback.set_client_connected(client_uuid, client_name)
        try:
            def op() -> bool:
                self._client.hset("connected_clients", client_uuid, client_name)
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
                self._client.hdel("connected_clients", client_uuid)
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
                return self._client.hgetall("connected_clients")

            return self._execute(op, description="get_connected_clients")
        except RedisUnavailableError as exc:
            raise RedisUnavailableError(exc.args[0], fallback_result=fallback_value) from exc

    def is_client_connected(self, client_uuid: str) -> bool:
        fallback_value = self.fallback.is_client_connected(client_uuid)
        try:
            def op() -> bool:
                return bool(self._client.hexists("connected_clients", client_uuid))

            return bool(self._execute(op, description="is_client_connected"))
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
                pipe = self._client.pipeline()
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
                topic_key = f"topic:{topic_id}"
                data = self._client.hgetall(topic_key)
                if data and "metadata" in data:
                    data["metadata"] = json.loads(data["metadata"])
                return data or None

            return self._execute(op, description="get_topic")
        except RedisUnavailableError as exc:
            raise RedisUnavailableError(exc.args[0], fallback_result=fallback_value) from exc

    def list_topics(self) -> list[str]:
        fallback_value = self.fallback.list_topics()
        try:
            def op() -> list[str]:
                topics = self._client.smembers("topics")
                return list(topics) if topics else []

            return self._execute(op, description="list_topics")
        except RedisUnavailableError as exc:
            raise RedisUnavailableError(exc.args[0], fallback_result=fallback_value) from exc

    def delete_topic(self, topic_id: str) -> bool:
        deleted = self.fallback.delete_topic(topic_id)
        if not deleted:
            return False
        try:
            def op() -> bool:
                pipe = self._client.pipeline()
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
                pipe = self._client.pipeline()
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
                pipe = self._client.pipeline()
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
                subscribers = self._client.smembers(f"topic:{topic_id}:subscribers")
                return set(subscribers) if subscribers else set()

            return self._execute(op, description="get_topic_subscribers")
        except RedisUnavailableError as exc:
            raise RedisUnavailableError(exc.args[0], fallback_result=fallback_value) from exc

    def get_client_topics(self, client_uuid: str) -> set[str]:
        fallback_value = self.fallback.get_client_topics(client_uuid)
        try:
            def op() -> set[str]:
                topics = self._client.smembers(f"client:{client_uuid}:topics")
                return set(topics) if topics else set()

            return self._execute(op, description="get_client_topics")
        except RedisUnavailableError as exc:
            raise RedisUnavailableError(exc.args[0], fallback_result=fallback_value) from exc

    def cleanup_client(self, client_uuid: str) -> bool:
        topics_for_cleanup = self.fallback.get_client_topics(client_uuid)
        self.fallback.cleanup_client(client_uuid)
        try:
            def op() -> bool:
                pipe = self._client.pipeline()
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

