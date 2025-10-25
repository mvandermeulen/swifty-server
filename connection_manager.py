"""WebSocket connection manager for handling client connections and message routing."""
from __future__ import annotations

import asyncio
import logging
import time
from dataclasses import dataclass, field
from uuid import UUID

from fastapi import WebSocket

from observability import (
    CONNECTION_EVENTS,
    CONNECTION_GAUGE,
    ERROR_COUNTER,
    MESSAGE_COUNTER,
    get_tracer,
)
from redis_store import RedisUnavailableError, redis_store

logger = logging.getLogger(__name__)
tracer = get_tracer("swifty.connection_manager")


class ConnectionLimitError(Exception):
    """Raised when the connection manager cannot accept additional clients."""


@dataclass
class PendingDelivery:
    """State for messages awaiting acknowledgment."""

    message: dict
    recipient_uuid: str
    sender_uuid: str
    attempts: int = 0
    max_attempts: int = 5
    next_attempt: float = field(default_factory=lambda: time.time())
    ack_required: bool = False
    stored_offline: bool = False
@dataclass
class DeliveryResult:
    """Result of a delivery attempt."""

    status: str
    detail: str


class ConnectionManager:
    """Manages WebSocket connections for all clients."""

    def __init__(self, max_connections: int | None = None):
        # Dictionary mapping client UUID to WebSocket connection (in-memory)
        self.active_connections: dict[str, WebSocket] = {}
        # Fallback dictionary for client names (when Redis is unavailable)
        self.client_names: dict[str, str] = {}
        # Pending deliveries awaiting acknowledgment
        self.pending_deliveries: dict[str, PendingDelivery] = {}
        self._pending_lock = asyncio.Lock()
        self._delivery_worker_task: asyncio.Task | None = None
        self._worker_started = False
        self._ack_timeout_seconds = 10
        self._base_backoff_seconds = 2
        self._max_backoff_seconds = 60
        self._max_connections = max_connections if max_connections and max_connections > 0 else None
        self._connection_lock = asyncio.Lock()

    def set_max_connections(self, limit: int | None) -> None:
        if limit and limit > 0:
            self._max_connections = limit
        else:
            self._max_connections = None

    async def connect(self, websocket: WebSocket, client_uuid: UUID, client_name: str):
        """
        Accept a new WebSocket connection and register the client.
        
        Args:
            websocket: WebSocket connection
            client_uuid: Client's UUID
            client_name: Client's name
        """
        client_uuid_str = str(client_uuid)
        with tracer.start_as_current_span(
            "websocket.connect",
            attributes={
                "client.uuid": client_uuid_str,
                "client.name": client_name,
            },
        ):
            async with self._connection_lock:
                if (
                    self._max_connections is not None
                    and len(self.active_connections) >= self._max_connections
                ):
                    raise ConnectionLimitError(
                        f"maximum connections reached ({self._max_connections})"
                    )

                await websocket.accept()
                self.active_connections[client_uuid_str] = websocket
                self.client_names[client_uuid_str] = client_name

            # Store in Redis
            try:
                redis_store.set_client_connected(client_uuid_str, client_name)
            except RedisUnavailableError as exc:
                logger.warning(
                    "Redis unavailable while marking client connected",  # noqa: TRY400
                    extra={
                        "client_uuid": client_uuid_str,
                        "error": str(exc),
                    },
                )

            redis_store.set_client_connected(client_uuid_str, client_name)

            # Ensure background worker is running
            await self._ensure_worker()

            CONNECTION_EVENTS.labels(event="connected").inc()
            CONNECTION_GAUGE.set(len(self.active_connections))
            logger.info(
                "client_connected",
                extra={
                    "client_uuid": client_uuid_str,
                    "client_name": client_name,
                    "active_connections": len(self.active_connections),
                },
            )

            # Drain any queued messages for the client
            await self._deliver_offline_messages(client_uuid_str)
    
    def disconnect(self, client_uuid: UUID):
        """
        Remove a client from the registry.
        
        Args:
            client_uuid: Client's UUID
        """
        client_uuid_str = str(client_uuid)
        if client_uuid_str in self.active_connections:
            client_name = self.client_names.get(client_uuid_str, "Unknown")
            del self.active_connections[client_uuid_str]
            if client_uuid_str in self.client_names:
                del self.client_names[client_uuid_str]
            
            # Clean up Redis
            with tracer.start_as_current_span(
                "websocket.disconnect",
                attributes={"client.uuid": client_uuid_str},
            ):
                try:
                    redis_store.cleanup_client(client_uuid_str)
                except RedisUnavailableError as exc:
                    logger.warning(
                        "Redis unavailable while cleaning up client",
                        extra={
                            "client_uuid": client_uuid_str,
                            "error": str(exc),
                        },
                    )

                CONNECTION_EVENTS.labels(event="disconnected").inc()
                CONNECTION_GAUGE.set(len(self.active_connections))
                logger.info(
                    "client_disconnected",
                    extra={
                        "client_uuid": client_uuid_str,
                        "client_name": client_name,
                        "active_connections": len(self.active_connections),
                    },
                )
    
    async def send_message(self, message: dict, recipient_uuid: UUID) -> DeliveryResult:
        """
        Send a message to a specific client by UUID.

        Args:
            message: Message dictionary
            recipient_uuid: Recipient's UUID

        Returns:
            DeliveryResult describing the delivery outcome
        """
        recipient_uuid_str = str(recipient_uuid)
        sender_uuid = str(message.get("from") or message.get("from_", ""))
        msgid = str(message.get("msgid"))
        ack_required = bool(message.get("acknowledge", False))

        with tracer.start_as_current_span(
            "websocket.send_message",
            attributes={
                "message.id": msgid,
                "recipient.uuid": recipient_uuid_str,
                "sender.uuid": sender_uuid,
                "ack_required": ack_required,
            },
        ):
            await self._ensure_worker()

            redis_store.record_delivery_attempt(
                msgid,
                {
                    "sender": sender_uuid,
                    "recipient": recipient_uuid_str,
                    "ack_required": ack_required,
                    "payload": message,
                },
            )

            success = await self._attempt_direct_delivery(message, recipient_uuid_str)
            now = time.time()

            if success:
                MESSAGE_COUNTER.labels(direction="outbound", channel="direct").inc()
                logger.info(
                    "direct_message_delivered",
                    extra={
                        "recipient_uuid": recipient_uuid_str,
                        "message_id": msgid,
                        "ack_required": ack_required,
                    },
                )
                status = "pending_ack" if ack_required else "delivered"
                redis_store.update_delivery_status(msgid, status, attempts=1, last_attempt=now)

                if ack_required:
                    await self._register_pending(msgid, message, recipient_uuid_str, sender_uuid)
                    return DeliveryResult("pending_ack", "Awaiting recipient acknowledgment")

                return DeliveryResult("delivered", "Message delivered to recipient")

            logger.warning(
                "recipient_offline_queueing",
                extra={
                    "recipient_uuid": recipient_uuid_str,
                    "message_id": msgid,
                },
            )
            queued = redis_store.enqueue_offline_message(recipient_uuid_str, message)
            MESSAGE_COUNTER.labels(direction="outbound", channel="offline_queue").inc()
            redis_store.update_delivery_status(msgid, "queued_offline", attempts=0, last_attempt=now)
            if ack_required:
                await self._register_pending(
                    msgid,
                    message,
                    recipient_uuid_str,
                    sender_uuid,
                    stored_offline=queued,
                )
            return DeliveryResult("queued_offline", "Recipient offline - message queued for delivery")
    
    async def broadcast(self, message: dict, exclude: UUID | None = None):
        """
        Broadcast a message to all connected clients.

        Args:
            message: Message dictionary
            exclude: UUID to exclude from broadcast when provided
        """
        exclude_str = str(exclude) if exclude else None
        sent_count = 0

        for client_uuid_str in list(self.active_connections.keys()):
            if exclude_str and client_uuid_str == exclude_str:
                continue

            websocket = self.active_connections[client_uuid_str]
            try:
                await websocket.send_json(message)
                sent_count += 1
            except Exception as e:
                ERROR_COUNTER.labels(type="broadcast_failure").inc()
                logger.error(
                    "broadcast_error",
                    extra={"client_uuid": client_uuid_str, "error": str(e)},
                )
                self.disconnect(UUID(client_uuid_str))

        if sent_count:
            MESSAGE_COUNTER.labels(direction="outbound", channel="topic").inc(sent_count)
        logger.info(
            "topic_broadcast_completed",
            extra={"topic_id": message.get("topic_id"), "recipient_count": sent_count},
        )
        return sent_count

    async def handle_acknowledgment(self, msgid: str, status: str, recipient_uuid: str) -> None:
        """Handle an acknowledgment from a recipient."""

        MESSAGE_COUNTER.labels(direction="inbound", channel="ack").inc()
        status = status.lower()

        with tracer.start_as_current_span(
            "websocket.acknowledgment",
            attributes={
                "message.id": msgid,
                "recipient.uuid": recipient_uuid,
                "status": status,
            },
        ):
            async with self._pending_lock:
                pending = self.pending_deliveries.get(msgid)

                if not pending:
                    redis_store.update_delivery_status(msgid, status)
                    return

                if pending.recipient_uuid != recipient_uuid:
                    ERROR_COUNTER.labels(type="ack_mismatch").inc()
                    logger.warning(
                        "acknowledgment_recipient_mismatch",
                        extra={
                            "message_id": msgid,
                            "expected_recipient": pending.recipient_uuid,
                            "actual_recipient": recipient_uuid,
                        },
                    )
                    return

                success_statuses = {"delivered", "received", "ack"}

                if status in success_statuses:
                    redis_store.update_delivery_status(
                        msgid,
                        "delivered",
                        ack_timestamp=time.time(),
                    )
                    await self._notify_sender_update(pending.sender_uuid, msgid, "delivered")
                    del self.pending_deliveries[msgid]
                    return

                pending.attempts += 1
                if pending.attempts >= pending.max_attempts:
                    redis_store.update_delivery_status(
                        msgid,
                        "failed",
                        ack_timestamp=time.time(),
                    )
                    await self._notify_sender_update(pending.sender_uuid, msgid, "failed")
                    del self.pending_deliveries[msgid]
                else:
                    pending.next_attempt = time.time() + self._compute_backoff(pending.attempts)
                    redis_store.update_delivery_status(
                        msgid,
                        "retrying",
                        attempts=pending.attempts,
                        next_attempt=pending.next_attempt,
                    )

    def is_client_connected(self, client_uuid: UUID) -> bool:
        """
        Check if a client is currently connected.
        
        Args:
            client_uuid: Client's UUID
            
        Returns:
            True if client is connected, False otherwise
        """
        return str(client_uuid) in self.active_connections
    
    def get_connected_clients(self) -> dict[str, str]:
        """
        Get a dictionary of all connected clients.
        
        Returns:
            Dictionary mapping UUIDs to client names
        """
        # Try Redis first, fallback to in-memory
        try:
            redis_clients = redis_store.get_connected_clients()
            if redis_clients:
                return redis_clients
        except RedisUnavailableError as exc:
            if isinstance(exc.fallback_result, dict):
                return exc.fallback_result
            logger.warning("Redis unavailable when listing clients: %s", exc)
        return self.client_names.copy()
    
    async def broadcast_to_topic(
        self, topic_id: str, message: dict, exclude: UUID | None = None
    ):
        """
        Broadcast a message to all subscribers of a topic.
        
        Args:
            topic_id: Topic identifier
            message: Message dictionary
            exclude: UUID to exclude from broadcast when provided
        """
        # Get topic subscribers from Redis
        try:
            subscribers = redis_store.get_topic_subscribers(topic_id)
        except RedisUnavailableError as exc:
            subscribers = exc.fallback_result or set()
            logger.warning(
                "topic_subscriber_lookup_fallback",
                extra={"topic_id": topic_id, "error": str(exc)},
            )
        exclude_str = str(exclude) if exclude else None

        sent_count = 0
        for client_uuid_str in subscribers:
            if exclude_str and client_uuid_str == exclude_str:
                continue

            if client_uuid_str in self.active_connections:
                websocket = self.active_connections[client_uuid_str]
                try:
                    await websocket.send_json(message)
                    sent_count += 1
                except Exception as e:
                    ERROR_COUNTER.labels(type="topic_broadcast_failure").inc()
                    logger.error(
                        "topic_broadcast_error",
                        extra={"client_uuid": client_uuid_str, "error": str(e)},
                    )
                    self.disconnect(UUID(client_uuid_str))

        if sent_count:
            MESSAGE_COUNTER.labels(direction="outbound", channel="topic").inc(sent_count)
        logger.info(
            "topic_broadcast_summary",
            extra={"topic_id": topic_id, "recipient_count": sent_count},
        )
        return sent_count

    async def _ensure_worker(self) -> None:
        if self._worker_started:
            return
        self._worker_started = True
        loop = asyncio.get_running_loop()
        self._delivery_worker_task = loop.create_task(self._delivery_worker())

    async def _delivery_worker(self) -> None:
        while True:
            await asyncio.sleep(1)
            now = time.time()

            async with self._pending_lock:
                pending_items = list(self.pending_deliveries.items())

            for msgid, pending in pending_items:
                if pending.next_attempt > now:
                    continue

                if pending.attempts >= pending.max_attempts:
                    redis_store.update_delivery_status(
                        msgid,
                        "failed",
                        attempts=pending.attempts,
                        last_attempt=now,
                    )
                    await self._notify_sender_update(pending.sender_uuid, msgid, "failed")
                    async with self._pending_lock:
                        self.pending_deliveries.pop(msgid, None)
                    continue

                delivered = await self._attempt_direct_delivery(pending.message, pending.recipient_uuid)
                pending.attempts += 1

                if delivered:
                    pending.next_attempt = time.time() + self._ack_timeout_seconds
                    MESSAGE_COUNTER.labels(direction="outbound", channel="direct").inc()
                    redis_store.update_delivery_status(
                        msgid,
                        "pending_ack",
                        attempts=pending.attempts,
                        last_attempt=time.time(),
                    )
                    pending.stored_offline = False
                else:
                    if not pending.stored_offline:
                        redis_store.enqueue_offline_message(pending.recipient_uuid, pending.message)
                        MESSAGE_COUNTER.labels(direction="outbound", channel="offline_queue").inc()
                        pending.stored_offline = True
                    redis_store.update_delivery_status(
                        msgid,
                        "queued_offline",
                        attempts=pending.attempts,
                        last_attempt=time.time(),
                    )
                    pending.next_attempt = time.time() + self._compute_backoff(pending.attempts)

    def get_worker_health(self) -> dict[str, int | bool]:
        """Return background worker health information."""

        task = self._delivery_worker_task
        running = False
        if task is not None:
            running = not task.done()
        return {
            "started": self._worker_started,
            "running": running,
            "pending_deliveries": len(self.pending_deliveries),
        }

    async def _attempt_direct_delivery(self, message: dict, recipient_uuid: str) -> bool:
        with tracer.start_as_current_span(
            "websocket.direct_delivery",
            attributes={
                "recipient.uuid": recipient_uuid,
                "message.id": str(message.get("msgid")),
            },
        ):
            if recipient_uuid in self.active_connections:
                websocket = self.active_connections[recipient_uuid]
                try:
                    await websocket.send_json(message)
                    return True
                except Exception as e:
                    ERROR_COUNTER.labels(type="direct_delivery_failure").inc()
                    logger.error(
                        "direct_delivery_error",
                        extra={"recipient_uuid": recipient_uuid, "error": str(e)},
                    )
                    self.disconnect(UUID(recipient_uuid))
            return False

    async def _register_pending(
        self,
        msgid: str,
        message: dict,
        recipient_uuid: str,
        sender_uuid: str,
        stored_offline: bool = False,
    ) -> None:
        async with self._pending_lock:
            pending = self.pending_deliveries.get(msgid)
            if not pending:
                pending = PendingDelivery(
                    message=message,
                    recipient_uuid=recipient_uuid,
                    sender_uuid=sender_uuid,
                    attempts=1 if not stored_offline else 0,
                    max_attempts=5,
                    next_attempt=time.time() + self._ack_timeout_seconds,
                    ack_required=True,
                    stored_offline=stored_offline,
                )
                self.pending_deliveries[msgid] = pending
            else:
                pending.next_attempt = time.time() + self._ack_timeout_seconds
                if stored_offline:
                    pending.stored_offline = True
                else:
                    pending.attempts = max(pending.attempts, 0) + 1
                    pending.stored_offline = False

    async def _notify_sender_update(self, sender_uuid: str, msgid: str, status: str) -> None:
        if not sender_uuid:
            return

        message = {
            "type": "delivery_update",
            "msgid": msgid,
            "status": status,
            "timestamp": time.time(),
        }

        if sender_uuid in self.active_connections:
            websocket = self.active_connections[sender_uuid]
            try:
                await websocket.send_json(message)
                MESSAGE_COUNTER.labels(direction="outbound", channel="delivery_update").inc()
            except Exception as e:
                ERROR_COUNTER.labels(type="sender_notify_failure").inc()
                logger.error(
                    "sender_notification_error",
                    extra={"sender_uuid": sender_uuid, "error": str(e)},
                )

    async def _deliver_offline_messages(self, recipient_uuid: str) -> None:
        queued_messages = redis_store.pop_offline_messages(recipient_uuid)
        if not queued_messages:
            return

        with tracer.start_as_current_span(
            "websocket.deliver_offline",
            attributes={
                "recipient.uuid": recipient_uuid,
                "queued_count": len(queued_messages),
            },
        ):
            logger.info(
                "deliver_offline_messages",
                extra={
                    "recipient_uuid": recipient_uuid,
                    "message_count": len(queued_messages),
                },
            )
            for message in queued_messages:
                msgid = str(message.get("msgid"))
                sender_uuid = str(message.get("from") or message.get("from_", ""))
                delivered = await self._attempt_direct_delivery(message, recipient_uuid)
                now = time.time()
                if delivered:
                    MESSAGE_COUNTER.labels(direction="outbound", channel="direct").inc()
                    redis_store.update_delivery_status(
                        msgid,
                        "pending_ack" if message.get("acknowledge") else "delivered",
                        last_attempt=now,
                    )
                    if message.get("acknowledge"):
                        await self._register_pending(msgid, message, recipient_uuid, sender_uuid)
                    else:
                        await self._notify_sender_update(sender_uuid, msgid, "delivered")
                else:
                    redis_store.enqueue_offline_message(recipient_uuid, message)
                    MESSAGE_COUNTER.labels(direction="outbound", channel="offline_queue").inc()

    def _compute_backoff(self, attempts: int) -> float:
        delay = self._base_backoff_seconds * (2 ** max(attempts - 1, 0))
        return min(delay, self._max_backoff_seconds)
