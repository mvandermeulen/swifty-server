"""
FastAPI WebSocket Server for Real-Time Communications.

This server provides:
- Client registration with name and UUID
- JWT authentication
- WebSocket connections for real-time messaging
- Message routing between clients by UUID
"""
from enum import Enum
from typing import Any, Optional

from fastapi import FastAPI, HTTPException, Query, Request, WebSocket, WebSocketDisconnect
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from starlette.middleware.base import BaseHTTPMiddleware
from uuid import UUID, uuid4
import json
import time
import logging

from models import (
    ClientRegistration,
    Message,
    MessageAcknowledgment,
    TopicCreate,
    TopicSubscribe,
    TopicMessage,
    TokenRefreshRequest,
    TokenRevokeRequest,
)
from auth import (
    issue_token_pair,
    verify_token,
    rotate_refresh_token,
    revoke_token,
    revoke_client_tokens,
)
from connection_manager import ConnectionManager
from observability import ERROR_COUNTER, MESSAGE_COUNTER, configure_observability, get_tracer
from redis_store import RedisOperationError, RedisUnavailableError, redis_store

logger = logging.getLogger(__name__)
tracer = get_tracer("swifty.main")

# Create FastAPI app
app = FastAPI(
    title="Swifty Server",
    description="FastAPI WebSocket Server for Real-Time Communications",
    version="1.0.0",
)

configure_observability(app)

# Create connection manager
manager = ConnectionManager()


class ErrorCode(str, Enum):
    """Enumerated error codes shared across HTTP and WebSocket responses."""

    INVALID_TOKEN = "INVALID_TOKEN"
    INVALID_REFRESH_TOKEN = "INVALID_REFRESH_TOKEN"
    REGISTRATION_FAILED = "REGISTRATION_FAILED"
    PERMISSION_DENIED = "PERMISSION_DENIED"
    MESSAGE_NOT_FOUND = "MESSAGE_NOT_FOUND"
    MESSAGE_ACCESS_FORBIDDEN = "MESSAGE_ACCESS_FORBIDDEN"
    UNDELIVERED_ACCESS_FORBIDDEN = "UNDELIVERED_ACCESS_FORBIDDEN"
    REDIS_UNAVAILABLE = "REDIS_UNAVAILABLE"
    REDIS_OPERATION_FAILED = "REDIS_OPERATION_FAILED"
    TOPIC_ALREADY_EXISTS = "TOPIC_ALREADY_EXISTS"
    TOPIC_NOT_FOUND = "TOPIC_NOT_FOUND"
    TOPIC_LOOKUP_FAILED = "TOPIC_LOOKUP_FAILED"
    TOPIC_CREATION_FAILED = "TOPIC_CREATION_FAILED"
    SUBSCRIPTION_FAILED = "SUBSCRIPTION_FAILED"
    UNSUBSCRIBE_FAILED = "UNSUBSCRIBE_FAILED"
    MISSING_REVOCATION_TARGET = "MISSING_REVOCATION_TARGET"
    TOKEN_NOT_FOUND = "TOKEN_NOT_FOUND"
    VALIDATION_ERROR = "VALIDATION_ERROR"
    INTERNAL_SERVER_ERROR = "INTERNAL_SERVER_ERROR"
    INVALID_ACK = "INVALID_ACK"
    ACK_SENDER_MISMATCH = "ACK_SENDER_MISMATCH"
    INVALID_TOPIC_MESSAGE = "INVALID_TOPIC_MESSAGE"
    NOT_SUBSCRIBED_TO_TOPIC = "NOT_SUBSCRIBED_TO_TOPIC"
    INVALID_MESSAGE_FORMAT = "INVALID_MESSAGE_FORMAT"
    MESSAGE_SENDER_MISMATCH = "MESSAGE_SENDER_MISMATCH"


def error_response(code: ErrorCode, message: str, details: Optional[Any] = None) -> dict[str, Any]:
    """Create a structured error payload used across the service."""

    return {
        "code": code.value,
        "message": message,
        "details": details if details is not None else {},
    }


def http_exception(status_code: int, code: ErrorCode, message: str, details: Optional[Any] = None) -> HTTPException:
    """Create a FastAPI HTTPException with a structured error payload."""

    return HTTPException(status_code=status_code, detail=error_response(code, message, details))


def websocket_error_frame(
    code: ErrorCode,
    message: str,
    recovery: str,
    details: Optional[Any] = None,
) -> dict[str, Any]:
    """Create a structured WebSocket error frame with recovery instructions."""

    return {
        "type": "error",
        "error": error_response(code, message, details),
        "recovery": recovery,
        "timestamp": time.time(),
    }


class RequestContextMiddleware(BaseHTTPMiddleware):
    """Middleware handling correlation IDs, logging, and validation errors."""

    async def dispatch(self, request: Request, call_next):  # type: ignore[override]
        correlation_id = request.headers.get("X-Correlation-ID") or str(uuid4())
        request.state.correlation_id = correlation_id
        start_time = time.time()

        try:
            response = await call_next(request)
            status_code = response.status_code
        except HTTPException as exc:
            status_code = exc.status_code
            ERROR_COUNTER.labels(type="http_exception").inc()
            logger.warning(
                "http_exception",
                extra={
                    "method": request.method,
                    "path": request.url.path,
                    "status_code": status_code,
                    "detail": exc.detail,
                    "correlation_id": correlation_id,
                },
            )
            response = JSONResponse(status_code=status_code, content={"detail": exc.detail})
            if exc.headers:
                for header, value in exc.headers.items():
                    response.headers[header] = value
        except RequestValidationError as exc:
            status_code = 422
            ERROR_COUNTER.labels(type="validation_error").inc()
            logger.warning(
                "request_validation_error",
                extra={
                    "method": request.method,
                    "path": request.url.path,
                    "errors": exc.errors(),
                    "correlation_id": correlation_id,
                },
            )
            response = JSONResponse(
                status_code=status_code,
                content={"detail": error_response(ErrorCode.VALIDATION_ERROR, "Request validation failed", exc.errors())},
            )
        except Exception as exc:  # noqa: BLE001
            status_code = 500
            ERROR_COUNTER.labels(type="http_internal_error").inc()
            logger.exception(
                "unhandled_http_error",
                extra={
                    "method": request.method,
                    "path": request.url.path,
                    "correlation_id": correlation_id,
                },
            )
            response = JSONResponse(
                status_code=status_code,
                content={"detail": error_response(ErrorCode.INTERNAL_SERVER_ERROR, "Internal server error")},
            )

        response.headers["X-Correlation-ID"] = correlation_id
        process_time_ms = (time.time() - start_time) * 1000
        logger.info(
            "http_request_completed",
            extra={
                "method": request.method,
                "path": request.url.path,
                "status_code": status_code,
                "duration_ms": round(process_time_ms, 2),
                "correlation_id": correlation_id,
            },
        )
        return response


app.add_middleware(RequestContextMiddleware)


def _require_admin(payload: dict) -> None:
    roles = payload.get("roles") or []
    if "admin" not in roles:
        raise http_exception(
            status_code=403,
            code=ErrorCode.PERMISSION_DENIED,
            message="Insufficient permissions",
        )


@app.get("/")
async def root():
    """Root endpoint."""
    return {
        "message": "Swifty Server - FastAPI WebSocket Server",
        "version": "1.0.0",
        "endpoints": {
            "register": "/register",
            "websocket": "/ws",
            "clients": "/clients",
            "topics": "/topics",
            "create_topic": "/topics/create",
            "subscribe": "/topics/subscribe",
            "unsubscribe": "/topics/unsubscribe",
            "health_live": "/health/live",
            "health_ready": "/health/ready",
            "metrics": "/metrics",
            "redis_health": "/health/redis",
            "delivery_status": "/messages/{msgid}/status",
            "undelivered": "/clients/{uuid}/undelivered",
            "delivery_metrics": "/delivery/metrics",
            "refresh": "/auth/refresh",
            "revoke_tokens": "/admin/tokens/revoke",
        }
    }


@app.get("/health/live")
async def health_live() -> dict[str, Any]:
    """Liveness probe that confirms the application is running."""

    return {
        "status": "live",
        "app": "ok",
        "timestamp": time.time(),
    }


@app.get("/health/ready")
async def health_ready() -> JSONResponse:
    """Readiness probe that reports dependency health."""

    redis_status = redis_store.check_health()
    worker_health = manager.get_worker_health()
    redis_ready = bool(redis_status.get("redis"))
    worker_ready = worker_health["running"] or worker_health["pending_deliveries"] == 0
    ready = redis_ready and worker_ready

    payload = {
        "status": "ready" if ready else "degraded",
        "app": "ok" if ready else "initializing",
        "redis": redis_status,
        "worker": worker_health,
        "timestamp": time.time(),
    }
    status_code = 200 if ready else 503
    return JSONResponse(status_code=status_code, content=payload)


@app.post("/register")
async def register_client(registration: ClientRegistration):
    """
    Register a client and receive a JWT token.
    
    Args:
        registration: Client registration data (name and UUID)
        
    Returns:
        Authentication tokens and client metadata
    """
    try:
        tokens = issue_token_pair(registration.uuid, registration.name)

        logger.info(
            "client_registered",
            extra={
                "client_uuid": str(registration.uuid),
                "client_name": registration.name,
            },
        )

        return {
            "access_token": tokens["access_token"],
            "access_token_expires_at": tokens["access_token_expires_at"],
            "access_token_expires_in": tokens["access_token_expires_in"],
            "refresh_token": tokens["refresh_token"],
            "refresh_token_expires_at": tokens["refresh_token_expires_at"],
            "refresh_token_expires_in": tokens["refresh_token_expires_in"],
            "token_type": tokens["token_type"],
            "roles": tokens["roles"],
            "uuid": str(registration.uuid),
            "name": registration.name,
            "message": "Registration successful"
        }
    except Exception as e:
        ERROR_COUNTER.labels(type="registration_error").inc()
        logger.error(
            "registration_error",
            extra={"client_uuid": str(registration.uuid), "error": str(e)},
        )
        raise http_exception(
            status_code=500,
            code=ErrorCode.REGISTRATION_FAILED,
            message="Registration failed",
            details={"error": str(e)},
        )


@app.post("/auth/refresh")
async def refresh_tokens(request: TokenRefreshRequest):
    """Exchange a refresh token for a new access/refresh pair."""

    tokens = rotate_refresh_token(request.refresh_token)
    if not tokens:
        raise http_exception(
            status_code=401,
            code=ErrorCode.INVALID_REFRESH_TOKEN,
            message="Invalid refresh token",
        )

    return tokens


@app.get("/clients")
async def get_clients():
    """
    Get list of currently connected clients.

    Returns:
        Dictionary of connected clients (UUID -> name)
    """
    clients = manager.get_connected_clients()
    return {
        "count": len(clients),
        "clients": clients
    }


@app.get("/messages/{msgid}/status")
async def get_message_status(msgid: UUID, token: str = Query(..., description="JWT authentication token")):
    """Return delivery metadata for a specific message."""
    payload = verify_token(token)
    if not payload:
        raise http_exception(401, ErrorCode.INVALID_TOKEN, "Invalid token")

    status = redis_store.get_delivery_status(str(msgid))
    if not status:
        raise http_exception(404, ErrorCode.MESSAGE_NOT_FOUND, "Message not found")

    requester_uuid = payload["sub"]
    if requester_uuid not in {status.get("sender"), status.get("recipient")}:
        raise http_exception(
            403,
            ErrorCode.MESSAGE_ACCESS_FORBIDDEN,
            "Not authorized to view this message",
        )

    return status


@app.get("/clients/{client_uuid}/undelivered")
async def get_undelivered_messages(client_uuid: UUID, token: str = Query(..., description="JWT authentication token")):
    """List undelivered messages for a client."""
    payload = verify_token(token)
    if not payload:
        raise http_exception(401, ErrorCode.INVALID_TOKEN, "Invalid token")

    if str(client_uuid) != payload["sub"]:
        raise http_exception(
            403,
            ErrorCode.UNDELIVERED_ACCESS_FORBIDDEN,
            "Cannot query undelivered messages for other clients",
        )

    messages = redis_store.list_undelivered_messages(str(client_uuid))
    return {
        "count": len(messages),
        "messages": messages
    }


@app.get("/delivery/metrics")
async def get_delivery_metrics(token: str = Query(..., description="JWT authentication token")):
    """Expose aggregate delivery metrics."""
    payload = verify_token(token)
    if not payload:
        raise http_exception(401, ErrorCode.INVALID_TOKEN, "Invalid token")

    metrics = redis_store.get_delivery_metrics()
    return metrics


@app.post("/topics/create")
async def create_topic(topic: TopicCreate, token: str = Query(..., description="JWT authentication token")):
    """
    Create a new topic/room.
    
    Args:
        topic: Topic creation data
        token: JWT authentication token
        
    Returns:
        Success message
    """
    # Verify token
    payload = verify_token(token, expected_type="access")
    if not payload:
        raise http_exception(401, ErrorCode.INVALID_TOKEN, "Invalid token")

    _require_admin(payload)

    client_uuid = payload["sub"]
    
    # Check if topic already exists
    try:
        existing = redis_store.get_topic(topic.topic_id)
    except RedisUnavailableError as exc:
        existing = exc.fallback_result
        if existing:
            raise http_exception(
                503,
                ErrorCode.REDIS_UNAVAILABLE,
                "Topic already exists (fallback store)",
                details={"topic": existing},
            )
    except RedisOperationError as exc:
        logger.error("Error retrieving topic %s: %s", topic.topic_id, exc)
        raise http_exception(
            500,
            ErrorCode.TOPIC_LOOKUP_FAILED,
            "Failed to check topic availability",
        )

    if existing:
        raise http_exception(400, ErrorCode.TOPIC_ALREADY_EXISTS, "Topic already exists")

    try:
        success = redis_store.create_topic(topic.topic_id, client_uuid, topic.metadata)
    except RedisUnavailableError as exc:
        logger.warning("Redis unavailable when creating topic %s: %s", topic.topic_id, exc)
        raise http_exception(
            503,
            ErrorCode.REDIS_UNAVAILABLE,
            "Redis unavailable; topic recorded via fallback store",
            details={
                "topic_id": topic.topic_id,
                "creator": client_uuid,
                "metadata": topic.metadata or {},
            },
        )
    except RedisOperationError as exc:
        logger.error("Error creating topic %s: %s", topic.topic_id, exc)
        raise http_exception(500, ErrorCode.TOPIC_CREATION_FAILED, "Failed to create topic")

    if not success:
        raise http_exception(400, ErrorCode.TOPIC_ALREADY_EXISTS, "Topic already exists")
    
    logger.info(f"Topic created: {topic.topic_id} by {client_uuid}")
    
    return {
        "message": "Topic created successfully",
        "topic_id": topic.topic_id,
        "creator": client_uuid
    }


@app.get("/topics")
async def list_topics():
    """
    List all available topics/rooms.
    
    Returns:
        List of topic IDs with details
    """
    source = "redis"
    try:
        topic_ids = redis_store.list_topics()
    except RedisUnavailableError as exc:
        topic_ids = exc.fallback_result or []
        source = "fallback"
        logger.warning("Redis unavailable when listing topics: %s", exc)
    except RedisOperationError as exc:
        logger.error("Error listing topics: %s", exc)
        raise http_exception(500, ErrorCode.REDIS_OPERATION_FAILED, "Failed to list topics")
    topics = []
    
    for topic_id in topic_ids:
        topic_data_source = "redis"
        try:
            topic_data = redis_store.get_topic(topic_id)
        except RedisUnavailableError as exc:
            topic_data = exc.fallback_result
            topic_data_source = "fallback"
        except RedisOperationError as exc:
            logger.error("Error retrieving topic %s: %s", topic_id, exc)
            continue
        if topic_data:
            try:
                subscribers = redis_store.get_topic_subscribers(topic_id)
            except RedisUnavailableError as exc:
                subscribers = exc.fallback_result or set()
                topic_data_source = "fallback"
            except RedisOperationError as exc:
                logger.error("Error retrieving topic subscribers for %s: %s", topic_id, exc)
                subscribers = set()
            topics.append({
                "id": topic_id,
                "creator": topic_data.get("creator"),
                "metadata": topic_data.get("metadata", {}),
                "subscriber_count": len(subscribers),
                "source": topic_data_source,
            })

    return {
        "count": len(topics),
        "topics": topics,
        "source": source,
    }


@app.post("/topics/subscribe")
async def subscribe_to_topic(subscription: TopicSubscribe, token: str = Query(..., description="JWT authentication token")):
    """
    Subscribe to a topic/room.
    
    Args:
        subscription: Subscription data
        token: JWT authentication token
        
    Returns:
        Success message
    """
    # Verify token
    payload = verify_token(token, expected_type="access")
    if not payload:
        raise http_exception(401, ErrorCode.INVALID_TOKEN, "Invalid token")
    
    client_uuid = payload["sub"]
    
    # Check if topic exists
    try:
        topic = redis_store.get_topic(subscription.topic_id)
    except RedisUnavailableError as exc:
        topic = exc.fallback_result
        if not topic:
            raise http_exception(
                503,
                ErrorCode.REDIS_UNAVAILABLE,
                "Redis unavailable; topic lookup failed",
                details={"topic_id": subscription.topic_id},
            )
    except RedisOperationError as exc:
        logger.error("Error retrieving topic %s: %s", subscription.topic_id, exc)
        raise http_exception(500, ErrorCode.TOPIC_LOOKUP_FAILED, "Failed to validate topic")
    if not topic:
        raise http_exception(404, ErrorCode.TOPIC_NOT_FOUND, "Topic not found")

    try:
        success = redis_store.subscribe_to_topic(subscription.topic_id, client_uuid)
    except RedisUnavailableError as exc:
        logger.warning(
            "Redis unavailable when subscribing %s to %s: %s",
            client_uuid,
            subscription.topic_id,
            exc,
        )
        raise http_exception(
            503,
            ErrorCode.REDIS_UNAVAILABLE,
            "Redis unavailable; subscription recorded via fallback store",
            details={
                "topic_id": subscription.topic_id,
                "client_uuid": client_uuid,
            },
        )
    except RedisOperationError as exc:
        logger.error(
            "Error subscribing %s to %s: %s", client_uuid, subscription.topic_id, exc
        )
        raise http_exception(500, ErrorCode.SUBSCRIPTION_FAILED, "Failed to subscribe")

    if not success:
        raise http_exception(500, ErrorCode.SUBSCRIPTION_FAILED, "Failed to subscribe")

    logger.info(f"Client {client_uuid} subscribed to topic {subscription.topic_id}")
    
    return {
        "message": "Subscribed successfully",
        "topic_id": subscription.topic_id,
        "client_uuid": client_uuid
    }


@app.post("/topics/unsubscribe")
async def unsubscribe_from_topic(subscription: TopicSubscribe, token: str = Query(..., description="JWT authentication token")):
    """
    Unsubscribe from a topic/room.
    
    Args:
        subscription: Subscription data
        token: JWT authentication token
        
    Returns:
        Success message
    """
    # Verify token
    payload = verify_token(token, expected_type="access")
    if not payload:
        raise http_exception(401, ErrorCode.INVALID_TOKEN, "Invalid token")
    
    client_uuid = payload["sub"]
    
    # Unsubscribe
    try:
        success = redis_store.unsubscribe_from_topic(subscription.topic_id, client_uuid)
    except RedisUnavailableError as exc:
        logger.warning(
            "Redis unavailable when unsubscribing %s from %s: %s",
            client_uuid,
            subscription.topic_id,
            exc,
        )
        raise http_exception(
            503,
            ErrorCode.REDIS_UNAVAILABLE,
            "Redis unavailable; unsubscribe recorded via fallback store",
            details={
                "topic_id": subscription.topic_id,
                "client_uuid": client_uuid,
            },
        )
    except RedisOperationError as exc:
        logger.error(
            "Error unsubscribing %s from %s: %s", client_uuid, subscription.topic_id, exc
        )
        raise http_exception(500, ErrorCode.UNSUBSCRIBE_FAILED, "Failed to unsubscribe")

    if not success:
        raise http_exception(500, ErrorCode.UNSUBSCRIBE_FAILED, "Failed to unsubscribe")

    logger.info(f"Client {client_uuid} unsubscribed from topic {subscription.topic_id}")
    
    return {
        "message": "Unsubscribed successfully",
        "topic_id": subscription.topic_id,
        "client_uuid": client_uuid
    }


@app.get("/topics/{topic_id}")
async def get_topic_info(topic_id: str):
    """
    Get information about a specific topic.
    
    Args:
        topic_id: Topic identifier
        
    Returns:
        Topic information
    """
    try:
        topic = redis_store.get_topic(topic_id)
    except RedisUnavailableError as exc:
        topic = exc.fallback_result
        if not topic:
            raise http_exception(
                503,
                ErrorCode.REDIS_UNAVAILABLE,
                "Redis unavailable; topic not found in fallback",
                details={"topic_id": topic_id},
            )
    except RedisOperationError as exc:
        logger.error("Error retrieving topic %s: %s", topic_id, exc)
        raise http_exception(500, ErrorCode.TOPIC_LOOKUP_FAILED, "Failed to get topic")
    if not topic:
        raise http_exception(404, ErrorCode.TOPIC_NOT_FOUND, "Topic not found")

    try:
        subscribers = redis_store.get_topic_subscribers(topic_id)
    except RedisUnavailableError as exc:
        subscribers = exc.fallback_result or set()
    except RedisOperationError as exc:
        logger.error("Error retrieving subscribers for %s: %s", topic_id, exc)
        subscribers = set()

    return {
        "id": topic_id,
        "creator": topic.get("creator"),
        "metadata": topic.get("metadata", {}),
        "subscriber_count": len(subscribers),
        "subscribers": list(subscribers)
    }


@app.get("/health/redis")
async def redis_health():
    """Redis health status and fallback information."""
    return redis_store.check_health()

@app.post("/admin/tokens/revoke")
async def admin_revoke_tokens(
    revoke_request: TokenRevokeRequest,
    token: str = Query(..., description="Administrator access token"),
):
    """Revoke tokens stored in Redis via an administrative API."""

    payload = verify_token(token, expected_type="access")
    if not payload:
        raise http_exception(401, ErrorCode.INVALID_TOKEN, "Invalid token")

    _require_admin(payload)

    revoked_count = 0
    if revoke_request.token or revoke_request.jti:
        revoked = revoke_token(token=revoke_request.token, jti=revoke_request.jti)
        revoked_count = 1 if revoked else 0
    elif revoke_request.client_uuid:
        revoked_count = revoke_client_tokens(revoke_request.client_uuid, revoke_request.token_type)
    else:
        raise http_exception(400, ErrorCode.MISSING_REVOCATION_TARGET, "No revocation target specified")

    if revoked_count == 0:
        raise http_exception(404, ErrorCode.TOKEN_NOT_FOUND, "Token(s) not found")

    return {"revoked": revoked_count}


@app.websocket("/ws")
async def websocket_endpoint(
    websocket: WebSocket,
    token: str = Query(..., description="JWT authentication token"),
):
    """Primary WebSocket endpoint used by registered clients."""

    payload = verify_token(token, expected_type="access")
    if not payload:
        await websocket.accept()
        ERROR_COUNTER.labels(type="websocket_auth").inc()
        error_frame = websocket_error_frame(
            ErrorCode.INVALID_TOKEN,
            "Invalid token",
            "Request a new access token via /auth/refresh and reconnect.",
        )
        await websocket.send_json(error_frame)
        MESSAGE_COUNTER.labels(direction="outbound", channel="websocket").inc()
        await websocket.close(code=1008, reason="Invalid token")
        return

    client_uuid = UUID(payload["sub"])
    client_name = payload["name"]

    async def send_payload(payload: dict[str, Any]) -> None:
        await websocket.send_json(payload)
        MESSAGE_COUNTER.labels(direction="outbound", channel="websocket").inc()

    session_attributes = {
        "client.uuid": str(client_uuid),
        "client.name": client_name,
    }

    with tracer.start_as_current_span("websocket.session", attributes=session_attributes):
        await manager.connect(websocket, client_uuid, client_name)
        logger.info(
            "websocket_client_connected",
            extra={"client_uuid": str(client_uuid), "client_name": client_name},
        )

        try:
            await send_payload(
                {
                    "type": "connection",
                    "message": "Connected successfully",
                    "uuid": str(client_uuid),
                    "name": client_name,
                    "timestamp": time.time(),
                }
            )

            while True:
                data = await websocket.receive_text()
                MESSAGE_COUNTER.labels(direction="inbound", channel="websocket").inc()

                with tracer.start_as_current_span(
                    "websocket.message",
                    attributes={"client.uuid": str(client_uuid)},
                ):
                    try:
                        message_data = json.loads(data)
                    except json.JSONDecodeError:
                        ERROR_COUNTER.labels(type="websocket_invalid_json").inc()
                        await send_payload(
                            websocket_error_frame(
                                ErrorCode.INVALID_MESSAGE_FORMAT,
                                "Invalid JSON format",
                                "Send JSON-encoded text frames that conform to the message schema.",
                            )
                        )
                        continue

                    if message_data.get("type") == "ack":
                        with tracer.start_as_current_span(
                            "websocket.ack",
                            attributes={"message.id": str(message_data.get("msgid"))},
                        ):
                            try:
                                acknowledgment = MessageAcknowledgment(**message_data)
                            except Exception as validation_error:
                                ERROR_COUNTER.labels(type="websocket_invalid_ack").inc()
                                await send_payload(
                                    websocket_error_frame(
                                        ErrorCode.INVALID_ACK,
                                        f"Invalid acknowledgment format: {validation_error}",
                                        "Ensure acknowledgment payload matches the documented schema and resend.",
                                        details={"error": str(validation_error)},
                                    )
                                )
                                continue

                            if acknowledgment.from_ != client_uuid:
                                ERROR_COUNTER.labels(type="ack_sender_mismatch").inc()
                                await send_payload(
                                    websocket_error_frame(
                                        ErrorCode.ACK_SENDER_MISMATCH,
                                        "Acknowledgment sender mismatch",
                                        "Resend the acknowledgment using your authenticated client UUID.",
                                        details={
                                            "expected": str(client_uuid),
                                            "received": str(acknowledgment.from_),
                                        },
                                    )
                                )
                                continue

                            await manager.handle_acknowledgment(
                                str(acknowledgment.msgid),
                                acknowledgment.status,
                                str(client_uuid),
                            )

                            await send_payload(
                                {
                                    "type": "ack_received",
                                    "msgid": str(acknowledgment.msgid),
                                    "status": acknowledgment.status,
                                    "timestamp": time.time(),
                                }
                            )
                        continue

                    if "topic_id" in message_data:
                        with tracer.start_as_current_span(
                            "websocket.topic_message",
                            attributes={
                                "topic.id": message_data.get("topic_id"),
                                "message.id": str(message_data.get("msgid")),
                            },
                        ):
                            try:
                                topic_msg = TopicMessage(**message_data)
                            except Exception as validation_error:
                                ERROR_COUNTER.labels(type="websocket_invalid_topic").inc()
                                await send_payload(
                                    websocket_error_frame(
                                        ErrorCode.INVALID_TOPIC_MESSAGE,
                                        f"Invalid topic message format: {validation_error}",
                                        "Ensure topic messages follow the TopicMessage schema and resend.",
                                        details={"error": str(validation_error)},
                                    )
                                )
                                continue

                            if topic_msg.from_ != client_uuid:
                                ERROR_COUNTER.labels(type="topic_sender_mismatch").inc()
                                await send_payload(
                                    websocket_error_frame(
                                        ErrorCode.MESSAGE_SENDER_MISMATCH,
                                        "Sender UUID does not match authenticated client",
                                        "Send the topic message using your authenticated client UUID.",
                                        details={
                                            "expected": str(client_uuid),
                                            "received": str(topic_msg.from_),
                                        },
                                    )
                                )
                                continue

                            try:
                                topic = redis_store.get_topic(topic_msg.topic_id)
                            except RedisUnavailableError as exc:
                                topic = exc.fallback_result
                            except RedisOperationError as exc:
                                ERROR_COUNTER.labels(type="topic_lookup_error").inc()
                                logger.error(
                                    "websocket_topic_lookup_error",
                                    extra={
                                        "topic_id": topic_msg.topic_id,
                                        "error": str(exc),
                                    },
                                )
                                topic = None
                            if not topic:
                                ERROR_COUNTER.labels(type="topic_missing").inc()
                                await send_payload(
                                    websocket_error_frame(
                                        ErrorCode.TOPIC_NOT_FOUND,
                                        f"Topic {topic_msg.topic_id} not found",
                                        "Create the topic first or verify the topic identifier before retrying.",
                                        details={"topic_id": topic_msg.topic_id},
                                    )
                                )
                                continue

                            try:
                                subscribers = redis_store.get_topic_subscribers(topic_msg.topic_id)
                            except RedisUnavailableError as exc:
                                subscribers = exc.fallback_result or set()
                            except RedisOperationError as exc:
                                ERROR_COUNTER.labels(type="topic_subscriber_error").inc()
                                logger.error(
                                    "websocket_subscriber_lookup_error",
                                    extra={
                                        "topic_id": topic_msg.topic_id,
                                        "error": str(exc),
                                    },
                                )
                                subscribers = set()

                            if str(client_uuid) not in subscribers:
                                ERROR_COUNTER.labels(type="topic_not_subscribed").inc()
                                await send_payload(
                                    websocket_error_frame(
                                        ErrorCode.NOT_SUBSCRIBED_TO_TOPIC,
                                        f"Not subscribed to topic {topic_msg.topic_id}",
                                        "Subscribe to the topic before publishing messages to it.",
                                        details={"topic_id": topic_msg.topic_id},
                                    )
                                )
                                continue

                            sent_count = await manager.broadcast_to_topic(
                                topic_msg.topic_id,
                                message_data,
                                exclude=client_uuid,
                            )

                            await send_payload(
                                {
                                    "type": "topic_sent",
                                    "message": f"Message sent to {sent_count} subscribers",
                                    "topic_id": topic_msg.topic_id,
                                    "msgid": str(topic_msg.msgid),
                                    "timestamp": time.time(),
                                }
                            )
                        continue

                    with tracer.start_as_current_span(
                        "websocket.direct_message",
                        attributes={"message.id": str(message_data.get("msgid"))},
                    ):
                        try:
                            message = Message(**message_data)
                        except Exception as validation_error:
                            ERROR_COUNTER.labels(type="websocket_invalid_message").inc()
                            await send_payload(
                                websocket_error_frame(
                                    ErrorCode.INVALID_MESSAGE_FORMAT,
                                    f"Invalid message format: {validation_error}",
                                    "Ensure direct messages follow the documented schema and resend.",
                                    details={"error": str(validation_error)},
                                )
                            )
                            continue

                        if message.from_ != client_uuid:
                            ERROR_COUNTER.labels(type="direct_sender_mismatch").inc()
                            await send_payload(
                                websocket_error_frame(
                                    ErrorCode.MESSAGE_SENDER_MISMATCH,
                                    "Sender UUID does not match authenticated client",
                                    "Send the message using your authenticated client UUID.",
                                    details={
                                        "expected": str(client_uuid),
                                        "received": str(message.from_),
                                    },
                                )
                            )
                            continue

                        delivery_result = await manager.send_message(
                            message_data,
                            message.to,
                        )

                        await send_payload(
                            {
                                "type": "delivery_status",
                                "status": delivery_result.status,
                                "message": delivery_result.detail,
                                "msgid": str(message.msgid),
                                "timestamp": time.time(),
                                "acknowledge": message.acknowledge,
                            }
                        )

        except WebSocketDisconnect:
            manager.disconnect(client_uuid)
            logger.info(
                "websocket_client_disconnected",
                extra={"client_uuid": str(client_uuid), "client_name": client_name},
            )
        except Exception as exc:  # noqa: BLE001
            ERROR_COUNTER.labels(type="websocket_error").inc()
            logger.exception(
                "websocket_session_error",
                extra={"client_uuid": str(client_uuid), "client_name": client_name},
            )
            manager.disconnect(client_uuid)


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
