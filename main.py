"""
FastAPI WebSocket Server for Real-Time Communications.

This server provides:
- Client registration with name and UUID
- JWT authentication
- WebSocket connections for real-time messaging
- Message routing between clients by UUID
"""
from fastapi import FastAPI, WebSocket, WebSocketDisconnect, HTTPException, Query
from uuid import UUID
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
from redis_store import redis_store

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Create FastAPI app
app = FastAPI(
    title="Swifty Server",
    description="FastAPI WebSocket Server for Real-Time Communications",
    version="1.0.0"
)

# Create connection manager
manager = ConnectionManager()


def _require_admin(payload: dict) -> None:
    roles = payload.get("roles") or []
    if "admin" not in roles:
        raise HTTPException(status_code=403, detail="Insufficient permissions")


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
            "delivery_status": "/messages/{msgid}/status",
            "undelivered": "/clients/{uuid}/undelivered",
            "delivery_metrics": "/delivery/metrics"
            "refresh": "/auth/refresh",
            "revoke_tokens": "/admin/tokens/revoke",
        }
    }


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

        logger.info(f"Client registered: {registration.name} ({registration.uuid})")

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
        logger.error(f"Registration error: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/auth/refresh")
async def refresh_tokens(request: TokenRefreshRequest):
    """Exchange a refresh token for a new access/refresh pair."""

    tokens = rotate_refresh_token(request.refresh_token)
    if not tokens:
        raise HTTPException(status_code=401, detail="Invalid refresh token")

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
        raise HTTPException(status_code=401, detail="Invalid token")

    status = redis_store.get_delivery_status(str(msgid))
    if not status:
        raise HTTPException(status_code=404, detail="Message not found")

    requester_uuid = payload["sub"]
    if requester_uuid not in {status.get("sender"), status.get("recipient")}:
        raise HTTPException(status_code=403, detail="Not authorized to view this message")

    return status


@app.get("/clients/{client_uuid}/undelivered")
async def get_undelivered_messages(client_uuid: UUID, token: str = Query(..., description="JWT authentication token")):
    """List undelivered messages for a client."""
    payload = verify_token(token)
    if not payload:
        raise HTTPException(status_code=401, detail="Invalid token")

    if str(client_uuid) != payload["sub"]:
        raise HTTPException(status_code=403, detail="Cannot query undelivered messages for other clients")

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
        raise HTTPException(status_code=401, detail="Invalid token")

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
        raise HTTPException(status_code=401, detail="Invalid token")

    _require_admin(payload)

    client_uuid = payload["sub"]
    
    # Check if topic already exists
    existing = redis_store.get_topic(topic.topic_id)
    if existing:
        raise HTTPException(status_code=400, detail="Topic already exists")
    
    # Create topic
    success = redis_store.create_topic(topic.topic_id, client_uuid, topic.metadata)
    if not success:
        raise HTTPException(status_code=500, detail="Failed to create topic")
    
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
    topic_ids = redis_store.list_topics()
    topics = []
    
    for topic_id in topic_ids:
        topic_data = redis_store.get_topic(topic_id)
        if topic_data:
            subscribers = redis_store.get_topic_subscribers(topic_id)
            topics.append({
                "id": topic_id,
                "creator": topic_data.get("creator"),
                "metadata": topic_data.get("metadata", {}),
                "subscriber_count": len(subscribers)
            })
    
    return {
        "count": len(topics),
        "topics": topics
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
        raise HTTPException(status_code=401, detail="Invalid token")
    
    client_uuid = payload["sub"]
    
    # Check if topic exists
    topic = redis_store.get_topic(subscription.topic_id)
    if not topic:
        raise HTTPException(status_code=404, detail="Topic not found")
    
    # Subscribe
    success = redis_store.subscribe_to_topic(subscription.topic_id, client_uuid)
    if not success:
        raise HTTPException(status_code=500, detail="Failed to subscribe")
    
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
        raise HTTPException(status_code=401, detail="Invalid token")
    
    client_uuid = payload["sub"]
    
    # Unsubscribe
    success = redis_store.unsubscribe_from_topic(subscription.topic_id, client_uuid)
    if not success:
        raise HTTPException(status_code=500, detail="Failed to unsubscribe")
    
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
    topic = redis_store.get_topic(topic_id)
    if not topic:
        raise HTTPException(status_code=404, detail="Topic not found")
    
    subscribers = redis_store.get_topic_subscribers(topic_id)
    
    return {
        "id": topic_id,
        "creator": topic.get("creator"),
        "metadata": topic.get("metadata", {}),
        "subscriber_count": len(subscribers),
        "subscribers": list(subscribers)
    }


@app.post("/admin/tokens/revoke")
async def admin_revoke_tokens(
    revoke_request: TokenRevokeRequest,
    token: str = Query(..., description="Administrator access token"),
):
    """Revoke tokens stored in Redis via an administrative API."""

    payload = verify_token(token, expected_type="access")
    if not payload:
        raise HTTPException(status_code=401, detail="Invalid token")

    _require_admin(payload)

    revoked_count = 0
    if revoke_request.token or revoke_request.jti:
        revoked = revoke_token(token=revoke_request.token, jti=revoke_request.jti)
        revoked_count = 1 if revoked else 0
    elif revoke_request.client_uuid:
        revoked_count = revoke_client_tokens(revoke_request.client_uuid, revoke_request.token_type)
    else:
        raise HTTPException(status_code=400, detail="No revocation target specified")

    if revoked_count == 0:
        raise HTTPException(status_code=404, detail="Token(s) not found")

    return {"revoked": revoked_count}


@app.websocket("/ws")
async def websocket_endpoint(
    websocket: WebSocket,
    token: str = Query(..., description="JWT authentication token")
):
    """
    WebSocket endpoint for real-time messaging.
    
    Clients must provide a valid JWT token obtained from /register.
    Messages are routed to recipients by their UUID.
    
    Args:
        websocket: WebSocket connection
        token: JWT authentication token
    """
    # Verify token
    payload = verify_token(token, expected_type="access")
    if not payload:
        await websocket.close(code=1008, reason="Invalid token")
        return
    
    client_uuid = UUID(payload["sub"])
    client_name = payload["name"]
    
    # Connect client
    await manager.connect(websocket, client_uuid, client_name)
    
    try:
        # Send welcome message
        await websocket.send_json({
            "type": "connection",
            "message": "Connected successfully",
            "uuid": str(client_uuid),
            "name": client_name,
            "timestamp": time.time()
        })
        
        # Listen for messages
        while True:
            # Receive message
            data = await websocket.receive_text()
            
            try:
                message_data = json.loads(data)

                # Handle delivery acknowledgments
                if message_data.get("type") == "ack":
                    try:
                        acknowledgment = MessageAcknowledgment(**message_data)
                    except Exception as validation_error:
                        await websocket.send_json({
                            "type": "error",
                            "message": f"Invalid acknowledgment format: {validation_error}",
                            "timestamp": time.time()
                        })
                        continue

                    if acknowledgment.from_ != client_uuid:
                        await websocket.send_json({
                            "type": "error",
                            "message": "Acknowledgment sender mismatch",
                            "timestamp": time.time()
                        })
                        continue

                    await manager.handle_acknowledgment(
                        str(acknowledgment.msgid),
                        acknowledgment.status,
                        str(client_uuid)
                    )

                    await websocket.send_json({
                        "type": "ack_received",
                        "msgid": str(acknowledgment.msgid),
                        "status": acknowledgment.status,
                        "timestamp": time.time()
                    })
                    continue

                # Check if this is a topic message
                if "topic_id" in message_data:
                    # Handle topic message
                    try:
                        topic_msg = TopicMessage(**message_data)
                    except Exception as validation_error:
                        await websocket.send_json({
                            "type": "error",
                            "message": f"Invalid topic message format: {validation_error}",
                            "timestamp": time.time()
                        })
                        continue
                    
                    # Verify sender UUID matches authenticated client
                    if topic_msg.from_ != client_uuid:
                        await websocket.send_json({
                            "type": "error",
                            "message": "Sender UUID does not match authenticated client",
                            "timestamp": time.time()
                        })
                        continue
                    
                    # Verify topic exists
                    topic = redis_store.get_topic(topic_msg.topic_id)
                    if not topic:
                        await websocket.send_json({
                            "type": "error",
                            "message": f"Topic {topic_msg.topic_id} not found",
                            "timestamp": time.time()
                        })
                        continue
                    
                    # Verify sender is subscribed to topic
                    if str(client_uuid) not in redis_store.get_topic_subscribers(topic_msg.topic_id):
                        await websocket.send_json({
                            "type": "error",
                            "message": f"Not subscribed to topic {topic_msg.topic_id}",
                            "timestamp": time.time()
                        })
                        continue
                    
                    # Broadcast to topic subscribers
                    sent_count = await manager.broadcast_to_topic(
                        topic_msg.topic_id,
                        message_data,
                        exclude=client_uuid
                    )
                    
                    # Send confirmation to sender
                    await websocket.send_json({
                        "type": "topic_sent",
                        "message": f"Message sent to {sent_count} subscribers",
                        "topic_id": topic_msg.topic_id,
                        "msgid": str(topic_msg.msgid),
                        "timestamp": time.time()
                    })
                
                else:
                    # Handle direct message (existing logic)
                    # Validate message structure
                    try:
                        message = Message(**message_data)
                    except Exception as validation_error:
                        await websocket.send_json({
                            "type": "error",
                            "message": f"Invalid message format: {validation_error}",
                            "timestamp": time.time()
                        })
                        continue
                    
                    # Verify sender UUID matches authenticated client
                    if message.from_ != client_uuid:
                        await websocket.send_json({
                            "type": "error",
                            "message": "Sender UUID does not match authenticated client",
                            "timestamp": time.time()
                        })
                        continue

                    # Route message to recipient
                    delivery_result = await manager.send_message(
                        message_data,
                        message.to
                    )

                    await websocket.send_json({
                        "type": "delivery_status",
                        "status": delivery_result.status,
                        "message": delivery_result.detail,
                        "msgid": str(message.msgid),
                        "timestamp": time.time(),
                        "acknowledge": message.acknowledge,
                    })
                    
            except json.JSONDecodeError:
                await websocket.send_json({
                    "type": "error",
                    "message": "Invalid JSON format",
                    "timestamp": time.time()
                })
            except Exception as e:
                logger.error(f"Error processing message: {e}")
                await websocket.send_json({
                    "type": "error",
                    "message": f"Error processing message: {str(e)}",
                    "timestamp": time.time()
                })
                
    except WebSocketDisconnect:
        manager.disconnect(client_uuid)
        logger.info(f"Client disconnected: {client_name} ({client_uuid})")
    except Exception as e:
        logger.error(f"WebSocket error for {client_name}: {e}")
        manager.disconnect(client_uuid)


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
