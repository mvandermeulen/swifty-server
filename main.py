"""
FastAPI WebSocket Server for Real-Time Communications.

This server provides:
- Client registration with name and UUID
- JWT authentication
- WebSocket connections for real-time messaging
- Message routing between clients by UUID
"""
from fastapi import FastAPI, WebSocket, WebSocketDisconnect, HTTPException, Query
from fastapi.responses import JSONResponse
from typing import Optional
from uuid import UUID
import json
import time
import logging

from models import ClientRegistration, Message, MessageAcknowledgment, TopicCreate, TopicSubscribe, TopicMessage
from auth import create_access_token, verify_token
from connection_manager import ConnectionManager
from redis_store import RedisOperationError, RedisUnavailableError, redis_store

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
            "redis_health": "/health/redis",
        }
    }


@app.post("/register")
async def register_client(registration: ClientRegistration):
    """
    Register a client and receive a JWT token.
    
    Args:
        registration: Client registration data (name and UUID)
        
    Returns:
        JWT token for authentication
    """
    try:
        # Create JWT token
        token = create_access_token(registration.uuid, registration.name)
        
        logger.info(f"Client registered: {registration.name} ({registration.uuid})")
        
        return {
            "token": token,
            "uuid": str(registration.uuid),
            "name": registration.name,
            "message": "Registration successful"
        }
    except Exception as e:
        logger.error(f"Registration error: {e}")
        raise HTTPException(status_code=500, detail=str(e))


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
    payload = verify_token(token)
    if not payload:
        raise HTTPException(status_code=401, detail="Invalid token")
    
    client_uuid = payload["sub"]
    
    # Check if topic already exists
    try:
        existing = redis_store.get_topic(topic.topic_id)
    except RedisUnavailableError as exc:
        existing = exc.fallback_result
        if existing:
            return JSONResponse(
                status_code=503,
                content={
                    "detail": "Topic already exists (fallback store)",
                    "topic": existing,
                },
            )
    except RedisOperationError as exc:
        logger.error("Error retrieving topic %s: %s", topic.topic_id, exc)
        raise HTTPException(status_code=500, detail="Failed to check topic availability")

    if existing:
        raise HTTPException(status_code=400, detail="Topic already exists")

    try:
        success = redis_store.create_topic(topic.topic_id, client_uuid, topic.metadata)
    except RedisUnavailableError as exc:
        logger.warning("Redis unavailable when creating topic %s: %s", topic.topic_id, exc)
        return JSONResponse(
            status_code=503,
            content={
                "detail": "Redis unavailable; topic recorded via fallback store",
                "topic_id": topic.topic_id,
                "creator": client_uuid,
                "metadata": topic.metadata or {},
            },
        )
    except RedisOperationError as exc:
        logger.error("Error creating topic %s: %s", topic.topic_id, exc)
        raise HTTPException(status_code=500, detail="Failed to create topic")

    if not success:
        raise HTTPException(status_code=400, detail="Topic already exists")
    
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
        raise HTTPException(status_code=500, detail="Failed to list topics")
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
    payload = verify_token(token)
    if not payload:
        raise HTTPException(status_code=401, detail="Invalid token")
    
    client_uuid = payload["sub"]
    
    # Check if topic exists
    try:
        topic = redis_store.get_topic(subscription.topic_id)
    except RedisUnavailableError as exc:
        topic = exc.fallback_result
        if not topic:
            return JSONResponse(
                status_code=503,
                content={
                    "detail": "Redis unavailable; topic lookup failed",
                    "topic_id": subscription.topic_id,
                },
            )
    except RedisOperationError as exc:
        logger.error("Error retrieving topic %s: %s", subscription.topic_id, exc)
        raise HTTPException(status_code=500, detail="Failed to validate topic")
    if not topic:
        raise HTTPException(status_code=404, detail="Topic not found")

    try:
        success = redis_store.subscribe_to_topic(subscription.topic_id, client_uuid)
    except RedisUnavailableError as exc:
        logger.warning(
            "Redis unavailable when subscribing %s to %s: %s",
            client_uuid,
            subscription.topic_id,
            exc,
        )
        return JSONResponse(
            status_code=503,
            content={
                "detail": "Redis unavailable; subscription recorded via fallback store",
                "topic_id": subscription.topic_id,
                "client_uuid": client_uuid,
            },
        )
    except RedisOperationError as exc:
        logger.error(
            "Error subscribing %s to %s: %s", client_uuid, subscription.topic_id, exc
        )
        raise HTTPException(status_code=500, detail="Failed to subscribe")

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
    payload = verify_token(token)
    if not payload:
        raise HTTPException(status_code=401, detail="Invalid token")
    
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
        return JSONResponse(
            status_code=503,
            content={
                "detail": "Redis unavailable; unsubscribe recorded via fallback store",
                "topic_id": subscription.topic_id,
                "client_uuid": client_uuid,
            },
        )
    except RedisOperationError as exc:
        logger.error(
            "Error unsubscribing %s from %s: %s", client_uuid, subscription.topic_id, exc
        )
        raise HTTPException(status_code=500, detail="Failed to unsubscribe")

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
    try:
        topic = redis_store.get_topic(topic_id)
    except RedisUnavailableError as exc:
        topic = exc.fallback_result
        if not topic:
            return JSONResponse(
                status_code=503,
                content={
                    "detail": "Redis unavailable; topic not found in fallback",
                    "topic_id": topic_id,
                },
            )
    except RedisOperationError as exc:
        logger.error("Error retrieving topic %s: %s", topic_id, exc)
        raise HTTPException(status_code=500, detail="Failed to get topic")
    if not topic:
        raise HTTPException(status_code=404, detail="Topic not found")

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
    payload = verify_token(token)
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
                    try:
                        topic = redis_store.get_topic(topic_msg.topic_id)
                    except RedisUnavailableError as exc:
                        topic = exc.fallback_result
                    except RedisOperationError as exc:
                        logger.error(
                            "Error fetching topic %s for websocket message: %s",
                            topic_msg.topic_id,
                            exc,
                        )
                        topic = None
                    if not topic:
                        await websocket.send_json({
                            "type": "error",
                            "message": f"Topic {topic_msg.topic_id} not found",
                            "timestamp": time.time()
                        })
                        continue

                    # Verify sender is subscribed to topic
                    try:
                        subscribers = redis_store.get_topic_subscribers(topic_msg.topic_id)
                    except RedisUnavailableError as exc:
                        subscribers = exc.fallback_result or set()
                    except RedisOperationError as exc:
                        logger.error(
                            "Error fetching subscribers for topic %s: %s",
                            topic_msg.topic_id,
                            exc,
                        )
                        subscribers = set()

                    if str(client_uuid) not in subscribers:
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
                    success = await manager.send_message(
                        message_data,
                        message.to
                    )
                    
                    # Send confirmation to sender
                    if success:
                        await websocket.send_json({
                            "type": "sent",
                            "message": "Message delivered",
                            "msgid": str(message.msgid),
                            "timestamp": time.time()
                        })
                    else:
                        await websocket.send_json({
                            "type": "error",
                            "message": f"Recipient {message.to} not connected",
                            "msgid": str(message.msgid),
                            "timestamp": time.time()
                        })
                    
                    # Handle acknowledgment if required
                    if message.acknowledge:
                        # The recipient should send an acknowledgment
                        # This is handled by the recipient's client logic
                        pass
                    
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
