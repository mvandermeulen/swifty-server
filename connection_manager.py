"""
WebSocket connection manager for handling client connections and message routing.
"""
from fastapi import WebSocket
from typing import Dict, Optional, Set
from uuid import UUID
import json
import logging

from redis_store import RedisUnavailableError, redis_store

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class ConnectionManager:
    """Manages WebSocket connections for all clients."""
    
    def __init__(self):
        # Dictionary mapping client UUID to WebSocket connection (in-memory)
        self.active_connections: Dict[str, WebSocket] = {}
        # Fallback dictionary for client names (when Redis is unavailable)
        self.client_names: Dict[str, str] = {}
    
    async def connect(self, websocket: WebSocket, client_uuid: UUID, client_name: str):
        """
        Accept a new WebSocket connection and register the client.
        
        Args:
            websocket: WebSocket connection
            client_uuid: Client's UUID
            client_name: Client's name
        """
        await websocket.accept()
        client_uuid_str = str(client_uuid)
        self.active_connections[client_uuid_str] = websocket
        self.client_names[client_uuid_str] = client_name
        
        # Store in Redis
        try:
            redis_store.set_client_connected(client_uuid_str, client_name)
        except RedisUnavailableError as exc:
            logger.warning(
                "Redis unavailable while marking client connected %s: %s", client_uuid_str, exc
            )
        
        logger.info(f"Client connected: {client_name} ({client_uuid_str})")
        logger.info(f"Active connections: {len(self.active_connections)}")
    
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
            try:
                redis_store.cleanup_client(client_uuid_str)
            except RedisUnavailableError as exc:
                logger.warning(
                    "Redis unavailable while cleaning up client %s: %s", client_uuid_str, exc
                )
            
            logger.info(f"Client disconnected: {client_name} ({client_uuid_str})")
            logger.info(f"Active connections: {len(self.active_connections)}")
    
    async def send_message(self, message: dict, recipient_uuid: UUID) -> bool:
        """
        Send a message to a specific client by UUID.
        
        Args:
            message: Message dictionary
            recipient_uuid: Recipient's UUID
            
        Returns:
            True if message was sent successfully, False otherwise
        """
        recipient_uuid_str = str(recipient_uuid)
        
        if recipient_uuid_str in self.active_connections:
            websocket = self.active_connections[recipient_uuid_str]
            try:
                await websocket.send_json(message)
                logger.info(f"Message sent to {recipient_uuid_str}")
                return True
            except Exception as e:
                logger.error(f"Error sending message to {recipient_uuid_str}: {e}")
                self.disconnect(recipient_uuid)
                return False
        else:
            logger.warning(f"Recipient {recipient_uuid_str} not connected")
            return False
    
    async def broadcast(self, message: dict, exclude: Optional[UUID] = None):
        """
        Broadcast a message to all connected clients.
        
        Args:
            message: Message dictionary
            exclude: Optional UUID to exclude from broadcast
        """
        exclude_str = str(exclude) if exclude else None
        
        for client_uuid_str in list(self.active_connections.keys()):
            if exclude_str and client_uuid_str == exclude_str:
                continue
            
            websocket = self.active_connections[client_uuid_str]
            try:
                await websocket.send_json(message)
            except Exception as e:
                logger.error(f"Error broadcasting to {client_uuid_str}: {e}")
                self.disconnect(UUID(client_uuid_str))
    
    def is_client_connected(self, client_uuid: UUID) -> bool:
        """
        Check if a client is currently connected.
        
        Args:
            client_uuid: Client's UUID
            
        Returns:
            True if client is connected, False otherwise
        """
        return str(client_uuid) in self.active_connections
    
    def get_connected_clients(self) -> Dict[str, str]:
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
    
    async def broadcast_to_topic(self, topic_id: str, message: dict, exclude: Optional[UUID] = None):
        """
        Broadcast a message to all subscribers of a topic.
        
        Args:
            topic_id: Topic identifier
            message: Message dictionary
            exclude: Optional UUID to exclude from broadcast
        """
        # Get topic subscribers from Redis
        try:
            subscribers = redis_store.get_topic_subscribers(topic_id)
        except RedisUnavailableError as exc:
            subscribers = exc.fallback_result or set()
            logger.warning(
                "Redis unavailable when fetching subscribers for %s: %s", topic_id, exc
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
                    logger.error(f"Error broadcasting to {client_uuid_str}: {e}")
                    self.disconnect(UUID(client_uuid_str))
        
        logger.info(f"Broadcast to topic {topic_id}: {sent_count} recipients")
        return sent_count
