"""
Redis storage layer for WebSocket connections, client data, and topics.
"""
import redis
import json
import os
import logging
import time
from collections import defaultdict
from typing import Any, Dict, List, Optional, Set
from uuid import UUID

logger = logging.getLogger(__name__)

# Redis configuration
REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))
REDIS_DB = int(os.getenv("REDIS_DB", "0"))
REDIS_PASSWORD = os.getenv("REDIS_PASSWORD", None)


class RedisStore:
    """Redis storage for client data, tokens, and topics."""
    
    def __init__(self):
        """Initialize Redis connection."""
        try:
            self.client = redis.Redis(
                host=REDIS_HOST,
                port=REDIS_PORT,
                db=REDIS_DB,
                password=REDIS_PASSWORD,
                decode_responses=True
            )
            # Test connection
            self.client.ping()
            logger.info(f"Redis connected: {REDIS_HOST}:{REDIS_PORT}")
        except redis.ConnectionError as e:
            logger.warning(f"Redis connection failed: {e}. Using in-memory fallback.")
            self.client = None

        # In-memory fallbacks for environments without Redis
        self._offline_queues: Dict[str, List[dict]] = defaultdict(list)
        self._delivery_status: Dict[str, Dict[str, Any]] = {}
        self._recipient_index: Dict[str, Set[str]] = defaultdict(set)
        self._sender_index: Dict[str, Set[str]] = defaultdict(set)
    
    # Client Management
    
    def register_client(self, client_uuid: str, client_name: str, token: str) -> bool:
        """
        Register a client with their token.
        
        Args:
            client_uuid: Client's UUID
            client_name: Client's name
            token: JWT token
            
        Returns:
            True if successful
        """
        if not self.client:
            return False
        
        try:
            client_key = f"client:{client_uuid}"
            client_data = {
                "uuid": client_uuid,
                "name": client_name,
                "token": token
            }
            self.client.hset(client_key, mapping=client_data)
            
            # Store token mapping for quick lookup
            token_key = f"token:{token}"
            self.client.setex(token_key, 3600, client_uuid)  # 1 hour expiry
            
            logger.info(f"Client registered in Redis: {client_name} ({client_uuid})")
            return True
        except Exception as e:
            logger.error(f"Error registering client in Redis: {e}")
            return False
    
    def get_client_by_uuid(self, client_uuid: str) -> Optional[Dict]:
        """
        Get client data by UUID.
        
        Args:
            client_uuid: Client's UUID
            
        Returns:
            Client data dict or None
        """
        if not self.client:
            return None
        
        try:
            client_key = f"client:{client_uuid}"
            data = self.client.hgetall(client_key)
            return data if data else None
        except Exception as e:
            logger.error(f"Error getting client from Redis: {e}")
            return None
    
    def get_client_by_token(self, token: str) -> Optional[str]:
        """
        Get client UUID by token.
        
        Args:
            token: JWT token
            
        Returns:
            Client UUID or None
        """
        if not self.client:
            return None
        
        try:
            token_key = f"token:{token}"
            return self.client.get(token_key)
        except Exception as e:
            logger.error(f"Error getting client by token: {e}")
            return None
    
    def update_client_metadata(self, client_uuid: str, metadata: Dict) -> bool:
        """
        Update client metadata.
        
        Args:
            client_uuid: Client's UUID
            metadata: Metadata dictionary
            
        Returns:
            True if successful
        """
        if not self.client:
            return False
        
        try:
            metadata_key = f"client:{client_uuid}:metadata"
            self.client.set(metadata_key, json.dumps(metadata))
            return True
        except Exception as e:
            logger.error(f"Error updating client metadata: {e}")
            return False
    
    def get_client_metadata(self, client_uuid: str) -> Optional[Dict]:
        """
        Get client metadata.
        
        Args:
            client_uuid: Client's UUID
            
        Returns:
            Metadata dictionary or None
        """
        if not self.client:
            return None
        
        try:
            metadata_key = f"client:{client_uuid}:metadata"
            data = self.client.get(metadata_key)
            return json.loads(data) if data else None
        except Exception as e:
            logger.error(f"Error getting client metadata: {e}")
            return None
    
    # Connection State Management
    
    def set_client_connected(self, client_uuid: str, client_name: str) -> bool:
        """
        Mark a client as connected.
        
        Args:
            client_uuid: Client's UUID
            client_name: Client's name
            
        Returns:
            True if successful
        """
        if not self.client:
            return False
        
        try:
            self.client.hset("connected_clients", client_uuid, client_name)
            logger.info(f"Client marked connected: {client_name} ({client_uuid})")
            return True
        except Exception as e:
            logger.error(f"Error marking client connected: {e}")
            return False
    
    def set_client_disconnected(self, client_uuid: str) -> bool:
        """
        Mark a client as disconnected.
        
        Args:
            client_uuid: Client's UUID
            
        Returns:
            True if successful
        """
        if not self.client:
            return False
        
        try:
            self.client.hdel("connected_clients", client_uuid)
            logger.info(f"Client marked disconnected: {client_uuid}")
            return True
        except Exception as e:
            logger.error(f"Error marking client disconnected: {e}")
            return False
    
    def get_connected_clients(self) -> Dict[str, str]:
        """
        Get all connected clients.
        
        Returns:
            Dictionary mapping UUID to client name
        """
        if not self.client:
            return {}
        
        try:
            return self.client.hgetall("connected_clients")
        except Exception as e:
            logger.error(f"Error getting connected clients: {e}")
            return {}
    
    def is_client_connected(self, client_uuid: str) -> bool:
        """
        Check if a client is connected.

        Args:
            client_uuid: Client's UUID

        Returns:
            True if connected
        """
        if not self.client:
            return False

        try:
            return self.client.hexists("connected_clients", client_uuid)
        except Exception as e:
            logger.error(f"Error checking client connection: {e}")
            return False

    # Message Queue and Delivery Tracking

    def enqueue_offline_message(self, recipient_uuid: str, message: Dict) -> bool:
        """Persist a message for an offline recipient."""
        if not message:
            return False

        if not self.client:
            self._offline_queues[recipient_uuid].append(message.copy())
            return True

        try:
            queue_key = f"offline:{recipient_uuid}"
            self.client.rpush(queue_key, json.dumps(message))
            return True
        except Exception as e:
            logger.error(f"Error queueing offline message: {e}")
            return False

    def pop_offline_messages(self, recipient_uuid: str) -> List[Dict]:
        """Retrieve and remove queued messages for a recipient."""
        if not self.client:
            messages = self._offline_queues.pop(recipient_uuid, [])
            return [msg.copy() for msg in messages]

        queue_key = f"offline:{recipient_uuid}"
        try:
            messages = self.client.lrange(queue_key, 0, -1)
            if messages:
                self.client.delete(queue_key)
            return [json.loads(item) for item in messages]
        except Exception as e:
            logger.error(f"Error retrieving offline messages: {e}")
            return []

    def record_delivery_attempt(self, msgid: str, metadata: Dict[str, Any]) -> None:
        """Record metadata for a delivery attempt."""
        metadata = metadata.copy()
        metadata.setdefault("msgid", msgid)
        metadata.setdefault("attempts", 0)
        metadata.setdefault("status", "created")
        metadata.setdefault("last_update", time.time())

        if not self.client:
            self._delivery_status[msgid] = metadata
            recipient = metadata.get("recipient")
            sender = metadata.get("sender")
            if recipient:
                self._recipient_index[recipient].add(msgid)
            if sender:
                self._sender_index[sender].add(msgid)
            return

        key = f"delivery:{msgid}"
        try:
            self.client.hset(key, mapping={k: json.dumps(v) if isinstance(v, (dict, list)) else v for k, v in metadata.items()})
            recipient = metadata.get("recipient")
            sender = metadata.get("sender")
            if recipient:
                self.client.sadd(f"recipient:{recipient}:messages", msgid)
            if sender:
                self.client.sadd(f"sender:{sender}:messages", msgid)
        except Exception as e:
            logger.error(f"Error recording delivery attempt: {e}")

    def update_delivery_status(self, msgid: str, status: str, **fields: Any) -> None:
        """Update the status and metadata of a delivery."""
        update = {"status": status, "last_update": time.time()}
        update.update(fields)

        if not self.client:
            current = self._delivery_status.get(msgid, {})
            current.update(update)
            self._delivery_status[msgid] = current
            if status in {"delivered", "failed"}:
                recipient = current.get("recipient")
                if recipient and msgid in self._recipient_index.get(recipient, set()):
                    self._recipient_index[recipient].discard(msgid)
            return

        key = f"delivery:{msgid}"
        try:
            mapping = {k: json.dumps(v) if isinstance(v, (dict, list)) else v for k, v in update.items()}
            self.client.hset(key, mapping=mapping)

            if status in {"delivered", "failed"}:
                data = self.client.hgetall(key)
                recipient = data.get("recipient")
                if recipient:
                    self.client.srem(f"recipient:{recipient}:messages", msgid)
        except Exception as e:
            logger.error(f"Error updating delivery status: {e}")

    def get_delivery_status(self, msgid: str) -> Optional[Dict[str, Any]]:
        """Fetch delivery metadata for a message."""
        if not self.client:
            record = self._delivery_status.get(msgid)
            return record.copy() if record else None

        key = f"delivery:{msgid}"
        try:
            data = self.client.hgetall(key)
            if not data:
                return None
            result: Dict[str, Any] = {}
            for k, v in data.items():
                try:
                    result[k] = json.loads(v)
                except (TypeError, json.JSONDecodeError):
                    result[k] = v
            return result
        except Exception as e:
            logger.error(f"Error retrieving delivery status: {e}")
            return None

    def list_undelivered_messages(self, client_uuid: str) -> List[Dict[str, Any]]:
        """List delivery records that are not yet completed for a client."""
        messages: List[Dict[str, Any]] = []

        if not self.client:
            msgids = list(self._recipient_index.get(client_uuid, set()))
            for msgid in msgids:
                record = self._delivery_status.get(msgid)
                if record and record.get("status") not in {"delivered", "failed"}:
                    messages.append(record.copy())
            return messages

        set_key = f"recipient:{client_uuid}:messages"
        try:
            msgids = self.client.smembers(set_key)
            for msgid in msgids:
                record = self.get_delivery_status(msgid)
                if record and record.get("status") not in {"delivered", "failed"}:
                    messages.append(record)
            return messages
        except Exception as e:
            logger.error(f"Error listing undelivered messages: {e}")
            return []

    def get_delivery_metrics(self) -> Dict[str, int]:
        """Aggregate delivery metrics for monitoring."""
        metrics = {"total": 0, "delivered": 0, "pending": 0, "queued": 0, "failed": 0}

        if not self.client:
            for record in self._delivery_status.values():
                metrics["total"] += 1
                status = record.get("status", "pending")
                if status in {"delivered", "failed", "queued", "pending", "pending_ack", "retrying"}:
                    if status == "delivered":
                        metrics["delivered"] += 1
                    elif status == "failed":
                        metrics["failed"] += 1
                    elif status in {"queued", "queued_offline"}:
                        metrics["queued"] += 1
                    else:
                        metrics["pending"] += 1
                else:
                    metrics["pending"] += 1
            return metrics

        try:
            for key in self.client.scan_iter(match="delivery:*"):
                metrics["total"] += 1
                record = self.get_delivery_status(key.split(":", 1)[1])
                if not record:
                    continue
                status = record.get("status", "pending")
                if status == "delivered":
                    metrics["delivered"] += 1
                elif status == "failed":
                    metrics["failed"] += 1
                elif status in {"queued", "queued_offline"}:
                    metrics["queued"] += 1
                else:
                    metrics["pending"] += 1
            return metrics
        except Exception as e:
            logger.error(f"Error calculating delivery metrics: {e}")
            return metrics
    
    # Topic/Room Management
    
    def create_topic(self, topic_id: str, creator_uuid: str, metadata: Optional[Dict] = None) -> bool:
        """
        Create a new topic/room.
        
        Args:
            topic_id: Topic identifier
            creator_uuid: UUID of the creator
            metadata: Optional metadata dictionary
            
        Returns:
            True if successful
        """
        if not self.client:
            return False
        
        try:
            topic_key = f"topic:{topic_id}"
            topic_data = {
                "id": topic_id,
                "creator": creator_uuid,
                "metadata": json.dumps(metadata or {})
            }
            self.client.hset(topic_key, mapping=topic_data)
            
            # Add to topics set
            self.client.sadd("topics", topic_id)
            
            logger.info(f"Topic created: {topic_id} by {creator_uuid}")
            return True
        except Exception as e:
            logger.error(f"Error creating topic: {e}")
            return False
    
    def get_topic(self, topic_id: str) -> Optional[Dict]:
        """
        Get topic data.
        
        Args:
            topic_id: Topic identifier
            
        Returns:
            Topic data dictionary or None
        """
        if not self.client:
            return None
        
        try:
            topic_key = f"topic:{topic_id}"
            data = self.client.hgetall(topic_key)
            if data and "metadata" in data:
                data["metadata"] = json.loads(data["metadata"])
            return data if data else None
        except Exception as e:
            logger.error(f"Error getting topic: {e}")
            return None
    
    def list_topics(self) -> List[str]:
        """
        List all available topics.
        
        Returns:
            List of topic IDs
        """
        if not self.client:
            return []
        
        try:
            topics = self.client.smembers("topics")
            return list(topics) if topics else []
        except Exception as e:
            logger.error(f"Error listing topics: {e}")
            return []
    
    def delete_topic(self, topic_id: str) -> bool:
        """
        Delete a topic.
        
        Args:
            topic_id: Topic identifier
            
        Returns:
            True if successful
        """
        if not self.client:
            return False
        
        try:
            topic_key = f"topic:{topic_id}"
            subscribers_key = f"topic:{topic_id}:subscribers"
            
            # Delete topic data
            self.client.delete(topic_key)
            # Delete subscribers
            self.client.delete(subscribers_key)
            # Remove from topics set
            self.client.srem("topics", topic_id)
            
            logger.info(f"Topic deleted: {topic_id}")
            return True
        except Exception as e:
            logger.error(f"Error deleting topic: {e}")
            return False
    
    def subscribe_to_topic(self, topic_id: str, client_uuid: str) -> bool:
        """
        Subscribe a client to a topic.
        
        Args:
            topic_id: Topic identifier
            client_uuid: Client's UUID
            
        Returns:
            True if successful
        """
        if not self.client:
            return False
        
        try:
            subscribers_key = f"topic:{topic_id}:subscribers"
            self.client.sadd(subscribers_key, client_uuid)
            
            # Add to client's subscriptions
            client_topics_key = f"client:{client_uuid}:topics"
            self.client.sadd(client_topics_key, topic_id)
            
            logger.info(f"Client {client_uuid} subscribed to topic {topic_id}")
            return True
        except Exception as e:
            logger.error(f"Error subscribing to topic: {e}")
            return False
    
    def unsubscribe_from_topic(self, topic_id: str, client_uuid: str) -> bool:
        """
        Unsubscribe a client from a topic.
        
        Args:
            topic_id: Topic identifier
            client_uuid: Client's UUID
            
        Returns:
            True if successful
        """
        if not self.client:
            return False
        
        try:
            subscribers_key = f"topic:{topic_id}:subscribers"
            self.client.srem(subscribers_key, client_uuid)
            
            # Remove from client's subscriptions
            client_topics_key = f"client:{client_uuid}:topics"
            self.client.srem(client_topics_key, topic_id)
            
            logger.info(f"Client {client_uuid} unsubscribed from topic {topic_id}")
            return True
        except Exception as e:
            logger.error(f"Error unsubscribing from topic: {e}")
            return False
    
    def get_topic_subscribers(self, topic_id: str) -> Set[str]:
        """
        Get all subscribers to a topic.
        
        Args:
            topic_id: Topic identifier
            
        Returns:
            Set of client UUIDs
        """
        if not self.client:
            return set()
        
        try:
            subscribers_key = f"topic:{topic_id}:subscribers"
            subscribers = self.client.smembers(subscribers_key)
            return set(subscribers) if subscribers else set()
        except Exception as e:
            logger.error(f"Error getting topic subscribers: {e}")
            return set()
    
    def get_client_topics(self, client_uuid: str) -> Set[str]:
        """
        Get all topics a client is subscribed to.
        
        Args:
            client_uuid: Client's UUID
            
        Returns:
            Set of topic IDs
        """
        if not self.client:
            return set()
        
        try:
            client_topics_key = f"client:{client_uuid}:topics"
            topics = self.client.smembers(client_topics_key)
            return set(topics) if topics else set()
        except Exception as e:
            logger.error(f"Error getting client topics: {e}")
            return set()
    
    def cleanup_client(self, client_uuid: str) -> bool:
        """
        Clean up all client data on disconnect.
        
        Args:
            client_uuid: Client's UUID
            
        Returns:
            True if successful
        """
        if not self.client:
            return False
        
        try:
            # Get client's topics
            topics = self.get_client_topics(client_uuid)
            
            # Unsubscribe from all topics
            for topic_id in topics:
                self.unsubscribe_from_topic(topic_id, client_uuid)
            
            # Mark as disconnected
            self.set_client_disconnected(client_uuid)
            
            logger.info(f"Client cleanup completed: {client_uuid}")
            return True
        except Exception as e:
            logger.error(f"Error cleaning up client: {e}")
            return False


# Global Redis store instance
redis_store = RedisStore()
