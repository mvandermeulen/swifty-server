"""Redis storage layer for WebSocket connections, client data, and topics."""
from __future__ import annotations

import json
import logging
import os
import time
from typing import Dict, List, Optional, Set
from uuid import UUID

import redis

from config import get_settings

logger = logging.getLogger(__name__)

# Redis configuration
REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))
REDIS_DB = int(os.getenv("REDIS_DB", "0"))
REDIS_PASSWORD = os.getenv("REDIS_PASSWORD", None)

_settings = get_settings()
TOKEN_META_PREFIX = f"{_settings.redis_token_prefix}_meta"
CLIENT_TOKEN_SET_PREFIX = "client_tokens"


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
                decode_responses=True,
            )
            # Test connection
            self.client.ping()
            logger.info(f"Redis connected: {REDIS_HOST}:{REDIS_PORT}")
        except redis.ConnectionError as e:
            logger.warning(f"Redis connection failed: {e}. Using in-memory fallback.")
            self.client = None

        # Fallback storage when Redis is unavailable
        self.fallback_tokens: Dict[str, Dict[str, str]] = {}
        self.fallback_token_lookup: Dict[str, str] = {}
        self.fallback_clients: Dict[str, Dict[str, str]] = {}
        self.fallback_topic_subscribers: Dict[str, Set[str]] = {}
        self.fallback_client_topics: Dict[str, Set[str]] = {}
    
    # Client Management
    
    def register_client(self, client_uuid: str, client_name: str, last_token_jti: Optional[str] = None) -> bool:
        """
        Register a client with their token.

        Args:
            client_uuid: Client's UUID
            client_name: Client's name
            last_token_jti: Last issued access token identifier

        Returns:
            True if successful
        """
        if not self.client:
            self.fallback_clients[client_uuid] = {
                "uuid": client_uuid,
                "name": client_name,
                "last_token_jti": last_token_jti or "",
            }
            return True

        try:
            client_key = f"client:{client_uuid}"
            client_data = {
                "uuid": client_uuid,
                "name": client_name,
            }
            if last_token_jti:
                client_data["last_token_jti"] = last_token_jti

            self.client.hset(client_key, mapping=client_data)

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
            return self.fallback_clients.get(client_uuid)

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
            jti = self.fallback_token_lookup.get(token)
            if not jti:
                return None
            record = self.fallback_tokens.get(jti)
            if not record:
                self.fallback_token_lookup.pop(token, None)
                return None
            if record["expires_at"] <= time.time():
                self.fallback_token_lookup.pop(token, None)
                self.fallback_tokens.pop(jti, None)
                return None
            return record.get("client_uuid")

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
            self.fallback_clients.setdefault(client_uuid, {"uuid": client_uuid}).update({"metadata": metadata})
            return True

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
            metadata = self.fallback_clients.get(client_uuid, {}).get("metadata")
            return metadata

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
            entry = self.fallback_clients.setdefault(client_uuid, {"uuid": client_uuid})
            entry["name"] = client_name
            entry["connected"] = True
            return True

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
            if client_uuid in self.fallback_clients:
                self.fallback_clients[client_uuid]["connected"] = False
                return True
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
            return {
                client_uuid: data.get("name", "Unknown")
                for client_uuid, data in self.fallback_clients.items()
                if data.get("connected")
            }

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
            return self.fallback_clients.get(client_uuid, {}).get("connected", False)

        try:
            return self.client.hexists("connected_clients", client_uuid)
        except Exception as e:
            logger.error(f"Error checking client connection: {e}")
            return False

    # Token management

    def store_token(self, jti: str, token: str, client_uuid: str, token_type: str, expires_in: int) -> bool:
        """Store a token record for validation and revocation."""

        if not self.client:
            expires_at = time.time() + expires_in
            self.fallback_tokens[jti] = {
                "jti": jti,
                "token": token,
                "client_uuid": client_uuid,
                "type": token_type,
                "expires_at": expires_at,
            }
            self.fallback_token_lookup[token] = jti
            return True

        token_key = f"token:{token}"
        meta_key = f"{TOKEN_META_PREFIX}:{jti}"
        client_tokens_key = f"{CLIENT_TOKEN_SET_PREFIX}:{client_uuid}"

        try:
            record = {
                "jti": jti,
                "token": token,
                "client_uuid": client_uuid,
                "type": token_type,
            }
            pipeline = self.client.pipeline()
            pipeline.setex(token_key, expires_in, client_uuid)
            pipeline.hset(meta_key, mapping=record)
            pipeline.expire(meta_key, expires_in)
            pipeline.sadd(client_tokens_key, jti)
            pipeline.execute()
            return True
        except Exception as exc:
            logger.error("Error storing token metadata: %s", exc)
            return False

    def get_token_record(self, jti: str) -> Optional[Dict[str, str]]:
        """Retrieve stored token metadata."""

        if not self.client:
            record = self.fallback_tokens.get(jti)
            if not record:
                return None
            if record["expires_at"] <= time.time():
                self.fallback_tokens.pop(jti, None)
                self.fallback_token_lookup.pop(record.get("token", ""), None)
                return None
            return record

        try:
            meta_key = f"{TOKEN_META_PREFIX}:{jti}"
            record = self.client.hgetall(meta_key)
            return record if record else None
        except Exception as exc:
            logger.error("Error retrieving token metadata: %s", exc)
            return None

    def is_token_active(self, jti: str, token: Optional[str] = None) -> bool:
        """Determine whether a token is still active (not revoked and not expired)."""

        record = self.get_token_record(jti)
        if not record:
            return False

        if token and record.get("token") != token:
            logger.warning("Token mismatch for jti %s", jti)
            return False

        if not self.client:
            if record["expires_at"] <= time.time():
                self.fallback_tokens.pop(jti, None)
                self.fallback_token_lookup.pop(record.get("token", ""), None)
                return False
            return True

        # Redis manages expiry; existence of metadata indicates active token
        return True

    def revoke_token(self, jti: str) -> bool:
        """Revoke a token by its identifier."""

        if not self.client:
            record = self.fallback_tokens.pop(jti, None)
            if not record:
                return False
            token = record.get("token")
            if token:
                self.fallback_token_lookup.pop(token, None)
            return True

        try:
            record = self.get_token_record(jti)
            if not record:
                return False

            token = record.get("token")
            client_uuid = record.get("client_uuid")

            pipeline = self.client.pipeline()
            meta_key = f"{TOKEN_META_PREFIX}:{jti}"
            pipeline.delete(meta_key)
            if token:
                pipeline.delete(f"token:{token}")
            if client_uuid:
                pipeline.srem(f"{CLIENT_TOKEN_SET_PREFIX}:{client_uuid}", jti)
            pipeline.execute()
            return True
        except Exception as exc:
            logger.error("Error revoking token %s: %s", jti, exc)
            return False

    def revoke_client_tokens(self, client_uuid: str, token_type: Optional[str] = None) -> int:
        """Revoke all tokens for a client, optionally filtered by token type."""

        if not self.client:
            revoked = [
                jti
                for jti, record in list(self.fallback_tokens.items())
                if record.get("client_uuid") == client_uuid
                and (not token_type or record.get("type") == token_type)
            ]
            for jti in revoked:
                record = self.fallback_tokens.pop(jti, None)
                if record and record.get("token"):
                    self.fallback_token_lookup.pop(record["token"], None)
            return len(revoked)

        try:
            tokens_key = f"{CLIENT_TOKEN_SET_PREFIX}:{client_uuid}"
            jtis = self.client.smembers(tokens_key) or []
            revoked_count = 0
            for token_jti in jtis:
                record = self.get_token_record(token_jti)
                if not record:
                    self.client.srem(tokens_key, token_jti)
                    continue
                if token_type and record.get("type") != token_type:
                    continue
                if self.revoke_token(token_jti):
                    revoked_count += 1
            return revoked_count
        except Exception as exc:
            logger.error("Error revoking tokens for client %s: %s", client_uuid, exc)
            return 0
    
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
            self.fallback_topic_subscribers.setdefault(topic_id, set()).add(client_uuid)
            self.fallback_client_topics.setdefault(client_uuid, set()).add(topic_id)
            return True

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
            self.fallback_topic_subscribers.get(topic_id, set()).discard(client_uuid)
            self.fallback_client_topics.get(client_uuid, set()).discard(topic_id)
            return True

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
            return set(self.fallback_topic_subscribers.get(topic_id, set()))
        
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
            return set(self.fallback_client_topics.get(client_uuid, set()))
        
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
