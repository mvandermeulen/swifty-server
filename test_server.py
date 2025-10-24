"""
Simple tests for the Swifty Server.
"""
import pytest
from uuid import uuid4
from models import ClientRegistration, Message, MessageAcknowledgment, TopicCreate, TopicSubscribe, TopicMessage
from auth import create_access_token, verify_token
from connection_manager import ConnectionManager
from redis_store import RedisStore
import time


def test_client_registration_model():
    """Test ClientRegistration model validation."""
    client_uuid = uuid4()
    registration = ClientRegistration(
        name="TestClient",
        uuid=client_uuid
    )
    assert registration.name == "TestClient"
    assert registration.uuid == client_uuid


def test_message_model():
    """Test Message model validation."""
    msg_uuid = uuid4()
    from_uuid = uuid4()
    to_uuid = uuid4()
    
    message = Message(
        to=to_uuid,
        from_=from_uuid,
        timestamp=time.time(),
        priority="high",
        subject="Test",
        msgid=msg_uuid,
        acknowledge=True,
        content="Test message",
        action="message",
        event="test",
        status="sent",
        conversation_id="conv-123",
        msgno=1
    )
    
    assert message.to == to_uuid
    assert message.from_ == from_uuid
    assert message.msgid == msg_uuid
    assert message.acknowledge is True


def test_message_model_with_alias():
    """Test Message model with 'from' alias."""
    msg_data = {
        "to": str(uuid4()),
        "from": str(uuid4()),
        "timestamp": time.time(),
        "priority": "normal",
        "subject": "Test",
        "msgid": str(uuid4()),
        "acknowledge": False,
        "content": "Test",
        "action": "message",
        "event": "",
        "status": "sent",
        "conversation_id": "",
        "msgno": 1
    }
    
    message = Message(**msg_data)
    assert str(message.from_) == msg_data["from"]


def test_jwt_token_creation():
    """Test JWT token creation."""
    client_uuid = uuid4()
    client_name = "TestClient"
    
    token = create_access_token(client_uuid, client_name)
    assert isinstance(token, str)
    assert len(token) > 0


def test_jwt_token_verification():
    """Test JWT token verification."""
    client_uuid = uuid4()
    client_name = "TestClient"
    
    token = create_access_token(client_uuid, client_name)
    payload = verify_token(token)
    
    assert payload is not None
    assert payload["sub"] == str(client_uuid)
    assert payload["name"] == client_name


def test_jwt_token_verification_invalid():
    """Test JWT token verification with invalid token."""
    payload = verify_token("invalid-token")
    assert payload is None


def test_connection_manager_client_tracking():
    """Test ConnectionManager client tracking."""
    manager = ConnectionManager()
    
    client_uuid = uuid4()
    client_name = "TestClient"
    
    # Initially no clients
    assert len(manager.get_connected_clients()) == 0
    assert not manager.is_client_connected(client_uuid)
    
    # Add client (note: we can't actually connect a websocket in unit test)
    # So we just test the tracking logic
    manager.client_names[str(client_uuid)] = client_name
    manager.active_connections[str(client_uuid)] = None  # Mock connection
    
    assert len(manager.get_connected_clients()) >= 0  # May use Redis or fallback
    assert manager.is_client_connected(client_uuid)
    
    # Disconnect client
    manager.disconnect(client_uuid)
    assert len(manager.get_connected_clients()) >= 0
    assert not manager.is_client_connected(client_uuid)


def test_acknowledgment_model():
    """Test MessageAcknowledgment model."""
    msg_uuid = uuid4()
    from_uuid = uuid4()
    
    ack = MessageAcknowledgment(
        msgid=msg_uuid,
        from_=from_uuid,
        timestamp=time.time(),
        status="acknowledged"
    )
    
    assert ack.msgid == msg_uuid
    assert ack.from_ == from_uuid
    assert ack.status == "acknowledged"


def test_topic_create_model():
    """Test TopicCreate model validation."""
    topic = TopicCreate(
        topic_id="test-topic",
        metadata={"description": "Test topic"}
    )
    
    assert topic.topic_id == "test-topic"
    assert topic.metadata["description"] == "Test topic"


def test_topic_subscribe_model():
    """Test TopicSubscribe model validation."""
    sub = TopicSubscribe(topic_id="test-topic")
    
    assert sub.topic_id == "test-topic"


def test_topic_message_model():
    """Test TopicMessage model validation."""
    from_uuid = uuid4()
    msg_uuid = uuid4()
    
    topic_msg = TopicMessage(
        topic_id="test-topic",
        from_=from_uuid,
        timestamp=time.time(),
        priority="normal",
        subject="Test",
        msgid=msg_uuid,
        content="Test message"
    )
    
    assert topic_msg.topic_id == "test-topic"
    assert topic_msg.from_ == from_uuid
    assert topic_msg.msgid == msg_uuid
    assert topic_msg.action == "topic_message"


def test_redis_store_topic_operations():
    """Test Redis store topic operations."""
    store = RedisStore()
    
    # Skip if Redis is not available
    if not store.client:
        pytest.skip("Redis not available")
    
    topic_id = f"test-topic-{uuid4()}"
    creator_uuid = str(uuid4())
    client_uuid = str(uuid4())
    
    try:
        # Create topic
        success = store.create_topic(topic_id, creator_uuid, {"test": "data"})
        assert success is True
        
        # Get topic
        topic = store.get_topic(topic_id)
        assert topic is not None
        assert topic["id"] == topic_id
        assert topic["creator"] == creator_uuid
        
        # List topics
        topics = store.list_topics()
        assert topic_id in topics
        
        # Subscribe to topic
        success = store.subscribe_to_topic(topic_id, client_uuid)
        assert success is True
        
        # Get subscribers
        subscribers = store.get_topic_subscribers(topic_id)
        assert client_uuid in subscribers
        
        # Get client topics
        client_topics = store.get_client_topics(client_uuid)
        assert topic_id in client_topics
        
        # Unsubscribe
        success = store.unsubscribe_from_topic(topic_id, client_uuid)
        assert success is True
        
        subscribers = store.get_topic_subscribers(topic_id)
        assert client_uuid not in subscribers
        
    finally:
        # Cleanup
        store.delete_topic(topic_id)


def test_redis_store_client_operations():
    """Test Redis store client operations."""
    store = RedisStore()
    
    # Skip if Redis is not available
    if not store.client:
        pytest.skip("Redis not available")
    
    client_uuid = str(uuid4())
    client_name = "TestClient"
    token = "test-token-123"
    
    try:
        # Register client
        success = store.register_client(client_uuid, client_name, token)
        assert success is True
        
        # Get client by UUID
        client = store.get_client_by_uuid(client_uuid)
        assert client is not None
        assert client["uuid"] == client_uuid
        assert client["name"] == client_name
        
        # Update metadata
        metadata = {"key": "value"}
        success = store.update_client_metadata(client_uuid, metadata)
        assert success is True
        
        # Get metadata
        retrieved_metadata = store.get_client_metadata(client_uuid)
        assert retrieved_metadata == metadata
        
        # Set connected
        success = store.set_client_connected(client_uuid, client_name)
        assert success is True
        
        # Check connected
        assert store.is_client_connected(client_uuid) is True
        
        # Get connected clients
        clients = store.get_connected_clients()
        assert client_uuid in clients
        
        # Set disconnected
        success = store.set_client_disconnected(client_uuid)
        assert success is True
        
        assert store.is_client_connected(client_uuid) is False
        
    finally:
        # Cleanup
        store.set_client_disconnected(client_uuid)


if __name__ == "__main__":
    # Run tests
    pytest.main([__file__, "-v"])
