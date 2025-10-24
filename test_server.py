"""
Simple tests for the Swifty Server.
"""
import pytest
from uuid import uuid4
from models import ClientRegistration, Message, MessageAcknowledgment
from auth import create_access_token, verify_token
from connection_manager import ConnectionManager
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
    
    assert len(manager.get_connected_clients()) == 1
    assert manager.is_client_connected(client_uuid)
    
    # Disconnect client
    manager.disconnect(client_uuid)
    assert len(manager.get_connected_clients()) == 0
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


if __name__ == "__main__":
    # Run tests
    pytest.main([__file__, "-v"])
