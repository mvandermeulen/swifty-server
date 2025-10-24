"""
Data models for the WebSocket messaging server.
"""
from pydantic import BaseModel, Field, ConfigDict
from typing import Optional
from uuid import UUID
from datetime import datetime


class ClientRegistration(BaseModel):
    """Model for client registration."""
    name: str = Field(..., description="Client name")
    uuid: UUID = Field(..., description="Client UUID")


class Message(BaseModel):
    """Model for messages exchanged between clients."""
    model_config = ConfigDict(populate_by_name=True)
    
    to: UUID = Field(..., description="Recipient UUID")
    from_: UUID = Field(..., alias="from", description="Sender UUID")
    timestamp: float = Field(..., description="Message timestamp")
    priority: str = Field(..., description="Message priority")
    subject: str = Field(..., description="Message subject")
    msgid: UUID = Field(..., description="Message ID")
    acknowledge: bool = Field(..., description="Acknowledgment required")
    content: str = Field(..., description="Message content")
    action: str = Field(..., description="Action type")
    event: str = Field(..., description="Event type")
    status: str = Field(..., description="Message status")
    conversation_id: str = Field(..., description="Conversation ID")
    msgno: int = Field(..., description="Message number")


class MessageAcknowledgment(BaseModel):
    """Model for message acknowledgment."""
    model_config = ConfigDict(populate_by_name=True)

    type: str = Field(default="ack", alias="type", description="Message type identifier")
    msgid: UUID = Field(..., description="Message ID being acknowledged")
    from_: UUID = Field(..., alias="from", description="Sender UUID")
    timestamp: float = Field(..., description="Acknowledgment timestamp")
    status: str = Field(..., description="Acknowledgment status")


class TopicCreate(BaseModel):
    """Model for creating a new topic/room."""
    topic_id: str = Field(..., description="Topic identifier (routable)", min_length=1, max_length=100)
    metadata: Optional[dict] = Field(default={}, description="Optional topic metadata")


class TopicSubscribe(BaseModel):
    """Model for subscribing to a topic."""
    topic_id: str = Field(..., description="Topic identifier to subscribe to")


class TopicMessage(BaseModel):
    """Model for messages sent to a topic."""
    model_config = ConfigDict(populate_by_name=True)
    
    topic_id: str = Field(..., description="Target topic ID")
    from_: UUID = Field(..., alias="from", description="Sender UUID")
    timestamp: float = Field(..., description="Message timestamp")
    priority: str = Field(..., description="Message priority")
    subject: str = Field(..., description="Message subject")
    msgid: UUID = Field(..., description="Message ID")
    content: str = Field(..., description="Message content")
    action: str = Field(default="topic_message", description="Action type")
    event: str = Field(default="", description="Event type")
    status: str = Field(default="sent", description="Message status")
    msgno: int = Field(default=1, description="Message number")
