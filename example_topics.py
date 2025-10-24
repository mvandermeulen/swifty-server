"""
Example script demonstrating topic/room functionality.

This script shows:
- Creating topics/rooms
- Subscribing to topics
- Sending messages to topics
- Multiple clients in a room
"""
import asyncio
import websockets
import json
import requests
from uuid import uuid4
import time
import sys


class TopicClient:
    """WebSocket client for testing topic/room features."""
    
    def __init__(self, server_url: str, name: str):
        """
        Initialize client.
        
        Args:
            server_url: Server URL (e.g., 'http://localhost:8000')
            name: Client name
        """
        self.server_url = server_url.rstrip('/')
        self.name = name
        self.uuid = uuid4()
        self.token = None
        self.ws = None
    
    def register(self):
        """Register with the server and obtain JWT token."""
        print(f"[{self.name}] Registering with UUID {self.uuid}...")
        
        response = requests.post(
            f"{self.server_url}/register",
            json={
                "name": self.name,
                "uuid": str(self.uuid)
            }
        )
        
        if response.status_code == 200:
            data = response.json()
            self.token = data["token"]
            print(f"[{self.name}] ✓ Registration successful!")
            return True
        else:
            print(f"[{self.name}] ✗ Registration failed: {response.text}")
            return False
    
    def create_topic(self, topic_id: str, metadata: dict = None):
        """Create a new topic/room."""
        print(f"[{self.name}] Creating topic: {topic_id}...")
        
        response = requests.post(
            f"{self.server_url}/topics/create",
            params={"token": self.token},
            json={
                "topic_id": topic_id,
                "metadata": metadata or {}
            }
        )
        
        if response.status_code == 200:
            print(f"[{self.name}] ✓ Topic created: {topic_id}")
            return True
        else:
            print(f"[{self.name}] ✗ Failed to create topic: {response.text}")
            return False
    
    def list_topics(self):
        """List all available topics."""
        response = requests.get(f"{self.server_url}/topics")
        
        if response.status_code == 200:
            data = response.json()
            print(f"\n📋 Available Topics ({data['count']}):")
            for topic in data['topics']:
                print(f"  - {topic['id']} (subscribers: {topic['subscriber_count']})")
            return data['topics']
        else:
            print(f"✗ Failed to list topics: {response.text}")
            return []
    
    def subscribe_to_topic(self, topic_id: str):
        """Subscribe to a topic."""
        print(f"[{self.name}] Subscribing to topic: {topic_id}...")
        
        response = requests.post(
            f"{self.server_url}/topics/subscribe",
            params={"token": self.token},
            json={"topic_id": topic_id}
        )
        
        if response.status_code == 200:
            print(f"[{self.name}] ✓ Subscribed to: {topic_id}")
            return True
        else:
            print(f"[{self.name}] ✗ Failed to subscribe: {response.text}")
            return False
    
    def get_topic_info(self, topic_id: str):
        """Get information about a topic."""
        response = requests.get(f"{self.server_url}/topics/{topic_id}")
        
        if response.status_code == 200:
            data = response.json()
            print(f"\n📊 Topic Info: {topic_id}")
            print(f"  Creator: {data['creator']}")
            print(f"  Subscribers: {data['subscriber_count']}")
            print(f"  Metadata: {data['metadata']}")
            return data
        else:
            print(f"✗ Failed to get topic info: {response.text}")
            return None
    
    async def connect(self):
        """Connect to WebSocket endpoint."""
        if not self.token:
            print(f"[{self.name}] ✗ No token available. Please register first.")
            return False
        
        ws_url = self.server_url.replace('http://', 'ws://').replace('https://', 'wss://')
        uri = f"{ws_url}/ws?token={self.token}"
        
        print(f"[{self.name}] Connecting to WebSocket...")
        try:
            self.ws = await websockets.connect(uri)
            print(f"[{self.name}] ✓ Connected to WebSocket!")
            return True
        except Exception as e:
            print(f"[{self.name}] ✗ Connection failed: {e}")
            return False
    
    async def receive_messages(self):
        """Listen for incoming messages."""
        try:
            async for message in self.ws:
                data = json.loads(message)
                msg_type = data.get("type", "message")
                
                if msg_type == "connection":
                    print(f"[{self.name}] 🔗 Connected")
                elif msg_type == "topic_sent":
                    print(f"[{self.name}] ✓ Topic message sent to {data.get('message', 'N/A')}")
                elif "topic_id" in data:
                    print(f"[{self.name}] 📨 Topic message from {data.get('from', 'unknown')}: {data.get('content', '')}")
                else:
                    print(f"[{self.name}] 📨 Message: {data.get('content', data)}")
        except websockets.exceptions.ConnectionClosed:
            print(f"[{self.name}] ✗ Connection closed")
        except Exception as e:
            print(f"[{self.name}] ✗ Error receiving messages: {e}")
    
    async def send_topic_message(self, topic_id: str, content: str, **kwargs):
        """
        Send a message to a topic.
        
        Args:
            topic_id: Topic identifier
            content: Message content
            **kwargs: Additional message fields
        """
        message = {
            "topic_id": topic_id,
            "from": str(self.uuid),
            "timestamp": time.time(),
            "priority": kwargs.get("priority", "normal"),
            "subject": kwargs.get("subject", ""),
            "msgid": str(uuid4()),
            "content": content,
            "action": "topic_message",
            "event": kwargs.get("event", ""),
            "status": "sent",
            "msgno": kwargs.get("msgno", 1)
        }
        
        print(f"[{self.name}] 📤 Sending to topic {topic_id}: {content}")
        await self.ws.send(json.dumps(message))


async def demo_topic_messaging():
    """Demonstrate topic-based messaging with multiple clients."""
    print("="*70)
    print("DEMO: Topic/Room Messaging")
    print("="*70 + "\n")
    
    server_url = "http://localhost:8000"
    
    # Create three clients
    alice = TopicClient(server_url, "Alice")
    bob = TopicClient(server_url, "Bob")
    charlie = TopicClient(server_url, "Charlie")
    
    # Register all clients
    print("📝 Registering clients...\n")
    if not (alice.register() and bob.register() and charlie.register()):
        print("Failed to register clients")
        return
    
    print("\n" + "="*70)
    print("Step 1: Alice creates a topic")
    print("="*70 + "\n")
    
    # Alice creates a topic
    topic_id = "demo-room"
    alice.create_topic(topic_id, {"description": "Demo room for testing"})
    await asyncio.sleep(1)
    
    # List topics
    alice.list_topics()
    
    print("\n" + "="*70)
    print("Step 2: All clients subscribe to the topic")
    print("="*70 + "\n")
    
    # All subscribe to the topic
    alice.subscribe_to_topic(topic_id)
    bob.subscribe_to_topic(topic_id)
    charlie.subscribe_to_topic(topic_id)
    await asyncio.sleep(1)
    
    # Get topic info
    alice.get_topic_info(topic_id)
    
    print("\n" + "="*70)
    print("Step 3: Connect to WebSocket")
    print("="*70 + "\n")
    
    # Connect all clients
    if not (await alice.connect() and await bob.connect() and await charlie.connect()):
        print("Failed to connect clients")
        return
    
    # Start listening for messages
    task_alice = asyncio.create_task(alice.receive_messages())
    task_bob = asyncio.create_task(bob.receive_messages())
    task_charlie = asyncio.create_task(charlie.receive_messages())
    
    await asyncio.sleep(2)
    
    print("\n" + "="*70)
    print("Step 4: Send messages to the topic")
    print("="*70 + "\n")
    
    # Alice sends a message to the topic
    await alice.send_topic_message(
        topic_id,
        "Hello everyone! This is Alice speaking.",
        subject="Greeting"
    )
    await asyncio.sleep(2)
    
    # Bob replies
    await bob.send_topic_message(
        topic_id,
        "Hi Alice! Bob here.",
        subject="Reply"
    )
    await asyncio.sleep(2)
    
    # Charlie joins the conversation
    await charlie.send_topic_message(
        topic_id,
        "Hey team! Charlie checking in.",
        subject="Check-in"
    )
    await asyncio.sleep(2)
    
    # Another round
    await alice.send_topic_message(
        topic_id,
        "Great to have everyone here!",
        subject="Follow-up"
    )
    await asyncio.sleep(2)
    
    print("\n" + "="*70)
    print("Demo completed!")
    print("="*70 + "\n")
    
    # Cleanup
    task_alice.cancel()
    task_bob.cancel()
    task_charlie.cancel()
    await alice.ws.close()
    await bob.ws.close()
    await charlie.ws.close()


if __name__ == "__main__":
    print("\n🚀 Starting Topic Messaging Demo...\n")
    print("Make sure the server is running:")
    print("  python main.py\n")
    
    try:
        asyncio.run(demo_topic_messaging())
    except KeyboardInterrupt:
        print("\n\nDemo interrupted by user")
    except Exception as e:
        print(f"\n\nError running demo: {e}")
        import traceback
        traceback.print_exc()
