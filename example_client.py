"""
Example WebSocket client for testing the Swifty Server.

This script demonstrates:
- Client registration
- JWT authentication
- WebSocket connection
- Sending and receiving messages
"""
import asyncio
import websockets
import json
import requests
from uuid import uuid4
import time
import sys


class SwiftyClient:
    """WebSocket client for Swifty Server."""
    
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
        self.access_token = None
        self.refresh_token = None
        self.ws = None
    
    def register(self):
        """Register with the server and obtain JWT token."""
        print(f"Registering as '{self.name}' with UUID {self.uuid}...")
        
        response = requests.post(
            f"{self.server_url}/register",
            json={
                "name": self.name,
                "uuid": str(self.uuid)
            }
        )
        
        if response.status_code == 200:
            data = response.json()
            self.access_token = data.get("access_token")
            self.refresh_token = data.get("refresh_token")
            print(f"✓ Registration successful!")
            if self.access_token:
                print(f"  Access token: {self.access_token[:50]}...")
            if self.refresh_token:
                print(f"  Refresh token: {self.refresh_token[:50]}...")
            return True
        else:
            print(f"✗ Registration failed: {response.text}")
            return False
    
    async def connect(self):
        """Connect to WebSocket endpoint."""
        if not self.access_token:
            print("✗ No token available. Please register first.")
            return False

        ws_url = self.server_url.replace('http://', 'ws://').replace('https://', 'wss://')
        uri = f"{ws_url}/ws?token={self.access_token}"
        
        print(f"Connecting to WebSocket...")
        try:
            self.ws = await websockets.connect(uri)
            print(f"✓ Connected to WebSocket!")
            return True
        except Exception as e:
            print(f"✗ Connection failed: {e}")
            return False
    
    async def receive_messages(self):
        """Listen for incoming messages."""
        try:
            async for message in self.ws:
                data = json.loads(message)
                print(f"\n📨 Received: {json.dumps(data, indent=2)}")
        except websockets.exceptions.ConnectionClosed:
            print("\n✗ Connection closed")
        except Exception as e:
            print(f"\n✗ Error receiving messages: {e}")
    
    async def send_message(self, to_uuid: str, content: str, **kwargs):
        """
        Send a message to another client.
        
        Args:
            to_uuid: Recipient's UUID
            content: Message content
            **kwargs: Additional message fields
        """
        message = {
            "to": to_uuid,
            "from": str(self.uuid),
            "timestamp": time.time(),
            "priority": kwargs.get("priority", "normal"),
            "subject": kwargs.get("subject", ""),
            "msgid": str(uuid4()),
            "acknowledge": kwargs.get("acknowledge", False),
            "content": content,
            "action": kwargs.get("action", "message"),
            "event": kwargs.get("event", ""),
            "status": kwargs.get("status", "sent"),
            "conversation_id": kwargs.get("conversation_id", ""),
            "msgno": kwargs.get("msgno", 1)
        }
        
        print(f"\n📤 Sending message to {to_uuid}...")
        await self.ws.send(json.dumps(message))
    
    async def run_interactive(self):
        """Run client in interactive mode."""
        if not await self.connect():
            return
        
        # Start listening for messages in background
        receive_task = asyncio.create_task(self.receive_messages())
        
        print("\n" + "="*60)
        print("Interactive mode - Commands:")
        print("  send <uuid> <message>  - Send a message")
        print("  quit                   - Disconnect and exit")
        print("="*60 + "\n")
        
        try:
            while True:
                # Get user input (in a real async app, use aioconsole)
                command = await asyncio.get_event_loop().run_in_executor(
                    None, input, "Command: "
                )
                
                if command.lower() == "quit":
                    break
                elif command.startswith("send "):
                    parts = command.split(maxsplit=2)
                    if len(parts) == 3:
                        _, to_uuid, content = parts
                        await self.send_message(to_uuid, content)
                    else:
                        print("Usage: send <uuid> <message>")
                else:
                    print("Unknown command")
        except KeyboardInterrupt:
            print("\nInterrupted")
        finally:
            receive_task.cancel()
            await self.ws.close()
            print("Disconnected")


async def demo_two_clients():
    """Demonstrate communication between two clients."""
    print("="*60)
    print("DEMO: Two clients communicating")
    print("="*60 + "\n")
    
    server_url = "http://localhost:8000"
    
    # Create two clients
    client1 = SwiftyClient(server_url, "Alice")
    client2 = SwiftyClient(server_url, "Bob")
    
    # Register both clients
    if not client1.register() or not client2.register():
        print("Failed to register clients")
        return
    
    print()
    
    # Connect both clients
    if not await client1.connect() or not await client2.connect():
        print("Failed to connect clients")
        return
    
    print()
    
    # Start listening for messages on both clients
    task1 = asyncio.create_task(client1.receive_messages())
    task2 = asyncio.create_task(client2.receive_messages())
    
    # Wait a bit for connections to establish
    await asyncio.sleep(1)
    
    # Alice sends a message to Bob
    print("\n" + "="*60)
    print("Alice sends message to Bob...")
    print("="*60)
    await client1.send_message(
        str(client2.uuid),
        "Hello Bob! This is Alice.",
        subject="Greeting",
        priority="high",
        acknowledge=True
    )
    
    await asyncio.sleep(2)
    
    # Bob sends a message to Alice
    print("\n" + "="*60)
    print("Bob sends message to Alice...")
    print("="*60)
    await client2.send_message(
        str(client1.uuid),
        "Hi Alice! Nice to hear from you.",
        subject="Reply",
        priority="normal"
    )
    
    await asyncio.sleep(2)
    
    # Cleanup
    task1.cancel()
    task2.cancel()
    await client1.ws.close()
    await client2.ws.close()
    
    print("\n" + "="*60)
    print("Demo completed!")
    print("="*60)


if __name__ == "__main__":
    if len(sys.argv) > 1:
        if sys.argv[1] == "demo":
            # Run demo
            asyncio.run(demo_two_clients())
        elif sys.argv[1] == "interactive":
            # Run interactive mode
            name = input("Enter your name: ")
            client = SwiftyClient("http://localhost:8000", name)
            if client.register():
                asyncio.run(client.run_interactive())
        else:
            print("Usage:")
            print("  python example_client.py demo         - Run demo with two clients")
            print("  python example_client.py interactive  - Run in interactive mode")
    else:
        print("Usage:")
        print("  python example_client.py demo         - Run demo with two clients")
        print("  python example_client.py interactive  - Run in interactive mode")
