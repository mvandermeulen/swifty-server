# Swifty Server

A FastAPI + WebSocket & JWT server providing real-time communications for helper devices. Each client registers with a name and UUID for addressable messaging. Supports direct messaging and topic-based (room) messaging with Redis persistence.

## Features

- **Client Registration**: Register clients with a name and UUID
- **JWT Authentication**: Secure token-based authentication
- **WebSocket Communication**: Real-time bidirectional messaging
- **Message Routing**: Route messages between clients using UUIDs
- **Topic/Room Support**: Create and subscribe to topics for group messaging
- **Redis Integration**: Persistent storage for connections, tokens, and topics
- **Message Acknowledgment**: Support for message acknowledgment tracking
- **Connection Management**: Track active clients and their connections

## Message Format

### Direct Messages

Messages between clients follow this JSON structure:

```json
{
  "to": "recipient-uuid",
  "from": "sender-uuid",
  "timestamp": 1234567890.123,
  "priority": "high",
  "subject": "Message subject",
  "msgid": "message-uuid",
  "acknowledge": true,
  "content": "Message content",
  "action": "message",
  "event": "event-type",
  "status": "sent",
  "conversation_id": "conversation-id",
  "msgno": 1
}
```

### Topic Messages

Messages sent to a topic/room:

```json
{
  "topic_id": "my-topic",
  "from": "sender-uuid",
  "timestamp": 1234567890.123,
  "priority": "normal",
  "subject": "Topic message",
  "msgid": "message-uuid",
  "content": "Message content",
  "action": "topic_message",
  "event": "",
  "status": "sent",
  "msgno": 1
}
```

## Installation

1. Clone the repository:
```bash
git clone https://github.com/mvandermeulen/swifty-server.git
cd swifty-server
```

2. Install dependencies:
```bash
pip install -r requirements.txt
```

**Security Note**: The requirements.txt file specifies minimum secure versions of dependencies to avoid known vulnerabilities. Always use the latest stable versions when possible.

3. (Optional) Set up Redis:
```bash
# Using Docker
docker run -d -p 6379:6379 redis:latest

# Or install Redis locally
# Ubuntu/Debian: sudo apt-get install redis-server
# macOS: brew install redis
```

**Note**: The server will work without Redis but with limited functionality:
- ✅ Client registration and authentication
- ✅ Direct WebSocket messaging
- ❌ Topic/room features (require Redis)
- ❌ Persistent token storage
- ❌ Connection state across restarts

For full functionality including topics/rooms, Redis is required.

4. (Optional) Set up environment variables:
```bash
# Create .env file
echo "JWT_SECRET_KEY=your-secret-key-here" > .env
echo "REDIS_HOST=localhost" >> .env
echo "REDIS_PORT=6379" >> .env
```

## Usage

### Starting the Server

Run the server with uvicorn:

```bash
python main.py
```

Or:

```bash
uvicorn main:app --host 0.0.0.0 --port 8000 --reload
```

The server will start on `http://localhost:8000`

### API Endpoints

#### 1. Root Endpoint
```
GET /
```
Returns server information and available endpoints.

#### 2. Register Client
```
POST /register
```

**Request Body:**
```json
{
  "name": "Client Name",
  "uuid": "client-uuid"
}
```

**Response:**
```json
{
  "token": "jwt-token",
  "uuid": "client-uuid",
  "name": "Client Name",
  "message": "Registration successful"
}
```

#### 3. Get Connected Clients
```
GET /clients
```

**Response:**
```json
{
  "count": 2,
  "clients": {
    "uuid-1": "Alice",
    "uuid-2": "Bob"
  }
}
```

#### 4. Create Topic/Room
```
POST /topics/create?token={jwt-token}
```

**Request Body:**
```json
{
  "topic_id": "my-room",
  "metadata": {
    "description": "My awesome room"
  }
}
```

**Response:**
```json
{
  "message": "Topic created successfully",
  "topic_id": "my-room",
  "creator": "creator-uuid"
}
```

#### 5. List Topics
```
GET /topics
```

**Response:**
```json
{
  "count": 2,
  "topics": [
    {
      "id": "my-room",
      "creator": "creator-uuid",
      "metadata": {},
      "subscriber_count": 5
    }
  ]
}
```

#### 6. Get Topic Info
```
GET /topics/{topic_id}
```

**Response:**
```json
{
  "id": "my-room",
  "creator": "creator-uuid",
  "metadata": {},
  "subscriber_count": 5,
  "subscribers": ["uuid-1", "uuid-2"]
}
```

#### 7. Subscribe to Topic
```
POST /topics/subscribe?token={jwt-token}
```

**Request Body:**
```json
{
  "topic_id": "my-room"
}
```

#### 8. Unsubscribe from Topic
```
POST /topics/unsubscribe?token={jwt-token}
```

**Request Body:**
```json
{
  "topic_id": "my-room"
}
```

#### 9. WebSocket Connection
```
WS /ws?token={jwt-token}
```

Connect to the WebSocket endpoint using the JWT token obtained from registration.

**Sending Direct Messages:** Send a message with a `to` field containing the recipient UUID.

**Sending Topic Messages:** Send a message with a `topic_id` field to broadcast to all topic subscribers.

## Example Usage

### Python Client Example

The repository includes an example client (`example_client.py`) that demonstrates:

1. **Demo Mode** - Shows two clients communicating:
```bash
python example_client.py demo
```

2. **Interactive Mode** - Run a client interactively:
```bash
python example_client.py interactive
```

### Manual Testing with Python

```python
import asyncio
from example_client import SwiftyClient

async def main():
    # Create and register client
    client = SwiftyClient("http://localhost:8000", "TestClient")
    client.register()
    
    # Connect to WebSocket
    await client.connect()
    
    # Send a message
    await client.send_message(
        to_uuid="recipient-uuid",
        content="Hello!",
        subject="Test",
        priority="high"
    )

asyncio.run(main())
```

### Testing with cURL

1. Register a client:
```bash
curl -X POST http://localhost:8000/register \
  -H "Content-Type: application/json" \
  -d '{"name": "TestClient", "uuid": "123e4567-e89b-12d3-a456-426614174000"}'
```

2. Check connected clients:
```bash
curl http://localhost:8000/clients
```

## Architecture

### Components

1. **main.py** - FastAPI application with REST and WebSocket endpoints
2. **models.py** - Pydantic data models for validation
3. **auth.py** - JWT token generation and verification with Redis integration
4. **connection_manager.py** - WebSocket connection management and message routing
5. **redis_store.py** - Redis storage layer for persistence
6. **example_client.py** - Example client implementation

### Message Flow

#### Direct Messaging
1. Client registers via `/register` and receives JWT token
2. Client connects to WebSocket endpoint `/ws` with token
3. Server validates token and accepts connection
4. Client sends messages with recipient UUID
5. Server routes messages to recipient by UUID
6. Both sender and recipient receive confirmations

#### Topic/Room Messaging
1. Client creates a topic via `/topics/create`
2. Clients subscribe to topic via `/topics/subscribe`
3. Client sends message with `topic_id` field via WebSocket
4. Server broadcasts message to all subscribers
5. Sender receives confirmation with subscriber count

### Security

- JWT tokens for authentication
- Token expiration (60 minutes by default)
- Tokens stored in Redis with automatic expiration
- Sender UUID verification (prevents impersonation)
- WebSocket connection tied to authenticated identity
- Topic subscription verification before message delivery

## Configuration

Set environment variables or modify defaults in the respective modules:

### JWT Configuration (`auth.py`)
- `JWT_SECRET_KEY` - Secret key for JWT signing (default: "your-secret-key-change-in-production")
- `ACCESS_TOKEN_EXPIRE_MINUTES` - Token expiration time (default: 60)

### Redis Configuration (`redis_store.py`)
- `REDIS_HOST` - Redis host (default: "localhost")
- `REDIS_PORT` - Redis port (default: 6379)
- `REDIS_DB` - Redis database number (default: 0)
- `REDIS_PASSWORD` - Redis password (default: None)

**Note**: If Redis is unavailable, the server falls back to in-memory storage for some features.

## Development

### Project Structure

```
swifty-server/
├── main.py                 # FastAPI application
├── models.py              # Data models
├── auth.py                # JWT authentication
├── connection_manager.py  # WebSocket manager
├── redis_store.py         # Redis storage layer
├── example_client.py      # Example client
├── requirements.txt       # Dependencies
├── test_server.py         # Unit tests
├── .gitignore            # Git ignore rules
├── README.md             # Documentation
├── QUICKSTART.md         # Quick start guide
└── IMPLEMENTATION.md     # Implementation details
```

### Adding Features

To extend the server:

1. Add new endpoints in `main.py`
2. Define new models in `models.py`
3. Extend `ConnectionManager` for additional routing logic
4. Update `auth.py` for custom authentication

## Testing

Start the server and run the demo:

```bash
# Terminal 1: Start server
python main.py

# Terminal 2: Run demo
python example_client.py demo
```

## License

MIT License

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.
