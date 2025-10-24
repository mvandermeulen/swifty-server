# Swifty Server

A FastAPI + WebSocket & JWT server providing real-time communications for helper devices. Each client registers with a name and UUID for addressable messaging.

## Features

- **Client Registration**: Register clients with a name and UUID
- **JWT Authentication**: Secure token-based authentication
- **WebSocket Communication**: Real-time bidirectional messaging
- **Message Routing**: Route messages between clients using UUIDs
- **Message Acknowledgment**: Support for message acknowledgment tracking
- **Connection Management**: Track active clients and their connections

## Message Format

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

3. (Optional) Set up environment variables:
```bash
# Create .env file
echo "JWT_SECRET_KEY=your-secret-key-here" > .env
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

#### 4. WebSocket Connection
```
WS /ws?token={jwt-token}
```

Connect to the WebSocket endpoint using the JWT token obtained from registration.

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
3. **auth.py** - JWT token generation and verification
4. **connection_manager.py** - WebSocket connection management and message routing
5. **example_client.py** - Example client implementation

### Message Flow

1. Client registers via `/register` and receives JWT token
2. Client connects to WebSocket endpoint `/ws` with token
3. Server validates token and accepts connection
4. Client sends messages with recipient UUID
5. Server routes messages to recipient by UUID
6. Both sender and recipient receive confirmations

### Security

- JWT tokens for authentication
- Token expiration (60 minutes by default)
- Sender UUID verification (prevents impersonation)
- WebSocket connection tied to authenticated identity

## Configuration

Set environment variables or modify defaults in `auth.py`:

- `JWT_SECRET_KEY` - Secret key for JWT signing (default: "your-secret-key-change-in-production")
- `ACCESS_TOKEN_EXPIRE_MINUTES` - Token expiration time (default: 60)

## Development

### Project Structure

```
swifty-server/
├── main.py                 # FastAPI application
├── models.py              # Data models
├── auth.py                # JWT authentication
├── connection_manager.py  # WebSocket manager
├── example_client.py      # Example client
├── requirements.txt       # Dependencies
├── .gitignore            # Git ignore rules
└── README.md             # Documentation
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
