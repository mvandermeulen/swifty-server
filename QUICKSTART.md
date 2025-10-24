# Quick Start Guide

## Prerequisites

- Python 3.8+
- (Optional) Redis server for topic/room features

## Starting the Server

1. Install dependencies:
```bash
pip install -r requirements.txt
```

2. (Optional) Start Redis:
```bash
# Using Docker
docker run -d -p 6379:6379 redis:latest

# Or install locally
# Ubuntu/Debian: sudo apt-get install redis-server
# macOS: brew install redis
```

3. Start the server:
```bash
python main.py
```

The server will be available at `http://localhost:8000`

**Note**: Without Redis, direct messaging works but topic/room features are disabled.

## Running the Demos

### Demo 1: Direct Messaging

To see the server in action with two clients communicating:

```bash
python example_client.py demo
```

This will:
1. Register two clients (Alice and Bob)
2. Connect them via WebSocket
3. Exchange messages between them
4. Show all the message routing in action

### Demo 2: Topic/Room Messaging

To see topic-based messaging with multiple clients:

```bash
python example_topics.py
```

This will:
1. Register three clients (Alice, Bob, Charlie)
2. Create a topic/room
3. Subscribe all clients to the topic
4. Exchange messages in the room
5. Show broadcast messaging in action

**Note**: Requires Redis to be running.

## Interactive Client Mode

To run a client interactively:

```bash
python example_client.py interactive
```

Then:
1. Enter your name
2. Use commands like:
   - `send <recipient-uuid> <message>` - Send a message
   - `quit` - Exit

## API Quick Reference

### Register a Client

```bash
curl -X POST http://localhost:8000/register \
  -H "Content-Type: application/json" \
  -d '{"name": "MyClient", "uuid": "123e4567-e89b-12d3-a456-426614174000"}'
```

Response includes short-lived access and long-lived refresh tokens.

### Create a Topic/Room

```bash
ACCESS_TOKEN="your-access-token-here"
curl -X POST "http://localhost:8000/topics/create?token=$ACCESS_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"topic_id": "my-room", "metadata": {"description": "My room"}}'
```

Only access tokens with the `admin` role can create topics.

### List Topics

```bash
curl http://localhost:8000/topics
```

### Subscribe to Topic

```bash
ACCESS_TOKEN="your-access-token-here"
curl -X POST "http://localhost:8000/topics/subscribe?token=$ACCESS_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"topic_id": "my-room"}'
```

### Refresh Tokens

```bash
REFRESH_TOKEN="your-refresh-token-here"
curl -X POST http://localhost:8000/auth/refresh \
  -H "Content-Type: application/json" \
  -d '{"refresh_token": "'$REFRESH_TOKEN'"}'
```

Rotating refresh tokens also revokes the previous refresh token when rotation is enabled.

### Connect to WebSocket

Use the access token from registration:
```
ws://localhost:8000/ws?token=<your-access-token>
```

### Send a Direct Message

After connecting, send JSON messages:
```json
{
  "to": "recipient-uuid",
  "from": "your-uuid",
  "timestamp": 1234567890.123,
  "priority": "high",
  "subject": "Hello",
  "msgid": "message-uuid",
  "acknowledge": true,
  "content": "Hello World!",
  "action": "message",
  "event": "greeting",
  "status": "sent",
  "conversation_id": "conv-123",
  "msgno": 1
}
```

### Send a Topic Message

Send to a topic/room (must be subscribed):
```json
{
  "topic_id": "my-room",
  "from": "your-uuid",
  "timestamp": 1234567890.123,
  "priority": "normal",
  "subject": "Group Hello",
  "msgid": "message-uuid",
  "content": "Hello everyone!",
  "action": "topic_message",
  "event": "",
  "status": "sent",
  "msgno": 1
}
```

### Get Connected Clients

```bash
curl http://localhost:8000/clients
```

## Testing

Run the test suite:

```bash
python -m pytest test_server.py -v
```

## Common Issues

### Port Already in Use

If port 8000 is already in use, modify `main.py`:

```python
uvicorn.run(app, host="0.0.0.0", port=8001)  # Change port
```

### Access Token Expired

Access tokens expire quickly (15 minutes by default). Use the `/auth/refresh` endpoint with your refresh token to obtain a new access token.

### Recipient Not Connected

Ensure the recipient is connected before sending messages. Check `/clients` endpoint to see active connections.

### Redis Not Available

Topic/room features require Redis. Without Redis:
- ✅ Client registration and authentication works
- ✅ Direct WebSocket messaging works
- ❌ Topic/room features are disabled

Install and start Redis to enable full functionality.

## Environment Variables

Set these in a `.env` file or environment:

- `JWT_SECRET_KEY` - Secret key for JWT signing (required for consistent tokens; otherwise an ephemeral key is generated)
- `JWT_SECRET_FILE` - Path to file containing the JWT secret key (optional)
- `ACCESS_TOKEN_EXPIRE_MINUTES` - Access token lifetime in minutes (default: 15)
- `REFRESH_TOKEN_EXPIRE_MINUTES` - Refresh token lifetime in minutes (default: 1440)
- `TOKEN_ROTATION_ENABLED` - Enable refresh token rotation (default: true)
- `DEFAULT_CLIENT_ROLES` - Comma-separated list of default client roles (default: `user`)
- `ADMIN_CLIENT_IDS` - Comma-separated or JSON list of UUIDs that should receive the `admin` role
- `REDIS_HOST` - Redis host (default: "localhost")
- `REDIS_PORT` - Redis port (default: 6379)
- `REDIS_DB` - Redis database number (default: 0)
- `REDIS_PASSWORD` - Redis password (default: None)

Example `.env`:
```
JWT_SECRET_KEY=my-super-secret-key-change-this-in-production
REDIS_HOST=localhost
REDIS_PORT=6379
```

## Next Steps

- Customize message handling in `main.py`
- Implement additional authentication methods
- Add rate limiting for production use
- Set up HTTPS/WSS for secure connections
- Explore topic/room features for group messaging
- Add custom metadata to topics
- Implement topic permissions and access control
