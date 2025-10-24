# Quick Start Guide

## Starting the Server

1. Install dependencies:
```bash
pip install -r requirements.txt
```

2. Start the server:
```bash
python main.py
```

The server will be available at `http://localhost:8000`

## Running the Demo

To see the server in action with two clients communicating:

```bash
python example_client.py demo
```

This will:
1. Register two clients (Alice and Bob)
2. Connect them via WebSocket
3. Exchange messages between them
4. Show all the message routing in action

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

Response includes a JWT token to use for WebSocket connection.

### Connect to WebSocket

Use the token from registration:
```
ws://localhost:8000/ws?token=<your-jwt-token>
```

### Send a Message

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

### JWT Token Expired

Tokens expire after 60 minutes. Re-register to get a new token.

### Recipient Not Connected

Ensure the recipient is connected before sending messages. Check `/clients` endpoint to see active connections.

## Environment Variables

Set these in a `.env` file or environment:

- `JWT_SECRET_KEY` - Secret key for JWT signing (default: "your-secret-key-change-in-production")

Example `.env`:
```
JWT_SECRET_KEY=my-super-secret-key-change-this-in-production
```

## Next Steps

- Customize message handling in `main.py`
- Add persistence by storing messages in a database
- Implement additional authentication methods
- Add rate limiting for production use
- Set up HTTPS/WSS for secure connections
