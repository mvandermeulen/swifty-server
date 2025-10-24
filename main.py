"""
FastAPI WebSocket Server for Real-Time Communications.

This server provides:
- Client registration with name and UUID
- JWT authentication
- WebSocket connections for real-time messaging
- Message routing between clients by UUID
"""
from fastapi import FastAPI, WebSocket, WebSocketDisconnect, HTTPException, Query
from fastapi.responses import JSONResponse
from typing import Optional
from uuid import UUID
import json
import time
import logging

from models import ClientRegistration, Message, MessageAcknowledgment
from auth import create_access_token, verify_token
from connection_manager import ConnectionManager

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Create FastAPI app
app = FastAPI(
    title="Swifty Server",
    description="FastAPI WebSocket Server for Real-Time Communications",
    version="1.0.0"
)

# Create connection manager
manager = ConnectionManager()


@app.get("/")
async def root():
    """Root endpoint."""
    return {
        "message": "Swifty Server - FastAPI WebSocket Server",
        "version": "1.0.0",
        "endpoints": {
            "register": "/register",
            "websocket": "/ws",
            "clients": "/clients"
        }
    }


@app.post("/register")
async def register_client(registration: ClientRegistration):
    """
    Register a client and receive a JWT token.
    
    Args:
        registration: Client registration data (name and UUID)
        
    Returns:
        JWT token for authentication
    """
    try:
        # Create JWT token
        token = create_access_token(registration.uuid, registration.name)
        
        logger.info(f"Client registered: {registration.name} ({registration.uuid})")
        
        return {
            "token": token,
            "uuid": str(registration.uuid),
            "name": registration.name,
            "message": "Registration successful"
        }
    except Exception as e:
        logger.error(f"Registration error: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/clients")
async def get_clients():
    """
    Get list of currently connected clients.
    
    Returns:
        Dictionary of connected clients (UUID -> name)
    """
    clients = manager.get_connected_clients()
    return {
        "count": len(clients),
        "clients": clients
    }


@app.websocket("/ws")
async def websocket_endpoint(
    websocket: WebSocket,
    token: str = Query(..., description="JWT authentication token")
):
    """
    WebSocket endpoint for real-time messaging.
    
    Clients must provide a valid JWT token obtained from /register.
    Messages are routed to recipients by their UUID.
    
    Args:
        websocket: WebSocket connection
        token: JWT authentication token
    """
    # Verify token
    payload = verify_token(token)
    if not payload:
        await websocket.close(code=1008, reason="Invalid token")
        return
    
    client_uuid = UUID(payload["sub"])
    client_name = payload["name"]
    
    # Connect client
    await manager.connect(websocket, client_uuid, client_name)
    
    try:
        # Send welcome message
        await websocket.send_json({
            "type": "connection",
            "message": "Connected successfully",
            "uuid": str(client_uuid),
            "name": client_name,
            "timestamp": time.time()
        })
        
        # Listen for messages
        while True:
            # Receive message
            data = await websocket.receive_text()
            
            try:
                message_data = json.loads(data)
                
                # Validate message structure
                try:
                    message = Message(**message_data)
                except Exception as validation_error:
                    await websocket.send_json({
                        "type": "error",
                        "message": f"Invalid message format: {validation_error}",
                        "timestamp": time.time()
                    })
                    continue
                
                # Verify sender UUID matches authenticated client
                if message.from_ != client_uuid:
                    await websocket.send_json({
                        "type": "error",
                        "message": "Sender UUID does not match authenticated client",
                        "timestamp": time.time()
                    })
                    continue
                
                # Route message to recipient
                success = await manager.send_message(
                    message_data,
                    message.to
                )
                
                # Send confirmation to sender
                if success:
                    await websocket.send_json({
                        "type": "sent",
                        "message": "Message delivered",
                        "msgid": str(message.msgid),
                        "timestamp": time.time()
                    })
                else:
                    await websocket.send_json({
                        "type": "error",
                        "message": f"Recipient {message.to} not connected",
                        "msgid": str(message.msgid),
                        "timestamp": time.time()
                    })
                
                # Handle acknowledgment if required
                if message.acknowledge:
                    # The recipient should send an acknowledgment
                    # This is handled by the recipient's client logic
                    pass
                    
            except json.JSONDecodeError:
                await websocket.send_json({
                    "type": "error",
                    "message": "Invalid JSON format",
                    "timestamp": time.time()
                })
            except Exception as e:
                logger.error(f"Error processing message: {e}")
                await websocket.send_json({
                    "type": "error",
                    "message": f"Error processing message: {str(e)}",
                    "timestamp": time.time()
                })
                
    except WebSocketDisconnect:
        manager.disconnect(client_uuid)
        logger.info(f"Client disconnected: {client_name} ({client_uuid})")
    except Exception as e:
        logger.error(f"WebSocket error for {client_name}: {e}")
        manager.disconnect(client_uuid)


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
