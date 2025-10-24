import asyncio
import json
import time
from uuid import uuid4

from fastapi import WebSocketDisconnect

import main


class WebSocketSimulator:
    """Lightweight in-memory websocket used for integration-style tests."""

    def __init__(self) -> None:
        self.accepted = False
        self.closed = False
        self.sent_messages: list[dict[str, object]] = []
        self._incoming: asyncio.Queue[str | None] = asyncio.Queue()

    async def accept(self) -> None:
        self.accepted = True

    async def send_json(self, data: dict[str, object]) -> None:
        self.sent_messages.append(data)

    async def receive_text(self) -> str:
        message = await self._incoming.get()
        if message is None:
            raise WebSocketDisconnect(code=1000)
        return message

    async def close(self, code: int = 1000, reason: str = "") -> None:  # noqa: ARG002
        self.closed = True
        await self._incoming.put(None)

    def queue_message(self, payload: dict[str, object]) -> None:
        self._incoming.put_nowait(json.dumps(payload))


def _build_message(sender: str, recipient: str, *, acknowledge: bool = False) -> dict[str, object]:
    msgid = str(uuid4())
    return {
        "to": recipient,
        "from": sender,
        "timestamp": time.time(),
        "priority": "normal",
        "subject": "Greeting",
        "msgid": msgid,
        "acknowledge": acknowledge,
        "content": "Hello",
        "action": "send",
        "event": "message",
        "status": "new",
        "conversation_id": "conv-1",
        "msgno": 1,
    }


def _build_topic_message(sender: str, topic_id: str) -> dict[str, object]:
    return {
        "topic_id": topic_id,
        "from": sender,
        "timestamp": time.time(),
        "priority": "normal",
        "subject": "Update",
        "msgid": str(uuid4()),
        "content": "Broadcast message",
        "action": "topic_message",
        "event": "",
        "status": "sent",
        "msgno": 1,
    }


def _run_websocket(token: str, websocket: WebSocketSimulator) -> asyncio.Task[None]:
    return asyncio.create_task(main.websocket_endpoint(websocket, token=token))


def test_websocket_direct_message_flow(token_registry, fake_redis_store):
    _ = fake_redis_store
    sender_id = str(uuid4())
    recipient_id = str(uuid4())
    token_registry("sender-token", sub=sender_id, name="Sender")
    token_registry("recipient-token", sub=recipient_id, name="Recipient")

    async def runner() -> None:
        sender_ws = WebSocketSimulator()
        recipient_ws = WebSocketSimulator()

        sender_task = _run_websocket("sender-token", sender_ws)
        recipient_task = _run_websocket("recipient-token", recipient_ws)
        await asyncio.sleep(0)

        assert sender_ws.sent_messages[0]["type"] == "connection"
        assert recipient_ws.sent_messages[0]["type"] == "connection"

        sender_ws.queue_message(_build_message(sender_id, recipient_id))
        await asyncio.sleep(0.05)

        direct_message = recipient_ws.sent_messages[1]
        assert direct_message["content"] == "Hello"
        assert direct_message["from"] == sender_id

        status_update = sender_ws.sent_messages[1]
        assert status_update["type"] == "delivery_status"
        assert status_update["status"] == "delivered"

        await sender_ws.close()
        await recipient_ws.close()
        await asyncio.wait_for(sender_task, timeout=1)
        await asyncio.wait_for(recipient_task, timeout=1)

    asyncio.run(runner())


def test_websocket_topic_broadcast(token_registry, fake_redis_store):
    sender_id = str(uuid4())
    other_id = str(uuid4())
    token_registry("sender-token", sub=sender_id, name="Sender")
    token_registry("other-token", sub=other_id, name="Listener")

    topic_id = "updates"
    fake_redis_store.create_topic(topic_id, sender_id, {"category": "news"})
    fake_redis_store.subscribe_to_topic(topic_id, sender_id)
    fake_redis_store.subscribe_to_topic(topic_id, other_id)

    async def runner() -> None:
        sender_ws = WebSocketSimulator()
        listener_ws = WebSocketSimulator()

        sender_task = _run_websocket("sender-token", sender_ws)
        listener_task = _run_websocket("other-token", listener_ws)
        await asyncio.sleep(0)

        sender_ws.queue_message(_build_topic_message(sender_id, topic_id))
        await asyncio.sleep(0.05)

        broadcast = listener_ws.sent_messages[1]
        assert broadcast["topic_id"] == topic_id
        assert broadcast["content"] == "Broadcast message"

        sender_feedback = sender_ws.sent_messages[1]
        assert sender_feedback["type"] == "topic_sent"
        assert sender_feedback["topic_id"] == topic_id

        await sender_ws.close()
        await listener_ws.close()
        await asyncio.wait_for(sender_task, timeout=1)
        await asyncio.wait_for(listener_task, timeout=1)

    asyncio.run(runner())


def test_websocket_acknowledgment_flow(token_registry, fake_redis_store):
    sender_id = str(uuid4())
    recipient_id = str(uuid4())
    token_registry("sender-token", sub=sender_id, name="Sender")
    token_registry("recipient-token", sub=recipient_id, name="Recipient")

    async def runner() -> None:
        sender_ws = WebSocketSimulator()
        recipient_ws = WebSocketSimulator()

        sender_task = _run_websocket("sender-token", sender_ws)
        recipient_task = _run_websocket("recipient-token", recipient_ws)
        await asyncio.sleep(0)

        message = _build_message(sender_id, recipient_id, acknowledge=True)
        sender_ws.queue_message(message)
        await asyncio.sleep(0.05)

        delivered = recipient_ws.sent_messages[1]
        msgid = delivered["msgid"]
        assert delivered["acknowledge"] is True

        pending = sender_ws.sent_messages[1]
        assert pending["status"] == "pending_ack"

        acknowledgment = {
            "type": "ack",
            "msgid": msgid,
            "from": recipient_id,
            "timestamp": time.time(),
            "status": "received",
        }
        recipient_ws.queue_message(acknowledgment)
        await asyncio.sleep(0.05)

        ack_confirmation = recipient_ws.sent_messages[2]
        assert ack_confirmation["type"] == "ack_received"
        assert ack_confirmation["msgid"] == msgid

        delivery_update = sender_ws.sent_messages[2]
        assert delivery_update["type"] == "delivery_update"
        assert delivery_update["status"] == "delivered"

        await sender_ws.close()
        await recipient_ws.close()
        await asyncio.wait_for(sender_task, timeout=1)
        await asyncio.wait_for(recipient_task, timeout=1)

    asyncio.run(runner())
