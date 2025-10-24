from __future__ import annotations

import json
import sys
import time
from collections import defaultdict
from pathlib import Path
from typing import Callable
from urllib.parse import urlencode
from uuid import uuid4

import pytest

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.append(str(ROOT_DIR))

import auth  # noqa: E402
import connection_manager  # noqa: E402
import main  # noqa: E402


class FakeRedisStore:
    """Minimal in-memory stand-in for the Redis repository used in tests."""

    def __init__(self) -> None:
        self.clients: dict[str, dict[str, str]] = {}
        self.connected_clients: dict[str, str] = {}
        self.topics: dict[str, dict[str, object]] = {}
        self.topic_subscribers: defaultdict[str, set[str]] = defaultdict(set)
        self.client_topics: defaultdict[str, set[str]] = defaultdict(set)
        self.offline_messages: defaultdict[str, list[dict]] = defaultdict(list)
        self.delivery_status: dict[str, dict[str, object]] = {}

    # Client connection helpers -------------------------------------------------
    def register_client(self, client_uuid: str, client_name: str, token: str) -> bool:
        self.clients[client_uuid] = {
            "uuid": client_uuid,
            "name": client_name,
            "token": token,
        }
        return True

    def set_client_connected(self, client_uuid: str, client_name: str) -> bool:
        self.connected_clients[client_uuid] = client_name
        return True

    def cleanup_client(self, client_uuid: str) -> bool:
        self.connected_clients.pop(client_uuid, None)
        for topic in self.topic_subscribers.values():
            topic.discard(client_uuid)
        self.client_topics.pop(client_uuid, None)
        return True

    def get_connected_clients(self) -> dict[str, str]:
        return dict(self.connected_clients)

    # Delivery bookkeeping -----------------------------------------------------
    def record_delivery_attempt(self, msgid: str, metadata: dict[str, object]) -> bool:
        entry = self.delivery_status.setdefault(msgid, {})
        entry.update(metadata)
        entry["status"] = "attempted"
        return True

    def update_delivery_status(self, msgid: str, status: str, **extra: object) -> bool:
        entry = self.delivery_status.setdefault(msgid, {})
        entry["status"] = status
        entry.update(extra)
        return True

    def enqueue_offline_message(self, recipient_uuid: str, message: dict) -> bool:
        self.offline_messages[recipient_uuid].append(message)
        return True

    def pop_offline_messages(self, recipient_uuid: str) -> list[dict]:
        return self.offline_messages.pop(recipient_uuid, [])

    # Topic management ---------------------------------------------------------
    def create_topic(
        self, topic_id: str, creator_uuid: str, metadata: dict[str, object] | None = None
    ) -> bool:
        if topic_id in self.topics:
            return False
        self.topics[topic_id] = {
            "id": topic_id,
            "creator": creator_uuid,
            "metadata": dict(metadata or {}),
        }
        self.topic_subscribers.setdefault(topic_id, set())
        return True

    def get_topic(self, topic_id: str) -> dict[str, object] | None:
        topic = self.topics.get(topic_id)
        if not topic:
            return None
        return {
            "id": topic["id"],
            "creator": topic["creator"],
            "metadata": dict(topic.get("metadata", {})),
        }

    def list_topics(self) -> list[str]:
        return list(self.topics.keys())

    def delete_topic(self, topic_id: str) -> bool:
        if topic_id not in self.topics:
            return False
        self.topics.pop(topic_id, None)
        self.topic_subscribers.pop(topic_id, None)
        for subscriptions in self.client_topics.values():
            subscriptions.discard(topic_id)
        return True

    def subscribe_to_topic(self, topic_id: str, client_uuid: str) -> bool:
        if topic_id not in self.topics:
            return False
        self.topic_subscribers[topic_id].add(client_uuid)
        self.client_topics[client_uuid].add(topic_id)
        return True

    def unsubscribe_from_topic(self, topic_id: str, client_uuid: str) -> bool:
        self.topic_subscribers[topic_id].discard(client_uuid)
        self.client_topics[client_uuid].discard(topic_id)
        return True

    def get_topic_subscribers(self, topic_id: str) -> set[str]:
        return set(self.topic_subscribers.get(topic_id, set()))

    def get_client_topics(self, client_uuid: str) -> set[str]:
        return set(self.client_topics.get(client_uuid, set()))


@pytest.fixture
def fake_redis_store(monkeypatch: pytest.MonkeyPatch) -> FakeRedisStore:
    store = FakeRedisStore()
    monkeypatch.setattr(main, "redis_store", store)
    monkeypatch.setattr(connection_manager, "redis_store", store)
    monkeypatch.setattr(auth, "redis_store", store)
    return store


@pytest.fixture
def token_registry(monkeypatch: pytest.MonkeyPatch) -> Callable[..., dict[str, object]]:
    tokens: dict[str, dict[str, object]] = {}

    def register(
        token: str,
        *,
        sub: str | None = None,
        name: str = "Test User",
        roles: list[str] | None = None,
        token_type: str = "access",
        jti: str | None = None,
    ) -> dict[str, object]:
        payload = {
            "sub": sub or str(uuid4()),
            "name": name,
            "roles": roles or ["user"],
            "type": token_type,
            "jti": jti or f"jti-{token}",
            "iat": time.time(),
            "nbf": time.time(),
            "exp": time.time() + 3600,
        }
        tokens[token] = payload
        return payload

    def verify(token: str, expected_type: str = "access") -> dict[str, object] | None:
        payload = tokens.get(token)
        if not payload:
            return None
        if expected_type and payload.get("type") != expected_type:
            return None
        return payload

    monkeypatch.setattr(main, "verify_token", verify)
    monkeypatch.setattr(auth, "verify_token", verify)
    return register


@pytest.fixture
def app(fake_redis_store: FakeRedisStore, monkeypatch: pytest.MonkeyPatch):
    # Reset the connection manager for each test to avoid cross-test state.
    new_manager = connection_manager.ConnectionManager()
    
    async def _noop() -> None:
        return None

    monkeypatch.setattr(new_manager, "_ensure_worker", _noop)
    monkeypatch.setattr(main, "manager", new_manager)
    return main.app


@pytest.fixture
def async_client(app) -> SimpleAsyncClient:
    return SimpleAsyncClient(app)


class SimpleResponse:
    def __init__(self, status_code: int, body: bytes, headers: list[tuple[bytes, bytes]]):
        self.status_code = status_code
        self._body = body
        self.headers = {key.decode(): value.decode() for key, value in headers}

    def json(self) -> dict[str, object]:
        if not self._body:
            return {}
        return json.loads(self._body.decode())


class SimpleAsyncClient:
    def __init__(self, app):
        self.app = app

    async def __aenter__(self) -> "SimpleAsyncClient":
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: object | None,
    ) -> None:
        return None

    async def request(
        self,
        method: str,
        path: str,
        *,
        params: dict[str, str] | None = None,
        json_body: dict[str, object] | None = None,
    ) -> SimpleResponse:
        query_string = urlencode(params or {}).encode()
        body_bytes = b""
        headers = [(b"host", b"testserver")]
        if json_body is not None:
            body_bytes = json.dumps(json_body).encode()
            headers.append((b"content-type", b"application/json"))

        body_sent = False

        async def receive() -> dict[str, object]:
            nonlocal body_sent
            if body_sent:
                return {"type": "http.disconnect"}
            body_sent = True
            return {"type": "http.request", "body": body_bytes, "more_body": False}

        response_body = bytearray()
        status_code = 500
        response_headers: list[tuple[bytes, bytes]] = []

        async def send(message: dict[str, object]) -> None:
            nonlocal status_code, response_headers
            if message["type"] == "http.response.start":
                status_code = int(message["status"])
                response_headers = list(message.get("headers", []))
            elif message["type"] == "http.response.body":
                response_body.extend(message.get("body", b""))

        scope = {
            "type": "http",
            "asgi": {"version": "3.0", "spec_version": "2.3"},
            "method": method.upper(),
            "path": path,
            "raw_path": path.encode(),
            "query_string": query_string,
            "headers": headers,
            "client": ("testclient", 50000),
            "server": ("testserver", 80),
            "scheme": "http",
        }

        await self.app(scope, receive, send)
        return SimpleResponse(status_code, bytes(response_body), response_headers)

    async def get(self, path: str, *, params: dict[str, str] | None = None) -> SimpleResponse:
        return await self.request("GET", path, params=params)

    async def post(
        self,
        path: str,
        *,
        params: dict[str, str] | None = None,
        json: dict[str, object] | None = None,
    ) -> SimpleResponse:
        return await self.request("POST", path, params=params, json_body=json)

