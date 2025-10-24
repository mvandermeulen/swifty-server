import asyncio
from uuid import UUID, uuid4

import main
import pytest
from redis_store import RedisUnavailableError


def test_register_client_success(async_client, monkeypatch: pytest.MonkeyPatch):
    client_uuid = uuid4()
    tokens = {
        "access_token": "access-token",
        "access_token_expires_at": "2030-01-01T00:00:00Z",
        "access_token_expires_in": 3600,
        "refresh_token": "refresh-token",
        "refresh_token_expires_at": "2030-01-02T00:00:00Z",
        "refresh_token_expires_in": 7200,
        "token_type": "bearer",
        "roles": ["user"],
    }

    def fake_issue_token_pair(client_id: UUID, name: str) -> dict[str, object]:
        assert client_id == client_uuid
        assert name == "Alice"
        return tokens

    monkeypatch.setattr(main, "issue_token_pair", fake_issue_token_pair)

    async def runner() -> None:
        response = await async_client.post(
            "/register",
            json={"name": "Alice", "uuid": str(client_uuid)},
        )
        assert response.status_code == 200
        body = response.json()
        for key, value in tokens.items():
            assert body[key] == value
        assert body["uuid"] == str(client_uuid)
        assert body["name"] == "Alice"

    asyncio.run(runner())


def test_create_topic_requires_valid_token(async_client, token_registry):
    token_registry(
        "valid-token",
        sub=str(uuid4()),
        name="Admin",
        roles=["admin"],
    )

    async def runner() -> None:
        response = await async_client.post(
            "/topics/create",
            params={"token": "invalid-token"},
            json={"topic_id": "alerts", "metadata": {"severity": "high"}},
        )
        assert response.status_code == 401
        detail = response.json()["detail"]
        assert detail["code"] == main.ErrorCode.INVALID_TOKEN.value

    asyncio.run(runner())


def test_topic_crud_flow(async_client, token_registry, fake_redis_store):
    client_uuid = str(uuid4())
    token_registry("admin-token", sub=client_uuid, roles=["admin"], name="Owner")

    async def runner() -> None:
        create_response = await async_client.post(
            "/topics/create",
            params={"token": "admin-token"},
            json={"topic_id": "updates", "metadata": {"priority": "low"}},
        )
        assert create_response.status_code == 200
        create_body = create_response.json()
        assert create_body["topic_id"] == "updates"
        assert create_body["creator"] == client_uuid

        list_response = await async_client.get("/topics")
        assert list_response.status_code == 200
        list_body = list_response.json()
        assert list_body["count"] == 1
        assert list_body["topics"][0]["id"] == "updates"

        info_response = await async_client.get("/topics/updates")
        assert info_response.status_code == 200
        info_body = info_response.json()
        assert info_body["id"] == "updates"
        assert info_body["creator"] == client_uuid

        subscribe_response = await async_client.post(
            "/topics/subscribe",
            params={"token": "admin-token"},
            json={"topic_id": "updates"},
        )
        assert subscribe_response.status_code == 200
        assert fake_redis_store.get_topic_subscribers("updates") == {client_uuid}

        unsubscribe_response = await async_client.post(
            "/topics/unsubscribe",
            params={"token": "admin-token"},
            json={"topic_id": "updates"},
        )
        assert unsubscribe_response.status_code == 200
        assert fake_redis_store.get_topic_subscribers("updates") == set()

    asyncio.run(runner())


def test_create_topic_redis_unavailable(async_client, token_registry, fake_redis_store, monkeypatch):
    client_uuid = str(uuid4())
    token_registry("admin-token", sub=client_uuid, roles=["admin"], name="Owner")

    def fail_create_topic(topic_id, creator_uuid, metadata):  # type: ignore[no-untyped-def]
        raise RedisUnavailableError("redis down")

    monkeypatch.setattr(fake_redis_store, "get_topic", lambda topic_id: None)
    monkeypatch.setattr(fake_redis_store, "create_topic", fail_create_topic)

    async def runner() -> None:
        response = await async_client.post(
            "/topics/create",
            params={"token": "admin-token"},
            json={"topic_id": "fail", "metadata": {}},
        )
        assert response.status_code == 503
        detail = response.json()["detail"]
        assert detail["code"] == main.ErrorCode.REDIS_UNAVAILABLE.value

    asyncio.run(runner())
