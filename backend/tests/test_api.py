"""HTTP contract."""

from __future__ import annotations

import uuid

from app.main import API_VERSION


async def test_post_message_returns_201_with_seq_one(client):
    response = await client.post(
        "/messages",
        json={"user_id": "u1", "conversation_id": "c1", "text": "Hi"},
    )
    assert response.status_code == 201
    body = response.json()
    assert body["seq"] == 1
    assert body["state"] == "RECEIVED"
    assert body["assistant_text"] is None
    assert body["attempt_count"] == 0
    uuid.UUID(body["message_id"])


async def test_seq_increments_within_a_conversation(client):
    seqs = []
    for text in ("one", "two", "three"):
        response = await client.post(
            "/messages",
            json={"user_id": "u1", "conversation_id": "c1", "text": text},
        )
        seqs.append(response.json()["seq"])
    assert seqs == [1, 2, 3]


async def test_seq_is_per_conversation_not_global(client):
    first = await client.post(
        "/messages", json={"user_id": "u1", "conversation_id": "c1", "text": "a"}
    )
    second = await client.post(
        "/messages", json={"user_id": "u1", "conversation_id": "c2", "text": "b"}
    )
    assert first.json()["seq"] == 1
    assert second.json()["seq"] == 1


async def test_status_404_for_unknown_message(client):
    response = await client.get(f"/messages/{uuid.uuid4()}/status")
    assert response.status_code == 404
    assert response.json()["detail"] == "Message not found"


async def test_status_reports_attempt_budget(client):
    created = await client.post(
        "/messages", json={"user_id": "u1", "conversation_id": "c1", "text": "Hi"}
    )
    message_id = created.json()["message_id"]
    response = await client.get(f"/messages/{message_id}/status")
    assert response.status_code == 200
    body = response.json()
    assert body["attempt_count"] == 0
    assert body["max_attempts"] == 3
    assert body["dead_lettered"] is False


async def test_conversations_group_and_paginate(client):
    for index in range(5):
        await client.post(
            "/messages",
            json={"user_id": "u1", "conversation_id": "c1", "text": f"m{index}"},
        )
    await client.post(
        "/messages", json={"user_id": "u1", "conversation_id": "c2", "text": "other"}
    )

    response = await client.get("/conversations/u1", params={"limit": 3})
    body = response.json()
    assert body["page"] == {"total": 6, "limit": 3, "offset": 0, "has_more": True}
    assert [c["conversation_id"] for c in body["conversations"]] == ["c1"]
    assert [m["seq"] for m in body["conversations"][0]["messages"]] == [1, 2, 3]

    rest = await client.get("/conversations/u1", params={"limit": 3, "offset": 3})
    assert rest.json()["page"]["has_more"] is False
    assert [c["conversation_id"] for c in rest.json()["conversations"]] == ["c1", "c2"]


async def test_conversation_filter_and_state_filter(client):
    await client.post(
        "/messages", json={"user_id": "u1", "conversation_id": "c1", "text": "a"}
    )
    await client.post(
        "/messages", json={"user_id": "u1", "conversation_id": "c2", "text": "b"}
    )
    filtered = await client.get("/conversations/u1", params={"conversation_id": "c2"})
    assert [c["conversation_id"] for c in filtered.json()["conversations"]] == ["c2"]

    completed = await client.get("/conversations/u1", params={"state": "COMPLETED"})
    assert completed.json()["conversations"] == []


async def test_empty_text_is_rejected(client):
    response = await client.post(
        "/messages", json={"user_id": "u1", "conversation_id": "c1", "text": ""}
    )
    assert response.status_code == 422


async def test_health_ready_and_metrics(client):
    health = await client.get("/healthz")
    assert health.status_code == 200
    assert health.json()["version"] == API_VERSION

    ready = await client.get("/readyz")
    assert ready.status_code == 200
    assert ready.json() == {"status": "ok", "database": "up", "detail": None}

    await client.post(
        "/messages", json={"user_id": "u1", "conversation_id": "c1", "text": "Hi"}
    )
    metrics = await client.get("/metrics")
    body = metrics.json()
    assert body["received"] == 1
    assert body["total"] == 1
    assert body["dead_letters"] == 0
    assert body["max_attempts"] == 3
    assert body["strict_conversation_order"] is True


async def test_dead_letters_endpoint_starts_empty(client):
    response = await client.get("/dead-letters")
    assert response.status_code == 200
    assert response.json() == {
        "items": [],
        "page": {"total": 0, "limit": 50, "offset": 0, "has_more": False},
    }
