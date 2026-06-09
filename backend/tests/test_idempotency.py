"""The same client_message_id must never queue two messages."""

from __future__ import annotations

import pytest

from app.repository import MessageRepository

KEY = "11111111-1111-1111-1111-111111111111"


async def test_replay_returns_the_original_message(client):
    payload = {
        "user_id": "u1",
        "conversation_id": "c1",
        "text": "Hi",
        "client_message_id": KEY,
    }
    first = await client.post("/messages", json=payload)
    second = await client.post("/messages", json=payload)

    assert first.status_code == 201
    assert second.status_code == 200
    assert second.headers["Idempotent-Replay"] == "true"
    assert second.json()["message_id"] == first.json()["message_id"]
    assert second.json()["seq"] == first.json()["seq"] == 1


async def test_replay_does_not_consume_a_sequence_number(client):
    payload = {
        "user_id": "u1",
        "conversation_id": "c1",
        "text": "Hi",
        "client_message_id": KEY,
    }
    await client.post("/messages", json=payload)
    await client.post("/messages", json=payload)
    follow_up = await client.post(
        "/messages", json={"user_id": "u1", "conversation_id": "c1", "text": "Next"}
    )
    assert follow_up.json()["seq"] == 2


async def test_same_key_in_another_conversation_is_a_different_message(client):
    first = await client.post(
        "/messages",
        json={
            "user_id": "u1",
            "conversation_id": "c1",
            "text": "Hi",
            "client_message_id": KEY,
        },
    )
    second = await client.post(
        "/messages",
        json={
            "user_id": "u1",
            "conversation_id": "c2",
            "text": "Hi",
            "client_message_id": KEY,
        },
    )
    assert second.status_code == 201
    assert second.json()["message_id"] != first.json()["message_id"]


async def test_same_key_from_another_user_is_a_different_message(client):
    """The unique index is scoped to the user, not only the conversation id."""
    first = await client.post(
        "/messages",
        json={
            "user_id": "u1",
            "conversation_id": "shared",
            "text": "Hi",
            "client_message_id": KEY,
        },
    )
    second = await client.post(
        "/messages",
        json={
            "user_id": "u2",
            "conversation_id": "shared",
            "text": "Hi",
            "client_message_id": KEY,
        },
    )
    assert first.status_code == 201
    assert second.status_code == 201
    assert second.json()["message_id"] != first.json()["message_id"]


async def test_lost_race_falls_back_to_the_existing_row(service, monkeypatch):
    """Two identical requests can both miss the pre-check.

    Only one insert wins the unique index; the loser must answer with the
    original message rather than a 500. Simulated by blinding the pre-check
    once, which is exactly what the race does.
    """
    await service.submit(
        user_id="u1", conversation_id="c1", text="Hi", client_message_id=KEY
    )

    calls = {"n": 0}
    original = MessageRepository.find_by_client_id

    async def blind_once(session, **kwargs):
        calls["n"] += 1
        if calls["n"] == 1:
            return None
        return await original(session, **kwargs)

    monkeypatch.setattr(MessageRepository, "find_by_client_id", blind_once)

    message, replayed = await service.submit(
        user_id="u1", conversation_id="c1", text="Hi", client_message_id=KEY
    )
    assert replayed is True
    assert message.seq == 1
    assert calls["n"] == 2


async def test_server_generates_a_key_when_none_is_given(service):
    message, replayed = await service.submit(
        user_id="u1", conversation_id="c1", text="Hi"
    )
    assert replayed is False
    assert message.client_message_id


@pytest.mark.parametrize("count", [3])
async def test_distinct_keys_queue_distinct_messages(service, count):
    ids = set()
    for index in range(count):
        message, _ = await service.submit(
            user_id="u1",
            conversation_id="c1",
            text=f"m{index}",
            client_message_id=f"key-{index}",
        )
        ids.add(message.id)
    assert len(ids) == count
