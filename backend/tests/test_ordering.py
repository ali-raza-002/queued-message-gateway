"""Per-conversation send order: what is claimed, and what is read back."""

from __future__ import annotations

from app.models import MessageState
from app.queue import DeliveryQueue

from .conftest import submit


async def test_claim_takes_the_head_of_each_conversation_only(service, queue):
    for text in ("first", "second", "third"):
        await submit(service, text)

    claimed = await queue.claim()
    assert [message.seq for message in claimed] == [1]


async def test_next_message_unblocks_once_the_head_is_terminal(service, queue):
    await submit(service, "first")
    await submit(service, "second")

    head = (await queue.claim())[0]
    assert await queue.claim() == []  # seq 2 is still blocked by seq 1

    await queue.complete(head, "done", provider="echo")
    assert [message.seq for message in await queue.claim()] == [2]


async def test_a_retried_message_still_blocks_its_successors(service, queue):
    """This is the case the ordering guarantee exists for.

    Without head-of-line blocking, seq 2 is delivered while seq 1 is waiting out
    its backoff, and the conversation is answered out of order.
    """
    await submit(service, "first")
    await submit(service, "second")

    head = (await queue.claim())[0]
    await queue.fail(head, "upstream hiccup")

    assert await queue.claim() == []


async def test_conversations_do_not_block_each_other(service, queue):
    await submit(service, "a1", conversation="c1")
    await submit(service, "a2", conversation="c1")
    await submit(service, "b1", conversation="c2")

    claimed = await queue.claim()
    assert sorted((message.conversation_id, message.seq) for message in claimed) == [
        ("c1", 1),
        ("c2", 1),
    ]


async def test_relaxed_mode_claims_the_whole_backlog(database, settings, service):
    """Strict ordering costs throughput, so it is a switch, not a law of nature."""
    for text in ("one", "two", "three"):
        await submit(service, text)

    relaxed = settings.model_copy(update={"strict_conversation_order": False})
    claimed = await DeliveryQueue(database, relaxed).claim()
    assert [message.seq for message in claimed] == [1, 2, 3]


async def test_read_order_is_send_order_not_completion_order(service, queue, client):
    first = await submit(service, "first")
    second = await submit(service, "second")

    # Answer the second message before the first: the transcript must not care.
    head = (await queue.claim())[0]
    await queue.complete(head, "reply-1", provider="echo")
    claimed_second = (await queue.claim())[0]
    await queue.complete(claimed_second, "reply-2", provider="echo")

    body = (await client.get("/conversations/u1")).json()
    messages = body["conversations"][0]["messages"]
    assert [m["seq"] for m in messages] == [1, 2]
    assert [m["message_id"] for m in messages] == [str(first.id), str(second.id)]
    assert [m["assistant_text"] for m in messages] == ["reply-1", "reply-2"]
    assert all(m["state"] == MessageState.COMPLETED.value for m in messages)
