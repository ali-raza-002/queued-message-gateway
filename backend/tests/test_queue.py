"""Claim, acknowledge, retry, reap."""

from __future__ import annotations

from datetime import timedelta

import pytest
from sqlalchemy import select, update

from app.models import Message, MessageState, utcnow
from app.queue import DeliveryQueue, FailureOutcome

from .conftest import submit


async def reload(database, message_id) -> Message:
    async with database.session() as session:
        return (
            await session.execute(select(Message).where(Message.id == message_id))
        ).scalar_one()


async def test_claim_moves_to_processing_and_takes_a_lease(service, queue, database):
    message = await submit(service, "hello")
    claimed = (await queue.claim())[0]

    row = await reload(database, message.id)
    assert row.state == MessageState.PROCESSING.value
    assert row.attempt_count == 1 == claimed.attempt
    assert row.lease_expires_at is not None
    assert row.lease_expires_at > utcnow()


async def test_claim_ignores_messages_that_are_not_due_yet(service, queue, database):
    message = await submit(service, "later")
    async with database.begin() as session:
        await session.execute(
            update(Message)
            .where(Message.id == message.id)
            .values(available_at=utcnow() + timedelta(minutes=5))
        )
    assert await queue.claim() == []


async def test_complete_is_rejected_after_the_lease_is_lost(service, queue, database):
    """A zombie task must not overwrite whoever re-claimed the message."""
    message = await submit(service, "hello")
    claimed = (await queue.claim())[0]

    async with database.begin() as session:
        await session.execute(
            update(Message)
            .where(Message.id == message.id)
            .values(
                state=MessageState.RECEIVED.value,
                attempt_count=2,
                lease_expires_at=None,
            )
        )

    assert await queue.complete(claimed, "too late", provider="echo") is False
    assert (await reload(database, message.id)).assistant_text is None


async def test_failure_schedules_a_retry_with_backoff(service, queue, database):
    message = await submit(service, "hello")
    claimed = (await queue.claim())[0]

    outcome = await queue.fail(claimed, "boom")
    assert outcome is FailureOutcome.RETRY_SCHEDULED

    row = await reload(database, message.id)
    assert row.state == MessageState.RECEIVED.value
    assert row.attempt_count == 1
    assert row.last_error == "boom"
    assert row.available_at > utcnow()


async def test_retries_stop_at_max_attempts_and_dead_letter(service, queue, database):
    message = await submit(service, "hello")
    outcomes = []
    for _ in range(3):
        async with database.begin() as session:
            await session.execute(
                update(Message)
                .where(Message.id == message.id)
                .values(available_at=utcnow())
            )
        claimed = (await queue.claim())[0]
        outcomes.append(await queue.fail(claimed, "boom"))

    assert outcomes == [
        FailureOutcome.RETRY_SCHEDULED,
        FailureOutcome.RETRY_SCHEDULED,
        FailureOutcome.DEAD_LETTERED,
    ]
    row = await reload(database, message.id)
    assert row.state == MessageState.FAILED.value
    assert row.attempt_count == 3
    assert row.dead_lettered is True


async def test_permanent_failure_skips_the_remaining_attempts(service, queue, database):
    message = await submit(service, "hello")
    claimed = (await queue.claim())[0]

    outcome = await queue.fail(claimed, "malformed payload", permanent=True)

    assert outcome is FailureOutcome.DEAD_LETTERED
    row = await reload(database, message.id)
    assert row.attempt_count == 1
    assert row.state == MessageState.FAILED.value


async def test_release_hands_the_message_back_without_spending_an_attempt(
    service, queue, database
):
    message = await submit(service, "hello")
    claimed = (await queue.claim())[0]

    assert await queue.release(claimed) is True
    row = await reload(database, message.id)
    assert row.state == MessageState.RECEIVED.value
    assert row.attempt_count == 0
    assert row.lease_expires_at is None


async def test_reaper_requeues_an_expired_lease(service, queue, database):
    message = await submit(service, "hello")
    await queue.claim()
    async with database.begin() as session:
        await session.execute(
            update(Message)
            .where(Message.id == message.id)
            .values(lease_expires_at=utcnow() - timedelta(seconds=1))
        )

    result = await queue.reap_expired_leases()
    assert (result.timed_out, result.requeued, result.dead_lettered) == (1, 1, 0)
    assert (await reload(database, message.id)).state == MessageState.RECEIVED.value


async def test_reaper_dead_letters_an_exhausted_lease(service, queue, database):
    """An expired lease at max attempts used to stall in TIMEOUT forever."""
    message = await submit(service, "hello")
    await queue.claim()
    async with database.begin() as session:
        await session.execute(
            update(Message)
            .where(Message.id == message.id)
            .values(
                attempt_count=3,
                lease_expires_at=utcnow() - timedelta(seconds=1),
            )
        )

    result = await queue.reap_expired_leases()
    assert (result.timed_out, result.requeued, result.dead_lettered) == (1, 0, 1)
    row = await reload(database, message.id)
    assert row.state == MessageState.FAILED.value
    assert row.dead_lettered is True


async def test_dead_letter_rows_are_visible_over_http(service, queue, client):
    await submit(service, "hello")
    claimed = (await queue.claim())[0]
    await queue.fail(claimed, "bad payload", permanent=True)

    body = (await client.get("/dead-letters")).json()
    assert body["page"]["total"] == 1
    entry = body["items"][0]
    assert entry["reason"] == "bad payload"
    assert entry["seq"] == 1
    assert entry["attempt_count"] == 1


@pytest.mark.parametrize(
    ("attempt", "expected"), [(1, 1.0), (2, 2.0), (3, 4.0), (10, 120.0)]
)
def test_backoff_grows_and_is_capped(database, settings, attempt, expected):
    queue = DeliveryQueue(database, settings)
    assert queue.backoff_seconds(attempt) == pytest.approx(expected)


def test_jitter_keeps_backoff_inside_its_band(database, settings):
    jittered = DeliveryQueue(
        database, settings.model_copy(update={"backoff_jitter": 0.5})
    )
    samples = [jittered.backoff_seconds(3) for _ in range(50)]
    assert all(2.0 <= value <= 6.0 for value in samples)
    assert len(set(samples)) > 1


async def test_depth_counts_every_state(service, queue):
    await submit(service, "a")
    await submit(service, "b", conversation="c2")
    await queue.claim()

    depth = await queue.depth()
    assert depth[MessageState.PROCESSING.value] == 2
    assert depth[MessageState.RECEIVED.value] == 0
    assert depth["conversations"] == 2
    assert depth["total"] == 2
