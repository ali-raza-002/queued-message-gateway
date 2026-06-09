"""Worker behaviour: poison messages, retries, and SIGTERM."""

from __future__ import annotations

import asyncio

import pytest
from sqlalchemy import select

from app.models import Message, MessageState
from app.providers import PermanentProviderError, ProviderError, ReplyRequest
from worker.worker import DeliveryWorker

from .conftest import submit


class ScriptedProvider:
    """Replays a scripted outcome per call so failure paths are deterministic."""

    name = "scripted"

    def __init__(self, *outcomes: object) -> None:
        self._outcomes = list(outcomes)
        self.calls = 0

    async def generate(self, request: ReplyRequest) -> str:
        self.calls += 1
        outcome = self._outcomes[min(self.calls - 1, len(self._outcomes) - 1)]
        if isinstance(outcome, BaseException):
            raise outcome
        return str(outcome)

    async def aclose(self) -> None:
        return None


class BlockingProvider:
    name = "blocking"

    def __init__(self) -> None:
        self.entered = asyncio.Event()
        self.release = asyncio.Event()

    async def generate(self, request: ReplyRequest) -> str:
        self.entered.set()
        await self.release.wait()
        return "finished"

    async def aclose(self) -> None:
        return None


async def reload(database, message_id) -> Message:
    async with database.session() as session:
        return (
            await session.execute(select(Message).where(Message.id == message_id))
        ).scalar_one()


async def test_worker_completes_a_message(database, settings, service, provider):
    message = await submit(service, "What is tax deduction?")
    worker = DeliveryWorker(database, settings, provider)

    assert await worker.run_once() == 1
    await worker.drain()

    row = await reload(database, message.id)
    assert row.state == MessageState.COMPLETED.value
    assert row.provider == "echo"
    assert "tax deduction" in row.assistant_text


@pytest.mark.parametrize(
    "error",
    [
        ProviderError("upstream down"),
        RuntimeError("provider blew up in a way nobody anticipated"),
    ],
)
async def test_a_bad_message_does_not_stop_the_loop(database, settings, service, error):
    """The poison-message check: the failing message is parked, the next one runs."""
    poison = await submit(service, "poison")
    healthy = await submit(service, "healthy")
    fast = settings.model_copy(update={"backoff_base_seconds": 0.01})
    worker = DeliveryWorker(database, fast, ScriptedProvider(error, "a reply"))

    await worker.run_once()
    await worker.drain()
    poison_row = await reload(database, poison.id)
    assert poison_row.state == MessageState.RECEIVED.value
    assert poison_row.attempt_count == 1

    # Burn the poison message's remaining attempts so it leaves the queue head.
    await asyncio.sleep(0.05)
    give_up = DeliveryWorker(
        database,
        fast.model_copy(update={"max_attempts": 1}),
        ScriptedProvider(error),
    )
    await give_up.run_once()
    await give_up.drain()
    assert (await reload(database, poison.id)).state == MessageState.FAILED.value

    # The loop is alive and the message behind the poison one gets answered.
    worker_two = DeliveryWorker(database, fast, ScriptedProvider("a reply"))
    await worker_two.run_once()
    await worker_two.drain()
    assert (await reload(database, healthy.id)).state == MessageState.COMPLETED.value


async def test_permanent_error_dead_letters_on_the_first_attempt(
    database, settings, service
):
    message = await submit(service, "malformed")
    worker = DeliveryWorker(
        database, settings, ScriptedProvider(PermanentProviderError("cannot parse"))
    )

    await worker.run_once()
    await worker.drain()

    row = await reload(database, message.id)
    assert row.state == MessageState.FAILED.value
    assert row.attempt_count == 1
    assert row.dead_lettered is True
    assert row.last_error == "cannot parse"


async def test_a_transient_failure_is_retried_until_it_succeeds(
    database, settings, service
):
    message = await submit(service, "flaky")
    fast = settings.model_copy(update={"backoff_base_seconds": 0.01})
    scripted = ScriptedProvider(ProviderError("first try"), "second try worked")
    worker = DeliveryWorker(database, fast, scripted)

    await worker.run_once()
    await worker.drain()
    assert (await reload(database, message.id)).state == MessageState.RECEIVED.value

    await asyncio.sleep(0.05)
    await worker.run_once()
    await worker.drain()

    row = await reload(database, message.id)
    assert row.state == MessageState.COMPLETED.value
    assert row.assistant_text == "second try worked"
    assert row.attempt_count == 2
    assert scripted.calls == 2


async def test_shutdown_returns_an_interrupted_message_to_the_queue(
    database, settings, service
):
    """SIGTERM mid-delivery must not cost the message one of its attempts."""
    message = await submit(service, "slow")
    blocking = BlockingProvider()
    abrupt = settings.model_copy(update={"shutdown_grace_seconds": 0.0})
    worker = DeliveryWorker(database, abrupt, blocking)

    runner = asyncio.create_task(worker.run())
    await asyncio.wait_for(blocking.entered.wait(), timeout=2)
    assert worker.inflight_count == 1

    worker.request_stop()  # what the SIGTERM handler calls
    await asyncio.wait_for(runner, timeout=5)

    row = await reload(database, message.id)
    assert row.state == MessageState.RECEIVED.value
    assert row.attempt_count == 0
    assert row.lease_expires_at is None
    assert worker.inflight_count == 0


async def test_shutdown_lets_an_in_flight_message_finish_within_the_grace_period(
    database, settings, service
):
    message = await submit(service, "slow")
    blocking = BlockingProvider()
    worker = DeliveryWorker(database, settings, blocking)

    runner = asyncio.create_task(worker.run())
    await asyncio.wait_for(blocking.entered.wait(), timeout=2)
    worker.request_stop()
    blocking.release.set()
    await asyncio.wait_for(runner, timeout=5)

    row = await reload(database, message.id)
    assert row.state == MessageState.COMPLETED.value
    assert row.assistant_text == "finished"


async def test_worker_never_claims_more_than_it_can_start(database, settings, service):
    """A claimed message already holds a lease, so over-claiming wastes it."""
    for index in range(6):
        await submit(service, f"m{index}", conversation=f"c{index}")
    narrow = settings.model_copy(update={"concurrency": 2, "claim_batch": 10})
    blocking = BlockingProvider()
    worker = DeliveryWorker(database, narrow, blocking)

    assert await worker.run_once() == 2
    assert await worker.run_once() == 0

    blocking.release.set()
    await worker.drain()


async def test_worker_is_idle_when_the_queue_is_empty(database, settings, provider):
    worker = DeliveryWorker(database, settings, provider)
    assert await worker.run_once() == 0
    assert worker.inflight_count == 0
