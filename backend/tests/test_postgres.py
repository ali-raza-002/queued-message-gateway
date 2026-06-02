"""The queue against a real PostgreSQL server.

SQLite has no row locks and a single writer, so it cannot show that two workers
never claim the same row or that concurrent sends get a gap-free ``seq``. These
tests can. They are skipped unless ``TEST_POSTGRES_URL`` is set, and they drop
and recreate every table in that database, so point it at a throwaway one.
"""

from __future__ import annotations

import asyncio
import os
from collections import defaultdict
from collections.abc import AsyncIterator

import pytest
import pytest_asyncio
from sqlalchemy import func, select

from app import models  # noqa: F401  (registers the tables on Base)
from app.db import Base, Database
from app.models import DeadLetter, Message, MessageState
from app.providers import ReplyRequest
from app.queue import DeliveryQueue
from app.service import MessageService
from app.settings import Settings
from worker.worker import DeliveryWorker

PG_URL = os.environ.get("TEST_POSTGRES_URL")

pytestmark = pytest.mark.skipif(not PG_URL, reason="TEST_POSTGRES_URL is not set")


@pytest.fixture
def pg_settings(settings: Settings) -> Settings:
    return settings.model_copy(update={"database_url": PG_URL})


@pytest_asyncio.fixture
async def pg(pg_settings: Settings) -> AsyncIterator[Database]:
    database = Database(PG_URL)
    async with database.engine.begin() as conn:
        await conn.run_sync(Base.metadata.drop_all)
        await conn.run_sync(Base.metadata.create_all)
    yield database
    await database.dispose()


@pytest.fixture
def pg_service(pg: Database, pg_settings: Settings) -> MessageService:
    return MessageService(pg, pg_settings)


async def count(database: Database, statement) -> int:
    async with database.session() as session:
        return (await session.execute(statement)).scalar_one()


async def test_dialect_is_postgres(pg: Database):
    assert pg.supports_skip_locked is True


async def test_concurrent_sends_get_gap_free_sequence_numbers(pg_service):
    results = await asyncio.gather(
        *(
            pg_service.submit(user_id="u1", conversation_id="c1", text=f"m{i}")
            for i in range(30)
        )
    )
    assert sorted(message.seq for message, _ in results) == list(range(1, 31))


async def test_racing_replays_store_one_message(pg, pg_service):
    results = await asyncio.gather(
        *(
            pg_service.submit(
                user_id="u1", conversation_id="c1", text="hi", client_message_id="k1"
            )
            for _ in range(10)
        )
    )
    assert len({message.id for message, _ in results}) == 1
    assert sum(not replayed for _, replayed in results) == 1
    assert await count(pg, select(func.count()).select_from(Message)) == 1

    message, replayed = await pg_service.submit(
        user_id="u1", conversation_id="c1", text="next"
    )
    assert (message.seq, replayed) == (2, False)


async def test_parallel_claims_never_share_a_message(pg, pg_settings, pg_service):
    for i in range(40):
        await pg_service.submit(user_id="u1", conversation_id=f"c{i}", text="hi")
    queues = [DeliveryQueue(pg, pg_settings) for _ in range(4)]

    claimed = []
    for _ in range(10):
        batches = await asyncio.gather(*(q.claim(15) for q in queues))
        assert all(len(batch) <= 15 for batch in batches)
        claimed += [message.id for batch in batches for message in batch]
        if len(claimed) >= 40:
            break

    assert len(claimed) == len(set(claimed)) == 40


async def test_parallel_claims_take_only_the_head_of_a_conversation(
    pg, pg_settings, pg_service
):
    for i in range(5):
        await pg_service.submit(user_id="u1", conversation_id="c1", text=f"m{i}")
    queues = [DeliveryQueue(pg, pg_settings) for _ in range(4)]

    batches = await asyncio.gather(*(q.claim(5) for q in queues))

    assert [m.seq for batch in batches for m in batch] == [1]


class RecordingProvider:
    """Answers instantly and records the order each conversation was served in."""

    name = "recording"

    def __init__(self) -> None:
        self.served: dict[str, list[int]] = defaultdict(list)

    async def generate(self, request: ReplyRequest) -> str:
        self.served[request.conversation_id].append(request.seq)
        await asyncio.sleep(0)
        return f"reply {request.seq}"

    async def aclose(self) -> None:
        return None


async def test_three_workers_deliver_everything_once_and_in_order(
    pg, pg_settings, pg_service
):
    for seq in range(5):
        for conversation in ("a", "b", "c", "d"):
            await pg_service.submit(
                user_id="u1", conversation_id=conversation, text=f"{conversation}{seq}"
            )
    provider = RecordingProvider()
    workers = [DeliveryWorker(pg, pg_settings, provider) for _ in range(3)]
    runners = [asyncio.create_task(worker.run()) for worker in workers]

    completed = select(func.count()).where(Message.state == MessageState.COMPLETED.value)
    for _ in range(500):
        if await count(pg, completed) == 20:
            break
        await asyncio.sleep(0.01)
    for worker in workers:
        worker.request_stop()
    await asyncio.wait_for(asyncio.gather(*runners), timeout=10)

    assert await count(pg, completed) == 20
    assert await count(pg, select(func.max(Message.attempt_count))) == 1
    assert dict(provider.served) == {c: [1, 2, 3, 4, 5] for c in "abcd"}


async def test_dead_letter_insert_is_idempotent_on_postgres(pg, pg_settings, pg_service):
    await pg_service.submit(user_id="u1", conversation_id="c1", text="hi")
    queue = DeliveryQueue(pg, pg_settings)
    [claimed] = await queue.claim()

    await queue.fail(claimed, "bad payload", permanent=True)
    async with pg.begin() as session:
        await session.execute(
            queue._dead_letter_insert(
                message_id=claimed.id,
                user_id="u1",
                conversation_id="c1",
                seq=1,
                attempt_count=1,
                reason="duplicate",
            )
        )

    assert await count(pg, select(func.count()).select_from(DeadLetter)) == 1
