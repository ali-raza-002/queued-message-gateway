"""Shared fixtures.

Everything runs against a throwaway SQLite file and the credential-free echo
provider, so ``pytest`` works on a fresh clone with no Postgres and no API key.
"""

from __future__ import annotations

from collections.abc import AsyncIterator

import pytest
import pytest_asyncio
from httpx import ASGITransport, AsyncClient

from app.db import Database
from app.main import create_app
from app.providers import EchoReplyProvider
from app.queue import DeliveryQueue
from app.service import MessageService
from app.settings import Settings


@pytest.fixture
def settings(tmp_path) -> Settings:
    return Settings(
        database_url=f"sqlite+aiosqlite:///{tmp_path / 'aila-test.db'}",
        reply_provider="echo",
        reply_min_delay_seconds=0,
        reply_max_delay_seconds=0,
        reply_fail_rate=0.0,
        reply_seed=1,
        max_attempts=3,
        backoff_base_seconds=1,
        backoff_jitter=0.0,
        lease_seconds=60,
        poll_interval_seconds=0.01,
        reap_interval_seconds=0.01,
        shutdown_grace_seconds=5.0,
        concurrency=4,
        claim_batch=10,
        log_json=False,
        log_level="WARNING",
    )


@pytest_asyncio.fixture
async def database(settings: Settings) -> AsyncIterator[Database]:
    db = Database(settings.database_url)
    await db.create_all()
    yield db
    await db.dispose()


@pytest.fixture
def queue(database: Database, settings: Settings) -> DeliveryQueue:
    return DeliveryQueue(database, settings)


@pytest.fixture
def service(database: Database, settings: Settings) -> MessageService:
    return MessageService(database, settings)


@pytest.fixture
def provider() -> EchoReplyProvider:
    return EchoReplyProvider()


@pytest_asyncio.fixture
async def client(settings: Settings, database: Database) -> AsyncIterator[AsyncClient]:
    app = create_app(settings, database)
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as http:
        yield http


async def submit(
    service: MessageService,
    text: str,
    *,
    conversation: str = "c1",
    user: str = "u1",
    key: str | None = None,
):
    message, _ = await service.submit(
        user_id=user, conversation_id=conversation, text=text, client_message_id=key
    )
    return message
