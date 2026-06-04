"""Engine, session factory and the couple of dialect facts the queue needs.

The delivery queue is written against PostgreSQL (``FOR UPDATE SKIP LOCKED``),
but the same statements have to run on SQLite so the test-suite works on a fresh
clone with no server. ``Database.supports_skip_locked`` is the one switch that
separates the two.
"""

from __future__ import annotations

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from functools import lru_cache

from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)
from sqlalchemy.orm import DeclarativeBase
from sqlalchemy.pool import StaticPool

from .settings import Settings, get_settings

# Dialects whose row locking the claim query can rely on.
_ROW_LOCKING_DIALECTS = frozenset({"postgresql"})


class Base(DeclarativeBase):
    pass


class Database:
    """Owns the engine and hands out sessions."""

    def __init__(self, url: str, *, echo: bool = False) -> None:
        self.url = url
        kwargs: dict[str, object] = {"echo": echo, "pool_pre_ping": True}
        if url.startswith("sqlite"):
            # SQLite has no server-side pool to ping and needs a single shared
            # connection when the database lives in memory.
            kwargs.pop("pool_pre_ping")
            if ":memory:" in url:
                kwargs["poolclass"] = StaticPool
                kwargs["connect_args"] = {"check_same_thread": False}
        self.engine: AsyncEngine = create_async_engine(url, **kwargs)
        self.session_factory = async_sessionmaker(
            self.engine, expire_on_commit=False, class_=AsyncSession
        )

    @property
    def dialect(self) -> str:
        return self.engine.dialect.name

    @property
    def supports_skip_locked(self) -> bool:
        return self.dialect in _ROW_LOCKING_DIALECTS

    @asynccontextmanager
    async def begin(self) -> AsyncIterator[AsyncSession]:
        """A session wrapped in one transaction, committed on clean exit."""
        async with self.session_factory() as session, session.begin():
            yield session

    @asynccontextmanager
    async def session(self) -> AsyncIterator[AsyncSession]:
        async with self.session_factory() as session:
            yield session

    async def create_all(self) -> None:
        from . import models  # noqa: F401  (registers mappers on Base.metadata)

        async with self.engine.begin() as conn:
            await conn.run_sync(Base.metadata.create_all)

    async def dispose(self) -> None:
        await self.engine.dispose()


@lru_cache(maxsize=1)
def get_database() -> Database:
    """The process-wide database. Tests build their own instead."""
    cfg: Settings = get_settings()
    return Database(cfg.database_url, echo=cfg.db_echo)
