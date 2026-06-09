"""The claim query is a Postgres design. Check it compiles to the SQL we claim.

Docker is not available in the test environment, so this compiles the statement
against the PostgreSQL dialect instead of executing it. It is the difference
between "the SQLite path works" and "the production path is what the README
says it is".
"""

from __future__ import annotations

from sqlalchemy.dialects import postgresql, sqlite

from app.db import Database
from app.queue import DeliveryQueue


def compile_claim(url: str, settings) -> str:
    database = Database(url)
    try:
        dialect = (
            postgresql.dialect() if database.dialect == "postgresql" else sqlite.dialect()
        )
        statement = DeliveryQueue(database, settings)._claimable(10)
        return str(statement.compile(dialect=dialect))
    finally:
        database.engine.sync_engine.dispose()


def test_postgres_claim_uses_skip_locked(settings):
    sql = compile_claim("postgresql+asyncpg://u:p@localhost/db", settings)
    assert "FOR UPDATE OF messages SKIP LOCKED" in sql
    assert "NOT (EXISTS" in sql


def test_sqlite_claim_drops_row_locking(settings):
    sql = compile_claim("sqlite+aiosqlite:///:memory:", settings)
    assert "FOR UPDATE" not in sql
    assert "LIMIT" in sql


def test_relaxed_mode_drops_the_head_of_line_predicate(settings):
    relaxed = settings.model_copy(update={"strict_conversation_order": False})
    sql = compile_claim("postgresql+asyncpg://u:p@localhost/db", relaxed)
    assert "NOT (EXISTS" not in sql
    assert "FOR UPDATE OF messages SKIP LOCKED" in sql


def test_postgres_supports_skip_locked_and_sqlite_does_not():
    pg = Database("postgresql+asyncpg://u:p@localhost/db")
    lite = Database("sqlite+aiosqlite:///:memory:")
    try:
        assert pg.supports_skip_locked is True
        assert lite.supports_skip_locked is False
    finally:
        pg.engine.sync_engine.dispose()
        lite.engine.sync_engine.dispose()
