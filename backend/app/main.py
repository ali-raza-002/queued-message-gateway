"""ASGI entrypoint. ``create_app`` exists so tests can build an app on a
throwaway database without touching process-wide state."""

from __future__ import annotations

import logging
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from fastapi import FastAPI

from .api import router
from .db import Database, get_database
from .logging_setup import configure_logging
from .queue import DeliveryQueue
from .service import MessageService
from .settings import Settings, get_settings

API_TITLE = "AILA Messaging Backend"
API_VERSION = "1.0.0"
API_DESCRIPTION = (
    "Messages are accepted synchronously and answered asynchronously. "
    "Replies are attached to the message they answer, and a conversation always "
    "reads back in send order."
)

logger = logging.getLogger(__name__)


def create_app(
    settings: Settings | None = None, database: Database | None = None
) -> FastAPI:
    config = settings or get_settings()
    configure_logging(config.log_level, as_json=config.log_json)
    db = database or get_database()

    @asynccontextmanager
    async def lifespan(_: FastAPI) -> AsyncIterator[None]:
        if config.auto_create_schema:
            # Convenient for a take-home. A deployment would run migrations.
            await db.create_all()
        logger.info(
            "api_started",
            extra={"dialect": db.dialect, "provider": config.reply_provider},
        )
        yield
        if database is None:
            await db.dispose()

    app = FastAPI(
        title=API_TITLE,
        version=API_VERSION,
        description=API_DESCRIPTION,
        lifespan=lifespan,
    )
    app.state.settings = config
    app.state.database = db
    app.state.service = MessageService(db, config)
    app.state.queue = DeliveryQueue(db, config)
    app.include_router(router)
    return app


app = create_app()
