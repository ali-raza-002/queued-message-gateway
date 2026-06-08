"""``python -m worker`` - the worker process entrypoint."""

from __future__ import annotations

import asyncio
import logging

from app.db import Database
from app.logging_setup import configure_logging
from app.providers import build_provider
from app.settings import get_settings

from .worker import DeliveryWorker

logger = logging.getLogger("worker")


async def main() -> None:
    settings = get_settings()
    configure_logging(settings.log_level, as_json=settings.log_json)
    database = Database(settings.database_url, echo=settings.db_echo)
    provider = build_provider(settings)
    if settings.auto_create_schema:
        await database.create_all()
    worker = DeliveryWorker(database, settings, provider)
    try:
        await worker.run()
    finally:
        await provider.aclose()
        await database.dispose()


if __name__ == "__main__":
    asyncio.run(main())
