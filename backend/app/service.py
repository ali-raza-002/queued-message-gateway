"""Application logic that sits between the HTTP layer and the tables."""

from __future__ import annotations

import logging
import uuid

from sqlalchemy.exc import IntegrityError

from .db import Database
from .models import Message, MessageState, utcnow
from .repository import (
    ConversationRepository,
    DeadLetterRepository,
    MessageRepository,
    Page,
)
from .settings import Settings

logger = logging.getLogger(__name__)


class MessageService:
    def __init__(self, database: Database, settings: Settings) -> None:
        self._db = database
        self._settings = settings

    async def submit(
        self,
        *,
        user_id: str,
        conversation_id: str,
        text: str,
        client_message_id: str | None = None,
    ) -> tuple[Message, bool]:
        """Accept a message and queue it. Returns ``(message, was_replay)``.

        Idempotency is enforced twice on purpose. The lookup handles the common
        case cheaply, and the ``IntegrityError`` catch handles the case the
        lookup cannot: two identical requests racing, where both miss the read
        and only one insert can win. Without the second path that race surfaces
        as a 500 for a client that did nothing wrong.
        """
        key = client_message_id or str(uuid.uuid4())

        async with self._db.session() as session:
            existing = await MessageRepository.find_by_client_id(
                session,
                user_id=user_id,
                conversation_id=conversation_id,
                client_message_id=key,
            )
            if existing is not None:
                return existing, True

        try:
            async with self._db.begin() as session:
                seq = await ConversationRepository.allocate_seq(
                    session, user_id=user_id, conversation_id=conversation_id
                )
                now = utcnow()
                message = Message(
                    id=uuid.uuid4(),
                    user_id=user_id,
                    conversation_id=conversation_id,
                    client_message_id=key,
                    seq=seq,
                    user_text=text,
                    state=MessageState.RECEIVED.value,
                    attempt_count=0,
                    available_at=now,
                    created_at=now,
                    updated_at=now,
                )
                await MessageRepository.add(session, message)
            return message, False
        except IntegrityError:
            logger.info(
                "idempotent_replay",
                extra={"user_id": user_id, "conversation_id": conversation_id},
            )
            async with self._db.session() as session:
                existing = await MessageRepository.find_by_client_id(
                    session,
                    user_id=user_id,
                    conversation_id=conversation_id,
                    client_message_id=key,
                )
            if existing is None:  # pragma: no cover - the key must exist by now
                raise
            return existing, True

    async def get(self, message_id: uuid.UUID) -> Message | None:
        async with self._db.session() as session:
            return await MessageRepository.get(session, message_id)

    async def list_for_user(
        self,
        *,
        user_id: str,
        conversation_id: str | None = None,
        state: MessageState | None = None,
        limit: int | None = None,
        offset: int = 0,
    ) -> Page:
        page_size = min(
            limit or self._settings.page_size_default, self._settings.page_size_max
        )
        async with self._db.session() as session:
            return await MessageRepository.page_for_user(
                session,
                user_id=user_id,
                conversation_id=conversation_id,
                state=state,
                limit=page_size,
                offset=max(0, offset),
            )

    async def list_dead_letters(
        self, *, user_id: str | None = None, limit: int | None = None, offset: int = 0
    ) -> Page:
        page_size = min(
            limit or self._settings.page_size_default, self._settings.page_size_max
        )
        async with self._db.session() as session:
            return await DeadLetterRepository.page(
                session, user_id=user_id, limit=page_size, offset=max(0, offset)
            )
