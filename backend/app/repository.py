"""Data access. Nothing in here decides policy - it only reads and writes rows."""

from __future__ import annotations

import uuid
from dataclasses import dataclass

from sqlalchemy import func, insert, select, update
from sqlalchemy.ext.asyncio import AsyncSession

from .models import Conversation, DeadLetter, Message, MessageState, utcnow


@dataclass(frozen=True, slots=True)
class Page:
    """One slice of a list endpoint."""

    items: list
    total: int
    limit: int
    offset: int

    @property
    def has_more(self) -> bool:
        return self.offset + len(self.items) < self.total


class ConversationRepository:
    @staticmethod
    async def allocate_seq(
        session: AsyncSession, *, user_id: str, conversation_id: str
    ) -> int:
        """Reserve the next send-order slot for a conversation.

        One ``UPDATE ... RETURNING`` rather than ``SELECT FOR UPDATE`` followed
        by an ``UPDATE``: it is a single atomic statement, so concurrent writers
        to the same conversation serialise on the row without a second round
        trip and without any risk of two callers reading the same ``next_seq``.
        """
        table = Conversation.__table__
        now = utcnow()
        dialect = session.get_bind().dialect.name
        if dialect == "postgresql":
            from sqlalchemy.dialects.postgresql import insert as dialect_insert
        elif dialect == "sqlite":
            from sqlalchemy.dialects.sqlite import insert as dialect_insert
        else:  # pragma: no cover - only pg and sqlite are supported
            dialect_insert = insert

        create = dialect_insert(table).values(
            user_id=user_id,
            conversation_id=conversation_id,
            next_seq=1,
            created_at=now,
            updated_at=now,
        )
        if hasattr(create, "on_conflict_do_nothing"):
            create = create.on_conflict_do_nothing(
                index_elements=["user_id", "conversation_id"]
            )
        await session.execute(create)

        allocated = (
            await session.execute(
                update(table)
                .where(
                    table.c.user_id == user_id,
                    table.c.conversation_id == conversation_id,
                )
                .values(next_seq=table.c.next_seq + 1, updated_at=now)
                .returning(table.c.next_seq)
            )
        ).scalar_one()
        # RETURNING gives the post-increment value; the caller gets the slot
        # that was just consumed.
        return int(allocated) - 1


class MessageRepository:
    @staticmethod
    async def find_by_client_id(
        session: AsyncSession,
        *,
        user_id: str,
        conversation_id: str,
        client_message_id: str,
    ) -> Message | None:
        result = await session.execute(
            select(Message).where(
                Message.user_id == user_id,
                Message.conversation_id == conversation_id,
                Message.client_message_id == client_message_id,
            )
        )
        return result.scalar_one_or_none()

    @staticmethod
    async def add(session: AsyncSession, message: Message) -> Message:
        session.add(message)
        await session.flush()
        return message

    @staticmethod
    async def get(session: AsyncSession, message_id: uuid.UUID) -> Message | None:
        result = await session.execute(select(Message).where(Message.id == message_id))
        return result.scalar_one_or_none()

    @staticmethod
    async def page_for_user(
        session: AsyncSession,
        *,
        user_id: str,
        conversation_id: str | None,
        state: MessageState | None,
        limit: int,
        offset: int,
    ) -> Page:
        """Messages for a user in send order.

        Paginated because a chat history is unbounded: the previous version of
        this query loaded every message a user had ever sent into memory.
        """
        filters = [Message.user_id == user_id]
        if conversation_id is not None:
            filters.append(Message.conversation_id == conversation_id)
        if state is not None:
            filters.append(Message.state == state.value)

        total = (
            await session.execute(
                select(func.count()).select_from(Message.__table__).where(*filters)
            )
        ).scalar_one()

        rows = (
            (
                await session.execute(
                    select(Message)
                    .where(*filters)
                    .order_by(Message.conversation_id.asc(), Message.seq.asc())
                    .limit(limit)
                    .offset(offset)
                )
            )
            .scalars()
            .all()
        )
        return Page(items=list(rows), total=int(total), limit=limit, offset=offset)


class DeadLetterRepository:
    @staticmethod
    async def page(
        session: AsyncSession,
        *,
        user_id: str | None,
        limit: int,
        offset: int,
    ) -> Page:
        filters = []
        if user_id is not None:
            filters.append(DeadLetter.user_id == user_id)
        total = (
            await session.execute(
                select(func.count()).select_from(DeadLetter.__table__).where(*filters)
            )
        ).scalar_one()
        rows = (
            (
                await session.execute(
                    select(DeadLetter)
                    .where(*filters)
                    .order_by(DeadLetter.created_at.desc())
                    .limit(limit)
                    .offset(offset)
                )
            )
            .scalars()
            .all()
        )
        return Page(items=list(rows), total=int(total), limit=limit, offset=offset)
