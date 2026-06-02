"""Tables for the message queue.

Two invariants live in the schema rather than in Python, because the worker runs
in more than one process and application-level checks would race:

* ``uq_message_client_id`` makes ``client_message_id`` the idempotency key.
* ``uq_message_seq`` makes the per-conversation sequence number a gap-free,
  never-reused ordering key.
"""

from __future__ import annotations

import enum
import uuid
from datetime import UTC, datetime

from sqlalchemy import (
    Boolean,
    CheckConstraint,
    DateTime,
    ForeignKey,
    ForeignKeyConstraint,
    Index,
    Integer,
    String,
    Text,
    TypeDecorator,
    Uuid,
)
from sqlalchemy.orm import Mapped, mapped_column, relationship

from .db import Base

USER_ID_LEN = 128
CONVERSATION_ID_LEN = 128
CLIENT_MESSAGE_ID_LEN = 128
STATE_LEN = 16
MAX_TEXT_LEN = 4000


def utcnow() -> datetime:
    """Timezone-aware UTC now.

    The original code used ``datetime.utcnow()``, which returns a *naive*
    datetime, and wrote it into ``timestamptz`` columns alongside values
    produced by SQL ``now()``. Comparing the two is undefined behaviour waiting
    to happen, so every timestamp in this schema is aware.
    """
    return datetime.now(UTC)


class UtcDateTime(TypeDecorator):
    """``DateTime(timezone=True)`` that also survives a round-trip through SQLite.

    SQLite drops the offset on write and returns a naive datetime on read; this
    normalises both directions so comparisons in Python are always aware-to-aware.
    """

    impl = DateTime(timezone=True)
    cache_ok = True

    def process_bind_param(self, value, dialect):  # noqa: ANN001, ANN201, ARG002
        if value is None:
            return None
        if value.tzinfo is None:
            raise ValueError("naive datetime rejected: use models.utcnow()")
        return value.astimezone(UTC)

    def process_result_value(self, value, dialect):  # noqa: ANN001, ANN201, ARG002
        if value is None:
            return None
        if value.tzinfo is None:
            return value.replace(tzinfo=UTC)
        return value.astimezone(UTC)


class MessageState(str, enum.Enum):
    """Lifecycle of one message.

    ``RECEIVED`` is the queued state the API writes and a retry returns to.
    ``TIMEOUT`` is only ever set by the lease reaper.
    """

    RECEIVED = "RECEIVED"
    PROCESSING = "PROCESSING"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"
    TIMEOUT = "TIMEOUT"

    @classmethod
    def values(cls) -> tuple[str, ...]:
        return tuple(member.value for member in cls)


#: States from which a message may still move. Anything else is terminal.
OPEN_STATES: frozenset[str] = frozenset(
    {MessageState.RECEIVED.value, MessageState.PROCESSING.value}
)
TERMINAL_STATES: frozenset[str] = frozenset(MessageState.values()) - OPEN_STATES

_STATE_LIST = ", ".join(f"'{value}'" for value in MessageState.values())


class Conversation(Base):
    """One thread. Holds the sequence counter that defines send order."""

    __tablename__ = "conversations"

    user_id: Mapped[str] = mapped_column(String(USER_ID_LEN), primary_key=True)
    conversation_id: Mapped[str] = mapped_column(
        String(CONVERSATION_ID_LEN), primary_key=True
    )
    next_seq: Mapped[int] = mapped_column(Integer, nullable=False, default=1)
    created_at: Mapped[datetime] = mapped_column(
        UtcDateTime, nullable=False, default=utcnow
    )
    updated_at: Mapped[datetime] = mapped_column(
        UtcDateTime, nullable=False, default=utcnow, onupdate=utcnow
    )

    messages: Mapped[list[Message]] = relationship(back_populates="conversation")

    __table_args__ = (
        CheckConstraint("next_seq >= 1", name="ck_conversation_next_seq_positive"),
    )


class Message(Base):
    __tablename__ = "messages"

    id: Mapped[uuid.UUID] = mapped_column(Uuid, primary_key=True, default=uuid.uuid4)
    user_id: Mapped[str] = mapped_column(String(USER_ID_LEN), nullable=False)
    conversation_id: Mapped[str] = mapped_column(
        String(CONVERSATION_ID_LEN), nullable=False
    )

    #: Client-supplied idempotency key. The API fills one in when it is omitted.
    client_message_id: Mapped[str] = mapped_column(
        String(CLIENT_MESSAGE_ID_LEN), nullable=False
    )

    #: Position in the conversation, allocated at write time. Read order, not
    #: completion order, is defined by this column.
    seq: Mapped[int] = mapped_column(Integer, nullable=False)

    user_text: Mapped[str] = mapped_column(Text, nullable=False)
    assistant_text: Mapped[str | None] = mapped_column(Text, nullable=True)

    state: Mapped[str] = mapped_column(
        String(STATE_LEN), nullable=False, default=MessageState.RECEIVED.value
    )
    attempt_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    provider: Mapped[str | None] = mapped_column(String(32), nullable=True)

    #: Earliest time a worker may claim this row. Retry backoff pushes it forward.
    available_at: Mapped[datetime] = mapped_column(
        UtcDateTime, nullable=False, default=utcnow
    )
    #: Lease expiry. A PROCESSING row past this time is assumed orphaned.
    lease_expires_at: Mapped[datetime | None] = mapped_column(UtcDateTime, nullable=True)
    processing_started_at: Mapped[datetime | None] = mapped_column(
        UtcDateTime, nullable=True
    )
    completed_at: Mapped[datetime | None] = mapped_column(UtcDateTime, nullable=True)

    last_error: Mapped[str | None] = mapped_column(Text, nullable=True)
    dead_lettered: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)

    created_at: Mapped[datetime] = mapped_column(
        UtcDateTime, nullable=False, default=utcnow
    )
    updated_at: Mapped[datetime] = mapped_column(
        UtcDateTime, nullable=False, default=utcnow, onupdate=utcnow
    )

    conversation: Mapped[Conversation] = relationship(
        back_populates="messages",
        primaryjoin=(
            "and_(Message.user_id == Conversation.user_id, "
            "Message.conversation_id == Conversation.conversation_id)"
        ),
        foreign_keys=[user_id, conversation_id],
    )

    __table_args__ = (
        ForeignKeyConstraint(
            ["user_id", "conversation_id"],
            ["conversations.user_id", "conversations.conversation_id"],
            name="fk_message_conversation",
            ondelete="CASCADE",
        ),
        # Idempotency key. Scoped to the user as well as the conversation so two
        # users cannot collide by picking the same conversation_id.
        Index(
            "uq_message_client_id",
            "user_id",
            "conversation_id",
            "client_message_id",
            unique=True,
        ),
        # Send order is unique and gap-free per conversation.
        Index("uq_message_seq", "user_id", "conversation_id", "seq", unique=True),
        # Serves the claim query: state filter, then availability, then send order.
        Index("ix_message_claim", "state", "available_at", "seq"),
        # Serves the lease reaper.
        Index("ix_message_lease", "state", "lease_expires_at"),
        CheckConstraint("attempt_count >= 0", name="ck_message_attempt_nonnegative"),
        CheckConstraint("seq >= 1", name="ck_message_seq_positive"),
        CheckConstraint(f"state IN ({_STATE_LIST})", name="ck_message_state_known"),
    )


class DeadLetter(Base):
    """A message that will not be retried again, kept for inspection and replay."""

    __tablename__ = "dead_letters"

    id: Mapped[uuid.UUID] = mapped_column(Uuid, primary_key=True, default=uuid.uuid4)
    message_id: Mapped[uuid.UUID] = mapped_column(
        Uuid, ForeignKey("messages.id", ondelete="CASCADE"), nullable=False
    )
    user_id: Mapped[str] = mapped_column(String(USER_ID_LEN), nullable=False)
    conversation_id: Mapped[str] = mapped_column(
        String(CONVERSATION_ID_LEN), nullable=False
    )
    seq: Mapped[int] = mapped_column(Integer, nullable=False)
    attempt_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    reason: Mapped[str] = mapped_column(Text, nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        UtcDateTime, nullable=False, default=utcnow
    )

    __table_args__ = (
        # One dead-letter row per message, so a duplicate final failure is a no-op.
        Index("uq_dead_letter_message", "message_id", unique=True),
        Index("ix_dead_letter_created", "created_at"),
    )
