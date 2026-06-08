"""Request and response shapes. These are the API contract, not the tables."""

from __future__ import annotations

from datetime import datetime
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field

from .models import CONVERSATION_ID_LEN, MAX_TEXT_LEN, USER_ID_LEN, MessageState


class MessageCreate(BaseModel):
    user_id: str = Field(..., min_length=1, max_length=USER_ID_LEN, examples=["u1"])
    conversation_id: str = Field(
        ..., min_length=1, max_length=CONVERSATION_ID_LEN, examples=["c1"]
    )
    text: str = Field(..., min_length=1, max_length=MAX_TEXT_LEN)
    client_message_id: str | None = Field(
        None,
        max_length=128,
        description=(
            "Idempotency key. Re-sending the same key returns the original "
            "message instead of queueing a second one."
        ),
    )


class MessageOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    message_id: UUID
    user_id: str
    conversation_id: str
    client_message_id: str
    seq: int
    text: str
    state: MessageState
    assistant_text: str | None = None
    last_error: str | None = None
    attempt_count: int
    provider: str | None = None
    created_at: datetime
    updated_at: datetime

    @classmethod
    def of(cls, message) -> MessageOut:  # noqa: ANN001
        return cls(
            message_id=message.id,
            user_id=message.user_id,
            conversation_id=message.conversation_id,
            client_message_id=message.client_message_id,
            seq=message.seq,
            text=message.user_text,
            state=MessageState(message.state),
            assistant_text=message.assistant_text,
            last_error=message.last_error,
            attempt_count=message.attempt_count,
            provider=message.provider,
            created_at=message.created_at,
            updated_at=message.updated_at,
        )


class MessageStatus(BaseModel):
    message_id: UUID
    state: MessageState
    assistant_text: str | None = None
    last_error: str | None = None
    attempt_count: int
    max_attempts: int
    dead_lettered: bool
    processing_started_at: datetime | None = None
    completed_at: datetime | None = None


class ConversationMessages(BaseModel):
    conversation_id: str
    messages: list[MessageOut]


class PageMeta(BaseModel):
    total: int
    limit: int
    offset: int
    has_more: bool


class ConversationsOut(BaseModel):
    user_id: str
    conversations: list[ConversationMessages]
    page: PageMeta


class DeadLetterOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: UUID
    message_id: UUID
    user_id: str
    conversation_id: str
    seq: int
    attempt_count: int
    reason: str
    created_at: datetime


class DeadLettersOut(BaseModel):
    items: list[DeadLetterOut]
    page: PageMeta


class HealthOut(BaseModel):
    status: str
    service: str
    version: str


class ReadinessOut(BaseModel):
    status: str
    database: str
    detail: str | None = None


class QueueMetricsOut(BaseModel):
    """Queue depth by state. Cheap enough to poll, honest enough to alert on."""

    received: int
    processing: int
    completed: int
    failed: int
    timeout: int
    dead_letters: int
    conversations: int
    total: int
    provider: str
    max_attempts: int
    strict_conversation_order: bool
