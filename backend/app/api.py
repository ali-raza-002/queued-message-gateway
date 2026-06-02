"""HTTP layer. Routes translate between JSON and the service - nothing else."""

from __future__ import annotations

from typing import Annotated
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, Query, Request, Response, status
from sqlalchemy import text

from .models import MessageState
from .queue import DeliveryQueue
from .repository import Page
from .schemas import (
    ConversationMessages,
    ConversationsOut,
    DeadLetterOut,
    DeadLettersOut,
    HealthOut,
    MessageCreate,
    MessageOut,
    MessageStatus,
    PageMeta,
    QueueMetricsOut,
    ReadinessOut,
)
from .service import MessageService
from .settings import Settings

router = APIRouter()


def get_service(request: Request) -> MessageService:
    return request.app.state.service


def get_queue(request: Request) -> DeliveryQueue:
    return request.app.state.queue


def get_config(request: Request) -> Settings:
    return request.app.state.settings


ServiceDep = Annotated[MessageService, Depends(get_service)]
QueueDep = Annotated[DeliveryQueue, Depends(get_queue)]
ConfigDep = Annotated[Settings, Depends(get_config)]


def _meta(page: Page) -> PageMeta:
    return PageMeta(
        total=page.total,
        limit=page.limit,
        offset=page.offset,
        has_more=page.has_more,
    )


@router.post(
    "/messages",
    response_model=MessageOut,
    status_code=status.HTTP_201_CREATED,
    summary="Queue a message",
    responses={200: {"description": "Idempotent replay of an existing message"}},
)
async def create_message(
    payload: MessageCreate, response: Response, service: ServiceDep
) -> MessageOut:
    """Persist the message and return immediately.

    The reply is produced later by the worker. A repeat of the same
    ``client_message_id`` answers 200 with the original message rather than
    201 with a new one.
    """
    message, replayed = await service.submit(
        user_id=payload.user_id,
        conversation_id=payload.conversation_id,
        text=payload.text,
        client_message_id=payload.client_message_id,
    )
    if replayed:
        response.status_code = status.HTTP_200_OK
        response.headers["Idempotent-Replay"] = "true"
    return MessageOut.of(message)


@router.get(
    "/messages/{message_id}/status",
    response_model=MessageStatus,
    summary="Poll one message",
)
async def get_message_status(
    message_id: UUID, service: ServiceDep, config: ConfigDep
) -> MessageStatus:
    message = await service.get(message_id)
    if message is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail="Message not found"
        )
    return MessageStatus(
        message_id=message.id,
        state=MessageState(message.state),
        assistant_text=message.assistant_text,
        last_error=message.last_error,
        attempt_count=message.attempt_count,
        max_attempts=config.max_attempts,
        dead_lettered=message.dead_lettered,
        processing_started_at=message.processing_started_at,
        completed_at=message.completed_at,
    )


@router.get(
    "/conversations/{user_id}",
    response_model=ConversationsOut,
    summary="Read a user's conversations in send order",
)
async def get_conversations(
    user_id: str,
    service: ServiceDep,
    conversation_id: str | None = Query(None, description="Filter to one thread"),
    state: MessageState | None = Query(None, description="Filter by message state"),
    limit: int | None = Query(None, ge=1, le=1000),
    offset: int = Query(0, ge=0),
) -> ConversationsOut:
    """Messages are ordered by ``seq``, the order they were sent.

    Completion order is deliberately not used: a retried message finishes late
    but keeps its place in the thread.
    """
    page = await service.list_for_user(
        user_id=user_id,
        conversation_id=conversation_id,
        state=state,
        limit=limit,
        offset=offset,
    )
    grouped: dict[str, list[MessageOut]] = {}
    for message in page.items:
        grouped.setdefault(message.conversation_id, []).append(MessageOut.of(message))
    conversations = [
        ConversationMessages(conversation_id=cid, messages=messages)
        for cid, messages in sorted(grouped.items())
    ]
    return ConversationsOut(
        user_id=user_id, conversations=conversations, page=_meta(page)
    )


@router.get(
    "/dead-letters",
    response_model=DeadLettersOut,
    summary="Messages that exhausted their retries",
)
async def list_dead_letters(
    service: ServiceDep,
    user_id: str | None = Query(None),
    limit: int | None = Query(None, ge=1, le=1000),
    offset: int = Query(0, ge=0),
) -> DeadLettersOut:
    page = await service.list_dead_letters(user_id=user_id, limit=limit, offset=offset)
    return DeadLettersOut(
        items=[DeadLetterOut.model_validate(row) for row in page.items],
        page=_meta(page),
    )


@router.get("/healthz", response_model=HealthOut, summary="Liveness")
async def healthz(request: Request) -> HealthOut:
    return HealthOut(
        status="ok",
        service=request.app.title,
        version=request.app.version,
    )


@router.get("/readyz", response_model=ReadinessOut, summary="Readiness")
async def readyz(request: Request, response: Response) -> ReadinessOut:
    """Readiness is a real query, not a constant: it fails when the DB is gone."""
    database = request.app.state.database
    try:
        async with database.session() as session:
            await session.execute(text("SELECT 1"))
    except Exception as exc:  # noqa: BLE001 - surfaced to the caller as 503
        response.status_code = status.HTTP_503_SERVICE_UNAVAILABLE
        return ReadinessOut(status="degraded", database="down", detail=str(exc))
    return ReadinessOut(status="ok", database="up")


@router.get("/metrics", response_model=QueueMetricsOut, summary="Queue depth")
async def metrics(queue: QueueDep, config: ConfigDep) -> QueueMetricsOut:
    counts = await queue.depth()
    return QueueMetricsOut(
        received=counts[MessageState.RECEIVED.value],
        processing=counts[MessageState.PROCESSING.value],
        completed=counts[MessageState.COMPLETED.value],
        failed=counts[MessageState.FAILED.value],
        timeout=counts[MessageState.TIMEOUT.value],
        dead_letters=counts["dead_letters"],
        conversations=counts["conversations"],
        total=counts["total"],
        provider=config.reply_provider,
        max_attempts=config.max_attempts,
        strict_conversation_order=config.strict_conversation_order,
    )
