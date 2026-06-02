"""The delivery queue: claim, acknowledge, retry, reap, dead-letter.

Every statement here is a single atomic UPDATE guarded by the state the caller
believed the row was in. That guard is what makes the queue safe with more than
one worker: a task whose lease has already been reaped updates zero rows and
says so, instead of overwriting the work of whoever re-claimed the message.

Delivery semantics are **at-least-once**. A message is moved to ``PROCESSING``
*before* the provider is called, and only moved to ``COMPLETED`` *after* the
provider returns. A worker that dies in between leaves a row whose lease
expires and is retried, so a provider with a side effect must dedupe on
``ReplyRequest.idempotency_key``.
"""

from __future__ import annotations

import enum
import random
import uuid
from dataclasses import dataclass
from datetime import timedelta

from sqlalchemy import Select, exists, func, insert, select, update
from sqlalchemy.sql import Insert

from .db import Database
from .models import (
    OPEN_STATES,
    Conversation,
    DeadLetter,
    Message,
    MessageState,
    utcnow,
)
from .settings import Settings

LEASE_EXPIRED_REASON = "processing lease expired"


@dataclass(frozen=True, slots=True)
class ClaimedMessage:
    """A message this worker holds a lease on."""

    id: uuid.UUID
    user_id: str
    conversation_id: str
    seq: int
    text: str
    attempt: int


class FailureOutcome(str, enum.Enum):
    RETRY_SCHEDULED = "retry_scheduled"
    DEAD_LETTERED = "dead_lettered"
    LEASE_LOST = "lease_lost"


@dataclass(frozen=True, slots=True)
class ReapResult:
    timed_out: int = 0
    requeued: int = 0
    dead_lettered: int = 0

    @property
    def touched(self) -> int:
        return self.timed_out + self.requeued + self.dead_lettered


class DeliveryQueue:
    """Data access for the queue. Holds no in-memory state between calls."""

    def __init__(self, database: Database, settings: Settings) -> None:
        self._db = database
        self._settings = settings
        self._random = random.Random(settings.reply_seed)

    # -- retry timing ---------------------------------------------------

    def backoff_seconds(self, attempt: int) -> float:
        """Exponential backoff with jitter.

        Jitter matters more than it looks: without it every message that failed
        in the same claim batch comes back at the same instant and hits the
        provider as one spike.
        """
        cfg = self._settings
        exponent = max(0, attempt - 1)
        delay = cfg.backoff_base_seconds * (cfg.backoff_factor**exponent)
        delay = min(delay, cfg.backoff_max_seconds)
        if cfg.backoff_jitter:
            spread = delay * cfg.backoff_jitter
            delay += self._random.uniform(-spread, spread)
        return max(0.0, delay)

    # -- claim ----------------------------------------------------------

    def _claimable(self, limit: int) -> Select:
        now = utcnow()
        msg = Message.__table__
        stmt = select(msg.c.id).where(
            msg.c.state == MessageState.RECEIVED.value,
            msg.c.available_at <= now,
        )
        if self._settings.strict_conversation_order:
            # Per-conversation head-of-line: a message is only claimable when no
            # lower-seq message in the same conversation is still open. Without
            # this, seq 2 can overtake seq 1 whenever seq 1 is retried.
            earlier = msg.alias("earlier")
            stmt = stmt.where(
                ~exists(
                    select(earlier.c.id).where(
                        earlier.c.user_id == msg.c.user_id,
                        earlier.c.conversation_id == msg.c.conversation_id,
                        earlier.c.seq < msg.c.seq,
                        earlier.c.state.in_(sorted(OPEN_STATES)),
                    )
                )
            )
        stmt = stmt.order_by(msg.c.available_at.asc(), msg.c.seq.asc()).limit(limit)
        if self._db.supports_skip_locked:
            # Two workers polling at once must not fight over the same rows.
            stmt = stmt.with_for_update(skip_locked=True, of=msg)
        return stmt

    async def claim(self, limit: int | None = None) -> list[ClaimedMessage]:
        """Lease up to ``limit`` due messages and return them."""
        batch = limit if limit is not None else self._settings.claim_batch
        if batch <= 0:
            return []
        now = utcnow()
        msg = Message.__table__
        stmt = (
            update(msg)
            .where(msg.c.id.in_(self._claimable(batch)))
            .values(
                state=MessageState.PROCESSING.value,
                attempt_count=msg.c.attempt_count + 1,
                processing_started_at=now,
                lease_expires_at=now + timedelta(seconds=self._settings.lease_seconds),
                updated_at=now,
            )
            .returning(
                msg.c.id,
                msg.c.user_id,
                msg.c.conversation_id,
                msg.c.seq,
                msg.c.user_text,
                msg.c.attempt_count,
            )
        )
        async with self._db.begin() as session:
            rows = (await session.execute(stmt)).mappings().all()
        return [
            ClaimedMessage(
                id=row["id"],
                user_id=row["user_id"],
                conversation_id=row["conversation_id"],
                seq=row["seq"],
                text=row["user_text"],
                attempt=row["attempt_count"],
            )
            for row in rows
        ]

    # -- acknowledgement ------------------------------------------------

    async def complete(
        self, message: ClaimedMessage, reply: str, *, provider: str
    ) -> bool:
        """Acknowledge a delivered message. False means the lease was lost."""
        now = utcnow()
        msg = Message.__table__
        stmt = (
            update(msg)
            .where(
                msg.c.id == message.id,
                msg.c.state == MessageState.PROCESSING.value,
                msg.c.attempt_count == message.attempt,
            )
            .values(
                state=MessageState.COMPLETED.value,
                assistant_text=reply,
                provider=provider,
                completed_at=now,
                lease_expires_at=None,
                last_error=None,
                updated_at=now,
            )
        )
        async with self._db.begin() as session:
            result = await session.execute(stmt)
        return result.rowcount == 1

    async def fail(
        self, message: ClaimedMessage, error: str, *, permanent: bool = False
    ) -> FailureOutcome:
        """Record a failed attempt, scheduling a retry or dead-lettering."""
        exhausted = permanent or message.attempt >= self._settings.max_attempts
        if not exhausted:
            return await self._schedule_retry(message, error)
        return await self._dead_letter(message, error)

    async def _schedule_retry(
        self, message: ClaimedMessage, error: str
    ) -> FailureOutcome:
        now = utcnow()
        msg = Message.__table__
        stmt = (
            update(msg)
            .where(
                msg.c.id == message.id,
                msg.c.state == MessageState.PROCESSING.value,
                msg.c.attempt_count == message.attempt,
            )
            .values(
                state=MessageState.RECEIVED.value,
                available_at=now
                + timedelta(seconds=self.backoff_seconds(message.attempt)),
                lease_expires_at=None,
                last_error=error,
                updated_at=now,
            )
        )
        async with self._db.begin() as session:
            result = await session.execute(stmt)
        if result.rowcount != 1:
            return FailureOutcome.LEASE_LOST
        return FailureOutcome.RETRY_SCHEDULED

    async def _dead_letter(self, message: ClaimedMessage, error: str) -> FailureOutcome:
        now = utcnow()
        msg = Message.__table__
        stmt = (
            update(msg)
            .where(
                msg.c.id == message.id,
                msg.c.state == MessageState.PROCESSING.value,
                msg.c.attempt_count == message.attempt,
            )
            .values(
                state=MessageState.FAILED.value,
                lease_expires_at=None,
                last_error=error,
                dead_lettered=True,
                updated_at=now,
            )
        )
        async with self._db.begin() as session:
            result = await session.execute(stmt)
            if result.rowcount != 1:
                return FailureOutcome.LEASE_LOST
            await session.execute(
                self._dead_letter_insert(
                    message_id=message.id,
                    user_id=message.user_id,
                    conversation_id=message.conversation_id,
                    seq=message.seq,
                    attempt_count=message.attempt,
                    reason=error,
                )
            )
        return FailureOutcome.DEAD_LETTERED

    async def release(self, message: ClaimedMessage) -> bool:
        """Hand a leased message back without consuming the attempt.

        Used on graceful shutdown: an interrupted message is not a failed
        message, so it must not count against ``max_attempts``.
        """
        now = utcnow()
        msg = Message.__table__
        stmt = (
            update(msg)
            .where(
                msg.c.id == message.id,
                msg.c.state == MessageState.PROCESSING.value,
                msg.c.attempt_count == message.attempt,
            )
            .values(
                state=MessageState.RECEIVED.value,
                attempt_count=msg.c.attempt_count - 1,
                available_at=now,
                lease_expires_at=None,
                processing_started_at=None,
                updated_at=now,
            )
        )
        async with self._db.begin() as session:
            result = await session.execute(stmt)
        return result.rowcount == 1

    # -- lease reaper ---------------------------------------------------

    async def reap_expired_leases(self) -> ReapResult:
        """Recover messages orphaned by a worker that died mid-flight.

        Three moves, in order: expire the lease, requeue what still has attempts
        left, and dead-letter what does not. The last step is the one that keeps
        an exhausted TIMEOUT row from sitting in the table forever.
        """
        now = utcnow()
        msg = Message.__table__
        max_attempts = self._settings.max_attempts

        async with self._db.begin() as session:
            expired = await session.execute(
                update(msg)
                .where(
                    msg.c.state == MessageState.PROCESSING.value,
                    msg.c.lease_expires_at.is_not(None),
                    msg.c.lease_expires_at < now,
                )
                .values(
                    state=MessageState.TIMEOUT.value,
                    last_error=func.coalesce(msg.c.last_error, LEASE_EXPIRED_REASON),
                    lease_expires_at=None,
                    updated_at=now,
                )
            )

            requeued = await session.execute(
                update(msg)
                .where(
                    msg.c.state == MessageState.TIMEOUT.value,
                    msg.c.attempt_count < max_attempts,
                )
                .values(
                    state=MessageState.RECEIVED.value,
                    available_at=now
                    + timedelta(seconds=self._settings.backoff_base_seconds),
                    processing_started_at=None,
                    updated_at=now,
                )
            )

            exhausted = (
                (
                    await session.execute(
                        update(msg)
                        .where(
                            msg.c.state == MessageState.TIMEOUT.value,
                            msg.c.attempt_count >= max_attempts,
                        )
                        .values(
                            state=MessageState.FAILED.value,
                            dead_lettered=True,
                            updated_at=now,
                        )
                        .returning(
                            msg.c.id,
                            msg.c.user_id,
                            msg.c.conversation_id,
                            msg.c.seq,
                            msg.c.attempt_count,
                            msg.c.last_error,
                        )
                    )
                )
                .mappings()
                .all()
            )

            for row in exhausted:
                await session.execute(
                    self._dead_letter_insert(
                        message_id=row["id"],
                        user_id=row["user_id"],
                        conversation_id=row["conversation_id"],
                        seq=row["seq"],
                        attempt_count=row["attempt_count"],
                        reason=row["last_error"] or LEASE_EXPIRED_REASON,
                    )
                )

        return ReapResult(
            timed_out=expired.rowcount or 0,
            requeued=requeued.rowcount or 0,
            dead_lettered=len(exhausted),
        )

    # -- introspection --------------------------------------------------

    async def depth(self) -> dict[str, int]:
        """Queue depth by state, plus totals. Backs the /metrics endpoint."""
        msg = Message.__table__
        async with self._db.session() as session:
            rows = (
                await session.execute(
                    select(msg.c.state, func.count()).group_by(msg.c.state)
                )
            ).all()
            dead = (
                await session.execute(
                    select(func.count()).select_from(DeadLetter.__table__)
                )
            ).scalar_one()
            conversations = (
                await session.execute(
                    select(func.count()).select_from(Conversation.__table__)
                )
            ).scalar_one()
        counts = {state.value: 0 for state in MessageState}
        for state, count in rows:
            counts[state] = count
        counts["dead_letters"] = int(dead)
        counts["conversations"] = int(conversations)
        counts["total"] = sum(counts[state.value] for state in MessageState)
        return counts

    # -- helpers --------------------------------------------------------

    def _dead_letter_insert(self, **values: object) -> Insert:
        """Insert a dead-letter row, ignoring a duplicate for the same message."""
        table = DeadLetter.__table__
        payload = {"id": uuid.uuid4(), "created_at": utcnow(), **values}
        dialect = self._db.dialect
        if dialect == "postgresql":
            from sqlalchemy.dialects.postgresql import insert as dialect_insert
        elif dialect == "sqlite":
            from sqlalchemy.dialects.sqlite import insert as dialect_insert
        else:  # pragma: no cover - only pg and sqlite are supported
            return insert(table).values(**payload)
        return (
            dialect_insert(table)
            .values(**payload)
            .on_conflict_do_nothing(index_elements=["message_id"])
        )
