"""The delivery worker.

Three things about this loop are deliberate and are the reason it is a class
rather than a ``while True``:

1. **It never blocks on the slowest job in a batch.** Each cycle claims only as
   many messages as there are free concurrency slots and then waits for the
   *first* task to finish, so a 4-minute message does not stall the other seven
   slots or the lease reaper behind it.
2. **One bad message cannot take the process down.** Every attempt is wrapped;
   an unexpected exception fails that one message and the loop carries on.
3. **SIGTERM does not strand work.** In-flight messages get a grace period to
   finish, and whatever is still running when it expires is handed back to the
   queue without consuming a retry.
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
import os
import signal
import uuid

from app.db import Database
from app.providers import (
    PermanentProviderError,
    ProviderError,
    ReplyProvider,
    ReplyRequest,
)
from app.queue import ClaimedMessage, DeliveryQueue, FailureOutcome
from app.settings import Settings

logger = logging.getLogger("worker")

#: Signals a container runtime sends on shutdown.
STOP_SIGNALS = (signal.SIGINT, signal.SIGTERM)


class DeliveryWorker:
    def __init__(
        self,
        database: Database,
        settings: Settings,
        provider: ReplyProvider,
        *,
        queue: DeliveryQueue | None = None,
        worker_id: str | None = None,
    ) -> None:
        self._db = database
        self._settings = settings
        self._provider = provider
        self._queue = queue or DeliveryQueue(database, settings)
        self.worker_id = worker_id or f"{os.getpid()}-{uuid.uuid4().hex[:6]}"
        self._stop = asyncio.Event()
        self._inflight: dict[asyncio.Task, ClaimedMessage] = {}
        self._last_reap = 0.0

    # -- lifecycle ------------------------------------------------------

    @property
    def stopping(self) -> bool:
        return self._stop.is_set()

    @property
    def inflight_count(self) -> int:
        return len(self._inflight)

    def request_stop(self) -> None:
        if not self._stop.is_set():
            logger.info("shutdown_requested", extra={"worker_id": self.worker_id})
            self._stop.set()

    def install_signal_handlers(self) -> None:
        loop = asyncio.get_running_loop()
        for sig in STOP_SIGNALS:
            with contextlib.suppress(NotImplementedError):
                loop.add_signal_handler(sig, self.request_stop)

    async def run(self) -> None:
        self.install_signal_handlers()
        logger.info(
            "worker_started",
            extra={
                "worker_id": self.worker_id,
                "concurrency": self._settings.concurrency,
                "provider": self._provider.name,
                "max_attempts": self._settings.max_attempts,
                "strict_order": self._settings.strict_conversation_order,
            },
        )
        try:
            while not self.stopping:
                await self.run_once()
        finally:
            await self.drain()
            logger.info("worker_stopped", extra={"worker_id": self.worker_id})

    async def run_once(self) -> int:
        """One poll cycle. Returns how many messages were claimed.

        Exposed separately so tests can step the loop without a background task.
        """
        await self._maybe_reap()
        claimed = await self._claim_available()
        for message in claimed:
            task = asyncio.create_task(self._attempt(message))
            self._inflight[task] = message
        await self._await_progress(claimed_any=bool(claimed))
        return len(claimed)

    async def drain(self) -> None:
        """Finish or hand back everything still in flight."""
        if not self._inflight:
            return
        grace = self._settings.shutdown_grace_seconds
        logger.info(
            "draining",
            extra={
                "worker_id": self.worker_id,
                "inflight": len(self._inflight),
                "grace_seconds": grace,
            },
        )
        if grace > 0:
            done, _ = await asyncio.wait(set(self._inflight), timeout=grace)
            for task in done:
                self._inflight.pop(task, None)

        stragglers = dict(self._inflight)
        self._inflight.clear()
        for task in stragglers:
            task.cancel()
        if stragglers:
            await asyncio.gather(*stragglers, return_exceptions=True)
        for message in stragglers.values():
            released = await self._queue.release(message)
            logger.warning(
                "released_on_shutdown",
                extra={
                    "worker_id": self.worker_id,
                    "message_id": message.id,
                    "attempt": message.attempt,
                    "released": released,
                },
            )

    # -- internals ------------------------------------------------------

    async def _maybe_reap(self) -> None:
        loop = asyncio.get_running_loop()
        now = loop.time()
        if now - self._last_reap < self._settings.reap_interval_seconds:
            return
        self._last_reap = now
        result = await self._queue.reap_expired_leases()
        if result.touched:
            logger.warning(
                "leases_reaped",
                extra={
                    "worker_id": self.worker_id,
                    "timed_out": result.timed_out,
                    "requeued": result.requeued,
                    "dead_lettered": result.dead_lettered,
                },
            )

    async def _claim_available(self) -> list[ClaimedMessage]:
        free = self._settings.concurrency - len(self._inflight)
        if free <= 0 or self.stopping:
            return []
        # Never claim more than can be started now: a claimed message is already
        # holding a lease, and one waiting on a semaphore burns that lease.
        return await self._queue.claim(min(free, self._settings.claim_batch))

    async def _await_progress(self, *, claimed_any: bool) -> None:
        timeout = self._settings.poll_interval_seconds
        if self._inflight:
            done, _ = await asyncio.wait(
                set(self._inflight),
                timeout=0 if claimed_any else timeout,
                return_when=asyncio.FIRST_COMPLETED,
            )
            for task in done:
                self._inflight.pop(task, None)
            if done or claimed_any:
                return
        if not claimed_any:
            # Idle: sleep, but wake instantly on SIGTERM.
            with contextlib.suppress(TimeoutError):
                await asyncio.wait_for(self._stop.wait(), timeout=timeout)

    async def _attempt(self, message: ClaimedMessage) -> None:
        """Run one delivery attempt. Never raises."""
        request = ReplyRequest(
            message_id=message.id,
            user_id=message.user_id,
            conversation_id=message.conversation_id,
            seq=message.seq,
            text=message.text,
            attempt=message.attempt,
        )
        try:
            reply = await self._provider.generate(request)
        except asyncio.CancelledError:
            # Shutdown. drain() hands the lease back, so do not mark it failed.
            raise
        except PermanentProviderError as exc:
            await self._finish_failure(message, str(exc), permanent=True)
            return
        except ProviderError as exc:
            await self._finish_failure(message, str(exc), permanent=False)
            return
        except Exception as exc:  # noqa: BLE001 - a poison payload must not kill the loop
            logger.exception(
                "attempt_crashed",
                extra={"worker_id": self.worker_id, "message_id": message.id},
            )
            await self._finish_failure(
                message, f"unhandled error: {exc}", permanent=False
            )
            return

        acknowledged = await self._queue.complete(
            message, reply, provider=self._provider.name
        )
        if acknowledged:
            logger.info(
                "message_completed",
                extra={
                    "worker_id": self.worker_id,
                    "message_id": message.id,
                    "conversation_id": message.conversation_id,
                    "seq": message.seq,
                    "attempt": message.attempt,
                },
            )
        else:
            # The lease expired and someone else owns this message now.
            logger.warning(
                "lease_lost",
                extra={
                    "worker_id": self.worker_id,
                    "message_id": message.id,
                    "attempt": message.attempt,
                },
            )

    async def _finish_failure(
        self, message: ClaimedMessage, error: str, *, permanent: bool
    ) -> None:
        outcome = await self._queue.fail(message, error, permanent=permanent)
        logger.warning(
            "message_failed",
            extra={
                "worker_id": self.worker_id,
                "message_id": message.id,
                "conversation_id": message.conversation_id,
                "seq": message.seq,
                "attempt": message.attempt,
                "permanent": permanent,
                "outcome": outcome.value,
                "error": error,
            },
        )
        if outcome is FailureOutcome.DEAD_LETTERED:
            logger.error(
                "message_dead_lettered",
                extra={"worker_id": self.worker_id, "message_id": message.id},
            )
