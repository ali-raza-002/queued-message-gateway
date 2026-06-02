"""The seam between the delivery machinery and whatever actually answers a message.

Everything downstream of the queue talks to a ``ReplyProvider``. That is what
lets the suite run with no credentials, and what lets a real model be swapped in
by changing one environment variable.
"""

from __future__ import annotations

import uuid
from dataclasses import dataclass
from typing import Protocol, runtime_checkable


@dataclass(frozen=True, slots=True)
class ReplyRequest:
    """Everything a provider is allowed to see about one delivery attempt."""

    message_id: uuid.UUID
    user_id: str
    conversation_id: str
    seq: int
    text: str
    attempt: int

    @property
    def idempotency_key(self) -> str:
        """Stable across retries of the same message.

        Delivery is at-least-once, so a provider that charges money or sends an
        SMS must dedupe on this rather than on the attempt number.
        """
        return f"{self.user_id}:{self.conversation_id}:{self.seq}"


class ProviderError(RuntimeError):
    """Transient failure. The message goes back on the queue with backoff."""


class PermanentProviderError(ProviderError):
    """The payload will never succeed. Dead-letter it without burning retries.

    This is the poison-message escape hatch: one malformed message is removed
    from the head of its conversation instead of cycling until MAX_ATTEMPTS.
    """


@runtime_checkable
class ReplyProvider(Protocol):
    """Produces the assistant reply for one message."""

    name: str

    async def generate(self, request: ReplyRequest) -> str: ...

    async def aclose(self) -> None: ...
