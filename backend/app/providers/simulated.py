"""The assignment's simulated backend: random delay, random failure.

It wraps a real provider rather than replacing it, so the delay/failure
behaviour and the reply generation stay independently testable. Seed it and the
whole thing becomes reproducible.
"""

from __future__ import annotations

import asyncio
import random

from .base import ProviderError, ReplyProvider, ReplyRequest


class SimulatedReplyProvider:
    """Adds latency and a failure rate on top of ``inner``."""

    name = "simulated"

    def __init__(
        self,
        inner: ReplyProvider,
        *,
        min_delay_seconds: float,
        max_delay_seconds: float,
        fail_rate: float,
        seed: int | None = None,
    ) -> None:
        if max_delay_seconds < min_delay_seconds:
            raise ValueError("max_delay_seconds must be >= min_delay_seconds")
        if not 0.0 <= fail_rate <= 1.0:
            raise ValueError("fail_rate must be between 0 and 1")
        self._inner = inner
        self._min_delay = min_delay_seconds
        self._max_delay = max_delay_seconds
        self._fail_rate = fail_rate
        # A private Random instance: seeding this must not disturb global random.
        self._random = random.Random(seed)

    async def generate(self, request: ReplyRequest) -> str:
        delay = self._random.uniform(self._min_delay, self._max_delay)
        if delay > 0:
            await asyncio.sleep(delay)
        if self._random.random() < self._fail_rate:
            raise ProviderError(
                f"simulated upstream failure after {delay:.1f}s "
                f"(attempt {request.attempt})"
            )
        return await self._inner.generate(request)

    async def aclose(self) -> None:
        await self._inner.aclose()
