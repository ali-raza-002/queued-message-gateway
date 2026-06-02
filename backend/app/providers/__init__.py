"""Reply provider registry.

``build_provider`` is the only place that knows which concrete class a name maps
to, so adding a channel means adding a module and one registry entry.
"""

from __future__ import annotations

from ..settings import Settings
from .base import (
    PermanentProviderError,
    ProviderError,
    ReplyProvider,
    ReplyRequest,
)
from .echo import EchoReplyProvider
from .openai import OpenAIReplyProvider
from .simulated import SimulatedReplyProvider

__all__ = [
    "EchoReplyProvider",
    "OpenAIReplyProvider",
    "PermanentProviderError",
    "ProviderError",
    "ReplyProvider",
    "ReplyRequest",
    "SimulatedReplyProvider",
    "build_provider",
]


def _build_base_provider(settings: Settings) -> ReplyProvider:
    if settings.openai_api_key:
        return OpenAIReplyProvider(
            api_key=settings.openai_api_key,
            base_url=settings.openai_base_url,
            model=settings.openai_model,
            timeout_seconds=settings.openai_timeout_seconds,
        )
    return EchoReplyProvider()


def build_provider(settings: Settings) -> ReplyProvider:
    """Resolve ``settings.reply_provider`` into a live provider.

    ``simulated`` deliberately layers the assignment's delay/failure injection
    over whichever real provider is configured, which is why it is the default:
    the retry, backoff and dead-letter paths are exercised out of the box.
    """
    if settings.reply_provider == "echo":
        return EchoReplyProvider()
    if settings.reply_provider == "openai":
        if not settings.openai_api_key:
            raise ValueError("reply_provider=openai requires openai_api_key")
        return OpenAIReplyProvider(
            api_key=settings.openai_api_key,
            base_url=settings.openai_base_url,
            model=settings.openai_model,
            timeout_seconds=settings.openai_timeout_seconds,
        )
    return SimulatedReplyProvider(
        _build_base_provider(settings),
        min_delay_seconds=settings.reply_min_delay_seconds,
        max_delay_seconds=settings.reply_max_delay_seconds,
        fail_rate=settings.reply_fail_rate,
        seed=settings.reply_seed,
    )
