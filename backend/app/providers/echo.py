"""Credential-free provider used by the tests and by a fresh clone.

Deterministic on purpose: the same text always yields the same reply, so
assertions can be exact rather than "contains something".
"""

from __future__ import annotations

import hashlib

from .base import PermanentProviderError, ReplyRequest

#: Picked by a hash of the message text, so the choice is stable per input.
_TEMPLATES: tuple[str, ...] = (
    "Here is what I can tell you about {topic}.",
    "Short answer on {topic}: it depends on your filing status.",
    "About {topic} - the relevant rule is the standard deduction.",
    "On {topic}, the usual starting point is your taxable income.",
)

MAX_TOPIC_CHARS = 80


class EchoReplyProvider:
    """Builds a reply from the message text alone. No network, no API key."""

    name = "echo"

    async def generate(self, request: ReplyRequest) -> str:
        topic = request.text.strip()
        if not topic:
            raise PermanentProviderError("empty message text cannot be answered")
        digest = hashlib.sha256(topic.encode("utf-8")).digest()
        template = _TEMPLATES[digest[0] % len(_TEMPLATES)]
        return template.format(topic=topic[:MAX_TOPIC_CHARS])

    async def aclose(self) -> None:
        return None
