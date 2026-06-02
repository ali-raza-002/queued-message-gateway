"""Optional real provider. Never constructed unless an API key is configured."""

from __future__ import annotations

import httpx

from .base import PermanentProviderError, ProviderError, ReplyRequest

SYSTEM_PROMPT = (
    "You are a concise assistant inside a messaging app. "
    "Answer the user's latest message in at most three sentences."
)
#: 4xx other than 429 means the request itself is wrong: retrying cannot help.
_RETRYABLE_STATUS_FLOOR = 500
_RATE_LIMIT_STATUS = 429


class OpenAIReplyProvider:
    """Chat-completions client for any OpenAI-compatible endpoint."""

    name = "openai"

    def __init__(
        self,
        *,
        api_key: str,
        base_url: str,
        model: str,
        timeout_seconds: float,
        client: httpx.AsyncClient | None = None,
    ) -> None:
        self._model = model
        self._owns_client = client is None
        self._client = client or httpx.AsyncClient(
            base_url=base_url.rstrip("/"),
            timeout=timeout_seconds,
            headers={"Authorization": f"Bearer {api_key}"},
        )

    async def generate(self, request: ReplyRequest) -> str:
        payload = {
            "model": self._model,
            "messages": [
                {"role": "system", "content": SYSTEM_PROMPT},
                {"role": "user", "content": request.text},
            ],
        }
        try:
            response = await self._client.post(
                "/chat/completions",
                json=payload,
                # Lets the upstream dedupe retries of the same message.
                headers={"Idempotency-Key": request.idempotency_key},
            )
        except httpx.HTTPError as exc:  # network, timeout, DNS
            raise ProviderError(f"openai transport error: {exc}") from exc

        if response.status_code >= _RETRYABLE_STATUS_FLOOR or (
            response.status_code == _RATE_LIMIT_STATUS
        ):
            raise ProviderError(f"openai returned {response.status_code}")
        if response.status_code >= httpx.codes.BAD_REQUEST:
            raise PermanentProviderError(
                f"openai rejected the request with {response.status_code}"
            )

        try:
            content = response.json()["choices"][0]["message"]["content"]
        except (KeyError, IndexError, ValueError) as exc:
            raise ProviderError(f"unexpected openai response shape: {exc}") from exc
        if not isinstance(content, str) or not content.strip():
            raise ProviderError("openai returned an empty completion")
        return content.strip()

    async def aclose(self) -> None:
        if self._owns_client:
            await self._client.aclose()
