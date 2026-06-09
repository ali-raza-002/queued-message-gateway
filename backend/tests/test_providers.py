"""Provider contract: deterministic offline, and honest about what can be retried."""

from __future__ import annotations

import uuid

import httpx
import pytest

from app.providers import (
    EchoReplyProvider,
    OpenAIReplyProvider,
    PermanentProviderError,
    ProviderError,
    ReplyProvider,
    ReplyRequest,
    SimulatedReplyProvider,
    build_provider,
)


def make_request(text: str = "What is tax deduction?", attempt: int = 1) -> ReplyRequest:
    return ReplyRequest(
        message_id=uuid.UUID(int=1),
        user_id="u1",
        conversation_id="c1",
        seq=7,
        text=text,
        attempt=attempt,
    )


def test_idempotency_key_is_stable_across_attempts():
    assert (
        make_request(attempt=1).idempotency_key == make_request(attempt=3).idempotency_key
    )
    assert make_request().idempotency_key == "u1:c1:7"


async def test_echo_is_deterministic():
    provider = EchoReplyProvider()
    first = await provider.generate(make_request())
    second = await provider.generate(make_request(attempt=2))
    assert first == second
    assert "tax deduction" in first


async def test_echo_rejects_an_unanswerable_payload():
    with pytest.raises(PermanentProviderError):
        await EchoReplyProvider().generate(make_request(text="   "))


async def test_simulated_failures_are_reproducible_for_a_seed():
    def run(seed: int) -> SimulatedReplyProvider:
        return SimulatedReplyProvider(
            EchoReplyProvider(),
            min_delay_seconds=0,
            max_delay_seconds=0,
            fail_rate=0.5,
            seed=seed,
        )

    results_a, results_b = [], []
    for sink, provider in ((results_a, run(7)), (results_b, run(7))):
        for attempt in range(1, 11):
            try:
                await provider.generate(make_request(attempt=attempt))
                sink.append(True)
            except ProviderError:
                sink.append(False)
    assert results_a == results_b
    assert False in results_a and True in results_a


async def test_simulated_with_zero_fail_rate_never_fails():
    provider = SimulatedReplyProvider(
        EchoReplyProvider(),
        min_delay_seconds=0,
        max_delay_seconds=0,
        fail_rate=0.0,
        seed=1,
    )
    for attempt in range(1, 6):
        assert await provider.generate(make_request(attempt=attempt))


def test_simulated_rejects_an_impossible_delay_range():
    with pytest.raises(ValueError, match="max_delay_seconds"):
        SimulatedReplyProvider(
            EchoReplyProvider(),
            min_delay_seconds=5,
            max_delay_seconds=1,
            fail_rate=0.0,
        )


@pytest.mark.parametrize(
    ("status", "expected"),
    [
        (500, ProviderError),
        (503, ProviderError),
        (429, ProviderError),
        (400, PermanentProviderError),
        (401, PermanentProviderError),
    ],
)
async def test_openai_error_classification(status, expected):
    transport = httpx.MockTransport(lambda _: httpx.Response(status, json={}))
    provider = OpenAIReplyProvider(
        api_key="unused",
        base_url="https://example.invalid/v1",
        model="test",
        timeout_seconds=1,
        client=httpx.AsyncClient(
            transport=transport, base_url="https://example.invalid/v1"
        ),
    )
    with pytest.raises(expected):
        await provider.generate(make_request())


async def test_openai_returns_the_completion_text():
    content = "  A deduction lowers taxable income. "
    payload = {"choices": [{"message": {"content": content}}]}
    transport = httpx.MockTransport(lambda _: httpx.Response(200, json=payload))
    provider = OpenAIReplyProvider(
        api_key="unused",
        base_url="https://example.invalid/v1",
        model="test",
        timeout_seconds=1,
        client=httpx.AsyncClient(
            transport=transport, base_url="https://example.invalid/v1"
        ),
    )
    assert await provider.generate(make_request()) == "A deduction lowers taxable income."


async def test_openai_sends_the_idempotency_key():
    seen: dict[str, str] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        seen["key"] = request.headers["Idempotency-Key"]
        return httpx.Response(200, json={"choices": [{"message": {"content": "ok"}}]})

    provider = OpenAIReplyProvider(
        api_key="unused",
        base_url="https://example.invalid/v1",
        model="test",
        timeout_seconds=1,
        client=httpx.AsyncClient(
            transport=httpx.MockTransport(handler), base_url="https://example.invalid/v1"
        ),
    )
    await provider.generate(make_request())
    assert seen["key"] == "u1:c1:7"


def test_registry_default_wraps_echo_in_the_simulator(settings):
    provider = build_provider(settings.model_copy(update={"reply_provider": "simulated"}))
    assert isinstance(provider, SimulatedReplyProvider)
    assert isinstance(provider, ReplyProvider)
    assert provider.name == "simulated"


def test_registry_returns_echo_with_no_credentials(settings):
    assert build_provider(settings).name == "echo"


def test_registry_refuses_openai_without_a_key(settings):
    with pytest.raises(ValueError, match="openai_api_key"):
        build_provider(settings.model_copy(update={"reply_provider": "openai"}))
