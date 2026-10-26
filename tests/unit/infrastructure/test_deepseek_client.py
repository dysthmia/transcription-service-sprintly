from __future__ import annotations

from types import SimpleNamespace

import httpx
import pytest
from pydantic import SecretStr

from app.core.exceptions import ContentError, TransientExternalError
from app.infrastructure.ai.deepseek import DeepSeekClient


def settings() -> SimpleNamespace:
    return SimpleNamespace(
        deepseek_model="deepseek-flash",
        deepseek_api_key=SecretStr("deepseek-secret"),
        deepseek_base_url="https://deepseek.example.test",
        api_request_timeout_seconds=10,
    )


@pytest.mark.asyncio
async def test_vision_request_returns_markdown_without_real_api_call() -> None:
    seen_request: httpx.Request | None = None

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal seen_request
        seen_request = request
        return httpx.Response(
            200, json={"choices": [{"message": {"content": "| $x$ |"}}]}
        )

    client = DeepSeekClient(
        settings(), httpx.AsyncClient(transport=httpx.MockTransport(handler))
    )
    result = await client.extract_vision_markdown(b"image", "image/png")

    assert result == "| $x$ |"
    assert seen_request is not None
    assert seen_request.url == "https://deepseek.example.test/chat/completions"
    assert "data:image/png;base64,aW1hZ2U=" in seen_request.content.decode()
    await client.close()


@pytest.mark.asyncio
async def test_temporary_deepseek_status_is_retryable() -> None:
    async_client = httpx.AsyncClient(
        transport=httpx.MockTransport(lambda _: httpx.Response(429))
    )
    client = DeepSeekClient(settings(), async_client)

    with pytest.raises(TransientExternalError, match="deepseek_temporary_failure"):
        await client.complete_text("text")

    await async_client.aclose()


@pytest.mark.asyncio
async def test_invalid_deepseek_response_is_not_retryable() -> None:
    async_client = httpx.AsyncClient(
        transport=httpx.MockTransport(
            lambda _: httpx.Response(200, json={"choices": []})
        )
    )
    client = DeepSeekClient(settings(), async_client)

    with pytest.raises(ContentError, match="invalid_model_output"):
        await client.complete_text("text")

    await async_client.aclose()
