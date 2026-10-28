from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace

import httpx
import pytest
from pydantic import SecretStr

from app.core.exceptions import ContentError, TransientExternalError
from app.infrastructure.ai.groq import GroqClient


def settings() -> SimpleNamespace:
    return SimpleNamespace(
        groq_base_url="https://groq.example.test/openai/v1",
        groq_api_key=SecretStr("groq-secret"),
        stt_model="whisper-large-v3-turbo",
        api_request_timeout_seconds=10,
    )


@pytest.mark.asyncio
async def test_transcribe_sends_multipart_file_and_returns_source_language_text(tmp_path: Path) -> None:
    source = tmp_path / "speech.mp3"
    source.write_bytes(b"audio")
    seen_request: httpx.Request | None = None

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal seen_request
        seen_request = request
        return httpx.Response(200, json={"text": "Hello, world"})

    async_client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    client = GroqClient(settings(), async_client)

    assert await client.transcribe(source) == "Hello, world"
    assert seen_request is not None
    assert seen_request.url == "https://groq.example.test/openai/v1/audio/transcriptions"
    assert b'name="model"' in seen_request.content
    assert b'filename="speech.mp3"' in seen_request.content
    await async_client.aclose()


@pytest.mark.asyncio
async def test_retryable_groq_status_is_mapped_to_external_error(tmp_path: Path) -> None:
    source = tmp_path / "speech.mp3"
    source.write_bytes(b"audio")
    async_client = httpx.AsyncClient(
        transport=httpx.MockTransport(lambda _: httpx.Response(429))
    )
    client = GroqClient(settings(), async_client)

    with pytest.raises(TransientExternalError, match="groq_temporary_failure"):
        await client.transcribe(source)

    await async_client.aclose()


@pytest.mark.asyncio
async def test_empty_groq_transcript_is_content_error(tmp_path: Path) -> None:
    source = tmp_path / "speech.mp3"
    source.write_bytes(b"audio")
    async_client = httpx.AsyncClient(
        transport=httpx.MockTransport(lambda _: httpx.Response(200, json={"text": " "}))
    )
    client = GroqClient(settings(), async_client)

    with pytest.raises(ContentError, match="empty_content"):
        await client.transcribe(source)

    await async_client.aclose()
