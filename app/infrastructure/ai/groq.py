"""HTTP-клиент Groq Speech-to-Text API."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import httpx

from app.core.config import Settings
from app.core.exceptions import ContentError, TransientExternalError


class GroqClient:
    """Отправляет аудиофайлы в OpenAI-compatible transcription endpoint Groq."""

    def __init__(self, settings: Settings, client: httpx.AsyncClient | None = None) -> None:
        self._model = settings.stt_model
        self._api_key = settings.groq_api_key.get_secret_value()
        self._base_url = settings.groq_base_url.rstrip("/")
        self._client = client or httpx.AsyncClient(timeout=settings.api_request_timeout_seconds)
        self._owns_client = client is None

    async def close(self) -> None:
        if self._owns_client:
            await self._client.aclose()

    async def transcribe(self, source: Path) -> str:
        """Возвращает распознанный текст, оставляя язык исходного материала неизменным."""
        try:
            with source.open("rb") as audio:
                response = await self._client.post(
                    f"{self._base_url}/audio/transcriptions",
                    headers={"Authorization": f"Bearer {self._api_key}"},
                    data={"model": self._model, "response_format": "json"},
                    files={"file": (source.name, audio, "audio/mpeg")},
                )
        except (OSError, httpx.RequestError) as error:
            raise TransientExternalError(
                "groq_temporary_failure", "Сервис распознавания речи временно недоступен."
            ) from error

        if response.status_code == 429 or response.status_code >= 500:
            raise TransientExternalError(
                "groq_temporary_failure", "Сервис распознавания речи временно недоступен."
            )
        if response.status_code >= 400:
            raise ContentError("invalid_media", "Сервис распознавания не принял аудиофайл.")

        try:
            payload: dict[str, Any] = response.json()
            text = payload["text"]
        except (KeyError, TypeError, ValueError) as error:
            raise ContentError(
                "invalid_model_output", "Сервис распознавания вернул некорректный ответ."
            ) from error

        if not isinstance(text, str) or not text.strip():
            raise ContentError("empty_content", "В аудио нет содержательной речи.")
        return text.strip()
