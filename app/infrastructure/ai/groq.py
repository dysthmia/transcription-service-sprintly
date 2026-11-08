"""HTTP-клиент Groq Speech-to-Text API."""

from __future__ import annotations

from pathlib import Path
from time import perf_counter
from typing import Any

import httpx
import structlog

from app.core.config import Settings
from app.core.exceptions import ContentError, TransientExternalError

logger = structlog.get_logger(__name__)


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
        started_at = perf_counter()
        try:
            with source.open("rb") as audio:
                response = await self._client.post(
                    f"{self._base_url}/audio/transcriptions",
                    headers={"Authorization": f"Bearer {self._api_key}"},
                    data={"model": self._model, "response_format": "json"},
                    files={"file": (source.name, audio, "audio/mpeg")},
                )
        except (OSError, httpx.RequestError) as error:
            self._log_failed_request(started_at, retryable=True, error_code="groq_temporary_failure")
            raise TransientExternalError(
                "groq_temporary_failure", "Сервис распознавания речи временно недоступен."
            ) from error

        if response.status_code == 429 or response.status_code >= 500:
            self._log_failed_request(
                started_at,
                status_code=response.status_code,
                retryable=True,
                error_code="groq_temporary_failure",
            )
            raise TransientExternalError(
                "groq_temporary_failure", "Сервис распознавания речи временно недоступен."
            )
        if response.status_code >= 400:
            self._log_failed_request(
                started_at,
                status_code=response.status_code,
                retryable=False,
                error_code="invalid_media",
            )
            raise ContentError("invalid_media", "Сервис распознавания не принял аудиофайл.")

        logger.info(
            "external_request.completed",
            provider="groq",
            operation="transcribe",
            duration_ms=round((perf_counter() - started_at) * 1000, 2),
            outcome="success",
            status_code=response.status_code,
            model=self._model,
        )
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

    def _log_failed_request(
        self,
        started_at: float,
        *,
        retryable: bool,
        error_code: str,
        status_code: int | None = None,
    ) -> None:
        fields: dict[str, Any] = {
            "provider": "groq",
            "operation": "transcribe",
            "duration_ms": round((perf_counter() - started_at) * 1000, 2),
            "outcome": "failure",
            "model": self._model,
            "retryable": retryable,
            "error_code": error_code,
        }
        if status_code is not None:
            fields["status_code"] = status_code
        logger.warning("external_request.failed", **fields)
