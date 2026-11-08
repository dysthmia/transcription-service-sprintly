"""Небольшой HTTP-клиент DeepSeek для текста и vision."""

from __future__ import annotations

import base64
from time import perf_counter
from typing import Any

import httpx
import structlog

from app.core.config import Settings
from app.core.exceptions import ContentError, TransientExternalError

logger = structlog.get_logger(__name__)


class DeepSeekClient:
    """Отправляет одиночные запросы к совместимому с Chat Completions API."""

    def __init__(
        self, settings: Settings, client: httpx.AsyncClient | None = None
    ) -> None:
        self._model = settings.deepseek_model
        self._api_key = settings.deepseek_api_key.get_secret_value()
        self._base_url = settings.deepseek_base_url.rstrip("/")
        self._client = client or httpx.AsyncClient(
            timeout=settings.api_request_timeout_seconds,
        )
        self._owns_client = client is None

    async def close(self) -> None:
        if self._owns_client:
            await self._client.aclose()

    async def complete_text(self, prompt: str) -> str:
        return await self._request([{"role": "user", "content": prompt}])

    async def extract_vision_markdown(self, image: bytes, mime_type: str) -> str:
        encoded_image = base64.b64encode(image).decode("ascii")
        prompt = (
            "Точно извлеки содержание учебного материала с изображения. Верни только "
            "Markdown без номеров страниц, таймкодов и ссылок на источник. Не пересказывай, "
            "не исправляй и не дополняй материал: сохрани текст, порядок, таблицы в виде "
            "Markdown-таблиц и формулы в LaTeX, когда они различимы."
        )
        content: list[dict[str, Any]] = [
            {"type": "text", "text": prompt},
            {
                "type": "image_url",
                "image_url": {"url": f"data:{mime_type};base64,{encoded_image}"},
            },
        ]
        return await self._request([{"role": "user", "content": content}])

    async def _request(self, messages: list[dict[str, Any]]) -> str:
        started_at = perf_counter()
        try:
            response = await self._client.post(
                f"{self._base_url}/chat/completions",
                headers={"Authorization": f"Bearer {self._api_key}"},
                json={"model": self._model, "messages": messages},
            )
        except (httpx.ConnectError, httpx.ReadTimeout, httpx.TimeoutException) as error:
            self._log_failed_request(started_at, retryable=True, error_code="deepseek_temporary_failure")
            raise TransientExternalError(
                "deepseek_temporary_failure",
                "Сервис распознавания временно недоступен.",
            ) from error

        if response.status_code == 429 or response.status_code >= 500:
            self._log_failed_request(
                started_at,
                status_code=response.status_code,
                retryable=True,
                error_code="deepseek_temporary_failure",
            )
            raise TransientExternalError(
                "deepseek_temporary_failure",
                "Сервис распознавания временно недоступен.",
            )
        if response.status_code >= 400:
            self._log_failed_request(
                started_at,
                status_code=response.status_code,
                retryable=False,
                error_code="deepseek_request_rejected",
            )
            raise ContentError(
                "deepseek_request_rejected", "Сервис распознавания отклонил материал."
            )

        logger.info(
            "external_request.completed",
            provider="deepseek",
            operation="chat_completion",
            duration_ms=round((perf_counter() - started_at) * 1000, 2),
            outcome="success",
            status_code=response.status_code,
            model=self._model,
        )
        try:
            payload = response.json()
            content = payload["choices"][0]["message"]["content"]
        except (IndexError, KeyError, TypeError, ValueError) as error:
            raise ContentError(
                "invalid_model_output",
                "Сервис распознавания вернул некорректный ответ.",
            ) from error

        if not isinstance(content, str) or not content.strip():
            raise ContentError(
                "empty_content", "В материале нет извлекаемого содержимого."
            )
        return content.strip()

    def _log_failed_request(
        self,
        started_at: float,
        *,
        retryable: bool,
        error_code: str,
        status_code: int | None = None,
    ) -> None:
        fields: dict[str, Any] = {
            "provider": "deepseek",
            "operation": "chat_completion",
            "duration_ms": round((perf_counter() - started_at) * 1000, 2),
            "outcome": "failure",
            "model": self._model,
            "retryable": retryable,
            "error_code": error_code,
        }
        if status_code is not None:
            fields["status_code"] = status_code
        logger.warning("external_request.failed", **fields)
