"""Небольшой HTTP-клиент DeepSeek для текста и vision."""

from __future__ import annotations

import base64
from typing import Any

import httpx

from app.core.config import Settings
from app.core.exceptions import ContentError, TransientExternalError


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
        try:
            response = await self._client.post(
                f"{self._base_url}/chat/completions",
                headers={"Authorization": f"Bearer {self._api_key}"},
                json={"model": self._model, "messages": messages},
            )
        except (httpx.ConnectError, httpx.ReadTimeout, httpx.TimeoutException) as error:
            raise TransientExternalError(
                "deepseek_temporary_failure",
                "Сервис распознавания временно недоступен.",
            ) from error

        if response.status_code == 429 or response.status_code >= 500:
            raise TransientExternalError(
                "deepseek_temporary_failure",
                "Сервис распознавания временно недоступен.",
            )
        if response.status_code >= 400:
            raise ContentError(
                "deepseek_request_rejected", "Сервис распознавания отклонил материал."
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
