"""Создание подробного русскоязычного Markdown-конспекта."""

from __future__ import annotations

import re
from typing import Protocol

from app.core.config import Settings
from app.core.exceptions import ContentError, TransientExternalError
from app.features.summarization.chunker import split_into_chunks
from app.features.summarization.prompts import (
    build_chunk_summary_prompt,
    build_merge_prompt,
    build_summary_prompt,
)
from app.infrastructure.ai.deepseek import DeepSeekClient


class TextCompletionClient(Protocol):
    async def complete_text(self, prompt: str) -> str: ...


class SummarizationService:
    """Оркестрирует один или несколько запросов DeepSeek для создания конспекта."""

    def __init__(self, settings: Settings, client: TextCompletionClient | None = None) -> None:
        self._chunk_size = settings.summary_chunk_size
        self._client = client or DeepSeekClient(settings)

    async def summarize(self, source: str) -> str:
        """Возвращает подробный русскоязычный Markdown-конспект материала."""
        chunks = split_into_chunks(source, self._chunk_size)
        if not chunks:
            raise ContentError("llm_invalid_output", "Материал для создания конспекта пуст.")

        if len(chunks) == 1:
            return await self._complete(build_summary_prompt(chunks[0]))

        partial_summaries = [
            await self._complete(build_chunk_summary_prompt(chunk, number, len(chunks)))
            for number, chunk in enumerate(chunks, start=1)
        ]
        merged_parts = _remove_exact_duplicate_blocks(partial_summaries)
        return await self._complete(build_merge_prompt(merged_parts))

    async def _complete(self, prompt: str) -> str:
        try:
            result = await self._client.complete_text(prompt)
        except TransientExternalError as error:
            raise TransientExternalError(
                "llm_unavailable", "Сервис создания конспекта временно недоступен."
            ) from error
        except ContentError as error:
            if error.error_code in {"invalid_model_output", "empty_content"}:
                raise ContentError(
                    "llm_invalid_output", "Сервис создания конспекта вернул некорректный ответ."
                ) from error
            raise

        if not isinstance(result, str) or not result.strip():
            raise ContentError(
                "llm_invalid_output", "Сервис создания конспекта вернул пустой ответ."
            )
        return result.strip()


def _remove_exact_duplicate_blocks(parts: list[str]) -> list[str]:
    """Удаляет только полностью совпадающие блоки, не меняя таблицы и формулы."""
    seen: set[str] = set()
    unique_parts: list[str] = []
    for part in parts:
        blocks = re.split(r"\n\s*\n", part)
        unique_blocks = []
        for block in blocks:
            normalized = block.strip()
            if normalized and normalized not in seen:
                seen.add(normalized)
                unique_blocks.append(normalized)
        if unique_blocks:
            unique_parts.append("\n\n".join(unique_blocks))
    return unique_parts
