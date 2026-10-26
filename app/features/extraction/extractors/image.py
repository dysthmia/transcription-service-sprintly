"""Vision-извлечение текста из PNG и JPEG."""

from __future__ import annotations

from pathlib import Path
from typing import Protocol

from app.core.exceptions import ContentError
from app.features.extraction.base import DetectedType, ExtractionResult


class VisionClient(Protocol):
    async def extract_vision_markdown(self, image: bytes, mime_type: str) -> str: ...


class ImageExtractor:
    """Передаёт изображение vision-модели и возвращает её Markdown-ответ."""

    def __init__(
        self, vision_client: VisionClient, detected_type: DetectedType
    ) -> None:
        self._vision_client = vision_client
        self.detected_type = detected_type

    async def extract(self, source: Path) -> ExtractionResult:
        mime_type = (
            "image/png" if self.detected_type is DetectedType.PNG else "image/jpeg"
        )
        content = await self._vision_client.extract_vision_markdown(
            source.read_bytes(), mime_type
        )
        if not content.strip():
            raise ContentError(
                "empty_content", "На изображении нет извлекаемого содержимого."
            )
        return ExtractionResult(content=content, detected_type=self.detected_type)
