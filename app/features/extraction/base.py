"""Общие типы для извлечения содержимого материалов."""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import StrEnum
from pathlib import Path
from typing import Protocol


class DetectedType(StrEnum):
    """Фактические типы материалов, определённые по их содержимому."""

    TXT = "txt"
    DOCX = "docx"
    PDF = "pdf"
    JPEG = "jpeg"
    PNG = "png"
    MP3 = "mp3"
    MP4 = "mp4"


@dataclass(frozen=True, slots=True)
class ExtractionResult:
    """Результат извлечения, передаваемый на следующий этап pipeline."""

    content: str
    detected_type: DetectedType
    metadata: dict[str, int | bool] = field(default_factory=dict)


class Extractor(Protocol):
    """Минимальный контракт для extractor-а одного типа материала."""

    detected_type: DetectedType

    def extract(self, source: Path) -> ExtractionResult:
        """Извлекает содержимое из локального файла."""
