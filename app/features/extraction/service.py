"""Маршрутизация извлечения для обычных документов."""

from __future__ import annotations

from pathlib import Path

from app.core.config import Settings
from app.core.exceptions import ContentError
from app.features.extraction.base import DetectedType, ExtractionResult
from app.features.extraction.detector import detect_type
from app.features.extraction.extractors.docx import DocxExtractor
from app.features.extraction.extractors.pdf import PdfExtractor
from app.features.extraction.extractors.txt import TxtExtractor


class ExtractionService:
    """Выбирает готовый extractor для текстовых документов."""

    def __init__(self, settings: Settings) -> None:
        self._settings = settings

    def extract(self, source: Path) -> ExtractionResult:
        """Проверяет размер, определяет тип и запускает подходящий extractor."""
        self._check_file_size(source)
        detected_type = detect_type(source)

        if detected_type is DetectedType.TXT:
            return TxtExtractor().extract(source)
        if detected_type is DetectedType.DOCX:
            return DocxExtractor().extract(source)
        if detected_type is DetectedType.PDF:
            return PdfExtractor(self._settings.max_pdf_pages).extract(source)

        raise ContentError(
            "unsupported_format",
            "Извлечение для этого типа материала пока недоступно.",
        )

    def _check_file_size(self, source: Path) -> None:
        max_size = self._settings.max_document_file_size_mb * 1024 * 1024
        if source.stat().st_size > max_size:
            raise ContentError("file_too_large", "Превышен допустимый размер документа.")
