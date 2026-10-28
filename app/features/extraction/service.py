"""Маршрутизация извлечения для обычных документов."""

from __future__ import annotations

from pathlib import Path

from app.core.config import Settings
from app.core.exceptions import ContentError
from app.features.extraction.base import DetectedType, ExtractionResult
from app.features.extraction.detector import detect_type
from app.features.extraction.extractors.docx import DocxExtractor
from app.features.extraction.extractors.image import ImageExtractor, VisionClient
from app.features.extraction.extractors.media import MediaExtractor, SpeechToTextClient
from app.features.extraction.extractors.pdf import PdfExtractor
from app.features.extraction.extractors.txt import TxtExtractor
from app.features.extraction.extractors.youtube import YoutubeExtractor
from app.infrastructure.ai.deepseek import DeepSeekClient


class ExtractionService:
    """Выбирает готовый extractor для текстовых документов."""

    def __init__(
        self,
        settings: Settings,
        vision_client: VisionClient | None = None,
        stt_client: SpeechToTextClient | None = None,
    ) -> None:
        self._settings = settings
        self._vision_client = vision_client or DeepSeekClient(settings)
        self._stt_client = stt_client

    async def extract(self, source: Path) -> ExtractionResult:
        """Проверяет размер, определяет тип и запускает подходящий extractor."""
        self._check_file_size(source)
        detected_type = detect_type(source)

        if detected_type is DetectedType.TXT:
            return TxtExtractor().extract(source)
        if detected_type is DetectedType.DOCX:
            return DocxExtractor().extract(source)
        if detected_type is DetectedType.PDF:
            return await PdfExtractor(self._settings.max_pdf_pages, self._vision_client).extract(source)
        if detected_type in {DetectedType.PNG, DetectedType.JPEG}:
            return await ImageExtractor(self._vision_client, detected_type).extract(source)
        if detected_type in {DetectedType.MP3, DetectedType.MP4}:
            return await self._media_extractor(detected_type).extract(source)

        raise ContentError(
            "unsupported_format",
            "Извлечение для этого типа материала пока недоступно.",
        )

    async def extract_youtube(self, source_url: str) -> ExtractionResult:
        """Скачивает разрешённую аудиодорожку YouTube и передаёт её в общий STT pipeline."""
        return await YoutubeExtractor(
            self._media_extractor(DetectedType.YOUTUBE)
        ).extract(source_url)

    def _media_extractor(self, detected_type: DetectedType) -> MediaExtractor:
        stt_client = self._stt_client
        if stt_client is None:
            from app.infrastructure.ai.groq import GroqClient

            stt_client = GroqClient(self._settings)

        return MediaExtractor(
            stt_client,
            detected_type,
            self._settings.max_media_file_size_mb,
            self._settings.max_media_duration_seconds,
            self._settings.stt_chunk_seconds,
        )

    def _check_file_size(self, source: Path) -> None:
        max_size = self._settings.max_document_file_size_mb * 1024 * 1024
        if source.stat().st_size > max_size:
            raise ContentError("file_too_large", "Превышен допустимый размер документа.")
