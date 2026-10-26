"""Извлечение текста из TXT-файлов."""

from __future__ import annotations

from pathlib import Path

from app.core.exceptions import ContentError
from app.features.extraction.base import DetectedType, ExtractionResult


class TxtExtractor:
    """Читает текст в UTF-8 и распространённых русских кодировках."""

    detected_type = DetectedType.TXT

    def extract(self, source: Path) -> ExtractionResult:
        """Декодирует файл и возвращает непустой текст."""
        content = self._decode(source.read_bytes())
        if not content.strip():
            raise ContentError("empty_content", "В текстовом файле нет содержательного текста.")

        return ExtractionResult(content=content, detected_type=self.detected_type)

    @staticmethod
    def _decode(content: bytes) -> str:
        for encoding in ("utf-8-sig", "utf-8", "cp1251", "koi8-r"):
            try:
                return content.decode(encoding)
            except UnicodeDecodeError:
                continue

        raise ContentError("corrupted_file", "Не удалось определить кодировку текстового файла.")
