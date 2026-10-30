"""Извлечение текстового слоя и простых таблиц из PDF."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pymupdf

from app.core.exceptions import ContentError
from app.features.extraction.base import DetectedType, ExtractionResult
from app.features.extraction.extractors.image import VisionClient


@dataclass(frozen=True, slots=True)
class _PdfPage:
    text: str
    tables: list[str]
    vision_image: bytes | None


class PdfExtractor:
    """Работает только с text layer PDF и не запускает OCR."""

    detected_type = DetectedType.PDF

    def __init__(self, max_pages: int, vision_client: VisionClient) -> None:
        self._max_pages = max_pages
        self._vision_client = vision_client

    async def extract(self, source: Path) -> ExtractionResult:
        """Извлекает text layer и применяет vision лишь к почти пустым страницам."""
        try:
            page_count, pages, table_count = await asyncio.to_thread(self._read_pages, source)
        except ContentError:
            raise
        except Exception as error:
            raise ContentError("corrupted_file", "Не удалось прочитать PDF-файл.") from error

        page_contents: list[str] = []
        vision_page_count = 0
        for page in pages:
            page_content = "\n\n".join(part for part in [page.text, *page.tables] if part)
            if page.vision_image is not None:
                page_content = await self._extract_page_with_vision(page.vision_image)
                vision_page_count += 1
            if page_content:
                page_contents.append(page_content)

        content = "\n\n".join(page_contents)
        return ExtractionResult(
            content=content,
            detected_type=self.detected_type,
            metadata={
                "page_count": page_count,
                "table_count": table_count,
                "needs_vision_fallback": vision_page_count > 0,
                "low_text_page_count": vision_page_count,
            },
        )

    async def _extract_page_with_vision(self, image: bytes) -> str:
        """Передаёт заранее отрендеренную страницу в Vision без временного файла."""
        return await self._vision_client.extract_vision_markdown(image, "image/png")

    def _read_pages(self, source: Path) -> tuple[int, list[_PdfPage], int]:
        """Выполняет блокирующее чтение PDF в worker thread."""
        with pymupdf.open(source) as document:
            page_count = document.page_count
            if page_count > self._max_pages:
                raise ContentError("file_too_large", "Превышено допустимое число страниц PDF.")

            pages: list[_PdfPage] = []
            table_count = 0
            for page in document:
                page_text = page.get_text("text", sort=True).strip()
                tables = self._extract_tables(page)
                table_count += len(tables)
                vision_image = self._render_page(page) if len(page_text) < 20 and not tables else None
                pages.append(_PdfPage(page_text, tables, vision_image))

        return page_count, pages, table_count

    @staticmethod
    def _render_page(page: pymupdf.Page) -> bytes:
        return page.get_pixmap(matrix=pymupdf.Matrix(2, 2), alpha=False).tobytes("png")

    @staticmethod
    def _extract_tables(page: pymupdf.Page) -> list[str]:
        """Возвращает только таблицы с регулярной структурой строк и столбцов."""
        try:
            finder = page.find_tables()
        except (AttributeError, RuntimeError, ValueError):
            return []

        tables: list[str] = []
        for table in finder.tables:
            markdown = PdfExtractor._table_to_markdown(table.extract())
            if markdown:
                tables.append(markdown)
        return tables

    @staticmethod
    def _table_to_markdown(rows: list[list[Any]]) -> str:
        if len(rows) < 2 or not rows[0]:
            return ""

        column_count = len(rows[0])
        if any(len(row) != column_count for row in rows):
            return ""

        normalized_rows = [
            ["" if value is None else " ".join(str(value).split()).replace("|", "\\|") for value in row]
            for row in rows
        ]
        return "\n".join(
            "| " + " | ".join(row) + " |"
            for row in (normalized_rows[0], ["---"] * column_count, *normalized_rows[1:])
        )
