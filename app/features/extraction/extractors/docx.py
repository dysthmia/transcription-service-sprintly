"""Извлечение абзацев и таблиц из DOCX-файлов."""

from __future__ import annotations

from pathlib import Path

from docx import Document
from docx.oxml.table import CT_Tbl
from docx.oxml.text.paragraph import CT_P
from docx.table import Table
from docx.text.paragraph import Paragraph

from app.core.exceptions import ContentError
from app.features.extraction.base import DetectedType, ExtractionResult


class DocxExtractor:
    """Сохраняет исходный порядок абзацев и таблиц документа."""

    detected_type = DetectedType.DOCX

    def extract(self, source: Path) -> ExtractionResult:
        """Преобразует абзацы и таблицы DOCX в текст и Markdown-таблицы."""
        try:
            document = Document(source)
        except Exception as error:
            raise ContentError("corrupted_file", "Не удалось прочитать DOCX-файл.") from error

        blocks: list[str] = []
        table_count = 0
        for child in document.element.body.iterchildren():
            if isinstance(child, CT_P):
                text = Paragraph(child, document).text.strip()
                if text:
                    blocks.append(text)
            elif isinstance(child, CT_Tbl):
                table = self._table_to_markdown(Table(child, document))
                if table:
                    blocks.append(table)
                    table_count += 1

        content = "\n\n".join(blocks)
        if not content.strip():
            raise ContentError("empty_content", "В DOCX-файле нет извлекаемого текста.")

        return ExtractionResult(
            content=content,
            detected_type=self.detected_type,
            metadata={"table_count": table_count},
        )

    @staticmethod
    def _table_to_markdown(table: Table) -> str:
        rows = [
            [DocxExtractor._normalize_cell(cell.text) for cell in row.cells]
            for row in table.rows
        ]
        if not rows or not rows[0]:
            return ""

        column_count = max(len(row) for row in rows)
        normalized_rows = [row + [""] * (column_count - len(row)) for row in rows]
        header = normalized_rows[0]
        separator = ["---"] * column_count
        return "\n".join(
            "| " + " | ".join(row) + " |"
            for row in (header, separator, *normalized_rows[1:])
        )

    @staticmethod
    def _normalize_cell(value: str) -> str:
        return " ".join(value.split()).replace("|", "\\|")
