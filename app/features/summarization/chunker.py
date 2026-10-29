"""Разбиение длинного текста на части для запросов к модели."""

from __future__ import annotations

import re


def split_into_chunks(text: str, max_chars: int) -> list[str]:
    """Делит текст по абзацам, переходя к строкам только для длинного абзаца."""
    if max_chars <= 0:
        raise ValueError("max_chars must be positive")

    normalized = text.strip()
    if not normalized:
        return []
    if len(normalized) <= max_chars:
        return [normalized]

    chunks: list[str] = []
    current = ""
    paragraphs = (part.strip() for part in re.split(r"\n\s*\n", normalized))
    for paragraph in paragraphs:
        if not paragraph:
            continue
        if len(paragraph) > max_chars:
            if current:
                chunks.append(current)
                current = ""
            chunks.extend(_split_long_paragraph(paragraph, max_chars))
            continue

        candidate = paragraph if not current else f"{current}\n\n{paragraph}"
        if len(candidate) <= max_chars:
            current = candidate
        else:
            chunks.append(current)
            current = paragraph

    if current:
        chunks.append(current)
    return chunks


def _split_long_paragraph(paragraph: str, max_chars: int) -> list[str]:
    """Сохраняет строки целиком, если только одна строка не превышает лимит."""
    chunks: list[str] = []
    current = ""
    for line in paragraph.splitlines():
        if len(line) > max_chars:
            if current:
                chunks.append(current)
                current = ""
            chunks.extend(_split_long_line(line, max_chars))
            continue

        candidate = line if not current else f"{current}\n{line}"
        if len(candidate) <= max_chars:
            current = candidate
        else:
            chunks.append(current)
            current = line
    if current:
        chunks.append(current)
    return chunks


def _split_long_line(line: str, max_chars: int) -> list[str]:
    """Делит очень длинную строку по пробелам; срез — только крайний случай."""
    parts: list[str] = []
    remaining = line
    while len(remaining) > max_chars:
        boundary = remaining.rfind(" ", 0, max_chars + 1)
        if boundary <= 0:
            boundary = max_chars
        parts.append(remaining[:boundary].rstrip())
        remaining = remaining[boundary:].lstrip()
    if remaining:
        parts.append(remaining)
    return parts
