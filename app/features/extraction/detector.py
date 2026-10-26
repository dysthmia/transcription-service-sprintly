"""Определение формата файла по содержимому."""

from __future__ import annotations

from pathlib import Path
from zipfile import BadZipFile, ZipFile

from app.core.exceptions import ContentError
from app.features.extraction.base import DetectedType

_MIME_TYPES = {
    "text/plain": DetectedType.TXT,
    "application/pdf": DetectedType.PDF,
    "image/jpeg": DetectedType.JPEG,
    "image/png": DetectedType.PNG,
    "audio/mpeg": DetectedType.MP3,
    "audio/mp3": DetectedType.MP3,
    "video/mp4": DetectedType.MP4,
    "application/vnd.openxmlformats-officedocument.wordprocessingml.document": DetectedType.DOCX,
}


def detect_type(source: Path) -> DetectedType:
    """Возвращает тип файла, проверяя MIME через libmagic и структуру DOCX."""
    mime_type = _mime_type(source)
    if mime_type in {
        "application/zip",
        "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
    }:
        if _is_docx(source):
            return DetectedType.DOCX
        if mime_type != "application/zip":
            raise ContentError("corrupted_file", "DOCX-файл имеет некорректную структуру.")

    detected_type = _MIME_TYPES.get(mime_type)
    if detected_type is None:
        raise ContentError("unsupported_format", "Формат исходного файла не поддерживается.")
    return detected_type


def _mime_type(source: Path) -> str:
    """Определяет MIME-тип через python-magic без доверия к расширению файла."""
    try:
        import magic
    except ImportError as error:
        raise RuntimeError("Для определения формата требуется системная библиотека libmagic.") from error

    return str(magic.from_file(str(source), mime=True))


def _is_docx(source: Path) -> bool:
    """Проверяет обязательные части OOXML-документа внутри ZIP-контейнера."""
    try:
        with ZipFile(source) as archive:
            files = set(archive.namelist())
    except BadZipFile:
        return False

    return {"[Content_Types].xml", "word/document.xml"}.issubset(files)
