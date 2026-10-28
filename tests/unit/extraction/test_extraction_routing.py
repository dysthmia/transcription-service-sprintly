from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace

import pytest

from app.core.exceptions import ContentError, TransientExternalError
from app.features.extraction.base import DetectedType, ExtractionResult
from app.features.extraction.service import ExtractionService


def settings() -> SimpleNamespace:
    return SimpleNamespace(
        max_document_file_size_mb=1,
        max_pdf_pages=3,
        max_media_file_size_mb=2,
        max_media_duration_seconds=60,
        stt_chunk_seconds=30,
    )


class SyncExtractor:
    def __init__(self, result: ExtractionResult) -> None:
        self._result = result

    def extract(self, _: Path) -> ExtractionResult:
        return self._result


class AsyncExtractor:
    def __init__(self, result: ExtractionResult) -> None:
        self._result = result

    async def extract(self, _: Path) -> ExtractionResult:
        return self._result


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("detected_type", "extractor_name"),
    [
        (DetectedType.TXT, "TxtExtractor"),
        (DetectedType.DOCX, "DocxExtractor"),
        (DetectedType.PDF, "PdfExtractor"),
        (DetectedType.PNG, "ImageExtractor"),
        (DetectedType.JPEG, "ImageExtractor"),
        (DetectedType.MP3, "media"),
        (DetectedType.MP4, "media"),
    ],
)
async def test_local_source_routes_to_matching_extractor(
    detected_type: DetectedType,
    extractor_name: str,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    result = ExtractionResult("Первая строка\r\n\r\n\r\nВторая строка", detected_type)
    source = tmp_path / "source"
    source.write_bytes(b"source")
    service = ExtractionService(settings(), vision_client=object())
    monkeypatch.setattr("app.features.extraction.service.detect_type", lambda _: detected_type)

    if extractor_name == "TxtExtractor":
        monkeypatch.setattr(
            "app.features.extraction.service.TxtExtractor", lambda: SyncExtractor(result)
        )
    elif extractor_name == "DocxExtractor":
        monkeypatch.setattr(
            "app.features.extraction.service.DocxExtractor", lambda: SyncExtractor(result)
        )
    elif extractor_name == "media":
        monkeypatch.setattr(service, "_media_extractor", lambda _: AsyncExtractor(result))
    else:
        monkeypatch.setattr(
            f"app.features.extraction.service.{extractor_name}",
            lambda *_: AsyncExtractor(result),
        )

    extracted = await service.extract(source)

    assert extracted.detected_type is detected_type
    assert extracted.content == "Первая строка\n\nВторая строка"


@pytest.mark.asyncio
async def test_youtube_source_routes_directly_to_youtube_extractor(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    result = ExtractionResult("Распознанная речь", DetectedType.YOUTUBE)
    created_with: list[object] = []

    class FakeYoutubeExtractor:
        def __init__(self, audio_pipeline: object) -> None:
            created_with.append(audio_pipeline)

        async def extract(self, source_url: str) -> ExtractionResult:
            assert source_url == "https://www.youtube.com/watch?v=dQw4w9WgXcQ"
            return result

    service = ExtractionService(settings(), vision_client=object())
    media_pipeline = object()
    monkeypatch.setattr(service, "_media_extractor", lambda _: media_pipeline)
    monkeypatch.setattr(
        "app.features.extraction.service.YoutubeExtractor", FakeYoutubeExtractor
    )

    extracted = await service.extract("https://www.youtube.com/watch?v=dQw4w9WgXcQ")

    assert extracted.content == result.content
    assert extracted.detected_type is DetectedType.YOUTUBE
    assert created_with == [media_pipeline]


@pytest.mark.asyncio
async def test_content_error_code_and_transient_semantics_are_preserved(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    source = tmp_path / "source.txt"
    source.write_text("text", encoding="utf-8")
    service = ExtractionService(settings(), vision_client=object())
    monkeypatch.setattr("app.features.extraction.service.detect_type", lambda _: DetectedType.TXT)

    class BrokenExtractor:
        def extract(self, _: Path) -> ExtractionResult:
            raise ContentError("corrupted_file", "Файл повреждён.")

    monkeypatch.setattr("app.features.extraction.service.TxtExtractor", BrokenExtractor)
    with pytest.raises(ContentError) as content_error:
        await service.extract(source)
    assert content_error.value.error_code == "corrupted_file"

    class TemporaryFailureExtractor:
        def extract(self, _: Path) -> ExtractionResult:
            raise TransientExternalError("vision_temporary_failure", "Внешний сервис недоступен.")

    monkeypatch.setattr(
        "app.features.extraction.service.TxtExtractor", TemporaryFailureExtractor
    )
    with pytest.raises(TransientExternalError) as transient_error:
        await service.extract(source)
    assert transient_error.value.error_code == "vision_temporary_failure"
