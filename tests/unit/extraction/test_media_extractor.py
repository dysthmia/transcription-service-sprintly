from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace

import pytest

from app.core.exceptions import ContentError
from app.features.extraction.base import DetectedType
from app.features.extraction.extractors.media import MediaExtractor
from app.features.extraction.service import ExtractionService


class FakeSttClient:
    def __init__(self, responses: list[str] | None = None) -> None:
        self.responses = responses or ["Распознанная речь"]
        self.sources: list[Path] = []

    async def transcribe(self, source: Path) -> str:
        self.sources.append(source)
        return self.responses.pop(0)


class FakeVisionClient:
    async def extract_vision_markdown(self, image: bytes, mime_type: str) -> str:
        return "unused"


def media_settings() -> SimpleNamespace:
    return SimpleNamespace(
        max_document_file_size_mb=1,
        max_pdf_pages=3,
        max_media_file_size_mb=1,
        max_media_duration_seconds=60,
        stt_chunk_seconds=30,
    )


async def fake_duration(_: Path) -> float:
    return 12.0


@pytest.mark.asyncio
async def test_mp3_flow_uses_original_audio_without_ffmpeg_conversion(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    source = tmp_path / "speech.mp3"
    source.write_bytes(b"mp3")
    stt = FakeSttClient(["Hello world"])
    extractor = MediaExtractor(stt, DetectedType.MP3, 1, 60, 30)
    monkeypatch.setattr(extractor, "_duration", fake_duration)

    result = await extractor.extract(source)

    assert result.content == "Hello world"
    assert stt.sources == [source]
    assert result.metadata == {"duration_seconds": 12, "chunk_count": 1}


@pytest.mark.asyncio
async def test_mp4_extracts_only_audio_before_stt(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    source = tmp_path / "lesson.mp4"
    source.write_bytes(b"mp4")
    stt = FakeSttClient()
    extractor = MediaExtractor(stt, DetectedType.MP4, 1, 60, 30)
    monkeypatch.setattr(extractor, "_duration", fake_duration)
    commands: list[list[str]] = []

    async def run_command(command: list[str]) -> SimpleNamespace:
        commands.append(command)
        Path(command[-1]).write_bytes(b"audio")
        return SimpleNamespace(stdout="")

    monkeypatch.setattr(extractor, "_run_command", run_command)
    result = await extractor.extract(source)

    assert result.content == "Распознанная речь"
    assert "-vn" in commands[0]
    assert stt.sources[0].name == "audio.mp3"


@pytest.mark.asyncio
async def test_media_size_and_duration_limits_are_content_errors(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    source = tmp_path / "speech.mp3"
    source.write_bytes(b"123")
    extractor = MediaExtractor(FakeSttClient(), DetectedType.MP3, 0, 60, 30)

    with pytest.raises(ContentError, match="file_too_large"):
        await extractor.extract(source)

    extractor = MediaExtractor(FakeSttClient(), DetectedType.MP3, 1, 60, 30)

    async def too_long(_: Path) -> float:
        return 61.0

    monkeypatch.setattr(extractor, "_duration", too_long)
    with pytest.raises(ContentError, match="media_too_long"):
        await extractor.extract(source)


@pytest.mark.asyncio
async def test_empty_speech_is_reported_as_empty_content(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    source = tmp_path / "speech.mp3"
    source.write_bytes(b"mp3")
    extractor = MediaExtractor(FakeSttClient(["  "]), DetectedType.MP3, 1, 60, 30)
    monkeypatch.setattr(extractor, "_duration", fake_duration)

    with pytest.raises(ContentError, match="empty_content"):
        await extractor.extract(source)


@pytest.mark.asyncio
async def test_service_routes_mp3_to_stt(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    source = tmp_path / "speech.mp3"
    source.write_bytes(b"mp3")
    stt = FakeSttClient()
    monkeypatch.setattr("app.features.extraction.service.detect_type", lambda _: DetectedType.MP3)
    monkeypatch.setattr(MediaExtractor, "_duration", staticmethod(fake_duration))

    result = await ExtractionService(
        media_settings(), vision_client=FakeVisionClient(), stt_client=stt
    ).extract(source)

    assert result.content == "Распознанная речь"
