from __future__ import annotations

from pathlib import Path

import pytest
import yt_dlp

from app.core.exceptions import ContentError, TransientExternalError
from app.features.extraction.base import DetectedType, ExtractionResult
from app.features.extraction.extractors.youtube import (
    YoutubeExtractor,
    YtDlpAudioDownloader,
)


class FakeDownloader:
    def __init__(self) -> None:
        self.urls: list[str] = []
        self.audio: Path | None = None

    async def download(self, source_url: str, destination: Path) -> Path:
        self.urls.append(source_url)
        self.audio = destination / "audio.m4a"
        self.audio.write_bytes(b"audio")
        return self.audio


class FakeAudioPipeline:
    def __init__(self) -> None:
        self.sources: list[Path] = []

    async def extract(self, source: Path) -> ExtractionResult:
        assert source.exists()
        self.sources.append(source)
        return ExtractionResult("Распознанная речь", DetectedType.YOUTUBE)


@pytest.mark.asyncio
async def test_youtube_reuses_audio_pipeline_and_removes_temporary_audio() -> None:
    downloader = FakeDownloader()
    pipeline = FakeAudioPipeline()

    result = await YoutubeExtractor(pipeline, downloader).extract(
        "https://www.youtube.com/watch?v=dQw4w9WgXcQ"
    )

    assert result.content == "Распознанная речь"
    assert downloader.urls == ["https://www.youtube.com/watch?v=dQw4w9WgXcQ"]
    assert pipeline.sources == [downloader.audio]
    assert downloader.audio is not None
    assert not downloader.audio.exists()


@pytest.mark.parametrize(
    "message",
    [
        "ERROR: Private video",
        "ERROR: Sign in to confirm your age",
        "ERROR: Video unavailable",
        "ERROR: This video is not available in your country",
    ],
)
def test_ytdlp_access_errors_are_source_unavailable(message: str) -> None:
    error = yt_dlp.utils.DownloadError(message)

    mapped = YtDlpAudioDownloader._mapped_download_error(error)

    assert isinstance(mapped, ContentError)
    assert mapped.error_code == "source_unavailable"


def test_ytdlp_temporary_network_error_is_retryable() -> None:
    mapped = YtDlpAudioDownloader._mapped_download_error(
        yt_dlp.utils.DownloadError("ERROR: [youtube] network is unreachable")
    )

    assert isinstance(mapped, TransientExternalError)
    assert mapped.error_code == "youtube_temporary_failure"
