"""Загрузка разрешённой аудиодорожки YouTube для общего STT pipeline."""

from __future__ import annotations

import asyncio
import logging
import tempfile
from collections.abc import Callable
from pathlib import Path
from typing import Protocol

import yt_dlp

from app.core.exceptions import ContentError, TransientExternalError
from app.features.extraction.base import ExtractionResult

logger = logging.getLogger(__name__)


class AudioPipeline(Protocol):
    async def extract(self, source: Path) -> ExtractionResult: ...


class AudioDownloader(Protocol):
    async def download(self, source_url: str, destination: Path) -> Path: ...


class YtDlpAudioDownloader:
    """Минимальная обёртка над yt-dlp без cookies, прокси и обходов доступа."""

    async def download(self, source_url: str, destination: Path) -> Path:
        return await asyncio.to_thread(self._download, source_url, destination)

    @staticmethod
    def _download(source_url: str, destination: Path) -> Path:
        options: dict[str, object] = {
            "format": "bestaudio",
            "outtmpl": str(destination / "audio.%(ext)s"),
            "noplaylist": True,
            "quiet": True,
            "no_warnings": True,
            "noprogress": True,
        }
        try:
            with yt_dlp.YoutubeDL(options) as downloader:
                info = downloader.extract_info(source_url, download=True)
                requested_downloads = info.get("requested_downloads") or []
                filename = requested_downloads[0].get("filepath") if requested_downloads else None
                output = Path(filename or downloader.prepare_filename(info))
        except yt_dlp.utils.DownloadError as error:
            raise YtDlpAudioDownloader._mapped_download_error(error) from error

        if not output.is_file() or output.stat().st_size == 0:
            raise ContentError(
                "source_unavailable",
                "Не удалось получить аудиодорожку из ролика YouTube.",
            )
        return output

    @staticmethod
    def _mapped_download_error(error: yt_dlp.utils.DownloadError) -> Exception:
        message = str(error).lower()
        temporary_markers = (
            "timed out",
            "temporary failure",
            "connection reset",
            "network is unreachable",
            "http error 429",
            "http error 5",
        )
        if any(marker in message for marker in temporary_markers):
            return TransientExternalError(
                "youtube_temporary_failure",
                "Сервис YouTube временно недоступен. Попробуйте позже.",
            )

        logger.info("YouTube audio is unavailable: %s", error)
        return ContentError(
            "source_unavailable",
            "Аудиодорожка ролика YouTube недоступна для обработки.",
        )


class YoutubeExtractor:
    """Извлекает речь из YouTube только через скачивание аудио и STT."""

    def __init__(
        self,
        audio_pipeline: AudioPipeline,
        downloader: AudioDownloader | None = None,
        temporary_directory: Callable[..., tempfile.TemporaryDirectory[str]] = tempfile.TemporaryDirectory,
    ) -> None:
        self._audio_pipeline = audio_pipeline
        self._downloader = downloader or YtDlpAudioDownloader()
        self._temporary_directory = temporary_directory

    async def extract(self, source_url: str) -> ExtractionResult:
        with self._temporary_directory(prefix="transcript-youtube-") as temporary_directory:
            audio = await self._downloader.download(source_url, Path(temporary_directory))
            return await self._audio_pipeline.extract(audio)
