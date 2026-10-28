"""Извлечение речи из MP3 и аудиодорожки MP4."""

from __future__ import annotations

import asyncio
import json
import subprocess
import tempfile
from pathlib import Path
from typing import Protocol

from app.core.exceptions import ContentError
from app.features.extraction.base import DetectedType, ExtractionResult


class SpeechToTextClient(Protocol):
    async def transcribe(self, source: Path) -> str: ...


class MediaExtractor:
    """Проверяет медиа и последовательно передаёт аудио в STT."""

    def __init__(
        self,
        stt_client: SpeechToTextClient,
        detected_type: DetectedType,
        max_file_size_mb: int,
        max_duration_seconds: int,
        chunk_seconds: int,
    ) -> None:
        self._stt_client = stt_client
        self.detected_type = detected_type
        self._max_file_size_bytes = max_file_size_mb * 1024 * 1024
        self._max_duration_seconds = max_duration_seconds
        self._chunk_seconds = chunk_seconds

    async def extract(self, source: Path) -> ExtractionResult:
        self._check_file_size(source)
        duration = await self._duration(source)
        if duration > self._max_duration_seconds:
            raise ContentError("media_too_long", "Превышена допустимая длительность медиа.")

        with tempfile.TemporaryDirectory(prefix="transcript-media-") as temporary_directory:
            workdir = Path(temporary_directory)
            audio = source if self.detected_type is DetectedType.MP3 else await self._extract_audio(source, workdir)
            chunks = await self._prepare_chunks(audio, workdir, duration)
            transcript_parts = []
            for chunk in chunks:
                transcript_parts.append(await self._stt_client.transcribe(chunk))
            transcript = "\n\n".join(transcript_parts).strip()

        if not transcript:
            raise ContentError("empty_content", "В аудио нет содержательной речи.")
        return ExtractionResult(
            content=transcript,
            detected_type=self.detected_type,
            metadata={"duration_seconds": int(duration), "chunk_count": len(chunks)},
        )

    def _check_file_size(self, source: Path) -> None:
        if source.stat().st_size > self._max_file_size_bytes:
            raise ContentError("file_too_large", "Превышен допустимый размер медиафайла.")

    async def _duration(self, source: Path) -> float:
        result = await self._run_command(
            ["ffprobe", "-v", "error", "-show_entries", "format=duration", "-of", "json", str(source)]
        )
        try:
            duration = float(json.loads(result.stdout).get("format", {}).get("duration"))
        except (TypeError, ValueError, json.JSONDecodeError) as error:
            raise ContentError("invalid_media", "Не удалось определить длительность медиа.") from error
        if duration <= 0:
            raise ContentError("invalid_media", "Медиафайл не содержит аудиодорожки.")
        return duration

    async def _extract_audio(self, source: Path, workdir: Path) -> Path:
        audio = workdir / "audio.mp3"
        await self._run_command(
            ["ffmpeg", "-y", "-i", str(source), "-vn", "-ac", "1", "-ar", "16000", "-b:a", "64k", str(audio)]
        )
        if not audio.exists() or not audio.stat().st_size:
            raise ContentError("invalid_media", "Не удалось извлечь аудиодорожку из видео.")
        return audio

    async def _prepare_chunks(self, audio: Path, workdir: Path, duration: float) -> list[Path]:
        if duration <= self._chunk_seconds:
            return [audio]

        pattern = workdir / "chunk-%03d.mp3"
        await self._run_command(
            [
                "ffmpeg", "-y", "-i", str(audio), "-f", "segment", "-segment_time",
                str(self._chunk_seconds), "-c:a", "libmp3lame", "-ac", "1", "-ar", "16000",
                "-b:a", "64k", str(pattern),
            ]
        )
        chunks = sorted(workdir.glob("chunk-*.mp3"))
        if not chunks:
            raise ContentError("invalid_media", "Не удалось подготовить части аудио.")
        return chunks

    @staticmethod
    async def _run_command(command: list[str]) -> subprocess.CompletedProcess[str]:
        try:
            return await asyncio.to_thread(
                subprocess.run, command, check=True, capture_output=True, text=True
            )
        except (OSError, subprocess.CalledProcessError) as error:
            raise ContentError("invalid_media", "Не удалось обработать медиафайл.") from error
