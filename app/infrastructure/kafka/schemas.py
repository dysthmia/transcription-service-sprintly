from __future__ import annotations

import re
from typing import Annotated, Literal
from urllib.parse import parse_qs, urlparse

from pydantic import BaseModel, ConfigDict, Field, field_validator


class KafkaEvent(BaseModel):
    """Общие поля для всех входящих/исходящих событий"""

    model_config = ConfigDict(extra="forbid")

    event_id: str
    event_type: str
    schema_version: int
    correlation_id: str
    job_id: str
    user_id: str


class S3Source(BaseModel):
    """Reference to a source object in S3-compatible storage."""
    
    #запрещает передавать в модель поля, не объявленные в классе.
    model_config = ConfigDict(extra="forbid")

    type: Literal["s3"]
    bucket: str
    key: str


class YouTubeSource(BaseModel):
    """A supported public YouTube video URL."""

    model_config = ConfigDict(extra="forbid")

    type: Literal["youtube"]
    url: str

    @field_validator("url")
    @classmethod
    def validate_youtube_url(cls, value: str) -> str:
        """Accept only common YouTube video URL forms, not arbitrary HTTP URLs."""
        parsed = urlparse(value)
        host = (parsed.hostname or "").lower()

        if parsed.scheme not in {"http", "https"} or parsed.username or parsed.password:
            raise ValueError("URL must be an HTTP(S) YouTube video URL")

        video_id: str | None = None
        if host in {"youtube.com", "www.youtube.com", "m.youtube.com", "music.youtube.com"}:
            if parsed.path == "/watch":
                video_id = parse_qs(parsed.query).get("v", [None])[0]
            else:
                path_parts = [part for part in parsed.path.split("/") if part]
                if len(path_parts) == 2 and path_parts[0] in {"shorts", "embed", "live"}:
                    video_id = path_parts[1]
        elif host in {"youtu.be", "www.youtu.be"}:
            path_parts = [part for part in parsed.path.split("/") if part]
            if len(path_parts) == 1:
                video_id = path_parts[0]

        if video_id is None or re.fullmatch(r"[A-Za-z0-9_-]{11}", video_id) is None:
            raise ValueError("URL must contain a valid YouTube video identifier")

        return value


Source = Annotated[S3Source | YouTubeSource, Field(discriminator="type")]


class MaterialUploadedEvent(KafkaEvent):
    """Входящее событие, запрашивающее обработку загруженного материала."""

    event_type: Literal["material_uploaded"]
    material_id: str
    source: Source


class SummaryLocation(BaseModel):
    """Местоположение сгенерированного Markdown."""

    model_config = ConfigDict(extra="forbid")

    bucket: str
    key: str


class SummaryGeneratedEvent(KafkaEvent):
    """Исходящее событие публикуется после сохранения MD."""

    event_type: Literal["summary_generated"]
    material_id: str
    summary: SummaryLocation


class TranscriptionFailedEvent(KafkaEvent):
    """Исходящее событие, когда задание достигает конечной точки сбоя."""

    event_type: Literal["transcription_failed"]
    material_id: str
    failed_stage: Literal["download", "extract", "clean", "summarize", "upload"]
    error_code: str
    error_message: str
    retryable: bool
