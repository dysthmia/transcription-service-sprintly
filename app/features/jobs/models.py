from __future__ import annotations

from datetime import datetime
from enum import StrEnum
from typing import Any

from sqlalchemy import (
    Boolean,
    CheckConstraint,
    DateTime,
    ForeignKey,
    Index,
    Integer,
    String,
    Text,
    UniqueConstraint,
    func,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


class Base(DeclarativeBase):
    pass


class JobStatus(StrEnum):
    PENDING = "pending"
    PROCESSING = "processing"
    RETRY_WAIT = "retry_wait"
    COMPLETED = "completed"
    FAILED = "failed"


class JobStage(StrEnum):
    DOWNLOAD = "download"
    EXTRACT = "extract"
    CLEAN = "clean"
    SUMMARIZE = "summarize"
    UPLOAD = "upload"


class OutboxStatus(StrEnum):
    PENDING = "pending"
    PUBLISHED = "published"


class TranscriptionJob(Base):
    __tablename__ = "transcription_jobs"
    __table_args__ = (
        CheckConstraint(
            "(source_bucket IS NOT NULL AND source_key IS NOT NULL AND source_url IS NULL) "
            "OR (source_bucket IS NULL AND source_key IS NULL AND source_url IS NOT NULL)",
            name="ck_transcription_jobs_exactly_one_source",
        ),
        CheckConstraint(
            "status IN ('pending', 'processing', 'retry_wait', 'completed', 'failed')",
            name="ck_transcription_jobs_status",
        ),
        Index(
            "ix_transcription_jobs_status_retry_lease",
            "status",
            "next_retry_at",
            "locked_until",
        ),
    )

    job_id: Mapped[str] = mapped_column(String(255), primary_key=True)
    material_id: Mapped[str] = mapped_column(String(255), nullable=False)
    user_id: Mapped[str] = mapped_column(String(255), nullable=False)
    correlation_id: Mapped[str] = mapped_column(String(255), nullable=False)
    source_event_id: Mapped[str] = mapped_column(String(255), nullable=False)
    source_bucket: Mapped[str | None] = mapped_column(String(255))
    source_key: Mapped[str | None] = mapped_column(Text)
    source_url: Mapped[str | None] = mapped_column(Text)
    detected_type: Mapped[str | None] = mapped_column(String(64))
    summary_bucket: Mapped[str | None] = mapped_column(String(255))
    summary_key: Mapped[str | None] = mapped_column(Text)
    status: Mapped[str] = mapped_column(
        String(32), default=JobStatus.PENDING.value, nullable=False
    )
    current_stage: Mapped[str | None] = mapped_column(String(32))
    attempt_count: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    next_retry_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    locked_until: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    error_code: Mapped[str | None] = mapped_column(String(128))
    error_message: Mapped[str | None] = mapped_column(Text)
    retryable: Mapped[bool] = mapped_column(Boolean, default=False, nullable=False)
    pipeline_version: Mapped[str] = mapped_column(String(64), nullable=False)
    models: Mapped[dict[str, Any]] = mapped_column(JSONB, default=dict, nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )
    started_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    completed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))


class ProcessedEvent(Base):
    __tablename__ = "processed_events"

    event_id: Mapped[str] = mapped_column(String(255), primary_key=True)
    processed_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )


class OutboxEvent(Base):
    __tablename__ = "outbox_events"
    __table_args__ = (
        CheckConstraint(
            "status IN ('pending', 'published')", name="ck_outbox_events_status"
        ),
        UniqueConstraint("job_id", "event_type", name="uq_outbox_events_job_id_event_type"),
        Index("ix_outbox_events_status_retry", "status", "next_retry_at"),
    )

    id: Mapped[str] = mapped_column(String(255), primary_key=True)
    job_id: Mapped[str] = mapped_column(
        ForeignKey("transcription_jobs.job_id"), nullable=False
    )
    topic: Mapped[str] = mapped_column(String(255), nullable=False)
    partition_key: Mapped[str] = mapped_column(String(255), nullable=False)
    event_type: Mapped[str] = mapped_column(String(128), nullable=False)
    payload: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False)
    status: Mapped[str] = mapped_column(
        String(32), default=OutboxStatus.PENDING.value, nullable=False
    )
    attempt_count: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    next_retry_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )
    published_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
