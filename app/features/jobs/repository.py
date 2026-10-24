from __future__ import annotations

from datetime import UTC, datetime, timedelta

from sqlalchemy import Select, and_, or_, select, update
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession

from app.features.jobs.models import (
    JobStage,
    JobStatus,
    OutboxEvent,
    OutboxStatus,
    ProcessedEvent,
    TranscriptionJob,
)
from app.features.jobs.schemas import NewJob, NewOutboxEvent
from app.features.jobs.service import ensure_transition


class JobsRepository:
    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def register_processed_event(self, event_id: str) -> bool:
        statement = (
            insert(ProcessedEvent)
            .values(event_id=event_id)
            .on_conflict_do_nothing(index_elements=[ProcessedEvent.event_id])
            .returning(ProcessedEvent.event_id)
        )
        return (await self._session.scalar(statement)) is not None

    async def create_job(self, job: NewJob) -> bool:
        statement = (
            insert(TranscriptionJob)
            .values(
                job_id=job.job_id,
                material_id=job.material_id,
                user_id=job.user_id,
                correlation_id=job.correlation_id,
                source_event_id=job.source_event_id,
                source_bucket=job.source_bucket,
                source_key=job.source_key,
                source_url=job.source_url,
                pipeline_version=job.pipeline_version,
                models=job.models,
            )
            .on_conflict_do_nothing(index_elements=[TranscriptionJob.job_id])
            .returning(TranscriptionJob.job_id)
        )
        return (await self._session.scalar(statement)) is not None

    async def claim_next_job(self, lease_for: timedelta) -> TranscriptionJob | None:
        """
        Берём следующую задачу, которую можно обработать.
        1 Категория:
            status = pending или status = retry_wait
            проверяется:
                next_retry_at отсутствует ИЛИ next_retry_at <= сейчас
                locked_until отсутствует ИЛИ locked_until <= сейчас
        2 Категория:
            Задача находится в processing но locked_until <= сейчас
        """
        now = datetime.now(UTC)
        available = or_(
            and_(
                TranscriptionJob.status.in_(
                    [JobStatus.PENDING.value, JobStatus.RETRY_WAIT.value]
                ),
                or_(
                    TranscriptionJob.next_retry_at.is_(None),
                    TranscriptionJob.next_retry_at <= now,
                ),
                or_(
                    TranscriptionJob.locked_until.is_(None),
                    TranscriptionJob.locked_until <= now,
                ),
            ),
            and_(
                TranscriptionJob.status == JobStatus.PROCESSING.value,
                TranscriptionJob.locked_until <= now,
            ),
        )
        statement: Select[tuple[TranscriptionJob]] = (
            select(TranscriptionJob)
            .where(available)
            .order_by(
                TranscriptionJob.next_retry_at.asc().nullsfirst(),
                TranscriptionJob.created_at,
            )
            .limit(1)
            .with_for_update(skip_locked=True)
        )
        job = (await self._session.scalars(statement)).first()
        if job is None:
            return None

        job.status = JobStatus.PROCESSING.value
        job.current_stage = JobStage.DOWNLOAD.value
        job.attempt_count += 1
        job.locked_until = now + lease_for
        job.started_at = job.started_at or now
        job.next_retry_at = None
        # flush() отправляет изменения в PostgreSQL в рамках текущей транзакции, но не завершает её.
        await self._session.flush()
        return job

    async def update_stage(self, job_id: str, stage: JobStage) -> bool:
        result = await self._session.execute(
            update(TranscriptionJob)
            .where(
                TranscriptionJob.job_id == job_id,
                TranscriptionJob.status == JobStatus.PROCESSING.value,
            )
            .values(current_stage=stage.value)
        )
        return result.rowcount == 1

    async def move_to_retry_wait(
        self,
        job_id: str,
        *,
        retry_at: datetime,
        error_code: str,
        error_message: str,
    ) -> bool:
        """Метод вызывается при временной ошибке."""
        result = await self._session.execute(
            update(TranscriptionJob)
            .where(
                TranscriptionJob.job_id == job_id,
                TranscriptionJob.status == JobStatus.PROCESSING.value,
            )
            .values(
                status=JobStatus.RETRY_WAIT.value,
                current_stage=None,
                next_retry_at=retry_at,
                locked_until=None,
                error_code=error_code,
                error_message=error_message,
                retryable=True,
            )
        )
        return result.rowcount == 1

    async def complete_job_with_outbox(
        self,
        job_id: str,
        *,
        summary_bucket: str,
        summary_key: str,
        models: dict[str, object],
        event: NewOutboxEvent,
    ) -> None:
        """Переводит job в completed, создаёт outbox_event."""
        async with self._session.begin():
            job = await self._get_processing_job(job_id)
            ensure_transition(JobStatus(job.status), JobStatus.COMPLETED)
            job.status = JobStatus.COMPLETED.value
            job.current_stage = None
            job.locked_until = None
            job.summary_bucket = summary_bucket
            job.summary_key = summary_key
            job.models = models
            job.retryable = False
            job.completed_at = datetime.now(UTC)
            self._session.add(self._outbox_event(job_id, event))

    async def fail_job_with_outbox(
        self,
        job_id: str,
        *,
        error_code: str,
        error_message: str,
        event: NewOutboxEvent,
    ) -> None:
        """Переводит job в failed, создаёт outbox_event."""
        async with self._session.begin():
            job = await self._get_processing_job(job_id)
            ensure_transition(JobStatus(job.status), JobStatus.FAILED)
            job.status = JobStatus.FAILED.value
            job.current_stage = None
            job.locked_until = None
            job.error_code = error_code
            job.error_message = error_message
            job.retryable = False
            job.completed_at = datetime.now(UTC)
            self._session.add(self._outbox_event(job_id, event))

    async def get_pending_outbox_events(self, limit: int = 100) -> list[OutboxEvent]:
        """
        Выбирается какое событие нужно отправить в Kafka.
        status = pending и (next_retry_at IS NULL или next_retry_at <= сейчас)
        """
        now = datetime.now(UTC)
        statement = (
            select(OutboxEvent)
            .where(
                OutboxEvent.status == OutboxStatus.PENDING.value,
                or_(
                    OutboxEvent.next_retry_at.is_(None),
                    OutboxEvent.next_retry_at <= now,
                ),
            )
            .order_by(OutboxEvent.created_at)
            .limit(limit)
        )
        return list((await self._session.scalars(statement)).all())

    async def mark_outbox_published(self, event_id: str) -> bool:
        """Когда Kafka producer успешно отправил событие pending -> published"""
        result = await self._session.execute(
            update(OutboxEvent)
            .where(
                OutboxEvent.id == event_id,
                OutboxEvent.status == OutboxStatus.PENDING.value,
            )
            .values(
                status=OutboxStatus.PUBLISHED.value,
                published_at=datetime.now(UTC),
                next_retry_at=None,
            )
        )
        return result.rowcount == 1

    async def _get_processing_job(self, job_id: str) -> TranscriptionJob:
        statement = (
            select(TranscriptionJob)
            .where(TranscriptionJob.job_id == job_id)
            .with_for_update()
        )
        job = await self._session.scalar(statement)
        if job is None:
            raise LookupError(f"Job not found: {job_id}")
        if job.status != JobStatus.PROCESSING.value:
            raise ValueError(f"Job is not processing: {job_id}")
        return job

    @staticmethod
    def _outbox_event(job_id: str, event: NewOutboxEvent) -> OutboxEvent:
        return OutboxEvent(
            id=event.event_id,
            job_id=job_id,
            topic=event.topic,
            partition_key=event.partition_key,
            event_type=event.event_type,
            payload=event.payload,
        )
