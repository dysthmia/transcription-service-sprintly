"""Обработка одной job: источник, извлечение, конспект и сохранение результата."""

from __future__ import annotations

import tempfile
from collections.abc import Callable
from datetime import UTC, datetime, timedelta
from pathlib import Path
from time import perf_counter
from typing import Any
from uuid import uuid4

import structlog

from app.core.config import Settings
from app.core.database import Database
from app.core.exceptions import ContentError, TranscriptError, TransientExternalError
from app.core.logging import bind_log_context, clear_log_context
from app.features.extraction.service import ExtractionService
from app.features.jobs.models import JobStage, TranscriptionJob
from app.features.jobs.repository import JobsRepository
from app.features.jobs.schemas import NewOutboxEvent
from app.features.summarization.service import SummarizationService
from app.infrastructure.s3.client import S3Client

logger = structlog.get_logger(__name__)


class JobProcessor:
    """Захватывает и обрабатывает jobs, не дублируя логику feature-сервисов."""

    def __init__(
        self,
        settings: Settings,
        database: Database,
        s3_client: S3Client,
        extraction_service: ExtractionService,
        summarization_service: SummarizationService,
        *,
        repository_factory: Callable[[Any], JobsRepository] = JobsRepository,
        temporary_directory: Callable[..., tempfile.TemporaryDirectory[str]] = tempfile.TemporaryDirectory,
    ) -> None:
        self._settings = settings
        self._database = database
        self._s3_client = s3_client
        self._extraction_service = extraction_service
        self._summarization_service = summarization_service
        self._repository_factory = repository_factory
        self._temporary_directory = temporary_directory
        self._lease_for = timedelta(seconds=settings.job_lease_seconds)

    async def process_next_job(self) -> bool:
        """Захватывает одну доступную job и полностью обрабатывает её."""
        job = await self._claim_next_job()
        if job is None:
            return False

        bind_log_context(
            job_id=job.job_id,
            correlation_id=job.correlation_id,
            material_id=job.material_id,
            attempt=job.attempt_count,
            pipeline_version=job.pipeline_version,
        )
        logger.info("job.claimed")
        await self._process_claimed_job(job)
        return True

    async def _claim_next_job(self) -> TranscriptionJob | None:
        async with self._database.session() as session, session.begin():
            repository = self._repository_factory(session)
            return await repository.claim_next_job(self._lease_for)

    async def _process_claimed_job(self, job: TranscriptionJob) -> None:
        current_stage = JobStage.DOWNLOAD
        temporary_directory: tempfile.TemporaryDirectory[str] | None = None
        try:
            temporary_directory = self._temporary_directory(prefix=f"transcript-job-{job.job_id}-")
            workdir = Path(temporary_directory.name)
            processing_started_at = perf_counter()
            logger.info("job.processing.started")
            source = await self._run_stage(
                job, current_stage, lambda: self._download_source(job, workdir)
            )

            current_stage = JobStage.EXTRACT
            extracted = await self._run_stage(
                job, current_stage, lambda: self._extraction_service.extract(source)
            )

            current_stage = JobStage.CLEAN
            cleaned_content = await self._run_stage(
                job, current_stage, lambda: self._clean_content(extracted.content)
            )

            current_stage = JobStage.SUMMARIZE
            summary = await self._run_stage(
                job,
                current_stage,
                lambda: self._summarization_service.summarize(cleaned_content),
            )

            current_stage = JobStage.UPLOAD
            summary_key = self._summary_key(job)
            await self._run_stage(
                job,
                current_stage,
                lambda: self._s3_client.upload_text(
                    summary,
                    summary_key,
                    bucket=self._settings.s3_output_bucket,
                ),
            )
        except TranscriptError as error:
            await self._handle_error(job, current_stage, error)
        except Exception as error:
            logger.error("job.unexpected_error", exc_info=error)
            await self._handle_error(
                job,
                current_stage,
                TransientExternalError(
                    "processing_temporary_failure",
                    "Не удалось временно обработать материал.",
                ),
            )
        else:
            await self._complete_job(
                job,
                summary_key,
                duration_ms=round((perf_counter() - processing_started_at) * 1000, 2),
            )
        finally:
            if temporary_directory is not None:
                temporary_directory.cleanup()
            clear_log_context()

    async def _run_stage(
        self,
        job: TranscriptionJob,
        stage: JobStage,
        operation: Callable[[], Any],
    ) -> Any:
        started_at = perf_counter()
        await self._start_stage(job.job_id, stage)
        result = operation()
        if hasattr(result, "__await__"):
            result = await result
        logger.info(
            "job.stage.completed",
            stage=stage.value,
            duration_ms=round((perf_counter() - started_at) * 1000, 2),
        )
        return result

    async def _download_source(self, job: TranscriptionJob, workdir: Path) -> Path | str:
        if job.source_url:
            return job.source_url
        if not job.source_bucket or not job.source_key:
            raise ContentError("source_unavailable", "У задания отсутствует исходный материал.")

        suffix = Path(job.source_key).suffix
        destination = workdir / f"source{suffix}"
        return await self._s3_client.download_file(job.source_bucket, job.source_key, destination)

    @staticmethod
    def _clean_content(content: str) -> str:
        cleaned = content.strip()
        if not cleaned:
            raise ContentError("empty_content", "В материале нет извлекаемого содержимого.")
        return cleaned

    async def _start_stage(self, job_id: str, stage: JobStage) -> None:
        async with self._database.session() as session, session.begin():
            repository = self._repository_factory(session)
            updated = await repository.update_stage_and_extend_lease(
                job_id, stage, self._lease_for
            )
        if not updated:
            logger.warning("job.lease.lost", stage=stage.value, error_code="lease_lost")
            raise TransientExternalError(
                "lease_lost", "Не удалось продлить аренду задания."
            )

    async def _handle_error(
        self,
        job: TranscriptionJob,
        stage: JobStage,
        error: TranscriptError,
    ) -> None:
        if isinstance(error, TransientExternalError) and job.attempt_count < self._settings.job_max_attempts:
            await self._move_to_retry_wait(job, error)
            return
        await self._fail_job(job, stage, error)

    async def _move_to_retry_wait(
        self, job: TranscriptionJob, error: TranscriptError
    ) -> None:
        retry_after_seconds = 5 * (2 ** max(job.attempt_count - 1, 0))
        retry_at = datetime.now(UTC) + timedelta(seconds=retry_after_seconds)
        async with self._database.session() as session, session.begin():
            repository = self._repository_factory(session)
            await repository.move_to_retry_wait(
                job.job_id,
                retry_at=retry_at,
                error_code=error.error_code,
                error_message=error.user_message,
            )
        logger.warning(
            "job.retry.scheduled",
            error_code=error.error_code,
            retryable=True,
        )

    async def _complete_job(
        self, job: TranscriptionJob, summary_key: str, *, duration_ms: float
    ) -> None:
        event_id = str(uuid4())
        event = NewOutboxEvent(
            event_id=event_id,
            topic=self._settings.kafka_output_topic,
            partition_key=job.user_id,
            event_type="summary_generated",
            payload={
                "event_id": event_id,
                "event_type": "summary_generated",
                "schema_version": 1,
                "correlation_id": job.correlation_id,
                "job_id": job.job_id,
                "user_id": job.user_id,
                "material_id": job.material_id,
                "summary": {
                    "bucket": self._settings.s3_output_bucket,
                    "key": summary_key,
                },
            },
        )
        async with self._database.session() as session:
            repository = self._repository_factory(session)
            await repository.complete_job_with_outbox(
                job.job_id,
                summary_bucket=self._settings.s3_output_bucket,
                summary_key=summary_key,
                models=dict(job.models),
                event=event,
            )
        logger.info("job.completed", duration_ms=duration_ms)

    async def _fail_job(
        self, job: TranscriptionJob, stage: JobStage, error: TranscriptError
    ) -> None:
        event_id = str(uuid4())
        event = NewOutboxEvent(
            event_id=event_id,
            topic=self._settings.kafka_output_topic,
            partition_key=job.user_id,
            event_type="transcription_failed",
            payload={
                "event_id": event_id,
                "event_type": "transcription_failed",
                "schema_version": 1,
                "correlation_id": job.correlation_id,
                "job_id": job.job_id,
                "user_id": job.user_id,
                "material_id": job.material_id,
                "failed_stage": stage.value,
                "error_code": error.error_code,
                "error_message": error.user_message,
                "retryable": False,
            },
        )
        async with self._database.session() as session:
            repository = self._repository_factory(session)
            await repository.fail_job_with_outbox(
                job.job_id,
                error_code=error.error_code,
                error_message=error.user_message,
                event=event,
            )
        log_method = logger.warning if isinstance(error, ContentError) else logger.error
        log_method(
            "job.failed",
            stage=stage.value,
            error_code=error.error_code,
            retryable=False,
        )

    @staticmethod
    def _summary_key(job: TranscriptionJob) -> str:
        return (
            f"users/{job.user_id}/materials/{job.material_id}/jobs/{job.job_id}/summary.md"
        )
