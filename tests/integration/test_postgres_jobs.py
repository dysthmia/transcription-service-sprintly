from __future__ import annotations

from contextlib import asynccontextmanager
from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest
from sqlalchemy import func, select, text, update
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.features.extraction.base import DetectedType, ExtractionResult
from app.features.jobs.models import JobStatus, OutboxEvent, TranscriptionJob
from app.features.jobs.repository import JobsRepository
from app.features.jobs.schemas import NewJob, NewOutboxEvent
from app.workers.processor import JobProcessor

pytestmark = pytest.mark.integration


def new_job(job_id: str, material_id: str = "material-1") -> NewJob:
    return NewJob(
        job_id=job_id,
        material_id=material_id,
        user_id="user-1",
        correlation_id=f"correlation-{job_id}",
        source_event_id=f"source-event-{job_id}",
        source_bucket="materials",
        source_key=f"sources/{job_id}.txt",
        pipeline_version="v1",
    )


async def register_job(
    session_factory: async_sessionmaker[AsyncSession],
    event_id: str,
    job: NewJob,
) -> tuple[bool, bool | None]:
    """Mirrors the consumer's one-transaction intake contract."""
    async with session_factory() as session, session.begin():
        repository = JobsRepository(session)
        event_is_new = await repository.register_processed_event(event_id)
        if not event_is_new:
            return False, None
        return True, await repository.create_job(job)


async def claim_job(
    session_factory: async_sessionmaker[AsyncSession],
    job_id: str,
) -> None:
    async with session_factory() as session, session.begin():
        claimed = await JobsRepository(session).claim_next_job(timedelta(minutes=5))
        assert claimed is not None
        assert claimed.job_id == job_id


async def job_count(session_factory: async_sessionmaker[AsyncSession]) -> int:
    async with session_factory() as session:
        return int(await session.scalar(select(func.count()).select_from(TranscriptionJob)) or 0)


@pytest.mark.asyncio
async def test_alembic_migration_creates_job_tables(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    async with session_factory() as session:
        tables = set(
            (
                await session.scalars(
                    text(
                        "SELECT table_name FROM information_schema.tables "
                        "WHERE table_schema = 'public' "
                        "AND table_name IN "
                        "('transcription_jobs', 'processed_events', 'outbox_events')"
                    )
                )
            ).all()
        )
        version = await session.scalar(text("SELECT version_num FROM alembic_version"))

    assert tables == {"transcription_jobs", "processed_events", "outbox_events"}
    assert version == "0001_create_jobs_tables"


@pytest.mark.asyncio
async def test_repeated_event_id_does_not_create_another_job(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    job = new_job("job-1")

    assert await register_job(session_factory, "event-1", job) == (True, True)
    assert await register_job(session_factory, "event-1", job) == (False, None)

    assert await job_count(session_factory) == 1


@pytest.mark.asyncio
async def test_duplicate_job_id_is_not_inserted_but_new_job_for_material_is(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    assert await register_job(session_factory, "event-1", new_job("job-1")) == (True, True)
    assert await register_job(session_factory, "event-2", new_job("job-1")) == (True, False)
    assert await register_job(session_factory, "event-3", new_job("job-2")) == (True, True)

    assert await job_count(session_factory) == 2


@pytest.mark.asyncio
async def test_claim_uses_skip_locked_for_concurrent_workers(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    await register_job(session_factory, "event-1", new_job("job-1"))
    await register_job(session_factory, "event-2", new_job("job-2"))

    async with (
        session_factory() as first_session,
        session_factory() as second_session,
        first_session.begin(),
        second_session.begin(),
    ):
        first_job = await JobsRepository(first_session).claim_next_job(timedelta(minutes=5))
        assert first_job is not None
        second_job = await JobsRepository(second_session).claim_next_job(timedelta(minutes=5))

    assert second_job is not None
    assert {first_job.job_id, second_job.job_id} == {"job-1", "job-2"}


@pytest.mark.asyncio
async def test_retry_wait_job_is_claimed_only_after_retry_time(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    await register_job(session_factory, "event-1", new_job("job-1"))
    await claim_job(session_factory, "job-1")

    async with session_factory() as session, session.begin():
        await JobsRepository(session).move_to_retry_wait(
            "job-1",
            retry_at=datetime.now(UTC) + timedelta(minutes=1),
            error_code="temporary_failure",
            error_message="Temporary failure",
        )

    async with session_factory() as session, session.begin():
        assert await JobsRepository(session).claim_next_job(timedelta(minutes=5)) is None

    async with session_factory() as session, session.begin():
        await session.execute(
            update(TranscriptionJob)
            .where(TranscriptionJob.job_id == "job-1")
            .values(next_retry_at=datetime.now(UTC) - timedelta(seconds=1))
        )

    async with session_factory() as session, session.begin():
        claimed = await JobsRepository(session).claim_next_job(timedelta(minutes=5))

    assert claimed is not None
    assert claimed.job_id == "job-1"


@pytest.mark.asyncio
async def test_complete_and_fail_create_outbox_rows_atomically(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    await register_job(session_factory, "event-complete", new_job("job-complete"))
    await claim_job(session_factory, "job-complete")
    complete_event = NewOutboxEvent(
        event_id="outbox-complete",
        topic="transcript.events",
        partition_key="user-1",
        event_type="summary_generated",
        payload={"event_id": "outbox-complete"},
    )
    async with session_factory() as session:
        await JobsRepository(session).complete_job_with_outbox(
            "job-complete",
            summary_bucket="summaries",
            summary_key="summaries/job-complete.md",
            models={},
            event=complete_event,
        )

    await register_job(session_factory, "event-fail", new_job("job-fail"))
    await claim_job(session_factory, "job-fail")
    failed_event = NewOutboxEvent(
        event_id="outbox-fail",
        topic="transcript.events",
        partition_key="user-1",
        event_type="transcription_failed",
        payload={"event_id": "outbox-fail"},
    )
    async with session_factory() as session:
        await JobsRepository(session).fail_job_with_outbox(
            "job-fail",
            error_code="empty_content",
            error_message="No content",
            event=failed_event,
        )

    async with session_factory() as session:
        jobs = {
            job.job_id: job.status
            for job in (await session.scalars(select(TranscriptionJob))).all()
        }
        events = {
            event.job_id: event.event_type
            for event in (await session.scalars(select(OutboxEvent))).all()
        }

    assert jobs == {"job-complete": JobStatus.COMPLETED.value, "job-fail": JobStatus.FAILED.value}
    assert events == {
        "job-complete": "summary_generated",
        "job-fail": "transcription_failed",
    }


@pytest.mark.asyncio
async def test_outbox_unique_event_type_rolls_back_completion(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    await register_job(session_factory, "event-1", new_job("job-1"))
    await claim_job(session_factory, "job-1")

    async with session_factory() as session, session.begin():
        session.add(
            OutboxEvent(
                id="existing-event",
                job_id="job-1",
                topic="transcript.events",
                partition_key="user-1",
                event_type="summary_generated",
                payload={},
            )
        )

    async with session_factory() as session:
        with pytest.raises(IntegrityError):
            await JobsRepository(session).complete_job_with_outbox(
                "job-1",
                summary_bucket="summaries",
                summary_key="summaries/job-1.md",
                models={},
                event=NewOutboxEvent(
                    event_id="duplicate-event",
                    topic="transcript.events",
                    partition_key="user-1",
                    event_type="summary_generated",
                    payload={},
                ),
            )

    async with session_factory() as session:
        job = await session.get(TranscriptionJob, "job-1")
        events = (await session.scalars(select(OutboxEvent))).all()

    assert job is not None
    assert job.status == JobStatus.PROCESSING.value
    assert [event.id for event in events] == ["existing-event"]


class DatabaseFromSessionFactory:
    def __init__(self, session_factory: async_sessionmaker[AsyncSession]) -> None:
        self._session_factory = session_factory

    @asynccontextmanager
    async def session(self) -> Any:
        async with self._session_factory() as session:
            yield session


class FakeS3:
    def __init__(self) -> None:
        self.uploads: list[tuple[str, str, str | None]] = []

    async def download_file(self, bucket: str, key: str, destination: Path) -> Path:
        destination.write_text("source", encoding="utf-8")
        return destination

    async def upload_text(self, text: str, key: str, bucket: str | None = None) -> None:
        self.uploads.append((text, key, bucket))


class FakeExtractionService:
    async def extract(self, source: Path | str) -> ExtractionResult:
        return ExtractionResult(
            content="Исходный материал",
            detected_type=DetectedType.TXT,
            metadata={},
        )


class FakeSummarizationService:
    async def summarize(self, source: str) -> str:
        return "# Конспект\n\nПодробное содержание."


@pytest.mark.asyncio
async def test_processor_happy_path_persists_completed_job_and_outbox(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    await register_job(session_factory, "event-1", new_job("job-1"))
    s3 = FakeS3()
    processor = JobProcessor(
        SimpleNamespace(
            job_lease_seconds=300,
            job_max_attempts=3,
            s3_output_bucket="summaries",
            kafka_output_topic="transcript.events",
        ),
        DatabaseFromSessionFactory(session_factory),
        s3,
        FakeExtractionService(),
        FakeSummarizationService(),
    )

    assert await processor.process_next_job() is True

    async with session_factory() as session:
        job = await session.get(TranscriptionJob, "job-1")
        event = await session.scalar(select(OutboxEvent).where(OutboxEvent.job_id == "job-1"))

    assert s3.uploads == [
        (
            "# Конспект\n\nПодробное содержание.",
            "users/user-1/materials/material-1/jobs/job-1/summary.md",
            "summaries",
        )
    ]
    assert job is not None
    assert job.status == JobStatus.COMPLETED.value
    assert event is not None
    assert event.event_type == "summary_generated"
