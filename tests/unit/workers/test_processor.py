from __future__ import annotations

import shutil
from contextlib import asynccontextmanager
from datetime import timedelta
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest

from app.core.exceptions import ContentError, TransientExternalError
from app.features.extraction.base import DetectedType, ExtractionResult
from app.workers.processor import JobProcessor


class FakeSession:
    @asynccontextmanager
    async def begin(self) -> Any:
        yield self


class FakeDatabase:
    @asynccontextmanager
    async def session(self) -> Any:
        yield FakeSession()


class FakeRepository:
    def __init__(self, job: SimpleNamespace | None) -> None:
        self.job = job
        self.claimed_for: timedelta | None = None
        self.stages: list[tuple[str, str, timedelta]] = []
        self.retries: list[dict[str, Any]] = []
        self.completed: list[dict[str, Any]] = []
        self.failed: list[dict[str, Any]] = []

    async def claim_next_job(self, lease_for: timedelta) -> SimpleNamespace | None:
        self.claimed_for = lease_for
        return self.job

    async def update_stage_and_extend_lease(
        self, job_id: str, stage: Any, lease_for: timedelta
    ) -> bool:
        self.stages.append((job_id, stage.value, lease_for))
        return True

    async def move_to_retry_wait(self, job_id: str, **kwargs: Any) -> bool:
        self.retries.append({"job_id": job_id, **kwargs})
        return True

    async def complete_job_with_outbox(self, job_id: str, **kwargs: Any) -> None:
        self.completed.append({"job_id": job_id, **kwargs})

    async def fail_job_with_outbox(self, job_id: str, **kwargs: Any) -> None:
        self.failed.append({"job_id": job_id, **kwargs})


class FakeS3:
    def __init__(self) -> None:
        self.downloads: list[tuple[str, str, Path]] = []
        self.uploads: list[tuple[str, str, str | None]] = []

    async def download_file(self, bucket: str, key: str, destination: Path) -> Path:
        self.downloads.append((bucket, key, destination))
        destination.write_text("source", encoding="utf-8")
        return destination

    async def upload_text(self, text: str, key: str, bucket: str | None = None) -> None:
        self.uploads.append((text, key, bucket))


class FakeExtractionService:
    def __init__(self, result: ExtractionResult | Exception) -> None:
        self._result = result
        self.sources: list[Path | str] = []

    async def extract(self, source: Path | str) -> ExtractionResult:
        self.sources.append(source)
        if isinstance(self._result, Exception):
            raise self._result
        return self._result


class FakeSummarizationService:
    def __init__(self, result: str | Exception) -> None:
        self._result = result
        self.sources: list[str] = []

    async def summarize(self, source: str) -> str:
        self.sources.append(source)
        if isinstance(self._result, Exception):
            raise self._result
        return self._result


class TemporaryDirectorySpy:
    def __init__(self, path: Path) -> None:
        self.name = str(path)
        path.mkdir()
        self.cleaned = False

    def cleanup(self) -> None:
        self.cleaned = True
        shutil.rmtree(self.name)


def settings() -> SimpleNamespace:
    return SimpleNamespace(
        job_lease_seconds=120,
        job_max_attempts=3,
        s3_output_bucket="summaries",
        kafka_output_topic="transcript.events",
    )


def job(*, attempt_count: int = 1) -> SimpleNamespace:
    return SimpleNamespace(
        job_id="job-1",
        material_id="material-1",
        user_id="user-1",
        correlation_id="corr-1",
        source_bucket="materials",
        source_key="uploads/source.pdf",
        source_url=None,
        attempt_count=attempt_count,
        pipeline_version="v1",
        models={},
    )


def build_processor(
    repository: FakeRepository,
    extraction: FakeExtractionService,
    summarization: FakeSummarizationService,
    s3: FakeS3,
    temporary_directory: TemporaryDirectorySpy,
) -> JobProcessor:
    return JobProcessor(
        settings(),
        FakeDatabase(),
        s3,
        extraction,
        summarization,
        repository_factory=lambda _: repository,
        temporary_directory=lambda **_: temporary_directory,
    )


@pytest.mark.asyncio
async def test_processes_s3_job_uploads_summary_and_creates_success_outbox(tmp_path: Path) -> None:
    repository = FakeRepository(job())
    s3 = FakeS3()
    extraction = FakeExtractionService(
        ExtractionResult("Извлечённый текст", DetectedType.PDF)
    )
    summarization = FakeSummarizationService("# Конспект")
    temporary_directory = TemporaryDirectorySpy(tmp_path / "job-temp")
    processor = build_processor(repository, extraction, summarization, s3, temporary_directory)

    assert await processor.process_next_job()

    expected_key = "users/user-1/materials/material-1/jobs/job-1/summary.md"
    assert repository.claimed_for == timedelta(seconds=120)
    assert extraction.sources == [s3.downloads[0][2]]
    assert summarization.sources == ["Извлечённый текст"]
    assert s3.uploads == [("# Конспект", expected_key, "summaries")]
    assert [stage for _, stage, _ in repository.stages] == [
        "download",
        "extract",
        "clean",
        "summarize",
        "upload",
    ]
    assert repository.completed[0]["summary_key"] == expected_key
    event = repository.completed[0]["event"]
    assert event.event_type == "summary_generated"
    assert event.payload["summary"] == {"bucket": "summaries", "key": expected_key}
    assert not repository.retries
    assert not repository.failed
    assert temporary_directory.cleaned
    assert not Path(temporary_directory.name).exists()


@pytest.mark.asyncio
async def test_retryable_error_moves_job_to_retry_wait_without_outbox(tmp_path: Path) -> None:
    repository = FakeRepository(job(attempt_count=1))
    temporary_directory = TemporaryDirectorySpy(tmp_path / "job-temp")
    processor = build_processor(
        repository,
        FakeExtractionService(TransientExternalError("s3_temporary_failure", "Временно.")),
        FakeSummarizationService("unused"),
        FakeS3(),
        temporary_directory,
    )

    assert await processor.process_next_job()

    assert len(repository.retries) == 1
    assert repository.retries[0]["error_code"] == "s3_temporary_failure"
    assert not repository.completed
    assert not repository.failed
    assert temporary_directory.cleaned


@pytest.mark.asyncio
async def test_content_error_fails_job_and_creates_failure_outbox(tmp_path: Path) -> None:
    repository = FakeRepository(job())
    temporary_directory = TemporaryDirectorySpy(tmp_path / "job-temp")
    processor = build_processor(
        repository,
        FakeExtractionService(ExtractionResult("текст", DetectedType.TXT)),
        FakeSummarizationService(ContentError("llm_invalid_output", "Некорректный ответ.")),
        FakeS3(),
        temporary_directory,
    )

    assert await processor.process_next_job()

    assert not repository.retries
    assert not repository.completed
    assert len(repository.failed) == 1
    event = repository.failed[0]["event"]
    assert event.event_type == "transcription_failed"
    assert event.payload["failed_stage"] == "summarize"
    assert event.payload["error_code"] == "llm_invalid_output"
    assert event.payload["retryable"] is False


@pytest.mark.asyncio
async def test_last_retryable_attempt_fails_job_and_creates_failure_outbox(tmp_path: Path) -> None:
    repository = FakeRepository(job(attempt_count=3))
    temporary_directory = TemporaryDirectorySpy(tmp_path / "job-temp")
    processor = build_processor(
        repository,
        FakeExtractionService(TransientExternalError("s3_temporary_failure", "Временно.")),
        FakeSummarizationService("unused"),
        FakeS3(),
        temporary_directory,
    )

    assert await processor.process_next_job()

    assert not repository.retries
    assert len(repository.failed) == 1
    assert repository.failed[0]["event"].payload["retryable"] is False


@pytest.mark.asyncio
async def test_returns_false_when_no_job_is_available(tmp_path: Path) -> None:
    repository = FakeRepository(None)
    temporary_directory = TemporaryDirectorySpy(tmp_path / "unused-temp")
    processor = build_processor(
        repository,
        FakeExtractionService(ExtractionResult("текст", DetectedType.TXT)),
        FakeSummarizationService("# Конспект"),
        FakeS3(),
        temporary_directory,
    )

    assert not await processor.process_next_job()
    assert not temporary_directory.cleaned
