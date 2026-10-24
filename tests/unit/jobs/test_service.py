from __future__ import annotations

import pytest

from app.features.jobs.models import JobStatus
from app.features.jobs.service import ensure_transition, is_terminal


def test_processing_can_complete() -> None:
    ensure_transition(JobStatus.PROCESSING, JobStatus.COMPLETED)


def test_terminal_job_cannot_be_reopened() -> None:
    with pytest.raises(ValueError, match="Invalid job status transition"):
        ensure_transition(JobStatus.COMPLETED, JobStatus.PROCESSING)


def test_terminal_statuses() -> None:
    assert is_terminal(JobStatus.COMPLETED)
    assert is_terminal(JobStatus.FAILED)
    assert not is_terminal(JobStatus.RETRY_WAIT)
