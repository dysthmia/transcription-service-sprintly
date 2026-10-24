from app.features.jobs.models import JobStatus

TERMINAL_STATUSES = frozenset({JobStatus.COMPLETED, JobStatus.FAILED})
ALLOWED_TRANSITIONS = {
    JobStatus.PENDING: frozenset({JobStatus.PROCESSING, JobStatus.FAILED}),
    JobStatus.PROCESSING: frozenset(
        {JobStatus.RETRY_WAIT, JobStatus.COMPLETED, JobStatus.FAILED}
    ),
    JobStatus.RETRY_WAIT: frozenset({JobStatus.PROCESSING, JobStatus.FAILED}),
    JobStatus.COMPLETED: frozenset(),
    JobStatus.FAILED: frozenset(),
}


def is_terminal(status: JobStatus) -> bool:
    return status in TERMINAL_STATUSES


def ensure_transition(current: JobStatus, target: JobStatus) -> None:
    if target not in ALLOWED_TRANSITIONS[current]:
        raise ValueError(f"Invalid job status transition: {current} -> {target}")
