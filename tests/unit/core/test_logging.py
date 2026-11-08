from __future__ import annotations

import asyncio
import json
import logging
from datetime import UTC, datetime

import pytest
import structlog

from app.core.logging import bind_log_context, clear_log_context, configure_logging


@pytest.fixture(autouse=True)
def clear_context() -> None:
    clear_log_context()
    yield
    clear_log_context()


def _configure_json() -> None:
    configure_logging(
        "INFO",
        log_format="json",
        service_name="transcription-service",
        service_version="1.2.3",
        app_env="production",
    )


def _one_json_line(capsys: pytest.CaptureFixture[str]) -> dict[str, object]:
    lines = capsys.readouterr().out.splitlines()
    assert len(lines) == 1
    return json.loads(lines[0])


def test_json_log_has_required_fields_and_utc_timestamp(
    capsys: pytest.CaptureFixture[str],
) -> None:
    _configure_json()

    structlog.get_logger("worker").info("job_started", message="Starting job")

    record = _one_json_line(capsys)
    assert record["event"] == "job_started"
    assert record["message"] == "Starting job"
    assert record["level"] == "info"
    assert record["logger"] == "worker"
    assert record["service"] == "transcription-service"
    assert record["environment"] == "production"
    assert record["version"] == "1.2.3"
    assert str(record["timestamp"]).endswith("Z")
    assert datetime.fromisoformat(str(record["timestamp"])).tzinfo == UTC


@pytest.mark.asyncio
async def test_context_is_isolated_between_concurrent_tasks(
    capsys: pytest.CaptureFixture[str],
) -> None:
    _configure_json()

    async def log_job(job_id: str) -> None:
        bind_log_context(job_id=job_id, attempt=1, stage="transcribing")
        await asyncio.sleep(0)
        structlog.get_logger("worker").info("job_progress")
        clear_log_context()

    await asyncio.gather(log_job("job-a"), log_job("job-b"))

    records = [json.loads(line) for line in capsys.readouterr().out.splitlines()]
    assert {record["job_id"] for record in records} == {"job-a", "job-b"}
    assert all(record["attempt"] == 1 for record in records)


def test_redacts_nested_secrets_content_and_url_query(
    capsys: pytest.CaptureFixture[str],
) -> None:
    _configure_json()

    structlog.get_logger("worker").info(
        "job_received",
        api_token="top-secret",
        nested={
            "password": "hunter2",
            "items": [{"authorization": "Bearer sensitive"}],
        },
        url="https://example.test/input?signature=private&expires=10",
        payload={"transcript": "do not emit"},
    )

    record = _one_json_line(capsys)
    rendered = json.dumps(record)
    assert "top-secret" not in rendered
    assert "hunter2" not in rendered
    assert "sensitive" not in rendered
    assert "do not emit" not in rendered
    assert "signature=private" not in rendered
    assert record["url"] == "https://example.test/input"


def test_exception_includes_type_and_redacted_traceback(
    capsys: pytest.CaptureFixture[str],
) -> None:
    _configure_json()

    try:
        raise ValueError("token=private-value")
    except ValueError:
        structlog.get_logger("worker").error("job_failed", exc_info=True)

    record = _one_json_line(capsys)
    exception = record["exception"]
    assert isinstance(exception, dict)
    assert exception["type"] == "ValueError"
    assert "ValueError" in str(exception["traceback"])
    assert "private-value" not in str(exception["traceback"])


def test_console_format_is_human_readable(capsys: pytest.CaptureFixture[str]) -> None:
    configure_logging("INFO", log_format="console")

    structlog.get_logger("worker").info("job_started", job_id="job-1")

    output = capsys.readouterr().out
    assert "job_started" in output
    assert not output.lstrip().startswith("{")


def test_reconfiguration_replaces_handlers_without_duplicate_output(
    capsys: pytest.CaptureFixture[str],
) -> None:
    _configure_json()
    _configure_json()

    logging.getLogger("legacy").info("legacy event")

    assert len(logging.getLogger().handlers) == 1
    record = _one_json_line(capsys)
    assert record["event"] == "stdlib_log"
    assert record["message"] == "legacy event"
