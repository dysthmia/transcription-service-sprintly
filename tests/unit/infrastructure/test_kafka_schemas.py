from __future__ import annotations

import pytest
from pydantic import ValidationError

from app.infrastructure.kafka.schemas import (
    MaterialUploadedEvent,
    SummaryGeneratedEvent,
    TranscriptionFailedEvent,
)

COMMON_FIELDS = {
    "event_id": "evt-1",
    "schema_version": 1,
    "correlation_id": "corr-1",
    "job_id": "job-1",
    "user_id": "user-1",
}


def test_material_uploaded_accepts_s3_source() -> None:
    event = MaterialUploadedEvent.model_validate(
        {
            **COMMON_FIELDS,
            "event_type": "material_uploaded",
            "material_id": "material-1",
            "source": {
                "type": "s3",
                "bucket": "learning-materials",
                "key": "users/user-1/materials/material-1/source.pdf",
            },
        }
    )

    assert event.source.type == "s3"
    assert event.source.bucket == "learning-materials"


def test_material_uploaded_accepts_youtube_source() -> None:
    event = MaterialUploadedEvent.model_validate(
        {
            **COMMON_FIELDS,
            "event_type": "material_uploaded",
            "material_id": "material-1",
            "source": {
                "type": "youtube",
                "url": "https://www.youtube.com/watch?v=dQw4w9WgXcQ",
            },
        }
    )

    assert event.source.type == "youtube"
    assert event.source.url == "https://www.youtube.com/watch?v=dQw4w9WgXcQ"


def test_material_uploaded_rejects_non_youtube_url() -> None:
    with pytest.raises(ValidationError, match="YouTube"):
        MaterialUploadedEvent.model_validate(
            {
                **COMMON_FIELDS,
                "event_type": "material_uploaded",
                "material_id": "material-1",
                "source": {"type": "youtube", "url": "https://example.com/video"},
            }
        )


def test_material_uploaded_rejects_wrong_event_type() -> None:
    with pytest.raises(ValidationError):
        MaterialUploadedEvent.model_validate(
            {
                **COMMON_FIELDS,
                "event_type": "summary_generated",
                "material_id": "material-1",
                "source": {
                    "type": "s3",
                    "bucket": "learning-materials",
                    "key": "source.pdf",
                },
            }
        )


def test_outbound_events_serialize_to_json_compatible_dicts() -> None:
    summary_event = SummaryGeneratedEvent.model_validate(
        {
            **COMMON_FIELDS,
            "event_type": "summary_generated",
            "material_id": "material-1",
            "summary": {"bucket": "transcript-output", "key": "jobs/job-1/summary.md"},
        }
    )
    failed_event = TranscriptionFailedEvent.model_validate(
        {
            **COMMON_FIELDS,
            "event_type": "transcription_failed",
            "material_id": "material-1",
            "failed_stage": "extract",
            "error_code": "source_unavailable",
            "error_message": "Источник недоступен.",
            "retryable": False,
        }
    )

    assert summary_event.model_dump(mode="json") == {
        **COMMON_FIELDS,
        "event_type": "summary_generated",
        "material_id": "material-1",
        "summary": {"bucket": "transcript-output", "key": "jobs/job-1/summary.md"},
    }
    assert failed_event.model_dump(mode="json") == {
        **COMMON_FIELDS,
        "event_type": "transcription_failed",
        "material_id": "material-1",
        "failed_stage": "extract",
        "error_code": "source_unavailable",
        "error_message": "Источник недоступен.",
        "retryable": False,
    }
