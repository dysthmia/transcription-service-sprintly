from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass(frozen=True, slots=True)
class NewJob:
    job_id: str
    material_id: str
    user_id: str
    correlation_id: str
    source_event_id: str
    pipeline_version: str
    source_bucket: str | None = None
    source_key: str | None = None
    source_url: str | None = None
    models: dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        has_s3_source = self.source_bucket is not None and self.source_key is not None
        has_partial_s3_source = (self.source_bucket is None) != (self.source_key is None)
        has_url_source = self.source_url is not None

        if has_partial_s3_source or has_s3_source == has_url_source:
            raise ValueError("A job must have exactly one complete source")


@dataclass(frozen=True, slots=True)
class NewOutboxEvent:
    event_id: str
    topic: str
    partition_key: str
    event_type: str
    payload: dict[str, Any]