from __future__ import annotations

import json
from contextlib import asynccontextmanager
from types import SimpleNamespace
from typing import Any

import pytest

from app.infrastructure.kafka.consumer import MaterialUploadedConsumer


class FakeSession:
    def __init__(self, calls: list[str]) -> None:
        self._calls = calls

    @asynccontextmanager
    async def begin(self) -> Any:
        try:
            yield self
        except Exception:
            self._calls.append("db_rollback")
            raise
        else:
            self._calls.append("db_commit")


class FakeDatabase:
    def __init__(self, calls: list[str]) -> None:
        self._session = FakeSession(calls)

    @asynccontextmanager
    async def session(self) -> Any:
        yield self._session


class FakeRepository:
    processed_event_is_new = True

    def __init__(self, _: FakeSession) -> None:
        pass

    async def register_processed_event(self, _: str) -> bool:
        return self.processed_event_is_new

    async def create_job(self, job: Any) -> bool:
        self.created_job = job
        return True


class FakeConsumer:
    def __init__(self, calls: list[str]) -> None:
        self._calls = calls
        self.commits: list[dict[Any, Any]] = []

    async def commit(self, offsets: dict[Any, Any]) -> None:
        self._calls.append("offset_commit")
        self.commits.append(offsets)


class FakeProducer:
    def __init__(self) -> None:
        self.sent: list[tuple[str, bytes]] = []

    async def send_and_wait(self, topic: str, value: bytes) -> None:
        self.sent.append((topic, value))


@pytest.fixture
def settings() -> SimpleNamespace:
    return SimpleNamespace(
        kafka_bootstrap_servers="kafka:9092",
        kafka_consumer_group="transcript-service",
        kafka_input_topic="material.events",
        kafka_input_dlq_topic="material.events.dlq",
        kafka_security_protocol=None,
        kafka_sasl_mechanism=None,
        kafka_sasl_username=None,
        kafka_sasl_password=None,
        pipeline_version="v1",
    )


def valid_message() -> SimpleNamespace:
    return SimpleNamespace(
        value=json.dumps(
            {
                "event_id": "evt-1",
                "event_type": "material_uploaded",
                "schema_version": 1,
                "correlation_id": "corr-1",
                "job_id": "job-1",
                "user_id": "user-1",
                "material_id": "material-1",
                "source": {
                    "type": "s3",
                    "bucket": "learning-materials",
                    "key": "users/user-1/materials/material-1/source.pdf",
                },
            }
        ).encode(),
        topic="material.events",
        partition=2,
        offset=7,
    )


@pytest.mark.asyncio
async def test_handler_commits_offset_only_after_database_commit(
    settings: SimpleNamespace,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[str] = []
    consumer_client = FakeConsumer(calls)
    monkeypatch.setattr("app.infrastructure.kafka.consumer.JobsRepository", FakeRepository)
    handler = MaterialUploadedConsumer(
        settings,
        FakeDatabase(calls),
        consumer=consumer_client,
        dlq_producer=FakeProducer(),
    )

    await handler.handle_message(valid_message())

    assert calls == ["db_commit", "offset_commit"]
    committed_offset = next(iter(consumer_client.commits[0].values()))
    assert committed_offset.offset == 8


@pytest.mark.asyncio
async def test_duplicate_event_does_not_create_another_job(
    settings: SimpleNamespace,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class DuplicateRepository(FakeRepository):
        processed_event_is_new = False

        async def create_job(self, job: Any) -> bool:
            raise AssertionError(f"Unexpected job creation: {job}")

    calls: list[str] = []
    monkeypatch.setattr("app.infrastructure.kafka.consumer.JobsRepository", DuplicateRepository)
    handler = MaterialUploadedConsumer(
        settings,
        FakeDatabase(calls),
        consumer=FakeConsumer(calls),
        dlq_producer=FakeProducer(),
    )

    await handler.handle_message(valid_message())

    assert calls == ["db_commit", "offset_commit"]


@pytest.mark.asyncio
async def test_invalid_payload_is_sent_to_dlq_before_offset_commit(
    settings: SimpleNamespace,
) -> None:
    calls: list[str] = []
    producer = FakeProducer()
    handler = MaterialUploadedConsumer(
        settings,
        FakeDatabase(calls),
        consumer=FakeConsumer(calls),
        dlq_producer=producer,
    )
    message = valid_message()
    message.value = b"not-json"

    await handler.handle_message(message)

    assert producer.sent == [
        (
            "material.events.dlq",
            json.dumps(
                {
                    "event_type": "invalid_material_uploaded",
                    "error_code": "invalid_payload",
                    "source_topic": "material.events",
                    "source_partition": 2,
                    "source_offset": 7,
                },
                ensure_ascii=False,
            ).encode("utf-8"),
        )
    ]
    assert calls == ["offset_commit"]
