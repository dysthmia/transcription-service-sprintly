from __future__ import annotations

from contextlib import asynccontextmanager
from types import SimpleNamespace
from typing import Any

import pytest

from app.core.exceptions import TransientExternalError
from app.workers.outbox_publisher import OutboxPublisher


class FakeSession:
    @asynccontextmanager
    async def begin(self) -> Any:
        yield self


class FakeDatabase:
    @asynccontextmanager
    async def session(self) -> Any:
        yield FakeSession()


class FakeRepository:
    def __init__(self, events: list[SimpleNamespace], calls: list[str]) -> None:
        self._events = events
        self._calls = calls
        self.marked: list[str] = []
        self.retries: list[dict[str, Any]] = []

    async def get_pending_outbox_events(self) -> list[SimpleNamespace]:
        return self._events

    async def mark_outbox_published(self, event_id: str) -> bool:
        self._calls.append("mark")
        self.marked.append(event_id)
        return True

    async def schedule_outbox_retry(self, event_id: str, **kwargs: Any) -> bool:
        self._calls.append("retry")
        self.retries.append({"event_id": event_id, **kwargs})
        for outbox_event in self._events:
            if outbox_event.id == event_id:
                outbox_event.attempt_count += 1
        return True


class FakeKafkaProducer:
    def __init__(self, responses: list[Exception | None], calls: list[str]) -> None:
        self._responses = iter(responses)
        self._calls = calls
        self.published: list[tuple[str, str, dict[str, Any]]] = []

    async def publish(self, topic: str, partition_key: str, payload: dict[str, Any]) -> None:
        self._calls.append("send")
        self.published.append((topic, partition_key, payload))
        response = next(self._responses)
        if response is not None:
            raise response


def event() -> SimpleNamespace:
    return SimpleNamespace(
        id="evt-1",
        topic="transcript.events",
        partition_key="user-1",
        payload={"event_id": "evt-1", "event_type": "summary_generated"},
        attempt_count=0,
    )


def publisher(
    repository: FakeRepository,
    kafka: FakeKafkaProducer,
) -> OutboxPublisher:
    return OutboxPublisher(
        SimpleNamespace(outbox_poll_interval_seconds=1),
        FakeDatabase(),
        kafka,
        repository_factory=lambda _: repository,
    )


@pytest.mark.asyncio
async def test_marks_event_published_only_after_kafka_send() -> None:
    calls: list[str] = []
    repository = FakeRepository([event()], calls)
    kafka = FakeKafkaProducer([None], calls)

    published = await publisher(repository, kafka).publish_pending()

    assert published == 1
    assert calls == ["send", "mark"]
    assert repository.marked == ["evt-1"]
    assert not repository.retries


@pytest.mark.asyncio
async def test_temporary_failure_schedules_retry_without_marking_published() -> None:
    calls: list[str] = []
    repository = FakeRepository([event()], calls)
    kafka = FakeKafkaProducer(
        [TransientExternalError("kafka_unavailable", "Kafka недоступна.")], calls
    )

    published = await publisher(repository, kafka).publish_pending()

    assert published == 0
    assert calls == ["send", "retry"]
    assert not repository.marked
    assert repository.retries[0]["event_id"] == "evt-1"
    assert repository._events[0].attempt_count == 1


@pytest.mark.asyncio
async def test_retry_publishes_the_same_event_id_again() -> None:
    calls: list[str] = []
    outbox_event = event()
    repository = FakeRepository([outbox_event], calls)
    kafka = FakeKafkaProducer(
        [
            TransientExternalError("kafka_unavailable", "Kafka недоступна."),
            None,
        ],
        calls,
    )
    service = publisher(repository, kafka)

    await service.publish_pending()
    await service.publish_pending()

    assert [payload["event_id"] for _, _, payload in kafka.published] == [
        "evt-1",
        "evt-1",
    ]
    assert repository.marked == ["evt-1"]
