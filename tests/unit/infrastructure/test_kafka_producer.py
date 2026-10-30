from __future__ import annotations

import json
from types import SimpleNamespace

import pytest

from app.infrastructure.kafka.producer import KafkaProducer


class FakeAIOKafkaProducer:
    def __init__(self) -> None:
        self.sent: list[tuple[str, bytes, bytes]] = []

    async def send_and_wait(self, topic: str, value: bytes, key: bytes) -> None:
        self.sent.append((topic, value, key))


@pytest.mark.asyncio
async def test_publishes_json_with_partition_key_as_kafka_key() -> None:
    client = FakeAIOKafkaProducer()
    producer = KafkaProducer(
        SimpleNamespace(
            kafka_bootstrap_servers="kafka:9092",
            kafka_security_protocol=None,
            kafka_sasl_mechanism=None,
            kafka_sasl_username=None,
            kafka_sasl_password=None,
        ),
        producer=client,
    )
    payload = {"event_id": "evt-1", "event_type": "summary_generated"}

    await producer.publish("transcript.events", "user-1", payload)

    assert client.sent == [
        (
            "transcript.events",
            json.dumps(payload, ensure_ascii=False, separators=(",", ":")).encode("utf-8"),
            b"user-1",
        )
    ]
