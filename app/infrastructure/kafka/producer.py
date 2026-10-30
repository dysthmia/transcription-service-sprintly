"""Kafka producer для исходящих событий сервиса."""

from __future__ import annotations

import json
from typing import Any

from aiokafka import AIOKafkaProducer
from aiokafka.errors import KafkaError

from app.core.config import Settings
from app.core.exceptions import TransientExternalError


def _kafka_connection_options(settings: Settings) -> dict[str, Any]:
    """Формирует параметры подключения Kafka из конфигурации приложения."""
    options: dict[str, Any] = {"bootstrap_servers": settings.kafka_bootstrap_servers}
    if settings.kafka_security_protocol:
        options["security_protocol"] = settings.kafka_security_protocol
    if settings.kafka_sasl_mechanism:
        options["sasl_mechanism"] = settings.kafka_sasl_mechanism
    if settings.kafka_sasl_username:
        options["sasl_plain_username"] = settings.kafka_sasl_username
    if settings.kafka_sasl_password:
        options["sasl_plain_password"] = settings.kafka_sasl_password.get_secret_value()
    return options


class KafkaProducer:
    """Публикует JSON-события в Kafka с ключом партиционирования."""

    def __init__(self, settings: Settings, *, producer: Any | None = None) -> None:
        self._producer = producer or AIOKafkaProducer(**_kafka_connection_options(settings))

    async def start(self) -> None:
        await self._producer.start()

    async def stop(self) -> None:
        await self._producer.stop()

    async def publish(
        self, topic: str, partition_key: str, payload: dict[str, Any]
    ) -> None:
        """Сериализует payload и ожидает broker acknowledgement."""
        try:
            await self._producer.send_and_wait(
                topic,
                value=json.dumps(payload, ensure_ascii=False, separators=(",", ":")).encode(
                    "utf-8"
                ),
                key=partition_key.encode("utf-8"),
            )
        except (KafkaError, TimeoutError) as error:
            raise TransientExternalError(
                "kafka_unavailable", "Kafka временно недоступна для публикации события."
            ) from error
