"""Kafka intake для регистрации заданий на обработку материалов."""

from __future__ import annotations

import json
import logging
from collections.abc import AsyncIterator
from typing import Any

from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from aiokafka.structs import OffsetAndMetadata, TopicPartition
from pydantic import ValidationError

from app.core.config import Settings
from app.core.database import Database
from app.features.jobs.repository import JobsRepository
from app.features.jobs.schemas import NewJob
from app.infrastructure.kafka.schemas import (
    MaterialUploadedEvent,
    S3Source,
    YouTubeSource,
)

logger = logging.getLogger(__name__)


def _kafka_connection_options(settings: Settings) -> dict[str, Any]:
    """Формирует общие параметры подключения к Kafka из настроек приложения."""
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


class MaterialUploadedConsumer:
    """Принимает и надёжно регистрирует события material_uploaded."""

    def __init__(
        self,
        settings: Settings,
        database: Database,
        *,
        consumer: Any | None = None,
        dlq_producer: Any | None = None,
    ) -> None:
        self._settings = settings
        self._database = database
        connection_options = _kafka_connection_options(settings)
        self._consumer = consumer or AIOKafkaConsumer(
            settings.kafka_input_topic,
            group_id=settings.kafka_consumer_group,
            enable_auto_commit=False,
            **connection_options,
        )
        self._dlq_producer = dlq_producer or AIOKafkaProducer(**connection_options)

    async def start(self) -> None:
        """Запускает consumer и producer, используемый только для входного DLQ."""
        await self._consumer.start()
        try:
            await self._dlq_producer.start()
        except Exception:
            await self._consumer.stop()
            raise

    async def stop(self) -> None:
        """Закрывает клиентов Kafka."""
        await self._dlq_producer.stop()
        await self._consumer.stop()

    async def run(self) -> None:
        """Обрабатывает сообщения, пока вызывающая задача не будет отменена."""
        await self.start()
        try:
            async for message in self._messages():
                await self.handle_message(message)
        finally:
            await self.stop()

    async def handle_message(self, message: Any) -> None:
        """Регистрирует сообщение и подтверждает offset только после сохранения."""
        try:
            payload = json.loads(message.value)
            event = MaterialUploadedEvent.model_validate(payload)
        except (UnicodeDecodeError, json.JSONDecodeError, ValidationError):
            await self._send_to_dlq(message)
            await self._commit_offset(message)
            return

        await self._register_job(event)
        await self._commit_offset(message)

    async def _messages(self) -> AsyncIterator[Any]:
        async for message in self._consumer:
            yield message

    async def _register_job(self, event: MaterialUploadedEvent) -> bool:
        """Сохраняет дедупликацию события и регистрацию job в одной транзакции."""
        async with self._database.session() as session, session.begin():
            repository = JobsRepository(session)
            is_new_event = await repository.register_processed_event(event.event_id)
            if not is_new_event:
                return False

            await repository.create_job(self._new_job(event))
            return True

    def _new_job(self, event: MaterialUploadedEvent) -> NewJob:
        source = event.source
        if isinstance(source, S3Source):
            return NewJob(
                job_id=event.job_id,
                material_id=event.material_id,
                user_id=event.user_id,
                correlation_id=event.correlation_id,
                source_event_id=event.event_id,
                pipeline_version=self._settings.pipeline_version,
                source_bucket=source.bucket,
                source_key=source.key,
            )

        assert isinstance(source, YouTubeSource)
        return NewJob(
            job_id=event.job_id,
            material_id=event.material_id,
            user_id=event.user_id,
            correlation_id=event.correlation_id,
            source_event_id=event.event_id,
            pipeline_version=self._settings.pipeline_version,
            source_url=source.url,
        )

    async def _send_to_dlq(self, message: Any) -> None:
        """Публикует диагностику, не содержащую тело невалидного сообщения."""
        diagnostic = {
            "event_type": "invalid_material_uploaded",
            "error_code": "invalid_payload",
            "source_topic": message.topic,
            "source_partition": message.partition,
            "source_offset": message.offset,
        }
        await self._dlq_producer.send_and_wait(
            self._settings.kafka_input_dlq_topic,
            json.dumps(diagnostic, ensure_ascii=False).encode("utf-8"),
        )
        logger.warning(
            "Invalid Kafka payload sent to input DLQ: topic=%s partition=%s offset=%s",
            message.topic,
            message.partition,
            message.offset,
        )

    async def _commit_offset(self, message: Any) -> None:
        topic_partition = TopicPartition(message.topic, message.partition)
        offset = OffsetAndMetadata(message.offset + 1, "")
        await self._consumer.commit({topic_partition: offset})
