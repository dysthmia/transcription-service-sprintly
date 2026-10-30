"""Публикация сохранённых outbox-событий в Kafka с at-least-once семантикой."""

from __future__ import annotations

import asyncio
from collections.abc import Callable
from datetime import UTC, datetime, timedelta
from typing import Any

from app.core.config import Settings
from app.core.database import Database
from app.core.exceptions import TransientExternalError
from app.features.jobs.repository import JobsRepository
from app.infrastructure.kafka.producer import KafkaProducer


class OutboxPublisher:
    """Публикует pending outbox events и отмечает их после Kafka acknowledgement."""

    def __init__(
        self,
        settings: Settings,
        database: Database,
        producer: KafkaProducer,
        *,
        repository_factory: Callable[[Any], JobsRepository] = JobsRepository,
    ) -> None:
        self._settings = settings
        self._database = database
        self._producer = producer
        self._repository_factory = repository_factory

    async def run(self) -> None:
        """Запускает периодическую публикацию до отмены задачи приложения."""
        await self._producer.start()
        try:
            while True:
                await self.publish_pending()
                await asyncio.sleep(self._settings.outbox_poll_interval_seconds)
        finally:
            await self._producer.stop()

    async def publish_pending(self) -> int:
        """Делает одну попытку опубликовать все доступные pending-события."""
        events = await self._pending_events()
        published_count = 0
        for event in events:
            try:
                await self._producer.publish(event.topic, event.partition_key, event.payload)
            except TransientExternalError:
                await self._schedule_retry(event.id, event.attempt_count)
            else:
                await self._mark_published(event.id)
                published_count += 1
        return published_count

    async def _pending_events(self) -> list[Any]:
        async with self._database.session() as session:
            repository = self._repository_factory(session)
            return await repository.get_pending_outbox_events()

    async def _mark_published(self, event_id: str) -> None:
        async with self._database.session() as session, session.begin():
            repository = self._repository_factory(session)
            await repository.mark_outbox_published(event_id)

    async def _schedule_retry(self, event_id: str, attempt_count: int) -> None:
        retry_after_seconds = 5 * (2**attempt_count)
        retry_at = datetime.now(UTC) + timedelta(seconds=retry_after_seconds)
        async with self._database.session() as session, session.begin():
            repository = self._repository_factory(session)
            await repository.schedule_outbox_retry(event_id, retry_at=retry_at)
