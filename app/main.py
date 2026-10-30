"""Точка запуска фонового приложения Transcript."""

from __future__ import annotations

import asyncio
import logging
import signal
from collections.abc import Awaitable, Callable
from typing import Any

from app.core.config import Settings, get_settings
from app.core.database import Database
from app.core.logging import configure_logging
from app.features.extraction.service import ExtractionService
from app.features.summarization.service import SummarizationService
from app.infrastructure.ai.deepseek import DeepSeekClient
from app.infrastructure.ai.groq import GroqClient
from app.infrastructure.kafka.consumer import MaterialUploadedConsumer
from app.infrastructure.kafka.producer import KafkaProducer
from app.infrastructure.s3.client import S3Client
from app.workers.outbox_publisher import OutboxPublisher
from app.workers.processor import JobProcessor

logger = logging.getLogger(__name__)


async def worker_loop(
    processor: JobProcessor,
    stop_event: asyncio.Event,
    poll_interval_seconds: float,
) -> None:
    """Последовательно обрабатывает jobs, пока приложение не запросит остановку."""
    while not stop_event.is_set():
        processed = await processor.process_next_job()
        if not processed:
            await _wait_for_stop_or_timeout(stop_event, poll_interval_seconds)


async def run_runtime(
    settings: Settings,
    consumer: MaterialUploadedConsumer,
    processor: JobProcessor,
    outbox_publisher: OutboxPublisher,
    *,
    stop_event: asyncio.Event | None = None,
    close_callbacks: list[Callable[[], Awaitable[None]]] | None = None,
    install_signal_handlers: bool = True,
) -> None:
    """Запускает фоновые задачи и завершает их в контролируемом порядке."""
    shutdown_requested = stop_event or asyncio.Event()
    if install_signal_handlers:
        _install_signal_handlers(shutdown_requested)

    consumer_task = asyncio.create_task(consumer.run(), name="kafka-consumer")
    publisher_task = asyncio.create_task(outbox_publisher.run(), name="outbox-publisher")
    worker_tasks = [
        asyncio.create_task(
            worker_loop(
                processor,
                shutdown_requested,
                settings.worker_poll_interval_seconds,
            ),
            name=f"job-worker-{index + 1}",
        )
        for index in range(settings.worker_concurrency)
    ]
    background_tasks = [consumer_task, publisher_task, *worker_tasks]
    stop_task = asyncio.create_task(shutdown_requested.wait(), name="shutdown-wait")

    try:
        done, _ = await asyncio.wait(
            [stop_task, *background_tasks], return_when=asyncio.FIRST_COMPLETED
        )
        if stop_task not in done:
            _log_finished_background_tasks(done)
            shutdown_requested.set()
    finally:
        stop_task.cancel()
        await asyncio.gather(stop_task, return_exceptions=True)

        for task in (consumer_task, publisher_task):
            task.cancel()
        await asyncio.gather(consumer_task, publisher_task, return_exceptions=True)

        await _finish_workers(worker_tasks, settings.shutdown_grace_seconds)
        await _close_resources(close_callbacks or [])


async def main() -> None:
    """Собирает зависимости приложения и запускает его до SIGTERM/SIGINT."""
    settings = get_settings()
    configure_logging(settings.log_level)

    database = Database(settings)
    deepseek_client = DeepSeekClient(settings)
    groq_client = GroqClient(settings)
    s3_client = S3Client(settings)
    extraction_service = ExtractionService(settings, deepseek_client, groq_client)
    summarization_service = SummarizationService(settings, deepseek_client)
    consumer = MaterialUploadedConsumer(settings, database)
    processor = JobProcessor(
        settings,
        database,
        s3_client,
        extraction_service,
        summarization_service,
    )
    outbox_publisher = OutboxPublisher(settings, database, KafkaProducer(settings))

    await run_runtime(
        settings,
        consumer,
        processor,
        outbox_publisher,
        close_callbacks=[deepseek_client.close, groq_client.close, database.dispose],
    )


async def _wait_for_stop_or_timeout(stop_event: asyncio.Event, timeout: float) -> None:
    try:
        await asyncio.wait_for(stop_event.wait(), timeout=timeout)
    except TimeoutError:
        pass


def _install_signal_handlers(stop_event: asyncio.Event) -> None:
    loop = asyncio.get_running_loop()
    for signal_number in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(signal_number, stop_event.set)
        except (NotImplementedError, RuntimeError):
            logger.warning("Signal handler is unavailable for %s", signal_number.name)


def _log_finished_background_tasks(tasks: set[asyncio.Task[Any]]) -> None:
    for task in tasks:
        if task.cancelled():
            continue
        error = task.exception()
        if error is None:
            logger.warning("Background task finished unexpectedly: %s", task.get_name())
        else:
            logger.error("Background task failed: %s", task.get_name(), exc_info=error)


async def _finish_workers(
    worker_tasks: list[asyncio.Task[None]], shutdown_grace_seconds: float
) -> None:
    _, pending = await asyncio.wait(worker_tasks, timeout=shutdown_grace_seconds)
    if pending:
        logger.warning("Cancelling %d worker(s) after shutdown grace period", len(pending))
        for task in pending:
            task.cancel()
    await asyncio.gather(*worker_tasks, return_exceptions=True)


async def _close_resources(close_callbacks: list[Callable[[], Awaitable[None]]]) -> None:
    for close in close_callbacks:
        try:
            await close()
        except Exception:
            logger.exception("Failed to close application resource")


if __name__ == "__main__":
    asyncio.run(main())
