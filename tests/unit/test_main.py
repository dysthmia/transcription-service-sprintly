from __future__ import annotations

import asyncio
from types import SimpleNamespace

import pytest

from app.main import run_runtime


class BlockingWorker:
    def __init__(self, expected_workers: int) -> None:
        self._expected_workers = expected_workers
        self.started = asyncio.Event()
        self.release = asyncio.Event()
        self.calls = 0

    async def process_next_job(self) -> bool:
        self.calls += 1
        if self.calls == self._expected_workers:
            self.started.set()
        await self.release.wait()
        return True


class StoppableLoop:
    def __init__(self) -> None:
        self.stopped = False

    async def run(self) -> None:
        try:
            await asyncio.Event().wait()
        finally:
            self.stopped = True


class ClosableResource:
    def __init__(self, name: str, closed: list[str]) -> None:
        self._name = name
        self._closed = closed

    async def close(self) -> None:
        self._closed.append(self._name)


@pytest.mark.asyncio
async def test_runtime_starts_configured_workers_and_closes_dependencies() -> None:
    settings = SimpleNamespace(
        worker_concurrency=3,
        worker_poll_interval_seconds=0.01,
        shutdown_grace_seconds=1,
    )
    stop_event = asyncio.Event()
    worker = BlockingWorker(expected_workers=3)
    consumer = StoppableLoop()
    publisher = StoppableLoop()
    closed: list[str] = []
    deepseek = ClosableResource("deepseek", closed)
    groq = ClosableResource("groq", closed)
    database = ClosableResource("database", closed)

    runtime = asyncio.create_task(
        run_runtime(
            settings,
            consumer,
            worker,
            publisher,
            stop_event=stop_event,
            close_callbacks=[deepseek.close, groq.close, database.close],
            install_signal_handlers=False,
        )
    )
    await asyncio.wait_for(worker.started.wait(), timeout=1)

    stop_event.set()
    worker.release.set()
    await asyncio.wait_for(runtime, timeout=1)

    assert worker.calls == 3
    assert consumer.stopped
    assert publisher.stopped
    assert closed == ["deepseek", "groq", "database"]
