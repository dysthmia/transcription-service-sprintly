from __future__ import annotations

import os
from collections.abc import AsyncGenerator, Generator
from pathlib import Path

import pytest
import pytest_asyncio
from alembic.config import Config
from sqlalchemy import delete
from sqlalchemy.engine import make_url
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from alembic import command
from app.features.jobs.models import OutboxEvent, ProcessedEvent, TranscriptionJob

PROJECT_ROOT = Path(__file__).resolve().parents[2]


@pytest.fixture(scope="session")
def test_database_url() -> str:
    database_url = os.environ.get("TEST_DATABASE_URL")
    if not database_url:
        pytest.skip("TEST_DATABASE_URL is required for PostgreSQL integration tests")
    database_name = make_url(database_url).database
    if not database_name or "test" not in database_name.lower():
        pytest.fail("TEST_DATABASE_URL must point to a dedicated database with 'test' in its name")
    return database_url


@pytest.fixture(scope="session", autouse=True)
def migrated_test_database(test_database_url: str) -> Generator[None, None, None]:
    """Rebuilds the dedicated database through Alembic before the integration suite."""
    config = Config(str(PROJECT_ROOT / "alembic.ini"))
    config.set_main_option("sqlalchemy.url", test_database_url)
    command.downgrade(config, "base")
    command.upgrade(config, "head")
    yield
    command.downgrade(config, "base")


@pytest_asyncio.fixture
async def session_factory(
    test_database_url: str,
    migrated_test_database: None,
) -> AsyncGenerator[async_sessionmaker[AsyncSession], None]:
    engine = create_async_engine(test_database_url)
    factory = async_sessionmaker(engine, expire_on_commit=False)

    async with factory() as session, session.begin():
        await session.execute(delete(OutboxEvent))
        await session.execute(delete(ProcessedEvent))
        await session.execute(delete(TranscriptionJob))

    try:
        yield factory
    finally:
        async with factory() as session, session.begin():
            await session.execute(delete(OutboxEvent))
            await session.execute(delete(ProcessedEvent))
            await session.execute(delete(TranscriptionJob))
        await engine.dispose()
