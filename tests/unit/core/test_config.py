from __future__ import annotations

import pytest
from pydantic import ValidationError

from app.core.config import Settings

REQUIRED_ENVIRONMENT = {
    "POSTGRES_HOST": "postgres",
    "POSTGRES_PORT": "5432",
    "POSTGRES_DB": "transcript",
    "POSTGRES_USER": "transcript",
    "POSTGRES_PASSWORD": "postgres-password",
    "KAFKA_BOOTSTRAP_SERVERS": "kafka:9092",
    "KAFKA_CONSUMER_GROUP": "transcript-service",
    "KAFKA_INPUT_TOPIC": "material.events",
    "KAFKA_OUTPUT_TOPIC": "transcript.events",
    "KAFKA_INPUT_DLQ_TOPIC": "material.events.dlq",
    "S3_ENDPOINT_URL": "https://s3.example.test",
    "S3_REGION": "ru-1",
    "S3_ACCESS_KEY_ID": "access-key",
    "S3_SECRET_ACCESS_KEY": "secret-access-key",
    "S3_OUTPUT_BUCKET": "transcript-output",
    "DEEPSEEK_API_KEY": "deepseek-secret",
    "GROQ_API_KEY": "groq-secret",
}


@pytest.fixture
def settings_environment(monkeypatch: pytest.MonkeyPatch) -> None:
    for name, value in REQUIRED_ENVIRONMENT.items():
        monkeypatch.setenv(name, value)


def test_settings_parse_numeric_values(
    settings_environment: None,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("WORKER_CONCURRENCY", "2")
    monkeypatch.setenv("MAX_PDF_PAGES", "250")
    monkeypatch.setenv("API_REQUEST_TIMEOUT_SECONDS", "90")

    settings = Settings(_env_file=None)

    assert settings.worker_concurrency == 2
    assert settings.max_pdf_pages == 250
    assert settings.api_request_timeout_seconds == 90


def test_settings_require_external_configuration(
    settings_environment: None,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("KAFKA_BOOTSTRAP_SERVERS")

    with pytest.raises(ValidationError, match="kafka_bootstrap_servers"):
        Settings(_env_file=None)


def test_settings_redact_secrets(settings_environment: None) -> None:
    settings = Settings(_env_file=None)

    rendered = repr(settings)

    assert "deepseek-secret" not in rendered
    assert "secret-access-key" not in rendered
    assert "**********" in rendered
