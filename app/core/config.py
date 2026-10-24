from __future__ import annotations

from functools import lru_cache

from pydantic import SecretStr
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Конфигурации приложения загружающиеся из переменных среды."""

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    app_env: str = "development"
    log_level: str = "INFO"
    worker_concurrency: int = 3
    job_target_seconds: int = 180
    job_max_attempts: int = 3
    pipeline_version: str = "v1"

    postgres_host_port: str | None = None
    postgres_host: str
    postgres_port: int
    postgres_db: str
    postgres_user: str
    postgres_password: SecretStr

    kafka_bootstrap_servers: str
    kafka_consumer_group: str
    kafka_input_topic: str
    kafka_output_topic: str
    kafka_input_dlq_topic: str
    kafka_security_protocol: str | None = None
    kafka_sasl_mechanism: str | None = None
    kafka_sasl_username: str | None = None
    kafka_sasl_password: SecretStr | None = None

    s3_endpoint_url: str
    s3_region: str
    s3_access_key_id: SecretStr
    s3_secret_access_key: SecretStr
    s3_output_bucket: str
    s3_addressing_style: str = "auto"

    deepseek_api_key: SecretStr
    deepseek_base_url: str = "https://api.deepseek.com"
    deepseek_model: str = "deepseek-flash"

    groq_api_key: SecretStr
    groq_base_url: str = "https://api.groq.com/openai/v1"
    stt_model: str = "whisper-large-v3-turbo"

    max_document_file_size_mb: int = 50
    max_pdf_pages: int = 500
    max_media_duration_seconds: int = 3600
    max_media_file_size_mb: int = 200
    api_request_timeout_seconds: int = 120


@lru_cache
def get_settings() -> Settings:
    """Последующие обращения возвращают ранее сохранённый результат."""
    return Settings()
