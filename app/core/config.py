from __future__ import annotations

from functools import lru_cache
from typing import Literal
from urllib.parse import urlparse

from pydantic import Field, SecretStr, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Конфигурации приложения загружающиеся из переменных среды."""

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    app_env: Literal["development", "test", "production"] = "development"
    log_level: str = "INFO"
    log_format: Literal["json", "console"] | None = None
    service_name: str = "transcription-service"
    service_version: str = "unknown"
    worker_concurrency: int = Field(default=3, ge=1, le=16)
    worker_poll_interval_seconds: float = Field(default=1.0, gt=0, le=60)
    shutdown_grace_seconds: float = Field(default=30.0, gt=0, le=600)
    job_target_seconds: int = Field(default=180, gt=0)
    job_max_attempts: int = Field(default=3, ge=1, le=10)
    job_lease_seconds: int = Field(default=300, ge=30, le=3600)
    outbox_poll_interval_seconds: float = Field(default=1.0, gt=0, le=60)
    pipeline_version: str = "v1"
    summary_chunk_size: int = Field(default=12_000, ge=1_000, le=100_000)

    postgres_host_port: str | None = None
    postgres_host: str
    postgres_port: int = Field(ge=1, le=65535)
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
    s3_allowed_source_buckets: str = ""
    s3_addressing_style: str = "auto"
    s3_allow_insecure_endpoint: bool = False

    deepseek_api_key: SecretStr
    deepseek_base_url: str = "https://api.deepseek.com"
    deepseek_model: str = "deepseek-flash"

    groq_api_key: SecretStr
    groq_base_url: str = "https://api.groq.com/openai/v1"
    stt_model: str = "whisper-large-v3-turbo"

    max_document_file_size_mb: int = Field(default=50, ge=1, le=1024)
    max_source_file_size_mb: int = Field(default=200, ge=1, le=2048)
    max_pdf_pages: int = Field(default=500, ge=1, le=5_000)
    max_media_duration_seconds: int = Field(default=3600, ge=1, le=28_800)
    max_media_file_size_mb: int = Field(default=200, ge=1, le=2048)
    stt_chunk_seconds: int = Field(default=600, ge=30, le=3_600)
    api_request_timeout_seconds: int = Field(default=120, ge=1, le=600)
    media_command_timeout_seconds: int = Field(default=300, ge=10, le=3_600)
    youtube_download_timeout_seconds: int = Field(default=300, ge=10, le=3_600)

    @model_validator(mode="after")
    def validate_production_security(self) -> Settings:
        if self.max_source_file_size_mb < max(
            self.max_document_file_size_mb, self.max_media_file_size_mb
        ):
            raise ValueError("max_source_file_size_mb must cover all accepted file limits")

        if self.app_env != "production":
            return self

        if self.log_format == "console":
            raise ValueError("LOG_FORMAT must be json in production")
        if self.kafka_security_protocol not in {"SSL", "SASL_SSL"}:
            raise ValueError("Kafka must use SSL or SASL_SSL in production")
        if self.kafka_security_protocol == "SASL_SSL" and not (
            self.kafka_sasl_mechanism
            and self.kafka_sasl_username
            and self.kafka_sasl_password
        ):
            raise ValueError("SASL_SSL requires Kafka SASL credentials")
        if not self.allowed_source_buckets:
            raise ValueError("S3_ALLOWED_SOURCE_BUCKETS must be set in production")
        for name, url, allow_http in (
            ("S3_ENDPOINT_URL", self.s3_endpoint_url, self.s3_allow_insecure_endpoint),
            ("DEEPSEEK_BASE_URL", self.deepseek_base_url, False),
            ("GROQ_BASE_URL", self.groq_base_url, False),
        ):
            if urlparse(url).scheme != "https" and not allow_http:
                raise ValueError(f"{name} must use HTTPS in production")
        return self

    @property
    def allowed_source_buckets(self) -> frozenset[str]:
        return frozenset(
            bucket.strip()
            for bucket in self.s3_allowed_source_buckets.split(",")
            if bucket.strip()
        )


@lru_cache
def get_settings() -> Settings:
    """Последующие обращения возвращают ранее сохранённый результат."""
    return Settings()
