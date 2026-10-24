from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest
from botocore.exceptions import ClientError, EndpointConnectionError
from pydantic import SecretStr

from app.core.exceptions import ContentError, TransientExternalError
from app.infrastructure.s3.client import S3Client

pytestmark = pytest.mark.asyncio


class FakeClientContext:
    def __init__(self, client: Any) -> None:
        self._client = client

    async def __aenter__(self) -> Any:
        return self._client

    async def __aexit__(self, *args: object) -> None:
        return None


class FakeSession:
    def __init__(self, client: Any) -> None:
        self.client_instance = client
        self.client_arguments: dict[str, Any] | None = None

    def client(self, service_name: str, **kwargs: Any) -> FakeClientContext:
        assert service_name == "s3"
        self.client_arguments = kwargs
        return FakeClientContext(self.client_instance)


class FakeBody:
    def __init__(self, chunks: list[bytes]) -> None:
        self._chunks = iter(chunks)
        self.closed = False

    async def read(self, _: int) -> bytes:
        return next(self._chunks, b"")

    def close(self) -> None:
        self.closed = True


class FakeS3Api:
    def __init__(self) -> None:
        self.calls: list[tuple[str, tuple[Any, ...], dict[str, Any]]] = []
        self.body = FakeBody([b"part one", b" part two"])
        self.error: Exception | None = None

    async def get_object(self, **kwargs: Any) -> dict[str, Any]:
        self.calls.append(("get_object", (), kwargs))
        if self.error is not None:
            raise self.error
        return {"Body": self.body}

    async def upload_file(self, *args: Any) -> None:
        self.calls.append(("upload_file", args, {}))

    async def put_object(self, **kwargs: Any) -> None:
        self.calls.append(("put_object", (), kwargs))

    async def head_object(self, **kwargs: Any) -> None:
        self.calls.append(("head_object", (), kwargs))
        if self.error is not None:
            raise self.error


@pytest.fixture
def s3_settings() -> SimpleNamespace:
    return SimpleNamespace(
        s3_endpoint_url="https://objects.example.test",
        s3_region="ru-1",
        s3_access_key_id=SecretStr("access-key"),
        s3_secret_access_key=SecretStr("secret-key"),
        s3_output_bucket="output-bucket",
        s3_addressing_style="auto",
    )


@pytest.fixture
def fake_api() -> FakeS3Api:
    return FakeS3Api()


@pytest.fixture
def s3_client(s3_settings: SimpleNamespace, fake_api: FakeS3Api) -> S3Client:
    return S3Client(s3_settings, session=FakeSession(fake_api))  # type: ignore[arg-type]


async def test_download_file_uses_source_bucket_and_key(
    s3_client: S3Client,
    fake_api: FakeS3Api,
    tmp_path: Path,
) -> None:
    destination = await s3_client.download_file(
        "source-bucket", "materials/source.pdf", tmp_path / "source.pdf"
    )

    assert destination.read_bytes() == b"part one part two"
    assert fake_api.calls == [
        ("get_object", (), {"Bucket": "source-bucket", "Key": "materials/source.pdf"})
    ]
    assert fake_api.body.closed


async def test_upload_text_uses_default_output_bucket_and_utf8_body(
    s3_client: S3Client,
    fake_api: FakeS3Api,
) -> None:
    await s3_client.upload_text("Конспект", "jobs/job-1/summary.md")

    assert fake_api.calls == [
        (
            "put_object",
            (),
            {
                "Bucket": "output-bucket",
                "Key": "jobs/job-1/summary.md",
                "Body": "Конспект".encode(),
                "ContentType": "text/markdown; charset=utf-8",
            },
        )
    ]


async def test_upload_file_uses_requested_bucket_and_key(
    s3_client: S3Client,
    fake_api: FakeS3Api,
    tmp_path: Path,
) -> None:
    source = tmp_path / "summary.md"
    source.write_text("content", encoding="utf-8")

    await s3_client.upload_file(source, "jobs/job-1/summary.md", "custom-bucket")

    assert fake_api.calls == [
        (
            "upload_file",
            (str(source), "custom-bucket", "jobs/job-1/summary.md"),
            {},
        )
    ]


async def test_missing_source_becomes_content_error(
    s3_client: S3Client,
    fake_api: FakeS3Api,
    tmp_path: Path,
) -> None:
    fake_api.error = ClientError(
        {"Error": {"Code": "NoSuchKey"}, "ResponseMetadata": {"HTTPStatusCode": 404}},
        "GetObject",
    )

    with pytest.raises(ContentError, match="source_not_found"):
        await s3_client.download_file("source-bucket", "missing.pdf", tmp_path / "missing.pdf")


async def test_network_failure_becomes_retryable_error(
    s3_client: S3Client,
    fake_api: FakeS3Api,
) -> None:
    fake_api.error = EndpointConnectionError(endpoint_url="https://objects.example.test")

    with pytest.raises(TransientExternalError, match="s3_temporary_failure"):
        await s3_client.exists("source-bucket", "source.pdf")


async def test_exists_returns_false_for_missing_object(
    s3_client: S3Client,
    fake_api: FakeS3Api,
) -> None:
    fake_api.error = ClientError(
        {"Error": {"Code": "NoSuchKey"}, "ResponseMetadata": {"HTTPStatusCode": 404}},
        "HeadObject",
    )

    assert not await s3_client.exists("source-bucket", "missing.pdf")
