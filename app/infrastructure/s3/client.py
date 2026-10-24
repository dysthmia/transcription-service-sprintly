from __future__ import annotations

import asyncio
import inspect
import tempfile
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any

import aioboto3
from botocore.config import Config
from botocore.exceptions import (
    ClientError,
    ConnectionClosedError,
    ConnectTimeoutError,
    EndpointConnectionError,
    ReadTimeoutError,
)

from app.core.config import Settings
from app.core.exceptions import ContentError, TransientExternalError


class S3Client:
    """Small client for an S3-compatible object storage API."""

    def __init__(self, settings: Settings, session: Any | None = None) -> None:
        self._settings = settings
        self._session = session or aioboto3.Session()

    @asynccontextmanager
    async def _client(self) -> AsyncGenerator[Any, None]:
        # AsyncGenerator[S3Client, None] ?
        config = Config(s3={"addressing_style": self._settings.s3_addressing_style})
        async with self._session.client(
            "s3",
            endpoint_url=self._settings.s3_endpoint_url,
            region_name=self._settings.s3_region,
            aws_access_key_id=self._settings.s3_access_key_id.get_secret_value(),
            aws_secret_access_key=self._settings.s3_secret_access_key.get_secret_value(),
            config=config,
        ) as client:
            yield client

    async def download_file(
        self,
        bucket: str,
        key: str,
        destination: Path | None = None,
    ) -> Path:
        """Метод скачивает объект из S3 на диск."""
        is_temporary = destination is None
        if destination is None:
            # Создаётся временный файл. (delete = false потому что после выхода файл должен остаться)
            with tempfile.NamedTemporaryFile(delete=False) as temporary_file:
                destination = Path(temporary_file.name)
        else:
            # создаем директории 
            destination.parent.mkdir(parents=True, exist_ok=True)

        try:
            async with self._client() as client:
                response = await client.get_object(Bucket=bucket, Key=key)
                await self._write_body(response["Body"], destination)
        except (
            ClientError,
            ConnectionClosedError,
            ConnectTimeoutError,
            EndpointConnectionError,
            ReadTimeoutError,
        ) as error:
            if is_temporary:
                # Удаляем, если его уже нет то всё ок
                destination.unlink(missing_ok=True)
            self._raise_mapped_error(error)

        return destination

    async def upload_file(
        self,
        source: Path,
        key: str,
        bucket: str | None = None,
    ) -> None:
        """Метод загружает локальный файл в S3."""
        try:
            async with self._client() as client:
                await client.upload_file(str(source), bucket or self._settings.s3_output_bucket, key)
        except (
            ClientError,
            ConnectionClosedError,
            ConnectTimeoutError,
            EndpointConnectionError,
            ReadTimeoutError,
        ) as error:
            self._raise_mapped_error(error)

    async def upload_text(
        self,
        text: str,
        key: str,
        bucket: str | None = None,
        content_type: str = "text/markdown; charset=utf-8",
    ) -> None:
        """Загружает текст напрямую в S3, не создавая файл."""
        try:
            async with self._client() as client:
                await client.put_object(
                    Bucket=bucket or self._settings.s3_output_bucket,
                    Key=key,
                    Body=text.encode("utf-8"),
                    ContentType=content_type,
                )
        except (
            ClientError,
            ConnectionClosedError,
            ConnectTimeoutError,
            EndpointConnectionError,
            ReadTimeoutError,
        ) as error:
            self._raise_mapped_error(error)

    async def exists(self, bucket: str, key: str) -> bool:
        """Проверяет существует ли файл в S3 хранилище."""
        try:
            async with self._client() as client:
                await client.head_object(Bucket=bucket, Key=key)
        except ClientError as error:
            if self._is_missing_source(error):
                return False
            self._raise_mapped_error(error)
        except (
            ConnectionClosedError,
            ConnectTimeoutError,
            EndpointConnectionError,
            ReadTimeoutError,
        ) as error:
            self._raise_mapped_error(error)
        return True

    async def _write_body(self, body: Any, destination: Path) -> None:
        """Вызывается из download_file() и сохраняет скачиваемые данные на диск"""
        try:
            # Открывает файл для записи в бинарном режиме.
            # w - запись с перезаписью содержимого.
            # b — работа с байтами.
            with destination.open("wb") as output:
                # читает данные частями по 1 МБ
                while chunk := await body.read(1024 * 1024):
                    await asyncio.to_thread(output.write, chunk)
        finally:
            # Гарантирует попытку закрытия сетевого потока
            close = getattr(body, "close", None)
            if close is not None:
                result = close()
                # проверяет, нужно ли применять await к результату вызова close().
                if inspect.isawaitable(result):
                    await result

    @staticmethod
    def _is_missing_source(error: ClientError) -> bool:
        """
        Метод проверяет два условия:
            1. Код ошибки принадлежит множеству {"NoSuchKey", "NoSuchBucket", "NotFound"}.
            2. HTTP-статус равен 404.
        Если хотя бы одно условие выполняется, возвращается True.
        """
        code = error.response.get("Error", {}).get("Code", "")
        status = error.response.get("ResponseMetadata", {}).get("HTTPStatusCode")
        return code in {"NoSuchKey", "NoSuchBucket", "NotFound"} or status == 404

    @staticmethod
    def _raise_mapped_error(error: Exception) -> None:
        """Преобразовывает технические ошибки S3 в исключения, понятные бизнес-логике."""
        if isinstance(error, ClientError):
            if S3Client._is_missing_source(error):
                raise ContentError("source_not_found", "Исходный файл не найден.") from error

            status = error.response.get("ResponseMetadata", {}).get("HTTPStatusCode", 0)
            if status >= 500:
                raise TransientExternalError(
                    "s3_temporary_failure", "Хранилище временно недоступно."
                ) from error
            raise error

        if isinstance(
            error,
            (EndpointConnectionError, ConnectionClosedError, ConnectTimeoutError, ReadTimeoutError),
        ):
            raise TransientExternalError(
                "s3_temporary_failure", "Хранилище временно недоступно."
            ) from error

        raise error
