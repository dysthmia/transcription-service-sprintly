"""Create local Kafka topics and MinIO buckets for manual integration testing."""

from __future__ import annotations

import asyncio
import os

import aioboto3
from aiokafka.admin import AIOKafkaAdminClient, NewTopic
from botocore.config import Config
from botocore.exceptions import ClientError


def required(name: str) -> str:
    value = os.environ.get(name)
    if not value:
        raise RuntimeError(f"{name} must be set")
    return value


async def create_topics() -> None:
    admin = AIOKafkaAdminClient(bootstrap_servers=required("KAFKA_BOOTSTRAP_SERVERS"))
    await admin.start()
    try:
        existing_topics = set(await admin.list_topics())
        topics = [
            NewTopic(name=required(name), num_partitions=1, replication_factor=1)
            for name in ("KAFKA_INPUT_TOPIC", "KAFKA_OUTPUT_TOPIC", "KAFKA_INPUT_DLQ_TOPIC")
            if required(name) not in existing_topics
        ]
        if topics:
            await admin.create_topics(topics)
    finally:
        await admin.close()


async def create_buckets() -> None:
    session = aioboto3.Session()
    buckets = {required("S3_SOURCE_BUCKET"), required("S3_OUTPUT_BUCKET")}
    async with session.client(
        "s3",
        endpoint_url=required("S3_ENDPOINT_URL"),
        region_name=required("S3_REGION"),
        aws_access_key_id=required("S3_ACCESS_KEY_ID"),
        aws_secret_access_key=required("S3_SECRET_ACCESS_KEY"),
        config=Config(s3={"addressing_style": required("S3_ADDRESSING_STYLE")}),
    ) as client:
        for bucket in buckets:
            try:
                await client.create_bucket(Bucket=bucket)
            except ClientError as error:
                if error.response["Error"]["Code"] not in {
                    "BucketAlreadyExists",
                    "BucketAlreadyOwnedByYou",
                }:
                    raise


async def main() -> None:
    last_error: Exception | None = None
    for attempt in range(30):
        try:
            await create_topics()
            await create_buckets()
            return
        except Exception as error:
            last_error = error
            if attempt == 29:
                raise
            await asyncio.sleep(2)

    assert last_error is not None
    raise last_error


if __name__ == "__main__":
    asyncio.run(main())
