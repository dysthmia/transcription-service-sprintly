"""Upload fixtures and publish valid material_uploaded events for local testing."""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import shlex
import sys
from dataclasses import dataclass
from pathlib import Path
from uuid import uuid4

from app.core.config import Settings
from app.infrastructure.kafka.producer import KafkaProducer
from app.infrastructure.kafka.schemas import (
    MaterialUploadedEvent,
    S3Source,
    YouTubeSource,
)
from app.infrastructure.s3.client import S3Client


@dataclass(frozen=True)
class EventIdentifiers:
    event_id: str
    job_id: str
    correlation_id: str
    material_id: str
    user_id: str


def generated_identifiers() -> EventIdentifiers:
    """Return independent identifiers suitable for a single manual test event."""
    return EventIdentifiers(
        event_id=f"evt_{uuid4().hex}",
        job_id=f"job_{uuid4().hex}",
        correlation_id=f"corr_{uuid4().hex}",
        material_id=f"material_{uuid4().hex}",
        user_id="manual-user",
    )


def identifiers_from_args(args: argparse.Namespace) -> EventIdentifiers:
    generated = generated_identifiers()
    return EventIdentifiers(
        event_id=args.event_id or generated.event_id,
        job_id=args.job_id or generated.job_id,
        correlation_id=args.correlation_id or generated.correlation_id,
        material_id=args.material_id or generated.material_id,
        user_id=args.user_id or generated.user_id,
    )


def build_s3_event(identifiers: EventIdentifiers, bucket: str, key: str) -> MaterialUploadedEvent:
    return MaterialUploadedEvent(
        event_id=identifiers.event_id,
        event_type="material_uploaded",
        schema_version=1,
        correlation_id=identifiers.correlation_id,
        job_id=identifiers.job_id,
        material_id=identifiers.material_id,
        user_id=identifiers.user_id,
        source=S3Source(type="s3", bucket=bucket, key=key),
    )


def build_youtube_event(identifiers: EventIdentifiers, url: str) -> MaterialUploadedEvent:
    return MaterialUploadedEvent(
        event_id=identifiers.event_id,
        event_type="material_uploaded",
        schema_version=1,
        correlation_id=identifiers.correlation_id,
        job_id=identifiers.job_id,
        material_id=identifiers.material_id,
        user_id=identifiers.user_id,
        source=YouTubeSource(type="youtube", url=url),
    )


def add_identifiers(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--event-id", help="Reuse an event ID to test duplicate delivery.")
    parser.add_argument("--job-id", help="Reuse or select the job ID.")
    parser.add_argument("--correlation-id", help="Reuse or select the correlation ID.")
    parser.add_argument("--material-id", help="Reuse or select the material ID.")
    parser.add_argument("--user-id", default="manual-user", help="Kafka partition key (default: manual-user).")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)

    upload = commands.add_parser("upload", help="Upload a local fixture to MinIO.")
    upload.add_argument("--file", required=True, type=Path)
    upload.add_argument("--bucket", help="Source bucket (default: S3_SOURCE_BUCKET).")
    upload.add_argument("--key", help="Object key (default: manual/<filename>).")

    publish_s3 = commands.add_parser("publish-s3", help="Publish a valid S3 material_uploaded event.")
    publish_s3.add_argument("--bucket", help="Source bucket (default: S3_SOURCE_BUCKET).")
    publish_s3.add_argument("--key", required=True)
    add_identifiers(publish_s3)

    publish_youtube = commands.add_parser("publish-youtube", help="Publish a valid YouTube event.")
    publish_youtube.add_argument("--url", required=True)
    add_identifiers(publish_youtube)

    send_s3 = commands.add_parser("send-s3", help="Upload a fixture and publish its S3 event.")
    send_s3.add_argument("--file", required=True, type=Path)
    send_s3.add_argument("--bucket", help="Source bucket (default: S3_SOURCE_BUCKET).")
    send_s3.add_argument("--key", help="Object key (default: manual/<filename>).")
    add_identifiers(send_s3)
    return parser


async def upload(settings: Settings, file_path: Path, bucket: str, key: str) -> None:
    if not file_path.is_file():
        raise ValueError(f"File does not exist or is not a file: {file_path}")
    await S3Client(settings).upload_file(file_path, key=key, bucket=bucket)


async def publish(settings: Settings, event: MaterialUploadedEvent) -> None:
    producer = KafkaProducer(settings)
    await producer.start()
    try:
        await producer.publish(
            settings.kafka_input_topic,
            event.user_id,
            event.model_dump(mode="json"),
        )
    finally:
        await producer.stop()


def print_result(event: MaterialUploadedEvent, command: str) -> None:
    payload = event.model_dump(mode="json")
    identifiers = {
        key: payload[key]
        for key in ("event_id", "job_id", "correlation_id", "material_id", "user_id")
    }
    print(json.dumps({"identifiers": identifiers, "payload": payload}, ensure_ascii=False, indent=2))
    print("\nReplay the same event:")
    print(command)


def replay_command(event: MaterialUploadedEvent) -> str:
    source = event.source
    command = ["docker", "compose", "--profile", "tools", "run", "--rm", "test-sender"]
    if isinstance(source, S3Source):
        command.extend(["publish-s3", "--bucket", source.bucket, "--key", source.key])
    elif isinstance(source, YouTubeSource):
        command.extend(["publish-youtube", "--url", source.url])
    else:  # pragma: no cover - the Pydantic discriminated union is exhaustive.
        raise TypeError("Unsupported source")
    command.extend(
        [
            "--event-id",
            event.event_id,
            "--job-id",
            event.job_id,
            "--correlation-id",
            event.correlation_id,
            "--material-id",
            event.material_id,
            "--user-id",
            event.user_id,
        ]
    )
    return shlex.join(command)


async def run(args: argparse.Namespace) -> None:
    settings = Settings()
    if args.command == "upload":
        bucket = args.bucket or required_environment("S3_SOURCE_BUCKET")
        key = args.key or f"manual/{args.file.name}"
        await upload(settings, args.file, bucket, key)
        print(json.dumps({"bucket": bucket, "key": key}, ensure_ascii=False, indent=2))
        return

    if args.command == "publish-s3":
        bucket = args.bucket or required_environment("S3_SOURCE_BUCKET")
        event = build_s3_event(identifiers_from_args(args), bucket, args.key)
    elif args.command == "publish-youtube":
        event = build_youtube_event(identifiers_from_args(args), args.url)
    else:
        bucket = args.bucket or required_environment("S3_SOURCE_BUCKET")
        key = args.key or f"manual/{args.file.name}"
        await upload(settings, args.file, bucket, key)
        event = build_s3_event(identifiers_from_args(args), bucket, key)

    await publish(settings, event)
    print_result(event, replay_command(event))


def required_environment(name: str) -> str:
    value = os.environ.get(name)
    if not value:
        raise ValueError(f"{name} must be set")
    return value


def main() -> int:
    try:
        asyncio.run(run(build_parser().parse_args()))
    except (ValueError, OSError) as error:
        print(f"sender error: {error}", file=sys.stderr)
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
