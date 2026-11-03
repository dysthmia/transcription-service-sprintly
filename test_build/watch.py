"""Observe local Transcript Kafka output and input-DLQ messages without committing offsets."""

from __future__ import annotations

import argparse
import asyncio
import json
import sys
from collections.abc import Mapping
from typing import Any
from uuid import uuid4

from aiokafka import AIOKafkaConsumer

from app.core.config import Settings

OBSERVED_EVENT_TYPES = frozenset(
    {"summary_generated", "transcription_failed", "invalid_material_uploaded"}
)


def kafka_options(settings: Settings) -> dict[str, Any]:
    options: dict[str, Any] = {"bootstrap_servers": settings.kafka_bootstrap_servers}
    if settings.kafka_security_protocol:
        options["security_protocol"] = settings.kafka_security_protocol
    if settings.kafka_sasl_mechanism:
        options["sasl_mechanism"] = settings.kafka_sasl_mechanism
    if settings.kafka_sasl_username:
        options["sasl_plain_username"] = settings.kafka_sasl_username
    if settings.kafka_sasl_password:
        options["sasl_plain_password"] = settings.kafka_sasl_password.get_secret_value()
    return options


def default_group_id() -> str:
    return f"transcript-manual-watch-{uuid4().hex}"


def decode_observed_payload(value: bytes) -> dict[str, Any] | None:
    try:
        payload = json.loads(value.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError):
        return None
    if not isinstance(payload, dict) or payload.get("event_type") not in OBSERVED_EVENT_TYPES:
        return None
    return payload


def matches_job_id(payload: Mapping[str, Any], job_id: str | None) -> bool:
    return job_id is None or payload.get("job_id") == job_id


def format_message(message: Any, payload: Mapping[str, Any], group_id: str) -> str:
    return json.dumps(
        {
            "topic": message.topic,
            "partition": message.partition,
            "offset": message.offset,
            "consumer_group": group_id,
            "event": payload,
        },
        ensure_ascii=False,
        indent=2,
        sort_keys=True,
    )


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--job-id", help="Only display messages for this job ID.")
    parser.add_argument("--once", action="store_true", help="Exit after the first matching message.")
    parser.add_argument(
        "--group-id",
        help="Dedicated manual watcher group (default: a new unique group for this run).",
    )
    parser.add_argument(
        "--timeout-seconds",
        type=float,
        help="Exit with status 1 if no matching message arrives before this timeout.",
    )
    return parser


async def watch(args: argparse.Namespace) -> int:
    settings = Settings()
    group_id = args.group_id or default_group_id()
    consumer = AIOKafkaConsumer(
        settings.kafka_output_topic,
        settings.kafka_input_dlq_topic,
        group_id=group_id,
        enable_auto_commit=False,
        auto_offset_reset="earliest",
        **kafka_options(settings),
    )
    await consumer.start()
    matched = False
    loop = asyncio.get_running_loop()
    deadline = loop.time() + args.timeout_seconds if args.timeout_seconds else None
    try:
        while True:
            timeout_ms = 1_000
            if deadline is not None:
                remaining = deadline - loop.time()
                if remaining <= 0:
                    return 0 if matched else 1
                timeout_ms = max(1, min(timeout_ms, int(remaining * 1_000)))
            records = await consumer.getmany(timeout_ms=timeout_ms)
            for messages in records.values():
                for message in messages:
                    payload = decode_observed_payload(message.value)
                    if payload is None or not matches_job_id(payload, args.job_id):
                        continue
                    print(format_message(message, payload, group_id))
                    matched = True
                    if args.once:
                        return 0
    finally:
        await consumer.stop()


def main() -> int:
    try:
        return asyncio.run(watch(build_parser().parse_args()))
    except KeyboardInterrupt:
        return 130
    except (OSError, ValueError) as error:
        print(f"watch error: {error}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
