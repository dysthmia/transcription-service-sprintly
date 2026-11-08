# Runbook: Kafka DLQ messages

**Signal:** warning `Transcription input DLQ messages` alert.

1. Query technical metadata only:
   ```logql
   {service="transcription-service"} | json | event="kafka.message.sent_to_dlq"
   ```
2. Identify the event schema/version or `error_code`; application logs never
   contain the original Kafka payload.
3. Check producer deployment and schema compatibility with the owning team.
4. Inspect the DLQ topic only through approved Kafka tooling and access controls.
   Do not copy payloads into tickets, dashboards, or logs.
5. Fix the producer/contract first. Replay requires an approved, idempotency-aware
   process because consumer offsets and job deduplication are separate concerns.
