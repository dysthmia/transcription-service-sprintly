# Runbook: outbox publishing stuck

**Signal:** warning `Transcription outbox retries high` or delayed downstream events.

1. Inspect:
   ```logql
   {service="transcription-service"} | json | event="outbox.retry.scheduled"
   {service="transcription-service"} | json | event="external_request.failed" | provider="kafka"
   ```
2. Confirm the outbox publisher component is running and Kafka broker/topic/ACL
   health is normal.
3. Check producer authentication and network connectivity using the secret
   manager and broker tooling; never log SASL credentials.
4. Preserve the outbox table. It is the durable handoff and supports at-least-once
   publishing; manually deleting or marking rows published can lose events.
5. After Kafka recovery, observe `outbox.publish.completed` and decreasing retry
   volume before closing the incident.
