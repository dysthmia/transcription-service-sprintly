# Runbook: background task failed

**Signal:** critical `Transcription background task failed` alert.

1. Find the structured event and its redacted exception:
   ```logql
   {service="transcription-service"} | json | event="background_task.failed"
   ```
2. Record `component`, `exception.type`, and event time. Do not copy a traceback
   containing identifiers into an unprotected system.
3. Correlate with `component.started`, `component.stopped`, `service.stopping`,
   and dependency failures.
4. Determine whether the consumer, processor, or outbox publisher stopped. Check
   Kafka lag and PostgreSQL connectivity with approved operations tooling.
5. Restart only the affected worker/container after identifying the cause; then
   confirm `service.started`, component lifecycle events, and normal job flow.

Escalate immediately if the task is repeatedly failing or no worker is processing
jobs.
