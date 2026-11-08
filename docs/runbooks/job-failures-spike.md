# Runbook: job failures spike

**Signal:** critical `Transcription job failure spike` alert.

1. Confirm the alert window and inspect the overview dashboard.
2. Group failures without exposing content:
   ```logql
   {service="transcription-service"} | json | event="job.failed"
   ```
   Filter `error_code`, `stage`, `job_id`, or `correlation_id` at query time.
3. Compare `external_request.failed` by `provider` with `job.stage.completed`
   duration. A provider outage should be handled by the provider runbook.
4. Check worker lifecycle and `job.retry.scheduled`; do not replay jobs by
   editing PostgreSQL rows manually.
5. If terminal content errors dominate, validate upstream material generation.
   If system errors dominate, mitigate the dependency, then allow existing retry
   semantics to proceed.

Escalate if failures persist for two alert periods or completed jobs stop.
