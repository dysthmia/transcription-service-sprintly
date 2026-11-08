# Transcription observability runbook

Replace `REPLACE_WITH_YOUR_ORG` in the provisioned alert annotations with the
canonical repository URL before production rollout.

## Alert thresholds

| Alert | Threshold | Pending period |
|---|---:|---:|
| Background task failure | >0 in 10m | 5m |
| Job failure spike | >4 in 10m | 10m |
| Job retry rate | >10 in 15m | 10m |
| Input DLQ | >0 in 10m | 5m |
| External dependency failures | >5 in 10m | 10m |
| Completed job duration | >180000 ms in 15m | 10m |
| Outbox retries | >5 in 15m | 10m |

The duration target mirrors the default `JOB_TARGET_SECONDS=180`. Update both
the application setting and `transcription-job-duration-high` together when the
operating target changes.

## Background task failed

Find the matching `background_task.failed` log and inspect `exception.type` and
the redacted traceback. Check the named component lifecycle events before a
controlled restart.

## Job failure spike and retries

Use the dashboard's `error_code`, `stage`, `job_id`, and `correlation_id`
filters. IDs are parsed from JSON at query time and are never Loki labels.

## DLQ

Check producer schema/version compatibility. The application deliberately does
not log the original Kafka payload; use the DLQ topic under approved access
controls if inspection is required.

## External dependency errors

Filter `external_request.failed` by `provider`, `operation`, and `error_code`.
Check provider status, credentials outside logs, and network connectivity.

## Job duration and outbox retries

Compare stage p95 with job p95. For outbox retries, correlate Kafka provider
errors and broker health; do not replay or delete events manually.

## Loki query error

Check Grafana datasource health, Loki `/ready`, Alloy write errors, free disk
space, and Loki compactor/WAL state. Loki filesystem storage stops accepting
writes when the host filesystem is too full.
