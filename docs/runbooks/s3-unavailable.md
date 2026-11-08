# Runbook: S3 unavailable

**Signal:** warning external dependency errors where `provider="s3"`.

1. Query:
   ```logql
   {service="transcription-service"} | json | event="external_request.failed" | provider="s3"
   ```
2. Determine whether failures occur in `download` or `upload` from job stage;
   never search for bucket credentials, object keys, or presigned URLs in logs.
3. Check S3 provider status, DNS/TLS, endpoint reachability, IAM policy, bucket
   existence, and capacity using approved infrastructure tools.
4. Confirm timeouts/5xx are retryable and allow scheduled retries to run.
5. If uploads are blocked, protect outbox/Kafka capacity and escalate; do not
   mark jobs completed manually because a summary object may be absent.
