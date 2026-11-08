# Runbook: DeepSeek or Groq unavailable

**Signal:** warning external dependency errors, elevated retries, or stage latency.

1. Filter the failing provider:
   ```logql
   {service="transcription-service"} | json | event="external_request.failed" | provider=~"deepseek|groq"
   ```
2. Review `operation`, `status_code`, `error_code`, `retryable`, and latency;
   do not log/request prompts, transcripts, headers, or response bodies.
3. Check provider status, quota, account state, DNS/egress, and secret validity
   outside Grafana. Never paste API keys into an incident channel.
4. Allow retryable jobs to follow their scheduled backoff. Avoid bulk manual
   retries while the provider is unhealthy.
5. Escalate to the provider and application owner if the outage exceeds the
   business tolerance or terminal failures appear.
