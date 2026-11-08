# Runbook: Alloy stopped shipping logs

**Signal:** recent application logs absent from Loki while `transcript` still emits stdout.

1. Check container status and logs:
   ```bash
   make observability-status
   docker compose --profile observability logs --tail=200 alloy
   ```
2. Validate the configuration and Docker socket mount:
   ```bash
   docker compose --profile observability exec alloy /bin/alloy validate /etc/alloy/config.alloy
   docker compose --profile observability exec grafana wget -qO- http://loki:3100/ready
   ```
3. Confirm Alloy can see the intended Compose services and that the `transcript`
   service still uses Docker's `local` logging driver. PostgreSQL is intentionally
   excluded.
4. Check `DATA_ROOT/observability/alloy` free space and host Docker daemon health.
5. Restart Alloy only after collecting diagnostics. The local Docker buffer is
   bounded, so a long outage can create an unrecoverable log gap.

Do not broaden the container allowlist just to troubleshoot; inspect specific
container logs directly instead.
