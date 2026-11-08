# Runbook: Grafana cannot query Loki

**Signal:** critical `Loki datasource query error`, dashboard query errors, or datasource health failure.

1. Check Grafana and Loki separately:
   ```bash
   curl -fsS http://127.0.0.1:${GRAFANA_PORT:-3000}/api/health
   docker compose --profile observability exec grafana wget -qO- http://loki:3100/ready
   docker compose --profile observability logs --tail=200 grafana loki
   ```
2. Check host disk and Loki config validation. Full disks commonly cause WAL or
   compactor failures.
3. Verify the provisioned datasource still points to `http://loki:3100` with UID
   `loki`; do not expose Loki to fix an internal networking issue.
4. Check Docker network/DNS from the Grafana container and Loki restart history.
5. Restore Loki from a verified backup only after preserving the failed data tree
   for investigation.
