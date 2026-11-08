# Runbook: Loki disk usage high

**Signal:** host filesystem warning/critical, Loki write failures, or compactor errors.

1. Measure scope:
   ```bash
   df -h "$DATA_ROOT"
   du -sh "$DATA_ROOT"/observability/loki "$DATA_ROOT"/pgdata
   docker compose --profile observability logs --tail=200 loki
   ```
2. Stop nonessential disk consumers or extend the filesystem. Do not delete Loki
   chunks, WAL, indexes, or compactor files while Loki is running.
3. Confirm 30-day retention and compactor health. Retention cleanup is delayed
   and cannot be relied on for an immediate emergency reclaim.
4. If necessary, take a backup, then reduce retention through the documented
   configuration change and restart Loki in a maintenance window.
5. After space recovery, verify `/ready`, Alloy write logs, and a recent query.

Escalate immediately at 90% utilisation; a full filesystem can also damage
PostgreSQL availability.
