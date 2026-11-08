# Production observability operations

This document describes the Docker Compose observability stack. It applies to a
single Linux host and assumes application logs use the structured JSON Lines
format. It does not grant access to Kafka, S3, or application data.

## Architecture and data boundaries

```text
transcript stdout (JSON Lines)
  -> Docker local logging-driver buffer
  -> Grafana Alloy via read-only Docker API
  -> Loki single-binary TSDB on filesystem
  -> Grafana (query, dashboards, alert evaluation)
```

The Docker `local` driver on `transcript` is a bounded transport buffer
(`max-size=20m`, `max-file=5`). It lets Alloy read recent container output, but
old segments are rotated and are not a log archive. Loki is the persistent log
store and has the configured 30-day retention. Neither system replaces a backup.

`DATA_ROOT` is a host path, not a path inside a container:

| Host directory | Contents | Recovery importance |
|---|---|---|
| `DATA_ROOT/pgdata` | PostgreSQL cluster: jobs, idempotency records, outbox, migration state | critical application data |
| `DATA_ROOT/observability/loki` | Loki TSDB index/chunks, WAL, compactor working data, delete markers | persistent logs and their index |
| `DATA_ROOT/observability/alloy` | Alloy durable positions/WAL and component state | helps resume Docker-log shipping without a needless replay/gap |
| `DATA_ROOT/observability/grafana` | Grafana SQLite database, users, sessions, alert state and UI-managed objects | Grafana state; provisioned datasource/dashboard/rules are reconstructed from Git |

Bind mounts keep data outside a container writable layer. They survive normal
container removal, but **a bind mount is not a backup**: host disk loss,
accidental deletion, corruption, ransomware, and a bad filesystem snapshot can
destroy it just as completely.

## Production preparation

Keep the production root outside the repository. In the deployment environment
or a root-owned environment file, set:

```bash
DATA_ROOT=/var/lib/transcription-service
APP_ENV=production
LOG_FORMAT=json
GRAFANA_ADMIN_USER=admin
GRAFANA_ADMIN_PASSWORD='read-from-a-secret-manager'
```

Create directories before the first start. The Docker images can use different
UIDs between versions, so determine the effective users from the exact pinned
images; do not copy ownership values blindly from another host.

```bash
sudo install -d -m 0750 -o root -g root /var/lib/transcription-service
sudo install -d -m 0750 -o root -g root \
  /var/lib/transcription-service/pgdata \
  /var/lib/transcription-service/observability/{loki,alloy,grafana}

docker image inspect --format '{{.Config.User}}' \
  postgres:16-alpine grafana/loki:3.7.8 grafana/alloy:v1.21.3 grafana/grafana:13.3.0
```

Assign each leaf directory to the UID:GID used by its service, retaining `0750`
or stricter permissions. For example, the current Loki image normally uses UID
`10001` and Grafana normally uses UID `472`; verify this on the target, then:

```bash
sudo chown -R 10001:10001 /var/lib/transcription-service/observability/loki
sudo chown -R 472:472 /var/lib/transcription-service/observability/grafana
# Use the inspected effective UID:GID for PostgreSQL and Alloy as well.
```

Only the host administrators and Docker daemon should be able to traverse this
tree. Do not make it world-readable: Loki records and PostgreSQL data can contain
operationally sensitive identifiers even though application log processors redact
secrets and content.

## Starting and stopping

Run from the repository checkout with the production environment loaded.

```bash
# Application only; PostgreSQL is started through its dependency.
make app-up

# Loki, Alloy, and Grafana only.
make observability-up

# Worker plus observability stack.
make app-with-observability-up

# Non-destructive stops.
make app-down
make observability-down
```

`docker compose stop` and the Make targets above stop containers but retain bind
mount data. `docker compose down` removes containers and networks but also keeps
these bind mounts unless destructive volume/path options are explicitly supplied.
A recreated container attaches the same data directories. If `DATA_ROOT` is lost,
PostgreSQL, Loki, Alloy state, and Grafana state are lost; rebuild from backups
and Git provisioning. A full filesystem can make Loki reject ingestion, prevent
PostgreSQL writes, and make Grafana unreliable.

## Health and routine checks

```bash
make ps
make observability-status
docker compose --profile runtime --profile observability ps

# Grafana is intentionally loopback-only on the host.
curl -fsS http://127.0.0.1:${GRAFANA_PORT:-3000}/api/health

# Internal readiness checks; Loki and Alloy have no host ports.
docker compose --profile observability exec grafana \
  wget -qO- http://loki:3100/ready
docker compose --profile observability exec grafana \
  wget -qO- http://alloy:12345/-/ready

# Configuration and shipping diagnostics.
docker compose --profile observability exec loki \
  /usr/bin/loki -verify-config=true -config.file=/etc/loki/config.yml
docker compose --profile observability exec alloy \
  /bin/alloy validate /etc/alloy/config.alloy
docker compose --profile runtime logs --tail=100 transcript
docker compose --profile observability logs --tail=100 loki alloy grafana
```

The worker is a background process and deliberately has no HTTP endpoint. Its
health signal is a running container plus expected structured lifecycle logs;
Kafka/S3/AI connectivity is observed from `external_request.*` events.

Check space before Loki reaches its own filesystem safety threshold:

```bash
df -h "$DATA_ROOT"
du -sh "$DATA_ROOT"/pgdata "$DATA_ROOT"/observability/*
docker system df
```

Alert on host filesystem usage separately. Start investigation before 80%; treat
90% as urgent. Do not delete files inside an active Loki or PostgreSQL directory
to free space.

## Querying structured logs

Only `service`, `environment`, `level`, `component`, and `container` are Loki
labels. IDs and error details stay JSON fields to avoid high-cardinality indexes.
Use Grafana Explore or the provisioned dashboard's log panel:

```logql
# A job or correlation id; neither is a label.
{service="transcription-service"} | json | job_id="JOB_ID"
{service="transcription-service"} | json | correlation_id="CORRELATION_ID"

# Terminal failures and their machine-readable reason or stage.
{service="transcription-service"} | json | event="job.failed" | error_code="PROVIDER_TIMEOUT"
{service="transcription-service"} | json | event="job.failed" | stage="summarize"

# A temporarily failing dependency, without source URLs or request bodies.
{service="transcription-service"} | json | event="external_request.failed" | provider="deepseek"
{service="transcription-service"} | json | event="external_request.failed" | provider="groq"
{service="transcription-service"} | json | event="external_request.failed" | provider="s3"
```

Use a bounded time range. Never add `job_id`, `correlation_id`, `material_id`,
`user_id`, `event_id`, or free-form error text as Loki labels.

## Retention and capacity

Loki runs in single-binary TSDB mode on filesystem storage. Its configuration
sets `limits_config.retention_period: 720h` (30 days) and enables the compactor,
which applies retention periodically. The configured ingestion/line-size and
old-sample limits protect the instance from oversized or stale traffic.

To change retention safely:

1. Estimate required disk including compaction headroom and backup capacity.
2. Change only `limits_config.retention_period` in
   `observability/loki/config.yml` to a duration supported by Loki, for example
   `336h` for 14 days.
3. Validate with `docker compose --profile observability config` and Loki's
   `-verify-config` command.
4. Restart Loki during a maintenance window: `docker compose --profile
   observability up -d --force-recreate loki`.
5. Confirm `/ready`, compactor logs, available disk, and expected query range.

Retention is asynchronous, not an immediate secure erase. Never edit chunks or
indexes manually to force deletion.

## Backup and recovery

For a filesystem Loki backup, prefer a storage/filesystem snapshot that is
application-consistent and includes the entire
`DATA_ROOT/observability/loki` tree. If the platform cannot guarantee that,
perform a coordinated stop before copying files:

```bash
docker compose --profile observability stop loki
# Create a filesystem-level archive/snapshot of $DATA_ROOT/observability/loki
# using the host backup system; retain ownership and permissions.
docker compose --profile observability start loki
```

Stopping Loki temporarily prevents new ingestion; Docker `local` buffer may only
retain a bounded recent window, so keep the operation short. Back up Grafana's
directory separately and keep the repository's provisioning files with the
backup. For PostgreSQL, use a PostgreSQL-aware backup procedure; copying its live
directory is not a substitute.

To restore Loki:

1. Stop Loki and keep a forensic copy of the failed directory.
2. Restore the complete Loki tree into `DATA_ROOT/observability/loki` with the
   correct owner/mode.
3. Start Loki, wait for `/ready`, and query a known historical time range.

To restore Grafana:

1. Stop Grafana.
2. Restore `DATA_ROOT/observability/grafana` with the Grafana service owner.
3. Start Grafana and confirm `/api/health`, the `Loki` datasource, dashboard,
   and rules. Git provisioning recreates file-provisioned dashboard, datasource,
   and rules even if UI state was lost; it cannot recreate users, sessions, or
   UI-created objects without the Grafana backup.

## Network and access

Grafana binds to `127.0.0.1` by default. Terminate TLS in a hardened reverse
proxy on the same host, proxy only to `http://127.0.0.1:3000`, and restrict the
public virtual host with your organisation's authentication policy. The proxy is
the place for TLS certificates, HSTS, request-size limits, access logging, and
an allowlist/VPN if Grafana is not intended for every employee.

Do not publish Loki or Alloy. Loki has no built-in tenant authentication in this
single-tenant configuration (`auth_enabled: false`), and an exposed ingestion API
would let an attacker poison logs or exhaust disk. Alloy exposes operational
state and receives a Docker socket mount. The `:ro` bind-mount flag does **not**
make Docker API operations read-only: a process that can connect to the socket
can still have Docker-daemon-level control. Treat the Alloy container and socket
as highly privileged, keep them off untrusted networks, and consider a narrowly
allowlisted Docker socket proxy before using this topology across trust boundaries.

`GF_SECURITY_ADMIN_PASSWORD` creates the initial administrator account; changing
the environment value later does not reset an existing account. For a known
current password, use Grafana's **Change Password** page. If the password is
lost, run Grafana's supported CLI reset command from a protected terminal against
the existing Grafana data directory, supplying the new secret without recording
it in shell history:

```bash
docker compose --profile observability exec grafana \
  grafana cli --homepath /usr/share/grafana admin reset-admin-password '<new-secret>'
```

Use a secret-manager injection mechanism instead of a literal command-line
argument in automated deployments. Verify login with the new password, update
the protected bootstrap secret for disaster recovery, then revoke the old secret.
Do not put either password in shell history, Compose files, dashboard JSON, or
an alert template.

The repository only supplies a non-secret contact-point template. Copy
`observability/grafana/contact-point.template.yml` to a protected deployment-only
provisioning mount, inject `ALERT_WEBHOOK_URL` from a secret manager, and mount
it read-only at `/etc/grafana/provisioning/alerting/contact-points.yml`. For
Telegram or email, provision the corresponding Grafana receiver type with its
token/SMTP secret from the same secret manager. Never commit the rendered file.

## Future S3 migration for Loki

Do not point the existing filesystem TSDB at S3 in place. Plan a migration:

1. Select an S3-compatible object store, bucket, encryption, lifecycle, IAM
   policy, and credentials managed outside Git.
2. Build a staging Loki configuration for the pinned Loki version using its
   supported TSDB object-store and compactor settings.
3. Test ingestion, retention deletion, query performance, restart/recovery, and
   rollback with production-like volumes.
4. Decide whether historical filesystem data is retained separately or migrated
   with a documented Loki-supported procedure; do not copy chunks ad hoc.
5. Schedule a controlled cutover, preserve a filesystem backup, and monitor
   queries, compaction, object-store errors, and cost.

## Security review record

Review performed against the tracked Compose/config/provisioning files and the
logging processor in this checkout:

| Check | Result |
|---|---|
| Secrets in Compose, Loki/Alloy config, dashboard, alert rules | no literal secret found; only required environment references/template placeholders |
| Grafana anonymous access | disabled with `GF_AUTH_ANONYMOUS_ENABLED=false` |
| Public Loki/Alloy ports | none declared |
| Docker socket | mounted only to Alloy; `:ro` does not restrict Docker API methods, so this remains daemon-level access |
| Sensitive logging | recursive redaction covers credential-like fields, bodies/payloads/prompts/transcripts/summaries/responses, and URL query strings |
| Application content logging | event instrumentation intentionally excludes material content, S3 keys, URLs, headers, bodies, prompts, transcripts, summaries, and API responses |
| Runtime data in Git | `out/` is ignored; production `DATA_ROOT` is outside the checkout |
| Example environment | contains placeholders only; no live credentials |

This is a source/configuration review, not proof that every future dependency or
operator command will be safe. Keep secret scanning in CI, protect the Docker
socket and `DATA_ROOT`, and review reverse-proxy authentication before exposing
Grafana.

## Incident runbooks

- [Job failures spike](runbooks/job-failures-spike.md)
- [Kafka DLQ messages](runbooks/kafka-dlq-messages.md)
- [DeepSeek or Groq unavailable](runbooks/ai-provider-unavailable.md)
- [S3 unavailable](runbooks/s3-unavailable.md)
- [Outbox publishing stuck](runbooks/outbox-publishing-stuck.md)
- [Loki disk usage high](runbooks/loki-disk-usage-high.md)
- [Alloy stopped shipping logs](runbooks/alloy-stopped-shipping.md)
- [Grafana cannot query Loki](runbooks/grafana-cannot-query-loki.md)
- [Background task failed](runbooks/background-task-failed.md)
