# Production-развёртывание Transcript

## 1. Общие сведения

Transcript — фоновый сервис учебной платформы Sprintly. Он читает из Kafka событие
`material_uploaded`, сохраняет задачу в PostgreSQL, получает материал из S3 либо
YouTube, создаёт подробный Markdown-конспект на русском языке, сохраняет
`summary.md` в S3 и публикует `summary_generated` или `transcription_failed` через
Kafka transactional outbox.

```text
Kafka -> consumer -> PostgreSQL (jobs, processed_events, outbox)
                         |                  |
                         v                  v
                  async workers -> S3 / DeepSeek / Groq -> S3 summary
                                                   |
                                                outbox publisher -> Kafka
```

Компоненты на одном сервере: контейнеры `postgres`, `migrate` (одноразовый),
`transcript`; необязательно — `loki`, `alloy`, `grafana`. Kafka, S3, DeepSeek и
Groq — внешние сервисы. HTTP API у приложения нет.

Используются Python 3.12, Docker Compose, PostgreSQL 16, Alembic, SQLAlchemy
async, aiokafka, aioboto3, FFmpeg, DeepSeek и Groq. Контейнер приложения работает
не от root, имеет ограничение CPU/RAM и `no-new-privileges`.

## 2. Требования к серверу

- Linux x86_64 или arm64 с Docker Engine и Docker Compose v2.
- Минимально: 2 vCPU, 4 GB RAM, 30 GB свободного SSD. Рекомендуется: 4 vCPU,
  8 GB RAM и диск с запасом для PostgreSQL, временных медиафайлов и Loki.
- Доступ исходящего трафика к Kafka, S3, DeepSeek и Groq; DNS и корректное UTC
  время обязательны.
- Входящие порты приложению не нужны. PostgreSQL и Grafana по умолчанию слушают
  только loopback; Loki и Alloy не имеют опубликованных портов.
- Для публичного доступа к Grafana нужен отдельный reverse proxy с TLS. Сам
  Transcript через reverse proxy не публикуется.

## 3. Подготовка сервера

Ниже пример для Ubuntu/Debian. Устанавливайте Docker по официальной инструкции
вашей ОС, затем добавьте отдельного оператора в группу `docker` только если это
согласовано с политикой доступа: членство в этой группе фактически равно root.

```bash
sudo apt-get update
sudo apt-get install -y ca-certificates git openssl
docker version
docker compose version

sudo install -d -m 0750 -o root -g root /opt/transcription-service
sudo install -d -m 0750 -o root -g root /var/lib/transcription-service
sudo install -d -m 0750 -o root -g root /var/backups/transcription-service
```

Перед стартом создайте каталоги данных. UID образов следует проверить на целевом
хосте: они могут меняться при обновлении образа.

```bash
sudo install -d -m 0750 /var/lib/transcription-service/{pgdata,observability/loki,observability/alloy,observability/grafana}
docker image inspect --format '{{.Config.User}}' postgres:16-alpine grafana/loki:3.7.8 grafana/grafana:13.3.0
```

Назначьте владельца каждого дочернего каталога соответствующему UID:GID образа.
Не делайте `DATA_ROOT` world-readable: в PostgreSQL и логах есть операционные
идентификаторы.

## 4. Получение проекта

```bash
sudo -u deploy git clone <REPOSITORY_URL> /opt/transcription-service/app
cd /opt/transcription-service/app
git checkout <TAG_OR_COMMIT>
cp .env.example .env
chmod 0600 .env
```

`DATA_ROOT` в `.env` для production должен быть `/var/lib/transcription-service`.
Файл `.env` не коммитится и не включается в Docker build context.

## 5. Переменные окружения

Ниже перечислены реальные настройки. Значения секретов приведены только как
заглушки.

| Группа | Переменные | Требование production |
|---|---|---|
| Application | `APP_ENV=production`, `LOG_FORMAT=json`, `LOG_LEVEL`, `SERVICE_NAME`, `SERVICE_VERSION` | обязательны первые две; версия должна быть tag/commit релиза |
| Worker | `WORKER_CONCURRENCY`, `JOB_MAX_ATTEMPTS`, `JOB_LEASE_SECONDS`, `WORKER_POLL_INTERVAL_SECONDS`, `SHUTDOWN_GRACE_SECONDS`, `JOB_TARGET_SECONDS` | положительные числа; concurrency подбирается по CPU и лимитам API |
| PostgreSQL | `POSTGRES_HOST`, `POSTGRES_PORT`, `POSTGRES_DB`, `POSTGRES_USER`, `POSTGRES_PASSWORD`, `POSTGRES_HOST_PORT` | пароль — случайный секрет; host для Compose: `postgres` |
| Kafka | `KAFKA_BOOTSTRAP_SERVERS`, `KAFKA_CONSUMER_GROUP`, `KAFKA_INPUT_TOPIC`, `KAFKA_OUTPUT_TOPIC`, `KAFKA_INPUT_DLQ_TOPIC`, `KAFKA_SECURITY_PROTOCOL`, `KAFKA_SASL_MECHANISM`, `KAFKA_SASL_USERNAME`, `KAFKA_SASL_PASSWORD` | только `SSL` или `SASL_SSL`; для `SASL_SSL` все SASL-поля обязательны |
| S3 | `S3_ENDPOINT_URL`, `S3_REGION`, `S3_ACCESS_KEY_ID`, `S3_SECRET_ACCESS_KEY`, `S3_OUTPUT_BUCKET`, `S3_ALLOWED_SOURCE_BUCKETS`, `S3_ADDRESSING_STYLE`, `S3_ALLOW_INSECURE_ENDPOINT` | HTTPS; allowlist source bucket обязателен; HTTP допустим только для отдельно утверждённого internal endpoint |
| AI | `DEEPSEEK_API_KEY`, `DEEPSEEK_BASE_URL`, `DEEPSEEK_MODEL`, `GROQ_API_KEY`, `GROQ_BASE_URL`, `STT_MODEL` | ключи — секреты, URL только HTTPS |
| Limits | `MAX_DOCUMENT_FILE_SIZE_MB`, `MAX_SOURCE_FILE_SIZE_MB`, `MAX_MEDIA_FILE_SIZE_MB`, `MAX_PDF_PAGES`, `MAX_MEDIA_DURATION_SECONDS`, `STT_CHUNK_SECONDS`, `API_REQUEST_TIMEOUT_SECONDS`, `MEDIA_COMMAND_TIMEOUT_SECONDS`, `YOUTUBE_DOWNLOAD_TIMEOUT_SECONDS`, `SUMMARY_CHUNK_SIZE` | все положительны; общий лимит source не меньше document/media limit |
| Storage/observability | `DATA_ROOT`, `GRAFANA_ADMIN_USER`, `GRAFANA_ADMIN_PASSWORD`, `GRAFANA_PORT` | `DATA_ROOT` вне checkout; пароль Grafana обязателен и уникален |

Пример минимального production-файла:

```dotenv
DATA_ROOT=/var/lib/transcription-service
APP_ENV=production
LOG_FORMAT=json
LOG_LEVEL=INFO
SERVICE_VERSION=2026.11.17
WORKER_CONCURRENCY=3
JOB_MAX_ATTEMPTS=3
JOB_LEASE_SECONDS=300

POSTGRES_HOST=postgres
POSTGRES_PORT=5432
POSTGRES_DB=transcript
POSTGRES_USER=transcript
POSTGRES_PASSWORD=<GENERATE_A_RANDOM_SECRET>

KAFKA_BOOTSTRAP_SERVERS=kafka-1.example.net:9093,kafka-2.example.net:9093
KAFKA_CONSUMER_GROUP=transcript-service
KAFKA_INPUT_TOPIC=material.events
KAFKA_OUTPUT_TOPIC=transcript.events
KAFKA_INPUT_DLQ_TOPIC=material.events.dlq
KAFKA_SECURITY_PROTOCOL=SASL_SSL
KAFKA_SASL_MECHANISM=SCRAM-SHA-512
KAFKA_SASL_USERNAME=<KAFKA_USER>
KAFKA_SASL_PASSWORD=<KAFKA_PASSWORD>

S3_ENDPOINT_URL=https://s3.example.net
S3_REGION=ru-1
S3_ACCESS_KEY_ID=<S3_ACCESS_KEY>
S3_SECRET_ACCESS_KEY=<S3_SECRET>
S3_OUTPUT_BUCKET=transcript-summaries
S3_ALLOWED_SOURCE_BUCKETS=learning-materials
S3_ADDRESSING_STYLE=auto
S3_ALLOW_INSECURE_ENDPOINT=false

DEEPSEEK_API_KEY=<DEEPSEEK_KEY>
DEEPSEEK_BASE_URL=https://api.deepseek.com
DEEPSEEK_MODEL=deepseek-flash
GROQ_API_KEY=<GROQ_KEY>
GROQ_BASE_URL=https://api.groq.com/openai/v1
STT_MODEL=whisper-large-v3-turbo

MAX_DOCUMENT_FILE_SIZE_MB=50
MAX_SOURCE_FILE_SIZE_MB=200
MAX_MEDIA_FILE_SIZE_MB=200
MAX_PDF_PAGES=500
MAX_MEDIA_DURATION_SECONDS=3600
STT_CHUNK_SECONDS=600
API_REQUEST_TIMEOUT_SECONDS=120
MEDIA_COMMAND_TIMEOUT_SECONDS=300
YOUTUBE_DOWNLOAD_TIMEOUT_SECONDS=300

GRAFANA_ADMIN_USER=admin
GRAFANA_ADMIN_PASSWORD=<GENERATE_A_SECOND_RANDOM_SECRET>
GRAFANA_PORT=3000
```

## 6. Инфраструктура и права

PostgreSQL принадлежит только Transcript. Пользователь S3 должен иметь минимально
необходимые права: `GetObject` только на bucket из `S3_ALLOWED_SOURCE_BUCKETS`, и
`PutObject` только на префикс output bucket. Kafka principal должен читать input и
DLQ topic, писать output/DLQ и коммитить consumer group offsets.

DeepSeek/Groq получают только извлечённый контент. Не передавайте ключи через
командную строку, историю shell, Grafana или Git. Храните `.env` в deployment
secret store либо с правами `0600`.

## 7. Production-сборка

Проверить интерполяцию без запуска:

```bash
cd /opt/transcription-service/app
docker compose --profile runtime --profile observability config --quiet
docker compose --profile runtime build transcript
```

`Dockerfile` устанавливает FFmpeg и libmagic, синхронизирует зависимости из
`uv.lock`, копирует исходники и переключается на системного пользователя
`transcript`. Не используйте `--no-cache` для каждого деплоя без необходимости.

## 8. База данных и миграции

Перед первой миграцией сделайте backup существующей БД, если она уже содержит
данные. Миграции выполняются отдельным одноразовым контейнером:

```bash
make postgres-up
make migrate
```

Текущая миграция `0002_add_job_lease_token` добавляет nullable token владения
lease. Она не удаляет пользовательские данные. Не запускайте `alembic downgrade`
на production без проверенного плана восстановления.

Проверка версии:

```bash
docker compose exec postgres psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" \
  -c 'SELECT version_num FROM alembic_version;'
```

## 9. Первый запуск

1. Создайте и защитите `.env`.
2. Проверьте network ACL, Kafka topics, S3 policy и AI API keys.
3. Соберите image и поднимите PostgreSQL: `make app-build && make postgres-up`.
4. Примените миграции: `make migrate`.
5. Запустите worker: `make app-up`.
6. Проверьте: `make ps`, `make app-logs`.
7. Отправьте согласованное тестовое `material_uploaded` с новым `event_id` и
   `job_id`; убедитесь в объекте S3 и одном исходящем Kafka event.

У background worker нет HTTP health endpoint. Его сигнал готовности — живой
контейнер, `service.started`, `component.started` в JSON-логах и успешный test
event. Healthchecks внешних Kafka/S3/API проверяются по структурным событиям
`external_request.*`.

## 10. Reverse proxy, домен и HTTPS

Transcript не принимает HTTP-трафик, поэтому reverse proxy для него неприменим.
Не публикуйте PostgreSQL, Loki или Alloy. Если нужно открыть Grafana, оставьте её
на `127.0.0.1:${GRAFANA_PORT}` и проксируйте только её через Nginx/Caddy с TLS,
аутентификацией и allowlist администраторов.

## 11. Повседневное управление

```bash
make app-up                 # запуск worker
make app-down               # аккуратная остановка worker
make app-logs               # логи worker
make postgres-logs          # логи PostgreSQL
make ps                     # состояние всех контейнеров
make observability-up       # Loki, Alloy, Grafana
make observability-status
make observability-logs
```

Не используйте `make postgres-cleanup` на production: он намеренно удаляет все
локальные PostgreSQL данные.

## 12. Обновление

```bash
cd /opt/transcription-service/app
git fetch --tags
git checkout <NEW_TAG>
docker compose --profile runtime build transcript
# Сначала обязательно backup, затем:
make migrate
make app-up
make app-logs
```

`make migrate` может вызвать краткий простой при DDL. Worker запускайте только
после успешной миграции. После выпуска проверьте version в логах, миграцию,
Kafka lag, outbox retry и тестовое событие.

## 13. Rollback

1. Остановите worker: `make app-down`.
2. Верните Git checkout и соберите предыдущий image.
3. Откатывайте только код, если схема обратно совместима.
4. При несовместимой миграции не выполняйте автоматический downgrade: восстановите
   PostgreSQL из проверенного backup или используйте заранее подготовленную
   forward-fix migration.
5. Запустите worker и проверьте outbox/lag.

Откат без backup при несовместимой схеме способен потерять данные.

## 14. Backup и Restore

Автоматический scheduler backup в репозитории не реализован. Настройте его в
инфраструктуре минимум ежедневно; RPO/RTO должны быть утверждены владельцем
сервиса. Обязательно резервируйте PostgreSQL и `.env`/secret references отдельно.

Пример ручного backup:

```bash
backup=/var/backups/transcription-service/transcript-$(date -u +%F-%H%M%S).dump
docker compose exec -T postgres sh -c 'pg_dump -U "$POSTGRES_USER" -d "$POSTGRES_DB" -Fc' > "$backup"
chmod 0600 "$backup"
pg_restore --list "$backup" | head
```

Храните шифрованные копии вне VPS и регулярно проверяйте restore на отдельной БД.
Restore разрушителен: остановите worker, восстановите в новую/проверенную БД и
переключайте production только после валидации.

## 15. Логирование и мониторинг

В production используется JSON Lines stdout. Alloy собирает только allowlist
контейнеров и направляет логи в Loki; Grafana содержит dashboard и alert rules.
Логи не содержат prompt, summary, transcript, ключи или URL query strings.

```bash
make observability-up
curl -fsS http://127.0.0.1:${GRAFANA_PORT:-3000}/api/health
docker compose --profile observability logs --tail=100 loki alloy grafana
```

Контролируйте job failures/retries, outbox retries, DLQ, latency стадий, свободное
место и Kafka lag. Runbooks находятся в `docs/runbooks/` и
`observability/RUNBOOK.md`.

## 16. Безопасность production

- [ ] `.env` имеет права `0600`, секреты не попали в Git или image.
- [ ] Kafka использует `SSL`/`SASL_SSL`; API и S3 — HTTPS.
- [ ] S3 IAM ограничен allowlist bucket и output prefix.
- [ ] PostgreSQL, Loki и Alloy не опубликованы наружу.
- [ ] Grafana доступна только администраторам через TLS proxy.
- [ ] Docker images и Python lockfile регулярно проверяются на CVE.
- [ ] Backup зашифрован, вынесен с VPS и протестирован restore.
- [ ] Не выполняются ручные изменения `jobs`/`outbox` в PostgreSQL.

## 17. Troubleshooting

| Проблема | Возможная причина | Диагностика | Решение |
|---|---|---|---|
| Worker не стартует | отсутствует/некорректен `.env` | `make app-logs` | исправить обязательную настройку, особенно TLS/SASL/S3 allowlist |
| Job в `retry_wait` | временный S3/Kafka/AI сбой | `external_request.failed`, `job.retry.scheduled` | восстановить dependency, не менять job вручную |
| Сообщение в DLQ | неверная схема или запрещённый bucket | `kafka.message.sent_to_dlq` | исправить producer/allowlist, replay только утверждённым процессом |
| Нет исходящего события | Kafka недоступна | `outbox.retry.scheduled` | восстановить Kafka; outbox не удалять |
| `file_too_large` | превышен source/document/media limit | `job.failed` и error code | изменить лимит только после оценки диска/API или отклонить материал |
| Lease lost | worker завис/перезапущен или БД недоступна | `job.lease.*` | проверить DB и длительность stage; новый владелец безопасно продолжит job |
| Нет логов в Grafana | Alloy/Loki или место на диске | `make observability-status`, `df -h "$DATA_ROOT"` | следовать runbook Alloy/Loki; не удалять WAL/chunks вручную |

## 18. Production checklist

- [ ] Проверены `docker compose config --quiet` и production image.
- [ ] Секреты безопасно настроены; `.env` не в Git.
- [ ] Kafka TLS/SASL, topics и ACL проверены.
- [ ] S3 HTTPS, IAM и `S3_ALLOWED_SOURCE_BUCKETS` проверены.
- [ ] PostgreSQL persistent storage и backup настроены.
- [ ] Миграции применены, версия Alembic подтверждена.
- [ ] Worker и structured logging работают.
- [ ] Пройдены unit/integration тесты и dependency audit.
- [ ] Выполнен тестовый end-to-end event.
- [ ] Проверены Grafana alerts, место на диске и процедуры rollback/restore.
