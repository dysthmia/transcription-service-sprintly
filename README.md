# transcription-service-sprintly

**Transcript** — отдельный Python-сервис обучающей платформы Sprintly. По событию из Kafka получает материал из S3 либо допустимую к обработке ссылку YouTube, извлекает содержание и создаёт **подробный конспект на русском языке в Markdown** для Teacher. Результат сохраняется в S3, а его местоположение отправляется через Kafka.

## 1. Границы ответственности и договорённости

| Transcript делает | Transcript не делает |
|---|---|
| Принимает Kafka-события и ведёт задания | Не хранит пользователей и метаданные материалов Sprintly |
| Определяет тип файлов и извлекает текст/речь | Не управляет UI-статусом Sprintly |
| Создаёт подробный русскоязычный Markdown-конспект | Не создаёт вопросы и категории Teacher |
| Сохраняет `summary.md` и публикует событие | Не обращается к чужим БД и HTTP-сервисам Sprintly/Teacher |
| Обрабатывает сбои, повторы и идемпотентность | Не запускает локальные LLM или локальный Speech-to-Text |

**Зафиксировано:** Python + Kafka + PostgreSQL + S3, SQLAlchemy + Alembic, три таблицы, Transactional Outbox, идемпотентность. Без REST API, Redis, Celery, Kubernetes и маркеров источников (номеров страниц и таймкодов) в конспекте. 2–3 одновременных задания при достаточных ресурсах.

**Инфраструктура:** Python-приложение и выделенный для него PostgreSQL запускаются через Docker Compose на VPS; Kafka и S3 существуют отдельно, доступ к ним задаётся переменными окружения. S3-провайдер **ещё неизвестен**: используем S3-совместимый клиент с конфигурируемыми endpoint, region, TLS, path-style (если понадобится). Не привязываем реализацию к AWS или MinIO.

**Ресурсы:** целевой VPS — 4 vCPU / 8 GB RAM; сервис также должен работать на 2 vCPU / 4 GB RAM при сниженной конкурентности. Предусмотрено CPU-friendly извлечение и ограниченное число параллельных OCR/FFmpeg операций.

**Производительность:** **до 3 минут на материал — цель, не жёсткое обещание**. Часовое видео и PDF на 500 сканированных страниц не обязаны укладываться в 180 секунд. Не сокращаем содержание молча ради дедлайна. Сторожевой таймер Sprintly из старого контракта (30 минут) требуется подтвердить с командой; до согласования считаем его внешним ограничением, а не доказанной характеристикой сервиса.

## 2. Поддерживаемые источники и стратегия извлечения

| Источник | Основной путь | Когда нужен API |
|---|---|---|
| `.txt` | Чтение байтов, проверка кодировки, нормализация | Только конспект DeepSeek |
| `.docx` | `python-docx`: абзацы, таблицы в порядке документа, заголовки | DeepSeek для конспекта; при сложной вёрстке допустима дополнительная обработка |
| `.pdf` (текстовый слой) | `PyMuPDF`: текст и извлечение таблиц, анализ качества извлечения | DeepSeek для конспекта; сложные формулы/графика — выборочно vision |
| `.pdf` (скан / смешанный) | Классификация страниц и **выборочный** OCR/vision без обработки каждой страницы без необходимости | `deepseek-flash` vision для нужных страниц, особенно формул и таблиц |
| `.png`, `.jpeg` / `.jpg` | Vision-извлечение текста, структур и формул | `deepseek-flash` vision |
| `.mp3` | Проверка длительности; при необходимости FFmpeg и чанки → Speech-to-Text | Groq Whisper Turbo + DeepSeek конспект |
| `.mp4` | **Только аудиодорожка**: FFmpeg → Speech-to-Text | Groq Whisper Turbo + DeepSeek конспект |
| YouTube URL | Проверка URL и права на доступ к аудио → извлечение доступного и разрешённого аудио → Speech-to-Text | Groq Whisper Turbo + DeepSeek конспект |

**YouTube:** для YouTube всегда применяется Speech-to-Text; готовые субтитры не используем даже как fallback. **Наличие ссылки не гарантирует доступ к аудио.** Загрузка или выделение аудио из YouTube может ограничиваться правилами сервиса и правами на контент. Работаем только с контентом, для которого доступ разрешён; не обходим DRM, авторизацию и ограничения доступа. Если допустимого способа получить аудио нет — `source_unavailable`, пользователь может загрузить свой MP3/MP4. Никаких неподтверждённых гарантий работы произвольных YouTube URL.

**Определение формата:** для файлов проверяем содержимое (signature/magic bytes) и структуру, а не доверяем одному расширению или MIME. DOCX дополнительно валидируем как OOXML ZIP. Для URL строго разрешённые YouTube-домены, валидный идентификатор видео; защита от SSRF, произвольных редиректов и обращений во внутренние сети.

**Формулы:** по возможности сохраняем как LaTeX (`$...$`, `$$...$$`), не выдаём выдуманные формулы, оставляем обозначения и числовые значения. **Таблицы:** небольшие — Markdown-таблицы; сложные — упорядоченные строки/списки с сохранением связей значений. Конспект должен сохранять важные определения, объяснения, примеры и выводы. PDF с большим числом сканов может обрабатываться дольше; если извлечение неполно или невозможно, фиксируем ошибку, не выдаём якобы полный конспект.

**Обработка речи:** распознавание на языке источника (`ru`/`en`; выбор языка или автоопределение), потом нормализация и **перевод содержания конспекта на русский** в DeepSeek. Технические термины, имена, код и формулы сохраняем корректно. Если аудио не содержит содержательной речи — `empty_content`.

### Лимиты для MVP

| Параметр | Первоначальное значение |
|---|---|
| Максимальный размер одного файла | 50 MB (уточнить после тестов) |
| Максимальное количество страниц PDF | 500 (цель поддержки, не гарантия OCR за 3 мин) |
| Длительность MP3/MP4/YouTube | 60 минут (первоначальный лимит) |
| Конкурентность обработки | 3 на основном VPS, 1 на минимальном; настраивается |
| Целевое время обработки | 180 секунд, **soft target** |
| Повторы задания при временных ошибках | До 3 **полных попыток** включая первоначальную |

Для видео/MP3 размер исходного контейнера и нагрузка на диск могут превышать 50 MB; до реализации нужно выбрать отдельный `MAX_MEDIA_FILE_SIZE_MB`, не смешивая лимит документов и медиаконтента. Сервис не загружает всё большое видео в оперативную память, применяет временные файлы и очистку по `finally`.

## 3. Внешние AI API

**Два ключа:**

1. **DeepSeek API** (`DEEPSEEK_API_KEY`): `deepseek-flash`, текстовая суммаризация и vision для изображений/отдельных страниц. Base URL из настроек, модель также из настроек. API поддерживает JPEG и PNG. Для OCR сложных страниц нужен собственный промпт на извлечение содержания, отличный от промпта на конспект.
2. **Groq Cloud API** (`GROQ_API_KEY`): `whisper-large-v3-turbo`, распознавание русской/английской речи. Передача MP3 или подготовленных аудиочанков. Учитывать лимиты тарифного плана и типы входа, контролировать `429`, `Retry-After`, 5xx и таймауты.

Ни DeepSeek, ни Groq не размещаются локально. Никакие приватные API-ключи не попадают в репозиторий или логи. Стоимость зависит от актуальных тарифов провайдера; **код не должен опираться на фиксированную цену**. На старте достаточно двух аккаунтов и ключей, дополнительных провайдеров не покупаем.

**Не обещаем абсолютной точности** формул и таблиц: до приёмки проверяем реальные материалы (русские и английские презентации, таблицы, математические PDF). Ответ от моделей валидируем на непустой текст; если недостоверна обязательная часть материала, лучше завершить ошибкой, чем сохранить вводящий в заблуждение результат.

## 4. Обработка и конкурентность

1. Kafka consumer получает `material_uploaded` и валидирует контракт.
2. В **одной транзакции** PostgreSQL регистрирует `transcription_jobs` (`ON CONFLICT DO NOTHING`) и `processed_events`; лишь после commit подтверждает Kafka offset. При повторном `event_id` — ACK без работы. При повторном `job_id` — ACK без нового задания.
3. Асинхронный worker забирает подходящие задания через `SELECT ... FOR UPDATE SKIP LOCKED`, устанавливает `locked_until`. Во время долгого этапа продлевает аренду. CPU-bound и блокирующие операции (PyMuPDF/FFmpeg) не блокируют event loop.
4. Загружает файл из S3 или получает разрешённое YouTube-аудио; валидирует формат/лимиты; извлекает полный доступный текст; нормализует.
5. Создаёт русский Markdown-конспект в DeepSeek. Для длинного текста — chunking по смысловым границам → частичные конспекты → иерархическое объединение. Сохраняем единство терминов, формул и таблиц; ограничиваем число одновременных LLM-запросов.
6. Загружает `summary.md` в S3 по **детерминированному ключу** `users/{user_id}/materials/{material_id}/jobs/{job_id}/summary.md`.
7. Одной транзакцией переводит job в `completed` и записывает событие `summary_generated` в `outbox_events`. При окончательной ошибке аналогично `failed` + `transcription_failed`.
8. Независимая async-задача outbox publisher отправляет события в Kafka с partition key `user_id` и отмечает `published`. Повторное событие имеет **тот же `event_id`**.

**At-least-once, не exactly-once:** возможно повторное Kafka-сообщение после сбоя publisher; downstream-сервисы должны дедуплицировать `event_id`. При падении после платного API-вызова, но до сохранения результата возможен повторный платный вызов: идемпотентность DB не решает exactly-once для внешних API. Отдельный кеш OCR/STT для первого MVP не делаем.

**Повторы:** временные S3/Kafka/API/сеть/429/5xx → экспоненциальный backoff с jitter и лимитом 3 попытки задания. Учитываем `Retry-After`. Не повторяем ошибочный файл, недоступный материал без перспектив восстановления, отсутствие речи или неподдерживаемый формат. Publisher outbox имеет **отдельные** попытки публикации и не переобрабатывает документ. Lease-истечение возвращает задачу в очередь; отметки завершения и outbox атомарны.

## 5. Архитектура каталогов

Функциональная модульность без лишних интерфейсов. Один контейнер приложения содержит consumer, scheduler/worker и outbox publisher как отдельные async-задачи; PostgreSQL — второй контейнер.

```text
transcript-service/
├── app/
│   ├── main.py                    # жизненный цикл приложения
│   ├── core/
│   │   ├── config.py              # Pydantic Settings + env
│   │   ├── database.py            # engine, sessionmaker
│   │   ├── logging.py
│   │   └── exceptions.py
│   ├── features/
│   │   ├── jobs/
│   │   │   ├── models.py          # ORM: jobs, processed_events, outbox_events
│   │   │   ├── schemas.py         # статусы и схемы задания
│   │   │   ├── repository.py      # SQL-запросы, захват lease, транзакции
│   │   │   └── service.py         # жизненный цикл задания
│   │   ├── extraction/
│   │   │   ├── service.py         # маршрутизация по типу
│   │   │   ├── detector.py        # проверка содержимого
│   │   │   └── extractors/
│   │   │       ├── base.py        # только полезный общий Protocol/ABC
│   │   │       ├── txt.py
│   │   │       ├── docx.py
│   │   │       ├── pdf.py
│   │   │       ├── image.py
│   │   │       ├── audio.py
│   │   │       ├── video.py
│   │   │       └── youtube.py
│   │   └── summarization/
│   │       ├── service.py
│   │       ├── client.py          # DeepSeek HTTP-клиент
│   │       ├── chunker.py
│   │       └── prompts.py
│   ├── infrastructure/
│   │   ├── kafka/
│   │   │   ├── consumer.py
│   │   │   ├── producer.py
│   │   │   └── schemas.py         # Pydantic Kafka-контракты
│   │   └── s3/
│   │       └── client.py
│   └── workers/
│       └── processor.py
├── alembic/
│   ├── env.py
│   └── versions/
├── tests/
│   ├── unit/
│   ├── integration/
│   └── fixtures/
├── Dockerfile
├── docker-compose.yml
├── alembic.ini
├── pyproject.toml
├── uv.lock
├── .env.example
├── .gitignore
└── README.md
```

**Принципы:** KISS, YAGNI, умеренные SOLID/ООП. Не создаём слой ради слоя, интерфейс для единственной реализации или отдельные контейнеры на каждый формат. Общий extractor допустим, поскольку поддерживается несколько реализаций. Функции для простого преобразования допустимы наряду с классами.

**Предварительные зависимости:** Python 3.12+, `uv`, SQLAlchemy 2.x async + `asyncpg`, Alembic, Pydantic v2 + `pydantic-settings`, `aiokafka`, `aioboto3`/`boto3` (выбрать один подход), `httpx`, `PyMuPDF`, `python-docx`, `python-magic`/libmagic, FFmpeg, `pytest` + `pytest-asyncio`. Конкретные версии фиксируем в `uv.lock`. Для PDF-таблиц/формул оцениваем качество на тестовых материалах прежде чем добавлять специализированные библиотеки.

## 6. PostgreSQL: три таблицы

### `transcription_jobs`

| Поле | Тип | Смысл |
|---|---|---|
| `job_id` | `varchar` PK | ID **из Sprintly**, ключ идемпотентности задания |
| `material_id`, `user_id`, `correlation_id`, `source_event_id` | `varchar` | ID из входного события |
| `source_bucket`, `source_key` | `varchar` / `text`, nullable | Исходник S3 |
| `source_url` | `text`, nullable | YouTube URL; взаимно исключается с S3-источником |
| `detected_type` | `varchar`, nullable | Фактический тип источника |
| `summary_bucket`, `summary_key` | `varchar` / `text`, nullable | Markdown в S3 |
| `status` | `varchar` | `pending`, `processing`, `retry_wait`, `completed`, `failed` |
| `current_stage` | `varchar`, nullable | `download`, `extract`, `clean`, `summarize`, `upload` |
| `attempt_count` | `int` | Количество запусков обработки |
| `next_retry_at`, `locked_until` | `timestamptz`, nullable | Backoff, lease |
| `error_code`, `error_message` | `varchar` / `text`, nullable | Последняя ошибка без секретов |
| `retryable` | `boolean` | Может ли быть повторена |
| `pipeline_version` | `varchar` | Версия пайплайна |
| `models` | `jsonb` | Использованные модели |
| `created_at`, `started_at`, `completed_at` | `timestamptz` | Временные отметки |

**Инварианты:** ровно один тип источника (S3-пара или URL); `job_id` уникален; терминальные задания не запускаются повторно; результат S3 назначается только успешному заданию. Предусмотреть индексы для выборки `pending/retry_wait`, `locked_until`. Возможная новая обработка того же материала имеет **новый `job_id`**.

### `processed_events`

- `event_id varchar PRIMARY KEY`
- `processed_at timestamptz NOT NULL`

Записываем в одной транзакции с регистрацией job. Дедупликация событий и заданий — разные задачи.

### `outbox_events`

- `id varchar PRIMARY KEY` — выходной `event_id`, генерируется один раз.
- `job_id varchar` — FK на `transcription_jobs`.
- `topic varchar`, `partition_key varchar`, `event_type varchar`, `payload jsonb`.
- `status varchar` (`pending`/`published`), `attempt_count int`, `next_retry_at timestamptz`.
- `created_at timestamptz`, `published_at timestamptz`.
- Ограничение `UNIQUE(job_id, event_type)` плюс бизнес-правило **ровно один терминальный исходящий тип** на job (проверка в транзакционном сервисе; при необходимости частичный индекс/дополнительное ограничение).

**Миграции:** Alembic. Отдельная одноразовая Docker Compose команда/сервис запускает `alembic upgrade head` **до** старта рабочего процесса. Не запускаем миграцию из каждого worker. PostgreSQL с persistent volume и healthcheck, наружу порт БД по умолчанию не публикуется.

## 7. Kafka-контракты

Из исходной спецификации сохраняются имена топиков/событий и общие поля. **Единственное необходимое расширение — `source.type` и `source.url` для YouTube.** Это изменение необходимо согласовать со Sprintly и Teacher до интеграции; `schema_version` обновлять совместно, не объявляем новый формат уже согласованным внешним контрактом. При необходимости временная обратная совместимость: сообщение без `source.type` трактуем как S3, если задана пара `bucket`/`key`.

Общие поля: `event_id`, `event_type`, `schema_version`, `correlation_id`, `job_id`, `user_id`. Kafka partition key: `user_id`. DLQ-топики: `material.events.dlq`, при необходимости `transcript.events.dlq` согласно конфигурации общего брокера.

### Вход: `material_uploaded` (`material.events`)

S3:

```json
{
  "event_id": "evt_001",
  "event_type": "material_uploaded",
  "schema_version": 1,
  "correlation_id": "corr_001",
  "job_id": "job_001",
  "material_id": "mat_001",
  "user_id": "user_42",
  "source": {
    "type": "s3",
    "bucket": "learning-materials",
    "key": "users/user_42/materials/mat_001/source.pdf"
  }
}
```

YouTube — **проект расширения, до согласования не production-контракт**:

```json
{
  "event_id": "evt_004",
  "event_type": "material_uploaded",
  "schema_version": 2,
  "correlation_id": "corr_004",
  "job_id": "job_004",
  "material_id": "mat_004",
  "user_id": "user_42",
  "source": {
    "type": "youtube",
    "url": "https://www.youtube.com/watch?v=VIDEO_ID"
  }
}
```

Номер `2` здесь — **предложение версии**, окончательный номер назначается совместно с Sprintly.

### Выход: `summary_generated` (`transcript.events`)

```json
{
  "event_id": "evt_002",
  "event_type": "summary_generated",
  "schema_version": 1,
  "correlation_id": "corr_001",
  "job_id": "job_001",
  "material_id": "mat_001",
  "user_id": "user_42",
  "summary": {
    "bucket": "learning-materials",
    "key": "users/user_42/materials/mat_001/jobs/job_001/summary.md"
  }
}
```

### Выход: `transcription_failed` (`transcript.events`)

```json
{
  "event_id": "evt_003",
  "event_type": "transcription_failed",
  "schema_version": 1,
  "correlation_id": "corr_001",
  "job_id": "job_001",
  "material_id": "mat_001",
  "user_id": "user_42",
  "failed_stage": "extract",
  "error_code": "source_unavailable",
  "error_message": "Не удалось получить доступ к аудио источника. Загрузите MP3 или MP4.",
  "retryable": false
}
```

**Невалидные входные сообщения:** в DLQ с техническими метаданными без утечки ключей, затем commit offset при подтверждённой доставке в DLQ; если отправка в DLQ недоступна — не теряем событие без следа. Для валидных событий offset commit только после commit БД. Outbox публикуется at-least-once.

## 8. Ошибки и ретраи

| `error_code` | Причина | По умолчанию retryable |
|---|---|---|
| `unsupported_format` | Формат не поддерживается | нет |
| `file_too_large` | Превышены согласованные лимиты размера, страниц или длительности | нет |
| `corrupted_file` | Файл повреждён, структура не читается | нет |
| `empty_content` | Нет извлечённого содержательного текста/речи | нет |
| `source_unavailable` **новый** | YouTube-аудио недоступно либо источник недоступен устойчиво | нет |
| `extraction_failed` | Временный сбой извлечения/OCR/STT | да |
| `llm_unavailable` | DeepSeek недоступен / временная API-ошибка | да |
| `llm_invalid_output` | Пустой/некорректный ответ модели после проверок | да |
| `timeout` | Таймаут этапа/жизненного цикла | да, пока попытки остались |
| `internal` | Прочие внутренние сбои | по классификации |

Из исходного README имена сохраняем, `source_unavailable` добавляем отдельно и согласуем с Sprintly. Ошибки 401/403 из-за неверного ключа **не повторяем бесконечно**: это операционная проблема конфигурации. Ошибки `429` повторяем с серверным `Retry-After`; таймауты и 5xx — с backoff. В логи не попадают приватные файлы, тексты транскрипций, секреты или полные URL с чувствительными параметрами.

## 9. Markdown-конспект

**Всегда на русском, независимо от исходного языка.** Сохраняем:

- заголовок `# ...`, разделы `## ...` / `### ...`;
- связное объяснение материала, важные определения, причинно-следственные связи, примеры и выводы;
- формулы в LaTeX (inline `$...$`, block `$$...$$`);
- таблицы в Markdown, если они читаемы, либо эквивалентное структурированное изложение;
- корректные единицы измерения, числа, имена, оригинальные термины и обозначения.

**Не включаем:** `source_locator`, номера страниц, таймкоды, служебные маркеры, XML-like reasoning-теги, выдуманные факты или ссылки на несуществующие источники. Удаление маркеров требует проверки ожиданий Teacher по `QuestionSources.source_locator`; Transcript не заполняет это поле.

Промпты храним в `features/summarization/prompts.py` и версионируем через `pipeline_version`. Пустой Markdown не публикуем. Потенциально вредные инструкции внутри пользовательского файла трактуем как **данные**, а не как системные команды.

## 10. Конфигурация и безопасность

Все адреса, ключи, топики, таймауты, модели и лимиты задаём **через environment variables**. `.env` в `.gitignore`, `.env.example` без секретов. Не кодируем реальные пароли, endpoint и пользовательские бакеты в Python.

Ожидаемые группы параметров (имена — проектный контракт окружения, уточняем при реализации):

```dotenv
# App
APP_ENV=development
LOG_LEVEL=INFO
WORKER_CONCURRENCY=3
JOB_TARGET_SECONDS=180
JOB_MAX_ATTEMPTS=3
PIPELINE_VERSION=v1

# PostgreSQL (compose)
POSTGRES_HOST=postgres
POSTGRES_PORT=5432
POSTGRES_DB=transcript
POSTGRES_USER=transcript
POSTGRES_PASSWORD=

# Kafka (external)
KAFKA_BOOTSTRAP_SERVERS=
KAFKA_CONSUMER_GROUP=transcript-service
KAFKA_INPUT_TOPIC=material.events
KAFKA_OUTPUT_TOPIC=transcript.events
KAFKA_INPUT_DLQ_TOPIC=material.events.dlq
KAFKA_SECURITY_PROTOCOL=PLAINTEXT
KAFKA_SASL_MECHANISM=
KAFKA_SASL_USERNAME=
KAFKA_SASL_PASSWORD=

# S3-compatible storage (external; vendor unknown)
S3_ENDPOINT_URL=
S3_REGION=
S3_ACCESS_KEY_ID=
S3_SECRET_ACCESS_KEY=
S3_OUTPUT_BUCKET=
S3_ADDRESSING_STYLE=auto

# AI APIs
DEEPSEEK_API_KEY=
DEEPSEEK_BASE_URL=https://api.deepseek.com
DEEPSEEK_MODEL=deepseek-flash
GROQ_API_KEY=
GROQ_BASE_URL=https://api.groq.com/openai/v1
STT_MODEL=whisper-large-v3-turbo

# Limits (preliminary)
MAX_DOCUMENT_FILE_SIZE_MB=50
MAX_PDF_PAGES=500
MAX_MEDIA_DURATION_SECONDS=3600
MAX_MEDIA_FILE_SIZE_MB=200
API_REQUEST_TIMEOUT_SECONDS=120
```

**Важное:** `KAFKA_SECURITY_PROTOCOL=PLAINTEXT` годится только для доверенной локальной сети; в реальной внешней инфраструктуре применять предоставленные Sprintly TLS/SASL-настройки. `MAX_MEDIA_FILE_SIZE_MB=200` — **предложение**, не согласованный лимит. Пароль БД задаётся при развёртывании и не публикуется в Git. S3-ключи выдаются с минимальными доступами, желательно отдельно на чтение источников и запись результата. Внешние вызовы должны использовать TLS.

Переменные `POSTGRES_*` — для контейнера DB; приложение получает собранный DSN через config без хардкода. Старт приложения прекращается с понятной ошибкой, если отсутствуют обязательные переменные окружения.

## 11. Docker Compose и развёртывание (план, без кода)

1. `postgres`: отдельная PostgreSQL-БД сервиса Transcript, volume для сохранности данных, healthcheck, сеть Compose, без публичного порта по умолчанию.
2. `transcript`: Python runtime с FFmpeg, подключение к PostgreSQL, внешним Kafka/S3 и AI API по env, `restart` policy.
3. Одноразовая команда/сервис `migrate`: `alembic upgrade head`; должна успешно завершиться **перед** рабочим процессом Transcript.
4. При первом запуске создать `.env` из `.env.example`, внести реальные адреса Kafka/S3 и ключи DeepSeek/Groq, проверить права и TLS.
5. Обновление: backup PostgreSQL, миграции, запуск приложения, проверка логов и тестового события.

**Не нужен HTTP server.** Наблюдаемость — структурированные логи по `job_id`/`correlation_id`, счётчики стадий и ошибок, SQL-запросы для текущих заданий; не создаём отдельный monitoring stack для MVP.

## 12. Тесты и критерии готовности

**Unit tests:** детектор формата; маршрутизация extractor; TXT/DOCX/PDF; обработка таблиц и LaTeX; chunking; валидация Kafka-событий; переходы статусов; классификация ошибок; retry/backoff; deterministic S3 key; безопасная конфигурация.

**Integration tests:** PostgreSQL + Alembic на чистой БД; уникальность `job_id` и `event_id`; consumer commit после транзакции; `SKIP LOCKED` при нескольких worker; lease recovery; outbox повторной публикации с тем же ID; S3 get/put через подменяемый тестовый endpoint; mock DeepSeek/Groq (реальные платные запросы не обязательны для CI).

**Smoke / ручные тесты:** русские и английские TXT/DOCX/PDF; скан с формулой и таблицей; PNG/JPEG; MP3 и MP4 с русской речью; один доступный и один недоступный YouTube URL; длительное медиа; повторный Kafka event; повторный `job_id`; новый `job_id` того же материала; рестарт в середине обработки.

**Готово к интеграции, когда:**

- Валидный `material_uploaded` приводит к одному `summary.md` в S3 и `summary_generated` в Kafka.
- Markdown полностью на русском, содержательно подробный, с корректно переданными тестовыми формулами и таблицами, без маркеров страниц/таймкодов.
- Ошибки дают один логический `transcription_failed` и понятный `error_code`.
- Повторная доставка события не запускает новое задание; новый `job_id` запускает.
- После остановки/перезапуска задания с истёкшим lease продолжаются, outbox не теряется.
- Сервис стартует на 4 vCPU / 8 GB и на 2 vCPU / 4 GB с меньшим `WORKER_CONCURRENCY`.
- В проекте нет локальных LLM/STT, секретов в репозитории, REST API и ненужных инфраструктурных компонентов.
- Время обработки измерено на небольшом наборе эталонных материалов; цель 180 секунд документирована **без ложных гарантий** для тяжёлых сканов и видео.

## 13. Внешние согласования / риски

1. **Sprintly:** версия и форма YouTube `source`; новый `source_unavailable`; лимиты размера/длительности; внешний watchdog 30 минут.
2. **Teacher:** отсутствие source markers и влияние на `QuestionSources.source_locator`; допустимость только `summary.md`.
3. **Инфраструктура:** фактический S3-провайдер, endpoint, region, path-style, TLS, bucket, IAM-права; адреса Kafka, SASL/TLS и существование DLQ-топиков.
4. **Технические ограничения:** законный доступ к YouTube-аудио; потенциальная недоступность отдельных роликов, особенно с VPS; результаты OCR/vision на сложных формулах и многостраничных сканах; реальные API rate limits.
5. **Производительность:** проверить тайминг/память на 2 vCPU / 4 GB, не обещать три минуты для всех файлов.

Эти пункты **не блокируют написание спецификации или подготовку окружения**, но должны быть подтверждены до полной интеграции с Sprintly/Teacher.

## 14. Последовательность реализации в Codex

1. Каркас проекта, `uv`, конфигурация и Docker Compose с PostgreSQL.
2. ORM, Alembic и миграция трёх таблиц; репозиторий заданий.
3. Kafka consumer, Pydantic-схемы, идемпотентность и commit offsets.
4. Worker/lease и S3 adapter; TXT/DOCX/PDF text-layer.
5. OCR/vision PNG/JPEG и сканированных PDF; формулы/таблицы.
6. Groq STT для MP3/MP4; YouTube adapter только для разрешённых источников.
7. DeepSeek summarizer, chunking, русский Markdown.
8. Transactional Outbox, ошибки, DLQ, retries.
9. Unit/integration tests, нагрузочный smoke-test, README-инструкция запуска.

## 15. Документация провайдеров

- [DeepSeek Vision](https://api-docs.deepseek.com/guides/vision/) — `deepseek-flash` и входные изображения.
- [DeepSeek model updates](https://api-docs.deepseek.com/updates/) — актуальные ID моделей.
- [Groq Speech-to-Text](https://console.groq.com/docs/speech-to-text) — Whisper, лимиты, чанки.
- [Groq Whisper Large V3 Turbo](https://console.groq.com/docs/model/whisper-large-v3-turbo) — модель STT.
- [YouTube API Developer Policies](https://developers.google.com/youtube/terms/developer-policies) — ограничения скачивания/выделения аудио.

---