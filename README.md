# transcription-service-sprintly

Сервис **Transcript** обучающей платформы. Получает загруженный пользователем материал, извлекает из него содержание и создаёт Markdown-конспект для Teacher.

> Соответствует общей спецификации MVP и выжимке по Sprintly. Названия событий, полей и коды ошибок — общий контракт; меняются только по согласованию с Sprintly и Teacher.

## 1. Роль и границы

| Transcript владеет | Transcript НЕ владеет |
|---|---|
| Определение типа материала | Материалы и их метаданные (Sprintly) |
| Извлечение содержания (текст, OCR, речь) | Статус обработки для UI (Sprintly) |
| Создание Markdown-конспекта и его ключ в S3 | Вопросы, категории, дедупликация (Teacher) |
| Свои задания конспектирования (`transcription_jobs`) | |

- Сервис не читает БД других сервисов и не вызывает их по HTTP. Взаимодействие только через Kafka, файлы — через S3.
- Вход: событие `material_uploaded` из топика `material.events`.
- Выход: `summary_generated` или `transcription_failed` в топик `transcript.events`.
- Сервис не создаёт собственный `job_id`: он использует `job_id`, выданный Sprintly. Повтор обработки в Sprintly — это **новый** `job_id`, и Transcript обрабатывает его как новое задание.

## 2. Обработка

| Тип материала | Извлечение | Далее |
|---|---|---|
| MP3 (аудио) | Whisper (`faster-whisper`), локально | очистка речи → конспект |
| TXT | чтение текста | конспект |
| DOCX | `python-docx` | конспект |
| PDF | `PyMuPDF`; при отсутствии текстового слоя — OCR страниц | конспект |
| JPG и изображения | Tesseract OCR | конспект |

Конспект строит локальная LLM: Ollama + Qwen3 4B. Для аудио модель дополнительно убирает слова-паразиты и восстанавливает пунктуацию.

Правила пайплайна:

- Тип определяется по содержимому файла (magic bytes), а не по расширению. Неподдерживаемый тип даёт `unsupported_format`.
- Пустой результат после всех способов извлечения (тишина в аудио, пустой скан) даёт `empty_content`, а не пустой конспект.
- Длинный текст не передаётся в модель целиком: он делится на фрагменты, по каждому строится частичный конспект, затем они объединяются.
- Thinking-режим Qwen3 отключён; если в ответе есть `<think>`, блок удаляется. Ответ, который не удалось привести к Markdown, даёт `llm_invalid_output`.

Лимиты размера файлов и длительности аудио фиксируются вместе со Sprintly (открытое решение «Поддерживаемые форматы и лимиты загрузки»). Длительность обработки должна укладываться в сторожевой таймер Sprintly (30 минут в активном статусе); превышение лимита отклоняется как `file_too_large`.

## 3. Архитектура

```text
transcription-service-sprintly/
├── app/
│   ├── main.py             # Сборка зависимостей, запуск consumer, worker и outbox publisher
│   ├── messaging/          # Kafka: consumer, producer, handlers, сериализация, DLQ
│   ├── services/           # Сценарий: приём → извлечение → очистка → конспект → публикация
│   ├── domain/             # Типы материала, статусы задания, коды ошибок, бизнес-правила
│   │   └──ports/       
│   ├── schemas/            # Pydantic-схемы входящих и исходящих событий (общий контракт)
│   ├── integrations/       # Адаптеры: s3/, kafka/, ollama/
│   ├── ml/                 # Whisper, OCR, извлечение текста, генерация конспекта
│   ├── repositories/       # transcription_jobs, outbox_events, processed_events
│   ├── db/                 # Подключение к PostgreSQL, сессии, Unit of Work
│   ├── models/             # ORM-модели SQLAlchemy
│   └── core/               # Конфигурация, логирование, общие настройки
├── alembic/versions/       # Миграции PostgreSQL
├── tests/
│   ├── unit/               # Бизнес-правила и ML-пайплайн
│   ├── integration/        # PostgreSQL, Kafka, S3, Ollama
│   └── fixtures/           # Тестовые материалы и моки
├── deploy/
│   ├── Dockerfile
│   ├── docker-compose.yml  # worker + Ollama + PostgreSQL; Kafka и S3 — по общей схеме развёртывания
│   └── ollama/
├── pyproject.toml
├── uv.lock
├── .env.example
└── README.md
```

### ML-модули

```text
app/ml/
├── detection.py            # Определение типа по содержимому
├── audio/
│   ├── transcriber.py      # Интерфейс транскрипции
│   └── whisper.py          # faster-whisper, сегменты с таймкодами
├── documents/
│   ├── extractor.py        # Общий интерфейс извлечения
│   ├── pdf.py              # Текстовый слой + фолбэк на OCR
│   ├── docx.py
│   └── txt.py
├── ocr/
│   └── tesseract.py
├── text/
│   ├── cleaner.py          # Удаление речевого мусора
│   ├── normalizer.py       # Нормализация текста
│   └── chunker.py          # Разбиение длинного текста на фрагменты
└── summarization/
    ├── summarizer.py       # Интерфейс создания конспекта
    ├── ollama.py           # Вызов локальной LLM
    └── prompts.py          # Шаблоны инструкций
```

## 4. Данные (PostgreSQL)

### `transcription_jobs`

| Поле | Назначение |
|---|---|
| `job_id` | ID задания из Sprintly, **первичный ключ** и ключ идемпотентности |
| `material_id` | ID материала в Sprintly |
| `user_id` | Владелец материала; нужен в событиях и как ключ партиции |
| `correlation_id` | Пробрасывается из входящего события в исходящее |
| `source_event_id` | `event_id` входящего `material_uploaded` |
| `source_bucket`, `source_key` | Исходник в S3 |
| `detected_type` | Определённый тип материала |
| `summary_bucket`, `summary_key` | Готовый Markdown в S3 |
| `status` | Состояние задания (см. ниже) |
| `current_stage` | `download`, `extract`, `clean`, `summarize`, `upload` |
| `attempt_count` | Число попыток |
| `next_retry_at` | Время следующей попытки при `retry_wait` |
| `locked_until` | Аренда задания worker'ом; просроченная аренда позволяет подобрать задание после падения |
| `error_code`, `error_message`, `retryable` | Последняя ошибка; `error_message` — понятный пользователю текст без технических деталей |
| `pipeline_version`, `models` | Версия пайплайна и моделей для воспроизводимости |
| `created_at`, `started_at`, `completed_at` | Время |

Статусы: `pending` → `processing` → (`retry_wait` → `processing`)* → `completed` | `failed`.

### `outbox_events`

| Поле | Назначение |
|---|---|
| `id` | `event_id` исходящего события; генерируется **один раз** при вставке |
| `job_id` | Ссылка на `transcription_jobs.job_id` |
| `topic` | `transcript.events` |
| `partition_key` | `user_id` |
| `event_type` | `summary_generated` или `transcription_failed` |
| `payload` | Тело события |
| `status` | `pending` или `published` |
| `attempt_count`, `next_retry_at` | Повторы публикации |
| `created_at`, `published_at` | Время |

### `processed_events`

`event_id` уже обработанных входящих сообщений. Защищает от повторной доставки одного сообщения Kafka.

## 5. События

Все события содержат общие поля: `event_id`, `event_type`, `schema_version`, `correlation_id`, `job_id`, `user_id`. Ключ партиции — `user_id`. У топиков есть `*.dlq`.

### Вход: `material_uploaded` (`material.events`, пишет Sprintly)

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
    "bucket": "learning-materials",
    "key": "users/user_42/materials/mat_001/source.pdf"
  }
}
```

### Выход: `summary_generated` (`transcript.events`, читают Teacher и Sprintly)

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

### Выход: `transcription_failed` (`transcript.events`, читает Sprintly)

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
  "error_code": "corrupted_file",
  "error_message": "Не удалось прочитать файл. Проверьте, что он не повреждён.",
  "retryable": false
}
```

### Коды ошибок

| Код | Когда | `retryable` |
|---|---|---|
| `unsupported_format` | Тип не поддерживается | нет |
| `file_too_large` | Превышен лимит размера или длительности | нет |
| `corrupted_file` | Файл не читается | нет |
| `empty_content` | Содержание не извлечено | нет |
| `extraction_failed` | Сбой извлечения после ретраев | да |
| `llm_unavailable` | Ollama недоступна после ретраев | да |
| `llm_invalid_output` | Модель не вернула пригодный конспект | да |
| `timeout` | Превышено время обработки | да |
| `internal` | Прочие ошибки | да |

## 6. Как всё работает

### 1. Приём события

1. Проверить схему. Невалидное сообщение сразу уходит в `material.events.dlq` с алертом.
2. Если `event_id` есть в `processed_events`, подтвердить сообщение и выйти.
3. В одной транзакции: вставить `transcription_jobs` по `job_id` (`ON CONFLICT DO NOTHING`) и `processed_events`.
4. Подтвердить offset в Kafka.

Задание с тем же `job_id` не создаётся повторно. Новый `job_id` для того же материала — новое задание, даже если исходник не изменился.

### 2. Выполнение пайплайна

Worker берёт задания в `pending` или `retry_wait` с наступившим `next_retry_at`, а также задания в `processing` с просроченным `locked_until` (worker упал посреди обработки). Захват — `SELECT … FOR UPDATE SKIP LOCKED` с установкой аренды; во время долгих этапов аренда продлевается.

Этапы: скачать исходник из S3 → определить тип → извлечь содержание → очистить → построить конспект. `current_stage` обновляется по мере прохождения.

Временные сбои (S3, Ollama, таймауты): до 3 попыток с экспоненциальным backoff через `retry_wait`. Ошибки содержания (`unsupported_format`, `corrupted_file`, `empty_content`, `file_too_large`) не повторяются.

### 3. Сохранение результата

Markdown загружается в S3 по детерминированному ключу задания:

```text
users/{user_id}/materials/{material_id}/jobs/{job_id}/summary.md
```

Повторная загрузка после сбоя перезаписывает тот же объект и не создаёт нового артефакта. Ключ включает `job_id`, поэтому повторная обработка материала не затирает конспект, который Teacher, возможно, ещё читает.

### 4. Завершение и публикация

В одной транзакции PostgreSQL:

- задание переводится в `completed` (или `failed`);
- в `outbox_events` добавляется `summary_generated` (или `transcription_failed`).

Отдельный publisher отправляет записи в `transcript.events` с ключом `user_id` и помечает их `published`.

> Transactional Outbox гарантирует, что событие не потеряется (at-least-once). Если publisher упадёт между отправкой и пометкой, событие уйдёт повторно с тем же `event_id`; получатели отсекают его через `processed_events`. Exactly-once на стороне Transcript не обеспечивается и не требуется.
