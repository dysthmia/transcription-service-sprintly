include .env
export

export PROJECT_ROOT := $(CURDIR)

.PHONY: help postgres-up postgres-down postgres-logs postgres-cleanup migrate app-build app-up app-down app-logs run-local-test local-test-down local-test-logs local-test-reset sender-help sender-upload sender-publish-s3 sender-publish-youtube sender-send-s3 watch-help watch-results ps test-unit test-integration test-all

help:
	@echo 'make postgres-up       - start PostgreSQL'
	@echo 'make postgres-down     - stop PostgreSQL'
	@echo 'make postgres-logs     - PostgreSQL logs'
	@echo 'make postgres-cleanup  - delete local PostgreSQL data (confirmation required)'
	@echo 'make migrate           - apply Alembic migrations'
	@echo 'make MIGRATE_POSTGRES_DB=transcript_test migrate - migrate a separate local database'
	@echo 'make app-build         - build Transcript image (once Dockerfile exists)'
	@echo 'make app-up            - start Transcript (once code exists)'
	@echo 'make app-down          - stop Transcript'
	@echo 'make app-logs          - Transcript logs'
	@echo 'make run-local-test    - start the complete local manual integration-test contour'
	@echo 'make local-test-down   - stop the local test contour without deleting data'
	@echo 'make local-test-logs   - follow Transcript, Kafka and MinIO logs'
	@echo 'make local-test-reset  - delete only local PostgreSQL and MinIO test data (confirmation required)'
	@echo 'make sender-help       - show local Kafka/MinIO test sender help'
	@echo 'make sender-upload FILE=path [SENDER_ARGS="..."] - upload a fixture to MinIO'
	@echo 'make sender-send-s3 FILE=path [SENDER_ARGS="..."] - upload and publish material_uploaded'
	@echo 'make sender-publish-s3 SENDER_ARGS="--key ..." - publish an S3 event'
	@echo 'make sender-publish-youtube SENDER_ARGS="--url ..." - publish a YouTube event'
	@echo 'make watch-help        - show Kafka result watcher help'
	@echo 'make watch-results WATCH_ARGS="--job-id ... --once" - read output and input DLQ'
	@echo 'make ps                - show containers'
	@echo 'make test-unit         - run unit tests'
	@echo 'make test-integration  - run PostgreSQL integration tests (TEST_DATABASE_URL required)'
	@echo 'make test-all          - run all tests'

postgres-up:
	@mkdir -p "$(PROJECT_ROOT)/out/pgdata"
	@docker compose up -d postgres

postgres-down:
	@docker compose stop postgres

postgres-logs:
	@docker compose logs -f postgres

postgres-cleanup:
	@read -r -p 'Очистить ВСЕ данные PostgreSQL? [YES/n]: ' ans; \
	if [ "$$ans" = "YES" ]; then \
		docker compose stop postgres && \
		docker compose rm -f postgres && \
		rm -rf -- "$(PROJECT_ROOT)/out/pgdata" && \
		echo 'Данные PostgreSQL очищены'; \
	else \
		echo 'Очистка отменена'; \
	fi

migrate:
	@docker compose --profile tools run --rm $(if $(MIGRATE_POSTGRES_DB),-e POSTGRES_DB="$(MIGRATE_POSTGRES_DB)") migrate

app-build:
	@docker compose --profile runtime build transcript

app-up:
	@docker compose --profile runtime up -d transcript

app-down:
	@docker compose --profile runtime stop transcript

app-logs:
	@docker compose --profile runtime logs -f transcript

run-local-test:
	@mkdir -p "$(PROJECT_ROOT)/out/pgdata" "$(PROJECT_ROOT)/out/minio-data"
	@docker compose up -d --wait postgres kafka minio
	@docker compose up --build --no-deps --abort-on-container-exit --exit-code-from init init
	@docker compose --profile tools run --rm --no-deps migrate
	@WORKER_CONCURRENCY=3 docker compose --profile runtime up -d --build transcript
	@echo ''
	@echo 'Local Transcript integration-test contour is ready.'
	@echo 'MinIO Console: http://localhost:9001'
	@echo 'Next: make local-test-logs'
	@echo 'Kafka input: docker compose exec kafka /opt/kafka/bin/kafka-console-producer.sh --bootstrap-server localhost:9092 --topic material.events'
	@echo 'Stop without deleting data: make local-test-down'

local-test-down:
	@docker compose --profile runtime stop transcript postgres kafka minio

local-test-logs:
	@docker compose --profile runtime logs -f transcript kafka minio

local-test-reset:
	@read -r -p 'Удалить локальные данные PostgreSQL и MinIO test-контура? [YES/n]: ' ans; \
	if [ "$$ans" = "YES" ]; then \
		docker compose --profile runtime stop transcript postgres kafka minio; \
		docker compose --profile runtime --profile tools rm -f transcript postgres kafka minio init migrate; \
		rm -rf -- "$(PROJECT_ROOT)/out/pgdata" "$(PROJECT_ROOT)/out/minio-data"; \
		echo 'Локальные данные test-контура удалены'; \
	else \
		echo 'Очистка отменена'; \
	fi

sender-help:
	@docker compose --profile tools run --rm test-sender --help

sender-upload:
	@test -n "$(FILE)" || (echo 'Specify FILE=path/to/fixture'; exit 2)
	@docker compose --profile tools run --rm -v "$(abspath $(FILE)):/input/$(notdir $(FILE)):ro" test-sender upload --file "/input/$(notdir $(FILE))" $(SENDER_ARGS)

sender-publish-s3:
	@docker compose --profile tools run --rm test-sender publish-s3 $(SENDER_ARGS)

sender-publish-youtube:
	@docker compose --profile tools run --rm test-sender publish-youtube $(SENDER_ARGS)

sender-send-s3:
	@test -n "$(FILE)" || (echo 'Specify FILE=path/to/fixture'; exit 2)
	@docker compose --profile tools run --rm -v "$(abspath $(FILE)):/input/$(notdir $(FILE)):ro" test-sender send-s3 --file "/input/$(notdir $(FILE))" $(SENDER_ARGS)

watch-help:
	@docker compose --profile tools run --rm test-watch --help

watch-results:
	@docker compose --profile tools run --rm test-watch $(WATCH_ARGS)

ps:
	@docker compose ps -a

test-unit:
	@uv run --frozen pytest tests/unit

test-integration:
	@uv run --frozen pytest -m integration

test-all:
	@uv run --frozen pytest
