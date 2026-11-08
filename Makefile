include .env
export

export PROJECT_ROOT := $(CURDIR)
export DATA_ROOT ?= $(PROJECT_ROOT)/out

.PHONY: help postgres-up postgres-down postgres-logs postgres-cleanup migrate app-build app-up app-down app-logs ps test-unit test-integration test-all observability-up observability-down observability-logs observability-status observability-config app-with-observability-up

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
	@echo 'make ps                - show containers'
	@echo 'make test-unit         - run unit tests'
	@echo 'make test-integration  - run PostgreSQL integration tests (TEST_DATABASE_URL required)'
	@echo 'make test-all          - run all tests'
	@echo 'make observability-up  - start Loki, Alloy, and Grafana'
	@echo 'make observability-down - stop Loki, Alloy, and Grafana without deleting data'
	@echo 'make observability-logs - follow observability logs'
	@echo 'make observability-status - show observability service status'
	@echo 'make observability-config - render the Compose configuration'
	@echo 'make app-with-observability-up - start app and observability profile'

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

ps:
	@docker compose ps -a

test-unit:
	@uv run --frozen pytest tests/unit

test-integration:
	@uv run --frozen pytest -m integration

test-all:
	@uv run --frozen pytest

observability-up:
	@mkdir -p "$(DATA_ROOT)/observability/loki" "$(DATA_ROOT)/observability/alloy" "$(DATA_ROOT)/observability/grafana"
	@docker compose --profile observability up -d loki alloy grafana

observability-down:
	@docker compose --profile observability stop loki alloy grafana

observability-logs:
	@docker compose --profile observability logs -f loki alloy grafana

observability-status:
	@docker compose --profile observability ps loki alloy grafana

observability-config:
	@docker compose --profile observability config

app-with-observability-up:
	@mkdir -p "$(DATA_ROOT)/observability/loki" "$(DATA_ROOT)/observability/alloy" "$(DATA_ROOT)/observability/grafana"
	@docker compose --profile runtime --profile observability up -d transcript loki alloy grafana
