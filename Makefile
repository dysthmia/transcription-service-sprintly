include .env
export

export PROJECT_ROOT := $(CURDIR)

.PHONY: help postgres-up postgres-down postgres-logs postgres-cleanup migrate app-build app-up app-down app-logs ps

help:
	@echo 'make postgres-up       - start PostgreSQL'
	@echo 'make postgres-down     - stop PostgreSQL'
	@echo 'make postgres-logs     - PostgreSQL logs'
	@echo 'make postgres-cleanup  - delete local PostgreSQL data (confirmation required)'
	@echo 'make migrate           - apply Alembic migrations (once code exists)'
	@echo 'make app-build         - build Transcript image (once Dockerfile exists)'
	@echo 'make app-up            - start Transcript (once code exists)'
	@echo 'make app-down          - stop Transcript'
	@echo 'make app-logs          - Transcript logs'
	@echo 'make ps                - show containers'

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
	@docker compose --profile tools run --rm migrate

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