# Column lineage from dbt-duckdb into OpenMetadata. See README.md.
SHELL := /bin/bash

.PHONY: help setup mssql-up mssql-down om-up om-down om-status dbt lineage verify nats-up nats-down nats-emit clean

help:
	@echo "make setup       uv venv, pinned packages, DuckDB extensions, cached holiday Parquet"
	@echo "make mssql-up    SQL Server 2022 in Apple container on localhost:11433, seeded"
	@echo "make om-up       OpenMetadata 2.0.2 (docker compose) on http://localhost:8595"
	@echo "make dbt         dbt build: staging in DuckDB, marts in SQL Server"
	@echo "make lineage     one-time OM setup (bot, namespace mapping, SQL Server ingestion), then emit"
	@echo "make verify      read the lineage back from OM's API"
	@echo "make nats-up / nats-emit   publish the same events to NATS JetStream instead"

setup:
	uv sync
	. scripts/env.sh; duckdb -no-init -c "INSTALL httpfs; INSTALL azure; FORCE INSTALL mssql FROM community;"
	dbt-project/cache-holidays.sh

mssql-up:    ; @scripts/mssql.sh up
mssql-down:  ; @scripts/mssql.sh down
om-up:       ; @scripts/om.sh up
om-down:     ; @scripts/om.sh down
om-status:   ; @scripts/om.sh status
dbt:         ; @dbt-project/01-dbt-build.sh
lineage:     ; @lineage/01-om-setup.sh && lineage/02-emit.sh
verify:      ; @lineage/03-verify.sh
nats-up:     ; @scripts/nats.sh up
nats-down:   ; @scripts/nats.sh down
nats-emit:   ; @lineage/04-nats-emit.sh

clean:
	rm -rf out dbt-project/target dbt-project/logs
