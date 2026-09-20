#!/bin/bash
# 04 Publish the last dbt run's lineage to NATS JetStream instead of POSTing it to OM.
# Prerequisites: dbt-project/01-dbt-build.sh (writes target/*.json) and scripts/nats.sh up.
set -eu
. "$(dirname "$0")/../scripts/env.sh"
cd "$DEMO_ROOT/dbt-project"      # the emitter reads ./target/*.json

export OL_SQLSERVER_CATALOGS="refdata=localhost:$MSSQL_PORT"
export OM_SQLSERVER_SERVICES="localhost:$MSSQL_PORT=mssql_demo"
export OL_DUCKDB_NAMESPACE="duckdb://trading_calendar"
export OM_SERVICE_DUCKDB="duckdb_trading_calendar"
export OL_JOB_NAMESPACE="trading_calendar"
export NATS_URL="${NATS_URL:-nats://localhost:$NATS_PORT}"
export NATS_SUBJECT="${NATS_SUBJECT:-openlineage.events}"

echo "== build the events from the dbt artifacts, publish them to $NATS_SUBJECT"
OL_DRY_RUN=1 "$DEMO_ROOT/.venv/bin/python" "$DEMO_ROOT/lineage/emit_openlineage.py" 2>/dev/null \
  | "$DEMO_ROOT/.venv/bin/python" "$DEMO_ROOT/lineage/publish_openlineage_nats.py"

echo
echo "== what the stream holds now"
"$DEMO_ROOT/scripts/nats.sh" stream
