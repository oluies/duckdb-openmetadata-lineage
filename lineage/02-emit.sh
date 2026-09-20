#!/bin/bash
# 02 Post the last dbt run's lineage to OpenMetadata (after dbt-project/01-dbt-build.sh and 01-om-setup.sh).
set -eu
. "$(dirname "$0")/../scripts/env.sh"
cd "$DEMO_ROOT/dbt-project"      # the emitter reads ./target/*.json

export OM_HOST="${OM_HOST:-http://localhost:8595}"
export OM_JWT_TOKEN_FILE="$DEMO_ROOT/out/om-lineage-bot.jwt"      # a bot token, not a login
export OL_SQLSERVER_CATALOGS="refdata=localhost:$MSSQL_PORT"        # dbt database -> SQL Server
export OM_SQLSERVER_SERVICES="localhost:$MSSQL_PORT=mssql_demo"     # SQL Server -> OM service
export OL_DUCKDB_NAMESPACE="duckdb://trading_calendar"
export OM_SERVICE_DUCKDB="duckdb_trading_calendar"
export OL_JOB_NAMESPACE="trading_calendar"

echo "== dry run: the first event"
OL_DRY_RUN=1 "$DEMO_ROOT/.venv/bin/python" "$DEMO_ROOT/lineage/emit_openlineage.py" 2>/dev/null \
  | jq '.events[] | select(.job.name | endswith("fct_holiday_calendar"))
        | {job: .job.name, inputs: [.inputs[] | "\(.namespace) \(.name)"],
           output: "\(.outputs[0].namespace) \(.outputs[0].name)",
           column_lineage: (.outputs[0].facets.columnLineage.fields // {} | to_entries
                            | map("\(.key) <- \(.value.inputFields | map(.field) | join(", "))"))}'

echo "== post"
"$DEMO_ROOT/.venv/bin/python" "$DEMO_ROOT/lineage/emit_openlineage.py"
