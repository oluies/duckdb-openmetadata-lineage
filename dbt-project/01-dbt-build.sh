#!/bin/bash
# 01 dbt build: seeds + models + tests, DuckDB as the engine, SQL Server as the target.
# Needs scripts/mssql.sh up. Reads the cached holiday Parquet if `make setup` fetched it,
# otherwise straight from Azure blob storage.
set -eu
. "$(dirname "$0")/../scripts/env.sh"
mkdir -p "$DEMO_ROOT/out"          # the DuckDB file dbt transforms in
cd "$DEMO_ROOT/dbt-project"
export DBT_PROFILES_DIR="$PWD"
DBT="$DEMO_ROOT/.venv/bin/dbt"

# The holiday source's location (models/staging/_sources.yml reads HOLIDAYS_PATH)
if [ -s "$DEMO_ROOT/cache/holidays.parquet" ]; then
    export HOLIDAYS_PATH="$DEMO_ROOT/cache/holidays.parquet"
    echo "holidays: local cache (offline)"
else
    echo "holidays: azure://holidaydatacontainer (online)"
fi

[ -d dbt_packages/dbt_utils ] || "$DBT" deps --quiet
start=$(date +%s)
# --full-refresh: on a re-run `dbt seed` would DELETE the old rows, and the mssql extension
# only DELETEs from tables with a primary key. Recreating the seed tables avoids that.
"$DBT" build --full-refresh
echo "dbt build: $(( $(date +%s) - start )) s"
# catalog.json (columns and types) is what section 10's lineage emitter reads
"$DBT" docs generate --quiet
