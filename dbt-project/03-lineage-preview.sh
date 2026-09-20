#!/bin/bash
# 03 What dbt knows about the lineage, the input for section 10: manifest.json has the graph
# and the compiled SQL, catalog.json the columns. Read with DuckDB, of course.
set -eu
. "$(dirname "$0")/../scripts/env.sh"
cd "$DEMO_ROOT/dbt-project"
duckdb -no-init -c "
WITH nodes AS (
    SELECT unnest(json_keys(nodes)) AS id, nodes FROM read_json_objects('target/manifest.json') t(j),
         LATERAL (SELECT j -> 'nodes' AS nodes)
)
SELECT split_part(id, '.', 3) AS node,
       nodes -> id ->> 'resource_type' AS type,
       (nodes -> id ->> 'database') || '.' || (nodes -> id ->> 'schema') || '.' || (nodes -> id ->> 'name') AS relation,
       list_transform(CAST(nodes -> id -> 'depends_on' -> 'nodes' AS VARCHAR[]), lambda d: split_part(d, '.', 3)) AS depends_on
FROM nodes
WHERE nodes -> id ->> 'resource_type' IN ('model', 'seed')
ORDER BY type DESC, node;"
