#!/bin/bash
# 03 Ask OpenMetadata what it now knows: the graph upstream of the holiday fact, with columns.
set -eu
. "$(dirname "$0")/../scripts/env.sh"
cd "$DEMO_ROOT"
OM_HOST="${OM_HOST:-http://localhost:8595}"
TOK=$(scripts/om.sh token)
FQN=mssql_demo.refdata.calendar.fct_holiday_calendar

curl -s -H "Authorization: Bearer $TOK" \
  "$OM_HOST/api/v1/lineage/table/name/$FQN?upstreamDepth=3&downstreamDepth=0" > out/lineage.json

echo "== nodes upstream of $FQN"
jq -r '[.entity] + .nodes | .[] | "  \(.type)  \(.fullyQualifiedName)"' out/lineage.json

echo "== table edges"
jq -r '(([.entity] + .nodes) | map({key: .id, value: .fullyQualifiedName}) | from_entries) as $n
       | .upstreamEdges[] | "  \($n[.fromEntity] | split(".")[-1]) -> \($n[.toEntity] | split(".")[-1])"' out/lineage.json

echo "== column edges"
jq -r '.upstreamEdges[].lineageDetails.columnsLineage[]?
       | "  \(.fromColumns | map(split(".")[-2:] | join(".")) | join(", ")) -> \(.toColumn | split(".")[-2:] | join("."))"' out/lineage.json

echo
echo "UI: $OM_HOST/table/$FQN/lineage  (admin@open-metadata.org / admin)"
