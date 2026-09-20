#!/bin/bash
# 01 One-time OpenMetadata setup (run after scripts/om.sh up and the section 09 dbt build):
#   1. a bot for the emitter, with IngestionBotRole + LineageBotRole
#   2. Settings -> OpenLineage -> namespaceToServiceMapping for the SQL Server namespace
#   3. OM's own SQL Server connector ingests refdata.calendar (as in production, where the
#      SQL Server services are ingested by OM and the emitter only adds the DuckDB layer)
set -eu
. "$(dirname "$0")/../scripts/env.sh"
cd "$DEMO_ROOT"
OM_HOST="${OM_HOST:-http://localhost:8595}"
BOT=duckdemo-lineage-bot
SERVICE=mssql_demo
mkdir -p out

ADMIN=$(scripts/om.sh token)
auth=(-H "Authorization: Bearer $ADMIN" -H 'Content-Type: application/json')
api() { curl -s "${auth[@]}" "$@"; }

echo "== 1. bot $BOT"
role_ref() { api "$OM_HOST/api/v1/roles/name/$1" | jq -c '{id, type: "role"}'; }
if ! api "$OM_HOST/api/v1/users/name/$BOT" | jq -e .id >/dev/null; then
    # the field is authenticationMechanism (authMechanism gives a 400); Unlimited = no exp claim
    api -X POST "$OM_HOST/api/v1/users" -d "{\"name\":\"$BOT\",\"email\":\"$BOT@example.com\",\"isBot\":true,
        \"authenticationMechanism\":{\"authType\":\"JWT\",\"config\":{\"JWTTokenExpiry\":\"Unlimited\"}}}" >/dev/null
    api -X POST "$OM_HOST/api/v1/bots" -d "{\"name\":\"$BOT\",\"botUser\":\"$BOT\"}" >/dev/null
fi
BOTID=$(api "$OM_HOST/api/v1/users/name/$BOT" | jq -r .id)
# A new bot has no roles: it authenticates, then gets 403 [EditLineage]. LineageBotRole
# alone is not enough either: creating the DuckDB entities needs Create (IngestionBotRole).
# (JSON Patch needs its own Content-Type, so plain curl rather than api())
curl -s -X PATCH -H "Authorization: Bearer $ADMIN" -H 'Content-Type: application/json-patch+json' "$OM_HOST/api/v1/users/$BOTID" \
    -d "[{\"op\":\"add\",\"path\":\"/roles\",\"value\":[$(role_ref IngestionBotRole),$(role_ref LineageBotRole)]}]" \
    >/dev/null
api "$OM_HOST/api/v1/users/name/$BOT?fields=roles" | jq -c '{bot: .name, roles: [.roles[].name]}'
api "$OM_HOST/api/v1/users/auth-mechanism/$BOTID" | jq -r .config.JWTToken > out/om-lineage-bot.jwt
echo "   token in out/om-lineage-bot.jwt ($(wc -c < out/om-lineage-bot.jwt | tr -d ' ') bytes)"

echo "== 2. namespaceToServiceMapping"
api "$OM_HOST/api/v1/system/settings/openLineageSettings" \
  | jq --arg ns "mssql://localhost:$MSSQL_PORT" --arg svc "$SERVICE" \
       '{config_type, config_value: (.config_value + {namespaceToServiceMapping: {($ns): $svc}})}' \
  | api -X PUT "$OM_HOST/api/v1/system/settings" -d @- >/dev/null
api "$OM_HOST/api/v1/system/settings/openLineageSettings" | jq -c .config_value

echo "== 3. OM's SQL Server connector: $SERVICE <- localhost:$MSSQL_PORT refdata.calendar"
INGEST_TOKEN=$(api "$OM_HOST/api/v1/users/auth-mechanism/$(api "$OM_HOST/api/v1/users/name/ingestion-bot" | jq -r .id)" | jq -r .config.JWTToken)
cat > out/om-mssql-ingest.yaml <<YAML
source:
  type: mssql
  serviceName: $SERVICE
  serviceConnection:
    config:
      type: Mssql
      scheme: mssql+pytds
      username: sa
      password: "$MSSQL_SA_PASSWORD"
      hostPort: localhost:$MSSQL_PORT
      database: refdata
  sourceConfig:
    config:
      type: DatabaseMetadata
      schemaFilterPattern:
        includes: ["^calendar\$"]
sink:
  type: metadata-rest
  config: {}
workflowConfig:
  loggerLevel: WARN
  openMetadataServerConfig:
    hostPort: $OM_HOST/api
    authProvider: openmetadata
    securityConfig:
      jwtToken: "$INGEST_TOKEN"
YAML
uvx --python 3.11 --from 'openmetadata-ingestion[mssql]~=2.0.2.0' metadata ingest -c out/om-mssql-ingest.yaml \
    > out/om-mssql-ingest.log 2>&1 || { tail -30 out/om-mssql-ingest.log; exit 1; }
api "$OM_HOST/api/v1/tables?service=$SERVICE&limit=50" | jq -r '"   ingested: " + (.data | map(.name) | sort | join(", "))'
