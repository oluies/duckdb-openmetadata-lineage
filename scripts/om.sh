#!/bin/bash
# Local OpenMetadata 2.0.2 for section 10: OpenMetadata's official docker compose (Postgres +
# Elasticsearch + server), with docker-compose.override.yml for its own names and ports.
# Docker, not Apple `container`: OpenMetadata's compose file targets Docker.
#   scripts/om.sh up | down | status | token
set -eu
. "$(dirname "$0")/env.sh"
mkdir -p "$DEMO_ROOT/out"
DIR="$DEMO_ROOT/lineage/om"
OM_VERSION="${OM_VERSION:-2.0.2}"
COMPOSE="$DIR/docker-compose-postgres.yml"
# OpenMetadata's own compose file (Apache-2.0), fetched rather than vendored here.
if [ ! -f "$COMPOSE" ]; then
    curl -sSfL -o "$COMPOSE" \
        "https://github.com/open-metadata/OpenMetadata/releases/download/${OM_VERSION}-release/docker-compose-postgres.yml"
fi
export OM_HOST="${OM_HOST:-http://localhost:8595}"
compose() { docker compose -p duckdemo-om -f "$COMPOSE" -f "$DIR/docker-compose.override.yml" "$@"; }

case "${1:-status}" in
up)
    start=$(date +%s)
    # The server's dependencies (Postgres, Elasticsearch, the migration job) start with it.
    # The Airflow ingestion container is not needed: lineage arrives over HTTP.
    if ! compose up -d --wait openmetadata-server >"$DEMO_ROOT/out/om-up.log" 2>&1; then
        grep -v "is obsolete" "$DEMO_ROOT/out/om-up.log" | tail -15; exit 1
    fi
    printf 'waiting for %s' "$OM_HOST"
    i=0
    until curl -sf "$OM_HOST/api/v1/system/version" >/dev/null; do
        i=$((i + 1)); [ $i -gt 100 ] && { echo " not up after 300 s"; exit 1; }
        printf '.'; sleep 3
    done
    echo " up after $(( $(date +%s) - start )) s: $(curl -s "$OM_HOST/api/v1/system/version")"
    echo "UI: $OM_HOST  (admin@open-metadata.org / admin)"
    ;;
down)
    compose down "${@:2}" 2>&1 | grep -vE "attribute .version. is obsolete" || true
    ;;
status)
    # All three containers, not just the API: OpenMetadata answers on HTTP even when
    # elasticsearch is down, and then search and the Explore pages are empty.
    rc=0
    for name in duckdemo_om_postgresql duckdemo_om_elasticsearch duckdemo_om_server; do
        state=$(docker inspect -f '{{.State.Status}}{{if .State.Health}} ({{.State.Health.Status}}){{end}}' "$name" 2>/dev/null) \
            || state="missing"
        printf '  %-28s %s\n' "$name" "$state"
        case "$state" in running*healthy*|running) ;; *) rc=1 ;; esac
    done
    curl -sf "$OM_HOST/api/v1/system/version" && echo || { echo "  API not answering at $OM_HOST"; rc=1; }
    [ "$rc" = 0 ] || { echo "not ready: scripts/om.sh up"; exit 1; }
    ;;
token)
    # Admin session token for local runs. The password must be sent base64-encoded:
    # a plain one gets a 401 with no hint why.
    curl -sf -X POST "$OM_HOST/api/v1/users/login" -H 'Content-Type: application/json' \
        -d "{\"email\":\"admin@open-metadata.org\",\"password\":\"$(printf admin | base64)\"}" \
        | sed -E 's/.*"accessToken":"([^"]+)".*/\1/'
    ;;
*)
    echo "usage: $0 up|down [-v]|status|token"; exit 2 ;;
esac
