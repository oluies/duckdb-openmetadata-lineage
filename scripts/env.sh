# shellcheck shell=bash
# Sourced by every demo script. bash 3.2 compatible (macOS /bin/bash).
#
# Corporate TLS inspection: behind a proxy that re-signs HTTPS, uv, requests and
# DuckDB httpfs all reject the proxy's certificate. Point DEMO_CA_BUNDLE at a PEM
# that contains the proxy CA (plus the normal roots) and everything below follows.
# Off the corporate network, leave it unset.
if [ -n "${DEMO_CA_BUNDLE:-}" ]; then
    if [ ! -f "$DEMO_CA_BUNDLE" ]; then
        echo "DEMO_CA_BUNDLE=$DEMO_CA_BUNDLE does not exist" >&2
        exit 1
    fi
    export SSL_CERT_FILE="$DEMO_CA_BUNDLE"
    export REQUESTS_CA_BUNDLE="$DEMO_CA_BUNDLE"
    export CURL_CA_BUNDLE="$DEMO_CA_BUNDLE"
    export UV_NATIVE_TLS=1
fi

DEMO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
export DEMO_ROOT
export DUCKDB_VERSION_PINNED="v1.5.5"

# SQL Server in Apple `container` (section 8/9). Port 11433 so it never collides
# with a SQL Server that already owns 1433 on the laptop.
export MSSQL_PORT="${MSSQL_PORT:-11433}"

# NATS with JetStream in Apple `container` (section 10, the OpenLineage-over-NATS path).
# Port 14222 so it never collides with a NATS that already owns 4222 on the laptop.
export NATS_PORT="${NATS_PORT:-14222}"
export MSSQL_SA_PASSWORD="${MSSQL_SA_PASSWORD:-DuckDemo!2026}"
export MSSQL_CONN="Server=localhost,${MSSQL_PORT};Database=demo;User Id=sa;Password=${MSSQL_SA_PASSWORD};TrustServerCertificate=true"  # trufflehog:ignore (demo container)
