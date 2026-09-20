#!/bin/bash
# SQL Server 2022 for sections 08/09, in Apple `container` (not Docker).
#   scripts/mssql.sh up | down | status | sql "<T-SQL>"
# The image is amd64-only; on Apple silicon it runs under Rosetta (--arch amd64 --rosetta).
# Connect via localhost:$MSSQL_PORT. The container IP that `container ls` prints is not
# reachable from macOS.
set -eu
. "$(dirname "$0")/env.sh"
NAME=duckdemo-mssql
IMAGE=mcr.microsoft.com/mssql/server:2022-latest
SQLCMD=/opt/mssql-tools18/bin/sqlcmd

sql() {  # run T-SQL inside the container
    container exec "$NAME" "$SQLCMD" -S localhost -U sa -P "$MSSQL_SA_PASSWORD" -C -b "$@"
}

case "${1:-status}" in
up)
    container system status >/dev/null 2>&1 || container system start
    if container ls --format json | grep -q "\"$NAME\""; then
        echo "$NAME already running on localhost:$MSSQL_PORT"; exit 0
    fi
    container rm "$NAME" >/dev/null 2>&1 || true
    start=$(date +%s)
    container run -d --name "$NAME" --arch amd64 --rosetta -m 4G -c 4 \
        -p "$MSSQL_PORT:1433" \
        -e ACCEPT_EULA=Y -e "MSSQL_SA_PASSWORD=$MSSQL_SA_PASSWORD" -e MSSQL_PID=Developer \
        "$IMAGE" >/dev/null 2>&1
    # -d returns when the VM is up, not when SQL Server accepts logins: ask SQL Server
    printf 'waiting for SQL Server'
    i=0
    until sql -Q "SELECT 1" >/dev/null 2>&1; do
        i=$((i + 1)); [ $i -gt 120 ] && { echo; echo "not ready after 240 s"; container logs "$NAME" | tail -20; exit 1; }
        printf '.'; sleep 2
    done
    echo " ready after $(( $(date +%s) - start )) s"
    container cp "$DEMO_ROOT/scripts/mssql-init/01-crm.sql" "$NAME:/tmp/01-crm.sql" >/dev/null
    sql -i /tmp/01-crm.sql -h -1 -W | grep -v "^Changed database context" | grep -v '^$'
    ;;
down)
    container stop "$NAME" >/dev/null 2>&1 || true
    container rm "$NAME" >/dev/null 2>&1 || true
    echo "$NAME removed"
    ;;
status)
    if sql -Q "SET NOCOUNT ON; SELECT @@SERVERNAME + ' ' + CAST(SERVERPROPERTY('ProductVersion') AS nvarchar(20))" -h -1 -W 2>/dev/null; then
        echo "listening on localhost:$MSSQL_PORT"
    else
        echo "$NAME is not running (scripts/mssql.sh up)"; exit 1
    fi
    ;;
sql)
    shift; sql -Q "$*" -W
    ;;
*)
    echo "usage: $0 up|down|status|sql <T-SQL>"; exit 2 ;;
esac
