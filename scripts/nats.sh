#!/bin/bash
# NATS with JetStream for section 10, in Apple `container` (like scripts/mssql.sh).
#   scripts/nats.sh up | down | status | stream | events
# The stream keeps the OpenLineage events that lineage/05-nats-emit.sh publishes, until
# OpenMetadata's OpenLineage connector consumes them. Connect via localhost:$NATS_PORT; the
# container IP that `container ls` prints is not reachable from macOS.
set -eu
. "$(dirname "$0")/env.sh"
NAME=duckdemo-nats
IMAGE=docker.io/library/nats:2
STREAM="${NATS_STREAM:-OPENLINEAGE}"
SUBJECTS="${NATS_SUBJECT:-openlineage.>}"

py() { "$DEMO_ROOT/.venv/bin/python" "$@"; }

jetstream() {  # JetStream admin over the wire; no `nats` CLI needed
    py - "$@" <<'PY'
import asyncio, json, sys
import nats
from nats.js.api import StreamConfig

async def main() -> None:
    action, url, stream, subjects = sys.argv[1:5]
    nc = await nats.connect(url)
    js = nc.jetstream()
    try:
        if action == "create":
            # max_age keeps a forgotten demo stream from growing forever; the events are
            # replayable from dbt anyway
            await js.add_stream(StreamConfig(name=stream, subjects=subjects.split(","),
                                             max_age=7 * 24 * 3600, duplicate_window=120))
            print(f"stream {stream} ready for {subjects}")
        elif action == "info":
            info = await js.stream_info(stream)
            print(json.dumps({"messages": info.state.messages, "bytes": info.state.bytes,
                              "consumers": info.state.consumer_count}, indent=2))
        elif action == "events":
            info = await js.stream_info(stream)
            for seq in range(1, info.state.last_seq + 1):
                msg = await js.get_msg(stream, seq)
                event = json.loads(msg.data)
                print(f"{seq:>3}  {event.get('eventType', '-'):<9} {event['job']['name']}"
                      f"  msg-id={(msg.headers or {}).get('Nats-Msg-Id', '-')}")
    finally:
        await nc.close()

asyncio.run(main())
PY
}

URL="nats://localhost:$NATS_PORT"

case "${1:-status}" in
up)
    container system status >/dev/null 2>&1 || container system start
    if container ls --format json | grep -q "\"$NAME\""; then
        echo "$NAME already running on $URL"
    else
        container rm "$NAME" >/dev/null 2>&1 || true
        container run -d --name "$NAME" -p "$NATS_PORT:4222" "$IMAGE" -js >/dev/null 2>&1
        printf 'waiting for NATS'
        i=0
        until nc -z localhost "$NATS_PORT" 2>/dev/null; do
            i=$((i + 1)); [ $i -gt 60 ] && { echo; echo "not ready after 60 s"; container logs "$NAME" | tail -20; exit 1; }
            printf '.'; sleep 1
        done
        echo " ready on $URL"
    fi
    jetstream create "$URL" "$STREAM" "$SUBJECTS"
    ;;
down)
    container stop "$NAME" >/dev/null 2>&1 || true
    container rm "$NAME" >/dev/null 2>&1 || true
    echo "$NAME stopped"
    ;;
status)
    container ls --format json | grep -q "\"$NAME\"" && echo "$NAME running on $URL" || echo "$NAME not running"
    ;;
stream)
    jetstream info "$URL" "$STREAM" ""
    ;;
events)
    jetstream events "$URL" "$STREAM" ""
    ;;
*)
    echo "usage: $0 up|down|status|stream|events" >&2
    exit 2
    ;;
esac
