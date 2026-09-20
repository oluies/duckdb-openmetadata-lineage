#!/usr/bin/env python3
"""Publish an OpenLineage batch to NATS JetStream instead of posting it to OpenMetadata.

    OL_DRY_RUN=1 emit_openlineage.py | publish_openlineage_nats.py

`emit_openlineage.py` builds the events from the dbt artifacts and, in dry-run mode, writes
the batch to stdout instead of POSTing it. This script publishes each of those events to a
JetStream stream, so the dbt run never needs to reach OpenMetadata: OM's OpenLineage
connector consumes the stream on its own schedule.

Why a stream rather than the HTTP endpoint
------------------------------------------
The POST needs OM reachable and authenticated at the moment dbt finishes. A run in a
short-lived pod cannot wait for an OM that is restarting, and a failed POST loses that
run's lineage. JetStream keeps the events until a consumer acknowledges them, so the
producer only needs the broker, and OM can be down for as long as the stream's `max-age`
allows. The transport waits for the stream's acknowledgement, so a publish that returns
means the event is stored.

Requirements
------------
    pip install "openlineage-python[nats] @ git+https://github.com/oluies/OpenLineage@nats-transport#subdirectory=client/python"

The NATS transport is not in an OpenLineage release yet: OpenLineage/OpenLineage#4972.

Configuration
    NATS_URL       nats://localhost:14222
    NATS_SUBJECT   openlineage.events
    NATS_CREDS     path to a .creds file, for a server with operator auth
"""

from __future__ import annotations

import json
import os
import sys

from openlineage.client.transport.nats import NatsConfig, NatsTransport


def log(msg: str) -> None:
    print(f"[nats] {msg}", file=sys.stderr)


def build_transport() -> NatsTransport:
    config = {
        "url": os.getenv("NATS_URL", "nats://localhost:14222"),
        "subject": os.getenv("NATS_SUBJECT", "openlineage.events"),
        "jetstream": True,
        "publishTimeout": 5,
    }
    if creds := os.getenv("NATS_CREDS"):
        config["credsFile"] = creds
    return NatsTransport(NatsConfig.from_dict(config))


def main() -> int:
    batch = json.load(sys.stdin)
    events = batch["events"] if isinstance(batch, dict) else batch
    if not events:
        log("no events on stdin - did emit_openlineage.py run with OL_DRY_RUN=1?")
        return 1

    transport = build_transport()
    try:
        for event in events:
            # The emitter already produced OpenLineage JSON, and the client serializes a
            # dict as it stands; the transport derives Nats-Msg-Id from the payload, so
            # re-running dbt and re-publishing the same events stores them once
            transport.emit(event)
            log(f"{event.get('eventType', '-'):<9} {event['job']['name']}")
    finally:
        transport.close(10)

    log(f"published {len(events)} events to {os.getenv('NATS_SUBJECT', 'openlineage.events')}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
