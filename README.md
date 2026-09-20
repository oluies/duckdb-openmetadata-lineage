# Column lineage from dbt-duckdb to OpenMetadata

The code behind the post **[Column lineage from dbt-duckdb to OpenMetadata with OpenLineage](https://oluies.github.io/duckdb-openmetadata-lineage/)**.

A dbt-duckdb project keeps its staging layer in a DuckDB file and writes its marts to SQL Server.
OpenMetadata's dbt integration cannot follow that: DuckDB is not an OM connector, and the file is
gone when the run ends, so the graph is cut in the middle. This repository posts OpenLineage events
to OpenMetadata instead, and derives column-level lineage from each model's compiled SQL with
sqlglot.

![Column lineage in OpenMetadata](docs/img/om-column-lineage.png)

## What is here

| Path | |
|---|---|
| `lineage/emit_openlineage.py` | The emitter: builds OpenLineage events from `target/manifest.json`, `catalog.json` and `run_results.json`, creates the entities OM needs, derives column lineage with sqlglot, and POSTs to `/api/v1/openlineage/lineage/batch`. |
| `lineage/01-om-setup.sh` | One-time OM setup: a bot with `IngestionBotRole` + `LineageBotRole`, the `namespaceToServiceMapping`, and OM's own SQL Server connector run from the `metadata` CLI. |
| `lineage/02-emit.sh` · `03-verify.sh` | Emit (dry run, then POST) and read the graph back from OM's API. |
| `lineage/04-nats-emit.sh` · `publish_openlineage_nats.py` | The same events published to NATS JetStream instead of POSTed. |
| `dbt-project/` | The dbt-duckdb project: a Kimball date and holiday calendar. Staging in DuckDB, marts in SQL Server. Vendored from [oluies/dbt-duckdb-trading-calendar](https://github.com/oluies/dbt-duckdb-trading-calendar). |
| `scripts/` | SQL Server 2022 and NATS in Apple `container`; OpenMetadata in Docker. |

## Run it

Needs macOS with [Apple `container`](https://github.com/apple/container) (or Docker, see
`scripts/mssql.sh`), Docker for OpenMetadata, [uv](https://docs.astral.sh/uv/), and the
DuckDB 1.5.5 CLI.

```bash
make setup        # venv, pinned packages, DuckDB extensions, cached holiday Parquet
make mssql-up     # SQL Server 2022 on localhost:11433, seeded (~6 s with the image cached)
make om-up        # OpenMetadata 2.0.2 on http://localhost:8595 (~60 s cold)
make dbt          # dbt build: staging in DuckDB, marts in SQL Server
make lineage      # OM setup once, then emit the events
make verify       # the graph and the column edges, from OM's API
```

Then open `http://localhost:8595` (admin@open-metadata.org / admin) → Explore →
`fct_holiday_calendar` → **Lineage**, expand the columns, and click one.

## Notes

- Pinned: DuckDB 1.5.5, dbt-core 1.11.11, dbt-duckdb 1.10.1, OpenMetadata 2.0.2, the community
  `mssql` extension, SQL Server 2022.
- OpenMetadata's own compose file is downloaded by `scripts/om.sh` rather than vendored here;
  `lineage/om/docker-compose.override.yml` gives it its own container names, ports and subnet so it
  can run next to another OpenMetadata.
- The post describes a production loader generically. No organisation, hostname or database name
  from it appears in this repository.

## Authors

- **Vladimir Gribanov** — [github.com/VGSML](https://github.com/VGSML) · [LinkedIn](https://www.linkedin.com/in/vladimirgribanov/)
- **Örjan Lundberg** — [github.com/oluies](https://github.com/oluies) · [LinkedIn](https://www.linkedin.com/in/orjanlundberg/)

Copyright © 2026 Örjan Lundberg. All rights reserved. No licence is granted; see GitHub's
[terms on public repositories](https://docs.github.com/site-policy/github-terms/github-terms-of-service#5-license-grant-to-other-users).
