#!/usr/bin/env python3
"""Emit OpenLineage events for the last dbt run straight to OpenMetadata.

Adapted for the DuckDB talk from a production loader (a dbt-duckdb project that runs in a
Kubernetes pod, reads SQL Server and files, keeps its staging layer in a pod-local DuckDB
file, and writes marts to SQL Server). Everything site-specific is configuration here; the
logic is unchanged apart from skipping dbt ephemeral models.

Why this exists, and why not the dbt artifacts
----------------------------------------------
The OM dbt agent matches every manifest node to a table that must ALREADY be ingested in
OM, and skips what it cannot resolve. The staging nodes live in a DuckDB catalog that is
created by the run and destroyed with it, and DuckDB is not an OM connector at all ("Not
supported by OpenMetadata" in their setup.py). When every mart depends only on DuckDB nodes
and every source feeds only DuckDB nodes, the dbt-agent graph is severed exactly in the
middle: the marts show no upstream and the sources show no downstream.

OM's OpenLineage HTTP endpoint does not have that restriction:

    POST {OM_HOST}/api/v1/openlineage/lineage/batch

Settings for it live under Settings -> OpenLineage in OM.

Why it bootstraps entities first
--------------------------------
`autoCreateEntities` (Settings -> OpenLineage) cannot be relied on. Measured
against a live OM 1.12.6, and identical by code inspection in 2.0.1/2.0.2:

  * it never creates the service -> database -> schema chain, only a table
    inside an ALREADY EXISTING schema ("Cannot create table, schema not found:
    staging");
  * the auto-create itself is broken. OpenLineageEntityResolver calls
    tableRepository.create(null, newTable) / pipelineRepository.create(null,
    newPipeline), and EntityRepository.createInternal(entity, null, null) sets
    updatedBy only when it is passed non-null and never sets updatedAt at all.
    The insert then dies on `null value in column "updatedat" of relation
    "table_entity" violates not-null constraint`, and the whole event is
    skipped. `withUpdatedAt` appears zero times in that resolver in both
    1.12.6 and 2.0.2.

So this script creates what it is about to reference — idempotently; anything OM
already has comes back 409 and is left alone — and only then posts the events.
The SQL Server services are ingested by OM's own connector, so in practice only the
DuckDB layer and the pipeline service get created. --no-bootstrap skips
it once you are confident the entities are there.

Dataset identity
----------------
OM resolves a dataset by splitting the NAME on "." and taking the last three
segments as database.schema.table, then finding the service via
`namespaceToServiceMapping` (namespace -> OM Database Service name); failing
that it falls back to an FQN-suffix search. See OpenLineageEntityResolver
.resolveCandidateFqn in the OM source. So the namespace carries the *server* and
the name carries the *catalog path* — which is exactly the split our three
catalogs across two servers need, and exactly what `dbt-ol` cannot express
(it derives one namespace from the adapter, so attached SQL Server catalogs would be
attributed to the DuckDB service).

Map each SQL Server namespace in OM once (Settings -> OpenLineage ->
namespaceToServiceMapping); leave the DuckDB namespace unmapped, its service is created:

    mssql://<host>:<port>   -> the OM database service that ingests that server
    duckdb://<name>         -> unmapped; created by this script

Authentication
--------------
Every request carries `Authorization: Bearer <jwt>`. The token is resolved from
the first of these that is configured, so the pod and a laptop can use the same
script with different credentials:

  1. OM_JWT_TOKEN        the token itself
  2. OM_JWT_TOKEN_FILE   read the token from a file — prefer this in Kubernetes:
                         a mounted Secret keeps the JWT out of the env, where it
                         would otherwise show up in `ps` and in pod descriptions
  3. OM_USER + OM_PASSWORD
                         log in via POST /api/v1/users/login and use the returned
                         accessToken. OM expects the password BASE64-ENCODED in
                         that call — a plain password returns 401 with no hint.
                         Session tokens are short-lived; this is for local runs,
                         not the DAG.
  4. OM_CLIENT_ID + OM_CLIENT_SECRET + OM_OAUTH_TOKEN_URL
                         OAuth2 client_credentials, for an OM behind SSO/OIDC.

For the DAG use a BOT token (OM Settings -> Bots -> ingestion-bot, or a bot of
its own), not a human login: bot JWTs can be issued long-lived, user sessions
expire within the hour. A token that has expired shows up as a 401 here, which
is reported and returns 1 — the caller should keep that non-fatal, so the load still
succeeds and only the lineage is missing.

Env:
  OM_HOST          e.g. http://openmetadata.dataplattform.svc:8585  (required)
  OM_JWT_TOKEN / OM_JWT_TOKEN_FILE / OM_USER + OM_PASSWORD /
  OM_CLIENT_ID + OM_CLIENT_SECRET + OM_OAUTH_TOKEN_URL   see above (one required)
  OM_OAUTH_SCOPE                  optional scope for the client_credentials grant
  OM_CA_BUNDLE                    CA bundle for an https OM behind a TLS-inspecting proxy
  OM_INSECURE=1                   skip TLS verification (local self-signed only)
  OL_SQLSERVER_CATALOGS           which dbt databases are attached SQL Servers, as
                                  "catalog=host:port,..." e.g. "refdata=localhost:11433"
  OM_SQLSERVER_SERVICES           OM service per server, "host:port=service,..."
                                  (default: the host name up to the first dot)
  OL_DUCKDB_NAMESPACE             default duckdb://dbt
  OL_JOB_NAMESPACE                default dbt — the job side, not datasets
  OM_SERVICE_DUCKDB               OM database service for the local DuckDB catalog
                                  (default duckdb_dbt, created by this script)
  DBT_TARGET_DIR                  dbt target directory (default ./target)
  OM_PIPELINE_SERVICE             pipeline service for the dbt jobs (default openlineage;
                                  must match defaultPipelineService in OM settings)
  OL_DRY_RUN=1                    print the batch instead of POSTing it
"""

from __future__ import annotations

import base64
import functools
import json
import os
import re
import ssl
import sys
import urllib.error
import urllib.parse
import urllib.request
import uuid
from collections.abc import Callable
from datetime import UTC, datetime

PRODUCER = "urn:duckdb-demo:emit_openlineage"
SCHEMA_URL = (
    "https://openlineage.io/spec/1-0-5/OpenLineage.json#/definitions/RunEvent"
)
TARGET = os.getenv("DBT_TARGET_DIR", "target")


def log(msg: str) -> None:
    print(f"emit_openlineage: {msg}", file=sys.stderr, flush=True)


def _pairs(env: str) -> dict[str, str]:
    """"a=b,c=d" from an env var -> {"a": "b", "c": "d"}. Pure apart from the read."""
    return dict(
        (k.strip(), v.strip())
        for k, _, v in (item.partition("=") for item in os.getenv(env, "").split(","))
        if k.strip() and v.strip()
    )


def sqlserver_for(database: str) -> str | None:
    """host:port of the SQL Server a dbt database (an ATTACH alias) lives on, or None."""
    catalogs = {k.lower(): v for k, v in _pairs("OL_SQLSERVER_CATALOGS").items()}
    return catalogs.get(database.lower())


def namespace_for(database: str) -> str:
    """Dataset namespace = the SERVER the catalog physically lives on.

    Attached SQL Server catalogs get mssql://host:port; everything else is the local
    DuckDB file. Keeping these apart is the whole point of emitting our own events.
    """
    server = sqlserver_for(database)
    if server:
        return f"mssql://{server}"
    return os.getenv("OL_DUCKDB_NAMESPACE", "duckdb://dbt")


# Filled in by bootstrap() when a configured service name matches an existing
# one apart from capitalisation. OM entity names ARE case-sensitive — a lookup
# for "MSSQL" 404s when the service is "mssql", for services and table FQNs
# alike — so the exact spelling has to be used, not merely a close one.
_SERVICE_OVERRIDE: dict[str, str] = {}


def default_service_name(server: str) -> str:
    """Guess the OM database service name from the SQL Server hostname.

    The host without the domain, case preserved: Sql01.corp.example -> "Sql01".
    Only a default: OM_SQLSERVER_SERVICES overrides it, and bootstrap() corrects a
    case mismatch against the services OM actually has.
    """
    return server.split(".", 1)[0].strip()


def service_for(database: str) -> tuple[str, bool]:
    """(OM database service name, whether we may create it).

    The SQL Server services are ingested by OM's own connectors and must already
    exist — creating a stub for them would fight the real ingestion, so the flag
    is False and bootstrap only reports when one is missing. The DuckDB catalog
    has no OM connector at all, so we own it.
    """
    server = sqlserver_for(database)
    if server:
        name = _pairs("OM_SQLSERVER_SERVICES").get(server) or default_service_name(server.split(":")[0])
        return _SERVICE_OVERRIDE.get(name, name), False
    return os.getenv("OM_SERVICE_DUCKDB", "duckdb_dbt"), True


# A dbt/DuckDB type string: a base name, optionally (precision[, scale]) or a
# trailing [] for a list. Parsed once here rather than in three places.
_TYPE_RE = re.compile(r"^\s*(?P<base>[A-Za-z0-9_ ]+?)\s*(?:\(\s*(?P<p>\d+)\s*(?:,\s*(?P<s>\d+)\s*)?\))?\s*$")


# OM requires a dataLength on char/varchar (it 400s without one), but DuckDB's
# VARCHAR is unbounded and reports no length. Say so explicitly rather than
# guessing a number: VARCHAR(MAX), with 2^31-1 as the length — the same
# convention SQL Server's own varchar(max) uses.
VARCHAR_MAX_LENGTH = 2147483647


# Plain name -> OM dataType. A lookup table, not a match: these are pure aliases
# with no structure to destructure, and `match` earns its keep just below where
# there IS structure (array suffix, parameterised types, where precision goes).
_TYPE_ALIASES = {
    "VARCHAR": "VARCHAR", "STRING": "VARCHAR", "TEXT": "VARCHAR",
    "CHAR": "CHAR", "BPCHAR": "CHAR",
    "BOOLEAN": "BOOLEAN", "BOOL": "BOOLEAN", "LOGICAL": "BOOLEAN",
    "TINYINT": "TINYINT", "INT1": "TINYINT",
    "SMALLINT": "SMALLINT", "INT2": "SMALLINT", "SHORT": "SMALLINT",
    "INTEGER": "INT", "INT": "INT", "INT4": "INT", "SIGNED": "INT",
    "BIGINT": "BIGINT", "INT8": "BIGINT", "LONG": "BIGINT",
    "HUGEINT": "LARGEINT", "UHUGEINT": "LARGEINT",
    "UTINYINT": "UINT", "USMALLINT": "UINT", "UINTEGER": "UINT", "UBIGINT": "UINT",
    "DECIMAL": "DECIMAL", "NUMERIC": "DECIMAL",
    "FLOAT": "FLOAT", "REAL": "FLOAT", "FLOAT4": "FLOAT",
    "DOUBLE": "DOUBLE", "FLOAT8": "DOUBLE",
    "DATE": "DATE", "TIME": "TIME",
    "TIMESTAMP": "TIMESTAMP", "DATETIME": "TIMESTAMP",
    "TIMESTAMPTZ": "TIMESTAMPZ", "TIMESTAMP WITH TIME ZONE": "TIMESTAMPZ",
    "INTERVAL": "INTERVAL",
    "BLOB": "BLOB", "BYTEA": "BLOB", "VARBINARY": "BLOB", "BINARY": "BLOB",
    "UUID": "UUID", "JSON": "JSON", "BIT": "BIT",
}


def om_column_type(raw: str) -> dict:
    """Map a dbt/DuckDB type string onto OM's dataType enum. Pure.

    `dataTypeDisplay` carries the original string, so a type that falls through
    to UNKNOWN still shows its real DuckDB spelling in the UI instead of becoming
    a lie. The one exception is the unbounded string family below, which is
    displayed as VARCHAR(MAX) — there the original spelling (VARCHAR / CHAR /
    TEXT / STRING) is deliberately normalised away, because all four mean the
    same thing and none of them states the absence of a limit.
    """
    text = (raw or "").strip()
    match text.upper():
        case s if s.endswith("[]"):
            inner = om_column_type(text[:-2])
            return {"dataType": "ARRAY", "arrayDataType": inner["dataType"], "dataTypeDisplay": text}
        case s if s.startswith("STRUCT") or s.startswith("ROW"):
            return {"dataType": "STRUCT", "dataTypeDisplay": text}
        case s if s.startswith("MAP"):
            return {"dataType": "MAP", "dataTypeDisplay": text}
        case s if s.startswith(("LIST", "ARRAY")):
            return {"dataType": "ARRAY", "dataTypeDisplay": text}

    parsed = _TYPE_RE.match(text)
    base = (parsed.group("base") if parsed else text).strip().upper()
    precision = int(parsed.group("p")) if parsed and parsed.group("p") else None
    scale = int(parsed.group("s")) if parsed and parsed.group("s") else None
    data_type = _TYPE_ALIASES.get(base, "UNKNOWN")

    # Where the parenthesised number belongs differs by family, and OM validates
    # them separately: it rejects a scale on a VARCHAR, and REJECTS char/varchar/
    # binary/varbinary that carry no dataLength at all ("dataLength must not be
    # null", HTTP 400). A genuine VARCHAR(50) keeps its real length; the
    # unbounded case is handled in its own arm below.
    match (data_type, precision):
        case ("DECIMAL", int()):
            extra = {"precision": precision, "scale": scale or 0}
        case ("VARCHAR" | "CHAR" | "BLOB", int()):
            extra = {"dataLength": precision}
        case ("VARCHAR" | "CHAR", None):
            # DuckDB's VARCHAR/CHAR/TEXT/STRING are one unbounded type with no
            # length to report. dataTypeDisplay is overridden here (rather than
            # keeping the raw "VARCHAR") so the UI says MAX instead of implying
            # some unstated limit.
            data_type = "VARCHAR"
            extra = {"dataLength": VARCHAR_MAX_LENGTH, "dataTypeDisplay": "VARCHAR(MAX)"}
        case _:
            extra = {}

    return {"dataType": data_type, "dataTypeDisplay": text or "UNKNOWN"} | extra


def om_columns(unique_id: str, entity: dict, catalog: dict) -> list[dict]:
    """Columns for one node, best source first. Pure.

    catalog.json is authoritative: `dbt docs generate` runs where the DuckDB
    tables physically live and introspects them, so it carries real names and
    types. It does NOT cover the attached SQL Server catalogs — but those are
    ingested by OM's own connectors and we never overwrite them, so the gap
    does not matter. Falling back to the manifest's declared columns (names
    only, no types) and finally to a single placeholder keeps a table creatable
    either way.
    """
    catalogued = (
        (catalog.get("nodes", {}).get(unique_id) or catalog.get("sources", {}).get(unique_id) or {})
        .get("columns", {})
        .values()
    )
    if columns := [
        {"name": c["name"], "ordinalPosition": c.get("index")} | om_column_type(c.get("type", ""))
        | ({"description": c["comment"]} if c.get("comment") else {})
        for c in catalogued
    ]:
        return columns
    if declared := [
        {"name": name, "dataType": "UNKNOWN"} | ({"description": d["description"]} if d.get("description") else {})
        for name, d in (entity.get("columns") or {}).items()
    ]:
        return declared
    return [{"name": "_unknown", "dataType": "UNKNOWN"}]


# sqlglot powers the column-level lineage below. It is the only third-party
# import in this script; everything else is stdlib, which is why the loader image
# needs no extra packaging. Guarded so a missing sqlglot degrades to table-level
# lineage instead of failing the emit.
try:
    from sqlglot.lineage import lineage as _sqlglot_lineage
except ImportError:  # pragma: no cover - exercised by the absence of the package
    _sqlglot_lineage = None


@functools.cache
def om_table_columns(fqn: str) -> tuple[str, ...]:
    """Column names OM already holds for a table, or (). Cached per FQN.

    This is how the attached SQL Server tables contribute to column lineage:
    `dbt docs generate` cannot introspect them, but OM's own connectors ingested
    them, so OM is the authority. Returns a tuple so lru_cache can hold it.
    """
    host = os.getenv("OM_HOST", "").rstrip("/")
    token, _ = bearer(host) if host else (None, "")
    if not (host and token):
        return ()
    status, body = http_json(
        f"{host}/api/v1/tables/name/{urllib.parse.quote(fqn)}?fields=columns",
        headers={"Authorization": f"Bearer {token}"},
    )
    if status != 200:
        return ()
    try:
        return tuple(c["name"] for c in json.loads(body).get("columns", []))
    except (ValueError, KeyError, TypeError):
        return ()


# Object-storage URIs read straight out of the compiled SQL. Only s3:// and the
# other schemes OpenMetadata treats as storage are worth extracting: OM's
# OpenLineageDatasetNameNormalizer.isStorageNamespace accepts gs://, s3://,
# s3a://, abfss://, abfs://, wasbs:// and adl:// — a file:// or https:// dataset
# resolves to nothing, so emitting one would add a dangling node.
_STORAGE_URI_RE = re.compile(r"\b((?:s3a?|gs|abfss?|wasbs|adl)://[^\s'\"),]+)")


def storage_inputs(node: dict) -> list[dict]:
    """OL datasets for every object-storage URI the model's SQL reads. Pure.

    The URI is split into namespace (the bucket) and name (the object key),
    which is the convention OM reassembles: fullPath = namespace + "/" + name.
    """
    uris = sorted(set(_STORAGE_URI_RE.findall(node.get("compiled_code") or "")))
    datasets = []
    for uri in uris:
        scheme, _, rest = uri.partition("://")
        bucket, _, key = rest.partition("/")
        if key:
            datasets.append({"namespace": f"{scheme}://{bucket}", "name": key})
    return datasets


def container_paths_in_om(host: str, auth: dict) -> set[str]:
    """Every fullPath OpenMetadata already has a Container for.

    Resolution matches on fullPath alone, and OM returns the first match, so a
    duplicate is worse than useless. Listing once is cheaper than a lookup per
    folder and tolerates the endpoint being unavailable (an empty set just means
    we fall through to creating, which is the pre-existing behaviour).
    """
    status, body = http_json(f"{host}/api/v1/containers?limit=1000&fields=", headers=auth)
    if status != 200:
        return set()
    try:
        return {
            c["fullPath"] for c in json.loads(body).get("data", []) if c.get("fullPath")
        }
    except (ValueError, TypeError):
        return set()


def container_folders(manifest: dict) -> dict[str, set[str]]:
    """bucket-namespace -> folder paths that need a Container in OM. Pure.

    Registered at the FOLDER, never per file. OM first tries an exact fullPath
    match and then strips the last path segment, so one container for
    s3://bucket/src/vendor matches whatever the object is called — which
    matters when a vendor renames its file on every publication, and per-file
    containers would accumulate forever.
    """
    folders: dict[str, set[str]] = {}
    for node in manifest["nodes"].values():
        for ds in storage_inputs(node):
            folder = ds["name"].rsplit("/", 1)[0] if "/" in ds["name"] else ""
            if folder:
                folders.setdefault(ds["namespace"], set()).add(folder)
    return folders


def is_ephemeral(entity: dict) -> bool:
    """A dbt ephemeral model is a CTE inlined into its children, not a table. Pure."""
    return (entity.get("config") or {}).get("materialized") == "ephemeral"


def relation_index(manifest: dict) -> dict[tuple[str, str, str], tuple[str, dict]]:
    """(catalog, schema, table) lowercased -> (unique_id, entity). Pure.

    sqlglot reports the relations it resolved in lower case, and dbt renders them
    with the original capitalisation ("Crm"."dbo"."Customer"), so the lookup has
    to be case-insensitive in both directions.
    """
    return {
        (e["database"].lower(), e["schema"].lower(), (e.get("alias") or e["name"]).lower()): (uid, e)
        for uid, e in list(manifest["nodes"].items()) + list(manifest["sources"].items())
        if e.get("resource_type") in ("model", "seed", "source") and not is_ephemeral(e)
    }


def build_sql_schema(manifest: dict, catalog: dict, om_columns: Callable[[str], list[str]]) -> dict:
    """A {catalog: {schema: {table: {column: type}}}} map for sqlglot.

    Without it sqlglot cannot expand the `select *` every staging model opens
    with, and every column resolves to the useless leaf "*". Columns come from
    catalog.json where dbt could introspect the relation, and otherwise from
    OpenMetadata — which is where the attached SQL Server tables' real columns
    live, since `dbt docs generate` cannot see into those catalogs.
    """
    schema: dict = {}
    for uid, e in list(manifest["nodes"].items()) + list(manifest["sources"].items()):
        if e.get("resource_type") not in ("model", "seed", "source") or is_ephemeral(e):
            continue
        catalogued = (catalog.get("nodes", {}).get(uid) or catalog.get("sources", {}).get(uid) or {})
        names = list(catalogued.get("columns", {}))
        if not names:
            svc, _owned = service_for(e["database"])
            names = om_columns(f"{svc}.{e['database']}.{e['schema']}.{e.get('alias') or e['name']}")
        if names:
            table = e.get("alias") or e["name"]
            schema.setdefault(e["database"], {}).setdefault(e["schema"], {})[table] = dict.fromkeys(names, "UNKNOWN")
    return schema


def column_lineage_facet(node: dict, columns: list[str], schema: dict, index: dict) -> dict:
    """OL columnLineage facet for one model: output column -> input columns.

    Returns {} when it cannot be derived — no sqlglot, no compiled SQL, or a
    statement sqlglot will not parse. Column lineage is a bonus on top of the
    table-level graph, so a failure here must never cost us the edge itself.
    """
    sql = node.get("compiled_code")
    if not (_sqlglot_lineage and sql):
        return {}

    # Our own sentinels, not real columns: asking sqlglot to trace one just
    # raises, since it does not appear in the SQL.
    fields = {}
    for column in (c for c in columns if c not in ("_unknown", "_placeholder")):
        try:
            root = _sqlglot_lineage(column, sql, schema=schema, dialect="duckdb")
        except Exception as exc:  # sqlglot raises a wide range on odd SQL
            log(f"  column lineage for {node['name']}.{column}: {type(exc).__name__}")
            continue
        inputs = []
        for leaf in (d for d in root.walk() if not d.downstream):
            source = leaf.source
            key = (
                (getattr(source, "catalog", "") or "").lower(),
                (getattr(source, "db", "") or "").lower(),
                (getattr(source, "name", "") or "").lower(),
            )
            # "institut.orgnr" -> "orgnr"; a bare "*" means sqlglot could not
            # expand a select *, which is not a real column reference.
            field = leaf.name.rsplit(".", 1)[-1]
            if field == "*" or key not in index:
                continue
            _uid, parent = index[key]
            inputs.append(dataset(parent) | {"field": original_case(field, parent, schema)})
        if inputs:
            fields[column] = {"inputFields": inputs, "transformationType": "DIRECT"}
    return {"fields": fields} if fields else {}


def original_case(field: str, entity: dict, schema: dict) -> str:
    """sqlglot lower-cases identifiers; give the column its real name back."""
    known = schema.get(entity["database"], {}).get(entity["schema"], {}).get(
        entity.get("alias") or entity["name"], {}
    )
    return next((k for k in known if k.lower() == field.lower()), field)


def dataset(entity: dict, unique_id: str = "", catalog: dict | None = None) -> dict:
    """An OL dataset: name is database.schema.table, namespace is the server.

    When the catalog knows the columns, attach OM's `schema` facet so the event
    carries the structure on its own — independent of whether the REST bootstrap
    ran. OM's OpenLineageMapper reads it.
    """
    name = f"{entity['database']}.{entity['schema']}.{entity['name']}"
    ds = {"namespace": namespace_for(entity["database"]), "name": name}
    fields = [
        {"name": c["name"], "type": c.get("type", "")}
        for c in ((catalog or {}).get("nodes", {}).get(unique_id, {}).get("columns", {})).values()
    ]
    return ds | ({"facets": {"schema": {"fields": fields}}} if fields else {})


def ssl_context() -> ssl.SSLContext | None:
    """TLS trust for an https OM. None lets urllib use its default."""
    if os.getenv("OM_INSECURE"):
        log("WARNING: OM_INSECURE set — TLS certificate verification disabled")
        ctx = ssl.create_default_context()
        ctx.check_hostname = False
        ctx.verify_mode = ssl.CERT_NONE
        return ctx
    ca = os.getenv("OM_CA_BUNDLE")
    if ca:
        return ssl.create_default_context(cafile=ca)
    return None


def http_json(url: str, payload: dict | None = None, headers: dict | None = None,
              data: bytes | None = None, timeout: int = 60) -> tuple[int, str]:
    """POST (or GET when there is no body) and return (status, text)."""
    if payload is not None:
        data = json.dumps(payload).encode()
    hdrs = {"Content-Type": "application/json"}
    hdrs.update(headers or {})
    req = urllib.request.Request(url, data=data, method="POST" if data else "GET",
                                 headers=hdrs)
    try:
        with urllib.request.urlopen(req, timeout=timeout, context=ssl_context()) as resp:
            return resp.status, resp.read().decode()
    except urllib.error.HTTPError as exc:
        return exc.code, exc.read().decode()


def login_with_password(host: str, user: str, password: str) -> str | None:
    """OM basic login. The password MUST be base64 — plain text just 401s."""
    url = host.rstrip("/") + "/api/v1/users/login"
    encoded = base64.b64encode(password.encode()).decode()
    status, text = http_json(url, {"email": user, "password": encoded})
    if status != 200:
        log(f"login as {user} failed: HTTP {status}: {text[:200]}")
        return None
    token = (json.loads(text) or {}).get("accessToken")
    if not token:
        log(f"login as {user} returned no accessToken: {text[:200]}")
    return token


def login_with_client_credentials() -> str | None:
    """OAuth2 client_credentials, for an OM fronted by an IdP."""
    token_url = os.getenv("OM_OAUTH_TOKEN_URL")
    form = {
        "grant_type": "client_credentials",
        "client_id": os.getenv("OM_CLIENT_ID", ""),
        "client_secret": os.getenv("OM_CLIENT_SECRET", ""),
    }
    scope = os.getenv("OM_OAUTH_SCOPE")
    if scope:
        form["scope"] = scope
    req = urllib.request.Request(
        token_url,
        data=urllib.parse.urlencode(form).encode(),
        method="POST",
        headers={"Content-Type": "application/x-www-form-urlencoded"},
    )
    try:
        with urllib.request.urlopen(req, timeout=30, context=ssl_context()) as resp:
            token = json.loads(resp.read().decode()).get("access_token")
    except urllib.error.HTTPError as exc:
        log(f"client_credentials grant failed: HTTP {exc.code}: {exc.read().decode()[:200]}")
        return None
    except urllib.error.URLError as exc:
        log(f"client_credentials grant failed: {exc.reason}")
        return None
    if not token:
        log("client_credentials grant returned no access_token")
    return token


def resolve_token(host: str) -> tuple[str | None, str]:
    """First configured credential wins; returns (token, how-we-got-it)."""
    token = os.getenv("OM_JWT_TOKEN")
    if token:
        return token.strip(), "OM_JWT_TOKEN"

    # Prefer a file in Kubernetes: a mounted Secret keeps the JWT out of the
    # environment, where it is visible in `ps` and `kubectl describe pod`.
    path = os.getenv("OM_JWT_TOKEN_FILE")
    if path:
        try:
            with open(path) as fh:
                return fh.read().strip(), f"OM_JWT_TOKEN_FILE ({path})"
        except OSError as exc:
            log(f"cannot read OM_JWT_TOKEN_FILE {path}: {exc}")
            return None, "OM_JWT_TOKEN_FILE"

    user, password = os.getenv("OM_USER"), os.getenv("OM_PASSWORD")
    if user and password:
        return login_with_password(host, user, password), f"login as {user}"

    if os.getenv("OM_CLIENT_ID") and os.getenv("OM_OAUTH_TOKEN_URL"):
        return login_with_client_credentials(), "client_credentials"

    return None, "no credentials configured"


_TOKEN: tuple[str | None, str] | None = None


def bearer(host: str) -> tuple[str | None, str]:
    """resolve_token(), memoised — otherwise bootstrap() would log in per entity."""
    global _TOKEN
    if _TOKEN is None:
        _TOKEN = resolve_token(host)
    return _TOKEN


def om_post(path: str, payload: dict, method: str = "POST") -> str:
    """Write to OM. Returns "created", "exists", or "error: ...".

    POST answers 409 for an entity that is already there, which is the normal
    case on every run after the first and for everything OM's own connectors
    ingested — so 409 is success, not failure.

    PUT is createOrUpdate. Use it ONLY for entities we own (the DuckDB service),
    where re-running must be able to correct what a previous run wrote; using it
    against an OM-ingested table would overwrite real metadata with our guesses.
    """
    host = os.environ["OM_HOST"].rstrip("/")
    token, _ = bearer(host)
    if not token:
        return "error: no OM credentials"
    req = urllib.request.Request(
        f"{host}{path}",
        data=json.dumps(payload).encode(),
        method=method,
        headers={"Content-Type": "application/json", "Authorization": f"Bearer {token}"},
    )
    try:
        urllib.request.urlopen(req, timeout=30, context=ssl_context())
        return "updated" if method == "PUT" else "created"
    except urllib.error.HTTPError as exc:
        if exc.code == 409:
            return "exists"
        return f"error: {exc.code} {exc.read().decode()[:160]}"
    except urllib.error.URLError as exc:
        return f"error: {exc.reason}"


def bootstrap(manifest: dict, catalog: dict, job_ns: str) -> None:
    """Create every entity the events are about to reference.

    Order matters: service -> database -> schema -> table, because OM rejects a
    child whose parent is missing (that is the "schema not found" the auto-create
    path trips over).
    """
    tally: dict[str, int] = {}

    def record(kind: str, name: str, result: str) -> None:
        tally[f"{kind} {result.split(':')[0]}"] = tally.get(f"{kind} {result.split(':')[0]}", 0) + 1
        if result.startswith("error"):
            log(f"  {kind} {name}: {result}")

    # A missing database service makes every database, schema and table under it
    # fail with a bare 404 ("Entity not found: databaseSchema <svc>.<db>.<schema>"),
    # which reads as a dozen unrelated errors rather than one wrong name. Check
    # the services we do NOT own up front and say exactly which env var to set.
    host = os.environ["OM_HOST"].rstrip("/")
    token, _ = bearer(host)
    auth = {"Authorization": f"Bearer {token}"} if token else {}
    entities_all = [
        e
        for e in list(manifest["nodes"].values()) + list(manifest["sources"].values())
        if e.get("resource_type") in ("model", "seed", "source") and not is_ephemeral(e)
    ]
    for svc in sorted({service_for(e["database"])[0] for e in entities_all
                       if not service_for(e["database"])[1]}):
        status, _body = http_json(
            f"{host}/api/v1/services/databaseServices/name/{svc}", headers=auth
        )
        if status == 404:
            # Before complaining, check for a pure capitalisation difference:
            # the hostname-derived default is "Sql01" but a site may have
            # registered "sql01". OM would 404 on that, yet the intent is
            # unambiguous, so adopt the real spelling rather than fail.
            _s, body = http_json(
                f"{host}/api/v1/services/databaseServices?limit=1000", headers=auth
            )
            try:
                existing = [d["name"] for d in json.loads(body).get("data", [])]
            except (ValueError, KeyError, TypeError):
                existing = []
            match = [n for n in existing if n.lower() == svc.lower()]
            if len(match) == 1:
                _SERVICE_OVERRIDE[svc] = match[0]
                log(f"database service '{svc}' not found; using '{match[0]}' "
                    f"(same name, different capitalisation — OM names are "
                    f"case-sensitive)")
                continue
            var = "OM_SQLSERVER_SERVICES"
            log(
                f"database service '{svc}' does not exist in OM — every database, "
                f"schema and table under it will fail with 404 and those events "
                f"will be skipped. Set {var} to the real service name. "
                f"OM currently has: {', '.join(sorted(existing)) or '(none)'}"
            )

    # Storage service + one Container per source folder. OM's OpenLineage
    # resolver RESOLVES containers but never creates them (unlike tables), so
    # without these the bulk-file inputs silently resolve to nothing. fullPath is
    # settable on create, and it is the only field the resolver matches on — the
    # service name is organisational.
    folders = container_folders(manifest)
    if folders:
        storage_service = os.getenv("OM_STORAGE_SERVICE", "object_storage")
        record("storage-service", storage_service, om_post(
            "/api/v1/services/storageServices",
            {"name": storage_service, "serviceType": "CustomStorage",
             "connection": {"config": {"type": "CustomStorage", "sourcePythonClass": "object_storage"}}}))
        existing = container_paths_in_om(host, auth)
        for namespace, paths in sorted(folders.items()):
            for path in sorted(paths):
                full_path = f"{namespace}/{path}"
                # Defer to whatever OM already has at this fullPath — a container
                # ingested by a storage service from the bucket's
                # openmetadata.json manifest, or one created by hand. Creating
                # our own anyway would leave two containers sharing a fullPath,
                # and searchContainerByFullPath takes containers.get(0),
                # arbitrarily. The name is ours only when nothing else owns it.
                if full_path in existing:
                    record("container", full_path, "exists")
                    continue
                record("container", full_path, om_post("/api/v1/containers", {
                    "name": path.replace("/", "_"),
                    "service": storage_service,
                    "fullPath": full_path,
                    "sourceUrl": full_path,
                }, method="PUT"))

    pipeline_service = os.getenv("OM_PIPELINE_SERVICE", "openlineage")
    record("pipeline-service", pipeline_service, om_post(
        "/api/v1/services/pipelineServices",
        {"name": pipeline_service, "serviceType": "CustomPipeline",
         "connection": {"config": {"type": "CustomPipeline", "sourcePythonClass": "openlineage"}}}))

    entities = [
        (uid, e)
        for uid, e in list(manifest["nodes"].items()) + list(manifest["sources"].items())
        if e.get("resource_type") in ("model", "seed", "source") and not is_ephemeral(e)
    ]

    # The DuckDB service is ours to create; the SQL Server ones are OM's.
    for svc in {service_for(e["database"])[0] for _uid, e in entities if service_for(e["database"])[1]}:
        record("db-service", svc, om_post(
            "/api/v1/services/databaseServices",
            {"name": svc, "serviceType": "CustomDatabase",
             "connection": {"config": {"type": "CustomDatabase", "sourcePythonClass": "duckdb"}}}))

    for svc, db in sorted({(service_for(e["database"])[0], e["database"]) for _uid, e in entities}):
        record("database", f"{svc}.{db}", om_post("/api/v1/databases", {"name": db, "service": svc}))
    for svc, db, sch in sorted(
        {(service_for(e["database"])[0], e["database"], e["schema"]) for _uid, e in entities}
    ):
        record("schema", f"{svc}.{db}.{sch}", om_post(
            "/api/v1/databaseSchemas", {"name": sch, "database": f"{svc}.{db}"}))

    for uid, e in entities:
        svc, owned = service_for(e["database"])
        payload = {
            "name": e["name"],
            "databaseSchema": f"{svc}.{e['database']}.{e['schema']}",
            "columns": om_columns(uid, e, catalog),
        } | ({"description": e["description"]} if e.get("description") else {})
        # PUT for the DuckDB service: it is ours, nothing else ingests it, and a
        # re-run must be able to replace the placeholder columns an earlier run
        # wrote. POST for the SQL Server services: 409 then leaves OM's real
        # ingested metadata (real columns and types) untouched.
        record("table", e["name"],
               om_post("/api/v1/tables", payload, method="PUT" if owned else "POST"))

    for uid, n in manifest["nodes"].items():
        if n["resource_type"] in ("model", "seed") and not is_ephemeral(n):
            record("pipeline", uid, om_post(
                "/api/v1/pipelines", {"name": f"{job_ns}-{uid}", "service": pipeline_service}))

    log("bootstrap: " + ", ".join(f"{v} {k}" for k, v in sorted(tally.items())))


def main() -> int:
    manifest_path = os.path.join(TARGET, "manifest.json")
    if not os.path.exists(manifest_path):
        log(f"no {manifest_path} — run dbt first; nothing to emit")
        return 1
    with open(manifest_path) as fh:
        manifest = json.load(fh)

    # catalog.json is what `dbt docs generate` introspected. It carries the real
    # columns and types for the DuckDB catalog (the loader runs docs generate in
    # the same pod, where those tables physically live) and NOT for the attached
    # SQL Server ones — which is the right way round: OM's own connectors ingest
    # those. Absent is fine; om_columns falls back.
    catalog_path = os.path.join(TARGET, "catalog.json")
    catalog: dict = {}
    if os.path.exists(catalog_path):
        with open(catalog_path) as fh:
            catalog = json.load(fh)
        log(f"using {catalog_path} ({len(catalog.get('nodes', {}))} nodes with columns)")
    else:
        log(f"no {catalog_path} — run `dbt docs generate`; tables get placeholder columns")

    # Prefer the build's results: `dbt docs generate` overwrites run_results.json
    # with its own catalog run.
    results_path = os.path.join(TARGET, "run_results.build.json")
    if not os.path.exists(results_path):
        results_path = os.path.join(TARGET, "run_results.json")
    statuses: dict[str, str] = {}
    if os.path.exists(results_path):
        with open(results_path) as fh:
            statuses = {r["unique_id"]: r["status"] for r in json.load(fh)["results"]}
        log(f"using {results_path} ({len(statuses)} node results)")
    else:
        log("no run_results — emitting lineage for every model in the manifest")

    def entity_for(uid: str) -> dict | None:
        return manifest["nodes"].get(uid) or manifest["sources"].get(uid)

    def upstream(uid: str) -> list[str]:
        """Parents of a node, looking through ephemeral models to what they read."""
        parent = entity_for(uid)
        if parent is None:
            return []
        if is_ephemeral(parent):
            return [p for dep in (parent.get("depends_on") or {}).get("nodes", []) for p in upstream(dep)]
        return [uid]

    run_id = str(uuid.uuid4())
    # OM 2.0.x REJECTS a "+00:00" offset with 400 "Invalid request format" and
    # accepts only the "Z" spelling of UTC; 1.12.6 took either. Python's
    # isoformat() emits "+00:00", so this is not cosmetic — measured against a
    # live 2.0.2: "...T09:00:00.123456+00:00" -> 400, "...T09:00:00.123456Z" -> 200
    # (any sub-second precision is accepted, as is none).
    now = datetime.now(UTC).isoformat().replace("+00:00", "Z")
    job_ns = os.getenv("OL_JOB_NAMESPACE", "dbt")

    # Built once: the relation lookup and the column map sqlglot needs to expand
    # `select *`. The OM lookups inside are cached per FQN.
    index = relation_index(manifest)
    sql_schema = build_sql_schema(manifest, catalog, lambda fqn: list(om_table_columns(fqn)))
    if _sqlglot_lineage is None:
        log("sqlglot not installed — emitting table-level lineage only")

    events, skipped = [], 0
    for uid, node in manifest["nodes"].items():
        if node["resource_type"] not in ("model", "seed") or is_ephemeral(node):
            continue
        # A node that errored produced no table; emitting it as COMPLETE would
        # assert lineage that does not exist. OM's default eventTypeFilter keeps
        # only COMPLETE, so a FAIL event is recorded without creating an edge.
        status = statuses.get(uid, "success")
        if status in ("error", "fail", "runtime error"):
            event_type = "FAIL"
        elif status == "skipped":
            skipped += 1
            continue
        else:
            event_type = "COMPLETE"

        inputs = [
            dataset(parent, dep, catalog)
            for direct in (node.get("depends_on") or {}).get("nodes", [])
            for dep in upstream(direct)
            if (parent := entity_for(dep))
        ] + storage_inputs(node)
        # Column-level lineage rides along on the output dataset. Derived from
        # the compiled SQL, so it is best-effort: an empty facet just means the
        # table-level edge stands on its own, which is how it worked before.
        output = dataset(node, uid, catalog)
        # Take the output column list from sql_schema, not om_columns: for the
        # SQL Server marts catalog.json has nothing, and sql_schema has already
        # filled those in from OM. Asking for lineage of a placeholder column
        # that does not exist just raises.
        out_cols = list(
            sql_schema.get(node["database"], {})
            .get(node["schema"], {})
            .get(node.get("alias") or node["name"], {})
        )
        if col_facet := column_lineage_facet(node, out_cols, sql_schema, index):
            output["facets"] = output.get("facets", {}) | {"columnLineage": col_facet}
        events.append(
            {
                "eventTime": now,
                "producer": PRODUCER,
                "schemaURL": SCHEMA_URL,
                "eventType": event_type,
                "run": {"runId": run_id},
                "job": {"namespace": job_ns, "name": uid},
                "inputs": inputs,
                "outputs": [output],
            }
        )

    if not events:
        log("no events to emit")
        return 0
    edges = sum(len(e["inputs"]) for e in events)
    log(f"{len(events)} events, {edges} input edges, {skipped} skipped nodes")

    body = json.dumps({"events": events}).encode()
    if os.getenv("OL_DRY_RUN"):
        print(json.dumps({"events": events}, indent=2))
        return 0

    host = os.getenv("OM_HOST")
    if not host:
        log("OM_HOST not set — skipping the POST (use OL_DRY_RUN=1 to inspect)")
        return 0

    token, source = bearer(host)
    if not token:
        if source == "no credentials configured":
            # Unconfigured is not a failure: the caller already decides whether
            # to call us, and a laptop run without OM should stay quiet.
            log("no OM credentials (OM_JWT_TOKEN / _FILE / OM_USER+OM_PASSWORD / "
                "OM_CLIENT_ID) — skipping the POST")
            return 0
        log(f"could not obtain a token via {source} — not posting")
        return 1
    log(f"authenticated via {source}")

    if "--no-bootstrap" not in sys.argv:
        bootstrap(manifest, catalog, job_ns)

    url = host.rstrip("/") + "/api/v1/openlineage/lineage/batch"
    req = urllib.request.Request(
        url,
        data=body,
        method="POST",
        headers={
            "Content-Type": "application/json",
            "Authorization": f"Bearer {token}",
        },
    )
    try:
        with urllib.request.urlopen(req, timeout=60, context=ssl_context()) as resp:
            log(f"POST {url} -> {resp.status}")
            payload = resp.read().decode()
            if payload:
                log(payload[:500])
            # A batch that resolves NOTHING still answers 200, with
            # {"summary": {"skipped": N}} and no lineage written. That is the
            # failure this whole script exists to avoid, and it is invisible in
            # the status code — so treat it as an error rather than let the DAG
            # go green on a run that catalogued nothing.
            try:
                summary = json.loads(payload).get("summary", {})
            except (ValueError, AttributeError):
                summary = {}
            if summary.get("skipped") and not summary.get("successful"):
                log("every event was SKIPPED - no lineage written. Check the OM "
                    "server log for 'Could not resolve' / 'Failed to create'; "
                    "usually a missing database service, database or schema.")
                return 1
    except urllib.error.HTTPError as exc:
        detail = exc.read().decode()[:500]
        log(f"POST {url} -> {exc.code}: {detail}")
        if exc.code in (401, 403):
            # By far the most common cause in the DAG: a bot JWT that has aged
            # out. OM returns 401 with no hint that the token merely expired.
            log(f"token came from {source}; if this is a bot JWT it has most "
                "likely expired — reissue it in OM Settings -> Bots")
        return 1
    except urllib.error.URLError as exc:
        log(f"POST {url} failed: {exc.reason}")
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
