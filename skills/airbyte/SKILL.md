---
name: airbyte
description: Airbyte ELT — source/destination connectors, sync modes (Full Refresh Overwrite/Append, Incremental Append/Deduped), cursor fields, primary keys, catalog/streams, deployment (abctl, Kubernetes Helm, Airbyte Cloud), Connector Builder, Python CDK (HttpStream, IncrementalMixin), normalization (dbt-based, _airbyte_raw_ tables), Airbyte API, Terraform provider, schema evolution, monitoring, Airflow AirbyteTriggerSyncOperator integration
---

# Airbyte ELT

## When to Use

Load this skill when the user needs to:
- Design or configure Airbyte connections (source → destination)
- Choose and configure sync modes (Full Refresh Overwrite/Append, Incremental Append/Deduped)
- Deploy Airbyte via abctl, Kubernetes Helm chart, or Airbyte Cloud
- Build custom connectors with the no-code Connector Builder or Python CDK
- Understand raw vs. normalized tables and dbt-based normalization
- Manage connections programmatically via the Airbyte REST API or Terraform provider
- Handle schema evolution (propagation policies, breaking changes)
- Monitor sync health, review logs, and configure alerting
- Orchestrate Airbyte syncs from Apache Airflow

---

## Architecture

```
Source System                Airbyte Platform                  Destination
─────────────  ──────────────────────────────────────────────  ─────────────
PostgreSQL  ─→  Source Connector (reads catalog / streams)  ─→  Snowflake
MySQL       ─→  Scheduler / Worker (orchestrates job)        ─→  BigQuery
REST API    ─→  Normalization (optional dbt run)             ─→  S3 / ADLS
Kafka       ─→  State store (cursor bookmarks)               ─→  Redshift
S3 Files    ─→  Catalog (stream list + config metadata)      ─→  ClickHouse
```

**Key concepts:**

| Term | Meaning |
|---|---|
| **Source** | Configured instance of a source connector (credentials + settings) |
| **Destination** | Configured instance of a destination connector |
| **Connection** | Pairing of source + destination with sync schedule, sync mode, and stream selection |
| **Catalog** | Metadata document listing all streams a source exposes and their schemas |
| **Stream** | A table, endpoint, or logical data entity exposed by the source |
| **Sync mode** | Per-stream policy for how Airbyte reads from source and writes to destination |
| **Cursor field** | Column used to track incremental progress (e.g., `updated_at`) |
| **Primary key** | Column(s) used for deduplication in Append+Deduped mode |
| **State** | JSON checkpoint Airbyte stores to resume incremental syncs |

---

## Sync Modes

Sync modes are configured **per stream** within a connection.

### Full Refresh — Overwrite

```
Source  ──(read all rows)──►  Destination table (TRUNCATE + INSERT)
```

- Destination table is dropped and recreated on every sync.
- No cursor or primary key needed.
- Use when: small tables, lookup/reference data, sources that do not support incremental.
- Trade-off: high cost for large tables; guaranteed consistency.

### Full Refresh — Append

```
Source  ──(read all rows)──►  Destination table (INSERT — never deletes)
```

- Every sync appends a complete snapshot; old data is never removed.
- Useful for retaining historical snapshots with `_airbyte_emitted_at` as a version column.
- Trade-off: table grows unboundedly; requires downstream deduplication.

### Incremental — Append

```
Source  ──(read rows WHERE cursor > last_state)──►  Destination table (INSERT new rows only)
```

- Reads only records changed since the last sync using a **cursor field** (`updated_at`, `id`, etc.).
- State is stored by Airbyte; each run picks up from the saved cursor value.
- Use when: event logs, append-only fact tables, high-volume sources.
- Limitation: hard deletes in the source are **not** propagated.

### Incremental — Append + Deduped

```
Source  ──(read changed rows)──►  _airbyte_raw_<stream> (append)
                                 └─► Normalized/final table (UPSERT using primary key)
```

- Combines incremental reads with upsert semantics at the destination.
- Requires both **cursor field** (for reads) and **primary key** (for deduplication).
- Destination maintains a deduplicated view that mirrors the source state.
- Use when: source tables have updates and deletes, and you need a current-state view.

### Sync Mode Selection Matrix

| Source has updates? | Source has deletes? | Volume | Recommended mode |
|---|---|---|---|
| No | No | Any | Full Refresh Overwrite |
| Yes | No | Small | Full Refresh Overwrite |
| Yes | No | Large | Incremental Append + Deduped |
| Yes | Yes | Any | Incremental Append + Deduped |
| Append-only log | No | Large | Incremental Append |

### Cursor Field Best Practices

- Prefer `updated_at` (timestamp) over `id` (integer) — timestamps handle out-of-order updates.
- Ensure the cursor column is **indexed** in the source — Airbyte issues `WHERE cursor > :state` queries.
- Avoid nullable cursors — NULL values break state comparison; use `COALESCE(updated_at, created_at)`.
- If using `id` as cursor, verify it is monotonically increasing (auto-increment, ULID, etc.).

---

## Deployment

### abctl (Local / Dev — Kubernetes-in-Docker)

`abctl` runs a kind-based local Kubernetes cluster and installs Airbyte via Helm. It is the recommended local deployment path as of Airbyte 1.x (Docker Compose was deprecated).

```bash
# Install abctl
curl -LsfS https://get.airbyte.com | bash -

# Deploy Airbyte locally (first run may take ~20 min)
abctl local install

# Check status
abctl local status

# Access UI at http://localhost:8000
# Default credentials printed after install

# Upgrade
abctl local upgrade

# Uninstall
abctl local uninstall
```

### Kubernetes — Helm Chart V2 (Production)

```bash
# Add Airbyte Helm repo
helm repo add airbyte https://airbytehq.github.io/helm-charts
helm repo update

# Inspect default values
helm show values airbyte/airbyte > airbyte-values.yaml
```

Minimal production `airbyte-values.yaml`:

```yaml
global:
  edition: community          # community | enterprise
  env_vars:
    AIRBYTE_VERSION: 1.8.0

webapp:
  replicaCount: 1
  ingress:
    enabled: true
    className: nginx
    hosts:
      - host: airbyte.example.com
        paths:
          - path: /
            pathType: Prefix

server:
  replicaCount: 1

worker:
  replicaCount: 2             # scale for parallel jobs

temporal:
  replicaCount: 1

postgresql:
  enabled: true               # embedded Postgres; use external for production
  # For external Postgres:
  # enabled: false
  # host: rds.example.com
  # port: 5432
  # database: airbyte
  # user: airbyte
  # password: <secret>

minio:
  enabled: true               # embedded MinIO for logs/state; use S3 for production

externalDatabase:
  host: ""
  port: 5432
  database: airbyte
  user: airbyte
  existingSecret: airbyte-db-secret
  existingSecretPasswordKey: password
```

```bash
helm install airbyte airbyte/airbyte \
  --namespace airbyte \
  --create-namespace \
  --values airbyte-values.yaml \
  --version 1.8.0
```

### Airbyte Cloud

- Fully managed SaaS at [cloud.airbyte.com](https://cloud.airbyte.com).
- No infrastructure to manage; connectors are updated by Airbyte.
- Pricing is credits-based per row synced.
- Supports all deployment-level concepts (sources, destinations, connections, API, Terraform) identically.

---

## Connector Catalog

Airbyte ships 370+ certified connectors and 600+ total (community included). Key connectors:

| Connector | Type | Notes |
|---|---|---|
| PostgreSQL | Source + Destination | CDC via pgoutput or Xmin; certified |
| MySQL | Source + Destination | CDC via binlog; certified |
| Microsoft SQL Server | Source | CDC via CDC_LSN cursor; certified |
| MongoDB | Source | Change streams; certified |
| S3 | Source + Destination | CSV, JSON, Parquet, Avro, JSONL formats |
| Google BigQuery | Source + Destination | Supports GCS staging; certified |
| Snowflake | Source + Destination | Internal stage or S3/GCS; certified |
| Redshift | Destination | S3 staging; certified |
| Apache Kafka | Source + Destination | JSON / Avro deserialization |
| REST API (Generic HTTP) | Source | Via Connector Builder |
| Salesforce | Source | Bulk API 2.0; certified |
| Stripe | Source | Incremental; certified |
| HubSpot | Source | Incremental; certified |
| GitHub | Source | Incremental; certified |
| dbt Cloud | Source | Run metadata |

**Connector versions** are pinned per connection. Upgrade via UI or API:

```bash
# List available connector versions
curl -X GET "https://api.airbyte.com/v1/sources/{sourceId}" \
  -H "Authorization: Bearer $AIRBYTE_API_KEY"

# Upgrade connector version (Airbyte Cloud API)
curl -X PATCH "https://api.airbyte.com/v1/sources/{sourceId}" \
  -H "Authorization: Bearer $AIRBYTE_API_KEY" \
  -H "Content-Type: application/json" \
  -d '{"dockerImageTag": "3.3.24"}'
```

---

## Custom Connectors

### Connector Builder (No-Code)

The Connector Builder is a YAML-driven UI for HTTP API sources. It generates a low-code manifest (no Python required) and supports:
- Pagination (page-number, cursor, offset, link-based)
- Incremental sync with date-range or cursor-based slicing
- Authentication (API key, Bearer token, OAuth 2.0, Basic Auth)
- Record transformation and filtering via jinja-like expressions
- Record selection using JSONPath

Resulting connector is a YAML manifest deployable as a custom source.

### Python CDK

Install: `pip install airbyte-cdk`

#### Full Refresh HTTP Connector

```python
# source_my_api/source.py
from typing import Any, Iterable, List, Mapping, MutableMapping, Optional, Tuple
from airbyte_cdk.sources import AbstractSource
from airbyte_cdk.sources.streams import Stream
from airbyte_cdk.sources.streams.http import HttpStream
from airbyte_cdk.sources.streams.http.auth import TokenAuthenticator
import requests


class ProductsStream(HttpStream):
    """Full refresh stream — reads all products from a REST API."""

    url_base = "https://api.example.com/v1/"
    primary_key = "id"

    def path(self, **kwargs) -> str:
        return "products"

    def next_page_token(
        self, response: requests.Response
    ) -> Optional[Mapping[str, Any]]:
        data = response.json()
        next_cursor = data.get("meta", {}).get("next_cursor")
        return {"cursor": next_cursor} if next_cursor else None

    def request_params(
        self,
        stream_state: Mapping[str, Any],
        next_page_token: Optional[Mapping[str, Any]] = None,
        **kwargs,
    ) -> MutableMapping[str, Any]:
        params: dict = {"limit": 200}
        if next_page_token:
            params["cursor"] = next_page_token["cursor"]
        return params

    def parse_response(
        self, response: requests.Response, **kwargs
    ) -> Iterable[Mapping]:
        yield from response.json().get("data", [])


class SourceMyApi(AbstractSource):
    def check_connection(
        self, logger, config: Mapping[str, Any]
    ) -> Tuple[bool, Optional[Any]]:
        try:
            auth = TokenAuthenticator(token=config["api_key"])
            stream = ProductsStream(authenticator=auth)
            # Attempt to read one record
            next(stream.read_records(sync_mode=None))
            return True, None
        except Exception as e:
            return False, str(e)

    def streams(self, config: Mapping[str, Any]) -> List[Stream]:
        auth = TokenAuthenticator(token=config["api_key"])
        return [ProductsStream(authenticator=auth)]
```

#### Incremental HTTP Connector

```python
from airbyte_cdk.sources.streams.http import HttpStream
from airbyte_cdk.sources.streams.core import IncrementalMixin
from typing import Any, Iterable, Mapping, MutableMapping, Optional
import requests
from datetime import datetime


class OrdersStream(HttpStream, IncrementalMixin):
    """Incremental stream — reads orders updated after the cursor."""

    url_base = "https://api.example.com/v1/"
    primary_key = "order_id"
    cursor_field = "updated_at"          # field in each record
    _cursor_value: str = ""              # internal state storage

    @property
    def state(self) -> MutableMapping[str, Any]:
        return {self.cursor_field: self._cursor_value}

    @state.setter
    def state(self, value: MutableMapping[str, Any]) -> None:
        self._cursor_value = value.get(self.cursor_field, "")

    def path(self, **kwargs) -> str:
        return "orders"

    def request_params(
        self,
        stream_state: Mapping[str, Any],
        next_page_token: Optional[Mapping[str, Any]] = None,
        **kwargs,
    ) -> MutableMapping[str, Any]:
        params: dict = {"limit": 500}
        cursor = stream_state.get(self.cursor_field) or self._cursor_value
        if cursor:
            params["updated_after"] = cursor
        if next_page_token:
            params["page"] = next_page_token["page"]
        return params

    def next_page_token(
        self, response: requests.Response
    ) -> Optional[Mapping[str, Any]]:
        body = response.json()
        page = body.get("pagination", {})
        if page.get("has_next"):
            return {"page": page["next_page"]}
        return None

    def parse_response(
        self,
        response: requests.Response,
        stream_state: Mapping[str, Any],
        **kwargs,
    ) -> Iterable[Mapping]:
        for record in response.json().get("orders", []):
            # Advance cursor to the latest seen value
            rec_cursor = record.get(self.cursor_field, "")
            if rec_cursor > self._cursor_value:
                self._cursor_value = rec_cursor
            yield record
```

#### OAuth2 Authenticator

```python
from airbyte_cdk.sources.streams.http.auth import Oauth2Authenticator

auth = Oauth2Authenticator(
    token_refresh_endpoint="https://api.example.com/oauth/token",
    client_id=config["client_id"],
    client_secret=config["client_secret"],
    refresh_token=config["refresh_token"],
    scopes=["read:orders", "read:products"],
)
```

#### Connector Project Layout

```
source-my-api/
├── main.py                  # entrypoint: python main.py spec|check|discover|read
├── source_my_api/
│   ├── __init__.py
│   ├── source.py            # AbstractSource subclass
│   └── streams.py           # stream definitions
├── integration_tests/
│   ├── configured_catalog.json
│   └── sample_config.json
├── unit_tests/
│   └── test_streams.py
├── Dockerfile
├── metadata.yaml            # connector metadata for registry
└── requirements.txt
```

```bash
# Test locally
python main.py spec
python main.py check    --config secrets/config.json
python main.py discover --config secrets/config.json
python main.py read     --config secrets/config.json \
                        --catalog integration_tests/configured_catalog.json

# Build and push custom image
docker build . -t my-registry/source-my-api:0.1.0
docker push my-registry/source-my-api:0.1.0
# Register in Airbyte UI: Settings → Sources → New connector → custom image
```

---

## Normalization

### Raw Tables

Every sync writes raw records to a staging table with the `_airbyte_raw_` prefix (legacy) or `_airbyte_meta`-enriched tables in destinations that support it (e.g., BigQuery, Snowflake v2 destinations).

Raw table columns:

| Column | Type | Description |
|---|---|---|
| `_airbyte_raw_id` | VARCHAR | UUID generated per record per sync |
| `_airbyte_extracted_at` | TIMESTAMP | When Airbyte read the record from source |
| `_airbyte_loaded_at` | TIMESTAMP | When the record was written to destination |
| `_airbyte_data` | JSONB / VARIANT | Original record as JSON |
| `_airbyte_meta` | JSON | Schema validation errors, `changes` array |

### dbt-Based Normalization (Basic Normalization)

Airbyte ships an internal dbt project that runs after each sync to flatten raw JSON into typed columns. This is called **Basic Normalization** and is toggled per connection:

```
_airbyte_raw_orders   (raw JSON layer)
      │
      ▼  dbt run (airbyte-generated models)
orders_scd            (SCD2 history table — all versions)
orders                (final deduplicated view, current rows only)
```

Generated model conventions:
- `<stream>` — deduplicated current state, one row per primary key.
- `<stream>_scd` — SCD type 2 history with `_airbyte_start_at` / `_airbyte_end_at`.
- `<stream>_stg` — intermediate staging model.

**Typed Destinations (Airbyte v2 destinations — preferred):** BigQuery v2, Snowflake v3, Redshift v3, and newer destination versions skip the legacy normalization model and write directly to typed final tables, handling JSON flattening internally.

### Custom dbt Transformations

For logic beyond basic normalization, run a custom dbt project downstream:

```sql
-- models/staging/stg_orders.sql
-- Reference the Airbyte-written final table directly
with source as (
    select * from {{ source('airbyte_raw', 'orders') }}
),
renamed as (
    select
        order_id,
        customer_id,
        cast(order_total as numeric(18,2))    as order_total_usd,
        cast(created_at as timestamp)         as created_at,
        _airbyte_extracted_at                 as airbyte_extracted_at
    from source
)
select * from renamed
```

---

## Airbyte REST API

The Airbyte API (v1) provides full CRUD over all platform objects.

### Authentication

```bash
export AIRBYTE_API_KEY="your-api-key"      # Airbyte Cloud
# Self-managed: use basic auth or generate token in Settings → API keys
```

### Create Source

```bash
curl -X POST "https://api.airbyte.com/v1/sources" \
  -H "Authorization: Bearer $AIRBYTE_API_KEY" \
  -H "Content-Type: application/json" \
  -d '{
    "name": "prod-postgres",
    "workspaceId": "WORKSPACE_UUID",
    "definitionId": "decd338e-5647-4c0b-adf4-da0e75f5a750",
    "configuration": {
      "sourceType": "postgres",
      "host": "db.example.com",
      "port": 5432,
      "database": "prod",
      "username": "airbyte_reader",
      "password": "secret",
      "ssl": true,
      "replication_method": {
        "method": "CDC",
        "plugin": "pgoutput",
        "replication_slot": "airbyte_slot",
        "publication": "airbyte_pub"
      }
    }
  }'
```

### Create Destination

```bash
curl -X POST "https://api.airbyte.com/v1/destinations" \
  -H "Authorization: Bearer $AIRBYTE_API_KEY" \
  -H "Content-Type: application/json" \
  -d '{
    "name": "snowflake-prod",
    "workspaceId": "WORKSPACE_UUID",
    "definitionId": "424892c4-daac-4491-b35d-c6688ba547ba",
    "configuration": {
      "destinationType": "snowflake",
      "host": "account.snowflakecomputing.com",
      "role": "AIRBYTE_ROLE",
      "warehouse": "AIRBYTE_WH",
      "database": "ANALYTICS",
      "schema": "raw",
      "username": "airbyte",
      "credentials": {
        "auth_type": "Username and Password",
        "password": "secret"
      }
    }
  }'
```

### Create Connection

```bash
curl -X POST "https://api.airbyte.com/v1/connections" \
  -H "Authorization: Bearer $AIRBYTE_API_KEY" \
  -H "Content-Type: application/json" \
  -d '{
    "name": "postgres-to-snowflake",
    "sourceId": "SOURCE_UUID",
    "destinationId": "DEST_UUID",
    "schedule": {
      "scheduleType": "cron",
      "cronExpression": "0 */6 * * *"
    },
    "dataResidency": "auto",
    "namespaceDefinition": "destination",
    "nonBreakingSchemaUpdatesBehavior": "propagate_columns",
    "configurations": {
      "streams": [
        {
          "name": "orders",
          "syncMode": "incremental_append_deduped",
          "cursorField": ["updated_at"],
          "primaryKey": [["order_id"]]
        },
        {
          "name": "customers",
          "syncMode": "full_refresh_overwrite"
        }
      ]
    }
  }'
```

### Trigger Manual Sync

```bash
curl -X POST "https://api.airbyte.com/v1/jobs" \
  -H "Authorization: Bearer $AIRBYTE_API_KEY" \
  -H "Content-Type: application/json" \
  -d '{
    "connectionId": "CONNECTION_UUID",
    "jobType": "sync"
  }'
```

---

## Terraform Provider

The `airbytehq/airbyte` Terraform provider wraps the Airbyte API. As of v1.1+, all connectors use generic `airbyte_source` / `airbyte_destination` resources.

```hcl
terraform {
  required_providers {
    airbyte = {
      source  = "airbytehq/airbyte"
      version = "~> 1.1"
    }
  }
}

provider "airbyte" {
  # Airbyte Cloud
  bearer_auth = var.airbyte_api_key
  # Self-managed OSS:
  # username = "airbyte"
  # password = var.airbyte_password
  # server_url = "http://airbyte.internal:8000/api/public"
}

resource "airbyte_source" "postgres_prod" {
  name         = "postgres-prod"
  workspace_id = var.workspace_id
  configuration = jsonencode({
    sourceType = "postgres"
    host       = "db.example.com"
    port       = 5432
    database   = "prod"
    username   = "airbyte_reader"
    password   = var.pg_password
    ssl        = true
    replication_method = {
      method           = "CDC"
      plugin           = "pgoutput"
      replication_slot = "airbyte_slot"
      publication      = "airbyte_pub"
    }
  })
}

resource "airbyte_destination" "snowflake_prod" {
  name         = "snowflake-prod"
  workspace_id = var.workspace_id
  configuration = jsonencode({
    destinationType = "snowflake"
    host            = "account.snowflakecomputing.com"
    role            = "AIRBYTE_ROLE"
    warehouse       = "AIRBYTE_WH"
    database        = "ANALYTICS"
    schema          = "raw"
    username        = "airbyte"
    credentials = {
      auth_type = "Username and Password"
      password  = var.sf_password
    }
  })
}

resource "airbyte_connection" "pg_to_sf" {
  name           = "postgres-to-snowflake"
  source_id      = airbyte_source.postgres_prod.source_id
  destination_id = airbyte_destination.snowflake_prod.destination_id

  schedule = {
    schedule_type   = "cron"
    cron_expression = "0 */6 * * *"
  }

  non_breaking_schema_updates_behavior = "propagate_columns"

  configurations = {
    streams = [
      {
        name      = "orders"
        sync_mode = "incremental_append_deduped"
        cursor_field = ["updated_at"]
        primary_key  = [["order_id"]]
      },
      {
        name      = "customers"
        sync_mode = "full_refresh_overwrite"
      }
    ]
  }
}
```

```bash
terraform init
terraform plan -out=tfplan
terraform apply tfplan
```

---

## Schema Evolution Handling

### Non-Breaking Changes

New columns added to the source are automatically handled based on the `nonBreakingSchemaUpdatesBehavior` setting:

| Policy | Behavior |
|---|---|
| `ignore` | Schema changes are detected but not applied; sync continues with old schema |
| `propagate_columns` | New columns are automatically added to the destination; removed columns are ignored |
| `propagate_fully` | New/removed columns and new/removed streams are auto-applied |
| `disable_connection` | Any schema change pauses the connection for manual review |

### Breaking Changes

A **breaking change** occurs when:
- An existing **primary key** column is removed from the source.
- An existing **cursor field** column is removed from the source.
- The source connector version introduces an incompatible protocol change.

When a breaking change is detected:
1. Airbyte **pauses the connection** immediately.
2. An alert appears in the UI and is sent via configured notification channels.
3. Manual action required: review the schema diff, update stream config, then trigger a **full refresh** (clear + resync) on affected streams.

### Schema Change Detection Workflow

```
Periodic catalog refresh  ──→  Diff detected?
                                    │
                           ┌────────┴────────┐
                        Non-breaking      Breaking
                           │                  │
                  Apply per policy       Pause + alert
                  (propagate/ignore)     manual review
                           │                  │
                     Next sync runs    User reconfigures
                     normally          + triggers full refresh
```

---

## Monitoring

### Connection Dashboard

The Airbyte UI **Connections** page shows:
- Per-connection status: Healthy / Failed / Running / Paused / Disabled.
- Last sync time, next scheduled sync.
- Rows synced and bytes synced per job.
- Stream-level status breakdown.

### Connection Timeline & Logs

```
Connection → Timeline tab
  └─ Each sync job shows: status, duration, rows synced, start/end time
       └─ Click job → View logs (structured log output per attempt)
            └─ Download logs as .txt for offline analysis
```

Log analysis tips:
- Search for `ERROR` or `WARN` tokens to locate failure origin.
- `SOURCE` or `DESTINATION` prefix in log lines indicates which side failed.
- Replication errors from CDC sources often appear as `Slot` or `WAL` warnings.

### Programmatic Status Check via API

```python
import requests, os

AIRBYTE_API_KEY = os.environ["AIRBYTE_API_KEY"]
BASE = "https://api.airbyte.com/v1"
HEADERS = {"Authorization": f"Bearer {AIRBYTE_API_KEY}"}


def get_connection_status(connection_id: str) -> dict:
    resp = requests.get(f"{BASE}/connections/{connection_id}", headers=HEADERS)
    resp.raise_for_status()
    return resp.json()


def list_recent_jobs(connection_id: str, limit: int = 10) -> list:
    resp = requests.get(
        f"{BASE}/jobs",
        headers=HEADERS,
        params={"connectionId": connection_id, "limit": limit, "orderBy": "createdAt|DESC"},
    )
    resp.raise_for_status()
    return resp.json().get("data", [])


def alert_on_failed_jobs(connection_id: str) -> None:
    jobs = list_recent_jobs(connection_id)
    for job in jobs:
        if job["status"] == "failed":
            print(
                f"ALERT: job {job['jobId']} failed at {job['lastUpdatedAt']} "
                f"for connection {connection_id}"
            )
            break  # alert once for most recent failure
```

### OpenTelemetry / Prometheus Metrics

For self-managed deployments, Airbyte exposes metrics via OpenTelemetry:

```yaml
# airbyte-values.yaml (Helm)
global:
  env_vars:
    PUBLISH_METRICS: "true"
    METRIC_CLIENT: "otel"
    OTEL_COLLECTOR_ENDPOINT: "http://otel-collector:4317"
```

Key metrics:

| Metric | Description |
|---|---|
| `airbyte_sync_job_duration_seconds` | Duration of sync jobs |
| `airbyte_records_emitted_total` | Total records read from source |
| `airbyte_records_committed_total` | Total records written to destination |
| `airbyte_bytes_emitted_total` | Bytes read from source |
| `num_pending_jobs` | Jobs waiting for a worker |
| `num_running_jobs` | Currently running sync jobs |

---

## Integration with Airflow

Install the provider:

```bash
pip install apache-airflow-providers-airbyte>=5.0.0
```

Configure an Airflow connection (UI: Admin → Connections):

| Field | Value |
|---|---|
| Connection Id | `airbyte_default` |
| Connection Type | `HTTP` |
| Host | `localhost` (self-managed) or `api.airbyte.com` (Cloud) |
| Port | `8000` (self-managed) or `443` (Cloud) |
| Password | API key (Cloud) or leave empty (OSS basic auth) |
| Schema | `http` / `https` |

### Synchronous Trigger (waits for completion)

```python
from airflow import DAG
from airflow.providers.airbyte.operators.airbyte import AirbyteTriggerSyncOperator
from datetime import datetime, timedelta

with DAG(
    dag_id="airbyte_sync_orders",
    start_date=datetime(2024, 1, 1),
    schedule="0 6 * * *",
    catchup=False,
    default_args={"retries": 1, "retry_delay": timedelta(minutes=5)},
) as dag:

    trigger_orders_sync = AirbyteTriggerSyncOperator(
        task_id="trigger_orders_sync",
        airbyte_conn_id="airbyte_default",
        connection_id="CONNECTION_UUID",   # Airbyte connection UUID
        asynchronous=False,                # block until sync completes
        timeout=3600,                      # fail if not done in 1 hour
        wait_seconds=30,                   # polling interval
    )
```

### Asynchronous Trigger (fire-and-poll)

```python
from airflow.providers.airbyte.operators.airbyte import AirbyteTriggerSyncOperator
from airflow.providers.airbyte.sensors.airbyte import AirbyteJobSensor

with DAG(dag_id="airbyte_async_pipeline", ...) as dag:

    trigger = AirbyteTriggerSyncOperator(
        task_id="trigger_sync",
        airbyte_conn_id="airbyte_default",
        connection_id="CONNECTION_UUID",
        asynchronous=True,                 # returns immediately with job_id
    )

    wait = AirbyteJobSensor(
        task_id="wait_for_sync",
        airbyte_conn_id="airbyte_default",
        airbyte_job_id=trigger.output,     # XCom from trigger task
        timeout=7200,
        poke_interval=60,
    )

    trigger >> wait
```

### Fan-Out: Trigger Multiple Connections in Parallel

```python
from airflow.operators.empty import EmptyOperator

CONNECTION_IDS = {
    "orders":    "UUID-1",
    "customers": "UUID-2",
    "products":  "UUID-3",
}

with DAG(dag_id="airbyte_multi_sync", schedule="0 3 * * *", ...) as dag:

    start = EmptyOperator(task_id="start")
    end   = EmptyOperator(task_id="end")

    for stream_name, conn_id in CONNECTION_IDS.items():
        sync_task = AirbyteTriggerSyncOperator(
            task_id=f"sync_{stream_name}",
            airbyte_conn_id="airbyte_default",
            connection_id=conn_id,
            asynchronous=False,
            timeout=1800,
        )
        start >> sync_task >> end
```

---

## Anti-Patterns

| Anti-Pattern | Problem | Fix |
|---|---|---|
| Full Refresh Overwrite on large tables (millions of rows) | Reads entire source on every sync; high cost and latency | Switch to Incremental Append + Deduped with a reliable cursor field |
| Using nullable column as cursor field | NULL records are skipped; state comparison breaks for NULL rows | Add `COALESCE(updated_at, created_at)` or pick a non-nullable monotonic field |
| Sharing one Airbyte destination schema across multiple sources | Table name collisions; `_airbyte_raw_` tables overwrite each other | Use per-source destination schemas or namespace prefixes |
| Disabling normalization without a downstream dbt model | Raw tables with `_airbyte_data` JSONB/VARIANT columns are unusable without flattening | Either enable basic normalization or implement a dbt staging model on top of raw tables |
| Ignoring breaking schema changes (leaving connection paused) | Sync backlog accumulates; downstream data freshness SLA breached | Set up alerting on connection status; automate schema review workflow |
| Pinning connector versions indefinitely | Miss security patches and bug fixes in certified connectors | Schedule quarterly connector version reviews; test upgrades in a staging workspace |
| Running full catalog discovery on every Airflow trigger | Triggers unnecessary schema refresh; slows DAG | Use `asynchronous=True` with `AirbyteJobSensor`; schedule discovery separately |
| Using the embedded Postgres + MinIO for production deployments | Single points of failure; no backup/restore; data loss risk | Use external managed Postgres (RDS) and S3/GCS for state/log storage in production Helm deployments |
| Creating connections manually via UI at scale (50+ connections) | Not reproducible; no version control; painful drift | Use Terraform provider or Airbyte API + CI/CD pipeline for all connection management |
| Incremental sync without monitoring cursor drift | Cursor can fall behind CDC log retention window; source data becomes unavailable | Monitor cursor lag; alert when `last_synced_cursor` age exceeds source WAL/binlog retention |

---

## References to Consult When Needed

- [Airbyte Documentation](https://docs.airbyte.com/)
- [Sync Modes Reference](https://docs.airbyte.com/platform/using-airbyte/core-concepts/sync-modes)
- [Schema Change Management](https://docs.airbyte.com/platform/using-airbyte/schema-change-management)
- [abctl Deployment Guide](https://docs.airbyte.com/platform/deploying-airbyte/abctl)
- [Helm Chart V2 (Community)](https://docs.airbyte.com/platform/deploying-airbyte/chart-v2-community)
- [Python CDK Overview](https://docs.airbyte.com/platform/connector-development/cdk-python)
- [HTTP Streams (CDK)](https://docs.airbyte.com/platform/connector-development/cdk-python/http-streams)
- [Incremental Streams (CDK)](https://docs.airbyte.com/connector-development/cdk-python/incremental-stream/)
- [Low-Code Connector Builder](https://docs.airbyte.com/platform/connector-development/config-based/low-code-cdk-overview)
- [Airbyte Terraform Provider](https://docs.airbyte.com/platform/terraform-documentation)
- [Terraform Provider Registry](https://registry.terraform.io/providers/airbytehq/airbyte/latest/docs)
- [Airflow Provider — AirbyteTriggerSyncOperator](https://airflow.apache.org/docs/apache-airflow-providers-airbyte/stable/operators/airbyte.html)
- [OpenTelemetry Metrics Monitoring](https://docs.airbyte.com/platform/operator-guides/open-telemetry)
- [Airbyte Connector Catalog](https://airbyte.com/connectors)
- [Airbyte API Reference](https://reference.airbyte.com/)
