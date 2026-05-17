---
name: feature-store
description: Feast feature store — feature repo layout, Entity/FeatureView/FeatureService definitions, offline store (BigQuery/Redshift/Snowflake/file), online store (Redis/DynamoDB/SQLite), point-in-time correct joins, feast materialize, feast apply, Python SDK get_online_features/get_historical_features, push sources, streaming feature views, Airflow integration
---

# Feast Feature Store

## When to Use

Load this skill when the user needs to:
- Set up a Feast feature repository with offline and online stores
- Define `Entity`, `FeatureView`, `FeatureService`, and `DataSource` objects
- Retrieve historical features with point-in-time correctness for model training
- Serve online features at low latency during model inference
- Materialize features from an offline store into an online store
- Wire Feast into Airflow DAGs for automated materialization and training pipelines
- Push real-time data into the online store via `PushSource` or streaming feature views
- Avoid training-serving skew by using the same feature definitions for training and serving
- Share feature definitions across multiple teams and ML models in an organization

**Feature store vs ad-hoc feature engineering:** Build a feature store when features are reused across multiple models, when training-serving skew is causing silent model degradation, or when a team needs point-in-time correct joins on event-driven data. Ad-hoc Pandas/Spark pipelines suffice for one-off exploration; Feast is for production systems where feature consistency and discoverability matter.

---

## Core Concepts

```
                    OFFLINE PATH (training data retrieval)
  ┌───────────────────────────────────────────────────────────────┐
  │  Raw Events / DWH Tables                                      │
  │  (BigQuery / Redshift / Parquet files / PostgreSQL)          │
  │             │                                                 │
  │             ▼                                                 │
  │  OfflineStore  ←──  feast materialize  ──→  OnlineStore      │
  │  (historical rows + event_timestamp)         (Redis /         │
  │             │                                 DynamoDB /      │
  │             ▼                                 SQLite)         │
  │  get_historical_features(entity_df, features)                 │
  │             │                                                 │
  │             ▼                                                 │
  │  Training DataFrame (point-in-time correct join)             │
  └───────────────────────────────────────────────────────────────┘

                    ONLINE PATH (real-time serving)
  ┌───────────────────────────────────────────────────────────────┐
  │  entity_rows = [{"user_id": 42}]                             │
  │             │                                                 │
  │             ▼                                                 │
  │  store.get_online_features(features, entity_rows)            │
  │             │                                                 │
  │             ▼                                                 │
  │  OnlineStore lookup  (< 5ms p99)                             │
  │  {"user_id": 42, "user_age": 34, "user_tx_count_30d": 18}   │
  └───────────────────────────────────────────────────────────────┘

Registry (file / SQL / GCS / S3):
  Stores Entity, FeatureView, FeatureService metadata — the schema of the
  feature store. feast apply writes to the registry; all SDK calls read from it.
```

| Concept | Role |
|---|---|
| `Entity` | Primary key of a feature record (e.g., `user_id`, `item_id`) |
| `DataSource` | Where raw feature data lives (BigQuery table, Parquet file, Kafka topic) |
| `FeatureView` | Named group of features sharing the same entity and source, with a TTL |
| `FeatureService` | Logical grouping of FeatureViews (and column subsets) for a model endpoint |
| `OfflineStore` | Columnar store for historical retrieval and materialization reads |
| `OnlineStore` | Low-latency KV store for real-time serving |
| `Registry` | Metadata store — persists all definitions registered via `feast apply` |

---

## Feature Repository Layout

```
feature_repo/
├── feature_store.yaml          # store backend config + registry location
├── data_sources.py             # FileSource / BigQuerySource / PushSource definitions
├── entities.py                 # Entity definitions
├── feature_views.py            # FeatureView / OnDemandFeatureView / StreamFeatureView
├── feature_services.py         # FeatureService groupings per model
└── tests/
    └── test_feature_views.py   # unit tests with local FileSource + SQLite
```

All Python files in the repo root are scanned by `feast apply`. Use `__init__.py` only if you need to share helpers — do not put feature objects in subdirectories unless you configure `feature_repo_path`.

---

## feature_store.yaml Configuration

### Local Development (file offline + SQLite online)

```yaml
# feature_repo/feature_store.yaml
project: my_feature_store
provider: local
registry:
  registry_type: file
  path: data/registry.db          # relative to feature_store.yaml

offline_store:
  type: file                      # reads Parquet files via Arrow

online_store:
  type: sqlite
  path: data/online_store.db

entity_key_serialization_version: 2
```

### Production: PostgreSQL Offline + Redis Online

```yaml
project: my_feature_store
provider: local
registry:
  registry_type: sql
  path: postgresql+psycopg2://feast:${FEAST_PG_PASSWORD}@pg-host:5432/feast_registry

offline_store:
  type: postgres
  host: pg-host
  port: 5432
  database: feast_offline
  user: feast
  password: ${FEAST_PG_PASSWORD}
  db_schema: feast

online_store:
  type: redis
  connection_string: "redis://redis-host:6379"
  # key_ttl_seconds: 86400     # optional global TTL override

entity_key_serialization_version: 2
```

### AWS: Redshift Offline + DynamoDB Online

```yaml
project: my_feature_store
provider: aws
registry:
  registry_type: s3
  path: s3://my-bucket/feast/registry/registry.pb

offline_store:
  type: redshift
  cluster_id: my-redshift-cluster
  region: us-east-1
  database: feast_db
  user: feast_user
  s3_staging_location: s3://my-bucket/feast/staging/
  iam_role: arn:aws:iam::123456789012:role/FeastRedshiftRole

online_store:
  type: dynamodb
  region: us-east-1
  # table_name_template: "{project}.{table}"

entity_key_serialization_version: 2
```

### GCP: BigQuery Offline + Bigtable Online

```yaml
project: my_feature_store
provider: gcp
registry:
  registry_type: gcs
  path: gs://my-bucket/feast/registry/registry.pb

offline_store:
  type: bigquery
  project: my-gcp-project
  dataset: feast_offline

online_store:
  type: bigtable
  project_id: my-gcp-project
  instance: feast-bigtable
  # min_channel_count: 8

entity_key_serialization_version: 2
```

---

## Defining Entities

```python
# feature_repo/entities.py
from feast import Entity, ValueType

# Single-key entity
user = Entity(
    name="user",
    join_keys=["user_id"],
    value_type=ValueType.INT64,
    description="Platform user identified by integer user_id",
    tags={"owner": "ml-platform", "domain": "user"},
)

# String-keyed entity
item = Entity(
    name="item",
    join_keys=["item_id"],
    value_type=ValueType.STRING,
    description="Product catalog item",
)

# Composite entity (multi-key join)
user_item_pair = Entity(
    name="user_item_pair",
    join_keys=["user_id", "item_id"],
    description="User-item interaction pair for ranking models",
)
```

**Entity DataFrame requirements for `get_historical_features`:**
- Must contain all `join_keys` declared on each referenced FeatureView.
- Must contain an `event_timestamp` column (datetime, timezone-aware, UTC).
- Rows represent the "as-of" timestamps: Feast returns the most recent feature row
  whose `event_timestamp` is at or before the entity row's timestamp — no future leakage.
- Labels belong in `entity_df`, never inside a FeatureView.

---

## Defining Data Sources

```python
# feature_repo/data_sources.py
from feast import FileSource, BigQuerySource, RedshiftSource, PushSource, RequestSource
from feast.infra.offline_stores.contrib.postgres_offline_store.postgres_source import PostgreSQLSource
from feast.types import Float32, Int64, String

# ── FileSource (Parquet, local dev or S3) ──────────────────────────────────
user_stats_source = FileSource(
    name="user_stats_source",
    path="data/user_stats.parquet",          # s3:// paths also supported
    timestamp_field="event_timestamp",        # when the row was valid
    created_timestamp_column="created",       # for dedup: keeps most recently created row
    description="Daily user engagement stats snapshot",
)

# ── BigQuerySource ─────────────────────────────────────────────────────────
user_bq_source = BigQuerySource(
    name="user_bq_source",
    table="my-gcp-project.feast_offline.user_features",
    timestamp_field="event_timestamp",
    created_timestamp_column="created_timestamp",
    description="User features from BigQuery warehouse",
)

# ── RedshiftSource ─────────────────────────────────────────────────────────
item_redshift_source = RedshiftSource(
    name="item_redshift_source",
    query="SELECT * FROM feast_db.item_features WHERE dt >= '2024-01-01'",
    timestamp_field="event_timestamp",
    created_timestamp_column="created_timestamp",
    description="Item features from Redshift",
)

# ── PostgreSQLSource (community offline store) ────────────────────────────
user_pg_source = PostgreSQLSource(
    name="user_pg_source",
    query="SELECT user_id, event_timestamp, age, signup_days, tx_count_30d FROM features.user_stats",
    timestamp_field="event_timestamp",
    created_timestamp_column="created_timestamp",
)

# ── PushSource (streaming / real-time pushes from application code) ────────
user_realtime_source = PushSource(
    name="user_realtime_source",
    batch_source=user_stats_source,   # fallback for materialization reads
)

# ── RequestSource (on-demand features computed at request time) ───────────
request_source = RequestSource(
    name="request_context",
    schema=[
        Field(name="order_value", dtype=Float32),
        Field(name="device_type", dtype=String),
    ],
)
```

---

## Defining Feature Views

### Standard FeatureView

```python
# feature_repo/feature_views.py
from datetime import timedelta
from feast import FeatureView, Field, on_demand_feature_view
from feast.types import Float32, Float64, Int64, String, Bool
from feast.stream_feature_view import stream_feature_view
import pandas as pd

from entities import user, item, user_item_pair
from data_sources import (
    user_stats_source, item_redshift_source,
    user_realtime_source, request_source,
)

# ── User features ──────────────────────────────────────────────────────────
user_features_view = FeatureView(
    name="user_features",
    entities=[user],
    ttl=timedelta(days=30),          # feature is stale after 30 days in online store
    schema=[
        Field(name="age",              dtype=Int64),
        Field(name="signup_days",      dtype=Int64),
        Field(name="tx_count_7d",      dtype=Int64),
        Field(name="tx_count_30d",     dtype=Int64),
        Field(name="avg_tx_amount_30d",dtype=Float64),
        Field(name="is_premium",       dtype=Bool),
        Field(name="country_code",     dtype=String),
    ],
    source=user_stats_source,
    tags={"owner": "user-team", "version": "v2"},
    description="Core user behavioral features refreshed daily",
)

# ── Item features ──────────────────────────────────────────────────────────
item_features_view = FeatureView(
    name="item_features",
    entities=[item],
    ttl=timedelta(days=7),
    schema=[
        Field(name="category",         dtype=String),
        Field(name="price_usd",        dtype=Float64),
        Field(name="view_count_7d",    dtype=Int64),
        Field(name="purchase_rate_7d", dtype=Float32),
        Field(name="avg_rating",       dtype=Float32),
        Field(name="is_in_stock",      dtype=Bool),
    ],
    source=item_redshift_source,
    tags={"owner": "catalog-team"},
)

# ── User-Item interaction features ────────────────────────────────────────
user_item_view = FeatureView(
    name="user_item_interactions",
    entities=[user_item_pair],
    ttl=timedelta(days=14),
    schema=[
        Field(name="view_count",        dtype=Int64),
        Field(name="last_viewed_days",  dtype=Int64),
        Field(name="was_purchased",     dtype=Bool),
        Field(name="affinity_score",    dtype=Float32),
    ],
    source=FileSource(
        name="user_item_source",
        path="data/user_item_interactions.parquet",
        timestamp_field="event_timestamp",
    ),
    tags={"owner": "ranking-team"},
)
```

### On-Demand Feature View (Python UDF on request + existing features)

```python
@on_demand_feature_view(
    sources=[user_features_view, request_source],   # combined at retrieval time
    schema=[
        Field(name="tx_order_ratio", dtype=Float64),
        Field(name="is_high_value",  dtype=Bool),
        Field(name="risk_tier",      dtype=String),
    ],
    description="Derived risk features computed on the fly",
)
def user_risk_features(inputs: pd.DataFrame) -> pd.DataFrame:
    # inputs has columns from user_features_view + request_source
    # Must return DataFrame with exactly the declared schema columns
    df = pd.DataFrame()
    df["tx_order_ratio"] = inputs["tx_count_30d"] / inputs["order_value"].clip(lower=0.01)
    df["is_high_value"]  = (inputs["avg_tx_amount_30d"] > 500.0) & (inputs["tx_count_30d"] > 10)
    df["risk_tier"]      = pd.cut(
        inputs["tx_count_30d"], bins=[-1, 2, 10, 50, float("inf")],
        labels=["new", "low", "medium", "high"],
    ).astype(str)
    return df
```

### Stream Feature View (Kafka real-time features)

```python
from feast import KafkaSource
from feast.data_format import JsonFormat

kafka_source = KafkaSource(
    name="user_events_kafka",
    kafka_bootstrap_servers="kafka-broker:9092",
    topic="user_events",
    batch_source=user_stats_source,          # fallback for offline materialization
    timestamp_field="event_timestamp",
    message_format=JsonFormat(schema_json=(
        '{"type":"record","name":"UserEvent","fields":['
        '{"name":"user_id","type":"long"},{"name":"event_timestamp","type":"string"},'
        '{"name":"session_tx_count","type":"int"},{"name":"session_value_usd","type":"double"}]}'
    )),
    watermark_delay_threshold=timedelta(minutes=5),
)

@stream_feature_view(
    entities=[user], ttl=timedelta(hours=2), mode="spark", online=True,
    source=kafka_source, timestamp_field="event_timestamp",
    schema=[
        Field(name="session_tx_count", dtype=Int64),
        Field(name="session_value_usd", dtype=Float64),
    ],
    tags={"owner": "streaming-team"},
)
def user_session_features(df: pd.DataFrame) -> pd.DataFrame:
    # Called on each Spark micro-batch from Kafka
    return df[["user_id", "event_timestamp", "session_tx_count", "session_value_usd"]]
```

---

## Feature Services

```python
# feature_repo/feature_services.py
from feast import FeatureService, FeatureLoggingConfig
from feature_views import (
    user_features_view,
    item_features_view,
    user_item_view,
    user_risk_features,
    user_session_features,
)

# ── Ranking model v1 ───────────────────────────────────────────────────────
ranking_v1_service = FeatureService(
    name="ranking_model_v1",
    features=[
        user_features_view[[
            "tx_count_7d", "tx_count_30d", "avg_tx_amount_30d",
            "is_premium", "country_code",
        ]],
        item_features_view[["category", "price_usd", "view_count_7d", "purchase_rate_7d"]],
        user_item_view,                # all fields
        user_risk_features,            # on-demand derived features
    ],
    description="Features for ranking model v1 — request-time risk tier included",
    logging_config=FeatureLoggingConfig(
        destination=FileLoggingDestination(path="data/feature_logs/"),
    ),
    tags={"model": "ranking", "version": "v1"},
)

# ── Fraud detection service (online only, no user-item interaction) ────────
fraud_service = FeatureService(
    name="fraud_detection_v2",
    features=[
        user_features_view[[
            "age", "signup_days", "tx_count_7d", "tx_count_30d",
            "avg_tx_amount_30d", "country_code",
        ]],
        user_session_features,         # real-time session stream features
    ],
    description="Feature service for real-time fraud detection v2",
    tags={"model": "fraud", "version": "v2"},
)
```

**Version management:** Create a new `FeatureService` with an incremented version suffix for incompatible changes. Old services keep serving until all dependent model versions are retired — never delete an active service.

---

## feast CLI Workflow

```bash
# Initialize a new feature repo (run once)
feast init my_feature_store && cd my_feature_store/feature_repo

# Register all Python objects to the registry
feast apply
# → Registered entities: user, item | feature views: user_features, ...

# Full materialization for a time window → writes to OnlineStore
feast materialize 2024-10-01T00:00:00 2024-11-01T00:00:00

# Incremental materialize since last high-watermark (run daily via Airflow)
feast materialize-incremental $(date -u +"%Y-%m-%dT%H:%M:%S")

# Start local feature server REST API
feast serve --host 0.0.0.0 --port 6566

# Open Feast UI (requires feast[ui] extra)
feast ui --host 0.0.0.0 --port 8888

# Inspect registry
feast feature-views list && feast feature-services list && feast entities list

# Tear down online store tables (does NOT delete registry or offline data)
feast teardown
```

---

## Historical Features (Offline, for Training)

### Point-in-Time Correct Join Explained

```
entity_df rows (training labels with timestamps):

  user_id | event_timestamp       | label
  --------|----------------------|-------
  1001    | 2024-11-01 08:00 UTC | 1
  1001    | 2024-11-03 07:00 UTC | 0

feature_view data (user_features, multiple snapshots):

  user_id | event_timestamp       | tx_count_30d | avg_tx_amount
  --------|----------------------|--------------|---------------
  1001    | 2024-10-29 00:00 UTC | 15           | 220.50
  1001    | 2024-11-01 00:00 UTC | 18           | 235.00   ← used for label row at 08:00
  1001    | 2024-11-02 00:00 UTC | 19           | 238.00
  1001    | 2024-11-03 00:00 UTC | 20           | 240.00   ← used for label row at 07:00

Result after point-in-time join:

  user_id | event_timestamp       | tx_count_30d | avg_tx_amount | label
  --------|----------------------|--------------|---------------|-------
  1001    | 2024-11-01 08:00 UTC | 18           | 235.00        | 1
  1001    | 2024-11-03 07:00 UTC | 20           | 240.00        | 0

Feast picks the LATEST feature row whose event_timestamp ≤ entity event_timestamp.
This prevents future leakage — the model never sees feature values it couldn't have
known at training/label creation time.
```

### Training Data Pipeline

```python
import pandas as pd
from feast import FeatureStore

store = FeatureStore(repo_path="feature_repo/")

# entity_df: labeled events from your DWH — event_timestamp is the "as-of" time
entity_df = pd.DataFrame({
    "user_id": [1001, 1002, 1003, 1004, 1005],
    "item_id": ["sku-A", "sku-B", "sku-C", "sku-A", "sku-D"],
    "event_timestamp": pd.to_datetime([
        "2024-11-01 08:00:00", "2024-11-01 09:30:00", "2024-11-02 14:00:00",
        "2024-11-03 07:00:00", "2024-11-03 11:00:00",
    ]).tz_localize("UTC"),
    "label": [1, 0, 1, 0, 1],
})

# Option A: specify feature views individually
retrieval_job = store.get_historical_features(
    entity_df=entity_df,
    features=[
        "user_features:tx_count_7d", "user_features:tx_count_30d",
        "user_features:avg_tx_amount_30d", "user_features:is_premium",
        "item_features:category", "item_features:price_usd",
        "user_item_interactions:affinity_score",
    ],
)
# Option B: reference a FeatureService (preferred — decouples from view names)
# retrieval_job = store.get_historical_features(
#     entity_df=entity_df,
#     features=store.get_feature_service("ranking_model_v1"),
# )

training_df = retrieval_job.to_df()              # → Pandas
training_table = retrieval_job.to_arrow()        # → PyArrow (memory efficient)
# retrieval_job.to_spark_df()                    # → Spark DataFrame (BigQuery/Spark offline)

print(training_df.isnull().sum())   # rows with NULL = fell outside TTL or not in source
```

---

## Online Features (Serving)

### Low-Latency Lookup with Python SDK

```python
from feast import FeatureStore
import pandas as pd

store = FeatureStore(repo_path="feature_repo/")

# Single entity — individual feature names
feature_dict = store.get_online_features(
    features=["user_features:tx_count_7d","user_features:tx_count_30d",
              "user_features:avg_tx_amount_30d","user_features:is_premium"],
    entity_rows=[{"user_id": 1001}],
).to_dict()
# {"user_id": [1001], "tx_count_7d": [12], "tx_count_30d": [40], ...}

# Batch lookup via FeatureService (preferred — up to ~1000 rows per call)
features_df = pd.DataFrame(
    store.get_online_features(
        features=store.get_feature_service("ranking_model_v1"),
        entity_rows=[{"user_id": uid} for uid in user_ids_to_score],
    ).to_dict()
)
```

### REST API via `feast serve`

```bash
feast serve --host 0.0.0.0 --port 6566

curl -X POST http://localhost:6566/get-online-features \
  -H "Content-Type: application/json" \
  -d '{"feature_service":"fraud_detection_v2","entities":{"user_id":[42,99,314]}}'
# → {"metadata":{"feature_names":["user_id","tx_count_7d",...]},
#    "results":[{"values":[42,12,...],"statuses":["PRESENT","PRESENT",...]},...]}
```

### FastAPI Serving Endpoint

```python
# serving/app.py — FeatureStore instantiated once at startup via lifespan
from contextlib import asynccontextmanager
from typing import List
import pandas as pd
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from feast import FeatureStore

class ScoreRequest(BaseModel):
    user_ids: List[int]
    order_values: List[float]   # passed to on-demand feature view

class ScoreResponse(BaseModel):
    user_ids: List[int]
    fraud_scores: List[float]

store: FeatureStore | None = None

@asynccontextmanager
async def lifespan(app: FastAPI):
    global store
    store = FeatureStore(repo_path="/app/feature_repo/")   # read registry once
    yield
    store = None

app = FastAPI(title="Fraud Scoring API", lifespan=lifespan)

@app.post("/score", response_model=ScoreResponse)
async def score(req: ScoreRequest) -> ScoreResponse:
    if store is None:
        raise HTTPException(status_code=503, detail="Feature store not initialized")
    response = store.get_online_features(
        features=store.get_feature_service("fraud_detection_v2"),
        entity_rows=[{"user_id": uid} for uid in req.user_ids],
    )
    feat = pd.DataFrame(response.to_dict())
    feat["order_value"] = req.order_values
    feature_cols = ["tx_count_7d","tx_count_30d","avg_tx_amount_30d","signup_days","session_tx_count","order_value"]
    scores = model.predict_proba(feat[feature_cols])[:, 1].tolist()
    return ScoreResponse(user_ids=req.user_ids, fraud_scores=scores)
```

---

## Push Sources (Streaming / Real-Time)

### Push Live Data Directly to Online Store

```python
import pandas as pd
from feast import FeatureStore
from feast.infra.common.materialization.batch_materialization_engine import PushMode
from datetime import datetime, timezone

store = FeatureStore(repo_path="feature_repo/")

push_df = pd.DataFrame({
    "user_id":           [1001, 1002],
    "session_tx_count":  [3, 7],
    "session_value_usd": [45.20, 190.50],
    "event_timestamp":   [datetime.now(tz=timezone.utc)] * 2,
})

# PushMode.ONLINE          → OnlineStore only (low-latency serving)
# PushMode.ONLINE_AND_OFFLINE → both stores (full audit trail)
store.push("user_realtime_source", push_df, to=PushMode.ONLINE)
```

### Direct Write to Online Store

```python
# Bypass push source — write directly by FeatureView name (hot path)
store.write_to_online_store("user_features", push_df, allow_registry_cache=True)
```

---

## Airflow Integration

### Daily Incremental Materialization DAG

```python
# dags/feast_materialize_dag.py
import pendulum
from datetime import timedelta
from airflow.sdk import dag, task

FEATURE_REPO_PATH = "/opt/airflow/feature_repo"


@dag(
    dag_id="feast_daily_materialize",
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    schedule="0 3 * * *",          # 03:00 UTC daily
    catchup=False,
    default_args={"retries": 2, "retry_delay": timedelta(minutes=5)},
    tags=["feast", "feature-store"],
)
def feast_daily_materialize():

    @task()
    def materialize_incremental() -> str:
        import subprocess
        from datetime import datetime, timezone, timedelta
        end_ts = datetime.now(tz=timezone.utc) - timedelta(minutes=10)
        end_str = end_ts.strftime("%Y-%m-%dT%H:%M:%S")
        subprocess.run(
            ["feast", "-c", FEATURE_REPO_PATH, "materialize-incremental", end_str],
            capture_output=True, text=True, check=True,
        )
        return end_str

    @task()
    def validate_online_store(end_ts: str) -> None:
        from feast import FeatureStore
        store = FeatureStore(repo_path=FEATURE_REPO_PATH)
        result = store.get_online_features(
            features=["user_features:tx_count_30d"],
            entity_rows=[{"user_id": 1001}],   # canary entity always in data
        ).to_dict()
        if result.get("tx_count_30d", [None])[0] is None:
            raise ValueError(f"Canary user_id=1001 NULL after materialize at {end_ts}")

    result = materialize_incremental()
    validate_online_store(result)


feast_daily_materialize()
```

### Training Pipeline DAG

```python
# dags/feast_training_pipeline.py
import pendulum
from airflow.sdk import dag, task

FEATURE_REPO_PATH = "/opt/airflow/feature_repo"
S3_TRAINING_PATH  = "s3://my-bucket/training/ranking_v1"


@dag(dag_id="feast_training_pipeline", start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
     schedule="@weekly", catchup=False, tags=["feast", "ml", "ranking"])
def feast_training_pipeline():

    @task()
    def extract_entity_df(ds: str) -> str:
        import pandas as pd
        entity_df = pd.DataFrame({
            "user_id": range(1000),
            "item_id": ["sku-" + str(i % 50) for i in range(1000)],
            "event_timestamp": pd.date_range(end=ds, periods=1000, freq="1H", tz="UTC"),
            "label": [i % 2 for i in range(1000)],
        })
        path = f"{S3_TRAINING_PATH}/entity_df/{ds}/entity_df.parquet"
        entity_df.to_parquet(path, index=False)
        return path

    @task()
    def get_historical_features(entity_df_path: str) -> str:
        import pandas as pd
        from feast import FeatureStore
        store = FeatureStore(repo_path=FEATURE_REPO_PATH)
        entity_df = pd.read_parquet(entity_df_path)
        entity_df["event_timestamp"] = pd.to_datetime(entity_df["event_timestamp"], utc=True)
        job = store.get_historical_features(
            entity_df=entity_df,
            features=store.get_feature_service("ranking_model_v1"),
        )
        out_path = entity_df_path.replace("entity_df", "training_data")
        job.to_df().to_parquet(out_path, index=False)
        return out_path

    @task()
    def train_and_register(training_data_path: str, ds: str) -> None:
        import mlflow, pandas as pd
        from sklearn.ensemble import GradientBoostingClassifier
        from sklearn.model_selection import train_test_split
        from sklearn.metrics import roc_auc_score
        df = pd.read_parquet(training_data_path).dropna()
        feat_cols = [c for c in df.columns if c not in ("user_id", "item_id", "event_timestamp", "label")]
        X_train, X_val, y_train, y_val = train_test_split(df[feat_cols], df["label"], test_size=0.2)
        with mlflow.start_run(run_name=f"ranking_v1_{ds}"):
            mlflow.log_params({"n_estimators": 300, "max_depth": 5, "feast_service": "ranking_model_v1"})
            model = GradientBoostingClassifier(n_estimators=300, max_depth=5).fit(X_train, y_train)
            mlflow.log_metric("val_auc", roc_auc_score(y_val, model.predict_proba(X_val)[:, 1]))
            mlflow.sklearn.log_model(model, "model", registered_model_name="ranking.v1")

    entity_path   = extract_entity_df()
    training_path = get_historical_features(entity_path)
    train_and_register(training_path)


feast_training_pipeline()
```

### feast apply in CI/CD

```python
@task()
def feast_apply() -> None:
    import subprocess
    result = subprocess.run(
        ["feast", "-c", FEATURE_REPO_PATH, "apply"],
        capture_output=True, text=True, check=True,
    )
    if "ERROR" in result.stderr:
        raise RuntimeError(f"feast apply failed:\n{result.stderr}")
```

---

## Testing Feature Views

Use `FileSource` + in-memory `SQLiteOnlineStore` + `RepoConfig` to test feature views without a real warehouse.

```python
# feature_repo/tests/test_feature_views.py
import os, tempfile
from datetime import timedelta, datetime, timezone
import pandas as pd
import pytest
from feast import FeatureStore, FeatureView, Entity, Field, FileSource
from feast.types import Int64, Float64
from feast.repo_config import RepoConfig, RegistryConfig
from feast.infra.offline_stores.file import FileOfflineStoreConfig
from feast.infra.online_stores.sqlite import SqliteOnlineStoreConfig


@pytest.fixture(scope="module")
def store_with_data():
    with tempfile.TemporaryDirectory() as tmp:
        # Create sample Parquet data — two rows for user_id=1001 at different timestamps
        df = pd.DataFrame({
            "user_id":         [1001, 1002, 1001],
            "event_timestamp": pd.to_datetime(["2024-11-01","2024-11-01","2024-11-02"]).tz_localize("UTC"),
            "created":         pd.to_datetime(["2024-11-01","2024-11-01","2024-11-02"]).tz_localize("UTC"),
            "tx_count_30d":    [10, 20, 12],
            "avg_tx_amount":   [150.0, 300.0, 160.0],
        })
        parquet_path = os.path.join(tmp, "user_stats.parquet")
        df.to_parquet(parquet_path, index=False)

        source = FileSource(name="user_stats_test", path=parquet_path,
                            timestamp_field="event_timestamp",
                            created_timestamp_column="created")
        user_entity = Entity(name="user", join_keys=["user_id"])
        user_view = FeatureView(
            name="user_features_test", entities=[user_entity], ttl=timedelta(days=30),
            schema=[Field(name="tx_count_30d", dtype=Int64),
                    Field(name="avg_tx_amount", dtype=Float64)],
            source=source,
        )
        config = RepoConfig(
            project="test_project",
            registry=RegistryConfig(registry_type="file", path=os.path.join(tmp, "registry.db")),
            provider="local",
            offline_store=FileOfflineStoreConfig(),
            online_store=SqliteOnlineStoreConfig(path=os.path.join(tmp, "online.db")),
            entity_key_serialization_version=2,
        )
        store = FeatureStore(config=config)
        store.apply([user_entity, source, user_view])
        yield store


def test_point_in_time_join(store_with_data):
    """user_id=1001 at 2024-11-02 must see tx_count_30d=12, not 10."""
    entity_df = pd.DataFrame({
        "user_id": [1001, 1002],
        "event_timestamp": pd.to_datetime(["2024-11-02","2024-11-01"]).tz_localize("UTC"),
    })
    df = store_with_data.get_historical_features(
        entity_df=entity_df,
        features=["user_features_test:tx_count_30d"],
    ).to_df()
    assert df.loc[df["user_id"] == 1001, "tx_count_30d"].iloc[0] == 12
    assert df.loc[df["user_id"] == 1002, "tx_count_30d"].iloc[0] == 20


def test_online_after_materialize(store_with_data):
    store_with_data.materialize(
        start_date=datetime(2024, 10, 31, tzinfo=timezone.utc),
        end_date=datetime(2024, 11, 3, tzinfo=timezone.utc),
    )
    result = store_with_data.get_online_features(
        features=["user_features_test:tx_count_30d"],
        entity_rows=[{"user_id": 1001}, {"user_id": 9999}],
    ).to_dict()
    assert result["tx_count_30d"][0] == 12       # latest value
    assert result["tx_count_30d"][1] is None     # unknown entity → None
```

```bash
pytest feature_repo/tests/test_feature_views.py -v
```

---

## Anti-Patterns

| Anti-Pattern | Why It Hurts | Fix |
|---|---|---|
| Not using point-in-time joins for training data | Future leakage — model sees features that weren't available at label creation time; inflated offline AUC | Always use `store.get_historical_features(entity_df)` with correct `event_timestamp`; never a plain SQL join |
| Missing or infinite `ttl` on FeatureView | Stale features silently served in online store after months; causes training-serving skew | Set `ttl` to the maximum staleness acceptable for the model (e.g., `timedelta(days=7)`) |
| Running `feast apply` in production without CI/CD review | Breaking changes to feature schemas corrupt downstream models | Gate `feast apply` on a PR merge; validate with `feast apply --dry-run` in CI |
| One FeatureView for dozens of unrelated features | Schema bloat; all entities must share the same join key; partial refreshes invalidate unrelated features | One FeatureView per cohesive entity-feature group; separate TTLs for slow/fast-changing features |
| Mixing training labels into FeatureView schema | Labels must not be stored as features — they will leak into training data retrieved at prediction time | Store labels only in the `entity_df` passed to `get_historical_features`; never in a FeatureView |
| Calling `store = FeatureStore(...)` inside each request handler | Re-reads registry on every call; adds 10-100ms latency | Instantiate `FeatureStore` once at app startup (FastAPI lifespan, module-level global) |
| Materializing with wide time windows in incremental runs | Reads huge offline data; slow; risk of OOM | Use `materialize-incremental` daily; only do full-window `materialize` on initial setup |
| No `created_timestamp_column` in FileSource / BigQuerySource | Dedup is non-deterministic when multiple rows share the same `event_timestamp` | Always provide `created_timestamp_column`; Feast uses it to pick the latest-created row among ties |
| Using SQLite online store beyond local dev | SQLite has no concurrent write support; degrades under load | Use Redis (self-hosted or ElastiCache) or DynamoDB in staging/production |
| Deleting a FeatureService that is still used by a live model | Breaks `get_online_features` calls in serving — `FeatureService not found` | Deprecate by adding a `_deprecated` tag; remove only after all model versions are retired |

---

## Output Expectations

- Every training pipeline calls `store.get_historical_features(entity_df, features)` with timezone-aware `event_timestamp` column — never a plain warehouse join.
- `FeatureView` definitions always include `ttl` and `tags` with owner and version.
- Production deployments use PostgreSQL or BigQuery offline store and Redis or DynamoDB online store — never SQLite.
- `feast apply` is gated on a CI/CD pipeline; never run manually against production registry.
- `feast materialize-incremental` runs daily via an Airflow DAG with a validation task.
- `FeatureStore` is instantiated once at process startup, not inside request handlers.
- On-demand features (`@on_demand_feature_view`) are used for features that depend on real-time request context, not for features that can be precomputed.

---

## References to Consult When Needed

- Feast documentation: https://docs.feast.dev/
- Feast Python SDK reference: https://rtd.feast.dev/
- Feast feature repo examples: https://github.com/feast-dev/feast/tree/master/examples
- Feast Redis online store setup: https://docs.feast.dev/reference/online-stores/redis
- Feast Redshift offline store: https://docs.feast.dev/reference/offline-stores/redshift
- Feast BigQuery offline store: https://docs.feast.dev/reference/offline-stores/bigquery
- Feast PostgreSQL offline store (community): https://docs.feast.dev/reference/offline-stores/postgres
- Feast streaming feature views: https://docs.feast.dev/reference/alpha-stream-ingestion
- Feast push sources: https://docs.feast.dev/reference/data-sources/push
- Point-in-time join explanation: https://docs.feast.dev/getting-started/concepts/point-in-time-joins
- Feast on-demand feature views: https://docs.feast.dev/reference/on-demand-feature-view
- Feast Airflow provider (community): https://github.com/feast-dev/feast-airflow-provider
