---
name: ray-data
description: Ray Data distributed data processing — Dataset API, read_parquet/read_csv/read_json/read_delta_sharing, map/filter/flat_map/map_batches, groupby/aggregations, Actors for stateful transforms, Ray remote functions, write_parquet/write_iceberg, streaming execution, GPU batch inference, integration with Spark and Airflow
---

# Ray Data Engineer

## When to Use

Load this skill when the user needs to:
- Build distributed data pipelines in Python using Ray Data (ray.data)
- Perform batch GPU inference over large datasets (image, text, tabular models)
- Write Python-native ML preprocessing pipelines that feed into Ray Train or Ray Serve
- Process heterogeneous workloads where Spark's JVM overhead or Dask's scheduler is a bottleneck
- Integrate data pipelines with Ray clusters on Kubernetes (KubeRay), EC2, or GCP
- Submit Ray jobs from Airflow using RayJobOperator or the Jobs API

### Ray Data vs Spark vs Dask

| Dimension | Ray Data | Spark | Dask |
|-----------|----------|-------|------|
| Primary strength | GPU inference, Python-native ML, heterogeneous compute | Large-scale SQL/ETL, mature ecosystem | Pandas-like API, lightweight scheduler |
| Execution model | Streaming, actor-based, zero-copy Arrow | DAG, JVM shuffle, stage-based | Task graph, Python scheduler |
| GPU support | Native (num_gpus per actor) | Limited (via Arrow Flight) | Via cuDF only |
| Python ecosystem | First-class (PyTorch, HuggingFace, sklearn) | JVM-centric, pandas UDF workarounds | Native Python |
| Shuffle / joins | Limited (no broadcast, no sort-merge SQL join) | Excellent | Moderate |
| SQL aggregations | Via DuckDB / Arrow bridge | Native SparkSQL | Via Dask-SQL / cuDF |
| Operational maturity | Growing (2.x) | Mature | Mature |

**Ray wins when**: you need GPU batch inference, want to unify data preprocessing and model training in one cluster, or have heterogeneous tasks with varying CPU/GPU/memory requirements.

**Spark wins when**: you have heavy SQL joins, need partition-level AQE, or operate in a Hive/Lakehouse environment with mature connectors.

**Dask wins when**: you want to run pandas-compatible code on a laptop-scale cluster without learning new APIs.

---

## Core Concepts

```
ray.data.Dataset
  └── Blocks (Arrow Tables, partitioned in-memory)
        └── Stored in Ray Object Store (shared memory, zero-copy)

Execution engine (Ray 2.x):
  ├── Lazy by default — operators are pipelined; no work until triggered
  ├── Streaming execution — blocks flow operator-to-operator without full materialization
  ├── .materialize()  — forces full execution and pins all blocks in object store
  └── Autoscaler     — Ray cluster scales workers up/down based on resource demand

Key classes:
  ray.data.Dataset          — logical plan of operators over blocks
  ray.data.Datasource       — pluggable read interface
  ray.data.Datasink         — pluggable write interface
  ray.actor.ActorHandle     — stateful compute unit for class-based map_batches
  ray.ObjectRef             — reference to data in the object store
```

### Lazy vs Eager Execution

Ray Data 2.x is **lazy** by default. The pipeline is assembled as a logical plan; execution starts when you consume the dataset (`.iter_batches()`, `.take()`, `.write_*()`, `.materialize()`). Avoid `.materialize()` in loops — it pins every block in the object store.

```python
import ray
import ray.data

ray.init()  # local; for cluster: ray.init(address="ray://head-node:10001")

# Build a lazy pipeline — no work yet
ds = (
    ray.data.read_parquet("s3://my-bucket/raw/events/")
    .filter(lambda row: row["event_type"] == "purchase")
    .map(lambda row: {**row, "amount_usd": row["amount"] / 100.0})
)

# Execution starts here:
ds.write_parquet("s3://my-bucket/processed/purchases/")
```

---

## Reading Data

### Parquet (S3, GCS, local)

```python
import ray.data

# S3 — auto-detects credentials from environment / IAM role
ds = ray.data.read_parquet(
    "s3://my-bucket/warehouse/events/dt=2026-05-17/",
    columns=["user_id", "event_type", "amount", "ts"],  # column pruning
)

# GCS
ds_gcs = ray.data.read_parquet(
    "gs://my-gcs-bucket/warehouse/events/",
    filesystem=None,   # auto-detected via pyarrow.fs.GcsFileSystem
)

# Local path
ds_local = ray.data.read_parquet("/mnt/data/events/")

# Control parallelism: one block per file by default; override if files are large
ds = ray.data.read_parquet(
    "s3://my-bucket/warehouse/events/",
    override_num_blocks=200,   # target block count; Ray splits files accordingly
)
```

### read_parquet_bulk — metadata-free fast reads

Use when files lack per-file row-group statistics and you do not need predicate pushdown:

```python
# Skips footer metadata scan; faster for very large file lists
ds = ray.data.read_parquet_bulk(
    paths=[
        "s3://my-bucket/raw/events/part-00001.parquet",
        "s3://my-bucket/raw/events/part-00002.parquet",
    ],
    override_num_blocks=50,
)
```

### CSV and JSON

```python
ds_csv  = ray.data.read_csv("s3://my-bucket/raw/users/")
ds_json = ray.data.read_json("s3://my-bucket/raw/events_json/", override_num_blocks=100)
```

### Custom Datasource

Implement `ray.data.Datasource` when reading from databases, REST APIs, or binary formats. Implement `get_read_tasks()` to return a list of `ReadTask` callables — one per parallel chunk:

```python
from ray.data import Datasource, ReadTask
from ray.data.block import BlockMetadata
import pyarrow as pa


class PostgresChunkDatasource(Datasource):
    """Read a PostgreSQL table in parallel offset chunks."""

    def __init__(self, dsn: str, table: str, chunk_size: int = 50_000):
        self._dsn, self._table, self._chunk_size = dsn, table, chunk_size

    def get_read_tasks(self, parallelism: int) -> list[ReadTask]:
        import psycopg2
        with psycopg2.connect(self._dsn) as conn:
            with conn.cursor() as cur:
                cur.execute(f"SELECT COUNT(*) FROM {self._table}")
                total = cur.fetchone()[0]

        def make_task(offset: int):
            meta = BlockMetadata(
                num_rows=min(self._chunk_size, total - offset),
                size_bytes=None, schema=None, input_files=None, exec_stats=None,
            )
            dsn, table, size = self._dsn, self._table, self._chunk_size

            def read_fn():
                import psycopg2, pyarrow as pa
                with psycopg2.connect(dsn) as conn:
                    with conn.cursor() as cur:
                        cur.execute(f"SELECT * FROM {table} LIMIT %s OFFSET %s", (size, offset))
                        cols = [d[0] for d in cur.description]
                        rows = cur.fetchall()
                return [pa.table({c: [r[i] for r in rows] for i, c in enumerate(cols)})]

            return ReadTask(read_fn, meta)

        return [make_task(o) for o in range(0, total, self._chunk_size)]


ds = ray.data.read_datasource(
    PostgresChunkDatasource("postgresql://user:pass@db-host:5432/analytics", "events")
)
```

---

## Transformations

### map — row-level transforms

Input/output are Python dicts (one row at a time). Prefer `map_batches` for vectorized ops.

```python
from datetime import datetime

ds_enriched = ds.map(lambda row: {
    **row,
    "event_date": datetime.utcfromtimestamp(row["ts"]).date().isoformat(),
    "amount_usd": round(row["amount_cents"] / 100.0, 2),
})
```

### map_batches — vectorized batch transforms

The workhorse transformation. Accepts pandas DataFrame, numpy dict, or PyArrow Table. Use `batch_format` to control what your function receives.

```python
import pandas as pd

# Pandas batch — mutate columns with vectorized operations
def normalize_amounts(batch: pd.DataFrame) -> pd.DataFrame:
    batch = batch.copy()
    batch["amount_normalized"] = (batch["amount_usd"] - batch["amount_usd"].mean()) / batch["amount_usd"].std()
    batch["country"] = batch["country"].str.upper()
    return batch

ds_normalized = ds.map_batches(
    normalize_amounts,
    batch_format="pandas",   # "pandas" | "numpy" | "pyarrow" (default Arrow)
    batch_size=4096,
)

# PyArrow batch — zero-copy columnar ops via pyarrow.compute
import pyarrow.compute as pc

def tag_high_value(batch: pa.Table) -> pa.Table:
    return batch.append_column("is_high_value", pc.greater(batch["amount_usd"], 500.0))

ds_tagged = ds.map_batches(tag_high_value, batch_format="pyarrow", batch_size=8192)
```

### filter and flat_map

```python
# filter — predicate on a dict row; keep row if True
ds_purchases = ds.filter(lambda row: row["event_type"] == "purchase" and row["amount_usd"] > 0)

# flat_map — one row in, zero or more rows out; useful for exploding arrays
def explode_items(row: dict) -> list[dict]:
    return [
        {"order_id": row["order_id"], "item_id": item_id, "ts": row["ts"]}
        for item_id in row["item_ids"]   # item_ids is a list column
    ]

ds_items = ds_orders.flat_map(explode_items)
```

### Column operations

```python
ds = ds.add_column("event_year", lambda batch: batch["event_date"].str[:4])
ds = ds.drop_columns(["internal_flag", "raw_payload"])
ds = ds.rename_columns({"user_id": "customer_id", "ts": "event_ts"})
ds = ds.select_columns(["customer_id", "event_type", "amount_usd", "event_date"])
```

### sort and limit

```python
ds_sorted = ds.sort("ts", descending=True)   # triggers a distributed shuffle; expensive
sample    = ds.limit(10_000)                 # efficient — stops after N rows
```

---

## Groupby and Aggregations

Ray Data supports grouped aggregations via `.groupby().aggregate()`. For complex multi-column SQL-style aggregations, prefer the DuckDB bridge.

```python
from ray.data.aggregate import Sum, Count, Mean, Max, Min, Std

ds = ray.data.read_parquet("s3://my-bucket/raw/events/")

# Simple single-column aggregation
agg_ds = (
    ds
    .filter(lambda row: row["event_type"] == "purchase")
    .groupby("country")
    .aggregate(
        Sum("amount_usd", alias="total_revenue"),
        Count("user_id", alias="num_transactions"),
        Mean("amount_usd", alias="avg_order_value"),
        Max("amount_usd", alias="max_order_value"),
        Min("amount_usd", alias="min_order_value"),
    )
)

# Multi-key groupby
daily_country = (
    ds
    .groupby(["event_date", "country"])
    .aggregate(
        Sum("amount_usd", alias="revenue"),
        Count(None, alias="event_count"),     # Count(None) counts rows
    )
)
```

### Custom AggregateFn

Implement `AggregateFn` when built-in aggregates are insufficient. Provide `init`, `accumulate_row`, `merge`, and `finalize` callables:

```python
from ray.data.aggregate import AggregateFn

class WeightedMean(AggregateFn):
    def __init__(self, value_col: str, weight_col: str, alias: str):
        self._v, self._w = value_col, weight_col
        super().__init__(
            name=alias,
            init=lambda k: (0.0, 0.0),
            accumulate_row=lambda acc, row: (acc[0] + row[self._v] * row[self._w], acc[1] + row[self._w]),
            merge=lambda a, b: (a[0] + b[0], a[1] + b[1]),
            finalize=lambda acc: acc[0] / acc[1] if acc[1] else None,
        )

ds.groupby("product_category").aggregate(WeightedMean("price", "quantity", "weighted_avg_price"))
```

---

## Stateful Transforms with Actors

Use class-based `map_batches` when transform state must be initialized once per worker (model loading, DB connections, tokenizers).

### Pattern: class-based stateful transform

```python
import ray.data, pandas as pd

class FraudScorer:
    """Model loaded once in __init__; reused across all __call__ batches on this actor."""

    def __init__(self):
        import joblib
        self._model = joblib.load("/models/fraud_detector_v3.pkl")
        self._feature_cols = ["amount_usd", "hour_of_day", "country_code", "device_type"]

    def __call__(self, batch: pd.DataFrame) -> pd.DataFrame:
        batch = batch.copy()
        batch["fraud_score"] = self._model.predict_proba(batch[self._feature_cols].fillna(0))[:, 1]
        batch["is_fraud"] = batch["fraud_score"] > 0.85
        return batch


(
    ray.data.read_parquet("s3://my-bucket/features/transactions/dt=2026-05-17/")
    .map_batches(FraudScorer, concurrency=8, batch_size=2048, batch_format="pandas", num_cpus=2)
    .write_parquet("s3://my-bucket/scored/transactions/dt=2026-05-17/")
)
```

### GPU Batch Inference Pattern

```python
import ray.data, torch, pandas as pd

class TextEmbedder:
    """HuggingFace sentence-transformers on GPU — model loaded once per actor."""

    def __init__(self):
        from sentence_transformers import SentenceTransformer
        self._model = SentenceTransformer(
            "sentence-transformers/all-MiniLM-L6-v2", device="cuda"
        )
        self._model.eval()

    def __call__(self, batch: pd.DataFrame) -> pd.DataFrame:
        with torch.no_grad():
            embeddings = self._model.encode(
                batch["review_text"].tolist(), batch_size=64, convert_to_numpy=True
            )
        batch = batch.copy()
        batch["embedding"] = list(embeddings)
        return batch[["review_id", "embedding"]]


(
    ray.data.read_parquet(
        "s3://my-bucket/raw/product_reviews/",
        columns=["review_id", "review_text"],
        override_num_blocks=400,
    )
    .map_batches(
        TextEmbedder,
        concurrency=4,      # 4 GPU actors — one GPU each
        num_gpus=1,
        num_cpus=4,
        batch_size=256,
        batch_format="pandas",
    )
    .write_parquet("s3://my-bucket/embeddings/product_reviews/")
)
```

### Memory resource hints

```python
# Reserve 32 GB per actor replica for models with large in-memory state
ds.map_batches(LargeModelActor, concurrency=2, num_gpus=1, num_cpus=8,
               memory=32 * 1024 ** 3, batch_size=512)
```

---

## Ray Remote Functions

Use `@ray.remote` for task-level parallelism — independent units of work outside the Dataset API. Tasks are non-blocking; collect results with `ray.get()`.

```python
import ray, pyarrow.parquet as pq

ray.init()

@ray.remote(num_cpus=2)
def validate_partition(s3_path: str) -> dict:
    table = pq.read_table(s3_path)
    total = len(table)
    return {"path": s3_path, "rows": total,
            "null_rates": {c: table[c].null_count / total for c in table.schema.names}}

paths = [
    "s3://my-bucket/raw/events/dt=2026-05-15/part-00001.parquet",
    "s3://my-bucket/raw/events/dt=2026-05-16/part-00001.parquet",
    "s3://my-bucket/raw/events/dt=2026-05-17/part-00001.parquet",
]

stats = ray.get([validate_partition.remote(p) for p in paths])
for s in stats:
    print(s["path"], s["rows"])
```

### ray.put — sharing large objects across tasks

```python
import pandas as pd, ray

# Serialize once; all tasks get a zero-copy read if on the same node
lookup_ref = ray.put(pd.read_csv("s3://my-bucket/reference/country_codes.csv"))

@ray.remote
def enrich_partition(s3_path: str, lookup_ref: ray.ObjectRef) -> str:
    import pyarrow.parquet as pq
    lookup = ray.get(lookup_ref)
    table = pq.read_table(s3_path)
    # ... join table with lookup ...
    out = s3_path.replace("/raw/", "/enriched/")
    pq.write_table(table, out)
    return out

results = ray.get([enrich_partition.remote(p, lookup_ref) for p in paths])
```

---

## Aggregations and Arrow/DuckDB Interop

### from_arrow / to_arrow

```python
import pyarrow as pa, ray.data

# Create Dataset from in-memory Arrow Table
ds = ray.data.from_arrow(pa.table({"user_id": ["u1", "u2"], "amount_usd": [12.50, 300.00]}))
arrow_table = ds.to_arrow()   # triggers execution; only for data that fits in memory
```

### DuckDB integration

```python
import duckdb, ray.data

con = duckdb.connect()

# Query S3 parquet directly — no Ray cluster needed for aggregation-only workloads
result = con.execute("""
    SELECT country,
           DATE_TRUNC('day', TO_TIMESTAMP(ts)) AS event_date,
           SUM(amount_usd) AS total_revenue,
           COUNT(*)        AS num_events
    FROM read_parquet('s3://my-bucket/processed/purchases/**/*.parquet')
    GROUP BY 1, 2 ORDER BY 2 DESC, 3 DESC
""").arrow()

# Bring Arrow result back into Ray Data for downstream operators
summary_ds = ray.data.from_arrow(result)

# Or register an in-memory dataset for ad-hoc SQL (small data only)
arrow = ray.data.read_parquet("s3://my-bucket/purchases/").to_arrow()
con.register("purchases", arrow)
con.execute("SELECT country, SUM(total_revenue) FROM purchases GROUP BY 1 ORDER BY 2 DESC LIMIT 10").df()
```

---

## Streaming Execution

### iter_batches — generator-based consumption

Use `.iter_batches()` to stream data without materializing the full dataset in memory.

```python
import ray.data

ds = ray.data.read_parquet("s3://my-bucket/raw/events/", override_num_blocks=500)

total_revenue, row_count = 0.0, 0
for batch in ds.iter_batches(batch_size=4096, batch_format="pandas", prefetch_batches=4):
    total_revenue += batch["amount_usd"].sum()
    row_count += len(batch)

print(f"Processed {row_count:,} rows, total revenue ${total_revenue:,.2f}")
```

### Streaming training loop (PyTorch pattern)

```python
import ray.data, torch

ds_train = ray.data.read_parquet("s3://my-bucket/ml/features/train/").map_batches(
    normalize_amounts, batch_format="pandas", batch_size=4096
)
feature_cols = ["amount_usd", "hour_of_day", "country_code"]
model, optimizer = torch.nn.Linear(len(feature_cols), 1), torch.optim.Adam(...)

for batch in ds_train.iter_batches(batch_size=512, batch_format="numpy", prefetch_batches=4):
    x = torch.tensor([batch[c] for c in feature_cols], dtype=torch.float32).T
    y = torch.tensor(batch["label"], dtype=torch.float32)
    loss = torch.nn.functional.binary_cross_entropy_with_logits(model(x).squeeze(), y)
    optimizer.zero_grad(); loss.backward(); optimizer.step()
```

### Backpressure and memory budget

Ray Data applies implicit backpressure; tune with `prefetch_batches` and the object store budget:

```python
ray.init(object_store_memory=20 * 1024**3)  # 20 GB global budget
# lower prefetch_batches → less memory pressure; higher → more pipeline overlap
for batch in ds.iter_batches(batch_size=4096, prefetch_batches=2):
    process(batch)
```

---

## Writing Data

### write_parquet — S3 with partitioning

```python
ds = ray.data.read_parquet("s3://my-bucket/raw/events/").map_batches(transform_fn)

# Simple write
ds.write_parquet("s3://my-bucket/processed/events/")

# Partitioned write (Hive-style partition directories)
ds.write_parquet(
    "s3://my-bucket/processed/events/",
    partition_cols=["event_date", "country"],
)
```

### write_csv and write_json

```python
ds.write_csv("s3://my-bucket/exports/events_csv/")
ds.write_json("s3://my-bucket/exports/events_json/")
```

### Custom Datasink (Iceberg via PyIceberg)

Implement `ray.data.Datasink` to write to arbitrary destinations. Each worker calls `write()` with a list of Arrow Table blocks:

```python
from ray.data import Datasink
from ray.data._internal.execution.interfaces import TaskContext
import pyarrow as pa


class IcebergDatasink(Datasink):
    def __init__(self, catalog_uri: str, namespace: str, table_name: str):
        self._catalog_uri, self._namespace, self._table_name = catalog_uri, namespace, table_name

    def write(self, blocks: list[pa.Table], ctx: TaskContext) -> None:
        from pyiceberg.catalog import load_catalog
        catalog = load_catalog("rest", **{"uri": self._catalog_uri, "s3.region": "us-east-1"})
        tbl = catalog.load_table(f"{self._namespace}.{self._table_name}")
        for block in blocks:
            tbl.append(block)

    @property
    def supports_distributed_writes(self) -> bool:
        return True


ray.data.read_parquet("s3://my-bucket/raw/events/dt=2026-05-17/").write_datasink(
    IcebergDatasink("https://iceberg-rest.internal:8181", "analytics", "events")
)
```

---

## Ray Cluster Setup

### Local development and cluster connection

```python
import ray

ray.init()                                            # single-node, all local CPUs
ray.init(num_cpus=4, object_store_memory=4 * 1024**3) # explicit resource override

# Connect from a local machine to a running Ray cluster (Ray Client)
ray.init(address="ray://ray-head.internal:10001")

# Inside a cluster node (ray job submit)
ray.init(address="auto")
```

### KubeRay — RayCluster CRD

```yaml
# ray-cluster.yaml
apiVersion: ray.io/v1
kind: RayCluster
metadata:
  name: ray-data-cluster
  namespace: data-platform
spec:
  rayVersion: "2.44.0"
  headGroupSpec:
    rayStartParams: { dashboard-host: "0.0.0.0", num-cpus: "4" }
    template:
      spec:
        containers:
          - name: ray-head
            image: rayproject/ray:2.44.0-py311
            resources:
              requests: { cpu: "4", memory: "16Gi" }
            env:
              - { name: AWS_REGION, value: us-east-1 }
  workerGroupSpecs:
    - groupName: cpu-workers
      replicas: 4
      minReplicas: 2
      maxReplicas: 20
      rayStartParams: { num-cpus: "8" }
      template:
        spec:
          containers:
            - name: ray-worker
              image: rayproject/ray:2.44.0-py311
              resources: { requests: { cpu: "8", memory: "32Gi" } }
    - groupName: gpu-workers
      replicas: 0
      minReplicas: 0
      maxReplicas: 8
      rayStartParams: { num-cpus: "8", num-gpus: "1" }
      template:
        spec:
          tolerations:
            - { key: nvidia.com/gpu, operator: Exists, effect: NoSchedule }
          containers:
            - name: ray-gpu-worker
              image: rayproject/ray:2.44.0-py311-gpu
              resources:
                requests: { cpu: "8", memory: "32Gi", "nvidia.com/gpu": "1" }
```

```bash
kubectl apply -f ray-cluster.yaml
kubectl get raycluster -n data-platform
```

### RayJob CRD — submit a job to KubeRay

```yaml
# ray-job.yaml
apiVersion: ray.io/v1
kind: RayJob
metadata:
  name: events-pipeline-2026-05-17
  namespace: data-platform
spec:
  entrypoint: "python /app/pipeline.py --date 2026-05-17"
  shutdownAfterJobFinishes: true
  ttlSecondsAfterFinished: 3600
  runtimeEnvYAML: |
    pip: [ray[data]==2.44.0, pandas==2.2.3, pyarrow==18.0.0, boto3]
    env_vars: { AWS_REGION: us-east-1 }
  rayClusterSpec:
    rayVersion: "2.44.0"
    headGroupSpec:
      rayStartParams: { num-cpus: "4" }
      template:
        spec:
          containers:
            - name: ray-head
              image: rayproject/ray:2.44.0-py311
              resources: { requests: { cpu: "4", memory: "16Gi" } }
    workerGroupSpecs:
      - groupName: workers
        replicas: 8
        rayStartParams: { num-cpus: "8" }
        template:
          spec:
            containers:
              - name: ray-worker
                image: rayproject/ray:2.44.0-py311
                resources: { requests: { cpu: "8", memory: "32Gi" } }
```

```bash
kubectl apply -f ray-job.yaml
kubectl get rayjob -n data-platform
```

### ray up — EC2 cluster (key fields)

```yaml
# cluster.yaml
cluster_name: data-pipeline-cluster
provider: { type: aws, region: us-east-1 }
auth:     { ssh_user: ubuntu }
head_node_type: m6i.4xlarge
worker_node_types:
  - node_type_name: cpu_worker
    node_config: { InstanceType: m6i.8xlarge, ImageId: ami-0c55b159cbfafe1f0 }
    min_workers: 2
    max_workers: 20
  - node_type_name: gpu_worker
    node_config: { InstanceType: g5.4xlarge, ImageId: ami-0c55b159cbfafe1f0 }
    min_workers: 0
    max_workers: 4
initialization_commands:
  - pip install -U "ray[data]==2.44.0" pyarrow pandas boto3
autoscaling_config: { upscaling_speed: 1.0, idle_timeout_minutes: 5 }
```

```bash
ray up cluster.yaml --yes
ray submit cluster.yaml pipeline.py -- --date 2026-05-17
ray down cluster.yaml --yes
```

---

## Airflow Integration

### @task + ray.init pattern

Submit Ray work from a standard Airflow PythonOperator. Use `address="ray://..."` to connect to a running cluster.

```python
from datetime import datetime
from airflow.decorators import dag, task


@dag(dag_id="ray_events_pipeline", start_date=datetime(2026, 1, 1), schedule="@daily", catchup=False)
def ray_events_pipeline():

    @task()
    def run_ray_pipeline(**context) -> str:
        """Returns S3 output path via XCom — never the dataset itself."""
        import ray, ray.data
        ray.init(
            address="ray://ray-head.data-platform.svc.cluster.local:10001",
            runtime_env={"pip": ["ray[data]==2.44.0", "pyarrow", "pandas"]},
        )
        date = context["ds"]
        try:
            output_path = f"s3://my-bucket/processed/events/dt={date}/"
            (
                ray.data.read_parquet(f"s3://my-bucket/raw/events/dt={date}/")
                .filter(lambda row: row["event_type"] == "purchase")
                .map_batches(normalize_amounts, batch_format="pandas", batch_size=4096)
                .write_parquet(output_path)
            )
            return output_path
        finally:
            ray.shutdown()

    @task()
    def validate_output(output_path: str) -> None:
        import pyarrow.parquet as pq
        ds = pq.ParquetDataset(output_path)
        row_count = sum(f.count_rows() for f in ds.fragments)
        if row_count == 0:
            raise ValueError(f"Empty output at {output_path}")

    validate_output(run_ray_pipeline())


dag = ray_events_pipeline()
```

### RayJobOperator (ray-provider)

```bash
pip install apache-airflow-providers-ray
```

```python
from ray_provider.operators.ray import SubmitRayJob

submit_job = SubmitRayJob(
    task_id="submit_events_pipeline",
    conn_id="ray_k8s",                       # Airflow connection: Ray dashboard URL
    entrypoint="python /app/pipeline.py",
    runtime_env={
        "working_dir": "s3://my-bucket/code/pipeline/",
        "pip": ["ray[data]==2.44.0", "pandas", "pyarrow"],
        "env_vars": {"RAY_DATE": "{{ ds }}"},
    },
    num_cpus=64,
    job_timeout_seconds=3600,
)
```

### Passing large results via S3 (XCom pattern)

Do not pass large DataFrames through XCom. Use S3 path XCom instead:

```python
@task()
def run_pipeline(ds: str) -> str:
    """Returns S3 path to output, not the data itself."""
    # ... Ray pipeline logic ...
    return f"s3://my-bucket/results/pipeline/dt={ds}/"


@task()
def downstream_task(input_path: str) -> None:
    # Read the path from XCom, not the data
    ds = ray.data.read_parquet(input_path)
    ...
```

---

## Performance Tuning

### override_num_blocks — parallelism control

```python
# Default: one block per file; override for large files or coarse partitioning
ds = ray.data.read_parquet(
    "s3://my-bucket/raw/events/",
    override_num_blocks=400,  # aim for ~128 MB per block
)

# Rule of thumb: target_block_size_mb = total_dataset_gb * 1024 / num_blocks
# For 100 GB dataset, 400 blocks → ~256 MB per block (reasonable)
# For GPU inference, match blocks to GPU batch budget: num_blocks = total_rows / (batch_size * concurrency * prefetch)
```

### prefetch_batches — pipeline overlap

```python
# prefetch N batches ahead; for map_batches tune concurrency to overlap I/O with compute
for batch in ds.iter_batches(batch_size=4096, prefetch_batches=4):
    process(batch)
```

### Inspect fusion and stats

```python
print(repr(ds))    # logical plan (before execution)
print(ds.stats())  # wall time, throughput, bytes per operator (after execution)
# Stage 0 ReadParquet: 200/200 blocks in 12.3s — 8.45 GiB input
# Stage 1 Filter: ...
```

### Memory-efficient patterns

```python
# GOOD: streaming — blocks freed after each stage
(
    ray.data.read_parquet("s3://my-bucket/raw/large_table/")
    .filter(predicate)
    .map_batches(transform, batch_size=4096)
    .write_parquet("s3://my-bucket/processed/large_table/")
)

# BAD: .materialize() in a loop pins all blocks → OOM
for date in date_range:
    ds = ray.data.read_parquet(f"s3://my-bucket/raw/events/dt={date}/")
    ds = ds.materialize()   # DO NOT do this in loops

# GOOD: process partitions sequentially; blocks freed after each write
for date in date_range:
    (
        ray.data.read_parquet(f"s3://my-bucket/raw/events/dt={date}/")
        .map_batches(transform, batch_size=4096)
        .write_parquet(f"s3://my-bucket/processed/events/dt={date}/")
    )
```

### Concurrency sizing for GPU actors

Rule: `concurrency = num_gpus_in_cluster / num_gpus_per_actor`. For an 8-GPU cluster with 1 GPU per actor use `concurrency=8`:

```python
ds.map_batches(GPUInferenceActor, concurrency=8, num_gpus=1, batch_size=256, prefetch_batches=2)
```

---

## Anti-Patterns

Do not:

- **Call `.materialize()` in loops** — each call pins all blocks in the object store, causing OOM. Process partitions sequentially without materializing.
- **Use `.take_all()` or `.to_pandas()` on large datasets** — these collect all data to the driver. Use only for small sampling or final small results.
- **Perform SQL joins inside `map_batches`** — Ray Data has no distributed join optimizer. Use Spark for heavy multi-table joins; use DuckDB for single-node SQL on partitioned data.
- **Ignore block sizes** — default one-block-per-file creates huge blocks from large files, starving parallelism. Always set `override_num_blocks` for large inputs.
- **Create actors without resource hints** — omitting `num_gpus`/`num_cpus` lets Ray over-schedule actors, causing OOM or GPU contention. Always specify resource requirements.
- **Pass large objects through XCom or function arguments** — use `ray.put()` for large reference data; use S3 path XCom for Airflow results.
- **Mix row-level `.map()` and batch-level `.map_batches()` for the same logic** — `map_batches` with pandas/arrow is 10-100x faster than `.map()` for columnar ops.
- **Use `.sort()` without necessity** — it triggers a global distributed shuffle. If you only need top-N, use `.limit()` on a pre-filtered dataset.
- **Forget `ray.shutdown()` in long-running processes** — failing to shut down leaves zombie actors and leaks object store references.
- **Use Ray 1.x Dataset API** — Ray 2.x dropped the old `ray.data.Dataset` experimental API. Use the stable `ray.data` module; avoid deprecated `pipeline()` and `window()` methods from Ray 1.x.
- **Store mutable state in remote function closures** — closures are serialized; mutable objects behave unexpectedly. Use actors for mutable state.

## Output Expectations

When producing Ray Data code:
- Include all imports (`ray`, `ray.data`, `pandas`, `pyarrow`, etc.)
- Use Ray 2.x API; avoid deprecated Ray 1.x patterns
- Specify `batch_format`, `batch_size`, `concurrency`, and resource hints (`num_cpus`, `num_gpus`) in every `map_batches` call
- Always specify `override_num_blocks` for large or heterogeneously-sized datasets
- Prefer `map_batches` with `batch_format="pandas"` or `"pyarrow"` over row-level `.map()` for columnar operations
- Use class-based actors for any state that must be initialized once (models, connections, tokenizers)
- Return S3 output paths from Airflow tasks; never pass datasets through XCom
- Call `ray.shutdown()` at the end of scripts and in Airflow task `finally` blocks
- Mention dataset statistics (`ds.stats()`) and cluster resource sizing for performance-sensitive pipelines

---

## References to Consult When Needed

- Ray Data User Guide: https://docs.ray.io/en/latest/data/data.html
- Ray Data API Reference: https://docs.ray.io/en/latest/data/api/api.html
- KubeRay Operator docs: https://ray-project.github.io/kuberay/
- Ray Jobs API: https://docs.ray.io/en/latest/cluster/running-applications/job-submission/index.html
- Ray Cluster on EC2 (ray up): https://docs.ray.io/en/latest/cluster/vms/getting-started.html
- Ray Autoscaler configuration: https://docs.ray.io/en/latest/cluster/vms/references/ray-cluster-configuration.html
- Airflow Ray provider (SubmitRayJob): https://astronomer.github.io/astronomer-providers/ray/
- PyIceberg write API: https://py.iceberg.apache.org/api/
