---
name: mlflow-data-pipelines
description: MLflow for data engineering — tracking server setup (PostgreSQL backend + S3 artifacts), experiment tracking (params/metrics/artifacts/autolog), ETL job metadata logging (row counts, DQ metrics, lineage tags), Model Registry (register/alias/promote), model serving (pyfunc/REST API/batch scoring), MLproject files, Airflow integration (MLflowClientHook/XCom/model promotion), Spark integration (mlflow.spark.autolog/log_model/Delta metadata)
---

# MLflow for Data Engineering

## When to Use

Load this skill when the user needs to:
- Set up a production MLflow tracking server (PostgreSQL + S3, Docker Compose)
- Track ETL pipeline runs: input/output row counts, processing time, data quality metrics
- Log params, metrics, tags, and file artifacts inside Python or PySpark jobs
- Enable autologging for scikit-learn, PySpark ML, or XGBoost
- Use nested (parent/child) runs for hyperparameter sweeps
- Register, version, alias, and promote models in the MLflow Model Registry
- Serve models via REST API or run batch scoring with `mlflow.pyfunc`
- Define portable `MLproject` files with conda or Docker environments
- Wire MLflow run tracking into Airflow DAGs using `MLflowClientHook` or `PythonOperator`
- Log Spark MLlib models or Delta table metadata as MLflow artifacts

---

## Tracking Server Setup

### CLI Launch (minimal local dev)

```bash
mlflow server \
  --backend-store-uri postgresql+psycopg2://mlflow:secret@localhost:5432/mlflowdb \
  --artifacts-destination s3://my-datalake/mlflow-artifacts \
  --host 0.0.0.0 \
  --port 5000 \
  --workers 4
```

- `--backend-store-uri` must point to a **database** (PostgreSQL/MySQL) to use the Model Registry.
- `--artifacts-destination` separates artifact blobs from metadata; the server proxies uploads/downloads.
- For > 50 concurrent jobs, put **PgBouncer** in transaction mode in front of PostgreSQL to avoid connection pool exhaustion.

### Environment Variable

```bash
export MLFLOW_TRACKING_URI="http://mlflow-server:5000"
# All mlflow.* calls in any Python process will automatically use this server.
```

### Docker Compose Deployment

```yaml
# docker-compose.yml
version: "3.9"

services:
  postgres:
    image: postgres:15-alpine
    environment:
      POSTGRES_DB: mlflowdb
      POSTGRES_USER: mlflow
      POSTGRES_PASSWORD: ${POSTGRES_PASSWORD}
    volumes:
      - pg_data:/var/lib/postgresql/data
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U mlflow"]
      interval: 10s
      retries: 5

  mlflow:
    image: ghcr.io/mlflow/mlflow:v2.16.0
    depends_on:
      postgres:
        condition: service_healthy
    ports:
      - "5000:5000"
    environment:
      MLFLOW_S3_ENDPOINT_URL: ${S3_ENDPOINT_URL}           # omit for AWS S3
      AWS_ACCESS_KEY_ID: ${AWS_ACCESS_KEY_ID}
      AWS_SECRET_ACCESS_KEY: ${AWS_SECRET_ACCESS_KEY}
    command: >
      mlflow server
        --backend-store-uri postgresql+psycopg2://mlflow:${POSTGRES_PASSWORD}@postgres:5432/mlflowdb
        --artifacts-destination s3://my-datalake/mlflow-artifacts
        --host 0.0.0.0
        --port 5000
        --workers 4

  pgbouncer:
    image: bitnami/pgbouncer:1.22.1
    depends_on: [postgres]
    environment:
      POSTGRESQL_HOST: postgres
      POSTGRESQL_DATABASE: mlflowdb
      POSTGRESQL_USERNAME: mlflow
      POSTGRESQL_PASSWORD: ${POSTGRES_PASSWORD}
      PGBOUNCER_POOL_MODE: transaction
      PGBOUNCER_MAX_CLIENT_CONN: "200"
      PGBOUNCER_DEFAULT_POOL_SIZE: "20"
    ports:
      - "6432:6432"

volumes:
  pg_data:
```

> Point `--backend-store-uri` at PgBouncer (`pgbouncer:6432`) in high-concurrency deployments.

---

## Experiment Tracking

### Basic Run with Explicit Logging

```python
import mlflow
import mlflow.sklearn
from sklearn.ensemble import GradientBoostingClassifier
from sklearn.metrics import roc_auc_score

MLFLOW_TRACKING_URI = "http://mlflow-server:5000"
EXPERIMENT_NAME    = "team_risk/churn_model"

mlflow.set_tracking_uri(MLFLOW_TRACKING_URI)
mlflow.set_experiment(EXPERIMENT_NAME)

params = {"n_estimators": 300, "max_depth": 5, "learning_rate": 0.05}

with mlflow.start_run(run_name="gbt_baseline") as run:
    # --- params (logged once, string-serialized) ---
    mlflow.log_params(params)
    mlflow.set_tags({
        "team":        "risk",
        "pipeline":    "churn_monthly",
        "data_version": "2025-04-01",
        "git_sha":      "a1b2c3d",
    })

    model = GradientBoostingClassifier(**params)
    model.fit(X_train, y_train)

    # --- metrics (step-indexed for curves, or scalar) ---
    mlflow.log_metric("train_auc", roc_auc_score(y_train, model.predict_proba(X_train)[:, 1]))
    mlflow.log_metric("val_auc",   roc_auc_score(y_val,   model.predict_proba(X_val)[:, 1]))

    # --- artifacts (local file paths or directories) ---
    mlflow.log_artifact("reports/feature_importance.png")
    mlflow.log_artifact("data/schema.json", artifact_path="metadata")

    # --- model ---
    signature = mlflow.models.infer_signature(X_train, model.predict_proba(X_train))
    mlflow.sklearn.log_model(model, "model", signature=signature, input_example=X_train[:3])

    run_id = run.info.run_id
    print(f"Run ID: {run_id}")
```

### Autologging

```python
# scikit-learn: logs params, metrics, feature importance, model
mlflow.sklearn.autolog(log_input_examples=True, log_model_signatures=True)

# XGBoost: logs eval metrics per boosting round
mlflow.xgboost.autolog()

# PySpark ML (requires Spark 3.0+)
mlflow.pyspark.ml.autolog()

# Enable globally for all supported flavors in one call
mlflow.autolog(
    log_input_examples=True,
    log_model_signatures=True,
    log_models=True,
    disable=False,
    silent=False,
)
```

### Nested Runs for Hyperparameter Sweeps

```python
import itertools

param_grid = {
    "max_depth":     [3, 5, 7],
    "learning_rate": [0.01, 0.05, 0.1],
}

with mlflow.start_run(run_name="gbt_sweep") as parent_run:
    mlflow.set_tag("sweep_type", "grid_search")
    best_val_auc = 0.0
    best_child_run_id = None

    for depth, lr in itertools.product(param_grid["max_depth"], param_grid["learning_rate"]):
        with mlflow.start_run(run_name=f"depth={depth}_lr={lr}", nested=True) as child_run:
            mlflow.log_params({"max_depth": depth, "learning_rate": lr})
            model = GradientBoostingClassifier(max_depth=depth, learning_rate=lr, n_estimators=200)
            model.fit(X_train, y_train)
            val_auc = roc_auc_score(y_val, model.predict_proba(X_val)[:, 1])
            mlflow.log_metric("val_auc", val_auc)

            if val_auc > best_val_auc:
                best_val_auc = val_auc
                best_child_run_id = child_run.info.run_id

    mlflow.log_metric("best_val_auc", best_val_auc)
    mlflow.set_tag("best_child_run_id", best_child_run_id)
```

---

## Data Pipeline Tracking (DE Focus)

Track ETL job metadata — not model metrics — to audit data movement, measure pipeline health, and build lineage.

### ETL Run Wrapper

```python
import time
import mlflow

MLFLOW_TRACKING_URI = "http://mlflow-server:5000"
mlflow.set_tracking_uri(MLFLOW_TRACKING_URI)
mlflow.set_experiment("data_engineering/silver_orders")

def run_orders_etl(spark, date: str) -> str:
    """Returns the MLflow run_id for downstream use."""
    with mlflow.start_run(run_name=f"silver_orders_{date}") as run:
        # --- pipeline lineage tags ---
        mlflow.set_tags({
            "pipeline":        "silver_orders",
            "layer":           "silver",
            "source_table":    "bronze.raw_orders",
            "target_table":    "silver.orders",
            "partition_date":  date,
            "triggered_by":    "airflow",
            "dag_id":          "silver_pipeline",
        })

        # --- input params ---
        mlflow.log_params({
            "partition_date":       date,
            "dedup_strategy":       "row_number",
            "null_threshold_pct":   5.0,
        })

        t0 = time.time()

        # read
        raw_df = spark.table("bronze.raw_orders").filter(f"dt = '{date}'")
        input_count = raw_df.count()
        mlflow.log_metric("input_row_count", input_count)

        # transform + dedup
        clean_df = (
            raw_df
            .dropDuplicates(["order_id"])
            .filter("amount > 0")
        )

        # data quality metrics
        null_rate = clean_df.filter("user_id IS NULL").count() / max(input_count, 1)
        mlflow.log_metric("null_user_id_rate", null_rate)
        mlflow.log_metric("negative_amount_dropped",
                          raw_df.filter("amount <= 0").count())

        # write
        (clean_df
            .write
            .format("delta")
            .mode("overwrite")
            .option("replaceWhere", f"dt = '{date}'")
            .saveAsTable("silver.orders"))

        output_count = clean_df.count()
        elapsed = time.time() - t0

        mlflow.log_metrics({
            "output_row_count":   output_count,
            "rows_dropped":       input_count - output_count,
            "processing_time_s":  round(elapsed, 2),
        })

        # assert data quality gate
        if null_rate > 0.05:
            mlflow.set_tag("dq_status", "WARN")
        else:
            mlflow.set_tag("dq_status", "OK")

        return run.info.run_id
```

### Logging Parquet / Delta Artifacts

```python
with mlflow.start_run():
    # Write a local sample to Parquet and ship to MLflow artifact store
    sample_df.toPandas().to_parquet("/tmp/sample_output.parquet", index=False)
    mlflow.log_artifact("/tmp/sample_output.parquet", artifact_path="data_samples")

    # Log Delta table location as a text artifact (metadata pointer)
    delta_meta = {
        "table":    "silver.orders",
        "location": "s3://my-datalake/silver/orders/",
        "format":   "delta",
        "partition": date,
    }
    import json, tempfile, os
    with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
        json.dump(delta_meta, f)
        tmp_path = f.name
    mlflow.log_artifact(tmp_path, artifact_path="delta_metadata")
    os.unlink(tmp_path)
```

---

## Model Registry

### Registering a Model

```python
from mlflow import MlflowClient

client = MlflowClient(tracking_uri="http://mlflow-server:5000")

# 1. Create the registered model (idempotent guard)
try:
    client.create_registered_model(
        name="risk.churn_model",
        tags={"team": "risk", "domain": "churn"},
        description="Monthly churn prediction — GBT.",
    )
except mlflow.exceptions.RestException:
    pass  # already exists

# 2. Register a specific run's model as a new version
model_version = client.create_model_version(
    name="risk.churn_model",
    source=f"runs:/{run_id}/model",    # logged artifact path
    run_id=run_id,
    tags={"val_auc": "0.853", "data_version": "2025-04"},
    description="GBT retrained on 2025-04 data.",
)
print(f"Registered version: {model_version.version}")
```

### Model Aliases (MLflow 2.x — preferred over Stages)

Stages (`Staging`/`Production`) are deprecated as of MLflow 2.9. Use **aliases** instead.

```python
# Promote to champion (replaces whatever was champion before)
client.set_registered_model_alias(
    name="risk.churn_model",
    alias="champion",
    version=model_version.version,
)

# Candidate under evaluation
client.set_registered_model_alias(
    name="risk.churn_model",
    alias="challenger",
    version="14",
)

# Load by alias — survives version changes
model = mlflow.pyfunc.load_model("models:/risk.churn_model@champion")

# Load a specific version directly
model_v12 = mlflow.pyfunc.load_model("models:/risk.churn_model/12")

# Remove alias (e.g., retire challenger)
client.delete_registered_model_alias(name="risk.churn_model", alias="challenger")
```

### Version Tags and Description Update

```python
client.set_model_version_tag(
    name="risk.churn_model",
    version=model_version.version,
    key="approved_by",
    value="alice@example.com",
)
client.update_model_version(
    name="risk.churn_model",
    version=model_version.version,
    description="Approved for production 2025-05-01.",
)
```

---

## Model Serving

### REST API Server

```bash
# Serve from registry alias
mlflow models serve \
  --model-uri "models:/risk.churn_model@champion" \
  --port 8080 \
  --env-manager conda \
  --timeout 120

# Serve from a run artifact
mlflow models serve \
  --model-uri "runs:/abc123def456/model" \
  --port 8080 \
  --env-manager virtualenv \
  --no-conda
```

The server exposes:
- `GET  /health`        — liveness probe
- `GET  /version`       — MLflow version
- `POST /invocations`   — inference endpoint

```bash
# Score a single record
curl -s http://localhost:8080/invocations \
  -H "Content-Type: application/json" \
  -d '{"dataframe_records": [{"feature_a": 1.2, "feature_b": 0.5}]}'

# Score with split orientation
curl -s http://localhost:8080/invocations \
  -H "Content-Type: application/json" \
  -d '{
    "dataframe_split": {
      "columns": ["feature_a", "feature_b"],
      "data": [[1.2, 0.5], [3.1, 0.9]]
    }
  }'
```

### Batch Scoring with `mlflow.pyfunc`

```python
import mlflow.pyfunc
import pandas as pd

# Load champion model once, reuse across batches
model = mlflow.pyfunc.load_model("models:/risk.churn_model@champion")

# Pandas batch
batch_df = pd.read_parquet("s3://my-datalake/features/2025-05-01/")
predictions = model.predict(batch_df)
batch_df["churn_score"] = predictions

# PySpark distributed batch via Spark UDF
spark_model_udf = mlflow.pyfunc.spark_udf(
    spark,
    model_uri="models:/risk.churn_model@champion",
    result_type="double",
)
features_sdf = spark.table("gold.churn_features").filter("dt = '2025-05-01'")
scored_sdf = features_sdf.withColumn(
    "churn_score",
    spark_model_udf(*[F.col(c) for c in feature_cols]),
)
scored_sdf.write.format("delta").mode("overwrite").saveAsTable("gold.churn_scores")
```

### Production Server Config (Gunicorn tuning)

```bash
export GUNICORN_CMD_ARGS="--workers=4 --timeout=120 --keep-alive=5"
export MLFLOW_SCORING_SERVER_REQUEST_TIMEOUT=120

mlflow models serve \
  --model-uri "models:/risk.churn_model@champion" \
  --port 8080 \
  --env-manager virtualenv
```

---

## MLflow Projects

### `MLproject` File

```yaml
# MLproject  (place in repo root)
name: churn_pipeline

# Option A: conda environment
conda_env: conda.yaml

# Option B: Docker environment (comment out conda_env and use this)
# docker_env:
#   image: my-registry/mlflow-runner:3.11
#   environment:
#     - MLFLOW_TRACKING_URI
#     - AWS_ACCESS_KEY_ID
#     - AWS_SECRET_ACCESS_KEY

entry_points:
  train:
    parameters:
      max_depth:     {type: int,   default: 5}
      learning_rate: {type: float, default: 0.05}
      n_estimators:  {type: int,   default: 300}
      data_date:     {type: str,   default: "2025-05-01"}
    command: >
      python src/train.py
        --max-depth {max_depth}
        --lr {learning_rate}
        --n-estimators {n_estimators}
        --date {data_date}

  score:
    parameters:
      model_alias: {type: str, default: "champion"}
      score_date:  {type: str, default: "2025-05-01"}
    command: python src/score.py --alias {model_alias} --date {score_date}

  etl:
    parameters:
      partition_date: {type: str}
    command: python src/etl.py --date {partition_date}
```

### `conda.yaml`

```yaml
name: churn_env
channels:
  - conda-forge
  - defaults
dependencies:
  - python=3.11
  - pip
  - pip:
    - mlflow==2.16.0
    - scikit-learn==1.5.2
    - pandas==2.2.3
    - pyarrow==17.0.0
    - boto3==1.35.0
    - psycopg2-binary==2.9.9
```

### Running Projects

```bash
# Local run (from repo root)
mlflow run . -e train -P max_depth=7 -P learning_rate=0.01

# Remote Git repo run
mlflow run https://github.com/myorg/churn-model.git \
  -e train \
  -P max_depth=5 \
  --experiment-name "team_risk/churn_model" \
  --env-manager conda

# Docker environment
mlflow run . -e etl -P partition_date=2025-05-01 --env-manager docker
```

---

## Airflow Integration

The official `apache-airflow-providers-mlflow` package is **deprecated and unmaintained**. The recommended pattern is to use `MLflowClient` directly inside `PythonOperator` (or `@task`) and pass `run_id` via XCom.

### Pattern 1 — `@task` with XCom

```python
import pendulum
from airflow.sdk import dag, task
import mlflow
from mlflow import MlflowClient

TRACKING_URI = "http://mlflow-server:5000"

@dag(
    dag_id="churn_ml_pipeline",
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
    schedule="@monthly",
    catchup=False,
    tags=["ml", "churn"],
)
def churn_ml_pipeline():

    @task()
    def run_etl(data_date: str) -> str:
        """Runs ETL, returns mlflow run_id."""
        mlflow.set_tracking_uri(TRACKING_URI)
        mlflow.set_experiment("data_engineering/silver_orders")
        with mlflow.start_run(run_name=f"etl_{data_date}") as run:
            mlflow.log_params({"partition_date": data_date})
            mlflow.set_tag("pipeline_step", "etl")
            # ... actual ETL logic ...
            mlflow.log_metrics({"input_rows": 1_200_000, "output_rows": 1_195_000})
        return run.info.run_id

    @task()
    def train_model(etl_run_id: str, data_date: str) -> str:
        """Trains model, logs parent run_id in tags for lineage, returns model run_id."""
        mlflow.set_tracking_uri(TRACKING_URI)
        mlflow.set_experiment("team_risk/churn_model")
        with mlflow.start_run(run_name=f"train_{data_date}") as run:
            mlflow.set_tags({
                "upstream_etl_run_id": etl_run_id,
                "partition_date":      data_date,
            })
            mlflow.log_params({"max_depth": 5, "learning_rate": 0.05})
            # ... training logic ...
            mlflow.log_metric("val_auc", 0.873)
            mlflow.sklearn.log_model(model, "model")
        return run.info.run_id

    @task()
    def promote_model(train_run_id: str) -> None:
        """Registers model and promotes to champion alias if val_auc > threshold."""
        client = MlflowClient(tracking_uri=TRACKING_URI)
        run = client.get_run(train_run_id)
        val_auc = float(run.data.metrics.get("val_auc", 0))

        if val_auc < 0.85:
            raise ValueError(f"val_auc {val_auc:.4f} below threshold 0.85 — not promoting.")

        mv = client.create_model_version(
            name="risk.churn_model",
            source=f"runs:/{train_run_id}/model",
            run_id=train_run_id,
            tags={"val_auc": str(round(val_auc, 4))},
        )
        client.set_registered_model_alias(
            name="risk.churn_model",
            alias="champion",
            version=mv.version,
        )

    # Wire dependencies via XCom
    data_date = "{{ ds[:7] }}"   # YYYY-MM
    etl_run_id   = run_etl(data_date)
    train_run_id = train_model(etl_run_id, data_date)
    promote_model(train_run_id)


churn_ml_pipeline()
```

### Pattern 2 — `MLflowClientHook` (legacy, provider still installed)

```python
from airflow.providers.mlflow.hooks.mlflow import MLflowClientHook

def create_experiment(**context):
    hook = MLflowClientHook(mlflow_conn_id="mlflow_default")
    with hook.get_conn() as client:
        exp = client.get_or_create_experiment("team_risk/churn_model")
        context["ti"].xcom_push(key="experiment_id", value=exp.experiment_id)

# Airflow Connection: mlflow_default
#   conn_type: HTTP
#   host: http://mlflow-server
#   port: 5000
```

---

## Spark Integration

### `mlflow.spark.autolog`

```python
from pyspark.ml.classification import GBTClassifier
from pyspark.ml.feature import VectorAssembler
from pyspark.ml import Pipeline
import mlflow
import mlflow.pyspark.ml

mlflow.set_tracking_uri("http://mlflow-server:5000")
mlflow.set_experiment("team_risk/churn_sparkml")

# Autolog MUST be called before any Pipeline.fit()
mlflow.pyspark.ml.autolog(
    log_models=True,
    log_input_examples=False,   # can be expensive for large datasets
    log_model_signatures=True,
    log_post_training_metrics=True,
)

assembler = VectorAssembler(inputCols=feature_cols, outputCol="features")
gbt = GBTClassifier(labelCol="label", featuresCol="features",
                    maxDepth=5, maxIter=100)
pipeline = Pipeline(stages=[assembler, gbt])

with mlflow.start_run(run_name="gbt_sparkml"):
    # autolog captures: params, metrics (trainingSummary), and the Spark model artifact
    pipeline_model = pipeline.fit(train_sdf)
```

### `mlflow.spark.log_model`

```python
import mlflow.spark

with mlflow.start_run(run_name="gbt_sparkml_explicit") as run:
    pipeline_model = pipeline.fit(train_sdf)

    # Log the Spark PipelineModel
    mlflow.spark.log_model(
        spark_model=pipeline_model,
        artifact_path="spark_pipeline",
        dfs_tmpdir="s3://my-datalake/tmp/mlflow_spark",  # staging dir on distributed FS
        registered_model_name="risk.churn_sparkml",
    )
    print(f"Registered model run: {run.info.run_id}")
```

### Logging Delta Table Metadata as Artifacts

```python
import json, tempfile, os
from delta.tables import DeltaTable

def log_delta_table_metadata(spark, table_name: str, artifact_path: str = "delta_metadata"):
    """Serialize Delta table history + schema as a JSON artifact."""
    dt = DeltaTable.forName(spark, table_name)
    history = dt.history(5).toPandas().to_dict(orient="records")

    meta = {
        "table":   table_name,
        "schema":  spark.table(table_name).schema.jsonValue(),
        "history": history,
    }
    with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
        json.dump(meta, f, default=str)
        tmp = f.name
    mlflow.log_artifact(tmp, artifact_path=artifact_path)
    os.unlink(tmp)

# Usage inside a run:
with mlflow.start_run(run_name="silver_orders_load"):
    # ... ETL work ...
    log_delta_table_metadata(spark, "silver.orders")
```

### Spark Datasource Autologging

```python
# Logs Parquet/Delta/JDBC paths automatically when spark.read is called.
# Requires the mlflow-spark JAR on the Spark classpath.
mlflow.spark.autolog(
    log_models=False,        # datasource-only — no ML involved
    disable=False,
    silent=True,
)

with mlflow.start_run(run_name="etl_with_datasource_tracking"):
    df = spark.read.format("delta").table("bronze.raw_orders")  # path is auto-logged
    # mlflow records: spark_datasource_info → table/path + delta version
```

---

## Anti-Patterns

| Anti-Pattern | Why It Hurts | Fix |
|---|---|---|
| Using the local filesystem as `--backend-store-uri` in production | Filesystem store cannot host the Model Registry; no concurrent access | Use PostgreSQL |
| Using Model Registry Stages (`Staging`/`Production`) | Deprecated in MLflow 2.9; removed in 3.x | Use aliases: `@champion`, `@challenger` |
| Logging metrics inside a loop without `step=` | All points collapse to a single x-position in the UI | Pass `step=epoch` or `step=iteration` to `log_metric` |
| One giant run for an entire multi-day pipeline | Mixing compute stages makes debugging hard | One run per logical pipeline stage; link via `upstream_run_id` tag |
| `mlflow.log_artifact` on a huge Parquet file per run | Duplicates storage; bloats artifact store | Log a metadata JSON pointing to the canonical S3 path instead |
| Calling `mlflow.set_experiment` inside every task function | Race condition — parallel tasks can corrupt experiment creation | Create experiments once at DAG/job entry point |
| Running `mlflow server` with SQLite in production | No concurrent writes; corrupts under parallel load | PostgreSQL only |
| `mlflow.autolog()` in PySpark batch ETL with no ML | Logs irrelevant datasource noise; performance overhead | Only enable `mlflow.spark.autolog()` when you actually need datasource lineage |
| Skipping `infer_signature` when logging models | Serving layer cannot validate input schema | Always call `mlflow.models.infer_signature(X_sample, y_sample)` |
| Hardcoding `run_id` strings across DAG tasks | Brittle coupling; breaks on re-run | Pass `run_id` via XCom / function return value |

---

## Output Expectations

- Every ETL run produces **one MLflow run** per pipeline stage with `input_row_count`, `output_row_count`, `processing_time_s`, and at least one DQ metric.
- Model runs always log a `signature`, `input_example`, and are registered in the Model Registry.
- Champion model is accessed via alias `@champion`, never by hard-coded version number.
- Airflow DAGs pass `run_id` via XCom; the `promote_model` task gate-checks metrics before assigning the alias.
- Spark MLlib models are logged with `mlflow.spark.log_model` using an S3 `dfs_tmpdir`; never the local driver disk.

---

## References to Consult When Needed

- MLflow Tracking Server docs: https://mlflow.org/docs/latest/ml/tracking/tutorials/remote-server/
- MLflow Model Registry workflows: https://mlflow.org/docs/latest/ml/model-registry/workflow/
- MLflow PyFunc API: https://mlflow.org/docs/latest/python_api/mlflow.pyfunc.html
- MLflow Spark integration: https://mlflow.org/docs/latest/ml/traditional-ml/sparkml/
- MLflow Projects: https://mlflow.org/docs/latest/projects/
- Airflow + MLflow guide (Astronomer): https://www.astronomer.io/docs/learn/2.x/airflow-mlflow
- Model aliases RFC (deprecating stages): https://github.com/mlflow/mlflow/issues/10336
