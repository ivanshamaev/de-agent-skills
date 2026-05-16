---
name: openlineage
description: OpenLineage data lineage tracking — RunEvent/Job/Dataset/facet spec, Marquez backend setup, Airflow/Spark/dbt integrations, column-level lineage, custom emitters, lineage-based impact analysis
---

# OpenLineage Data Lineage

## When to Use

Activate this skill when the task involves:
- Implementing data lineage tracking across Airflow, Spark, dbt, or Trino pipelines
- Setting up the OpenLineage spec emitters and a Marquez or OpenMetadata backend
- Designing column-level lineage for impact analysis
- Writing custom OpenLineage clients or enriching events with facets
- Debugging missing or broken lineage edges in the lineage graph
- Integrating lineage events with data catalogs (DataHub, Atlan, OpenMetadata)

---

## Core Model

```
┌─────────────────────────────────────────────────────────────┐
│  OpenLineage Model                                          │
│                                                             │
│   Dataset ──────── Job ──────── Dataset                    │
│  (input)          (Run)         (output)                    │
│                                                             │
│   Each entity carries Facets — atomic metadata blocks:     │
│   • Schema facet: column names + types                      │
│   • Column-level lineage: input col → output col mapping   │
│   • Data quality assertions                                 │
│   • SQL query text                                          │
│   • Source code location                                    │
└─────────────────────────────────────────────────────────────┘

Events flow:  Pipeline Tool → OpenLineage Client → HTTP Transport → Backend
              (Airflow, Spark, dbt)                                 (Marquez / DataHub)
```

Lineage is collected **passively** — tools emit events without modifying pipeline logic.

---

## RunEvent Specification

The canonical event JSON for an OpenLineage `RunEvent`:

```json
{
  "eventType": "COMPLETE",
  "eventTime": "2024-03-15T10:30:00.000Z",
  "producer": "https://github.com/OpenLineage/OpenLineage/tree/1.0.0/integration/spark",
  "schemaURL": "https://openlineage.io/spec/1-0-5/OpenLineage.json#/definitions/RunEvent",

  "run": {
    "runId": "d46e465b-d358-4d32-83d4-df660ff614dd",
    "facets": {
      "nominalTime": {
        "_producer": "...",
        "_schemaURL": "...",
        "nominalStartTime": "2024-03-15T10:00:00Z",
        "nominalEndTime":   "2024-03-15T10:30:00Z"
      },
      "parent": {
        "_producer": "...",
        "_schemaURL": "...",
        "run":  {"runId": "e9c85741-93ab-4b27-9c8a-f3f4a3c0e001"},
        "job":  {"namespace": "airflow", "name": "etl_pipeline.run_spark_job"}
      }
    }
  },

  "job": {
    "namespace": "spark://spark-master:7077",
    "name": "silver.transform_orders",
    "facets": {
      "sql": {
        "_producer": "...",
        "_schemaURL": "...",
        "query": "INSERT INTO silver.orders SELECT id, customer_id, total FROM bronze.orders WHERE status = 'valid'"
      },
      "sourceCodeLocation": {
        "_producer": "...",
        "_schemaURL": "...",
        "type": "git",
        "url": "https://github.com/org/repo",
        "repoUrl": "https://github.com/org/repo",
        "path": "jobs/transform_orders.py",
        "version": "abc123"
      }
    }
  },

  "inputs": [
    {
      "namespace": "spark://spark-master:7077",
      "name": "bronze.orders",
      "facets": {
        "schema": {
          "_producer": "...",
          "_schemaURL": "...",
          "fields": [
            {"name": "id",          "type": "BIGINT"},
            {"name": "customer_id", "type": "BIGINT"},
            {"name": "total",       "type": "DECIMAL(10,2)"},
            {"name": "status",      "type": "VARCHAR"}
          ]
        },
        "dataSource": {
          "_producer": "...",
          "_schemaURL": "...",
          "name": "spark_iceberg",
          "uri":  "iceberg://lakehouse/bronze"
        }
      }
    }
  ],

  "outputs": [
    {
      "namespace": "spark://spark-master:7077",
      "name": "silver.orders",
      "facets": {
        "schema": {
          "_producer": "...",
          "_schemaURL": "...",
          "fields": [
            {"name": "id",          "type": "BIGINT"},
            {"name": "customer_id", "type": "BIGINT"},
            {"name": "total",       "type": "DECIMAL(10,2)"}
          ]
        },
        "columnLineage": {
          "_producer": "...",
          "_schemaURL": "...",
          "fields": {
            "id": {
              "inputFields": [
                {"namespace": "spark://spark-master:7077", "name": "bronze.orders", "field": "id"}
              ]
            },
            "customer_id": {
              "inputFields": [
                {"namespace": "spark://spark-master:7077", "name": "bronze.orders", "field": "customer_id"}
              ]
            },
            "total": {
              "inputFields": [
                {"namespace": "spark://spark-master:7077", "name": "bronze.orders", "field": "total"}
              ]
            }
          }
        },
        "outputStatistics": {
          "_producer": "...",
          "_schemaURL": "...",
          "rowCount": 150000,
          "size": 45000000
        }
      }
    }
  ]
}
```

### Run States

| `eventType` | Meaning | When to Emit |
|-------------|---------|--------------|
| `START`     | Job execution began | Before first data read |
| `RUNNING`   | Periodic progress update | Long-running jobs (checkpoints) |
| `COMPLETE`  | Finished successfully | After last write |
| `FAIL`      | Execution failed | On exception/error |
| `ABORT`     | Killed externally | On timeout/cancel |
| `OTHER`     | Custom state | Custom tooling |

Every job **must** emit at least `START` + (`COMPLETE` | `FAIL` | `ABORT`).

---

## Facet Reference

### Job Facets

| Facet | Key Fields | Purpose |
|-------|-----------|---------|
| `sql` | `query` | Full SQL text of the transformation |
| `sourceCodeLocation` | `type`, `url`, `path`, `version` | Git repo + file |
| `jobType` | `jobType`, `processingType`, `integration` | BATCH vs STREAMING |
| `ownership` | `owners[].name`, `owners[].type` | Team/service owner |

### Run Facets

| Facet | Key Fields | Purpose |
|-------|-----------|---------|
| `nominalTime` | `nominalStartTime`, `nominalEndTime` | Logical execution window |
| `parent` | `run.runId`, `job.namespace`, `job.name` | Parent Airflow task → child Spark job |
| `errorMessage` | `message`, `programmingLanguage`, `stackTrace` | Structured error on FAIL |
| `externalQuery` | `externalQueryId`, `source` | Maps to DW query ID |

### Dataset Facets

| Facet | Key Fields | Purpose |
|-------|-----------|---------|
| `schema` | `fields[].name`, `fields[].type` | Column definitions |
| `columnLineage` | `fields.{col}.inputFields` | Column-level lineage |
| `dataSource` | `name`, `uri` | Connection identifier |
| `symlinks` | `identifiers[].name`, `.type` | Alternate dataset names |
| `dataQualityMetrics` | `columnMetrics.{col}.*`, `rowCount` | Quality stats |
| `dataQualityAssertions` | `assertions[].success`, `.assertion` | GE/Soda results |
| `lifecycleStateChange` | `lifecycleStateChange` | CREATE/DROP/OVERWRITE/RENAME |
| `storage` | `storageLayer`, `fileFormat` | Iceberg/Delta/Parquet |
| `outputStatistics` | `rowCount`, `size` | Write volume |

---

## Marquez Backend

Marquez is the OpenLineage reference implementation — stores and visualizes lineage events.

### Docker Compose

```yaml
version: "3.8"
services:
  marquez-db:
    image: postgres:14
    environment:
      POSTGRES_DB: marquez
      POSTGRES_USER: marquez
      POSTGRES_PASSWORD: marquez
    volumes:
      - marquez-db:/var/lib/postgresql/data

  marquez:
    image: marquezproject/marquez:0.47.0
    environment:
      MARQUEZ_PORT: 5000
      MARQUEZ_ADMIN_PORT: 5001
      MARQUEZ_DB_HOST: marquez-db
      MARQUEZ_DB_PORT: 5432
      MARQUEZ_DB_NAME: marquez
      MARQUEZ_DB_USER: marquez
      MARQUEZ_DB_PASSWORD: marquez
    ports:
      - "5000:5000"   # API
      - "5001:5001"   # Admin
    depends_on:
      - marquez-db

  marquez-web:
    image: marquezproject/marquez-web:0.47.0
    environment:
      MARQUEZ_HOST: marquez
      MARQUEZ_PORT: 5000
    ports:
      - "3000:3000"   # UI
    depends_on:
      - marquez

volumes:
  marquez-db:
```

### Marquez API

```bash
# List namespaces
curl http://localhost:5000/api/v1/namespaces | jq

# List jobs in namespace
curl "http://localhost:5000/api/v1/namespaces/airflow/jobs" | jq

# Get dataset with lineage
curl "http://localhost:5000/api/v1/namespaces/spark://master:7077/datasets/silver.orders" | jq

# Get lineage graph (upstream + downstream, depth=2)
curl "http://localhost:5000/api/v1/lineage?nodeId=dataset:spark://master:7077:silver.orders&depth=2" | jq

# Search datasets
curl "http://localhost:5000/api/v1/search?q=orders&type=DATASET" | jq
```

---

## Airflow Integration

### Installation

```bash
pip install apache-airflow-providers-openlineage
```

### Configuration (airflow.cfg or environment variables)

```ini
[openlineage]
transport = {"type": "http", "url": "http://marquez:5000", "endpoint": "api/v1/lineage"}
namespace = airflow
disabled = false
disabled_for_operators = airflow.operators.empty.EmptyOperator
```

Or via environment variables:

```bash
export OPENLINEAGE_URL=http://marquez:5000
export OPENLINEAGE_NAMESPACE=airflow
export AIRFLOW__OPENLINEAGE__TRANSPORT='{"type": "http", "url": "http://marquez:5000", "endpoint": "api/v1/lineage"}'
```

### What Airflow Captures Automatically

| Source | Lineage Captured |
|--------|-----------------|
| `SQLExecuteQueryOperator` | Input/output tables (SQL parsing via `openlineage-sql`) |
| `PythonOperator` | Job START/COMPLETE events (no dataset lineage without manual instrumentation) |
| `SparkSubmitOperator` | Parent facet linking to child Spark job run |
| `BigQueryInsertJobOperator` | Input/output tables + SQL |
| `S3CopyObjectOperator` | Input/output S3 dataset |
| `ExternalTaskSensor` | Upstream job dependency |
| `TriggerDagRunOperator` | Parent DAG → child DAG lineage |

### Custom Dataset Lineage in PythonOperator

```python
from openlineage.client.run import Dataset
from openlineage.client.facet import SchemaDatasetFacet, SchemaField
from airflow.providers.openlineage.extractors.base import OperatorLineage

class MyPandasOperator(BaseOperator):
    def get_openlineage_facets_on_complete(self, ti):
        return OperatorLineage(
            inputs=[
                Dataset(
                    namespace="postgres://prod-db:5432",
                    name="public.customers",
                    facets={
                        "schema": SchemaDatasetFacet(
                            fields=[
                                SchemaField("id", "BIGINT"),
                                SchemaField("email", "VARCHAR"),
                            ]
                        )
                    },
                )
            ],
            outputs=[
                Dataset(
                    namespace="s3://data-lake",
                    name="silver/customers",
                )
            ],
        )
```

---

## Spark Integration

### Maven / pip

```xml
<!-- pom.xml or build.sbt for JVM Spark -->
<dependency>
  <groupId>io.openlineage</groupId>
  <artifactId>openlineage-spark_2.12</artifactId>
  <version>1.15.0</version>
</dependency>
```

```bash
# Download JAR for spark-submit
wget https://repo1.maven.org/maven2/io/openlineage/openlineage-spark_2.12/1.15.0/openlineage-spark_2.12-1.15.0.jar
```

### spark-submit Configuration

```bash
spark-submit \
  --jars /opt/spark/jars/openlineage-spark_2.12-1.15.0.jar \
  --conf spark.extraListeners=io.openlineage.spark.agent.OpenLineageSparkListener \
  --conf spark.openlineage.transport.type=http \
  --conf spark.openlineage.transport.url=http://marquez:5000 \
  --conf spark.openlineage.transport.endpoint=/api/v1/lineage \
  --conf spark.openlineage.namespace=spark://spark-master:7077 \
  --conf spark.openlineage.parentJobNamespace=airflow \
  --conf spark.openlineage.parentJobName=etl_dag.run_spark_transform \
  --conf spark.openlineage.parentRunId=d46e465b-d358-4d32-83d4-df660ff614dd \
  my_etl_job.py
```

### PySpark Programmatic Configuration

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("silver_transform") \
    .config("spark.extraListeners",
            "io.openlineage.spark.agent.OpenLineageSparkListener") \
    .config("spark.openlineage.transport.type", "http") \
    .config("spark.openlineage.transport.url", "http://marquez:5000") \
    .config("spark.openlineage.transport.endpoint", "/api/v1/lineage") \
    .config("spark.openlineage.namespace", "spark://spark-master:7077") \
    .getOrCreate()

# All subsequent spark.read / spark.write / df.createOrReplaceTempView
# and SQL queries are automatically tracked
df = spark.read.table("bronze.orders")
result = df.filter("status = 'valid'").select("id", "customer_id", "total")
result.writeTo("silver.orders").append()
# → Emits RunEvent with inputs=[bronze.orders], outputs=[silver.orders]
```

### What Spark Captures

| Operation | Lineage |
|-----------|---------|
| `spark.read.table(name)` | Input dataset |
| `df.write.saveAsTable(name)` | Output dataset |
| `spark.sql("INSERT INTO ...")` | Parsed SQL → input/output |
| `df.join(other, ...)` | Both DFs as inputs |
| Column transformations | Column-level lineage (when SQL-based) |

---

## dbt Integration

```bash
pip install openlineage-dbt
```

### profiles.yml — no changes needed; OL is configured separately

```bash
# Run dbt with OpenLineage emission
export OPENLINEAGE_URL=http://marquez:5000
export OPENLINEAGE_NAMESPACE=dbt_prod
dbt run --target prod

# Or via dbt-openlineage package flags
dbt-ol run --target prod
```

### dbt_project.yml metadata captured

```yaml
# These model-level configs appear in lineage facets
models:
  my_project:
    staging:
      +meta:
        owner: "data-team"
        tags: ["daily", "customers"]
```

### What dbt Captures

| dbt Artifact | OpenLineage Event |
|-------------|-------------------|
| `dbt run` (model) | `RunEvent` per model: inputs = `ref()` / `source()`, output = model target |
| `dbt test` | `RunEvent` per test: dataset = tested model |
| `dbt snapshot` | `RunEvent`: input = source table, output = snapshot table |
| Column-level lineage | Parsed from model SQL via `openlineage-sql` |

---

## Custom Python Emitter

```python
from openlineage.client import OpenLineageClient
from openlineage.client.run import (
    RunEvent, RunState, Run, Job,
    Dataset, InputDataset, OutputDataset,
)
from openlineage.client.facet import (
    SchemaDatasetFacet, SchemaField,
    SqlJobFacet, NominalTimeRunFacet,
    ColumnLineageDatasetFacet, ColumnLineageDatasetFacetFieldsAdditional,
    ColumnLineageDatasetFacetFieldsAdditionalInputFields,
    OutputStatisticsOutputDatasetFacet,
)
from openlineage.client.transport.http import HttpTransport, HttpConfig
import uuid
from datetime import datetime, timezone

client = OpenLineageClient(
    transport=HttpTransport(
        HttpConfig(url="http://marquez:5000", endpoint="api/v1/lineage")
    )
)

run_id = str(uuid.uuid4())
now = datetime.now(timezone.utc).isoformat()

# Emit START
client.emit(RunEvent(
    eventType=RunState.START,
    eventTime=now,
    run=Run(runId=run_id),
    job=Job(namespace="my-pipeline", name="daily_revenue"),
    producer="https://github.com/org/pipeline",
    inputs=[InputDataset(namespace="postgres://db:5432", name="public.orders")],
    outputs=[OutputDataset(namespace="s3://warehouse", name="gold/revenue")],
))

# ... do actual work ...

# Emit COMPLETE with column lineage
client.emit(RunEvent(
    eventType=RunState.COMPLETE,
    eventTime=datetime.now(timezone.utc).isoformat(),
    run=Run(runId=run_id),
    job=Job(
        namespace="my-pipeline",
        name="daily_revenue",
        facets={"sql": SqlJobFacet(query="SELECT order_date, SUM(total) AS revenue FROM orders GROUP BY 1")},
    ),
    producer="https://github.com/org/pipeline",
    inputs=[
        InputDataset(
            namespace="postgres://db:5432",
            name="public.orders",
            facets={
                "schema": SchemaDatasetFacet(fields=[
                    SchemaField("order_date", "DATE"),
                    SchemaField("total", "NUMERIC"),
                ])
            },
        )
    ],
    outputs=[
        OutputDataset(
            namespace="s3://warehouse",
            name="gold/revenue",
            facets={
                "schema": SchemaDatasetFacet(fields=[
                    SchemaField("order_date", "DATE"),
                    SchemaField("revenue", "NUMERIC"),
                ]),
                "columnLineage": ColumnLineageDatasetFacet(
                    fields={
                        "order_date": ColumnLineageDatasetFacetFieldsAdditional(
                            inputFields=[ColumnLineageDatasetFacetFieldsAdditionalInputFields(
                                namespace="postgres://db:5432",
                                name="public.orders",
                                field="order_date",
                            )]
                        ),
                        "revenue": ColumnLineageDatasetFacetFieldsAdditional(
                            inputFields=[ColumnLineageDatasetFacetFieldsAdditionalInputFields(
                                namespace="postgres://db:5432",
                                name="public.orders",
                                field="total",
                                transformationType="AGGREGATE",
                                transformationDescription="SUM",
                            )]
                        ),
                    }
                ),
                "outputStatistics": OutputStatisticsOutputDatasetFacet(
                    rowCount=365,
                    size=4096,
                ),
            },
        )
    ],
))
```

---

## Namespace Conventions

Consistent namespaces are critical — if Airflow and Spark use different namespace strings for the same dataset, lineage will show two disconnected nodes.

| System | Namespace Pattern | Example |
|--------|------------------|---------|
| PostgreSQL | `postgres://<host>:<port>` | `postgres://prod-db:5432` |
| MySQL | `mysql://<host>:<port>` | `mysql://rds-mysql:3306` |
| S3 | `s3://<bucket>` | `s3://data-lake` |
| HDFS | `hdfs://<host>:<port>` | `hdfs://namenode:8020` |
| Kafka | `kafka://<bootstrap>` | `kafka://kafka:9092` |
| Spark tables | `spark://<master>` | `spark://spark-master:7077` |
| Trino | `trino://<host>:<port>` | `trino://trino:8080` |
| dbt | `dbt://<project>` | `dbt://my_project` |

Dataset names follow `<schema>.<table>` or `<database>.<schema>.<table>` conventions — match exactly what the SQL engine uses.

---

## Transport Types

| Transport | Config | Use Case |
|-----------|--------|---------|
| `http` | `url`, `endpoint`, `auth` | Marquez, DataHub, OpenMetadata |
| `file` | `log_file_path`, `append` | Local debugging, batch import |
| `console` | — | Development (prints to stdout) |
| `kafka` | `topic`, `bootstrap.servers` | High-throughput / decoupled |
| `composite` | `transports: [t1, t2]` | Fan-out to multiple backends |

```python
# Composite transport example: HTTP + file
from openlineage.client.transport.composite import CompositeTransport, CompositeConfig

transport = CompositeTransport(CompositeConfig(transports=[
    {"type": "http", "url": "http://marquez:5000", "endpoint": "api/v1/lineage"},
    {"type": "file", "log_file_path": "/var/log/openlineage.jsonl"},
]))
```

---

## Impact Analysis Pattern

Once lineage is in Marquez, use the API to answer "what breaks if I drop column X?":

```python
import requests

def get_downstream_jobs(namespace: str, dataset_name: str, depth: int = 5) -> list[dict]:
    """Return all jobs that read from the given dataset (transitively)."""
    resp = requests.get(
        "http://marquez:5000/api/v1/lineage",
        params={
            "nodeId": f"dataset:{namespace}:{dataset_name}",
            "depth": depth,
        },
    )
    graph = resp.json()
    return [
        node for node in graph["graph"]
        if node["type"] == "JOB" and node["id"] != f"dataset:{namespace}:{dataset_name}"
    ]

# Example: find all downstream jobs of silver.orders
downstream = get_downstream_jobs(
    namespace="spark://spark-master:7077",
    dataset_name="silver.orders",
)
for job in downstream:
    print(job["id"], job["data"]["latestRun"]["state"])
```

---

## Anti-Patterns

1. **Different namespace strings for the same database** — Airflow uses `postgres://host:5432`, Spark uses `postgresql` — graph shows two unconnected clusters. Standardize namespaces across all tools.

2. **Emitting only COMPLETE events without START** — many backends require START to create the run record; COMPLETE alone is silently dropped or creates orphan records.

3. **Not linking Spark/dbt child runs to parent Airflow run** — lineage is fragmented per system. Always pass `parentJobNamespace`, `parentJobName`, `parentRunId` to Spark and dbt.

4. **Omitting `nominalTime` facet on scheduled jobs** — makes time-partitioned lineage unusable. Use the Airflow logical date as `nominalStartTime`.

5. **Using file transport in production** — JSON files grow unboundedly and are not queryable. Use HTTP transport to Marquez.

6. **Relying solely on job-level lineage** — table-level lineage is insufficient for column impact analysis. Instrument SQL-based jobs with `openlineage-sql` to get column lineage.

7. **Custom dataset names that diverge from SQL names** — e.g., calling a dataset `"orders_v2"` while SQL uses `silver.orders` creates phantom nodes. Use the actual schema.table name.

8. **Not handling `FAIL` events** — a job that silently dies without emitting `FAIL` leaves runs in `START` state forever. Always wrap job logic in try/except and emit `FAIL` on error.

---

## References to Consult When Needed

- OpenLineage spec: `openlineage.io/spec`
- Marquez project: `marquezproject.ai`
- Airflow provider: `airflow.apache.org/docs/apache-airflow-providers-openlineage/`
- Spark integration: `openlineage.io/docs/integrations/spark`
- dbt integration: `openlineage.io/docs/integrations/dbt`
- openlineage-sql (SQL parser): `github.com/OpenLineage/OpenLineage/tree/main/integration/sql`
