---
name: de-root-cause-analysis
description: DE pipeline root cause analysis — failure taxonomy, lineage tracing, upstream/downstream impact, log analysis patterns, Airflow/Spark/dbt failure diagnosis, data quality anomaly RCA, 5-Whys template, incident timeline reconstruction, runbook-style investigation checklists
---

# DE Root Cause Analysis

## When to Use

Load this skill when the user needs to:
- Triage a failing or stuck data pipeline (Airflow DAG, Spark job, dbt run)
- Investigate a data quality incident — wrong numbers, missing rows, duplicates, nulls
- Analyze an SLA breach — pipeline ran but finished too late
- Respond to a production incident — unexpected data anomaly reported by stakeholders
- Trace blast radius — which downstream tables/dashboards/jobs are affected by a bad upstream table
- Write an RCA document or post-mortem after resolving an incident
- Build investigation checklists and runbooks for on-call engineers

---

## 1. Failure Taxonomy

Correctly classifying the failure type determines the fastest investigation path. Misclassifying (e.g., treating a logic bug as an infrastructure problem) wastes hours.

### 1.1 Infrastructure Failures

| Symptom | Likely Cause | First Check |
|---|---|---|
| `java.lang.OutOfMemoryError: Java heap space` | Executor OOM | Spark executor memory, data skew |
| `Container killed by YARN for exceeding memory limits` | Cluster container eviction | `spark.executor.memoryOverhead`, partition count |
| `No space left on device` | Disk full on worker or shuffle dir | `/tmp` usage, shuffle spill, HDFS quota |
| `Connection refused / Connection timed out` | Network partition, service down | VPC routing, security groups, DNS |
| `Task preempted` | Cluster eviction (spot/preemptible) | Cloud provider events, spot interruption notices |
| `Disk I/O error` | Storage hardware failure | Cloud disk health, HDFS block scan |

### 1.2 Data Failures

| Symptom | Likely Cause | First Check |
|---|---|---|
| Row count dropped 50%+ vs yesterday | Missing source data, source job failed | Source system logs, upstream DAG status |
| Unexpected NULLs in non-nullable columns | Schema drift in source, ETL bug | Source schema changelog, sample the staging table |
| Primary key violations | Duplicate data at source, re-ingestion overlap | Dedup logic, incremental filter range |
| Referential integrity broken (FK mismatches) | Out-of-order load, missing dimension row | Dimension load order, late-arriving dims |
| Volume 10x larger than normal | Re-ingestion of historical data, filter removed | Source query, watermark/cursor value |

### 1.3 Logic Failures

| Symptom | Likely Cause | First Check |
|---|---|---|
| Off-by-one in daily partition | Date filter uses `< today` instead of `<= yesterday` | Check WHERE clause boundary conditions |
| Wrong aggregation totals | Join fan-out (many-to-many producing extra rows) | Count rows before/after join |
| Timezone mismatch | Pipeline runs in UTC, business logic expects local time | `CONVERT_TZ`, `AT TIME ZONE`, DST transitions |
| Incorrect incremental logic | `is_incremental()` filter reads wrong watermark column | dbt model SQL, `max(updated_at)` vs `max(created_at)` |
| Negative revenue, impossible values | Sign inversion, unit mismatch (cents vs dollars) | Compare raw source values vs transformed |

### 1.4 Dependency Failures

| Symptom | Likely Cause | First Check |
|---|---|---|
| Task stuck in `queued` state for hours | Upstream DAG not finished; ExternalTaskSensor waiting | Parent DAG run status in Airflow UI |
| Empty table after successful run | Upstream API returned 200 but empty payload | API response sample, rate-limit headers |
| Kafka consumer 6h behind | Producer spike or consumer slowdown | Consumer lag (`kafka-consumer-groups.sh`), topic throughput |
| API call returns 503/429 | External service overloaded or rate-limited | HTTP response codes in logs, Retry-After header |

### 1.5 Configuration Failures

| Symptom | Likely Cause | First Check |
|---|---|---|
| `OperationalError: could not connect to server` | Wrong host/port in connection string | Airflow connection ID value, env var override |
| `KeyError: 'DB_PASSWORD'` | Missing environment variable in deployed environment | Secret manager, Kubernetes secret mount |
| `AnalysisException: Table not found` | Wrong schema/database prefix, wrong environment | `spark.catalog.currentDatabase()`, connection config |
| Wrong partition overwritten | Partition key column name changed | `partitionBy` argument vs actual column name |
| Wrong S3 bucket (dev data in prod) | Environment variable not injected | Compare `ENVIRONMENT` env var with actual path |

### 1.6 Concurrency Failures

| Symptom | Likely Cause | First Check |
|---|---|---|
| Duplicate rows after concurrent runs | Two DAG runs writing same partition simultaneously | Airflow `max_active_runs`, file-level locking |
| `LockWaitTimeout` / `DeadlockDetected` | Two writers competing on same table/row | PostgreSQL `pg_locks`, transaction isolation level |
| Partial data after apparent success | Race between writer and reader on non-ACID storage | Use ACID table format (Delta/Iceberg), two-phase write |
| XCom overwritten by parallel task | Two tasks pushing to the same XCom key | Unique XCom keys per task instance |

---

## 2. Investigation Framework — 5-Step RCA Process

### Step 1: Timeline Reconstruction

The timeline is the foundation of all RCA. Establish three timestamps first:
1. **Event time** — when did the bad data/failure actually occur in the data?
2. **Detection time** — when did a monitor, alert, or human notice?
3. **Impact time** — when did downstream consumers first see the problem?

```sql
-- Airflow: find first failure for a DAG in the last 7 days
SELECT
    dag_id,
    run_id,
    execution_date,
    state,
    start_date,
    end_date,
    (end_date - start_date) AS duration
FROM dag_run
WHERE dag_id = 'orders_etl'
  AND execution_date >= NOW() - INTERVAL '7 days'
ORDER BY execution_date DESC;

-- Find which task instance failed first in a run
SELECT
    task_id,
    state,
    start_date,
    end_date,
    try_number,
    hostname
FROM task_instance
WHERE dag_id   = 'orders_etl'
  AND run_id   = 'scheduled__2024-06-01T00:00:00+00:00'
ORDER BY start_date;
```

### Step 2: Blast Radius Assessment

Before digging into root cause, know what is broken. This prevents partial fixes and missed downstream alerts.

```python
# Quick blast radius checklist
BLAST_RADIUS_QUESTIONS = [
    "Which tables are directly written by the failing job?",
    "Which dbt models SELECT from those tables?",
    "Which dashboards/BI tools query those models?",
    "Which downstream DAGs depend on this DAG (ExternalTaskSensor)?",
    "Which SLAs are now at risk given current time?",
    "Are any customer-facing APIs/reports backed by this data?",
]
```

```bash
# dbt: find all models that depend on a broken source model
dbt ls --select "source:raw.orders+" --output path
# + suffix means "this node and all descendants"

# dbt: find direct and indirect dependents of a specific model
dbt ls --select "+stg_orders+" --output name
```

### Step 3: Evidence Collection

Collect evidence before forming hypotheses — evidence is objective, hypotheses are not.

**Checklist:**
- [ ] Task/job logs (full log, not just last N lines)
- [ ] Row counts before and after the failing step
- [ ] Sample of affected rows (5-20 rows is usually enough)
- [ ] Metric graphs for the failure window (Grafana, CloudWatch, Datadog)
- [ ] Schema of source and target tables at failure time
- [ ] Git history of pipeline code — any deploy in the last 24h?
- [ ] Infrastructure change log — cluster resize, config push, dependency upgrade?
- [ ] Upstream system status page or on-call contact

### Step 4: Hypothesis Generation and Elimination

Form hypotheses from the failure taxonomy (Section 1), then eliminate them systematically with evidence.

```
Hypothesis template:
  "The failure was caused by [X] because [evidence Y] supports it.
   It was NOT caused by [A] because [evidence B] rules it out."
```

Good elimination requires a binary question: "If hypothesis X is true, what observable artifact would exist?" If that artifact is absent, eliminate X.

### Step 5: Root Cause Statement and Contributing Factors

A root cause statement is a single sentence using the **because chain**:

> "The orders dashboard showed zero revenue on 2024-06-01 **because** the `fact_orders` dbt model produced zero rows **because** the incremental filter used `updated_at > '{{ ds }}'` with `ds` in UTC while source timestamps are in US/Eastern, excluding all rows written after 19:00 UTC the previous day."

**Contributing factors** are conditions that made the root cause possible or worse:
- No data volume alert on `fact_orders` to catch the zero-row scenario
- Timezone assumption was undocumented in the model's description
- The issue was not caught in staging because the staging run uses a small sample

---

## 3. Airflow Failure Diagnosis

### 3.1 Task Log Location and Key Patterns

```bash
# Default log path on Airflow worker
/opt/airflow/logs/<dag_id>/<task_id>/<execution_date>/<try_number>.log

# Kubernetes executor — fetch logs from pod
kubectl logs <pod-name> -n airflow -c base

# Grep for common failure signals in a task log
grep -E "(ERROR|CRITICAL|Traceback|Exception|killed|OOM|timeout)" task.log

# Find the actual exception line (last Traceback in log)
grep -n "Traceback" task.log | tail -1
# Then read from that line number
tail -n +<LINE> task.log | head -60
```

**Structured log patterns to search (JSON logs):**
```bash
# jq: extract ERROR entries with timestamps
cat task.log | jq -r 'select(.level=="ERROR") | "\(.timestamp) \(.event // .message)"'

# jq: find the first error occurrence
cat task.log | jq -r 'select(.level=="ERROR") | .timestamp' | head -1
```

### 3.2 Task Instance State Machine

| State | Meaning | Next action |
|---|---|---|
| `running` | Actively executing | Wait; check for hangs (no log output > 10 min) |
| `failed` | Raised an exception, no retries left | Check logs; fix and clear to re-run |
| `up_for_retry` | Failed but retries remain | Wait for retry; check transient vs permanent |
| `upstream_failed` | A task this depends on failed | Fix the upstream task first; then clear downstream |
| `skipped` | Upstream used BranchOperator and chose a different path | Expected behavior; verify branch logic |
| `up_for_reschedule` | Sensor in reschedule mode, condition not met yet | Check sensor poke condition; may need to investigate source |
| `deferred` | Task deferred to triggerer (async sensor) | Normal for deferrable operators; check triggerer logs |
| `zombie` | Worker died without updating state | Scheduler detects and marks failed; check worker health |

```sql
-- Find tasks stuck in running state for more than 2 hours (likely zombie)
SELECT dag_id, task_id, run_id, start_date, hostname
FROM task_instance
WHERE state = 'running'
  AND start_date < NOW() - INTERVAL '2 hours';

-- Failure rate per task over last 30 days
SELECT
    dag_id,
    task_id,
    COUNT(*)                                           AS total_runs,
    SUM(CASE WHEN state = 'failed' THEN 1 ELSE 0 END) AS failures,
    ROUND(100.0 * SUM(CASE WHEN state = 'failed' THEN 1 ELSE 0 END) / COUNT(*), 2) AS failure_pct,
    MAX(CASE WHEN state = 'failed' THEN start_date END) AS last_failure
FROM task_instance
WHERE start_date >= NOW() - INTERVAL '30 days'
GROUP BY dag_id, task_id
HAVING SUM(CASE WHEN state = 'failed' THEN 1 ELSE 0 END) > 0
ORDER BY failure_pct DESC;
```

### 3.3 DagBag Import Errors

DagBag errors prevent the DAG from appearing in the UI at all — the scheduler silently skips broken DAGs.

```bash
# Reproduce a DagBag import error locally
python -c "
from airflow.models import DagBag
bag = DagBag(dag_folder='/opt/airflow/dags', include_examples=False)
for dag_id, err in bag.import_errors.items():
    print(f'--- {dag_id} ---')
    print(err)
"

# Or using the CLI
airflow dags list-import-errors

# Test a single DAG file directly
python /opt/airflow/dags/orders_etl.py
```

Common DagBag import errors:
- `ModuleNotFoundError` — missing Python package in the Airflow environment
- `SyntaxError` — Python syntax error in the DAG file
- `NameError` / `AttributeError` — wrong import path or API version mismatch (Airflow 2 vs 3)
- Circular `dag_id` — two files define the same `dag_id`

### 3.4 Distinguishing Sensor Failures

```python
# Sensor timeout (total_timeout exceeded) — log signature:
# airflow.exceptions.AirflowSensorTimeout: Snap. Time is OUT.
# Fix: increase total_timeout or investigate why condition never became true

# Poke failure (poke() returned False indefinitely) — the sensor keeps retrying
# until timeout. Check: is the file/API/task actually completing?

# Operator failure (exception inside execute()) — stack trace visible in logs
# This is a code/config bug, not a sensor timing issue

# ExternalTaskSensor stuck: upstream DAG run doesn't exist for the execution_date
# Check: does the parent DAG use the same schedule and execution_date?
SELECT dag_id, execution_date, state
FROM dag_run
WHERE dag_id = 'upstream_dag'
  AND execution_date = '2024-06-01 00:00:00';
```

### 3.5 XCom Size Limit Causing Silent Failures

```python
# Airflow stores XComs in the metadata DB (default: 48KB limit in MySQL, 1GB in PostgreSQL)
# Large XComs silently truncate or raise OperationalError

# Detect oversized XCom push
from airflow.sdk import task
import sys

@task
def bad_xcom_task() -> list:
    data = list(range(1_000_000))   # ~8MB — will fail or truncate
    print(f"XCom size estimate: {sys.getsizeof(data)} bytes")
    return data  # DO NOT push large objects via XCom

# Fix: write to S3/GCS, push only the path
@task
def good_xcom_task() -> str:
    data = list(range(1_000_000))
    path = "s3://my-bucket/tmp/task_output.parquet"
    _write_to_s3(data, path)
    return path   # push only the reference
```

### 3.6 Worker OOM vs Task Timeout

```
Worker OOM signature in logs:
  "Killed" (from Linux OOM killer — no Python traceback)
  "Worker exited prematurely: signal 9 (SIGKILL)"
  "Container killed by YARN due to exceeding memory"

Task timeout signature in logs:
  "airflow.exceptions.AirflowTaskTimeout: Timeout"
  "DagRunTimeoutError" or "execution_timeout reached"
  Clean Python traceback visible

OOM fix: reduce memory footprint (chunked reads, Spark executor memory tuning)
Timeout fix: increase execution_timeout or optimize the slow step
```

---

## 4. Spark/PySpark Failure Diagnosis

### 4.1 Reading Spark UI

Navigate to: `http://<driver-host>:4040` (active job) or Spark History Server for completed jobs.

Key pages:
- **Jobs** — overall job status; failed jobs shown in red
- **Stages** — click a failed stage to see the failure reason
- **Failed stage > Tasks tab** — sort by `Error` column; first failed task has the root cause
- **Executors tab** — check `GC Time %` (>10% means memory pressure), spill to disk, failed tasks per executor
- **SQL tab** — query plan with row counts per node; large row count jumps indicate fan-out from bad joins

### 4.2 OOM: Executor vs Driver

```
Executor OOM:
  java.lang.OutOfMemoryError: Java heap space       (in executor log)
  Container killed by YARN for exceeding memory limits
  Error: ExecutorLostFailure (executor 3 exited caused by: Executor OOM)
  Fix:
    spark.executor.memory          = 4g -> 8g
    spark.executor.memoryOverhead  = 512m -> 1g
    spark.sql.shuffle.partitions   = 200 -> 800  (smaller partitions)
    repartition(n) before heavy shuffle

Driver OOM:
  java.lang.OutOfMemoryError in driver logs
  "Driver exited with exit code 1"
  Caused by: collect(), toPandas(), broadcast of large DF
  Fix:
    spark.driver.memory            = 2g -> 8g
    Replace collect() with write to storage
    Replace broadcast join with sort-merge join if table > 100MB
```

### 4.3 Data Skew Detection

```python
from pyspark.sql import functions as F

def detect_skew(df, join_key: str, top_n: int = 20):
    """Print key distribution to find skew."""
    dist = (
        df.groupBy(join_key)
          .agg(F.count("*").alias("row_count"))
          .orderBy(F.desc("row_count"))
    )
    print(f"Top {top_n} values for '{join_key}':")
    dist.show(top_n, truncate=False)

    stats = df.groupBy(join_key).count().agg(
        F.min("count").alias("min"),
        F.avg("count").alias("avg"),
        F.max("count").alias("max"),
        F.stddev("count").alias("stddev"),
    )
    stats.show()
    # Skew indicator: max / avg > 10x
```

```python
# Fix skew with salting
import pyspark.sql.functions as F
from pyspark.sql import DataFrame

SALT_BUCKETS = 50

def salt_join(large_df: DataFrame, small_df: DataFrame, key: str) -> DataFrame:
    """Salt-based skew join: explode small_df, add random salt to large_df."""
    large_salted = large_df.withColumn(
        "salt_key",
        F.concat(F.col(key).cast("string"), F.lit("_"),
                 (F.rand() * SALT_BUCKETS).cast("int").cast("string"))
    )
    small_exploded = small_df.withColumn(
        "salt_key",
        F.explode(F.array([
            F.concat(F.col(key).cast("string"), F.lit(f"_{i}"))
            for i in range(SALT_BUCKETS)
        ]))
    )
    return large_salted.join(small_exploded, "salt_key").drop("salt_key")
```

### 4.4 Serialization Errors

```
Error signature:
  org.apache.spark.SparkException: Task not serializable
  Caused by: java.io.NotSerializableException: com.example.MyClass

Root causes:
  1. Lambda captures a non-serializable object (DB connection, file handle)
  2. UDF references an instance variable of a non-serializable class
  3. Broadcast variable holds a non-serializable object

Fix:
```

```python
# BAD: captures self (often non-serializable)
class Processor:
    def __init__(self, multiplier):
        self.multiplier = multiplier

    def process(self, df):
        # self is captured — if Processor is not serializable, this fails
        return df.rdd.map(lambda row: row.value * self.multiplier)

# GOOD: extract the primitive value before passing to RDD/UDF
class Processor:
    def __init__(self, multiplier):
        self.multiplier = multiplier

    def process(self, df):
        m = self.multiplier   # extract serializable primitive
        return df.rdd.map(lambda row: row.value * m)
```

### 4.5 Shuffle Failures (FetchFailed)

```
Error signature:
  org.apache.spark.shuffle.FetchFailed: Failed to connect to <host>/<ip>:port

Root causes:
  - Executor that held shuffle data was evicted/OOM killed before downstream stage fetched it
  - Network timeout during large shuffle
  - Disk full on shuffle directory

Diagnosis:
  1. Check Executors tab: was the executor with shuffle data lost?
  2. Check worker disk usage: df -h /tmp on all workers
  3. Check spark.reducer.maxReqsInFlight and spark.shuffle.io.maxRetries

Fixes:
  spark.shuffle.io.maxRetries          = 3  -> 10
  spark.shuffle.io.retryWait           = 5s -> 30s
  spark.sql.shuffle.partitions         increase to reduce per-partition size
  spark.shuffle.service.enabled        = true  (external shuffle service)
  Consider Delta/Iceberg intermediate writes instead of in-memory shuffle
```

### 4.6 Schema Mismatch on Read

```python
# AnalysisException: cannot resolve column 'order_id' given input columns
# Causes:
#   - Column renamed in source
#   - Reading Parquet written with different schema evolution settings
#   - Hive metastore schema cached but underlying files differ

# Diagnose schema at read time
df = spark.read.parquet("s3://bucket/orders/")
df.printSchema()

# Compare with expected schema
from pyspark.sql.types import StructType, StructField, StringType, LongType, TimestampType

EXPECTED_SCHEMA = StructType([
    StructField("order_id",    StringType(),    nullable=False),
    StructField("customer_id", LongType(),      nullable=False),
    StructField("amount",      DoubleType(),    nullable=True),
    StructField("created_at",  TimestampType(), nullable=False),
])

actual_fields   = {f.name: f.dataType for f in df.schema.fields}
expected_fields = {f.name: f.dataType for f in EXPECTED_SCHEMA.fields}

missing  = set(expected_fields) - set(actual_fields)
extra    = set(actual_fields) - set(expected_fields)
mistyped = {k for k in actual_fields if k in expected_fields
            and type(actual_fields[k]) != type(expected_fields[k])}

print(f"Missing columns: {missing}")
print(f"Extra columns:   {extra}")
print(f"Type mismatches: {mistyped}")
```

---

## 5. dbt Failure Diagnosis

### 5.1 Failure Types

| Failure type | Signature | Investigation |
|---|---|---|
| Compilation error | `Compilation Error in model` | Jinja template error, undefined ref/source, wrong macro call |
| SQL runtime error | `Database Error in model` | SQL syntax error, type mismatch, missing table at query time |
| Test failure | `Failure in test` | Data does not satisfy constraint |
| Snapshot error | `Error running snapshot` | Strategy mismatch, missing `updated_at`, PK conflict |
| Connection error | `Database Error: connection refused` | Profile configuration, wrong target environment |

```bash
# Reproduce a compilation error locally
dbt compile --select orders_mart
cat target/compiled/my_project/models/orders_mart.sql

# Run a single model with full output
dbt run --select orders_mart --full-refresh 2>&1 | tee /tmp/dbt_run.log

# Run only failing tests for a model
dbt test --select orders_mart 2>&1 | grep "FAIL"
```

### 5.2 Reading run_results.json

```python
import json
from pathlib import Path

def parse_dbt_run_results(target_dir: str = "target") -> list[dict]:
    path = Path(target_dir) / "run_results.json"
    data = json.loads(path.read_text())

    failures = []
    for result in data["results"]:
        if result["status"] not in ("success", "pass", "warn"):
            failures.append({
                "unique_id": result["unique_id"],
                "status":    result["status"],
                "message":   result.get("message", ""),
                "timing":    result.get("timing", []),
            })
    return failures

# Usage in CI
failures = parse_dbt_run_results()
for f in failures:
    print(f"FAILED: {f['unique_id']}")
    print(f"  Status:  {f['status']}")
    print(f"  Message: {f['message'][:500]}")
```

### 5.3 Incremental Model Issues

```sql
-- Common bug: is_incremental() filter uses wrong watermark column
-- If source has both created_at and updated_at, use updated_at for incrementals
-- BAD: misses rows that were updated after creation
{% if is_incremental() %}
WHERE created_at > (SELECT MAX(created_at) FROM {{ this }})
{% endif %}

-- GOOD: catches updates
{% if is_incremental() %}
WHERE updated_at > (SELECT MAX(updated_at) FROM {{ this }})
{% endif %}
```

```sql
-- Diagnose unique key violation in incremental merge strategy
-- Run this against the staging data before the dbt run
SELECT order_id, COUNT(*) AS cnt
FROM {{ ref('stg_orders') }}
GROUP BY order_id
HAVING COUNT(*) > 1
ORDER BY cnt DESC
LIMIT 20;
```

### 5.4 dbt Test Failure Triage

```bash
# Run tests and show the failing SQL
dbt test --select orders_mart --store-failures

# dbt stores failures in the database — query them
# Table name: <schema>_dbt_test__audit.<test_name>

# Find which rows violated not_null test
SELECT *
FROM dbt_test__audit.not_null_orders_mart_order_id
LIMIT 20;

# Find which rows violated unique test
SELECT *
FROM dbt_test__audit.unique_orders_mart_order_id
LIMIT 20;
```

```python
# Programmatic test result check from CI
import subprocess, json, sys

result = subprocess.run(
    ["dbt", "test", "--select", "orders_mart", "--output", "json"],
    capture_output=True, text=True
)
for line in result.stdout.splitlines():
    try:
        ev = json.loads(line)
        if ev.get("info", {}).get("level") == "error":
            print(ev["info"]["msg"])
    except json.JSONDecodeError:
        pass
if result.returncode != 0:
    sys.exit(1)
```

### 5.5 Snapshot Failures

```sql
-- Snapshot fails if the configured unique_key is not actually unique in the source
-- Diagnose before running snapshot:
SELECT order_id, COUNT(*) AS cnt
FROM {{ source('raw', 'orders') }}
GROUP BY order_id
HAVING COUNT(*) > 1;

-- Snapshot fails if updated_at column contains NULLs (for timestamp strategy)
SELECT COUNT(*) AS null_updated_at
FROM {{ source('raw', 'orders') }}
WHERE updated_at IS NULL;
```

---

## 6. Data Quality Anomaly Investigation

### 6.1 Volume Anomaly — Today vs 7-Day Rolling Average

```sql
-- PostgreSQL / Trino
WITH daily_counts AS (
    SELECT
        event_date,
        COUNT(*) AS row_count
    FROM orders
    WHERE event_date >= CURRENT_DATE - INTERVAL '8 days'
    GROUP BY event_date
),
stats AS (
    SELECT
        event_date,
        row_count,
        AVG(row_count) OVER (
            ORDER BY event_date
            ROWS BETWEEN 7 PRECEDING AND 1 PRECEDING
        ) AS rolling_7d_avg,
        STDDEV(row_count) OVER (
            ORDER BY event_date
            ROWS BETWEEN 7 PRECEDING AND 1 PRECEDING
        ) AS rolling_7d_stddev
    FROM daily_counts
)
SELECT
    event_date,
    row_count,
    ROUND(rolling_7d_avg, 0)    AS avg_7d,
    ROUND(rolling_7d_stddev, 0) AS stddev_7d,
    ROUND(100.0 * (row_count - rolling_7d_avg) / NULLIF(rolling_7d_avg, 0), 1) AS pct_vs_avg,
    CASE
        WHEN ABS(row_count - rolling_7d_avg) > 3 * rolling_7d_stddev THEN 'ANOMALY'
        WHEN ABS(row_count - rolling_7d_avg) > 2 * rolling_7d_stddev THEN 'WARNING'
        ELSE 'OK'
    END AS status
FROM stats
ORDER BY event_date DESC;
```

### 6.2 Freshness Anomaly — Late vs Missing Data

```sql
-- Distinguish late-arriving data from completely missing data
-- Trino / PostgreSQL

-- Check 1: is there ANY data for today (missing)?
SELECT
    COUNT(*) AS today_rows,
    MIN(created_at) AS first_event,
    MAX(created_at) AS last_event
FROM orders
WHERE DATE(created_at) = CURRENT_DATE;

-- Check 2: what is the maximum lag between event_time and load_time?
SELECT
    PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY load_lag_minutes) AS p50_lag_min,
    PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY load_lag_minutes) AS p95_lag_min,
    PERCENTILE_CONT(0.99) WITHIN GROUP (ORDER BY load_lag_minutes) AS p99_lag_min,
    MAX(load_lag_minutes) AS max_lag_min
FROM (
    SELECT
        EXTRACT(EPOCH FROM (loaded_at - event_time)) / 60.0 AS load_lag_minutes
    FROM orders
    WHERE DATE(event_time) = CURRENT_DATE
) t;
```

### 6.3 Distribution Shift

```sql
-- Compare min/max/avg/stddev between two time periods
-- Spark SQL / Trino / PostgreSQL
SELECT
    'today'     AS period,
    MIN(amount) AS min_val,
    MAX(amount) AS max_val,
    AVG(amount) AS avg_val,
    STDDEV(amount) AS stddev_val,
    COUNT(*) AS row_count
FROM orders WHERE event_date = CURRENT_DATE
UNION ALL
SELECT
    'yesterday' AS period,
    MIN(amount),
    MAX(amount),
    AVG(amount),
    STDDEV(amount),
    COUNT(*)
FROM orders WHERE event_date = CURRENT_DATE - 1;
```

### 6.4 Null Explosion Detection

```python
# PySpark: per-column null rate comparison across two DataFrames
from pyspark.sql import functions as F, DataFrame

def null_rate_comparison(df_today: DataFrame, df_yesterday: DataFrame) -> None:
    """Print columns where null rate changed by more than 5 percentage points."""
    cols = df_today.columns

    def null_rates(df: DataFrame) -> dict:
        total = df.count()
        exprs = [
            F.round(F.sum(F.col(c).isNull().cast("int")) / total * 100, 2).alias(c)
            for c in cols
        ]
        return df.agg(*exprs).collect()[0].asDict()

    today_rates     = null_rates(df_today)
    yesterday_rates = null_rates(df_yesterday)

    print(f"{'Column':<40} {'Yesterday%':>12} {'Today%':>10} {'Delta':>8}")
    print("-" * 74)
    for col in sorted(cols):
        prev = yesterday_rates.get(col, 0) or 0
        curr = today_rates.get(col, 0) or 0
        delta = curr - prev
        flag = " <-- ALERT" if abs(delta) > 5 else ""
        print(f"{col:<40} {prev:>12.2f} {curr:>10.2f} {delta:>+8.2f}{flag}")
```

```sql
-- PostgreSQL: null rate per column for a specific table partition
SELECT
    attname AS column_name,
    null_frac * 100 AS null_pct
FROM pg_stats
WHERE tablename = 'orders'
  AND schemaname = 'public'
ORDER BY null_frac DESC;
```

### 6.5 Duplicate Explosion with Source Tracing

```sql
-- Find duplicates and trace them to source batch/file
SELECT
    order_id,
    COUNT(*)                            AS dup_count,
    MIN(loaded_at)                      AS first_seen,
    MAX(loaded_at)                      AS last_seen,
    COUNT(DISTINCT source_batch_id)     AS distinct_batches,
    ARRAY_AGG(DISTINCT source_batch_id) AS batch_ids
FROM orders
GROUP BY order_id
HAVING COUNT(*) > 1
ORDER BY dup_count DESC
LIMIT 50;
```

```sql
-- Trino / Spark SQL: check if duplicates are exact row duplicates or key-only
WITH dupes AS (
    SELECT order_id
    FROM orders
    GROUP BY order_id
    HAVING COUNT(*) > 1
)
SELECT o.*
FROM orders o
JOIN dupes d ON o.order_id = d.order_id
ORDER BY o.order_id, o.loaded_at;
```

---

## 7. Lineage-Based Impact Analysis

### 7.1 dbt — Find All Downstream Models

```bash
# All nodes downstream of a model (+ means "and all descendants")
dbt ls --select "orders_mart+" --output name

# All nodes upstream and downstream (full lineage path)
dbt ls --select "+orders_mart+" --output name

# Find which models use a specific source
dbt ls --select "source:raw.orders+" --output path

# Generate a lineage graph to a file (requires dbt-artifacts or dbt-docs)
dbt docs generate && dbt docs serve
```

### 7.2 OpenLineage / Marquez API

```python
import requests

MARQUEZ_URL = "http://marquez:5000"

def get_downstream_datasets(namespace: str, job_name: str) -> list[dict]:
    """Return datasets produced by this job — these are the immediate blast radius."""
    resp = requests.get(
        f"{MARQUEZ_URL}/api/v1/namespaces/{namespace}/jobs/{job_name}",
        timeout=10,
    )
    resp.raise_for_status()
    job = resp.json()
    return job.get("outputs", [])  # list of {namespace, name} dicts


def get_dataset_lineage(namespace: str, dataset_name: str, depth: int = 3) -> dict:
    """Get complete upstream + downstream lineage graph for a dataset."""
    resp = requests.get(
        f"{MARQUEZ_URL}/api/v1/lineage",
        params={"nodeId": f"dataset:{namespace}:{dataset_name}", "depth": depth},
        timeout=10,
    )
    resp.raise_for_status()
    return resp.json()  # {graph: [{id, type, data, inEdges, outEdges}]}


# Example: find all jobs that READ from a broken table
def find_consumers(namespace: str, broken_table: str) -> list[str]:
    lineage = get_dataset_lineage(namespace, broken_table, depth=5)
    consumers = []
    for node in lineage.get("graph", []):
        if node["type"] == "JOB":
            for edge in node.get("inEdges", []):
                if broken_table in edge["origin"]:
                    consumers.append(node["id"])
    return consumers
```

### 7.3 DataHub GraphQL — Downstream Lineage

```python
import requests

DATAHUB_URL = "http://datahub-gms:8080"

LINEAGE_QUERY = """
query GetDownstreamLineage($urn: String!) {
  dataset(urn: $urn) {
    downstream: lineage(input: {direction: DOWNSTREAM, count: 100}) {
      relationships {
        entity {
          urn
          ... on Dataset {
            name
            platform { name }
            schemaMetadata { fields { fieldPath } }
          }
        }
      }
    }
  }
}
"""

def get_downstream_lineage(dataset_urn: str) -> list[str]:
    resp = requests.post(
        f"{DATAHUB_URL}/api/graphql",
        json={"query": LINEAGE_QUERY, "variables": {"urn": dataset_urn}},
        headers={"Authorization": f"Bearer {DATAHUB_TOKEN}"},
        timeout=10,
    )
    resp.raise_for_status()
    data = resp.json()
    relationships = (
        data.get("data", {})
            .get("dataset", {})
            .get("downstream", {})
            .get("relationships", [])
    )
    return [r["entity"]["urn"] for r in relationships]
```

### 7.4 Quick Manual Impact List

When automated lineage tools are unavailable:

```bash
# Grep dbt project SQL files for references to a broken table
grep -r "ref('orders_mart')" dbt_project/models/ --include="*.sql" -l
grep -r "source('raw', 'orders')" dbt_project/models/ --include="*.sql" -l

# Search Airflow DAGs for references to a table name
grep -r "orders_mart" airflow/dags/ --include="*.py" -l

# Search SQL files for a table name
find . -name "*.sql" | xargs grep -l "orders_mart"
```

---

## 8. Log Analysis Patterns

### 8.1 Structured Log Grep with jq

```bash
# Count errors by error type over time
cat application.log \
  | jq -r 'select(.level=="ERROR") | "\(.timestamp[:13]) \(.error_type // "unknown")"' \
  | sort | uniq -c | sort -rn

# Find first occurrence of a specific error
cat application.log \
  | jq 'select(.level=="ERROR" and (.message | contains("NullPointerException")))' \
  | jq -r '.timestamp' | sort | head -1

# Trace all log lines for a specific correlation ID
cat application.log \
  | jq -c 'select(.correlation_id == "abc-123-def")'

# Extract error messages and stack traces
cat application.log \
  | jq -r 'select(.level=="ERROR") | "\(.timestamp) | \(.message) | \(.exception // "")"'
```

### 8.2 Correlation ID Tracing Across Services

```python
import json
from pathlib import Path
from collections import defaultdict

def trace_by_correlation_id(log_files: list[str], correlation_id: str) -> list[dict]:
    """Collect all log events for a given correlation_id across multiple log files."""
    events = []
    for log_file in log_files:
        for line in Path(log_file).read_text().splitlines():
            try:
                entry = json.loads(line)
                if entry.get("correlation_id") == correlation_id:
                    entry["_source_file"] = log_file
                    events.append(entry)
            except json.JSONDecodeError:
                continue
    return sorted(events, key=lambda e: e.get("timestamp", ""))


# Usage
events = trace_by_correlation_id(
    log_files=[
        "/var/log/airflow/orders_ingest.log",
        "/var/log/spark/orders_transform.log",
        "/var/log/dbt/orders_mart.log",
    ],
    correlation_id="run-2024-06-01-orders",
)
for ev in events:
    print(f"{ev['timestamp']} [{ev['level']}] {ev['_source_file']}: {ev['message']}")
```

### 8.3 Error Frequency Analysis — When Did It Start?

```python
import json
from datetime import datetime
from collections import Counter
from pathlib import Path

def error_frequency_by_hour(log_file: str) -> None:
    """Count errors per hour to find when the issue started."""
    hourly_errors: Counter = Counter()
    hourly_total: Counter  = Counter()

    for line in Path(log_file).read_text().splitlines():
        try:
            entry = json.loads(line)
            ts = entry.get("timestamp", "")
            hour = ts[:13]  # "2024-06-01T14"
            hourly_total[hour] += 1
            if entry.get("level") in ("ERROR", "CRITICAL"):
                hourly_errors[hour] += 1
        except json.JSONDecodeError:
            continue

    print(f"{'Hour':<17} {'Total':>8} {'Errors':>8} {'Error%':>8}")
    print("-" * 45)
    for hour in sorted(hourly_total):
        total  = hourly_total[hour]
        errors = hourly_errors.get(hour, 0)
        pct    = 100.0 * errors / total if total else 0
        flag   = " <-- spike" if pct > 5 else ""
        print(f"{hour:<17} {total:>8} {errors:>8} {pct:>7.1f}%{flag}")
```

### 8.4 Airflow Log Aggregation with Pandas

```python
import re
import pandas as pd
from pathlib import Path

LOG_PATTERN = re.compile(
    r"\[(?P<ts>[^\]]+)\] \{[^}]+\} (?P<logger>\S+) - (?P<level>\w+) - (?P<message>.+)"
)

def parse_airflow_logs(log_path: str) -> pd.DataFrame:
    records = []
    for line in Path(log_path).read_text().splitlines():
        m = LOG_PATTERN.match(line)
        if m:
            records.append(m.groupdict())
    df = pd.DataFrame(records)
    if df.empty:
        return df
    df["ts"] = pd.to_datetime(df["ts"], utc=True)
    return df

# Usage
df = parse_airflow_logs("/opt/airflow/logs/orders_etl/load_task/2024-06-01/1.log")

# Show only errors
print(df[df["level"].isin(["ERROR", "CRITICAL"])][["ts", "logger", "message"]])

# Find first error timestamp
first_error = df[df["level"] == "ERROR"]["ts"].min()
print(f"First error at: {first_error}")
```

---

## 9. RCA Document Template

Use this template immediately after resolving an incident. Fill it within 24 hours while memory is fresh.

```markdown
# RCA: <Short Incident Title>

**Incident ID:** INC-YYYY-NNN
**Severity:**    P1 / P2 / P3
**Status:**      Resolved

---

## Incident Summary

| Field | Value |
|---|---|
| What broke | <one-line description of what failed> |
| Data affected | <tables, partitions, date ranges> |
| Users/systems impacted | <dashboards, reports, downstream pipelines> |
| Duration | <start datetime> → <end datetime> (<N hours>) |
| Detection method | Automated alert / user report / monitoring |

---

## Timeline (UTC)

| Time | Event |
|---|---|
| 2024-06-01 14:00 | Anomaly begins (root cause event) |
| 2024-06-01 15:42 | Alert fires / stakeholder reports issue |
| 2024-06-01 15:50 | On-call acknowledged, investigation started |
| 2024-06-01 16:15 | Root cause identified |
| 2024-06-01 16:45 | Fix deployed |
| 2024-06-01 17:00 | Data validated — incident resolved |
| 2024-06-01 17:30 | Stakeholders notified of resolution |

---

## Root Cause Statement

> <Single sentence: "X failed because Y, caused by Z.">

Example:
> The `orders_mart` model produced zero rows for 2024-06-01 because the incremental
> filter used `created_at` instead of `updated_at`, causing no new rows to be
> selected after a source migration changed the update timestamp field name.

---

## Contributing Factors

1. No data volume alert existed for `orders_mart` — zero rows went undetected for 90 minutes.
2. The source schema change was not communicated to the data engineering team.
3. The dbt model had no row-count test that would have failed the CI run.

---

## Evidence

- Airflow task logs: `gs://airflow-logs/orders_etl/orders_mart/2024-06-01/1.log`
- dbt run_results.json: `s3://artifacts/dbt/2024-06-01/run_results.json`
- Grafana dashboard showing row count drop: `https://grafana.internal/d/abc123`
- Source schema diff showing field rename: `https://github.com/org/source/pull/456`

---

## Fix Applied

```sql
-- Changed incremental filter from created_at to updated_at
-- File: models/marts/orders_mart.sql
{% if is_incremental() %}
WHERE updated_at > (SELECT MAX(updated_at) FROM {{ this }})
{% endif %}
```

Full refresh run to repopulate the partition:
```bash
dbt run --select orders_mart --full-refresh --vars '{"start_date": "2024-06-01"}'
```

---

## Prevention

| Action | Owner | Due Date |
|---|---|---|
| Add Great Expectations row_count check with warn < 1000, fail = 0 | @data-eng | 2024-06-08 |
| Add source schema change notification to Slack #data-eng-changes | @platform | 2024-06-15 |
| Add dbt test: `assert_table_has_rows` on orders_mart | @data-eng | 2024-06-08 |
| Update dbt model documentation with explicit timestamp field note | @data-eng | 2024-06-05 |
| Add schema drift detection in ingestion layer | @data-eng | 2024-06-22 |
```

---

## 10. Common Pitfalls in RCA

### Blaming the Person, Not the Process

An RCA that concludes "engineer X forgot to update the filter" is useless — it doesn't prevent recurrence. Correct framing: "the process had no automated check that would catch this class of error." Always ask: "What process, tool, or alert would have prevented a different engineer from making the same mistake?"

### Stopping at the First Cause

The first cause is almost always a symptom. Apply the **5-Whys** method:

```
1. Why did the dashboard show wrong revenue?
   → Because orders_mart had zero rows.

2. Why did orders_mart have zero rows?
   → Because the incremental filter matched nothing.

3. Why did the filter match nothing?
   → Because it used `created_at > max(created_at in target)`, but the source
     only updates `updated_at` on modifications.

4. Why was the wrong column used?
   → Because the model was written before the source added `updated_at`,
     and was never updated when the source schema changed.

5. Why was the model not updated when the source schema changed?
   → Because there is no process to notify the data team of source schema changes.

Root cause: no schema change notification process exists.
Fix target: automate source schema change detection and notification.
```

### Confusing Symptom with Cause

"The pipeline failed" is a symptom. "The Spark job failed" is still a symptom. "Executor 7 was OOM-killed during the shuffle phase because customer_id = NULL (3M rows) was all assigned to the same partition" is a cause.

### Missing Timezone / DST Issues

```python
# Common timezone bug: comparing naive and aware datetimes
from datetime import datetime
import pytz

# BAD: creates naive datetime — comparison with timezone-aware breaks
cutoff = datetime(2024, 3, 10, 2, 30)  # In US/Eastern, this time doesn't exist (DST gap)

# GOOD: always use UTC internally, convert only for display
import pendulum
cutoff = pendulum.datetime(2024, 3, 10, 7, 30, tz="UTC")  # 02:30 ET = 07:30 UTC

# DST off-by-one: on spring-forward Sunday, US/Eastern goes 1:59 -> 3:00
# A daily partition cutoff at midnight US/Eastern produces different UTC offsets
# in summer (UTC-4) vs winter (UTC-5) — partition boundary shifts by 1 hour
```

```sql
-- Always store and compare in UTC; convert in the SELECT for display
-- BAD
WHERE event_time >= '2024-03-10 00:00:00'  -- ambiguous without timezone

-- GOOD
WHERE event_time >= TIMESTAMP '2024-03-10 05:00:00 UTC'  -- midnight US/Eastern winter
-- or
WHERE event_time AT TIME ZONE 'America/New_York' >= DATE '2024-03-10'
```

### Assuming Idempotency

```python
# BAD pattern: assumes INSERT is idempotent, but on retry it creates duplicates
def load_partition(conn, date: str):
    conn.execute(f"""
        INSERT INTO orders SELECT * FROM orders_staging WHERE date = '{date}'
    """)
    # If this fails halfway through and retries, you get partial duplicates

# GOOD: make idempotency explicit
def load_partition(conn, date: str):
    conn.execute(f"DELETE FROM orders WHERE date = '{date}'")  # clean slate
    conn.execute(f"""
        INSERT INTO orders SELECT * FROM orders_staging WHERE date = '{date}'
    """)
    # Or use MERGE / INSERT ON CONFLICT
```

---

## Anti-Patterns

| Anti-Pattern | Problem | Correct Approach |
|---|---|---|
| Starting investigation in the middle (not at step 1) | Miss the actual first failure; chase downstream symptoms | Always reconstruct timeline first |
| Running fixes before collecting evidence | Fix masks root cause; no evidence for prevention | Collect evidence first, fix second |
| Fixing only the immediate failure without checking blast radius | Downstream tables still broken | Assess blast radius before declaring resolved |
| RCA document written weeks later | Key details forgotten, timeline inaccurate | Write within 24 hours of resolution |
| Declaring root cause without ruling out alternatives | Wrong fix; recurrence | Explicitly eliminate alternative hypotheses |
| "Works on my machine" — not reproducing in prod environment | Different data volume, different configs | Always reproduce with prod data sample or exact prod config |
| Checking only the last task log, not full DAG history | Missing recurring pattern (fails every Monday at DST change) | Query task_instance table for historical pattern |
| Treating data anomaly as code bug | Fix code when data is actually correct (expected outlier) | Sample the data before assuming a code change caused it |
| Skipping stakeholder communication during RCA | Stakeholders make incorrect decisions based on bad data | Send a brief "we are investigating" message within 15 minutes of confirmation |
| No post-incident action items | Same incident recurs in 3 months | Every RCA must have at least one concrete prevention action item with owner and date |

---

## References to Consult When Needed

- `skills/airflow_dags/SKILL.md` — Full Airflow operator, sensor, XCom, TaskFlow reference
- `skills/pyspark_etl/SKILL.md` — PySpark DataFrame transforms, joins, write patterns
- `skills/pyspark_streaming/SKILL.md` — Structured Streaming checkpointing, watermarks
- `skills/dbt_core/SKILL.md` — dbt materializations, incremental strategies, is_incremental()
- `skills/dbt_trino/SKILL.md` — dbt + Trino/Iceberg specifics
- `skills/soda_core/SKILL.md` — SodaCL checks, freshness, volume, distribution checks
- `skills/great_expectations/SKILL.md` — Expectation suites, Checkpoint actions
- `skills/openlineage/SKILL.md` — RunEvent spec, Marquez API, column-level lineage
- `skills/datahub_catalog/SKILL.md` — DataHub GraphQL, lineage API, ingestion recipes
- `skills/de_production_readiness/SKILL.md` — Idempotency, retry, SLA monitoring, reconciliation
- `skills/delta_lake/SKILL.md` — Time Travel (RESTORE, VERSION AS OF), Change Data Feed
- `skills/trino_iceberg/SKILL.md` — Iceberg time travel, metadata tables, table maintenance
- [Google SRE Book — Postmortem Culture](https://sre.google/sre-book/postmortem-culture/)
- [Airflow Task Instance States — Apache Docs](https://airflow.apache.org/docs/apache-airflow/stable/concepts/tasks.html#task-instances)
- [Spark Troubleshooting Guide — Databricks](https://docs.databricks.com/en/optimizations/troubleshoot-performance.html)
- [dbt run_results.json Schema — dbt Docs](https://docs.getdbt.com/reference/artifacts/run-results-json)
