---
name: dataops-root-cause-analysis
description: Root cause analysis for DataOps incidents — 5-Why analysis for pipeline failures, failure taxonomy (infrastructure/data/logic/dependency/config/concurrency), Airflow diagnosis (task state SQL/scheduler heartbeat/DagBag errors), Spark diagnosis (OOM/skew/FetchFailed/serialization), Kafka consumer lag spike RCA, data quality anomaly investigation (volume/freshness/distribution shift), log correlation across components, timeline reconstruction, impact quantification SQL
---

# DataOps Root Cause Analysis

## When to Use

- A production pipeline failed and you need to find the root cause
- Data quality anomaly detected (volume drop, null spike, distribution shift)
- SLA breach investigation
- Diagnosing Airflow task failures, Spark job failures, or Kafka consumer lag
- Reconstructing a timeline of events for a postmortem

---

## Failure Taxonomy

```
Pipeline Failure
├── Infrastructure
│   ├── Node OOM (pod eviction)
│   ├── Disk full (HDFS/PVC)
│   ├── Network timeout (S3/Trino unreachable)
│   └── Scheduler crash (heartbeat stale)
├── Data
│   ├── Volume anomaly (upstream sent 0 rows)
│   ├── Schema change (new column / renamed column)
│   ├── Data quality (nulls in required fields)
│   └── Late arriving data (source system delay)
├── Logic
│   ├── Query timeout (no partition filter)
│   ├── Incremental logic bug (wrong watermark)
│   └── Type casting error (int overflow)
├── Dependency
│   ├── Upstream DAG failed
│   ├── External API down
│   └── dbt model dependency broken
├── Configuration
│   ├── Wrong connection string
│   ├── Credential expired
│   └── Resource limit too low
└── Concurrency
    ├── Two DAG runs writing same partition
    └── Lock contention on target table
```

---

## Airflow Diagnosis

```sql
-- Step 1: Find the failed task run
SELECT
    dag_id,
    task_id,
    run_id,
    state,
    start_date,
    end_date,
    try_number,
    TIMESTAMPDIFF(SECOND, start_date, COALESCE(end_date, NOW())) AS duration_sec
FROM task_instance
WHERE dag_id = 'etl_orders'
  AND state IN ('failed', 'upstream_failed', 'skipped')
  AND start_date > NOW() - INTERVAL '24 HOURS'
ORDER BY start_date DESC
LIMIT 20;

-- Step 2: Check for concurrent runs (concurrency issue)
SELECT
    dag_id, run_id, state, start_date, end_date
FROM dag_run
WHERE dag_id = 'etl_orders'
  AND start_date > NOW() - INTERVAL '24 HOURS'
ORDER BY start_date DESC;

-- Step 3: Recent task retries (transient errors)
SELECT
    dag_id, task_id, try_number, state, start_date
FROM task_instance
WHERE dag_id = 'etl_orders'
  AND try_number > 1
  AND start_date > NOW() - INTERVAL '7 DAYS'
ORDER BY start_date DESC;

-- Step 4: Scheduler heartbeat (is scheduler alive?)
SELECT
    job_type,
    hostname,
    latest_heartbeat,
    TIMESTAMPDIFF(SECOND, latest_heartbeat, NOW()) AS seconds_since_heartbeat,
    state
FROM job
WHERE job_type = 'SchedulerJob'
ORDER BY latest_heartbeat DESC
LIMIT 5;
```

```bash
# Step 5: Get task logs
airflow tasks logs etl_orders load_orders 2024-01-15 --task-id load_orders

# Step 6: Check for DagBag import errors
airflow dags list-import-errors

# Step 7: Test task in isolation
airflow tasks test etl_orders load_orders 2024-01-15
```

---

## Spark Diagnosis

```bash
# OOM: find failed executor in Spark UI or driver logs
kubectl logs -n spark $DRIVER_POD | grep -E "OutOfMemoryError|Killing.*over memory limit"

# Find executor OOM events
kubectl get events -n spark | grep -E "OOMKilled|Evicted"

# Skew detection: check task duration distribution
# In Spark UI: Stages tab → look for tasks with >> median duration
# Or from history server:
curl -s http://spark-history:18080/api/v1/applications/$APP_ID/stages | \
  jq '.[] | {stageId, numTasks, maxTaskDuration, medianDuration: (.taskMetrics.executorRunTime / .numTasks)}'

# FetchFailed (shuffle read error):
kubectl logs $DRIVER_POD | grep "FetchFailed\|org.apache.spark.shuffle"

# Serialization error:
kubectl logs $DRIVER_POD | grep "NotSerializableException\|Task not serializable"
```

```python
# Spark skew detection
def detect_partition_skew(df, key_col: str) -> bool:
    counts = df.groupBy(key_col).count().collect()
    sizes = [row["count"] for row in counts]
    if not sizes:
        return False
    median = sorted(sizes)[len(sizes) // 2]
    max_size = max(sizes)
    # Skewed if max partition is > 10x median
    return max_size > 10 * median
```

---

## Kafka Consumer Lag RCA

```bash
# Step 1: Current consumer lag
kafka-consumer-groups.sh \
  --bootstrap-server kafka:9092 \
  --group orders-processor \
  --describe

# Step 2: Was the lag sudden (consumer restart) or gradual (throughput drop)?
# Check consumer group history
kafka-consumer-groups.sh \
  --bootstrap-server kafka:9092 \
  --group orders-processor \
  --describe --verbose

# Step 3: Check producer throughput
kafka-topics.sh --bootstrap-server kafka:9092 \
  --topic orders-raw \
  --describe  # check partition count, replication factor

# Step 4: Consumer pod restarts (OOM / crash loop)
kubectl get pods -n kafka -l app=orders-consumer | grep -v Running

# Step 5: Consumer log errors
kubectl logs -n kafka -l app=orders-consumer --since=1h \
  | grep -E "ERROR|WARN|exception" | tail -50

# Step 6: Check if specific partitions are stuck
kafka-consumer-groups.sh \
  --bootstrap-server kafka:9092 \
  --group orders-processor \
  --describe \
  | sort -k5 -rn   # sort by lag, largest first
```

---

## Data Quality Anomaly Investigation

```sql
-- Volume drop analysis
WITH daily_counts AS (
    SELECT
        order_date,
        COUNT(*) AS row_count,
        AVG(COUNT(*)) OVER (
            ORDER BY order_date
            ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING
        ) AS rolling_avg_7d,
        STDDEV(COUNT(*)) OVER (
            ORDER BY order_date
            ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING
        ) AS rolling_std_7d
    FROM gold.fact_orders
    WHERE order_date >= CURRENT_DATE - INTERVAL '14' DAY
    GROUP BY order_date
)
SELECT
    order_date,
    row_count,
    ROUND(rolling_avg_7d) AS expected_rows,
    ROUND((row_count - rolling_avg_7d) / NULLIF(rolling_std_7d, 0), 2) AS z_score,
    CASE
        WHEN row_count < rolling_avg_7d * 0.5 THEN 'SEVERE_DROP'
        WHEN row_count < rolling_avg_7d * 0.8 THEN 'MODERATE_DROP'
        WHEN row_count > rolling_avg_7d * 1.5 THEN 'SPIKE'
        ELSE 'NORMAL'
    END AS anomaly_type
FROM daily_counts
ORDER BY order_date DESC;

-- Null explosion: which columns spiked in nulls?
SELECT
    'order_id' AS column_name,
    SUM(CASE WHEN order_id IS NULL THEN 1 ELSE 0 END) AS null_count,
    COUNT(*) AS total_rows,
    ROUND(100.0 * SUM(CASE WHEN order_id IS NULL THEN 1 ELSE 0 END) / COUNT(*), 2) AS null_pct
FROM gold.fact_orders
WHERE order_date = CURRENT_DATE
UNION ALL
SELECT 'customer_id', SUM(CASE WHEN customer_id IS NULL THEN 1 ELSE 0 END), COUNT(*),
       ROUND(100.0 * SUM(CASE WHEN customer_id IS NULL THEN 1 ELSE 0 END) / COUNT(*), 2)
FROM gold.fact_orders WHERE order_date = CURRENT_DATE;
```

---

## Timeline Reconstruction

```python
def reconstruct_incident_timeline(dag_id: str, incident_date: str) -> list[dict]:
    """Build a chronological timeline of events for RCA."""
    events = []

    # 1. Airflow task events
    task_events = query("""
        SELECT start_date AS ts, 'AIRFLOW' AS source,
               CONCAT(task_id, ': ', state) AS event
        FROM task_instance
        WHERE dag_id = %s
          AND DATE(start_date) = %s
        ORDER BY start_date
    """, [dag_id, incident_date])
    events.extend(task_events)

    # 2. Kubernetes events (pod restarts, evictions)
    k8s_events = subprocess.run([
        "kubectl", "get", "events", "-n", "airflow",
        "--field-selector", f"involvedObject.namespace=airflow",
        "-o", "json"
    ], capture_output=True, text=True)
    # parse and add to timeline...

    # 3. Infrastructure alerts that fired
    alert_events = query_prometheus_alerts(incident_date)
    events.extend(alert_events)

    return sorted(events, key=lambda x: x["ts"])
```

---

## 5-Why Template

```markdown
## Incident: ETL Orders Failed 2024-01-15

**What failed**: `etl_orders` DAG failed on `load_orders` task at 03:47 UTC

**Why 1**: The `load_orders` task raised `OperationalError: connection refused`
**Why 2**: The Trino coordinator pod was OOMKilled at 03:45 UTC
**Why 3**: A concurrent ad-hoc query consumed all coordinator memory
**Why 4**: No resource group limit existed for ad-hoc queries in production
**Why 5**: Resource groups were only configured for the `default` group; the `adhoc` workgroup had unlimited memory

**Root Cause**: Missing memory limit on Trino `adhoc` resource group allowed ad-hoc queries to starve ETL workloads

**Fix**: Set `soft_memory_limit = 40%` on the `adhoc` resource group; `etl` group gets `soft_memory_limit = 50%`

**Detection gap**: No alert on Trino coordinator memory pressure (added)
**Prevention**: Resource group limits now enforced via Terraform + OPA policy
```

---

## Anti-Patterns

1. **Jumping to fix without timeline reconstruction** — fixing the symptom (restarting a pod) without understanding the cause guarantees recurrence.
2. **Looking at only the failing task** — failure often starts upstream (scheduler OOM, network timeout, source system delay); check the full dependency chain.
3. **Ignoring the retry pattern** — a task failing on try #3 but succeeding on try #4 suggests transient infrastructure issue, not code bug.
4. **No structured logging** — unstructured logs make it impossible to correlate events across Airflow, Spark, and Kafka; enforce JSON logging everywhere.
5. **Stopping at the first "Why"** — "the Spark job ran out of memory" is not a root cause; ask why 5 times to find the actionable fix.

---

## References

- Airflow task diagnosis: `airflow.apache.org/docs/apache-airflow/stable/core-concepts/tasks.html`
- Spark troubleshooting: `spark.apache.org/docs/latest/troubleshooting.html`
- Google SRE: blameless postmortems: `sre.google/sre-book/postmortem-culture/`
- Related skills: `[[de-rca]]`, `[[dataops-postmortem-generator]]`, `[[dataops-airflow-observability]]`
