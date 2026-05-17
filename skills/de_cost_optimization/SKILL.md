---
name: de-cost-optimization
description: DE cost optimization — query cost analysis (Trino/Spark/ClickHouse/BigQuery), compute right-sizing, storage tier recommendations, spot/preemptible instances, partition pruning, Z-order/clustering, materialized view economics, data lifecycle policies, FinOps tagging, cost attribution, budget alerts
---

# DE Cost Optimization

## When to Use

Load this skill when the user needs to:
- Review a cloud bill and identify which pipelines, queries, or clusters drive cost
- Right-size a Spark, Trino, ClickHouse, or Airflow cluster (instance type, memory, core count)
- Reduce query scan costs via partition pruning, Z-order clustering, or file compaction
- Move data to cheaper storage tiers (S3-IA, Glacier, ClickHouse cold volumes)
- Implement spot/preemptible instances for Spark executors or Airflow workers
- Decide whether to materialize a view or a summary table (refresh cost vs query savings)
- Define or enforce a data lifecycle and retention policy
- Set up AWS/GCP budget alerts, FinOps tagging, or per-team cost attribution
- Run a FinOps review and produce a prioritized cost reduction plan

---

## 1. Cost Model Overview

Every data engineering cloud bill breaks down into three levers. Attack them in this order — storage is cheapest per GB, but compute at scale dominates the total.

| Lever | Driver | Typical Share | First Action |
|---|---|---|---|
| **Compute** | vCPU-hours × instance price | 50-70% | Right-size + spot instances |
| **Storage** | GB-months × tier price | 20-35% | Tiering + compaction + lifecycle rules |
| **Data transfer** | GB egressed across AZs/regions | 5-15% | Co-locate compute with storage; avoid cross-region reads |

### How Each Scales

- **Compute** scales with cluster lifetime × size. A cluster that is 2× over-provisioned and never auto-scales burns double money even when idle. Idle Spark clusters during off-peak hours are the single most common waste category.
- **Storage** scales with raw data volume × replication factor × tier. Columnar + compressed formats (Parquet/zstd) reduce this 5-10× vs raw JSON/CSV.
- **Data transfer** is often invisible until bill arrives. Reading Parquet on S3 from a same-region EMR cluster is free; reading it cross-region costs $0.02/GB and can exceed compute cost on large jobs.

### Which to Attack First

```
Priority 1 — Compute
  ├─ Idle clusters / always-on servers with <20% avg CPU
  ├─ Fixed-size clusters that could be spot + auto-scale
  └─ Over-provisioned executor memory

Priority 2 — Storage
  ├─ Raw JSON/CSV on S3 Standard (should be Parquet + tiered)
  ├─ Small files (<10 MB) causing excess LIST/GET API calls
  └─ Data past retention policy still sitting in S3 Standard

Priority 3 — Data Transfer
  ├─ Queries on cross-region tables
  └─ Shuffles writing to S3 between EMR and Glue in different AZs
```

---

## 2. Query Cost Analysis

### 2.1 Trino / Presto

#### Reading EXPLAIN (TYPE DISTRIBUTED)

```sql
EXPLAIN (TYPE DISTRIBUTED)
SELECT customer_id, SUM(amount) AS revenue
FROM orders
WHERE order_date >= DATE '2024-01-01'
  AND region = 'US'
GROUP BY customer_id;
```

Key tokens to look for in the output:

| Token | Meaning | Cost Signal |
|---|---|---|
| `TableScan` | Rows and bytes read from storage | High bytes → missing partition filter |
| `Filter` | Pushed down to connector vs applied in Trino | `filterPushdown=false` → scan waste |
| `RemoteExchange REPARTITION` | Network shuffle | Many reshuffles → expensive join |
| `Estimates: {rows=..., cpu=..., network=..., memory=...}` | Planner estimates | Cross-check with actual |

Partition filter pushdown is confirmed when the `TableScan` node shows `partitionedBy` columns in its predicate. If your `WHERE order_date = ...` clause does NOT appear in the TableScan predicate, it means the filter is not pushed down — you are scanning the whole table and filtering in memory.

#### Finding Expensive Queries via system tables

```sql
-- Top 20 most expensive queries in the last 24 hours (Trino)
SELECT
    query_id,
    LEFT(query, 120)                                          AS query_snippet,
    user,
    source,
    state,
    CAST(read_data_size AS VARCHAR)                           AS scanned,
    -- Estimate cost at $5/TB scanned (Athena pricing proxy)
    ROUND(read_data_size / 1e12 * 5.0, 4)                    AS est_cost_usd,
    DATE_DIFF('second', created, "end")                       AS wall_sec,
    peak_memory_bytes / 1e9                                   AS peak_memory_gb,
    cpu_time_ms / 1000.0                                      AS cpu_sec
FROM system.runtime.query_history
WHERE created >= NOW() - INTERVAL '24' HOUR
  AND state = 'FINISHED'
ORDER BY read_data_size DESC
NULLS LAST
LIMIT 20;
```

```sql
-- Aggregate cost per source (team / application) over last 7 days
SELECT
    source,
    COUNT(*)                                                  AS query_count,
    ROUND(SUM(read_data_size) / 1e12, 3)                     AS total_tb_scanned,
    ROUND(SUM(read_data_size) / 1e12 * 5.0, 2)               AS est_total_cost_usd,
    ROUND(AVG(DATE_DIFF('second', created, "end")), 1)        AS avg_wall_sec
FROM system.runtime.query_history
WHERE created >= NOW() - INTERVAL '7' DAY
  AND state = 'FINISHED'
GROUP BY source
ORDER BY total_tb_scanned DESC;
```

#### File Size Optimization

- **Too-small files** (<10 MB): excessive S3 LIST + GET overhead; Trino opens one reader thread per file, so 10,000 files × overhead > reading one 1 GB file.
- **Too-large files** (>1 GB): cannot be split across workers in parallel; one task processes the whole file.
- **Sweet spot**: 128–512 MB per file after compression.

```sql
-- Find partitions with small-file problem (Iceberg metadata)
SELECT
    partition,
    COUNT(*)           AS file_count,
    AVG(file_size_in_bytes) / 1e6 AS avg_file_mb,
    SUM(file_size_in_bytes) / 1e9 AS total_gb
FROM "catalog"."schema"."table$files"
GROUP BY partition
HAVING COUNT(*) > 100 OR AVG(file_size_in_bytes) < 10e6
ORDER BY file_count DESC
LIMIT 30;
```

### 2.2 Spark (EMR / Databricks / Self-Hosted)

#### Spark UI Cost Signals

Navigate: Jobs → select expensive job → Stages → sort by **Duration** descending.

Key indicators:
- **Shuffle Read/Write size**: A stage with 500 GB shuffle write followed by a stage with 500 GB shuffle read means you are moving 500 GB over the network. Each GB shuffled costs executor CPU time + network I/O + potential spill to disk.
- **GC time % > 10%**: executors spending >10% in garbage collection → memory is too low for the data size; increase `spark.executor.memory` or reduce partition size.
- **Task skew**: longest task 10× median → one partition is much larger than others (data skew). Fix with salting or AQE.
- **Spill (Memory) / Spill (Disk)**: non-zero spill means tasks ran out of memory and wrote to disk → slower and wastes I/O.

#### EXPLAIN COST

```python
spark.sql("""
    EXPLAIN COST
    SELECT customer_id, SUM(amount) AS revenue
    FROM orders
    WHERE order_date >= '2024-01-01'
    GROUP BY customer_id
""").show(truncate=False)
```

Look for `Statistics(sizeInBytes=..., rowCount=...)`. If estimates are wildly wrong, run `ANALYZE TABLE orders COMPUTE STATISTICS FOR ALL COLUMNS` to refresh them — bad statistics cause the planner to choose wrong join strategies (BroadcastHashJoin vs SortMergeJoin).

#### Executor Sizing Formula

```
spark.executor.memory  = 4–8 GB per executor core (memory-heavy ETL)
spark.executor.cores   = 4–5 cores per executor (limits context switches)
Total executors        = (Total vCPUs allocated) / spark.executor.cores

Example for r5.2xlarge (8 vCPU, 64 GB):
  spark.executor.cores   = 4
  spark.executor.memory  = 12g   (leaving 4g for OS + overhead)
  spark.executor.memoryOverhead = 2g
  Executors per node     = 2
```

#### Dynamic Allocation vs Fixed: Cost Comparison

```python
# Fixed cluster: 10 executors running for 8 hours
fixed_cost = 10 * 8 * instance_cost_per_hour   # e.g. 10 * 8 * $0.50 = $40

# Dynamic allocation: average 3 executors over 8 hours
dynamic_cost = 3 * 8 * instance_cost_per_hour  # $12  → 70% savings

# Enable dynamic allocation
spark = (
    SparkSession.builder
    .config("spark.dynamicAllocation.enabled", "true")
    .config("spark.dynamicAllocation.minExecutors", "1")
    .config("spark.dynamicAllocation.maxExecutors", "50")
    .config("spark.dynamicAllocation.initialExecutors", "2")
    .config("spark.dynamicAllocation.executorIdleTimeout", "120s")
    .config("spark.dynamicAllocation.schedulerBacklogTimeout", "30s")
    .config("spark.shuffle.service.enabled", "true")   # required for dynamic alloc
    .getOrCreate()
)
```

#### Reading EMR Utilization from CloudWatch

```python
import boto3
from datetime import datetime, timedelta

def get_cluster_cpu_utilization(cluster_id: str, days: int = 7) -> float:
    """Return average YARNMemoryAvailablePercentage over last N days."""
    cw = boto3.client("cloudwatch", region_name="us-east-1")
    resp = cw.get_metric_statistics(
        Namespace="AWS/ElasticMapReduce",
        MetricName="YARNMemoryAvailablePercentage",
        Dimensions=[{"Name": "JobFlowId", "Value": cluster_id}],
        StartTime=datetime.utcnow() - timedelta(days=days),
        EndTime=datetime.utcnow(),
        Period=3600,
        Statistics=["Average"],
    )
    datapoints = resp["Datapoints"]
    if not datapoints:
        return 0.0
    avg = sum(d["Average"] for d in datapoints) / len(datapoints)
    return round(100 - avg, 1)   # invert: available% → used%
```

### 2.3 ClickHouse

#### Identifying Expensive Queries

```sql
-- Top 20 most expensive queries by bytes read, last 24 hours
SELECT
    query_id,
    user,
    LEFT(query, 100)                                       AS query_snippet,
    read_bytes / 1e9                                       AS read_gb,
    read_rows,
    memory_usage / 1e9                                     AS memory_gb,
    query_duration_ms / 1000.0                             AS duration_sec,
    -- Mark queries that could benefit from projection
    result_rows < read_rows / 100                          AS high_reduction_ratio
FROM system.query_log
WHERE event_time >= NOW() - INTERVAL 1 DAY
  AND type = 'QueryFinish'
  AND query_kind = 'Select'
ORDER BY read_bytes DESC
LIMIT 20;
```

```sql
-- Aggregate read cost by user over last 7 days
SELECT
    user,
    COUNT(*)                        AS query_count,
    ROUND(SUM(read_bytes) / 1e12, 3) AS total_tb_read,
    ROUND(AVG(query_duration_ms) / 1000, 1) AS avg_sec,
    ROUND(SUM(memory_usage) / 1e12, 3)       AS total_memory_tb
FROM system.query_log
WHERE event_time >= NOW() - INTERVAL 7 DAY
  AND type = 'QueryFinish'
  AND query_kind = 'Select'
GROUP BY user
ORDER BY total_tb_read DESC;
```

#### Verifying Projection Usage

```sql
-- Check whether a projection is being used for a specific query
-- Run EXPLAIN SELECT ... and look for "ReadFromMergeTree (projection: <name>)"
EXPLAIN
SELECT region, SUM(amount)
FROM orders
WHERE order_date >= '2024-01-01'
GROUP BY region;

-- List all projections in a table
SELECT
    name,
    part_name,
    rows,
    data_compressed_bytes / 1e6 AS compressed_mb
FROM system.projection_parts
WHERE table = 'orders'
ORDER BY data_compressed_bytes DESC;
```

#### Cold vs Hot Data Cost Model (TTL MOVE)

```sql
-- Configure tiered storage: hot data on NVMe, cold data on S3
ALTER TABLE events
    MODIFY TTL
        event_date + INTERVAL 30 DAY TO VOLUME 'cold_s3',  -- move to S3 after 30d
        event_date + INTERVAL 365 DAY DELETE;               -- delete after 1 year

-- Estimate cost: if S3 = $0.023/GB-month vs NVMe = $0.10/GB-month
-- Moving 10 TB of 31-90 day data from NVMe to S3 saves ~$0.077/GB-month = $770/month
```

### 2.4 BigQuery

#### Finding Expensive Queries

```sql
-- Top 20 most bytes-billed queries, last 7 days (BigQuery INFORMATION_SCHEMA)
SELECT
    job_id,
    user_email,
    LEFT(query, 120)                                         AS query_snippet,
    total_bytes_billed / POW(1024, 4)                        AS tb_billed,
    -- On-demand: $6.25/TB
    ROUND(total_bytes_billed / POW(1024, 4) * 6.25, 4)      AS est_cost_usd,
    total_slot_ms / 1000.0 / 3600.0                          AS slot_hours,
    TIMESTAMP_DIFF(end_time, start_time, SECOND)             AS duration_sec,
    creation_time
FROM `region-us`.INFORMATION_SCHEMA.JOBS_BY_PROJECT
WHERE creation_time >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 7 DAY)
  AND job_type = 'QUERY'
  AND state = 'DONE'
  AND error_result IS NULL
ORDER BY total_bytes_billed DESC
LIMIT 20;
```

```sql
-- Cost by user over last 30 days
SELECT
    user_email,
    COUNT(*)                                                  AS query_count,
    ROUND(SUM(total_bytes_billed) / POW(1024, 4), 2)         AS total_tb_billed,
    ROUND(SUM(total_bytes_billed) / POW(1024, 4) * 6.25, 2)  AS est_cost_usd,
    ROUND(SUM(total_slot_ms) / 1e6, 1)                       AS total_slot_ksec
FROM `region-us`.INFORMATION_SCHEMA.JOBS_BY_PROJECT
WHERE creation_time >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 30 DAY)
  AND job_type = 'QUERY'
GROUP BY user_email
ORDER BY est_cost_usd DESC;
```

#### Partition Expiration and Clustering Verification

```sql
-- Check whether clustering columns are being used (table metadata)
SELECT
    table_name,
    clustering_fields,
    partition_expiration_days,
    require_partition_filter   -- TRUE = force WHERE on partition column
FROM `project.dataset`.INFORMATION_SCHEMA.TABLES
WHERE table_name = 'orders';

-- Identify tables without partition filter requirement (cost risk)
SELECT table_name, row_count, size_bytes / 1e9 AS size_gb
FROM `project.dataset`.INFORMATION_SCHEMA.TABLE_STORAGE
WHERE total_partitions > 10
  AND table_name NOT IN (
      SELECT table_name FROM `project.dataset`.INFORMATION_SCHEMA.TABLES
      WHERE require_partition_filter = TRUE
  )
ORDER BY size_bytes DESC;
```

---

## 3. Compute Right-Sizing

### Memory-to-vCPU Ratios for DE Workloads

| Engine | Recommended GB/core | Notes |
|---|---|---|
| Spark (shuffle-heavy ETL) | 6–8 GB/core | Joins and aggregations need more memory |
| Spark (lightweight transforms) | 4 GB/core | Simple projections, filters |
| Trino coordinator | 4–8 GB/core | Query planning is CPU-bound |
| Trino worker | 2–4 GB/core | Memory-mapped reads; spill to disk if too low |
| Airflow worker (I/O-bound tasks) | 1–2 GB/core | Mostly waiting on network/DB |
| Airflow worker (CPU-bound transforms) | 4 GB/core | Local pandas, Python ETL |
| ClickHouse server | 4–8 GB/core | Vectorized execution benefits from large L3 cache |

### Identifying Over-Provisioned Clusters

A cluster with average CPU utilization below 20% over 7 days is over-provisioned. Correct action is to either downsize or enable auto-scaling.

```python
import boto3
from datetime import datetime, timedelta

def is_over_provisioned(cluster_id: str, threshold_pct: float = 20.0) -> bool:
    """Return True if EMR cluster avg CPU < threshold over last 7 days."""
    cw = boto3.client("cloudwatch", region_name="us-east-1")
    resp = cw.get_metric_statistics(
        Namespace="AWS/ElasticMapReduce",
        MetricName="CoreNodesPending",
        Dimensions=[{"Name": "JobFlowId", "Value": cluster_id}],
        StartTime=datetime.utcnow() - timedelta(days=7),
        EndTime=datetime.utcnow(),
        Period=86400,
        Statistics=["Average"],
    )
    # Also check YARNMemoryAvailablePercentage — high available% = over-provisioned
    resp2 = cw.get_metric_statistics(
        Namespace="AWS/ElasticMapReduce",
        MetricName="YARNMemoryAvailablePercentage",
        Dimensions=[{"Name": "JobFlowId", "Value": cluster_id}],
        StartTime=datetime.utcnow() - timedelta(days=7),
        EndTime=datetime.utcnow(),
        Period=3600,
        Statistics=["Average"],
    )
    datapoints = resp2["Datapoints"]
    if not datapoints:
        return False
    avg_available = sum(d["Average"] for d in datapoints) / len(datapoints)
    avg_used = 100 - avg_available
    return avg_used < threshold_pct
```

### Kubernetes Resource Requests vs Limits

Every pod that requests 4 CPU but uses 0.5 CPU reserves 3.5 CPU that the cluster cannot schedule elsewhere — you pay for reserved-but-idle capacity.

```yaml
# Over-provisioned (anti-pattern): requests 4 CPU but job uses ~0.5 CPU
resources:
  requests:
    cpu: "4"
    memory: "16Gi"
  limits:
    cpu: "8"
    memory: "32Gi"

# Right-sized (based on 7-day P95 from Prometheus):
resources:
  requests:
    cpu: "1"        # P50 usage + 20% headroom
    memory: "4Gi"
  limits:
    cpu: "2"        # burst headroom
    memory: "6Gi"
```

```bash
# Find over-provisioned pods: requested >> actual usage (requires metrics-server)
kubectl top pods -n data-engineering --sort-by=cpu | head -30

# Goldilocks can auto-recommend right-sized requests/limits
helm install goldilocks fairwinds-stable/goldilocks
kubectl label namespace data-engineering goldilocks.fairwinds.com/enabled=true
```

### Auto-Scaling Rules

```yaml
# Kubernetes HPA for Airflow workers — scale at 70% CPU
apiVersion: autoscaling/v2
kind: HorizontalPodAutoscaler
metadata:
  name: airflow-worker-hpa
  namespace: airflow
spec:
  scaleTargetRef:
    apiVersion: apps/v1
    kind: Deployment
    name: airflow-worker
  minReplicas: 1
  maxReplicas: 20
  metrics:
    - type: Resource
      resource:
        name: cpu
        target:
          type: Utilization
          averageUtilization: 70
  behavior:
    scaleDown:
      stabilizationWindowSeconds: 600   # wait 10 min before scaling down
      policies:
        - type: Pods
          value: 2
          periodSeconds: 120
    scaleUp:
      stabilizationWindowSeconds: 60
      policies:
        - type: Percent
          value: 100
          periodSeconds: 30
```

---

## 4. Spot / Preemptible Instances Strategy

### Which Workloads Are Spot-Safe

| Workload | Spot-Safe? | Reason |
|---|---|---|
| Spark executors (batch ETL) | Yes | Checkpointed; tasks retry automatically |
| Airflow KubernetesPodOperator workers | Yes | Task is atomic; Airflow retries on failure |
| Trino workers (stateless query nodes) | Yes | Query fails and is retried from client |
| dbt model runs | Yes | dbt retries or re-runs are safe |
| Spark streaming executors | Conditional | Only if checkpointing is enabled |
| Spark driver | No | Loss kills the entire job with no recovery |
| Airflow scheduler | No | Stateful; loss causes DAG execution gaps |
| Kafka brokers | No | Stateful; losing a broker risks data loss |
| ClickHouse server (primary) | No | Stateful storage engine |
| Flink JobManager | No | Checkpoint coordinator; must be stable |

### Checkpointing Requirement for Spot-Safe Spark Jobs

```python
# A Spark job is spot-safe ONLY when:
# 1. Checkpoint location is on persistent storage (S3, HDFS)
# 2. Task max failures is high enough to survive interruptions
# 3. Job output is idempotent (partition overwrite or MERGE, not append)

spark = (
    SparkSession.builder
    .appName("spot_safe_etl")
    .config("spark.task.maxFailures", "8")          # allow more task retries
    .config("spark.stage.maxConsecutiveAttempts", "8")
    .config("spark.speculation", "false")            # disable speculation on spot
    .config("spark.checkpoint.compress", "true")
    .getOrCreate()
)

sc = spark.sparkContext
sc.setCheckpointDir("s3://my-checkpoints/spark/spot_safe_etl/")
```

### AWS Spot Interruption Handler Pattern

```python
# Install on each Spot instance via EC2 user-data or init action
# Polls IMDS for interruption notice every 5 seconds; on SIGTERM gracefully drains

import threading
import signal
import urllib.request
import logging

logger = logging.getLogger("spot_handler")

_INTERRUPTED = threading.Event()

def _poll_interruption_notice(interval: float = 5.0):
    """AWS provides a 2-minute warning via IMDS before spot reclamation."""
    while not _INTERRUPTED.is_set():
        try:
            url = "http://169.254.169.254/latest/meta-data/spot/interruption-action"
            with urllib.request.urlopen(url, timeout=1) as r:
                action = r.read().decode()
                logger.warning("spot_interruption_notice", extra={"action": action})
                _INTERRUPTED.set()
                # Graceful shutdown: flush buffers, checkpoint, notify Spark
                _graceful_shutdown()
        except Exception:
            pass  # 404 = no interruption notice yet
        _INTERRUPTED.wait(timeout=interval)


def _graceful_shutdown():
    """Flush write buffers and send SIGTERM to Spark driver."""
    import os
    logger.info("initiating_graceful_shutdown")
    # Allow Spark tasks to finish current micro-batch
    os.kill(os.getpid(), signal.SIGTERM)


# Start poller thread in each executor / worker process
threading.Thread(target=_poll_interruption_notice, daemon=True).start()
```

### Multi-AZ Spot Fleet with Fallback

```json
{
  "SpotFleetRequestConfig": {
    "AllocationStrategy": "diversified",
    "TargetCapacity": 20,
    "IamFleetRole": "arn:aws:iam::123456789:role/SpotFleetRole",
    "LaunchSpecifications": [
      {
        "InstanceType": "r5.2xlarge",
        "SubnetId": "subnet-us-east-1a",
        "WeightedCapacity": 4
      },
      {
        "InstanceType": "r5.2xlarge",
        "SubnetId": "subnet-us-east-1b",
        "WeightedCapacity": 4
      },
      {
        "InstanceType": "r5a.2xlarge",
        "SubnetId": "subnet-us-east-1a",
        "WeightedCapacity": 4
      },
      {
        "InstanceType": "m5.4xlarge",
        "SubnetId": "subnet-us-east-1c",
        "WeightedCapacity": 4
      }
    ],
    "OnDemandTargetCapacity": 2,
    "SpotMaintenanceStrategies": {
      "CapacityRebalance": {"ReplacementStrategy": "priceCapacityOptimized"}
    }
  }
}
```

Savings: Spot instances typically cost **60–90% less** than on-demand. Moving 80% of Spark executors to spot with 2 on-demand fallback nodes cuts executor cost by ~70%.

---

## 5. Storage Optimization

### 5.1 Partitioning and File Layout

#### Partition Key Selection Rules

| Cardinality | Example | Verdict |
|---|---|---|
| < 10 distinct values | `status` (active/inactive) | Too low — useless partition; Z-order instead |
| 100–10,000 distinct values | `country` (200), `event_date` (365/yr) | Ideal partition key |
| > 100,000 distinct values | `user_id`, `order_id` | Too high — creates millions of tiny partitions |

Always partition on the column that appears in `WHERE` clauses of 80%+ of queries. `event_date` is the canonical choice for time-series data.

#### File Compaction (Delta Lake)

```sql
-- Compact small files in a partition, targeting 256 MB files
OPTIMIZE orders
WHERE event_date >= '2024-01-01'
ZORDER BY (customer_id, region);

-- Schedule as off-peak Airflow task
```

```python
# PySpark: manual compaction for non-Delta Parquet
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

# Read all small files in partition, write back as fewer large files
df = spark.read.parquet("s3://datalake/orders/event_date=2024-01-01/")

(
    df.repartition(8)           # 8 files × ~256MB = ~2GB partition
    .write
    .mode("overwrite")
    .parquet("s3://datalake/orders/event_date=2024-01-01/")
)
```

Compaction trigger thresholds:
- Run compaction when average file size drops below **32 MB** in a partition
- Or when file count in a partition exceeds **500 files**

#### Z-Order / Clustering for High-Cardinality Filter Columns

Z-ordering is for columns you filter on that have too high a cardinality to partition by. It co-locates rows with similar values in the same files, enabling data-skipping.

```sql
-- Delta Lake: Z-order on customer_id and product_category
-- (these are NOT partition columns — they are high-cardinality filter columns)
OPTIMIZE orders
ZORDER BY (customer_id, product_category);

-- Iceberg: sort order for clustering (write-time sort, not post-hoc)
ALTER TABLE orders
SET WRITE DISTRIBUTED BY PARTITION LOCALLY ORDERED BY (customer_id, product_category);

-- Verify data-skipping effect: check files_scanned before and after
-- Before Z-ORDER: EXPLAIN shows 10,000 files scanned for customer_id = 12345
-- After Z-ORDER:  EXPLAIN shows 12 files scanned for customer_id = 12345
```

#### Z-ORDER Cost vs Benefit Analysis

```
OPTIMIZE + ZORDER cost:
  - Reads all files in target partition(s): e.g. 500 GB read
  - Rewrites all files sorted: 500 GB write
  - S3 cost at $0.023/GB: (500 + 500) × $0.023 = $23 one-time

Daily savings (if top query runs 100× per day, scanning 100 GB without Z-ORDER):
  - Without Z-ORDER: 100 queries × 100 GB × $5/TB × 0.001 = $0.05/day scan cost (Athena proxy)
  - With Z-ORDER (99% skip rate): 100 queries × 1 GB × ... = $0.0005/day
  - Daily savings ≈ $0.05 - $0.0005 = ~$0.0495/day
  - Break-even: $23 / $0.0495 ≈ 465 days (not worth it at this scale)
  
Rule of thumb: Z-ORDER pays off when the workload scans > 1 TB/day on a column
and achieves > 80% skip rate, OR when compute cost (not scan cost) dominates.
```

#### Iceberg Table Maintenance

```sql
-- Expire old snapshots (keep last 7 days of history)
CALL catalog.system.expire_snapshots(
    table => 'schema.orders',
    older_than => TIMESTAMP '2024-01-01 00:00:00',
    retain_last => 5
);

-- Remove orphan files (unreferenced files left by failed writes)
CALL catalog.system.remove_orphan_files(
    table => 'schema.orders',
    older_than => TIMESTAMP '2024-01-01 00:00:00'
);

-- Schedule both as weekly Airflow maintenance tasks
```

### 5.2 Storage Tiers

#### S3 Tier Selection Guide

| Tier | Price (us-east-1) | Use Case | Break-Even vs Standard |
|---|---|---|---|
| S3 Standard | $0.023/GB-month | Hot data, daily access | — |
| S3 Standard-IA | $0.0125/GB-month | Silver layer, weekly access | Access < 1×/month |
| S3 Intelligent-Tiering | $0.023 + $0.0025/1k | Unknown access pattern | >30 days avg access interval |
| S3 Glacier Instant Retrieval | $0.004/GB-month | Cold archive, ms retrieval needed | Access < 1×/quarter |
| S3 Glacier Deep Archive | $0.00099/GB-month | Compliance archive, 12h retrieval OK | Access < 1×/year |

**S3 Intelligent-Tiering** break-even: worthwhile when >30% of objects are not accessed for >30 days. Enable on the raw/bronze bucket where access patterns are unpredictable.

#### S3 Lifecycle Rules (JSON)

```json
{
  "Rules": [
    {
      "ID": "bronze-to-ia-after-30d",
      "Status": "Enabled",
      "Filter": {"Prefix": "bronze/"},
      "Transitions": [
        {"Days": 30,  "StorageClass": "STANDARD_IA"},
        {"Days": 90,  "StorageClass": "GLACIER_IR"},
        {"Days": 365, "StorageClass": "DEEP_ARCHIVE"}
      ]
    },
    {
      "ID": "silver-to-ia-after-90d",
      "Status": "Enabled",
      "Filter": {"Prefix": "silver/"},
      "Transitions": [
        {"Days": 90,  "StorageClass": "STANDARD_IA"},
        {"Days": 730, "StorageClass": "GLACIER_IR"}
      ]
    },
    {
      "ID": "gold-keep-standard",
      "Status": "Enabled",
      "Filter": {"Prefix": "gold/"},
      "Transitions": [
        {"Days": 1825, "StorageClass": "STANDARD_IA"}
      ]
    },
    {
      "ID": "tmp-delete-after-7d",
      "Status": "Enabled",
      "Filter": {"Prefix": "tmp/"},
      "Expiration": {"Days": 7}
    }
  ]
}
```

Apply with Terraform:

```hcl
resource "aws_s3_bucket_lifecycle_configuration" "datalake_lifecycle" {
  bucket = aws_s3_bucket.datalake.id

  rule {
    id     = "bronze-tiering"
    status = "Enabled"
    filter { prefix = "bronze/" }

    transition {
      days          = 30
      storage_class = "STANDARD_IA"
    }
    transition {
      days          = 90
      storage_class = "GLACIER_IR"
    }
    transition {
      days          = 365
      storage_class = "DEEP_ARCHIVE"
    }
  }

  rule {
    id     = "tmp-cleanup"
    status = "Enabled"
    filter { prefix = "tmp/" }
    expiration { days = 7 }
  }
}
```

### 5.3 Format and Compression Comparison

| Format | Avg Size vs CSV | Query Performance | Write Speed | Best For |
|---|---|---|---|---|
| CSV | 1× (baseline) | Slow (no skip) | Fast | Input only |
| JSON | 1.3× (larger) | Very slow | Medium | API payloads |
| Avro | 0.7× | Medium (row scan) | Fast | Kafka, streaming |
| ORC | 0.3–0.4× | Fast (col skip) | Medium | Hive/Spark batch |
| Parquet | 0.2–0.35× | Fast (col skip) | Medium | Universal default |
| Parquet+zstd | 0.15–0.25× | Fast | Slower | Archive, large tables |

#### Compression Codec Decision

| Codec | Ratio | Compress Speed | Decompress Speed | Use |
|---|---|---|---|---|
| snappy | Moderate | Very fast | Very fast | Default Parquet — best latency |
| lz4 | Moderate | Very fast | Very fast | Streaming sinks |
| zstd (level 3) | Best | Fast | Fast | Archival, large batch jobs |
| gzip | Good | Slow | Medium | Interoperability with older tools |
| brotli | Best | Very slow | Fast | Read-heavy cold storage |

```python
# Write Parquet with zstd compression (best ratio for archival)
df.write \
    .option("compression", "zstd") \
    .parquet("s3://archive/orders/")

# Write Parquet with snappy (default — best for hot/interactive)
df.write \
    .option("compression", "snappy") \
    .parquet("s3://datalake/orders/")
```

#### Column Pruning Impact

Reading only the columns needed reduces scan cost proportionally to column width.

```python
# Expensive: reads all 200 columns
df = spark.read.parquet("s3://datalake/events/")

# Cheap: reads only 5 of 200 columns — 97.5% scan reduction
df = spark.read.parquet("s3://datalake/events/").select(
    "event_id", "user_id", "event_type", "event_time", "amount"
)
```

In Trino, `SELECT *` from a wide Parquet table is the single easiest cost win to avoid — always select only needed columns.

---

## 6. Materialized View Economics

### When to Materialize

Apply the **10-10-15 rule**: materialize when the query:
1. Runs more than **10 times per day**
2. Takes more than **10 seconds** (or scans > 10 GB) each run
3. Has a stale-data tolerance of more than **15 minutes**

### Refresh Cost vs Query Savings — Break-Even Formula

```
Daily query cost (without MV) = query_count_per_day × scan_GB × $/GB
Daily refresh cost (with MV)  = refreshes_per_day × full_table_scan_GB × $/GB

Break-even condition:
  refreshes_per_day × full_table_scan_GB < query_count_per_day × scan_GB × hit_rate

Example:
  Query runs 200×/day, scans 50 GB each: cost = 200 × 50 × $0.005 = $50/day
  MV refresh every 15 min (96 refreshes/day), full scan = 200 GB:
    cost = 96 × 200 × $0.005 = $96/day   → NOT WORTH IT

  MV refresh every 1h (24 refreshes/day), incremental scan = 10 GB:
    cost = 24 × 10 × $0.005 = $1.20/day  → SAVINGS = $50 - $1.20 = $48.80/day ✓
```

### ClickHouse Materialized Views: Zero-Cost Streaming Aggregation

ClickHouse MVs process only new rows as they are inserted — no periodic full refresh cost.

```sql
-- Source table
CREATE TABLE orders_raw (
    order_id  UInt64,
    region    LowCardinality(String),
    amount    Float64,
    created_at DateTime
) ENGINE = MergeTree()
ORDER BY (created_at, region);

-- AggregatingMergeTree target for the MV
CREATE TABLE orders_hourly_agg (
    hour      DateTime,
    region    LowCardinality(String),
    order_count AggregateFunction(count),
    total_amount AggregateFunction(sum, Float64)
) ENGINE = AggregatingMergeTree()
ORDER BY (hour, region);

-- Materialized view: fires on every INSERT to orders_raw, zero extra cost
CREATE MATERIALIZED VIEW orders_hourly_mv TO orders_hourly_agg AS
SELECT
    toStartOfHour(created_at)   AS hour,
    region,
    countState()                AS order_count,
    sumState(amount)            AS total_amount
FROM orders_raw
GROUP BY hour, region;

-- Query using -Merge combinator to finalize aggregates
SELECT
    hour,
    region,
    countMerge(order_count)     AS orders,
    sumMerge(total_amount)      AS revenue
FROM orders_hourly_agg
WHERE hour >= NOW() - INTERVAL 7 DAY
GROUP BY hour, region
ORDER BY hour, region;
```

### Trino / Spark: CTAS-Based Materialized Table + Scheduled Refresh

```sql
-- Step 1: create materialized summary table (run once)
CREATE TABLE analytics.orders_daily_summary
WITH (
    format = 'PARQUET',
    partitioned_by = ARRAY['summary_date']
)
AS
SELECT
    DATE(order_time)               AS summary_date,
    region,
    COUNT(*)                       AS order_count,
    SUM(amount)                    AS revenue,
    COUNT(DISTINCT customer_id)    AS unique_customers
FROM raw.orders
GROUP BY DATE(order_time), region;
```

```python
# Airflow DAG: incremental refresh of materialized summary (runs hourly)
from airflow.sdk import dag, task
import pendulum

@dag(
    dag_id="refresh_orders_daily_summary",
    schedule="@hourly",
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    catchup=False,
)
def refresh_summary():

    @task
    def refresh_last_2_days():
        from trino import dbapi
        conn = dbapi.connect(host="trino", port=8080, user="etl")
        cur = conn.cursor()
        # Delete and re-insert only the last 2 days (incremental, not full scan)
        cur.execute("""
            DELETE FROM analytics.orders_daily_summary
            WHERE summary_date >= CURRENT_DATE - INTERVAL '2' DAY
        """)
        cur.execute("""
            INSERT INTO analytics.orders_daily_summary
            SELECT
                DATE(order_time),
                region,
                COUNT(*),
                SUM(amount),
                COUNT(DISTINCT customer_id)
            FROM raw.orders
            WHERE DATE(order_time) >= CURRENT_DATE - INTERVAL '2' DAY
            GROUP BY DATE(order_time), region
        """)
        conn.commit()

    refresh_last_2_days()

refresh_summary()
```

---

## 7. Data Lifecycle and Retention Policies

### Retention Policy Matrix

| Layer | Hot Retention | Warm Retention | Cold Archive | Delete After |
|---|---|---|---|---|
| Bronze (raw ingest) | 30 days S3 Standard | 60 days S3-IA | 90 days Glacier | 1 year |
| Silver (cleaned) | 90 days S3 Standard | 2 years S3-IA | — | 2 years |
| Gold (aggregated) | 1 year S3 Standard | 4 years S3-IA | — | 5 years |
| PII / regulated data | Per compliance policy | Encrypted vault | — | Per legal hold |
| Temp / scratch | 7 days auto-delete | — | — | 7 days |
| Checkpoint / metadata | Duration of job | — | — | 30 days after job end |

### Implementing Retention via Delta VACUUM

```sql
-- Delete data older than retention threshold (Delta Lake)
-- VACUUM removes files no longer referenced by the table; default retention = 7 days
VACUUM orders RETAIN 168 HOURS;  -- keep 7 days of time-travel; delete older files

-- Set table-level retention property
ALTER TABLE orders SET TBLPROPERTIES (
    'delta.deletedFileRetentionDuration' = 'interval 7 days',
    'delta.logRetentionDuration'         = 'interval 30 days'
);
```

```python
# Airflow task: weekly VACUUM for all Delta tables
from airflow.sdk import dag, task
import pendulum

@dag(
    dag_id="delta_maintenance",
    schedule="0 2 * * 0",   # every Sunday 02:00 UTC
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    catchup=False,
)
def delta_maintenance():

    @task
    def vacuum_tables():
        from pyspark.sql import SparkSession
        spark = SparkSession.builder.getOrCreate()
        tables = [
            "datalake.bronze.orders",
            "datalake.bronze.events",
            "datalake.silver.orders_clean",
        ]
        for table in tables:
            spark.sql(f"VACUUM {table} RETAIN 168 HOURS")

    vacuum_tables()

delta_maintenance()
```

### Implementing Retention via Iceberg expire_snapshots

```sql
-- Airflow-triggered Trino call
CALL catalog.system.expire_snapshots(
    table        => 'bronze.orders',
    older_than   => CURRENT_TIMESTAMP - INTERVAL '30' DAY,
    retain_last  => 3,
    max_concurrent_deletes => 10
);

CALL catalog.system.remove_orphan_files(
    table      => 'bronze.orders',
    older_than => CURRENT_TIMESTAMP - INTERVAL '7' DAY
);
```

---

## 8. Cost Attribution and Tagging

### AWS Resource Tagging Strategy

All data engineering resources must carry these tags consistently:

| Tag Key | Values | Purpose |
|---|---|---|
| `team` | `analytics`, `platform`, `ml` | Chargeback per team |
| `project` | `orders-etl`, `user-360`, `recsys` | Project-level cost |
| `env` | `prod`, `staging`, `dev` | Separate prod vs non-prod spend |
| `dag_id` | `orders_daily_load` | Attribute cost to Airflow DAG |
| `cost_center` | `CC-1234` | Finance chargeback code |
| `data_layer` | `bronze`, `silver`, `gold` | Medallion layer cost visibility |

### Tagging EMR Clusters from Airflow

```python
from airflow.providers.amazon.aws.operators.emr import EmrCreateJobFlowOperator

emr_task = EmrCreateJobFlowOperator(
    task_id="create_emr_cluster",
    aws_conn_id="aws_default",
    job_flow_overrides={
        "Name": "orders-daily-load",
        "ReleaseLabel": "emr-7.0.0",
        "Instances": {
            "MasterInstanceType": "m5.xlarge",
            "SlaveInstanceType":  "r5.2xlarge",
            "InstanceCount": 5,
        },
        "Tags": [
            {"Key": "team",         "Value": "analytics"},
            {"Key": "project",      "Value": "orders-etl"},
            {"Key": "env",          "Value": "prod"},
            {"Key": "dag_id",       "Value": "orders_daily_load"},
            {"Key": "cost_center",  "Value": "CC-1234"},
        ],
        "Applications": [{"Name": "Spark"}],
        "JobFlowRole": "EMR_EC2_DefaultRole",
        "ServiceRole": "EMR_DefaultRole",
    },
)
```

### Spark Job Tagging

```python
spark = (
    SparkSession.builder
    .appName("orders_daily_load")
    .config("spark.yarn.tags", "team=analytics,project=orders-etl,env=prod")
    # On K8s: labels propagate to pod metadata → show up in K8s cost tools
    .config("spark.kubernetes.driver.label.team",        "analytics")
    .config("spark.kubernetes.driver.label.project",     "orders-etl")
    .config("spark.kubernetes.executor.label.team",      "analytics")
    .config("spark.kubernetes.executor.label.project",   "orders-etl")
    .getOrCreate()
)
```

### Trino Query Tagging

```sql
-- Prepend a comment tag to every Trino query from your application
-- Trino records source + query text; the tag becomes searchable in system.runtime.query_history

-- In application code:
SELECT /* @tag team=analytics project=orders-etl dag_id=orders_daily */ 
    customer_id, SUM(amount)
FROM orders
WHERE order_date = CURRENT_DATE
GROUP BY customer_id;
```

```python
# Python: inject tags into every query via connection wrapper
import trino

class TaggedTrinoConnection:
    def __init__(self, host, port, user, tags: dict):
        self._conn = trino.dbapi.connect(
            host=host, port=port, user=user,
            source=",".join(f"{k}={v}" for k, v in tags.items()),
        )

    def execute(self, sql: str):
        cur = self._conn.cursor()
        tag_comment = " ".join(f"@{k}={v}" for k, v in self._tags.items())
        cur.execute(f"/* {tag_comment} */ {sql}")
        return cur
```

### Cost per DAG Run Calculation

```python
# Post-run cost attribution: query EMR billing + query bytes
import boto3
from datetime import datetime

def calculate_dag_run_cost(
    cluster_id: str,
    start_time: datetime,
    end_time: datetime,
    instance_type: str = "r5.2xlarge",
    instance_count: int = 5,
) -> dict:
    # On-demand price (look up from AWS Price List API or hardcode)
    INSTANCE_PRICES = {"r5.2xlarge": 0.504, "m5.xlarge": 0.192}
    hourly_rate = INSTANCE_PRICES.get(instance_type, 0.5)
    
    duration_hours = (end_time - start_time).total_seconds() / 3600
    compute_cost = duration_hours * instance_count * hourly_rate
    
    # Spot savings (if 80% executors on spot at 30% of on-demand)
    spot_savings = compute_cost * 0.8 * 0.7
    effective_cost = compute_cost - spot_savings
    
    return {
        "dag_id": "orders_daily_load",
        "cluster_id": cluster_id,
        "duration_hours": round(duration_hours, 3),
        "on_demand_cost_usd": round(compute_cost, 4),
        "effective_cost_usd": round(effective_cost, 4),
        "spot_savings_usd": round(spot_savings, 4),
    }
```

---

## 9. Budget Alerts and FinOps Metrics

### AWS Budgets Setup (Terraform)

```hcl
# Terraform: budget alert per team tag, alert at 80% and 100% of monthly limit

resource "aws_budgets_budget" "analytics_team" {
  name              = "analytics-team-monthly"
  budget_type       = "COST"
  limit_amount      = "5000"
  limit_unit        = "USD"
  time_unit         = "MONTHLY"

  cost_filter {
    name   = "TagKeyValue"
    values = ["user:team$analytics"]
  }

  notification {
    comparison_operator        = "GREATER_THAN"
    threshold                  = 80
    threshold_type             = "PERCENTAGE"
    notification_type          = "FORECASTED"
    subscriber_email_addresses = ["data-eng-lead@company.com"]
  }

  notification {
    comparison_operator        = "GREATER_THAN"
    threshold                  = 100
    threshold_type             = "PERCENTAGE"
    notification_type          = "ACTUAL"
    subscriber_email_addresses = ["data-eng-lead@company.com", "finops@company.com"]
    subscriber_sns_topic_arns  = [aws_sns_topic.cost_alerts.arn]
  }
}

resource "aws_sns_topic" "cost_alerts" {
  name = "de-cost-alerts"
}

resource "aws_sns_topic_subscription" "cost_alert_slack" {
  topic_arn = aws_sns_topic.cost_alerts.arn
  protocol  = "https"
  endpoint  = var.slack_webhook_url   # via Lambda proxy for SNS → Slack
}

# Anomaly detection: alert on unexpected cost spikes (>20% WoW)
resource "aws_ce_anomaly_monitor" "de_services" {
  name              = "de-services-monitor"
  monitor_type      = "DIMENSIONAL"
  monitor_dimension = "SERVICE"
}

resource "aws_ce_anomaly_subscription" "de_alert" {
  name      = "de-cost-anomaly-alert"
  frequency = "DAILY"

  monitor_arn_list = [aws_ce_anomaly_monitor.de_services.arn]

  subscriber {
    type    = "SNS"
    address = aws_sns_topic.cost_alerts.arn
  }

  threshold_expression {
    dimension {
      key           = "ANOMALY_TOTAL_IMPACT_PERCENTAGE"
      values        = ["20"]
      match_options = ["GREATER_THAN_OR_EQUAL"]
    }
  }
}
```

### Key FinOps KPIs

| KPI | Formula | Target | Frequency |
|---|---|---|---|
| Cost per TB processed | `total_compute_cost / TB_processed` | < $2/TB | Weekly |
| Cost per DAG run | `cluster_cost × duration / concurrent_runs` | Establish baseline | Per run |
| Storage cost per GB-month | `storage_bill / total_GB` | < $0.025/GB | Monthly |
| Spot coverage % | `spot_hours / total_hours × 100` | > 70% | Weekly |
| Idle cluster hours | Hours with avg CPU < 10% | < 5% of total | Daily |
| Query scan efficiency | `avg result_rows / avg read_rows` | > 10% | Weekly |
| MV cache hit rate | `MV queries / total queries on base tables` | > 60% | Weekly |

### Weekly Cost Report Query

```sql
-- Trino: weekly cost report from query history
SELECT
    DATE_TRUNC('day', created)                                    AS query_day,
    source                                                        AS team_source,
    COUNT(*)                                                      AS query_count,
    ROUND(SUM(read_data_size) / 1e12, 3)                         AS tb_scanned,
    -- Cost proxy at $5/TB (adjust to your Trino cluster hourly cost / throughput)
    ROUND(SUM(read_data_size) / 1e12 * 5.0, 2)                   AS est_scan_cost_usd,
    ROUND(AVG(DATE_DIFF('second', created, "end")), 1)            AS avg_wall_sec,
    ROUND(PERCENTILE_APPROX(read_data_size, 0.95) / 1e9, 1)      AS p95_scan_gb,
    -- Queries that scanned >100 GB are candidates for optimization
    COUNT_IF(read_data_size > 100e9)                              AS heavy_queries
FROM system.runtime.query_history
WHERE created >= NOW() - INTERVAL '7' DAY
  AND state = 'FINISHED'
GROUP BY DATE_TRUNC('day', created), source
ORDER BY query_day DESC, tb_scanned DESC;
```

```sql
-- ClickHouse: daily cost report
SELECT
    toDate(event_time)              AS query_day,
    user,
    COUNT(*)                        AS query_count,
    ROUND(SUM(read_bytes) / 1e12, 3) AS tb_read,
    ROUND(AVG(query_duration_ms) / 1000, 1) AS avg_sec,
    ROUND(quantile(0.95)(read_bytes) / 1e9, 1) AS p95_read_gb
FROM system.query_log
WHERE event_time >= NOW() - INTERVAL 7 DAY
  AND type = 'QueryFinish'
  AND query_kind = 'Select'
GROUP BY query_day, user
ORDER BY query_day DESC, tb_read DESC;
```

### Grafana Dashboard Layout for Cost Visibility

```
Row 1 — Overview
  [Stat] Total monthly spend (from AWS CUR via Athena)
  [Stat] Spot coverage % (from EC2 usage tags)
  [Stat] Top 3 cost drivers (services)
  [Stat] MoM cost change %

Row 2 — Compute
  [Time series] Daily compute cost by team (EMR + EKS tags)
  [Bar chart]   Cost per DAG run (top 10 DAGs)
  [Gauge]       Avg cluster CPU utilization (7-day rolling)

Row 3 — Storage
  [Pie chart]   Storage by tier (Standard / IA / Glacier)
  [Time series] S3 storage growth by prefix (bronze/silver/gold)
  [Stat]        Data older than retention policy (GB to clean up)

Row 4 — Query Efficiency
  [Time series] Daily TB scanned (Trino query_history)
  [Table]       Top 10 heavy queries this week (query_id, user, scan_gb, cost)
  [Gauge]       Query scan efficiency % (result_rows / read_rows)
```

---

## 10. Quick Wins Checklist

Ordered by **effort ÷ impact** (lowest effort, highest impact first):

- [ ] **Enable S3 Intelligent-Tiering on the raw/bronze bucket** — zero code change, saves 30–50% on infrequently accessed data; break-even in 30 days. Apply via Terraform lifecycle rule or S3 console.

- [ ] **Move Spark executors to Spot instances** — set `instanceFleets` with `diversified` allocation strategy across 3+ instance types and 2+ AZs. Typical savings: 60–80% of executor compute cost. Requires checkpointing + idempotent writes.

- [ ] **Add partition filters to top-10 most expensive queries** — run the query cost analysis SQL above, identify queries missing `WHERE event_date = ...`, add filters. Each missing filter likely scans the entire table.

- [ ] **Schedule OPTIMIZE/compaction during off-peak hours** — if any partition has >200 files averaging <32 MB, run `OPTIMIZE` (Delta) or a repartition job weekly during 02:00–05:00 UTC. Reduces scan overhead 5–20×.

- [ ] **Delete data beyond retention policy** — query `s3://` bucket with `s3 ls --recursive` or S3 Inventory, identify objects older than policy thresholds. Delete or apply lifecycle rules. Often finds 10–30% of storage bill in forgotten old data.

- [ ] **Right-size over-provisioned clusters** — use the CloudWatch CPU utilization query above; clusters below 20% average CPU can be halved. This is often the single largest savings action.

- [ ] **Add `expire_snapshots` to Iceberg maintenance DAG** — snapshot accumulation is silent: each snapshot holds references to old data files that cannot be deleted. Weekly expire_snapshots runs free up storage immediately.

- [ ] **Enable ClickHouse TTL for old partitions** — add `TTL event_date + INTERVAL N DAY DELETE` or `MOVE TO VOLUME 'cold'` to tables with historical data. Moves cold data to cheaper storage automatically.

- [ ] **Select only needed columns in queries** — audit top queries for `SELECT *` on wide tables. Column projection is free to add and reduces scan proportionally to column count ratio.

- [ ] **Enable `require_partition_filter` on large BigQuery tables** — prevents accidental full-table scans by rejecting queries without a partition predicate.

---

## 11. Anti-Patterns

| Anti-Pattern | Problem | Fix |
|---|---|---|
| Always-on cluster for batch jobs | Cluster idles 16h/day, billed 24h | Use transient clusters (EMR step mode) or auto-terminate after job |
| `SELECT *` on wide Parquet tables | Scans all columns even if 2 are needed | Explicitly list required columns |
| No partition filter on large table queries | Full table scan on every run | Add `WHERE event_date = ...` or enforce `require_partition_filter` |
| Spot driver node on Spark | Job dies unrecoverably if driver interrupted | Driver must always be on-demand |
| Single-AZ spot fleet | AZ spot price spike evicts entire fleet | Always span 3+ AZs and 3+ instance types |
| Appending to non-partitioned table | Scan grows unbounded with every row | Partition by time; use overwrite for idempotent runs |
| No file compaction | Millions of tiny files accumulate; every query opens thousands of S3 readers | Weekly OPTIMIZE / repartition job |
| Infinite snapshot/changelog retention | Iceberg/Delta metadata balloons storage | Weekly `expire_snapshots` + `VACUUM` |
| No retention policy | Data accumulates indefinitely on S3 Standard | Define policy matrix; apply lifecycle rules immediately |
| Materializing everything eagerly | Refresh cost exceeds query savings | Use 10-10-15 rule before materializing |
| Tagging only in code comments | Tags absent from AWS Cost Explorer → no chargeback visibility | Enforce tags via AWS tag policies + SCP; fail deployments without required tags |
| Unmonitored budget with no alerts | Cost spike discovered at month-end bill | AWS Budgets alert at 80% forecasted + anomaly detection |
| Compression choice ignored | gzip doubles CPU time vs snappy for same ratio in hot path | Use snappy for hot, zstd for cold/archive |
| Cross-region data reads | $0.02/GB transfer cost invisible in query metrics | Co-locate Trino/Spark cluster with S3 bucket in same region |

---

## 12. References to Consult When Needed

- `skills/trino_iceberg/SKILL.md` — EXPLAIN plan reading, Iceberg maintenance procedures, partition transform design
- `skills/spark_sql/SKILL.md` — Spark DDL, partition overwrite, dynamic partition pruning
- `skills/pyspark_etl/SKILL.md` — Executor configuration, AQE, broadcast join thresholds
- `skills/delta_lake/SKILL.md` — OPTIMIZE, Z-ORDER, VACUUM, RESTORE, Change Data Feed
- `skills/clickhouse_olap/SKILL.md` — MergeTree engine selection, TTL, materialized view -State/-Merge pattern, projections
- `skills/medallion_architecture/SKILL.md` — Bronze/Silver/Gold layer design, deduplication strategies, DQ gates
- `skills/airflow_dags/SKILL.md` — DAG authoring, TaskFlow, dynamic task mapping for parallel compaction
- `skills/kubernetes_data/SKILL.md` — Spark-on-K8s, resource quotas, LimitRange, pod labels for cost tools
- `skills/github_actions_dataops/SKILL.md` — OIDC for AWS (no static keys), CI/CD cost gates
- [AWS Cost Optimization Pillar — Well-Architected Framework](https://docs.aws.amazon.com/wellarchitected/latest/cost-optimization-pillar/welcome.html)
- [AWS Spot Instance Best Practices](https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/spot-best-practices.html)
- [Delta Lake OPTIMIZE Documentation](https://docs.delta.io/latest/optimizations-oss.html)
- [Iceberg Table Maintenance — Apache Docs](https://iceberg.apache.org/docs/latest/maintenance/)
- [FinOps Framework — finops.org](https://www.finops.org/framework/)
