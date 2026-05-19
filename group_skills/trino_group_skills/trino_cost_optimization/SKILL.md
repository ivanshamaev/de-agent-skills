---
name: trino-cost-optimization
description: Trino warehouse cost optimization — query scan cost analysis (system.runtime.queries/system.runtime.tasks), identifying expensive queries by CPU time and data scanned, scan reduction via partition pruning and file compaction, worker autoscaling patterns (scale-to-zero for batch), spot instance strategies for workers, S3 object storage cost (storage vs request costs), Iceberg compaction economics (fewer splits = fewer S3 GET requests), cost attribution by team/user, materialized view break-even analysis, result caching
---

# Trino Cost Optimization

## When to Use

- Cloud infrastructure bill is growing faster than data volume
- Identifying which queries/teams are consuming the most resources
- Reducing S3 GET request costs from excessive small-file scans
- Rightsizing worker instance types for workload patterns
- Implementing cost attribution for chargeback reporting

---

## Query Cost Analysis

### Identify Top Resource Consumers

```sql
-- Top 20 most expensive queries by CPU time (last 24h)
SELECT
    q.query_id,
    q.user,
    q.source,
    ROUND(q.total_cpu_time_secs / 3600.0, 2)   AS cpu_hours,
    ROUND(q.total_bytes_read / 1e9, 2)           AS gb_scanned,
    ROUND(q.wall_time_secs / 60.0, 1)            AS wall_min,
    q.state,
    SUBSTR(q.query, 1, 120)                      AS query_preview
FROM system.runtime.queries q
WHERE q.create_time >= NOW() - INTERVAL '24' HOUR
  AND q.state = 'FINISHED'
ORDER BY q.total_cpu_time_secs DESC
LIMIT 20;
```

```sql
-- Cost per team (proxy: CPU seconds as cost unit)
SELECT
    source,
    user,
    COUNT(*)                                          AS query_count,
    ROUND(SUM(total_cpu_time_secs) / 3600.0, 1)      AS total_cpu_hours,
    ROUND(SUM(total_bytes_read) / 1e12, 3)            AS total_tb_scanned,
    ROUND(AVG(wall_time_secs), 1)                     AS avg_wall_sec,
    COUNT(*) FILTER (WHERE state = 'FAILED')          AS failed_queries
FROM system.runtime.queries
WHERE create_time >= NOW() - INTERVAL '7' DAY
GROUP BY source, user
ORDER BY total_cpu_hours DESC
LIMIT 30;
```

```sql
-- Queries scanning more than 1TB (expensive full-table scans)
SELECT
    query_id,
    user,
    ROUND(total_bytes_read / 1e12, 2) AS tb_scanned,
    ROUND(total_cpu_time_secs / 3600.0, 2) AS cpu_hours,
    SUBSTR(query, 1, 200) AS query_preview
FROM system.runtime.queries
WHERE create_time >= NOW() - INTERVAL '7' DAY
  AND total_bytes_read > 1e12          -- > 1TB
  AND state = 'FINISHED'
ORDER BY total_bytes_read DESC
LIMIT 20;
```

---

## Estimated Query Cost Model

```python
# Simplified cost model for Trino on AWS
# Adjust coefficients for your cloud provider and instance pricing

def estimate_query_cost(
    cpu_time_sec: float,
    bytes_scanned: float,
    worker_hourly_rate_usd: float = 0.30,   # e.g. r5.2xlarge spot
    s3_get_cost_per_1k: float = 0.0004,      # S3 GET requests
    avg_rows_per_s3_get: int = 50000,         # rows per Parquet split
) -> dict:

    compute_cost = (cpu_time_sec / 3600) * worker_hourly_rate_usd
    s3_get_requests = bytes_scanned / (128 * 1024 * 1024)          # ~1 GET per 128MB split
    s3_cost = (s3_get_requests / 1000) * s3_get_cost_per_1k

    return {
        "compute_cost_usd": round(compute_cost, 4),
        "s3_get_cost_usd":  round(s3_cost, 6),
        "total_cost_usd":   round(compute_cost + s3_cost, 4),
    }
```

---

## Scan Reduction Strategies

### 1. Enforce Partition Filters

Use `query-partition-filter-required` to force users to provide partition predicates on large tables:

```properties
# etc/catalog/iceberg.properties
iceberg.query-partition-filter-required=true   # fails queries without partition filter
iceberg.query-partition-filter-required-schemas=gold,silver
```

### 2. Limit Physical Data Scan per Query

```json
// etc/rules.json — resource group with scan limit
{
  "tables": [
    {
      "group":   "analysts",
      "catalog": "iceberg",
      "schema":  "gold",
      "privileges": ["SELECT"],
      "hardPhysicalDataScanLimit": "500GB"   // reject queries scanning > 500GB
    }
  ]
}
```

```properties
# Per-query session limit
SET SESSION query_max_scan_physical_bytes = '200GB';
```

### 3. Compaction Reduces S3 Costs

More files = more S3 GET requests per query:

```
Before compaction: 10,000 × 10MB files → 10,000 S3 GETs per scan
After compaction:     200 × 500MB files →    200 S3 GETs per scan
Cost reduction: 98% fewer S3 GET requests
```

```sql
-- Run compaction to reduce file count and S3 requests
ALTER TABLE iceberg.silver.orders EXECUTE optimize(file_size_threshold => '256MB');
```

---

## Worker Autoscaling

### Kubernetes HPA

```yaml
apiVersion: autoscaling/v2
kind: HorizontalPodAutoscaler
metadata:
  name: trino-worker-hpa
spec:
  scaleTargetRef:
    apiVersion: apps/v1
    kind: Deployment
    name: trino-worker
  minReplicas: 2      # keep warm for interactive queries
  maxReplicas: 20
  metrics:
    - type: External
      external:
        metric:
          name: trino_queued_queries    # from Prometheus adapter
        target:
          type: AverageValue
          averageValue: "5"             # scale up when > 5 queries queued per worker
  behavior:
    scaleUp:
      stabilizationWindowSeconds: 60
      policies:
        - type:          Pods
          value:         4
          periodSeconds: 60             # add up to 4 workers/min
    scaleDown:
      stabilizationWindowSeconds: 600   # 10-min cooldown before scale-down
      policies:
        - type:          Pods
          value:         2
          periodSeconds: 120
```

### Scale-to-Zero for Batch Workloads

For batch jobs that run nightly, scale workers to 0 during idle hours:

```python
# Airflow DAG: scale Trino workers before/after batch run
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator

scale_up = KubernetesPodOperator(
    task_id = 'scale_trino_workers_up',
    image   = 'bitnami/kubectl:latest',
    cmds    = ['kubectl', 'scale', 'deployment/trino-worker',
               '--replicas=10', '-n', 'trino'],
)

scale_down = KubernetesPodOperator(
    task_id = 'scale_trino_workers_down',
    image   = 'bitnami/kubectl:latest',
    cmds    = ['kubectl', 'scale', 'deployment/trino-worker',
               '--replicas=2', '-n', 'trino'],
)

scale_up >> run_batch_pipeline >> scale_down
```

---

## Spot Instance Strategy

Use spot/preemptible instances for workers (not coordinator):

```yaml
# Kubernetes node selector for spot workers
worker:
  nodeSelector:
    node.kubernetes.io/lifecycle: spot
  tolerations:
    - key:    "node.kubernetes.io/lifecycle"
      value:  "spot"
      effect: "NoSchedule"
```

```yaml
# AWS Node Group with mixed instance types
ManagedNodeGroups:
  - name: trino-workers-spot
    instanceTypes: ["r5.4xlarge", "r5a.4xlarge", "r5d.4xlarge"]
    spot: true
    minSize: 2
    maxSize: 20
```

Enable fault-tolerant execution so spot interruptions don't fail queries:

```properties
# etc/config.properties
retry-policy=TASK
task-retry-attempts-per-task=4
```

---

## Materialized View Break-Even

A materialized view is worth it when:

```
query_frequency × query_scan_cost > refresh_cost + storage_cost
```

```sql
-- Estimate MV break-even
WITH costs AS (
    SELECT
        -- How much the base query costs per run
        ROUND(total_bytes_read / 1e12, 3) AS base_tb_per_run,
        ROUND(total_cpu_time_secs / 3600.0, 2) AS base_cpu_hours
    FROM system.runtime.queries
    WHERE query LIKE '%FROM iceberg.silver.orders%GROUP BY%'
      AND create_time >= NOW() - INTERVAL '7' DAY
    ORDER BY create_time DESC
    LIMIT 10
)
SELECT
    AVG(base_tb_per_run)                    AS avg_tb_per_run,
    AVG(base_cpu_hours)                     AS avg_cpu_hours_per_run,
    COUNT(*) * 7.0 / 7                      AS runs_per_day,
    -- If MV refresh scans same data once/day:
    'Breakeven at ' || CAST(ROUND(1.0 / (COUNT(*) * 7.0 / 7), 1) AS VARCHAR) || ' runs/day' AS breakeven
FROM costs;
```

---

## Cost Attribution Reporting

```python
from airflow.providers.trino.hooks.trino import TrinoHook

def generate_weekly_cost_report() -> list[dict]:
    hook = TrinoHook(trino_conn_id='trino_default')

    rows = hook.get_records("""
        SELECT
            COALESCE(source, 'unknown')                               AS team,
            user,
            COUNT(*)                                                  AS queries,
            ROUND(SUM(total_cpu_time_secs) / 3600.0, 1)              AS cpu_hours,
            ROUND(SUM(total_bytes_read) / 1e12, 3)                    AS tb_scanned,
            -- Estimated cost: $0.30/cpu-hour + $0.0004/1000 S3 GETs
            ROUND(
                SUM(total_cpu_time_secs) / 3600.0 * 0.30 +
                SUM(total_bytes_read) / (128e6) / 1000 * 0.0004,
                2
            ) AS estimated_cost_usd
        FROM system.runtime.queries
        WHERE create_time >= NOW() - INTERVAL '7' DAY
          AND state = 'FINISHED'
        GROUP BY 1, 2
        ORDER BY estimated_cost_usd DESC
        LIMIT 50
    """)

    return [
        {'team': r[0], 'user': r[1], 'queries': r[2],
         'cpu_hours': r[3], 'tb_scanned': r[4], 'cost_usd': r[5]}
        for r in rows
    ]
```

---

## Storage Cost Optimization

```sql
-- Find over-retained data (partitions older than policy)
SELECT
    partition,
    ROUND(total_size / 1e9, 1)      AS total_gb,
    record_count,
    file_count
FROM iceberg.bronze."orders_raw$partitions"
WHERE CAST(partition.ingested_date AS DATE) < CURRENT_DATE - INTERVAL '30' DAY
ORDER BY total_size DESC;

-- Drop old Bronze partitions (3× faster and cheaper than DELETE)
ALTER TABLE iceberg.bronze.orders_raw
DROP PARTITION WHERE ingested_date < DATE '2024-01-01';
```

```sql
-- Find tables without compression (uncompressed Parquet wastes storage)
SELECT table_name, table_schema,
       element_at(properties, 'compression_codec') AS codec
FROM iceberg.information_schema.tables
WHERE table_schema IN ('bronze', 'silver', 'gold')
  AND element_at(properties, 'compression_codec') IS DISTINCT FROM 'ZSTD';
```

---

## Anti-Patterns

1. **No scan limit for analyst queries** — an analyst accidentally running `SELECT * FROM iceberg.silver.events` (5TB) costs as much as running 50 normal queries; always set `hardPhysicalDataScanLimit` per group.
2. **Keeping Bronze data forever** — raw ingest tables grow unbounded; define a retention policy (e.g., 30 days for Bronze, 1 year for Silver) and automate partition drops.
3. **Running large batch jobs during business hours** — batch workloads compete with interactive analysts for the same workers; schedule heavy batches at night with dedicated resource groups.
4. **Spot workers without fault-tolerant execution** — a spot interruption without FTE retries kills all in-flight queries; always pair spot workers with `retry-policy=TASK`.
5. **Not compacting Iceberg tables** — 10,000 small files means 10,000 S3 GET requests per full-table scan; compacting to 200 files reduces S3 costs by ~98%.

---

## References

- Query system tables: `trino.io/docs/current/connector/system.html`
- Resource group scan limits: `trino.io/docs/current/admin/resource-groups.html`
- Fault-tolerant execution: `trino.io/docs/current/admin/fault-tolerant-execution.html`
- Related skills: `[[trino-admin-cluster-health]]`, `[[trino-resource-group-governance]]`, `[[trino-file-layout-optimization]]`, `[[trino-iceberg-best-practices]]`
